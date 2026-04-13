from __future__ import annotations

import cgi
import csv
import json
import mimetypes
import re
import uuid
from datetime import datetime
from http import HTTPStatus
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote, unquote, urlparse

from .assistant import FinancialQAEngine
from .config import AppConfig
from .database import database_status, ingest_all
from .pdf_tools import extract_text_safe
from .question_bank import build_question_bank_payload, build_sample_questions
from .utils import ensure_relative_path, normalize_text, parse_question_payload
from .xlsx_tools import read_workbook, write_simple_xlsx


SESSION_COOKIE_NAME = "financial_qa_session"
VALID_TABLE_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
TEXT_SUFFIXES = {".txt", ".md", ".json", ".py", ".yaml", ".yml", ".log"}
SHEET_SUFFIXES = {".xlsx", ".xlsm"}
INLINE_FILE_SUFFIXES = {".svg", ".png", ".jpg", ".jpeg", ".webp", ".pdf"}
DOWNLOADABLE_SUFFIXES = INLINE_FILE_SUFFIXES | {".xlsx", ".json", ".csv", ".zip", ".txt", ".md"}
UPLOAD_SOURCE_SUFFIXES = {".xlsx", ".xlsm", ".pdf", ".csv", ".json", ".txt"}
MAX_HISTORY = 60
MAX_CUSTOM_QUESTIONS = 200


def _iso_now() -> str:
    return datetime.now().isoformat(timespec="seconds")



def _format_bytes(size: int) -> str:
    units = ["B", "KB", "MB", "GB"]
    value = float(size)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            return f"{value:.1f}{unit}" if unit != "B" else f"{int(value)}B"
        value /= 1024
    return f"{size}B"



def _read_json_file(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default



def _write_json_file(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")



def _safe_path(root: Path, raw_path: str) -> Path | None:
    raw_path = str(raw_path or "").strip()
    if not raw_path:
        return None
    candidate = Path(raw_path)
    if not candidate.is_absolute():
        candidate = root / raw_path
    try:
        resolved = candidate.resolve()
        workspace = root.resolve()
        if resolved == workspace or workspace in resolved.parents:
            return resolved
    except Exception:
        return None
    return None



def _file_record(path: Path, root: Path, category: str) -> dict[str, Any]:
    stat = path.stat()
    return {
        "name": path.name,
        "path": ensure_relative_path(path, root),
        "category": category,
        "suffix": path.suffix.lower(),
        "size": stat.st_size,
        "size_text": _format_bytes(stat.st_size),
        "updated_at": datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds"),
        "previewable": path.suffix.lower() in INLINE_FILE_SUFFIXES | SHEET_SUFFIXES | TEXT_SUFFIXES | {".csv"},
        "downloadable": path.suffix.lower() in DOWNLOADABLE_SUFFIXES,
    }



def _list_dir_files(folder: Path, root: Path, category: str, limit: int = 100) -> list[dict[str, Any]]:
    if not folder.exists():
        return []
    files = [path for path in folder.rglob("*") if path.is_file()]
    files.sort(key=lambda item: item.stat().st_mtime, reverse=True)
    return [_file_record(path, root, category) for path in files[:limit]]



def _load_custom_questions(config: AppConfig) -> list[dict[str, Any]]:
    rows = _read_json_file(config.question_library_path, [])
    return rows if isinstance(rows, list) else []



def _save_custom_questions(config: AppConfig, rows: list[dict[str, Any]]) -> None:
    _write_json_file(config.question_library_path, rows[:MAX_CUSTOM_QUESTIONS])



def _load_answer_history(config: AppConfig) -> list[dict[str, Any]]:
    rows = _read_json_file(config.answer_history_path, [])
    return rows if isinstance(rows, list) else []



def _load_hidden_system_question_ids(config: AppConfig) -> set[str]:
    payload = _read_json_file(config.system_question_state_path, {"hidden_ids": []})
    hidden_ids = payload.get("hidden_ids") if isinstance(payload, dict) else []
    if not isinstance(hidden_ids, list):
        return set()
    return {normalize_text(str(item)) for item in hidden_ids if normalize_text(str(item))}



def _save_hidden_system_question_ids(config: AppConfig, hidden_ids: set[str]) -> None:
    _write_json_file(config.system_question_state_path, {"hidden_ids": sorted(hidden_ids)})



def _hide_system_question(config: AppConfig, question_id: str) -> bool:
    question_id = normalize_text(str(question_id or ""))
    if not question_id:
        return False
    hidden_ids = _load_hidden_system_question_ids(config)
    if question_id in hidden_ids:
        return True
    hidden_ids.add(question_id)
    _save_hidden_system_question_ids(config, hidden_ids)
    return True



def _save_answer_history(config: AppConfig, rows: list[dict[str, Any]]) -> None:
    _write_json_file(config.answer_history_path, rows[:MAX_HISTORY])



def _append_answer_history(config: AppConfig, raw_question: str, answers: list[dict[str, Any]]) -> None:
    history = _load_answer_history(config)
    preview = "\n".join(answer.get("a", {}).get("content", "") for answer in answers[:1])[:140]
    history.insert(
        0,
        {
            "id": uuid.uuid4().hex,
            "asked_at": _iso_now(),
            "raw_question": raw_question,
            "summary": preview,
            "answers": answers,
        },
    )
    _save_answer_history(config, history)



def _compact_references(references: list[dict[str, Any]] | None) -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    for reference in references or []:
        if not isinstance(reference, dict):
            continue
        cleaned: dict[str, str] = {}
        for key in ("paper_path", "text", "paper_image"):
            value = reference.get(key)
            if value:
                cleaned[key] = str(value)
        if cleaned:
            items.append(cleaned)
    return items



def _public_answer(answer: dict[str, Any]) -> dict[str, Any]:
    payload = answer.get("A", {})
    reply: dict[str, Any] = {
        "content": str(payload.get("content", "")),
        "sql": str(payload.get("sql", "")),
    }
    images = [str(path) for path in (payload.get("image") or []) if path]
    if images:
        reply["image"] = images
    references = _compact_references(payload.get("references"))
    if references:
        reply["references"] = references
    return {"q": str(answer.get("Q", "")), "a": reply}



def _answer_public(engine: FinancialQAEngine, raw_payload: str, context: dict[str, Any]) -> list[dict[str, Any]]:
    answers: list[dict[str, Any]] = []
    payload = parse_question_payload(raw_payload)
    resolved_question_id = engine.resolve_question_id(raw_payload, context)
    if resolved_question_id:
        context["_question_id"] = resolved_question_id
        context["_image_seq"] = 0
    for item in payload:
        question = str(item.get("Q", ""))
        if not resolved_question_id and not engine.should_reuse_context(question, context):
            context.pop("_question_id", None)
            context.pop("_image_seq", None)
        answer = engine.answer_question(question, context)
        answers.append(_public_answer({"Q": question, "A": answer.as_dict()}))
    return answers



def _safe_count(engine: FinancialQAEngine, table_name: str) -> int:
    if not engine.database.table_exists(table_name):
        return 0
    return int(engine.database.scalar(f"SELECT COUNT(1) FROM {table_name}") or 0)



def _source_summary(config: AppConfig) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []

    def add_entry(label: str, path: Path | None, fallback: str = "未发现") -> None:
        if path is None:
            rows.append({"label": label, "value": fallback, "path": ""})
            return
        rows.append({"label": label, "value": path.name, "path": ensure_relative_path(path, config.workspace_root)})

    rows.append(
        {
            "label": "赛题数据目录",
            "value": "正式数据" if config.contest_data_dir != config.workspace_root else "工作区回退",
            "path": ensure_relative_path(config.contest_data_dir, config.workspace_root),
        }
    )

    try:
        add_entry("基础信息 Excel", config.company_info_file())
    except FileNotFoundError:
        add_entry("基础信息 Excel", None)
    try:
        add_entry("字段说明 Excel", config.schema_file())
    except FileNotFoundError:
        add_entry("字段说明 Excel", None)
    try:
        question_files = config.question_files()
        rows.append({"label": "问题汇总", "value": f"{len(question_files)} 个文件", "path": "、".join(path.name for path in question_files[:3])})
    except FileNotFoundError:
        rows.append({"label": "问题汇总", "value": "未发现", "path": ""})
    rows.append({"label": "财报 PDF", "value": f"{len(config.financial_report_pdfs())} 个", "path": ""})
    rows.append({"label": "研报 PDF", "value": f"{len(config.research_report_pdfs())} 个", "path": ""})
    return rows



def _overview_payload(engine: FinancialQAEngine, config: AppConfig) -> dict[str, Any]:
    db_size = config.db_path.stat().st_size if config.db_path.exists() else 0
    status = database_status(config)
    return {
        "stats": [
            {"label": "企业档案", "value": _safe_count(engine, "company_info"), "hint": "上市公司基础信息"},
            {"label": "题库问题", "value": _safe_count(engine, "question_bank"), "hint": "官方问题与样例集合"},
            {"label": "证据分片", "value": _safe_count(engine, "document_chunks"), "hint": "财报、研报与简介切片"},
            {"label": "结构化指标", "value": _safe_count(engine, "financial_metric_facts"), "hint": "可直接问数的指标事实表"},
            {"label": "上传文件", "value": len(_list_dir_files(config.manual_source_dir, config.workspace_root, "manual_source")) + len(_list_dir_files(config.manual_import_dir, config.workspace_root, "manual_import")), "hint": "手工补充的源文件与 CSV"},
            {"label": "问答历史", "value": len(_load_answer_history(config)), "hint": "最近演示记录"},
        ],
        "database": {
            "path": ensure_relative_path(config.db_path, config.workspace_root),
            "exists": config.db_path.exists(),
            "size": _format_bytes(db_size),
            "table_count": len(_list_tables(engine)),
            "ready": bool(status["ready"]),
            "status_code": str(status["code"]),
            "status_text": str(status["message"]),
            "action_text": str(status["action"]),
        },
        "sources": _source_summary(config),
        "exports": _list_dir_files(config.export_dir, config.workspace_root, "exports", limit=6),
    }


def _official_questions(engine: FinancialQAEngine) -> list[dict[str, Any]]:
    if not engine.database.table_exists("question_bank"):
        return []
    rows = engine.database.query(
        "SELECT question_id, question_type, question_payload, source_file FROM question_bank ORDER BY question_id, rowid"
    )
    items: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for row in rows:
        question_id = str(row["question_id"] or "").strip()
        payload = str(row["question_payload"] or "").strip()
        if not payload:
            continue
        key = (question_id, payload)
        if key in seen:
            continue
        seen.add(key)
        parsed = parse_question_payload(payload)
        display = " / ".join(item["Q"] for item in parsed[:2]) if parsed else payload
        items.append(
            {
                "id": question_id or uuid.uuid4().hex[:8],
                "question_id": question_id,
                "question_type": str(row["question_type"] or ""),
                "question_payload": payload,
                "display": display,
                "source_file": str(row["source_file"] or ""),
            }
        )
    return items



def _sample_questions(engine: FinancialQAEngine, config: AppConfig) -> list[str]:
    return build_sample_questions(engine, _load_hidden_system_question_ids(config))



def _list_tables(engine: FinancialQAEngine) -> list[dict[str, Any]]:
    rows = engine.database.query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
    tables: list[dict[str, Any]] = []
    for row in rows:
        name = str(row["name"])
        if not VALID_TABLE_NAME.match(name):
            continue
        columns = [str(item["name"]) for item in engine.database.query(f"PRAGMA table_info({name})")]
        tables.append({"name": name, "count": engine.database.table_row_count(name), "column_count": len(columns), "columns": columns[:8]})
    return tables



def _table_preview(engine: FinancialQAEngine, table_name: str, page: int, page_size: int) -> dict[str, Any]:
    if not VALID_TABLE_NAME.match(table_name) or not engine.database.table_exists(table_name):
        raise ValueError("无效的数据表名称")
    page = max(page, 1)
    page_size = min(max(page_size, 5), 100)
    offset = (page - 1) * page_size
    columns = [str(item["name"]) for item in engine.database.query(f"PRAGMA table_info({table_name})")]
    total = engine.database.table_row_count(table_name)
    rows = engine.database.query(f"SELECT * FROM {table_name} LIMIT ? OFFSET ?", (page_size, offset))
    return {
        "name": table_name,
        "columns": columns,
        "rows": [{column: "" if row[column] is None else str(row[column]) for column in columns} for row in rows],
        "page": page,
        "page_size": page_size,
        "total": total,
    }



def _preview_text_blocks(text: str, limit: int = 8) -> list[str]:
    blocks: list[str] = []
    for fragment in re.split(r"(?:\r?\n){2,}", text or ""):
        cleaned = normalize_text(fragment)
        if cleaned:
            blocks.append(cleaned[:600])
        if len(blocks) >= limit:
            break
    if not blocks and text:
        blocks.append(normalize_text(text)[:600])
    return blocks



def _query_reference_matches(engine: FinancialQAEngine, reference: str) -> list[dict[str, str]]:
    token = str(reference or "").strip()
    if not token:
        return []
    candidates = [token]
    if "/" in token or "\\" in token:
        candidates.append(Path(token).name)
    rows: list[Any] = []
    for candidate in candidates:
        exact = engine.database.query(
            "SELECT title, file_path, text FROM document_chunks WHERE file_path = ? OR title = ? LIMIT 12",
            (candidate, candidate),
        )
        if exact:
            rows = exact
            break
    if not rows:
        like = f"%{candidates[-1]}%"
        rows = engine.database.query(
            "SELECT title, file_path, text FROM document_chunks WHERE file_path LIKE ? OR title LIKE ? LIMIT 12",
            (like, like),
        )
    return [{"title": str(row["title"] or ""), "file_path": str(row["file_path"] or ""), "text": str(row["text"] or "")[:800]} for row in rows]



def _preview_reference(engine: FinancialQAEngine, config: AppConfig, reference: str) -> dict[str, Any]:
    reference = unquote(str(reference or "").strip())
    if not reference:
        raise ValueError("ref 不能为空")

    candidate = _safe_path(config.workspace_root, reference)
    if candidate and candidate.exists() and candidate.is_file():
        suffix = candidate.suffix.lower()
        relative_path = ensure_relative_path(candidate, config.workspace_root)
        if suffix in {".xlsx", ".xlsm"}:
            workbook = read_workbook(candidate)
            sheets = []
            for sheet_name, rows in list(workbook.items())[:3]:
                headers = [str(cell) for cell in (rows[0] if rows else [])]
                preview_rows = []
                for row in rows[1:13]:
                    preview_rows.append([str(row[index]) if index < len(row) else "" for index in range(len(headers))])
                sheets.append({"name": sheet_name, "headers": headers, "rows": preview_rows})
            return {"kind": "xlsx", "title": candidate.name, "path": relative_path, "sheets": sheets}
        if suffix == ".csv":
            with candidate.open("r", encoding="utf-8-sig", errors="ignore", newline="") as handle:
                reader = list(csv.reader(handle))
            headers = reader[0] if reader else []
            return {"kind": "csv", "title": candidate.name, "path": relative_path, "headers": headers, "rows": reader[1:13]}
        if suffix in TEXT_SUFFIXES:
            text = candidate.read_text(encoding="utf-8", errors="ignore")
            if suffix == ".json":
                try:
                    text = json.dumps(json.loads(text), ensure_ascii=False, indent=2)
                except Exception:
                    pass
            return {"kind": "text", "title": candidate.name, "path": relative_path, "text": text[:10000], "blocks": _preview_text_blocks(text)}
        if suffix in {".jpg", ".jpeg", ".png", ".webp", ".svg"}:
            return {"kind": "image", "title": candidate.name, "path": relative_path, "url": "/" + relative_path}
        if suffix == ".pdf":
            matches = _query_reference_matches(engine, str(candidate)) or _query_reference_matches(engine, relative_path)
            snippets = [normalize_text(item["text"]) for item in matches if normalize_text(item["text"])]
            if not snippets:
                snippets = _preview_text_blocks(extract_text_safe(candidate), limit=10)
            return {"kind": "pdf", "title": candidate.name, "path": relative_path, "blocks": snippets[:10], "text": "\n\n".join(snippets[:10])}

    matches = _query_reference_matches(engine, reference)
    if matches:
        blocks = [normalize_text(item["text"]) for item in matches if normalize_text(item["text"])]
        return {"kind": "retrieval", "title": reference, "path": matches[0]["file_path"], "blocks": blocks[:10], "matches": matches[:10]}
    return {"kind": "missing", "title": reference, "path": "", "text": "未找到可预览的文件或证据内容。"}



def _files_payload(config: AppConfig) -> dict[str, Any]:
    return {
        "manual_import": _list_dir_files(config.manual_import_dir, config.workspace_root, "manual_import", limit=80),
        "manual_source": _list_dir_files(config.manual_source_dir, config.workspace_root, "manual_source", limit=80),
        "exports": _list_dir_files(config.export_dir, config.workspace_root, "exports", limit=30),
        "artifacts": _list_dir_files(config.artifact_dir, config.workspace_root, "artifacts", limit=24),
        "sources": _source_summary(config),
    }



def _upsert_custom_question(config: AppConfig, payload: dict[str, Any]) -> dict[str, Any]:
    rows = _load_custom_questions(config)
    question = normalize_text(str(payload.get("question", "")))
    if not question:
        raise ValueError("question 不能为空")
    raw_tags = payload.get("tags") or []
    if isinstance(raw_tags, str):
        tags = [normalize_text(item) for item in raw_tags.replace("，", ",").split(",") if normalize_text(item)]
    else:
        tags = [normalize_text(str(item)) for item in raw_tags if normalize_text(str(item))]
    record = {
        "id": str(payload.get("id") or uuid.uuid4().hex),
        "title": normalize_text(str(payload.get("title", ""))) or question[:28],
        "question": question,
        "tags": tags,
        "note": normalize_text(str(payload.get("note", ""))),
        "updated_at": _iso_now(),
    }
    replaced = False
    for index, row in enumerate(rows):
        if str(row.get("id", "")) == record["id"]:
            rows[index] = record
            replaced = True
            break
    if not replaced:
        rows.insert(0, record)
    _save_custom_questions(config, rows)
    return record



def _delete_custom_question(config: AppConfig, question_id: str) -> bool:
    rows = _load_custom_questions(config)
    filtered = [row for row in rows if str(row.get("id", "")) != question_id]
    if len(filtered) == len(rows):
        return False
    _save_custom_questions(config, filtered)
    return True



def _save_upload(config: AppConfig, filename: str, content: bytes, category: str) -> dict[str, Any]:
    safe_name = Path(filename).name
    if not safe_name:
        raise ValueError("文件名不能为空")
    suffix = Path(safe_name).suffix.lower()
    if category == "manual_import":
        if suffix != ".csv":
            raise ValueError("结构化导入仅支持 CSV 文件")
        target_dir = config.manual_import_dir
    else:
        if suffix not in UPLOAD_SOURCE_SUFFIXES:
            raise ValueError("源文件导入仅支持 xlsx/xlsm/pdf/csv/json/txt")
        target_dir = config.manual_source_dir
    target = target_dir / safe_name
    target.write_bytes(content)
    return _file_record(target, config.workspace_root, category)



def _flatten_export_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    flattened: list[dict[str, Any]] = []
    for row in records:
        flattened.append(
            {
                "asked_at": str(row.get("asked_at", "")),
                "question": str(row.get("question", "")),
                "content": str(row.get("content", "")),
                "sql": str(row.get("sql", "")),
                "references": row.get("references") or [],
                "images": row.get("images") or [],
            }
        )
    return flattened



def _export_records(config: AppConfig, records: list[dict[str, Any]], file_format: str) -> dict[str, str]:
    rows = _flatten_export_records(records)
    if not rows:
        raise ValueError("没有可导出的记录")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if file_format == "json":
        output_path = config.export_dir / f"ask_results_{timestamp}.json"
        _write_json_file(output_path, rows)
    else:
        output_path = config.export_dir / f"ask_results_{timestamp}.xlsx"
        workbook_rows: list[list[object]] = [["时间", "问题", "回答", "SQL", "参考文件", "图表"]]
        for row in rows:
            references = "\n".join(str(item.get("paper_path", "")) for item in row["references"] if isinstance(item, dict))
            images = "\n".join(str(item) for item in row["images"])
            workbook_rows.append([row["asked_at"], row["question"], row["content"], row["sql"], references, images])
        write_simple_xlsx(output_path, "问答导出", workbook_rows)
    relative = ensure_relative_path(output_path, config.workspace_root)
    return {"path": relative, "download_url": f"/api/download?path={quote(relative)}"}


def serve(engine: FinancialQAEngine, config: AppConfig, host: str = "127.0.0.1", port: int = 8000) -> None:
    static_root = config.static_dir
    workspace_root = config.workspace_root
    session_contexts: dict[str, dict[str, Any]] = {}

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
            return

        def _get_session_id(self) -> str:
            cookie_header = self.headers.get("Cookie", "")
            if cookie_header:
                cookie = SimpleCookie()
                cookie.load(cookie_header)
                morsel = cookie.get(SESSION_COOKIE_NAME)
                if morsel and morsel.value:
                    return morsel.value
            session_id = uuid.uuid4().hex
            self._new_session_cookie = session_id
            return session_id

        def _session_context(self) -> dict[str, Any]:
            session_id = self._get_session_id()
            return session_contexts.setdefault(session_id, {})

        def _send_json(self, payload: dict[str, Any], status: HTTPStatus = HTTPStatus.OK) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            session_id = getattr(self, "_new_session_cookie", None)
            if session_id:
                self.send_header("Set-Cookie", f"{SESSION_COOKIE_NAME}={session_id}; Path=/; HttpOnly; SameSite=Lax")
            self.end_headers()
            self.wfile.write(body)

        def _read_json_body(self) -> dict[str, Any]:
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length) if length else b"{}"
            if not raw:
                return {}
            return json.loads(raw)

        def _serve_file(self, path: Path, content_type: str, as_attachment: bool = False) -> None:
            if not path.exists() or not path.is_file():
                self.send_error(HTTPStatus.NOT_FOUND)
                return
            body = path.read_bytes()
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            if as_attachment:
                self.send_header("Content-Disposition", f"attachment; filename*=UTF-8''{quote(path.name)}")
            self.end_headers()
            self.wfile.write(body)

        def _download_file(self, raw_path: str) -> None:
            candidate = _safe_path(workspace_root, raw_path)
            if candidate is None or not candidate.exists() or candidate.suffix.lower() not in DOWNLOADABLE_SUFFIXES:
                self.send_error(HTTPStatus.NOT_FOUND)
                return
            content_type = mimetypes.guess_type(candidate.name)[0] or "application/octet-stream"
            self._serve_file(candidate, content_type, as_attachment=True)

        def _refresh_engine(self) -> None:
            fresh_database = ingest_all(config, log=lambda message: print(message, flush=True))
            engine.database = fresh_database
            engine.refresh()

        def _handle_upload(self) -> None:
            form = cgi.FieldStorage(
                fp=self.rfile,
                headers=self.headers,
                environ={
                    "REQUEST_METHOD": "POST",
                    "CONTENT_TYPE": self.headers.get("Content-Type", ""),
                    "CONTENT_LENGTH": self.headers.get("Content-Length", "0"),
                },
            )
            category = str(form.getvalue("category") or "manual_source")
            file_item = form["file"] if "file" in form else None
            if file_item is None or not getattr(file_item, "filename", ""):
                return self._send_json({"error": "未选择上传文件"}, status=HTTPStatus.BAD_REQUEST)
            record = _save_upload(config, file_item.filename, file_item.file.read(), category)
            return self._send_json({"ok": True, "file": record})

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            query = parse_qs(parsed.query)
            route = parsed.path

            if route in ("/", "/index.html"):
                return self._serve_file(static_root / "index.html", "text/html; charset=utf-8")
            if route == "/styles.css":
                return self._serve_file(static_root / "styles.css", "text/css; charset=utf-8")
            if route == "/app.js":
                return self._serve_file(static_root / "app.js", "application/javascript; charset=utf-8")
            if route == "/api/sample-questions":
                return self._send_json({"samples": _sample_questions(engine, config)})
            if route == "/api/overview":
                return self._send_json(_overview_payload(engine, config))
            if route == "/api/reset-context":
                session_contexts.pop(self._get_session_id(), None)
                return self._send_json({"ok": True})
            if route == "/api/files":
                return self._send_json(_files_payload(config))
            if route == "/api/question-bank":
                return self._send_json(build_question_bank_payload(engine, _load_custom_questions(config), _load_hidden_system_question_ids(config)))
            if route == "/api/answer-history":
                return self._send_json({"history": _load_answer_history(config)})
            if route == "/api/tables":
                return self._send_json({"tables": _list_tables(engine)})
            if route == "/api/table-preview":
                table_name = str(query.get("name", [""])[0])
                page = int(str(query.get("page", ["1"])[0] or "1"))
                page_size = int(str(query.get("page_size", ["20"])[0] or "20"))
                try:
                    return self._send_json(_table_preview(engine, table_name, page, page_size))
                except ValueError as exc:
                    return self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            if route == "/api/reference-preview":
                reference = str(query.get("ref", [""])[0])
                try:
                    return self._send_json(_preview_reference(engine, config, reference))
                except ValueError as exc:
                    return self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            if route == "/api/download":
                return self._download_file(str(query.get("path", [""])[0]))

            file_candidate = _safe_path(workspace_root, route.lstrip("/"))
            if file_candidate and file_candidate.exists() and file_candidate.suffix.lower() in INLINE_FILE_SUFFIXES:
                content_type = mimetypes.guess_type(file_candidate.name)[0] or "application/octet-stream"
                return self._serve_file(file_candidate, content_type)
            self.send_error(HTTPStatus.NOT_FOUND)

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            route = parsed.path

            if route == "/api/ask":
                status = database_status(config)
                if not status["ready"]:
                    return self._send_json(
                        {
                            "error": f"{status['message']} {status['action']}",
                        },
                        status=HTTPStatus.CONFLICT,
                    )
                payload = self._read_json_body()
                question = str(payload.get("question", "")).strip()
                if not question:
                    return self._send_json({"error": "question 不能为空"}, status=HTTPStatus.BAD_REQUEST)
                answers = _answer_public(engine, question, self._session_context())
                _append_answer_history(config, question, answers)
                return self._send_json({"answers": answers})
            if route == "/api/upload":
                try:
                    return self._handle_upload()
                except ValueError as exc:
                    return self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            if route == "/api/rebuild-database":
                self._refresh_engine()
                return self._send_json({"ok": True, "overview": _overview_payload(engine, config)})
            if route == "/api/custom-questions/save":
                try:
                    payload = self._read_json_body()
                    record = _upsert_custom_question(config, payload)
                    return self._send_json({"ok": True, "record": record})
                except ValueError as exc:
                    return self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            if route == "/api/custom-questions/delete":
                payload = self._read_json_body()
                deleted = _delete_custom_question(config, str(payload.get("id", "")))
                return self._send_json({"ok": deleted})
            if route == "/api/system-questions/delete":
                payload = self._read_json_body()
                deleted = _hide_system_question(config, str(payload.get("id", "")))
                return self._send_json({"ok": deleted})
            if route == "/api/answer-history/clear":
                _save_answer_history(config, [])
                return self._send_json({"ok": True})
            if route == "/api/export-results":
                payload = self._read_json_body()
                file_format = str(payload.get("format", "xlsx") or "xlsx").lower()
                records = payload.get("records") or []
                try:
                    exported = _export_records(config, records, "json" if file_format == "json" else "xlsx")
                    return self._send_json({"ok": True, **exported})
                except ValueError as exc:
                    return self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            self.send_error(HTTPStatus.NOT_FOUND)

    server = ThreadingHTTPServer((host, port), Handler)
    print(f"Web UI 已启动: http://{host}:{port}")
    server.serve_forever()
