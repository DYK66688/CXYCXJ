from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .assistant import FinancialQAEngine
from .bundle import create_clean_bundle
from .config import AppConfig
from .database import Database, database_status, ensure_web_bootstrap_database, ingest_all
from .utils import dump_json
from .web import serve


def _resolve_export_output(config: AppConfig, question_file: Path, explicit_output: str = "") -> Path:
    if explicit_output:
        return Path(explicit_output)
    name = question_file.name
    if "附件4" in name:
        return config.submission_dir / "result_2.xlsx"
    if "附件6" in name:
        return config.submission_dir / "result_3.xlsx"
    try:
        from .xlsx_tools import read_workbook, rows_to_dicts

        workbook = read_workbook(question_file)
        rows = rows_to_dicts(next(iter(workbook.values()), []))
        question_ids = [str(row.get("编号", "")).strip() for row in rows if str(row.get("编号", "")).strip()]
        if question_ids and all(question_id.startswith("B1") for question_id in question_ids):
            return config.submission_dir / "result_2.xlsx"
        if question_ids and all(question_id.startswith("B2") for question_id in question_ids):
            return config.submission_dir / "result_3.xlsx"
    except Exception:
        pass
    return config.submission_dir / f"{question_file.stem}_答案结果.xlsx"


def _ensure_database(config: AppConfig, log=None) -> Database:
    status = database_status(config)
    if not status["ready"]:
        return ingest_all(config, log=log)
    database = Database(config.db_path)
    try:
        count = database.scalar("SELECT COUNT(1) FROM company_info")
    except Exception:
        return ingest_all(config, log=log)
    if not count:
        return ingest_all(config, log=log)
    return database


def _database_for_serve(config: AppConfig) -> Database:
    status = database_status(config)
    if status["ready"]:
        return Database(config.db_path)
    return ensure_web_bootstrap_database(config)


def _emit(text: str) -> None:
    sys.stdout.buffer.write((text + "\n").encode("utf-8", "ignore"))
    sys.stdout.flush()


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv:
        _emit("未检测到命令，默认启动 Web 界面: http://127.0.0.1:8000")
        argv = ["serve"]

    parser = argparse.ArgumentParser(description="上市公司财报智能问数助手")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("ingest", help="导入 Excel/PDF 并构建 SQLite 数据库")

    ask_parser = subparsers.add_parser("ask", help="单次提问")
    ask_parser.add_argument("question", help="问题文本，支持 JSON 数组格式")

    serve_parser = subparsers.add_parser("serve", help="启动本地 Web 服务")
    serve_parser.add_argument("--host", default="127.0.0.1")
    serve_parser.add_argument("--port", type=int, default=8000)

    export_parser = subparsers.add_parser("export", help="批量导出问题答案")
    export_parser.add_argument("--question-file", default="", help="指定问题汇总 xlsx；为空时导出全部")
    export_parser.add_argument("--output", default="", help="输出 xlsx 路径")

    demo_parser = subparsers.add_parser("demo", help="输出样例问题答案 JSON")
    demo_parser.add_argument("--limit", type=int, default=20)

    package_parser = subparsers.add_parser("package", help="生成不含缓存与运行产物的干净交付包")
    package_parser.add_argument("--name", default="financial_qa_assistant", help="输出压缩包名称前缀")

    args = parser.parse_args(argv)
    config = AppConfig.discover()

    if args.command == "ingest":
        ingest_all(config, log=_emit)
        _emit(f"已完成数据导入，数据库位置: {config.db_path}")
        return 0

    if args.command == "package":
        bundle_path = create_clean_bundle(config, stem=args.name)
        _emit(f"已打包: {bundle_path}")
        return 0

    database = _database_for_serve(config) if args.command == "serve" else _ensure_database(config, log=_emit)
    engine = FinancialQAEngine(config, database)

    if args.command == "ask":
        _emit(dump_json(engine.answer_payload(args.question)))
        return 0
    if args.command == "serve":
        serve(engine, config, host=args.host, port=args.port)
        return 0
    if args.command == "export":
        question_files = [Path(args.question_file)] if args.question_file else config.question_files()
        for question_file in question_files:
            output = _resolve_export_output(config, question_file, args.output)
            engine.batch_export(question_file, output)
            _emit(f"已导出: {output}")
        return 0
    if args.command == "demo":
        rows = database.query(
            "SELECT question_id, question_type, question_payload FROM question_bank ORDER BY question_id LIMIT ?",
            (args.limit,),
        )
        demo_payload = []
        for row in rows:
            demo_payload.append(
                {
                    "编号": row["question_id"],
                    "问题类型": row["question_type"],
                    "答案": engine.answer_payload(row["question_payload"]),
                }
            )
        _emit(dump_json(demo_payload))
        return 0
    return 1
