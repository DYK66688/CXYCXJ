from __future__ import annotations

import gc
import json
from datetime import datetime
from pathlib import Path
from time import perf_counter, sleep
from typing import Any, Callable

from .database_base import (
    Database,
    create_base_tables,
    create_financial_tables,
    load_company_info,
    load_company_profile_chunks,
    load_manual_csvs,
    load_question_bank,
    load_research,
    load_research_metadata_chunks,
    load_seed_csvs,
    refresh_metric_facts,
)
from .database_extract import load_financial_reports, load_research_pdf_chunks
from .validation import run_validation


LogFn = Callable[[str], None]


def _emit(log: LogFn | None, message: str) -> None:
    if log is not None:
        log(message)


def _run_step(log: LogFn | None, label: str, action) -> float:
    started = perf_counter()
    _emit(log, f"[开始] {label}")
    action()
    elapsed = perf_counter() - started
    _emit(log, f"[完成] {label} · {elapsed:.1f}s")
    return elapsed


def _swap_database(temp_path: Path, target_path: Path, log: LogFn | None = None) -> None:
    backup_path = target_path.with_name(f"{target_path.stem}.previous{target_path.suffix}")
    if backup_path.exists():
        backup_path.unlink()
    if target_path.exists():
        _emit(log, f"[开始] 备份旧数据库：{backup_path.name}")
        target_path.replace(backup_path)
        _emit(log, f"[完成] 备份旧数据库：{backup_path.name}")

    gc.collect()
    last_error: Exception | None = None
    for attempt in range(1, 6):
        try:
            temp_path.replace(target_path)
            _emit(log, f"[完成] 替换正式数据库：{target_path.name}")
            last_error = None
            break
        except PermissionError as exc:
            last_error = exc
            _emit(log, f"[等待] 替换正式数据库失败，{attempt}/5 次重试：{exc}")
            sleep(1.0)
            gc.collect()

    if last_error is not None:
        if backup_path.exists() and not target_path.exists():
            backup_path.replace(target_path)
            _emit(log, "[回滚] 新数据库替换失败，已恢复旧数据库")
        raise last_error

    if backup_path.exists():
        backup_path.unlink()


def _write_ingest_manifest(config) -> None:
    payload = {
        "built_at": datetime.now().isoformat(timespec="seconds"),
        "data_root": str(config.contest_data_dir.resolve()),
        "database": str(config.db_path.resolve()),
    }
    config.ingest_manifest_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_ingest_report(config, payload: dict[str, object]) -> None:
    config.ingest_report_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def ingest_manifest_matches(config) -> bool:
    if not config.db_path.exists():
        return False
    if not config.ingest_manifest_path.exists():
        return config.contest_data_dir.resolve() == config.workspace_root.resolve()
    try:
        payload = json.loads(config.ingest_manifest_path.read_text(encoding="utf-8"))
    except Exception:
        return False
    manifest_root = str(payload.get("data_root", "")).strip()
    return manifest_root == str(config.contest_data_dir.resolve())


def database_status(config) -> dict[str, object]:
    status = {
        "ready": False,
        "code": "missing",
        "message": "数据库尚未构建。",
        "action": "请先重建数据库或运行 python run.py ingest。",
    }
    if not config.db_path.exists():
        return status
    if not ingest_manifest_matches(config):
        status["code"] = "stale"
        status["message"] = "当前数据库不是按正式数据目录构建的。"
        return status

    database = Database(config.db_path)
    try:
        if not database.table_exists("company_info"):
            status["code"] = "broken"
            status["message"] = "数据库缺少 company_info 表。"
            return status
        company_count = int(database.scalar("SELECT COUNT(1) FROM company_info") or 0)
    except Exception:
        status["code"] = "broken"
        status["message"] = "数据库结构异常或无法读取。"
        return status

    if company_count <= 0:
        status["code"] = "empty"
        status["message"] = "数据库已创建，但尚未导入正式数据。"
        return status

    return {
        "ready": True,
        "code": "ready",
        "message": "数据库已按正式数据构建，可直接问数。",
        "action": "",
    }


def ensure_web_bootstrap_database(config) -> Database:
    bootstrap_path = config.runtime_dir / "web_bootstrap.sqlite3"
    database = Database(bootstrap_path)
    create_base_tables(database)
    try:
        create_financial_tables(database, config.schema_file())
    except FileNotFoundError:
        pass
    return database


def ingest_all(config, log: LogFn | None = None):
    total_started = perf_counter()
    config.ensure_directories()
    temp_db_path = config.db_path.with_name(f"{config.db_path.stem}.building{config.db_path.suffix}")
    if temp_db_path.exists():
        temp_db_path.unlink()
    database = Database(temp_db_path)
    built_at = datetime.now().isoformat(timespec="seconds")
    financial_pdf_count = len(config.financial_report_pdfs())
    research_pdf_count = len(config.research_report_pdfs())
    stage_durations: dict[str, float] = {}
    extraction_report: dict[str, Any] = {}
    validation_report: dict[str, Any] = {}

    _emit(log, f"建库目标：{config.db_path}")
    _emit(log, f"正式数据目录：{config.contest_data_dir}")
    _emit(log, f"临时数据库：{temp_db_path}")

    try:
        stage_durations["创建基础表"] = _run_step(log, "创建基础表", lambda: create_base_tables(database))
        stage_durations["创建财务表结构"] = _run_step(log, "创建财务表结构", lambda: create_financial_tables(database, config.schema_file()))
        stage_durations["导入公司基础信息"] = _run_step(log, "导入公司基础信息", lambda: load_company_info(database, config))
        _emit(log, f"公司基础信息：{database.table_row_count('company_info')} 条")

        stage_durations["导入官方题库"] = _run_step(log, "导入官方题库", lambda: load_question_bank(database, config))
        _emit(log, f"官方题库：{database.table_row_count('question_bank')} 条")

        stage_durations["导入研报元数据表"] = _run_step(log, "导入研报元数据表", lambda: load_research(database, config))
        _emit(
            log,
            "研报元数据："
            f"个股 {database.table_row_count('stock_research')} 条，"
            f"行业 {database.table_row_count('industry_research')} 条",
        )

        stage_durations["清空历史文档分片"] = _run_step(log, "清空历史文档分片", lambda: database.execute("DELETE FROM document_chunks"))
        stage_durations["写入公司简介分片"] = _run_step(log, "写入公司简介分片", lambda: load_company_profile_chunks(database))
        stage_durations["写入研报元数据分片"] = _run_step(log, "写入研报元数据分片", lambda: load_research_metadata_chunks(database))

        def _load_financial_reports() -> None:
            nonlocal extraction_report
            extraction_report = load_financial_reports(database, config, log=log) or {}

        stage_durations["解析财报 PDF"] = _run_step(log, "解析财报 PDF", _load_financial_reports)
        stage_durations["解析研报 PDF"] = _run_step(log, "解析研报 PDF", lambda: load_research_pdf_chunks(database, config, log=log))
        stage_durations["导入种子 CSV"] = _run_step(log, "导入种子 CSV", lambda: load_seed_csvs(database, config))
        stage_durations["导入手工补录 CSV"] = _run_step(log, "导入手工补录 CSV", lambda: load_manual_csvs(database, config))
        stage_durations["刷新指标事实表"] = _run_step(log, "刷新指标事实表", lambda: refresh_metric_facts(database))

        def _run_validation_stage() -> None:
            nonlocal validation_report
            validation_report = run_validation(database, config, extraction_report=extraction_report, log=log)

        stage_durations["运行建库校验"] = _run_step(log, "运行建库校验", _run_validation_stage)

        row_counts = {
            "company_info": database.table_row_count("company_info"),
            "question_bank": database.table_row_count("question_bank"),
            "stock_research": database.table_row_count("stock_research"),
            "industry_research": database.table_row_count("industry_research"),
            "document_chunks": database.table_row_count("document_chunks"),
            "financial_metric_facts": database.table_row_count("financial_metric_facts"),
        }
        _emit(log, f"文档分片：{row_counts['document_chunks']} 条")
        _emit(log, f"指标事实：{row_counts['financial_metric_facts']} 条")
        _emit(log, f"校验报告：{config.runtime_dir / 'validation_report.json'}")

        stage_durations["替换正式数据库"] = _run_step(log, "替换正式数据库", lambda: _swap_database(temp_db_path, config.db_path, log))
        _write_ingest_manifest(config)

        total_elapsed = perf_counter() - total_started
        report_payload = {
            "status": "success",
            "built_at": built_at,
            "finished_at": datetime.now().isoformat(timespec="seconds"),
            "elapsed_seconds": round(total_elapsed, 3),
            "data_root": str(config.contest_data_dir.resolve()),
            "database": str(config.db_path.resolve()),
            "temporary_database": str(temp_db_path.resolve()),
            "pdf_counts": {
                "financial_reports": financial_pdf_count,
                "research_reports": research_pdf_count,
                "total": financial_pdf_count + research_pdf_count,
            },
            "row_counts": row_counts,
            "stage_durations_seconds": {key: round(value, 3) for key, value in stage_durations.items()},
            "validation_report": str((config.runtime_dir / "validation_report.json").resolve()),
            "validation_summary": validation_report.get("summary", {}),
        }
        _write_ingest_report(config, report_payload)
        _emit(log, f"建库报告：{config.ingest_report_path}")
        _emit(log, f"建库完成，总耗时 {total_elapsed:.1f}s")
        return Database(config.db_path)
    except Exception as exc:
        total_elapsed = perf_counter() - total_started
        failure_payload = {
            "status": "failed",
            "built_at": built_at,
            "finished_at": datetime.now().isoformat(timespec="seconds"),
            "elapsed_seconds": round(total_elapsed, 3),
            "data_root": str(config.contest_data_dir.resolve()),
            "database": str(config.db_path.resolve()),
            "temporary_database": str(temp_db_path.resolve()),
            "pdf_counts": {
                "financial_reports": financial_pdf_count,
                "research_reports": research_pdf_count,
                "total": financial_pdf_count + research_pdf_count,
            },
            "row_counts": {
                "company_info": database.table_row_count("company_info") if database.table_exists("company_info") else 0,
                "question_bank": database.table_row_count("question_bank") if database.table_exists("question_bank") else 0,
                "stock_research": database.table_row_count("stock_research") if database.table_exists("stock_research") else 0,
                "industry_research": database.table_row_count("industry_research") if database.table_exists("industry_research") else 0,
                "document_chunks": database.table_row_count("document_chunks") if database.table_exists("document_chunks") else 0,
                "financial_metric_facts": database.table_row_count("financial_metric_facts") if database.table_exists("financial_metric_facts") else 0,
            },
            "stage_durations_seconds": {key: round(value, 3) for key, value in stage_durations.items()},
            "validation_report": str((config.runtime_dir / "validation_report.json").resolve()),
            "validation_summary": validation_report.get("summary", {}),
            "error": {
                "type": type(exc).__name__,
                "message": str(exc),
            },
        }
        _write_ingest_report(config, failure_payload)
        _emit(log, f"建库报告：{config.ingest_report_path}")
        raise


__all__ = ["Database", "database_status", "ensure_web_bootstrap_database", "ingest_all", "ingest_manifest_matches", "_write_ingest_report"]
