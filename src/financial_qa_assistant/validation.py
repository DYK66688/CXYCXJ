from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from .config import AppConfig
from .database_base import Database, refresh_metric_facts
from .utils import STANDARD_METRIC_CATALOG, has_encoding_issue, is_valid_report_period, normalize_report_period, to_float


LogFn = Callable[[str], None]

VALIDATION_TABLES = [
    "income_sheet",
    "core_performance_indicators_sheet",
    "balance_sheet",
    "cash_flow_sheet",
]

CRITICAL_FIELDS: dict[str, list[str]] = {
    "income_sheet": [
        "total_operating_revenue",
        "main_business_revenue",
        "net_profit",
        "total_profit",
    ],
    "core_performance_indicators_sheet": [
        "eps",
        "diluted_eps",
        "roe",
    ],
    "balance_sheet": [
        "asset_total_assets",
        "asset_cash_and_cash_equivalents",
        "asset_accounts_receivable",
        "asset_inventory",
        "equity_parent_net_assets",
    ],
    "cash_flow_sheet": [
        "operating_cf_net_amount",
        "investing_cf_net_amount",
        "financing_cf_net_amount",
    ],
}

AMOUNT_LIMITS: dict[str, float] = {
    "total_operating_revenue": 100_000_000.0,
    "main_business_revenue": 100_000_000.0,
    "net_profit": 50_000_000.0,
    "total_profit": 50_000_000.0,
    "asset_total_assets": 500_000_000.0,
    "asset_cash_and_cash_equivalents": 100_000_000.0,
    "asset_accounts_receivable": 100_000_000.0,
    "asset_inventory": 100_000_000.0,
    "liability_total_liabilities": 500_000_000.0,
    "equity_parent_net_assets": 300_000_000.0,
    "operating_cf_net_amount": 50_000_000.0,
    "investing_cf_net_amount": 50_000_000.0,
    "financing_cf_net_amount": 50_000_000.0,
}

PERCENTAGE_COLUMNS = {
    "net_profit_yoy_growth",
    "operating_revenue_yoy_growth",
    "main_business_revenue_yoy_growth",
    "total_profit_yoy_growth",
    "asset_total_assets_yoy_growth",
    "liability_total_liabilities_yoy_growth",
    "net_cash_flow_yoy_growth",
    "asset_liability_ratio",
    "gross_profit_margin",
    "net_profit_margin",
    "roe",
    "roe_weighted_excl_non_recurring",
    "operating_revenue_qoq_growth",
    "net_profit_qoq_growth",
}

EPS_COLUMNS = {"eps", "diluted_eps", "net_asset_per_share", "operating_cf_per_share"}

COLUMN_LABEL_MAP = {item["column"]: item["label"] for item in STANDARD_METRIC_CATALOG}


def _emit(log: LogFn | None, message: str) -> None:
    if log is not None:
        log(message)


def _validation_report_path(config: AppConfig) -> Path:
    return config.runtime_dir / "validation_report.json"


def _duplicate_key_issues(database: Database) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    for table in VALIDATION_TABLES:
        if not database.table_exists(table):
            continue
        rows = database.query(
            f"""
            SELECT stock_code, report_period, COUNT(1) AS duplicate_count
            FROM {table}
            GROUP BY stock_code, report_period
            HAVING COUNT(1) > 1
            ORDER BY duplicate_count DESC, stock_code, report_period
            """
        )
        for row in rows[:100]:
            issues.append(
                {
                    "table": table,
                    "stock_code": row["stock_code"],
                    "report_period": row["report_period"],
                    "duplicate_count": int(row["duplicate_count"] or 0),
                }
            )
    return issues


def _period_issues(database: Database) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    for table in VALIDATION_TABLES:
        if not database.table_exists(table):
            continue
        rows = database.query(f"SELECT rowid, stock_code, report_period FROM {table}")
        for row in rows:
            report_period = str(row["report_period"] or "")
            if is_valid_report_period(report_period):
                continue
            issues.append(
                {
                    "table": table,
                    "rowid": int(row["rowid"]),
                    "stock_code": str(row["stock_code"] or ""),
                    "report_period": report_period,
                    "normalized_period": normalize_report_period(report_period),
                }
            )
    return issues


def _key_field_issues(database: Database) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    for table in VALIDATION_TABLES:
        if not database.table_exists(table):
            continue
        rows = database.query(f"SELECT rowid, stock_code, report_period FROM {table}")
        for row in rows:
            if str(row["stock_code"] or "").strip() and str(row["report_period"] or "").strip():
                continue
            issues.append(
                {
                    "table": table,
                    "rowid": int(row["rowid"]),
                    "stock_code": str(row["stock_code"] or ""),
                    "report_period": str(row["report_period"] or ""),
                }
            )
    return issues


def _detect_suspicious_value(column: str, value: Any) -> str | None:
    number = to_float(value)
    if number is None:
        return None
    if column in PERCENTAGE_COLUMNS or column.endswith("_growth") or column.endswith("_ratio"):
        return "abnormal_growth_or_ratio" if abs(number) > 500 else None
    if column in EPS_COLUMNS:
        return "abnormal_eps" if abs(number) > 100 else None
    limit = AMOUNT_LIMITS.get(column)
    if limit is not None and abs(number) > limit:
        return "abnormal_amount"
    return None


def _sanitize_suspicious_values(database: Database) -> tuple[list[dict[str, Any]], int]:
    issues: list[dict[str, Any]] = []
    sanitized = 0
    for table in VALIDATION_TABLES:
        if not database.table_exists(table):
            continue
        columns = [
            column
            for column in database.table_columns(table)
            if column not in {"serial_number", "stock_code", "stock_abbr", "report_period", "report_year", "report_date", "source_file", "source_excerpt"}
        ]
        if not columns:
            continue
        rows = database.query(f"SELECT rowid, {', '.join(columns)}, stock_code, report_period FROM {table}")
        for row in rows:
            for column in columns:
                reason = _detect_suspicious_value(column, row[column])
                if not reason:
                    continue
                database.execute(f"UPDATE {table} SET {column} = NULL WHERE rowid = ?", (row["rowid"],))
                sanitized += 1
                issues.append(
                    {
                        "table": table,
                        "rowid": int(row["rowid"]),
                        "stock_code": str(row["stock_code"] or ""),
                        "report_period": str(row["report_period"] or ""),
                        "column": column,
                        "value": row[column],
                        "reason": reason,
                    }
                )
    return issues, sanitized


def _encoding_issues(database: Database) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    if not database.table_exists("financial_metric_facts"):
        return issues
    rows = database.query("SELECT rowid, metric_key, metric_label, stock_code, report_period FROM financial_metric_facts")
    for row in rows:
        if not has_encoding_issue(row["metric_label"]):
            continue
        issues.append(
            {
                "table": "financial_metric_facts",
                "rowid": int(row["rowid"]),
                "metric_key": str(row["metric_key"] or ""),
                "metric_label": str(row["metric_label"] or ""),
                "stock_code": str(row["stock_code"] or ""),
                "report_period": str(row["report_period"] or ""),
            }
        )
    return issues


def _stock_alignment_issues(database: Database) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    known_codes = {str(row[0]) for row in database.query("SELECT stock_code FROM company_info WHERE stock_code <> ''")}
    for table in VALIDATION_TABLES:
        if not database.table_exists(table):
            continue
        rows = database.query(f"SELECT DISTINCT stock_code FROM {table} WHERE stock_code <> ''")
        missing_codes = sorted(str(row["stock_code"]) for row in rows if str(row["stock_code"]) not in known_codes)
        for stock_code in missing_codes[:100]:
            issues.append({"table": table, "stock_code": stock_code})
    return issues


def _coverage_by_table(database: Database) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for table, fields in CRITICAL_FIELDS.items():
        if not database.table_exists(table):
            payload[table] = {"row_count": 0, "fields": {}}
            continue
        row_count = database.table_row_count(table)
        field_payload: dict[str, Any] = {}
        for field in fields:
            if not database.has_column(table, field):
                field_payload[field] = {"non_null": 0, "coverage_ratio": 0.0}
                continue
            non_null = int(database.scalar(f"SELECT COUNT(1) FROM {table} WHERE {field} IS NOT NULL") or 0)
            field_payload[field] = {
                "non_null": non_null,
                "coverage_ratio": round((non_null / row_count) if row_count else 0.0, 4),
            }
        payload[table] = {"row_count": row_count, "fields": field_payload}
    return payload


def _critical_field_missing(database: Database) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    for table, fields in CRITICAL_FIELDS.items():
        if not database.table_exists(table):
            continue
        for field in fields:
            if not database.has_column(table, field):
                issues.append({"table": table, "column": field, "missing_rows": None, "reason": "column_missing"})
                continue
            missing_rows = int(database.scalar(f"SELECT COUNT(1) FROM {table} WHERE {field} IS NULL") or 0)
            issues.append({"table": table, "column": field, "missing_rows": missing_rows, "reason": "null_value"})
    return issues


def run_validation(
    database: Database,
    config: AppConfig,
    extraction_report: dict[str, Any] | None = None,
    log: LogFn | None = None,
) -> dict[str, Any]:
    _emit(log, "[开始] 运行建库校验")
    duplicate_key_issues = _duplicate_key_issues(database)
    period_issues = _period_issues(database)
    key_field_issues = _key_field_issues(database)
    suspicious_value_issues, sanitized_count = _sanitize_suspicious_values(database)
    if sanitized_count:
        refresh_metric_facts(database)
    encoding_issues = _encoding_issues(database)
    stock_alignment_issues = _stock_alignment_issues(database)
    coverage = _coverage_by_table(database)
    critical_field_issues = _critical_field_missing(database)

    report = {
        "status": "success",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "data_root": str(config.contest_data_dir.resolve()),
        "database": str(database.path.resolve()),
        "validation_rules": {
            "allowed_report_periods": ["FY", "Q1", "Q2", "Q3", "HY"],
            "max_abs_yoy_or_ratio": 500,
            "max_abs_eps": 100,
            "amount_limits_10k": AMOUNT_LIMITS,
        },
        "summary": {
            "duplicate_key_issue_count": len(duplicate_key_issues),
            "invalid_period_issue_count": len(period_issues),
            "key_field_issue_count": len(key_field_issues),
            "critical_field_issue_count": len(critical_field_issues),
            "suspicious_value_issue_count": len(suspicious_value_issues),
            "sanitized_value_count": sanitized_count,
            "encoding_issue_count": len(encoding_issues),
            "stock_alignment_issue_count": len(stock_alignment_issues),
            "document_chunk_count": database.table_row_count("document_chunks") if database.table_exists("document_chunks") else 0,
            "financial_metric_fact_count": database.table_row_count("financial_metric_facts") if database.table_exists("financial_metric_facts") else 0,
        },
        "duplicate_keys": duplicate_key_issues,
        "invalid_periods": period_issues,
        "key_fields": key_field_issues,
        "critical_field_missing": critical_field_issues,
        "suspicious_values": suspicious_value_issues,
        "encoding_issues": encoding_issues,
        "coverage_by_table": coverage,
        "stock_code_alignment": stock_alignment_issues,
        "source_conflicts": (extraction_report or {}).get("conflicts", []),
        "source_priority_rule": (extraction_report or {}).get("source_priority_rule", []),
        "extraction_coverage": (extraction_report or {}).get("coverage", {}),
    }
    path = _validation_report_path(config)
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    _emit(log, f"[完成] 运行建库校验 · 输出 {path}")
    return report


__all__ = ["run_validation"]
