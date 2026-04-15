from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from .config import AppConfig
from .database_base import Database, refresh_metric_facts
from .utils import (
    CORE_FINANCIAL_TABLES,
    RATIO_GROWTH_COLUMNS,
    FOCUSED_AMOUNT_COLUMNS,
    get_metric_label_by_column,
    has_encoding_issue,
    has_order_of_magnitude_gap,
    is_valid_report_period,
    median_value,
    normalize_report_period,
    percentile_value,
    previous_report_period,
    relative_change_multiple,
    safe_abs_ratio,
    to_float,
)


LogFn = Callable[[str], None]

VALIDATION_TABLES = list(CORE_FINANCIAL_TABLES)

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

AMOUNT_LIMITS_10K: dict[str, float] = {
    "total_operating_revenue": 30_000_000.0,
    "main_business_revenue": 30_000_000.0,
    "net_profit": 5_000_000.0,
    "total_profit": 5_000_000.0,
    "asset_total_assets": 40_000_000.0,
    "asset_cash_and_cash_equivalents": 8_000_000.0,
    "asset_accounts_receivable": 8_000_000.0,
    "asset_inventory": 8_000_000.0,
    "liability_total_liabilities": 35_000_000.0,
    "equity_parent_net_assets": 20_000_000.0,
    "operating_cf_net_amount": 8_000_000.0,
    "investing_cf_net_amount": 8_000_000.0,
    "financing_cf_net_amount": 8_000_000.0,
}

EPS_COLUMNS = {"eps", "diluted_eps", "net_asset_per_share", "operating_cf_per_share"}
NON_NEGATIVE_AMOUNT_COLUMNS = {
    "total_operating_revenue",
    "main_business_revenue",
    "asset_total_assets",
    "asset_cash_and_cash_equivalents",
    "asset_accounts_receivable",
    "asset_inventory",
    "equity_parent_net_assets",
}
COMPONENT_TO_TOTAL_RULES = {
    "main_business_revenue": "total_operating_revenue",
    "asset_cash_and_cash_equivalents": "asset_total_assets",
    "asset_accounts_receivable": "asset_total_assets",
    "asset_inventory": "asset_total_assets",
    "equity_parent_net_assets": "asset_total_assets",
}
GROWTH_BASE_COLUMNS = {
    "net_profit_yoy_growth": "net_profit",
    "operating_revenue_yoy_growth": "total_operating_revenue",
    "main_business_revenue_yoy_growth": "main_business_revenue",
    "total_profit_yoy_growth": "total_profit",
    "asset_total_assets_yoy_growth": "asset_total_assets",
    "liability_total_liabilities_yoy_growth": "liability_total_liabilities",
}
BASE_TO_GROWTH_COLUMNS: dict[str, list[str]] = defaultdict(list)
for _growth_column, _base_column in GROWTH_BASE_COLUMNS.items():
    BASE_TO_GROWTH_COLUMNS[_base_column].append(_growth_column)
PEER_OUTLIER_MIN_COUNT = 4
PEER_AMOUNT_FACTOR = 8.0
PEER_MEDIAN_FACTOR = 12.0
HISTORY_AMOUNT_FACTOR = 15.0
HISTORY_RATIO_FACTOR = 8.0
MAX_ABS_GROWTH_OR_RATIO = 300.0
MAX_ABS_EPS = 20.0
MAX_ABS_ROE = 80.0
MAX_ABS_ASSET_LIABILITY_RATIO = 100.0
AMOUNT_HISTORY_FACTORS: dict[str, float] = {
    "total_operating_revenue": 12.0,
    "main_business_revenue": 12.0,
    "net_profit": 10.0,
    "total_profit": 10.0,
    "asset_total_assets": 8.0,
    "operating_cf_net_amount": 10.0,
    "investing_cf_net_amount": 10.0,
    "financing_cf_net_amount": 10.0,
}
AMOUNT_MIN_PEER_LIMITS_10K: dict[str, float] = {
    "total_operating_revenue": 1500.0,
    "main_business_revenue": 1500.0,
    "net_profit": 200.0,
    "total_profit": 200.0,
    "asset_total_assets": 2500.0,
    "operating_cf_net_amount": 300.0,
    "investing_cf_net_amount": 300.0,
    "financing_cf_net_amount": 300.0,
}
RATIO_HISTORY_FACTORS: dict[str, float] = {
    "roe": 6.0,
    "asset_liability_ratio": 4.0,
    "diluted_eps": 6.0,
}

COLUMN_LABEL_MAP: dict[str, str] = {
    "total_operating_revenue": "营业总收入",
    "main_business_revenue": "主营业务收入",
    "net_profit": "净利润",
    "total_profit": "利润总额",
    "asset_total_assets": "总资产",
    "operating_cf_net_amount": "经营活动现金流量净额",
    "investing_cf_net_amount": "投资活动现金流量净额",
    "financing_cf_net_amount": "筹资活动现金流量净额",
    "roe": "加权平均净资产收益率",
    "diluted_eps": "稀释每股收益",
}


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
                    "stock_code": str(row["stock_code"] or ""),
                    "report_period": str(row["report_period"] or ""),
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


def _load_validation_rows(database: Database) -> dict[str, list[dict[str, Any]]]:
    payload: dict[str, list[dict[str, Any]]] = {}
    for table in VALIDATION_TABLES:
        if not database.table_exists(table):
            payload[table] = []
            continue
        payload[table] = [dict(row) for row in database.query(f"SELECT rowid, * FROM {table}")]
    return payload


def _build_row_lookup(table_rows: dict[str, list[dict[str, Any]]]) -> dict[str, dict[tuple[str, str], dict[str, Any]]]:
    lookup: dict[str, dict[tuple[str, str], dict[str, Any]]] = {}
    for table, rows in table_rows.items():
        row_map: dict[tuple[str, str], dict[str, Any]] = {}
        for row in rows:
            stock_code = str(row.get("stock_code") or "")
            report_period = normalize_report_period(row.get("report_period"))
            if stock_code and report_period:
                row_map[(stock_code, report_period)] = row
        lookup[table] = row_map
    return lookup


def _build_peer_profiles(table_rows: dict[str, list[dict[str, Any]]]) -> dict[str, dict[str, dict[str, dict[str, Any]]]]:
    raw_profiles: dict[str, dict[str, dict[str, list[float]]]] = {}
    for table, rows in table_rows.items():
        if not rows:
            raw_profiles.setdefault(table, {})
            continue
        columns = [
            column
            for column in rows[0].keys()
            if column not in {"rowid", "serial_number", "stock_code", "stock_abbr", "report_period", "report_year", "report_date", "source_file", "source_excerpt"}
        ]
        table_profile: dict[str, dict[str, list[float]]] = raw_profiles.setdefault(table, {})
        for row in rows:
            report_period = normalize_report_period(row.get("report_period"))
            if not report_period:
                continue
            for column in columns:
                number = to_float(row.get(column))
                if number is None:
                    continue
                table_profile.setdefault(column, {}).setdefault(report_period, []).append(number)
    peer_profiles: dict[str, dict[str, dict[str, dict[str, Any]]]] = {}
    for table, column_payload in raw_profiles.items():
        peer_profiles[table] = {}
        for column, period_payload in column_payload.items():
            peer_profiles[table][column] = {}
            for report_period, values in period_payload.items():
                peer_profiles[table][column][report_period] = {
                    "count": len(values),
                    "median": median_value(values),
                    "p90": percentile_value(values, 0.9),
                }
    return peer_profiles


def _row_amount(row: dict[str, Any], *columns: str) -> float | None:
    for column in columns:
        number = to_float(row.get(column))
        if number is not None:
            return number
    return None


def _column_label(column: str) -> str:
    return get_metric_label_by_column(column)


def _history_amount_factor(column: str) -> float:
    return AMOUNT_HISTORY_FACTORS.get(column, HISTORY_AMOUNT_FACTOR)


def _ratio_history_factor(column: str) -> float:
    return RATIO_HISTORY_FACTORS.get(column, HISTORY_RATIO_FACTOR)


def _amount_reason(
    column: str,
    number: float,
    row: dict[str, Any],
    previous_row: dict[str, Any] | None,
    peer_profile: dict[str, Any] | None,
) -> str | None:
    limit = AMOUNT_LIMITS_10K.get(column)
    if limit is not None and abs(number) > limit:
        return "amount_exceeds_absolute_limit"
    if column in NON_NEGATIVE_AMOUNT_COLUMNS and number < 0:
        return "negative_amount_not_allowed"

    revenue = _row_amount(row, "main_business_revenue", "total_operating_revenue")
    assets = _row_amount(row, "asset_total_assets")
    liabilities = _row_amount(row, "liability_total_liabilities")

    if column in {"net_profit", "total_profit"} and revenue is not None and abs(number) > max(abs(revenue) * 1.05, 10.0):
        return "profit_exceeds_revenue"
    if column == "main_business_revenue":
        total_revenue = _row_amount(row, "total_operating_revenue")
        if total_revenue is not None and abs(number) > max(abs(total_revenue) * 1.05, 10.0):
            return "main_business_exceeds_total_revenue"
    if column in COMPONENT_TO_TOTAL_RULES:
        total_value = _row_amount(row, COMPONENT_TO_TOTAL_RULES[column])
        if total_value is not None and abs(number) > max(abs(total_value) * 1.05, 10.0):
            return "component_exceeds_total"
    if column == "liability_total_liabilities" and assets is not None and abs(number) > max(abs(assets) * 1.5, 10.0):
        return "liability_exceeds_assets"
    if column == "equity_parent_net_assets" and assets is not None and abs(number) > max(abs(assets) * 1.2, 10.0):
        return "equity_exceeds_assets"
    if column in {"operating_cf_net_amount", "investing_cf_net_amount", "financing_cf_net_amount"}:
        anchor = max(abs(revenue or 0.0), abs(assets or 0.0), abs(liabilities or 0.0), 1.0)
        if abs(number) > anchor * 3.5:
            return "cashflow_scale_mismatch"

    previous_value = to_float(previous_row.get(column)) if previous_row else None
    if previous_value is not None and abs(number) >= 50.0 and has_order_of_magnitude_gap(number, previous_value, factor=_history_amount_factor(column)):
        return "history_scale_jump"

    if peer_profile and int(peer_profile.get("count") or 0) >= PEER_OUTLIER_MIN_COUNT:
        peer_p90 = to_float(peer_profile.get("p90"))
        peer_median = to_float(peer_profile.get("median"))
        peer_limit = max(
            abs(peer_p90 or 0.0) * PEER_AMOUNT_FACTOR,
            abs(peer_median or 0.0) * PEER_MEDIAN_FACTOR,
            AMOUNT_MIN_PEER_LIMITS_10K.get(column, 500.0),
        )
        if peer_limit > 0 and abs(number) > peer_limit:
            return "peer_outlier"
    return None


def _ratio_reason(
    table: str,
    column: str,
    number: float,
    row: dict[str, Any],
    previous_row: dict[str, Any] | None,
    row_lookup: dict[tuple[str, str], dict[str, Any]],
) -> str | None:
    if column == "asset_liability_ratio" and not (0.0 <= number <= MAX_ABS_ASSET_LIABILITY_RATIO):
        return "asset_liability_ratio_out_of_range"
    if column in {"roe", "roe_weighted_excl_non_recurring"} and abs(number) > MAX_ABS_ROE:
        return "abnormal_roe"
    if abs(number) > MAX_ABS_GROWTH_OR_RATIO:
        return "abnormal_growth_or_ratio"

    previous_value = to_float(previous_row.get(column)) if previous_row else None
    if previous_value is not None and abs(number) > 10.0 and has_order_of_magnitude_gap(number, previous_value, factor=_ratio_history_factor(column)):
        return "ratio_scale_jump"

    base_column = GROWTH_BASE_COLUMNS.get(column)
    stock_code = str(row.get("stock_code") or "")
    report_period = normalize_report_period(row.get("report_period"))
    if base_column and stock_code and report_period:
        previous_period = previous_report_period(report_period)
        base_current = to_float(row.get(base_column))
        base_previous_row = row_lookup.get((stock_code, previous_period)) if previous_period else None
        base_previous = to_float(base_previous_row.get(base_column)) if base_previous_row else None
        if base_current is not None and base_previous is not None:
            if has_order_of_magnitude_gap(base_current, base_previous, factor=_history_amount_factor(base_column)):
                return "growth_scale_mismatch"
            if base_previous != 0 and base_current * base_previous >= 0:
                computed = (base_current - base_previous) / abs(base_previous) * 100.0
                if abs(computed) > MAX_ABS_GROWTH_OR_RATIO:
                    return "recomputed_growth_out_of_range"
                if abs(number - computed) > max(20.0, abs(computed) * 0.6):
                    return "growth_recompute_mismatch"
            relative_multiple = relative_change_multiple(base_current, base_previous)
            if relative_multiple is not None and relative_multiple >= _history_amount_factor(base_column) and abs(number) < 80.0:
                return "growth_scale_mismatch"
    return None


def _eps_reason(column: str, number: float, previous_row: dict[str, Any] | None) -> str | None:
    if abs(number) > MAX_ABS_EPS:
        return "abnormal_eps"
    previous_value = to_float(previous_row.get(column)) if previous_row else None
    if previous_value is not None and abs(number) > 0.5 and has_order_of_magnitude_gap(number, previous_value, factor=_ratio_history_factor(column)):
        return "eps_scale_jump"
    return None


def _detect_suspicious_value(
    table: str,
    column: str,
    row: dict[str, Any],
    row_lookup: dict[tuple[str, str], dict[str, Any]],
    peer_profiles: dict[str, dict[str, dict[str, dict[str, Any]]]],
) -> str | None:
    number = to_float(row.get(column))
    if number is None:
        return None

    stock_code = str(row.get("stock_code") or "")
    report_period = normalize_report_period(row.get("report_period"))
    previous_period = previous_report_period(report_period)
    previous_row = row_lookup.get((stock_code, previous_period)) if stock_code and previous_period else None
    peer_profile = ((peer_profiles.get(table) or {}).get(column) or {}).get(report_period)

    if column in FOCUSED_AMOUNT_COLUMNS:
        return _amount_reason(column, number, row, previous_row, peer_profile)
    if column in EPS_COLUMNS:
        return _eps_reason(column, number, previous_row)
    if column in RATIO_GROWTH_COLUMNS or column.endswith("_growth") or column.endswith("_ratio"):
        return _ratio_reason(table, column, number, row, previous_row, row_lookup)
    return None


def _collect_suspicious_candidates(
    database: Database,
) -> tuple[dict[tuple[str, int, str], dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    table_rows = _load_validation_rows(database)
    row_lookup = _build_row_lookup(table_rows)
    peer_profiles = _build_peer_profiles(table_rows)
    issues: dict[tuple[str, int, str], dict[str, Any]] = {}
    for table, rows in table_rows.items():
        if not rows:
            continue
        columns = [
            column
            for column in rows[0].keys()
            if column not in {"rowid", "serial_number", "stock_code", "stock_abbr", "report_period", "report_year", "report_date", "source_file", "source_excerpt"}
        ]
        for row in rows:
            for column in columns:
                reason = _detect_suspicious_value(table, column, row, row_lookup.get(table, {}), peer_profiles)
                if not reason:
                    continue
                issues[(table, int(row["rowid"]), column)] = {
                    "table": table,
                    "rowid": int(row["rowid"]),
                    "stock_code": str(row.get("stock_code") or ""),
                    "report_period": str(row.get("report_period") or ""),
                    "column": column,
                    "column_label": _column_label(column),
                    "value": row.get(column),
                    "reason": reason,
                    "source_file": str(row.get("source_file") or ""),
                    "source_excerpt": str(row.get("source_excerpt") or ""),
                }
    for table, rows in table_rows.items():
        for row in rows:
            for column in list(BASE_TO_GROWTH_COLUMNS.keys()):
                if (table, int(row["rowid"]), column) not in issues:
                    continue
                for dependent_column in BASE_TO_GROWTH_COLUMNS.get(column, []):
                    if row.get(dependent_column) in (None, ""):
                        continue
                    key = (table, int(row["rowid"]), dependent_column)
                    if key in issues:
                        continue
                    dependent_reason = _detect_suspicious_value(
                        table,
                        dependent_column,
                        row,
                        row_lookup.get(table, {}),
                        peer_profiles,
                    ) or "base_metric_sanitized"
                    issues[key] = {
                        "table": table,
                        "rowid": int(row["rowid"]),
                        "stock_code": str(row.get("stock_code") or ""),
                        "report_period": str(row.get("report_period") or ""),
                        "column": dependent_column,
                        "column_label": _column_label(dependent_column),
                        "value": row.get(dependent_column),
                        "reason": dependent_reason,
                        "source_file": str(row.get("source_file") or ""),
                        "source_excerpt": str(row.get("source_excerpt") or ""),
                    }
    return issues, table_rows


def _sanitize_suspicious_values(database: Database) -> tuple[list[dict[str, Any]], int]:
    issues_by_cell, table_rows = _collect_suspicious_candidates(database)
    sanitized = 0
    for issue in sorted(issues_by_cell.values(), key=lambda item: (item["table"], item["rowid"], item["column"])):
        database.execute(f"UPDATE {issue['table']} SET {issue['column']} = NULL WHERE rowid = ?", (issue["rowid"],))
        for row in table_rows.get(issue["table"], []):
            if int(row.get("rowid") or 0) == int(issue["rowid"]):
                row[issue["column"]] = None
                break
        sanitized += 1
    return list(issues_by_cell.values()), sanitized


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
    alias_codes: dict[str, dict[str, str]] = {}
    if database.table_exists("financial_company_aliases"):
        for row in database.query("SELECT stock_code, stock_abbr, company_name, source_table, source FROM financial_company_aliases"):
            stock_code = str(row["stock_code"] or "")
            if stock_code:
                alias_codes[stock_code] = {
                    "stock_abbr": str(row["stock_abbr"] or ""),
                    "company_name": str(row["company_name"] or ""),
                    "source_table": str(row["source_table"] or ""),
                    "source": str(row["source"] or ""),
                }
    for table in VALIDATION_TABLES:
        if not database.table_exists(table):
            continue
        rows = database.query(f"SELECT DISTINCT stock_code, stock_abbr FROM {table} WHERE stock_code <> ''")
        for row in rows:
            stock_code = str(row["stock_code"] or "")
            if not stock_code or stock_code in known_codes:
                continue
            alias_payload = alias_codes.get(stock_code)
            issues.append(
                {
                    "table": table,
                    "stock_code": stock_code,
                    "stock_abbr": str(row["stock_abbr"] or ""),
                    "status": "covered_by_alias_table" if alias_payload else "missing_master_data",
                    "alias_source_table": (alias_payload or {}).get("source_table", ""),
                    "alias_source": (alias_payload or {}).get("source", ""),
                }
            )
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


def _count_by(items: list[dict[str, Any]], key: str) -> dict[str, int]:
    counter = Counter(str(item.get(key) or "") for item in items if str(item.get(key) or ""))
    return dict(sorted(counter.items(), key=lambda item: (-item[1], item[0])))


def _sanitized_rows_by_table(items: list[dict[str, Any]]) -> dict[str, int]:
    payload: dict[str, set[int]] = defaultdict(set)
    for item in items:
        payload[str(item.get("table") or "")].add(int(item.get("rowid") or 0))
    return {table: len(row_ids) for table, row_ids in sorted(payload.items()) if table}


def _top_risky_companies(items: list[dict[str, Any]], limit: int = 10) -> list[dict[str, Any]]:
    counter = Counter(str(item.get("stock_code") or "") for item in items if str(item.get("stock_code") or ""))
    return [
        {"stock_code": stock_code, "issue_count": count}
        for stock_code, count in counter.most_common(limit)
    ]


def _load_lineage_conflicts(database: Database, extraction_report: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    conflicts: list[dict[str, Any]] = []
    if database.table_exists("structured_field_lineage"):
        rows = database.query(
            """
            SELECT table_name, stock_code, report_period, field_name, field_value,
                   source_file, source_excerpt, source_priority, extractor_stage,
                   updated_at, decision, candidate_value, candidate_source_file,
                   candidate_source_excerpt, candidate_priority, candidate_extractor_stage
            FROM structured_field_lineage
            WHERE decision <> 'chosen'
            ORDER BY table_name, stock_code, report_period, field_name, id
            """
        )
        conflicts = [
            {
                "table": str(row["table_name"] or ""),
                "stock_code": str(row["stock_code"] or ""),
                "report_period": str(row["report_period"] or ""),
                "field_name": str(row["field_name"] or ""),
                "field_value": row["field_value"],
                "source_file": str(row["source_file"] or ""),
                "source_excerpt": str(row["source_excerpt"] or ""),
                "source_priority": int(row["source_priority"] or 0),
                "extractor_stage": str(row["extractor_stage"] or ""),
                "updated_at": str(row["updated_at"] or ""),
                "decision": str(row["decision"] or ""),
                "candidate_value": row["candidate_value"],
                "candidate_source_file": str(row["candidate_source_file"] or ""),
                "candidate_source_excerpt": str(row["candidate_source_excerpt"] or ""),
                "candidate_priority": int(row["candidate_priority"] or 0),
                "candidate_extractor_stage": str(row["candidate_extractor_stage"] or ""),
            }
            for row in rows
        ]
    if conflicts:
        return conflicts
    return list((extraction_report or {}).get("lineage_conflicts", []))


def _conflicted_fields_by_table(lineage_conflicts: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    payload: dict[str, Counter[str]] = defaultdict(Counter)
    for item in lineage_conflicts:
        table = str(item.get("table") or "")
        field = str(item.get("field_name") or item.get("field") or "")
        if table and field:
            payload[table][field] += 1
    return {
        table: dict(sorted(counter.items(), key=lambda item: (-item[1], item[0])))
        for table, counter in sorted(payload.items())
    }


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
    lineage_conflicts = _load_lineage_conflicts(database, extraction_report)

    report = {
        "status": "success",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "data_root": str(config.contest_data_dir.resolve()),
        "database": str(database.path.resolve()),
        "validation_rules": {
            "allowed_report_periods": ["FY", "Q1", "Q2", "Q3", "HY"],
            "max_abs_growth_or_ratio": MAX_ABS_GROWTH_OR_RATIO,
            "max_abs_eps": MAX_ABS_EPS,
            "max_abs_roe": MAX_ABS_ROE,
            "amount_limits_10k": AMOUNT_LIMITS_10K,
            "history_amount_factor": HISTORY_AMOUNT_FACTOR,
            "amount_history_factors": AMOUNT_HISTORY_FACTORS,
            "peer_amount_factor": PEER_AMOUNT_FACTOR,
            "peer_median_factor": PEER_MEDIAN_FACTOR,
            "ratio_history_factor": HISTORY_RATIO_FACTOR,
            "ratio_history_factors": RATIO_HISTORY_FACTORS,
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
            "suspicious_values_by_column": _count_by(suspicious_value_issues, "column"),
            "sanitized_rows_by_table": _sanitized_rows_by_table(suspicious_value_issues),
            "top_risky_companies": _top_risky_companies(suspicious_value_issues),
        },
        "duplicate_keys": duplicate_key_issues,
        "invalid_periods": period_issues,
        "key_fields": key_field_issues,
        "critical_field_missing": critical_field_issues,
        "suspicious_values": suspicious_value_issues,
        "encoding_issues": encoding_issues,
        "coverage_by_table": coverage,
        "stock_code_alignment": stock_alignment_issues,
        "source_conflicts": list((extraction_report or {}).get("conflicts", [])),
        "lineage_conflicts": lineage_conflicts,
        "conflicted_fields_by_table": _conflicted_fields_by_table(lineage_conflicts),
        "source_priority_rule": (extraction_report or {}).get("source_priority_rule", []),
        "extraction_coverage": (extraction_report or {}).get("coverage", {}),
    }
    path = _validation_report_path(config)
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    _emit(log, f"[完成] 运行建库校验 -> {path}")
    return report


__all__ = ["run_validation"]
