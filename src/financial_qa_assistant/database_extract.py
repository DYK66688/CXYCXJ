from __future__ import annotations

import re
from pathlib import Path
from time import perf_counter
from typing import Any, Callable

from .config import AppConfig
from .database_base import (
    Database,
    NUMBER_TOKEN_PATTERN,
    insert_document_chunks,
    write_financial_table,
)
from .pdf_tools import extract_text_safe, infer_pdf_metadata
from .utils import ensure_relative_path, normalize_stock_code, to_float


LogFn = Callable[[str], None]


def _progress_step(total: int) -> int:
    if total <= 20:
        return 1
    if total <= 100:
        return 5
    if total <= 500:
        return 10
    return 10


def _emit_progress(log: LogFn | None, label: str, index: int, total: int, path: Path) -> None:
    if log is None:
        return
    step = _progress_step(total)
    if index == 1 or index == total or index % step == 0:
        log(f"{label}开始解析：{index}/{total} · {path.name}")


def _label_pattern(label: str) -> re.Pattern[str]:
    escaped = [re.escape(char) for char in label]
    return re.compile(r"\s*".join(escaped))


def _find_first_label(text: str, labels: list[str]) -> re.Match[str] | None:
    matches: list[re.Match[str]] = []
    for label in labels:
        match = _label_pattern(label).search(text)
        if match:
            matches.append(match)
    if not matches:
        return None
    return min(matches, key=lambda item: item.start())


def _section_after_anchor(text: str, anchors: list[str]) -> str:
    match = _find_first_label(text, anchors)
    if not match:
        return text
    return text[match.start() :]


def _snippet_after_label(text: str, labels: list[str], window: int = 260) -> str:
    match = _find_first_label(text, labels)
    if not match:
        return ""
    return text[match.end() : match.end() + window]


def _should_merge_number_tokens(first: str, second: str) -> bool:
    if not first or not second:
        return False
    if first.endswith("%") or second.endswith("%"):
        return False
    if "." in first:
        return False
    if to_float(first) is None or to_float(second) is None:
        return False
    if not re.fullmatch(r"-?\d[\d,]*", first):
        return False
    if "." not in second:
        return False
    second_body = second.lstrip("+-")
    if not second_body or (not second_body[0].isdigit() and not second_body.startswith(",")):
        return False
    if second_body.startswith(","):
        return True
    last_group = first.split(",")[-1]
    return "," in first and len(last_group) < 3


def _merge_broken_number_tokens(tokens: list[str]) -> list[str]:
    merged: list[str] = []
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if index + 1 < len(tokens) and _should_merge_number_tokens(token, tokens[index + 1]):
            merged.append(token + tokens[index + 1])
            index += 2
            continue
        merged.append(token)
        index += 1
    return merged


def _separate_glued_numbers(snippet: str) -> str:
    patterns = [
        r"(?<=\.\d{2})(?=[-+]?\d{1,3},)",
        r"(?<=\.\d{2})(?=[-+]?\d+\.\d{2,4}%?)",
        r"(?<=\.\d{2})(?=20\d{2}(?:\D|$))",
        r"(?<=\.\d{4})(?=[-+]?\d{1,3},)",
        r"(?<=\.\d{4})(?=[-+]?\d+\.\d{2,4}%?)",
        r"(?<=\.\d{4})(?=20\d{2}(?:\D|$))",
    ]
    while True:
        updated = snippet
        for pattern in patterns:
            updated = re.sub(pattern, " ", updated)
        if updated == snippet:
            return updated
        snippet = updated


def _trim_snippet_to_stop_labels(snippet: str, stop_labels: list[str] | None) -> str:
    if not snippet or not stop_labels:
        return snippet
    end = len(snippet)
    for label in stop_labels:
        match = _label_pattern(label).search(snippet)
        if match and match.start() < end:
            end = match.start()
    return snippet[:end]


FINANCIAL_ROW_STOP_LABELS = [
    "\u8425\u4e1a\u603b\u6536\u5165",
    "\u8425\u4e1a\u6536\u5165",
    "\u5229\u6da6\u603b\u989d",
    "\u5f52\u5c5e\u4e8e\u4e0a\u5e02\u516c\u53f8\u80a1\u4e1c\u7684\u51c0\u5229\u6da6",
    "\u5f52\u5c5e\u4e8e\u6bcd\u516c\u53f8\u80a1\u4e1c\u7684\u51c0\u5229\u6da6",
    "\u5f52\u5c5e\u4e8e\u4e0a\u5e02\u516c\u53f8\u80a1\u4e1c\u7684\u6263\u9664\u975e\u7ecf\u5e38\u6027\u635f\u76ca\u7684\u51c0\u5229\u6da6",
    "\u7ecf\u8425\u6d3b\u52a8\u4ea7\u751f\u7684\u73b0\u91d1\u6d41\u91cf\u51c0\u989d",
    "\u57fa\u672c\u6bcf\u80a1\u6536\u76ca",
    "\u7a00\u91ca\u6bcf\u80a1\u6536\u76ca",
    "\u52a0\u6743\u5e73\u5747\u51c0\u8d44\u4ea7\u6536\u76ca\u7387",
    "\u603b\u8d44\u4ea7",
    "\u5f52\u5c5e\u4e8e\u4e0a\u5e02\u516c\u53f8\u80a1\u4e1c\u7684\u51c0\u8d44\u4ea7",
    "\u62a5\u544a\u671f\u5206\u5b63\u5ea6\u7684\u4e3b\u8981\u4f1a\u8ba1\u6570\u636e",
    "\u5206\u5b63\u5ea6\u4e3b\u8981\u8d22\u52a1\u6307\u6807",
    "\u975e\u7ecf\u5e38\u6027\u635f\u76ca\u9879\u76ee\u548c\u91d1\u989d",
    "\u5355\u4f4d\uff1a",
    "\u5355\u4f4d:",
]

PROFIT_STATEMENT_STOP_LABELS = FINANCIAL_ROW_STOP_LABELS + [
    "\u51cf\uff1a\u6240\u5f97\u7a0e\u8d39\u7528",
    "\u4e94\u3001\u51c0\u5229\u6da6",
    "\u51c0\u5229\u6da6",
    "\u516d\u3001\u5176\u4ed6\u7efc\u5408\u6536\u76ca\u7684\u7a0e\u540e\u51c0\u989d",
    "\u4e03\u3001\u7efc\u5408\u6536\u76ca\u603b\u989d",
]


def _tokens_after_labels(
    text: str,
    labels: list[str],
    window: int = 260,
    limit: int = 8,
    stop_labels: list[str] | None = None,
) -> list[str]:
    snippet = _snippet_after_label(text, labels, window)
    if not snippet:
        return []
    snippet = _trim_snippet_to_stop_labels(snippet, stop_labels)
    snippet = _separate_glued_numbers(snippet)
    return _merge_broken_number_tokens(NUMBER_TOKEN_PATTERN.findall(snippet))[:limit]


def _is_unavailable_token(token: str) -> bool:
    return token == "\u4e0d\u9002\u7528"


def _is_year_token(token: str) -> bool:
    return bool(re.fullmatch(r"20\d{2}", token))


def _is_numeric_token(token: str) -> bool:
    return to_float(token.rstrip("%")) is not None


def _is_growth_token(token: str) -> bool:
    if not token:
        return False
    if _is_unavailable_token(token) or token.endswith("%"):
        return True
    if _is_year_token(token):
        return False
    value = to_float(token)
    if value is None:
        return False
    normalized = token.replace(",", "")
    if not re.fullmatch(r"-?\d+(?:\.\d+)?", normalized):
        return False
    return abs(value) <= 1000


def _parse_first_annual_quad(tokens: list[str]) -> dict[str, float | None] | None:
    cleaned = [token for token in tokens if token]
    for index in range(max(0, len(cleaned) - 3)):
        current, previous, yoy_token, previous2 = cleaned[index : index + 4]
        if not _is_numeric_token(current):
            continue
        if not _is_numeric_token(previous):
            continue
        if not _is_growth_token(yoy_token):
            continue
        if not _is_numeric_token(previous2):
            continue
        return {
            "current": to_float(current.rstrip("%")),
            "previous": to_float(previous.rstrip("%")),
            "previous2": to_float(previous2.rstrip("%")),
            "yoy": None if _is_unavailable_token(yoy_token) else to_float(yoy_token.rstrip("%")),
        }
    return None


def _parse_adjusted_annual_row(tokens: list[str]) -> dict[str, float | None] | None:
    cleaned = [token for token in tokens if token]
    if len(cleaned) < 6:
        return None
    if not _is_numeric_token(cleaned[0]):
        return None
    if not _is_numeric_token(cleaned[1]) or not _is_numeric_token(cleaned[2]):
        return None
    if not _is_growth_token(cleaned[3]):
        return None
    if not _is_numeric_token(cleaned[4]) or not _is_numeric_token(cleaned[5]):
        return None
    return {
        "current": to_float(cleaned[0].rstrip("%")),
        "previous": to_float(cleaned[2].rstrip("%")),
        "previous2": to_float(cleaned[5].rstrip("%")),
        "yoy": None if _is_unavailable_token(cleaned[3]) else to_float(cleaned[3].rstrip("%")),
    }


def _normalize_annual_growth(
    current: float | None,
    previous: float | None,
    yoy: float | None,
) -> float | None:
    if current is None or previous is None:
        return yoy if yoy is not None and abs(yoy) <= 500 else None
    if previous == 0:
        return None
    computed = (current - previous) / abs(previous) * 100.0
    if previous * current > 0:
        if yoy is not None and abs(yoy) <= 500 and abs(yoy - computed) <= max(1.0, abs(computed) * 0.25):
            return yoy
        if abs(computed) <= 500:
            return computed
        return None
    if current == 0 and abs(computed) <= 500:
        return computed
    return None


def _parse_annual_row_tokens(tokens: list[str]) -> dict[str, float | None]:
    cleaned = [token for token in tokens if token]
    adjusted_row = _parse_adjusted_annual_row(cleaned)
    if adjusted_row is not None:
        adjusted_row["yoy"] = _normalize_annual_growth(
            adjusted_row.get("current"),
            adjusted_row.get("previous"),
            adjusted_row.get("yoy"),
        )
        return adjusted_row
    parsed_quad = _parse_first_annual_quad(cleaned)
    if parsed_quad is not None:
        parsed_quad["yoy"] = _normalize_annual_growth(
            parsed_quad.get("current"),
            parsed_quad.get("previous"),
            parsed_quad.get("yoy"),
        )
        return parsed_quad
    if len(cleaned) >= 4 and cleaned[2].endswith("%"):
        parsed = {
            "current": to_float(cleaned[0].rstrip("%")),
            "previous": to_float(cleaned[1].rstrip("%")),
            "previous2": to_float(cleaned[3].rstrip("%")),
            "yoy": to_float(cleaned[2].rstrip("%")),
        }
        parsed["yoy"] = _normalize_annual_growth(parsed["current"], parsed["previous"], parsed["yoy"])
        return parsed
    stripped = [token.rstrip("%") for token in cleaned]
    if len(stripped) >= 6:
        parsed = {
            "current": to_float(stripped[0]),
            "previous": to_float(stripped[2]),
            "previous2": to_float(stripped[5]),
            "yoy": to_float(stripped[3]),
        }
        parsed["yoy"] = _normalize_annual_growth(parsed["current"], parsed["previous"], parsed["yoy"])
        return parsed
    if len(stripped) >= 4:
        parsed = {
            "current": to_float(stripped[0]),
            "previous": to_float(stripped[1]),
            "previous2": to_float(stripped[3]),
            "yoy": None if _is_unavailable_token(stripped[2]) else to_float(stripped[2]),
        }
        parsed["yoy"] = _normalize_annual_growth(parsed["current"], parsed["previous"], parsed["yoy"])
        return parsed
    if len(stripped) >= 3:
        parsed = {
            "current": to_float(stripped[0]),
            "previous": to_float(stripped[1]),
            "yoy": None if _is_unavailable_token(stripped[2]) else to_float(stripped[2]),
            "previous2": None,
        }
        parsed["yoy"] = _normalize_annual_growth(parsed["current"], parsed["previous"], parsed["yoy"])
        return parsed
    if len(stripped) >= 2:
        return {
            "current": to_float(stripped[0]),
            "previous": to_float(stripped[1]),
            "yoy": None,
            "previous2": None,
        }
    return {"current": None, "previous": None, "yoy": None, "previous2": None}


def _to_growth_value(token: str) -> float | None:
    return None if _is_unavailable_token(token) else to_float(token.rstrip("%"))


def _looks_like_adjusted_periodic_row(current: str, previous_before: str, previous_after: str) -> bool:
    current_value = abs(to_float(current.rstrip("%")) or 0.0)
    if current_value == 0:
        return False
    previous_before_value = abs(to_float(previous_before.rstrip("%")) or 0.0)
    previous_after_value = abs(to_float(previous_after.rstrip("%")) or 0.0)
    return previous_before_value > current_value * 0.2 and previous_after_value > current_value * 0.2


def _parse_periodic_row_tokens(tokens: list[str]) -> dict[str, float | None]:
    cleaned = _merge_broken_number_tokens([token for token in tokens if token])
    if (
        len(cleaned) >= 8
        and _is_numeric_token(cleaned[0])
        and _is_numeric_token(cleaned[1])
        and _is_numeric_token(cleaned[2])
        and _is_growth_token(cleaned[3])
        and _is_numeric_token(cleaned[4])
        and _is_numeric_token(cleaned[5])
        and _is_numeric_token(cleaned[6])
        and _is_growth_token(cleaned[7])
    ):
        return {
            "current": to_float(cleaned[0].rstrip("%")),
            "current_yoy": _to_growth_value(cleaned[3]),
            "ytd": to_float(cleaned[4].rstrip("%")),
            "ytd_yoy": _to_growth_value(cleaned[7]),
        }
    if (
        len(cleaned) >= 4
        and _is_numeric_token(cleaned[0])
        and _is_numeric_token(cleaned[1])
        and _is_numeric_token(cleaned[2])
        and _is_growth_token(cleaned[3])
        and _looks_like_adjusted_periodic_row(cleaned[0], cleaned[1], cleaned[2])
    ):
        current_value = to_float(cleaned[0].rstrip("%"))
        growth_value = _to_growth_value(cleaned[3])
        return {
            "current": current_value,
            "current_yoy": growth_value,
            "ytd": current_value,
            "ytd_yoy": growth_value,
        }
    if (
        len(cleaned) >= 4
        and _is_numeric_token(cleaned[0])
        and _is_growth_token(cleaned[1])
        and _is_numeric_token(cleaned[2])
        and _is_growth_token(cleaned[3])
    ):
        return {
            "current": to_float(cleaned[0].rstrip("%")),
            "current_yoy": _to_growth_value(cleaned[1]),
            "ytd": to_float(cleaned[2].rstrip("%")),
            "ytd_yoy": _to_growth_value(cleaned[3]),
        }
    if (
        len(cleaned) >= 4
        and _is_numeric_token(cleaned[0])
        and _is_numeric_token(cleaned[1])
        and _is_numeric_token(cleaned[2])
        and _is_numeric_token(cleaned[3])
        and abs(to_float(cleaned[2].rstrip("%")) or 0.0) > abs(to_float(cleaned[0].rstrip("%")) or 0.0) * 1.05
    ):
        return {
            "current": to_float(cleaned[0].rstrip("%")),
            "current_yoy": _to_growth_value(cleaned[1]),
            "ytd": to_float(cleaned[2].rstrip("%")),
            "ytd_yoy": _to_growth_value(cleaned[3]),
        }
    if len(cleaned) >= 3 and _is_numeric_token(cleaned[0]) and _is_numeric_token(cleaned[1]) and _is_growth_token(cleaned[2]):
        current_value = to_float(cleaned[0].rstrip("%"))
        growth_value = _to_growth_value(cleaned[2])
        return {
            "current": current_value,
            "current_yoy": growth_value,
            "ytd": current_value,
            "ytd_yoy": growth_value,
        }
    if len(cleaned) >= 2 and _is_numeric_token(cleaned[0]) and _is_growth_token(cleaned[1]):
        current_value = to_float(cleaned[0].rstrip("%"))
        growth_value = _to_growth_value(cleaned[1])
        return {
            "current": current_value,
            "current_yoy": growth_value,
            "ytd": current_value,
            "ytd_yoy": growth_value,
        }
    if len(cleaned) >= 1 and _is_numeric_token(cleaned[0]):
        current_value = to_float(cleaned[0].rstrip("%"))
        return {
            "current": current_value,
            "current_yoy": None,
            "ytd": current_value,
            "ytd_yoy": None,
        }
    return {"current": None, "current_yoy": None, "ytd": None, "ytd_yoy": None}


def _parse_quarterly_breakdown_tokens(tokens: list[str]) -> list[float | None]:
    values: list[float | None] = []
    cleaned = _merge_broken_number_tokens([token for token in tokens if token])
    for token in cleaned:
        if token.endswith("%") or _is_year_token(token):
            continue
        value = to_float(token.rstrip("%"))
        if value is None:
            continue
        values.append(value)
        if len(values) == 4:
            break
    while len(values) < 4:
        values.append(None)
    return values


INCOME_GROWTH_FIELDS: list[tuple[str, str]] = [
    ("total_operating_revenue", "operating_revenue_yoy_growth"),
    ("main_business_revenue", "main_business_revenue_yoy_growth"),
    ("net_profit", "net_profit_yoy_growth"),
    ("total_profit", "total_profit_yoy_growth"),
]


ROW_METADATA_FIELDS = {
    "stock_code",
    "stock_abbr",
    "report_period",
    "report_year",
    "report_date",
    "source_file",
    "source_excerpt",
    "serial_number",
}


def _document_priority(title: str, text: str) -> int:
    probe = re.sub(r"\s+", "", f"{title}{text[:300]}")
    priority = 0
    if any(
        keyword in probe
        for keyword in (
            "年度报告",
            "半年度报告",
            "季度报告",
            "一季度报告",
            "三季度报告",
        )
    ):
        priority += 10
    if "摘要" in probe:
        priority -= 5
    return priority


def _allowed_periods_for_report(report_period: str) -> set[str]:
    allowed = {report_period}
    if report_period.endswith("FY") and report_period[:4].isdigit():
        year = report_period[:4]
        allowed.update({f"{year}Q1", f"{year}HY", f"{year}Q3"})
    return allowed


def _row_has_metric_values(row: dict[str, Any]) -> bool:
    for key, value in row.items():
        if key.startswith("__") or key in ROW_METADATA_FIELDS:
            continue
        if value not in (None, ""):
            return True
    return False


def _filter_rows_for_allowed_periods(
    rows: dict[tuple[str, str], dict[str, Any]],
    allowed_periods: dict[str, set[str]],
) -> dict[tuple[str, str], dict[str, Any]]:
    filtered: dict[tuple[str, str], dict[str, Any]] = {}
    for key, row in rows.items():
        stock_code, report_period = key
        if report_period not in allowed_periods.get(stock_code, set()):
            continue
        if not _row_has_metric_values(row):
            continue
        filtered[key] = row
    return filtered


def _previous_same_period(report_period: str) -> str | None:
    match = re.fullmatch(r"(20\d{2})(FY|HY|Q1|Q2|Q3|Q4)", report_period)
    if not match:
        return None
    return f"{int(match.group(1)) - 1}{match.group(2)}"


def _recompute_growth(current: float | None, previous: float | None) -> float | None:
    if current is None or previous is None or previous == 0:
        return None
    if current * previous < 0:
        return None
    return (current - previous) / abs(previous) * 100.0


def _period_metric_value(report_period: str, values: dict[str, float | None]) -> float | None:
    if report_period.endswith(("Q3", "Q4")):
        return values.get("ytd") if values.get("ytd") is not None else values.get("current")
    return values.get("current") if values.get("current") is not None else values.get("ytd")


def _period_metric_growth(report_period: str, values: dict[str, float | None]) -> float | None:
    if report_period.endswith(("Q3", "Q4")):
        return values.get("ytd_yoy") if values.get("ytd_yoy") is not None else values.get("current_yoy")
    return values.get("current_yoy") if values.get("current_yoy") is not None else values.get("ytd_yoy")


def _recompute_income_growths(income_rows: dict[tuple[str, str], dict[str, Any]]) -> None:
    for (stock_code, report_period), row in list(income_rows.items()):
        previous_period = _previous_same_period(report_period)
        if not previous_period:
            continue
        previous_row = income_rows.get((stock_code, previous_period))
        if not previous_row:
            continue
        for value_field, yoy_field in INCOME_GROWTH_FIELDS:
            row[yoy_field] = _recompute_growth(
                to_float(row.get(value_field)),
                to_float(previous_row.get(value_field)),
            )


def _money_to_10k(value: float | None) -> float | None:
    if value is None:
        return None
    return value / 10000.0


def _ensure_row(
    store: dict[tuple[str, str], dict[str, Any]],
    stock_code: str,
    stock_abbr: str,
    report_period: str,
    report_date: str,
    source_file: str,
    source_excerpt: str,
    priority: int = 0,
) -> dict[str, Any]:
    key = (stock_code, report_period)
    row = store.setdefault(
        key,
        {
            "stock_code": stock_code,
            "stock_abbr": stock_abbr,
            "report_period": report_period,
            "report_year": report_period[:4] if report_period[:4].isdigit() else "",
            "report_date": report_date,
            "source_file": source_file,
            "source_excerpt": source_excerpt,
            "__row_priority__": priority,
            "__field_priority__": {},
        },
    )
    current_priority = int(row.get("__row_priority__", -1) or -1)
    if priority >= current_priority:
        if report_date:
            row["report_date"] = report_date
        if source_file:
            row["source_file"] = source_file
        if source_excerpt:
            row["source_excerpt"] = source_excerpt
        row["__row_priority__"] = priority
    elif source_excerpt and not row.get("source_excerpt"):
        row["source_excerpt"] = source_excerpt
    if stock_abbr and not row.get("stock_abbr"):
        row["stock_abbr"] = stock_abbr
    if report_period[:4].isdigit() and not row.get("report_year"):
        row["report_year"] = report_period[:4]
    return row

def _assign(row: dict[str, Any], field: str, value: Any, priority: int = 0) -> None:
    if value is None:
        return
    priorities = row.setdefault("__field_priority__", {})
    current_priority = int(priorities.get(field, -1) or -1)
    if priority < current_priority:
        return
    row[field] = value
    priorities[field] = priority

def _assign_prefer_reasonable(row: dict[str, Any], field: str, value: Any, priority: int = 0) -> None:
    candidate = to_float(value)
    if candidate is None:
        return
    priorities = row.setdefault("__field_priority__", {})
    current_priority = int(priorities.get(field, -1) or -1)
    if priority < current_priority:
        return
    current = to_float(row.get(field))
    if current is None or priority > current_priority or (abs(current) < 1 and abs(candidate) >= 1):
        row[field] = value
        priorities[field] = priority

def _cumulative_quarter_values(values: list[float | None]) -> list[float | None]:
    running_total = 0.0
    seen_any = False
    cumulative: list[float | None] = []
    for value in values:
        if value is None:
            cumulative.append(None if not seen_any else running_total)
            continue
        running_total += value
        seen_any = True
        cumulative.append(running_total)
    return cumulative


def _apply_annual_quarter_breakdown(
    text: str,
    stock_code: str,
    stock_abbr: str,
    report_period: str,
    report_date: str,
    source_file: str,
    income_rows: dict[tuple[str, str], dict[str, Any]],
    cash_rows: dict[tuple[str, str], dict[str, Any]],
    priority: int = 0,
) -> None:
    anchors = [
        "分季度主要财务指标",
        "报告期分季度的主要会计数据",
        "分季度主要财务数据",
    ]
    if not _find_first_label(text, anchors):
        return
    year = int(report_period[:4])
    section = _section_after_anchor(text, anchors)
    period_labels = [f"{year}Q1", f"{year}HY", f"{year}Q3", f"{year}FY"]

    def assign_income_row(labels: list[str], field: str) -> None:
        values = _parse_quarterly_breakdown_tokens(_tokens_after_labels(section, labels, 420, 12, stop_labels=FINANCIAL_ROW_STOP_LABELS))
        for period, value in zip(period_labels, _cumulative_quarter_values(values), strict=False):
            if value is None:
                continue
            row = _ensure_row(
                income_rows,
                stock_code,
                stock_abbr,
                period,
                report_date,
                source_file,
                "年度分季度数据",
                priority=priority,
            )
            _assign_prefer_reasonable(row, field, _money_to_10k(value), priority=priority)

    def assign_cash_row(labels: list[str]) -> None:
        values = _parse_quarterly_breakdown_tokens(_tokens_after_labels(section, labels, 420, 12, stop_labels=FINANCIAL_ROW_STOP_LABELS))
        for period, value in zip(period_labels, _cumulative_quarter_values(values), strict=False):
            if value is None:
                continue
            row = _ensure_row(
                cash_rows,
                stock_code,
                stock_abbr,
                period,
                report_date,
                source_file,
                "年度分季度数据",
                priority=priority,
            )
            _assign_prefer_reasonable(row, "operating_cf_net_amount", _money_to_10k(value), priority=priority)

    assign_income_row(["营业总收入", "营业收入"], "total_operating_revenue")
    assign_income_row(["营业总收入", "营业收入"], "main_business_revenue")
    assign_income_row(
        ["归属于上市公司股东的净利润", "归属于母公司股东的净利润"],
        "net_profit",
    )
    assign_cash_row(["经营活动产生的现金流量净额"])


def _apply_annual_key_data(
    text: str,
    stock_code: str,
    stock_abbr: str,
    report_period: str,
    report_date: str,
    source_file: str,
    income_rows: dict[tuple[str, str], dict[str, Any]],
    kpi_rows: dict[tuple[str, str], dict[str, Any]],
    balance_rows: dict[tuple[str, str], dict[str, Any]],
    cash_rows: dict[tuple[str, str], dict[str, Any]],
    priority: int = 0,
) -> None:
    if not report_period.endswith("FY"):
        return
    section = _section_after_anchor(text, ["近三年主要会计数据和财务指标", "主要会计数据和财务指标", "主要财务数据"])

    current_income = _ensure_row(income_rows, stock_code, stock_abbr, report_period, report_date, source_file, "年度主要财务指标", priority=priority)
    current_cash = _ensure_row(cash_rows, stock_code, stock_abbr, report_period, report_date, source_file, "年度主要财务指标", priority=priority)
    current_kpi = _ensure_row(kpi_rows, stock_code, stock_abbr, report_period, report_date, source_file, "年度主要财务指标", priority=priority)
    current_balance = _ensure_row(balance_rows, stock_code, stock_abbr, report_period, report_date, source_file, "年度主要财务指标", priority=priority)

    revenue_labels = ["营业总收入", "营业收入"]
    revenue = _parse_annual_row_tokens(_tokens_after_labels(section, revenue_labels, 360, stop_labels=FINANCIAL_ROW_STOP_LABELS))
    revenue_current = _money_to_10k(revenue.get("current"))
    _assign(current_income, "total_operating_revenue", revenue_current, priority=priority)
    _assign(current_income, "main_business_revenue", revenue_current, priority=priority)
    revenue_yoy = revenue.get("yoy")
    if revenue_yoy is None:
        revenue_yoy = _recompute_growth(revenue.get("current"), revenue.get("previous"))
    _assign(current_income, "operating_revenue_yoy_growth", revenue_yoy, priority=priority)
    _assign(current_income, "main_business_revenue_yoy_growth", revenue_yoy, priority=priority)

    net_profit_labels = ["归属于上市公司股东的净利润", "归属于母公司股东的净利润"]
    net_profit = _parse_annual_row_tokens(_tokens_after_labels(section, net_profit_labels, 360, stop_labels=FINANCIAL_ROW_STOP_LABELS))
    _assign(current_income, "net_profit", _money_to_10k(net_profit.get("current")), priority=priority)
    net_profit_yoy = net_profit.get("yoy")
    if net_profit_yoy is None:
        net_profit_yoy = _recompute_growth(net_profit.get("current"), net_profit.get("previous"))
    _assign(current_income, "net_profit_yoy_growth", net_profit_yoy, priority=priority)

    cash_values = _parse_annual_row_tokens(_tokens_after_labels(section, ["经营活动产生的现金流量净额"], 320, stop_labels=FINANCIAL_ROW_STOP_LABELS))
    _assign(current_cash, "operating_cf_net_amount", _money_to_10k(cash_values.get("current")), priority=priority)

    eps_values = _parse_annual_row_tokens(_tokens_after_labels(section, ["基本每股收益", "每股收益"], 220, stop_labels=FINANCIAL_ROW_STOP_LABELS))
    _assign(current_kpi, "eps", eps_values.get("current"), priority=priority)

    asset_values = _parse_annual_row_tokens(_tokens_after_labels(section, ["总资产"], 260, stop_labels=FINANCIAL_ROW_STOP_LABELS))
    _assign(current_balance, "asset_total_assets", _money_to_10k(asset_values.get("current")), priority=priority)
    asset_yoy = asset_values.get("yoy")
    if asset_yoy is None:
        asset_yoy = _recompute_growth(asset_values.get("current"), asset_values.get("previous"))
    _assign(current_balance, "asset_total_assets_yoy_growth", asset_yoy, priority=priority)

    _apply_annual_quarter_breakdown(
        text,
        stock_code,
        stock_abbr,
        report_period,
        report_date,
        source_file,
        income_rows,
        cash_rows,
        priority=max(0, priority - 10),
    )


def _apply_periodic_key_data(
    text: str,
    stock_code: str,
    stock_abbr: str,
    report_period: str,
    report_date: str,
    source_file: str,
    income_rows: dict[tuple[str, str], dict[str, Any]],
    kpi_rows: dict[tuple[str, str], dict[str, Any]],
    cash_rows: dict[tuple[str, str], dict[str, Any]],
    priority: int = 0,
) -> None:
    section = _section_after_anchor(text, ["主要财务数据", "主要会计数据和财务指标"])
    income_row = _ensure_row(income_rows, stock_code, stock_abbr, report_period, report_date, source_file, "季度指标", priority=priority)

    revenue = _parse_periodic_row_tokens(_tokens_after_labels(section, ["营业总收入", "营业收入"], 320, stop_labels=FINANCIAL_ROW_STOP_LABELS))
    _assign(income_row, "total_operating_revenue", _money_to_10k(_period_metric_value(report_period, revenue)), priority=priority)
    _assign(income_row, "main_business_revenue", _money_to_10k(_period_metric_value(report_period, revenue)), priority=priority)
    _assign(income_row, "operating_revenue_yoy_growth", _period_metric_growth(report_period, revenue), priority=priority)
    _assign(income_row, "main_business_revenue_yoy_growth", _period_metric_growth(report_period, revenue), priority=priority)

    total_profit = _parse_periodic_row_tokens(_tokens_after_labels(section, ["利润总额"], 280, stop_labels=FINANCIAL_ROW_STOP_LABELS))
    _assign(income_row, "total_profit", _money_to_10k(_period_metric_value(report_period, total_profit)), priority=priority)
    _assign(income_row, "total_profit_yoy_growth", _period_metric_growth(report_period, total_profit), priority=priority)

    net_profit = _parse_periodic_row_tokens(_tokens_after_labels(section, ["归属于上市公司股东的净利润", "归属于母公司股东的净利润"], 320, stop_labels=FINANCIAL_ROW_STOP_LABELS))
    _assign(income_row, "net_profit", _money_to_10k(_period_metric_value(report_period, net_profit)), priority=priority)
    _assign(income_row, "net_profit_yoy_growth", _period_metric_growth(report_period, net_profit), priority=priority)

    cash_values = _parse_periodic_row_tokens(_tokens_after_labels(section, ["经营活动产生的现金流量净额"], 280, stop_labels=FINANCIAL_ROW_STOP_LABELS))
    cash_row = _ensure_row(cash_rows, stock_code, stock_abbr, report_period, report_date, source_file, "经营现金流", priority=priority)
    _assign(cash_row, "operating_cf_net_amount", _money_to_10k(_period_metric_value(report_period, cash_values)), priority=priority)

    eps_values = _parse_periodic_row_tokens(_tokens_after_labels(section, ["基本每股收益", "每股收益"], 220, stop_labels=FINANCIAL_ROW_STOP_LABELS))
    kpi_row = _ensure_row(kpi_rows, stock_code, stock_abbr, report_period, report_date, source_file, "每股收益", priority=priority)
    _assign(kpi_row, "eps", _period_metric_value(report_period, eps_values), priority=priority)


def _apply_profit_statement_total_profit(
    text: str,
    stock_code: str,
    stock_abbr: str,
    report_period: str,
    report_date: str,
    source_file: str,
    income_rows: dict[tuple[str, str], dict[str, Any]],
    priority: int = 0,
) -> None:
    anchors = ["合并利润表", "合并年初到报告期末利润表"]
    if not _find_first_label(text, anchors):
        return
    section = _section_after_anchor(text, anchors)
    tokens = _tokens_after_labels(
        section,
        ["四、利润总额", "利润总额"],
        240,
        6,
        stop_labels=PROFIT_STATEMENT_STOP_LABELS,
    )
    values = [to_float(token.rstrip("%")) for token in tokens if token]
    values = [value for value in values if value is not None]
    if not values:
        return
    current_row = _ensure_row(
        income_rows,
        stock_code,
        stock_abbr,
        report_period,
        report_date,
        source_file,
        "利润总额",
        priority=priority,
    )
    current_value = _money_to_10k(values[0])
    _assign(current_row, "total_profit", current_value, priority=priority)
    if len(values) >= 2:
        previous_value = _money_to_10k(values[1])
        _assign(current_row, "total_profit_yoy_growth", _recompute_growth(current_value, previous_value), priority=priority)


def _should_extract_report(path: Path) -> bool:
    return path.stat().st_size <= 25_000_000


def load_financial_reports(database: Database, config: AppConfig, log: LogFn | None = None) -> None:
    company_rows = {
        row["stock_code"]: dict(row)
        for row in database.query("SELECT stock_code, stock_abbr, company_name FROM company_info ORDER BY stock_code")
    }
    name_lookup: dict[str, dict[str, Any]] = {}
    for row in company_rows.values():
        name_lookup[row["stock_abbr"]] = row
        name_lookup[row["company_name"]] = row

    income_rows: dict[tuple[str, str], dict[str, Any]] = {}
    kpi_rows: dict[tuple[str, str], dict[str, Any]] = {}
    balance_rows: dict[tuple[str, str], dict[str, Any]] = {}
    cash_rows: dict[tuple[str, str], dict[str, Any]] = {}
    allowed_periods: dict[str, set[str]] = {}
    documents: list[dict[str, Any]] = []
    pdf_paths = config.financial_report_pdfs()
    total = len(pdf_paths)

    if log is not None:
        log(f"开始解析财报 PDF，共 {total} 份")

    for index, pdf_path in enumerate(pdf_paths, start=1):
        _emit_progress(log, "财报 PDF", index, total, pdf_path)
        started = perf_counter()
        relative_path = ensure_relative_path(pdf_path, config.workspace_root)
        text = extract_text_safe(pdf_path) if _should_extract_report(pdf_path) else pdf_path.stem
        elapsed = perf_counter() - started
        metadata = infer_pdf_metadata(pdf_path, text)
        stock_code = normalize_stock_code(metadata.get("stock_code", ""))
        stock_name = metadata.get("stock_name", "")
        report_period = metadata.get("report_period", "")
        report_date = metadata.get("report_date", "")

        company = company_rows.get(stock_code) if stock_code else None
        if not company and stock_name:
            company = name_lookup.get(stock_name)
        if company:
            stock_code = normalize_stock_code(company["stock_code"])
            stock_abbr = company["stock_abbr"]
        else:
            stock_abbr = stock_name

        if stock_code and report_period:
            allowed_periods.setdefault(stock_code, set()).update(_allowed_periods_for_report(report_period))

        documents.append(
            {
                "pdf_path": pdf_path,
                "relative_path": relative_path,
                "text": text,
                "metadata": metadata,
                "stock_code": stock_code,
                "stock_abbr": stock_abbr,
                "report_period": report_period,
                "report_date": report_date,
            }
        )
        if log is not None and (index <= 3 or index == total or index % _progress_step(total) == 0 or elapsed >= 10):
            log(f"财报 PDF 已解析：{index}/{total} · {pdf_path.name} · {elapsed:.1f}s")

    for document in documents:
        pdf_path = document["pdf_path"]
        relative_path = document["relative_path"]
        text = document["text"]
        metadata = document["metadata"]
        stock_code = document["stock_code"]
        stock_abbr = document["stock_abbr"]
        report_period = document["report_period"]
        report_date = document["report_date"]

        insert_document_chunks(
            database,
            "financial_report_pdf",
            metadata.get("title", pdf_path.stem),
            stock_code,
            stock_abbr,
            report_period,
            relative_path,
            text,
        )

        if not stock_code or not report_period or text == pdf_path.stem:
            continue

        document_priority = _document_priority(str(metadata.get("title", pdf_path.stem)), text)
        if report_period.endswith("FY"):
            _apply_annual_key_data(
                text,
                stock_code,
                stock_abbr,
                report_period,
                report_date,
                relative_path,
                income_rows,
                kpi_rows,
                balance_rows,
                cash_rows,
                priority=document_priority + 20,
            )
        else:
            _apply_periodic_key_data(
                text,
                stock_code,
                stock_abbr,
                report_period,
                report_date,
                relative_path,
                income_rows,
                kpi_rows,
                cash_rows,
                priority=document_priority + 30,
            )
        _apply_profit_statement_total_profit(
            text,
            stock_code,
            stock_abbr,
            report_period,
            report_date,
            relative_path,
            income_rows,
            priority=document_priority + 40,
        )

    _recompute_income_growths(income_rows)
    income_rows = _filter_rows_for_allowed_periods(income_rows, allowed_periods)
    kpi_rows = _filter_rows_for_allowed_periods(kpi_rows, allowed_periods)
    balance_rows = _filter_rows_for_allowed_periods(balance_rows, allowed_periods)
    cash_rows = _filter_rows_for_allowed_periods(cash_rows, allowed_periods)
    write_financial_table(database, "income_sheet", income_rows)
    write_financial_table(database, "core_performance_indicators_sheet", kpi_rows)
    write_financial_table(database, "balance_sheet", balance_rows)
    write_financial_table(database, "cash_flow_sheet", cash_rows)
    if log is not None:
        log(
            "财报 PDF 解析完成："
            f"收入表 {len(income_rows)} 行，核心指标表 {len(kpi_rows)} 行，"
            f"资产负债表 {len(balance_rows)} 行，现金流量表 {len(cash_rows)} 行"
        )

def load_research_pdf_chunks(database: Database, config: AppConfig, log: LogFn | None = None) -> None:
    stock_meta = {row["title"]: dict(row) for row in database.query("SELECT * FROM stock_research")}
    industry_titles = {row["title"] for row in database.query("SELECT * FROM industry_research")}
    pdf_paths = config.research_report_pdfs()
    total = len(pdf_paths)

    if log is not None:
        log(f"开始解析研报 PDF，共 {total} 份")

    for index, pdf_path in enumerate(pdf_paths, start=1):
        _emit_progress(log, "研报 PDF", index, total, pdf_path)
        started = perf_counter()
        text = extract_text_safe(pdf_path)
        elapsed = perf_counter() - started
        relative_path = ensure_relative_path(pdf_path, config.workspace_root)
        metadata = infer_pdf_metadata(pdf_path, text)
        source_type = "research_pdf"
        stock_code = ""
        stock_name = metadata.get("stock_name", "")
        title = pdf_path.stem
        report_period = metadata.get("report_period", "")

        if title in stock_meta:
            source_type = "stock_research_pdf"
            stock_code = normalize_stock_code(stock_meta[title].get("stockCode", ""))
            stock_name = stock_meta[title].get("stockName", "")
        elif title in industry_titles:
            source_type = "industry_research_pdf"

        insert_document_chunks(
            database,
            source_type,
            title,
            stock_code,
            stock_name,
            report_period,
            relative_path,
            text,
        )
        if log is not None and (index <= 3 or index == total or index % _progress_step(total) == 0 or elapsed >= 10):
            log(f"研报 PDF 已解析：{index}/{total} · {pdf_path.name} · {elapsed:.1f}s")
    if log is not None:
        log(f"研报 PDF 解析完成：共写入 {total} 份 PDF 的分片")
