from __future__ import annotations

import json
import re
from typing import Any

from .assistant import FinancialQAEngine
from .planner import plan_subtasks
from .utils import METRIC_ALIASES, normalize_text, parse_question_payload


SCOPE_SYSTEM = "\u7cfb\u7edf\u751f\u6210"
SCOPE_OFFICIAL = "\u5b98\u65b9\u9898\u5e93"
SCOPE_CUSTOM = "\u81ea\u5b9a\u4e49\u95ee\u9898"

QUESTION_TYPES = (
    "\u6570\u636e\u57fa\u672c\u67e5\u8be2",
    "\u6570\u636e\u7edf\u8ba1\u5206\u6790\u67e5\u8be2",
    "\u591a\u610f\u56fe",
    "\u610f\u56fe\u6a21\u7cca",
    "\u5f52\u56e0\u5206\u6790",
    "\u5f00\u653e\u6027\u95ee\u9898",
    "\u878d\u5408\u67e5\u8be2",
    "\u6570\u636e\u6821\u9a8c",
)
ALLOWED_QUESTION_TAGS = QUESTION_TYPES
ALLOWED_QUESTION_TAG_SET = set(ALLOWED_QUESTION_TAGS)
QUESTION_TYPE_ORDER = {name: index for index, name in enumerate(QUESTION_TYPES)}
TAG_PRIORITY = {name: index for index, name in enumerate(ALLOWED_QUESTION_TAGS)}
PRIMARY_TYPE_PRIORITY = (
    "\u6570\u636e\u6821\u9a8c",
    "\u878d\u5408\u67e5\u8be2",
    "\u5f00\u653e\u6027\u95ee\u9898",
    "\u5f52\u56e0\u5206\u6790",
    "\u610f\u56fe\u6a21\u7cca",
    "\u591a\u610f\u56fe",
    "\u6570\u636e\u7edf\u8ba1\u5206\u6790\u67e5\u8be2",
    "\u6570\u636e\u57fa\u672c\u67e5\u8be2",
)

ANALYSIS_KEYWORDS = (
    "\u8d8b\u52bf",
    "\u53d8\u5316",
    "\u540c\u6bd4",
    "\u73af\u6bd4",
    "\u5360\u6bd4",
    "\u6392\u540d",
    "\u6392\u884c",
    "top",
    "\u524d\u4e94",
    "\u524d\u5341",
    "\u6700\u9ad8",
    "\u6700\u4f4e",
    "\u5747\u503c",
    "\u5e73\u5747",
    "\u5bf9\u6bd4",
    "\u6bd4\u8f83",
    "\u6ce2\u52a8",
    "\u53ef\u89c6\u5316",
    "\u7ed8\u56fe",
    "\u56fe\u8868",
    "\u753b\u56fe",
    "\u7edf\u8ba1",
    "\u5206\u6790",
    "\u8fd1\u4e00\u5e74",
    "\u8fd1\u4e24\u5e74",
    "\u8fd1\u4e09\u5e74",
    "\u8fd1\u4e94\u5e74",
)
CAUSE_KEYWORDS = (
    "\u539f\u56e0",
    "\u4e3a\u4ec0\u4e48",
    "\u4e3a\u4f55",
    "\u8bc4\u56e0",
    "\u9a71\u52a8",
    "\u63a8\u52a8",
    "\u5f71\u54cd\u56e0\u7d20",
    "\u5bfc\u81f4",
)
OPEN_KEYWORDS = (
    "\u8bc4\u4ef7",
    "\u770b\u6cd5",
    "\u600e\u4e48\u770b",
    "\u5982\u4f55\u770b\u5f85",
    "\u5efa\u8bae",
    "\u5c55\u671b",
    "\u98ce\u9669",
    "\u673a\u4f1a",
    "\u603b\u7ed3",
    "\u6982\u62ec",
    "\u89e3\u8bfb",
    "\u5224\u65ad",
    "\u4eae\u70b9",
)
FUSION_KEYWORDS = (
    "\u7ed3\u5408",
    "\u7efc\u5408",
    "\u878d\u5408",
    "\u8054\u5408",
    "\u4ea4\u53c9",
    "\u540c\u65f6\u53c2\u8003",
    "\u591a\u6e90",
    "\u8d22\u62a5\u548c\u7814\u62a5",
    "\u7ed3\u5408\u8d22\u62a5",
    "\u7ed3\u5408\u7814\u62a5",
)
VALIDATION_KEYWORDS = (
    "\u6821\u9a8c",
    "\u6821\u6838",
    "\u6838\u5bf9",
    "\u6838\u9a8c",
    "\u9a8c\u8bc1",
    "\u4e00\u81f4",
    "\u662f\u5426\u4e00\u81f4",
    "\u662f\u5426\u5339\u914d",
    "\u5dee\u5f02",
    "\u52fe\u7a3d",
    "\u6bd4\u5bf9",
    "\u68c0\u67e5",
)
AMBIGUOUS_KEYWORDS = (
    "\u6700\u8fd1",
    "\u90fd\u6709\u54ea\u4e9b",
    "\u6709\u54ea\u4e9b",
    "\u54ea\u4e9b",
    "\u4ec0\u4e48\u60c5\u51b5",
    "\u8868\u73b0\u5982\u4f55",
    "\u600e\u4e48\u6837",
)

METRIC_KEY_LABELS = {
    "net_profit": "\u51c0\u5229\u6da6",
    "total_profit": "\u5229\u6da6\u603b\u989d",
    "main_business_revenue": "\u4e3b\u8425\u4e1a\u52a1\u6536\u5165",
    "total_operating_revenue": "\u8425\u4e1a\u603b\u6536\u5165",
    "operating_cf_net_amount": "\u7ecf\u8425\u6d3b\u52a8\u4ea7\u751f\u7684\u73b0\u91d1\u6d41\u91cf\u51c0\u989d",
    "net_cash_flow": "\u51c0\u73b0\u91d1\u6d41",
    "eps": "\u6bcf\u80a1\u6536\u76ca",
}
for alias, (_, metric_key, metric_label) in METRIC_ALIASES:
    if metric_key not in METRIC_KEY_LABELS:
        METRIC_KEY_LABELS[metric_key] = metric_label or alias


def _payload_display(payload: str) -> str:
    parsed = parse_question_payload(payload)
    questions = [normalize_text(str(item.get("Q", ""))) for item in parsed if normalize_text(str(item.get("Q", "")))]
    if not questions:
        return normalize_text(payload)
    return " / ".join(questions[:2])


def _dedupe_tags(tags: list[str]) -> list[str]:
    seen: set[str] = set()
    items: list[str] = []
    for tag in tags:
        cleaned = normalize_text(str(tag or ""))
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        items.append(cleaned)
    return items


def _question_payload(*questions: str) -> str:
    cleaned = [normalize_text(question) for question in questions if normalize_text(question)]
    if not cleaned:
        return ""
    if len(cleaned) == 1:
        return cleaned[0]
    return json.dumps([{"Q": question} for question in cleaned], ensure_ascii=False)


def _payload_questions(payload: str) -> list[str]:
    parsed = parse_question_payload(payload)
    questions = [normalize_text(str(item.get("Q", ""))) for item in parsed if normalize_text(str(item.get("Q", "")))]
    if questions:
        return questions
    cleaned = normalize_text(payload)
    return [cleaned] if cleaned else []


def _normalize_allowed_label(value: str) -> str:
    cleaned = normalize_text(value)
    return cleaned if cleaned in ALLOWED_QUESTION_TAG_SET else ""


def _allowed_labels(values: list[str] | None) -> list[str]:
    return _dedupe_tags([_normalize_allowed_label(value) for value in (values or []) if _normalize_allowed_label(value)])


def _contains_any(text: str, keywords: tuple[str, ...]) -> bool:
    return any(keyword in text for keyword in keywords if keyword)


def _is_multi_intent(questions: list[str], combined: str) -> bool:
    if len(questions) > 1:
        return True
    return plan_subtasks(combined).multi_intent


def _is_ambiguous_question(
    combined: str,
    *,
    has_analysis: bool,
    has_cause: bool,
    has_open: bool,
    has_validation: bool,
    has_fusion: bool,
) -> bool:
    if has_analysis or has_cause or has_open or has_validation or has_fusion:
        return False
    if re.search(r"20\d{2}", combined):
        return False
    return _contains_any(combined, AMBIGUOUS_KEYWORDS)


def _classify_question_labels(
    payload: str,
    declared_type: str = "",
    manual_labels: list[str] | None = None,
) -> list[str]:
    questions = _payload_questions(payload)
    combined = normalize_text(" ".join(questions))
    lower_combined = combined.lower()
    labels = set(_allowed_labels(manual_labels))
    plan = plan_subtasks(combined)
    intent_set = set(plan.intents)

    has_multi = len(questions) > 1 or plan.multi_intent
    has_validation = _contains_any(combined, VALIDATION_KEYWORDS)
    has_cause = _contains_any(combined, CAUSE_KEYWORDS)
    has_open = _contains_any(combined, OPEN_KEYWORDS)
    has_fusion = _contains_any(combined, FUSION_KEYWORDS)

    has_analysis = (
        _contains_any(combined, ANALYSIS_KEYWORDS)
        or bool(re.search(r"top\s*\d+", lower_combined))
        or bool(re.search(r"近[一二三四五六七八九十\d]+年", combined))
        or bool(intent_set.intersection({"ranking", "yoy", "maxmin", "trend", "attribution"}))
        or has_validation
        or has_cause
        or has_open
        or has_fusion
    )
    has_ambiguous = _is_ambiguous_question(
        combined,
        has_analysis=has_analysis,
        has_cause=has_cause,
        has_open=has_open,
        has_validation=has_validation,
        has_fusion=has_fusion,
    )

    base_label = "\u6570\u636e\u7edf\u8ba1\u5206\u6790\u67e5\u8be2" if has_analysis else "\u6570\u636e\u57fa\u672c\u67e5\u8be2"
    labels.add(base_label)

    if has_multi:
        labels.add("\u591a\u610f\u56fe")
    if has_ambiguous:
        labels.add("\u610f\u56fe\u6a21\u7cca")
    if has_cause:
        labels.add("\u5f52\u56e0\u5206\u6790")
    if has_open:
        labels.add("\u5f00\u653e\u6027\u95ee\u9898")
    if has_validation:
        labels.add("\u6570\u636e\u6821\u9a8c")
    if has_fusion or (has_multi and (has_cause or has_open)) or (_contains_any(combined, ("\u8d22\u62a5", "\u7814\u62a5")) and (has_cause or has_open)):
        labels.add("\u878d\u5408\u67e5\u8be2")

    normalized_declared = _normalize_allowed_label(declared_type)
    if normalized_declared:
        labels.add(normalized_declared)

    return [label for label in ALLOWED_QUESTION_TAGS if label in labels]


def _primary_question_type(labels: list[str], declared_type: str = "") -> str:
    normalized_declared = _normalize_allowed_label(declared_type)
    if normalized_declared and normalized_declared in labels:
        return normalized_declared
    for label in PRIMARY_TYPE_PRIORITY:
        if label in labels:
            return label
    return "\u6570\u636e\u57fa\u672c\u67e5\u8be2"


def _query_companies(engine: FinancialQAEngine) -> list[dict[str, str]]:
    if engine.database.table_exists("company_info") and engine.database.table_row_count("company_info"):
        rows = engine.database.query(
            "SELECT stock_code, stock_abbr, company_name FROM company_info ORDER BY stock_code"
        )
        companies = []
        for row in rows:
            abbr = normalize_text(str(row["stock_abbr"] or ""))
            name = normalize_text(str(row["company_name"] or ""))
            code = normalize_text(str(row["stock_code"] or ""))
            if not (abbr or name or code):
                continue
            companies.append({"stock_code": code, "stock_abbr": abbr or name, "company_name": name or abbr})
        if companies:
            return companies
    rows = engine.database.query(
        "SELECT DISTINCT stock_code, stock_abbr FROM financial_metric_facts WHERE stock_code <> '' OR stock_abbr <> '' ORDER BY stock_code"
    )
    return [
        {
            "stock_code": normalize_text(str(row["stock_code"] or "")),
            "stock_abbr": normalize_text(str(row["stock_abbr"] or "")),
            "company_name": normalize_text(str(row["stock_abbr"] or "")),
        }
        for row in rows
        if normalize_text(str(row["stock_code"] or "")) or normalize_text(str(row["stock_abbr"] or ""))
    ]


def _extract_company_tags(text: str, companies: list[dict[str, str]]) -> list[str]:
    matched: list[str] = []
    normalized = normalize_text(text)
    for company in companies:
        for candidate in (company.get("stock_abbr", ""), company.get("company_name", "")):
            candidate = normalize_text(candidate)
            if candidate and candidate in normalized:
                matched.append(candidate)
                break
    return _dedupe_tags(matched)


def _report_years(engine: FinancialQAEngine, stock_code: str = "") -> list[int]:
    sql = "SELECT DISTINCT report_period FROM financial_metric_facts"
    params: tuple[Any, ...] = ()
    if stock_code:
        sql += " WHERE stock_code = ?"
        params = (stock_code,)
    years: set[int] = set()
    for row in engine.database.query(sql, params):
        match = re.search(r"(20\d{2})", str(row["report_period"] or ""))
        if match:
            years.add(int(match.group(1)))
    return sorted(years, reverse=True)


def _metric_keys(engine: FinancialQAEngine, stock_code: str = "") -> set[str]:
    sql = "SELECT DISTINCT metric_key FROM financial_metric_facts WHERE metric_key <> ''"
    params: tuple[Any, ...] = ()
    if stock_code:
        sql += " AND stock_code = ?"
        params = (stock_code,)
    return {normalize_text(str(row["metric_key"] or "")) for row in engine.database.query(sql, params) if normalize_text(str(row["metric_key"] or ""))}


def _metric_label(keys: set[str], *candidates: str) -> str:
    for candidate in candidates:
        if candidate in keys:
            return METRIC_KEY_LABELS.get(candidate, candidate)
    for candidate in candidates:
        if candidate in METRIC_KEY_LABELS:
            return METRIC_KEY_LABELS[candidate]
    return "\u5173\u952e\u6307\u6807"


def _question_record(
    *,
    question_id: str,
    scope: str,
    title: str,
    payload: str,
    question_type: str,
    tags: list[str],
    source_file: str = "",
    note: str = "",
) -> dict[str, Any]:
    display = _payload_display(payload)
    return {
        "id": question_id,
        "question_id": question_id,
        "title": normalize_text(title) or display,
        "question": payload,
        "question_payload": payload,
        "display": display,
        "question_type": normalize_text(question_type),
        "scope": scope,
        "source_file": normalize_text(source_file),
        "note": normalize_text(note),
        "tags": _dedupe_tags(tags),
    }


def _build_official_questions(engine: FinancialQAEngine, companies: list[dict[str, str]]) -> list[dict[str, Any]]:
    if not engine.database.table_exists("question_bank"):
        return []
    rows = engine.database.query(
        "SELECT question_id, question_type, question_payload, source_file FROM question_bank ORDER BY question_id, rowid"
    )
    items: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for row in rows:
        question_id = normalize_text(str(row["question_id"] or ""))
        payload = normalize_text(str(row["question_payload"] or ""))
        if not payload:
            continue
        key = (question_id, payload)
        if key in seen:
            continue
        seen.add(key)
        declared_type = normalize_text(str(row["question_type"] or ""))
        tags = _classify_question_labels(payload, declared_type)
        question_type = _primary_question_type(tags, declared_type)
        title = question_id or "\u5b98\u65b9\u9898\u76ee"
        source_file = normalize_text(str(row["source_file"] or ""))
        items.append(
            _question_record(
                question_id=question_id or f"OFF{len(items) + 1:03d}",
                scope=SCOPE_OFFICIAL,
                title=title,
                payload=payload,
                question_type=question_type,
                tags=tags,
                source_file=source_file,
            )
        )
    return items


def _build_system_questions(
    engine: FinancialQAEngine,
    companies: list[dict[str, str]],
    hidden_ids: set[str] | None = None,
) -> list[dict[str, Any]]:
    primary = companies[0] if companies else {"stock_code": "", "stock_abbr": "\u534e\u6da6\u4e09\u4e5d", "company_name": "\u534e\u6da6\u4e09\u4e5d"}
    years = _report_years(engine, primary.get("stock_code", "")) or _report_years(engine)
    latest_year = str(years[0]) if years else "2024"
    metric_keys = _metric_keys(engine, primary.get("stock_code", "")) or _metric_keys(engine)
    revenue_label = _metric_label(metric_keys, "main_business_revenue", "total_operating_revenue")
    profit_label = _metric_label(metric_keys, "net_profit", "total_profit")
    cash_label = _metric_label(metric_keys, "operating_cf_net_amount", "net_cash_flow")
    company_name = primary.get("stock_abbr") or primary.get("company_name") or "\u76ee\u6807\u4f01\u4e1a"

    counters = {name: 0 for name in QUESTION_TYPES}
    items: list[dict[str, Any]] = []
    hidden_ids = {normalize_text(item) for item in (hidden_ids or set()) if normalize_text(item)}

    def add_question(question_type: str, title: str, payload: str) -> None:
        payload = normalize_text(payload) if not payload.startswith("[") else payload
        if not payload:
            return
        counters[question_type] = counters.get(question_type, 0) + 1
        question_id = f"SYS{QUESTION_TYPE_ORDER.get(question_type, 9) + 1}{counters[question_type]:03d}"
        tags = _classify_question_labels(payload, question_type)
        primary_type = _primary_question_type(tags, question_type)
        if question_id in hidden_ids:
            return
        items.append(
            _question_record(
                question_id=question_id,
                scope=SCOPE_SYSTEM,
                title=title,
                payload=payload,
                question_type=primary_type,
                tags=tags,
                source_file="",
                note="\u7cfb\u7edf\u57fa\u4e8e\u5f53\u524d\u672c\u5730\u6570\u636e\u81ea\u52a8\u751f\u6210",
            )
        )

    add_question(
        "\u6570\u636e\u57fa\u672c\u67e5\u8be2",
        "\u57fa\u7840\u4fe1\u606f\u76f4\u67e5",
        _question_payload(f"{company_name}\u7684\u5458\u5de5\u4eba\u6570\u662f\u591a\u5c11"),
    )
    add_question(
        "\u6570\u636e\u57fa\u672c\u67e5\u8be2",
        "\u5e74\u5ea6\u6838\u5fc3\u6307\u6807\u76f4\u67e5",
        _question_payload(f"{company_name}{latest_year}\u5e74\u7684{profit_label}\u662f\u591a\u5c11"),
    )
    add_question(
        "\u6570\u636e\u7edf\u8ba1\u5206\u6790\u67e5\u8be2",
        "\u8fd1\u4e09\u5e74\u8d8b\u52bf\u7ed8\u56fe",
        _question_payload(f"{company_name}\u8fd1\u4e09\u5e74\u7684{revenue_label}\u60c5\u51b5\u505a\u53ef\u89c6\u5316\u7ed8\u56fe"),
    )
    add_question(
        "\u6570\u636e\u7edf\u8ba1\u5206\u6790\u67e5\u8be2",
        "\u6392\u540d\u4e0e\u7edf\u8ba1\u5206\u6790",
        _question_payload(f"{latest_year}\u5e74\u5229\u6da6\u6700\u9ad8\u7684top5\u4f01\u4e1a\u662f\u54ea\u4e9b"),
    )
    add_question(
        "\u591a\u610f\u56fe",
        "\u8d8b\u52bf\u4e0e\u5f52\u56e0\u8054\u5408\u8ffd\u95ee",
        _question_payload(
            f"{company_name}\u8fd1\u4e09\u5e74\u7684{revenue_label}\u60c5\u51b5\u505a\u53ef\u89c6\u5316\u7ed8\u56fe",
            f"{company_name}{revenue_label}\u4e0a\u5347\u7684\u539f\u56e0\u662f\u4ec0\u4e48",
        ),
    )
    add_question(
        "\u591a\u610f\u56fe",
        "\u6307\u6807\u7ec4\u5408\u67e5\u8be2",
        _question_payload(
            f"{company_name}{latest_year}\u5e74\u7684{profit_label}\u662f\u591a\u5c11",
            f"{company_name}{latest_year}\u5e74\u7684{cash_label}\u662f\u591a\u5c11",
        ),
    )
    add_question(
        "\u610f\u56fe\u6a21\u7cca",
        "\u6a21\u7cca\u4ea7\u54c1\u68c0\u7d22",
        _question_payload("\u6700\u8fd1\u56fd\u5bb6\u533b\u4fdd\u76ee\u5f55\u65b0\u8fdb\u7684\u4e2d\u836f\u4ea7\u54c1\u6709\u54ea\u4e9b"),
    )
    add_question(
        "\u610f\u56fe\u6a21\u7cca",
        "\u6a21\u7cca\u7814\u62a5\u68c0\u7d22",
        _question_payload(f"{company_name}\u6700\u8fd1\u90fd\u51fa\u4e86\u54ea\u4e9b\u7814\u62a5"),
    )
    add_question(
        "\u5f52\u56e0\u5206\u6790",
        "\u4e3b\u8425\u4e1a\u52a1\u6536\u5165\u5f52\u56e0",
        _question_payload(f"{company_name}{revenue_label}\u4e0a\u5347\u7684\u539f\u56e0\u662f\u4ec0\u4e48"),
    )
    add_question(
        "\u5f52\u56e0\u5206\u6790",
        "\u5229\u6da6\u53d8\u5316\u5f52\u56e0",
        _question_payload(f"{company_name}{profit_label}\u589e\u957f\u7684\u539f\u56e0\u662f\u4ec0\u4e48"),
    )
    add_question(
        "\u5f00\u653e\u6027\u95ee\u9898",
        "\u7ecf\u8425\u4eae\u70b9\u4e0e\u98ce\u9669\u6982\u62ec",
        _question_payload(f"\u7ed3\u5408{company_name}\u8fd1\u4e09\u5e74\u7684\u7ecf\u8425\u8868\u73b0\uff0c\u6982\u62ec\u5176\u4e1a\u52a1\u4eae\u70b9\u4e0e\u6f5c\u5728\u98ce\u9669"),
    )
    add_question(
        "\u878d\u5408\u67e5\u8be2",
        "\u8d22\u62a5\u4e0e\u7814\u62a5\u878d\u5408\u95ee\u7b54",
        _question_payload(f"\u7ed3\u5408{company_name}\u8fd1\u4e09\u5e74\u7684{revenue_label}\u53d8\u5316\u548c\u7814\u62a5\u89c2\u70b9\uff0c\u8bf4\u660e\u589e\u957f\u539f\u56e0\u5e76\u603b\u7ed3\u4e3b\u8981\u4f9d\u636e"),
    )
    add_question(
        "\u6570\u636e\u6821\u9a8c",
        "\u6307\u6807\u4e00\u81f4\u6027\u6838\u9a8c",
        _question_payload(f"\u8bf7\u6838\u5bf9{company_name}{latest_year}\u5e74\u7684{profit_label}\u5728\u5229\u6da6\u8868\u548c\u6307\u6807\u4e8b\u5b9e\u8868\u4e2d\u662f\u5426\u4e00\u81f4"),
    )
    return items


def _build_custom_questions(custom_questions: list[dict[str, Any]], companies: list[dict[str, str]]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for row in custom_questions:
        payload = str(row.get("question", "") or "").strip()
        if not payload:
            continue
        user_tags = row.get("tags") or []
        if isinstance(user_tags, str):
            user_tag_list = [normalize_text(item) for item in user_tags.replace("\uff0c", ",").split(",") if normalize_text(item)]
        else:
            user_tag_list = [normalize_text(str(item)) for item in user_tags if normalize_text(str(item))]
        allowed_user_tags = _allowed_labels(user_tag_list)
        declared_type = normalize_text(str(row.get("question_type", "")))
        tags = _classify_question_labels(payload, declared_type, allowed_user_tags)
        question_type = _primary_question_type(tags, declared_type or (allowed_user_tags[0] if allowed_user_tags else ""))
        record = _question_record(
            question_id=str(row.get("id") or ""),
            scope=SCOPE_CUSTOM,
            title=str(row.get("title") or ""),
            payload=payload,
            question_type=question_type,
            tags=tags,
            source_file="",
            note=str(row.get("note") or ""),
        )
        record["user_tags"] = allowed_user_tags
        items.append(record)
    return items


def _tag_summary(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: dict[str, int] = {}
    for item in items:
        for tag in item.get("tags", []):
            if tag in ALLOWED_QUESTION_TAG_SET:
                counts[tag] = counts.get(tag, 0) + 1
    def sort_key(entry: tuple[str, int]) -> tuple[int, int, str]:
        tag, count = entry
        return (TAG_PRIORITY.get(tag, 100), -count, tag)
    return [{"name": tag, "count": count} for tag, count in sorted(counts.items(), key=sort_key)]


def build_question_bank_payload(
    engine: FinancialQAEngine,
    custom_questions: list[dict[str, Any]],
    hidden_system_ids: set[str] | None = None,
) -> dict[str, Any]:
    companies = _query_companies(engine)
    official = _build_official_questions(engine, companies)
    system = _build_system_questions(engine, companies, hidden_system_ids)
    custom = _build_custom_questions(custom_questions, companies)
    all_items = [*system, *official, *custom]
    return {
        "official": official,
        "system": system,
        "custom": custom,
        "tags": _tag_summary(all_items),
    }


def build_sample_questions(engine: FinancialQAEngine, hidden_system_ids: set[str] | None = None) -> list[str]:
    payload = build_question_bank_payload(engine, [], hidden_system_ids)
    samples: list[str] = []
    for item in [*payload["system"], *payload["official"]]:
        display = normalize_text(str(item.get("display") or ""))
        if display and display not in samples:
            samples.append(display)
        if len(samples) >= 8:
            break
    if not samples:
        return [
            "\u534e\u6da6\u4e09\u4e5d\u7684\u5458\u5de5\u4eba\u6570\u662f\u591a\u5c11",
            "2025\u5e74\u53d1\u5e03\u4e86\u54ea\u4e9b\u5173\u4e8e\u534e\u6da6\u4e09\u4e5d\u7684\u7814\u62a5\uff1f",
            "\u56fd\u5bb6\u533b\u4fdd\u76ee\u5f55\u65b0\u589e\u7684\u4e2d\u836f\u4ea7\u54c1\u6709\u54ea\u4e9b\uff1f",
        ]
    return samples
