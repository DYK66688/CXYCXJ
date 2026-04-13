from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any


COMPANY_FIELD_ALIASES: dict[str, tuple[str, str]] = {
    "\u5458\u5de5\u4eba\u6570": ("employee_count", "\u5458\u5de5\u4eba\u6570"),
    "\u96c7\u5458\u4eba\u6570": ("employee_count", "\u5458\u5de5\u4eba\u6570"),
    "\u7ba1\u7406\u4eba\u5458\u4eba\u6570": ("management_count", "\u7ba1\u7406\u4eba\u5458\u4eba\u6570"),
    "\u6ce8\u518c\u8d44\u672c": ("registered_capital", "\u6ce8\u518c\u8d44\u672c"),
    "\u4e0a\u5e02\u4ea4\u6613\u6240": ("listed_exchange", "\u4e0a\u5e02\u4ea4\u6613\u6240"),
    "\u6240\u5c5e\u884c\u4e1a": ("industry", "\u6240\u5c5e\u8bc1\u76d1\u4f1a\u884c\u4e1a"),
    "\u884c\u4e1a": ("industry", "\u6240\u5c5e\u8bc1\u76d1\u4f1a\u884c\u4e1a"),
    "\u82f1\u6587\u540d\u79f0": ("english_name", "\u82f1\u6587\u540d\u79f0"),
    "\u516c\u53f8\u540d\u79f0": ("company_name", "\u516c\u53f8\u540d\u79f0"),
}

METRIC_ALIASES: list[tuple[str, tuple[str, str, str]]] = [
    ("\u5229\u6da6\u603b\u989d", ("income_sheet", "total_profit", "\u5229\u6da6\u603b\u989d")),
    ("\u51c0\u5229\u6da6", ("income_sheet", "net_profit", "\u51c0\u5229\u6da6")),
    ("\u4e3b\u8425\u4e1a\u52a1\u6536\u5165", ("income_sheet", "main_business_revenue", "\u4e3b\u8425\u4e1a\u52a1\u6536\u5165")),
    ("\u8425\u4e1a\u603b\u6536\u5165", ("income_sheet", "total_operating_revenue", "\u8425\u4e1a\u603b\u6536\u5165")),
    ("\u8425\u4e1a\u6536\u5165", ("income_sheet", "total_operating_revenue", "\u8425\u4e1a\u6536\u5165")),
    ("\u9500\u552e\u989d", ("income_sheet", "total_operating_revenue", "\u9500\u552e\u989d")),
    ("\u6bcf\u80a1\u6536\u76ca", ("core_performance_indicators_sheet", "eps", "\u6bcf\u80a1\u6536\u76ca")),
    ("\u8d27\u5e01\u8d44\u91d1", ("balance_sheet", "asset_cash_and_cash_equivalents", "\u8d27\u5e01\u8d44\u91d1")),
    ("\u5e94\u6536\u8d26\u6b3e", ("balance_sheet", "asset_accounts_receivable", "\u5e94\u6536\u8d26\u6b3e")),
    ("\u5b58\u8d27", ("balance_sheet", "asset_inventory", "\u5b58\u8d27")),
    ("\u7ecf\u8425\u6027\u73b0\u91d1\u6d41", ("cash_flow_sheet", "operating_cf_net_amount", "\u7ecf\u8425\u6027\u73b0\u91d1\u6d41")),
    ("\u7ecf\u8425\u6d3b\u52a8\u4ea7\u751f\u7684\u73b0\u91d1\u6d41\u91cf\u51c0\u989d", ("cash_flow_sheet", "operating_cf_net_amount", "\u7ecf\u8425\u6d3b\u52a8\u4ea7\u751f\u7684\u73b0\u91d1\u6d41\u91cf\u51c0\u989d")),
    ("\u51c0\u73b0\u91d1\u6d41", ("cash_flow_sheet", "net_cash_flow", "\u51c0\u73b0\u91d1\u6d41")),
    ("\u5229\u6da6", ("income_sheet", "net_profit", "\u5229\u6da6")),
]

CAUSE_KEYWORDS = (
    "\u539f\u56e0",
    "\u4e3b\u8981\u7cfb",
    "\u53d7\u76ca\u4e8e",
    "\u7531\u4e8e",
    "\u9a71\u52a8",
    "\u5e26\u52a8",
    "\u63a8\u52a8",
    "\u5bfc\u81f4",
    "\u589e\u957f",
    "\u63d0\u5347",
    "\u6062\u590d",
    "\u6539\u5584",
)


QUARTER_1_FLAGS = ("\u7b2c\u4e00\u5b63\u5ea6", "\u4e00\u5b63\u5ea6", "Q1", "q1")
QUARTER_2_FLAGS = ("\u7b2c\u4e8c\u5b63\u5ea6", "\u4e8c\u5b63\u5ea6", "Q2", "q2")
QUARTER_3_FLAGS = ("\u7b2c\u4e09\u5b63\u5ea6", "\u4e09\u5b63\u5ea6", "Q3", "q3")
QUARTER_4_FLAGS = ("\u7b2c\u56db\u5b63\u5ea6", "\u56db\u5b63\u5ea6", "Q4", "q4")
HALF_YEAR_FLAGS = ("\u534a\u5e74\u5ea6\u62a5\u544a", "\u534a\u5e74\u62a5", "\u534a\u5e74\u5ea6", "\u534a\u5e74")


def normalize_stock_code(value: Any) -> str:
    text = str(value or "").strip()
    digits = re.sub(r"\D", "", text)
    if not digits:
        return text
    if len(digits) <= 6:
        return digits.zfill(6)
    return digits


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def compact_text(text: str) -> str:
    return re.sub(r"\s+", "", text or "")


def parse_question_payload(raw: str) -> list[dict[str, str]]:
    raw = (raw or "").strip()
    if not raw:
        return []
    if raw.startswith("["):
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return [{"Q": raw}]
        payload: list[dict[str, str]] = []
        for item in data:
            if isinstance(item, dict) and "Q" in item:
                payload.append({"Q": str(item["Q"]).strip()})
            elif isinstance(item, str):
                payload.append({"Q": item.strip()})
        return payload
    return [{"Q": raw}]


def dump_json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, indent=2)


def detect_company_field(question: str) -> tuple[str, str] | None:
    for alias, target in sorted(COMPANY_FIELD_ALIASES.items(), key=lambda item: len(item[0]), reverse=True):
        if alias in question:
            return target
    return None


def detect_metric(question: str) -> tuple[str, str, str] | None:
    for alias, target in sorted(METRIC_ALIASES, key=lambda item: len(item[0]), reverse=True):
        if alias in question:
            return target
    return None


def detect_top_k(question: str) -> int | None:
    lowered = question.lower()
    patterns = (
        r"top\s*(\d+)",
        r"\u524d\s*(\d+)",
        r"\u6392\u540d\u524d\s*(\d+)",
    )
    for pattern in patterns:
        match = re.search(pattern, lowered)
        if match:
            return int(match.group(1))
    return None


def parse_period(question: str, context: dict[str, Any] | None = None) -> dict[str, str] | None:
    context = context or {}
    compact = compact_text(question)
    year_match = re.search(r"(20\d{2})\u5e74", compact)
    year = year_match.group(1) if year_match else context.get("year")
    if not year:
        return context.get("period_info")

    if any(flag in compact for flag in QUARTER_1_FLAGS):
        return {"year": year, "report_period": f"{year}Q1", "label": f"{year}\u5e74\u7b2c\u4e00\u5b63\u5ea6"}
    if any(flag in compact for flag in QUARTER_2_FLAGS):
        return {"year": year, "report_period": f"{year}Q2", "label": f"{year}\u5e74\u7b2c\u4e8c\u5b63\u5ea6"}
    if any(flag in compact for flag in QUARTER_3_FLAGS):
        return {"year": year, "report_period": f"{year}Q3", "label": f"{year}\u5e74\u7b2c\u4e09\u5b63\u5ea6"}
    if any(flag in compact for flag in QUARTER_4_FLAGS):
        return {"year": year, "report_period": f"{year}Q4", "label": f"{year}\u5e74\u7b2c\u56db\u5b63\u5ea6"}
    if any(flag in compact for flag in HALF_YEAR_FLAGS):
        return {"year": year, "report_period": f"{year}HY", "label": f"{year}\u5e74\u534a\u5e74\u5ea6"}
    return {"year": year, "report_period": f"{year}FY", "label": f"{year}\u5e74\u5e74\u5ea6"}


def sortable_period(period: str) -> tuple[int, int]:
    period = str(period or "")
    match = re.search(r"(20\d{2})", period)
    year = int(match.group(1)) if match else 0
    order = {"Q1": 1, "Q2": 2, "HY": 2, "Q3": 3, "Q4": 4, "FY": 5, "E": 6}
    suffix = ""
    for key in order:
        if key in period:
            suffix = key
            break
    return year, order.get(suffix, 0)


def tokenize(text: str) -> list[str]:
    tokens: list[str] = []
    for part in re.findall(r"[\u4e00-\u9fff]+|[A-Za-z0-9_.%-]+", text or ""):
        if re.fullmatch(r"[\u4e00-\u9fff]+", part):
            tokens.append(part)
            if len(part) > 1:
                tokens.extend(part[index : index + 2] for index in range(len(part) - 1))
        else:
            tokens.append(part.lower())
    return tokens


def score_text(query: str, text: str) -> float:
    query_tokens = tokenize(query)
    target_tokens = tokenize(text)
    if not query_tokens or not target_tokens:
        return 0.0
    target_set = set(target_tokens)
    overlap = sum(1 for token in query_tokens if token in target_set)
    phrase_bonus = 2.0 if normalize_text(query) in normalize_text(text) else 0.0
    return overlap + phrase_bonus


def split_sentences(text: str) -> list[str]:
    fragments = re.split(r"(?<=[\u3002\uff01\uff1f\uff1b!?;\n])", text or "")
    return [normalize_text(fragment) for fragment in fragments if normalize_text(fragment)]


def ensure_relative_path(path: Path, root: Path) -> str:
    try:
        return str(path.resolve().relative_to(root.resolve())).replace("\\", "/")
    except ValueError:
        return str(path).replace("\\", "/")


def artifact_name(prefix: str, value: str, suffix: str) -> str:
    digest = hashlib.md5(value.encode("utf-8")).hexdigest()[:10]
    return f"{prefix}_{digest}{suffix}"


def to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    multiplier = 1.0
    if text.endswith("%"):
        text = text[:-1]
    if text.endswith("\u4ebf"):
        multiplier = 100000000.0
        text = text[:-1]
    elif text.endswith("\u4e07"):
        multiplier = 10000.0
        text = text[:-1]
    elif text.endswith("\u5143"):
        text = text[:-1]
    try:
        return float(text) * multiplier
    except ValueError:
        return None


def format_money_from_10k(value: Any) -> str:
    number = to_float(value)
    if number is None:
        return "\u672a\u77e5"
    if abs(number) >= 10000:
        return f"{number / 10000:.2f}\u4ebf\u5143"
    return f"{number:,.2f}\u4e07\u5143"
