from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any


PERIOD_PATTERN = re.compile(r"20\d{2}(?:FY|Q1|Q2|Q3|HY)$")


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

STANDARD_METRIC_CATALOG: list[dict[str, Any]] = [
    {
        "table": "income_sheet",
        "column": "total_profit",
        "label": "利润总额",
        "yoy_column": "total_profit_yoy_growth",
        "aliases": ["利润总额"],
    },
    {
        "table": "income_sheet",
        "column": "net_profit",
        "label": "净利润",
        "yoy_column": "net_profit_yoy_growth",
        "aliases": ["净利润", "利润"],
    },
    {
        "table": "income_sheet",
        "column": "main_business_revenue",
        "label": "主营业务收入",
        "yoy_column": "main_business_revenue_yoy_growth",
        "aliases": ["主营业务收入"],
    },
    {
        "table": "income_sheet",
        "column": "total_operating_revenue",
        "label": "营业总收入",
        "yoy_column": "operating_revenue_yoy_growth",
        "aliases": ["营业总收入", "营业收入", "销售额"],
    },
    {
        "table": "income_sheet",
        "column": "operating_expense_rnd_expenses",
        "label": "研发费用",
        "aliases": ["研发费用"],
    },
    {
        "table": "income_sheet",
        "column": "operating_expense_selling_expenses",
        "label": "销售费用",
        "aliases": ["销售费用"],
    },
    {
        "table": "income_sheet",
        "column": "operating_expense_administrative_expenses",
        "label": "管理费用",
        "aliases": ["管理费用"],
    },
    {
        "table": "income_sheet",
        "column": "operating_expense_financial_expenses",
        "label": "财务费用",
        "aliases": ["财务费用"],
    },
    {
        "table": "income_sheet",
        "column": "operating_expense_taxes_and_surcharges",
        "label": "税金及附加",
        "aliases": ["税金及附加"],
    },
    {
        "table": "income_sheet",
        "column": "total_operating_expenses",
        "label": "营业总支出",
        "aliases": ["营业总支出"],
    },
    {
        "table": "balance_sheet",
        "column": "asset_cash_and_cash_equivalents",
        "label": "货币资金",
        "aliases": ["货币资金"],
    },
    {
        "table": "balance_sheet",
        "column": "asset_accounts_receivable",
        "label": "应收账款",
        "aliases": ["应收账款"],
    },
    {
        "table": "balance_sheet",
        "column": "asset_inventory",
        "label": "存货",
        "aliases": ["存货"],
    },
    {
        "table": "balance_sheet",
        "column": "asset_total_assets",
        "label": "总资产",
        "yoy_column": "asset_total_assets_yoy_growth",
        "aliases": ["总资产", "资产总额"],
    },
    {
        "table": "balance_sheet",
        "column": "liability_total_liabilities",
        "label": "负债总额",
        "yoy_column": "liability_total_liabilities_yoy_growth",
        "aliases": ["负债总额", "总负债"],
    },
    {
        "table": "balance_sheet",
        "column": "asset_liability_ratio",
        "label": "资产负债率",
        "aliases": ["资产负债率"],
    },
    {
        "table": "balance_sheet",
        "column": "equity_parent_net_assets",
        "label": "归母净资产",
        "aliases": ["归母净资产", "归属于上市公司股东的净资产", "归属于母公司股东的净资产"],
    },
    {
        "table": "core_performance_indicators_sheet",
        "column": "eps",
        "label": "基本每股收益",
        "aliases": ["基本每股收益", "每股收益"],
    },
    {
        "table": "core_performance_indicators_sheet",
        "column": "diluted_eps",
        "label": "稀释每股收益",
        "aliases": ["稀释每股收益"],
    },
    {
        "table": "core_performance_indicators_sheet",
        "column": "operating_revenue_yoy_growth",
        "label": "营业总收入同比增长率",
        "aliases": ["营业总收入同比增长率", "营业总收入同比增长"],
    },
    {
        "table": "core_performance_indicators_sheet",
        "column": "operating_revenue_qoq_growth",
        "label": "营业总收入环比增长率",
        "aliases": ["营业总收入环比增长率", "营业总收入季度环比增长"],
    },
    {
        "table": "core_performance_indicators_sheet",
        "column": "net_profit_yoy_growth",
        "label": "净利润同比增长率",
        "aliases": ["净利润同比增长率", "净利润同比增长"],
    },
    {
        "table": "core_performance_indicators_sheet",
        "column": "net_profit_qoq_growth",
        "label": "净利润环比增长率",
        "aliases": ["净利润环比增长率", "净利润季度环比增长"],
    },
    {
        "table": "core_performance_indicators_sheet",
        "column": "gross_profit_margin",
        "label": "销售毛利率",
        "aliases": ["销售毛利率"],
    },
    {
        "table": "core_performance_indicators_sheet",
        "column": "net_profit_margin",
        "label": "销售净利率",
        "aliases": ["销售净利率"],
    },
    {
        "table": "core_performance_indicators_sheet",
        "column": "roe",
        "label": "加权平均净资产收益率",
        "aliases": ["净资产收益率", "加权平均净资产收益率"],
    },
    {
        "table": "core_performance_indicators_sheet",
        "column": "roe_weighted_excl_non_recurring",
        "label": "加权平均净资产收益率（扣非）",
        "aliases": ["加权平均净资产收益率（扣非）", "加权平均净资产收益率(扣非)"],
    },
    {
        "table": "cash_flow_sheet",
        "column": "operating_cf_net_amount",
        "label": "经营活动现金流量净额",
        "aliases": ["经营活动现金流量净额", "经营性现金流", "经营性现金流量净额"],
    },
    {
        "table": "cash_flow_sheet",
        "column": "investing_cf_net_amount",
        "label": "投资活动现金流量净额",
        "aliases": ["投资活动现金流量净额", "投资性现金流量净额"],
    },
    {
        "table": "cash_flow_sheet",
        "column": "financing_cf_net_amount",
        "label": "筹资活动现金流量净额",
        "aliases": ["筹资活动现金流量净额", "融资活动现金流量净额", "融资性现金流量净额"],
    },
    {
        "table": "cash_flow_sheet",
        "column": "net_cash_flow",
        "label": "净现金流",
        "yoy_column": "net_cash_flow_yoy_growth",
        "aliases": ["净现金流"],
    },
]

STANDARD_METRIC_LABELS: dict[tuple[str, str], str] = {
    (item["table"], item["column"]): item["label"] for item in STANDARD_METRIC_CATALOG
}

FACT_METRIC_SPECS: dict[str, list[tuple[str, str, str | None]]] = {}
for _metric in STANDARD_METRIC_CATALOG:
    FACT_METRIC_SPECS.setdefault(_metric["table"], []).append(
        (_metric["column"], _metric["label"], _metric.get("yoy_column"))
    )

METRIC_ALIASES: list[tuple[str, tuple[str, str, str]]] = [
    (alias, (item["table"], item["column"], item["label"]))
    for item in STANDARD_METRIC_CATALOG
    for alias in item.get("aliases", [])
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

CHART_KEYWORDS = ("\u53ef\u89c6\u5316", "\u7ed8\u56fe", "\u56fe\u8868", "\u6298\u7ebf\u56fe", "\u67f1\u72b6\u56fe")
TREND_KEYWORDS = ("\u8d8b\u52bf", "\u53d8\u5316", "\u60c5\u51b5")
ATTRIBUTION_KEYWORDS = ("\u539f\u56e0", "\u5f52\u56e0", "\u4e3a\u4ec0\u4e48", "\u4e3a\u4f55")
LATEST_KEYWORDS = ("\u6700\u65b0", "\u6700\u8fd1\u4e00\u671f", "\u6700\u8fd1\u4e00\u5b63", "\u5f53\u524d")
RESEARCH_KEYWORDS = ("\u7814\u62a5", "\u8bc4\u7ea7", "\u5238\u5546")
INDUSTRY_QUERY_KEYWORDS = ("\u533b\u4fdd", "\u76ee\u5f55", "\u8c08\u5224", "\u4e2d\u836f", "\u884c\u4e1a")
PRODUCT_QUERY_KEYWORDS = ("\u65b0\u589e", "\u4ea7\u54c1", "\u54ea\u4e9b")
FOLLOW_UP_HINTS = ("\u90a3", "\u90a3\u4e48", "\u8fd9\u4e2a", "\u8fd9\u4e9b", "\u5176\u4e2d", "\u8fd9\u5bb6", "\u8be5\u516c\u53f8", "\u5b83", "\u5176", "\u7ee7\u7eed", "\u5462")
RANKING_KEYWORDS = ("\u6392\u540d", "\u524d", "top", "topk")
YOY_KEYWORDS = ("\u540c\u6bd4", "\u589e\u901f", "\u589e\u5e45", "\u589e\u957f\u7387", "\u8f83\u4e0a\u5e74", "\u6bd4\u4e0a\u5e74")
MAX_KEYWORDS = ("\u6700\u5927", "\u6700\u9ad8", "\u6700\u591a")
MIN_KEYWORDS = ("\u6700\u5c0f", "\u6700\u4f4e", "\u6700\u5c11")
MULTI_INTENT_CONNECTORS = ("\u540c\u65f6", "\u4ee5\u53ca", "\u5e76\u4e14", "\u5e76\u8bf4\u660e", "\u5e76\u5206\u6790", "\u5e76\u89e3\u91ca", "\u5e76\u7ed9\u51fa", "\u5e76\u6307\u51fa", "\u5206\u522b")


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


def normalize_report_period(value: Any) -> str:
    text = compact_text(str(value or "")).upper()
    if not text:
        return ""
    normalized = (
        text.replace("年年度", "FY")
        .replace("半年报", "HY")
        .replace("半年度", "HY")
        .replace("年度", "FY")
        .replace("年报", "FY")
        .replace("第一季度", "Q1")
        .replace("一季度", "Q1")
        .replace("第二季度", "Q2")
        .replace("二季度", "Q2")
        .replace("第三季度", "Q3")
        .replace("三季度", "Q3")
        .replace("第四季度", "Q4")
        .replace("四季度", "Q4")
        .replace("年", "")
    )
    match = re.search(r"(20\d{2})(FY|HY|Q1|Q2|Q3|Q4)", normalized)
    return f"{match.group(1)}{match.group(2)}" if match else text


def is_valid_report_period(value: Any) -> bool:
    return bool(PERIOD_PATTERN.fullmatch(normalize_report_period(value)))


def has_encoding_issue(text: Any) -> bool:
    probe = normalize_text(str(text or ""))
    if not probe:
        return False
    meaningful = [char for char in probe if not char.isspace()]
    if not meaningful:
        return False
    bad_count = 0
    for char in meaningful:
        code = ord(char)
        allowed = (char.isascii() and char.isprintable()) or ("\u4e00" <= char <= "\u9fff")
        allowed = allowed or (0x3000 <= code <= 0x303F) or (0xFF00 <= code <= 0xFFEF)
        if not allowed:
            bad_count += 1
    return bad_count / len(meaningful) > 0.12


def get_standard_metric_label(table: str, column: str) -> str:
    return STANDARD_METRIC_LABELS[(table, column)]


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
