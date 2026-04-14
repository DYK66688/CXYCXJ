from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from .utils import (
    ATTRIBUTION_KEYWORDS,
    CHART_KEYWORDS,
    INDUSTRY_QUERY_KEYWORDS,
    LATEST_KEYWORDS,
    MAX_KEYWORDS,
    MIN_KEYWORDS,
    MULTI_INTENT_CONNECTORS,
    PRODUCT_QUERY_KEYWORDS,
    RANKING_KEYWORDS,
    RESEARCH_KEYWORDS,
    TREND_KEYWORDS,
    YOY_KEYWORDS,
    compact_text,
    detect_metric,
    detect_top_k,
    normalize_text,
)


INTENT_ORDER = ("ranking", "single_value", "yoy", "maxmin", "trend", "attribution")


@dataclass(slots=True)
class PlannedSubtask:
    intent: str
    question: str
    params: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class QueryPlan:
    normalized_question: str
    compact_question: str
    intents: list[str]
    query_mode: str
    subtasks: list[PlannedSubtask]
    fragments: list[str]
    top_k: int | None
    wants_chart: bool
    wants_trend: bool
    wants_attribution: bool
    wants_research: bool
    wants_latest: bool
    industry_query: bool
    product_query: bool
    multi_intent: bool


def _contains_any(text: str, keywords: tuple[str, ...]) -> bool:
    return any(keyword in text for keyword in keywords if keyword)


def normalize_question(question: str) -> tuple[str, str]:
    normalized = normalize_text(question)
    return normalized, compact_text(normalized)


def split_question_fragments(question: str) -> list[str]:
    normalized, compact = normalize_question(question)
    if not normalized:
        return []
    fragments = re.split(r"[；;？?]", normalized)
    parts: list[str] = []
    for fragment in fragments:
        cleaned = normalize_text(fragment)
        if not cleaned:
            continue
        matched = False
        for connector in sorted(MULTI_INTENT_CONNECTORS, key=len, reverse=True):
            if connector in cleaned:
                split_parts = [normalize_text(item) for item in cleaned.split(connector) if normalize_text(item)]
                if len(split_parts) >= 2:
                    parts.extend(split_parts)
                    matched = True
                    break
        if not matched:
            parts.append(cleaned)
    unique_parts: list[str] = []
    for part in parts or [normalized]:
        if part not in unique_parts:
            unique_parts.append(part)
    if len(unique_parts) <= 1 and compact.count("、") >= 2 and _contains_any(compact, YOY_KEYWORDS + MAX_KEYWORDS + MIN_KEYWORDS):
        return [normalized]
    return unique_parts or [normalized]


def detect_plan_intents(question: str) -> list[str]:
    normalized, compact = normalize_question(question)
    metric = detect_metric(normalized)
    top_k = detect_top_k(normalized)
    wants_chart = _contains_any(compact, CHART_KEYWORDS)
    wants_trend = wants_chart or _contains_any(compact, TREND_KEYWORDS) or bool(re.search(r"近[一二三四五六七八九十\d]+年", compact))
    wants_attribution = _contains_any(compact, ATTRIBUTION_KEYWORDS)
    intents: list[str] = []
    if top_k or _contains_any(compact, RANKING_KEYWORDS):
        intents.append("ranking")
    if _contains_any(compact, YOY_KEYWORDS):
        intents.append("yoy")
    if _contains_any(compact, MAX_KEYWORDS + MIN_KEYWORDS):
        intents.append("maxmin")
    if wants_trend:
        intents.append("trend")
    if wants_attribution:
        intents.append("attribution")
    if metric and not intents:
        intents.append("single_value")
    ordered = [intent for intent in INTENT_ORDER if intent in intents]
    return ordered or (["single_value"] if metric else [])


def detect_query_mode(
    question: str,
    *,
    has_company_field: bool = False,
    has_company: bool = False,
    has_metric: bool = False,
) -> str:
    _normalized, compact = normalize_question(question)
    top_k = detect_top_k(question)
    wants_chart = _contains_any(compact, CHART_KEYWORDS)
    wants_trend = wants_chart or _contains_any(compact, TREND_KEYWORDS)
    wants_attribution = _contains_any(compact, ATTRIBUTION_KEYWORDS)
    wants_research = _contains_any(compact, RESEARCH_KEYWORDS)
    industry_query = _contains_any(compact, INDUSTRY_QUERY_KEYWORDS)
    if has_company_field:
        return "company_profile"
    if wants_research:
        return "research"
    if industry_query:
        return "industry_lookup"
    if wants_attribution:
        return "attribution"
    if top_k:
        return "ranking"
    if wants_trend:
        return "trend"
    if has_company or has_metric:
        return "structured"
    return "retrieval"


def plan_subtasks(question: str) -> QueryPlan:
    normalized, compact = normalize_question(question)
    fragments = split_question_fragments(normalized)
    intents = detect_plan_intents(normalized)
    top_k = detect_top_k(normalized)
    wants_chart = _contains_any(compact, CHART_KEYWORDS)
    wants_trend = wants_chart or _contains_any(compact, TREND_KEYWORDS) or bool(re.search(r"近[一二三四五六七八九十\d]+年", compact))
    wants_attribution = _contains_any(compact, ATTRIBUTION_KEYWORDS)
    wants_research = _contains_any(compact, RESEARCH_KEYWORDS)
    wants_latest = _contains_any(compact, LATEST_KEYWORDS)
    industry_query = _contains_any(compact, INDUSTRY_QUERY_KEYWORDS)
    product_query = industry_query and _contains_any(compact, PRODUCT_QUERY_KEYWORDS)
    metric = detect_metric(normalized)
    query_mode = detect_query_mode(normalized, has_metric=metric is not None)

    subtasks: list[PlannedSubtask] = []
    if len(fragments) > 1:
        for fragment in fragments:
            fragment_intents = detect_plan_intents(fragment)
            subtasks.append(
                PlannedSubtask(
                    intent=fragment_intents[0] if fragment_intents else "single_value",
                    question=fragment,
                    params={"fragment_intents": fragment_intents},
                )
            )
    elif {"ranking", "yoy", "maxmin"}.issubset(set(intents)):
        subtasks = [
            PlannedSubtask("ranking", normalized, {"top_k": top_k}),
            PlannedSubtask("yoy", normalized, {}),
            PlannedSubtask("maxmin", normalized, {"kind": "max"}),
        ]
    elif {"trend", "attribution"}.issubset(set(intents)):
        subtasks = [
            PlannedSubtask("trend", normalized, {"chart": wants_chart}),
            PlannedSubtask("attribution", normalized, {}),
        ]
    else:
        primary_intent = intents[0] if intents else ("single_value" if metric else "retrieval")
        subtasks = [PlannedSubtask(primary_intent, normalized, {"top_k": top_k})]

    return QueryPlan(
        normalized_question=normalized,
        compact_question=compact,
        intents=intents,
        query_mode=query_mode,
        subtasks=subtasks,
        fragments=fragments,
        top_k=top_k,
        wants_chart=wants_chart,
        wants_trend=wants_trend,
        wants_attribution=wants_attribution,
        wants_research=wants_research,
        wants_latest=wants_latest,
        industry_query=industry_query,
        product_query=product_query,
        multi_intent=len(subtasks) > 1 or len(intents) > 1 or len(fragments) > 1,
    )


__all__ = [
    "PlannedSubtask",
    "QueryPlan",
    "detect_plan_intents",
    "detect_query_mode",
    "normalize_question",
    "plan_subtasks",
    "split_question_fragments",
]
