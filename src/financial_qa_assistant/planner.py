from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from .utils import (
    ATTRIBUTION_KEYWORDS,
    CHART_KEYWORDS,
    COMPANY_SET_FOLLOW_UP_HINTS,
    COMPARE_KEYWORDS,
    INDUSTRY_QUERY_KEYWORDS,
    LATEST_KEYWORDS,
    MAX_GROWTH_HINTS,
    MAX_KEYWORDS,
    MIN_KEYWORDS,
    MULTI_INTENT_CONNECTORS,
    PRODUCT_QUERY_KEYWORDS,
    RANKING_KEYWORDS,
    RESEARCH_KEYWORDS,
    THRESHOLD_KEYWORDS,
    TREND_KEYWORDS,
    YOY_KEYWORDS,
    canonicalize_query_text,
    compact_text,
    detect_metric,
    detect_top_k,
    normalize_text,
)


INTENT_ORDER = ("ranking", "single_value", "yoy", "maxmin", "trend", "attribution")
LISTING_KEYWORDS = ("\u5217\u51fa", "\u5217\u4e3e", "\u627e\u51fa", "\u54ea\u4e9b\u516c\u53f8", "\u54ea\u4e9b\u4f01\u4e1a")
TREND_YEAR_PATTERN = re.compile(r"\u8fd1[\u4e00\u4e8c\u4e09\u56db\u4e94\u516d\u4e03\u516b\u4e5d0-9]+\u5e74")


@dataclass(slots=True)
class PlannedSubtask:
    intent: str
    question: str
    params: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class QueryPlan:
    normalized_question: str
    canonical_question: str
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
    fallback_reason: str = ""


def _contains_any(text: str, keywords: tuple[str, ...]) -> bool:
    return any(keyword in text for keyword in keywords if keyword)


def normalize_question(question: str) -> tuple[str, str]:
    normalized = normalize_text(question)
    return normalized, compact_text(normalized)


def _trend_requested(compact: str, wants_chart: bool) -> bool:
    return wants_chart or _contains_any(compact, TREND_KEYWORDS) or bool(TREND_YEAR_PATTERN.search(compact))


def _listing_compare_requested(compact: str) -> bool:
    return (_contains_any(compact, LISTING_KEYWORDS) or _contains_any(compact, COMPANY_SET_FOLLOW_UP_HINTS)) and (
        _contains_any(compact, COMPARE_KEYWORDS) or _contains_any(compact, YOY_KEYWORDS)
    )


def _threshold_then_extreme_requested(compact: str) -> bool:
    return _contains_any(compact, THRESHOLD_KEYWORDS) and _contains_any(compact, MAX_KEYWORDS + MIN_KEYWORDS)


def _max_kind(compact: str) -> str:
    return "min" if _contains_any(compact, MIN_KEYWORDS) else "max"


def _maybe_split_draw_then_explain(fragment: str) -> list[str]:
    match = re.search(
        r"(.+?(?:\u7ed8\u56fe|\u753b\u56fe|\u53ef\u89c6\u5316|\u56fe\u8868))(?:\u518d|\u7136\u540e)(.+)",
        fragment,
    )
    if not match:
        return [fragment]
    left = normalize_text(match.group(1))
    right = normalize_text(match.group(2))
    return [item for item in (left, right) if item]


def split_question_fragments(question: str) -> list[str]:
    normalized = normalize_text(question)
    canonical = canonicalize_query_text(normalized)
    if not canonical:
        return []
    fragments = re.split(r"[\uff0c\uff1f\u3002]", canonical)
    parts: list[str] = []
    for fragment in fragments:
        cleaned = normalize_text(fragment)
        if not cleaned:
            continue
        draw_then_explain = _maybe_split_draw_then_explain(cleaned)
        if len(draw_then_explain) > 1:
            parts.extend(draw_then_explain)
            continue
        if _trend_requested(compact_text(cleaned), wants_chart=False) and _contains_any(compact_text(cleaned), ATTRIBUTION_KEYWORDS):
            split_index = min(
                (cleaned.find(keyword) for keyword in ATTRIBUTION_KEYWORDS if keyword in cleaned),
                default=-1,
            )
            if split_index > 0:
                left = normalize_text(cleaned[:split_index])
                if left:
                    parts.extend([left, cleaned])
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
    for part in parts or [canonical]:
        if part not in unique_parts:
            unique_parts.append(part)
    return unique_parts or [canonical]


def detect_plan_intents(question: str) -> list[str]:
    normalized = normalize_text(question)
    canonical = canonicalize_query_text(normalized)
    _normalized, compact = normalize_question(canonical)
    metric = detect_metric(canonical)
    top_k = detect_top_k(canonical)
    wants_chart = _contains_any(compact, CHART_KEYWORDS)
    wants_trend = _trend_requested(compact, wants_chart)
    wants_attribution = _contains_any(compact, ATTRIBUTION_KEYWORDS)
    wants_listing = _contains_any(compact, LISTING_KEYWORDS)
    listing_compare = _listing_compare_requested(compact)
    threshold_then_extreme = _threshold_then_extreme_requested(compact)
    intents: list[str] = []
    if top_k or _contains_any(compact, RANKING_KEYWORDS) or listing_compare or threshold_then_extreme or (
        wants_listing and (_contains_any(compact, YOY_KEYWORDS) or _contains_any(compact, MAX_KEYWORDS + MIN_KEYWORDS))
    ):
        intents.append("ranking")
    if _contains_any(compact, YOY_KEYWORDS) or _contains_any(compact, MAX_GROWTH_HINTS) or listing_compare:
        intents.append("yoy")
    if _contains_any(compact, MAX_KEYWORDS + MIN_KEYWORDS) or _contains_any(compact, MAX_GROWTH_HINTS) or threshold_then_extreme:
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
    canonical = canonicalize_query_text(question)
    _normalized, compact = normalize_question(canonical)
    top_k = detect_top_k(canonical)
    wants_chart = _contains_any(compact, CHART_KEYWORDS)
    wants_trend = _trend_requested(compact, wants_chart)
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
    if top_k or _listing_compare_requested(compact) or _threshold_then_extreme_requested(compact):
        return "ranking"
    if wants_trend:
        return "trend"
    if has_company or has_metric:
        return "structured"
    return "retrieval"


def plan_subtasks(question: str) -> QueryPlan:
    normalized = normalize_text(question)
    canonical = canonicalize_query_text(normalized)
    _normalized, compact = normalize_question(canonical)
    fragments = split_question_fragments(canonical)
    intents = detect_plan_intents(canonical)
    top_k = detect_top_k(canonical)
    wants_chart = _contains_any(compact, CHART_KEYWORDS)
    wants_trend = _trend_requested(compact, wants_chart)
    wants_attribution = _contains_any(compact, ATTRIBUTION_KEYWORDS)
    wants_research = _contains_any(compact, RESEARCH_KEYWORDS)
    wants_latest = _contains_any(compact, LATEST_KEYWORDS)
    industry_query = _contains_any(compact, INDUSTRY_QUERY_KEYWORDS)
    product_query = industry_query and _contains_any(compact, PRODUCT_QUERY_KEYWORDS)
    metric = detect_metric(canonical)
    query_mode = detect_query_mode(canonical, has_metric=metric is not None)
    listing_compare = _listing_compare_requested(compact)
    threshold_then_extreme = _threshold_then_extreme_requested(compact)

    subtasks: list[PlannedSubtask] = []
    fallback_reason = ""
    if {"ranking", "yoy", "maxmin"}.issubset(set(intents)):
        subtasks = [
            PlannedSubtask("ranking", canonical, {"top_k": top_k}),
            PlannedSubtask("yoy", canonical, {"top_k": top_k}),
            PlannedSubtask("maxmin", canonical, {"kind": _max_kind(compact)}),
        ]
        fallback_reason = "ranking_yoy_max_pipeline"
    elif {"trend", "attribution"}.issubset(set(intents)):
        subtasks = [
            PlannedSubtask("trend", canonical, {"chart": wants_chart}),
            PlannedSubtask("attribution", canonical, {}),
        ]
        fallback_reason = "trend_then_attribution"
    elif len(fragments) > 1:
        for fragment in fragments:
            fragment_intents = detect_plan_intents(fragment)
            fragment_compact = compact_text(fragment)
            chosen_intent = fragment_intents[0] if fragment_intents else "single_value"
            if "attribution" in fragment_intents and _contains_any(fragment_compact, ATTRIBUTION_KEYWORDS):
                chosen_intent = "attribution"
            elif "trend" in fragment_intents and _trend_requested(fragment_compact, wants_chart):
                chosen_intent = "trend"
            subtasks.append(
                PlannedSubtask(
                    intent=chosen_intent,
                    question=fragment,
                    params={"fragment_intents": fragment_intents},
                )
            )
        fallback_reason = "trend_then_attribution" if {"trend", "attribution"}.issubset(set(intents)) else "fragment_split"
    elif threshold_then_extreme:
        subtasks = [
            PlannedSubtask("ranking", canonical, {"top_k": top_k, "threshold": True}),
            PlannedSubtask("maxmin", canonical, {"kind": _max_kind(compact)}),
        ]
        fallback_reason = "threshold_then_extreme"
    elif listing_compare:
        subtasks = [
            PlannedSubtask("ranking", canonical, {"top_k": top_k}),
            PlannedSubtask("yoy", canonical, {"top_k": top_k}),
        ]
        fallback_reason = "list_then_compare"
    else:
        primary_intent = intents[0] if intents else ("single_value" if metric else "retrieval")
        subtasks = [PlannedSubtask(primary_intent, canonical, {"top_k": top_k})]
        fallback_reason = "single_intent"

    return QueryPlan(
        normalized_question=normalized,
        canonical_question=canonical,
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
        fallback_reason=fallback_reason,
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
