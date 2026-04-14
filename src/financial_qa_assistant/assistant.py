from __future__ import annotations

import re
import shutil
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .charting import bar_chart_svg, line_chart_svg, write_bar_chart_jpg, write_line_chart_jpg
from .config import AppConfig
from .database import Database
from .planner import QueryPlan, plan_subtasks
from .utils import (
    ATTRIBUTION_KEYWORDS,
    CAUSE_KEYWORDS,
    CHART_KEYWORDS,
    FOLLOW_UP_HINTS,
    INDUSTRY_QUERY_KEYWORDS,
    LATEST_KEYWORDS,
    PRODUCT_QUERY_KEYWORDS,
    RESEARCH_KEYWORDS,
    TREND_KEYWORDS,
    artifact_name,
    compact_text,
    detect_company_field,
    detect_metric,
    detect_top_k,
    dump_json,
    ensure_relative_path,
    format_money_from_10k,
    normalize_stock_code,
    normalize_text,
    parse_period,
    parse_question_payload,
    score_text,
    sortable_period,
    split_sentences,
    tokenize,
    to_float,
)
CROSS_COMPANY_KEYWORDS = (
    "\u54ea\u4e9b\u4f01\u4e1a",
    "\u54ea\u4e9b\u516c\u53f8",
    "\u516c\u53f8\u6709\u54ea\u4e9b",
    "\u4f01\u4e1a\u6709\u54ea\u4e9b",
    "\u6392\u540d",
    "\u524d\u4e94",
    "\u524d\u5341",
    "top",
    "\u8d85\u8fc7",
    "\u9ad8\u4e8e",
    "\u4f4e\u4e8e",
    "\u5c11\u4e8e",
    "\u4e0d\u4f4e\u4e8e",
    "\u4e0d\u5c11\u4e8e",
    "\u4e0d\u8d85\u8fc7",
)
RETRIEVAL_STOPWORDS = {
    "\u516c\u53f8",
    "\u4f01\u4e1a",
    "\u54ea\u4e9b",
    "\u4ec0\u4e48",
    "\u4e3a\u4ec0\u4e48",
    "\u5982\u4f55",
    "\u60c5\u51b5",
    "\u53d8\u5316",
    "\u8d8b\u52bf",
    "\u5206\u6790",
    "\u539f\u56e0",
    "\u4e0a\u5347",
    "\u4e0b\u964d",
    "\u589e\u957f",
    "\u7814\u62a5",
}
GENERIC_COMPANY_FRAGMENTS = {
    "\u516c\u53f8",
    "\u80a1\u4efd",
    "\u6709\u9650",
    "\u96c6\u56e2",
    "\u836f\u4e1a",
    "\u533b\u836f",
    "\u5236\u836f",
    "\u751f\u7269",
    "\u79d1\u6280",
}


@dataclass(slots=True)
class AnswerPayload:
    content: str
    sql: str = ""
    image: list[str] = field(default_factory=list)
    references: list[dict[str, str]] = field(default_factory=list)
    chart_types: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "content": self.content,
            "sql": self.sql,
            "image": self.image,
            "references": self.references,
        }


@dataclass(slots=True)
class PlannedQuestionState:
    question: str
    compact_question: str
    plan: QueryPlan
    follow_up: bool
    company: dict[str, Any] | None
    metric: tuple[str, str, str] | None
    period_info: dict[str, str] | None
    company_field: tuple[str, str] | None
    context_before: dict[str, Any]
    debug_trace: dict[str, Any] = field(default_factory=dict)


class FinancialQAEngine:
    def __init__(self, config: AppConfig, database: Database | None = None) -> None:
        self.config = config
        self.database = database or Database(config.db_path)
        self.last_debug_trace: dict[str, Any] = {}
        self.company_rows = self.database.query("SELECT * FROM company_info ORDER BY stock_code")
        self.company_aliases = self._build_company_aliases()
        self.company_code_map = self._build_company_code_map()
        self.company_fragment_aliases = self._build_company_fragment_aliases()
        self._ensure_runtime_indexes()
        self.question_bank_rows = self._load_question_bank_rows()

    def _build_company_aliases(self) -> dict[str, dict[str, Any]]:
        aliases: dict[str, dict[str, Any]] = {}
        for row in self.company_rows:
            payload = dict(row)
            names = {
                str(row["stock_abbr"]).strip(),
                str(row["company_name"]).strip(),
                normalize_stock_code(row["stock_code"]),
                str(int(row["stock_code"])) if str(row["stock_code"]).isdigit() else str(row["stock_code"]),
            }
            for name in names:
                if name:
                    aliases[name] = payload
        return aliases

    def _build_company_code_map(self) -> dict[str, dict[str, Any]]:
        return {
            normalize_stock_code(row["stock_code"]): dict(row)
            for row in self.company_rows
            if normalize_stock_code(row["stock_code"])
        }

    def _build_company_fragment_aliases(self) -> dict[str, dict[str, Any]]:
        fragment_candidates: dict[str, list[dict[str, Any]]] = {}
        for row in self.company_rows:
            payload = dict(row)
            fragments: set[str] = set()
            for source in (str(row["stock_abbr"] or "").strip(), str(row["company_name"] or "").strip()):
                source = normalize_text(source)
                if len(source) < 2:
                    continue
                if source.endswith("股份有限公司"):
                    source = source[:-6]
                if source.endswith("有限公司"):
                    source = source[:-4]
                source = re.sub(r"(集团股份有限公司|集团股份公司|股份有限公司|股份公司|集团有限公司|有限公司)$", "", source)
                if source:
                    for size in range(2, min(5, len(source)) + 1):
                        fragments.add(source[:size])
                        fragments.add(source[-size:])
            for fragment in fragments:
                if fragment in GENERIC_COMPANY_FRAGMENTS:
                    continue
                if any(token in fragment for token in ("公司", "股份", "有限")):
                    continue
                fragment_candidates.setdefault(fragment, []).append(payload)

        aliases: dict[str, dict[str, Any]] = {}
        for fragment, rows in fragment_candidates.items():
            if len(rows) == 1:
                aliases[fragment] = rows[0]
        return aliases

    def _ensure_runtime_indexes(self) -> None:
        statements = [
            "CREATE INDEX IF NOT EXISTS idx_document_chunks_source_stock ON document_chunks(source_type, stock_code)",
            "CREATE INDEX IF NOT EXISTS idx_document_chunks_stock_name ON document_chunks(stock_name)",
            "CREATE INDEX IF NOT EXISTS idx_document_chunks_report_period ON document_chunks(report_period)",
            "CREATE INDEX IF NOT EXISTS idx_income_sheet_stock_period ON income_sheet(stock_code, report_period)",
            "CREATE INDEX IF NOT EXISTS idx_balance_sheet_stock_period ON balance_sheet(stock_code, report_period)",
            "CREATE INDEX IF NOT EXISTS idx_cash_flow_sheet_stock_period ON cash_flow_sheet(stock_code, report_period)",
            "CREATE INDEX IF NOT EXISTS idx_kpi_sheet_stock_period ON core_performance_indicators_sheet(stock_code, report_period)",
        ]
        for statement in statements:
            try:
                self.database.execute(statement)
            except Exception:
                continue

    def refresh(self) -> None:
        self.company_rows = self.database.query("SELECT * FROM company_info ORDER BY stock_code")
        self.company_aliases = self._build_company_aliases()
        self.company_code_map = self._build_company_code_map()
        self.company_fragment_aliases = self._build_company_fragment_aliases()
        self.question_bank_rows = self._load_question_bank_rows()

    def _load_question_bank_rows(self) -> list[dict[str, str]]:
        if not self.database.table_exists("question_bank"):
            return []
        rows = self.database.query("SELECT DISTINCT question_id, question_payload FROM question_bank ORDER BY question_id")
        return [
            {
                "question_id": str(row["question_id"] or "").strip(),
                "question_payload": str(row["question_payload"] or "").strip(),
            }
            for row in rows
            if row["question_id"] and row["question_payload"]
        ]

    def resolve_question_id(self, raw_payload: str, context: dict[str, Any] | None = None) -> str | None:
        questions = [normalize_text(item["Q"]) for item in parse_question_payload(raw_payload) if item.get("Q")]
        if not questions:
            return None

        exact_matches: list[str] = []
        single_question_matches: list[str] = []
        current_id = str((context or {}).get("_question_id", "")).strip()
        for row in self.question_bank_rows:
            row_questions = [normalize_text(item["Q"]) for item in parse_question_payload(row["question_payload"]) if item.get("Q")]
            if row_questions == questions:
                exact_matches.append(row["question_id"])
            if len(questions) == 1 and questions[0] in row_questions:
                single_question_matches.append(row["question_id"])

        if exact_matches:
            return sorted(set(exact_matches))[0]
        if current_id and current_id in single_question_matches:
            return current_id
        if len(set(single_question_matches)) == 1:
            return single_question_matches[0]
        return None

    def should_reuse_context(self, question: str, context: dict[str, Any] | None = None) -> bool:
        context = context or {}
        question = normalize_text(question)
        compact = compact_text(question)
        detected_company = self._detect_company(question)
        detected_metric = detect_metric(question)
        company_field = detect_company_field(question)
        top_k = detect_top_k(question)
        wants_chart = self._contains_any(compact, CHART_KEYWORDS)
        wants_trend = wants_chart or self._contains_any(compact, TREND_KEYWORDS)
        wants_attribution = self._contains_any(compact, ATTRIBUTION_KEYWORDS)
        wants_research = self._contains_any(compact, RESEARCH_KEYWORDS)
        latest_requested = self._contains_any(compact, LATEST_KEYWORDS)
        return self._should_reuse_context(
            question=question,
            compact=compact,
            context=context,
            detected_company=detected_company,
            detected_metric=detected_metric,
            company_field=company_field,
            top_k=top_k,
            wants_trend=wants_trend,
            wants_attribution=wants_attribution,
            wants_research=wants_research,
            latest_requested=latest_requested,
        )

    def _clear_semantic_context(self, context: dict[str, Any]) -> None:
        for key in ("company", "metric", "period_info", "year"):
            context.pop(key, None)

    def _should_reuse_context(
        self,
        question: str,
        compact: str,
        context: dict[str, Any],
        detected_company: dict[str, Any] | None,
        detected_metric: tuple[str, str, str] | None,
        company_field: tuple[str, str] | None,
        top_k: int | None,
        wants_trend: bool,
        wants_attribution: bool,
        wants_research: bool,
        latest_requested: bool,
    ) -> bool:
        if not context or not any(key in context for key in ("company", "metric", "period_info", "year")):
            return False
        if self._contains_any(compact, INDUSTRY_QUERY_KEYWORDS) and not detected_company and not detected_metric and not company_field:
            return False
        if wants_research and not detected_company:
            return False

        standalone_period = parse_period(question, {})
        if any(hint in compact for hint in FOLLOW_UP_HINTS):
            return True
        if company_field and not detected_company:
            return True
        if wants_attribution and not detected_company:
            return True
        if detected_metric and not detected_company and context.get("company"):
            return True
        if standalone_period and (context.get("metric") or detected_metric):
            return True
        if detected_company and not detected_metric and context.get("metric") and (standalone_period or wants_attribution or len(compact) <= 16):
            return True
        if not detected_company and not detected_metric and not top_k and not wants_trend and not latest_requested and len(compact) <= 12:
            return True
        if compact.endswith("\u7684") and len(compact) <= 20:
            return True
        return False

    def _chart_type_label(self, chart_type: str) -> str:
        return {
            "line": "\u6298\u7ebf\u56fe",
            "bar": "\u67f1\u72b6\u56fe",
        }.get(chart_type, chart_type)

    def _next_chart_path(self, context: dict[str, Any], seed: str) -> Path:
        question_id = str(context.get("_question_id", "")).strip()
        next_index = int(context.get("_image_seq", 0) or 0) + 1
        context["_image_seq"] = next_index
        if question_id:
            file_name = f"{question_id}_{next_index}.jpg"
        else:
            file_name = artifact_name("chart", f"{seed}_{next_index}", ".jpg")
        return self.config.artifact_dir / file_name

    def _save_chart(
        self,
        chart_type: str,
        context: dict[str, Any],
        title: str,
        labels: list[str],
        values: list[float],
        seed: str,
    ) -> str | None:
        artifact = self._next_chart_path(context, seed)
        try:
            if chart_type == "line":
                write_line_chart_jpg(artifact, title, labels, values)
            else:
                write_bar_chart_jpg(artifact, title, labels, values)
        except Exception:
            return None
        return ensure_relative_path(artifact, self.config.workspace_root)

    def _bad_text_ratio(self, text: str) -> float:
        meaningful = [char for char in normalize_text(text) if char and not char.isspace()]
        if not meaningful:
            return 1.0
        bad_count = 0
        for char in meaningful:
            code = ord(char)
            allowed = (char.isascii() and char.isprintable()) or ("\u4e00" <= char <= "\u9fff")
            allowed = allowed or (0x3000 <= code <= 0x303F) or (0xFF00 <= code <= 0xFFEF)
            if not allowed:
                bad_count += 1
        return bad_count / len(meaningful)

    def _clip_clean_text(self, text: str, limit: int = 180) -> str:
        text = normalize_text(text)
        for bad_char in ("\u00ee", "\u00ef", "\u00ec", "\u00f0", "\u00f1", "\u00f2", "\ufffd"):
            text = text.replace(bad_char, " ")
        text = normalize_text(text)
        return text[:limit]

    def _clean_reference_snippet(self, text: str, limit: int = 120) -> str:
        cleaned = self._clip_clean_text(text, limit=limit)
        if len(cleaned) < 12:
            return ""
        if self._bad_text_ratio(cleaned) > 0.08:
            return ""
        digit_ratio = sum(1 for char in cleaned if char.isdigit()) / max(len(cleaned), 1)
        if digit_ratio > 0.55 and not any("\u4e00" <= char <= "\u9fff" for char in cleaned):
            return ""
        return cleaned

    def _dedupe_references(self, references: list[dict[str, str]], limit: int = 3) -> list[dict[str, str]]:
        cleaned_items: list[dict[str, str]] = []
        seen: set[tuple[str, str]] = set()
        for reference in references:
            paper_path = str(reference.get("paper_path") or "").strip()
            snippet = self._clean_reference_snippet(str(reference.get("text") or ""))
            if not paper_path or not snippet:
                continue
            key = (paper_path, snippet)
            if key in seen:
                continue
            seen.add(key)
            cleaned_items.append({"paper_path": paper_path, "text": snippet, "paper_image": ""})
            if len(cleaned_items) >= limit:
                break
        return cleaned_items

    def _is_clean_sentence(self, text: str) -> bool:
        cleaned = self._clip_clean_text(text, limit=400)
        if len(cleaned) < 8:
            return False
        if cleaned.count("\\") >= 3:
            return False
        if self._bad_text_ratio(cleaned) > 0.10:
            return False
        if self._text_cjk_ratio(cleaned) < 0.08 and not any(char.isalpha() for char in cleaned):
            return False
        return True

    def answer_payload(self, raw_payload: str, question_id: str = "") -> list[dict[str, Any]]:
        answers = self._answer_items(raw_payload, question_id=question_id)
        return [{"Q": question, "A": answer.as_dict()} for question, answer in answers]

    def _answer_items(self, raw_payload: str, question_id: str = "") -> list[tuple[str, AnswerPayload]]:
        payload = parse_question_payload(raw_payload)
        context: dict[str, Any] = {}
        resolved_question_id = question_id or self.resolve_question_id(raw_payload)
        if resolved_question_id:
            context["_question_id"] = resolved_question_id
            context["_image_seq"] = 0
        answers: list[tuple[str, AnswerPayload]] = []
        for item in payload:
            question = item["Q"]
            answer = self.answer_question(question, context)
            answers.append((question, answer))
        return answers

    def _normalize_question(self, question: str) -> tuple[str, str]:
        normalized = normalize_text(question)
        return normalized, compact_text(normalized)

    def _detect_metric_step(self, question: str) -> tuple[str, str, str] | None:
        return detect_metric(question)

    def _detect_period_step(
        self,
        question: str,
        context: dict[str, Any],
        follow_up: bool,
    ) -> dict[str, str] | None:
        return parse_period(question, context if follow_up else {})

    def _resolve_context(
        self,
        *,
        question: str,
        compact: str,
        context: dict[str, Any],
        detected_company: dict[str, Any] | None,
        detected_metric: tuple[str, str, str] | None,
        company_field: tuple[str, str] | None,
        top_k: int | None,
        wants_trend: bool,
        wants_attribution: bool,
        wants_research: bool,
        latest_requested: bool,
    ) -> bool:
        follow_up = self._should_reuse_context(
            question=question,
            compact=compact,
            context=context,
            detected_company=detected_company,
            detected_metric=detected_metric,
            company_field=company_field,
            top_k=top_k,
            wants_trend=wants_trend,
            wants_attribution=wants_attribution,
            wants_research=wants_research,
            latest_requested=latest_requested,
        )
        if not follow_up:
            self._clear_semantic_context(context)
        return follow_up

    def _detect_query_mode(self, state: PlannedQuestionState) -> str:
        if state.company_field:
            return "company_profile"
        if state.plan.wants_research:
            return "research"
        if state.plan.industry_query:
            return "industry_lookup"
        if state.plan.wants_attribution:
            return "attribution"
        if state.plan.top_k:
            return "ranking"
        if state.plan.wants_trend:
            return "trend"
        if state.metric or state.company:
            return "structured"
        return "retrieval"

    def _build_question_state(self, question: str, context: dict[str, Any]) -> PlannedQuestionState:
        normalized_question, compact = self._normalize_question(question)
        detected_company = self._detect_company(normalized_question)
        company_field = detect_company_field(normalized_question)
        detected_metric = self._detect_metric_step(normalized_question)
        top_k = detect_top_k(normalized_question)
        wants_chart = self._contains_any(compact, CHART_KEYWORDS)
        wants_trend = wants_chart or self._contains_any(compact, TREND_KEYWORDS)
        wants_attribution = self._contains_any(compact, ATTRIBUTION_KEYWORDS)
        wants_research = self._contains_any(compact, RESEARCH_KEYWORDS)
        latest_requested = self._contains_any(compact, LATEST_KEYWORDS)
        context_before = {
            "company": context.get("company"),
            "metric": context.get("metric"),
            "period_info": context.get("period_info"),
            "year": context.get("year"),
        }
        follow_up = self._resolve_context(
            question=normalized_question,
            compact=compact,
            context=context,
            detected_company=detected_company,
            detected_metric=detected_metric,
            company_field=company_field,
            top_k=top_k,
            wants_trend=wants_trend,
            wants_attribution=wants_attribution,
            wants_research=wants_research,
            latest_requested=latest_requested,
        )
        company = detected_company or context.get("company")
        metric = detected_metric or context.get("metric")
        period_info = self._detect_period_step(normalized_question, context, follow_up)
        if company:
            context["company"] = company
        if metric:
            context["metric"] = metric
        if period_info:
            context["period_info"] = period_info
            context["year"] = period_info["year"]
        plan = plan_subtasks(normalized_question)
        debug_trace = {
            "normalize_question": normalized_question,
            "resolve_context": {
                "follow_up": follow_up,
                "context_before": context_before,
                "context_after": {
                    "company": bool(company),
                    "metric": metric[2] if metric else "",
                    "period": (period_info or {}).get("report_period", ""),
                },
            },
            "detect_company": (company or {}).get("stock_abbr", "") if company else "",
            "detect_metric": metric[2] if metric else "",
            "detect_period": (period_info or {}).get("report_period", ""),
            "detect_query_mode": "",
            "plan_subtasks": [subtask.intent for subtask in plan.subtasks],
        }
        state = PlannedQuestionState(
            question=normalized_question,
            compact_question=compact,
            plan=plan,
            follow_up=follow_up,
            company=company,
            metric=metric,
            period_info=period_info,
            company_field=company_field,
            context_before=context_before,
            debug_trace=debug_trace,
        )
        state.debug_trace["detect_query_mode"] = self._detect_query_mode(state)
        self.last_debug_trace = state.debug_trace
        context["_debug_trace"] = state.debug_trace
        return state

    def _need_clarification(self, state: PlannedQuestionState) -> AnswerPayload | None:
        query_mode = self._detect_query_mode(state)
        cross_company_query = self._is_cross_company_query(state.compact_question, state.plan.top_k)
        if not state.company and state.metric and not cross_company_query and not state.plan.industry_query and not state.plan.wants_research:
            if query_mode == "attribution":
                return self._answer_missing_company_clarification(state.metric[2], "归因分析")
            if query_mode == "trend":
                return self._answer_missing_company_clarification(state.metric[2], "趋势分析")
            if not state.plan.wants_latest:
                return self._answer_missing_company_clarification(state.metric[2], "指标查询")
        if not state.company and state.plan.wants_attribution and not state.plan.industry_query:
            return AnswerPayload(content="当前问题缺少公司名称。请先补充公司后再做归因分析，例如：华润三九主营业务收入上升的原因是什么？")
        if state.company and not state.metric and not state.company_field and query_mode == "structured" and not state.plan.industry_query and not state.plan.wants_research:
            return self._answer_missing_metric_clarification(state.company)
        if (
            state.company
            and state.metric
            and query_mode == "structured"
            and not state.period_info
            and not state.plan.wants_trend
            and not state.plan.top_k
            and not state.plan.wants_latest
        ):
            clarification = self._answer_period_clarification(state.company, state.metric)
            if clarification:
                return clarification
        return None

    def _plan_subtasks(self, state: PlannedQuestionState) -> list[dict[str, Any]]:
        return [{"intent": subtask.intent, "question": subtask.question, "params": dict(subtask.params)} for subtask in state.plan.subtasks]

    def _execute_direct_question(self, state: PlannedQuestionState, context: dict[str, Any]) -> AnswerPayload:
        company = state.company
        metric = state.metric
        period_info = state.period_info

        if state.company_field and company:
            return self._answer_company_profile(company, state.company_field)

        if state.plan.wants_research:
            research_answer = self._answer_research(company)
            if research_answer:
                return research_answer

        medical_insurance_answer = self._answer_medical_insurance_products(state.question)
        if medical_insurance_answer:
            return medical_insurance_answer

        if state.plan.top_k and "同比" in state.compact_question and "利润" in state.compact_question:
            return self._answer_profit_ranking_with_growth_v2(period_info, state.plan.top_k, context)

        if state.plan.wants_attribution and company and metric:
            return self._answer_task3_attribution(state.question, company, metric, period_info)

        if metric:
            structured = self._answer_structured_metric(
                question=state.question,
                company=company,
                metric=metric,
                period_info=period_info,
                wants_trend=state.plan.wants_trend,
                wants_chart=state.plan.wants_chart,
                top_k=state.plan.top_k,
                latest_requested=state.plan.wants_latest,
                context=context,
            )
            if structured:
                return structured

        if state.plan.wants_attribution:
            return self._answer_retrieval_v2(state.question, company, prefer_causes=True)
        return self._answer_retrieval_v2(state.question, company, prefer_causes=False)

    def _execute_subtasks(self, state: PlannedQuestionState, context: dict[str, Any]) -> list[AnswerPayload]:
        if {"ranking", "yoy", "maxmin"}.issubset(set(state.plan.intents)) and state.plan.top_k and "利润" in state.compact_question:
            return [self._answer_profit_ranking_with_growth_v2(state.period_info, state.plan.top_k, context)]

        fragment_depth = int(context.get("_fragment_depth", 0) or 0)
        if len(state.plan.fragments) > 1 and fragment_depth < 2:
            fragment_context = dict(context)
            fragment_context["_fragment_depth"] = fragment_depth + 1
            return [self.answer_question(fragment, fragment_context) for fragment in state.plan.fragments]

        return [self._execute_direct_question(state, context)]

    def _assemble_answer(self, state: PlannedQuestionState, payloads: list[AnswerPayload]) -> AnswerPayload:
        if not payloads:
            return AnswerPayload(content="当前未检索到足够的离线证据。可以补充公司名称、报告期或指标后再提问。")
        if len(payloads) == 1:
            return payloads[0]

        content_lines = [f"{index}. {payload.content}" for index, payload in enumerate(payloads, start=1) if payload.content]
        sql_parts = [payload.sql for payload in payloads if payload.sql]
        images: list[str] = []
        chart_types: list[str] = []
        references: list[dict[str, str]] = []
        seen_refs: set[tuple[str, str]] = set()
        for payload in payloads:
            for image in payload.image:
                if image and image not in images:
                    images.append(image)
            for chart_type in payload.chart_types:
                if chart_type and chart_type not in chart_types:
                    chart_types.append(chart_type)
            for reference in payload.references:
                key = (str(reference.get("paper_path") or ""), str(reference.get("text") or ""))
                if key in seen_refs:
                    continue
                seen_refs.add(key)
                references.append(reference)
        return AnswerPayload(
            content="\n".join(content_lines),
            sql="\n\n".join(sql_parts),
            image=images,
            references=references,
            chart_types=chart_types,
        )

    def answer_question(self, question: str, context: dict[str, Any] | None = None) -> AnswerPayload:
        if context is None:
            context = {}
        state = self._build_question_state(question, context)
        clarification = self._need_clarification(state)
        if clarification:
            state.debug_trace["need_clarification"] = clarification.content
            return clarification
        state.debug_trace["need_clarification"] = ""
        state.debug_trace["plan_subtasks"] = self._plan_subtasks(state)
        payloads = self._execute_subtasks(state, context)
        answer = self._assemble_answer(state, payloads)
        state.debug_trace["assemble_answer"] = {"content_length": len(answer.content), "reference_count": len(answer.references)}
        self.last_debug_trace = state.debug_trace
        context["_debug_trace"] = state.debug_trace
        return answer

    def _contains_any(self, text: str, keywords: tuple[str, ...]) -> bool:
        return any(keyword in text for keyword in keywords)

    def _detect_company(self, question: str) -> dict[str, Any] | None:
        for alias, payload in sorted(self.company_aliases.items(), key=lambda item: len(item[0]), reverse=True):
            if alias and alias in question:
                return payload
        for alias, payload in sorted(self.company_fragment_aliases.items(), key=lambda item: len(item[0]), reverse=True):
            if alias and alias in question:
                return payload
        return None

    def _company_label(self, stock_code: Any, fallback: str = "") -> str:
        code = normalize_stock_code(stock_code)
        company = self.company_code_map.get(code) if code else None
        if company:
            return str(company.get("stock_abbr") or company.get("company_name") or fallback)
        return str(fallback or code or "")

    def _is_cross_company_query(self, compact: str, top_k: int | None) -> bool:
        if top_k:
            return True
        return any(keyword in compact for keyword in CROSS_COMPANY_KEYWORDS)

    def _answer_missing_company_clarification(self, metric_label: str, scene: str) -> AnswerPayload:
        return AnswerPayload(
            content=(
                f"\u5f53\u524d\u95ee\u9898\u9700\u8981\u5148\u786e\u5b9a\u516c\u53f8\u540d\u79f0\uff0c\u624d\u80fd\u8fdb\u884c{scene}\u3002"
                f"\u6211\u5df2\u8bc6\u522b\u5230\u4f60\u5728\u95ee\u201c{metric_label}\u201d\uff0c"
                "\u8bf7\u8865\u5145\u516c\u53f8\uff0c\u4f8b\u5982\uff1a\u534e\u6da6\u4e09\u4e5d"
                f"{metric_label}\u4e0a\u5347\u7684\u539f\u56e0\u662f\u4ec0\u4e48\uff1f"
            )
        )

    def _answer_missing_metric_clarification(self, company: dict[str, Any]) -> AnswerPayload:
        return AnswerPayload(
            content=(
                f"当前问题已经识别到公司为{company['stock_abbr']}，但还缺少具体指标。"
                "请补充你要查询的指标，例如：净利润、主营业务收入、总资产。"
            )
        )

    def _attribution_target_period(
        self,
        company: dict[str, Any],
        metric: tuple[str, str, str],
        period_info: dict[str, str] | None,
    ) -> str | None:
        if period_info:
            return period_info["report_period"]
        periods = self._available_periods(metric[0], company)
        if not periods:
            return None
        fy_periods = [period for period in periods if period.endswith("FY")]
        candidates = fy_periods or periods
        return sorted(candidates, key=sortable_period)[-1]

    def _structured_attribution_conclusion(
        self,
        company: dict[str, Any],
        metric: tuple[str, str, str],
        period_info: dict[str, str] | None,
    ) -> str:
        table_name, column_name, metric_label = metric
        target_period = self._attribution_target_period(company, metric, period_info)
        if not target_period:
            return ""
        expr = self._metric_sql_expr(column_name)
        value = self._load_metric_value(table_name, expr, company["stock_code"], target_period)
        if self._is_suspicious_value(metric_label, value):
            return ""
        stored_yoy = self.database.scalar(
            """
            SELECT yoy_value
            FROM financial_metric_facts
            WHERE stock_code = ? AND report_period = ? AND metric_key = ?
            LIMIT 1
            """,
            (company["stock_code"], target_period, column_name),
        )
        yoy_text, yoy_value = self._fy_yoy_display(table_name, company["stock_code"], target_period, expr, stored_yoy)
        value_text = self._format_metric_value(metric_label, value)
        if yoy_value is not None:
            direction = "上升" if yoy_value >= 0 else "下降"
            return (
                f"从结构化数据库看，{company['stock_abbr']}{target_period}的{metric_label}为{value_text}，"
                f"同比{direction}{abs(yoy_value):.2f}%。"
            )
        if yoy_text not in ("", "未知"):
            return f"从结构化数据库看，{company['stock_abbr']}{target_period}的{metric_label}为{value_text}，同比{yoy_text}。"
        return f"从结构化数据库看，{company['stock_abbr']}{target_period}的{metric_label}为{value_text}。"

    def _answer_task3_attribution(
        self,
        question: str,
        company: dict[str, Any],
        metric: tuple[str, str, str],
        period_info: dict[str, str] | None,
    ) -> AnswerPayload:
        structured_conclusion = self._structured_attribution_conclusion(company, metric, period_info)
        if metric[1] in ("main_business_revenue", "total_operating_revenue"):
            retrieval_payload = self._answer_revenue_attribution(company) or self._answer_retrieval_v2(
                f"{company['stock_abbr']} {metric[2]} 上升原因",
                company,
                prefer_causes=True,
            )
        else:
            retrieval_payload = self._answer_retrieval_v2(
                f"{company['stock_abbr']} {metric[2]} 上升原因",
                company,
                prefer_causes=True,
            )
        references = self._dedupe_references(retrieval_payload.references, limit=3)
        if not references:
            content = structured_conclusion or retrieval_payload.content
            if "不足" not in content:
                content = content + " 当前离线证据不足以稳定支撑归因分析。"
            return AnswerPayload(content=content, sql=retrieval_payload.sql, references=references)
        evidence_lines = [f"{index}. {reference['text']}" for index, reference in enumerate(references, start=1)]
        fallback_conclusion = self._clean_reference_snippet(retrieval_payload.content, limit=120) or retrieval_payload.content
        conclusion = structured_conclusion or fallback_conclusion
        content = conclusion + "\n证据依据：\n" + "\n".join(evidence_lines)
        return AnswerPayload(content=content, sql=retrieval_payload.sql, references=references)

    def _format_publish_date(self, value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}.*", text):
            return text[:10]
        if re.fullmatch(r"\d{8}", text):
            return f"{text[:4]}-{text[4:6]}-{text[6:]}"
        if re.fullmatch(r"\d+(?:\.0+)?", text):
            serial = int(float(text))
            if 30000 <= serial <= 70000:
                return (datetime(1899, 12, 30) + timedelta(days=serial)).strftime("%Y-%m-%d")
        return text[:10]

    def _metric_sql_expr(self, column_name: str) -> str:
        if column_name == "main_business_revenue":
            return "COALESCE(main_business_revenue, total_operating_revenue)"
        if column_name == "main_business_revenue_yoy_growth":
            return "COALESCE(main_business_revenue_yoy_growth, operating_revenue_yoy_growth)"
        return column_name

    def _quote(self, value: Any) -> str:
        return "'" + str(value).replace("'", "''") + "'"

    def _is_amount_metric(self, metric_label: str) -> bool:
        return metric_label != "\u6bcf\u80a1\u6536\u76ca"

    def _is_suspicious_value(self, metric_label: str, value: Any) -> bool:
        number = to_float(value)
        if number is None:
            return True
        if self._is_amount_metric(metric_label) and abs(number) < 1:
            return True
        return False

    def _format_metric_value(self, metric_label: str, value: Any) -> str:
        number = to_float(value)
        if number is None:
            return "\u672a\u77e5"
        if metric_label == "\u6bcf\u80a1\u6536\u76ca":
            return f"{number:.4f}"
        return format_money_from_10k(number)

    def _is_reasonable_yoy(self, value: Any) -> bool:
        number = to_float(value)
        return number is not None and abs(number) <= 500


    def _load_metric_value(self, table_name: str, value_expr: str, stock_code: str, report_period: str) -> float | None:
        rows = self.database.query(
            f"SELECT {value_expr} AS metric_value FROM {table_name} WHERE stock_code = ? AND report_period = ? LIMIT 1",
            (stock_code, report_period),
        )
        return to_float(rows[0]["metric_value"]) if rows else None

    def _fy_yoy_display(self, table_name: str, stock_code: str, report_period: str, value_expr: str, stored_yoy: Any) -> tuple[str, float | None]:
        stored_value = to_float(stored_yoy)
        if stored_value is not None and abs(stored_value) <= 500:
            return f"{stored_value:.2f}%", stored_value
        if not report_period.endswith("FY"):
            return "未知", None
        previous_period = f"{int(report_period[:4]) - 1}FY"
        current_value = self._load_metric_value(table_name, value_expr, stock_code, report_period)
        previous_value = self._load_metric_value(table_name, value_expr, stock_code, previous_period)
        if current_value is None or previous_value is None:
            return "未知", None
        if previous_value == 0 or current_value * previous_value < 0:
            return "不适用", None
        yoy_value = (current_value - previous_value) / abs(previous_value) * 100.0
        if abs(yoy_value) > 500:
            return "未知", None
        return f"{yoy_value:.2f}%", yoy_value

    def _available_periods(self, table_name: str, company: dict[str, Any]) -> list[str]:
        rows = self.database.query(
            f"SELECT DISTINCT report_period FROM {table_name} WHERE stock_code = ? AND report_period <> '' ORDER BY report_period",
            (company["stock_code"],),
        )
        return [str(row[0]) for row in rows if row[0]]

    def _answer_period_clarification(self, company: dict[str, Any], metric: tuple[str, str, str]) -> AnswerPayload | None:
        table_name, _column_name, metric_label = metric
        periods = self._available_periods(table_name, company)
        if len(periods) <= 1:
            return None
        latest_examples = ", ".join(periods[-3:])
        return AnswerPayload(
            content=f"\u8bf7\u95ee\u4f60\u8981\u67e5\u8be2{company['stock_abbr']}\u54ea\u4e2a\u62a5\u544a\u671f\u7684{metric_label}\uff1f\u4f8b\u5982 {latest_examples}\u3002"
        )

    def _answer_company_profile(self, company: dict[str, Any], company_field: tuple[str, str]) -> AnswerPayload:
        field_name, field_label = company_field
        sql = f"SELECT {field_name} FROM company_info WHERE stock_code = {self._quote(company['stock_code'])}"
        value = self.database.scalar(
            f"SELECT {field_name} FROM company_info WHERE stock_code = ?",
            (company["stock_code"],),
        )
        return AnswerPayload(content=f"{company['stock_abbr']}\u7684{field_label}\u4e3a\uff1a{value}\u3002", sql=sql)

    def _is_percentage_metric(self, metric_label: str, column_name: str) -> bool:
        return (
            "\u7387" in metric_label
            or "\u540c\u6bd4" in metric_label
            or "\u73af\u6bd4" in metric_label
            or "\u5360\u6bd4" in metric_label
            or column_name.endswith("_ratio")
            or column_name.endswith("_growth")
        )

    def _parse_threshold_condition(
        self,
        question: str,
        column_name: str,
        metric_label: str,
    ) -> tuple[str, float, str] | None:
        compact = compact_text(question)
        if "\u8d1f\u6570" in compact:
            return ("<", 0.0, "\u8d1f\u6570")
        if "\u6b63\u6570" in compact:
            return (">", 0.0, "\u6b63\u6570")

        patterns = [
            (r"(?:\u4e0d\u5c11\u4e8e|\u4e0d\u4f4e\u4e8e|>=)([-+]?\d+(?:\.\d+)?)\s*(\u4ebf|\u4e07|%|\uff05|\u5143)?", ">="),
            (r"(?:\u8d85\u8fc7|\u9ad8\u4e8e|\u5927\u4e8e|>)([-+]?\d+(?:\.\d+)?)\s*(\u4ebf|\u4e07|%|\uff05|\u5143)?", ">"),
            (r"(?:\u4e0d\u8d85\u8fc7|\u4e0d\u9ad8\u4e8e|<=)([-+]?\d+(?:\.\d+)?)\s*(\u4ebf|\u4e07|%|\uff05|\u5143)?", "<="),
            (r"(?:\u4f4e\u4e8e|\u5c11\u4e8e|\u5c0f\u4e8e|<)([-+]?\d+(?:\.\d+)?)\s*(\u4ebf|\u4e07|%|\uff05|\u5143)?", "<"),
        ]
        for pattern, operator in patterns:
            match = re.search(pattern, compact)
            if not match:
                continue
            number_text, unit = match.groups()
            raw_value = to_float(f"{number_text}{unit or ''}") if unit else float(number_text)
            if raw_value is None:
                continue
            if unit and not self._is_percentage_metric(metric_label, column_name) and self._is_amount_metric(metric_label):
                raw_value /= 10000.0
            return operator, raw_value, f"{operator} {number_text}{unit or ''}".strip()
        return None

    def _answer_threshold_filter(
        self,
        table_name: str,
        column_name: str,
        metric_label: str,
        period_info: dict[str, str] | None,
        question: str,
    ) -> AnswerPayload | None:
        threshold = self._parse_threshold_condition(question, column_name, metric_label)
        if threshold is None:
            return None
        operator, threshold_value, threshold_label = threshold
        expr = self._metric_sql_expr(column_name)
        period = period_info["report_period"] if period_info else self._latest_fy_period()
        sql = (
            "SELECT stock_code, stock_abbr, report_period, "
            f"{expr} AS metric_value FROM {table_name} WHERE report_period = {self._quote(period)} "
            f"AND {expr} {operator} {threshold_value} ORDER BY metric_value DESC LIMIT 50"
        )
        rows = self.database.query(
            f"""
            SELECT stock_code, stock_abbr, report_period, {expr} AS metric_value
            FROM {table_name}
            WHERE report_period = ? AND {expr} {operator} ?
            ORDER BY metric_value DESC
            LIMIT 50
            """,
            (period, threshold_value),
        )
        valid_rows = []
        for row in rows:
            value = to_float(row["metric_value"])
            if value is None:
                continue
            valid_rows.append((str(row["stock_code"] or ""), self._company_label(row["stock_code"], str(row["stock_abbr"] or "")), value))
        if not valid_rows:
            return AnswerPayload(content=f"{period}\u6ca1\u6709\u547d\u4e2d{metric_label}{threshold_label}\u7684\u516c\u53f8\u3002", sql=sql)

        lines = [
            f"{index}. {name}（{code}）: {self._format_metric_value(metric_label, value)}"
            for index, (code, name, value) in enumerate(valid_rows, start=1)
        ]
        return AnswerPayload(
            content=f"{period}\u547d\u4e2d{metric_label}{threshold_label}\u7684\u516c\u53f8\u5171 {len(valid_rows)} \u5bb6\uff1a\n" + "\n".join(lines),
            sql=sql,
        )

    def _text_cjk_ratio(self, text: str) -> float:
        meaningful = [char for char in text if not char.isspace()]
        if not meaningful:
            return 0.0
        cjk = sum(1 for char in meaningful if "\u4e00" <= char <= "\u9fff")
        return cjk / len(meaningful)

    def _answer_revenue_attribution(self, company: dict[str, Any]) -> AnswerPayload | None:
        rows = self.database.query(
            """
            SELECT title, file_path, source_type, text
            FROM document_chunks
            WHERE stock_code = ? OR stock_name IN (?, ?)
            """,
            (company["stock_code"], company["stock_abbr"], company["company_name"]),
        )
        theme_rules = [
            ("CHC\u4e1a\u52a1\u56de\u6696", ("CHC", "\u547c\u5438", "\u54c1\u724c", "\u56de\u6696")),
            ("\u5904\u65b9\u836f\u4e1a\u52a1\u9010\u6b65\u6062\u590d", ("\u5904\u65b9\u836f", "\u96c6\u91c7", "\u6062\u590d")),
            ("\u5e76\u8d2d\u6574\u5408\u4e0e\u5e76\u8868\u8d21\u732e", ("\u6606\u836f", "\u5929\u58eb\u529b", "\u5e76\u8868", "\u878d\u5408")),
            ("\u65b0\u54c1\u653e\u91cf\u548c\u6e20\u9053\u6539\u5584", ("\u65b0\u54c1", "\u6e20\u9053", "\u5e93\u5b58", "\u76ca\u6c14\u6e05\u80ba")),
        ]
        references: list[dict[str, str]] = []
        themes: list[str] = []
        for theme, keywords in theme_rules:
            chosen_row = None
            chosen_sentence = ""
            for row in rows:
                if row["source_type"] not in ("stock_research_pdf", "stock_research_meta", "financial_report_pdf"):
                    continue
                for sentence in split_sentences(str(row["text"])):
                    if not any(keyword in sentence for keyword in keywords):
                        continue
                    if self._text_cjk_ratio(sentence) < 0.2:
                        continue
                    if not self._is_clean_sentence(sentence):
                        continue
                    chosen_row = row
                    chosen_sentence = sentence[:180]
                    break
                if chosen_row:
                    break
            if chosen_row and chosen_sentence:
                themes.append(theme)
                references.append({"paper_path": chosen_row["file_path"] or chosen_row["title"], "text": chosen_sentence, "paper_image": ""})
        if not themes:
            return None
        theme_text = "\u3001".join(themes)
        content = f"\u7efc\u5408\u672c\u5730\u8d22\u62a5\u4e0e\u7814\u62a5\u8bc1\u636e，{company['stock_abbr']}\u4e3b\u8425\u4e1a\u52a1\u6536\u5165\u4e0a\u5347\u4e3b\u8981\u53d7{theme_text}\u9a71\u52a8\u3002"
        return AnswerPayload(content=content, references=references)

    def _answer_research(self, company: dict[str, Any] | None) -> AnswerPayload | None:
        if company:
            sql = (
                "SELECT title, orgName, publishDate, emRatingName FROM stock_research "
                f"WHERE stockCode = {self._quote(company['stock_code'])} OR stockName = {self._quote(company['stock_abbr'])} "
                "ORDER BY publishDate DESC"
            )
            rows = self.database.query(
                """
                SELECT title, orgName, publishDate, emRatingName
                FROM stock_research
                WHERE stockCode = ? OR stockName = ?
                ORDER BY publishDate DESC
                """,
                (company["stock_code"], company["stock_abbr"]),
            )
            if not rows:
                return None
            lines = []
            for row in rows[:5]:
                suffix = f"\uff0c\u8bc4\u7ea7\uff1a{row['emRatingName']}" if row["emRatingName"] else ""
                publish_date = self._format_publish_date(row["publishDate"])
                lines.append(f"{publish_date} {row['orgName']}\u300a{row['title']}\u300b{suffix}".strip())
            return AnswerPayload(content=f"\u68c0\u7d22\u5230\u4e0e{company['stock_abbr']}\u76f8\u5173\u7684\u7814\u62a5\u5982\u4e0b\uff1a\n" + "\n".join(lines), sql=sql)

        sql = "SELECT title, orgName, publishDate FROM industry_research ORDER BY publishDate DESC"
        rows = self.database.query(sql)
        if not rows:
            return None
        lines = [f"{self._format_publish_date(row['publishDate'])} {row['orgName']}\u300a{row['title']}\u300b".strip() for row in rows[:5]]
        return AnswerPayload(content="\u68c0\u7d22\u5230\u884c\u4e1a\u4fa7\u76f8\u5173\u7814\u62a5\u5982\u4e0b\uff1a\n" + "\n".join(lines), sql=sql)

    def _answer_structured_metric(
        self,
        question: str,
        company: dict[str, Any] | None,
        metric: tuple[str, str, str],
        period_info: dict[str, str] | None,
        wants_trend: bool,
        wants_chart: bool,
        top_k: int | None,
        latest_requested: bool,
        context: dict[str, Any],
    ) -> AnswerPayload | None:
        table_name, column_name, metric_label = metric
        if not self.database.has_column(table_name, column_name):
            return None
        if self.database.table_row_count(table_name) == 0:
            return None
        if top_k:
            return self._answer_top_k_v2(table_name, column_name, metric_label, period_info, top_k, context)
        if wants_trend and company:
            return self._answer_trend(question, table_name, column_name, metric_label, company, wants_chart, context)
        if company and period_info:
            return self._answer_scalar(table_name, column_name, metric_label, company, period_info)
        if company and latest_requested:
            return self._answer_latest_scalar(table_name, column_name, metric_label, company)
        if not company:
            return self._answer_threshold_filter_v2(table_name, column_name, metric_label, period_info, question)
        return None

    def _answer_scalar(
        self,
        table_name: str,
        column_name: str,
        metric_label: str,
        company: dict[str, Any],
        period_info: dict[str, str],
    ) -> AnswerPayload:
        expr = self._metric_sql_expr(column_name)
        sql = (
            f"SELECT {expr} AS metric_value FROM {table_name} "
            f"WHERE stock_code = {self._quote(company['stock_code'])} "
            f"AND report_period = {self._quote(period_info['report_period'])} LIMIT 1"
        )
        rows = self.database.query(
            f"SELECT {expr} AS metric_value FROM {table_name} WHERE stock_code = ? AND report_period = ? LIMIT 1",
            (company["stock_code"], period_info["report_period"]),
        )
        value = rows[0]["metric_value"] if rows else None
        if self._is_suspicious_value(metric_label, value):
            return self._answer_retrieval_v2(
                f"{company['stock_abbr']} {period_info['label']} {metric_label}",
                company,
                prefer_causes=False,
                sql=sql,
            )
        value_text = self._format_metric_value(metric_label, value)
        return AnswerPayload(content=f"{company['stock_abbr']}{period_info['label']}\u7684{metric_label}\u4e3a\uff1a{value_text}\u3002", sql=sql)

    def _answer_latest_scalar(
        self,
        table_name: str,
        column_name: str,
        metric_label: str,
        company: dict[str, Any],
    ) -> AnswerPayload:
        expr = self._metric_sql_expr(column_name)
        sql = (
            f"SELECT report_period, {expr} AS metric_value FROM {table_name} "
            f"WHERE stock_code = {self._quote(company['stock_code'])} "
            "ORDER BY report_period DESC LIMIT 1"
        )
        rows = self.database.query(
            f"SELECT report_period, {expr} AS metric_value FROM {table_name} WHERE stock_code = ? ORDER BY report_period DESC LIMIT 1",
            (company["stock_code"],),
        )
        if not rows or self._is_suspicious_value(metric_label, rows[0]["metric_value"]):
            return self._answer_retrieval_v2(f"{company['stock_abbr']} {metric_label}", company, False, sql)
        row = rows[0]
        value_text = self._format_metric_value(metric_label, row["metric_value"])
        return AnswerPayload(content=f"{company['stock_abbr']}\u6700\u8fd1\u4e00\u671f\uff08{row['report_period']}\uff09\u7684{metric_label}\u4e3a\uff1a{value_text}\u3002", sql=sql)

    def _clean_series(self, metric_label: str, rows: list[Any]) -> list[tuple[str, float]]:
        series: list[tuple[str, float]] = []
        for row in rows:
            value = to_float(row["metric_value"])
            if value is None:
                continue
            if self._is_amount_metric(metric_label) and abs(value) < 1:
                continue
            series.append((str(row["report_period"]), value))
        series.sort(key=lambda item: sortable_period(item[0]))
        return series

    def _series_sql(self, table_name: str, expr: str, stock_code: str, periods: list[str]) -> str:
        if not periods:
            return (
                f"SELECT report_period, {expr} AS metric_value FROM {table_name} "
                f"WHERE stock_code = {self._quote(stock_code)} ORDER BY report_period"
            )
        quoted_periods = ", ".join(self._quote(period) for period in periods)
        order_case = " ".join(
            f"WHEN {self._quote(period)} THEN {index}"
            for index, period in enumerate(periods, start=1)
        )
        return (
            f"SELECT report_period, {expr} AS metric_value FROM {table_name} "
            f"WHERE stock_code = {self._quote(stock_code)} AND report_period IN ({quoted_periods}) "
            f"ORDER BY CASE report_period {order_case} END"
        )

    def _series_span_text(self, labels: list[str]) -> str:
        years = [label[:4] for label in labels if len(label) >= 4 and label[:4].isdigit()]
        if not years:
            return ""
        start_year = years[0]
        end_year = years[-1]
        latest_label = labels[-1]
        if latest_label.endswith("FY"):
            return f"{start_year}-{end_year}年"
        return f"{start_year}-{end_year}年（截至{latest_label}）"

    def _trend_analysis_text(self, company: dict[str, Any], metric_label: str, series: list[tuple[str, float]]) -> str:
        company_name = f"{company['stock_abbr']}（{company['stock_code']}）"
        if not series:
            return f"{company_name}{metric_label}暂无可用趋势数据"
        if len(series) == 1:
            label, value = series[0]
            return f"{company_name}{metric_label}目前仅有{label}一期数据（{self._format_metric_value(metric_label, value)}）"

        labels = [item[0] for item in series]
        values = [item[1] for item in series]
        min_index = min(range(len(values)), key=values.__getitem__)
        max_index = max(range(len(values)), key=values.__getitem__)
        span_text = self._series_span_text(labels)
        mixed_periods = any(not label.endswith("FY") for label in labels)

        if len(values) >= 3 and min_index not in (0, len(values) - 1) and values[0] > values[min_index] < values[-1]:
            pattern = "V型反转强波动走势"
        elif len(values) >= 3 and max_index not in (0, len(values) - 1) and values[0] < values[max_index] > values[-1]:
            pattern = "倒V型波动走势"
        elif all(values[index] <= values[index + 1] for index in range(len(values) - 1)):
            pattern = "持续上升走势"
        elif all(values[index] >= values[index + 1] for index in range(len(values) - 1)):
            pattern = "持续下降走势"
        else:
            pattern = "波动走势"

        parts: list[str] = [f"{company_name}{span_text}的{metric_label}呈“{pattern}”"]

        if mixed_periods and min_index >= 1:
            if all(values[index] >= values[index + 1] for index in range(min_index)):
                parts.append(f"{labels[0]}至{labels[min_index]}持续回落并在{labels[min_index]}触底")
            else:
                parts.append(f"{labels[min_index]}为阶段低点")
            if min_index < len(values) - 1:
                if all(values[index] <= values[index + 1] for index in range(min_index, len(values) - 1)):
                    parts.append(f"此后持续修复至{labels[-1]}")
                else:
                    parts.append(f"此后震荡修复至{labels[-1]}")
        else:
            parts.append(f"{labels[min_index]}为阶段低点（{self._format_metric_value(metric_label, values[min_index])}）")
            parts.append(f"{labels[max_index]}为阶段高点（{self._format_metric_value(metric_label, values[max_index])}）")

        if len(labels) >= 3:
            recent_labels = labels[-3:]
            recent_values = values[-3:]
            if len({label[:4] for label in recent_labels}) == 1:
                if all(recent_values[index] <= recent_values[index + 1] for index in range(len(recent_values) - 1)):
                    parts.append(f"{recent_labels[0]}至{recent_labels[-1]}季度改善显著")
                elif all(recent_values[index] >= recent_values[index + 1] for index in range(len(recent_values) - 1)):
                    parts.append(f"{recent_labels[0]}至{recent_labels[-1]}季度主要表现为回落")

        return "，".join(parts) + "。"

    def _answer_trend(
        self,
        question: str,
        table_name: str,
        column_name: str,
        metric_label: str,
        company: dict[str, Any],
        wants_chart: bool,
        context: dict[str, Any],
    ) -> AnswerPayload:
        expr = self._metric_sql_expr(column_name)
        base_sql = (
            f"SELECT report_period, {expr} AS metric_value FROM {table_name} "
            f"WHERE stock_code = {self._quote(company['stock_code'])} ORDER BY report_period"
        )
        rows = self.database.query(
            f"SELECT report_period, {expr} AS metric_value FROM {table_name} WHERE stock_code = ? ORDER BY report_period",
            (company["stock_code"],),
        )
        all_series = self._clean_series(metric_label, rows)
        annual_series = [item for item in all_series if item[0].endswith("FY")]
        compact = compact_text(question)
        use_all_periods = ("近几年" in compact or "截至三季报" in compact or "季度" in compact) and len(all_series) >= 4
        series = all_series if use_all_periods else (annual_series if len(annual_series) >= 3 else all_series)
        if "近三年" in compact and len(annual_series) >= 3:
            series = annual_series[-3:]
        elif use_all_periods and len(series) > 12:
            series = series[-12:]
        elif "近几年" in compact and len(series) > 8 and not use_all_periods:
            series = series[-8:]
        elif len(series) > 6 and not use_all_periods:
            series = series[-6:]
        if not series:
            return self._answer_retrieval_v2(f"{company['stock_abbr']} {metric_label} 变化趋势", company, False, base_sql)

        labels = [item[0] for item in series]
        values = [item[1] for item in series]
        sql = self._series_sql(table_name, expr, company["stock_code"], labels)
        relative_path = self._save_chart(
            "line",
            context,
            f"{company['stock_abbr']}{metric_label}趋势",
            labels,
            values,
            company["stock_code"] + metric_label + "_trend",
        )
        analysis = self._trend_analysis_text(company, metric_label, series)
        images = [relative_path] if relative_path else []
        chart_types = [self._chart_type_label("line")] if relative_path else []
        chart_tail = "已生成可视化图表。" if relative_path else ""
        content = f"{analysis}{chart_tail}".strip()
        return AnswerPayload(
            content=content,
            sql=sql,
            image=images,
            chart_types=chart_types,
        )

    def _answer_top_k(
        self,
        table_name: str,
        column_name: str,
        metric_label: str,
        period_info: dict[str, str] | None,
        top_k: int,
        context: dict[str, Any],
    ) -> AnswerPayload:
        expr = self._metric_sql_expr(column_name)
        period = period_info["report_period"] if period_info else self._latest_fy_period()
        sql = (
            f"SELECT stock_abbr, report_period, {expr} AS metric_value FROM {table_name} "
            f"WHERE report_period = {self._quote(period)} ORDER BY metric_value DESC LIMIT {top_k}"
        )
        rows = self.database.query(
            f"SELECT stock_abbr, report_period, {expr} AS metric_value FROM {table_name} WHERE report_period = ? ORDER BY metric_value DESC LIMIT ?",
            (period, top_k),
        )
        valid_rows = []
        for row in rows:
            value = to_float(row["metric_value"])
            if value is None:
                continue
            if self._is_amount_metric(metric_label) and abs(value) < 1:
                continue
            valid_rows.append((row["stock_abbr"], value))
        if not valid_rows:
            return self._answer_retrieval(f"{metric_label} top {top_k}", None, False, sql)
        relative_path = self._save_chart(
            "bar",
            context,
            f"{metric_label} Top{top_k}",
            [item[0] for item in valid_rows],
            [item[1] for item in valid_rows],
            metric_label + period + "_ranking",
        )
        summary = "\uff1b".join(
            f"{index + 1}. {name}: {self._format_metric_value(metric_label, value)}"
            for index, (name, value) in enumerate(valid_rows)
        )
        images = [relative_path] if relative_path else []
        chart_types = [self._chart_type_label("bar")] if relative_path else []
        return AnswerPayload(
            content=f"{period}\u7684{metric_label}\u6392\u540d\u7ed3\u679c\u5982\u4e0b\uff1a{summary}\u3002",
            sql=sql,
            image=images,
            chart_types=chart_types,
        )

    def _answer_profit_ranking_with_growth(self, period_info: dict[str, str] | None, top_k: int, context: dict[str, Any]) -> AnswerPayload:
        latest_period = self._latest_fy_period()
        year = period_info["year"] if period_info else latest_period[:4]
        period = f"{year}FY"
        total_profit_count = int(
            self.database.scalar(
                "SELECT COUNT(1) FROM income_sheet WHERE report_period = ? AND total_profit IS NOT NULL AND ABS(total_profit) >= 1",
                (period,),
            )
            or 0
        )
        metric_column = "total_profit" if total_profit_count >= 2 else "net_profit"
        profit_yoy_expr = "COALESCE(total_profit_yoy_growth, net_profit_yoy_growth)" if metric_column == "total_profit" else "net_profit_yoy_growth"
        sales_yoy_expr = "COALESCE(main_business_revenue_yoy_growth, operating_revenue_yoy_growth)"
        sql = (
            "SELECT stock_abbr, "
            f"{metric_column} AS profit_value, {profit_yoy_expr} AS profit_yoy, {sales_yoy_expr} AS sales_yoy "
            f"FROM income_sheet WHERE report_period = {self._quote(period)} ORDER BY profit_value DESC LIMIT {top_k}"
        )
        rows = self.database.query(
            f"""
            SELECT stock_code,
                   stock_abbr,
                   {metric_column} AS profit_value,
                   {profit_yoy_expr} AS profit_yoy,
                   {sales_yoy_expr} AS sales_yoy
            FROM income_sheet
            WHERE report_period = ?
            ORDER BY profit_value DESC
            LIMIT ?
            """,
            (period, top_k),
        )
        valid_rows = []
        for row in rows:
            profit_value = to_float(row["profit_value"])
            if profit_value is None or abs(profit_value) < 1:
                continue
            valid_rows.append(row)
        if not valid_rows:
            return self._answer_retrieval(f"{year}年利润排名及同比", None, False, sql)

        relative_path = self._save_chart(
            "bar",
            context,
            f"{year}年利润排名",
            [row["stock_abbr"] for row in valid_rows],
            [to_float(row["profit_value"]) or 0.0 for row in valid_rows],
            period + "_top_profit",
        )

        yoy_candidates: list[tuple[dict[str, Any], float]] = []
        ranking_lines = []
        for index, row in enumerate(valid_rows, start=1):
            profit = format_money_from_10k(row["profit_value"])
            profit_yoy_text, profit_yoy_value = self._fy_yoy_display(
                "income_sheet",
                str(row["stock_code"]),
                period,
                metric_column,
                row["profit_yoy"],
            )
            sales_yoy_text, _ = self._fy_yoy_display(
                "income_sheet",
                str(row["stock_code"]),
                period,
                "COALESCE(main_business_revenue, total_operating_revenue)",
                row["sales_yoy"],
            )
            if profit_yoy_value is not None:
                yoy_candidates.append((row, profit_yoy_value))
            ranking_lines.append(
                f"{index}. {row['stock_abbr']}：利润 {profit}，利润同比 {profit_yoy_text}，销售额同比 {sales_yoy_text}"
            )
        best_pair = max(yoy_candidates, key=lambda item: item[1]) if yoy_candidates else None
        suffix = (
            f"利润同比上涨幅度最大的是{best_pair[0]['stock_abbr']}，为 {best_pair[1]:.2f}%。"
            if best_pair is not None
            else "当前可抽取数据中未能稳定识别全部利润同比。"
        )
        metric_name = "利润总额" if metric_column == "total_profit" else "净利润"
        available_count = len(valid_rows)
        if available_count < top_k:
            prefix = f"{year}年当前数据库中可用于该问题统计的企业共{available_count}家，按{metric_name}口径排序结果如下："
        else:
            prefix = f"{year}年按{metric_name}口径统计的Top{top_k}企业如下："
        images = [relative_path] if relative_path else []
        chart_types = [self._chart_type_label("bar")] if relative_path else []
        return AnswerPayload(
            content=prefix + "\n" + "\n".join(ranking_lines) + "\n" + suffix,
            sql=sql,
            image=images,
            chart_types=chart_types,
        )
    def _latest_fy_period(self) -> str:
        value = self.database.scalar("SELECT report_period FROM income_sheet WHERE report_period LIKE '%FY' ORDER BY report_period DESC LIMIT 1")
        return str(value or "2024FY")

    def _is_medical_insurance_product_question(self, compact: str) -> bool:
        has_policy = any(keyword in compact for keyword in ("医保", "目录", "谈判"))
        has_product = any(keyword in compact for keyword in ("产品", "药品", "哪些", "名单"))
        has_herbal = any(keyword in compact for keyword in ("中药", "中成药"))
        has_new = any(keyword in compact for keyword in ("新增", "新进", "纳入"))
        return has_policy and has_product and has_herbal and has_new

    def _answer_medical_insurance_products(self, question: str) -> AnswerPayload | None:
        compact = compact_text(question)
        if not self._is_medical_insurance_product_question(compact):
            return None
        if not self.database.table_exists("medical_insurance_product_facts"):
            return None

        year_match = re.search(r"(20\d{2})年", compact)
        target_year = year_match.group(1) if year_match else str(
            self.database.scalar(
                "SELECT year FROM medical_insurance_product_facts WHERE year <> '' ORDER BY year DESC LIMIT 1"
            )
            or ""
        )
        if not target_year:
            return None

        sql = (
            "SELECT year, product_name, drug_category, addition_type, source_title, source_path, evidence_text, company_name "
            "FROM medical_insurance_product_facts WHERE year = "
            + self._quote(target_year)
            + " ORDER BY product_name"
        )
        rows = self.database.query(
            """
            SELECT year, product_name, drug_category, addition_type, source_title, source_path, evidence_text, company_name
            FROM medical_insurance_product_facts
            WHERE year = ?
            ORDER BY product_name
            """,
            (target_year,),
        )
        product_rows = [
            row
            for row in rows
            if str(row["product_name"] or "").strip()
            and any(
                keyword in str(row["drug_category"] or "")
                for keyword in ("中药", "中成药")
            )
        ]
        if not product_rows:
            return None

        product_names: list[str] = []
        for row in product_rows:
            name = str(row["product_name"] or "").strip()
            if name and name not in product_names:
                product_names.append(name)

        references: list[dict[str, str]] = []
        source_title = str(product_rows[0]["source_title"] or "").strip()
        source_path = str(product_rows[0]["source_path"] or source_title).strip()
        evidence_text = str(product_rows[0]["evidence_text"] or "").strip()
        if source_path or source_title:
            references.append(
                {
                    "paper_path": source_path or source_title,
                    "text": evidence_text or source_title,
                    "paper_image": "",
                }
            )

        content = (
            f"根据{target_year}年医保药品目录新增药品名单，"
            f"{target_year}年国家医保目录新增的中药产品包括："
            + "、".join(product_names)
            + f"，共{len(product_names)}个。"
        )
        return AnswerPayload(content=content, sql=sql, references=references)

    def _answer_retrieval(self, question: str, company: dict[str, Any] | None, prefer_causes: bool, sql: str = "") -> AnswerPayload:
        compact = compact_text(question)
        industry_query = self._contains_any(compact, INDUSTRY_QUERY_KEYWORDS)
        product_query = industry_query and self._contains_any(compact, PRODUCT_QUERY_KEYWORDS)
        rows = self.database.query("SELECT source_type, title, stock_code, stock_name, report_period, file_path, text FROM document_chunks")
        scored: list[tuple[float, Any]] = []
        for row in rows:
            row_text = str(row["text"] or "")
            if company and row["stock_code"] not in ("", company["stock_code"]):
                if row["stock_name"] not in ("", company["stock_abbr"], company["company_name"]):
                    continue
            if product_query and row["source_type"] not in ("industry_research_meta", "industry_research_pdf"):
                continue
            if row["source_type"] in ("financial_report_pdf", "industry_research_pdf", "stock_research_pdf") and self._bad_text_ratio(row_text) > 0.22:
                continue
            score = score_text(question, row["title"] + " " + row_text)
            if industry_query and row["source_type"] in ("industry_research_meta", "industry_research_pdf"):
                score += 4.0
            if industry_query and row["source_type"] == "financial_report_pdf":
                score -= 3.0
            if product_query and row["source_type"] == "financial_report_pdf":
                score -= 4.0
            if industry_query and any(keyword in row_text for keyword in ("\u533b\u4fdd", "\u76ee\u5f55", "\u8c08\u5224", "\u65b0\u589e")):
                score += 3.0
            if "\u7814\u62a5" in compact and "research" in row["source_type"]:
                score += 2.0
            if prefer_causes and row["source_type"] in ("stock_research_pdf", "stock_research_meta"):
                score += 3.0
            if prefer_causes and row["source_type"] == "financial_report_pdf":
                score -= 1.0
            if prefer_causes and any(keyword in row["text"] for keyword in CAUSE_KEYWORDS):
                score += 1.5
            if score > 0:
                scored.append((score, row))
        scored.sort(key=lambda item: item[0], reverse=True)
        top_rows = [row for _, row in scored[:5]]

        if not top_rows and industry_query:
            titles = self.database.query("SELECT title, publishDate, orgName FROM industry_research ORDER BY publishDate DESC LIMIT 3")
            if titles:
                lines = [f"{row['publishDate'][:10]} {row['orgName']}\u300a{row['title']}\u300b" for row in titles]
                content = "\u5df2\u547d\u4e2d\u672c\u5730\u884c\u4e1a\u7814\u62a5\u5143\u6570\u636e\uff0c\u4f46\u5f53\u524d\u79bb\u7ebf\u73af\u5883\u4e0b\u8be5\u884c\u4e1a\u7814\u62a5\u6b63\u6587\u62bd\u53d6\u4e0d\u8db3\uff0c\u6682\u65f6\u65e0\u6cd5\u7a33\u5b9a\u5217\u51fa\u5168\u90e8\u4ea7\u54c1\u540d\u79f0\u3002\u53ef\u5148\u53c2\u8003\u76f8\u5173\u7814\u62a5\u9898\u540d\uff1a\n" + "\n".join(lines)
                return AnswerPayload(content=content, sql=sql)

        if not top_rows:
            return AnswerPayload(content="\u5f53\u524d\u672a\u68c0\u7d22\u5230\u8db3\u591f\u7684\u79bb\u7ebf\u8bc1\u636e\u3002\u53ef\u4ee5\u8865\u5145\u624b\u5de5\u62bd\u53d6\u7ed3\u679c\u540e\u91cd\u65b0\u6267\u884c ingest\u3002", sql=sql)

        references: list[dict[str, str]] = []
        summary_sentences: list[str] = []
        for row in top_rows:
            sentences = [sentence for sentence in split_sentences(str(row["text"])) if self._is_clean_sentence(sentence)]
            chosen: list[str] = []
            if industry_query:
                chosen = [sentence for sentence in sentences if any(keyword in sentence for keyword in ("\u533b\u4fdd", "\u76ee\u5f55", "\u8c08\u5224", "\u65b0\u589e", "\u4e2d\u836f"))]
            if prefer_causes and not chosen:
                chosen = [sentence for sentence in sentences if any(keyword in sentence for keyword in CAUSE_KEYWORDS)]
            if not chosen:
                chosen = [sentence for sentence in sentences if score_text(question, sentence) > 0][:3]
            if not chosen and self._is_clean_sentence(str(row["title"])):
                chosen = [str(row["title"])]
            chosen_limit = 1 if prefer_causes else 2
            snippet = self._clip_clean_text(" ".join(chosen[:chosen_limit]))
            if not snippet:
                continue
            references.append({"paper_path": row["file_path"] or row["title"], "text": snippet, "paper_image": ""})
            if snippet not in summary_sentences:
                summary_sentences.append(snippet)

        if not summary_sentences:
            if industry_query:
                titles = []
                for row in top_rows:
                    title = self._clip_clean_text(str(row["title"]), limit=120)
                    if title and title not in titles:
                        titles.append(title)
                if titles:
                    content = "\u5f53\u524d\u672c\u5730\u884c\u4e1a\u7814\u62a5\u6b63\u6587\u62bd\u53d6\u4e0d\u8db3\uff0c\u65e0\u6cd5\u7a33\u5b9a\u5217\u51fa\u5b8c\u6574\u4ea7\u54c1\u540d\u5355\u3002\u53ef\u5148\u53c2\u8003\u8fd9\u4e9b\u76f8\u5173\u9898\u540d\uff1a" + "\uff1b".join(titles[:3])
                    return AnswerPayload(content=content, sql=sql, references=references)
            return AnswerPayload(content="\u5f53\u524d\u672a\u68c0\u7d22\u5230\u8db3\u591f\u7684\u79bb\u7ebf\u8bc1\u636e\u3002\u53ef\u4ee5\u8865\u5145\u624b\u5de5\u62bd\u53d6\u7ed3\u679c\u540e\u91cd\u65b0\u6267\u884c ingest\u3002", sql=sql)

        if product_query:
            product_sentences = [sentence for sentence in summary_sentences if "\u3001" in sentence and "\u65b0\u589e" in sentence]
            if not product_sentences:
                titles = []
                for row in top_rows:
                    title = row["title"]
                    if title and title not in titles:
                        titles.append(title)
                title_text = "\uff1b".join(titles[:3])
                content = "\u5f53\u524d\u672c\u5730\u7814\u62a5\u6b63\u6587\u62bd\u53d6\u4e0d\u8db3\uff0c\u65e0\u6cd5\u7a33\u5b9a\u679a\u4e3e\u201c\u56fd\u5bb6\u533b\u4fdd\u76ee\u5f55\u65b0\u589e\u7684\u4e2d\u836f\u4ea7\u54c1\u201d\u540d\u5355\u3002\u5f53\u524d\u53ef\u53c2\u8003\u7684\u8bc1\u636e\u6765\u6e90\u4e3a\uff1a" + title_text
                return AnswerPayload(content=content, sql=sql, references=references)

        prefix = "\u7efc\u5408\u672c\u5730\u8d22\u62a5\u4e0e\u7814\u62a5\u8bc1\u636e\uff0c\u53ef\u5f97\u5230\u5982\u4e0b\u7ed3\u8bba\uff1a" if prefer_causes else ""
        content = prefix + (" ".join(summary_sentences[:3]))
        return AnswerPayload(content=content, sql=sql, references=references)

    def _answer_threshold_filter_v2(
        self,
        table_name: str,
        column_name: str,
        metric_label: str,
        period_info: dict[str, str] | None,
        question: str,
    ) -> AnswerPayload | None:
        threshold = self._parse_threshold_condition(question, column_name, metric_label)
        if threshold is None:
            return None
        operator, threshold_value, threshold_label = threshold
        expr = self._metric_sql_expr(column_name)
        period = period_info["report_period"] if period_info else self._latest_fy_period()
        sql = (
            "SELECT stock_code, stock_abbr, report_period, "
            f"{expr} AS metric_value FROM {table_name} WHERE report_period = {self._quote(period)} "
            f"AND {expr} {operator} {threshold_value} ORDER BY metric_value DESC LIMIT 50"
        )
        rows = self.database.query(
            f"""
            SELECT stock_code, stock_abbr, report_period, {expr} AS metric_value
            FROM {table_name}
            WHERE report_period = ? AND {expr} {operator} ?
            ORDER BY metric_value DESC
            LIMIT 50
            """,
            (period, threshold_value),
        )
        valid_rows: list[tuple[str, str, float]] = []
        for row in rows:
            value = to_float(row["metric_value"])
            if value is None:
                continue
            valid_rows.append(
                (
                    str(row["stock_code"] or ""),
                    self._company_label(row["stock_code"], str(row["stock_abbr"] or "")),
                    value,
                )
            )
        if not valid_rows:
            return AnswerPayload(content=f"{period}没有命中{metric_label}{threshold_label}的公司。", sql=sql)

        lines = [
            f"{index}. {name}（{code}）: {self._format_metric_value(metric_label, value)}"
            for index, (code, name, value) in enumerate(valid_rows, start=1)
        ]
        return AnswerPayload(
            content=f"{period}命中{metric_label}{threshold_label}的公司共 {len(valid_rows)} 家：\n" + "\n".join(lines),
            sql=sql,
        )

    def _answer_top_k_v2(
        self,
        table_name: str,
        column_name: str,
        metric_label: str,
        period_info: dict[str, str] | None,
        top_k: int,
        context: dict[str, Any],
    ) -> AnswerPayload:
        expr = self._metric_sql_expr(column_name)
        period = period_info["report_period"] if period_info else self._latest_fy_period()
        sql = (
            f"SELECT stock_code, stock_abbr, report_period, {expr} AS metric_value FROM {table_name} "
            f"WHERE report_period = {self._quote(period)} ORDER BY metric_value DESC LIMIT {top_k}"
        )
        rows = self.database.query(
            f"""
            SELECT stock_code, stock_abbr, report_period, {expr} AS metric_value
            FROM {table_name}
            WHERE report_period = ?
            ORDER BY metric_value DESC
            LIMIT ?
            """,
            (period, top_k),
        )
        valid_rows: list[tuple[str, str, float]] = []
        for row in rows:
            value = to_float(row["metric_value"])
            if value is None:
                continue
            if self._is_amount_metric(metric_label) and abs(value) < 1:
                continue
            valid_rows.append(
                (
                    str(row["stock_code"] or ""),
                    self._company_label(row["stock_code"], str(row["stock_abbr"] or "")),
                    value,
                )
            )
        if not valid_rows:
            return self._answer_retrieval_v2(f"{metric_label} top {top_k}", None, False, sql)

        relative_path = self._save_chart(
            "bar",
            context,
            f"{metric_label} Top{top_k}",
            [item[1] for item in valid_rows],
            [item[2] for item in valid_rows],
            metric_label + period + "_ranking",
        )
        summary = "；".join(
            f"{index + 1}. {name}（{code}）: {self._format_metric_value(metric_label, value)}"
            for index, (code, name, value) in enumerate(valid_rows)
        )
        images = [relative_path] if relative_path else []
        chart_types = [self._chart_type_label("bar")] if relative_path else []
        return AnswerPayload(
            content=f"{period}的{metric_label}排名结果如下：{summary}。",
            sql=sql,
            image=images,
            chart_types=chart_types,
        )

    def _answer_profit_ranking_with_growth_v2(
        self,
        period_info: dict[str, str] | None,
        top_k: int,
        context: dict[str, Any],
    ) -> AnswerPayload:
        latest_period = self._latest_fy_period()
        year = period_info["year"] if period_info else latest_period[:4]
        period = f"{year}FY"
        total_profit_count = int(
            self.database.scalar(
                "SELECT COUNT(1) FROM income_sheet WHERE report_period = ? AND total_profit IS NOT NULL AND ABS(total_profit) >= 1",
                (period,),
            )
            or 0
        )
        metric_column = "total_profit" if total_profit_count >= 2 else "net_profit"
        profit_yoy_expr = (
            "COALESCE(total_profit_yoy_growth, net_profit_yoy_growth)"
            if metric_column == "total_profit"
            else "net_profit_yoy_growth"
        )
        sales_yoy_expr = "COALESCE(main_business_revenue_yoy_growth, operating_revenue_yoy_growth)"
        sql = (
            "SELECT stock_code, stock_abbr, "
            f"{metric_column} AS profit_value, {profit_yoy_expr} AS profit_yoy, {sales_yoy_expr} AS sales_yoy "
            f"FROM income_sheet WHERE report_period = {self._quote(period)} ORDER BY profit_value DESC LIMIT {top_k}"
        )
        rows = self.database.query(
            f"""
            SELECT stock_code,
                   stock_abbr,
                   {metric_column} AS profit_value,
                   {profit_yoy_expr} AS profit_yoy,
                   {sales_yoy_expr} AS sales_yoy
            FROM income_sheet
            WHERE report_period = ?
            ORDER BY profit_value DESC
            LIMIT ?
            """,
            (period, top_k),
        )
        valid_rows = []
        for row in rows:
            profit_value = to_float(row["profit_value"])
            if profit_value is None or abs(profit_value) < 1:
                continue
            valid_rows.append(row)
        if not valid_rows:
            return self._answer_retrieval_v2(f"{year}年利润排名及同比", None, False, sql)

        relative_path = self._save_chart(
            "bar",
            context,
            f"{year}年利润排名",
            [self._company_label(row["stock_code"], str(row["stock_abbr"] or "")) for row in valid_rows],
            [to_float(row["profit_value"]) or 0.0 for row in valid_rows],
            period + "_top_profit",
        )

        yoy_candidates: list[tuple[dict[str, Any], float]] = []
        ranking_lines: list[str] = []
        for index, row in enumerate(valid_rows, start=1):
            display_name = self._company_label(row["stock_code"], str(row["stock_abbr"] or ""))
            profit = format_money_from_10k(row["profit_value"])
            profit_yoy_text, profit_yoy_value = self._fy_yoy_display(
                "income_sheet",
                str(row["stock_code"]),
                period,
                metric_column,
                row["profit_yoy"],
            )
            sales_yoy_text, _ = self._fy_yoy_display(
                "income_sheet",
                str(row["stock_code"]),
                period,
                "COALESCE(main_business_revenue, total_operating_revenue)",
                row["sales_yoy"],
            )
            if profit_yoy_value is not None:
                yoy_candidates.append((row, profit_yoy_value))
            ranking_lines.append(
                f"{index}. {display_name}：利润 {profit}，利润同比 {profit_yoy_text}，营收同比 {sales_yoy_text}"
            )

        best_pair = max(yoy_candidates, key=lambda item: item[1]) if yoy_candidates else None
        if best_pair is not None:
            best_name = self._company_label(best_pair[0]["stock_code"], str(best_pair[0]["stock_abbr"] or ""))
            suffix = f"利润同比增幅最高的是{best_name}，为 {best_pair[1]:.2f}%。"
        else:
            suffix = "当前可抽取数据中未能稳定识别全部利润同比。"
        metric_name = "利润总额" if metric_column == "total_profit" else "净利润"
        available_count = len(valid_rows)
        if available_count < top_k:
            prefix = f"{year}年当前数据库中可用于该问题统计的企业共 {available_count} 家，按{metric_name}口径排序结果如下："
        else:
            prefix = f"{year}年按{metric_name}口径统计的 Top{top_k} 企业如下："

        images = [relative_path] if relative_path else []
        chart_types = [self._chart_type_label("bar")] if relative_path else []
        return AnswerPayload(
            content=prefix + "\n" + "\n".join(ranking_lines) + "\n" + suffix,
            sql=sql,
            image=images,
            chart_types=chart_types,
        )

    def _extract_retrieval_terms_v2(self, question: str, company: dict[str, Any] | None = None) -> list[str]:
        metric = detect_metric(question)
        period_info = parse_period(question)
        raw_terms: list[str] = []
        if metric:
            raw_terms.append(metric[2])
        if period_info:
            raw_terms.extend([period_info["year"], period_info["report_period"]])
        raw_terms.extend(re.findall(r"[\u4e00-\u9fff]{2,8}|[A-Za-z0-9_.%-]+", normalize_text(question)))
        if company:
            raw_terms.extend(
                [
                    str(company.get("stock_abbr") or ""),
                    str(company.get("company_name") or ""),
                    normalize_stock_code(company.get("stock_code")),
                ]
            )

        terms: list[str] = []
        seen: set[str] = set()
        for raw_term in raw_terms:
            text = compact_text(str(raw_term or ""))
            if not text:
                continue
            variants = [text]
            if re.fullmatch(r"[\u4e00-\u9fff]+", text):
                if len(text) >= 4:
                    variants.extend([text[:4], text[-4:]])
                if len(text) > 2:
                    variants.extend(text[index : index + 2] for index in range(len(text) - 1))
            else:
                variants = [text.lower()]
            for variant in variants:
                if len(variant) < 2:
                    continue
                if variant in RETRIEVAL_STOPWORDS:
                    continue
                if variant.isdigit() and len(variant) < 4:
                    continue
                if variant in seen:
                    continue
                seen.add(variant)
                terms.append(variant)
                if len(terms) >= 8:
                    return terms
        return terms

    def _retrieval_source_priority_v2(
        self,
        source_type: str,
        prefer_causes: bool,
        industry_query: bool,
        product_query: bool,
    ) -> int:
        if product_query or industry_query:
            order = {
                "industry_research_pdf": 1,
                "industry_research_meta": 2,
                "stock_research_pdf": 3,
                "stock_research_meta": 4,
                "financial_report_pdf": 5,
            }
        elif prefer_causes:
            order = {
                "stock_research_pdf": 1,
                "stock_research_meta": 2,
                "financial_report_pdf": 3,
                "industry_research_pdf": 4,
                "industry_research_meta": 5,
            }
        else:
            order = {
                "stock_research_pdf": 1,
                "financial_report_pdf": 2,
                "stock_research_meta": 3,
                "industry_research_pdf": 4,
                "industry_research_meta": 5,
            }
        return order.get(str(source_type or ""), 9)

    def _load_retrieval_candidates_v2(
        self,
        question: str,
        company: dict[str, Any] | None,
        prefer_causes: bool,
        industry_query: bool,
        product_query: bool,
    ) -> tuple[list[Any], str, list[str]]:
        conditions: list[str] = []
        params: list[Any] = []
        display_conditions: list[str] = []

        if company:
            conditions.append("(stock_code = ? OR stock_name IN (?, ?))")
            params.extend([company["stock_code"], company["stock_abbr"], company["company_name"]])
            display_conditions.append(
                "("
                f"stock_code = {self._quote(company['stock_code'])} OR "
                f"stock_name IN ({self._quote(company['stock_abbr'])}, {self._quote(company['company_name'])})"
                ")"
            )

        source_types: list[str] = []
        compact = compact_text(question)
        if product_query:
            source_types = ["industry_research_pdf", "industry_research_meta"]
        elif industry_query:
            source_types = ["industry_research_pdf", "industry_research_meta", "stock_research_pdf", "stock_research_meta"]
        elif prefer_causes:
            source_types = ["stock_research_pdf", "stock_research_meta", "financial_report_pdf"]
        elif "研报" in compact:
            source_types = ["stock_research_pdf", "stock_research_meta", "industry_research_pdf", "industry_research_meta"]
        if source_types:
            placeholders = ", ".join("?" for _ in source_types)
            conditions.append(f"source_type IN ({placeholders})")
            params.extend(source_types)
            display_conditions.append("source_type IN (" + ", ".join(self._quote(item) for item in source_types) + ")")

        period_info = parse_period(question)
        if period_info and not industry_query:
            conditions.append("(report_period = ? OR report_period LIKE ?)")
            params.extend([period_info["report_period"], f"{period_info['year']}%"])
            display_conditions.append(
                f"(report_period = {self._quote(period_info['report_period'])} "
                f"OR report_period LIKE {self._quote(period_info['year'] + '%')})"
            )

        terms = self._extract_retrieval_terms_v2(question, company)
        like_terms = terms[:6]
        if like_terms:
            like_conditions: list[str] = []
            display_like_conditions: list[str] = []
            for term in like_terms:
                like_conditions.append("(title LIKE ? OR text LIKE ?)")
                params.extend([f"%{term}%", f"%{term}%"])
                display_like_conditions.append(
                    f"(title LIKE {self._quote('%' + term + '%')} OR text LIKE {self._quote('%' + term + '%')})"
                )
            conditions.append("(" + " OR ".join(like_conditions) + ")")
            display_conditions.append("(" + " OR ".join(display_like_conditions) + ")")

        base_sql = "SELECT source_type, title, stock_code, stock_name, report_period, file_path, text FROM document_chunks"
        if conditions:
            base_sql += " WHERE " + " AND ".join(conditions)
        display_sql = base_sql
        if display_conditions:
            display_sql = (
                "SELECT source_type, title, stock_code, stock_name, report_period, file_path, text "
                "FROM document_chunks WHERE " + " AND ".join(display_conditions)
            )

        priority_order = (
            "CASE source_type "
            "WHEN 'stock_research_pdf' THEN 1 "
            "WHEN 'stock_research_meta' THEN 2 "
            "WHEN 'financial_report_pdf' THEN 3 "
            "WHEN 'industry_research_pdf' THEN 4 "
            "WHEN 'industry_research_meta' THEN 5 "
            "ELSE 9 END"
        )
        limit = 600 if company else (400 if industry_query or prefer_causes else 300)
        query_sql = f"{base_sql} ORDER BY {priority_order}, report_period DESC, chunk_index ASC LIMIT ?"
        rows = self.database.query(query_sql, (*params, limit))
        display_sql = f"{display_sql} ORDER BY {priority_order}, report_period DESC, chunk_index ASC LIMIT {limit}"
        return rows, display_sql, like_terms

    def _answer_retrieval_v2(
        self,
        question: str,
        company: dict[str, Any] | None,
        prefer_causes: bool,
        sql: str = "",
    ) -> AnswerPayload:
        compact = compact_text(question)
        industry_query = self._contains_any(compact, INDUSTRY_QUERY_KEYWORDS)
        product_query = industry_query and self._contains_any(compact, PRODUCT_QUERY_KEYWORDS)
        rows, candidate_sql, query_terms = self._load_retrieval_candidates_v2(
            question,
            company,
            prefer_causes,
            industry_query,
            product_query,
        )
        if not sql:
            sql = candidate_sql

        scored: list[tuple[float, Any]] = []
        for row in rows:
            row_text = str(row["text"] or "")
            if row["source_type"] in ("financial_report_pdf", "industry_research_pdf", "stock_research_pdf") and self._bad_text_ratio(row_text) > 0.22:
                continue
            combined_text = f"{row['title']} {row_text}"
            score = score_text(question, combined_text)
            term_overlap = sum(1 for term in query_terms if term and term in combined_text)
            score += min(term_overlap, 4) * 1.2
            if company and row["stock_code"] == company["stock_code"]:
                score += 5.0
            elif company and row["stock_name"] in (company["stock_abbr"], company["company_name"]):
                score += 3.0
            if industry_query and row["source_type"] in ("industry_research_meta", "industry_research_pdf"):
                score += 4.0
            if industry_query and row["source_type"] == "financial_report_pdf":
                score -= 3.0
            if product_query and row["source_type"] == "financial_report_pdf":
                score -= 4.0
            if industry_query and any(keyword in row_text for keyword in ("医保", "目录", "谈判", "新增")):
                score += 3.0
            if "研报" in compact and "research" in str(row["source_type"]):
                score += 2.0
            if prefer_causes and row["source_type"] in ("stock_research_pdf", "stock_research_meta"):
                score += 3.0
            if prefer_causes and row["source_type"] == "financial_report_pdf":
                score -= 1.0
            cause_hits = sum(1 for keyword in CAUSE_KEYWORDS if keyword in row_text)
            if prefer_causes and cause_hits:
                score += min(cause_hits, 2) * 1.5
            if prefer_causes and cause_hits == 0:
                score -= 2.0
            minimum_score = 3.0 if prefer_causes else 2.0
            if score >= minimum_score:
                scored.append((score, row))

        scored.sort(
            key=lambda item: (
                item[0],
                -self._retrieval_source_priority_v2(item[1]["source_type"], prefer_causes, industry_query, product_query),
            ),
            reverse=True,
        )
        top_rows = [row for _, row in scored[:5]]

        if not top_rows and industry_query:
            titles = self.database.query("SELECT title, publishDate, orgName FROM industry_research ORDER BY publishDate DESC LIMIT 3")
            if titles:
                lines = [f"{self._format_publish_date(row['publishDate'])} {row['orgName']}《{row['title']}》" for row in titles]
                content = "已命中本地行业研报元数据，但当前离线环境下该行业研报正文抽取不足，暂时无法稳定列出全部产品名称。可先参考相关研报题名：\n" + "\n".join(lines)
                return AnswerPayload(content=content, sql=sql)

        if not top_rows:
            return AnswerPayload(content="当前未检索到足够的离线证据。可以补充公司名称、报告期或指标后再提问。", sql=sql)

        references: list[dict[str, str]] = []
        summary_sentences: list[str] = []
        cause_reference_count = 0
        for row in top_rows:
            sentences = [sentence for sentence in split_sentences(str(row["text"])) if self._is_clean_sentence(sentence)]
            chosen: list[str] = []
            if industry_query:
                chosen = [
                    sentence
                    for sentence in sentences
                    if any(keyword in sentence for keyword in ("医保", "目录", "谈判", "新增", "中药"))
                    and (not query_terms or any(term in sentence for term in query_terms))
                ]
            if prefer_causes and not chosen:
                chosen = [
                    sentence
                    for sentence in sentences
                    if any(keyword in sentence for keyword in CAUSE_KEYWORDS)
                    and (not query_terms or any(term in sentence for term in query_terms))
                ]
            if not chosen:
                ranked_sentences = [
                    (
                        score_text(question, sentence) + sum(1 for term in query_terms if term and term in sentence),
                        sentence,
                    )
                    for sentence in sentences
                ]
                ranked_sentences = [item for item in ranked_sentences if item[0] > 0]
                ranked_sentences.sort(key=lambda item: item[0], reverse=True)
                chosen = [sentence for _, sentence in ranked_sentences[:3]]
            title_text = str(row["title"] or "")
            if not chosen and self._is_clean_sentence(title_text) and (
                not query_terms or any(term in title_text for term in query_terms)
            ):
                chosen = [title_text]
            chosen_limit = 1 if prefer_causes else 2
            snippet = self._clean_reference_snippet(" ".join(chosen[:chosen_limit]), limit=120)
            if not snippet:
                continue
            if prefer_causes and any(keyword in snippet for keyword in CAUSE_KEYWORDS):
                cause_reference_count += 1
            references.append({"paper_path": row["file_path"] or row["title"], "text": snippet, "paper_image": ""})
            if snippet not in summary_sentences:
                summary_sentences.append(snippet)

        references = self._dedupe_references(references, limit=3)
        summary_sentences = [reference["text"] for reference in references if reference.get("text")] or summary_sentences

        if not summary_sentences:
            if industry_query:
                titles = []
                for row in top_rows:
                    title = self._clip_clean_text(str(row["title"]), limit=120)
                    if title and title not in titles:
                        titles.append(title)
                if titles:
                    content = "当前本地行业研报正文抽取不足，无法稳定列出完整产品名单。可先参考这些相关题名：" + "；".join(titles[:3])
                    return AnswerPayload(content=content, sql=sql, references=references)
            return AnswerPayload(content="当前未检索到足够的离线证据。可以补充公司名称、报告期或指标后再提问。", sql=sql)

        if product_query:
            product_sentences = [sentence for sentence in summary_sentences if "、" in sentence and "新增" in sentence]
            if not product_sentences:
                titles = []
                for row in top_rows:
                    title = str(row["title"] or "")
                    if title and title not in titles:
                        titles.append(title)
                title_text = "；".join(titles[:3])
                content = "当前本地研报正文抽取不足，无法稳定枚举“国家医保目录新增的中药产品”名单。当前可参考的证据来源为：" + title_text
                return AnswerPayload(content=content, sql=sql, references=references)

        if prefer_causes and cause_reference_count == 0:
            return AnswerPayload(
                content="当前检索到的离线证据不足以稳定支持归因分析。建议补充公司名称、报告期或更具体的指标后再提问。",
                sql=sql,
                references=references,
            )

        if prefer_causes:
            conclusion = summary_sentences[0]
            evidence_text = "；".join(reference["text"] for reference in references[:2])
            content = f"结论：{conclusion}"
            if evidence_text:
                content += f"\n证据依据：{evidence_text}"
        else:
            content = " ".join(summary_sentences[:3])
        return AnswerPayload(content=content, sql=sql, references=references)

    def _export_variant(self, question_file: Path, output_file: Path, rows: list[dict[str, str]]) -> str:
        id_key = "编号"
        name_text = f"{question_file.name} {output_file.name}"
        question_ids = [str(row.get(id_key, "")).strip() for row in rows if str(row.get(id_key, "")).strip()]
        if "附件4" in name_text or "result_2" in output_file.stem:
            return "task2"
        if "附件6" in name_text or "result_3" in output_file.stem:
            return "task3"
        if question_ids and all(question_id.startswith("B1") for question_id in question_ids):
            return "task2"
        if question_ids and all(question_id.startswith("B2") for question_id in question_ids):
            return "task3"
        return "task2"

    def _export_headers(self, variant: str) -> list[str]:
        if variant == "task2":
            return ["编号", "问题", "SQL查询语句", "图形格式", "回答"]
        if variant == "task3":
            return ["编号", "问题", "SQL查询语句", "回答"]
        return ["编号", "问题", "SQL查询语句", "图形格式", "回答"]

    def _normalize_chart_type(self, chart_type: str) -> str:
        text = normalize_text(str(chart_type or "")).lower()
        if "line" in text or "折线" in text:
            return "折线图"
        if "bar" in text or "柱状" in text:
            return "柱状图"
        return ""

    def _export_chart_cell_value(self, chart_types: list[str]) -> str:
        for chart_type in chart_types:
            normalized = self._normalize_chart_type(chart_type)
            if normalized:
                return normalized
        return "无"

    def _export_image_destination(self, question_id: str, seq: int) -> tuple[Path, str]:
        file_name = f"{question_id}_{seq}.jpg"
        return self.config.artifact_dir / file_name, f"./result/{file_name}"

    def _copy_export_image(self, image: str, question_id: str, seq: int) -> str | None:
        if not question_id:
            return None
        image_text = str(image or "").strip()
        if not image_text:
            return None
        normalized_path = image_text[2:] if image_text.startswith("./") else image_text
        src = self.config.workspace_root / normalized_path
        if not src.exists():
            return None
        dst, export_path = self._export_image_destination(question_id, seq)
        if src.resolve() != dst.resolve():
            shutil.copy2(src, dst)
        return export_path

    def _clean_export_reference(self, reference: Any) -> dict[str, str]:
        if not isinstance(reference, dict):
            return {}
        cleaned_reference: dict[str, str] = {}
        for key in ("paper_path", "text", "paper_image"):
            value = reference.get(key)
            if value:
                cleaned_reference[key] = str(value)
        return cleaned_reference

    def _build_export_answer_record(
        self,
        *,
        variant: str,
        question: str,
        payload: AnswerPayload,
        question_id: str,
        seq: int,
    ) -> tuple[dict[str, Any], str]:
        export_images: list[str] = []
        for image in payload.image:
            export_path = self._copy_export_image(str(image), question_id, seq)
            if export_path and export_path not in export_images:
                export_images.append(export_path)

        export_answer: dict[str, Any] = {"content": str(payload.content or "")}
        if export_images:
            export_answer["image"] = export_images

        if variant == "task3":
            export_references: list[dict[str, str]] = []
            for reference in payload.references:
                cleaned_reference = self._clean_export_reference(reference)
                if cleaned_reference:
                    export_references.append(cleaned_reference)
            if export_references:
                export_answer["references"] = export_references

        chart_value = self._export_chart_cell_value(payload.chart_types)
        return {"Q": question, "A": export_answer}, chart_value

    def batch_export(self, question_file: Path, output_file: Path) -> Path:
        from .xlsx_tools import read_workbook, rows_to_dicts, write_simple_xlsx

        id_key = "编号"
        type_key = "问题类型"
        question_key = "问题"
        sql_key = "SQL查询语句"
        chart_key = "图形格式"
        answer_key = "回答"

        workbook = read_workbook(question_file)
        rows = rows_to_dicts(next(iter(workbook.values()), []))
        variant = self._export_variant(question_file, output_file, rows)
        headers = self._export_headers(variant)
        output_rows: list[list[object]] = [headers]
        for row in rows:
            question_id = str(row.get(id_key, "")).strip()
            answers = self._answer_items(row.get(question_key, ""), question_id=question_id)
            sql_lines: list[str] = []
            chart_value = "无"
            export_answers: list[dict[str, Any]] = []
            for index, (question, payload) in enumerate(answers, start=1):
                if payload.sql:
                    sql_lines.append(str(payload.sql))
                export_record, current_chart = self._build_export_answer_record(
                    variant=variant,
                    question=question,
                    payload=payload,
                    question_id=question_id,
                    seq=index,
                )
                if chart_value == "无" and current_chart != "无":
                    chart_value = current_chart
                export_answers.append(export_record)
            row_map: dict[str, object] = {
                id_key: question_id,
                type_key: row.get(type_key, ""),
                question_key: row.get(question_key, ""),
                sql_key: "\n".join(sql_lines),
                chart_key: chart_value if variant == "task2" else "",
                answer_key: dump_json(export_answers),
            }
            output_rows.append([row_map[header] for header in headers])
        write_simple_xlsx(output_file, "答案结果", output_rows, column_count=len(headers))
        return output_file
