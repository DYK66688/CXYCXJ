from __future__ import annotations

import sys
import unittest
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parents[1]
if str(WORKSPACE / "src") not in sys.path:
    sys.path.insert(0, str(WORKSPACE / "src"))

from financial_qa_assistant.planner import plan_subtasks


class PlannerRegressionTests(unittest.TestCase):
    def test_ranking_growth_variant_hits_multi_intent_pipeline(self) -> None:
        plan = plan_subtasks("2024年前十企业里谁增幅最大")

        self.assertTrue(plan.multi_intent)
        self.assertEqual("ranking_yoy_max_pipeline", plan.fallback_reason)
        self.assertIn("ranking", plan.intents)
        self.assertIn("yoy", plan.intents)
        self.assertIn("maxmin", plan.intents)
        self.assertIn("同比增幅最大", plan.canonical_question)

    def test_trend_reason_variant_splits_into_two_subtasks(self) -> None:
        plan = plan_subtasks("华润三九近几年趋势并分析原因")

        self.assertTrue(plan.multi_intent)
        self.assertGreaterEqual(len(plan.subtasks), 2)
        self.assertIn("trend", [item.intent for item in plan.subtasks])
        self.assertIn("attribution", [item.intent for item in plan.subtasks])
        self.assertEqual("trend_then_attribution", plan.fallback_reason)

    def test_draw_then_explain_variant_is_canonicalized(self) -> None:
        plan = plan_subtasks("先画图再解释华润三九主营业务收入变化原因")

        self.assertTrue(plan.multi_intent)
        self.assertIn("趋势绘图", plan.canonical_question)
        self.assertIn("attribution", plan.intents)

    def test_list_and_compare_yoy_variant_is_not_reduced_to_single_value(self) -> None:
        plan = plan_subtasks("列出企业并比较同比")

        self.assertTrue(plan.multi_intent or "yoy" in plan.intents)
        self.assertIn("yoy", plan.intents)

    def test_threshold_then_extreme_variant_keeps_multi_intent(self) -> None:
        plan = plan_subtasks("哪些公司满足净利润高于50万元并找出最高者")

        self.assertTrue(plan.multi_intent)
        self.assertIn("ranking", plan.intents)
        self.assertIn("maxmin", plan.intents)
        self.assertEqual("threshold_then_extreme", plan.fallback_reason)


if __name__ == "__main__":
    unittest.main()
