from __future__ import annotations

import sys
import unittest
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parents[1]
if str(WORKSPACE / "src") not in sys.path:
    sys.path.insert(0, str(WORKSPACE / "src"))

from financial_qa_assistant.planner import detect_plan_intents, plan_subtasks, split_question_fragments


class PlannerTests(unittest.TestCase):
    def test_detects_ranking_yoy_and_max_intents(self) -> None:
        plan = plan_subtasks("2024年利润最高的top5企业中，谁的利润同比增幅最大？")

        self.assertTrue(plan.multi_intent)
        self.assertIn("ranking", plan.intents)
        self.assertIn("yoy", plan.intents)
        self.assertIn("maxmin", plan.intents)
        self.assertEqual(["ranking", "yoy", "maxmin"], [item.intent for item in plan.subtasks])

    def test_splits_trend_and_attribution_fragments(self) -> None:
        fragments = split_question_fragments("华润三九近三年的主营业务收入情况做可视化绘图，并说明上升原因")
        plan = plan_subtasks("华润三九近三年的主营业务收入情况做可视化绘图，并说明上升原因")

        self.assertGreaterEqual(len(fragments), 2)
        self.assertTrue(plan.multi_intent)
        self.assertEqual(2, len(plan.subtasks))

    def test_single_value_question_remains_single_intent(self) -> None:
        intents = detect_plan_intents("华润三九2024年的净利润是多少")
        plan = plan_subtasks("华润三九2024年的净利润是多少")

        self.assertEqual(["single_value"], intents)
        self.assertFalse(plan.multi_intent)
        self.assertEqual("structured", plan.query_mode)


if __name__ == "__main__":
    unittest.main()
