from __future__ import annotations

import json
import re
import sys
import unittest
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parents[1]
if str(WORKSPACE / "src") not in sys.path:
    sys.path.insert(0, str(WORKSPACE / "src"))

from financial_qa_assistant.assistant import AnswerPayload, FinancialQAEngine
from financial_qa_assistant.cli import _resolve_export_output
from financial_qa_assistant.config import AppConfig
from financial_qa_assistant.xlsx_tools import read_workbook, write_simple_xlsx


class ExportContractTests(unittest.TestCase):
    def _prepare_paths(self, prefix: str) -> tuple[AppConfig, Path, Path, Path, Path]:
        config = AppConfig.discover(WORKSPACE)
        question_file = WORKSPACE / "build" / f"{prefix}_questions.xlsx"
        output_file = WORKSPACE / "build" / f"{prefix}_output.xlsx"
        source_image = config.artifact_dir / f"{prefix}_source.jpg"
        copied_image = config.artifact_dir / f"{prefix}.jpg"
        for path in (question_file, output_file, source_image, copied_image):
            path.unlink(missing_ok=True)
            self.addCleanup(path.unlink, missing_ok=True)
        return config, question_file, output_file, source_image, copied_image

    def test_task2_export_exact_schema(self) -> None:
        config, question_file, output_file, source_image, copied_image = self._prepare_paths("B1001_1")
        write_simple_xlsx(
            question_file,
            "Sheet1",
            [["编号", "问题类型", "问题"], ["B1001", "测试", '[{"Q": "趋势问题"}]']],
        )
        source_image.write_bytes(b"test")

        engine = FinancialQAEngine.__new__(FinancialQAEngine)
        engine.config = config
        engine._answer_items = lambda raw_payload, question_id="": [
            (
                "趋势问题",
                AnswerPayload(
                    content="第一答",
                    sql="SELECT 1",
                    image=[f"result/{source_image.name}"],
                    chart_types=["line"],
                    references=[{"paper_path": "x.pdf", "text": "不应导出", "paper_image": ""}],
                ),
            )
        ]

        FinancialQAEngine.batch_export(engine, question_file, output_file)
        workbook = read_workbook(output_file)
        rows = workbook["答案结果"]
        answer_payload = json.loads(rows[1][4])

        self.assertEqual(["编号", "问题", "SQL查询语句", "图形格式", "回答"], rows[0])
        self.assertEqual("折线图", rows[1][3])
        self.assertEqual([{"Q": "趋势问题", "A": {"content": "第一答", "image": ["./result/B1001_1.jpg"]}}], answer_payload)
        self.assertNotIn("sql", rows[1][4])
        self.assertNotIn("references", rows[1][4])
        self.assertTrue(copied_image.exists())

    def test_task3_export_exact_schema(self) -> None:
        config, question_file, output_file, source_image, copied_image = self._prepare_paths("B2001_1")
        write_simple_xlsx(
            question_file,
            "Sheet1",
            [["编号", "问题类型", "问题"], ["B2001", "测试", '[{"Q": "归因问题"}]']],
        )
        source_image.write_bytes(b"test")

        engine = FinancialQAEngine.__new__(FinancialQAEngine)
        engine.config = config
        engine._answer_items = lambda raw_payload, question_id="": [
            (
                "归因问题",
                AnswerPayload(
                    content="第一答",
                    sql="SELECT 1",
                    image=[f"result/{source_image.name}"],
                    chart_types=["bar"],
                    references=[
                        {"paper_path": "sample/report.pdf", "text": "证据片段", "paper_image": ""},
                        {"paper_path": "sample/preview.pdf", "text": "带预览的证据", "paper_image": "preview.jpg"},
                    ],
                ),
            )
        ]

        FinancialQAEngine.batch_export(engine, question_file, output_file)
        workbook = read_workbook(output_file)
        rows = workbook["答案结果"]
        answer_payload = json.loads(rows[1][3])

        self.assertEqual(["编号", "问题", "SQL查询语句", "回答"], rows[0])
        self.assertEqual(
            [
                {
                    "Q": "归因问题",
                    "A": {
                        "content": "第一答",
                        "image": ["./result/B2001_1.jpg"],
                        "references": [
                            {"paper_path": "sample/report.pdf", "text": "证据片段"},
                            {"paper_path": "sample/preview.pdf", "text": "带预览的证据", "paper_image": "preview.jpg"},
                        ],
                    },
                }
            ],
            answer_payload,
        )
        self.assertNotIn("sql", rows[1][3])
        self.assertTrue(copied_image.exists())

    def test_image_path_uses_exact_result_prefix(self) -> None:
        config, question_file, output_file, source_image, copied_image = self._prepare_paths("B1002_1")
        write_simple_xlsx(
            question_file,
            "Sheet1",
            [["编号", "问题类型", "问题"], ["B1002", "测试", '[{"Q": "绘图问题"}]']],
        )
        source_image.write_bytes(b"test")

        engine = FinancialQAEngine.__new__(FinancialQAEngine)
        engine.config = config
        engine._answer_items = lambda raw_payload, question_id="": [
            ("绘图问题", AnswerPayload(content="图", image=[f"./result/{source_image.name}"], chart_types=["柱状图"]))
        ]

        FinancialQAEngine.batch_export(engine, question_file, output_file)
        workbook = read_workbook(output_file)
        answer_payload = json.loads(workbook["答案结果"][1][4])
        image_path = answer_payload[0]["A"]["image"][0]

        self.assertRegex(image_path, r"^\./result/B1002_1\.jpg$")
        self.assertNotRegex(image_path, r"^(?:/|result/)")
        self.assertTrue(copied_image.exists())

    def test_cli_resolves_result_file_names(self) -> None:
        config = AppConfig.discover(WORKSPACE)

        attachment4 = WORKSPACE / "build" / "附件4：问题汇总.xlsx"
        attachment6 = WORKSPACE / "build" / "附件6：问题汇总.xlsx"
        for path in (attachment4, attachment6):
            path.unlink(missing_ok=True)
            self.addCleanup(path.unlink, missing_ok=True)
            write_simple_xlsx(path, "Sheet1", [["编号", "问题类型", "问题"], ["B1001", "测试", '[{"Q":"x"}]']])

        output2 = _resolve_export_output(config, attachment4)
        output3 = _resolve_export_output(config, attachment6)

        self.assertEqual(config.submission_dir / "result_2.xlsx", output2)
        self.assertEqual(config.submission_dir / "result_3.xlsx", output3)

    def test_chart_type_is_normalized_or_none(self) -> None:
        engine = FinancialQAEngine.__new__(FinancialQAEngine)

        self.assertEqual("折线图", FinancialQAEngine._export_chart_cell_value(engine, ["line"]))
        self.assertEqual("柱状图", FinancialQAEngine._export_chart_cell_value(engine, ["bar", "line"]))
        self.assertEqual("无", FinancialQAEngine._export_chart_cell_value(engine, []))


if __name__ == "__main__":
    unittest.main()
