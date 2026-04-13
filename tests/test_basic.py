from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parents[1]
if str(WORKSPACE / "src") not in sys.path:
    sys.path.insert(0, str(WORKSPACE / "src"))

from financial_qa_assistant.assistant import AnswerPayload, FinancialQAEngine
from financial_qa_assistant.charting import bar_chart_svg, line_chart_svg, write_bar_chart_jpg, write_line_chart_jpg
from financial_qa_assistant.config import AppConfig
from financial_qa_assistant.database import _write_ingest_report, database_status
from financial_qa_assistant.database_extract import (
    _apply_annual_key_data,
    _apply_profit_statement_total_profit,
    _filter_rows_for_allowed_periods,
    _parse_annual_row_tokens,
    _parse_periodic_row_tokens,
)
from financial_qa_assistant.database_base import Database, create_base_tables
from financial_qa_assistant.question_bank import ALLOWED_QUESTION_TAGS, build_question_bank_payload
from financial_qa_assistant.utils import parse_period, parse_question_payload, sortable_period
from financial_qa_assistant.xlsx_tools import read_workbook, write_simple_xlsx


class BasicTests(unittest.TestCase):
    def test_question_payload(self) -> None:
        payload = parse_question_payload('[{"Q":"A"},{"Q":"B"}]')
        self.assertEqual(["A", "B"], [item["Q"] for item in payload])

    def test_period_parse(self) -> None:
        period = parse_period("2025年")
        fallback = parse_period("2025年第三季度利润")
        self.assertEqual("2025FY", period["report_period"])
        self.assertEqual("2025Q3", fallback["report_period"])
        self.assertLess(sortable_period("2024Q3"), sortable_period("2024FY"))

    def test_xlsx_roundtrip(self) -> None:
        path = WORKSPACE / "build" / "test_roundtrip.xlsx"
        if path.exists():
            path.unlink()
        write_simple_xlsx(path, "Sheet1", [["id", "question"], ["B1", "demo"]])
        workbook = read_workbook(path)
        self.assertEqual("demo", workbook["Sheet1"][1][1])
        path.unlink(missing_ok=True)

    def test_svg_generation(self) -> None:
        line_svg = line_chart_svg("trend", ["2023", "2024"], [1.0, 2.0])
        bar_svg = bar_chart_svg("bar", ["A", "B"], [3.0, 4.0])
        self.assertIn("<svg", line_svg)
        self.assertIn("bar", bar_svg)

    def test_jpg_generation(self) -> None:
        line_path = WORKSPACE / "build" / "test_line_chart.jpg"
        bar_path = WORKSPACE / "build" / "test_bar_chart.jpg"
        line_path.unlink(missing_ok=True)
        bar_path.unlink(missing_ok=True)
        write_line_chart_jpg(line_path, "trend", ["2023", "2024"], [1.0, 2.0])
        write_bar_chart_jpg(bar_path, "bar", ["A", "B"], [3.0, 4.0])
        self.assertTrue(line_path.exists())
        self.assertTrue(bar_path.exists())
        self.assertGreater(line_path.stat().st_size, 0)
        self.assertGreater(bar_path.stat().st_size, 0)
        line_path.unlink(missing_ok=True)
        bar_path.unlink(missing_ok=True)

    def test_batch_export_task2_schema(self) -> None:
        config = AppConfig.discover(WORKSPACE)
        question_file = WORKSPACE / "build" / "task2_questions.xlsx"
        output_file = WORKSPACE / "build" / "result_2_test.xlsx"
        source_image = config.artifact_dir / "test_export_chart.jpg"
        copied_image = config.artifact_dir / "B1001_1.jpg"

        for path in (question_file, output_file, source_image, copied_image):
            path.unlink(missing_ok=True)

        write_simple_xlsx(
            question_file,
            "Sheet1",
            [["编号", "问题类型", "问题"], ["B1001", "测试", '[{"Q": "趋势问题"}]']],
        )
        source_image.write_bytes(b"test")

        engine = FinancialQAEngine.__new__(FinancialQAEngine)
        engine.config = config
        engine._answer_items = lambda raw_payload, question_id="": [
            ("趋势问题", AnswerPayload(content="第一答", sql="SELECT 1", image=["result/test_export_chart.jpg"], chart_types=["折线图"])),
        ]

        FinancialQAEngine.batch_export(engine, question_file, output_file)
        workbook = read_workbook(output_file)
        rows = workbook["答案结果"]

        self.assertEqual(["编号", "问题", "SQL查询语句", "图形格式", "回答"], rows[0])
        self.assertEqual("B1001", rows[1][0])
        self.assertEqual("SELECT 1", rows[1][2])
        self.assertEqual("折线图", rows[1][3])
        self.assertIn('"content": "第一答"', rows[1][4])
        self.assertIn('result/B1001_1.jpg', rows[1][4])
        self.assertNotIn('"sql"', rows[1][4])
        self.assertNotIn('"references"', rows[1][4])
        self.assertTrue(copied_image.exists())

        for path in (question_file, output_file, source_image, copied_image):
            path.unlink(missing_ok=True)

    def test_batch_export_task3_schema(self) -> None:
        config = AppConfig.discover(WORKSPACE)
        question_file = WORKSPACE / "build" / "task3_questions.xlsx"
        output_file = WORKSPACE / "build" / "result_3_test.xlsx"
        source_image = config.artifact_dir / "test_export_chart.jpg"
        copied_image = config.artifact_dir / "B2001_1.jpg"

        for path in (question_file, output_file, source_image, copied_image):
            path.unlink(missing_ok=True)

        write_simple_xlsx(
            question_file,
            "Sheet1",
            [["编号", "问题类型", "问题"], ["B2001", "测试", '[{"Q": "排名问题"}]']],
        )
        source_image.write_bytes(b"test")

        engine = FinancialQAEngine.__new__(FinancialQAEngine)
        engine.config = config
        engine._answer_items = lambda raw_payload, question_id="": [
            (
                "排名问题",
                AnswerPayload(
                    content="第一答",
                    sql="SELECT 1",
                    image=["result/test_export_chart.jpg"],
                    chart_types=["柱状图"],
                    references=[
                        {
                            "paper_path": "sample/report.pdf",
                            "text": "证据片段",
                            "paper_image": "",
                        }
                    ],
                ),
            ),
        ]

        FinancialQAEngine.batch_export(engine, question_file, output_file)
        workbook = read_workbook(output_file)
        rows = workbook["答案结果"]

        self.assertEqual(["编号", "问题", "SQL查询语句", "回答"], rows[0])
        self.assertEqual("B2001", rows[1][0])
        self.assertEqual("SELECT 1", rows[1][2])
        self.assertIn('"content": "第一答"', rows[1][3])
        self.assertIn('result/B2001_1.jpg', rows[1][3])
        self.assertIn('"references": [', rows[1][3])
        self.assertIn('"paper_path": "sample/report.pdf"', rows[1][3])
        self.assertIn('"text": "证据片段"', rows[1][3])
        self.assertNotIn('"paper_image"', rows[1][3])
        self.assertNotIn('"sql"', rows[1][3])
        self.assertTrue(copied_image.exists())

        for path in (question_file, output_file, source_image, copied_image):
            path.unlink(missing_ok=True)

    def test_medical_insurance_product_answer(self) -> None:
        config = AppConfig.discover(WORKSPACE)
        db_path = WORKSPACE / "build" / "test_medical_insurance.sqlite3"
        try:
            db_path.unlink(missing_ok=True)
        except PermissionError:
            pass

        database = Database(db_path)
        create_base_tables(database)
        database.executemany(
            """
            INSERT INTO medical_insurance_product_facts (
                year, product_name, drug_category, addition_type, source_title, source_path, evidence_text, company_name
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "2025",
                    "参郁宁神片",
                    "中成药",
                    "谈判新增",
                    "2025年医保药品目录新增药品名单",
                    "https://example.com/official-list.pdf",
                    "2025年医保药品目录新增中成药包括参郁宁神片和玉女煎颗粒。",
                    "",
                ),
                (
                    "2025",
                    "玉女煎颗粒",
                    "中成药",
                    "谈判新增",
                    "2025年医保药品目录新增药品名单",
                    "https://example.com/official-list.pdf",
                    "2025年医保药品目录新增中成药包括参郁宁神片和玉女煎颗粒。",
                    "",
                ),
            ],
        )
        engine = FinancialQAEngine(config, database)
        answer = engine.answer_question("国家医保目录新增的中药产品有哪些")

        self.assertIn("参郁宁神片", answer.content)
        self.assertIn("玉女煎颗粒", answer.content)
        self.assertIn("2025", answer.content)
        self.assertIn("medical_insurance_product_facts", answer.sql)
        self.assertTrue(answer.references)

        try:
            db_path.unlink(missing_ok=True)
        except PermissionError:
            pass

    def test_periodic_row_parser_handles_multi_layouts(self) -> None:
        q1_row = _parse_periodic_row_tokens([
            "7,294,070,557.82",
            "6,352,408,318.71",
            "14.82%",
        ])
        self.assertAlmostEqual(7294070557.82, q1_row["current"])
        self.assertAlmostEqual(14.82, q1_row["current_yoy"])
        self.assertAlmostEqual(7294070557.82, q1_row["ytd"])
        self.assertAlmostEqual(14.82, q1_row["ytd_yoy"])

        q3_row = _parse_periodic_row_tokens([
            "5,634,274,230.28",
            "3.16%",
            "19,740,286,995.36",
            "6.08%",
        ])
        self.assertAlmostEqual(5634274230.28, q3_row["current"])
        self.assertAlmostEqual(3.16, q3_row["current_yoy"])
        self.assertAlmostEqual(19740286995.36, q3_row["ytd"])
        self.assertAlmostEqual(6.08, q3_row["ytd_yoy"])

        q3_no_percent = _parse_periodic_row_tokens([
            "21,307,271.49",
            "6,028.76",
            "30,770,165.28",
            "807.59",
        ])
        self.assertAlmostEqual(21307271.49, q3_no_percent["current"])
        self.assertAlmostEqual(6028.76, q3_no_percent["current_yoy"])
        self.assertAlmostEqual(30770165.28, q3_no_percent["ytd"])
        self.assertAlmostEqual(807.59, q3_no_percent["ytd_yoy"])

    def test_annual_row_parser_handles_not_applicable_yoy(self) -> None:
        revenue = _parse_annual_row_tokens([
            "585,461,786.23",
            "565,403,410.11",
            "3.55",
            "579,374,501.21",
            "74,611,329.88",
        ])
        self.assertAlmostEqual(585461786.23, revenue["current"])
        self.assertAlmostEqual(565403410.11, revenue["previous"])
        self.assertAlmostEqual(579374501.21, revenue["previous2"])
        self.assertAlmostEqual(3.55, revenue["yoy"])

        net_profit = _parse_annual_row_tokens([
            "74,611,329.88",
            "-42,890,580.25",
            "\u4e0d\u9002\u7528",
            "33,459,505.16",
            "13,774,886.74",
        ])
        self.assertAlmostEqual(74611329.88, net_profit["current"])
        self.assertAlmostEqual(-42890580.25, net_profit["previous"])
        self.assertAlmostEqual(33459505.16, net_profit["previous2"])
        self.assertIsNone(net_profit["yoy"])


    def test_periodic_row_parser_handles_adjusted_q3_layout(self) -> None:
        q3_adjusted = _parse_periodic_row_tokens([
            "7,176,243,743.45",
            "5,634,274,230.28",
            "5,634,274,230.28",
            "27.37%",
            "21,986,403,962.07",
            "19,740,286,995.36",
            "19,740,286,995.36",
            "11.38%",
        ])
        self.assertAlmostEqual(7176243743.45, q3_adjusted["current"])
        self.assertAlmostEqual(27.37, q3_adjusted["current_yoy"])
        self.assertAlmostEqual(21986403962.07, q3_adjusted["ytd"])
        self.assertAlmostEqual(11.38, q3_adjusted["ytd_yoy"])

    def test_profit_statement_parser_requires_anchor(self) -> None:
        income_rows: dict[tuple[str, str], dict[str, object]] = {}
        text = "industry profit total 3,420.7 with yoy 1.1%"
        _apply_profit_statement_total_profit(
            text=text,
            stock_code="600080",
            stock_abbr="Ginwa",
            report_period="2024FY",
            report_date="2025-04-25",
            source_file="summary.pdf",
            income_rows=income_rows,
        )
        self.assertFalse(income_rows)


    def test_annual_key_data_keeps_only_current_fy_row(self) -> None:
        income_rows: dict[tuple[str, str], dict[str, object]] = {}
        kpi_rows: dict[tuple[str, str], dict[str, object]] = {}
        balance_rows: dict[tuple[str, str], dict[str, object]] = {}
        cash_rows: dict[tuple[str, str], dict[str, object]] = {}
        text = (
            "??????????? "
            "???? 585,461,786.23 565,403,410.11 3.55 579,374,501.21 "
            "????????????? 74,611,329.88 -42,890,580.25 ??? 33,459,505.16 "
            "????????????? 41,728,335.16 46,322,344.18 -9.92 46,362,635.96 "
            "?????? 0.1364 -0.0784 ??? 0.0612 "
            "??? 2,151,989,341.37 2,067,584,032.87 4.08 2,183,773,341.36"
        )
        _apply_annual_key_data(
            text=text,
            stock_code="600080",
            stock_abbr="????",
            report_period="2024FY",
            report_date="2025-04-25",
            source_file="annual.pdf",
            income_rows=income_rows,
            kpi_rows=kpi_rows,
            balance_rows=balance_rows,
            cash_rows=cash_rows,
        )
        self.assertEqual({("600080", "2024FY")}, set(income_rows))
        self.assertEqual({("600080", "2024FY")}, set(kpi_rows))
        self.assertEqual({("600080", "2024FY")}, set(balance_rows))
        self.assertEqual({("600080", "2024FY")}, set(cash_rows))

    def test_filter_rows_drops_unallowed_periods(self) -> None:
        rows = {
            ("600080", "2021FY"): {"stock_code": "600080", "report_period": "2021FY", "total_profit": 1.0},
            ("600080", "2022FY"): {"stock_code": "600080", "report_period": "2022FY", "total_profit": 2.0},
            ("600080", "2022Q1"): {"stock_code": "600080", "report_period": "2022Q1", "total_profit": 3.0},
        }
        filtered = _filter_rows_for_allowed_periods(rows, {"600080": {"2022FY", "2022Q1"}})
        self.assertNotIn(("600080", "2021FY"), filtered)
        self.assertIn(("600080", "2022FY"), filtered)
        self.assertIn(("600080", "2022Q1"), filtered)

    def test_question_bank_uses_controlled_question_labels(self) -> None:
        db_path = WORKSPACE / "build" / "test_question_bank_tags.sqlite3"
        db_path.unlink(missing_ok=True)

        database = Database(db_path)
        create_base_tables(database)
        database.executemany(
            """
            INSERT INTO company_info (
                serial_number, stock_code, stock_abbr, company_name, english_name,
                industry, listed_exchange, security_type, registered_region,
                registered_capital, employee_count, management_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "1",
                    "000999",
                    "华润三九",
                    "华润三九医药股份有限公司",
                    "",
                    "中药",
                    "深交所",
                    "A股",
                    "深圳",
                    "",
                    "10000",
                    "200",
                )
            ],
        )
        database.executemany(
            """
            INSERT INTO financial_metric_facts (
                stock_code, stock_abbr, report_period, report_date, metric_key,
                metric_label, metric_value, yoy_value, source_type, source_file, source_excerpt
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "000999",
                    "华润三九",
                    "2025FY",
                    "2025-12-31",
                    "net_profit",
                    "净利润",
                    1.0,
                    0.1,
                    "income_sheet",
                    "annual.pdf",
                    "test",
                ),
                (
                    "000999",
                    "华润三九",
                    "2025FY",
                    "2025-12-31",
                    "main_business_revenue",
                    "主营业务收入",
                    2.0,
                    0.2,
                    "income_sheet",
                    "annual.pdf",
                    "test",
                ),
            ],
        )
        database.executemany(
            "INSERT INTO question_bank VALUES (?, ?, ?, ?)",
            [
                ("B1001", "数据基本查询", '[{"Q":"华润三九的员工人数是多少"}]', "附件4：问题汇总.xlsx"),
                ("B2003", "归因分析", '[{"Q":"华润三九近三年的主营业务收入情况做可视化绘图"},{"Q":"主营业务收入上升的原因是什么"}]', "附件6：问题汇总.xlsx"),
            ],
        )

        config = AppConfig.discover(WORKSPACE)
        engine = FinancialQAEngine(config, database)
        payload = build_question_bank_payload(
            engine,
            [
                {
                    "id": "custom-1",
                    "title": "校验问题",
                    "question": "请核对华润三九2025年的净利润在利润表和指标事实表中是否一致",
                    "tags": ["数据校验", "华润三九", "乱填标签"],
                    "note": "",
                }
            ],
        )

        self.assertTrue(payload["system"])
        for item in payload["system"]:
            self.assertTrue(set(item["tags"]).issubset(set(ALLOWED_QUESTION_TAGS)))

        self.assertTrue(any(item["question_type"] == "开放性问题" for item in payload["system"]))
        self.assertTrue(any(item["question_type"] == "融合查询" for item in payload["system"]))
        self.assertTrue(any(item["question_type"] == "数据校验" for item in payload["system"]))

        official_item = next(item for item in payload["official"] if item["question_id"] == "B2003")
        self.assertIn("数据统计分析查询", official_item["tags"])
        self.assertIn("多意图", official_item["tags"])
        self.assertIn("归因分析", official_item["tags"])
        self.assertIn("融合查询", official_item["tags"])

        custom_item = payload["custom"][0]
        self.assertEqual("数据校验", custom_item["question_type"])
        self.assertIn("数据校验", custom_item["tags"])
        self.assertNotIn("华润三九", custom_item["tags"])
        self.assertNotIn("乱填标签", custom_item["tags"])

        try:
            db_path.unlink(missing_ok=True)
        except PermissionError:
            pass

    def test_database_status_reports_empty_database(self) -> None:
        config = AppConfig.discover(WORKSPACE)
        db_path = WORKSPACE / "build" / "test_empty_status.sqlite3"
        manifest_path = WORKSPACE / "build" / "runtime" / "test_empty_status_manifest.json"
        original_db_path = config.db_path
        original_manifest_path = config.ingest_manifest_path
        config.db_path = db_path
        config.ingest_manifest_path = manifest_path

        db_path.unlink(missing_ok=True)
        manifest_path.unlink(missing_ok=True)

        database = Database(db_path)
        create_base_tables(database)
        manifest_path.write_text(
            '{"data_root": "%s"}' % str(config.contest_data_dir.resolve()).replace("\\", "\\\\"),
            encoding="utf-8",
        )

        status = database_status(config)
        self.assertFalse(status["ready"])
        self.assertEqual("empty", status["code"])

        try:
            db_path.unlink(missing_ok=True)
        except PermissionError:
            pass
        manifest_path.unlink(missing_ok=True)
        config.db_path = original_db_path
        config.ingest_manifest_path = original_manifest_path

    def test_database_file_can_be_replaced_after_queries(self) -> None:
        temp_path = WORKSPACE / "build" / "test_replace_source.sqlite3"
        target_path = WORKSPACE / "build" / "test_replace_target.sqlite3"
        for path in (temp_path, target_path):
            try:
                path.unlink(missing_ok=True)
            except PermissionError:
                pass

        database = Database(temp_path)
        create_base_tables(database)
        database.execute("INSERT INTO company_info VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (
            1, "000001", "测试", "测试公司", "", "", "", "", "", "", "", "",
        ))
        self.assertEqual(1, database.scalar("SELECT COUNT(1) FROM company_info"))

        temp_path.replace(target_path)
        self.assertTrue(target_path.exists())

        try:
            target_path.unlink(missing_ok=True)
        except PermissionError:
            pass

    def test_ingest_report_can_be_written(self) -> None:
        config = AppConfig.discover(WORKSPACE)
        report_path = config.ingest_report_path
        original = report_path.read_text(encoding="utf-8") if report_path.exists() else None
        payload = {
            "status": "success",
            "elapsed_seconds": 1.23,
            "pdf_counts": {"financial_reports": 1, "research_reports": 2, "total": 3},
            "row_counts": {"document_chunks": 10, "financial_metric_facts": 5},
            "stage_durations_seconds": {"解析财报 PDF": 0.5},
        }

        _write_ingest_report(config, payload)
        self.assertTrue(report_path.exists())
        self.assertEqual(payload["pdf_counts"]["total"], json.loads(report_path.read_text(encoding="utf-8"))["pdf_counts"]["total"])

        if original is None:
            report_path.unlink(missing_ok=True)
        else:
            report_path.write_text(original, encoding="utf-8")

if __name__ == "__main__":
    unittest.main()
