from __future__ import annotations

import json
import shutil
import sys
import unittest
from dataclasses import replace
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parents[1]
if str(WORKSPACE / "src") not in sys.path:
    sys.path.insert(0, str(WORKSPACE / "src"))

from financial_qa_assistant.assistant import FinancialQAEngine
from financial_qa_assistant.config import AppConfig
from financial_qa_assistant.database_base import Database, create_base_tables, create_financial_tables


class EndToEndRegressionTests(unittest.TestCase):
    def _temporary_config(self, name: str) -> AppConfig:
        base = AppConfig.discover(WORKSPACE)
        temp_root = WORKSPACE / "build" / name
        shutil.rmtree(temp_root, ignore_errors=True)
        self.addCleanup(lambda: shutil.rmtree(temp_root, ignore_errors=True))
        runtime_dir = temp_root / "runtime"
        config = replace(
            base,
            build_dir=temp_root,
            artifact_dir=temp_root / "result",
            submission_dir=temp_root / "submission",
            manual_import_dir=temp_root / "manual_import",
            manual_source_dir=temp_root / "manual_source",
            export_dir=temp_root / "exports",
            package_dir=temp_root / "packages",
            runtime_dir=runtime_dir,
            db_path=temp_root / "artifacts" / "finance_qa_assistant.sqlite3",
            question_library_path=runtime_dir / "question_library.json",
            answer_history_path=runtime_dir / "answer_history.json",
            system_question_state_path=runtime_dir / "system_question_state.json",
            ingest_manifest_path=runtime_dir / "ingest_manifest.json",
            ingest_report_path=runtime_dir / "ingest_report.json",
        )
        config.ensure_directories()
        return config

    def _seed_company(self, database: Database, serial: int, stock_code: str, stock_abbr: str, company_name: str) -> None:
        database.execute(
            """
            INSERT INTO company_info (
                serial_number, stock_code, stock_abbr, company_name, english_name,
                industry, listed_exchange, security_type, registered_region,
                registered_capital, employee_count, management_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (serial, stock_code, stock_abbr, company_name, "", "医药制造业", "深交所", "A股", "深圳", "", "1000", "100"),
        )

    def _seed_income(self, database: Database, stock_code: str, stock_abbr: str, report_period: str, revenue: float, net_profit: float) -> None:
        database.execute(
            """
            INSERT INTO income_sheet (
                stock_code, stock_abbr, report_period, report_date,
                total_operating_revenue, main_business_revenue,
                net_profit, total_profit,
                operating_revenue_yoy_growth, main_business_revenue_yoy_growth, net_profit_yoy_growth
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                stock_code,
                stock_abbr,
                report_period,
                f"{int(report_period[:4]) + 1}-03-01",
                revenue,
                revenue,
                net_profit,
                net_profit * 1.1,
                None,
                None,
                None,
            ),
        )

    def _seed_balance(self, database: Database, stock_code: str, stock_abbr: str, report_period: str, ratio: float) -> None:
        database.execute(
            """
            INSERT INTO balance_sheet (
                stock_code, stock_abbr, report_period, report_date,
                asset_total_assets, asset_liability_ratio
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (stock_code, stock_abbr, report_period, f"{int(report_period[:4]) + 1}-03-01", 3000.0, ratio),
        )

    def _seed_chunk(self, database: Database, source_type: str, title: str, stock_code: str, stock_name: str, report_period: str, file_path: str, text: str) -> None:
        database.execute(
            """
            INSERT INTO document_chunks (
                source_type, title, stock_code, stock_name, report_period, file_path, chunk_index, text
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (source_type, title, stock_code, stock_name, report_period, file_path, 0, text),
        )

    def _build_engine(self) -> tuple[FinancialQAEngine, AppConfig]:
        config = self._temporary_config("test_end_to_end_regression")
        database = Database(config.db_path)
        create_base_tables(database)
        create_financial_tables(database, config.schema_file())

        self._seed_company(database, 1, "000999", "华润三九", "华润三九医药股份有限公司")
        self._seed_company(database, 2, "000001", "甲公司", "甲公司股份有限公司")
        self._seed_company(database, 3, "000002", "乙公司", "乙公司股份有限公司")

        for period, revenue, profit in (("2022FY", 420.0, 60.0), ("2023FY", 500.0, 80.0), ("2024FY", 620.0, 120.0)):
            self._seed_income(database, "000999", "华润三九", period, revenue, profit)
        for period, revenue, profit in (("2023FY", 450.0, 70.0), ("2024FY", 580.0, 95.0)):
            self._seed_income(database, "000001", "甲公司", period, revenue, profit)
        for period, revenue, profit in (("2023FY", 440.0, 68.0), ("2024FY", 610.0, 110.0)):
            self._seed_income(database, "000002", "乙公司", period, revenue, profit)

        self._seed_balance(database, "000999", "华润三九", "2024FY", 42.0)
        self._seed_balance(database, "000001", "甲公司", "2024FY", 65.0)
        self._seed_balance(database, "000002", "乙公司", "2024FY", 58.0)

        self._seed_chunk(
            database,
            "stock_research_pdf",
            "华润三九：渠道恢复推动收入增长",
            "000999",
            "华润三九",
            "2024FY",
            "reports/hrsjs_growth.pdf",
            "CHC业务持续回暖，品牌力提升与渠道恢复共同推动主营业务收入增长。",
        )
        self._seed_chunk(
            database,
            "financial_report_pdf",
            "华润三九2024年年度报告",
            "000999",
            "华润三九",
            "2024FY",
            "reports/hrsjs_annual.pdf",
            "新品放量和渠道改善带动主营业务收入提升，库存结构同步优化。",
        )

        engine = FinancialQAEngine(config, database)
        return engine, config

    def test_regression_cases(self) -> None:
        engine, _config = self._build_engine()
        cases = json.loads((WORKSPACE / "tests" / "fixtures" / "regression_cases.json").read_text(encoding="utf-8"))
        contexts = {"fresh": {}, "scalar_chain": {}, "ranking_chain": {}}

        for case in cases:
            context = contexts[case["context_group"]]
            answer = engine.answer_question(case["question"], context)

            self.assertTrue(answer.content, case["id"])
            if case.get("expect_sql"):
                self.assertTrue(answer.sql, case["id"])
            if case.get("expect_image"):
                self.assertTrue(answer.image, case["id"])
            if case.get("expect_references"):
                self.assertTrue(answer.references, case["id"])
            if case.get("expect_clarification"):
                self.assertIn("请", answer.content, case["id"])
            else:
                self.assertNotEqual("retrieval", context.get("_debug_trace", {}).get("fallback_reason"), case["id"])


if __name__ == "__main__":
    unittest.main()
