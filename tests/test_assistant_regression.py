from __future__ import annotations

import sys
import unittest
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parents[1]
if str(WORKSPACE / "src") not in sys.path:
    sys.path.insert(0, str(WORKSPACE / "src"))

from financial_qa_assistant.assistant import FinancialQAEngine
from financial_qa_assistant.config import AppConfig
from financial_qa_assistant.database_base import Database, create_base_tables, create_financial_tables


class AssistantRegressionTests(unittest.TestCase):
    def _build_database(self, name: str) -> tuple[AppConfig, Database, Path]:
        config = AppConfig.discover(WORKSPACE)
        db_path = WORKSPACE / "build" / name
        db_path.unlink(missing_ok=True)
        self.addCleanup(db_path.unlink, missing_ok=True)
        database = Database(db_path)
        create_base_tables(database)
        create_financial_tables(database, config.schema_file())
        return config, database, db_path

    def _seed_company(self, database: Database, stock_code: str, stock_abbr: str, company_name: str) -> None:
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
                    stock_code,
                    stock_abbr,
                    company_name,
                    "",
                    "医药制造业",
                    "深圳证券交易所",
                    "A股",
                    "深圳",
                    "",
                    "1000",
                    "10",
                )
            ],
        )

    def test_missing_company_attribution_returns_clarification(self) -> None:
        config, database, _db_path = self._build_database("test_missing_company_attr.sqlite3")
        engine = FinancialQAEngine(config, database)

        answer = engine.answer_question("\u4e3b\u8425\u4e1a\u52a1\u6536\u5165\u4e0a\u5347\u7684\u539f\u56e0\u662f\u4ec0\u4e48", {})

        self.assertIn("\u5148\u786e\u5b9a\u516c\u53f8\u540d\u79f0", answer.content)
        self.assertEqual("", answer.sql)

    def test_research_publish_date_formats_excel_serial(self) -> None:
        config, database, _db_path = self._build_database("test_research_publish_date.sqlite3")
        self._seed_company(database, "000999", "\u534e\u6da6\u4e09\u4e5d", "\u534e\u6da6\u4e09\u4e5d\u533b\u836f\u80a1\u4efd\u6709\u9650\u516c\u53f8")
        database.executemany(
            """
            INSERT INTO stock_research (
                title, stockName, stockCode, orgName, publishDate, emRatingName
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "\u54c1\u724c\u529b\u6301\u7eed\u5f70\u663e",
                    "\u534e\u6da6\u4e09\u4e5d",
                    "000999",
                    "\u6d4b\u8bd5\u5238\u5546",
                    "46022",
                    "\u4e70\u5165",
                )
            ],
        )
        engine = FinancialQAEngine(config, database)

        answer = engine.answer_question("\u0032\u0030\u0032\u0035\u5e74\u53d1\u5e03\u4e86\u54ea\u4e9b\u5173\u4e8e\u534e\u6da6\u4e09\u4e5d\u7684\u7814\u62a5", {})

        self.assertIn("2025-12-31", answer.content)
        self.assertNotIn("46022", answer.content)

    def test_generic_company_words_do_not_trigger_company_detection(self) -> None:
        config, database, _db_path = self._build_database("test_generic_company_detection.sqlite3")
        self._seed_company(database, "000590", "\u542f\u8fea\u836f\u4e1a", "\u542f\u8fea\u836f\u4e1a\u96c6\u56e2\u80a1\u4efd\u516c\u53f8")
        engine = FinancialQAEngine(config, database)

        detected = engine._detect_company("\u8d44\u4ea7\u8d1f\u503a\u7387\u8d85\u8fc7\u0036\u0030\u0025\u7684\u516c\u53f8\u6709\u54ea\u4e9b")

        self.assertIsNone(detected)

    def test_threshold_filter_without_company_returns_cross_company_result(self) -> None:
        config, database, _db_path = self._build_database("test_threshold_filter.sqlite3")
        self._seed_company(database, "000001", "\u7532\u516c\u53f8", "\u7532\u516c\u53f8\u80a1\u4efd\u6709\u9650\u516c\u53f8")
        self._seed_company(database, "000002", "\u4e59\u516c\u53f8", "\u4e59\u516c\u53f8\u80a1\u4efd\u6709\u9650\u516c\u53f8")
        database.executemany(
            """
            INSERT INTO balance_sheet (
                stock_code, stock_abbr, report_period, asset_liability_ratio
            ) VALUES (?, ?, ?, ?)
            """,
            [
                ("000001", "\u7532\u516c\u53f8", "2024FY", 65.0),
                ("000002", "\u4e59\u516c\u53f8", "2024FY", 58.0),
            ],
        )
        engine = FinancialQAEngine(config, database)

        answer = engine.answer_question("\u8d44\u4ea7\u8d1f\u503a\u7387\u8d85\u8fc7\u0036\u0030\u0025\u7684\u516c\u53f8\u6709\u54ea\u4e9b", {})

        self.assertIn("2024FY命中资产负债率> 60%的公司共 1 家", answer.content)
        self.assertIn("\u7532\u516c\u53f8", answer.content)
        self.assertNotIn("\u8bf7\u95ee\u4f60\u8981\u67e5\u8be2", answer.content)


if __name__ == "__main__":
    unittest.main()
