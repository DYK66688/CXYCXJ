from __future__ import annotations

from dataclasses import replace
import json
import shutil
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

WORKSPACE = Path(__file__).resolve().parents[1]
if str(WORKSPACE / "src") not in sys.path:
    sys.path.insert(0, str(WORKSPACE / "src"))

from financial_qa_assistant.config import AppConfig
from financial_qa_assistant.database import ingest_all
from financial_qa_assistant.database_base import Database, create_base_tables, create_financial_tables, refresh_metric_facts
from financial_qa_assistant.validation import run_validation


class ValidationTests(unittest.TestCase):
    def _build_database(self, name: str) -> tuple[AppConfig, Database, Path]:
        config = self._temporary_config(name.replace(".sqlite3", ""))
        database = Database(config.db_path)
        create_base_tables(database)
        create_financial_tables(database, config.schema_file())
        return config, database, config.db_path

    def _seed_company(self, database: Database, stock_code: str = "000001") -> None:
        database.execute(
            "INSERT INTO company_info VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (1, stock_code, "测试公司", "测试公司股份有限公司", "", "", "", "", "", "", "", ""),
        )

    def _temporary_config(self, name: str) -> AppConfig:
        base = AppConfig.discover(WORKSPACE)
        temp_root = WORKSPACE / "build" / name
        shutil.rmtree(temp_root, ignore_errors=True)
        self.addCleanup(lambda: shutil.rmtree(temp_root, ignore_errors=True))
        runtime_dir = temp_root / "runtime"
        db_path = temp_root / "artifacts" / "finance_qa_assistant.sqlite3"
        config = replace(
            base,
            contest_data_dir=base.contest_data_dir,
            build_dir=temp_root,
            artifact_dir=temp_root / "result",
            submission_dir=temp_root / "submission",
            manual_import_dir=temp_root / "manual_import",
            manual_source_dir=temp_root / "manual_source",
            export_dir=temp_root / "exports",
            package_dir=temp_root / "packages",
            runtime_dir=runtime_dir,
            db_path=db_path,
            question_library_path=runtime_dir / "question_library.json",
            answer_history_path=runtime_dir / "answer_history.json",
            system_question_state_path=runtime_dir / "system_question_state.json",
            ingest_manifest_path=runtime_dir / "ingest_manifest.json",
            ingest_report_path=runtime_dir / "ingest_report.json",
        )
        config.ensure_directories()
        return config

    def test_validation_report_is_written(self) -> None:
        config, database, _db_path = self._build_database("test_validation_report.sqlite3")
        self._seed_company(database)
        database.execute(
            """
            INSERT INTO income_sheet (
                serial_number, stock_code, stock_abbr, report_period, report_date,
                net_profit, total_operating_revenue
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (1, "000001", "测试公司", "2024FY", "2025-03-01", 100.0, 200.0),
        )
        refresh_metric_facts(database)

        report = run_validation(database, config)

        report_path = config.runtime_dir / "validation_report.json"
        self.assertTrue(report_path.exists())
        payload = json.loads(report_path.read_text(encoding="utf-8"))
        self.assertEqual("success", payload["status"])
        self.assertIn("coverage_by_table", payload)
        self.assertEqual(report["summary"]["duplicate_key_issue_count"], payload["summary"]["duplicate_key_issue_count"])

    def test_suspicious_value_is_nullified(self) -> None:
        config, database, _db_path = self._build_database("test_validation_suspicious.sqlite3")
        self._seed_company(database)
        database.execute(
            """
            INSERT INTO balance_sheet (
                serial_number, stock_code, stock_abbr, report_period, report_date,
                asset_total_assets
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (1, "000001", "测试公司", "2024FY", "2025-03-01", 999999999.0),
        )
        refresh_metric_facts(database)

        report = run_validation(database, config)

        sanitized_value = database.scalar(
            "SELECT asset_total_assets FROM balance_sheet WHERE stock_code = ? AND report_period = ?",
            ("000001", "2024FY"),
        )
        self.assertIsNone(sanitized_value)
        self.assertGreater(report["summary"]["sanitized_value_count"], 0)
        self.assertTrue(any(item["column"] == "asset_total_assets" for item in report["suspicious_values"]))

    def test_encoding_issue_is_detected(self) -> None:
        config, database, _db_path = self._build_database("test_validation_encoding.sqlite3")
        self._seed_company(database)
        database.execute(
            """
            INSERT INTO financial_metric_facts (
                stock_code, stock_abbr, report_period, report_date, metric_key,
                metric_label, metric_value, yoy_value, source_type, source_file, source_excerpt
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("000001", "测试公司", "2024FY", "2025-03-01", "net_profit", "鍑€鍒╂鼎", 1.0, None, "structured", "annual.pdf", ""),
        )

        report = run_validation(database, config)

        self.assertGreater(report["summary"]["encoding_issue_count"], 0)
        self.assertTrue(any(item["metric_key"] == "net_profit" for item in report["encoding_issues"]))

    def test_ingest_all_writes_validation_report(self) -> None:
        config = self._temporary_config("test_ingest_validation")

        def _create_minimal_financial_tables(database: Database, _schema_file: Path) -> None:
            database.execute(
                """
                CREATE TABLE IF NOT EXISTS income_sheet (
                    serial_number INTEGER,
                    stock_code TEXT,
                    stock_abbr TEXT,
                    report_period TEXT,
                    report_year TEXT,
                    report_date TEXT,
                    total_operating_revenue REAL,
                    main_business_revenue REAL,
                    net_profit REAL,
                    total_profit REAL,
                    source_file TEXT,
                    source_excerpt TEXT
                )
                """
            )
            database.execute(
                """
                CREATE TABLE IF NOT EXISTS core_performance_indicators_sheet (
                    serial_number INTEGER,
                    stock_code TEXT,
                    stock_abbr TEXT,
                    report_period TEXT,
                    report_year TEXT,
                    report_date TEXT,
                    eps REAL,
                    diluted_eps REAL,
                    roe REAL,
                    source_file TEXT,
                    source_excerpt TEXT
                )
                """
            )
            database.execute(
                """
                CREATE TABLE IF NOT EXISTS balance_sheet (
                    serial_number INTEGER,
                    stock_code TEXT,
                    stock_abbr TEXT,
                    report_period TEXT,
                    report_year TEXT,
                    report_date TEXT,
                    asset_total_assets REAL,
                    asset_cash_and_cash_equivalents REAL,
                    asset_accounts_receivable REAL,
                    asset_inventory REAL,
                    equity_parent_net_assets REAL,
                    source_file TEXT,
                    source_excerpt TEXT
                )
                """
            )
            database.execute(
                """
                CREATE TABLE IF NOT EXISTS cash_flow_sheet (
                    serial_number INTEGER,
                    stock_code TEXT,
                    stock_abbr TEXT,
                    report_period TEXT,
                    report_year TEXT,
                    report_date TEXT,
                    operating_cf_net_amount REAL,
                    investing_cf_net_amount REAL,
                    financing_cf_net_amount REAL,
                    source_file TEXT,
                    source_excerpt TEXT
                )
                """
            )

        def _load_company_info(database: Database, _config: AppConfig) -> None:
            self._seed_company(database)

        def _load_financial_reports(database: Database, _config: AppConfig, log=None) -> dict[str, object]:
            database.execute(
                """
                INSERT INTO income_sheet (
                    serial_number, stock_code, stock_abbr, report_period, report_year, report_date,
                    total_operating_revenue, main_business_revenue, net_profit, total_profit, source_file, source_excerpt
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (1, "000001", "测试公司", "2024FY", "2024", "2025-03-01", 200.0, 180.0, 100.0, 120.0, "annual.pdf", "摘要"),
            )
            database.execute(
                """
                INSERT INTO core_performance_indicators_sheet (
                    serial_number, stock_code, stock_abbr, report_period, report_year, report_date,
                    eps, diluted_eps, roe, source_file, source_excerpt
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (1, "000001", "测试公司", "2024FY", "2024", "2025-03-01", 0.5, 0.48, 12.0, "annual.pdf", "摘要"),
            )
            database.execute(
                """
                INSERT INTO balance_sheet (
                    serial_number, stock_code, stock_abbr, report_period, report_year, report_date,
                    asset_total_assets, asset_cash_and_cash_equivalents, asset_accounts_receivable,
                    asset_inventory, equity_parent_net_assets, source_file, source_excerpt
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (1, "000001", "测试公司", "2024FY", "2024", "2025-03-01", 500.0, 120.0, 60.0, 70.0, 260.0, "annual.pdf", "摘要"),
            )
            database.execute(
                """
                INSERT INTO cash_flow_sheet (
                    serial_number, stock_code, stock_abbr, report_period, report_year, report_date,
                    operating_cf_net_amount, investing_cf_net_amount, financing_cf_net_amount, source_file, source_excerpt
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (1, "000001", "测试公司", "2024FY", "2024", "2025-03-01", 80.0, -30.0, 20.0, "annual.pdf", "摘要"),
            )
            return {
                "source_priority_rule": ["利润表正文", "主要会计数据和财务指标", "分季度拆解"],
                "conflicts": [],
                "coverage": {
                    "income_sheet": 1,
                    "core_performance_indicators_sheet": 1,
                    "balance_sheet": 1,
                    "cash_flow_sheet": 1,
                },
            }

        no_op = lambda *args, **kwargs: None

        with (
            patch("financial_qa_assistant.database.create_financial_tables", side_effect=_create_minimal_financial_tables),
            patch.object(AppConfig, "schema_file", return_value=Path("ignored.xlsx")),
            patch.object(AppConfig, "financial_report_pdfs", return_value=[]),
            patch.object(AppConfig, "research_report_pdfs", return_value=[]),
            patch("financial_qa_assistant.database.load_company_info", side_effect=_load_company_info),
            patch("financial_qa_assistant.database.load_question_bank", side_effect=no_op),
            patch("financial_qa_assistant.database.load_research", side_effect=no_op),
            patch("financial_qa_assistant.database.load_company_profile_chunks", side_effect=no_op),
            patch("financial_qa_assistant.database.load_research_metadata_chunks", side_effect=no_op),
            patch("financial_qa_assistant.database.load_financial_reports", side_effect=_load_financial_reports),
            patch("financial_qa_assistant.database.load_research_pdf_chunks", side_effect=no_op),
            patch("financial_qa_assistant.database.load_seed_csvs", side_effect=no_op),
            patch("financial_qa_assistant.database.load_manual_csvs", side_effect=no_op),
        ):
            ingest_all(config)

        report_path = config.runtime_dir / "validation_report.json"
        self.assertTrue(report_path.exists())
        payload = json.loads(report_path.read_text(encoding="utf-8"))
        self.assertEqual("success", payload["status"])
        self.assertEqual(0, payload["summary"]["encoding_issue_count"])
        self.assertEqual(1, payload["coverage_by_table"]["income_sheet"]["row_count"])


if __name__ == "__main__":
    unittest.main()
