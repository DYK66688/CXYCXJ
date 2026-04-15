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

from financial_qa_assistant.config import AppConfig
from financial_qa_assistant.database_base import Database, create_base_tables, create_financial_tables, refresh_metric_facts
from financial_qa_assistant.validation import run_validation


class ValidationRegressionTests(unittest.TestCase):
    def _temporary_config(self, name: str) -> AppConfig:
        base = AppConfig.discover(WORKSPACE)
        temp_root = WORKSPACE / "build" / name
        shutil.rmtree(temp_root, ignore_errors=True)
        self.addCleanup(lambda: shutil.rmtree(temp_root, ignore_errors=True))
        runtime_dir = temp_root / "runtime"
        return replace(
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

    def _build_database(self, name: str) -> tuple[AppConfig, Database]:
        config = self._temporary_config(name)
        config.ensure_directories()
        database = Database(config.db_path)
        create_base_tables(database)
        create_financial_tables(database, config.schema_file())
        database.execute(
            "INSERT INTO company_info VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (1, "000001", "测试公司", "测试公司股份有限公司", "", "", "", "", "", "", "", ""),
        )
        return config, database

    def test_mid_strength_dirty_profit_is_sanitized(self) -> None:
        config, database = self._build_database("test_validation_mid_dirty_profit")
        database.execute(
            """
            INSERT INTO income_sheet (
                stock_code, stock_abbr, report_period, report_date,
                total_operating_revenue, main_business_revenue, net_profit, total_profit,
                source_file, source_excerpt
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("000001", "测试公司", "2024FY", "2025-03-01", 500.0, 480.0, 900.0, 950.0, "annual.pdf", "主要会计数据"),
        )
        refresh_metric_facts(database)

        report = run_validation(database, config)

        self.assertIsNone(database.scalar("SELECT net_profit FROM income_sheet WHERE stock_code = '000001' AND report_period = '2024FY'"))
        self.assertIsNone(database.scalar("SELECT total_profit FROM income_sheet WHERE stock_code = '000001' AND report_period = '2024FY'"))
        self.assertEqual(500.0, database.scalar("SELECT total_operating_revenue FROM income_sheet WHERE stock_code = '000001' AND report_period = '2024FY'"))
        self.assertGreaterEqual(report["summary"]["suspicious_values_by_column"]["net_profit"], 1)
        self.assertGreaterEqual(report["summary"]["suspicious_values_by_column"]["total_profit"], 1)

    def test_growth_scale_mismatch_and_ratio_outlier_are_sanitized(self) -> None:
        config, database = self._build_database("test_validation_growth_scale")
        database.executemany(
            """
            INSERT INTO income_sheet (
                stock_code, stock_abbr, report_period, report_date,
                total_operating_revenue, operating_revenue_yoy_growth, source_file, source_excerpt
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                ("000001", "测试公司", "2023FY", "2024-03-01", 1000.0, 8.0, "annual_2023.pdf", "主要会计数据"),
                ("000001", "测试公司", "2024FY", "2025-03-01", 40000.0, 35.0, "annual_2024.pdf", "主要会计数据"),
            ],
        )
        database.execute(
            """
            INSERT INTO balance_sheet (
                stock_code, stock_abbr, report_period, report_date,
                asset_total_assets, asset_liability_ratio, source_file, source_excerpt
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("000001", "测试公司", "2024FY", "2025-03-01", 3000.0, 180.0, "annual_2024.pdf", "资产负债数据"),
        )
        refresh_metric_facts(database)

        report = run_validation(database, config)

        self.assertIsNone(database.scalar("SELECT operating_revenue_yoy_growth FROM income_sheet WHERE stock_code = '000001' AND report_period = '2024FY'"))
        self.assertIsNone(database.scalar("SELECT asset_liability_ratio FROM balance_sheet WHERE stock_code = '000001' AND report_period = '2024FY'"))
        reasons = {item["reason"] for item in report["suspicious_values"]}
        self.assertIn("growth_scale_mismatch", reasons)
        self.assertIn("asset_liability_ratio_out_of_range", reasons)

    def test_validation_summary_rollups_are_written(self) -> None:
        config, database = self._build_database("test_validation_rollups")
        database.execute(
            """
            INSERT INTO cash_flow_sheet (
                stock_code, stock_abbr, report_period, report_date,
                operating_cf_net_amount, source_file, source_excerpt
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            ("000001", "测试公司", "2024FY", "2025-03-01", 50000.0, "annual.pdf", "现金流数据"),
        )
        refresh_metric_facts(database)

        report = run_validation(database, config)
        payload = json.loads((config.runtime_dir / "validation_report.json").read_text(encoding="utf-8"))

        self.assertIn("suspicious_values_by_column", payload["summary"])
        self.assertIn("sanitized_rows_by_table", payload["summary"])
        self.assertIn("top_risky_companies", payload["summary"])
        self.assertEqual(report["summary"]["sanitized_rows_by_table"], payload["summary"]["sanitized_rows_by_table"])


if __name__ == "__main__":
    unittest.main()
