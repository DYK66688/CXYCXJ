from __future__ import annotations

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
from financial_qa_assistant.database import _rebuild_financial_company_aliases
from financial_qa_assistant.database_base import Database, create_base_tables, create_financial_tables
from financial_qa_assistant.validation import run_validation


class MasterDataAlignmentTests(unittest.TestCase):
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

    def test_missing_company_info_is_covered_by_financial_aliases(self) -> None:
        config = self._temporary_config("test_master_data_alignment")
        database = Database(config.db_path)
        create_base_tables(database)
        create_financial_tables(database, config.schema_file())
        database.execute(
            """
            INSERT INTO income_sheet (
                stock_code, stock_abbr, report_period, report_date,
                net_profit, total_operating_revenue, main_business_revenue
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            ("301888", "星辰制药", "2024FY", "2025-03-01", 88.0, 560.0, 540.0),
        )

        stats = _rebuild_financial_company_aliases(database)
        report = run_validation(database, config)
        engine = FinancialQAEngine(config, database)

        self.assertEqual(1, stats["missing_company_info_count"])
        alias_row = database.query("SELECT stock_code, stock_abbr, source FROM financial_company_aliases WHERE stock_code = '301888'")[0]
        self.assertEqual("星辰制药", alias_row["stock_abbr"])
        self.assertEqual("inferred_from_financial_tables", alias_row["source"])

        detected = engine._detect_company("星辰制药2024年的净利润是多少")
        self.assertIsNotNone(detected)
        self.assertEqual("301888", detected["stock_code"])

        alignment_items = [item for item in report["stock_code_alignment"] if item["stock_code"] == "301888"]
        self.assertTrue(alignment_items)
        self.assertEqual("covered_by_alias_table", alignment_items[0]["status"])


if __name__ == "__main__":
    unittest.main()
