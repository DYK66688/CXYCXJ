from __future__ import annotations

import sys
import unittest
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parents[1]
if str(WORKSPACE / "src") not in sys.path:
    sys.path.insert(0, str(WORKSPACE / "src"))

from financial_qa_assistant.config import AppConfig
from financial_qa_assistant.database_base import Database, create_base_tables, create_financial_tables, refresh_metric_facts, write_financial_table
from financial_qa_assistant.database_extract import _assign, _ensure_row


class LineageTraceTests(unittest.TestCase):
    def test_key_field_lineage_and_conflicts_are_persisted(self) -> None:
        config = AppConfig.discover(WORKSPACE)
        db_path = WORKSPACE / "build" / "test_lineage_trace.sqlite3"
        db_path.unlink(missing_ok=True)
        self.addCleanup(db_path.unlink, missing_ok=True)

        database = Database(db_path)
        create_base_tables(database)
        create_financial_tables(database, config.schema_file())

        income_rows: dict[tuple[str, str], dict[str, object]] = {}
        row = _ensure_row(
            income_rows,
            "000001",
            "测试公司",
            "2024FY",
            "2025-03-01",
            "annual_stage_a.pdf",
            "年度主要会计数据",
            priority=10,
        )
        _assign(row, "net_profit", 100.0, priority=10)

        row = _ensure_row(
            income_rows,
            "000001",
            "测试公司",
            "2024FY",
            "2025-03-01",
            "annual_stage_b.pdf",
            "利润表正文",
            priority=30,
        )
        _assign(row, "net_profit", 120.0, priority=30)
        _assign(row, "total_operating_revenue", 800.0, priority=30)

        write_financial_table(database, "income_sheet", income_rows)
        refresh_metric_facts(database)

        chosen_lineage = database.query(
            """
            SELECT source_file, source_excerpt, decision
            FROM structured_field_lineage
            WHERE table_name = 'income_sheet'
              AND stock_code = '000001'
              AND report_period = '2024FY'
              AND field_name = 'net_profit'
              AND decision = 'chosen'
            """
        )
        self.assertEqual(1, len(chosen_lineage))
        self.assertEqual("annual_stage_b.pdf", chosen_lineage[0]["source_file"])
        self.assertEqual("利润表正文", chosen_lineage[0]["source_excerpt"])

        conflict_rows = database.query(
            """
            SELECT decision, candidate_source_file, candidate_source_excerpt
            FROM structured_field_lineage
            WHERE table_name = 'income_sheet'
              AND stock_code = '000001'
              AND report_period = '2024FY'
              AND field_name = 'net_profit'
              AND decision <> 'chosen'
            """
        )
        self.assertTrue(conflict_rows)
        self.assertEqual("replace_existing", conflict_rows[0]["decision"])
        self.assertEqual("annual_stage_a.pdf", conflict_rows[0]["candidate_source_file"])
        self.assertEqual("年度主要会计数据", conflict_rows[0]["candidate_source_excerpt"])

        fact_row = database.query(
            """
            SELECT source_file, source_excerpt
            FROM financial_metric_facts
            WHERE stock_code = '000001' AND report_period = '2024FY' AND metric_key = 'net_profit'
            """
        )[0]
        self.assertEqual("annual_stage_b.pdf", fact_row["source_file"])
        self.assertEqual("利润表正文", fact_row["source_excerpt"])


if __name__ == "__main__":
    unittest.main()
