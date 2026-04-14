from __future__ import annotations

import csv
import re
import sqlite3
from pathlib import Path
from typing import Any, Iterable

from .config import AppConfig
from .pdf_tools import chunk_text
from .utils import FACT_METRIC_SPECS, get_standard_metric_label, normalize_stock_code, sortable_period
from .xlsx_tools import read_workbook, rows_to_dicts


SQLITE_TYPE_MAPPING = {
    "int": "INTEGER",
    "bigint": "INTEGER",
    "decimal": "REAL",
    "float": "REAL",
    "double": "REAL",
    "varchar": "TEXT",
    "char": "TEXT",
    "text": "TEXT",
    "date": "TEXT",
    "datetime": "TEXT",
}

EXTRA_COLUMNS: dict[str, list[tuple[str, str]]] = {
    "income_sheet": [
        ("total_profit", "REAL"),
        ("total_profit_yoy_growth", "REAL"),
        ("main_business_revenue", "REAL"),
        ("main_business_revenue_yoy_growth", "REAL"),
        ("source_file", "TEXT"),
        ("source_excerpt", "TEXT"),
    ],
    "core_performance_indicators_sheet": [
        ("diluted_eps", "REAL"),
        ("source_file", "TEXT"),
        ("source_excerpt", "TEXT"),
    ],
    "balance_sheet": [
        ("equity_parent_net_assets", "REAL"),
        ("source_file", "TEXT"),
        ("source_excerpt", "TEXT"),
    ],
    "cash_flow_sheet": [
        ("source_file", "TEXT"),
        ("source_excerpt", "TEXT"),
    ],
}

FACT_COLUMN_MAP: dict[str, list[tuple[str, str, str | None]]] = FACT_METRIC_SPECS

NUMBER_TOKEN_PATTERN = re.compile(r"不适用|-?[\d,]+(?:\.\d+)?%?")


class Database:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        return connection

    def execute(self, sql: str, params: Iterable[Any] = ()) -> None:
        connection = self.connect()
        try:
            connection.execute(sql, tuple(params))
            connection.commit()
        finally:
            connection.close()

    def executemany(self, sql: str, rows: Iterable[Iterable[Any]]) -> None:
        connection = self.connect()
        try:
            connection.executemany(sql, rows)
            connection.commit()
        finally:
            connection.close()

    def query(self, sql: str, params: Iterable[Any] = ()) -> list[sqlite3.Row]:
        connection = self.connect()
        try:
            return connection.execute(sql, tuple(params)).fetchall()
        finally:
            connection.close()

    def scalar(self, sql: str, params: Iterable[Any] = ()) -> Any:
        rows = self.query(sql, params)
        if not rows:
            return None
        return rows[0][0]

    def table_columns(self, table: str) -> set[str]:
        rows = self.query(f"PRAGMA table_info({table})")
        return {str(row['name']) for row in rows}

    def table_column_order(self, table: str) -> list[str]:
        rows = self.query(f"PRAGMA table_info({table})")
        return [str(row["name"]) for row in rows]

    def table_row_count(self, table: str) -> int:
        return int(self.scalar(f"SELECT COUNT(1) FROM {table}") or 0)

    def has_column(self, table: str, column: str) -> bool:
        return column in self.table_columns(table)

    def table_exists(self, table: str) -> bool:
        return bool(self.scalar("SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?", (table,)))


def sqlite_type(raw_type: str) -> str:
    lowered = (raw_type or "").lower()
    for key, value in SQLITE_TYPE_MAPPING.items():
        if lowered.startswith(key):
            return value
    return "TEXT"


def ensure_table_columns(database: Database, table: str, columns: list[tuple[str, str]]) -> None:
    existing = database.table_columns(table)
    for column_name, column_type in columns:
        if column_name not in existing:
            database.execute(f"ALTER TABLE {table} ADD COLUMN {column_name} {column_type}")


def create_base_tables(database: Database) -> None:
    database.execute(
        """
        CREATE TABLE IF NOT EXISTS company_info (
            serial_number INTEGER,
            stock_code TEXT,
            stock_abbr TEXT,
            company_name TEXT,
            english_name TEXT,
            industry TEXT,
            listed_exchange TEXT,
            security_type TEXT,
            registered_region TEXT,
            registered_capital TEXT,
            employee_count TEXT,
            management_count TEXT
        )
        """
    )
    database.execute(
        """
        CREATE TABLE IF NOT EXISTS question_bank (
            question_id TEXT,
            question_type TEXT,
            question_payload TEXT,
            source_file TEXT
        )
        """
    )
    database.execute(
        """
        CREATE TABLE IF NOT EXISTS stock_research (
            title TEXT,
            stockName TEXT,
            stockCode TEXT,
            orgCode TEXT,
            orgName TEXT,
            orgSName TEXT,
            publishDate TEXT,
            predictNextTwoYearEps TEXT,
            predictNextTwoYearPe TEXT,
            predictNextYearEps TEXT,
            predictNextYearPe TEXT,
            predictThisYearEps TEXT,
            predictThisYearPe TEXT,
            predictLastYearEps TEXT,
            predictLastYearPe TEXT,
            indvInduName TEXT,
            emRatingName TEXT,
            lastEmRatingName TEXT,
            indvIsNew TEXT,
            researcher TEXT,
            newListingDate TEXT,
            newPurchaseDate TEXT,
            newIssuePrice TEXT,
            newPeIssueA TEXT,
            indvAimPriceT TEXT,
            indvAimPriceL TEXT,
            sRatingName TEXT,
            sRatingCode TEXT,
            market TEXT
        )
        """
    )
    database.execute(
        """
        CREATE TABLE IF NOT EXISTS industry_research (
            title TEXT,
            orgCode TEXT,
            orgName TEXT,
            orgSName TEXT,
            publishDate TEXT,
            industryName TEXT,
            emRatingName TEXT,
            lastEmRatingName TEXT,
            researcher TEXT,
            sRatingName TEXT,
            sRatingCode TEXT
        )
        """
    )
    database.execute(
        """
        CREATE TABLE IF NOT EXISTS document_chunks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_type TEXT,
            title TEXT,
            stock_code TEXT,
            stock_name TEXT,
            report_period TEXT,
            file_path TEXT,
            chunk_index INTEGER,
            text TEXT
        )
        """
    )
    database.execute(
        """
        CREATE TABLE IF NOT EXISTS financial_metric_facts (
            stock_code TEXT,
            stock_abbr TEXT,
            report_period TEXT,
            report_date TEXT,
            metric_key TEXT,
            metric_label TEXT,
            metric_value REAL,
            yoy_value REAL,
            source_type TEXT,
            source_file TEXT,
            source_excerpt TEXT
        )
        """
    )
    database.execute(
        """
        CREATE TABLE IF NOT EXISTS medical_insurance_product_facts (
            year TEXT,
            product_name TEXT,
            drug_category TEXT,
            addition_type TEXT,
            source_title TEXT,
            source_path TEXT,
            evidence_text TEXT,
            company_name TEXT
        )
        """
    )


def create_financial_tables(database: Database, schema_file: Path) -> None:
    workbook = read_workbook(schema_file)
    table_name_rows = rows_to_dicts(workbook.get("数据库表名", []))
    name_mapping = {row.get("中文名称", ""): row.get("英文名称", "") for row in table_name_rows}
    common_columns = [
        ("stock_code", "TEXT"),
        ("stock_abbr", "TEXT"),
        ("report_period", "TEXT"),
        ("report_date", "TEXT"),
    ]
    for sheet_name, rows in workbook.items():
        if sheet_name == "数据库表名":
            continue
        table_name = name_mapping.get(sheet_name)
        if not table_name and sheet_name == "核心业绩指标表":
            table_name = "core_performance_indicators_sheet"
        if not table_name:
            continue
        column_rows = rows_to_dicts(rows)
        columns: list[tuple[str, str]] = []
        for row in column_rows:
            field_name = (row.get("字段名称") or "").strip()
            if not field_name:
                continue
            field_type = sqlite_type((row.get("字段类型") or row.get("字段类型 ") or "").strip())
            columns.append((field_name, field_type))
        known = {name for name, _ in columns}
        for column_name, column_type in common_columns:
            if column_name not in known:
                columns.insert(0, (column_name, column_type))
        column_sql = ", ".join(f"{name} {column_type}" for name, column_type in columns)
        database.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({column_sql})")
        ensure_table_columns(database, table_name, EXTRA_COLUMNS.get(table_name, []))


def load_company_info(database: Database, config: AppConfig) -> None:
    workbook = read_workbook(config.company_info_file())
    rows = rows_to_dicts(workbook.get("基本信息表", []))
    database.execute("DELETE FROM company_info")
    values = []
    for row in rows:
        values.append(
            (
                row.get("序号", ""),
                normalize_stock_code(row.get("股票代码", "")),
                row.get("A股简称", ""),
                row.get("公司名称", ""),
                row.get("英文名称", ""),
                row.get("所属证监会行业", ""),
                row.get("上市交易所", ""),
                row.get("证券类别", ""),
                row.get("注册区域", ""),
                row.get("注册资本", ""),
                row.get("雇员人数", ""),
                row.get("管理人员人数", ""),
            )
        )
    database.executemany("INSERT INTO company_info VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", values)


def load_question_bank(database: Database, config: AppConfig) -> None:
    database.execute("DELETE FROM question_bank")
    rows: list[tuple[str, str, str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for path in config.question_files():
        workbook = read_workbook(path)
        first_sheet = next(iter(workbook.values()), [])
        for record in rows_to_dicts(first_sheet):
            item = (
                record.get("编号", ""),
                record.get("问题类型", ""),
                record.get("问题", ""),
                path.name,
            )
            dedupe_key = item[:3]
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            rows.append(item)
    if rows:
        database.executemany("INSERT INTO question_bank VALUES (?, ?, ?, ?)", rows)


def insert_rows(database: Database, table: str, rows: list[dict[str, str]]) -> None:
    database.execute(f"DELETE FROM {table}")
    if not rows:
        return
    columns = list(rows[0].keys())
    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join('?' for _ in columns)})"
    database.executemany(sql, ([row.get(column, "") for column in columns] for row in rows))


def load_research(database: Database, config: AppConfig) -> None:
    stock_path = config.stock_research_file()
    industry_path = config.industry_research_file()
    if stock_path:
        workbook = read_workbook(stock_path)
        sheet_rows = rows_to_dicts(next(iter(workbook.values()), []))
        for row in sheet_rows:
            row["stockCode"] = normalize_stock_code(row.get("stockCode", ""))
        insert_rows(database, "stock_research", sheet_rows)
    else:
        database.execute("DELETE FROM stock_research")

    if industry_path:
        workbook = read_workbook(industry_path)
        sheet_rows = rows_to_dicts(next(iter(workbook.values()), []))
        insert_rows(database, "industry_research", sheet_rows)
    else:
        database.execute("DELETE FROM industry_research")


def insert_document_chunks(
    database: Database,
    source_type: str,
    title: str,
    stock_code: str,
    stock_name: str,
    report_period: str,
    file_path: str,
    text: str,
) -> None:
    chunks = chunk_text(text)
    if not chunks:
        return
    rows = [
        (source_type, title, stock_code, stock_name, report_period, file_path, index, chunk)
        for index, chunk in enumerate(chunks)
    ]
    database.executemany(
        """
        INSERT INTO document_chunks (
            source_type, title, stock_code, stock_name, report_period, file_path, chunk_index, text
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
def load_company_profile_chunks(database: Database) -> None:
    for company in database.query("SELECT * FROM company_info ORDER BY stock_code"):
        summary = (
            f"{company['stock_abbr']}（{company['stock_code']}），公司全称为{company['company_name']}。"
            f"所属行业：{company['industry']}。上市交易所：{company['listed_exchange']}。"
            f"注册资本：{company['registered_capital']}。员工人数：{company['employee_count']}。"
            f"管理人员人数：{company['management_count']}。"
        )
        insert_document_chunks(
            database,
            "company_profile",
            company["stock_abbr"],
            company["stock_code"],
            company["stock_abbr"],
            "",
            "",
            summary,
        )


def load_research_metadata_chunks(database: Database) -> None:
    for row in database.query("SELECT * FROM stock_research ORDER BY publishDate"):
        text = (
            f"个股研报标题：{row['title']}。股票：{row['stockName']}（{row['stockCode']}）。"
            f"机构：{row['orgName']}。发布日期：{row['publishDate']}。"
            f"评级：{row['emRatingName']}。研究员：{row['researcher']}。"
        )
        insert_document_chunks(
            database,
            "stock_research_meta",
            row["title"],
            row["stockCode"],
            row["stockName"],
            "",
            "",
            text,
        )

    for row in database.query("SELECT * FROM industry_research ORDER BY publishDate"):
        text = (
            f"行业研报标题：{row['title']}。行业：{row['industryName']}。机构：{row['orgName']}。"
            f"发布日期：{row['publishDate']}。评级：{row['emRatingName']}。研究员：{row['researcher']}。"
        )
        insert_document_chunks(
            database,
            "industry_research_meta",
            row["title"],
            "",
            "",
            "",
            "",
            text,
        )


def _load_csv_into_table(database: Database, csv_path: Path, replace: bool = True) -> None:
    table_name = csv_path.stem
    if not database.table_exists(table_name):
        return
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
    if not rows:
        return
    if replace:
        database.execute(f"DELETE FROM {table_name}")
    columns = list(rows[0].keys())
    sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join('?' for _ in columns)})"
    database.executemany(sql, ([row.get(column, "") for column in columns] for row in rows))


def load_seed_csvs(database: Database, config: AppConfig) -> None:
    seed_dir = config.workspace_root / "seed_data"
    if not seed_dir.exists():
        return
    for csv_path in sorted(seed_dir.glob("*.csv")):
        _load_csv_into_table(database, csv_path, replace=True)


def load_manual_csvs(database: Database, config: AppConfig) -> None:
    for csv_path in sorted(config.manual_import_dir.glob("*.csv")):
        _load_csv_into_table(database, csv_path, replace=True)


def write_financial_table(database: Database, table: str, rows: dict[tuple[str, str], dict[str, Any]]) -> None:
    database.execute(f"DELETE FROM {table}")
    if not rows:
        return
    ordered_columns = database.table_column_order(table)
    ordered_rows = sorted(
        rows.values(),
        key=lambda row: (row.get("stock_code", ""), sortable_period(str(row.get("report_period", "")))),
    )
    values = []
    for index, row in enumerate(ordered_rows, start=1):
        if "serial_number" in ordered_columns and row.get("serial_number") in (None, ""):
            row["serial_number"] = index
        values.append([row.get(column) for column in ordered_columns])
    sql = f"INSERT INTO {table} ({', '.join(ordered_columns)}) VALUES ({', '.join('?' for _ in ordered_columns)})"
    database.executemany(sql, values)


def refresh_metric_facts(database: Database) -> None:
    database.execute("DELETE FROM financial_metric_facts")
    rows: list[tuple[Any, ...]] = []
    for table, mappings in FACT_COLUMN_MAP.items():
        if not database.table_exists(table):
            continue
        column_set = database.table_columns(table)
        query_columns = ["stock_code", "stock_abbr", "report_period", "report_date"]
        if "source_file" in column_set:
            query_columns.append("source_file")
        if "source_excerpt" in column_set:
            query_columns.append("source_excerpt")
        for column, _, yoy_column in mappings:
            if column in column_set and column not in query_columns:
                query_columns.append(column)
            if yoy_column and yoy_column in column_set and yoy_column not in query_columns:
                query_columns.append(yoy_column)
        table_rows = database.query(f"SELECT {', '.join(query_columns)} FROM {table}")
        for row in table_rows:
            for column, _label, yoy_column in mappings:
                if column not in row.keys():
                    continue
                value = row[column]
                if value in (None, ""):
                    continue
                label = get_standard_metric_label(table, column)
                rows.append(
                    (
                        row["stock_code"],
                        row["stock_abbr"],
                        row["report_period"],
                        row["report_date"],
                        column,
                        label,
                        value,
                        row[yoy_column] if yoy_column and yoy_column in row.keys() else None,
                        "structured",
                        row["source_file"] if "source_file" in row.keys() else "",
                        row["source_excerpt"] if "source_excerpt" in row.keys() else "",
                    )
                )
    if rows:
        database.executemany(
            """
            INSERT INTO financial_metric_facts (
                stock_code, stock_abbr, report_period, report_date,
                metric_key, metric_label, metric_value, yoy_value,
                source_type, source_file, source_excerpt
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )


