from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


ANSWER_KEYWORD = "\u7b54\u6848\u7ed3\u679c"
FORMAL_DATA_RELATIVE = Path("\u9898\u76ee") / "B\u9898\u6570\u636e\u53ca\u63d0\u4ea4\u8bf4\u660e" / "\u5168\u90e8\u6570\u636e" / "\u6b63\u5f0f\u6570\u636e"


def _looks_like_contest_data_dir(path: Path) -> bool:
    if not path.is_dir():
        return False
    required = (
        "\u9644\u4ef61\uff1a\u4e2d\u836f\u4e0a\u5e02\u516c\u53f8\u57fa\u672c\u4fe1\u606f\uff08\u622a\u81f3\u52302025\u5e7412\u670822\u65e5\uff09.xlsx",
        "\u9644\u4ef63\uff1a\u6570\u636e\u5e93-\u8868\u540d\u53ca\u5b57\u6bb5\u8bf4\u660e.xlsx",
        "\u9644\u4ef64\uff1a\u95ee\u9898\u6c47\u603b.xlsx",
        "\u9644\u4ef66\uff1a\u95ee\u9898\u6c47\u603b.xlsx",
    )
    return all((path / name).exists() for name in required)


def _find_contest_data_dir(root: Path) -> Path:
    direct = root / FORMAL_DATA_RELATIVE
    if _looks_like_contest_data_dir(direct):
        return direct
    for path in sorted(root.rglob("\u6b63\u5f0f\u6570\u636e")):
        parts = {part.lower() for part in path.parts}
        if any(part in {"build", "result", ".venv", ".idea"} for part in parts):
            continue
        if _looks_like_contest_data_dir(path):
            return path
    return root


def _first_match(root: Path, suffix: str, *keywords: str, skip_generated: bool = True) -> Path | None:
    lowered = [keyword.lower() for keyword in keywords]
    for path in sorted(root.rglob(f"*{suffix}")):
        parts = {part.lower() for part in path.parts}
        if skip_generated and ("build" in parts or "result" in parts):
            continue
        name = path.name.lower()
        if path.name.startswith("~$"):
            continue
        if ANSWER_KEYWORD in path.name:
            continue
        if all(keyword in name for keyword in lowered):
            return path
    return None


def _all_matches(root: Path, suffix: str, *keywords: str, skip_generated: bool = True) -> list[Path]:
    lowered = [keyword.lower() for keyword in keywords]
    matches: list[Path] = []
    for path in sorted(root.rglob(f"*{suffix}")):
        parts = {part.lower() for part in path.parts}
        if skip_generated and ("build" in parts or "result" in parts):
            continue
        name = path.name.lower()
        if path.name.startswith("~$"):
            continue
        if ANSWER_KEYWORD in path.name:
            continue
        if all(keyword in name for keyword in lowered):
            matches.append(path)
    return matches


@dataclass(slots=True)
class AppConfig:
    workspace_root: Path
    contest_data_dir: Path
    build_dir: Path
    artifact_dir: Path
    submission_dir: Path
    manual_import_dir: Path
    manual_source_dir: Path
    export_dir: Path
    package_dir: Path
    runtime_dir: Path
    db_path: Path
    static_dir: Path
    question_library_path: Path
    answer_history_path: Path
    system_question_state_path: Path
    ingest_manifest_path: Path
    ingest_report_path: Path

    @classmethod
    def discover(cls, workspace_root: Path | None = None) -> "AppConfig":
        root = (workspace_root or Path(__file__).resolve().parents[2]).resolve()
        contest_data_dir = _find_contest_data_dir(root)
        build_dir = root / "build"
        artifact_dir = root / "result"
        submission_dir = root / "提交文件"
        manual_import_dir = build_dir / "manual_import"
        manual_source_dir = build_dir / "manual_source"
        export_dir = build_dir / "exports"
        package_dir = build_dir / "packages"
        runtime_dir = build_dir / "runtime"
        static_dir = root / "web" / "static"
        config = cls(
            workspace_root=root,
            contest_data_dir=contest_data_dir,
            build_dir=build_dir,
            artifact_dir=artifact_dir,
            submission_dir=submission_dir,
            manual_import_dir=manual_import_dir,
            manual_source_dir=manual_source_dir,
            export_dir=export_dir,
            package_dir=package_dir,
            runtime_dir=runtime_dir,
            db_path=build_dir / "artifacts" / "finance_qa_assistant.sqlite3",
            static_dir=static_dir,
            question_library_path=runtime_dir / "question_library.json",
            answer_history_path=runtime_dir / "answer_history.json",
            system_question_state_path=runtime_dir / "system_question_state.json",
            ingest_manifest_path=runtime_dir / "ingest_manifest.json",
            ingest_report_path=runtime_dir / "ingest_report.json",
        )
        config.ensure_directories()
        return config

    def ensure_directories(self) -> None:
        self.build_dir.mkdir(parents=True, exist_ok=True)
        self.artifact_dir.mkdir(parents=True, exist_ok=True)
        self.submission_dir.mkdir(parents=True, exist_ok=True)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.manual_import_dir.mkdir(parents=True, exist_ok=True)
        self.manual_source_dir.mkdir(parents=True, exist_ok=True)
        self.export_dir.mkdir(parents=True, exist_ok=True)
        self.package_dir.mkdir(parents=True, exist_ok=True)
        self.runtime_dir.mkdir(parents=True, exist_ok=True)

    def _search_roots(self) -> list[tuple[Path, bool]]:
        roots: list[tuple[Path, bool]] = []
        seen: set[Path] = set()
        for root, skip_generated in (
            (self.manual_source_dir, False),
            (self.contest_data_dir, True),
        ):
            resolved = root.resolve()
            if resolved in seen:
                continue
            roots.append((root, skip_generated))
            seen.add(resolved)
        return roots

    def _first_match_in_roots(self, suffix: str, *keywords: str) -> Path | None:
        for root, skip_generated in self._search_roots():
            match = _first_match(root, suffix, *keywords, skip_generated=skip_generated)
            if match:
                return match
        return None

    def _all_matches_in_roots(self, suffix: str, *keywords: str) -> list[Path]:
        matches: list[Path] = []
        seen: set[Path] = set()
        for root, skip_generated in self._search_roots():
            for path in _all_matches(root, suffix, *keywords, skip_generated=skip_generated):
                resolved = path.resolve()
                if resolved not in seen:
                    matches.append(path)
                    seen.add(resolved)
        return matches

    def company_info_file(self) -> Path:
        match = self._first_match_in_roots(".xlsx", "基本信息")
        if not match:
            raise FileNotFoundError("未找到公司基本信息 Excel。")
        return match

    def schema_file(self) -> Path:
        match = self._first_match_in_roots(".xlsx", "表名及字段说明") or self._first_match_in_roots(
            ".xlsx",
            "字段说明",
        )
        if not match:
            raise FileNotFoundError("未找到数据库字段说明 Excel。")
        return match

    def question_files(self) -> list[Path]:
        matches = self._all_matches_in_roots(".xlsx", "问题汇总")
        if not matches:
            raise FileNotFoundError("未找到问题汇总 Excel。")
        return matches

    def stock_research_file(self) -> Path | None:
        return self._first_match_in_roots(".xlsx", "个股_研报信息")

    def industry_research_file(self) -> Path | None:
        return self._first_match_in_roots(".xlsx", "行业_研报信息")

    def problem_statement_pdf(self) -> Path | None:
        for root, skip_generated in self._search_roots():
            for path in sorted(root.glob("*.pdf")):
                parts = {part.lower() for part in path.parts}
                if skip_generated and ("build" in parts or "result" in parts):
                    continue
                if "智能问数" in path.name or "上市公司财报" in path.name:
                    return path
        return None

    def financial_report_pdfs(self) -> list[Path]:
        reports: list[Path] = []
        seen: set[Path] = set()
        for root, skip_generated in self._search_roots():
            for path in sorted(root.rglob("*.pdf")):
                parts = {part.lower() for part in path.parts}
                if skip_generated and ("build" in parts or "result" in parts):
                    continue
                if any(parent.name.startswith("reports-") for parent in path.parents):
                    resolved = path.resolve()
                    if resolved not in seen:
                        reports.append(path)
                        seen.add(resolved)
        return reports

    def research_report_pdfs(self) -> list[Path]:
        reports: list[Path] = []
        seen: set[Path] = set()
        for root, skip_generated in self._search_roots():
            for path in sorted(root.rglob("*.pdf")):
                parts = {part.lower() for part in path.parts}
                if skip_generated and ("build" in parts or "result" in parts):
                    continue
                if any("研报" in parent.name for parent in path.parents):
                    resolved = path.resolve()
                    if resolved not in seen:
                        reports.append(path)
                        seen.add(resolved)
        return reports
