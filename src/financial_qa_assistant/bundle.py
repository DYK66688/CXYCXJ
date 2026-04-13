from __future__ import annotations

import json
import sys
import zipfile
from datetime import datetime
from pathlib import Path

from .config import AppConfig


EXCLUDED_PARTS = {
    ".git",
    ".idea",
    ".venv",
    "__pycache__",
    ".pytest_cache",
    "build",
    "result",
    "提交文件",
}
EXCLUDED_SUFFIXES = {".pyc", ".pyo", ".pyd", ".sqlite3"}
INCLUDED_TOP_LEVEL = {
    "src",
    "web",
    "scripts",
    "tests",
    "seed_data",
    "题目",
    "README.md",
    "requirements.txt",
    "run.py",
    ".gitignore",
    "REPRODUCE.md",
}


def _should_include(path: Path, root: Path) -> bool:
    if not path.is_file():
        return False
    relative = path.relative_to(root)
    if relative.parts[0] not in INCLUDED_TOP_LEVEL:
        return False
    if any(part in EXCLUDED_PARTS for part in relative.parts):
        return False
    if path.suffix.lower() in EXCLUDED_SUFFIXES:
        return False
    return True


def create_clean_bundle(config: AppConfig, stem: str = "financial_qa_assistant") -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = config.package_dir / f"{stem}_{timestamp}.zip"
    if output_path.exists():
        output_path.unlink()

    manifest: dict[str, object] = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "entrypoint": "python run.py serve",
        "python": sys.version.split()[0],
        "files": [],
    }

    with zipfile.ZipFile(output_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(config.workspace_root.rglob("*")):
            if not _should_include(path, config.workspace_root):
                continue
            relative = path.relative_to(config.workspace_root).as_posix()
            archive.write(path, relative)
            manifest["files"].append(relative)
        archive.writestr("BUNDLE_MANIFEST.json", json.dumps(manifest, ensure_ascii=False, indent=2))
    return output_path
