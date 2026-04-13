from __future__ import annotations

import re
import zlib
from pathlib import Path
from typing import Any

from .utils import normalize_text


_TEXT_PATTERN = re.compile(
    rb"/([A-Za-z0-9_]+)\s+[-0-9.]+\s+Tf|"
    rb"\[(.*?)\]\s*TJ|"
    rb"<([0-9A-Fa-f]+)>\s*Tj|"
    rb"\((.*?)\)\s*Tj|"
    rb"T\*|"
    rb"([-0-9.]+)\s+([-0-9.]+)\s+TD|"
    rb"([-0-9.]+)\s+([-0-9.]+)\s+Td",
    re.S,
)


def _parse_objects(data: bytes) -> dict[int, bytes]:
    objects = {
        int(match.group(1)): match.group(2)
        for match in re.finditer(rb"(\d+)\s+0\s+obj(.*?)endobj", data, re.S)
    }
    for _, body in list(objects.items()):
        if b"/Type/ObjStm" not in body and b"/Type /ObjStm" not in body:
            continue
        first_match = re.search(rb"/First\s+(\d+)", body)
        count_match = re.search(rb"/N\s+(\d+)", body)
        stream_match = re.search(rb"stream\r?\n(.*)\r?\nendstream", body, re.S)
        if not first_match or not count_match or not stream_match:
            continue
        first = int(first_match.group(1))
        count = int(count_match.group(1))
        decoded = zlib.decompress(stream_match.group(1))
        numbers = list(map(int, decoded[:first].decode("latin1", "ignore").strip().split()))
        pairs = [(numbers[index], numbers[index + 1]) for index in range(0, len(numbers), 2)]
        for index, (obj_num, offset) in enumerate(pairs[:count]):
            end = pairs[index + 1][1] if index + 1 < len(pairs) else len(decoded) - first
            objects[obj_num] = decoded[first + offset : first + end]
    return objects


def _stream_bytes(body: bytes) -> bytes | None:
    match = re.search(rb"stream\r?\n(.*)\r?\nendstream", body, re.S)
    if not match:
        return None
    raw = match.group(1)
    try:
        return zlib.decompress(raw)
    except zlib.error:
        return raw


def _parse_cmap(cmap_stream: bytes | None) -> dict[bytes, str]:
    if not cmap_stream:
        return {}
    text = cmap_stream.decode("latin1", "ignore")
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    mapping: dict[bytes, str] = {}
    index = 0
    while index < len(lines):
        match = re.match(r"(\d+)\s+beginbfchar", lines[index])
        if match:
            count = int(match.group(1))
            index += 1
            for _ in range(count):
                if index >= len(lines):
                    break
                pair = re.match(r"<([0-9A-Fa-f]+)>\s*<([0-9A-Fa-f]+)>", lines[index])
                if pair:
                    source, target = pair.groups()
                    try:
                        mapping[bytes.fromhex(source)] = (
                            bytes.fromhex(target).decode("utf-16-be")
                            if len(target) % 4 == 0
                            else chr(int(target, 16))
                        )
                    except (ValueError, UnicodeDecodeError):
                        pass
                index += 1
            continue

        match = re.match(r"(\d+)\s+beginbfrange", lines[index])
        if match:
            count = int(match.group(1))
            index += 1
            for _ in range(count):
                if index >= len(lines):
                    break
                line = lines[index]
                triplet = re.match(
                    r"<([0-9A-Fa-f]+)>\s*<([0-9A-Fa-f]+)>\s*<([0-9A-Fa-f]+)>",
                    line,
                )
                if triplet:
                    start_hex, end_hex, target_hex = triplet.groups()
                    start = int(start_hex, 16)
                    end = int(end_hex, 16)
                    target = int(target_hex, 16)
                    width = len(start_hex) // 2
                    for offset, code in enumerate(range(start, end + 1)):
                        mapping[code.to_bytes(width, "big")] = chr(target + offset)
                else:
                    array_match = re.match(
                        r"<([0-9A-Fa-f]+)>\s*<([0-9A-Fa-f]+)>\s*\[(.+)\]",
                        line,
                    )
                    if array_match:
                        start_hex, end_hex, array_text = array_match.groups()
                        start = int(start_hex, 16)
                        end = int(end_hex, 16)
                        width = len(start_hex) // 2
                        values = re.findall(r"<([0-9A-Fa-f]+)>", array_text)
                        for offset, code in enumerate(range(start, end + 1)):
                            if offset >= len(values):
                                break
                            value = values[offset]
                            try:
                                mapping[code.to_bytes(width, "big")] = (
                                    bytes.fromhex(value).decode("utf-16-be")
                                    if len(value) % 4 == 0
                                    else chr(int(value, 16))
                                )
                            except (ValueError, UnicodeDecodeError):
                                pass
                index += 1
            continue
        index += 1
    return mapping


def _decode_hex_string(value: bytes, cmap: dict[bytes, str]) -> str:
    raw = bytes.fromhex(value.decode("ascii", "ignore"))
    if not cmap:
        return raw.decode("latin1", "ignore")
    sizes = sorted({len(key) for key in cmap}, reverse=True)
    text_parts: list[str] = []
    cursor = 0
    while cursor < len(raw):
        for size in sizes:
            chunk = raw[cursor : cursor + size]
            if chunk in cmap:
                text_parts.append(cmap[chunk])
                cursor += size
                break
        else:
            text_parts.append(raw[cursor : cursor + 1].decode("latin1", "ignore"))
            cursor += 1
    return "".join(text_parts)


def _extract_ref_map(body: bytes, key: str) -> dict[str, int]:
    pattern = re.compile(fr"/{key}\s*<<(.*?)>>".encode("ascii"), re.S)
    match = pattern.search(body)
    if not match:
        return {}
    refs: dict[str, int] = {}
    for name, ref in re.findall(rb"/([A-Za-z0-9_]+)\s+(\d+)\s+0\s+R", match.group(1)):
        refs[name.decode("latin1")] = int(ref)
    return refs


def _build_font_maps(objects: dict[int, bytes]) -> dict[int, dict[bytes, str]]:
    font_maps: dict[int, dict[bytes, str]] = {}
    for obj_num, body in objects.items():
        if not re.search(rb"/Type\s*/Font\b", body):
            continue
        match = re.search(rb"/ToUnicode\s+(\d+)\s+0\s+R", body)
        if not match:
            continue
        ref = int(match.group(1))
        if ref not in objects:
            continue
        font_maps[obj_num] = _parse_cmap(_stream_bytes(objects[ref]))
    return font_maps


def _decode_content_stream(content: bytes | None, font_refs: dict[str, int], font_maps: dict[int, dict[bytes, str]]) -> list[str]:
    if not content:
        return []
    parts: list[str] = []
    current_font: str | None = None
    for match in _TEXT_PATTERN.finditer(content):
        font_name, text_array, hex_text, literal_text, td_y_1, td_y_2, td_x, td_y_3 = match.groups()
        if font_name is not None:
            current_font = font_name.decode("latin1")
            continue
        if text_array is not None or hex_text is not None or literal_text is not None:
            cmap = font_maps.get(font_refs.get(current_font or "", -1), {})
            segment = ""
            if text_array is not None:
                for inner in re.finditer(rb"<([0-9A-Fa-f]+)>|\((.*?)\)", text_array, re.S):
                    if inner.group(1) is not None:
                        segment += _decode_hex_string(inner.group(1), cmap)
                    else:
                        segment += inner.group(2).decode("latin1", "ignore")
            elif hex_text is not None:
                segment = _decode_hex_string(hex_text, cmap)
            else:
                segment = literal_text.decode("latin1", "ignore")
            if segment:
                parts.append(segment)
        else:
            raw = match.group(0)
            if b"T*" in raw:
                parts.append("\n")
            else:
                try:
                    delta_y = float((td_y_1 or td_y_3).decode("latin1"))
                    if delta_y < -1:
                        parts.append("\n")
                except (AttributeError, ValueError):
                    pass
    return parts


def _extract_form_stream(
    ref: int,
    objects: dict[int, bytes],
    font_maps: dict[int, dict[bytes, str]],
    inherited_fonts: dict[str, int],
    visited: set[int],
) -> list[str]:
    if ref in visited:
        return []
    visited.add(ref)
    body = objects.get(ref, b"")
    if not body:
        return []
    font_refs = dict(inherited_fonts)
    font_refs.update(_extract_ref_map(body, "Font"))
    xobject_refs = _extract_ref_map(body, "XObject")
    parts = _decode_content_stream(_stream_bytes(body), font_refs, font_maps)
    for child_ref in xobject_refs.values():
        child_body = objects.get(child_ref, b"")
        if re.search(rb"/Subtype\s*/Form\b", child_body):
            parts.extend(_extract_form_stream(child_ref, objects, font_maps, font_refs, visited))
    return parts


def _clean_extracted_text(text: str) -> str:
    text = text.replace("\x00", " ")
    text = "".join(char if char.isprintable() or char in "\n\t" else " " for char in text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _is_preferred_char(char: str) -> bool:
    code = ord(char)
    if char in "\r\n\t ":
        return True
    if char.isascii() and char.isprintable():
        return True
    if 0x4E00 <= code <= 0x9FFF:
        return True
    if 0x3000 <= code <= 0x303F:
        return True
    return False


def _is_noisy(text: str) -> bool:
    meaningful = [char for char in text if char.strip()]
    if not meaningful:
        return True
    weird = sum(1 for char in meaningful if not _is_preferred_char(char))
    return weird / len(meaningful) > 0.25


def extract_text(path: Path) -> str:
    data = path.read_bytes()
    objects = _parse_objects(data)
    font_maps = _build_font_maps(objects)
    parts: list[str] = []
    for _, body in sorted(objects.items()):
        if not re.search(rb"/Type\s*/Page\b", body) or b"/Parent" not in body:
            continue
        font_refs = _extract_ref_map(body, "Font")
        xobject_refs = _extract_ref_map(body, "XObject")
        contents_match = re.search(rb"/Contents\s*\[(.*?)\]", body, re.S)
        content_refs = (
            [int(item) for item in re.findall(rb"(\d+)\s+0\s+R", contents_match.group(1))]
            if contents_match
            else []
        )
        if not content_refs:
            single_match = re.search(rb"/Contents\s+(\d+)\s+0\s+R", body)
            if single_match:
                content_refs = [int(single_match.group(1))]

        for ref in content_refs:
            parts.extend(_decode_content_stream(_stream_bytes(objects.get(ref, b"")), font_refs, font_maps))

        visited: set[int] = set()
        for ref in xobject_refs.values():
            child_body = objects.get(ref, b"")
            if re.search(rb"/Subtype\s*/Form\b", child_body):
                parts.extend(_extract_form_stream(ref, objects, font_maps, font_refs, visited))

    text = _clean_extracted_text(" ".join(part if part != "\n" else "\n" for part in parts))
    if len(text) < 20:
        return path.stem
    if _is_noisy(text) and len(text) < 200:
        return path.stem
    return normalize_text(text)


def extract_text_safe(path: Path) -> str:
    try:
        return extract_text(path)
    except Exception:
        return path.stem


def infer_pdf_metadata(path: Path, text: str = "") -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "title": path.stem,
        "file_path": str(path),
        "stock_code": "",
        "stock_name": "",
        "report_period": "",
        "report_date": "",
    }
    stem = path.stem
    code_date_match = re.search(r"(?P<code>\d{6})_(?P<date>\d{8})", stem)
    if code_date_match:
        metadata["stock_code"] = code_date_match.group("code")
        raw_date = code_date_match.group("date")
        metadata["report_date"] = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}"

    cn_match = re.match(r"(?P<name>[^：:]+)[：:](?P<title>.+)", stem)
    if cn_match:
        metadata["stock_name"] = cn_match.group("name").strip()
        metadata["title"] = cn_match.group("title").strip()

    head = re.sub(r"\s+", "", text[:800])
    if not metadata["stock_code"]:
        match = re.search(r"证券代码[:：]?(\d{6})", head)
        if match:
            metadata["stock_code"] = match.group(1)
    if not metadata["stock_name"]:
        match = re.search(r"证券简称[:：]?([A-Za-z0-9\u4e00-\u9fff]+)", head)
        if match:
            metadata["stock_name"] = match.group(1)

    title_space = re.sub(r"\s+", "", metadata["title"] or stem)
    title_probe = title_space + head[:200]
    period_match = re.search(r"(20\d{2})年", title_probe)
    year = period_match.group(1) if period_match else ""
    if year:
        if "第一季度报告" in title_probe or "一季度报告" in title_probe:
            metadata["report_period"] = f"{year}Q1"
        elif "第二季度报告" in title_probe or "二季度报告" in title_probe:
            metadata["report_period"] = f"{year}Q2"
        elif "第三季度报告" in title_probe or "三季度报告" in title_probe:
            metadata["report_period"] = f"{year}Q3"
        elif "半年度报告" in title_probe or "半年度报告摘要" in title_probe or "半年度" in title_probe:
            metadata["report_period"] = f"{year}HY"
        elif "年度报告" in title_probe:
            metadata["report_period"] = f"{year}FY"
    return metadata


def chunk_text(text: str, max_chars: int = 500, overlap: int = 80) -> list[str]:
    text = normalize_text(text)
    if not text:
        return []
    if len(text) <= max_chars:
        return [text]
    chunks: list[str] = []
    start = 0
    while start < len(text):
        end = min(len(text), start + max_chars)
        chunks.append(text[start:end])
        if end == len(text):
            break
        start = max(0, end - overlap)
    return chunks
