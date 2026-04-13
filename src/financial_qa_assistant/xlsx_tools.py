from __future__ import annotations

import html
import re
import zipfile
from pathlib import Path
from xml.etree import ElementTree as ET


NS = {
    "a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
}


def _col_to_number(column: str) -> int:
    value = 0
    for char in column:
        if char.isalpha():
            value = value * 26 + ord(char.upper()) - 64
    return value


def _number_to_col(value: int) -> str:
    chars: list[str] = []
    while value:
        value, remainder = divmod(value - 1, 26)
        chars.append(chr(65 + remainder))
    return "".join(reversed(chars)) or "A"


def read_workbook(path: Path) -> dict[str, list[list[str]]]:
    with zipfile.ZipFile(path) as archive:
        shared_strings: list[str] = []
        if "xl/sharedStrings.xml" in archive.namelist():
            shared_root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
            for item in shared_root.findall("a:si", NS):
                shared_strings.append("".join(node.text or "" for node in item.iterfind(".//a:t", NS)))

        workbook_root = ET.fromstring(archive.read("xl/workbook.xml"))
        rel_root = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
        relationships = {
            rel.attrib["Id"]: rel.attrib["Target"]
            for rel in rel_root
            if "Id" in rel.attrib and "Target" in rel.attrib
        }

        sheets: dict[str, list[list[str]]] = {}
        sheets_node = workbook_root.find("a:sheets", NS)
        for sheet in sheets_node if sheets_node is not None else []:
            name = sheet.attrib["name"]
            rel_id = sheet.attrib["{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"]
            target = "xl/" + relationships[rel_id]
            worksheet_root = ET.fromstring(archive.read(target))
            rows: list[list[str]] = []
            for row in worksheet_root.findall(".//a:sheetData/a:row", NS):
                values: list[str] = []
                current_col = 1
                for cell in row.findall("a:c", NS):
                    reference = cell.attrib.get("r", "A1")
                    match = re.match(r"([A-Z]+)", reference)
                    target_col = _col_to_number(match.group(1)) if match else current_col
                    while current_col < target_col:
                        values.append("")
                        current_col += 1

                    cell_type = cell.attrib.get("t")
                    value_node = cell.find("a:v", NS)
                    text = ""
                    if cell_type == "s" and value_node is not None:
                        text = shared_strings[int(value_node.text or "0")]
                    elif cell_type == "inlineStr":
                        text = "".join(node.text or "" for node in cell.iterfind(".//a:t", NS))
                    elif value_node is not None:
                        text = value_node.text or ""
                    values.append(text)
                    current_col += 1
                rows.append(values)
            sheets[name] = rows
        return sheets


def rows_to_dicts(rows: list[list[str]]) -> list[dict[str, str]]:
    if not rows:
        return []
    header = [str(cell).strip() for cell in rows[0]]
    records: list[dict[str, str]] = []
    for row in rows[1:]:
        record = {header[index]: row[index] if index < len(row) else "" for index in range(len(header))}
        records.append(record)
    return records


def _cell_xml(reference: str, value: object) -> str:
    if value is None:
        return f'<c r="{reference}" t="inlineStr"><is><t></t></is></c>'

    text = str(value)
    if re.fullmatch(r"-?\d+(?:\.\d+)?", text):
        return f'<c r="{reference}"><v>{text}</v></c>'
    escaped = html.escape(text)
    return f'<c r="{reference}" t="inlineStr"><is><t xml:space="preserve">{escaped}</t></is></c>'


def write_simple_xlsx(path: Path, sheet_name: str, rows: list[list[object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    max_col = max((len(row) for row in rows), default=1)
    max_row = max(len(rows), 1)
    dimension = f"A1:{_number_to_col(max_col)}{max_row}"
    row_xml: list[str] = []
    for row_index, row in enumerate(rows, start=1):
        cells = []
        for col_index, value in enumerate(row, start=1):
            reference = f"{_number_to_col(col_index)}{row_index}"
            cells.append(_cell_xml(reference, value))
        row_xml.append(f'<row r="{row_index}">' + "".join(cells) + "</row>")

    worksheet_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f'<dimension ref="{dimension}"/>'
        "<sheetViews><sheetView workbookViewId=\"0\"/></sheetViews>"
        "<sheetFormatPr defaultRowHeight=\"15\"/>"
        "<sheetData>"
        + "".join(row_xml)
        + "</sheetData>"
        "</worksheet>"
    )
    workbook_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        "<sheets>"
        f'<sheet name="{html.escape(sheet_name)}" sheetId="1" r:id="rId1"/>'
        "</sheets>"
        "</workbook>"
    )
    workbook_rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
        'Target="worksheets/sheet1.xml"/>'
        '<Relationship Id="rId2" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" '
        'Target="styles.xml"/>'
        "</Relationships>"
    )
    root_rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
        'Target="xl/workbook.xml"/>'
        "</Relationships>"
    )
    content_types = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/xl/workbook.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
        '<Override PartName="/xl/worksheets/sheet1.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        '<Override PartName="/xl/styles.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>'
        "</Types>"
    )
    styles_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        "<fonts count=\"1\"><font><sz val=\"11\"/><name val=\"Calibri\"/></font></fonts>"
        "<fills count=\"1\"><fill><patternFill patternType=\"none\"/></fill></fills>"
        "<borders count=\"1\"><border/></borders>"
        "<cellStyleXfs count=\"1\"><xf/></cellStyleXfs>"
        "<cellXfs count=\"1\"><xf xfId=\"0\"/></cellXfs>"
        "</styleSheet>"
    )
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("[Content_Types].xml", content_types)
        archive.writestr("_rels/.rels", root_rels)
        archive.writestr("xl/workbook.xml", workbook_xml)
        archive.writestr("xl/_rels/workbook.xml.rels", workbook_rels)
        archive.writestr("xl/styles.xml", styles_xml)
        archive.writestr("xl/worksheets/sheet1.xml", worksheet_xml)
