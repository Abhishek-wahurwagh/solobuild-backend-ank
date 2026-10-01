from __future__ import annotations

import csv
import io
import re
import unicodedata


def normalize_text(text: str) -> str:
    text = unicodedata.normalize("NFKC", text)
    text = re.sub(r"[\x00-\x08\x0b-\x1f\x7f]", "", text)
    text = re.sub(r"\r\n?", "\n", text)
    text = re.sub(r"[ \t\f\v]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def extract_text_from_pdf(data: bytes) -> str:
    from pypdf import PdfReader

    reader = PdfReader(io.BytesIO(data))
    pages = [page.extract_text() or "" for page in reader.pages]
    return normalize_text("\n".join(pages))


def extract_text_from_docx(data: bytes) -> str:
    from docx import Document

    doc = Document(io.BytesIO(data))
    parts: list[str] = [paragraph.text for paragraph in doc.paragraphs]
    for table in doc.tables:
        for row in table.rows:
            parts.append(" | ".join(cell.text for cell in row.cells))
    return normalize_text("\n".join(parts))


def extract_text_from_txt(data: bytes) -> str:
    for encoding in ("utf-8", "latin-1", "cp1252"):
        try:
            return normalize_text(data.decode(encoding))
        except (UnicodeDecodeError, ValueError):
            continue
    return normalize_text(data.decode("utf-8", errors="replace"))


def extract_csv_rows(data: bytes) -> tuple[list[str], list[dict[str, str]]]:
    """Parse a CSV file and return (headers, rows).

    Tries UTF-8 first, then common fallback encodings.
    Returns a tuple of:
    - headers: list of column names (may be empty for header-less CSVs)
    - rows: list of dicts mapping header -> cell value
    """
    for encoding in ("utf-8-sig", "utf-8", "latin-1", "cp1252"):
        try:
            text = data.decode(encoding)
            break
        except (UnicodeDecodeError, ValueError):
            continue
    else:
        text = data.decode("utf-8", errors="replace")

    reader = csv.DictReader(io.StringIO(text))
    headers: list[str] = list(reader.fieldnames or [])
    rows: list[dict[str, str]] = []
    for row in reader:
        # Strip whitespace from keys and values
        cleaned = {k.strip(): (v.strip() if v else "") for k, v in row.items() if k}
        rows.append(cleaned)
    return headers, rows


def csv_rows_to_text(headers: list[str], rows: list[dict[str, str]]) -> str:
    """Serialise CSV rows back to a human-readable text block for LLM ingestion."""
    lines: list[str] = []
    for i, row in enumerate(rows, start=1):
        parts = [f"{k}: {v}" for k, v in row.items() if v]
        lines.append(f"--- Row {i} ---\n" + "\n".join(parts))
    return "\n\n".join(lines)


def extract_document_text(data: bytes, extension: str) -> str | None:
    try:
        if extension == ".pdf":
            return extract_text_from_pdf(data)
        if extension == ".docx":
            return extract_text_from_docx(data)
        if extension == ".txt":
            return extract_text_from_txt(data)
        if extension == ".csv":
            headers, rows = extract_csv_rows(data)
            return csv_rows_to_text(headers, rows)
    except Exception:
        return None
    return None


def validate_magic_bytes(data: bytes, extension: str) -> bool:
    if extension == ".pdf":
        return data[:5] == b"%PDF-"
    if extension == ".docx":
        return data[:4] == b"PK\x03\x04"
    if extension == ".txt":
        sample = data[:512]
        control = sum(
            1
            for byte in sample
            if byte < 0x09 or (0x0E <= byte <= 0x1F) or byte == 0x7F
        )
        return control <= 2
    if extension == ".csv":
        # CSV has no magic-byte signature; accept any non-empty file with
        # the .csv extension (encoding detection happens during extraction).
        return len(data) > 0
    return False
