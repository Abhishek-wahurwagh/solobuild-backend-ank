from __future__ import annotations

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


def extract_document_text(data: bytes, extension: str) -> str | None:
    try:
        if extension == ".pdf":
            return extract_text_from_pdf(data)
        if extension == ".docx":
            return extract_text_from_docx(data)
        if extension == ".txt":
            return extract_text_from_txt(data)
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
    return False
