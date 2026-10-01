from __future__ import annotations

import io
import logging
import zipfile
from pathlib import Path

logger = logging.getLogger("ingestion.files")

PROCESSABLE_EXTENSIONS = {".pdf", ".docx", ".txt", ".csv"}
JUNK_NAMES = {"__MACOSX", ".DS_Store", "Thumbs.db", "desktop.ini"}


def safe_extract_zip(zip_bytes: bytes, destination: Path) -> list[Path]:
    extracted: list[Path] = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as archive:
        for info in archive.infolist():
            if info.is_dir():
                continue

            name = info.filename
            path = Path(name)
            if ".." in path.parts or path.is_absolute() or name.startswith(("/", "\\")):
                continue
            if (info.external_attr >> 16) & 0o120000 == 0o120000:
                continue
            if path.name.startswith(".") or path.name in JUNK_NAMES or "__MACOSX" in path.parts:
                continue
            if path.suffix.lower() not in PROCESSABLE_EXTENSIONS:
                continue

            target = destination / "__expanded__" / path
            counter = 1
            while target.exists():
                target = target.with_name(f"{path.stem}_{counter}{path.suffix.lower()}")
                counter += 1

            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(archive.read(name))
            extracted.append(target)

    return extracted


def collect_processable_files(directory: Path) -> list[Path]:
    files: list[Path] = []
    archives: list[Path] = []

    for path in directory.rglob("*"):
        if not path.is_file() or path.name in JUNK_NAMES or path.name.startswith("."):
            continue
        # Skip already-expanded files so they aren't double-processed
        if "__expanded__" in path.parts:
            continue
        if path.suffix.lower() == ".zip":
            archives.append(path)
        elif path.suffix.lower() in PROCESSABLE_EXTENSIONS:
            files.append(path)

    for archive in archives:
        try:
            files.extend(safe_extract_zip(archive.read_bytes(), directory))
        except Exception:
            logger.exception("Failed to unpack archive: %s", archive.name)

    return files
