from __future__ import annotations

import io
import logging
import shutil
import zipfile
from pathlib import Path, PurePosixPath

from app.core.config import settings

logger = logging.getLogger("ingestion.files")

PROCESSABLE_EXTENSIONS = {".pdf", ".docx", ".txt", ".csv"}
JUNK_NAMES = {"__MACOSX", ".DS_Store", "Thumbs.db", "desktop.ini"}


def safe_extract_zip(
    zip_source: bytes | Path,
    destination: Path,
    *,
    max_file_count: int | None = None,
    max_uncompressed_bytes: int | None = None,
    max_ratio: int | None = None,
) -> list[Path]:
    max_file_count = (
        settings.RESUME_MAX_FILE_COUNT if max_file_count is None else max_file_count
    )
    max_uncompressed_bytes = (
        settings.ZIP_MAX_UNCOMPRESSED_BYTES
        if max_uncompressed_bytes is None
        else max_uncompressed_bytes
    )
    max_ratio = settings.ZIP_MAX_RATIO if max_ratio is None else max_ratio
    extracted: list[Path] = []
    archive_source = io.BytesIO(zip_source) if isinstance(zip_source, bytes) else zip_source
    with zipfile.ZipFile(archive_source) as archive:
        infos = archive.infolist()
        if len(infos) > max_file_count:
            raise ValueError(
                f"ZIP contains {len(infos)} entries (maximum {max_file_count})."
            )

        total_uncompressed = sum(info.file_size for info in infos)
        total_compressed = sum(info.compress_size for info in infos)
        if total_uncompressed > max_uncompressed_bytes:
            raise ValueError("ZIP exceeds the maximum uncompressed size.")
        if (
            total_uncompressed
            and (
                total_compressed == 0
                or total_uncompressed / total_compressed > max_ratio
            )
        ):
            raise ValueError("ZIP compression ratio exceeds the allowed maximum.")

        expanded_root = (destination / "__expanded__").resolve()
        actual_uncompressed = 0
        for info in infos:
            if info.is_dir():
                continue

            name = info.filename
            path = PurePosixPath(name.replace("\\", "/"))
            if (
                ".." in path.parts
                or path.is_absolute()
                or (path.parts and path.parts[0].endswith(":"))
                or name.startswith(("/", "\\"))
            ):
                continue
            if (info.external_attr >> 16) & 0o120000 == 0o120000:
                continue
            if path.name.startswith(".") or path.name in JUNK_NAMES or "__MACOSX" in path.parts:
                continue
            if path.suffix.lower() not in PROCESSABLE_EXTENSIONS:
                continue

            target = expanded_root.joinpath(*path.parts)
            if not target.resolve().is_relative_to(expanded_root):
                continue
            counter = 1
            while target.exists():
                target = target.with_name(f"{path.stem}_{counter}{path.suffix.lower()}")
                counter += 1

            target.parent.mkdir(parents=True, exist_ok=True)
            written = 0
            with archive.open(info) as source, target.open("wb") as output:
                while chunk := source.read(1024 * 1024):
                    written += len(chunk)
                    actual_uncompressed += len(chunk)
                    if written > info.file_size:
                        target.unlink(missing_ok=True)
                        raise ValueError(f"ZIP entry {name!r} exceeded its declared size.")
                    if actual_uncompressed > max_uncompressed_bytes:
                        target.unlink(missing_ok=True)
                        raise ValueError("ZIP exceeds the maximum uncompressed size.")
                    output.write(chunk)
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
