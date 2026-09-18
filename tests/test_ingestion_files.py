import io
import zipfile
from pathlib import Path

from app.domains.ingestion.files import collect_processable_files


def test_collect_processable_files_expands_supported_members(tmp_path: Path):
    archive = io.BytesIO()
    with zipfile.ZipFile(archive, "w") as zipped:
        zipped.writestr("nested/profile.txt", "candidate text")
        zipped.writestr("nested/ignored.exe", "ignored")
        zipped.writestr("../unsafe.txt", "unsafe")

    (tmp_path / "profiles.zip").write_bytes(archive.getvalue())

    files = collect_processable_files(tmp_path)

    assert [path.relative_to(tmp_path).as_posix() for path in files] == [
        "__expanded__/nested/profile.txt"
    ]
    assert files[0].read_text() == "candidate text"
