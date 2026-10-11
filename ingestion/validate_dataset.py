"""Strictly validate the nine registered Olist source CSV files."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
import csv
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path

from transformation.common.registry import BRONZE_METADATA_COLUMNS, TABLE_SPECS


class DatasetValidationError(ValueError):
    """Report that a staged Olist dataset violates its registry contract."""


@dataclass(frozen=True)
class DatasetFile:
    """Describe one validated source file."""

    table_name: str
    path: Path
    size_bytes: int
    columns: Sequence[str]
    sha256: str = ""


@dataclass(frozen=True)
class DatasetManifest:
    """Describe all validated source files in registry order."""

    files: Sequence[DatasetFile]


def _source_columns(table_name: str) -> tuple[str, ...]:
    metadata_names = {column.name for column in BRONZE_METADATA_COLUMNS}
    return tuple(
        column.name
        for column in TABLE_SPECS[table_name].columns
        if column.name not in metadata_names
    )


def _read_header(path: Path) -> tuple[str, ...]:
    with path.open("r", encoding="utf-8-sig", newline="") as source:
        header = next(csv.reader(source), None)
    if not header:
        raise DatasetValidationError(f"header CSV rỗng: {path.name}")
    return tuple(header)


def validate_dataset(raw_dir: Path) -> DatasetManifest:
    """Validate exact file membership, content, and headers for an Olist staging directory."""

    if not raw_dir.is_dir():
        raise DatasetValidationError(f"thư mục raw không tồn tại: {raw_dir}")

    specs_by_file = {spec.csv_file: spec for spec in TABLE_SPECS.values()}
    expected_names = set(specs_by_file)
    actual_names = {
        path.name
        for path in raw_dir.iterdir()
        if path.is_file() and path.suffix.lower() == ".csv"
    }
    missing = sorted(expected_names - actual_names)
    extra = sorted(actual_names - expected_names)
    membership_errors = []
    if missing:
        membership_errors.append(f"thiếu file CSV: {', '.join(missing)}")
    if extra:
        membership_errors.append(f"thừa file CSV: {', '.join(extra)}")
    if membership_errors:
        raise DatasetValidationError("; ".join(membership_errors))

    files = []
    for table_name, spec in TABLE_SPECS.items():
        path = raw_dir / spec.csv_file
        size_bytes = path.stat().st_size
        if size_bytes == 0:
            raise DatasetValidationError(f"file CSV rỗng: {path.name}")

        columns = _read_header(path)
        duplicates = sorted({name for name in columns if columns.count(name) > 1})
        if duplicates:
            raise DatasetValidationError(
                f"header CSV trùng lặp trong {path.name}: {', '.join(duplicates)}"
            )

        expected_columns = _source_columns(table_name)
        if columns != expected_columns:
            raise DatasetValidationError(
                f"header không khớp cho {path.name}: cần {list(expected_columns)}, "
                f"nhận được {list(columns)}"
            )

        files.append(
            DatasetFile(
                table_name=table_name,
                path=path,
                size_bytes=size_bytes,
                columns=columns,
            )
        )

    return DatasetManifest(files=tuple(files))


def _write_manifest(manifest: DatasetManifest, destination: Path) -> None:
    payload = {
        "files": [
            {
                "table_name": item.table_name,
                "path": item.path.name,
                "size_bytes": item.size_bytes,
                "sha256": item.sha256,
                "columns": list(item.columns),
            }
            for item in manifest.files
        ]
    }
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def main(argv: Sequence[str] | None = None) -> int:
    """Core: Validate staging directory without requiring CLI flags."""
    raw_dir_env = os.environ.get("RAW_DIR")
    if not raw_dir_env:
        for candidate in (Path("/opt/lakehouse/data/raw"), Path("data/raw")):
            if candidate.is_dir():
                raw_dir_env = str(candidate)
                break
    raw_dir = Path(raw_dir_env or "data/raw")

    manifest = validate_dataset(raw_dir)
    print(f"Validated {len(manifest.files)} Olist CSV files successfully in {raw_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


# ==============================================================================
# UNNECESSARY CLI PARSER & SHA256 (Commented out - moved to bottom for reference)
# ==============================================================================
# def _parser() -> argparse.ArgumentParser:
#     parser = argparse.ArgumentParser(
#         description="Validate Olist dataset files before uploading to MinIO."
#     )
#     parser.add_argument(
#         "--raw-dir", type=Path, required=True, help="Directory containing the nine Olist CSV files."
#     )
#     parser.add_argument(
#         "--write-manifest", type=Path, help="Optional output path to write validation manifest JSON."
#     )
#     return parser
#
# def _sha256(path: Path) -> str:
#     """Legacy chunk-based SHA-256 file hashing (Overkill for local lakehouse)."""
#     digest = hashlib.sha256()
#     with path.open("rb") as source:
#         for chunk in iter(lambda: source.read(1024 * 1024), b""):
#             digest.update(chunk)
#     return digest.hexdigest()
