"""Upload a validated Olist dataset to the MinIO raw zone idempotently."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from dataclasses import dataclass
import os
from pathlib import Path
from typing import Any
import unicodedata

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

from ingestion.validate_dataset import validate_dataset


@dataclass(frozen=True)
class UploadSummary:
    """Count upload outcomes for one validated directory."""

    uploaded: int
    skipped: int
    replaced: int


def _stored_object(s3_client: Any, bucket: str, key: str) -> tuple[bool, int | None]:
    """Check if object already exists in S3 and return its size in bytes."""
    try:
        response = s3_client.head_object(Bucket=bucket, Key=key)
        return True, response.get("ContentLength")
    except ClientError as exc:
        error_code = str(exc.response.get("Error", {}).get("Code", ""))
        if error_code in {"404", "NoSuchKey", "NotFound"}:
            return False, None
        raise


def _validated_prefix(prefix: str) -> str:
    components = prefix.split("/")
    contains_control = any(unicodedata.category(character) == "Cc" for character in prefix)
    contains_whitespace = any(character.isspace() for character in prefix)
    if (
        "\\" in prefix
        or contains_control
        or contains_whitespace
        or any(not component or component in {".", ".."} for component in components)
    ):
        raise ValueError(
            "prefix không an toàn: hãy dùng các thành phần object name không rỗng, "
            "phân tách bằng dấu gạch chéo"
        )
    return prefix


def upload_directory(
    raw_dir: Path,
    bucket: str,
    prefix: str,
    s3_client: Any,
) -> UploadSummary:
    """Upload all validated CSVs, skipping existing identical-size files."""

    if not bucket:
        raise ValueError("bucket không được để trống")
    validated_prefix = _validated_prefix(prefix)
    manifest = validate_dataset(raw_dir)

    uploaded = 0
    skipped = 0
    replaced = 0
    for item in manifest.files:
        key = f"{validated_prefix}/{item.path.name}"
        object_exists, stored_size = _stored_object(s3_client, bucket, key)
        if object_exists and stored_size == item.size_bytes:
            skipped += 1
            continue

        s3_client.upload_file(str(item.path), bucket, key)
        if object_exists:
            replaced += 1
        else:
            uploaded += 1

    return UploadSummary(uploaded=uploaded, skipped=skipped, replaced=replaced)


def _build_s3_client() -> Any:
    load_dotenv()
    access_key = os.environ.get("S3_ACCESS_KEY") or os.environ.get("MINIO_UPLOAD_USER")
    secret_key = os.environ.get("S3_SECRET_KEY") or os.environ.get(
        "MINIO_UPLOAD_PASSWORD"
    )
    if not access_key or not secret_key:
        raise ValueError(
            "S3_ACCESS_KEY/S3_SECRET_KEY or MINIO_UPLOAD_USER/MINIO_UPLOAD_PASSWORD "
            "phải được thiết lập"
        )

    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_UPLOAD_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=os.environ.get("S3_REGION", "us-east-1"),
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Core: Upload validated local Olist dataset to MinIO without requiring CLI flags."""
    raw_dir_env = os.environ.get("RAW_DIR")
    if not raw_dir_env:
        for candidate in (Path("/opt/lakehouse/data/raw"), Path("data/raw")):
            if candidate.is_dir():
                raw_dir_env = str(candidate)
                break
    raw_dir = Path(raw_dir_env or "data/raw")
    bucket = os.environ.get("RAW_BUCKET", "raw")
    prefix = os.environ.get("RAW_PREFIX", "olist")

    summary = upload_directory(
        raw_dir=raw_dir,
        bucket=bucket,
        prefix=prefix,
        s3_client=_build_s3_client(),
    )
    print(
        f"Raw zone upload complete: uploaded={summary.uploaded}, "
        f"skipped={summary.skipped}, replaced={summary.replaced}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


# ==============================================================================
# UNNECESSARY CLI PARSER & SHA-256 METADATA (Commented out - moved to bottom for reference)
# ==============================================================================
# def _parser() -> argparse.ArgumentParser:
#     parser = argparse.ArgumentParser(
#         description="Upload validated Olist dataset files to MinIO raw zone."
#     )
#     parser.add_argument(
#         "--raw-dir", type=Path, required=True, help="Directory containing the validated Olist dataset."
#     )
#     parser.add_argument("--bucket", required=True, help="Target MinIO bucket.")
#     parser.add_argument("--prefix", required=True, help="Target object prefix in bucket.")
#     return parser
#
# # Legacy: Storing custom SHA-256 metadata and chunk-by-chunk hash comparison
# # s3_client.upload_file(str(item.path), bucket, key, ExtraArgs={"Metadata": {"sha256": item.sha256}})
# # stored_checksum = response.get("Metadata", {}).get("sha256")
