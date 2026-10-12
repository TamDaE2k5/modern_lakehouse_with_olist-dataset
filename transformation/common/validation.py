"""Spark-expression validation primitives shared by Silver transformations."""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


_ERROR_CODE = re.compile(r"^[A-Z][A-Z0-9_]*$")


@dataclass(frozen=True)
class TransformResult:
    """Accepted and rejected views of one input DataFrame."""

    accepted: DataFrame
    rejected: DataFrame


@dataclass(frozen=True)
class ValidationRule:
    """Associate a stable error code with a Spark invalidity expression."""

    error_code: str
    invalid_when: Column

    def __post_init__(self) -> None:
        if not _ERROR_CODE.fullmatch(self.error_code):
            raise ValueError(f"Invalid validation error code: {self.error_code}")


def split_valid_records(
    df: DataFrame,
    rules: Sequence[ValidationRule],
) -> TransformResult:
    """Split every row into accepted and rejected (quarantine) using Spark SQL expressions."""
    if rules:
        raw_errors = F.array(
            *(F.when(rule.invalid_when, F.lit(rule.error_code)) for rule in rules)
        )
        error_codes = F.array_sort(F.filter(raw_errors, lambda value: value.isNotNull()))
    else:
        error_codes = F.array().cast("array<string>")

    classified = df.withColumn("_error_codes", error_codes)
    accepted = classified.where(F.size("_error_codes") == 0).drop("_error_codes")
    rejected = classified.where(F.size("_error_codes") > 0).withColumn(
        "_rejected_at", F.current_timestamp()
    )
    return TransformResult(accepted=accepted, rejected=rejected)
