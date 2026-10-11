"""Immutable metadata models used by the table registry."""

from collections.abc import Sequence
from dataclasses import dataclass


@dataclass(frozen=True)
class ColumnSpec:
    """Describe one source or technical table column."""

    name: str
    spark_type: str
    nullable: bool


@dataclass(frozen=True)
class QualityRuleSpec:
    """Describe one registry-driven table quality rule."""

    name: str
    rule_type: str
    severity: str


@dataclass(frozen=True)
class TableSpec:
    """Describe the Bronze contract for one source table."""

    name: str
    csv_file: str
    grain: str
    key: Sequence[str]
    columns: Sequence[ColumnSpec]
    quality_rules: Sequence[QualityRuleSpec]
    bronze_partitions: Sequence[str] = ("days(_ingestion_timestamp)",)
