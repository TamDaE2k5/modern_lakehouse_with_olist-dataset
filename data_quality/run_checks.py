"""Xác thực bộ kiểm định khai báo Great Expectations và ủy quyền kiểm tra Spark.

Mô-đun này kết hợp giữa:
1. Declarative Suite Contracts: Đọc các file JSON trong suites/ (bronze.json, silver.json, gold.json).
2. Schema Drift Detection: Phát hiện sớm bất kỳ sự sai lệch schema nào giữa file suite và mã nguồn code trước khi Spark chạy.
3. Great Expectations Integration: Khởi tạo FileDataContext từ gx/gx.yml, tạo ExpectationSuite, DataFrame Asset và chạy Validation.
4. Chuyển đổi kết quả: Ánh xạ kết quả GX sang cấu trúc QualityResult chuẩn để lưu trữ vào bảng Iceberg lakehouse.quality.quality_results.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
import re
import sys

from pyspark.sql import functions as F

from transformation.quality.checks import QualityResult, unavailable_result
from transformation.quality.job import SUPPORTED_LAYERS, declared_layer_columns, main


ROOT = Path(__file__).resolve().parent
_SAFE_NAME = re.compile(r"[^A-Za-z0-9_]+")


def _suite_documents() -> dict[str, dict]:
    """Tải nội dung 3 file hợp đồng suite JSON: bronze.json, silver.json, gold.json."""
    return {
        layer: json.loads(
            (ROOT / "suites" / f"{layer}.json").read_text(encoding="utf-8")
        )
        for layer in SUPPORTED_LAYERS
    }


def validate_suite_contracts() -> None:
    """Kiểm tra tính tương thích giữa file hợp đồng Suite JSON và mã nguồn code (Chống Schema Drift)."""
    known = declared_layer_columns()
    for layer, suite in _suite_documents().items():
        path = ROOT / "suites" / f"{layer}.json"
        if suite.get("great_expectations_version") != "1.21.0":
            raise ValueError(f"{path}: Phiên bản Great Expectations phải là 1.21.0")
        tables = suite.get("tables", {})
        if set(tables) != set(known[layer]):
            raise ValueError(f"{path}: Tập table không khớp với khai báo trong contract {layer}")
        for table_name, contract in tables.items():
            expectations = contract.get("expectations", ())
            if not any(
                item.get("expectation_type")
                == "expect_table_row_count_to_be_between"
                for item in expectations
            ):
                raise ValueError(f"{path}: Bảng {table_name} không có expectation về số lượng dòng")
            for expectation in expectations:
                severity = expectation.get("meta", {}).get("severity")
                if severity not in {"ERROR", "WARN", "INFO"}:
                    raise ValueError(
                        f"{path}: Expectation của {table_name} có mức severity không hợp lệ: {severity}"
                    )
                kwargs = expectation.get("kwargs", {})
                columns = set(kwargs.get("column_list", ()))
                if "column" in kwargs:
                    columns.add(kwargs["column"])
                if not columns <= known[layer][table_name]:
                    raise ValueError(
                        f"{path}: Expectation của {table_name} tham chiếu cột không xác định: "
                        f"{columns - known[layer][table_name]}"
                    )


def load_gx_context():
    """Tải Great Expectations FileDataContext từ file cấu hình gx.yml."""
    import great_expectations as gx
    from great_expectations.data_context.types.base import DataContextConfig
    from ruamel.yaml import YAML

    config_path = ROOT / "gx" / "gx.yml"
    with config_path.open(encoding="utf-8") as stream:
        config = DataContextConfig(**YAML(typ="safe").load(stream))
    runtime_root = ROOT.parent / "runtime" / "gx"
    runtime_root.mkdir(parents=True, exist_ok=True)
    return gx.get_context(
        project_config=config,
        context_root_dir=str(runtime_root),
    )


def build_gx_suites() -> dict[str, object]:
    """Khởi tạo các đối tượng ExpectationSuite có thể thực thi từ hợp đồng JSON."""
    import great_expectations as gx
    from great_expectations.expectations.expectation_configuration import (
        ExpectationConfiguration,
    )

    suites: dict[str, object] = {}
    for layer, document in _suite_documents().items():
        for table_name, contract in document["tables"].items():
            expectations = [
                ExpectationConfiguration(
                    type=item["expectation_type"],
                    kwargs=item["kwargs"],
                    meta=item["meta"],
                )
                for item in contract["expectations"]
            ]
            name = f"{layer}_{table_name}_quality"
            suites[name] = gx.ExpectationSuite(
                name=name,
                expectations=expectations,
                meta={"source_contract": document["suite_name"]},
            )
    return suites


def _safe_name(value: str) -> str:
    return _SAFE_NAME.sub("_", value).strip("_")


def _render(value: object) -> str:
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, sort_keys=True, default=str)
    return str(value)


def run_gx_table_validation(
    *,
    context,
    frame,
    layer: str,
    table_name: str,
    run_id: str,
) -> tuple[QualityResult, ...]:
    """Thực thi một bộ kiểm tra GX trên một DataFrame Spark và ánh xạ kết quả sang QualityResult."""
    import great_expectations as gx
    from great_expectations.core.run_identifier import RunIdentifier
    from great_expectations.expectations.expectation_configuration import (
        ExpectationConfiguration,
    )

    document = _suite_documents()[layer]
    contract = document["tables"][table_name]
    suite_name = f"{layer}_{table_name}_quality"
    suite = gx.ExpectationSuite(
        name=suite_name,
        expectations=[
            ExpectationConfiguration(
                type=item["expectation_type"],
                kwargs=item["kwargs"],
                meta=item["meta"],
            )
            for item in contract["expectations"]
        ],
        meta={"source_contract": document["suite_name"]},
    )
    suite = context.suites.add_or_update(suite)
    base_name = f"{_safe_name(run_id)}_{layer}_{table_name}"
    datasource = context.data_sources.add_spark(
        name=f"datasource_{base_name}",
        persist=True,
    )
    asset = datasource.add_dataframe_asset(name=f"asset_{base_name}")
    batch_definition = asset.add_batch_definition_whole_dataframe(
        name=f"batch_{base_name}"
    )
    definition_name = base_name if base_name.startswith("gx_") else f"gx_{base_name}"
    definition = context.validation_definitions.add_or_update(
        gx.ValidationDefinition(
            name=definition_name,
            data=batch_definition,
            suite=suite,
        )
    )
    validation = definition.run(
        batch_parameters={"dataframe": frame},
        run_id=RunIdentifier(run_name=run_id, run_time=datetime.now(timezone.utc)),
    )
    checked_at = datetime.now(timezone.utc)
    mapped: list[QualityResult] = []
    for result in validation.results:
        config = result.expectation_config
        severity = str(config.meta["severity"]).upper()
        kwargs = {
            key: value for key, value in config.kwargs.items() if key != "batch_id"
        }
        observed = result.result.get(
            "observed_value", result.result.get("unexpected_count", "unavailable")
        )
        mapped.append(
            QualityResult(
                layer=layer,
                table_name=table_name,
                rule_name=f"gx_{config.type}",
                severity=severity,
                observed_value=_render(observed),
                threshold=_render(kwargs),
                status="PASS" if result.success else severity,
                run_id=run_id,
                checked_at=checked_at,
            )
        )
    return tuple(mapped)


def run_gx_layer_validations(
    *,
    context,
    spark,
    settings,
    layer: str,
    namespace: str,
    run_id: str,
    batch_id: str | None = None,
) -> tuple[QualityResult, ...]:
    """Thực thi toàn bộ các bài kiểm tra GX cho tất cả các bảng trong một tầng dữ liệu."""
    results: list[QualityResult] = []
    tables = _suite_documents()[layer]["tables"]
    for table_name in tables:
        target = f"{settings.iceberg_catalog_name}.{namespace}.{table_name}"
        try:
            frame = spark.table(target)
            if layer == "bronze" and batch_id is not None:
                frame = frame.where(F.col("_batch_id") == batch_id)
            results.extend(
                run_gx_table_validation(
                    context=context,
                    frame=frame,
                    layer=layer,
                    table_name=table_name,
                    run_id=run_id,
                )
            )
        except Exception as exc:
            results.append(
                unavailable_result(
                    layer=layer,
                    table_name=table_name,
                    detail=f"gx:{type(exc).__name__}",
                    run_id=run_id,
                )
            )
    return tuple(results)


if __name__ == "__main__":
    validate_suite_contracts()
    load_gx_context()
    build_gx_suites()
    raise SystemExit(main(sys.argv[1:]))
