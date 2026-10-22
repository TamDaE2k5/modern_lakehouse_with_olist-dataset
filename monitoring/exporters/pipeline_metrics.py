"""Export bounded Olist pipeline audit metrics from Trino."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
import json
import math
import os
import re
import signal
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen


Row = Mapping[str, Any]
Rows = tuple[Row, ...]
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_JOB_LABELS = {
    "bronze_ingestion": "bronze_ingestion",
    "silver_transformation": "silver_transformation",
    "gold_transformation": "gold_transformation",
    "quality_checks": "quality_checks",
    "iceberg_maintenance": "iceberg_maintenance",
}
_LAYER_LABELS = {
    "bronze": "bronze",
    "silver": "silver",
    "gold": "gold",
    "quality": "quality",
    "maintenance": "maintenance",
}
_STATUS_LABELS = {
    "STARTED": "STARTED",
    "SUCCESS": "SUCCESS",
    "FAILED": "FAILED",
    "UNKNOWN": "UNKNOWN",
}
_QUALITY_SEVERITY_LABELS = {value: value for value in ("INFO", "WARN", "ERROR")}
_QUALITY_STATUS_LABELS = {value: value for value in ("PASS", "INFO", "WARN", "ERROR")}
_MAINTENANCE_ACTION_LABELS = {
    "rewrite-data-files": "rewrite-data-files",
    "expire-snapshots": "expire-snapshots",
    "remove-orphan-files": "remove-orphan-files",
    "dry-run": "dry-run",
}


class TrinoError(RuntimeError):
    """Raised when a Trino statement cannot produce a complete result."""


@dataclass(frozen=True)
class AuditRows:
    """Bounded aggregate rows consumed by the Prometheus encoder."""

    pipelines: Sequence[Row]
    quality: Sequence[Row]
    maintenance: Sequence[Row]


def _label(value: object) -> str:
    return str(value).replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _labels(values: Mapping[str, object]) -> str:
    return "{" + ",".join(f'{name}="{_label(value)}"' for name, value in values.items()) + "}"


def _finite_float(value: object, *, field: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be numeric")
    numeric_value: Any = value
    try:
        number = float(numeric_value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be numeric") from exc
    except OverflowError as exc:
        raise ValueError(f"{field} must be finite") from exc
    if not math.isfinite(number):
        raise ValueError(f"{field} must be finite")
    return number


def _external_number(value: object, *, field: str, none_as_zero: bool = True) -> float:
    if value is None:
        if none_as_zero:
            return 0.0
        raise ValueError(f"{field} must be numeric")
    return _finite_float(value, field=field)


def _number(value: object) -> str:
    number = _finite_float(value, field="metric value")
    return str(int(number)) if number.is_integer() else repr(number)


def _bounded_label(
    value: object,
    mapping: Mapping[str, str],
    *,
    uppercase: bool = False,
) -> str:
    normalized = str(value).strip()
    if uppercase:
        normalized = normalized.upper()
    return mapping.get(normalized, "OTHER" if uppercase else "other")


def _family(lines: list[str], name: str, help_text: str, metric_type: str = "gauge") -> None:
    lines.extend((f"# HELP {name} {help_text}", f"# TYPE {name} {metric_type}"))


def build_snapshot(rows: AuditRows, *, collected_at: float) -> str:
    """Encode one successful audit read as deterministic Prometheus text."""

    lines: list[str] = []
    pipeline_families = (
        ("olist_pipeline_run_status", "Whether the latest pipeline run has this status."),
        ("olist_pipeline_duration_seconds", "Duration of the latest pipeline run in seconds."),
        ("olist_pipeline_input_rows", "Input rows in the latest pipeline run."),
        ("olist_pipeline_output_rows", "Output rows in the latest pipeline run."),
        ("olist_pipeline_rejected_rows", "Rejected rows in the latest pipeline run."),
        ("olist_pipeline_rejected_ratio", "Rejected-to-input ratio of the latest pipeline run."),
        (
            "olist_pipeline_last_success_timestamp_seconds",
            "Unix timestamp of the latest successful pipeline completion.",
        ),
    )
    for name, help_text in pipeline_families:
        _family(lines, name, help_text)

    seen_pipelines: set[tuple[str, str]] = set()
    for row in rows.pipelines:
        job_name = _bounded_label(row["job_name"], _JOB_LABELS)
        layer = _bounded_label(row["layer"], _LAYER_LABELS)
        status = _bounded_label(row["status"], _STATUS_LABELS, uppercase=True)
        duration_ms = _external_number(row.get("duration_ms"), field="duration_ms")
        input_count = _external_number(row.get("input_count"), field="input_count")
        output_count = _external_number(row.get("output_count"), field="output_count")
        rejected_count = _external_number(row.get("rejected_count"), field="rejected_count")
        freshness = row.get("last_success_timestamp_seconds")
        freshness_number = (
            None
            if freshness is None
            else _external_number(
                freshness,
                field="last_success_timestamp_seconds",
                none_as_zero=False,
            )
        )
        pipeline_key = (job_name, layer)
        if pipeline_key in seen_pipelines:
            continue
        seen_pipelines.add(pipeline_key)
        common = {"job_name": job_name, "layer": layer}
        status_labels = {**common, "status": status}
        ratio = 0.0 if input_count <= 0 else rejected_count / input_count
        lines.extend(
            (
                f"olist_pipeline_run_status{_labels(status_labels)} 1",
                f"olist_pipeline_duration_seconds{_labels(common)} {_number(duration_ms / 1000)}",
                f"olist_pipeline_input_rows{_labels(common)} {_number(input_count)}",
                f"olist_pipeline_output_rows{_labels(common)} {_number(output_count)}",
                f"olist_pipeline_rejected_rows{_labels(common)} {_number(rejected_count)}",
                f"olist_pipeline_rejected_ratio{_labels(common)} {_number(ratio)}",
            )
        )
        if freshness_number is not None:
            lines.append(
                "olist_pipeline_last_success_timestamp_seconds"
                f"{_labels(common)} {_number(freshness_number)}"
            )

    _family(lines, "olist_quality_checks_count", "Quality checks grouped by bounded outcome.")
    quality_counts: dict[tuple[str, str, str], float] = {}
    for row in rows.quality:
        quality_key = (
            _bounded_label(row["layer"], _LAYER_LABELS),
            _bounded_label(row["severity"], _QUALITY_SEVERITY_LABELS, uppercase=True),
            _bounded_label(row["status"], _QUALITY_STATUS_LABELS, uppercase=True),
        )
        quality_counts[quality_key] = quality_counts.get(
            quality_key, 0.0
        ) + _external_number(row.get("check_count"), field="check_count")
    for (layer, severity, status), count in sorted(quality_counts.items()):
        labels = {"layer": layer, "severity": severity, "status": status}
        lines.append(f"olist_quality_checks_count{_labels(labels)} {_number(count)}")

    _family(lines, "olist_maintenance_runs_count", "Maintenance runs grouped by action and status.")
    _family(
        lines,
        "olist_maintenance_rewritten_data_files_count",
        "Data files rewritten by recorded maintenance runs.",
    )
    _family(
        lines,
        "olist_maintenance_duration_seconds",
        "Average duration of recorded maintenance runs in seconds.",
    )
    maintenance_groups: dict[tuple[str, str], list[float]] = {}
    for row in rows.maintenance:
        maintenance_key = (
            _bounded_label(row["action"], _MAINTENANCE_ACTION_LABELS),
            _bounded_label(row["status"], _STATUS_LABELS, uppercase=True),
        )
        run_count = _external_number(row.get("run_count"), field="run_count")
        rewritten_count = _external_number(
            row.get("rewritten_data_files_count"), field="rewritten_data_files_count"
        )
        duration_ms = _external_number(row.get("duration_ms"), field="duration_ms")
        group = maintenance_groups.setdefault(maintenance_key, [0.0, 0.0, 0.0, 0.0])
        group[0] += run_count
        group[1] += rewritten_count
        group[2] += duration_ms * run_count
        group[3] += run_count
    for (action, status), values in sorted(maintenance_groups.items()):
        run_count, rewritten_count, weighted_duration_ms, duration_weight = values
        duration_ms = 0.0 if duration_weight <= 0 else weighted_duration_ms / duration_weight
        labels = {"action": action, "status": status}
        lines.extend(
            (
                f"olist_maintenance_runs_count{_labels(labels)} {_number(run_count)}",
                "olist_maintenance_rewritten_data_files_count"
                f"{_labels(labels)} {_number(rewritten_count)}",
                f"olist_maintenance_duration_seconds{_labels(labels)} {_number(duration_ms / 1000)}",
            )
        )

    _family(
        lines,
        "olist_pipeline_metrics_snapshot_timestamp_seconds",
        "Unix timestamp when the cached business metrics were collected.",
    )
    snapshot_time = _external_number(collected_at, field="collected_at", none_as_zero=False)
    lines.append(f"olist_pipeline_metrics_snapshot_timestamp_seconds {_number(snapshot_time)}")
    return "\n".join(lines) + "\n"


class TrinoClient:
    """Small client for Trino's statement protocol with a strict request timeout."""

    def __init__(
        self,
        base_url: str,
        *,
        user: str = "pipeline-metrics",
        timeout_seconds: float = 10.0,
    ) -> None:
        timeout_seconds = _finite_float(timeout_seconds, field="Trino timeout")
        if timeout_seconds <= 0:
            raise ValueError("Trino timeout must be positive")
        parsed_base = urlsplit(base_url)
        if parsed_base.scheme not in {"http", "https"} or parsed_base.hostname is None:
            raise ValueError("Trino URL must use HTTP or HTTPS with a host")
        if parsed_base.username is not None or parsed_base.password is not None:
            raise ValueError("Trino URL must not contain credentials")
        self._origin = (
            parsed_base.scheme.lower(),
            parsed_base.hostname.lower(),
            parsed_base.port or (443 if parsed_base.scheme.lower() == "https" else 80),
        )
        self._statement_url = base_url.rstrip("/") + "/v1/statement"
        self._user = user
        self._timeout = timeout_seconds

    def _request(self, request: Request, *, timeout_seconds: float) -> Mapping[str, Any]:
        if timeout_seconds <= 0:
            raise TrinoError("Trino request timed out")
        try:
            with urlopen(request, timeout=timeout_seconds) as response:
                payload = json.load(response)
        except (HTTPError, URLError, TimeoutError, socket.timeout, json.JSONDecodeError) as exc:
            if isinstance(exc, (TimeoutError, socket.timeout)) or "timed out" in str(exc).lower():
                raise TrinoError("Trino request timed out") from exc
            raise TrinoError(f"Trino request failed: {exc}") from exc
        if not isinstance(payload, dict):
            raise TrinoError("Trino returned a non-object response")
        error = payload.get("error")
        if error:
            message = error.get("message", "unknown query error") if isinstance(error, dict) else error
            raise TrinoError(f"Trino query failed: {message}")
        return payload

    def query(self, sql: str) -> Rows:
        deadline = time.monotonic() + self._timeout

        def request_before_deadline(request: Request) -> Mapping[str, Any]:
            return self._request(request, timeout_seconds=deadline - time.monotonic())

        request = Request(
            self._statement_url,
            data=sql.encode("utf-8"),
            headers={
                "Content-Type": "text/plain; charset=utf-8",
                "X-Trino-User": self._user,
                "X-Trino-Source": "olist-pipeline-metrics",
            },
            method="POST",
        )
        payload = request_before_deadline(request)
        columns: list[str] | None = None
        data: list[list[Any]] = []
        while True:
            if payload.get("columns"):
                columns = [str(column["name"]) for column in payload["columns"]]
            if payload.get("data"):
                data.extend(payload["data"])
            next_uri = payload.get("nextUri")
            if not next_uri:
                break
            parsed_next = urlsplit(str(next_uri))
            next_origin = (
                parsed_next.scheme.lower(),
                "" if parsed_next.hostname is None else parsed_next.hostname.lower(),
                parsed_next.port
                or (443 if parsed_next.scheme.lower() == "https" else 80),
            )
            if (
                next_origin != self._origin
                or parsed_next.username is not None
                or parsed_next.password is not None
            ):
                raise TrinoError("Trino nextUri origin is not allowed")
            payload = request_before_deadline(
                Request(str(next_uri), headers={"X-Trino-User": self._user})
            )
        if columns is None:
            return ()
        return tuple(dict(zip(columns, values, strict=True)) for values in data)


class QueryExecutor(Protocol):
    """Structural boundary used by the aggregate collector."""

    def query(self, sql: str) -> Rows: ...


class TrinoAuditCollector:
    """Read only the low-cardinality aggregates needed by dashboards and alerts."""

    def __init__(
        self,
        client: QueryExecutor,
        *,
        catalog: str = "lakehouse",
        control_schema: str = "control",
        quality_schema: str = "quality",
    ) -> None:
        for identifier in (catalog, control_schema, quality_schema):
            if _IDENTIFIER.fullmatch(identifier) is None:
                raise ValueError(f"invalid Trino identifier: {identifier}")
        self._client = client
        self._catalog = catalog
        self._control = control_schema
        self._quality = quality_schema

    def __call__(self) -> AuditRows:
        table_rows = self._client.query(
            f"""
SELECT table_schema, table_name
FROM {self._catalog}.information_schema.tables
WHERE (table_schema = '{self._control}' AND table_name IN ('pipeline_runs', 'maintenance_results'))
   OR (table_schema = '{self._quality}' AND table_name = 'quality_results')
""".strip()
        )
        available = {(str(row["table_schema"]), str(row["table_name"])) for row in table_rows}
        pipeline = (
            self._client.query(
            f"""
WITH ranked AS (
    SELECT job_name, layer, status, duration_ms, input_count, output_count,
           rejected_count,
           max(CASE WHEN status = 'SUCCESS' THEN to_unixtime(completed_at) END)
               OVER (PARTITION BY job_name, layer) AS last_success_timestamp_seconds,
           row_number() OVER (
               PARTITION BY job_name, layer ORDER BY started_at DESC, run_id DESC
           ) AS recency
    FROM {self._catalog}.{self._control}.pipeline_runs
)
SELECT job_name, layer, status, duration_ms, input_count, output_count,
       rejected_count, last_success_timestamp_seconds
FROM ranked
WHERE recency = 1
""".strip()
            )
            if (self._control, "pipeline_runs") in available
            else ()
        )
        quality = (
            self._client.query(
            f"""
WITH quality_runs AS (
    SELECT layer, run_id, max(checked_at) AS last_checked_at
    FROM {self._catalog}.{self._quality}.quality_results
    GROUP BY layer, run_id
), latest_runs AS (
    SELECT layer, run_id,
           row_number() OVER (
               PARTITION BY layer ORDER BY last_checked_at DESC, run_id DESC
           ) AS recency
    FROM quality_runs
)
SELECT results.layer, results.severity, results.status, count(*) AS check_count
FROM {self._catalog}.{self._quality}.quality_results AS results
JOIN latest_runs
  ON results.layer = latest_runs.layer AND results.run_id = latest_runs.run_id
WHERE latest_runs.recency = 1
GROUP BY results.layer, results.severity, results.status
""".strip()
            )
            if (self._quality, "quality_results") in available
            else ()
        )
        maintenance = (
            self._client.query(
            f"""
SELECT action, status, count(*) AS run_count,
       coalesce(sum(rewritten_data_files_count), 0) AS rewritten_data_files_count,
       coalesce(
           avg(CAST(date_diff('millisecond', started_at, completed_at) AS double)),
           0
       ) AS duration_ms
FROM {self._catalog}.{self._control}.maintenance_results
GROUP BY action, status
""".strip()
            )
            if (self._control, "maintenance_results") in available
            else ()
        )
        return AuditRows(pipeline, quality, maintenance)


class PipelineMetricsApplication:
    """Thread-safe cache whose health reflects the most recent poll attempt."""

    def __init__(
        self,
        collector: Callable[[], AuditRows],
        *,
        poll_interval_seconds: float = 30.0,
        clock: Callable[[], float] = time.time,
        monotonic_clock: Callable[[], float] = time.monotonic,
        waiter: Callable[[threading.Event, float], bool] = lambda event, seconds: event.wait(
            seconds
        ),
        logger: Callable[[str], None] = lambda message: print(message, flush=True),
    ) -> None:
        poll_interval_seconds = _finite_float(
            poll_interval_seconds,
            field="poll interval",
        )
        if poll_interval_seconds <= 0:
            raise ValueError("poll interval must be positive")
        self._collector = collector
        self._poll_interval = poll_interval_seconds
        self._clock = clock
        self._monotonic = monotonic_clock
        self._waiter = waiter
        self._logger = logger
        self._lock = threading.Lock()
        self._snapshot = ""
        self._last_success = 0.0
        self._healthy = False
        self._failure_counts = {reason: 0 for reason in ("trino_error", "invalid_data", "unexpected")}

    def poll_once(self) -> None:
        try:
            collected_at = self._clock()
            snapshot = build_snapshot(self._collector(), collected_at=collected_at)
        except TrinoError:
            self._record_failure("trino_error")
            return
        except ValueError:
            self._record_failure("invalid_data")
            return
        except Exception:
            self._record_failure("unexpected")
            return
        with self._lock:
            self._snapshot = snapshot
            self._last_success = collected_at
            self._healthy = True

    def _record_failure(self, reason: str) -> None:
        message = json.dumps(
            {"event": "pipeline_metrics_poll_failed", "reason": reason},
            separators=(",", ":"),
            sort_keys=False,
        )
        try:
            self._logger(message)
        except Exception:
            pass
        with self._lock:
            self._healthy = False
            self._failure_counts[reason] += 1

    def run_poll_loop(self, stopped: threading.Event) -> None:
        next_poll = self._monotonic()
        while not stopped.is_set():
            self.poll_once()
            next_poll += self._poll_interval
            now = self._monotonic()
            if next_poll <= now:
                missed = math.floor((now - next_poll) / self._poll_interval) + 1
                next_poll += missed * self._poll_interval
            if self._waiter(stopped, next_poll - now):
                break

    def metrics(self) -> bytes:
        with self._lock:
            snapshot = self._snapshot
            healthy = self._healthy
            last_success = self._last_success
            failure_counts = dict(self._failure_counts)
        health_metrics = (
            "# HELP olist_pipeline_exporter_up Whether the latest Trino poll succeeded.\n"
            "# TYPE olist_pipeline_exporter_up gauge\n"
            f"olist_pipeline_exporter_up {1 if healthy else 0}\n"
            "# HELP olist_pipeline_exporter_last_success_timestamp_seconds Unix timestamp of the latest successful Trino poll.\n"
            "# TYPE olist_pipeline_exporter_last_success_timestamp_seconds gauge\n"
            f"olist_pipeline_exporter_last_success_timestamp_seconds {_number(last_success)}\n"
            "# HELP olist_pipeline_exporter_poll_failures_total Poll failures grouped by bounded reason.\n"
            "# TYPE olist_pipeline_exporter_poll_failures_total counter\n"
            + "".join(
                "olist_pipeline_exporter_poll_failures_total"
                f'{{reason="{reason}"}} {count}\n'
                for reason, count in failure_counts.items()
            )
        )
        return (snapshot + health_metrics).encode("utf-8")

    def is_healthy(self) -> bool:
        with self._lock:
            return self._healthy


class _MetricsServer(ThreadingHTTPServer):
    app: PipelineMetricsApplication


def start_http_server(
    app: PipelineMetricsApplication,
    *,
    host: str = "0.0.0.0",
    port: int = 9101,
) -> ThreadingHTTPServer:
    """Construct the HTTP server without starting its serving thread."""

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            server = self.server
            assert isinstance(server, _MetricsServer)
            if self.path == "/metrics":
                self._respond(200, server.app.metrics(), "text/plain; version=0.0.4")
            elif self.path == "/healthz":
                healthy = server.app.is_healthy()
                self._respond(200 if healthy else 503, b"ok\n" if healthy else b"unhealthy\n")
            else:
                self._respond(404, b"not found\n")

        def _respond(self, status: int, body: bytes, content_type: str = "text/plain") -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, _format: str, *_args: object) -> None:
            return

    server = _MetricsServer((host, port), Handler)
    server.app = app
    return server


def _positive_float(env: Mapping[str, str], name: str, default: str) -> float:
    value = _finite_float(env.get(name, default), field=name)
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


def _bounded_env_integer(
    env: Mapping[str, str],
    name: str,
    default: str,
    *,
    minimum: int,
    maximum: int,
) -> int:
    message = f"{name} must be an integer between {minimum} and {maximum}"
    raw_value = env.get(name, default)
    if re.fullmatch(r"[0-9]+", raw_value) is None:
        raise ValueError(message)
    try:
        value = int(raw_value)
    except (OverflowError, ValueError):
        raise ValueError(message) from None
    if not minimum <= value <= maximum:
        raise ValueError(message)
    return value


def main(env: Mapping[str, str] | None = None) -> int:
    """Run the polling exporter until SIGTERM or SIGINT."""

    values = os.environ if env is None else env
    port = _bounded_env_integer(
        values,
        "PORT",
        "9101",
        minimum=1,
        maximum=65535,
    )
    client = TrinoClient(
        values.get("TRINO_URL", "http://trino:8080"),
        timeout_seconds=_positive_float(values, "TRINO_TIMEOUT_SECONDS", "10"),
    )
    collector = TrinoAuditCollector(
        client,
        catalog=values.get("TRINO_CATALOG", "lakehouse"),
        control_schema=values.get("TRINO_CONTROL_SCHEMA", "control"),
        quality_schema=values.get("TRINO_QUALITY_SCHEMA", "quality"),
    )
    app = PipelineMetricsApplication(
        collector,
        poll_interval_seconds=_positive_float(values, "POLL_INTERVAL_SECONDS", "30"),
    )
    server = start_http_server(app, port=port)
    stopped = threading.Event()

    def stop(_signum: int, _frame: object) -> None:
        stopped.set()
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    poller = threading.Thread(target=app.run_poll_loop, args=(stopped,), daemon=True)
    poller.start()
    try:
        server.serve_forever()
    finally:
        stopped.set()
        poller.join(timeout=5)
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
