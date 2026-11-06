"""Các tiện ích dùng chung, Task Factory và Cơ chế bảo vệ Runtime an toàn cho Airflow DAGs.

Vai trò và Kiến trúc của Module Điều phối Dùng Chung (Airflow Common Layer):
-----------------------------------------------------------------------------
1. Chuẩn hóa Khởi tạo Task Thực thi Spark (SparkSubmitOperator):
   - Mọi task Spark chạy trong Airflow đều sử dụng `spark_submit_task()` để kết nối tới
     cụm Spark Master (`spark://spark-master:7077`) ở chế độ `client` mode.
   - Tự động đính kèm cấu hình đồng bộ: MinIO S3A endpoints, S3FileIO, thư mục ghi sự kiện
     `spark.eventLog.dir`, và môi trường Python virtualenv chuẩn.

2. Điều tiết Tài nguyên & Ngăn chặn Nghẽn Cụm (Resource Pool Throttling):
   - Sử dụng Airflow Pool `lakehouse_writes` với số slot giới hạn.
   - Ngăn chặn việc bắn quá nhiều job ghi đồng thời làm cạn kiệt tài nguyên bộ nhớ/CPU của Spark Worker
     hoặc gây xung đột ghi (concurrency conflict) trên Iceberg catalog.

3. Kiểm tra An toàn Tham số Thực thi (Runtime Guardrails):
   - Đảm bảo các tham số dòng lệnh quan trọng (`run_id`, `batch_id`, `mode`) tuân thủ nghiêm ngặt
     biểu thức chính quy (Regex: chỉ chứa ký tự chữ cái, số, dấu gạch nối, dấu chấm).
   - Ngăn chặn triệt để nguy cơ Command Injection hoặc ký tự đặc biệt làm hỏng tên file và đường dẫn S3.

4. Kích hoạt DAG con Bất biến & An toàn (Idempotent Trigger DAG Operator):
   - Khi nối các DAG lại với nhau (ví dụ: Ingestion -> Transformation -> Quality), nếu chạy lại một lần chạy cũ
     (re-run), toán tử thông thường sẽ báo lỗi `DagRunAlreadyExists`.
   - `IdempotentTriggerDagRunOperator` tự động tái sử dụng phiên chạy hiện có (reuse exact run) nếu trùng khớp cấu hình,
     đảm bảo tính bất biến (Idempotency) tuyệt đối cho hệ thống.

5. Bắt lỗi và Kiểm toán Sự cố An toàn (Sanitized Failure Event Auditing):
   - Khi có bất kỳ task nào thất bại, `failure_callback` tự động ghi nhận sự kiện có cấu trúc (NDJSON)
     vào `/opt/airflow/logs/failures.ndjson` mà không để lộ mật khẩu hay chuỗi bí mật.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import time
from typing import Any

from airflow.api.common.trigger_dag import trigger_dag
from airflow.exceptions import AirflowException, DagRunAlreadyExists
from airflow.models.dagrun import DagRun
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import (
    XCOM_LOGICAL_DATE_ISO,
    XCOM_RUN_ID,
    TriggerDagRunOperator,
)
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils import timezone as airflow_timezone
from airflow.utils.state import DagRunState
from sqlalchemy.exc import IntegrityError


# Biểu thức chính quy kiểm tra chuỗi tham số an toàn (tránh command injection)
SAFE_RUNTIME_VALUE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,79}$")
SAFE_SUFFIX = re.compile(r"^[a-z0-9][a-z0-9-]{0,63}$")

SPARK_CONNECTION_ID = "spark_default"
WRITE_POOL = "lakehouse_writes"
DAG_RUN_IDENTITY_CONSTRAINT = "dag_run_dag_id_run_id_key"
COLLISION_READBACK_TIMEOUT = 5.0
COLLISION_READBACK_INTERVAL = 0.05

# Cấu hình Spark Submit mặc định kết nối Object Storage và môi trường Python
SPARK_SUBMIT_CONF = {
    "spark.eventLog.enabled": "true",
    "spark.eventLog.dir": "file:///opt/airflow/logs/spark-events",
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.endpoint.region": "us-east-1",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.aws.credentials.provider": (
        "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"
    ),
    "spark.pyspark.driver.python": "/opt/lakehouse/venv/bin/python",
    "spark.pyspark.python": "/usr/local/bin/python3.11",
}

# Cấu hình thử lại mặc định cho các task: thử lại 2 lần, mỗi lần cách nhau 2 phút
DEFAULT_TASK_ARGS: dict[str, Any] = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}


# ==============================================================================
# TOÁN TỬ KÍCH HOẠT DAG CON BẤT BIẾN (IDEMPOTENT TRIGGER OPERATOR)
# ==============================================================================

class IdempotentTriggerDagRunOperator(TriggerDagRunOperator):
    """Toán tử kích hoạt DAG con an toàn, tái sử dụng Run ID cũ nếu đã tồn tại và chờ hoàn tất."""

    def __init__(
        self,
        *,
        wait_timeout: float = 1800,
        wait_for_completion: bool = True,
        **kwargs: Any,
    ) -> None:
        trigger_run_id = kwargs.get("trigger_run_id")
        if not isinstance(trigger_run_id, str) or not trigger_run_id:
            raise ValueError("Cần cung cấp một trigger_run_id cố định")
        if not wait_for_completion:
            raise ValueError("wait_for_completion phải là True")
        if wait_timeout <= 0:
            raise ValueError("wait_timeout phải là số dương")
        self.wait_timeout = float(wait_timeout)
        super().__init__(wait_for_completion=True, **kwargs)

    @staticmethod
    def _canonical_conf(conf: object) -> str:
        """Tuần tự hóa cấu hình sang chuỗi JSON chuẩn có sắp xếp key để so sánh tính nhất quán."""
        try:
            return json.dumps(conf or {}, sort_keys=True, separators=(",", ":"))
        except (TypeError, ValueError) as error:
            raise AirflowException("Tham số conf phải tuần tự hóa được thành JSON") from error

    def _assert_matching_conf(self, dag_run: DagRun) -> None:
        expected = self._canonical_conf(self.conf)
        actual = self._canonical_conf(dag_run.conf)
        if actual != expected:
            raise AirflowException(
                f"Cấu hình không khớp với phiên chạy con hiện có: {dag_run.run_id}"
            )

    def _find_exact_run(self, run_id: str) -> DagRun | None:
        existing = DagRun.find(dag_id=self.trigger_dag_id, run_id=[run_id])
        if len(existing) > 1:
            raise AirflowException(
                f"Tìm thấy nhiều phiên chạy con trùng ID cho {self.trigger_dag_id}/{run_id}"
            )
        return existing[0] if existing else None

    @staticmethod
    def _is_identity_collision(error: IntegrityError) -> bool:
        diagnostic = getattr(error.orig, "diag", None)
        return (
            getattr(diagnostic, "constraint_name", None)
            == DAG_RUN_IDENTITY_CONSTRAINT
        )

    def _read_collision_winner(
        self, error: IntegrityError, run_id: str
    ) -> DagRun | None:
        if not self._is_identity_collision(error):
            return None
        deadline = time.monotonic() + COLLISION_READBACK_TIMEOUT
        while True:
            try:
                dag_run = self._find_exact_run(run_id)
                if dag_run is not None:
                    self._assert_matching_conf(dag_run)
                    return dag_run
            except AirflowException:
                return None
            if time.monotonic() >= deadline:
                return None
            time.sleep(COLLISION_READBACK_INTERVAL)

    def _find_or_create_run(self, run_id: str) -> DagRun:
        existing = self._find_exact_run(run_id)
        if existing is not None:
            return existing
        try:
            dag_run = trigger_dag(
                dag_id=self.trigger_dag_id,
                run_id=run_id,
                conf=self.conf,
                execution_date=airflow_timezone.utcnow(),
                replace_microseconds=False,
            )
        except DagRunAlreadyExists as error:
            dag_run = error.dag_run
        except IntegrityError as error:
            dag_run = self._read_collision_winner(error, run_id)
            if dag_run is None:
                raise
        if dag_run is None:
            raise AirflowException(
                f"Không thể khởi tạo hoặc tìm thấy phiên chạy con cho {self.trigger_dag_id}/{run_id}"
            )
        return dag_run

    def execute(self, context: Mapping[str, Any]) -> None:
        run_id = str(self.trigger_run_id)
        dag_run = self._find_or_create_run(run_id)
        self._assert_matching_conf(dag_run)

        task_instance = context["task_instance"]
        task_instance.xcom_push(
            key=XCOM_LOGICAL_DATE_ISO,
            value=dag_run.logical_date.isoformat(),
        )
        task_instance.xcom_push(key=XCOM_RUN_ID, value=dag_run.run_id)

        # Chờ phiên chạy hoàn tất trong giới hạn thời gian cho phép
        deadline = time.monotonic() + self.wait_timeout
        while True:
            state = DagRunState(dag_run.state)
            if state in self.failed_states:
                raise AirflowException(
                    f"Phiên chạy con {run_id} thất bại với trạng thái: {state.value}"
                )
            if state in self.allowed_states:
                return
            if time.monotonic() >= deadline:
                raise AirflowException(
                    f"Hết thời gian chờ phiên chạy con {run_id} sau {self.wait_timeout:g} giây"
                )
            time.sleep(self.poke_interval)
            dag_run.refresh_from_db()


# ==============================================================================
# HÀM TRỢ GIÚP THAM SỐ VÀ TASK FACTORIES
# ==============================================================================

def runtime_params(*, default_mode: str = "incremental") -> dict[str, Param]:
    """Định nghĩa bộ tham số runtime tiêu chuẩn có kiểm tra schema chặt chẽ."""
    return {
        "run_id": Param(
            "auto",
            type="string",
            minLength=1,
            maxLength=80,
            pattern=SAFE_RUNTIME_VALUE.pattern,
            description="Mã định danh phiên chạy workflow (mặc định: auto)",
        ),
        "batch_id": Param(
            "auto",
            type="string",
            minLength=1,
            maxLength=80,
            pattern=SAFE_RUNTIME_VALUE.pattern,
            description="Mã định danh lô dữ liệu nguồn (mặc định: auto)",
        ),
        "mode": Param(
            default_mode,
            type="string",
            enum=["full", "incremental"],
            description="Chế độ xử lý: 'full' (toàn bộ) hoặc 'incremental' (gia tăng)",
        ),
    }


def validate_runtime_params(params: Mapping[str, object], **_context: object) -> None:
    """Xác thực tham số runtime trước khi thực thi bất kỳ task nặng nào."""
    for name in ("run_id", "batch_id"):
        value = params.get(name)
        if not isinstance(value, str) or SAFE_RUNTIME_VALUE.fullmatch(value) is None:
            raise ValueError(f"Tham số {name} không hợp lệ: {value!r}. Phải khớp {SAFE_RUNTIME_VALUE.pattern}")
    if params.get("mode") not in {"full", "incremental"}:
        raise ValueError("mode phải là 'full' hoặc 'incremental'")


def runtime_validation_task() -> PythonOperator:
    """Tạo task kiểm tra hợp lệ tham số đầu vào ở đầu mỗi DAG."""
    return PythonOperator(
        task_id="validate_runtime_params",
        python_callable=validate_runtime_params,
    )


def build_job_arguments(*, run_suffix: str, include_mode: bool) -> list[str]:
    """Tạo danh sách tham số dòng lệnh CLI có Jinja templating an toàn cho Spark job.

    Cơ chế sinh mã Run ID độc nhất:
    - Khi người dùng kích hoạt thủ công và truyền run_id (ví dụ 'manual-af-01'): Sử dụng chính run_id đó.
    - Khi Airflow tự động chạy theo lịch định kỳ (run_id mặc định là 'auto'): Sử dụng biểu thức Jinja
      `{{ ts_nodash }}` (ví dụ '20261021T000000') để đảm bảo mỗi lần chạy tự động đều có Run ID duy nhất,
      tránh va chạm bản ghi trạng thái trong bảng audit `lakehouse.control.pipeline_runs`.
    """
    if SAFE_SUFFIX.fullmatch(run_suffix) is None:
        raise ValueError(f"Hậu tố run_suffix không hợp lệ: {run_suffix}")
    run_template = "{% if params.run_id == 'auto' %}{{ ts_nodash }}{% else %}{{ params.run_id }}{% endif %}"
    batch_template = "{% if params.batch_id == 'auto' %}{{ ts_nodash }}{% else %}{{ params.batch_id }}{% endif %}"
    arguments = [
        "--run-id",
        f"{run_template}-{run_suffix}",
        "--attempt-id",
        "{{ ti.try_number }}",
        "--batch-id",
        batch_template,
    ]
    if include_mode:
        arguments.extend(("--mode", "{{ params.mode }}"))
    return arguments


def spark_submit_task(
    *,
    task_id: str,
    application: str,
    application_args: Sequence[str],
    write_pool_slots: int = 0,
) -> SparkSubmitOperator:
    """Khởi tạo một SparkSubmitOperator giao tiếp trực tiếp với Spark Master."""
    if write_pool_slots < 0:
        raise ValueError("write_pool_slots không được âm")
    pool_args: dict[str, object] = {}
    if write_pool_slots:
        pool_args = {"pool": WRITE_POOL, "pool_slots": write_pool_slots}
    return SparkSubmitOperator(
        task_id=task_id,
        conn_id=SPARK_CONNECTION_ID,
        application=application,
        application_args=list(application_args),
        conf=dict(SPARK_SUBMIT_CONF),
        deploy_mode="client",
        verbose=False,
        **pool_args,
    )


def _safe_event_value(value: object) -> str:
    rendered = "unknown" if value is None else str(value)
    return re.sub(r"[^A-Za-z0-9_.:+-]", "_", rendered)[:200]


def write_failure_event(context: Mapping[str, object], destination: Path) -> None:
    """Ghi nhận sự kiện task thất bại ra file NDJSON an toàn (đã loại bỏ mật khẩu/secrets)."""
    dag = context.get("dag")
    task = context.get("task")
    task_instance = context.get("ti")
    payload = {
        "dag_id": _safe_event_value(getattr(dag, "dag_id", None)),
        "task_id": _safe_event_value(getattr(task, "task_id", None)),
        "run_id": _safe_event_value(context.get("run_id")),
        "try_number": int(getattr(task_instance, "try_number", 0)),
        "status": "FAILED",
        "event_at": datetime.now(timezone.utc).isoformat(),
    }
    destination.parent.mkdir(parents=True, exist_ok=True)
    encoded = (json.dumps(payload, sort_keys=True) + "\n").encode("utf-8")
    descriptor = os.open(destination, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
    try:
        os.write(descriptor, encoded)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def failure_callback(context: Mapping[str, object]) -> None:
    """Hàm callback được Airflow tự động kích hoạt khi một task gặp lỗi.

    Đầu vào : ``context`` do Airflow cung cấp (chứa dag, task, run_id, exception...).
    Xử lý   : (1) ghi sự kiện có cấu trúc vào file NDJSON để truy vết nguội,
              (2) bắn email cảnh báo tức thì cho Quản trị viên để xử lý nóng.
    Đầu ra  : không trả về giá trị.

    Vì sao đặt phần gửi email trong khối try/except riêng? Vì ``failure_callback`` là hàm
    chạy khi hệ thống ĐÃ gặp sự cố. Nếu bản thân nó ném thêm ngoại lệ, Airflow sẽ nuốt mất
    log gốc và ta sẽ không còn biết nguyên nhân thật sự của lỗi ban đầu là gì.
    """
    destination = Path(os.environ.get("AIRFLOW_FAILURE_LOG", "/opt/airflow/logs/failures.ndjson"))
    write_failure_event(context, destination)

    try:
        from infrastructure.alerts.email_alert import send_alert_email

        dag_id = _safe_event_value(getattr(context.get("dag"), "dag_id", None))
        task_id = _safe_event_value(getattr(context.get("task"), "task_id", None))
        run_id = _safe_event_value(context.get("run_id"))
        try_number = int(getattr(context.get("ti"), "try_number", 0))
        # Lấy 1.000 ký tự CUỐI chứ không phải đầu: với SparkSubmitOperator, phần đầu thông báo
        # lỗi chỉ là dòng lệnh spark-submit dài dằng dặc, còn nguyên nhân gốc nằm ở cuối.
        reason = str(context.get("exception") or "Không có thông tin ngoại lệ")[-1_000:]
        send_alert_email(
            subject=f"Task {task_id} trong DAG {dag_id} đã THẤT BẠI",
            body_text=(
                "Một task trong Olist Data Lakehouse vừa thất bại.\n\n"
                f"  DAG        : {dag_id}\n"
                f"  Task       : {task_id}\n"
                f"  Run ID     : {run_id}\n"
                f"  Lần thử    : {try_number}\n"
                f"  Thời điểm  : {datetime.now(timezone.utc).isoformat()}\n\n"
                f"Nguyên nhân:\n{reason}\n\n"
                "Hãy mở Airflow UI tại http://localhost:8083 để xem log chi tiết.\n"
            ),
            severity="CRITICAL",
        )
    except Exception:  # noqa: BLE001 - tuyệt đối không để kênh cảnh báo che mất lỗi gốc
        pass


def dag_default_args() -> dict[str, object]:
    """Trả về cấu hình mặc định cho một DAG kèm theo failure callback."""
    return {**DEFAULT_TASK_ARGS, "on_failure_callback": failure_callback}
