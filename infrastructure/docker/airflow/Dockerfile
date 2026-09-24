FROM apache/spark:3.5.9-scala2.12-java17-python3-ubuntu@sha256:f3d6eaa8bab8ec2e38f3c3918a5b2f8b253c95bcb19f9e87ad0e0cdacf9df2d5 AS spark-client

FROM apache/airflow:2.11.2-python3.11@sha256:aac8468234dfa802c5ac4d6318bbf3b620793b55569ac230f584dc3c1f0ca49d AS airflow-runtime

ARG AIRFLOW_CONSTRAINTS_URL=https://raw.githubusercontent.com/apache/airflow/constraints-2.11.2/constraints-3.11.txt
ARG AIRFLOW_CONSTRAINTS_SHA256=135015e278ea7967f11dcd78d4f8b756701bba8e12f5b2c332292ee644f9d4b3
ARG MAVEN_CENTRAL=https://repo1.maven.org/maven2
ARG ICEBERG_VERSION=1.11.0
ARG POSTGRES_JDBC_VERSION=42.7.7
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.262
ARG HADOOP_AWS_SHA512=b48a67a6af0b9885c05459b81d97ec07c30e30f889f6730966771d6a6d70cb3e856c29661b17c16a62a07044a4655467d682147b52a89651a137d146181fc284
ARG AWS_SDK_BUNDLE_SHA512=4231e69dfc4b952575579d5310e97abfc931f44f2184a71ce2072970d12623200f3adb346fea131c75c515188f665ae7912d2516becc14307cda144acb52e3f6

USER root

RUN apt-get update \
    && apt-get install --no-install-recommends -y openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY --from=spark-client /opt/spark /opt/spark

RUN set -eux; \
    cd /opt/spark/jars; \
    iceberg_runtime="iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar"; \
    iceberg_aws="iceberg-aws-bundle-${ICEBERG_VERSION}.jar"; \
    postgres_jdbc="postgresql-${POSTGRES_JDBC_VERSION}.jar"; \
    hadoop_aws="hadoop-aws-${HADOOP_AWS_VERSION}.jar"; \
    aws_sdk_bundle="aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar"; \
    curl -fsSLO "${MAVEN_CENTRAL}/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/${iceberg_runtime}"; \
    curl -fsSLO "${MAVEN_CENTRAL}/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/${iceberg_runtime}.sha512"; \
    echo "$(cat "${iceberg_runtime}.sha512")  ${iceberg_runtime}" | sha512sum --check -; \
    curl -fsSLO "${MAVEN_CENTRAL}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/${iceberg_aws}"; \
    curl -fsSLO "${MAVEN_CENTRAL}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/${iceberg_aws}.sha512"; \
    echo "$(cat "${iceberg_aws}.sha512")  ${iceberg_aws}" | sha512sum --check -; \
    curl -fsSLO "${MAVEN_CENTRAL}/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/${postgres_jdbc}"; \
    curl -fsSLO "${MAVEN_CENTRAL}/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/${postgres_jdbc}.sha1"; \
    echo "$(cat "${postgres_jdbc}.sha1")  ${postgres_jdbc}" | sha1sum --check -; \
    curl -fsSLO "${MAVEN_CENTRAL}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/${hadoop_aws}"; \
    echo "${HADOOP_AWS_SHA512}  ${hadoop_aws}" | sha512sum --check -; \
    curl -fsSLO "${MAVEN_CENTRAL}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/${aws_sdk_bundle}"; \
    echo "${AWS_SDK_BUNDLE_SHA512}  ${aws_sdk_bundle}" | sha512sum --check -; \
    rm -f -- *.sha512 *.sha1; \
    chown -R airflow:root /opt/spark; \
    install -d -o airflow -g root /opt/lakehouse

COPY --chown=airflow:root requirements/runtime.lock /tmp/runtime-requirements.lock
COPY --chown=airflow:root requirements/airflow-compatibility.txt /tmp/airflow-compatibility.txt
COPY --chown=airflow:root infrastructure/docker/airflow/verify_pool.py /opt/lakehouse/airflow/verify_pool.py

USER airflow

RUN set -eux; \
    curl -fsSL "${AIRFLOW_CONSTRAINTS_URL}" -o /tmp/constraints.txt; \
    echo "${AIRFLOW_CONSTRAINTS_SHA256}  /tmp/constraints.txt" | sha256sum --check -; \
    pip install --no-cache-dir 'apache-airflow==2.11.2' \
      --constraint /tmp/constraints.txt; \
    python -m venv /opt/lakehouse/venv; \
    /opt/lakehouse/venv/bin/pip install --no-cache-dir \
      --require-hashes --requirement /tmp/runtime-requirements.lock; \
    pip install --no-cache-dir --requirement /tmp/airflow-compatibility.txt; \
    pip check; \
    /opt/lakehouse/venv/bin/pip check; \
    rm -f /tmp/constraints.txt /tmp/runtime-requirements.lock /tmp/airflow-compatibility.txt

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    SPARK_HOME=/opt/spark \
    PATH=/opt/spark/bin:${PATH} \
    PYTHONPATH=/opt/lakehouse \
    PYSPARK_PYTHON=/usr/local/bin/python3.11 \
    PYSPARK_DRIVER_PYTHON=/opt/lakehouse/venv/bin/python

FROM airflow-runtime AS airflow-test

RUN pip install --no-cache-dir 'pytest==8.4.1' \
    && python -m pip check
