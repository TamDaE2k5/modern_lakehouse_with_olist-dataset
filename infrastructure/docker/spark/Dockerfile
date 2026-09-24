FROM python:3.11.13-slim-bookworm@sha256:86adf8dbadc3d6e82ee5dd2c74bec2e1c2467cdad47886280501df722372d2e1 AS python-runtime

COPY requirements/runtime.lock /tmp/runtime-requirements.lock
RUN python -m pip install --no-cache-dir --require-hashes --requirement /tmp/runtime-requirements.lock

FROM apache/spark:3.5.9-scala2.12-java17-python3-ubuntu@sha256:f3d6eaa8bab8ec2e38f3c3918a5b2f8b253c95bcb19f9e87ad0e0cdacf9df2d5

ARG MAVEN_CENTRAL=https://repo1.maven.org/maven2
ARG ICEBERG_VERSION=1.11.0
ARG POSTGRES_JDBC_VERSION=42.7.7
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.262
ARG HADOOP_AWS_SHA512=b48a67a6af0b9885c05459b81d97ec07c30e30f889f6730966771d6a6d70cb3e856c29661b17c16a62a07044a4655467d682147b52a89651a137d146181fc284
ARG AWS_SDK_BUNDLE_SHA512=4231e69dfc4b952575579d5310e97abfc931f44f2184a71ce2072970d12623200f3adb346fea131c75c515188f665ae7912d2516becc14307cda144acb52e3f6

USER root

COPY --from=python-runtime /usr/local /usr/local

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
    chown spark:spark \
        "${iceberg_runtime}" \
        "${iceberg_aws}" \
        "${postgres_jdbc}" \
        "${hadoop_aws}" \
        "${aws_sdk_bundle}"; \
    install -d -o spark -g root /opt/spark-events /opt/lakehouse /opt/lakehouse/bin

COPY --chown=spark:root infrastructure/docker/spark/spark-defaults.conf /opt/spark/conf/spark-defaults.conf.template
COPY --chown=spark:root infrastructure/docker/spark/render-spark-config.sh /opt/lakehouse/bin/render-spark-config.sh

RUN chmod 0555 /opt/lakehouse/bin/render-spark-config.sh

ENV PATH="/opt/spark/bin:/usr/local/bin:${PATH}" \
    PYTHONPATH=/opt/lakehouse \
    PYSPARK_PYTHON=/usr/local/bin/python3.11 \
    PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.11 \
    SPARK_CONF_DIR=/tmp/spark-conf

USER spark

ENTRYPOINT ["/opt/lakehouse/bin/render-spark-config.sh"]
