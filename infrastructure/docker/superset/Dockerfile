FROM python:3.11-slim-bookworm@sha256:0bee7276f83efd4a1ee05bbbf4281d95ed28e079220a9457f25a93e3f1e3c31b

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    SUPERSET_HOME=/var/lib/superset

COPY infrastructure/docker/superset/constraints.txt /tmp/superset-constraints.txt

RUN groupadd --system --gid 10001 superset \
    && useradd --system --uid 10001 --gid superset --home-dir /var/lib/superset --create-home superset \
    && python -m pip install --no-cache-dir --upgrade pip==26.0.1 \
    && python -m pip install --no-cache-dir --constraint /tmp/superset-constraints.txt \
        apache-superset==6.1.0 \
        "trino[sqlalchemy]==0.338.0" \
        "psycopg2-binary==2.9.10" \
        "gunicorn==23.0.0" \
        "flask-caching==2.3.1" \
        "rich==13.9.4" \
        async-timeout \
        cachetools \
        filelock \
        numexpr \
        openapi-schema-validator \
        setuptools

COPY --chown=superset:superset infrastructure/superset/superset_config.py /opt/lakehouse/superset_config.py
COPY --chown=superset:superset infrastructure/superset/init_superset.sh /opt/lakehouse/init_superset.sh

RUN chmod 0555 /opt/lakehouse/init_superset.sh \
    && chown -R superset:superset /var/lib/superset

USER superset
WORKDIR /var/lib/superset
EXPOSE 8088
