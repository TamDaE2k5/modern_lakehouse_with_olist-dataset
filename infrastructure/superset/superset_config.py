"""Production-safe local Superset configuration for the Olist lakehouse."""

from __future__ import annotations

import os
from urllib.parse import quote_plus


def _required_environment(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} must be provided through the environment")
    return value


SECRET_KEY = _required_environment("SUPERSET_SECRET_KEY")
_database_password = quote_plus(_required_environment("SUPERSET_DB_PASSWORD"))
SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://superset:{_database_password}@postgres:5432/superset"
)
SQLALCHEMY_TRACK_MODIFICATIONS = False

WTF_CSRF_ENABLED = True
WTF_CSRF_TIME_LIMIT = None
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SAMESITE = "Lax"
SESSION_COOKIE_SECURE = False
ENABLE_PROXY_FIX = False
TALISMAN_ENABLED = True

FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
}

ROW_LIMIT = 50_000
SUPERSET_WEBSERVER_PORT = 8088
