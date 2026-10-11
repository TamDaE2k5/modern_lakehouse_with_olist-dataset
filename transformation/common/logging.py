"""Small structured logging helpers shared by Spark jobs."""

from __future__ import annotations

import json
import logging
from typing import Any


def configure_logging(level: int = logging.INFO) -> None:
    """Configure concise English JSON log messages once per process."""
    logging.basicConfig(level=level, format="%(message)s", force=True)


def log_event(logger: logging.Logger, event: str, **fields: Any) -> None:
    """Emit one deterministic structured event."""
    logger.info(json.dumps({"event": event, **fields}, sort_keys=True, default=str))
