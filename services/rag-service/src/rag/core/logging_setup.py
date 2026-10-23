"""Process-wide logging setup. All services log in UTC so lines from different hosts correlate."""

from __future__ import annotations

import logging
import os
import time

_FORMAT = "%(asctime)s.%(msecs)03dZ %(levelname)s %(name)s: %(message)s"
_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"


def configure(level: str | int | None = None) -> None:
    """Configure the root logger. Safe to call more than once: it replaces the handler rather
    than adding another. level defaults to $LOG_LEVEL, then INFO."""
    if level is None:
        level = os.environ.get("LOG_LEVEL", "INFO")
    if isinstance(level, str):
        level = level.upper()
    formatter = logging.Formatter(_FORMAT, _DATE_FORMAT)
    formatter.converter = time.gmtime
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logging.basicConfig(level=level, handlers=[handler], force=True)
