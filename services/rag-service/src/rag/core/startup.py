"""Checks the consumer and the server run before doing any work (spec R5, R7, A7, A8)."""

from __future__ import annotations


class StartupError(Exception):
    """The process cannot start; the message says how to fix it. Entry points exit 2."""
