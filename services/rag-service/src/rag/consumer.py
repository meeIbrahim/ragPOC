"""Entry point of the ingest consumer: `python -m rag.consumer [--config PATH] [--mode MODE]`.

Phase 0 runs the stub processor (`--mode` / $RAG_STUB_MODE picks its scripted outcome); Phase 1
replaces it with the real ingest service behind the same wiring.

Exit codes: 0 after a normal stop, 2 for a usage or configuration error (nothing is started),
130 after Ctrl-C.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from collections.abc import Sequence

from redis import Redis

from rag.core import config_reader, logging_setup
from rag.ingest.stub import Mode, StubProcessor
from rag.transport.status_publisher import StatusPublisher
from rag.transport.stream_consumer import ConsumerSettings, StreamConsumer
from rag.transport.watchdog import Watchdog

logger = logging.getLogger(__name__)

EXIT_OK = 0
EXIT_USAGE = 2
EXIT_INTERRUPTED = 130


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="python -m rag.consumer", description=__doc__.splitlines()[0])
    parser.add_argument("--config", help="path to config.toml (default: $RAG_CONFIG, else the repo's config.toml)")
    parser.add_argument(
        "--mode",
        help="stub mode (default: $RAG_STUB_MODE): " + ", ".join(mode.value for mode in Mode),
    )
    return parser.parse_args(argv)


def _resolve_mode(flag: str | None) -> Mode | None:
    """The stub mode from the flag, else the environment. None (after logging why) when it is
    missing or not one of the documented modes."""
    raw = flag if flag is not None else os.environ.get("RAG_STUB_MODE")
    valid = ", ".join(mode.value for mode in Mode)
    if raw is None:
        logger.error("no stub mode: pass --mode or set RAG_STUB_MODE (one of: %s)", valid)
        return None
    try:
        return Mode(raw)
    except ValueError:
        logger.error("invalid stub mode %r (one of: %s)", raw, valid)
        return None


def main(argv: Sequence[str] | None = None) -> int:
    logging_setup.configure()
    try:
        args = _parse_args(argv)
    except SystemExit as exc:  # argparse already printed its usage message
        return int(exc.code or 0)

    mode = _resolve_mode(args.mode)
    if mode is None:
        return EXIT_USAGE
    try:
        config = config_reader.load(args.config)
    except config_reader.ConfigError as exc:
        logger.error("%s", exc)
        return EXIT_USAGE
    config_reader.warn_unknown_keys(config, logger)

    consumer_config = config.rag.consumer
    call_timeout = config.rag.call_timeout.total_seconds()
    redis = Redis(
        host=config.redis.host,
        port=config.redis.port,
        decode_responses=True,
        # Reads outlast the longest BLOCK (never more than the recovery interval), so a quiet
        # stream is not mistaken for a dead connection.
        socket_timeout=call_timeout,
        socket_connect_timeout=call_timeout,
    )
    consumer = StreamConsumer(
        redis,
        ConsumerSettings.from_config(config),
        StatusPublisher(redis, config.redis.status_stream, config.redis.stream_max_len),
        StubProcessor(mode),
        # A job that runs past job_timeout + watchdog_grace never returned from a step: exit and
        # let the container restart. min_idle is validated to exceed this, so the entry is not
        # reclaimed while the job could still be alive.
        Watchdog((consumer_config.job_timeout + consumer_config.watchdog_grace).total_seconds()),
    )
    logger.info("starting ingest consumer %s in stub mode %s", consumer.name, mode.value)
    try:
        consumer.run()
    except KeyboardInterrupt:
        logger.info("interrupted")
        return EXIT_INTERRUPTED
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
