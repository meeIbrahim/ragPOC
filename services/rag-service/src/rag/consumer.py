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
from redis.backoff import NoBackoff
from redis.retry import Retry

from rag.core import config_reader, logging_setup
from rag.ingest.stub import Mode, StubProcessor
from rag.transport.status_publisher import StatusPublisher
from rag.transport.stream_consumer import ConsumerSettings, StreamConsumer, socket_timeout_for
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


def make_redis(config: config_reader.Config, *, host: str | None = None, port: int | None = None) -> Redis:
    """The Redis client, built for a consumer that must survive whatever is in the stream.

    - encoding_errors="replace": a stream entry with bytes that are not UTF-8 would otherwise
      raise UnicodeDecodeError while the reply is decoded, before the consumer sees the entry, so
      it could never be acknowledged and would crash the process again at every reclaim. With
      replacement it fails validation and is acknowledged like any other bad entry.
    - retry with zero retries: redis-py's default is 10 retries with backoff, which would let one
      XADD take minutes after the watchdog is disarmed (the entry could be reclaimed and re-run
      meanwhile) and would re-send a timed-out XAUTOCLAIM, bumping a delivery count twice. The
      consumer's own bounded retries are the only ones.
    - protocol=2: the consumer relies on the RESP2 reply shapes (RESP3 returns dicts for XREADGROUP).
    - the read timeout outlasts the longest BLOCK, so a quiet stream is never mistaken for a dead
      connection, even when call_timeout is configured very small.
    """
    call_timeout = config.rag.call_timeout.total_seconds()
    read_block = ConsumerSettings.from_config(config).read_block.total_seconds()
    return Redis(
        host=host or config.redis.host,
        port=port or config.redis.port,
        decode_responses=True,
        encoding_errors="replace",
        protocol=2,
        retry=Retry(NoBackoff(), 0),
        socket_timeout=socket_timeout_for(call_timeout, read_block),
        socket_connect_timeout=call_timeout,
    )


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
    redis = make_redis(config)
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
