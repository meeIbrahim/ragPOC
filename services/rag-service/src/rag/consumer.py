"""Entry point of the ingest consumer: `python -m rag.consumer [--config PATH] [--mode MODE]`.

With `--mode` or a non-empty $RAG_STUB_MODE it runs the Phase 0 stub (the compose e2e uses it);
otherwise it runs the real ingest service, after checking that the Qdrant collection exists and
that the embedding model's output size matches [rag].embedding_dim.

Exit codes: 0 after a normal stop, 1 when a dependency is unreachable at startup (a restart may
help), 2 for a usage, configuration or setup error (nothing is started), 130 after Ctrl-C.
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

from rag.core import config_reader, logging_setup, wiring
from rag.core.startup import StartupError, check_collection, check_embedding_dim
from rag.ingest.outcome import Processor
from rag.ingest.stub import Mode, StubProcessor
from rag.transport.status_publisher import StatusPublisher
from rag.transport.stream_consumer import ConsumerSettings, StreamConsumer, socket_timeout_for
from rag.transport.watchdog import Watchdog

logger = logging.getLogger(__name__)

EXIT_OK = 0
EXIT_UNAVAILABLE = 1
EXIT_USAGE = 2
EXIT_INTERRUPTED = 130


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="python -m rag.consumer", description=__doc__.splitlines()[0])
    parser.add_argument("--config", help="path to config.toml (default: $RAG_CONFIG, else the repo's config.toml)")
    parser.add_argument(
        "--mode",
        help="run the stub in this mode (default: $RAG_STUB_MODE; unset or empty runs the real "
        "ingest service): " + ", ".join(mode.value for mode in Mode),
    )
    return parser.parse_args(argv)


class _InvalidMode(Exception):
    pass


def _resolve_mode(flag: str | None) -> Mode | None:
    """The stub mode from the flag, else the environment; None means the real ingest service.
    An unset or empty RAG_STUB_MODE means real (compose passes the variable through even when it
    is unset); an explicit --mode must name a mode. Raises _InvalidMode (after logging why)
    for a value that is not a documented mode."""
    raw = flag if flag is not None else os.environ.get("RAG_STUB_MODE", "")
    if flag is None and raw == "":
        return None
    try:
        return Mode(raw)
    except ValueError:
        logger.error("invalid stub mode %r (one of: %s)", raw, ", ".join(mode.value for mode in Mode))
        raise _InvalidMode from None


def _real_processor(config: config_reader.Config) -> Processor:
    """The real ingest service, after the startup checks (spec R5). Raises StartupError."""
    check_collection(wiring.index(config), config.rag.collection)
    # The fault runbook points Ollama at a black hole to prove the watchdog; it needs the
    # environment flag AND the marker that only its config file sets, so a stray variable in a
    # normal deployment does nothing.
    if os.environ.get("RAG_SKIP_DIM_CHECK") == "1" and config.rag.consumer.allow_skip_dim_check:
        logger.warning("embedding dimension check skipped (RAG_SKIP_DIM_CHECK with allow_skip_dim_check)")
    else:
        check_embedding_dim(wiring.embedder(config), config.rag.embedding_dim, config.rag.embedding_model)
    return wiring.ingest_service(config)


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

    try:
        mode = _resolve_mode(args.mode)
    except _InvalidMode:
        return EXIT_USAGE
    try:
        config = config_reader.load(args.config)
    except config_reader.ConfigError as exc:
        logger.error("%s", exc)
        return EXIT_USAGE
    config_reader.warn_unknown_keys(config, logger)

    if mode is not None:
        processor: Processor = StubProcessor(mode)
    else:
        try:
            processor = _real_processor(config)
        except StartupError as exc:
            logger.error("%s", exc)
            return EXIT_UNAVAILABLE if exc.transient else EXIT_USAGE

    consumer_config = config.rag.consumer
    redis = make_redis(config)
    consumer = StreamConsumer(
        redis,
        ConsumerSettings.from_config(config),
        StatusPublisher(redis, config.redis.status_stream, config.redis.stream_max_len),
        processor,
        # A job that runs past job_timeout + watchdog_grace never returned from a step: exit and
        # let the container restart. min_idle is validated to exceed this, so the entry is not
        # reclaimed while the job could still be alive.
        Watchdog((consumer_config.job_timeout + consumer_config.watchdog_grace).total_seconds()),
    )
    if mode is not None:
        logger.info("starting ingest consumer %s in stub mode %s", consumer.name, mode.value)
    else:
        logger.info("starting ingest consumer %s (real ingest, model %s)", consumer.name, config.rag.embedding_model)
    try:
        consumer.run()
    except KeyboardInterrupt:
        logger.info("interrupted")
        return EXIT_INTERRUPTED
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
