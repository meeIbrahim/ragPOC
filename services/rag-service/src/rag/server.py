"""Entry point of the query server: `python -m rag.server [--config PATH]`.

Serves rag.v1.RagEngineService on [rag_grpc].port after checking that GROQ_API_KEY is set, the
Qdrant collection exists, and the embedding model's output size matches [rag].embedding_dim.

Exit codes: 0 after a normal stop, 1 when a dependency is unreachable at startup (a restart may
help), 2 for a usage, configuration or setup error (nothing is started), 130 after Ctrl-C.
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Sequence

from rag.core import config_reader, logging_setup, wiring
from rag.core.startup import StartupError, check_collection, check_embedding_dim
from rag.transport.grpc_server import SERVICE_NAME, serve

logger = logging.getLogger(__name__)

EXIT_OK = 0
EXIT_UNAVAILABLE = 1
EXIT_USAGE = 2
EXIT_INTERRUPTED = 130


def main(argv: Sequence[str] | None = None) -> int:
    logging_setup.configure()
    parser = argparse.ArgumentParser(prog="python -m rag.server", description=__doc__.splitlines()[0])
    parser.add_argument("--config", help="path to config.toml (default: $RAG_CONFIG, else the repo's config.toml)")
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:  # argparse already printed its usage message
        return int(exc.code or 0)

    try:
        config = config_reader.load(args.config)
    except config_reader.ConfigError as exc:
        logger.error("%s", exc)
        return EXIT_USAGE
    config_reader.warn_unknown_keys(config, logger)

    try:
        llm = wiring.llm(config)  # first: no key means nothing else is worth checking
        check_collection(wiring.index(config), config.rag.collection)
        check_embedding_dim(wiring.embedder(config), config.rag.embedding_dim, config.rag.embedding_model)
    except StartupError as exc:
        logger.error("%s", exc)
        return EXIT_UNAVAILABLE if exc.transient else EXIT_USAGE

    server, port = serve(
        wiring.query_service(config, llm), port=config.rag_grpc.port, max_workers=config.rag_grpc.max_workers
    )
    logger.info("serving %s on port %d (model %s)", SERVICE_NAME, port, config.rag.llm_model)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("interrupted")
        server.stop(grace=None)
        return EXIT_INTERRUPTED
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
