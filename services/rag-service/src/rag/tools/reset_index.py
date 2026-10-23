"""Dev tool: empty the index so documents can be re-ingested (spec R8b, D10).

    python -m rag.tools.reset_index [--config PATH] [--yes]

Deletes every point in [rag].collection and every indexed_documents row; keeps the collection,
its payload index and the table. Without --yes it only prints what it would delete. It acts on
whatever the config points at: never point it at shared data.

Needed after changing chunk_size or chunk_overlap: otherwise every document is skipped as
already indexed.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence

from rag.core import config_reader, wiring
from rag.ports import DependencyUnavailable, IndexRegistry, VectorIndex

EXIT_OK = 0
EXIT_NOT_CONFIRMED = 1
EXIT_ERROR = 2


def run(index: VectorIndex, registry: IndexRegistry, collection: str, yes: bool) -> int:
    points, rows = index.count(), registry.count()
    if not yes:
        print(f"would delete {points} points from {collection!r} and {rows} indexed_documents rows; "
              "run again with --yes to do it")
        return EXIT_NOT_CONFIRMED
    # Rows first: a crash in between leaves points without rows, which re-ingest replaces.
    registry.clear()
    index.clear()
    print(f"deleted {points} points from {collection!r} and {rows} indexed_documents rows")
    return EXIT_OK


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m rag.tools.reset_index", description=__doc__.splitlines()[0])
    parser.add_argument("--config", help="path to config.toml (default: $RAG_CONFIG, else the repo's config.toml)")
    parser.add_argument("--yes", action="store_true", help="really delete")
    args = parser.parse_args(argv)
    try:
        config = config_reader.load(args.config)
        return run(wiring.index(config), wiring.registry(config), config.rag.collection, args.yes)
    except (config_reader.ConfigError, DependencyUnavailable) as exc:
        print(exc, file=sys.stderr)
        return EXIT_ERROR


if __name__ == "__main__":
    sys.exit(main())
