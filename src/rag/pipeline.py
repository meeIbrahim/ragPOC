from pathlib import Path

from core import utils
from migration.sqlite_create_table_v1 import CreateSqLiteTables
from migration.qdrant_collection_v1 import QdrantCollection
from rag import ingest, search

DOC_DIR = utils.REPO_ROOT / "doc"

query = "which area has the most diverse geography"


def main():
    CreateSqLiteTables().run()
    QdrantCollection().run()

    ingest.ingest_directory(DOC_DIR)

    results = search.search(query)
    for result in results:
        print(result.id)
        print(result.score)
        if result.payload:
            print(result.payload.get("text"))

    reranked = search.rerank(results, query)
    print("-------- RERANKED --------")
    for result, rerank_score in reranked:
        print(result.id)
        print(result.score)
        if result.payload:
            print(result.payload.get("text"))
        print(rerank_score)


if __name__ == "__main__":
    main()
