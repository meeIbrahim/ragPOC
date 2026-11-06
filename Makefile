.PHONY: grpc-generate run_migrations run_worker run_server reset_index eval test test-integration e2e e2e-faults up stack down go_migrate go_migrate_down

# gRPC
# The proto's own directory is the include root: with -I./protos, protoc writes
# `from rag.v1 import rag_pb2`, which resolves to this service's `rag` package and fails. The
# sed turns the bare `import rag_pb2` into an import from the `generated` package.
grpc-generate:
	cd services/rag-service && uv run python -m grpc_tools.protoc \
		-I../../protos/rag/v1 \
		--python_out=src/generated \
		--pyi_out=src/generated \
		--grpc_python_out=src/generated \
		../../protos/rag/v1/rag.proto
	sed -i.bak 's/^import rag_pb2 as rag__pb2$$/from generated import rag_pb2 as rag__pb2/' \
		services/rag-service/src/generated/rag_pb2_grpc.py
	rm services/rag-service/src/generated/rag_pb2_grpc.py.bak

# Needs Qdrant (`make up`); needs no Ollama and leaves the stack running. MinIO buckets are
# created by `make go_migrate`.
run_migrations:
	cd services/rag-service && uv run python -m migration.main

# The real ingest consumer (needs Ollama with [rag].embedding_model pulled). For the Phase 0 stub:
# `make run_worker ARGS="--mode indexed"`.
run_worker:
	cd services/rag-service && uv run python -m rag.consumer $(ARGS)

# The gRPC query server on [rag_grpc].port; needs GROQ_API_KEY in the environment.
run_server:
	cd services/rag-service && uv run python -m rag.server $(ARGS)

# Deletes every indexed point and indexed_documents row (dev only). Dry run unless ARGS=--yes.
reset_index:
	cd services/rag-service && uv run python -m rag.tools.reset_index $(ARGS)

# Retrieval eval: ingests evals/corpus (needs run_worker) and reports hit@k; no Groq calls.
eval:
	cd services/rag-service && uv run python ../../evals/run_eval.py $(ARGS)

test:
	cd services/center && go test -race ./...
	cd services/rag-service && uv run pytest

# Tests against the dev infra (`make up`: Redis, MinIO, Qdrant on localhost). Each test skips
# when its service is not configured or not reachable. The MinIO test also needs MINIO_URL,
# MINIO_ACCESS_KEY and MINIO_SECRET_KEY in your environment (not set here, to keep credentials
# out of the Makefile).
REDIS_ADDR ?= localhost:6379
REDIS_URL ?= redis://localhost:6379/0
QDRANT_URL ?= http://localhost:6333
test-integration:
	cd services/center && REDIS_ADDR=$(REDIS_ADDR) go test -race -count=1 -tags integration ./internal/events/redis/
	cd services/rag-service && REDIS_URL=$(REDIS_URL) QDRANT_URL=$(QDRANT_URL) uv run pytest -m integration -rs

# Compose e2e of the whole system in an isolated project (needs Docker, takes a few minutes).
# `make e2e SCENARIOS="crash hang"` runs only those scenarios.
e2e:
	./scripts/e2e.sh $(SCENARIOS)

# Failure-injection e2e (Redis outages, killed workers, concurrent duplicates). Separate from
# `make e2e` so a timing flake here never turns the stable run red.
# `make e2e-faults SCENARIOS=redis_outage_short` runs only that one.
e2e-faults:
	./scripts/e2e.sh --faults $(SCENARIOS)

go_migrate:
	go build -C services/center -o ../../bin/migrate ./migrations
	./bin/migrate -config=config.toml

go_migrate_down:
	go build -C services/center -o ../../bin/migrate ./migrations
	./bin/migrate -config=config.toml -down

# Infrastructure only (MinIO, Qdrant, Redis): the host dev loop runs the API and the worker itself.
up:
	docker compose up -d

# The whole system in containers.
stack:
	docker compose --profile app up -d --build

down:
	docker compose --profile app down
