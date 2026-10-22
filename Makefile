.PHONY: grpc-generate run_migrations run_worker test e2e up stack down go_migrate go_migrate_down

# gRPC
grpc-generate:
	uv run python -m grpc_tools.protoc \
		-I./protos \
		--python_out=./services/rag-service/src/generated \
		--pyi_out=./services/rag-service/src/generated \
		--grpc_python_out=./services/rag-service/src/generated \
		./protos/rag/v1/rag.proto

# Needs Qdrant (`make up`); needs no Ollama and leaves the stack running. MinIO buckets are
# created by `make go_migrate`.
run_migrations:
	cd services/rag-service && uv run python -m migration.main

run_worker:
	cd services/rag-service && uv run python -m rag.consumer

test:
	cd services/center && go test -race ./...
	cd services/rag-service && uv run pytest

# Compose e2e of the whole system in an isolated project (needs Docker, takes a few minutes).
# `make e2e SCENARIOS="crash hang"` runs only those scenarios.
e2e:
	./scripts/e2e.sh $(SCENARIOS)

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
