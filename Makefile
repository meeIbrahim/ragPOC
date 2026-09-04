.PHONY: grpc-generate run_migrations run_worker up down

# gRPC
grpc-generate:
	uv run python -m grpc_tools.protoc \
		-I./protos \
		--python_out=./services/rag-service/src/generated \
		--pyi_out=./services/rag-service/src/generated \
		--grpc_python_out=./services/rag-service/src/generated \
		./protos/rag/v1/rag.proto

run_migrations: up
	uv run src/migration/main.py
	docker compose down

run_worker:
	uv run python -m rag.consumer

up:
	docker compose up -d

down:
	docker compose down
