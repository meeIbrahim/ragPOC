.PHONY: generate run_migrations run up down
# gRPC
generate:
	uv run python -m grpc_tools.protoc \
		-I./protos \
		--python_out=./src/generated \
		--pyi_out=./src/generated \
		--grpc_python_out=./src/generated \
		./protos/rag/v1/rag.proto

run_migrations: up
	uv run src/migration/main.py
	docker compose down

run:
	uv run src/rag/pipeline.py

up:
	docker compose up -d

down:
	docker compose down
