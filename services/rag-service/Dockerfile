# RAG service image. Build context: services/rag-service.
# One image, two entry points: the consumer (default) and the migration
# (`python -m migration.main`, run by the rag-migrate service in docker-compose.yml).

FROM python:3.13-slim

# uv, pinned to the version the lockfile was written with.
COPY --from=ghcr.io/astral-sh/uv:0.10.9 /uv /usr/local/bin/uv

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH" \
    # There is no checkout here, so the default config location cannot be derived from the code.
    RAG_CONFIG=/config/config.toml

WORKDIR /app
# hatchling builds the project, so the files named in pyproject.toml must be present.
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
RUN uv sync --frozen --no-dev

RUN useradd --uid 10001 --create-home app && mkdir -p /data && chown app /data
USER app
WORKDIR /data

# No SIGTERM handler on purpose (PRD 5.3: no graceful drain); run with `init: true` so PID 1
# is an init process that forwards signals.
CMD ["python", "-m", "rag.consumer"]
