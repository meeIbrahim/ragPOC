# Center service image. Build context: services/center.
# The runtime image also holds the web UI, built from a second context named "web":
#
#   docker build --target runtime --build-context web=../web .   the API, migrate and the UI
#   docker build --target e2e .       the compose e2e test binary (see ../../scripts/e2e.sh)

FROM node:24-alpine@sha256:116b800c3754b440ee0aa3ed2f20f5bf3d99a554bfd9adeea09ea8017f6e85f5 AS ui-build
WORKDIR /web
COPY --from=web package.json package-lock.json .npmrc ./
RUN npm ci --ignore-scripts
COPY --from=web . .
# Directories 755, files 644: COPY --chmod would give files the execute bit too.
RUN npm run build \
 && find dist -type d -exec chmod 755 {} + \
 && find dist -type f -exec chmod 644 {} +

FROM golang:1.26@sha256:4165ee6bf3b7811fd7b40373290405c83d0167dcb87ee5f29c427cdcef45ff8f AS build
WORKDIR /src
# Dependencies first, so a code change does not re-download them.
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# CGO off: the SQLite driver (modernc) is pure Go, so the binaries run on any base image.
ENV CGO_ENABLED=0
RUN go build -trimpath -o /out/api ./cmd/api \
 && go build -trimpath -o /out/migrate ./migrations

FROM build AS e2e-build
RUN go test -c -tags e2e -o /out/e2e ./e2e

# A non-root user owns /data. A named volume mounted there takes its ownership from the image,
# so the API can write its SQLite files.
FROM alpine:3.20@sha256:d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc AS base
RUN adduser -D -u 10001 app && mkdir -p /data && chown app /data
USER app
WORKDIR /data

FROM base AS runtime
COPY --from=build /out/api /out/migrate /usr/local/bin/
# Owned by root and only readable by the app user: the API serves it, never writes it.
COPY --from=ui-build --chown=root:root /web/dist /srv/web
# The config is mounted at /config/config.toml (see docker-compose.yml).
ENTRYPOINT ["api"]
CMD ["-config=/config/config.toml"]

FROM base AS e2e
COPY --from=e2e-build /out/e2e /usr/local/bin/e2e
ENTRYPOINT ["e2e"]
