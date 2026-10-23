# Center service image. Build context: services/center.
#
#   docker build --target runtime .   the API and the migrate binary
#   docker build --target e2e .       the compose e2e test binary (see ../../scripts/e2e.sh)

FROM golang:1.26 AS build
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
FROM alpine:3.20 AS base
RUN adduser -D -u 10001 app && mkdir -p /data && chown app /data
USER app
WORKDIR /data

FROM base AS runtime
COPY --from=build /out/api /out/migrate /usr/local/bin/
# The config is mounted at /config/config.toml (see docker-compose.yml).
ENTRYPOINT ["api"]
CMD ["-config=/config/config.toml"]

FROM base AS e2e
COPY --from=e2e-build /out/e2e /usr/local/bin/e2e
ENTRYPOINT ["e2e"]
