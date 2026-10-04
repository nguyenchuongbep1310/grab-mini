# One image per Go service, from the same file:
#
#   docker build -f deploy/docker/Dockerfile --build-arg SERVICE=order-service -t grab-mini/order-service .
#
# Build from the repository root: the services share modules through go.work.
# SERVICE is a directory under services/ (order-service, trip-service,
# matching-service, driver-location-service, notification-service,
# rating-service, location-gateway, dlq-redrive, ...).

ARG GO_VERSION=1.26
FROM golang:${GO_VERSION} AS build
ARG SERVICE
WORKDIR /src

# Dependencies first, so editing code doesn't download them again. Every
# go.mod is needed (the workspace lists them all), but no source.
COPY go.work go.work.sum ./
COPY services/ services/
RUN test -n "$SERVICE" && test -d "services/$SERVICE" || { echo "set --build-arg SERVICE=<a directory under services/>"; exit 1; }
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    cd "services/$SERVICE" && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/app .

# distroless/static: no shell, no package manager, just the CA certificates
# (for TLS to Kafka and the collector) and a non-root user. The binary is
# static, and its SQL migrations are embedded in it.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
USER 65532:65532
ENTRYPOINT ["/app"]
