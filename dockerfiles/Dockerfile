# syntax=docker/dockerfile:1.6
# Multi-stage build: produces a small static-ish kroxy image.
#
# Build:   docker build -t kroxy:dev -f dockerfiles/Dockerfile .
# Run:     docker run --rm -p 9092:9092 -p 9090:9090 -v $PWD/config.yaml:/etc/kroxy/config.yaml kroxy:dev

FROM golang:1.25-alpine AS build
WORKDIR /src

# Cache deps separately for faster rebuilds.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

ARG VERSION=dev
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux \
    go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/kroxy ./cmd/kroxy

FROM alpine:3.20
RUN apk add --no-cache ca-certificates && \
    addgroup -S kroxy && adduser -S -G kroxy kroxy
COPY --from=build /out/kroxy /usr/local/bin/kroxy
USER kroxy
EXPOSE 9092 9090
ENTRYPOINT ["/usr/local/bin/kroxy"]
CMD ["-config", "/etc/kroxy/config.yaml"]
