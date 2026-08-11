# syntax=docker/dockerfile:1.18@sha256:dabfc0969b935b2080555ace70ee69a5261af8a8f1b4df97b9e7fbcf6722eddf

FROM docker.io/library/golang:1.26.5-alpine3.24@sha256:0178a641fbb4858c5f1b48e34bdaabe0350a330a1b1149aabd498d0699ff5fb2 AS build

ENV GOTOOLCHAIN=local

RUN --mount=type=cache,target=/go/pkg/mod \
    GOBIN=/usr/local/bin go install github.com/go-task/task/v3/cmd/task@v3.50.0

WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY Task.yml ./
COPY cmd ./cmd
COPY internal ./internal

ARG TARGETARCH
ARG VERSION=dev
ARG REVISION=unknown
ARG BUILD_TIME=unknown

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    task --taskfile Task.yml build:container \
    TARGETARCH="$TARGETARCH" \
    OUTPUT=/out/memory-recall-coin \
    VERSION="$VERSION" \
    REVISION="$REVISION" \
    BUILD_TIME="$BUILD_TIME" && \
    mkdir -p /out/home

FROM scratch

ARG VERSION=dev
ARG REVISION=unknown
ARG BUILD_TIME=unknown

LABEL org.opencontainers.image.title="memory-recall-coin" \
      org.opencontainers.image.description="Tailnet-only cross-device MCP memory service" \
      org.opencontainers.image.version="$VERSION" \
      org.opencontainers.image.revision="$REVISION" \
      org.opencontainers.image.created="$BUILD_TIME" \
      org.opencontainers.image.source="https://github.com/Alhamdulillah-R/memory-recall-coin"

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build --chown=65532:65532 /out/home /home/nonroot
COPY --from=build --chown=65532:65532 /out/memory-recall-coin /memory-recall-coin

ENV HOME=/home/nonroot

USER 65532:65532

EXPOSE 8080

ENTRYPOINT ["/memory-recall-coin"]
CMD ["serve"]
