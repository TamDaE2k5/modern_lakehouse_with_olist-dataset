FROM golang:1.24.8-bookworm@sha256:4ed690d6649d63c312b99a6120025ec79ce3b542968a37da53d6236c7c61a848 AS build

ARG TARGETARCH
ARG MINIO_TAG=RELEASE.2025-10-15T17-29-55Z
ARG MINIO_COMMIT=9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a

WORKDIR /build

RUN set -eux; \
    git clone --branch "${MINIO_TAG}" --depth 1 https://github.com/minio/minio.git source; \
    cd source; \
    test "$(git rev-parse HEAD)" = "${MINIO_COMMIT}"; \
    test "$(git describe --tags --exact-match)" = "${MINIO_TAG}"; \
    build_arch="${TARGETARCH:-$(go env GOARCH)}"; \
    ldflags="$(MINIO_RELEASE=RELEASE go run buildscripts/gen-ldflags.go)"; \
    CGO_ENABLED=0 GOOS=linux GOARCH="${build_arch}" \
      go build -tags kqueue -trimpath \
      --ldflags "${ldflags}" \
      -o /build/minio .; \
    /build/minio --version | grep -F "${MINIO_TAG}"

FROM busybox:1.37.0-musl@sha256:fc6dddc4c44b1bfe37f41cae8e67d1693828e8f42a91862816d7953e2c9d3f23 AS healthcheck

FROM debian:12.12-slim@sha256:d5d3f9c23164ea16f31852f95bd5959aad1c5e854332fe00f7b3a20fcc9f635c

ARG MINIO_TAG=RELEASE.2025-10-15T17-29-55Z

RUN set -eux; \
    mkdir -p /data; \
    chown 1000:1000 /data

COPY --from=build --chown=1000:1000 /build/minio /usr/local/bin/minio
COPY --from=healthcheck /bin/busybox /usr/local/bin/busybox
COPY --from=build /build/source/CREDITS /licenses/CREDITS
COPY --from=build /build/source/LICENSE /licenses/LICENSE

LABEL org.opencontainers.image.title="MinIO" \
      org.opencontainers.image.version="${MINIO_TAG}" \
      org.opencontainers.image.revision="9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a" \
      org.opencontainers.image.source="https://github.com/minio/minio"

USER 1000:1000
EXPOSE 9000 9001
VOLUME ["/data"]
ENTRYPOINT ["/usr/local/bin/minio"]
CMD ["server", "/data"]
