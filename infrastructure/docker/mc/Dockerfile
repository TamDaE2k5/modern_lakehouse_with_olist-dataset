FROM alpine:3.20
ARG MC_VERSION=RELEASE.2025-08-13T08-35-41Z
RUN apk add --no-cache curl ca-certificates bash \
    && curl -fsSL -o /usr/local/bin/mc "https://github.com/minio/mc/releases/download/${MC_VERSION}/mc.linux-amd64.${MC_VERSION}" \
    && chmod +x /usr/local/bin/mc
ENTRYPOINT ["mc"]
