# syntax=docker/dockerfile:1

ARG ACE_VERSION=latest

FROM debian:bookworm-slim AS downloader

ARG ACE_VERSION
ARG TARGETOS=linux
ARG TARGETARCH

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates curl tar; \
    rm -rf /var/lib/apt/lists/*

RUN set -eux; \
    case "${TARGETOS}" in \
        linux) ACE_OS="Linux" ;; \
        *) echo "unsupported target os: ${TARGETOS}" >&2; exit 1 ;; \
    esac; \
    case "${TARGETARCH}" in \
        amd64) ACE_ARCH="x86_64" ;; \
        arm64) ACE_ARCH="arm64" ;; \
        *) echo "unsupported target arch: ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    if [ "${ACE_VERSION}" = "latest" ]; then \
        ACE_URL="https://github.com/pgedge/ace/releases/latest/download/ace_${ACE_OS}_${ACE_ARCH}.tar.gz"; \
    else \
        ACE_URL="https://github.com/pgedge/ace/releases/download/${ACE_VERSION}/ace_${ACE_OS}_${ACE_ARCH}.tar.gz"; \
    fi; \
    mkdir -p /opt/ace; \
    curl -fsSL "${ACE_URL}" -o /tmp/ace.tar.gz; \
    tar -xzf /tmp/ace.tar.gz -C /opt/ace --strip-components=1; \
    chmod +x /opt/ace/ace; \
    install -d -m 0775 /workspace && chown 65532:65532 /workspace

FROM gcr.io/distroless/base-debian12:nonroot AS runtime

COPY --from=downloader --chown=nonroot:nonroot /workspace /workspace

WORKDIR /workspace

COPY --from=downloader --chown=nonroot:nonroot /opt/ace/ace /usr/local/bin/ace
COPY --from=downloader /opt/ace/LICENSE /licenses/LICENSE
COPY --from=downloader /opt/ace/README.md /licenses/README.md
COPY --from=downloader /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

COPY --chown=nonroot:nonroot ace.yaml /etc/ace/ace.yaml

ENV ACE_CONFIG=/etc/ace/ace.yaml

ENTRYPOINT ["/usr/local/bin/ace"]
CMD ["--help"]
