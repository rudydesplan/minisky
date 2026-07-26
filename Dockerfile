# Stage 1: Build the UI
FROM node:22.18.0-alpine@sha256:1b2479dd35a99687d6638f5976fd235e26c5b37e8122f786fcd5fe231d63de5b AS ui-builder
WORKDIR /app/ui
COPY ui/package*.json ./
RUN npm ci
COPY ui/ ./
RUN npm run build
ARG TARGETARCH
ARG PACK_VERSION=0.40.8
RUN set -eux; \
    case "${TARGETARCH}" in \
        amd64) pack_suffix="" ;; \
        arm64) pack_suffix="-arm64" ;; \
        *) echo "Unsupported architecture: ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    asset="pack-v${PACK_VERSION}-linux${pack_suffix}.tgz"; \
    cd /tmp; \
    wget -q "https://github.com/buildpacks/pack/releases/download/v${PACK_VERSION}/${asset}"; \
    wget -q "https://github.com/buildpacks/pack/releases/download/v${PACK_VERSION}/${asset}.sha256"; \
    sha256sum -c "${asset}.sha256"; \
    tar -xzf "${asset}" -C /usr/local/bin pack; \
    rm "${asset}" "${asset}.sha256"

# Stage 2: Build the Go Binary
FROM golang:1.26.5-bookworm@sha256:1ecb7edf62a0408027bd5729dfd6b1b8766e578e8df93995b225dfd0944eb651 AS binary-builder
WORKDIR /app
ARG VERSION=dev
COPY go.mod go.sum ./
RUN go mod download
COPY . .
COPY --from=ui-builder /app/ui/dist /app/ui/dist
RUN CGO_ENABLED=1 go build -trimpath \
    -ldflags "-s -w -X minisky/pkg/version.Version=${VERSION}" \
    -o minisky ./cmd/minisky

# Stage 3: Copy the Docker CLI without its daemon.
FROM docker:29-cli@sha256:be132a9f282288de4afaf63379dff75711fda0147c6b72a9df44e51841402144 AS docker-cli

# Stage 4: Minimal glibc runtime for DuckDB.
FROM gcr.io/distroless/cc-debian12@sha256:e8e7ee4b8b106d4c5fde9e422a321b2b8a2d5cca546c97adcce927f3e1d36e36
WORKDIR /app

COPY --from=docker-cli /usr/local/bin/docker /usr/local/bin/docker
COPY --from=ui-builder /usr/local/bin/pack /usr/local/bin/pack

COPY --from=binary-builder /app/minisky /app/minisky

ENV MINISKY_BIND=0.0.0.0

EXPOSE 8080 8081

# MiniSky needs access to the host's Docker socket
VOLUME /var/run/docker.sock

ENTRYPOINT ["/app/minisky", "start"]
