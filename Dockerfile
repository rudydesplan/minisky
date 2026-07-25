# Stage 1: Build the UI
FROM node:22.18.0-alpine AS ui-builder
WORKDIR /app/ui
COPY ui/package*.json ./
RUN npm ci
COPY ui/ ./
RUN npm run build
RUN wget -qO- \
    https://github.com/buildpacks/pack/releases/download/v0.34.2/pack-v0.34.2-linux.tgz \
    | tar -xz -C /usr/local/bin pack

# Stage 2: Build the Go Binary
FROM golang:1.26.5-bookworm AS binary-builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
COPY --from=ui-builder /app/ui/dist /app/ui/dist
RUN CGO_ENABLED=1 go build -trimpath -o minisky ./cmd/minisky

# Stage 3: Copy the Docker CLI without its daemon.
FROM docker:29-cli AS docker-cli

# Stage 4: Minimal glibc runtime for DuckDB.
FROM gcr.io/distroless/cc-debian12
WORKDIR /app

COPY --from=docker-cli /usr/local/bin/docker /usr/local/bin/docker
COPY --from=ui-builder /usr/local/bin/pack /usr/local/bin/pack

COPY --from=binary-builder /app/minisky /app/minisky

EXPOSE 8080 8081

# MiniSky needs access to the host's Docker socket
VOLUME /var/run/docker.sock

ENTRYPOINT ["/app/minisky", "start"]
