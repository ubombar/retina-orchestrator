FROM docker.io/library/golang:1.26.5-bookworm AS builder

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# cgo is required by the DuckDB capturer.
RUN CGO_ENABLED=1 GOOS=linux \
    go build -trimpath -ldflags="-s -w" \
    -o retina-orchestrator .

# ---- runtime ----------------------------------------------------------------
FROM docker.io/library/debian:bookworm-slim

LABEL  org.opencontainers.image.authors="Dioptra <contact@dioptra.io>"

RUN apt-get update \
    && apt-get install --no-install-recommends --yes ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# The orchestrator runs as an unprivileged user that owns its working
# directory, where the capture files are written by default.
RUN useradd --system --user-group retina \
    && mkdir /app \
    && chown retina:retina /app

WORKDIR /app
COPY --from=builder /build/retina-orchestrator .

USER retina

ENTRYPOINT ["./retina-orchestrator"]
