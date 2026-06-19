# Single image builds both binaries; the SERVICE env var picks which one runs.
# Railway (and docker compose) create two services from this one image:
#   SERVICE=api      -> HTTP + WebSocket front-end (scale to N instances)
#   SERVICE=matcher  -> the matcher worker (run 1, or a few for failover)

FROM rust:1.90-slim-bookworm AS builder
WORKDIR /app
COPY . .
RUN cargo build --release --bin api --bin matcher

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/api /usr/local/bin/api
COPY --from=builder /app/target/release/matcher /usr/local/bin/matcher

# Default to the API; override SERVICE=matcher for the worker service.
ENV SERVICE=api
ENTRYPOINT ["/bin/sh", "-c", "exec \"$SERVICE\""]
