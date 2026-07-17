FROM rust:1.85-bookworm AS builder
WORKDIR /src
COPY . .
RUN cargo build --locked --release --manifest-path examples/multi-node/Cargo.toml

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 app
WORKDIR /app
COPY --from=builder /src/examples/multi-node/target/release/hatchet-multi-node /app/hatchet-multi-node
COPY --from=builder /src/examples/multi-node/target/release/trigger /app/trigger
USER app
ENTRYPOINT ["/app/hatchet-multi-node"]
