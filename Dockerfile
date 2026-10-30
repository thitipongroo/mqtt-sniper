# Stage 1: Build binary with Alpine musl toolchain
FROM rust:alpine AS builder
RUN apk add --no-cache musl-dev gcc
WORKDIR /usr/src/mqtt-sniper

# Copy manifests and source (benches is declared in Cargo.toml, so it must exist to parse it)
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY benches ./benches

# Build release binary (stripped & LTO via Cargo.toml release profile)
RUN cargo build --release

# Stage 2: Minimal runtime image (<20MB)
FROM alpine:latest
RUN apk add --no-cache ca-certificates tzdata && \
    adduser -D -u 10001 -g "" sniper

COPY --from=builder /usr/src/mqtt-sniper/target/release/mqtt-sniper /usr/local/bin/mqtt-sniper

USER sniper

ENTRYPOINT ["/usr/local/bin/mqtt-sniper"]
CMD ["--help"]