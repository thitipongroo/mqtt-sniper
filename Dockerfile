# Stage 1: Build binary
FROM rust:1.82-alpine AS builder
RUN apk add --no-cache musl-dev
WORKDIR /usr/src/mqtt-sniper

# Copy manifests and source
COPY Cargo.toml Cargo.lock ./
COPY src ./src

# Build release binary
RUN cargo build --release

# Stage 2: Minimal runtime image
FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=builder /usr/src/mqtt-sniper/target/release/mqtt-sniper /usr/local/bin/mqtt-sniper

ENTRYPOINT ["mqtt-sniper"]