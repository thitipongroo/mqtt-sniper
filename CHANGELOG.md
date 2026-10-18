# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.5.0] - 2026-10-18

### Added

- **Client Start Index & Prefix (`--start-number` / `-n`, `--client-prefix`)**: Added configurable starting index for `{device_id}` and client IDs (e.g., `-n 1` for 1-based device pools `sensor-1..sensor-N` or distributed multi-node load sharding).
- **Dynamic Per-Device Credentials**: Enabled `{device_id}` variable interpolation inside `--username` and `--password` for brokers enforcing per-device authentication and ACLs.
- **External Payload Template Files (`--payload-file`)**: Load JSON/text payload templates directly from disk with full dynamic placeholder interpolation.
- **Chaos / DLQ Fault Injection (`--malformed-rate`)**: Added configurable malformed payload injection rate (`0.0` to `1.0`) to stress-test Dead-Letter Queue (DLQ) routing and consumer resilience, tracked via `malformed_sent` in terminal and JSON reports.
- **Multi-Precision Timestamps & Boolean Placeholders**: Added `{timestamp_secs}` (epoch seconds), `{timestamp_us}` (epoch microseconds), and `{random_bool}` (`true`/`false`) placeholders.
- **Multi-Key & Auto-Unit Subscribe Latency**: Enhanced `--mode sub` end-to-end latency calculation to recognize `"ts"`, `"timestamp"`, and `"time"` fields and automatically detect epoch seconds, milliseconds, or microseconds.

## [0.4.0] - 2026-10-18

### Added

- **Duration-based Soak Testing (`--duration-secs` / `-d`)**: Added time-based benchmark execution mode for both Publish and Subscribe workloads.
- **Dynamic Random Telemetry Placeholders**: Added `{random_int}` (`0..=100`), `{random_float}` (`20.00..=40.00`), and `{uuid}` (UUID v4) placeholders in payload templates.
- **Advanced MQTT Protocol Options**: Added `--retain` flag for publishing retained messages and `--keep-alive` for configurable MQTT keep-alive intervals.
- **Automated Docker Image Publishing (GHCR)**: Added `publish-docker` job in GitHub Actions release workflow to automatically publish `ghcr.io/thitipongroo/mqtt-sniper` container images.

## [0.3.0] - 2026-10-18

### Added

- **SSL/TLS & mTLS Support**: Added `--tls` for encrypted `mqtts://` connections using OS native root certificates, `--ca` for custom CA certificates (PEM), and `--cert` / `--key` for mutual TLS (mTLS) client authentication.
- **Subscribe / Fan-out Benchmark Mode**: Added `--mode pub|sub` (`Mode::Pub` and `Mode::Sub`) to benchmark both message ingestion and fan-out subscriber delivery, with automatic end-to-end latency extraction when payloads contain a `"ts"` epoch timestamp.
- **Active Connection Limiter**: Added `--max-connections` backed by `tokio::sync::Semaphore` to prevent OS file descriptor exhaustion (`Too many open files`) during massive client simulations.
- Added integration test covering Subscribe mode (`--mode sub`) and `--max-connections`.

### Changed

- **Clean Architecture Refactoring**: Modularized codebase into `src/lib.rs`, `src/main.rs`, `src/cli.rs`, `src/payload.rs`, `src/report.rs`, and `src/runner.rs`.
- Extended `BenchmarkReport` JSON schema and terminal summary to include `mode` and `tls` status.

## [0.2.0] - 2026-10-18

### Added

- **Real-time Terminal Progress Bar**: Integrated `indicatif` to display live progress, message throughput (`msgs/sec`), and ETA.
- **HDR Histogram Latency Metrics**: Integrated `hdrhistogram` to record per-message latencies and report `min`, `avg`, `p50`, `p95`, `p99`, and `max` in milliseconds.
- **JSON Report Export**: Added `--output-json <PATH>` using `serde` and `serde_json` for CI/CD pipeline integration.

## [0.1.1] - 2026-10-18

### Added

- Added `--ramp-up-ms` to stagger client connection spawning and prevent TCP SYN floods.
- Added `--interval-ms` (`-i`) to control delay between consecutive messages per client.
- Added `--payload` template with `{device_id}`, `{seq}`, and `{timestamp}` placeholders, plus `--payload-size` padding.
- Added `--username` (`-u`) and `--password` for MQTT broker authentication.
- Added end-to-end integration tests with an in-memory mock MQTT broker covering QoS 0, 1, and 2.

### Fixed

- Fixed premature disconnect on QoS 1 (`AtLeastOnce`) and QoS 2 (`ExactlyOnce`) by waiting for `PUBACK` and `PUBCOMP` packets from the broker before disconnecting.

## [0.1.0] - 2026-10-18

### Added

- Initial open-source release of `mqtt-sniper`, an asynchronous MQTT load testing CLI powered by Rust, Tokio, and `rumqttc`.
