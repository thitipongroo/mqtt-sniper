# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.0] - 2026-10-24

* Added - **Production Multi-Stage Dockerfile & GHCR Multi-Arch Publishing**: Built an ultra-lightweight (<20MB) multi-stage Alpine Docker image running under non-root user `sniper` (UID 10001) with pre-installed `ca-certificates` and `tzdata`, backed by automated multi-architecture (`linux/amd64`, `linux/arm64`) GitHub Actions (`.github/workflows/docker-publish.yml`).
* Added - **Reusable Composite GitHub Action (`action.yml`)**: Root-level composite action allowing CI/CD workflows to assert broker SLAs (`--max-p95-ms`, `--min-success-rate`) with automated quality gating, outputs (`status`, `report-json`), and Mosquitto test example (`.github/workflows/sla-check-example.yml`).
* Added - **Observability & Structured Formats (`--output`, `--output-file`, `--quiet`)**: Added `--output <text|json|prometheus>` and `--output-file <PATH>` supporting structured JSON schema with ISO-8601 timestamps, percentiles (p50, p90, p95, p99), and standard Prometheus OpenMetrics text format.
* Added - **Broker URL Auto-Normalization**: Added automatic scheme parsing (`mqtt://`, `mqtts://`, `tcp://`, `ssl://`) in `--broker` to auto-configure ports and TLS flags.

## [1.0.0] - 2026-10-19

* Added - **MQTT v5.0 Protocol Support (`--mqtt-version 5`)**: Added full support for MQTT v5.0 (`rumqttc::v5`) across `pub`, `sub`, and `conn` modes while maintaining backward compatibility with MQTT v3.1.1 (`--mqtt-version 3` or `4`).
* Added - **Multi-Architecture Docker Image Publishing (`linux/amd64`, `linux/arm64`)**: Upgraded the GitHub Actions `publish-docker` workflow with QEMU and Docker Buildx to publish native multi-arch container images to GHCR (`ghcr.io/thitipongroo/mqtt-sniper`).
* Added - **Payload Template Examples (`examples/`)**: Added ready-to-use JSON payload templates (`examples/telemetry-v2.json` and `examples/device-shadow.json`) for `--payload-file`.
* Added - **Open-Source Governance & Supply-Chain Security**: Added `.github/dependabot.yml` (automated Cargo and GitHub Actions updates), `SECURITY.md`, and `CONTRIBUTING.md`.

## [0.6.0] - 2026-10-19

* Added - **Bounded $O(1)$ Memory Per-Client HDR Histograms**: Replaced unbounded per-client `Vec<u64>` latency buffers with per-client `Histogram<u64>` instances merged on completion, ensuring constant memory usage during multi-hour soak tests.
* Added - **Graceful Shutdown (`Ctrl+C` / `SIGINT`)**: Added signal handling via `tokio::signal::ctrl_c()` to cleanly disconnect active MQTT clients and output full latency statistics and JSON reports when interrupted.
* Added - **Connection Storm Mode (`--mode conn`) & Persistent Sessions (`--clean-session`)**: Added `conn` benchmark mode to measure `CONNECT` -> `CONNACK` handshake latency and hold concurrent connections open.
* Added - **CI/CD Automated SLO Quality Gates (`--max-p95-ms`, `--min-success-rate`)**: Added automatic SLO evaluation (`slo_passed`, `success_rate`) that exits with code `1` when latency or delivery rate thresholds are breached.

## [0.5.0] - 2026-10-18

* Added - **Client Start Index & Prefix (`--start-number` / `-n`, `--client-prefix`)**: Added configurable starting index for `{device_id}` and client IDs (e.g., `-n 1` for 1-based device pools `sensor-1..sensor-N` or distributed multi-node load sharding).
* Added - **Dynamic Per-Device Credentials**: Enabled `{device_id}` variable interpolation inside `--username` and `--password` for brokers enforcing per-device authentication and ACLs.
* Added - **External Payload Template Files (`--payload-file`)**: Load JSON/text payload templates directly from disk with full dynamic placeholder interpolation.
* Added - **Chaos / DLQ Fault Injection (`--malformed-rate`)**: Added configurable malformed payload injection rate (`0.0` to `1.0`) to stress-test Dead-Letter Queue (DLQ) routing and consumer resilience, tracked via `malformed_sent` in terminal and JSON reports.
* Added - **Multi-Precision Timestamps & Boolean Placeholders**: Added `{timestamp_secs}` (epoch seconds), `{timestamp_us}` (epoch microseconds), and `{random_bool}` (`true`/`false`) placeholders.
* Added - **Multi-Key & Auto-Unit Subscribe Latency**: Enhanced `--mode sub` end-to-end latency calculation to recognize `"ts"`, `"timestamp"`, and `"time"` fields and automatically detect epoch seconds, milliseconds, or microseconds.

## [0.4.0] - 2026-10-18

* Added - **Duration-based Soak Testing (`--duration-secs` / `-d`)**: Added time-based benchmark execution mode for both Publish and Subscribe workloads.
* Added - **Dynamic Random Telemetry Placeholders**: Added `{random_int}` (`0..=100`), `{random_float}` (`20.00..=40.00`), and `{uuid}` (UUID v4) placeholders in payload templates.
* Added - **Advanced MQTT Protocol Options**: Added `--retain` flag for publishing retained messages and `--keep-alive` for configurable MQTT keep-alive intervals.
* Added - **Automated Docker Image Publishing (GHCR)**: Added `publish-docker` job in GitHub Actions release workflow to automatically publish `ghcr.io/thitipongroo/mqtt-sniper` container images.

## [0.3.0] - 2026-10-18

* Added - **SSL/TLS & mTLS Support**: Added `--tls` for encrypted `mqtts://` connections using OS native root certificates, `--ca` for custom CA certificates (PEM), and `--cert` / `--key` for mutual TLS (mTLS) client authentication.
* Added - **Subscribe / Fan-out Benchmark Mode**: Added `--mode pub|sub` (`Mode::Pub` and `Mode::Sub`) to benchmark both message ingestion and fan-out subscriber delivery, with automatic end-to-end latency extraction when payloads contain a `"ts"` epoch timestamp.
* Added - **Active Connection Limiter**: Added `--max-connections` backed by `tokio::sync::Semaphore` to prevent OS file descriptor exhaustion (`Too many open files`) during massive client simulations.
* Added - integration test covering Subscribe mode (`--mode sub`) and `--max-connections`.

* Changed - **Clean Architecture Refactoring**: Modularized codebase into `src/lib.rs`, `src/main.rs`, `src/cli.rs`, `src/payload.rs`, `src/report.rs`, and `src/runner.rs`.
* Changed - Extended `BenchmarkReport` JSON schema and terminal summary to include `mode` and `tls` status.

## [0.2.0] - 2026-10-18

* Added -  **Real-time Terminal Progress Bar**: Integrated `indicatif` to display live progress, message throughput (`msgs/sec`), and ETA.
* Added -  **HDR Histogram Latency Metrics**: Integrated `hdrhistogram` to record per-message latencies and report `min`, `avg`, `p50`, `p95`, `p99`, and `max` in milliseconds.
* Added -  **JSON Report Export**: Added `--output-json <PATH>` using `serde` and `serde_json` for CI/CD pipeline integration.

## [0.1.1] - 2026-10-18

* Added - `--ramp-up-ms` to stagger client connection spawning and prevent TCP SYN floods.
* Added - `--interval-ms` (`-i`) to control delay between consecutive messages per client.
* Added - `--payload` template with `{device_id}`, `{seq}`, and `{timestamp}` placeholders, plus `--payload-size` padding.
* Added - `--username` (`-u`) and `--password` for MQTT broker authentication.
* Added - end-to-end integration tests with an in-memory mock MQTT broker covering QoS 0, 1, and 2.

* Fixed - premature disconnect on QoS 1 (`AtLeastOnce`) and QoS 2 (`ExactlyOnce`) by waiting for `PUBACK` and `PUBCOMP` packets from the broker before disconnecting.

## [0.1.0] - 2026-10-18

* Initial open-source release of `mqtt-sniper`, an asynchronous MQTT load testing CLI powered by Rust, Tokio, and `rumqttc`.
