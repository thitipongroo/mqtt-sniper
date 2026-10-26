# 🎯 MQTT Sniper

[![Crates.io](https://img.shields.io/crates/v/mqtt-sniper.svg?style=for-the-badge&logo=rust)](https://crates.io/crates/mqtt-sniper)
[![Docker](https://img.shields.io/badge/docker-multi--arch%20(amd64%2Farm64)-blue?style=for-the-badge&logo=docker)](https://github.com/thitipongroo/mqtt-sniper/pkgs/container/mqtt-sniper)
[![MQTT](https://img.shields.io/badge/MQTT-v3.1.1%20%7C%20v5.0-660066?style=for-the-badge&logo=mqtt)](https://mqtt.org/)
[![CI/CD](https://img.shields.io/github/actions/workflow/status/thitipongroo/mqtt-sniper/ci.yml?branch=main&style=for-the-badge&logo=github)](https://github.com/thitipongroo/mqtt-sniper/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg?style=for-the-badge)](https://opensource.org/licenses/MIT)
[![GitHub Sponsors](https://img.shields.io/github/sponsors/thitipongroo?style=flat-square&color=ea4aaa&logo=github)](https://github.com/sponsors/thitipongroo)

**MQTT Sniper** is an enterprise-grade, high-performance MQTT load testing and benchmarking tool written in Rust. Powered by the **Tokio** asynchronous runtime and **rumqttc**, it can simulate hundreds of thousands of concurrent IoT devices across **MQTT v3.1.1** and **MQTT v5.0** brokers from a single machine with bounded O(1) memory footprint.

Whether testing broker throughput, connection handshake storms, end-to-end fan-out latency, or Dead-Letter Queue (DLQ) pipeline resilience, MQTT Sniper provides real-time HDR histogram latency statistics and automated CI/CD SLO quality gates.

---

## 🚀 Key Capabilities

* **⚡ Massive Asynchronous Concurrency**: Spawns tens of thousands of lightweight Tokio green-threads instead of heavy OS threads, with an active connection limiter (`--max-connections`) to prevent file descriptor exhaustion.
* **📡 Dual Protocol Engine (MQTT v3.1.1 & v5.0)**: Native protocol negotiation supporting standard MQTT 3.1.1 (`--mqtt-version 4` or `3`) and modern MQTT 5.0 (`--mqtt-version 5`).
* **🔄 Triple Operational Modes**:
  * **Publish Mode (`--mode pub`)**: High-throughput message ingestion with strict acknowledgement tracking for QoS 0, QoS 1 (`PUBACK`), and QoS 2 (`PUBCOMP`).
  * **Subscribe Mode (`--mode sub`)**: Fan-out subscriber benchmarking with automatic multi-key (`ts`, `timestamp`, `time`) and multi-precision (seconds, ms, microseconds) end-to-end latency extraction.
  * **Connection Storm Mode (`--mode conn`)**: Handshake benchmark measuring `CONNECT` -> `CONNACK` latency, supporting persistent sessions (`--clean-session=false`) and long-lived connection soak tests.
* **⏱️ Bounded O(1) HDR Histograms**: Per-client microsecond-precision `hdrhistogram` merged on completion, ensuring memory usage remains constant even during 24-hour soak tests (`--duration-secs`).
* **🔒 Enterprise Security (TLS & mTLS)**: Encrypted `mqtts://` connections using native root certificates (`--tls`), custom CA certificate bundles (`--ca`), and mutual client certificate authentication (`--cert`, `--key`).
* **🧬 Dynamic Per-Device Authentication & Topics**: Variable interpolation of `{device_id}` inside `--topic`, `--username`, and `--password`, with customizable `--start-number` (`-n`) and `--client-prefix` for distributed worker clusters.
* **🎲 Rich Telemetry Templates**: Dynamic template evaluation supporting `{device_id}`, `{seq}`, `{timestamp}`, `{timestamp_secs}`, `{timestamp_us}`, `{random_int}`, `{random_float}`, `{random_bool}`, and `{uuid}` loaded inline (`--payload`) or from disk (`--payload-file`).
* **💥 Chaos & DLQ Fault Injection**: Inject a configurable percentage of corrupted non-parseable payloads (`--malformed-rate 0.05`) to stress-test ingest pipeline error handling and Dead-Letter Queues.
* **🚦 Automated CI/CD SLO Quality Gates**: Exit with code `1` when p95 latency exceeds limits (`--max-p95-ms`) or message delivery success rate drops (`--min-success-rate`).
* **🛑 Graceful Shutdown**: Intercepts `Ctrl+C` (SIGINT) to cleanly disconnect active MQTT clients and immediately compute and export full latency reports.

---

## 📦 Installation

### Option 1: Cargo (Recommended)

```bash
cargo install mqtt-sniper
```

### Option 2: Docker Quickstart (Multi-Arch `linux/amd64` & `linux/arm64`)

Production-ready, ultra-lightweight (<20MB) multi-architecture container images are automatically built and published to GitHub Container Registry (GHCR):

```bash
# 1. Pull the official multi-arch image
docker pull ghcr.io/thitipongroo/mqtt-sniper:latest

# 2. Run benchmark directly against a local broker using host networking
docker run --rm --network host ghcr.io/thitipongroo/mqtt-sniper:latest \
  --broker localhost \
  --clients 100 \
  --messages 10

# 3. Mount current directory to pass template files and export JSON/Prometheus reports
docker run --rm --network host \
  -v "$(pwd)":/work -w /work \
  ghcr.io/thitipongroo/mqtt-sniper:latest \
  --broker 127.0.0.1 \
  --output json \
  --output-file report.json
```

*Runs as unprivileged non-root user `sniper` (UID 10001) with pre-bundled `ca-certificates` and `tzdata`.*

### Option 3: Pre-compiled GitHub Release Binaries

Download standalone pre-built binaries for Linux, macOS (Apple Silicon / Intel), and Windows from the [Releases](https://github.com/thitipongroo/mqtt-sniper/releases) page.

### Option 4: Build from Source

```bash
git clone https://github.com/thitipongroo/mqtt-sniper.git
cd mqtt-sniper
cargo build --release
./target/release/mqtt-sniper --help
```

---

## 💡 Practical Scenarios & Recipes

### 1. High-Throughput MQTT v5.0 Telemetry Ingestion (QoS 1 with SLO Gate)

Simulate 1,000 MQTT v5.0 sensors streaming 100 messages each, asserting that p95 latency remains under `30ms` and delivery success rate is at least `99.9%`:

```bash
mqtt-sniper \
  --broker 127.0.0.1 \
  --port 1883 \
  --mqtt-version 5 \
  --clients 1000 \
  --messages 100 \
  --qos 1 \
  --payload-file examples/telemetry-v2.json \
  --max-p95-ms 30.0 \
  --min-success-rate 0.999 \
  --output-json benchmark-v5.json
```

### 2. Multi-Tenant IoT Platform Simulation (Dynamic Auth + DLQ Chaos)

Simulate devices `sensor-1` through `sensor-200` authenticating with per-device credentials, injecting `5%` malformed JSON payloads to verify pipeline Dead-Letter Queue (DLQ) routing:

```bash
mqtt-sniper \
  --broker emqx.iot.internal \
  --port 1883 \
  --clients 200 \
  --start-number 1 \
  --client-prefix "sensor-" \
  --username "sensor-{device_id}" \
  --password "secret-{device_id}" \
  --topic "tenants/org-1/devices/sensor-{device_id}/telemetry" \
  --qos 1 \
  --malformed-rate 0.05 \
  --duration-secs 120
```

### 3. Connection Handshake Storm & Soak Test (`--mode conn`)

Measure raw broker handshake capability by opening 5,000 concurrent persistent sessions (`clean_session=false`) and holding connections open for 60 seconds:

```bash
mqtt-sniper \
  --broker 127.0.0.1 \
  --mode conn \
  --mqtt-version 5 \
  --clients 5000 \
  --clean-session=false \
  --duration-secs 60 \
  --ramp-up-ms 1
```

### 4. End-to-End Fan-out Subscriber Latency (`--mode sub`)

Subscribe 500 fan-out consumers to test message delivery and automatically extract end-to-end latency from the payload timestamp:

```bash
mqtt-sniper \
  --broker 127.0.0.1 \
  --mode sub \
  --clients 500 \
  --topic "telemetry/broadcast/#" \
  --messages 50 \
  --qos 1
```

### 5. Mutual TLS (mTLS) Enterprise Authentication

Connect over encrypted TLS with custom private CA verification and client certificate authentication:

```bash
mqtt-sniper \
  --broker mqtts.iot.enterprise.com \
  --port 8883 \
  --tls \
  --ca certs/ca.pem \
  --cert certs/client-cert.pem \
  --key certs/client-key.pem \
  --clients 100 \
  --messages 50 \
  --qos 1
```

---

## ⚙️ CLI Reference

```text
Usage: mqtt-sniper [OPTIONS]
```

### Connection & Target Configuration

| Option | Short | Default | Description |
| :--- | :---: | :---: | :--- |
| `--broker <HOST>` | `-b` | `localhost` | Target MQTT broker hostname or IP address |
| `--port <PORT>` | `-p` | `1883` | Target MQTT broker port (`1883` for plaintext, `8883` for TLS) |
| `--mode <MODE>` | - | `pub` | Benchmark mode: `pub` (publish), `sub` (subscribe), or `conn` (connection storm) |
| `--mqtt-version <VER>` | - | `4` | MQTT protocol version: `3` or `4` (MQTT v3.1.1), `5` (MQTT v5.0) |
| `--tls` | - | `false` | Enable SSL/TLS encryption (`mqtts://`) |
| `--ca <PATH>` | - | - | Path to custom CA certificate file (PEM) |
| `--cert <PATH>` | - | - | Path to client certificate file (PEM) for mTLS |
| `--key <PATH>` | - | - | Path to client private key file (PEM) for mTLS |

### Client Fleet & Identity

| Option | Short | Default | Description |
| :--- | :---: | :---: | :--- |
| `--clients <COUNT>` | `-c` | `100` | Number of concurrent simulated IoT clients |
| `--start-number <INDEX>` | `-n` | `0` | Starting index for client IDs and `{device_id}` interpolation |
| `--client-prefix <PREFIX>` | - | `sniper-client-` | Prefix for generated MQTT client IDs (`<prefix><device_id>`) |
| `--max-connections <LIMIT>` | - | `0` | Max active concurrent connections semaphore (`0` = unlimited) |
| `--ramp-up-ms <MS>` | - | `0` | Delay (ms) between spawning consecutive clients (prevents SYN flood) |
| `--username <USER>` | `-u` | - | MQTT username for authentication (supports `{device_id}`) |
| `--password <PASS>` | - | - | MQTT password for authentication (supports `{device_id}`) |

### Workload & Protocol Controls

| Option | Short | Default | Description |
| :--- | :---: | :---: | :--- |
| `--topic <TOPIC>` | `-t` | `sensor/sniper` | Target MQTT topic (supports `{device_id}` placeholder) |
| `--qos <LEVEL>` | `-q` | `0` | Quality of Service level (`0` = AtMostOnce, `1` = AtLeastOnce, `2` = ExactlyOnce) |
| `--messages <COUNT>` | `-m` | `10` | Messages published or received per client (ignored in duration mode) |
| `--duration-secs <SECS>` | `-d` | `0` | Soak test duration in seconds (`0` = use `--messages` count) |
| `--interval-ms <MS>` | `-i` | `0` | Delay (ms) between consecutive messages per client |
| `--retain` | - | `false` | Publish messages with the MQTT retain flag set |
| `--clean-session <BOOL>` | - | `true` | Clean session / clean start (`false` retains subscription state on broker) |
| `--keep-alive <SECS>` | - | `5` | MQTT keep-alive interval in seconds |

### Payload Templating & Chaos Injection

| Option | Short | Default | Description |
| :--- | :---: | :---: | :--- |
| `--payload <STRING>` | - | *see below* | Inline payload template string with dynamic variable placeholders |
| `--payload-file <PATH>` | - | - | Path to an external payload template file (overrides `--payload`) |
| `--payload-size <BYTES>` | - | `0` | Minimum payload byte size (padded with `x` bytes if rendered string is smaller) |
| `--malformed-rate <RATE>` | - | `0.0` | Fraction of messages (`0.0` to `1.0`) to corrupt for DLQ/chaos resilience testing |

### CI/CD Quality Gates & Reporting

| Option | Short | Default | Description |
| :--- | :---: | :---: | :--- |
| `--max-p95-ms <MS>` | - | - | SLO Gate: Max allowed p95 latency in ms (exits with code 1 if breached) |
| `--min-success-rate <RATIO>`| - | - | SLO Gate: Min required success rate `0.0` to `1.0` (exits with code 1 if breached) |
| `--output <FORMAT>` | `-o` | `text` | Output format: `text` (human summary), `json` (machine schema), or `prometheus` |
| `--output-file <PATH>` | - | - | Path to write the output report file directly to disk |
| `--quiet` / `--silent` | `-s` | `false` | Suppress interactive progress bar and pre-flight banners for automated CI jobs |
| `--output-json <PATH>` | - | - | Export comprehensive JSON benchmark report to disk (legacy alias) |
| `--help` | `-h` | - | Print command-line help |
| `--version` | `-V` | - | Print version information |

---

## 🧬 Dynamic Payload Placeholders

When publishing payloads via `--payload` or `--payload-file`, MQTT Sniper evaluates dynamic variables on every single message:

| Variable | Type | Description | Sample Output |
| :--- | :--- | :--- | :--- |
| `{device_id}` | Integer | Current client index (`--start-number` + offset) | `42` |
| `{seq}` | Integer | Incremental message sequence counter per client | `1` |
| `{timestamp}` | Integer | Epoch timestamp in milliseconds | `1713400000000` |
| `{timestamp_secs}` | Integer | Epoch timestamp in seconds | `1713400000` |
| `{timestamp_us}` | Integer | Epoch timestamp in microseconds | `1713400000000000` |
| `{random_int}` | Integer | Uniform pseudo-random integer between `0` and `100` | `84` |
| `{random_int:min:max}` | Integer | Uniform pseudo-random integer within inclusive range `min..=max` | `{random_int:2000:5000}` -> `3412` |
| `{random_float}` | Float | Uniform pseudo-random float between `20.00` and `40.00` (2 decimals) | `26.85` |
| `{random_float:min:max}` | Float | Uniform pseudo-random float between `min` and `max` (2 decimals) | `{random_float:85.0:120.0}` -> `104.37` |
| `{random_float:min:max:prec}` | Float | Uniform pseudo-random float between `min` and `max` with `prec` decimals | `{random_float:0.01:0.09:3}` -> `0.048` |
| `{random_bool}` | Boolean | Random boolean value (`true` or `false`) | `true` |
| `{uuid}` | String | RFC 4122 UUID v4 unique identifier | `6ba7b810-9dad-11d1-80b4-00c04fd430c8` |

### Ready-to-Use Payload Templates (`examples/`)

* **`examples/telemetry-v2.json`**: Standard multi-metric sensor telemetry.
* **`examples/device-shadow.json`**: AWS/Azure IoT-style Device Shadow reported state payload.
* **`examples/anomaly-alert.json`**: High-temperature/vibration anomaly payload designed for AI Alert & Anomaly load triggers.

### Sample External Payload File (`examples/telemetry-v2.json`)

```json
{
  "schema_version": "2.0",
  "device_id": "sensor-{device_id}",
  "seq": {seq},
  "timestamp_ms": {timestamp},
  "timestamp_us": {timestamp_us},
  "trace_id": "{uuid}",
  "metrics": {
    "temperature_c": {random_float},
    "battery_pct": {random_int},
    "alarm_active": {random_bool}
  }
}
```

---

## 📊 Observability & Structured Outputs

MQTT Sniper is built from the ground up for deep observability, headless automated testing, and seamless integration with CI/CD quality pipelines and Prometheus/Grafana monitoring stacks.

### 1. Structured JSON Report Schema (`--output json`)

Run with `--output json` to output parseable, machine-readable JSON to `stdout` or save it directly using `--output-file report.json`:

```bash
mqtt-sniper \
  --broker 127.0.0.1 \
  --port 1883 \
  --clients 1000 \
  --messages 100 \
  --qos 1 \
  --max-p95-ms 50.0 \
  --min-success-rate 0.99 \
  --output json \
  --output-file benchmark-report.json
```

```json
{
  "timestamp": "2026-10-24T15:30:00Z",
  "target": {
    "broker": "mqtt://127.0.0.1:1883",
    "clients": 1000,
    "qos": 1
  },
  "metrics": {
    "total_messages": 100000,
    "success_count": 99980,
    "failure_count": 20,
    "success_rate_percent": 99.98,
    "duration_seconds": 10.45,
    "throughput_msg_per_sec": 9569.37
  },
  "latency_ms": {
    "min": 1.2,
    "p50": 3.4,
    "p90": 8.1,
    "p95": 12.5,
    "p99": 24.8,
    "max": 85.0
  },
  "sla": {
    "passed": true,
    "max_p95_threshold_ms": 50.0,
    "min_success_rate_threshold": 99.0
  }
}
```

#### Parsing with `jq` in CI Scripts

Combine with `jq` to extract metrics or enforce custom assertions:

```bash
# Extract delivery success rate percentage
mqtt-sniper --quiet --output json | jq .metrics.success_rate_percent

# Extract 95th percentile latency (ms)
mqtt-sniper --quiet --output json | jq .latency_ms.p95

# Automated Quality Gate script assertion
REPORT=$(mqtt-sniper --quiet --output json --max-p95-ms 30.0)
STATUS=$(echo "$REPORT" | jq -r .sla.passed)
if [ "$STATUS" != "true" ]; then
  echo "❌ Performance regression: SLA breached!"
  exit 1
fi
```

### 2. Prometheus OpenMetrics Export (`--output prometheus`)

Generate standard Prometheus text exposition format for scraping or piping directly to Prometheus Pushgateway:

```bash
mqtt-sniper --broker 127.0.0.1 --clients 500 --output prometheus
```

```prometheus
# HELP mqtt_sniper_total_messages Total messages in benchmark target
# TYPE mqtt_sniper_total_messages counter
mqtt_sniper_total_messages 50000
# HELP mqtt_sniper_success_count Successful messages or connections
# TYPE mqtt_sniper_success_count counter
mqtt_sniper_success_count 50000
# HELP mqtt_sniper_failure_count Failed messages or clients
# TYPE mqtt_sniper_failure_count counter
mqtt_sniper_failure_count 0
# HELP mqtt_sniper_success_rate_percent Success rate percentage
# TYPE mqtt_sniper_success_rate_percent gauge
mqtt_sniper_success_rate_percent 100.00
# HELP mqtt_sniper_duration_seconds Total benchmark duration in seconds
# TYPE mqtt_sniper_duration_seconds gauge
mqtt_sniper_duration_seconds 4.21
# HELP mqtt_sniper_throughput_msg_per_sec Messages published per second
# TYPE mqtt_sniper_throughput_msg_per_sec gauge
mqtt_sniper_throughput_msg_per_sec 11876.48
# HELP mqtt_sniper_latency_min_ms Minimum latency in milliseconds
# TYPE mqtt_sniper_latency_min_ms gauge
mqtt_sniper_latency_min_ms 0.85
# HELP mqtt_sniper_latency_p50_ms P50 latency in milliseconds
# TYPE mqtt_sniper_latency_p50_ms gauge
mqtt_sniper_latency_p50_ms 2.15
# HELP mqtt_sniper_latency_p90_ms P90 latency in milliseconds
# TYPE mqtt_sniper_latency_p90_ms gauge
mqtt_sniper_latency_p90_ms 4.50
# HELP mqtt_sniper_latency_p95_ms P95 latency in milliseconds
# TYPE mqtt_sniper_latency_p95_ms gauge
mqtt_sniper_latency_p95_ms 6.80
# HELP mqtt_sniper_latency_p99_ms P99 latency in milliseconds
# TYPE mqtt_sniper_latency_p99_ms gauge
mqtt_sniper_latency_p99_ms 11.20
# HELP mqtt_sniper_latency_max_ms Maximum latency in milliseconds
# TYPE mqtt_sniper_latency_max_ms gauge
mqtt_sniper_latency_max_ms 24.50
# HELP mqtt_sniper_sla_passed Whether the benchmark satisfied SLA quality gates (1 = passed, 0 = breached)
# TYPE mqtt_sniper_sla_passed gauge
mqtt_sniper_sla_passed 1
```

### 3. Human-Readable Terminal Summary (`--output text`)

```text
🚀 Starting MQTT Sniper (MQTT v5.0)...
Broker: tcp://127.0.0.1:1883
Spawning 500 concurrent Pub clients (200 msgs/client, QoS 1)...
[00:00:03] [████████████████████████████████████████] 100000/100000 (100%) | 29239.77/s | ETA: 0s
✅ Load test completed in 3.42s (MQTT v5.0)
📊 Total messages sent: 100000 / 100000 (Malformed injected: 0, Failed clients: 0, Success rate: 100.00%)
⚡ Throughput: 29239.77 msgs/sec
⏱️  Latency (ms): min=0.42 | avg=1.85 | p50=1.62 | p90=2.85 | p95=3.40 | p99=5.12 | max=11.80
```

---

## 🤖 CI/CD Quality Gate (GitHub Actions)

Integrate MQTT Sniper directly into your pull requests and release pipelines to enforce latency SLAs and prevent throughput regressions before deploying brokers to production.

A ready-to-use sample workflow is provided at [`.github/workflows/ci-sla-gate.yml`](.github/workflows/ci-sla-gate.yml).

### GitHub Action Integration (`thitipongroo/mqtt-sniper@v1.1.0`)

You can reference `thitipongroo/mqtt-sniper@v1.1.0` in your workflow:

```yaml
name: 'Pulse Sniper Broker SLA Gate'

on:
  push:
    branches: [ main ]
  pull_request:
    branches: [ main ]
  workflow_dispatch:

jobs:
  mqtt-sla-benchmark:
    name: 'MQTT Performance & Latency SLA Gate'
    runs-on: ubuntu-latest
    services:
      mosquitto:
        image: eclipse-mosquitto:2
        ports:
          - 1883:1883

    steps:
      - name: Checkout Code
        uses: actions/checkout@v4

      - name: Run Single-Node MQTT Sniper SLA Check
        uses: thitipongroo/mqtt-sniper@v1.1.0
        with:
          broker-url: 'localhost:1883'
          clients: '100'
          count: '20'
          max-p95-ms: '50'
          min-success-rate: '99.9'
          output-format: 'json'
          output-file: 'benchmark-report.json'

      - name: Archive Benchmark Report
        uses: actions/upload-artifact@v4
        if: always()
        with:
          name: mqtt-sniper-benchmark-report
          path: benchmark-report.json
```

### Action Configuration Inputs

| Input | Required | Default | Description |
| :--- | :---: | :---: | :--- |
| `broker-url` | **Yes** | — | Target MQTT broker host or URL (e.g. `localhost:1883`, `mqtt://127.0.0.1:1883`). |
| `clients` | No | `100` | Number of concurrent simulated IoT clients. |
| `count` | No | `10` | Number of messages published or received per client. |
| `max-p95-ms` | No | `""` | **SLA Assertion**: Maximum allowable p95 latency threshold in ms (fails job if exceeded). |
| `min-success-rate` | No | `""` | **SLA Assertion**: Minimum required delivery rate (e.g. `99.9` for 99.9% or `0.999`). |
| `duration` | No | `0` | Soak test duration in seconds (`0` uses message `count`). |
| `output-format` | No | `json` | Report output format: `json`, `text`, or `prometheus`. |
| `output-file` | No | `mqtt-sniper-report.json` | Destination file path for benchmark telemetry report. |

#### Action Outputs

| Output | Description |
| :--- | :--- |
| `report-json` | File path of generated JSON/Prometheus benchmark metrics. |
| `status` | SLA quality gate evaluation status (`Pass` or `Fail`). |

---

## 🔗 Related Projects in the Ecosystem

* **[sentinel-iot-platform](https://github.com/thitipongroo/sentinel-iot-platform)**: Complete, enterprise IoT ingestion and device shadow orchestration platform.
* **[mqtt-mock-sensor](https://github.com/thitipongroo/mqtt-mock-sensor)**: Zero-config Node.js IoT sensor telemetry simulator (`npx mqtt-mock-sensor`) for realistic GPS, power, and environmental telemetry generation.

---

## 💖 Sponsoring & Enterprise Backers

`mqtt-sniper` is an open-source, high-performance load testing engine built to help teams build resilient IoT infrastructures. Sponsoring helps ensure continued active maintenance, performance optimizations, and future enhancements.

If your organization relies on `mqtt-sniper` for IoT benchmarking or CI/CD SLA validation, consider becoming a sponsor:

- 🥉 **Bronze Sponsor ($100/mo):** Company logo featured in this README and priority issue handling.
- 🥈 **Silver Sponsor ($300/mo):** Medium logo featured, priority bug fixes, and direct advisory channel.
- 🥇 **Gold / Enterprise Sponsor ($1,000/mo):** Hero logo placement, custom feature prioritization, and monthly architecture review.

👉 **[Become a Sponsor on GitHub](https://github.com/sponsors/thitipongroo)**

---

### ⚡ Need 1M - 10M Concurrent Sockets or an Independent Broker Audit?
For enterprise-grade distributed mesh testing across multiple cloud regions or pre-launch broker certification:
👉 **[Explore Testing-as-a-Service (TaaS) & Enterprise Mesh](https://pulseiot-cloud.com/#taas)**

---

## 🤝 Contributing & Security

* Please see [CONTRIBUTING.md](CONTRIBUTING.md) for local development, formatting, and testing guidelines.
* Review our vulnerability disclosure policy in [SECURITY.md](SECURITY.md).

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
