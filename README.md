# 🎯 MQTT Sniper (v1.0.0)

[![Crates.io](https://img.shields.io/crates/v/mqtt-sniper.svg?style=for-the-badge&logo=rust)](https://crates.io/crates/mqtt-sniper)
[![Docker](https://img.shields.io/badge/docker-multi--arch%20(amd64%2Farm64)-blue?style=for-the-badge&logo=docker)](https://github.com/thitipongroo/mqtt-sniper/pkgs/container/mqtt-sniper)
[![MQTT](https://img.shields.io/badge/MQTT-v3.1.1%20%7C%20v5.0-660066?style=for-the-badge&logo=mqtt)](https://mqtt.org/)
[![CI/CD](https://img.shields.io/github/actions/workflow/status/thitipongroo/mqtt-sniper/ci.yml?branch=main&style=for-the-badge&logo=github)](https://github.com/thitipongroo/mqtt-sniper/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg?style=for-the-badge)](https://opensource.org/licenses/MIT)

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

### Option 2: Docker Container (Multi-Arch `linux/amd64` & `linux/arm64`)

```bash
# Pull and run the latest image from GitHub Container Registry
docker run --rm ghcr.io/thitipongroo/mqtt-sniper:latest --help
```

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
| `--output-json <PATH>` | - | - | Export comprehensive JSON benchmark report to disk |
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

## 📊 Benchmark Report & Terminal Output

### Real-Time Terminal Summary

```text
🚀 Starting MQTT Sniper (MQTT v5.0)...
Broker: tcp://127.0.0.1:1883
Spawning 500 concurrent Pub clients (200 msgs/client, QoS 1)...
[00:00:03] [████████████████████████████████████████] 100000/100000 (100%) | 29239.77/s | ETA: 0s
✅ Load test completed in 3.42s (MQTT v5.0)
📊 Total messages sent: 100000 / 100000 (Malformed injected: 0, Failed clients: 0, Success rate: 100.00%)
⚡ Throughput: 29239.77 msgs/sec
⏱️  Latency (ms): min=0.42 | avg=1.85 | p50=1.62 | p95=3.40 | p99=5.12 | max=11.80
💾 Saved JSON benchmark report to benchmark.json
```

### Machine-Readable JSON Export (`--output-json`)

```json
{
  "broker": "127.0.0.1",
  "port": 1883,
  "mode": "pub",
  "mqtt_version": 5,
  "tls": false,
  "clients": 500,
  "messages_per_client": 200,
  "duration_secs": 0,
  "qos": 1,
  "retain": false,
  "clean_session": true,
  "target_messages": 100000,
  "actual_sent": 100000,
  "malformed_sent": 0,
  "failed_clients": 0,
  "success_rate": 1.0,
  "slo_passed": true,
  "elapsed_secs": 3.42,
  "throughput_msgs_per_sec": 29239.77,
  "latency_min_ms": 0.42,
  "latency_avg_ms": 1.85,
  "latency_p50_ms": 1.62,
  "latency_p95_ms": 3.40,
  "latency_p99_ms": 5.12,
  "latency_max_ms": 11.80
}
```

---

## 🤖 GitHub Actions CI/CD Integration

Use MQTT Sniper in your GitHub Actions workflows to automatically detect performance regressions and verify broker SLO compliance on every commit:

```yaml
name: MQTT Performance Gate

on: [push, pull_request]

jobs:
  mqtt-benchmark:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Start EMQX Broker
        run: docker run -d --name emqx -p 1883:1883 emqx/emqx:5.8.0

      - name: Run MQTT Sniper Benchmark
        run: |
          docker run --rm --network host ghcr.io/thitipongroo/mqtt-sniper:latest \
            --broker 127.0.0.1 \
            --mqtt-version 5 \
            --clients 500 \
            --messages 100 \
            --qos 1 \
            --max-p95-ms 25.0 \
            --min-success-rate 0.999 \
            --output-json report.json

      - name: Archive Benchmark Artifacts
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: benchmark-report
          path: report.json
```

---

## 🔗 Related Projects in the Ecosystem

* **[sentinel-iot-platform](https://github.com/thitipongroo/sentinel-iot-platform)**: Complete, enterprise IoT ingestion and device shadow orchestration platform.
* **[mqtt-mock-sensor](https://github.com/thitipongroo/mqtt-mock-sensor)**: Zero-config Node.js IoT sensor telemetry simulator (`npx mqtt-mock-sensor`) for realistic GPS, power, and environmental telemetry generation.

---

## 🤝 Contributing & Security

* Please see [CONTRIBUTING.md](CONTRIBUTING.md) for local development, formatting, and testing guidelines.
* Review our vulnerability disclosure policy in [SECURITY.md](SECURITY.md).

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
