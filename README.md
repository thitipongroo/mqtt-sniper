# 🎯 MQTT Sniper

![Rust](https://img.shields.io/badge/rust-%23000000.svg?style=for-the-badge&logo=rust&logoColor=white)
![Tokio](https://img.shields.io/badge/Tokio-Async-blue?style=for-the-badge)
![MQTT](https://img.shields.io/badge/MQTT-3.1.1%20%7C%205.0-660066?style=for-the-badge)
![License](https://img.shields.io/badge/license-MIT-green?style=for-the-badge)

**MQTT Sniper** is a blazing-fast, memory-efficient MQTT load testing CLI written in Rust. Built on top of the **Tokio** asynchronous runtime and **rumqttc**, it can simulate tens of thousands of concurrent IoT devices (`pub`, `sub`, and `conn` modes) from a single machine with bounded $O(1)$ memory overhead per client.

---

## ✨ Key Features

* **🚀 Massive Concurrency**: Spawns thousands of lightweight Tokio green-threads instead of heavy OS threads, protected by an optional `--max-connections` semaphore limiter.
* **📡 MQTT v3.1.1 & v5.0 Protocol Support**: Switch seamlessly between MQTT v3.1.1 (`--mqtt-version 4`) and MQTT v5.0 (`--mqtt-version 5`).
* **🔄 Publish, Subscribe & Connection Storm Modes**:
  * `--mode pub`: High-throughput message ingestion benchmarking with QoS 0, 1 (`PUBACK`), and 2 (`PUBCOMP`) acknowledgement tracking.
  * `--mode sub`: Fan-out subscriber benchmarking with automatic end-to-end latency extraction from `"ts"`, `"timestamp"`, or `"time"` JSON fields (auto-detecting seconds, milliseconds, or microseconds).
  * `--mode conn`: Connection storm and session persistence (`--clean-session=false`) benchmarking (`CONNECT` -> `CONNACK` handshake latency).
* **⏱️ Bounded $O(1)$ HDR Histogram Latency**: Per-client `hdrhistogram` instances record microsecond-precision latencies (`min`, `avg`, `p50`, `p95`, `p99`, `max`) with constant memory usage during multi-hour soak tests (`--duration-secs`).
* **🔒 SSL/TLS & Mutual TLS (mTLS)**: Native support for `mqtts://` via system root certificates (`--tls`), custom CA certificates (`--ca`), and client certificate authentication (`--cert`, `--key`).
* **🧬 Dynamic Telemetry & Per-Device Auth**:
  * Supports `{device_id}` interpolation in `--topic`, `--username`, and `--password` alongside configurable `--start-number` (`-n`) and `--client-prefix`.
  * Supports `{device_id}`, `{seq}`, `{timestamp}`, `{timestamp_secs}`, `{timestamp_us}`, `{random_int}`, `{random_float}`, `{random_bool}`, and `{uuid}` in `--payload` or `--payload-file`.
* **💥 Chaos / DLQ Fault Injection**: Inject a deterministic percentage of malformed payloads via `--malformed-rate` (`0.0` to `1.0`) to verify Dead-Letter Queue (DLQ) pipelines.
* **🚦 CI/CD SLO Quality Gates & Graceful Shutdown**: Enforce latency (`--max-p95-ms`) and reliability (`--min-success-rate`) thresholds with automatic non-zero exit codes, JSON export (`--output-json`), and `Ctrl+C` graceful shutdown.

---

## 📦 Installation

### Option 1: Install via Cargo (Crates.io)

```bash
cargo install mqtt-sniper
```

### Option 2: Run via Docker (Multi-Arch linux/amd64 & linux/arm64)

```bash
docker run --rm ghcr.io/thitipongroo/mqtt-sniper:latest --help
```

### Option 3: Build from Source

```bash
git clone https://github.com/thitipongroo/mqtt-sniper.git
cd mqtt-sniper
cargo build --release
./target/release/mqtt-sniper --help
```

## 💡 Example Scenarios

### 1. MQTT v5.0 High-Throughput Publish with SLO Quality Gates

```bash
mqtt-sniper \
  --broker 127.0.0.1 \
  --mqtt-version 5 \
  --clients 500 \
  --messages 200 \
  --qos 1 \
  --payload-file examples/telemetry-v2.json \
  --max-p95-ms 50.0 \
  --min-success-rate 0.999 \
  --output-json report.json
```

### 2. Multi-Tenant IoT Platform Simulation (Per-Device Auth + DLQ Chaos)

```bash
mqtt-sniper \
  --broker localhost \
  --port 1883 \
  --clients 100 \
  --start-number 1 \
  --client-prefix "sensor-" \
  --username "sensor-{device_id}" \
  --password "secret-{device_id}" \
  --topic "tenants/org-1/devices/sensor-{device_id}/telemetry" \
  --qos 1 \
  --malformed-rate 0.05 \
  --duration-secs 60
```

### 3. Connection Storm & Persistent Session Benchmark (--mode conn)

```bash
mqtt-sniper \
  --broker 127.0.0.1 \
  --mode conn \
  --mqtt-version 5 \
  --clients 2000 \
  --clean-session=false \
  --duration-secs 30 \
  --ramp-up-ms 1
```

## ⚙️ CLI Options

| Flag | Short | Default | Description |
| :--- | :---: | :---: | :--- |
| `--broker` | `-b` | `localhost` | Target MQTT broker hostname or IP address |
| `--port` | `-p` | `1883` | Target MQTT broker TCP port |
| `--mode` | - | `pub` | Benchmark mode: `pub` (publish) or `sub` (subscribe) |
| `--clients` | `-c` | `100` | Number of concurrent simulated IoT clients |
| `--max-connections` | - | `0` | Maximum number of active concurrent connections (`0` = unlimited) |
| `--messages` | `-m` | `10` | Number of messages published (or received in `sub` mode) per client |
| `--duration-secs` | `-d` | `0` | Duration (seconds) for time-based soak testing (`0` = use `--messages` count) |
| `--topic` | `-t` | `sensor/sniper` | MQTT topic to publish or subscribe to (supports `{device_id}`) |
| `--qos` | `-q` | `0` | MQTT Quality of Service level (`0`, `1`, or `2`) |
| `--retain` | - | `false` | Publish messages with the MQTT retain flag set |
| `--keep-alive` | - | `5` | MQTT keep-alive interval in seconds |
| `--ramp-up-ms` | - | `0` | Delay (ms) before spawning consecutive clients (prevents TCP SYN flood) |
| `--interval-ms` | `-i` | `0` | Delay (ms) between consecutive messages from the same client |
| `--payload` | - | `{"device_id": {device_id}, "seq": {seq}, "ts": {timestamp}}` | Custom payload template (supports `{device_id}`, `{seq}`, `{timestamp}`, `{random_int}`, `{random_float}`, `{uuid}`) |
| `--payload-size` | - | `0` | Minimum payload size in bytes (padded with `x` if rendered payload is smaller) |
| `--username` | `-u` | - | Username for MQTT broker authentication |
| `--password` | - | - | Password for MQTT broker authentication |
| `--tls` | - | `false` | Enable SSL/TLS connection (`mqtts://`) |
| `--ca` | - | - | Path to custom CA certificate file (PEM) for TLS verification |
| `--cert` | - | - | Path to client certificate file (PEM) for mTLS authentication |
| `--key` | - | - | Path to client private key file (PEM) for mTLS authentication |
| `--output-json` | - | - | Export benchmark results to a JSON file |
| `--help` | `-h` | - | Print help information |
| `--version` | `-V` | - | Print version information |

## 📦 Payload Format

Each client (`sniper-client-{i}`) publishes JSON payloads to the target topic. By default:

```json
{"device_id": 0, "seq": 0, "ts": 1713400000000}
```

The payload template can be customized via `--payload`, and dynamic variables are substituted automatically:

* `{device_id}`: Simulated client index (0, 1, 2, ...)
* `{seq}`: Message sequence number per client
* `{timestamp}`: Current Unix timestamp in milliseconds
* `{random_int}`: Random integer between `0` and `100`
* `{random_float}`: Random floating-point number between `20.00` and `40.00`
* `{uuid}`: Random RFC 4122 UUID v4 string

Use `--payload-size` to enforce a minimum payload byte size (useful for benchmarking network bandwidth under large packet loads).

## 📊 Example Output

```text
🚀 Starting MQTT Sniper...
Broker: tcp://localhost:1883
Spawning 1000 concurrent Pub clients (50 msgs/client, QoS 0)...
✅ Load test completed in 1.42s
📊 Total messages sent: 50000 / 50000 (Failed clients: 0)
⚡ Throughput: 35211.27 msgs/sec
⏱️  Latency (ms): min=0.42 | avg=3.18 | p50=2.85 | p95=7.40 | p99=12.15 | max=18.90
```

## 🔗 Related Projects in the Ecosystem

* **[mqtt-mock-sensor](https://github.com/thitipongroo/mqtt-mock-sensor)**: Need realistic IoT sensor telemetry (GPS, Env, Power), anomaly injection, or bi-directional Device Shadow testing? Check out our zero-config Node.js simulator (`npx mqtt-mock-sensor`).

## 🤝 Contributing & Security

* Please see `CONTRIBUTING.md` for development guidelines and `SECURITY.md` for our vulnerability reporting policy.
