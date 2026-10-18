# 🎯 MQTT Sniper

[![Crates.io Version](https://img.shields.io/crates/v/mqtt-sniper?style=flat&logo=rust)](https://crates.io/crates/mqtt-sniper)
[![Crates.io Downloads](https://img.shields.io/crates/d/mqtt-sniper?style=flat&color=28a745)](https://crates.io/crates/mqtt-sniper)
[![CI Status](https://github.com/thitipongroo/mqtt-sniper/actions/workflows/ci.yml/badge.svg)](https://github.com/thitipongroo/mqtt-sniper/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A blazing-fast, memory-efficient MQTT load testing CLI written in Rust. Designed to stress-test enterprise IoT architectures by spawning thousands of concurrent asynchronous clients.

## 🚀 Why MQTT Sniper?

When building enterprise IoT platforms, testing the ingestion boundary is critical. Traditional load testing tools written in Java or Node.js often become the bottleneck themselves due to memory overhead and garbage collection pauses when managing 10,000+ concurrent TCP connections.

MQTT Sniper leverages **Rust** and the **Tokio async runtime** to deliver extreme throughput (msgs/sec) with a near-zero memory footprint. It ensures that your load tester is never the bottleneck.

## ⚡ Features

- **High Concurrency**: Spawns thousands of independent MQTT clients in milliseconds using Tokio light-weight tasks.
- **Fearless Performance**: Zero garbage collection pauses, ensuring accurate throughput measurement.
- **Plug-and-Play**: Compiled to a single native binary, no external runtimes (like JVM or Node) required.

## 🛠️ Installation & Usage

Build from source:

```bash
cargo build --release
```

Cargo:

```bash
cargo install mqtt-sniper
```

Docker:

```bash
docker build -t mqtt-sniper .
docker run --rm mqtt-sniper --broker host.docker.internal --clients 500
```

### Quick Start

Run with default settings (`localhost:1883`, 100 clients, 10 messages per client):

```bash
./target/release/mqtt-sniper
```

Stress-test with custom parameters (e.g., 1,000 clients sending 50 messages each with QoS 1):

```bash
./target/release/mqtt-sniper \
  --broker 127.0.0.1 \
  --port 1883 \
  --clients 1000 \
  --messages 50 \
  --topic "sensor/telemetry" \
  --qos 1
```

### ⚙️ CLI Options

| Flag | Short | Default | Description |
| :--- | :---: | :---: | :--- |
| `--broker` | `-b` | `localhost` | Target MQTT broker hostname or IP address |
| `--port` | `-p` | `1883` | Target MQTT broker TCP port |
| `--clients` | `-c` | `100` | Number of concurrent simulated IoT clients |
| `--messages` | `-m` | `10` | Number of messages published per client |
| `--topic` | `-t` | `sensor/sniper` | MQTT topic to publish messages to (supports `{device_id}`) |
| `--qos` | `-q` | `0` | MQTT Quality of Service level (`0`, `1`, or `2`) |
| `--ramp-up-ms` | - | `0` | Delay (ms) before spawning consecutive clients (prevents TCP SYN flood) |
| `--interval-ms` | `-i` | `0` | Delay (ms) between consecutive messages from the same client |
| `--payload` | - | `{"device_id": {device_id}, "seq": {seq}, "ts": {timestamp}}` | Custom payload template (supports `{device_id}`, `{seq}`, `{timestamp}`) |
| `--payload-size` | - | `0` | Minimum payload size in bytes (padded with `x` if rendered payload is smaller) |
| `--username` | `-u` | - | Username for MQTT broker authentication |
| `--password` | - | - | Password for MQTT broker authentication |
| `--help` | `-h` | - | Print help information |
| `--version` | `-V` | - | Print version information |

### 📦 Payload Format

Each client (`sniper-client-{i}`) publishes JSON payloads to the target topic. By default:

```json
{"device_id": 0, "seq": 0, "ts": 1713400000000}
```

The payload template can be customized via `--payload`, and dynamic variables are substituted automatically:
- `{device_id}`: Simulated client index (0, 1, 2, ...)
- `{seq}`: Message sequence number per client
- `{timestamp}`: Current Unix timestamp in milliseconds

Use `--payload-size` to enforce a minimum payload byte size (useful for benchmarking network bandwidth under large packet loads).

### 📊 Example Output

```text
🚀 Starting MQTT Sniper...
Broker: tcp://localhost:1883
Spawning 1000 concurrent clients (50 msgs/client, QoS 0)...
✅ Load test completed in 1.42s
📊 Total messages sent: 50000 / 50000 (Failed clients: 0)
⚡ Throughput: 35211.27 msgs/sec
```

## 🔗 Related Projects in the Ecosystem

- **[mqtt-mock-sensor](https://github.com/thitipongroo/mqtt-mock-sensor)**: Need realistic IoT sensor telemetry (GPS, Env, Power), anomaly injection, or bi-directional Device Shadow testing? Check out our zero-config Node.js simulator (`npx mqtt-mock-sensor`).
