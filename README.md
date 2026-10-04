# 🎯 MQTT Sniper

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