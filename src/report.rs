use crate::cli::{Args, Mode};
use hdrhistogram::Histogram;
use serde::Serialize;
use std::fs;
use std::time::Duration;

#[derive(Debug, Clone, Serialize)]
pub struct BenchmarkReport {
    pub broker: String,
    pub port: u16,
    pub mode: Mode,
    pub tls: bool,
    pub clients: usize,
    pub messages_per_client: usize,
    pub duration_secs: u64,
    pub qos: u8,
    pub retain: bool,
    pub target_messages: usize,
    pub actual_sent: usize,
    pub malformed_sent: usize,
    pub failed_clients: usize,
    pub elapsed_secs: f64,
    pub throughput_msgs_per_sec: f64,
    pub latency_min_ms: f64,
    pub latency_avg_ms: f64,
    pub latency_p50_ms: f64,
    pub latency_p95_ms: f64,
    pub latency_p99_ms: f64,
    pub latency_max_ms: f64,
}

impl BenchmarkReport {
    pub fn new(
        args: &Args,
        elapsed: Duration,
        actual_sent: usize,
        malformed_sent: usize,
        failed_clients: usize,
        hist: &Histogram<u64>,
    ) -> Self {
        let target_messages = if args.duration_secs > 0 {
            actual_sent
        } else {
            args.clients * args.messages
        };
        let elapsed_secs = elapsed.as_secs_f64();
        let throughput = if elapsed_secs > 0.0 {
            (actual_sent as f64) / elapsed_secs
        } else {
            0.0
        };

        let (min_ms, avg_ms, p50_ms, p95_ms, p99_ms, max_ms) = if !hist.is_empty() {
            (
                hist.min() as f64 / 1000.0,
                hist.mean() / 1000.0,
                hist.value_at_quantile(0.50) as f64 / 1000.0,
                hist.value_at_quantile(0.95) as f64 / 1000.0,
                hist.value_at_quantile(0.99) as f64 / 1000.0,
                hist.max() as f64 / 1000.0,
            )
        } else {
            (0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
        };

        let use_tls = args.tls || args.ca.is_some() || args.cert.is_some() || args.key.is_some();

        Self {
            broker: args.broker.clone(),
            port: args.port,
            mode: args.mode,
            tls: use_tls,
            clients: args.clients,
            messages_per_client: args.messages,
            duration_secs: args.duration_secs,
            qos: args.qos,
            retain: args.retain,
            target_messages,
            actual_sent,
            malformed_sent,
            failed_clients,
            elapsed_secs,
            throughput_msgs_per_sec: throughput,
            latency_min_ms: min_ms,
            latency_avg_ms: avg_ms,
            latency_p50_ms: p50_ms,
            latency_p95_ms: p95_ms,
            latency_p99_ms: p99_ms,
            latency_max_ms: max_ms,
        }
    }

    pub fn print_summary(&self, elapsed: Duration) {
        let action_label = match self.mode {
            Mode::Pub => "sent",
            Mode::Sub => "received",
        };
        println!("✅ Load test completed in {:.2?}", elapsed);
        if self.duration_secs > 0 {
            println!(
                "📊 Total messages {}: {} (Duration target: {}s, Malformed injected: {}, Failed clients: {})",
                action_label,
                self.actual_sent,
                self.duration_secs,
                self.malformed_sent,
                self.failed_clients
            );
        } else {
            println!(
                "📊 Total messages {}: {} / {} (Malformed injected: {}, Failed clients: {})",
                action_label,
                self.actual_sent,
                self.target_messages,
                self.malformed_sent,
                self.failed_clients
            );
        }
        if self.elapsed_secs > 0.0 {
            println!(
                "⚡ Throughput: {:.2} msgs/sec",
                self.throughput_msgs_per_sec
            );
        }
        println!(
            "⏱️  Latency (ms): min={:.2} | avg={:.2} | p50={:.2} | p95={:.2} | p99={:.2} | max={:.2}",
            self.latency_min_ms,
            self.latency_avg_ms,
            self.latency_p50_ms,
            self.latency_p95_ms,
            self.latency_p99_ms,
            self.latency_max_ms
        );
    }

    pub fn save_json(&self, path: &str) {
        if let Ok(json_str) = serde_json::to_string_pretty(self) {
            if let Err(e) = fs::write(path, json_str) {
                eprintln!("⚠️ Failed to write JSON report to {}: {}", path, e);
            } else {
                println!("💾 Saved JSON benchmark report to {}", path);
            }
        }
    }
}
