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
    pub mqtt_version: u8,
    pub tls: bool,
    pub clients: usize,
    pub messages_per_client: usize,
    pub duration_secs: u64,
    pub qos: u8,
    pub retain: bool,
    pub clean_session: bool,
    pub target_messages: usize,
    pub actual_sent: usize,
    pub malformed_sent: usize,
    pub failed_clients: usize,
    pub success_rate: f64,
    pub slo_passed: bool,
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
        let target_messages = if args.mode == Mode::Conn {
            args.clients
        } else if args.duration_secs > 0 {
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

        let mut success_rate = if target_messages > 0 {
            (actual_sent as f64 / target_messages as f64).clamp(0.0, 1.0)
        } else if failed_clients == 0 {
            1.0
        } else {
            0.0
        };

        if failed_clients > 0 && args.clients > 0 {
            let client_ok_rate =
                args.clients.saturating_sub(failed_clients) as f64 / args.clients as f64;
            success_rate = success_rate.min(client_ok_rate);
        }

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

        let p95_ok = args.max_p95_ms.is_none_or(|max_p95| p95_ms <= max_p95);
        let rate_ok = args
            .min_success_rate
            .is_none_or(|min_rate| success_rate >= min_rate);
        let slo_passed = p95_ok && rate_ok;

        let use_tls = args.tls || args.ca.is_some() || args.cert.is_some() || args.key.is_some();

        Self {
            broker: args.broker.clone(),
            port: args.port,
            mode: args.mode,
            mqtt_version: args.mqtt_version,
            tls: use_tls,
            clients: args.clients,
            messages_per_client: args.messages,
            duration_secs: args.duration_secs,
            qos: args.qos,
            retain: args.retain,
            clean_session: args.clean_session,
            target_messages,
            actual_sent,
            malformed_sent,
            failed_clients,
            success_rate,
            slo_passed,
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
            Mode::Pub => "messages sent",
            Mode::Sub => "messages received",
            Mode::Conn => "clients connected",
        };
        let proto_label = if self.mqtt_version == 5 {
            "v5.0"
        } else {
            "v3.1.1"
        };
        println!(
            "✅ Load test completed in {:.2?} (MQTT {})",
            elapsed, proto_label
        );
        if self.duration_secs > 0 && self.mode != Mode::Conn {
            println!(
                "📊 Total {}: {} (Duration target: {}s, Malformed injected: {}, Failed clients: {}, Success rate: {:.2}%)",
                action_label,
                self.actual_sent,
                self.duration_secs,
                self.malformed_sent,
                self.failed_clients,
                self.success_rate * 100.0
            );
        } else {
            println!(
                "📊 Total {}: {} / {} (Malformed injected: {}, Failed clients: {}, Success rate: {:.2}%)",
                action_label,
                self.actual_sent,
                self.target_messages,
                self.malformed_sent,
                self.failed_clients,
                self.success_rate * 100.0
            );
        }
        if self.elapsed_secs > 0.0 {
            let unit = if self.mode == Mode::Conn {
                "conns/sec"
            } else {
                "msgs/sec"
            };
            println!(
                "⚡ Throughput: {:.2} {}",
                self.throughput_msgs_per_sec, unit
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
        if !self.slo_passed {
            eprintln!(
                "❌ SLO Gate FAILED: p95={:.2}ms, success_rate={:.2}%",
                self.latency_p95_ms,
                self.success_rate * 100.0
            );
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn test_slo_gate_evaluation() {
        let mut args = Args::parse_from([
            "mqtt-sniper",
            "-c",
            "10",
            "-m",
            "10",
            "--mqtt-version",
            "5",
            "--max-p95-ms",
            "50.0",
            "--min-success-rate",
            "0.99",
        ]);
        let mut hist = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
        let _ = hist.record(10_000); // 10 ms

        let pass_report = BenchmarkReport::new(&args, Duration::from_secs(1), 100, 0, 0, &hist);
        assert!(pass_report.slo_passed);
        assert_eq!(pass_report.mqtt_version, 5);
        assert!((pass_report.success_rate - 1.0).abs() < f64::EPSILON);

        // Breach success rate (95/100 < 0.99)
        let fail_rate_report = BenchmarkReport::new(&args, Duration::from_secs(1), 95, 0, 1, &hist);
        assert!(!fail_rate_report.slo_passed);

        // Breach p95 latency (max_p95_ms = 5.0 < 10.0 ms)
        args.max_p95_ms = Some(5.0);
        let fail_lat_report = BenchmarkReport::new(&args, Duration::from_secs(1), 100, 0, 0, &hist);
        assert!(!fail_lat_report.slo_passed);
    }
}
