use crate::cli::{Args, Mode};
use hdrhistogram::Histogram;
use serde::Serialize;
use std::fs;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Format a `SystemTime` into an ISO-8601 UTC string (e.g. `2026-10-28T15:30:00Z`).
pub fn format_iso8601(time: SystemTime) -> String {
    let dur = time.duration_since(UNIX_EPOCH).unwrap_or_default();
    let total_secs = dur.as_secs();

    let sec = (total_secs % 60) as u32;
    let total_mins = total_secs / 60;
    let min = (total_mins % 60) as u32;
    let total_hours = total_mins / 60;
    let hour = (total_hours % 24) as u32;
    let mut days = (total_hours / 24) as i64;

    // Civil calendar algorithm (Howard Hinnant)
    days += 719468;
    let era = if days >= 0 { days } else { days - 146096 } / 146097;
    let doe = (days - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = (yoe as i64) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if m <= 2 { y + 1 } else { y };

    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        year, m, d, hour, min, sec
    )
}

/// Target specification in the benchmark report.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct TargetReport {
    /// Broker endpoint URL (e.g. `mqtt://127.0.0.1:1883`)
    pub broker: String,
    /// Number of simulated clients
    pub clients: usize,
    /// QoS level tested
    pub qos: u8,
}

/// Aggregated throughput and message count metrics.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct MetricsReport {
    /// Total expected messages in benchmark target
    pub total_messages: usize,
    /// Successfully published or received messages
    pub success_count: usize,
    /// Number of failed deliveries or disconnected clients
    pub failure_count: usize,
    /// Success rate percentage (0.0 to 100.0)
    pub success_rate_percent: f64,
    /// Benchmark wall clock duration in seconds
    pub duration_seconds: f64,
    /// Effective throughput in messages per second
    pub throughput_msg_per_sec: f64,
}

/// Latency distribution percentiles in milliseconds.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct LatencyMsReport {
    /// Minimum recorded latency
    pub min: f64,
    /// 50th percentile (median) latency
    pub p50: f64,
    /// 90th percentile latency
    pub p90: f64,
    /// 95th percentile latency
    pub p95: f64,
    /// 99th percentile latency
    pub p99: f64,
    /// Maximum recorded latency
    pub max: f64,
}

/// Service Level Agreement (SLA) verification result.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct SlaReport {
    /// Whether all configured SLA gates passed
    pub passed: bool,
    /// Configured maximum p95 threshold in milliseconds, if set
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_p95_threshold_ms: Option<f64>,
    /// Configured minimum success rate threshold percentage, if set
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_success_rate_threshold: Option<f64>,
}

/// Comprehensive benchmark report generated at the completion of a load test run.
#[derive(Debug, Clone, Serialize)]
pub struct BenchmarkReport {
    /// ISO-8601 UTC timestamp of report generation
    pub timestamp: String,
    /// Target broker details
    pub target: TargetReport,
    /// Execution message metrics
    pub metrics: MetricsReport,
    /// Latency breakdown across percentiles
    pub latency_ms: LatencyMsReport,
    /// SLA pass/fail evaluation
    pub sla: SlaReport,

    // Flat fields retained for backward compatibility with runner.rs and tests
    #[serde(skip_serializing)]
    pub broker: String,
    #[serde(skip_serializing)]
    pub port: u16,
    #[serde(skip_serializing)]
    pub mode: Mode,
    #[serde(skip_serializing)]
    pub mqtt_version: u8,
    #[serde(skip_serializing)]
    pub tls: bool,
    #[serde(skip_serializing)]
    pub clients: usize,
    #[serde(skip_serializing)]
    pub messages_per_client: usize,
    #[serde(skip_serializing)]
    pub duration_secs: u64,
    #[serde(skip_serializing)]
    pub qos: u8,
    #[serde(skip_serializing)]
    pub retain: bool,
    #[serde(skip_serializing)]
    pub clean_session: bool,
    #[serde(skip_serializing)]
    pub target_messages: usize,
    #[serde(skip_serializing)]
    pub actual_sent: usize,
    #[serde(skip_serializing)]
    pub malformed_sent: usize,
    #[serde(skip_serializing)]
    pub failed_clients: usize,
    #[serde(skip_serializing)]
    pub success_rate: f64,
    #[serde(skip_serializing)]
    pub slo_passed: bool,
    #[serde(skip_serializing)]
    pub elapsed_secs: f64,
    #[serde(skip_serializing)]
    pub throughput_msgs_per_sec: f64,
    #[serde(skip_serializing)]
    pub latency_min_ms: f64,
    #[serde(skip_serializing)]
    pub latency_avg_ms: f64,
    #[serde(skip_serializing)]
    pub latency_p50_ms: f64,
    #[serde(skip_serializing)]
    pub latency_p90_ms: f64,
    #[serde(skip_serializing)]
    pub latency_p95_ms: f64,
    #[serde(skip_serializing)]
    pub latency_p99_ms: f64,
    #[serde(skip_serializing)]
    pub latency_max_ms: f64,
}

impl BenchmarkReport {
    /// Construct a new `BenchmarkReport` from execution metrics and histogram.
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

        let (min_ms, avg_ms, p50_ms, p90_ms, p95_ms, p99_ms, max_ms) = if !hist.is_empty() {
            (
                hist.min() as f64 / 1000.0,
                hist.mean() / 1000.0,
                hist.value_at_quantile(0.50) as f64 / 1000.0,
                hist.value_at_quantile(0.90) as f64 / 1000.0,
                hist.value_at_quantile(0.95) as f64 / 1000.0,
                hist.value_at_quantile(0.99) as f64 / 1000.0,
                hist.max() as f64 / 1000.0,
            )
        } else {
            (0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
        };

        let p95_ok = args.max_p95_ms.is_none_or(|max_p95| p95_ms <= max_p95);
        let rate_ok = args
            .min_success_rate
            .is_none_or(|min_rate| success_rate >= min_rate);
        let slo_passed = p95_ok && rate_ok;

        let use_tls = args.tls || args.ca.is_some() || args.cert.is_some() || args.key.is_some();

        let broker_scheme = if use_tls { "mqtts" } else { "mqtt" };
        let broker_target = format!("{}://{}:{}", broker_scheme, args.broker, args.port);

        let failure_count = target_messages.saturating_sub(actual_sent) + failed_clients;

        let target = TargetReport {
            broker: broker_target,
            clients: args.clients,
            qos: args.qos,
        };

        let metrics = MetricsReport {
            total_messages: target_messages,
            success_count: actual_sent,
            failure_count,
            success_rate_percent: ((success_rate * 100.0 * 100.0).round()) / 100.0,
            duration_seconds: ((elapsed_secs * 100.0).round()) / 100.0,
            throughput_msg_per_sec: ((throughput * 100.0).round()) / 100.0,
        };

        let latency_ms = LatencyMsReport {
            min: ((min_ms * 100.0).round()) / 100.0,
            p50: ((p50_ms * 100.0).round()) / 100.0,
            p90: ((p90_ms * 100.0).round()) / 100.0,
            p95: ((p95_ms * 100.0).round()) / 100.0,
            p99: ((p99_ms * 100.0).round()) / 100.0,
            max: ((max_ms * 100.0).round()) / 100.0,
        };

        let sla = SlaReport {
            passed: slo_passed,
            max_p95_threshold_ms: args.max_p95_ms,
            min_success_rate_threshold: args.min_success_rate.map(|r| {
                if r <= 1.0 {
                    ((r * 100.0 * 100.0).round()) / 100.0
                } else {
                    ((r * 100.0).round()) / 100.0
                }
            }),
        };

        let timestamp = format_iso8601(SystemTime::now());

        Self {
            timestamp,
            target,
            metrics,
            latency_ms,
            sla,
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
            latency_p90_ms: p90_ms,
            latency_p95_ms: p95_ms,
            latency_p99_ms: p99_ms,
            latency_max_ms: max_ms,
        }
    }

    /// Print formatted human-readable summary table to stdout.
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
            "⏱️  Latency (ms): min={:.2} | avg={:.2} | p50={:.2} | p90={:.2} | p95={:.2} | p99={:.2} | max={:.2}",
            self.latency_min_ms,
            self.latency_avg_ms,
            self.latency_p50_ms,
            self.latency_p90_ms,
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

        println!("\n┌────────────────────────────────────────────────────────────────────────┐");
        println!("│ 🎯 MQTT Sniper: Open Source MQTT Load Testing & Benchmarking CLI       │");
        println!("│    ⭐ Star on GitHub: https://github.com/thitipongroo/mqtt-sniper     │");
        println!("└────────────────────────────────────────────────────────────────────────┘");
    }

    /// Serialize benchmark report to a pretty JSON string.
    pub fn to_json_string(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_else(|_| "{}".to_string())
    }

    /// Export benchmark metrics into Prometheus / OpenMetrics text format.
    pub fn to_prometheus(&self) -> String {
        let mut out = String::new();
        out.push_str("# HELP mqtt_sniper_total_messages Total messages in benchmark target\n");
        out.push_str("# TYPE mqtt_sniper_total_messages counter\n");
        out.push_str(&format!(
            "mqtt_sniper_total_messages {}\n",
            self.metrics.total_messages
        ));

        out.push_str("# HELP mqtt_sniper_success_count Successful messages or connections\n");
        out.push_str("# TYPE mqtt_sniper_success_count counter\n");
        out.push_str(&format!(
            "mqtt_sniper_success_count {}\n",
            self.metrics.success_count
        ));

        out.push_str("# HELP mqtt_sniper_failure_count Failed messages or clients\n");
        out.push_str("# TYPE mqtt_sniper_failure_count counter\n");
        out.push_str(&format!(
            "mqtt_sniper_failure_count {}\n",
            self.metrics.failure_count
        ));

        out.push_str("# HELP mqtt_sniper_success_rate_percent Success rate percentage\n");
        out.push_str("# TYPE mqtt_sniper_success_rate_percent gauge\n");
        out.push_str(&format!(
            "mqtt_sniper_success_rate_percent {:.2}\n",
            self.metrics.success_rate_percent
        ));

        out.push_str("# HELP mqtt_sniper_duration_seconds Total benchmark duration in seconds\n");
        out.push_str("# TYPE mqtt_sniper_duration_seconds gauge\n");
        out.push_str(&format!(
            "mqtt_sniper_duration_seconds {:.2}\n",
            self.metrics.duration_seconds
        ));

        out.push_str("# HELP mqtt_sniper_throughput_msg_per_sec Messages published per second\n");
        out.push_str("# TYPE mqtt_sniper_throughput_msg_per_sec gauge\n");
        out.push_str(&format!(
            "mqtt_sniper_throughput_msg_per_sec {:.2}\n",
            self.metrics.throughput_msg_per_sec
        ));

        out.push_str("# HELP mqtt_sniper_latency_min_ms Minimum latency in milliseconds\n");
        out.push_str("# TYPE mqtt_sniper_latency_min_ms gauge\n");
        out.push_str(&format!(
            "mqtt_sniper_latency_min_ms {:.2}\n",
            self.latency_ms.min
        ));

        out.push_str("# HELP mqtt_sniper_latency_p50_ms P50 latency in milliseconds\n");
        out.push_str("# TYPE mqtt_sniper_latency_p50_ms gauge\n");
        out.push_str(&format!(
            "mqtt_sniper_latency_p50_ms {:.2}\n",
            self.latency_ms.p50
        ));

        out.push_str("# HELP mqtt_sniper_latency_p90_ms P90 latency in milliseconds\n");
        out.push_str("# TYPE mqtt_sniper_latency_p90_ms gauge\n");
        out.push_str(&format!(
            "mqtt_sniper_latency_p90_ms {:.2}\n",
            self.latency_ms.p90
        ));

        out.push_str("# HELP mqtt_sniper_latency_p95_ms P95 latency in milliseconds\n");
        out.push_str("# TYPE mqtt_sniper_latency_p95_ms gauge\n");
        out.push_str(&format!(
            "mqtt_sniper_latency_p95_ms {:.2}\n",
            self.latency_ms.p95
        ));

        out.push_str("# HELP mqtt_sniper_latency_p99_ms P99 latency in milliseconds\n");
        out.push_str("# TYPE mqtt_sniper_latency_p99_ms gauge\n");
        out.push_str(&format!(
            "mqtt_sniper_latency_p99_ms {:.2}\n",
            self.latency_ms.p99
        ));

        out.push_str("# HELP mqtt_sniper_latency_max_ms Maximum latency in milliseconds\n");
        out.push_str("# TYPE mqtt_sniper_latency_max_ms gauge\n");
        out.push_str(&format!(
            "mqtt_sniper_latency_max_ms {:.2}\n",
            self.latency_ms.max
        ));

        out.push_str("# HELP mqtt_sniper_sla_passed Whether the benchmark satisfied SLA quality gates (1 = passed, 0 = breached)\n");
        out.push_str("# TYPE mqtt_sniper_sla_passed gauge\n");
        out.push_str(&format!(
            "mqtt_sniper_sla_passed {}\n",
            if self.sla.passed { 1 } else { 0 }
        ));

        out
    }

    /// Save benchmark report as JSON to the specified filesystem path.
    pub fn save_json(&self, path: &str) {
        let json_str = self.to_json_string();
        if let Some(parent) = std::path::Path::new(path)
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
        {
            let _ = fs::create_dir_all(parent);
        }
        if let Err(e) = fs::write(path, json_str) {
            eprintln!("⚠️ Failed to write JSON report to {path}: {e}");
        } else {
            eprintln!("💾 Saved JSON benchmark report to {path}");
        }
    }

    /// Save report text to the specified filesystem path.
    pub fn save_file(&self, path: &str, content: &str) {
        if let Some(parent) = std::path::Path::new(path)
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
        {
            let _ = fs::create_dir_all(parent);
        }
        if let Err(e) = fs::write(path, content) {
            eprintln!("⚠️ Failed to write report to {path}: {e}");
        } else {
            eprintln!("💾 Saved benchmark report to {path}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn test_format_iso8601() {
        let epoch = UNIX_EPOCH;
        assert_eq!(format_iso8601(epoch), "1970-01-01T00:00:00Z");

        let now = SystemTime::now();
        let formatted = format_iso8601(now);
        assert!(formatted.ends_with('Z'));
        assert_eq!(formatted.len(), 20);
        assert_eq!(&formatted[4..5], "-");
        assert_eq!(&formatted[7..8], "-");
        assert_eq!(&formatted[10..11], "T");
        assert_eq!(&formatted[13..14], ":");
        assert_eq!(&formatted[16..17], ":");
    }

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
        let mut hist = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3)
            .expect("Valid bounds 1..60_000_000 must succeed");
        let _ = hist.record(10_000); // 10 ms

        let pass_report = BenchmarkReport::new(&args, Duration::from_secs(1), 100, 0, 0, &hist);
        assert!(pass_report.slo_passed);
        assert!(pass_report.sla.passed);
        assert_eq!(pass_report.mqtt_version, 5);
        assert!((pass_report.success_rate - 1.0).abs() < f64::EPSILON);
        assert_eq!(pass_report.sla.max_p95_threshold_ms, Some(50.0));
        assert_eq!(pass_report.sla.min_success_rate_threshold, Some(99.0));

        // Breach success rate (95/100 < 0.99)
        let fail_rate_report = BenchmarkReport::new(&args, Duration::from_secs(1), 95, 0, 1, &hist);
        assert!(!fail_rate_report.slo_passed);
        assert!(!fail_rate_report.sla.passed);

        // Breach p95 latency (max_p95_ms = 5.0 < 10.0 ms)
        args.max_p95_ms = Some(5.0);
        let fail_lat_report = BenchmarkReport::new(&args, Duration::from_secs(1), 100, 0, 0, &hist);
        assert!(!fail_lat_report.slo_passed);
        assert!(!fail_lat_report.sla.passed);
    }

    #[test]
    fn test_json_and_prometheus_serialization() {
        let args = Args::parse_from([
            "mqtt-sniper",
            "-b",
            "localhost",
            "-p",
            "1883",
            "-c",
            "10",
            "-m",
            "5",
            "-q",
            "1",
            "--max-p95-ms",
            "20.0",
            "--min-success-rate",
            "0.95",
        ]);
        let mut hist = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3)
            .expect("Valid bounds 1..60_000_000 must succeed");
        let _ = hist.record(5_000); // 5 ms
        let _ = hist.record(12_000); // 12 ms

        let report = BenchmarkReport::new(&args, Duration::from_millis(1500), 50, 0, 0, &hist);
        let json_str = report.to_json_string();

        let parsed: serde_json::Value = serde_json::from_str(&json_str).expect("Valid JSON");
        assert!(parsed.get("timestamp").is_some());
        assert_eq!(parsed["target"]["broker"], "mqtt://localhost:1883");
        assert_eq!(parsed["target"]["clients"], 10);
        assert_eq!(parsed["target"]["qos"], 1);
        assert_eq!(parsed["metrics"]["total_messages"], 50);
        assert_eq!(parsed["metrics"]["success_count"], 50);
        assert_eq!(parsed["metrics"]["failure_count"], 0);
        assert_eq!(parsed["metrics"]["success_rate_percent"], 100.0);
        assert!(parsed["latency_ms"]["p90"].is_number());
        assert!(parsed["latency_ms"]["p95"].is_number());
        assert_eq!(parsed["sla"]["passed"], true);
        assert_eq!(parsed["sla"]["max_p95_threshold_ms"], 20.0);
        assert_eq!(parsed["sla"]["min_success_rate_threshold"], 95.0);

        let prom_str = report.to_prometheus();
        assert!(prom_str.contains("mqtt_sniper_throughput_msg_per_sec"));
        assert!(prom_str.contains("mqtt_sniper_latency_p95_ms"));
        assert!(prom_str.contains("mqtt_sniper_sla_passed 1"));
    }
}
