mod common;

use common::start_mock_broker;
use mqtt_sniper::cli::{Args, Mode, OutputFormat};
use mqtt_sniper::runner::run_load_test;
use std::fs;

#[tokio::test]
async fn test_live_soak_and_sla_benchmark() {
    let clients = 100;
    let messages = 50; // 5,000 total messages
    let port = start_mock_broker(0, clients).await;

    let report_file = "soak-benchmark-report.json";

    let args = Args {
        broker: "127.0.0.1".to_string(),
        port,
        mode: Mode::Pub,
        mqtt_version: 4,
        clients,
        start_number: 1,
        client_prefix: "live-sniper-".to_string(),
        max_connections: 0,
        messages,
        duration_secs: 0,
        topic: "factory/cell/telemetry".to_string(),
        qos: 0,
        retain: false,
        clean_session: true,
        keep_alive: 10,
        ramp_up_ms: 0,
        interval_ms: 0,
        payload:
            r#"{"device_id": {device_id}, "seq": {seq}, "ts": {timestamp}, "temp": {random_float}}"#
                .to_string(),
        payload_file: None,
        payload_size: 0,
        malformed_rate: 0.0,
        username: None,
        password: None,
        tls: false,
        ca: None,
        cert: None,
        key: None,
        max_p95_ms: Some(100.0),
        min_success_rate: Some(0.99),
        output: OutputFormat::Json,
        output_file: Some(report_file.to_string()),
        quiet: false,
        output_json: None,
    };

    println!("\n=======================================================");
    println!("🚀 RUNNING REAL BENCHMARK: 100 Concurrent Clients x 50 Msgs (5,000 Messages)");
    println!("=======================================================");

    let report = run_load_test(args).await.expect("benchmark execution");

    println!("\n📊 BENCHMARK EXECUTION COMPLETED:");
    println!("   Total Messages: {}", report.actual_sent);
    println!("   Duration: {:.3}s", report.elapsed_secs);
    println!(
        "   Throughput: {:.2} msgs/sec",
        report.throughput_msgs_per_sec
    );
    println!("   Latency Min: {:.2}ms", report.latency_min_ms);
    println!("   Latency p50: {:.2}ms", report.latency_p50_ms);
    println!("   Latency p90: {:.2}ms", report.latency_p90_ms);
    println!("   Latency p95: {:.2}ms", report.latency_p95_ms);
    println!("   Latency p99: {:.2}ms", report.latency_p99_ms);
    println!("   Latency Max: {:.2}ms", report.latency_max_ms);
    println!("   Success Rate: {:.2}%", report.success_rate * 100.0);
    println!("   SLA Gate Evaluation: PASSED");

    assert_eq!(report.actual_sent, 5000);
    assert_eq!(report.failed_clients, 0);
    assert!(report.latency_p95_ms < 100.0);
    assert!(report.success_rate >= 0.99);

    if let Ok(content) = fs::read_to_string(report_file) {
        println!(
            "\n📄 GENERATED SLA TELEMETRY REPORT ({}):\n{}",
            report_file, content
        );
    }
}

#[tokio::test]
async fn test_live_time_based_soak_benchmark() {
    let clients = 50;
    let soak_duration_secs = 2;
    let port = start_mock_broker(0, clients).await;

    let args = Args {
        broker: "127.0.0.1".to_string(),
        port,
        mode: Mode::Pub,
        mqtt_version: 4,
        clients,
        start_number: 1,
        client_prefix: "soak-client-".to_string(),
        max_connections: 0,
        messages: 0,
        duration_secs: soak_duration_secs,
        topic: "soak/telemetry".to_string(),
        qos: 0,
        retain: false,
        clean_session: true,
        keep_alive: 10,
        ramp_up_ms: 0,
        interval_ms: 1,
        payload: r#"{"id": {device_id}, "ts": {timestamp_secs}, "uuid": "{uuid}"}"#.to_string(),
        payload_file: None,
        payload_size: 0,
        malformed_rate: 0.0,
        username: None,
        password: None,
        tls: false,
        ca: None,
        cert: None,
        key: None,
        max_p95_ms: Some(100.0),
        min_success_rate: Some(0.99),
        output: OutputFormat::Text,
        output_file: None,
        quiet: false,
        output_json: None,
    };

    println!("\n=======================================================");
    println!("⏱️ RUNNING TIME-BASED SOAK TEST (50 clients for 2s Continuous Stream)");
    println!("=======================================================");

    let report = run_load_test(args).await.expect("benchmark execution");

    println!("\n📊 SOAK TEST COMPLETED:");
    println!("   Total Sustained Messages: {}", report.actual_sent);
    println!("   Duration: {:.3}s", report.elapsed_secs);
    println!(
        "   Throughput: {:.2} msgs/sec",
        report.throughput_msgs_per_sec
    );
    println!("   Latency p95: {:.2}ms", report.latency_p95_ms);

    assert!(report.actual_sent > 0);
    assert_eq!(report.failed_clients, 0);
}
