mod common;

use common::{default_test_args, start_mock_broker, start_mock_broker_with_version};
use mqtt_sniper::cli::PayloadFormat;
use mqtt_sniper::runner::run_load_test;
use std::time::Duration;
use tokio::process::Command;

#[tokio::test]
async fn test_chaos_packet_drop_rate_accuracy() {
    let clients = 10;
    let messages = 300;
    let port = start_mock_broker(0, clients).await;

    let mut args = default_test_args(port, clients, messages, 0, "chaos/test/drop");
    args.packet_drop = 0.20;

    let report = run_load_test(args).await.expect("benchmark execution");

    assert_eq!(report.target_messages, clients * messages);
    let observed_drop_rate = report.dropped_packets as f64 / report.target_messages as f64;

    // Requirement: Check that --packet-drop 0.20 observed rate is within 20% +/- 3% (0.17 to 0.23)
    assert!(
        (0.17..=0.23).contains(&observed_drop_rate),
        "Observed drop rate was {:.4} ({:.2}%), expected strictly within [0.17, 0.23] (20% +/- 3%)",
        observed_drop_rate,
        observed_drop_rate * 100.0
    );

    // Verify consistency across internal metrics and reports
    assert_eq!(report.metrics.dropped_packets, report.dropped_packets);
    assert_eq!(
        report.metrics.success_count + report.metrics.dropped_packets,
        report.target_messages
    );

    // Verify JSON serialization includes dropped_packets metric
    let json_str = report.to_json_string();
    let file_json: serde_json::Value =
        serde_json::from_str(&json_str).expect("Valid JSON required");
    assert_eq!(
        file_json["metrics"]["dropped_packets"],
        report.dropped_packets
    );
    assert!(file_json["metrics"]["drop_rate_percent"].is_number());

    // Verify Prometheus text export includes dropped_packets counter
    let prom_str = report.to_prometheus();
    assert!(
        prom_str.contains(&format!(
            "mqtt_sniper_dropped_packets {}",
            report.dropped_packets
        )),
        "Prometheus metrics must export mqtt_sniper_dropped_packets"
    );
}

#[tokio::test]
async fn test_chaos_packet_drop_qos1_no_deadlock() {
    let clients = 4;
    let messages = 40;
    let port = start_mock_broker(1, clients).await;

    let mut args = default_test_args(port, clients, messages, 1, "chaos/test/qos1");
    args.packet_drop = 0.25;

    // Must complete safely within timeout window without any deadlock
    let report_res = tokio::time::timeout(Duration::from_secs(10), run_load_test(args)).await;
    let report = report_res
        .expect("Load test must complete without deadlock")
        .expect("Execution should succeed");

    assert_eq!(report.target_messages, clients * messages);
    assert!(report.dropped_packets > 0);
    assert_eq!(
        report.metrics.success_count + report.metrics.dropped_packets,
        report.target_messages
    );
}

#[tokio::test]
async fn test_chaos_jitter_average_latency_and_no_deadlock() {
    let clients = 1;
    let messages = 10;
    let port = start_mock_broker(0, clients).await;

    // 1. Baseline run without jitter
    let mut args_base = default_test_args(port, clients, messages, 0, "chaos/test/jitter/base");
    args_base.jitter_ms = 0;
    let report_base = run_load_test(args_base)
        .await
        .expect("Baseline run execution");

    // 2. Jitter run with 40ms maximum jitter per message
    let port_jitter = start_mock_broker(0, clients).await;
    let mut args_jitter =
        default_test_args(port_jitter, clients, messages, 0, "chaos/test/jitter/run");
    args_jitter.jitter_ms = 40;

    let report_jitter = tokio::time::timeout(Duration::from_secs(5), run_load_test(args_jitter))
        .await
        .expect("Jitter run must not deadlock")
        .expect("Execution succeeds");

    // Verify jitter metrics and samples
    assert_eq!(report_jitter.jitter_samples, messages);
    assert!(
        report_jitter.jitter_ms.is_some(),
        "Jitter report must be populated"
    );
    let jitter_rep = report_jitter.jitter_ms.as_ref().unwrap();
    assert_eq!(jitter_rep.samples, messages);
    assert!(jitter_rep.avg > 0.0);
    // HdrHistogram buckets track precision with slight bin rounding (e.g. 40.03 ms for 40 ms)
    assert!(jitter_rep.max <= 45.0);

    // Verify jitter genuinely increases wall clock elapsed time vs baseline
    assert!(
        report_jitter.elapsed_secs > report_base.elapsed_secs,
        "Jitter run elapsed ({:.3}s) should exceed baseline elapsed ({:.3}s)",
        report_jitter.elapsed_secs,
        report_base.elapsed_secs
    );

    // Verify Prometheus export includes jitter metrics
    let prom_str = report_jitter.to_prometheus();
    assert!(prom_str.contains(&format!("mqtt_sniper_jitter_samples {}", messages)));
    assert!(prom_str.contains("mqtt_sniper_jitter_avg_ms"));
}

#[tokio::test]
async fn test_chaos_mqtt_v5_packet_drop_and_jitter() {
    let clients = 3;
    let messages = 25;
    let port = start_mock_broker_with_version(1, clients, 5).await;

    let mut args = default_test_args(port, clients, messages, 1, "chaos/test/v5");
    args.mqtt_version = 5;
    args.packet_drop = 0.20;
    args.jitter_ms = 15;

    let report = tokio::time::timeout(Duration::from_secs(10), run_load_test(args))
        .await
        .expect("MQTT v5 load test must not deadlock")
        .expect("MQTT v5 test succeeds");

    assert_eq!(report.target_messages, clients * messages);
    assert!(report.dropped_packets > 0);
    assert_eq!(report.jitter_samples, clients * messages);
    assert_eq!(
        report.metrics.success_count + report.metrics.dropped_packets,
        report.target_messages
    );
}

#[tokio::test]
async fn test_dynamic_binary_payload_formats() {
    let clients = 2;
    let messages = 5;

    // Test Protobuf format
    let port1 = start_mock_broker(0, clients).await;
    let mut args_proto =
        default_test_args(port1, clients, messages, 0, "binary/protobuf/{device_id}");
    args_proto.payload_format = PayloadFormat::Protobuf;
    let report_proto = run_load_test(args_proto).await.expect("Protobuf run");
    assert_eq!(report_proto.metrics.success_count, clients * messages);

    // Test Avro format
    let port2 = start_mock_broker(0, clients).await;
    let mut args_avro = default_test_args(port2, clients, messages, 0, "binary/avro/{device_id}");
    args_avro.payload_format = PayloadFormat::Avro;
    let report_avro = run_load_test(args_avro).await.expect("Avro run");
    assert_eq!(report_avro.metrics.success_count, clients * messages);

    // Test Raw format
    let port3 = start_mock_broker(0, clients).await;
    let mut args_raw = default_test_args(port3, clients, messages, 0, "binary/raw/{device_id}");
    args_raw.payload_format = PayloadFormat::Raw;
    args_raw.payload = "TEMP_C:{random_float:10.0:30.0}".to_string();
    let report_raw = run_load_test(args_raw).await.expect("Raw run");
    assert_eq!(report_raw.metrics.success_count, clients * messages);
}

#[tokio::test]
async fn test_cli_fault_injection_flags_binary() {
    let clients = 2;
    let messages = 10;
    let port = start_mock_broker(0, clients).await;

    let bin = env!("CARGO_BIN_EXE_mqtt-sniper");
    let output = Command::new(bin)
        .args([
            "--broker",
            "127.0.0.1",
            "--port",
            &port.to_string(),
            "--clients",
            &clients.to_string(),
            "--messages",
            &messages.to_string(),
            "--packet-drop",
            "0.20",
            "--jitter-ms",
            "10",
            "--payload-format",
            "protobuf",
            "--output",
            "prometheus",
            "--quiet",
        ])
        .output()
        .await
        .expect("CLI execution");

    assert!(
        output.status.success(),
        "CLI failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("mqtt_sniper_dropped_packets"));
    assert!(stdout.contains("mqtt_sniper_jitter_samples"));
}
