mod common;

use common::{default_test_args, start_mock_broker, start_mock_broker_with_version};
use mqtt_sniper::cli::Mode;
use mqtt_sniper::runner::run_load_test;
use std::fs;

#[tokio::test]
async fn test_load_test_qos0() {
    let clients = 2;
    let messages = 3;
    let port = start_mock_broker(0, clients).await;

    let args = default_test_args(port, clients, messages, 0, "test/qos0");

    let report = run_load_test(args).await;
    assert_eq!(report.actual_sent, 6);
    assert_eq!(report.failed_clients, 0);
    assert!(report.latency_max_ms >= report.latency_min_ms);
}

#[tokio::test]
async fn test_load_test_qos1_waits_for_puback() {
    let clients = 2;
    let messages = 3;
    let port = start_mock_broker(1, clients).await;

    let args = default_test_args(port, clients, messages, 1, "test/qos1");

    let report = run_load_test(args).await;
    assert_eq!(report.actual_sent, 6);
    assert_eq!(report.failed_clients, 0);
    assert!(report.latency_p99_ms > 0.0);
}

#[tokio::test]
async fn test_load_test_qos2_waits_for_pubcomp() {
    let clients = 2;
    let messages = 3;
    let port = start_mock_broker(2, clients).await;

    let args = default_test_args(port, clients, messages, 2, "test/qos2");

    let report = run_load_test(args).await;
    assert_eq!(report.actual_sent, 6);
    assert_eq!(report.failed_clients, 0);
    assert!(report.latency_p99_ms > 0.0);
}

#[tokio::test]
async fn test_json_report_export() {
    let clients = 1;
    let messages = 2;
    let port = start_mock_broker(1, clients).await;
    let tmp_path = std::env::temp_dir().join("mqtt_sniper_test_report.json");
    let tmp_path_str = tmp_path.to_str().unwrap().to_string();

    let mut args = default_test_args(port, clients, messages, 1, "test/json");
    args.output_json = Some(tmp_path_str);

    let report = run_load_test(args).await;
    assert_eq!(report.actual_sent, 2);
    assert!(tmp_path.exists());

    let content = fs::read_to_string(&tmp_path).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&content).expect("Valid JSON");
    assert_eq!(parsed["metrics"]["success_count"], 2);
    assert_eq!(parsed["metrics"]["total_messages"], 2);
    assert!(parsed.get("latency_ms").is_some());
    assert!(parsed.get("sla").is_some());
    let _ = fs::remove_file(&tmp_path);
}

#[tokio::test]
async fn test_output_json_option_and_output_file() {
    let clients = 1;
    let messages = 2;
    let port = start_mock_broker(1, clients).await;
    let tmp_path = std::env::temp_dir().join("mqtt_sniper_test_output_file.json");
    let tmp_path_str = tmp_path.to_str().unwrap().to_string();

    let mut args = default_test_args(port, clients, messages, 1, "test/output_file");
    args.output = mqtt_sniper::cli::OutputFormat::Json;
    args.output_file = Some(tmp_path_str);
    args.max_p95_ms = Some(100.0);
    args.min_success_rate = Some(0.99);

    let report = run_load_test(args).await;
    assert!(report.slo_passed);
    assert!(tmp_path.exists());

    let content = fs::read_to_string(&tmp_path).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&content).expect("Valid JSON");
    assert_eq!(parsed["sla"]["passed"], true);
    assert_eq!(parsed["sla"]["max_p95_threshold_ms"], 100.0);
    assert_eq!(parsed["sla"]["min_success_rate_threshold"], 99.0);
    assert_eq!(parsed["target"]["qos"], 1);
    let _ = fs::remove_file(&tmp_path);
}

#[tokio::test]
async fn test_output_prometheus_option() {
    let clients = 1;
    let messages = 2;
    let port = start_mock_broker(1, clients).await;
    let tmp_path = std::env::temp_dir().join("mqtt_sniper_test_metrics.prom");
    let tmp_path_str = tmp_path.to_str().unwrap().to_string();

    let mut args = default_test_args(port, clients, messages, 1, "test/prom");
    args.output = mqtt_sniper::cli::OutputFormat::Prometheus;
    args.output_file = Some(tmp_path_str);

    let report = run_load_test(args).await;
    assert_eq!(report.actual_sent, 2);
    assert!(tmp_path.exists());

    let content = fs::read_to_string(&tmp_path).unwrap();
    assert!(content.contains("mqtt_sniper_throughput_msg_per_sec"));
    assert!(content.contains("mqtt_sniper_latency_p95_ms"));
    assert!(content.contains("mqtt_sniper_sla_passed 1"));
    let _ = fs::remove_file(&tmp_path);
}

#[tokio::test]
async fn test_sla_breach_detection() {
    let clients = 1;
    let messages = 2;
    let port = start_mock_broker(1, clients).await;
    let tmp_path = std::env::temp_dir().join("mqtt_sniper_test_sla_fail.json");
    let tmp_path_str = tmp_path.to_str().unwrap().to_string();

    let mut args = default_test_args(port, clients, messages, 1, "test/sla_fail");
    args.output = mqtt_sniper::cli::OutputFormat::Json;
    args.output_file = Some(tmp_path_str);
    args.max_p95_ms = Some(0.00001);

    let report = run_load_test(args).await;
    assert!(!report.slo_passed);
    assert!(!report.sla.passed);

    let content = fs::read_to_string(&tmp_path).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&content).expect("Valid JSON");
    assert_eq!(parsed["sla"]["passed"], false);
    let _ = fs::remove_file(&tmp_path);
}

#[tokio::test]
async fn test_load_test_subscribe_mode_and_max_connections() {
    let clients = 2;
    let messages = 3;
    let port = start_mock_broker(0, clients).await;

    let mut args = default_test_args(port, clients, messages, 0, "test/sub");
    args.mode = Mode::Sub;
    args.max_connections = 1;

    let report = run_load_test(args).await;
    assert_eq!(report.actual_sent, 6);
    assert_eq!(report.failed_clients, 0);
    assert_eq!(report.mode, Mode::Sub);
}

#[tokio::test]
async fn test_load_test_duration_mode() {
    let clients = 1;
    let port = start_mock_broker(0, clients).await;

    let mut args = default_test_args(port, clients, 0, 0, "test/duration");
    args.duration_secs = 1;
    args.interval_ms = 200;
    args.retain = true;
    args.keep_alive = 10;

    let report = run_load_test(args).await;
    assert!(report.actual_sent >= 2);
    assert_eq!(report.failed_clients, 0);
    assert_eq!(report.duration_secs, 1);
    assert!(report.retain);
}

#[tokio::test]
async fn test_start_number_dynamic_credentials_and_chaos() {
    let clients = 2;
    let messages = 2;
    let port = start_mock_broker(1, clients).await;

    let mut args = default_test_args(
        port,
        clients,
        messages,
        1,
        "tenants/org-1/devices/sensor-{device_id}/telemetry",
    );
    args.start_number = 1;
    args.client_prefix = "sensor-".to_string();
    args.username = Some("sensor-{device_id}".to_string());
    args.password = Some("dev-pass-{device_id}".to_string());
    args.malformed_rate = 1.0;

    let report = run_load_test(args).await;
    assert_eq!(report.actual_sent, 4);
    assert_eq!(report.malformed_sent, 4);
    assert_eq!(report.failed_clients, 0);
}

#[tokio::test]
async fn test_conn_mode_and_slo_gate() {
    let clients = 3;
    let port = start_mock_broker(0, clients).await;

    let mut args = default_test_args(port, clients, 0, 0, "test/conn");
    args.mode = Mode::Conn;
    args.clean_session = false;
    args.max_p95_ms = Some(500.0);
    args.min_success_rate = Some(0.99);

    let report = run_load_test(args).await;
    assert_eq!(report.mode, Mode::Conn);
    assert_eq!(report.actual_sent, 3);
    assert_eq!(report.failed_clients, 0);
    assert!(!report.clean_session);
    assert!(report.slo_passed);
}

#[tokio::test]
async fn test_mqtt_v5_publish_qos1() {
    let clients = 2;
    let messages = 2;
    let port = start_mock_broker_with_version(1, clients, 5).await;

    let mut args = default_test_args(port, clients, messages, 1, "test/v5/qos1");
    args.mqtt_version = 5;

    let report = run_load_test(args).await;
    assert_eq!(report.mqtt_version, 5);
    assert_eq!(report.actual_sent, 4);
    assert_eq!(report.failed_clients, 0);
}
