mod common;

use common::start_mock_broker;
use std::fs;
use std::path::Path;
use tokio::process::Command;

#[tokio::test]
async fn test_cli_output_json_and_output_file() {
    let clients = 2;
    let messages = 3;
    let port = start_mock_broker(0, clients).await;

    let target_file = "target/test-report.json";
    if Path::new(target_file).exists() {
        let _ = fs::remove_file(target_file);
    }

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
            "--output",
            "json",
            "--output-file",
            target_file,
            "--quiet",
        ])
        .output()
        .await
        .expect("Failed to execute mqtt-sniper CLI");

    assert!(
        output.status.success(),
        "CLI execution failed with status: {:?}, stderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );

    // 1. Verify output file exists and conforms strictly to benchmark schema
    assert!(
        Path::new(target_file).exists(),
        "Target file {} was not created",
        target_file
    );
    let file_content = fs::read_to_string(target_file).expect("Failed to read output file");
    let file_json: serde_json::Value =
        serde_json::from_str(&file_content).expect("Output file content must be valid JSON");

    // Check schema fields
    assert!(file_json.get("timestamp").is_some(), "Missing timestamp");
    assert!(
        file_json["timestamp"]
            .as_str()
            .unwrap_or_default()
            .ends_with('Z'),
        "Timestamp should be ISO-8601 UTC"
    );

    assert_eq!(
        file_json["target"]["broker"],
        format!("mqtt://127.0.0.1:{}", port)
    );
    assert_eq!(file_json["target"]["clients"], clients);
    assert_eq!(file_json["target"]["qos"], 0);

    assert_eq!(file_json["metrics"]["total_messages"], clients * messages);
    assert_eq!(file_json["metrics"]["success_count"], clients * messages);
    assert_eq!(file_json["metrics"]["failure_count"], 0);
    assert_eq!(file_json["metrics"]["success_rate_percent"], 100.0);
    assert!(file_json["metrics"]["duration_seconds"].as_f64().unwrap() >= 0.0);
    assert!(
        file_json["metrics"]["throughput_msg_per_sec"]
            .as_f64()
            .unwrap()
            >= 0.0
    );

    assert!(file_json["latency_ms"]["min"].is_number());
    assert!(file_json["latency_ms"]["p50"].is_number());
    assert!(file_json["latency_ms"]["p90"].is_number());
    assert!(file_json["latency_ms"]["p95"].is_number());
    assert!(file_json["latency_ms"]["p99"].is_number());
    assert!(file_json["latency_ms"]["max"].is_number());

    assert_eq!(file_json["sla"]["passed"], true);

    // 2. Verify stdout is also clean valid JSON without decorative banners
    let stdout_str = String::from_utf8_lossy(&output.stdout);
    let stdout_json: serde_json::Value =
        serde_json::from_str(&stdout_str).expect("stdout must be strictly parseable JSON");
    assert_eq!(stdout_json["metrics"]["total_messages"], clients * messages);
    assert!(!stdout_str.contains("Pulse Sniper: Enterprise"));
    assert!(!stdout_str.contains("Starting MQTT Sniper"));
    assert!(!stdout_str.contains("┌─"));
    assert!(!stdout_str.contains("└─"));

    let _ = fs::remove_file(target_file);
}

#[tokio::test]
async fn test_cli_output_prometheus_and_output_file() {
    let clients = 2;
    let messages = 3;
    let port = start_mock_broker(0, clients).await;

    let target_file = "target/test-metrics.prom";
    if Path::new(target_file).exists() {
        let _ = fs::remove_file(target_file);
    }

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
            "--output",
            "prometheus",
            "--output-file",
            target_file,
            "--quiet",
        ])
        .output()
        .await
        .expect("Failed to execute mqtt-sniper CLI");

    assert!(
        output.status.success(),
        "CLI execution failed with status: {:?}, stderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );

    // 1. Verify output file exists and contains clean Prometheus OpenMetrics
    assert!(
        Path::new(target_file).exists(),
        "Target file {} was not created",
        target_file
    );
    let prom_content = fs::read_to_string(target_file).expect("Failed to read prometheus file");

    assert!(prom_content.contains("# HELP mqtt_sniper_total_messages"));
    assert!(prom_content.contains("# TYPE mqtt_sniper_total_messages counter"));
    assert!(prom_content.contains(&format!(
        "mqtt_sniper_total_messages {}",
        clients * messages
    )));
    assert!(prom_content.contains(&format!("mqtt_sniper_success_count {}", clients * messages)));
    assert!(prom_content.contains("mqtt_sniper_failure_count 0"));
    assert!(prom_content.contains("mqtt_sniper_success_rate_percent 100.00"));
    assert!(prom_content.contains("mqtt_sniper_duration_seconds"));
    assert!(prom_content.contains("mqtt_sniper_throughput_msg_per_sec"));
    assert!(prom_content.contains("mqtt_sniper_latency_min_ms"));
    assert!(prom_content.contains("mqtt_sniper_latency_p50_ms"));
    assert!(prom_content.contains("mqtt_sniper_latency_p90_ms"));
    assert!(prom_content.contains("mqtt_sniper_latency_p95_ms"));
    assert!(prom_content.contains("mqtt_sniper_latency_p99_ms"));
    assert!(prom_content.contains("mqtt_sniper_latency_max_ms"));
    assert!(prom_content.contains("mqtt_sniper_sla_passed 1"));

    // 2. Verify stdout also contains Prometheus metrics without decorative banners
    let stdout_str = String::from_utf8_lossy(&output.stdout);
    assert!(stdout_str.contains("mqtt_sniper_total_messages"));
    assert!(stdout_str.contains("mqtt_sniper_latency_p95_ms"));
    assert!(!stdout_str.contains("Pulse Sniper: Enterprise"));
    assert!(!stdout_str.contains("Starting MQTT Sniper"));
    assert!(!stdout_str.contains("┌─"));
    assert!(!stdout_str.contains("└─"));

    let _ = fs::remove_file(target_file);
}

#[tokio::test]
async fn test_cli_quiet_mode_outputs_zero_progress_noise() {
    let clients = 2;
    let messages = 2;
    let port = start_mock_broker(0, clients).await;

    let bin = env!("CARGO_BIN_EXE_mqtt-sniper");

    // Test with --quiet
    let output_quiet = Command::new(bin)
        .args([
            "--broker",
            "127.0.0.1",
            "--port",
            &port.to_string(),
            "--clients",
            &clients.to_string(),
            "--messages",
            &messages.to_string(),
            "--output",
            "json",
            "--quiet",
        ])
        .output()
        .await
        .expect("Failed to execute mqtt-sniper CLI");

    assert!(output_quiet.status.success());
    let stdout_quiet = String::from_utf8_lossy(&output_quiet.stdout);
    let stderr_quiet = String::from_utf8_lossy(&output_quiet.stderr);

    // No spinner frames or progress bar noise
    assert!(!stdout_quiet.contains("█"));
    assert!(!stdout_quiet.contains("░"));
    assert!(!stdout_quiet.contains("ETA:"));
    assert!(!stdout_quiet.contains("Starting MQTT Sniper"));
    assert!(!stderr_quiet.contains("█"));
    assert!(!stderr_quiet.contains("ETA:"));

    // Test with -s alias
    let output_silent = Command::new(bin)
        .args([
            "--broker",
            "127.0.0.1",
            "--port",
            &port.to_string(),
            "--clients",
            &clients.to_string(),
            "--messages",
            &messages.to_string(),
            "--output",
            "prometheus",
            "-s",
        ])
        .output()
        .await
        .expect("Failed to execute mqtt-sniper CLI");

    assert!(output_silent.status.success());
    let stdout_silent = String::from_utf8_lossy(&output_silent.stdout);
    assert!(!stdout_silent.contains("█"));
    assert!(!stdout_silent.contains("ETA:"));
    assert!(!stdout_silent.contains("Starting MQTT Sniper"));
    assert!(stdout_silent.contains("mqtt_sniper_total_messages"));
}

#[tokio::test]
async fn test_cli_parameter_handlers_validation() {
    let bin = env!("CARGO_BIN_EXE_mqtt-sniper");

    // Invalid format should fail clap argument parsing
    let output_invalid = Command::new(bin)
        .args(["--output", "invalid_format"])
        .output()
        .await
        .expect("Failed to execute");

    assert!(!output_invalid.status.success());
    let stderr_invalid = String::from_utf8_lossy(&output_invalid.stderr);
    assert!(
        stderr_invalid.contains("invalid value 'invalid_format' for '--output <OUTPUT>'"),
        "stderr was: {}",
        stderr_invalid
    );

    // Valid formats accepted by CLI
    use clap::Parser;
    use mqtt_sniper::cli::{Args, OutputFormat};

    let args_text = Args::parse_from(["mqtt-sniper", "--output", "text"]);
    assert_eq!(args_text.output, OutputFormat::Text);

    let args_json = Args::parse_from(["mqtt-sniper", "-o", "json", "--output-file", "out.json"]);
    assert_eq!(args_json.output, OutputFormat::Json);
    assert_eq!(args_json.output_file, Some("out.json".to_string()));

    let args_prom = Args::parse_from(["mqtt-sniper", "-o", "prometheus", "-s"]);
    assert_eq!(args_prom.output, OutputFormat::Prometheus);
    assert!(args_prom.quiet);
}
