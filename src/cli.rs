use clap::{Parser, ValueEnum};
use rumqttc::{QoS, Transport};
use serde::Serialize;
use std::fs;

/// Benchmark operation mode
#[derive(ValueEnum, Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    /// Publish messages to the broker
    #[default]
    Pub,
    /// Subscribe to topics and receive messages from the broker
    Sub,
    /// Establish connections and benchmark CONNECT -> CONNACK handshake latency
    Conn,
}

/// CLI argument parsing structure
#[derive(Parser, Debug, Clone)]
#[command(author, version, about = "Blazing fast MQTT Load Tester")]
pub struct Args {
    /// Target MQTT broker hostname or IP address
    #[arg(short, long, default_value = "localhost")]
    pub broker: String,

    /// Target MQTT broker TCP port
    #[arg(short, long, default_value_t = 1883)]
    pub port: u16,

    /// Benchmark mode: 'pub' (publish), 'sub' (subscribe), or 'conn' (connection storm)
    #[arg(long, value_enum, default_value_t = Mode::Pub)]
    pub mode: Mode,

    /// Number of concurrent simulated IoT clients
    #[arg(short, long, default_value_t = 100)]
    pub clients: usize,

    /// Starting index for client IDs and {device_id} variable (e.g., 1 for sensor-1..sensor-N)
    #[arg(short = 'n', long, default_value_t = 0)]
    pub start_number: usize,

    /// Prefix for generated MQTT client IDs (client ID = <prefix><device_id>)
    #[arg(long, default_value = "sniper-client-")]
    pub client_prefix: String,

    /// Maximum number of active concurrent connections (0 = unlimited)
    #[arg(long, default_value_t = 0)]
    pub max_connections: usize,

    /// Number of messages published (or received in 'sub' mode) per client
    #[arg(short, long, default_value_t = 10)]
    pub messages: usize,

    /// Duration (seconds) for time-based soak testing (0 = use --messages count)
    #[arg(short = 'd', long, default_value_t = 0)]
    pub duration_secs: u64,

    /// MQTT topic to publish or subscribe to (supports {device_id})
    #[arg(short, long, default_value = "sensor/sniper")]
    pub topic: String,

    /// MQTT Quality of Service level (0 = AtMostOnce, 1 = AtLeastOnce, 2 = ExactlyOnce)
    #[arg(short, long, default_value_t = 0, value_parser = clap::value_parser!(u8).range(0..=2))]
    pub qos: u8,

    /// Publish messages with the MQTT retain flag set
    #[arg(long, default_value_t = false)]
    pub retain: bool,

    /// MQTT clean session flag (set false for persistent sessions)
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub clean_session: bool,

    /// MQTT keep-alive interval in seconds
    #[arg(long, default_value_t = 5)]
    pub keep_alive: u64,

    /// Delay (ms) before spawning consecutive clients (prevents TCP SYN flood)
    #[arg(long, default_value_t = 0)]
    pub ramp_up_ms: u64,

    /// Delay (ms) between consecutive messages from the same client
    #[arg(short, long, default_value_t = 0)]
    pub interval_ms: u64,

    /// Custom payload template (supports {device_id}, {seq}, {timestamp}, {timestamp_secs}, {timestamp_us}, {random_int}, {random_float}, {random_bool}, {uuid})
    #[arg(
        long,
        default_value = r#"{"device_id": {device_id}, "seq": {seq}, "ts": {timestamp}}"#
    )]
    pub payload: String,

    /// Path to a custom payload template file (overrides --payload)
    #[arg(long)]
    pub payload_file: Option<String>,

    /// Minimum payload size in bytes (padded with 'x' if shorter)
    #[arg(long, default_value_t = 0)]
    pub payload_size: usize,

    /// Fraction of published messages (0.0 to 1.0) to intentionally corrupt for DLQ/chaos testing
    #[arg(long, default_value_t = 0.0)]
    pub malformed_rate: f64,

    /// Username for MQTT broker authentication (supports {device_id})
    #[arg(short, long)]
    pub username: Option<String>,

    /// Password for MQTT broker authentication (supports {device_id})
    #[arg(long)]
    pub password: Option<String>,

    /// Enable SSL/TLS connection (mqtts://)
    #[arg(long, default_value_t = false)]
    pub tls: bool,

    /// Path to custom CA certificate file (PEM) for TLS verification
    #[arg(long)]
    pub ca: Option<String>,

    /// Path to client certificate file (PEM) for mTLS authentication
    #[arg(long)]
    pub cert: Option<String>,

    /// Path to client private key file (PEM) for mTLS authentication
    #[arg(long)]
    pub key: Option<String>,

    /// Maximum allowed p95 latency in milliseconds for CI/CD SLO gate
    #[arg(long)]
    pub max_p95_ms: Option<f64>,

    /// Minimum required success rate (0.0 to 1.0, e.g. 0.995) for CI/CD SLO gate
    #[arg(long)]
    pub min_success_rate: Option<f64>,

    /// Export benchmark results to a JSON file
    #[arg(long)]
    pub output_json: Option<String>,
}

pub fn parse_qos(qos: u8) -> QoS {
    match qos {
        1 => QoS::AtLeastOnce,
        2 => QoS::ExactlyOnce,
        _ => QoS::AtMostOnce,
    }
}

pub fn resolve_payload_template(args: &Args) -> Result<String, String> {
    if !(0.0..=1.0).contains(&args.malformed_rate) {
        return Err(format!(
            "--malformed-rate must be between 0.0 and 1.0 (got {})",
            args.malformed_rate
        ));
    }

    if let Some(rate) = args.min_success_rate
        && !(0.0..=1.0).contains(&rate)
    {
        return Err(format!(
            "--min-success-rate must be between 0.0 and 1.0 (got {})",
            rate
        ));
    }

    if let Some(max_p95) = args.max_p95_ms
        && max_p95 < 0.0
    {
        return Err(format!(
            "--max-p95-ms must be non-negative (got {})",
            max_p95
        ));
    }

    if let Some(ref path) = args.payload_file {
        let content = fs::read_to_string(path)
            .map_err(|e| format!("Failed to read payload file '{}': {}", path, e))?;
        Ok(content.trim().to_string())
    } else {
        Ok(args.payload.clone())
    }
}

pub fn build_transport(args: &Args) -> Result<Transport, String> {
    let use_tls = args.tls || args.ca.is_some() || args.cert.is_some() || args.key.is_some();
    if !use_tls {
        return Ok(Transport::Tcp);
    }

    let client_auth = match (&args.cert, &args.key) {
        (Some(cert_path), Some(key_path)) => {
            let cert_bytes = fs::read(cert_path)
                .map_err(|e| format!("Failed to read client cert '{}': {}", cert_path, e))?;
            let key_bytes = fs::read(key_path)
                .map_err(|e| format!("Failed to read client key '{}': {}", key_path, e))?;
            Some((cert_bytes, key_bytes))
        }
        (Some(_), None) | (None, Some(_)) => {
            return Err(
                "Both --cert and --key must be provided for mTLS authentication".to_string(),
            );
        }
        (None, None) => None,
    };

    if let Some(ref ca_path) = args.ca {
        let ca_bytes = fs::read(ca_path)
            .map_err(|e| format!("Failed to read CA cert '{}': {}", ca_path, e))?;
        Ok(Transport::tls(ca_bytes, client_auth, None))
    } else if client_auth.is_some() {
        Err("Custom --ca certificate is required when using --cert and --key for mTLS".to_string())
    } else {
        Ok(Transport::tls_with_default_config())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_qos() {
        assert_eq!(parse_qos(0), QoS::AtMostOnce);
        assert_eq!(parse_qos(1), QoS::AtLeastOnce);
        assert_eq!(parse_qos(2), QoS::ExactlyOnce);
        assert_eq!(parse_qos(99), QoS::AtMostOnce);
    }

    #[test]
    fn test_cli_default_args() {
        let args = Args::parse_from(["mqtt-sniper"]);
        assert_eq!(args.broker, "localhost");
        assert_eq!(args.port, 1883);
        assert_eq!(args.mode, Mode::Pub);
        assert_eq!(args.clients, 100);
        assert_eq!(args.start_number, 0);
        assert_eq!(args.client_prefix, "sniper-client-");
        assert_eq!(args.max_connections, 0);
        assert_eq!(args.messages, 10);
        assert_eq!(args.duration_secs, 0);
        assert_eq!(args.topic, "sensor/sniper");
        assert_eq!(args.qos, 0);
        assert!(!args.retain);
        assert!(args.clean_session);
        assert_eq!(args.keep_alive, 5);
        assert_eq!(args.ramp_up_ms, 0);
        assert_eq!(args.interval_ms, 0);
        assert_eq!(
            args.payload,
            r#"{"device_id": {device_id}, "seq": {seq}, "ts": {timestamp}}"#
        );
        assert_eq!(args.payload_file, None);
        assert_eq!(args.payload_size, 0);
        assert_eq!(args.malformed_rate, 0.0);
        assert_eq!(args.username, None);
        assert_eq!(args.password, None);
        assert!(!args.tls);
        assert_eq!(args.ca, None);
        assert_eq!(args.cert, None);
        assert_eq!(args.key, None);
        assert_eq!(args.max_p95_ms, None);
        assert_eq!(args.min_success_rate, None);
        assert_eq!(args.output_json, None);
    }

    #[test]
    fn test_cli_custom_args() {
        let args = Args::parse_from([
            "mqtt-sniper",
            "-b",
            "10.0.0.1",
            "-p",
            "8883",
            "--mode",
            "conn",
            "-c",
            "50",
            "-n",
            "1",
            "--client-prefix",
            "sensor-",
            "--max-connections",
            "25",
            "-m",
            "20",
            "-d",
            "60",
            "-t",
            "test/topic/{device_id}",
            "-q",
            "2",
            "--retain",
            "--clean-session",
            "false",
            "--keep-alive",
            "30",
            "--ramp-up-ms",
            "5",
            "-i",
            "10",
            "--payload",
            r#"{"id": {device_id}}"#,
            "--payload-file",
            "template.json",
            "--payload-size",
            "64",
            "--malformed-rate",
            "0.05",
            "-u",
            "sensor-{device_id}",
            "--password",
            "pass-{device_id}",
            "--tls",
            "--ca",
            "ca.pem",
            "--cert",
            "client.crt",
            "--key",
            "client.key",
            "--max-p95-ms",
            "100.0",
            "--min-success-rate",
            "0.995",
            "--output-json",
            "report.json",
        ]);
        assert_eq!(args.broker, "10.0.0.1");
        assert_eq!(args.port, 8883);
        assert_eq!(args.mode, Mode::Conn);
        assert_eq!(args.clients, 50);
        assert_eq!(args.start_number, 1);
        assert_eq!(args.client_prefix, "sensor-");
        assert_eq!(args.max_connections, 25);
        assert_eq!(args.messages, 20);
        assert_eq!(args.duration_secs, 60);
        assert_eq!(args.topic, "test/topic/{device_id}");
        assert_eq!(args.qos, 2);
        assert!(args.retain);
        assert!(!args.clean_session);
        assert_eq!(args.keep_alive, 30);
        assert_eq!(args.ramp_up_ms, 5);
        assert_eq!(args.interval_ms, 10);
        assert_eq!(args.payload, r#"{"id": {device_id}}"#);
        assert_eq!(args.payload_file, Some("template.json".to_string()));
        assert_eq!(args.payload_size, 64);
        assert!((args.malformed_rate - 0.05).abs() < f64::EPSILON);
        assert_eq!(args.username, Some("sensor-{device_id}".to_string()));
        assert_eq!(args.password, Some("pass-{device_id}".to_string()));
        assert!(args.tls);
        assert_eq!(args.ca, Some("ca.pem".to_string()));
        assert_eq!(args.cert, Some("client.crt".to_string()));
        assert_eq!(args.key, Some("client.key".to_string()));
        assert_eq!(args.max_p95_ms, Some(100.0));
        assert_eq!(args.min_success_rate, Some(0.995));
        assert_eq!(args.output_json, Some("report.json".to_string()));
    }

    #[test]
    fn test_resolve_payload_template_and_validation() {
        let mut args = Args::parse_from(["mqtt-sniper"]);
        assert_eq!(resolve_payload_template(&args).unwrap(), args.payload);

        args.malformed_rate = 1.5;
        assert!(resolve_payload_template(&args).is_err());

        args.malformed_rate = 0.05;
        args.min_success_rate = Some(1.2);
        assert!(resolve_payload_template(&args).is_err());

        args.min_success_rate = Some(0.995);
        args.max_p95_ms = Some(-5.0);
        assert!(resolve_payload_template(&args).is_err());

        args.max_p95_ms = Some(100.0);
        let tmp_file = std::env::temp_dir().join("mqtt_sniper_test_payload.json");
        fs::write(&tmp_file, "  {\"deviceId\": \"sensor-{device_id}\"} \n").unwrap();
        args.payload_file = Some(tmp_file.to_str().unwrap().to_string());
        assert_eq!(
            resolve_payload_template(&args).unwrap(),
            r#"{"deviceId": "sensor-{device_id}"}"#
        );
        let _ = fs::remove_file(&tmp_file);
    }

    #[test]
    fn test_build_transport_validation() {
        let mut args = Args::parse_from(["mqtt-sniper"]);
        assert!(build_transport(&args).is_ok());

        args.tls = true;
        assert!(build_transport(&args).is_ok());

        args.cert = Some("cert.pem".to_string());
        assert!(build_transport(&args).is_err());
    }
}
