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

    /// Benchmark mode: 'pub' (publish) or 'sub' (subscribe)
    #[arg(long, value_enum, default_value_t = Mode::Pub)]
    pub mode: Mode,

    /// Number of concurrent simulated IoT clients
    #[arg(short, long, default_value_t = 100)]
    pub clients: usize,

    /// Maximum number of active concurrent connections (0 = unlimited)
    #[arg(long, default_value_t = 0)]
    pub max_connections: usize,

    /// Number of messages published (or received in 'sub' mode) per client
    #[arg(short, long, default_value_t = 10)]
    pub messages: usize,

    /// MQTT topic to publish or subscribe to (supports {device_id})
    #[arg(short, long, default_value = "sensor/sniper")]
    pub topic: String,

    /// MQTT Quality of Service level (0 = AtMostOnce, 1 = AtLeastOnce, 2 = ExactlyOnce)
    #[arg(short, long, default_value_t = 0, value_parser = clap::value_parser!(u8).range(0..=2))]
    pub qos: u8,

    /// Delay (ms) before spawning consecutive clients (prevents TCP SYN flood)
    #[arg(long, default_value_t = 0)]
    pub ramp_up_ms: u64,

    /// Delay (ms) between consecutive messages from the same client
    #[arg(short, long, default_value_t = 0)]
    pub interval_ms: u64,

    /// Custom payload template (supports {device_id}, {seq}, {timestamp})
    #[arg(
        long,
        default_value = r#"{"device_id": {device_id}, "seq": {seq}, "ts": {timestamp}}"#
    )]
    pub payload: String,

    /// Minimum payload size in bytes (padded with 'x' if shorter)
    #[arg(long, default_value_t = 0)]
    pub payload_size: usize,

    /// Username for MQTT broker authentication
    #[arg(short, long)]
    pub username: Option<String>,

    /// Password for MQTT broker authentication
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
        assert_eq!(args.max_connections, 0);
        assert_eq!(args.messages, 10);
        assert_eq!(args.topic, "sensor/sniper");
        assert_eq!(args.qos, 0);
        assert_eq!(args.ramp_up_ms, 0);
        assert_eq!(args.interval_ms, 0);
        assert_eq!(
            args.payload,
            r#"{"device_id": {device_id}, "seq": {seq}, "ts": {timestamp}}"#
        );
        assert_eq!(args.payload_size, 0);
        assert_eq!(args.username, None);
        assert_eq!(args.password, None);
        assert!(!args.tls);
        assert_eq!(args.ca, None);
        assert_eq!(args.cert, None);
        assert_eq!(args.key, None);
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
            "sub",
            "-c",
            "50",
            "--max-connections",
            "25",
            "-m",
            "20",
            "-t",
            "test/topic/{device_id}",
            "-q",
            "2",
            "--ramp-up-ms",
            "5",
            "-i",
            "10",
            "--payload",
            r#"{"id": {device_id}}"#,
            "--payload-size",
            "64",
            "-u",
            "myuser",
            "--password",
            "mypass",
            "--tls",
            "--ca",
            "ca.pem",
            "--cert",
            "client.crt",
            "--key",
            "client.key",
            "--output-json",
            "report.json",
        ]);
        assert_eq!(args.broker, "10.0.0.1");
        assert_eq!(args.port, 8883);
        assert_eq!(args.mode, Mode::Sub);
        assert_eq!(args.clients, 50);
        assert_eq!(args.max_connections, 25);
        assert_eq!(args.messages, 20);
        assert_eq!(args.topic, "test/topic/{device_id}");
        assert_eq!(args.qos, 2);
        assert_eq!(args.ramp_up_ms, 5);
        assert_eq!(args.interval_ms, 10);
        assert_eq!(args.payload, r#"{"id": {device_id}}"#);
        assert_eq!(args.payload_size, 64);
        assert_eq!(args.username, Some("myuser".to_string()));
        assert_eq!(args.password, Some("mypass".to_string()));
        assert!(args.tls);
        assert_eq!(args.ca, Some("ca.pem".to_string()));
        assert_eq!(args.cert, Some("client.crt".to_string()));
        assert_eq!(args.key, Some("client.key".to_string()));
        assert_eq!(args.output_json, Some("report.json".to_string()));
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
