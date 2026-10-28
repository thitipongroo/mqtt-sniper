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

/// Output format for benchmark reports
#[derive(ValueEnum, Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum OutputFormat {
    /// Human-readable terminal output with real-time statistics
    #[default]
    Text,
    /// Structured JSON schema formatted for automated analysis and CI pipelines
    Json,
    /// Prometheus OpenMetrics text format for scraping and metric collectors
    Prometheus,
}

/// CLI argument parsing structure
#[derive(Parser, Debug, Clone)]
#[command(author, version, about = "Blazing fast MQTT Load Tester")]
pub struct Args {
    /// Target MQTT broker hostname, IP address, or URL (e.g. localhost, mqtt://127.0.0.1:1883, or mqtts://broker:8883)
    #[arg(short, long, default_value = "localhost")]
    pub broker: String,

    /// Target MQTT broker TCP port
    #[arg(short, long, default_value_t = 1883)]
    pub port: u16,

    /// Benchmark mode: 'pub' (publish), 'sub' (subscribe), or 'conn' (connection storm)
    #[arg(long, value_enum, default_value_t = Mode::Pub)]
    pub mode: Mode,

    /// MQTT protocol version (3 or 4 = MQTT v3.1.1, 5 = MQTT v5.0)
    #[arg(long, default_value_t = 4, value_parser = clap::value_parser!(u8).range(3..=5))]
    pub mqtt_version: u8,

    /// Number of concurrent simulated IoT clients
    #[arg(short, long, default_value_t = 100)]
    pub clients: usize,

    /// Starting index for client IDs and {device_id} variable (e.g., 1 for sensor-1..sensor-N)
    #[arg(short = 'n', long, default_value_t = 0)]
    pub start_number: usize,

    /// Prefix for generated MQTT client IDs (client ID = <prefix><device_id>)
    #[arg(long, default_value = "sniper-client-")]
    pub client_prefix: String,

    /// Maximum number of active concurrent connections (0 = unlimited / auto-bounded by OS limit)
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

    /// SLO Gate: Maximum allowed p95 latency in milliseconds (exits with code 1 if exceeded)
    #[arg(long)]
    pub max_p95_ms: Option<f64>,

    /// SLO Gate: Minimum required delivery/connection success rate between 0.0 and 1.0 (exits with code 1 if below)
    #[arg(long)]
    pub min_success_rate: Option<f64>,

    /// Output format: 'text', 'json', or 'prometheus'
    #[arg(short = 'o', long, value_enum, default_value_t = OutputFormat::Text)]
    pub output: OutputFormat,

    /// Path to write output report to a file
    #[arg(long)]
    pub output_file: Option<String>,

    /// Suppress interactive progress bar and informational logs
    #[arg(long, short = 's', alias = "silent", default_value_t = false)]
    pub quiet: bool,

    /// Path to export benchmark report as a JSON file (legacy alias)
    #[arg(long)]
    pub output_json: Option<String>,
}

pub fn normalize_broker_target(args: &mut Args) {
    let raw = args.broker.trim().to_string();
    if let Some(rest) = raw.strip_prefix("mqtts://") {
        args.tls = true;
        parse_host_and_port(rest, args, 8883);
    } else if let Some(rest) = raw.strip_prefix("ssl://") {
        args.tls = true;
        parse_host_and_port(rest, args, 8883);
    } else if let Some(rest) = raw.strip_prefix("mqtt://") {
        parse_host_and_port(rest, args, 1883);
    } else if let Some(rest) = raw.strip_prefix("tcp://") {
        parse_host_and_port(rest, args, 1883);
    } else {
        parse_host_and_port(&raw, args, 1883);
    }
}

fn parse_host_and_port(input: &str, args: &mut Args, default_port: u16) {
    if input.starts_with('[') {
        if let Some((bracketed, port_str)) = input.rsplit_once(':')
            && bracketed.ends_with(']')
            && let Ok(p) = port_str.parse::<u16>()
        {
            args.broker = bracketed.trim_matches(|c| c == '[' || c == ']').to_string();
            args.port = p;
            return;
        }
        args.broker = input.trim_matches(|c| c == '[' || c == ']').to_string();
        if args.port == 1883 && default_port != 1883 {
            args.port = default_port;
        }
    } else if let Some((host, port_str)) = input.rsplit_once(':') {
        if !host.contains(':')
            && let Ok(p) = port_str.parse::<u16>()
        {
            args.broker = host.to_string();
            args.port = p;
            return;
        }
        args.broker = input.to_string();
        if args.port == 1883 && default_port != 1883 {
            args.port = default_port;
        }
    } else {
        args.broker = input.to_string();
        if args.port == 1883 && default_port != 1883 {
            args.port = default_port;
        }
    }
}

pub fn parse_qos(qos: u8) -> QoS {
    match qos {
        0 => QoS::AtMostOnce,
        1 => QoS::AtLeastOnce,
        2 => QoS::ExactlyOnce,
        _ => {
            eprintln!("⚠️ Invalid QoS level '{}'. Defaulting to QoS 0.", qos);
            QoS::AtMostOnce
        }
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

/// Reserved file descriptors for stdio, epoll, log files, and runtime overhead
pub const FD_SAFETY_MARGIN: u64 = 64;

/// Result of checking OS file descriptor limits against requested benchmark concurrency
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FdLimitCheck {
    pub soft_limit: u64,
    pub hard_limit: u64,
    pub safe_limit: usize,
    pub target_connections: usize,
    pub is_exceeded: bool,
    pub warning: Option<String>,
}

/// Query OS file descriptor limits (soft_limit, hard_limit)
#[cfg(unix)]
pub fn get_os_fd_limits() -> (u64, u64) {
    let mut rlim = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    let ret = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlim) };
    if ret == 0 {
        let cur = if rlim.rlim_cur == libc::RLIM_INFINITY {
            u64::MAX
        } else {
            rlim.rlim_cur as u64
        };
        let max = if rlim.rlim_max == libc::RLIM_INFINITY {
            u64::MAX
        } else {
            rlim.rlim_max as u64
        };
        (cur, max)
    } else {
        (1024, 4096)
    }
}

/// Attempt to raise the soft file descriptor limit up to hard limit on Unix
#[cfg(unix)]
pub fn try_raise_fd_limit(desired: u64) -> u64 {
    let mut rlim = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    let ret = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlim) };
    if ret != 0 {
        return 1024;
    }
    let current_soft = if rlim.rlim_cur == libc::RLIM_INFINITY {
        u64::MAX
    } else {
        rlim.rlim_cur as u64
    };
    let hard = if rlim.rlim_max == libc::RLIM_INFINITY {
        u64::MAX
    } else {
        rlim.rlim_max as u64
    };

    if desired > current_soft && hard > current_soft {
        let target_soft = desired.min(hard);
        let mut new_rlim = rlim;
        new_rlim.rlim_cur = if target_soft == u64::MAX {
            libc::RLIM_INFINITY
        } else {
            target_soft as libc::rlim_t
        };
        if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &new_rlim) } == 0 {
            return target_soft;
        }
    }
    current_soft
}

/// Query OS file descriptor / socket limits on non-unix platforms (Windows fallback)
#[cfg(not(unix))]
pub fn get_os_fd_limits() -> (u64, u64) {
    (16_384, 65_535)
}

/// Attempt to raise limit on non-unix platforms (no-op clamp)
#[cfg(not(unix))]
pub fn try_raise_fd_limit(desired: u64) -> u64 {
    let (soft, hard) = get_os_fd_limits();
    desired.min(hard).max(soft)
}

/// Deterministically evaluate file descriptor safety against given limits
pub fn evaluate_fd_limits(
    clients: usize,
    max_connections: usize,
    soft_limit: u64,
    hard_limit: u64,
) -> FdLimitCheck {
    let target = if max_connections > 0 {
        max_connections.min(clients)
    } else {
        clients
    };

    let safe_limit = soft_limit
        .saturating_sub(FD_SAFETY_MARGIN)
        .max(1) as usize;

    let is_exceeded = (target as u64) > soft_limit.saturating_sub(FD_SAFETY_MARGIN);

    let warning = if is_exceeded {
        let required = (target as u64).saturating_add(FD_SAFETY_MARGIN);
        let recommended = required.max(65_535);
        Some(format!(
            "⚠️  File Descriptor Warning: Requested concurrency ({target}) exceeds OS file descriptor safe limit ({safe_limit} connections; OS soft={soft_limit}, hard={hard_limit}).\n\
             ⚠️  Risk: Potential 'Too many open files' (EMFILE / OS error 24) process crash.\n\
             💡 Recommendation: Increase OS file descriptor limit before running:\n\
                * Temporary: Run `ulimit -n {recommended}` in your shell\n\
                * Permanent: Add `* soft nofile {recommended}` and `* hard nofile {recommended}` to /etc/security/limits.conf\n\
             🛡️  Safeguard: Concurrency will be bounded by semaphore to {safe_limit} active connections."
        ))
    } else {
        None
    };

    FdLimitCheck {
        soft_limit,
        hard_limit,
        safe_limit,
        target_connections: target,
        is_exceeded,
        warning,
    }
}

/// Pre-flight check examining OS soft/hard file descriptor limits and returning safety bounds
pub fn check_file_descriptor_limits(clients: usize, max_connections: usize) -> FdLimitCheck {
    let target = if max_connections > 0 {
        max_connections.min(clients)
    } else {
        clients
    };

    let (mut soft, hard) = get_os_fd_limits();
    let required = (target as u64).saturating_add(FD_SAFETY_MARGIN);

    if required > soft && hard > soft {
        soft = try_raise_fd_limit(required);
    }

    evaluate_fd_limits(clients, max_connections, soft, hard)
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
        assert_eq!(args.mqtt_version, 4);
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
        assert_eq!(args.output, OutputFormat::Text);
        assert_eq!(args.output_file, None);
        assert!(!args.quiet);
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
            "--mqtt-version",
            "5",
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
            "--output",
            "json",
            "--output-file",
            "report.json",
            "--quiet",
            "--output-json",
            "legacy-report.json",
        ]);
        assert_eq!(args.broker, "10.0.0.1");
        assert_eq!(args.port, 8883);
        assert_eq!(args.mode, Mode::Conn);
        assert_eq!(args.mqtt_version, 5);
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
        assert_eq!(args.output, OutputFormat::Json);
        assert_eq!(args.output_file, Some("report.json".to_string()));
        assert!(args.quiet);
        assert_eq!(args.output_json, Some("legacy-report.json".to_string()));

        // Check --silent alias
        let silent_args = Args::parse_from(["mqtt-sniper", "--silent"]);
        assert!(silent_args.quiet);
    }

    #[test]
    fn test_normalize_broker_target() {
        let mut args1 = Args::parse_from(["mqtt-sniper", "-b", "mqtt://10.0.0.1:1884"]);
        normalize_broker_target(&mut args1);
        assert_eq!(args1.broker, "10.0.0.1");
        assert_eq!(args1.port, 1884);
        assert!(!args1.tls);

        let mut args2 = Args::parse_from(["mqtt-sniper", "-b", "mqtts://secure-broker.io"]);
        normalize_broker_target(&mut args2);
        assert_eq!(args2.broker, "secure-broker.io");
        assert_eq!(args2.port, 8883);
        assert!(args2.tls);

        let mut args3 = Args::parse_from(["mqtt-sniper", "-b", "tcp://my-broker:1883"]);
        normalize_broker_target(&mut args3);
        assert_eq!(args3.broker, "my-broker");
        assert_eq!(args3.port, 1883);

        let mut args4 = Args::parse_from(["mqtt-sniper", "-b", "plain-host", "-p", "1885"]);
        normalize_broker_target(&mut args4);
        assert_eq!(args4.broker, "plain-host");
        assert_eq!(args4.port, 1885);

        let mut args5 = Args::parse_from(["mqtt-sniper", "-b", "localhost:1883"]);
        normalize_broker_target(&mut args5);
        assert_eq!(args5.broker, "localhost");
        assert_eq!(args5.port, 1883);

        let mut args6 = Args::parse_from(["mqtt-sniper", "-b", "[::1]:1883"]);
        normalize_broker_target(&mut args6);
        assert_eq!(args6.broker, "::1");
        assert_eq!(args6.port, 1883);
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

    #[test]
    fn test_fd_limits_safe_range() {
        let check = evaluate_fd_limits(100, 0, 1024, 4096);
        assert_eq!(check.target_connections, 100);
        assert_eq!(check.safe_limit, 960);
        assert!(!check.is_exceeded);
        assert!(check.warning.is_none());
    }

    #[test]
    fn test_fd_limits_exceeded_triggers_warning_and_safe_bound() {
        let check = evaluate_fd_limits(100_000, 0, 1024, 4096);
        assert_eq!(check.target_connections, 100_000);
        assert_eq!(check.safe_limit, 960);
        assert!(check.is_exceeded);
        let warn = check.warning.expect("Warning must be generated");
        assert!(warn.contains("ulimit -n"));
        assert!(warn.contains("Too many open files"));
        assert!(warn.contains("bounded by semaphore to 960"));
    }

    #[test]
    fn test_fd_limits_user_max_connections_respected() {
        let check = evaluate_fd_limits(100_000, 500, 1024, 4096);
        assert_eq!(check.target_connections, 500);
        assert_eq!(check.safe_limit, 960);
        assert!(!check.is_exceeded);
        assert!(check.warning.is_none());
    }

    #[test]
    fn test_check_file_descriptor_limits_live() {
        let check = check_file_descriptor_limits(50, 0);
        assert_eq!(check.target_connections, 50);
        assert!(check.safe_limit > 0);
        assert!(!check.is_exceeded);
    }
}
