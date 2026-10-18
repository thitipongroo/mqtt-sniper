use clap::Parser;
use rumqttc::QoS;

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

    /// Number of concurrent simulated IoT clients
    #[arg(short, long, default_value_t = 100)]
    pub clients: usize,

    /// Number of messages published per client
    #[arg(short, long, default_value_t = 10)]
    pub messages: usize,

    /// MQTT topic to publish messages to (supports {device_id})
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
        assert_eq!(args.clients, 100);
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
        assert_eq!(args.output_json, None);
    }

    #[test]
    fn test_cli_custom_args() {
        let args = Args::parse_from([
            "mqtt-sniper",
            "-b",
            "10.0.0.1",
            "-p",
            "1884",
            "-c",
            "50",
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
            "--output-json",
            "report.json",
        ]);
        assert_eq!(args.broker, "10.0.0.1");
        assert_eq!(args.port, 1884);
        assert_eq!(args.clients, 50);
        assert_eq!(args.messages, 20);
        assert_eq!(args.topic, "test/topic/{device_id}");
        assert_eq!(args.qos, 2);
        assert_eq!(args.ramp_up_ms, 5);
        assert_eq!(args.interval_ms, 10);
        assert_eq!(args.payload, r#"{"id": {device_id}}"#);
        assert_eq!(args.payload_size, 64);
        assert_eq!(args.username, Some("myuser".to_string()));
        assert_eq!(args.password, Some("mypass".to_string()));
        assert_eq!(args.output_json, Some("report.json".to_string()));
    }
}
