use clap::Parser;
use hdrhistogram::Histogram;
use indicatif::{ProgressBar, ProgressStyle};
use rumqttc::{AsyncClient, Event, MqttOptions, Outgoing, Packet, QoS};
use serde::Serialize;
use std::collections::HashMap;
use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio::task;

// 1. CLI argument parsing structure
#[derive(Parser, Debug)]
#[command(author, version, about = "Blazing fast MQTT Load Tester")]
struct Args {
    /// Target MQTT broker hostname or IP address
    #[arg(short, long, default_value = "localhost")]
    broker: String,

    /// Target MQTT broker TCP port
    #[arg(short, long, default_value_t = 1883)]
    port: u16,

    /// Number of concurrent simulated IoT clients
    #[arg(short, long, default_value_t = 100)]
    clients: usize,

    /// Number of messages published per client
    #[arg(short, long, default_value_t = 10)]
    messages: usize,

    /// MQTT topic to publish messages to (supports {device_id})
    #[arg(short, long, default_value = "sensor/sniper")]
    topic: String,

    /// MQTT Quality of Service level (0 = AtMostOnce, 1 = AtLeastOnce, 2 = ExactlyOnce)
    #[arg(short, long, default_value_t = 0, value_parser = clap::value_parser!(u8).range(0..=2))]
    qos: u8,

    /// Delay (ms) before spawning consecutive clients (prevents TCP SYN flood)
    #[arg(long, default_value_t = 0)]
    ramp_up_ms: u64,

    /// Delay (ms) between consecutive messages from the same client
    #[arg(short, long, default_value_t = 0)]
    interval_ms: u64,

    /// Custom payload template (supports {device_id}, {seq}, {timestamp})
    #[arg(
        long,
        default_value = r#"{"device_id": {device_id}, "seq": {seq}, "ts": {timestamp}}"#
    )]
    payload: String,

    /// Minimum payload size in bytes (padded with 'x' if shorter)
    #[arg(long, default_value_t = 0)]
    payload_size: usize,

    /// Username for MQTT broker authentication
    #[arg(short, long)]
    username: Option<String>,

    /// Password for MQTT broker authentication
    #[arg(long)]
    password: Option<String>,

    /// Export benchmark results to a JSON file
    #[arg(long)]
    output_json: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct BenchmarkReport {
    broker: String,
    port: u16,
    clients: usize,
    messages_per_client: usize,
    qos: u8,
    target_messages: usize,
    actual_sent: usize,
    failed_clients: usize,
    elapsed_secs: f64,
    throughput_msgs_per_sec: f64,
    latency_min_ms: f64,
    latency_avg_ms: f64,
    latency_p50_ms: f64,
    latency_p95_ms: f64,
    latency_p99_ms: f64,
    latency_max_ms: f64,
}

fn parse_qos(qos: u8) -> QoS {
    match qos {
        1 => QoS::AtLeastOnce,
        2 => QoS::ExactlyOnce,
        _ => QoS::AtMostOnce,
    }
}

fn build_payload(template: &str, device_id: usize, seq: usize, min_size: usize) -> Vec<u8> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();

    // Replace placeholders in template with dynamic values
    let mut rendered = template
        .replace("{device_id}", &device_id.to_string())
        .replace("{seq}", &seq.to_string())
        .replace("{timestamp}", &timestamp.to_string());

    // Pad payload with 'x' bytes if minimum size is requested
    if min_size > rendered.len() {
        let padding_len = min_size - rendered.len();
        rendered.push_str(&"x".repeat(padding_len));
    }

    rendered.into_bytes()
}

// 2. Main entry point (wrapped in tokio async runtime)
#[tokio::main]
async fn main() {
    let args = Args::parse();
    let _ = run_load_test(args).await;
}

async fn run_load_test(args: Args) -> BenchmarkReport {
    let qos = parse_qos(args.qos);
    let target_messages = args.clients * args.messages;

    println!("🚀 Starting MQTT Sniper...");
    println!("Broker: tcp://{}:{}", args.broker, args.port);
    println!(
        "Spawning {} concurrent clients ({} msgs/client, QoS {})...",
        args.clients, args.messages, args.qos
    );

    let pb = ProgressBar::new(target_messages as u64);
    if let Ok(style) = ProgressStyle::with_template(
        "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({percent}%) | {per_sec} | ETA: {eta}",
    ) {
        pb.set_style(style.progress_chars("█▓░"));
    }

    let start_time = Instant::now();
    let mut handles = Vec::with_capacity(args.clients);
    let published_count = Arc::new(AtomicUsize::new(0));
    let failed_clients = Arc::new(AtomicUsize::new(0));

    // 3. Spawn concurrent client tasks
    for i in 0..args.clients {
        // Apply ramp-up delay before spawning the next client if configured
        if args.ramp_up_ms > 0 && i > 0 {
            tokio::time::sleep(Duration::from_millis(args.ramp_up_ms)).await;
        }

        let broker = args.broker.clone();
        let topic = args.topic.replace("{device_id}", &i.to_string());
        let payload_template = args.payload.clone();
        let payload_size = args.payload_size;
        let port = args.port;
        let messages = args.messages;
        let interval_ms = args.interval_ms;
        let username = args.username.clone();
        let password = args.password.clone();
        let published_counter = Arc::clone(&published_count);
        let failed_counter = Arc::clone(&failed_clients);
        let pb_clone = pb.clone();

        // Spawn independent task for each simulated client
        let handle = task::spawn(async move {
            let client_id = format!("sniper-client-{}", i);
            let mut mqttoptions = MqttOptions::new(client_id, broker, port);
            mqttoptions.set_keep_alive(Duration::from_secs(5));

            if let Some(ref user) = username {
                mqttoptions.set_credentials(user, password.as_deref().unwrap_or(""));
            }

            let (client, mut eventloop) = AsyncClient::new(mqttoptions, 50);
            let client_clone = client.clone();
            let (tx_time, mut rx_time) = mpsc::unbounded_channel::<Instant>();

            // Publish messages asynchronously to the client's request channel
            let publisher = tokio::spawn(async move {
                for j in 0..messages {
                    let payload_bytes = build_payload(&payload_template, i, j, payload_size);
                    let _ = tx_time.send(Instant::now());
                    if client
                        .publish(topic.as_str(), qos, false, payload_bytes)
                        .await
                        .is_err()
                    {
                        break;
                    }
                    if interval_ms > 0 && j + 1 < messages {
                        tokio::time::sleep(Duration::from_millis(interval_ms)).await;
                    }
                }
                // For QoS 0 (or when 0 messages), disconnect immediately once all messages are queued
                if qos == QoS::AtMostOnce || messages == 0 {
                    let _ = client.disconnect().await;
                }
            });

            // Poll eventloop to drive network I/O:
            // - For QoS 0: wait until Disconnect is flushed out
            // - For QoS 1/2: wait for all PUBACK / PUBCOMP acknowledgments before disconnecting
            let mut acked_count = 0usize;
            let mut inflight: HashMap<u16, Instant> = HashMap::new();
            let mut latencies_us: Vec<u64> = Vec::with_capacity(messages);

            loop {
                match eventloop.poll().await {
                    Ok(Event::Outgoing(Outgoing::Publish(pkid))) => {
                        if qos == QoS::AtMostOnce {
                            if let Ok(t0) = rx_time.try_recv() {
                                latencies_us.push(t0.elapsed().as_micros() as u64);
                            }
                            published_counter.fetch_add(1, Ordering::Relaxed);
                            pb_clone.inc(1);
                        } else if let Ok(t0) = rx_time.try_recv() {
                            inflight.insert(pkid, t0);
                        }
                    }
                    Ok(Event::Incoming(Packet::PubAck(ack))) => {
                        if qos == QoS::AtLeastOnce {
                            if let Some(t0) = inflight.remove(&ack.pkid) {
                                latencies_us.push(t0.elapsed().as_micros() as u64);
                            }
                            published_counter.fetch_add(1, Ordering::Relaxed);
                            pb_clone.inc(1);
                            acked_count += 1;
                            if acked_count == messages {
                                let _ = client_clone.disconnect().await;
                            }
                        }
                    }
                    Ok(Event::Incoming(Packet::PubComp(comp))) => {
                        if qos == QoS::ExactlyOnce {
                            if let Some(t0) = inflight.remove(&comp.pkid) {
                                latencies_us.push(t0.elapsed().as_micros() as u64);
                            }
                            published_counter.fetch_add(1, Ordering::Relaxed);
                            pb_clone.inc(1);
                            acked_count += 1;
                            if acked_count == messages {
                                let _ = client_clone.disconnect().await;
                            }
                        }
                    }
                    Ok(Event::Outgoing(Outgoing::Disconnect)) => {
                        break;
                    }
                    Ok(_) => {}
                    Err(_) => {
                        failed_counter.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                }
            }

            publisher.abort();
            let _ = publisher.await;
            latencies_us
        });

        handles.push(handle);
    }

    // 4. Await completion of all client tasks and aggregate latency histogram
    let mut hist =
        Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).expect("Failed to create histogram");

    for handle in handles {
        if let Ok(client_latencies) = handle.await {
            for lat_us in client_latencies {
                let _ = hist.record(lat_us.clamp(1, 60_000_000));
            }
        }
    }

    pb.finish_and_clear();

    // 5. Calculate and print benchmark summary
    let elapsed = start_time.elapsed();
    let elapsed_secs = elapsed.as_secs_f64();
    let actual_sent = published_count.load(Ordering::Relaxed);
    let errors = failed_clients.load(Ordering::Relaxed);
    let throughput = if elapsed_secs > 0.0 {
        (actual_sent as f64) / elapsed_secs
    } else {
        0.0
    };

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

    println!("✅ Load test completed in {:.2?}", elapsed);
    println!(
        "📊 Total messages sent: {} / {} (Failed clients: {})",
        actual_sent, target_messages, errors
    );
    if elapsed_secs > 0.0 {
        println!("⚡ Throughput: {:.2} msgs/sec", throughput);
    }
    println!(
        "⏱️  Latency (ms): min={:.2} | avg={:.2} | p50={:.2} | p95={:.2} | p99={:.2} | max={:.2}",
        min_ms, avg_ms, p50_ms, p95_ms, p99_ms, max_ms
    );

    let report = BenchmarkReport {
        broker: args.broker,
        port: args.port,
        clients: args.clients,
        messages_per_client: args.messages,
        qos: args.qos,
        target_messages,
        actual_sent,
        failed_clients: errors,
        elapsed_secs,
        throughput_msgs_per_sec: throughput,
        latency_min_ms: min_ms,
        latency_avg_ms: avg_ms,
        latency_p50_ms: p50_ms,
        latency_p95_ms: p95_ms,
        latency_p99_ms: p99_ms,
        latency_max_ms: max_ms,
    };

    if let Some(ref path) = args.output_json
        && let Ok(json_str) = serde_json::to_string_pretty(&report)
    {
        if let Err(e) = fs::write(path, json_str) {
            eprintln!("⚠️ Failed to write JSON report to {}: {}", path, e);
        } else {
            println!("💾 Saved JSON benchmark report to {}", path);
        }
    }

    report
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

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

    #[test]
    fn test_build_payload() {
        let template = r#"{"id":{device_id},"s":{seq}}"#;
        let payload = build_payload(template, 42, 7, 0);
        let s = String::from_utf8(payload).unwrap();
        assert_eq!(s, r#"{"id":42,"s":7}"#);

        // Test with minimum size padding
        let padded = build_payload(template, 42, 7, 25);
        assert_eq!(padded.len(), 25);
        let padded_str = String::from_utf8(padded).unwrap();
        assert!(padded_str.starts_with(r#"{"id":42,"s":7}"#));
        assert!(padded_str.ends_with("xxxxxxxxx"));
    }

    async fn start_mock_broker(expected_qos: u8, expected_clients: usize) -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        tokio::spawn(async move {
            let mut client_tasks = Vec::new();
            for _ in 0..expected_clients {
                if let Ok((mut socket, _)) = listener.accept().await {
                    let task = tokio::spawn(async move {
                        let mut buf = [0u8; 1024];
                        // Read CONNECT
                        let _ = socket.read(&mut buf).await;
                        // Send CONNACK
                        let _ = socket.write_all(&[0x20, 0x02, 0x00, 0x00]).await;

                        loop {
                            let mut header = [0u8; 2];
                            if socket.read_exact(&mut header).await.is_err() {
                                break;
                            }
                            let ptype = header[0] >> 4;
                            let rem_len = header[1] as usize;
                            let mut body = vec![0u8; rem_len];
                            if socket.read_exact(&mut body).await.is_err() {
                                break;
                            }

                            if ptype == 3 {
                                // PUBLISH
                                if expected_qos == 1 {
                                    let topic_len = u16::from_be_bytes([body[0], body[1]]) as usize;
                                    let pkid = &body[2 + topic_len..2 + topic_len + 2];
                                    let _ = socket.write_all(&[0x40, 0x02, pkid[0], pkid[1]]).await;
                                } else if expected_qos == 2 {
                                    let topic_len = u16::from_be_bytes([body[0], body[1]]) as usize;
                                    let pkid = &body[2 + topic_len..2 + topic_len + 2];
                                    let _ = socket.write_all(&[0x50, 0x02, pkid[0], pkid[1]]).await;
                                }
                            } else if ptype == 6 {
                                // PUBREL -> Send PUBCOMP
                                let _ = socket.write_all(&[0x70, 0x02, body[0], body[1]]).await;
                            } else if ptype == 14 {
                                // DISCONNECT
                                break;
                            }
                        }
                    });
                    client_tasks.push(task);
                }
            }
            for task in client_tasks {
                let _ = task.await;
            }
        });

        port
    }

    #[tokio::test]
    async fn test_load_test_qos0() {
        let clients = 2;
        let messages = 3;
        let port = start_mock_broker(0, clients).await;

        let args = Args {
            broker: "127.0.0.1".to_string(),
            port,
            clients,
            messages,
            topic: "test/qos0".to_string(),
            qos: 0,
            ramp_up_ms: 0,
            interval_ms: 0,
            payload: "{}".to_string(),
            payload_size: 0,
            username: None,
            password: None,
            output_json: None,
        };

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

        let args = Args {
            broker: "127.0.0.1".to_string(),
            port,
            clients,
            messages,
            topic: "test/qos1".to_string(),
            qos: 1,
            ramp_up_ms: 0,
            interval_ms: 0,
            payload: "{}".to_string(),
            payload_size: 0,
            username: None,
            password: None,
            output_json: None,
        };

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

        let args = Args {
            broker: "127.0.0.1".to_string(),
            port,
            clients,
            messages,
            topic: "test/qos2".to_string(),
            qos: 2,
            ramp_up_ms: 0,
            interval_ms: 0,
            payload: "{}".to_string(),
            payload_size: 0,
            username: None,
            password: None,
            output_json: None,
        };

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

        let args = Args {
            broker: "127.0.0.1".to_string(),
            port,
            clients,
            messages,
            topic: "test/json".to_string(),
            qos: 1,
            ramp_up_ms: 0,
            interval_ms: 0,
            payload: "{}".to_string(),
            payload_size: 0,
            username: None,
            password: None,
            output_json: Some(tmp_path_str.clone()),
        };

        let report = run_load_test(args).await;
        assert_eq!(report.actual_sent, 2);
        assert!(tmp_path.exists());

        let content = fs::read_to_string(&tmp_path).unwrap();
        assert!(content.contains(r#""actual_sent": 2"#));
        assert!(content.contains(r#""latency_p99_ms""#));
        let _ = fs::remove_file(&tmp_path);
    }
}
