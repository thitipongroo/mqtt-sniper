use mqtt_sniper::cli::{Args, Mode};
use mqtt_sniper::runner::run_load_test;
use std::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

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
                        } else if ptype == 8 {
                            // SUBSCRIBE -> Send SUBACK and push expected QoS 0 PUBLISH packets
                            let pkid = [body[0], body[1]];
                            let _ = socket
                                .write_all(&[0x90, 0x03, pkid[0], pkid[1], 0x00])
                                .await;

                            let topic = b"test/sub";
                            let payload = br#"{"device_id": 0, "seq": 0}"#;
                            let rem = 2 + topic.len() + payload.len();
                            let mut pub_pkt = vec![0x30, rem as u8];
                            pub_pkt.extend_from_slice(&(topic.len() as u16).to_be_bytes());
                            pub_pkt.extend_from_slice(topic);
                            pub_pkt.extend_from_slice(payload);

                            for _ in 0..3 {
                                let _ = socket.write_all(&pub_pkt).await;
                            }
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

fn default_test_args(port: u16, clients: usize, messages: usize, qos: u8, topic: &str) -> Args {
    Args {
        broker: "127.0.0.1".to_string(),
        port,
        mode: Mode::Pub,
        clients,
        start_number: 0,
        client_prefix: "sniper-client-".to_string(),
        max_connections: 0,
        messages,
        duration_secs: 0,
        topic: topic.to_string(),
        qos,
        retain: false,
        keep_alive: 5,
        ramp_up_ms: 0,
        interval_ms: 0,
        payload: "{}".to_string(),
        payload_file: None,
        payload_size: 0,
        malformed_rate: 0.0,
        username: None,
        password: None,
        tls: false,
        ca: None,
        cert: None,
        key: None,
        output_json: None,
    }
}

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
    assert!(content.contains(r#""actual_sent": 2"#));
    assert!(content.contains(r#""latency_p99_ms""#));
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
