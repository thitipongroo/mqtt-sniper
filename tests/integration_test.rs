use mqtt_sniper::cli::Args;
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
