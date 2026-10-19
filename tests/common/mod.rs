use mqtt_sniper::cli::{Args, Mode};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

pub async fn start_mock_broker(expected_qos: u8, expected_clients: usize) -> u16 {
    start_mock_broker_with_version(expected_qos, expected_clients, 4).await
}

pub async fn start_mock_broker_with_version(
    expected_qos: u8,
    expected_clients: usize,
    mqtt_version: u8,
) -> u16 {
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
                    // Send CONNACK (v5 includes 1-byte properties length = 0x00)
                    if mqtt_version == 5 {
                        let _ = socket.write_all(&[0x20, 0x03, 0x00, 0x00, 0x00]).await;
                    } else {
                        let _ = socket.write_all(&[0x20, 0x02, 0x00, 0x00]).await;
                    }

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

pub fn default_test_args(port: u16, clients: usize, messages: usize, qos: u8, topic: &str) -> Args {
    Args {
        broker: "127.0.0.1".to_string(),
        port,
        mode: Mode::Pub,
        mqtt_version: 4,
        clients,
        start_number: 0,
        client_prefix: "sniper-client-".to_string(),
        max_connections: 0,
        messages,
        duration_secs: 0,
        topic: topic.to_string(),
        qos,
        retain: false,
        clean_session: true,
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
        max_p95_ms: None,
        min_success_rate: None,
        output_json: None,
    }
}
