use super::client::ClientSpawnContext;
use super::stats::extract_payload_latency_us;
use crate::cli::Mode;
use crate::payload::build_payload_with_chaos;
use rumqttc::Outgoing;
use rumqttc::v5::mqttbytes::QoS as QoS5;
use rumqttc::v5::mqttbytes::v5::Packet as Packet5;
use rumqttc::v5::{AsyncClient as AsyncClient5, Event as Event5, MqttOptions as MqttOptions5};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;

/// Execute an MQTT v5.0 client session to completion or until shutdown signal.
pub async fn run_client_v5(ctx: ClientSpawnContext) {
    let ClientSpawnContext {
        device_id,
        client_id,
        broker,
        port,
        mode,
        qos_num,
        topic,
        payload_template,
        payload_size,
        malformed_rate,
        messages,
        duration_secs,
        retain,
        clean_session,
        keep_alive,
        pacer,
        username,
        password,
        client_transport,
        stats,
        pb_clone,
        sem_clone,
        mut worker_shutdown,
        mut loop_shutdown,
    } = ctx;

    let qos = match qos_num {
        0 => QoS5::AtMostOnce,
        1 => QoS5::AtLeastOnce,
        2 => QoS5::ExactlyOnce,
        _ => QoS5::AtMostOnce,
    };

    let _permit = match sem_clone {
        Some(ref sem) => sem.acquire().await.ok(),
        None => None,
    };

    let mut mqttoptions = MqttOptions5::new(client_id, broker, port);
    mqttoptions.set_keep_alive(Duration::from_secs(keep_alive.max(5)));
    mqttoptions.set_clean_start(clean_session);
    mqttoptions.set_transport(client_transport);

    if let Some(ref user) = username {
        mqttoptions.set_credentials(user, password.as_deref().unwrap_or(""));
    }

    let connect_start = Instant::now();
    let (client, mut eventloop) = AsyncClient5::new(mqttoptions, 50);
    let client_clone = client.clone();
    let (tx_time, mut rx_time) = mpsc::unbounded_channel::<Instant>();

    let stats_clone = stats.clone();
    let worker = tokio::spawn(async move {
        match mode {
            Mode::Pub => {
                if duration_secs > 0 {
                    let deadline = Instant::now() + Duration::from_secs(duration_secs);
                    let mut j = 0usize;
                    while Instant::now() < deadline && !*worker_shutdown.borrow() {
                        let (payload_bytes, is_malformed) = build_payload_with_chaos(
                            &payload_template,
                            device_id,
                            j,
                            payload_size,
                            malformed_rate,
                        );
                        if is_malformed {
                            stats_clone.record_malformed();
                        }
                        let _ = tx_time.send(Instant::now());
                        if client
                            .publish(topic.as_str(), qos, retain, payload_bytes)
                            .await
                            .is_err()
                        {
                            break;
                        }
                        j += 1;
                        if pacer
                            .pace_interval(Some(deadline), &mut worker_shutdown)
                            .await
                        {
                            break;
                        }
                    }
                    let _ = client.disconnect().await;
                } else {
                    for j in 0..messages {
                        if *worker_shutdown.borrow() {
                            break;
                        }
                        let (payload_bytes, is_malformed) = build_payload_with_chaos(
                            &payload_template,
                            device_id,
                            j,
                            payload_size,
                            malformed_rate,
                        );
                        if is_malformed {
                            stats_clone.record_malformed();
                        }
                        let _ = tx_time.send(Instant::now());
                        if client
                            .publish(topic.as_str(), qos, retain, payload_bytes)
                            .await
                            .is_err()
                        {
                            break;
                        }
                        if j + 1 < messages && pacer.pace_interval(None, &mut worker_shutdown).await
                        {
                            break;
                        }
                    }
                    if qos == QoS5::AtMostOnce || messages == 0 || *worker_shutdown.borrow() {
                        let _ = client.disconnect().await;
                    }
                }
            }
            Mode::Sub => {
                if duration_secs > 0 {
                    if client.subscribe(topic.as_str(), qos).await.is_ok() {
                        tokio::select! {
                            _ = tokio::time::sleep(Duration::from_secs(duration_secs)) => {}
                            _ = worker_shutdown.changed() => {}
                        }
                    }
                    let _ = client.disconnect().await;
                } else if messages == 0 {
                    let _ = client.disconnect().await;
                } else {
                    let _ = client.subscribe(topic.as_str(), qos).await;
                }
            }
            Mode::Conn => {
                if duration_secs > 0 {
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_secs(duration_secs)) => {}
                        _ = worker_shutdown.changed() => {}
                    }
                    let _ = client.disconnect().await;
                }
            }
        }
    });

    let mut completed_count = 0usize;
    let mut inflight: HashMap<u16, Instant> = HashMap::new();
    let mut last_recv = Instant::now();

    loop {
        tokio::select! {
            biased;
            _ = loop_shutdown.changed() => {
                let _ = client_clone.disconnect().await;
                break;
            }
            poll_res = eventloop.poll() => {
                match poll_res {
                    Ok(Event5::Incoming(Packet5::ConnAck(_))) if mode == Mode::Conn => {
                        let lat_us = (connect_start.elapsed().as_micros() as u64).clamp(1, 60_000_000);
                        stats.record_success(device_id, lat_us);
                        pb_clone.inc(1);
                        if duration_secs == 0 {
                            let _ = client_clone.disconnect().await;
                        }
                    }
                    Ok(Event5::Outgoing(Outgoing::Publish(pkid))) if mode == Mode::Pub => {
                        if qos == QoS5::AtMostOnce {
                            if let Ok(t0) = rx_time.try_recv() {
                                let lat_us = (t0.elapsed().as_micros() as u64).clamp(1, 60_000_000);
                                stats.record_success(device_id, lat_us);
                            }
                            pb_clone.inc(1);
                        } else if let Ok(t0) = rx_time.try_recv() {
                            inflight.insert(pkid, t0);
                        }
                    }
                    Ok(Event5::Incoming(Packet5::PubAck(ack))) if mode == Mode::Pub => {
                        if qos == QoS5::AtLeastOnce {
                            if let Some(t0) = inflight.remove(&ack.pkid) {
                                let lat_us = (t0.elapsed().as_micros() as u64).clamp(1, 60_000_000);
                                stats.record_success(device_id, lat_us);
                            }
                            pb_clone.inc(1);
                            completed_count += 1;
                            if duration_secs == 0 && completed_count == messages {
                                let _ = client_clone.disconnect().await;
                            }
                        }
                    }
                    Ok(Event5::Incoming(Packet5::PubComp(comp))) if mode == Mode::Pub => {
                        if qos == QoS5::ExactlyOnce {
                            if let Some(t0) = inflight.remove(&comp.pkid) {
                                let lat_us = (t0.elapsed().as_micros() as u64).clamp(1, 60_000_000);
                                stats.record_success(device_id, lat_us);
                            }
                            pb_clone.inc(1);
                            completed_count += 1;
                            if duration_secs == 0 && completed_count == messages {
                                let _ = client_clone.disconnect().await;
                            }
                        }
                    }
                    Ok(Event5::Incoming(Packet5::Publish(publish))) if mode == Mode::Sub => {
                        let lat_us = extract_payload_latency_us(&publish.payload, last_recv)
                            .clamp(1, 60_000_000);
                        last_recv = Instant::now();
                        stats.record_success(device_id, lat_us);
                        pb_clone.inc(1);
                        completed_count += 1;
                        if duration_secs == 0 && completed_count == messages {
                            let _ = client_clone.disconnect().await;
                        }
                    }
                    Ok(Event5::Outgoing(Outgoing::Disconnect)) => {
                        break;
                    }
                    Ok(_) => {}
                    Err(_) => {
                        stats.record_failure();
                        break;
                    }
                }
            }
        }
    }

    worker.abort();
    let _ = worker.await;
}
