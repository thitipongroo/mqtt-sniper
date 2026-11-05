use super::pacer::Pacer;
use super::stats::{RunnerStats, extract_payload_latency_us};
use crate::cli::{Mode, PayloadFormat, parse_qos};
use crate::payload::{build_payload_formatted_with_chaos, should_drop_packet};
use indicatif::ProgressBar;
use rumqttc::{AsyncClient, Event, MqttOptions, Outgoing, Packet, QoS, Transport};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::{Semaphore, mpsc, watch};
use tokio::time::Instant;

pub use super::client_v5::run_client_v5;

/// Runtime parameters required to spawn and execute an individual simulated MQTT client worker.
pub struct ClientSpawnContext {
    /// Simulated device sequence index
    pub device_id: usize,
    /// Generated MQTT client ID
    pub client_id: String,
    /// Target broker hostname or IP
    pub broker: String,
    /// Target broker TCP port
    pub port: u16,
    /// Benchmark operating mode (pub, sub, conn)
    pub mode: Mode,
    /// Requested QoS level integer (0, 1, 2)
    pub qos_num: u8,
    /// Target MQTT topic
    pub topic: String,
    /// Resolved payload template string
    pub payload_template: String,
    /// Minimum padded payload size in bytes
    pub payload_size: usize,
    /// Serialization payload format (JSON, Protobuf, Avro, Raw)
    pub payload_format: PayloadFormat,
    /// Fraction of messages to corrupt for chaos/DLQ testing
    pub malformed_rate: f64,
    /// Fraction of messages to drop before socket transmission
    pub packet_drop: f64,
    /// Number of messages to publish/receive per client
    pub messages: usize,
    /// Soak test duration in seconds (0 = message-count mode)
    pub duration_secs: u64,
    /// MQTT retain message flag
    pub retain: bool,
    /// Clean session / clean start flag
    pub clean_session: bool,
    /// Keep-alive ping interval in seconds
    pub keep_alive: u64,
    /// Pacer controlling message intervals
    pub pacer: Pacer,
    /// Optional username for broker authentication
    pub username: Option<String>,
    /// Optional password for broker authentication
    pub password: Option<String>,
    /// Configured network transport (TCP or TLS)
    pub client_transport: Transport,
    /// Shared statistics tracker
    pub stats: RunnerStats,
    /// UI progress bar
    pub pb_clone: ProgressBar,
    /// Concurrency-bounding semaphore permit pool
    pub sem_clone: Option<Arc<Semaphore>>,
    /// Shutdown signal watcher for the worker task
    pub worker_shutdown: watch::Receiver<bool>,
    /// Shutdown signal watcher for the event loop
    pub loop_shutdown: watch::Receiver<bool>,
}

/// Execute an MQTT v3.1.1 client session to completion or until shutdown signal.
pub async fn run_client_v3(ctx: ClientSpawnContext) {
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
        payload_format,
        malformed_rate,
        packet_drop,
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

    if *loop_shutdown.borrow() {
        return;
    }

    let qos = parse_qos(qos_num);
    let _permit = match sem_clone {
        Some(ref sem) => {
            tokio::select! {
                permit = sem.acquire() => permit.ok(),
                _ = loop_shutdown.changed() => return,
            }
        }
        None => None,
    };

    let mut mqttoptions = MqttOptions::new(client_id, broker, port);
    mqttoptions.set_keep_alive(Duration::from_secs(keep_alive.max(1)));
    mqttoptions.set_clean_session(clean_session);
    mqttoptions.set_transport(client_transport);

    if let Some(ref user) = username {
        mqttoptions.set_credentials(user, password.as_deref().unwrap_or(""));
    }

    let connect_start = Instant::now();
    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 50);
    let client_clone = client.clone();
    let (tx_time, mut rx_time) = mpsc::unbounded_channel::<Instant>();

    let client_dropped = Arc::new(AtomicUsize::new(0));
    let client_dropped_clone = Arc::clone(&client_dropped);
    let worker_done = Arc::new(AtomicBool::new(false));
    let worker_done_clone = Arc::clone(&worker_done);

    let stats_clone = stats.clone();
    let worker = tokio::spawn(async move {
        match mode {
            Mode::Pub => {
                if duration_secs > 0 {
                    let deadline = Instant::now() + Duration::from_secs(duration_secs);
                    let mut j = 0usize;
                    while Instant::now() < deadline && !*worker_shutdown.borrow() {
                        if pacer.jitter_ms > 0 {
                            let jitter = pacer.calculate_jitter(device_id, j);
                            stats_clone.record_jitter(device_id, jitter);
                            if pacer
                                .pace_jitter(jitter, Some(deadline), &mut worker_shutdown)
                                .await
                            {
                                break;
                            }
                        }

                        if packet_drop > 0.0 && should_drop_packet(packet_drop, device_id, j) {
                            stats_clone.record_dropped();
                            client_dropped_clone.fetch_add(1, Ordering::Relaxed);
                            j += 1;
                            if pacer
                                .pace_interval(Some(deadline), &mut worker_shutdown)
                                .await
                            {
                                break;
                            }
                            continue;
                        }

                        let (payload_bytes, is_malformed) = build_payload_formatted_with_chaos(
                            payload_format,
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
                    worker_done_clone.store(true, Ordering::Relaxed);
                    let _ = client.disconnect().await;
                } else {
                    for j in 0..messages {
                        if *worker_shutdown.borrow() {
                            break;
                        }
                        if pacer.jitter_ms > 0 {
                            let jitter = pacer.calculate_jitter(device_id, j);
                            stats_clone.record_jitter(device_id, jitter);
                            if pacer.pace_jitter(jitter, None, &mut worker_shutdown).await {
                                break;
                            }
                        }

                        if packet_drop > 0.0 && should_drop_packet(packet_drop, device_id, j) {
                            stats_clone.record_dropped();
                            client_dropped_clone.fetch_add(1, Ordering::Relaxed);
                            if j + 1 < messages
                                && pacer.pace_interval(None, &mut worker_shutdown).await
                            {
                                break;
                            }
                            continue;
                        }

                        let (payload_bytes, is_malformed) = build_payload_formatted_with_chaos(
                            payload_format,
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
                    worker_done_clone.store(true, Ordering::Relaxed);
                    let dropped = client_dropped_clone.load(Ordering::Relaxed);
                    if qos == QoS::AtMostOnce
                        || messages == 0
                        || *worker_shutdown.borrow()
                        || dropped >= messages
                    {
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
                    worker_done_clone.store(true, Ordering::Relaxed);
                    let _ = client.disconnect().await;
                } else if messages == 0 {
                    worker_done_clone.store(true, Ordering::Relaxed);
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
                    worker_done_clone.store(true, Ordering::Relaxed);
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
                    Ok(Event::Incoming(Packet::ConnAck(_))) if mode == Mode::Conn => {
                        let lat_us = (connect_start.elapsed().as_micros() as u64).clamp(1, 60_000_000);
                        stats.record_success(device_id, lat_us);
                        pb_clone.inc(1);
                        if duration_secs == 0 {
                            let _ = client_clone.disconnect().await;
                        }
                    }
                    Ok(Event::Outgoing(Outgoing::Publish(pkid))) if mode == Mode::Pub => {
                        if qos == QoS::AtMostOnce {
                            if let Ok(t0) = rx_time.try_recv() {
                                let lat_us = (t0.elapsed().as_micros() as u64).clamp(1, 60_000_000);
                                stats.record_success(device_id, lat_us);
                            }
                            pb_clone.inc(1);
                        } else if let Ok(t0) = rx_time.try_recv() {
                            inflight.insert(pkid, t0);
                        }
                    }
                    Ok(Event::Incoming(Packet::PubAck(ack))) if mode == Mode::Pub => {
                        if qos == QoS::AtLeastOnce {
                            if let Some(t0) = inflight.remove(&ack.pkid) {
                                let lat_us = (t0.elapsed().as_micros() as u64).clamp(1, 60_000_000);
                                stats.record_success(device_id, lat_us);
                            }
                            pb_clone.inc(1);
                            completed_count += 1;
                            let dropped = client_dropped.load(Ordering::Relaxed);
                            if duration_secs == 0 && (completed_count + dropped >= messages) {
                                let _ = client_clone.disconnect().await;
                            }
                        }
                    }
                    Ok(Event::Incoming(Packet::PubComp(comp))) if mode == Mode::Pub => {
                        if qos == QoS::ExactlyOnce {
                            if let Some(t0) = inflight.remove(&comp.pkid) {
                                let lat_us = (t0.elapsed().as_micros() as u64).clamp(1, 60_000_000);
                                stats.record_success(device_id, lat_us);
                            }
                            pb_clone.inc(1);
                            completed_count += 1;
                            let dropped = client_dropped.load(Ordering::Relaxed);
                            if duration_secs == 0 && (completed_count + dropped >= messages) {
                                let _ = client_clone.disconnect().await;
                            }
                        }
                    }
                    Ok(Event::Incoming(Packet::Publish(publish))) if mode == Mode::Sub => {
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
                    Ok(Event::Outgoing(Outgoing::Disconnect)) => {
                        break;
                    }
                    Ok(_) => {}
                    Err(e) => {
                        eprintln!("⚠️  device {device_id}: MQTT connection error: {e}");
                        stats.record_failure();
                        break;
                    }
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(100)), if worker_done.load(Ordering::Relaxed) && inflight.is_empty() => {
                let _ = client_clone.disconnect().await;
                break;
            }
            _ = tokio::time::sleep(Duration::from_secs(10)), if worker_done.load(Ordering::Relaxed) => {
                let _ = client_clone.disconnect().await;
                break;
            }
        }
    }

    worker.abort();
    let _ = worker.await;
}
