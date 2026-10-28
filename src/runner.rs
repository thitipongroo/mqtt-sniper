use crate::cli::{
    Args, Mode, OutputFormat, build_transport, check_file_descriptor_limits,
    normalize_broker_target, parse_qos, resolve_payload_template,
};
use crate::metrics::ShardedHistogramPool;
use crate::payload::build_payload_with_chaos;
use crate::report::BenchmarkReport;
use indicatif::{ProgressBar, ProgressStyle};
use rumqttc::{AsyncClient, Event, MqttOptions, Outgoing, Packet, QoS, Transport};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{Semaphore, mpsc, watch};
use tokio::task;
use tokio::time::Instant;

struct ClientSpawnContext {
    device_id: usize,
    client_id: String,
    broker: String,
    port: u16,
    mode: Mode,
    qos_num: u8,
    topic: String,
    payload_template: String,
    payload_size: usize,
    malformed_rate: f64,
    messages: usize,
    duration_secs: u64,
    retain: bool,
    clean_session: bool,
    keep_alive: u64,
    interval_ms: u64,
    username: Option<String>,
    password: Option<String>,
    client_transport: Transport,
    published_counter: Arc<AtomicUsize>,
    malformed_counter: Arc<AtomicUsize>,
    failed_counter: Arc<AtomicUsize>,
    hist_pool: Arc<ShardedHistogramPool>,
    pb_clone: ProgressBar,
    sem_clone: Option<Arc<Semaphore>>,
    worker_shutdown: watch::Receiver<bool>,
    loop_shutdown: watch::Receiver<bool>,
}

fn extract_payload_latency_us(payload: &[u8], fallback_start: Instant) -> u64 {
    if let Ok(val) = serde_json::from_slice::<serde_json::Value>(payload)
        && let Some(raw_ts) = val
            .get("ts")
            .or_else(|| val.get("timestamp"))
            .or_else(|| val.get("time"))
            .and_then(|v| v.as_u64())
    {
        // Auto-detect Unix timestamp precision: seconds (~10^9), milliseconds (~10^12), or microseconds (~10^15)
        let ts_us = if raw_ts < 10_000_000_000 {
            raw_ts.saturating_mul(1_000_000)
        } else if raw_ts < 10_000_000_000_000 {
            raw_ts.saturating_mul(1_000)
        } else {
            raw_ts
        };

        let now_us = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;
        if now_us >= ts_us {
            return (now_us - ts_us).max(1);
        }
    }
    fallback_start.elapsed().as_micros().max(1) as u64
}

async fn run_client_v3(ctx: ClientSpawnContext) {
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
        interval_ms,
        username,
        password,
        client_transport,
        published_counter,
        malformed_counter,
        failed_counter,
        hist_pool,
        pb_clone,
        sem_clone,
        mut worker_shutdown,
        mut loop_shutdown,
    } = ctx;

    let qos = parse_qos(qos_num);
    let _permit = match sem_clone {
        Some(ref sem) => sem.acquire().await.ok(),
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
                            malformed_counter.fetch_add(1, Ordering::Relaxed);
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
                        if interval_ms > 0 {
                            let remaining = deadline.saturating_duration_since(Instant::now());
                            if remaining.is_zero() {
                                break;
                            }
                            tokio::select! {
                                _ = tokio::time::sleep(Duration::from_millis(interval_ms).min(remaining)) => {}
                                _ = worker_shutdown.changed() => break,
                            }
                        } else {
                            tokio::task::yield_now().await;
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
                            malformed_counter.fetch_add(1, Ordering::Relaxed);
                        }
                        let _ = tx_time.send(Instant::now());
                        if client
                            .publish(topic.as_str(), qos, retain, payload_bytes)
                            .await
                            .is_err()
                        {
                            break;
                        }
                        if interval_ms > 0 && j + 1 < messages {
                            tokio::select! {
                                _ = tokio::time::sleep(Duration::from_millis(interval_ms)) => {}
                                _ = worker_shutdown.changed() => break,
                            }
                        }
                    }
                    if qos == QoS::AtMostOnce || messages == 0 || *worker_shutdown.borrow() {
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
                    Ok(Event::Incoming(Packet::ConnAck(_))) if mode == Mode::Conn => {
                        let lat_us = (connect_start.elapsed().as_micros() as u64).clamp(1, 60_000_000);
                        hist_pool.record(device_id, lat_us);
                        published_counter.fetch_add(1, Ordering::Relaxed);
                        pb_clone.inc(1);
                        if duration_secs == 0 {
                            let _ = client_clone.disconnect().await;
                        }
                    }
                    Ok(Event::Outgoing(Outgoing::Publish(pkid))) if mode == Mode::Pub => {
                        if qos == QoS::AtMostOnce {
                            if let Ok(t0) = rx_time.try_recv() {
                                let lat_us = (t0.elapsed().as_micros() as u64).clamp(1, 60_000_000);
                                hist_pool.record(device_id, lat_us);
                            }
                            published_counter.fetch_add(1, Ordering::Relaxed);
                            pb_clone.inc(1);
                        } else if let Ok(t0) = rx_time.try_recv() {
                            inflight.insert(pkid, t0);
                        }
                    }
                    Ok(Event::Incoming(Packet::PubAck(ack))) if mode == Mode::Pub => {
                        if qos == QoS::AtLeastOnce {
                            if let Some(t0) = inflight.remove(&ack.pkid) {
                                let lat_us = (t0.elapsed().as_micros() as u64).clamp(1, 60_000_000);
                                hist_pool.record(device_id, lat_us);
                            }
                            published_counter.fetch_add(1, Ordering::Relaxed);
                            pb_clone.inc(1);
                            completed_count += 1;
                            if duration_secs == 0 && completed_count == messages {
                                let _ = client_clone.disconnect().await;
                            }
                        }
                    }
                    Ok(Event::Incoming(Packet::PubComp(comp))) if mode == Mode::Pub => {
                        if qos == QoS::ExactlyOnce {
                            if let Some(t0) = inflight.remove(&comp.pkid) {
                                let lat_us = (t0.elapsed().as_micros() as u64).clamp(1, 60_000_000);
                                hist_pool.record(device_id, lat_us);
                            }
                            published_counter.fetch_add(1, Ordering::Relaxed);
                            pb_clone.inc(1);
                            completed_count += 1;
                            if duration_secs == 0 && completed_count == messages {
                                let _ = client_clone.disconnect().await;
                            }
                        }
                    }
                    Ok(Event::Incoming(Packet::Publish(publish))) if mode == Mode::Sub => {
                        let lat_us = extract_payload_latency_us(&publish.payload, last_recv)
                            .clamp(1, 60_000_000);
                        last_recv = Instant::now();
                        hist_pool.record(device_id, lat_us);
                        published_counter.fetch_add(1, Ordering::Relaxed);
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
                    Err(_) => {
                        failed_counter.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                }
            }
        }
    }

    worker.abort();
    let _ = worker.await;
}

async fn run_client_v5(ctx: ClientSpawnContext) {
    use rumqttc::v5::mqttbytes::QoS as QoS5;
    use rumqttc::v5::mqttbytes::v5::Packet as Packet5;
    use rumqttc::v5::{AsyncClient as AsyncClient5, Event as Event5, MqttOptions as MqttOptions5};

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
        interval_ms,
        username,
        password,
        client_transport,
        published_counter,
        malformed_counter,
        failed_counter,
        hist_pool,
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
                            malformed_counter.fetch_add(1, Ordering::Relaxed);
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
                        if interval_ms > 0 {
                            let remaining = deadline.saturating_duration_since(Instant::now());
                            if remaining.is_zero() {
                                break;
                            }
                            tokio::select! {
                                _ = tokio::time::sleep(Duration::from_millis(interval_ms).min(remaining)) => {}
                                _ = worker_shutdown.changed() => break,
                            }
                        } else {
                            tokio::task::yield_now().await;
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
                            malformed_counter.fetch_add(1, Ordering::Relaxed);
                        }
                        let _ = tx_time.send(Instant::now());
                        if client
                            .publish(topic.as_str(), qos, retain, payload_bytes)
                            .await
                            .is_err()
                        {
                            break;
                        }
                        if interval_ms > 0 && j + 1 < messages {
                            tokio::select! {
                                _ = tokio::time::sleep(Duration::from_millis(interval_ms)) => {}
                                _ = worker_shutdown.changed() => break,
                            }
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
                        hist_pool.record(device_id, lat_us);
                        published_counter.fetch_add(1, Ordering::Relaxed);
                        pb_clone.inc(1);
                        if duration_secs == 0 {
                            let _ = client_clone.disconnect().await;
                        }
                    }
                    Ok(Event5::Outgoing(Outgoing::Publish(pkid))) if mode == Mode::Pub => {
                        if qos == QoS5::AtMostOnce {
                            if let Ok(t0) = rx_time.try_recv() {
                                let lat_us = (t0.elapsed().as_micros() as u64).clamp(1, 60_000_000);
                                hist_pool.record(device_id, lat_us);
                            }
                            published_counter.fetch_add(1, Ordering::Relaxed);
                            pb_clone.inc(1);
                        } else if let Ok(t0) = rx_time.try_recv() {
                            inflight.insert(pkid, t0);
                        }
                    }
                    Ok(Event5::Incoming(Packet5::PubAck(ack))) if mode == Mode::Pub => {
                        if qos == QoS5::AtLeastOnce {
                            if let Some(t0) = inflight.remove(&ack.pkid) {
                                let lat_us = (t0.elapsed().as_micros() as u64).clamp(1, 60_000_000);
                                hist_pool.record(device_id, lat_us);
                            }
                            published_counter.fetch_add(1, Ordering::Relaxed);
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
                                hist_pool.record(device_id, lat_us);
                            }
                            published_counter.fetch_add(1, Ordering::Relaxed);
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
                        hist_pool.record(device_id, lat_us);
                        published_counter.fetch_add(1, Ordering::Relaxed);
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
                        failed_counter.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                }
            }
        }
    }

    worker.abort();
    let _ = worker.await;
}

pub async fn run_load_test(mut args: Args) -> BenchmarkReport {
    normalize_broker_target(&mut args);
    let target_messages = if args.mode == Mode::Conn {
        args.clients
    } else {
        args.clients * args.messages
    };
    let resolved_payload = resolve_payload_template(&args).unwrap_or_else(|err| {
        eprintln!("❌ Payload Configuration Error: {}", err);
        std::process::exit(1);
    });
    let transport = build_transport(&args).unwrap_or_else(|err| {
        eprintln!("❌ TLS Configuration Error: {}", err);
        std::process::exit(1);
    });
    let scheme = if args.tls || args.ca.is_some() || args.cert.is_some() || args.key.is_some() {
        "mqtts"
    } else {
        "tcp"
    };
    let proto_label = if args.mqtt_version == 5 {
        "v5.0"
    } else {
        "v3.1.1"
    };

    // Pre-flight check for OS file descriptor / socket limits against requested concurrency
    let fd_check = check_file_descriptor_limits(args.clients, args.max_connections);
    if !args.quiet
        && fd_check.is_exceeded
        && let Some(ref warn) = fd_check.warning
    {
        eprintln!("{}\n", warn);
    }

    let is_quiet = args.quiet || args.output != OutputFormat::Text;

    if !is_quiet {
        println!("🚀 Starting MQTT Sniper (MQTT {})...", proto_label);
        println!("Broker: {}://{}:{}", scheme, args.broker, args.port);
        if args.mode == Mode::Conn {
            println!(
                "Spawning {} concurrent Conn clients (hold {}s, clean_session={})...",
                args.clients, args.duration_secs, args.clean_session
            );
        } else if args.duration_secs > 0 {
            println!(
                "Spawning {} concurrent {:?} clients (duration {}s, QoS {})...",
                args.clients, args.mode, args.duration_secs, args.qos
            );
        } else {
            println!(
                "Spawning {} concurrent {:?} clients ({} msgs/client, QoS {})...",
                args.clients, args.mode, args.messages, args.qos
            );
        }
    }

    let pb = if is_quiet {
        ProgressBar::hidden()
    } else if args.duration_secs > 0 && args.mode != Mode::Conn {
        let spinner = ProgressBar::new_spinner();
        if let Ok(style) = ProgressStyle::with_template(
            "{spinner:.green} [{elapsed_precise}] {pos} msgs | {per_sec}",
        ) {
            spinner.set_style(style);
        }
        spinner
    } else {
        let bar = ProgressBar::new(target_messages as u64);
        if let Ok(style) = ProgressStyle::with_template(
            "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({percent}%) | {per_sec} | ETA: {eta}",
        ) {
            bar.set_style(style.progress_chars("█▓░"));
        }
        bar
    };

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let signal_tx = shutdown_tx.clone();
    let signal_handle = tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            eprintln!(
                "\n🛑 Received Ctrl+C — initiating graceful shutdown and generating report..."
            );
            let _ = signal_tx.send(true);
        }
    });

    let start_time = Instant::now();
    let mut handles = Vec::with_capacity(args.clients);
    let published_count = Arc::new(AtomicUsize::new(0));
    let malformed_count = Arc::new(AtomicUsize::new(0));
    let failed_clients = Arc::new(AtomicUsize::new(0));

    // Core-partitioned sharded histogram pool: eliminates per-client ~40 KB memory allocations
    let hist_pool = Arc::new(ShardedHistogramPool::default());

    // Concurrency bounded semaphore: bounds active simultaneous sockets to safe limit
    let semaphore = if args.max_connections > 0 {
        let capped = args.max_connections.min(fd_check.safe_limit);
        Some(Arc::new(Semaphore::new(capped)))
    } else if fd_check.is_exceeded {
        Some(Arc::new(Semaphore::new(fd_check.safe_limit)))
    } else {
        None
    };

    for i in 0..args.clients {
        if *shutdown_tx.borrow() {
            break;
        }

        if args.ramp_up_ms > 0 && i > 0 {
            let mut ramp_rx = shutdown_rx.clone();
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(args.ramp_up_ms)) => {}
                _ = ramp_rx.changed() => break,
            }
        }

        let device_id = args.start_number + i;
        let device_id_str = device_id.to_string();
        let mqtt_version = args.mqtt_version;
        let ctx = ClientSpawnContext {
            device_id,
            client_id: format!("{}{}", args.client_prefix, device_id),
            broker: args.broker.clone(),
            port: args.port,
            mode: args.mode,
            qos_num: args.qos,
            topic: args.topic.replace("{device_id}", &device_id_str),
            payload_template: resolved_payload.clone(),
            payload_size: args.payload_size,
            malformed_rate: args.malformed_rate,
            messages: args.messages,
            duration_secs: args.duration_secs,
            retain: args.retain,
            clean_session: args.clean_session,
            keep_alive: args.keep_alive,
            interval_ms: args.interval_ms,
            username: args
                .username
                .as_ref()
                .map(|u| u.replace("{device_id}", &device_id_str)),
            password: args
                .password
                .as_ref()
                .map(|p| p.replace("{device_id}", &device_id_str)),
            client_transport: transport.clone(),
            published_counter: Arc::clone(&published_count),
            malformed_counter: Arc::clone(&malformed_count),
            failed_counter: Arc::clone(&failed_clients),
            hist_pool: Arc::clone(&hist_pool),
            pb_clone: pb.clone(),
            sem_clone: semaphore.clone(),
            worker_shutdown: shutdown_rx.clone(),
            loop_shutdown: shutdown_rx.clone(),
        };

        let handle = task::spawn(async move {
            if mqtt_version == 5 {
                run_client_v5(ctx).await;
            } else {
                run_client_v3(ctx).await;
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        let _ = handle.await;
    }

    // Merge only the small, fixed number of core-partitioned shards (< 1 ms vs ~3.2s for 100k merges)
    let hist = hist_pool.merge_all();

    signal_handle.abort();
    pb.finish_and_clear();

    let elapsed = start_time.elapsed();
    let actual_sent = published_count.load(Ordering::Relaxed);
    let malformed_sent = malformed_count.load(Ordering::Relaxed);
    let errors = failed_clients.load(Ordering::Relaxed);

    let report = BenchmarkReport::new(&args, elapsed, actual_sent, malformed_sent, errors, &hist);

    match args.output {
        OutputFormat::Text => {
            report.print_summary(elapsed);
            if let Some(ref path) = args.output_file {
                if path.ends_with(".json") {
                    report.save_json(path);
                } else {
                    let action_label = match report.mode {
                        Mode::Pub => "messages sent",
                        Mode::Sub => "messages received",
                        Mode::Conn => "clients connected",
                    };
                    let summary_str = format!(
                        "Load test completed in {:.2?}\nTotal {}: {} / {}\nThroughput: {:.2} msgs/sec\nLatency (ms): min={:.2} | p50={:.2} | p90={:.2} | p95={:.2} | p99={:.2} | max={:.2}\nSLO passed: {}\n",
                        elapsed,
                        action_label,
                        report.actual_sent,
                        report.target_messages,
                        report.throughput_msgs_per_sec,
                        report.latency_min_ms,
                        report.latency_p50_ms,
                        report.latency_p90_ms,
                        report.latency_p95_ms,
                        report.latency_p99_ms,
                        report.latency_max_ms,
                        report.slo_passed
                    );
                    report.save_file(path, &summary_str);
                }
            }
        }
        OutputFormat::Json => {
            let json_str = report.to_json_string();
            println!("{}", json_str);
            if let Some(ref path) = args.output_file {
                report.save_file(path, &json_str);
            }
        }
        OutputFormat::Prometheus => {
            let prom_str = report.to_prometheus();
            print!("{}", prom_str);
            if let Some(ref path) = args.output_file {
                report.save_file(path, &prom_str);
            }
        }
    }

    if let Some(ref path) = args.output_json {
        report.save_json(path);
    }

    report
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn test_extract_payload_latency_supports_ts_timestamp_and_units() {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
        let now_ms = now.as_millis() as u64;
        let now_secs = now.as_secs();
        let fallback = Instant::now();

        let payload_ts = format!(r#"{{"ts": {}}}"#, now_ms.saturating_sub(15));
        let lat_ts = extract_payload_latency_us(payload_ts.as_bytes(), fallback);
        assert!(lat_ts >= 10_000);

        let payload_timestamp = format!(r#"{{"timestamp": {}}}"#, now_ms.saturating_sub(25));
        let lat_timestamp = extract_payload_latency_us(payload_timestamp.as_bytes(), fallback);
        assert!(lat_timestamp >= 20_000);

        let payload_secs = format!(r#"{{"timestamp": {}}}"#, now_secs.saturating_sub(1));
        let lat_secs = extract_payload_latency_us(payload_secs.as_bytes(), fallback);
        assert!(lat_secs >= 500_000);
    }

    #[test]
    fn test_extract_payload_latency_ntp_backward_step_resilience() {
        let fallback = Instant::now();
        // Payload timestamp far in the future simulating a backwards step of the local system clock
        let future_payload = r#"{"ts": 9999999999999999}"#;
        let lat = extract_payload_latency_us(future_payload.as_bytes(), fallback);
        assert!(lat >= 1);
    }

    #[test]
    fn test_file_descriptor_safeguard_bounds_semaphore() {
        let args = Args::parse_from(["mqtt-sniper", "-c", "100000"]);
        let fd_check = check_file_descriptor_limits(args.clients, args.max_connections);
        assert!(fd_check.safe_limit > 0);
    }

    #[test]
    fn test_sharded_histogram_pool_integration() {
        let pool = ShardedHistogramPool::default();
        for i in 0..10_000 {
            pool.record(i, ((i % 1000) + 1) as u64);
        }
        let merged = pool.merge_all();
        assert_eq!(merged.len(), 10_000);
        assert_eq!(merged.min(), 1);
        assert_eq!(merged.max(), 1000);
    }
}
