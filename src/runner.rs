use crate::cli::{Args, Mode, build_transport, parse_qos};
use crate::payload::build_payload;
use crate::report::BenchmarkReport;
use hdrhistogram::Histogram;
use indicatif::{ProgressBar, ProgressStyle};
use rumqttc::{AsyncClient, Event, MqttOptions, Outgoing, Packet, QoS};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{Semaphore, mpsc};
use tokio::task;

fn extract_payload_latency_us(payload: &[u8], fallback_start: Instant) -> u64 {
    if let Ok(val) = serde_json::from_slice::<serde_json::Value>(payload)
        && let Some(ts_ms) = val.get("ts").and_then(|v| v.as_u64())
    {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        if now_ms >= ts_ms {
            return (now_ms - ts_ms).saturating_mul(1000).max(1);
        }
    }
    fallback_start.elapsed().as_micros() as u64
}

pub async fn run_load_test(args: Args) -> BenchmarkReport {
    let qos = parse_qos(args.qos);
    let target_messages = args.clients * args.messages;
    let transport = build_transport(&args).unwrap_or_else(|err| {
        eprintln!("❌ TLS Configuration Error: {}", err);
        std::process::exit(1);
    });
    let scheme = if args.tls || args.ca.is_some() || args.cert.is_some() || args.key.is_some() {
        "mqtts"
    } else {
        "tcp"
    };

    println!("🚀 Starting MQTT Sniper...");
    println!("Broker: {}://{}:{}", scheme, args.broker, args.port);
    if args.duration_secs > 0 {
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

    let pb = if args.duration_secs > 0 {
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

    let start_time = Instant::now();
    let mut handles = Vec::with_capacity(args.clients);
    let published_count = Arc::new(AtomicUsize::new(0));
    let failed_clients = Arc::new(AtomicUsize::new(0));
    let semaphore = if args.max_connections > 0 {
        Some(Arc::new(Semaphore::new(args.max_connections)))
    } else {
        None
    };

    // Spawn concurrent client tasks
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
        let mode = args.mode;
        let messages = args.messages;
        let duration_secs = args.duration_secs;
        let retain = args.retain;
        let keep_alive = args.keep_alive;
        let interval_ms = args.interval_ms;
        let username = args.username.clone();
        let password = args.password.clone();
        let client_transport = transport.clone();
        let published_counter = Arc::clone(&published_count);
        let failed_counter = Arc::clone(&failed_clients);
        let pb_clone = pb.clone();
        let sem_clone = semaphore.clone();

        // Spawn independent task for each simulated client
        let handle = task::spawn(async move {
            let _permit = match sem_clone {
                Some(ref sem) => sem.acquire().await.ok(),
                None => None,
            };

            let client_id = format!("sniper-client-{}", i);
            let mut mqttoptions = MqttOptions::new(client_id, broker, port);
            mqttoptions.set_keep_alive(Duration::from_secs(keep_alive.max(1)));
            mqttoptions.set_transport(client_transport);

            if let Some(ref user) = username {
                mqttoptions.set_credentials(user, password.as_deref().unwrap_or(""));
            }

            let (client, mut eventloop) = AsyncClient::new(mqttoptions, 50);
            let client_clone = client.clone();
            let (tx_time, mut rx_time) = mpsc::unbounded_channel::<Instant>();

            // Worker task for Publish or Subscribe action
            let worker = tokio::spawn(async move {
                match mode {
                    Mode::Pub => {
                        if duration_secs > 0 {
                            let deadline = Instant::now() + Duration::from_secs(duration_secs);
                            let mut j = 0usize;
                            while Instant::now() < deadline {
                                let payload_bytes =
                                    build_payload(&payload_template, i, j, payload_size);
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
                                    let remaining =
                                        deadline.saturating_duration_since(Instant::now());
                                    if remaining.is_zero() {
                                        break;
                                    }
                                    tokio::time::sleep(
                                        Duration::from_millis(interval_ms).min(remaining),
                                    )
                                    .await;
                                } else {
                                    tokio::task::yield_now().await;
                                }
                            }
                            let _ = client.disconnect().await;
                        } else {
                            for j in 0..messages {
                                let payload_bytes =
                                    build_payload(&payload_template, i, j, payload_size);
                                let _ = tx_time.send(Instant::now());
                                if client
                                    .publish(topic.as_str(), qos, retain, payload_bytes)
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
                        }
                    }
                    Mode::Sub => {
                        if duration_secs > 0 {
                            let _ = client.subscribe(topic.as_str(), qos).await;
                            tokio::time::sleep(Duration::from_secs(duration_secs)).await;
                            let _ = client.disconnect().await;
                        } else if messages == 0 {
                            let _ = client.disconnect().await;
                        } else {
                            let _ = client.subscribe(topic.as_str(), qos).await;
                        }
                    }
                }
            });

            // Poll eventloop to drive network I/O
            let mut completed_count = 0usize;
            let mut inflight: HashMap<u16, Instant> = HashMap::new();
            let mut latencies_us: Vec<u64> = Vec::with_capacity(messages);
            let mut last_recv = Instant::now();

            loop {
                match eventloop.poll().await {
                    Ok(Event::Outgoing(Outgoing::Publish(pkid))) if mode == Mode::Pub => {
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
                    Ok(Event::Incoming(Packet::PubAck(ack))) if mode == Mode::Pub => {
                        if qos == QoS::AtLeastOnce {
                            if let Some(t0) = inflight.remove(&ack.pkid) {
                                latencies_us.push(t0.elapsed().as_micros() as u64);
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
                                latencies_us.push(t0.elapsed().as_micros() as u64);
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
                        let lat_us = extract_payload_latency_us(&publish.payload, last_recv);
                        last_recv = Instant::now();
                        latencies_us.push(lat_us);
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

            worker.abort();
            let _ = worker.await;
            latencies_us
        });

        handles.push(handle);
    }

    // Await completion of all client tasks and aggregate latency histogram
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

    // Calculate and print benchmark summary
    let elapsed = start_time.elapsed();
    let actual_sent = published_count.load(Ordering::Relaxed);
    let errors = failed_clients.load(Ordering::Relaxed);

    let report = BenchmarkReport::new(&args, elapsed, actual_sent, errors, &hist);
    report.print_summary(elapsed);

    if let Some(ref path) = args.output_json {
        report.save_json(path);
    }

    report
}