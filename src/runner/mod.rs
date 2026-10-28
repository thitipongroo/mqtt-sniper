//! Benchmark execution engine and client orchestration module for MQTT Sniper.

pub mod client;
pub mod client_v5;
pub mod pacer;
pub mod stats;

use crate::cli::{
    Args, Mode, OutputFormat, build_transport, check_file_descriptor_limits,
    normalize_broker_target, resolve_payload_template,
};
use crate::error::Result;
use crate::metrics::ShardedHistogramPool;
use crate::report::BenchmarkReport;
use client::{ClientSpawnContext, run_client_v3, run_client_v5};
use indicatif::{ProgressBar, ProgressStyle};
use pacer::Pacer;
use stats::RunnerStats;
use std::sync::Arc;
use tokio::sync::{Semaphore, watch};
use tokio::task;
use tokio::time::Instant;

/// Run a complete MQTT load test execution according to the supplied CLI options.
///
/// # Errors
/// Returns [`crate::error::SniperError`] if payload configuration, TLS transport,
/// or histogram allocation/merging fails.
pub async fn run_load_test(mut args: Args) -> Result<BenchmarkReport> {
    normalize_broker_target(&mut args);
    let target_messages = if args.mode == Mode::Conn {
        args.clients
    } else {
        args.clients * args.messages
    };
    let resolved_payload = resolve_payload_template(&args)?;
    let transport = build_transport(&args)?;
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
    let hist_pool = Arc::new(ShardedHistogramPool::default());
    let stats = RunnerStats::new(Arc::clone(&hist_pool));

    // Concurrency bounded semaphore: bounds active simultaneous sockets to safe limit
    let semaphore = if args.max_connections > 0 {
        let capped = args.max_connections.min(fd_check.safe_limit);
        Some(Arc::new(Semaphore::new(capped)))
    } else if fd_check.is_exceeded {
        Some(Arc::new(Semaphore::new(fd_check.safe_limit)))
    } else {
        None
    };

    let pacer = Pacer::new(args.interval_ms, args.ramp_up_ms);

    for i in 0..args.clients {
        if *shutdown_tx.borrow() {
            break;
        }

        let mut ramp_rx = shutdown_rx.clone();
        if pacer.pace_ramp_up(i, &mut ramp_rx).await {
            break;
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
            pacer,
            username: args
                .username
                .as_ref()
                .map(|u| u.replace("{device_id}", &device_id_str)),
            password: args
                .password
                .as_ref()
                .map(|p| p.replace("{device_id}", &device_id_str)),
            client_transport: transport.clone(),
            stats: stats.clone(),
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
    let hist = hist_pool.try_merge_all()?;

    signal_handle.abort();
    pb.finish_and_clear();

    let elapsed = start_time.elapsed();
    let (actual_sent, malformed_sent, errors) = stats.snapshot();

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

    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn test_file_descriptor_safeguard_bounds_semaphore() {
        let args = Args::parse_from(["mqtt-sniper", "-c", "100000"]);
        let fd_check = check_file_descriptor_limits(args.clients, args.max_connections);
        assert!(fd_check.safe_limit > 0);
    }
}
