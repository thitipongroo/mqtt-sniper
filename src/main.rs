#![forbid(unsafe_code)]

use anyhow::Context;
use clap::Parser;
use mqtt_sniper::cli::Args;
use mqtt_sniper::runner::run_load_test;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let broker_target = format!("{}:{}", args.broker, args.port);

    let report = run_load_test(args).await.with_context(|| {
        format!("Failed to complete MQTT benchmark against broker '{broker_target}'")
    })?;

    if !report.slo_passed {
        std::process::exit(1);
    }
    Ok(())
}
