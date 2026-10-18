use clap::Parser;
use mqtt_sniper::cli::Args;
use mqtt_sniper::runner::run_load_test;

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let _ = run_load_test(args).await;
}
