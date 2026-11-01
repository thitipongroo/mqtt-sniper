#![forbid(unsafe_code)]

//! # mqtt-sniper
//!
//! A blazing-fast, memory-efficient MQTT load testing CLI written in Rust using Tokio.
//! Designed for high-concurrency stress testing, broker audits, and SLA verification.

/// Command-line argument parsing and configuration options.
pub mod cli;
/// Error definitions and specialized result types.
pub mod error;
/// Core-partitioned sharded histogram metrics engine.
pub mod metrics;
/// Dynamic parametric payload rendering and chaos injection.
pub mod payload;
/// Benchmark report generation, formatting, and file export.
pub mod report;
/// Client worker orchestration and load test runner.
pub mod runner;

pub use error::{Result, SniperError};
