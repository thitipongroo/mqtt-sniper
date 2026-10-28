#![forbid(unsafe_code)]

//! # mqtt-sniper
//!
//! A blazing-fast, memory-efficient MQTT load testing CLI written in Rust using Tokio.
//! Designed for high-concurrency stress testing, broker audits, and SLA verification.

pub mod cli;
pub mod error;
pub mod metrics;
pub mod payload;
pub mod report;
pub mod runner;

pub use error::{Result, SniperError};
