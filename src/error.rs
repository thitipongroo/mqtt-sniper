use thiserror::Error;

/// Core error types for the `mqtt-sniper` load testing engine.
#[derive(Error, Debug)]
pub enum SniperError {
    /// Failure during payload template resolution, validation, or file reading.
    #[error("Payload error: {0}")]
    Payload(String),

    /// Error encountered during SSL/TLS configuration or certificate parsing.
    #[error("TLS configuration error: {0}")]
    Tls(String),

    /// File I/O error occurred during benchmark execution or report generation.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Error during JSON serialization or report formatting.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// Error encountered during histogram allocation, recording, or merging.
    #[error("Histogram error: {0}")]
    Histogram(String),

    /// Error during MQTT communication, connection handshake, or eventloop polling.
    #[error("MQTT client error: {0}")]
    Mqtt(String),

    /// Benchmark configuration validation error.
    #[error("Configuration error: {0}")]
    Config(String),
}

/// Specialized Result type for `mqtt-sniper` operations.
pub type Result<T> = std::result::Result<T, SniperError>;
