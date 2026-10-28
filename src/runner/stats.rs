use crate::metrics::ShardedHistogramPool;
use hdrhistogram::Histogram;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::Instant;

/// Extract latency in microseconds from a received JSON payload containing a timestamp.
/// If payload parsing fails or clock stepped backward, falls back to monotonic elapsed time.
pub fn extract_payload_latency_us(payload: &[u8], fallback_start: Instant) -> u64 {
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

/// Shared statistics tracker collecting metrics and sharded latency histograms across all workers.
#[derive(Clone)]
pub struct RunnerStats {
    /// Count of successfully delivered messages or established connections
    pub published_counter: Arc<AtomicUsize>,
    /// Count of intentionally injected malformed chaos messages
    pub malformed_counter: Arc<AtomicUsize>,
    /// Count of failed clients or connection drops
    pub failed_counter: Arc<AtomicUsize>,
    /// Core-partitioned sharded histogram pool for low-memory latency aggregation
    pub hist_pool: Arc<ShardedHistogramPool>,
}

impl RunnerStats {
    /// Create a new `RunnerStats` instance backed by the provided `ShardedHistogramPool`.
    pub fn new(hist_pool: Arc<ShardedHistogramPool>) -> Self {
        Self {
            published_counter: Arc::new(AtomicUsize::new(0)),
            malformed_counter: Arc::new(AtomicUsize::new(0)),
            failed_counter: Arc::new(AtomicUsize::new(0)),
            hist_pool,
        }
    }

    /// Record a successful message transmission or connection with its observed latency.
    #[inline]
    pub fn record_success(&self, device_id: usize, lat_us: u64) {
        self.published_counter.fetch_add(1, Ordering::Relaxed);
        self.hist_pool.record(device_id, lat_us);
    }

    /// Increment the malformed message counter.
    #[inline]
    pub fn record_malformed(&self) {
        self.malformed_counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment the failed client / error counter.
    #[inline]
    pub fn record_failure(&self) {
        self.failed_counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Return a snapshot of `(published_count, malformed_count, failed_clients)`.
    pub fn snapshot(&self) -> (usize, usize, usize) {
        (
            self.published_counter.load(Ordering::Relaxed),
            self.malformed_counter.load(Ordering::Relaxed),
            self.failed_counter.load(Ordering::Relaxed),
        )
    }

    /// Merge all latency samples across shards into a single consolidated `Histogram<u64>`.
    pub fn merge_histogram(&self) -> Histogram<u64> {
        self.hist_pool.merge_all()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_payload_latency_supports_ts_timestamp_and_units() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
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
        let future_payload = r#"{"ts": 9999999999999999}"#;
        let lat = extract_payload_latency_us(future_payload.as_bytes(), fallback);
        assert!(lat >= 1);
    }

    #[test]
    fn test_runner_stats_recording() {
        let pool = Arc::new(ShardedHistogramPool::default());
        let stats = RunnerStats::new(pool);
        stats.record_success(1, 2500);
        stats.record_malformed();
        stats.record_failure();

        let (pub_cnt, mal_cnt, fail_cnt) = stats.snapshot();
        assert_eq!(pub_cnt, 1);
        assert_eq!(mal_cnt, 1);
        assert_eq!(fail_cnt, 1);

        let hist = stats.merge_histogram();
        assert_eq!(hist.len(), 1);
        assert_eq!(hist.min(), 2500);
    }
}
