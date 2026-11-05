use crate::payload::next_random_u64;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::Instant;

/// Controls pacing, ramp-up delays, inter-message intervals, and chaos jitter for simulated clients.
#[derive(Debug, Clone, Copy)]
pub struct Pacer {
    /// Delay between consecutive messages in milliseconds
    pub interval_ms: u64,
    /// Delay before spawning consecutive clients in milliseconds
    pub ramp_up_ms: u64,
    /// Maximum random jitter delay in milliseconds (0 to N ms)
    pub jitter_ms: u64,
}

impl Pacer {
    /// Create a new `Pacer` instance.
    pub fn new(interval_ms: u64, ramp_up_ms: u64) -> Self {
        Self {
            interval_ms,
            ramp_up_ms,
            jitter_ms: 0,
        }
    }

    /// Create a new `Pacer` instance with random jitter configuration.
    pub fn with_jitter(interval_ms: u64, ramp_up_ms: u64, jitter_ms: u64) -> Self {
        Self {
            interval_ms,
            ramp_up_ms,
            jitter_ms,
        }
    }

    /// Calculate pseudo-random jitter delay in milliseconds in the range [0, jitter_ms].
    pub fn calculate_jitter(&self, device_id: usize, seq: usize) -> u64 {
        if self.jitter_ms == 0 {
            return 0;
        }
        let seed = ((device_id as u64) << 32) ^ (seq as u64) ^ 0x9E37_79B9_7F4A_7C15;
        next_random_u64(seed) % (self.jitter_ms + 1)
    }

    /// Wait for the calculated jitter delay.
    /// Returns `true` if shutdown was signaled or deadline expired, `false` if completed normally.
    pub async fn pace_jitter(
        &self,
        jitter_ms: u64,
        deadline: Option<Instant>,
        shutdown: &mut watch::Receiver<bool>,
    ) -> bool {
        if jitter_ms == 0 {
            return false;
        }

        let sleep_dur = if let Some(dl) = deadline {
            let remaining = dl.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return true;
            }
            Duration::from_millis(jitter_ms).min(remaining)
        } else {
            Duration::from_millis(jitter_ms)
        };

        tokio::select! {
            _ = tokio::time::sleep(sleep_dur) => false,
            _ = shutdown.changed() => true,
        }
    }

    /// Wait for the ramp-up delay before spawning consecutive clients.
    /// Returns `true` if shutdown was signaled, `false` if completed normally.
    pub async fn pace_ramp_up(
        &self,
        client_index: usize,
        shutdown: &mut watch::Receiver<bool>,
    ) -> bool {
        if self.ramp_up_ms > 0 && client_index > 0 {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(self.ramp_up_ms)) => false,
                _ = shutdown.changed() => true,
            }
        } else {
            false
        }
    }

    /// Wait for the inter-message interval.
    /// Returns `true` if shutdown was signaled or deadline expired, `false` if completed normally.
    pub async fn pace_interval(
        &self,
        deadline: Option<Instant>,
        shutdown: &mut watch::Receiver<bool>,
    ) -> bool {
        if self.interval_ms == 0 {
            tokio::task::yield_now().await;
            return false;
        }

        let sleep_dur = if let Some(dl) = deadline {
            let remaining = dl.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return true;
            }
            Duration::from_millis(self.interval_ms).min(remaining)
        } else {
            Duration::from_millis(self.interval_ms)
        };

        tokio::select! {
            _ = tokio::time::sleep(sleep_dur) => false,
            _ = shutdown.changed() => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_pacer_ramp_up_skip_for_first_client() {
        let pacer = Pacer::new(0, 100);
        let (_tx, mut rx) = watch::channel(false);
        let cancelled = pacer.pace_ramp_up(0, &mut rx).await;
        assert!(!cancelled);
    }

    #[tokio::test]
    async fn test_pacer_zero_interval_yields() {
        let pacer = Pacer::new(0, 0);
        let (_tx, mut rx) = watch::channel(false);
        let cancelled = pacer.pace_interval(None, &mut rx).await;
        assert!(!cancelled);
    }

    #[tokio::test]
    async fn test_pacer_interval_deadline_expired() {
        let pacer = Pacer::new(100, 0);
        let (_tx, mut rx) = watch::channel(false);
        let past = Instant::now() - Duration::from_millis(10);
        let expired = pacer.pace_interval(Some(past), &mut rx).await;
        assert!(expired);
    }

    #[tokio::test]
    async fn test_pacer_jitter_calculation_and_pacing() {
        let pacer = Pacer::with_jitter(10, 0, 50);
        assert_eq!(pacer.jitter_ms, 50);

        for seq in 0..100 {
            let j = pacer.calculate_jitter(1, seq);
            assert!(j <= 50, "Jitter {} must be <= 50", j);
        }

        let (_tx, mut rx) = watch::channel(false);
        let start = Instant::now();
        let cancelled = pacer.pace_jitter(20, None, &mut rx).await;
        assert!(!cancelled);
        assert!(start.elapsed() >= Duration::from_millis(15));
    }
}
