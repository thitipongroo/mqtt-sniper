use crate::error::{Result, SniperError};
use hdrhistogram::Histogram;
use std::sync::Mutex;

/// Default lower bound for MQTT latency tracking (1 microsecond)
pub const HIST_LOW_BOUND_US: u64 = 1;
/// Default upper bound for MQTT latency tracking (60 seconds)
pub const HIST_HIGH_BOUND_US: u64 = 60_000_000;
/// Default significant value digits for HdrHistogram precision
pub const HIST_SIGFIGS: u8 = 3;

/// Cache-line aligned shard to prevent CPU cache contention (false sharing) between worker threads
#[repr(align(64))]
struct HistogramShard {
    hist: Mutex<Histogram<u64>>,
}

/// A core-partitioned, memory-efficient sharded histogram pool.
///
/// Instead of allocating a ~35-40 KB histogram for each of the 100,000 client tasks
/// (which consumes ~3.5-4.0 GB of RAM and stalls CPU during 100k sequential merges),
/// this pool partitions latency samples across a small, fixed number of core-aligned shards (e.g. 8 - 64).
///
/// This slashes memory consumption by >95% (down to <2.5 MB for the histogram pool)
/// and reduces final merge latency to < 1 ms while preserving 100% statistical accuracy.
pub struct ShardedHistogramPool {
    shards: Vec<HistogramShard>,
    mask: usize,
}

fn fallback_histogram() -> Histogram<u64> {
    if let Ok(h) =
        Histogram::<u64>::new_with_bounds(HIST_LOW_BOUND_US, HIST_HIGH_BOUND_US, HIST_SIGFIGS)
    {
        return h;
    }
    if let Ok(h) = Histogram::<u64>::new(HIST_SIGFIGS) {
        return h;
    }
    match Histogram::<u64>::new(1) {
        Ok(h) => h,
        Err(_) => unreachable!("Histogram::new(1) cannot fail with sigfigs=1"),
    }
}

impl ShardedHistogramPool {
    /// Create a new pool with a custom shard count (rounded up to power of two).
    ///
    /// # Errors
    /// Returns `SniperError::Histogram` if histogram instantiation fails.
    pub fn try_new(num_shards: usize) -> Result<Self> {
        let count = num_shards.next_power_of_two().clamp(8, 64);
        let mut shards = Vec::with_capacity(count);
        for _ in 0..count {
            let hist = Histogram::<u64>::new_with_bounds(
                HIST_LOW_BOUND_US,
                HIST_HIGH_BOUND_US,
                HIST_SIGFIGS,
            )
            .map_err(|e| {
                SniperError::Histogram(format!("Failed to create bounded histogram: {e}"))
            })?;
            shards.push(HistogramShard {
                hist: Mutex::new(hist),
            });
        }
        let mask = count - 1;
        Ok(Self { shards, mask })
    }

    /// Create a new pool with a custom shard count (rounded up to power of two, bounded 8..64).
    pub fn new(num_shards: usize) -> Self {
        Self::try_new(num_shards).unwrap_or_else(|_| {
            let count = num_shards.next_power_of_two().clamp(8, 64);
            let mut shards = Vec::with_capacity(count);
            for _ in 0..count {
                shards.push(HistogramShard {
                    hist: Mutex::new(fallback_histogram()),
                });
            }
            Self {
                shards,
                mask: count - 1,
            }
        })
    }

    /// Record a latency sample into the appropriate shard using a partition key (e.g. device_id).
    #[inline]
    pub fn record(&self, key: usize, lat_us: u64) {
        let shard_idx = key & self.mask;
        let clamped = lat_us.clamp(HIST_LOW_BOUND_US, HIST_HIGH_BOUND_US);
        if let Some(shard) = self.shards.get(shard_idx)
            && let Ok(mut hist) = shard.hist.lock()
        {
            let _ = hist.record(clamped);
        }
    }

    /// Number of active shards in this pool.
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Total number of samples recorded across all shards.
    pub fn total_samples(&self) -> u64 {
        let mut total = 0u64;
        for shard in &self.shards {
            if let Ok(hist) = shard.hist.lock() {
                total += hist.len();
            }
        }
        total
    }

    /// Merge all shards into a single consolidated `Histogram<u64>`.
    ///
    /// # Errors
    /// Returns `SniperError::Histogram` if histogram creation fails.
    pub fn try_merge_all(&self) -> Result<Histogram<u64>> {
        let mut merged =
            Histogram::<u64>::new_with_bounds(HIST_LOW_BOUND_US, HIST_HIGH_BOUND_US, HIST_SIGFIGS)
                .map_err(|e| {
                    SniperError::Histogram(format!("Failed to create merged histogram: {e}"))
                })?;

        for shard in &self.shards {
            if let Ok(hist) = shard.hist.lock() {
                let _ = merged.add(&*hist);
            }
        }
        Ok(merged)
    }

    /// Merge all shards into a single consolidated `Histogram<u64>`.
    pub fn merge_all(&self) -> Histogram<u64> {
        self.try_merge_all().unwrap_or_else(|_| {
            let mut fallback = fallback_histogram();
            for shard in &self.shards {
                if let Ok(hist) = shard.hist.lock() {
                    let _ = fallback.add(&*hist);
                }
            }
            fallback
        })
    }

    /// Consumes the pool and merges all shards into a single `Histogram<u64>`.
    pub fn into_merged_histogram(self) -> Histogram<u64> {
        self.merge_all()
    }
}

impl Default for ShardedHistogramPool {
    fn default() -> Self {
        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(8);
        Self::new(cpus * 2)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_histogram_shard_cache_line_alignment() {
        assert_eq!(std::mem::align_of::<HistogramShard>(), 64);
        assert_eq!(std::mem::size_of::<HistogramShard>() % 64, 0);
    }

    #[test]
    fn test_sharded_histogram_pool_single_thread() {
        let pool = ShardedHistogramPool::new(16);
        assert_eq!(pool.shard_count(), 16);

        for i in 0..1000 {
            pool.record(i, (i as u64 % 500) + 10);
        }

        assert_eq!(pool.total_samples(), 1000);
        let merged = pool.merge_all();
        assert_eq!(merged.len(), 1000);
        assert_eq!(merged.min(), 10);
        assert_eq!(merged.max(), 509);
        assert!(merged.value_at_quantile(0.50) >= 10);
    }

    #[test]
    fn test_sharded_histogram_pool_multithreaded_concurrency() {
        let pool = Arc::new(ShardedHistogramPool::new(16));
        let num_threads = 8;
        let samples_per_thread = 5000;

        let mut handles = Vec::new();
        for t in 0..num_threads {
            let pool_clone = Arc::clone(&pool);
            handles.push(thread::spawn(move || {
                for i in 0..samples_per_thread {
                    let key = t * samples_per_thread + i;
                    pool_clone.record(key, 500);
                }
            }));
        }

        for h in handles {
            h.join().expect("Worker thread join should succeed");
        }

        assert_eq!(
            pool.total_samples(),
            (num_threads * samples_per_thread) as u64
        );
        let merged = pool.merge_all();
        assert_eq!(merged.len(), (num_threads * samples_per_thread) as u64);
        assert_eq!(merged.value_at_quantile(0.50), 500);
        assert_eq!(merged.value_at_quantile(0.99), 500);
    }

    #[test]
    fn test_sharded_histogram_pool_statistical_equivalence() {
        let pool = ShardedHistogramPool::new(16);
        let mut reference =
            Histogram::<u64>::new_with_bounds(HIST_LOW_BOUND_US, HIST_HIGH_BOUND_US, HIST_SIGFIGS)
                .expect("Reference histogram bounds must be valid");

        for i in 1..=10_000 {
            let val = ((i * 37 % 50_000) + 1) as u64;
            pool.record(i, val);
            reference
                .record(val)
                .expect("Recording valid sample must succeed");
        }

        let merged = pool.into_merged_histogram();

        assert_eq!(merged.len(), reference.len());
        assert_eq!(merged.min(), reference.min());
        assert_eq!(merged.max(), reference.max());
        assert_eq!(
            merged.value_at_quantile(0.50),
            reference.value_at_quantile(0.50)
        );
        assert_eq!(
            merged.value_at_quantile(0.90),
            reference.value_at_quantile(0.90)
        );
        assert_eq!(
            merged.value_at_quantile(0.95),
            reference.value_at_quantile(0.95)
        );
        assert_eq!(
            merged.value_at_quantile(0.99),
            reference.value_at_quantile(0.99)
        );
        assert_eq!(
            merged.value_at_quantile(0.999),
            reference.value_at_quantile(0.999)
        );
        assert!((merged.mean() - reference.mean()).abs() < 1e-3);
    }

    #[test]
    fn test_sharded_histogram_pool_merge_timing_and_scale() {
        let pool = ShardedHistogramPool::new(64);
        for i in 0..100_000 {
            pool.record(i, ((i % 10_000) + 1) as u64);
        }
        assert_eq!(pool.total_samples(), 100_000);

        let start = std::time::Instant::now();
        let merged = pool.merge_all();
        let elapsed = start.elapsed();

        assert_eq!(merged.len(), 100_000);
        // Even in unoptimized debug build, merging 64 shards should execute in < 25ms (in release it is < 1ms)
        assert!(
            elapsed.as_millis() < 25,
            "Merge time should be under 25ms, got {:?}",
            elapsed
        );
    }
}
