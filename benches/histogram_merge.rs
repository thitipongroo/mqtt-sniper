//! Merge throughput of `ShardedHistogramPool`, measured in optimized builds.
//!
//! Run with `cargo bench --bench histogram_merge`. Kept out of `cargo test` because
//! wall-clock assertions are unreliable on shared CI runners and unoptimized test builds.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use mqtt_sniper::metrics::ShardedHistogramPool;
use std::hint::black_box;

const SAMPLES: usize = 100_000;

fn bench_merge_all(c: &mut Criterion) {
    let mut group = c.benchmark_group("sharded_histogram_merge_all");

    for shards in [8, 16, 64] {
        let pool = ShardedHistogramPool::new(shards);
        for i in 0..SAMPLES {
            pool.record(i, (i as u64 % 10_000) + 1);
        }

        group.bench_with_input(BenchmarkId::from_parameter(shards), &pool, |b, pool| {
            b.iter(|| black_box(pool.merge_all()));
        });
    }

    group.finish();
}

criterion_group!(benches, bench_merge_all);
criterion_main!(benches);
