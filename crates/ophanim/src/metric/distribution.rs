//! Distribution: exponential histogram storage for high-performance recording.
//!
//! Uses base-2 exponential buckets with pre-allocated, thread-sharded cells.
//! The hot path is a TLS read + bitmask index + 3 relaxed atomics (bucket
//! increment, sum add, count add) — identical cost to a sharded counter.
//!
//! OTLP export produces a native `ExponentialHistogram`.
//! DogStatsD export produces count + sum counters (same as `Histogram`).

use crate::exp_buckets::{ExpBuckets, ExpBucketsSnapshot};
use crate::thread_id::thread_id;
use crossbeam_utils::CachePadded;

/// Exponential histogram distribution for high-performance metric recording.
///
/// Cells are pre-allocated and indexed by `thread_id & mask`, so recording is
/// lock-free with no per-call TLS bookkeeping.
pub struct Distribution {
    cells: Vec<CachePadded<ExpBuckets>>,
    shard_mask: usize,
}

impl Distribution {
    /// Create a new Distribution with `shard_count` shards.
    ///
    /// The count is rounded up to the next power of two for fast modulo.
    pub fn new(shard_count: usize) -> Self {
        let shard_count = shard_count.next_power_of_two();
        Self {
            cells: (0..shard_count)
                .map(|_| CachePadded::new(ExpBuckets::new()))
                .collect(),
            shard_mask: shard_count - 1,
        }
    }

    /// Record a value.
    #[inline]
    pub fn record(&self, value: u64) {
        let idx = thread_id() & self.shard_mask;
        // SAFETY: idx is always < cells.len() due to power-of-two masking
        let cell = if cfg!(debug_assertions) {
            self.cells.get(idx).expect("index out of bounds")
        } else {
            unsafe { self.cells.get_unchecked(idx) }
        };
        cell.record(value);
    }

    /// Cumulative count across all shards.
    pub fn count(&self) -> u64 {
        self.cells.iter().map(|c| c.get_count()).sum()
    }

    /// Cumulative sum across all shards.
    pub fn sum(&self) -> u64 {
        self.cells.iter().map(|c| c.get_sum()).sum()
    }

    /// Approximate minimum from bucket boundaries.
    pub fn min(&self) -> Option<u64> {
        self.buckets_snapshot().min()
    }

    /// Approximate maximum from bucket boundaries.
    pub fn max(&self) -> Option<u64> {
        self.buckets_snapshot().max()
    }

    /// Mean value, or `None` if no values have been recorded.
    pub fn mean(&self) -> Option<f64> {
        let count = self.count();
        if count == 0 {
            return None;
        }
        Some(self.sum() as f64 / count as f64)
    }

    /// Merge bucket counts from all shards into a single snapshot.
    pub fn buckets_snapshot(&self) -> ExpBucketsSnapshot {
        let mut positive = [0u64; 64];
        let mut zero_count = 0u64;
        let mut sum = 0u64;
        let mut count = 0u64;

        for cell in &self.cells {
            let shard_buckets = cell.get_positive_buckets();
            for (i, &c) in shard_buckets.iter().enumerate() {
                positive[i] += c;
            }
            zero_count += cell.get_zero_count();
            sum += cell.get_sum();
            count += cell.get_count();
        }

        ExpBucketsSnapshot {
            positive,
            zero_count,
            sum,
            count,
        }
    }
}

impl Default for Distribution {
    fn default() -> Self {
        Self::new(4)
    }
}

impl std::fmt::Debug for Distribution {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Distribution")
            .field("count", &self.count())
            .field("sum", &self.sum())
            .field("min", &self.min())
            .field("max", &self.max())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn basic_recording() {
        let dist = Distribution::new(4);
        dist.record(100);
        dist.record(200);
        dist.record(300);

        assert_eq!(dist.count(), 3);
        assert_eq!(dist.sum(), 600);
        assert!(dist.min().is_some());
        assert!(dist.max().is_some());
    }

    #[test]
    fn empty() {
        let dist = Distribution::new(4);
        assert_eq!(dist.count(), 0);
        assert_eq!(dist.min(), None);
        assert_eq!(dist.max(), None);
        assert_eq!(dist.mean(), None);
    }

    #[test]
    fn mean() {
        let dist = Distribution::new(4);
        dist.record(100);
        dist.record(200);
        dist.record(300);

        let mean = dist.mean().expect("should have mean");
        assert!((mean - 200.0).abs() < 0.01);
    }

    #[test]
    fn concurrent_recording() {
        let dist = Arc::new(Distribution::new(8));
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let d = Arc::clone(&dist);
                std::thread::spawn(move || {
                    for i in 1..=1000u64 {
                        d.record(i);
                    }
                })
            })
            .collect();

        for t in threads {
            t.join().expect("thread panicked");
        }
        assert_eq!(dist.count(), 8000);
        assert_eq!(dist.sum(), 8 * (1000 * 1001 / 2));
    }

    #[test]
    fn multiple_distributions_independent() {
        let dist1 = Distribution::new(4);
        let dist2 = Distribution::new(4);

        dist1.record(100);
        dist2.record(200);

        assert_eq!(dist1.count(), 1);
        assert_eq!(dist1.sum(), 100);
        assert_eq!(dist2.count(), 1);
        assert_eq!(dist2.sum(), 200);
    }

    #[test]
    fn buckets_snapshot_merges_threads() {
        let dist = Arc::new(Distribution::new(4));

        let threads: Vec<_> = (0..4)
            .map(|_| {
                let d = Arc::clone(&dist);
                std::thread::spawn(move || {
                    d.record(100); // bucket 6 (64..128)
                })
            })
            .collect();

        for t in threads {
            t.join().expect("thread panicked");
        }

        let snap = dist.buckets_snapshot();
        assert_eq!(snap.count, 4);
        assert_eq!(snap.sum, 400);
        assert_eq!(snap.positive[6], 4); // value 100 → bucket 6
    }

    #[test]
    fn records_zero() {
        let dist = Distribution::new(4);
        dist.record(0);
        dist.record(0);
        dist.record(42);

        assert_eq!(dist.count(), 3);
        assert_eq!(dist.sum(), 42);

        let snap = dist.buckets_snapshot();
        assert_eq!(snap.zero_count, 2);
        assert_eq!(snap.min(), Some(0));
    }

    #[test]
    fn min_max_approximate() {
        let dist = Distribution::new(4);
        dist.record(100); // bucket 6: [64, 128)

        // min returns lower bound of lowest non-zero bucket
        assert_eq!(dist.min(), Some(64));
        // max returns upper bound of highest non-zero bucket
        assert_eq!(dist.max(), Some(127));
    }
}
