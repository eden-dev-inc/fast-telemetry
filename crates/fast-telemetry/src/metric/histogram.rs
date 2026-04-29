//! Fixed-bucket histogram using sharded counters.
//!
//! Each bucket is a thread-sharded `Counter`, so recording is contention-free.
//! Bucket boundaries are defined at construction time.

use crate::Counter;

/// A fixed-bucket histogram with thread-sharded counters.
///
/// Buckets are defined by upper bounds (inclusive). Values are placed in the
/// first bucket whose bound is >= the value. Values exceeding all bounds go
/// in the final "+Inf" bucket.
pub struct Histogram {
    /// Upper bounds for each bucket (last is always +Inf conceptually)
    bounds: Vec<u64>,
    /// One sharded counter per bucket, plus one for +Inf
    buckets: Vec<Counter>,
    /// Sum of all recorded values (for computing mean)
    sum: Counter,
    /// Total count (for Prometheus _count)
    count: Counter,
}

impl Histogram {
    /// Create a histogram with the given bucket boundaries.
    ///
    /// Boundaries should be sorted ascending. Each boundary represents the
    /// upper bound (inclusive) of a bucket. An implicit +Inf bucket is added.
    ///
    /// `shard_count` is passed to each underlying `Counter`.
    pub fn new(bounds: &[u64], shard_count: usize) -> Self {
        let buckets = (0..=bounds.len())
            .map(|_| Counter::new(shard_count))
            .collect();

        Self {
            bounds: bounds.to_vec(),
            buckets,
            sum: Counter::new(shard_count),
            count: Counter::new(shard_count),
        }
    }

    /// Create a histogram with default latency buckets (in microseconds).
    ///
    /// Buckets: 10µs, 50µs, 100µs, 500µs, 1ms, 5ms, 10ms, 50ms, 100ms, 500ms, 1s, 5s, 10s
    pub fn with_latency_buckets(shard_count: usize) -> Self {
        Self::new(
            &[
                10,         // 10µs
                50,         // 50µs
                100,        // 100µs
                500,        // 500µs
                1_000,      // 1ms
                5_000,      // 5ms
                10_000,     // 10ms
                50_000,     // 50ms
                100_000,    // 100ms
                500_000,    // 500ms
                1_000_000,  // 1s
                5_000_000,  // 5s
                10_000_000, // 10s
            ],
            shard_count,
        )
    }

    /// Record a value in the histogram.
    #[inline]
    pub fn record(&self, value: u64) {
        // Find bucket via linear search (should be fast for small bucket counts)
        let bucket_idx = self
            .bounds
            .iter()
            .position(|&bound| value <= bound)
            .unwrap_or(self.bounds.len());

        self.buckets[bucket_idx].inc();
        self.sum.add(value as isize);
        self.count.inc();
    }

    /// Get cumulative bucket counts -- for Prometheus exposition.
    ///
    /// Returns pairs of (upper_bound, cumulative_count). The last entry
    /// has bound `u64::MAX` representing +Inf.
    ///
    /// Prefer [`Self::buckets_cumulative_iter`] on the export path; it avoids
    /// the `Vec` allocation per call.
    pub fn buckets_cumulative(&self) -> Vec<(u64, u64)> {
        self.buckets_cumulative_iter().collect()
    }

    /// Iterator form of [`Self::buckets_cumulative`] that skips the `Vec`
    /// allocation. Used by the Prometheus and OTLP export paths.
    pub fn buckets_cumulative_iter(&self) -> impl Iterator<Item = (u64, u64)> + '_ {
        let mut cumulative = 0i64;
        self.buckets.iter().enumerate().map(move |(i, counter)| {
            cumulative += counter.sum() as i64;
            let bound = if i < self.bounds.len() {
                self.bounds[i]
            } else {
                u64::MAX
            };
            (bound, cumulative as u64)
        })
    }

    pub fn sum(&self) -> u64 {
        self.sum.sum() as u64
    }

    pub fn count(&self) -> u64 {
        self.count.sum() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_recording() {
        let h = Histogram::new(&[10, 100, 1000], 4);

        h.record(5); // bucket 0 (≤10)
        h.record(50); // bucket 1 (≤100)
        h.record(500); // bucket 2 (≤1000)
        h.record(5000); // bucket 3 (+Inf)

        let buckets = h.buckets_cumulative();
        assert_eq!(buckets.len(), 4);
        assert_eq!(buckets[0], (10, 1)); // ≤10: 1 cumulative
        assert_eq!(buckets[1], (100, 2)); // ≤100: 2 cumulative
        assert_eq!(buckets[2], (1000, 3)); // ≤1000: 3 cumulative
        assert_eq!(buckets[3], (u64::MAX, 4)); // +Inf: 4 cumulative

        assert_eq!(h.count(), 4);
        assert_eq!(h.sum(), 5 + 50 + 500 + 5000);
    }

    #[test]
    fn test_boundary_values() {
        let h = Histogram::new(&[10, 100], 4);

        h.record(10); // exactly on boundary, goes in bucket 0
        h.record(100); // exactly on boundary, goes in bucket 1

        let buckets = h.buckets_cumulative();
        assert_eq!(buckets[0], (10, 1));
        assert_eq!(buckets[1], (100, 2));
    }

    #[test]
    fn test_latency_buckets() {
        let h = Histogram::with_latency_buckets(4);

        h.record(5); // 5µs
        h.record(1_000); // 1ms
        h.record(1_000_000); // 1s

        assert_eq!(h.count(), 3);
    }
}
