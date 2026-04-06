//! Thread-local exponential histogram bucket storage.
//!
//! Uses base-2 exponential boundaries (scale 0 in OTLP terms) for lock-free,
//! fixed-memory recording.  Each thread gets its own `ExpBuckets` instance;
//! recording is a single bucket increment + sum add + count add (3 relaxed
//! atomics, no CAS).

use std::sync::atomic::{AtomicU64, Ordering};

/// Number of positive buckets.  Bucket *i* covers values in [2^i, 2^(i+1)).
/// 64 buckets span the entire u64 range.
const POSITIVE_BUCKETS: usize = 64;

/// Snapshot of merged bucket data across all threads.
///
/// Produced by `Distribution::buckets_snapshot()` for export.
pub struct ExpBucketsSnapshot {
    /// Per-bucket counts.  Index *i* covers [2^i, 2^(i+1)).
    pub positive: [u64; POSITIVE_BUCKETS],
    /// Count of zero-valued recordings.
    pub zero_count: u64,
    /// Cumulative sum of all recorded values.
    pub sum: u64,
    /// Cumulative count of all recorded values.
    pub count: u64,
}

impl ExpBucketsSnapshot {
    /// Representative midpoint value for bucket `i`.
    ///
    /// Bucket *i* covers [2^i, 2^(i+1)).  The midpoint is the arithmetic
    /// mean of the boundaries: (2^i + 2^(i+1)) / 2 = 3 × 2^(i−1).
    /// Special case: bucket 0 contains only the value 1, so midpoint = 1.
    pub fn bucket_midpoint(i: usize) -> u64 {
        if i == 0 { 1 } else { 3u64 << (i - 1) }
    }

    /// Iterate over non-zero bucket entries as `(representative_value, count)` pairs.
    ///
    /// Useful for reconstructing approximate samples for DogStatsD `|d` export.
    /// Zero-valued recordings come first (value = 0), then positive buckets in
    /// ascending order using [`bucket_midpoint`](Self::bucket_midpoint) as the
    /// representative value.
    pub fn iter_samples(&self) -> impl Iterator<Item = (u64, u64)> + '_ {
        let zero = (self.zero_count > 0).then_some((0u64, self.zero_count));

        let positive = self.positive.iter().enumerate().filter_map(|(i, &count)| {
            if count > 0 {
                Some((Self::bucket_midpoint(i), count))
            } else {
                None
            }
        });

        zero.into_iter().chain(positive)
    }

    /// Approximate minimum from the lowest non-zero bucket.
    pub fn min(&self) -> Option<u64> {
        if self.zero_count > 0 {
            return Some(0);
        }
        for (i, &c) in self.positive.iter().enumerate() {
            if c > 0 {
                // Lower bound of bucket i is 2^i (except bucket 0 which is [1, 2))
                return Some(1u64 << i);
            }
        }
        None
    }

    /// Approximate maximum from the highest non-zero bucket.
    pub fn max(&self) -> Option<u64> {
        for i in (0..POSITIVE_BUCKETS).rev() {
            if self.positive[i] > 0 {
                // Upper bound of bucket i is 2^(i+1) - 1
                if i == 63 {
                    return Some(u64::MAX);
                }
                return Some((1u64 << (i + 1)) - 1);
            }
        }
        if self.zero_count > 0 {
            return Some(0);
        }
        None
    }
}

/// Per-thread exponential histogram storage.
///
/// Base-2 boundaries (OTLP scale = 0): bucket *i* covers [2^i, 2^(i+1)).
/// Zero-valued samples go in `zero_count`.
///
/// All fields use `Relaxed` ordering — the only cross-thread access is from
/// the export path which reads approximate snapshots.
pub(crate) struct ExpBuckets {
    positive: [AtomicU64; POSITIVE_BUCKETS],
    zero_count: AtomicU64,
    sum: AtomicU64,
    count: AtomicU64,
}

impl ExpBuckets {
    pub(crate) fn new() -> Self {
        Self {
            positive: std::array::from_fn(|_| AtomicU64::new(0)),
            zero_count: AtomicU64::new(0),
            sum: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    /// Record a value.  3 relaxed atomics, no CAS.
    #[inline]
    pub(crate) fn record(&self, value: u64) {
        if value == 0 {
            self.zero_count.fetch_add(1, Ordering::Relaxed);
        } else {
            let idx = bucket_index(value);
            self.positive[idx].fetch_add(1, Ordering::Relaxed);
        }
        self.sum.fetch_add(value, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn get_count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    pub(crate) fn get_sum(&self) -> u64 {
        self.sum.load(Ordering::Relaxed)
    }

    pub(crate) fn get_zero_count(&self) -> u64 {
        self.zero_count.load(Ordering::Relaxed)
    }

    /// Read all positive bucket counts into a fixed-size array.
    pub(crate) fn get_positive_buckets(&self) -> [u64; POSITIVE_BUCKETS] {
        let mut out = [0u64; POSITIVE_BUCKETS];
        for (i, bucket) in self.positive.iter().enumerate() {
            out[i] = bucket.load(Ordering::Relaxed);
        }
        out
    }
}

/// Map a non-zero u64 value to its bucket index.
///
/// Bucket *i* covers [2^i, 2^(i+1)).  This computes `floor(log2(value))` via
/// the CLZ (count leading zeros) CPU instruction — a single cycle on all
/// modern architectures.
#[inline]
fn bucket_index(value: u64) -> usize {
    debug_assert!(value > 0, "bucket_index called with zero");
    63 - value.leading_zeros() as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_index_powers_of_two() {
        assert_eq!(bucket_index(1), 0); // 2^0
        assert_eq!(bucket_index(2), 1); // 2^1
        assert_eq!(bucket_index(4), 2); // 2^2
        assert_eq!(bucket_index(1024), 10); // 2^10
        assert_eq!(bucket_index(1 << 63), 63);
    }

    #[test]
    fn bucket_index_non_powers() {
        assert_eq!(bucket_index(3), 1); // floor(log2(3)) = 1
        assert_eq!(bucket_index(5), 2); // floor(log2(5)) = 2
        assert_eq!(bucket_index(7), 2);
        assert_eq!(bucket_index(255), 7);
        assert_eq!(bucket_index(256), 8);
        assert_eq!(bucket_index(u64::MAX), 63);
    }

    #[test]
    fn record_zero() {
        let b = ExpBuckets::new();
        b.record(0);
        b.record(0);
        assert_eq!(b.get_zero_count(), 2);
        assert_eq!(b.get_count(), 2);
        assert_eq!(b.get_sum(), 0);

        let buckets = b.get_positive_buckets();
        assert!(buckets.iter().all(|&c| c == 0));
    }

    #[test]
    fn record_nonzero() {
        let b = ExpBuckets::new();
        b.record(1); // bucket 0
        b.record(2); // bucket 1
        b.record(3); // bucket 1
        b.record(100); // bucket 6

        assert_eq!(b.get_count(), 4);
        assert_eq!(b.get_sum(), 106);
        assert_eq!(b.get_zero_count(), 0);

        let buckets = b.get_positive_buckets();
        assert_eq!(buckets[0], 1); // value 1
        assert_eq!(buckets[1], 2); // values 2, 3
        assert_eq!(buckets[6], 1); // value 100
    }

    #[test]
    fn snapshot_min_max() {
        let snap = ExpBucketsSnapshot {
            positive: {
                let mut a = [0u64; 64];
                a[3] = 5; // values in [8, 16)
                a[7] = 2; // values in [128, 256)
                a
            },
            zero_count: 0,
            sum: 500,
            count: 7,
        };

        assert_eq!(snap.min(), Some(8)); // 2^3
        assert_eq!(snap.max(), Some(255)); // 2^8 - 1
    }

    #[test]
    fn snapshot_min_max_with_zero() {
        let snap = ExpBucketsSnapshot {
            positive: [0u64; 64],
            zero_count: 3,
            sum: 0,
            count: 3,
        };

        assert_eq!(snap.min(), Some(0));
        assert_eq!(snap.max(), Some(0));
    }

    #[test]
    fn snapshot_empty() {
        let snap = ExpBucketsSnapshot {
            positive: [0u64; 64],
            zero_count: 0,
            sum: 0,
            count: 0,
        };

        assert_eq!(snap.min(), None);
        assert_eq!(snap.max(), None);
    }

    #[test]
    fn concurrent_recording() {
        use std::sync::Arc;

        let b = Arc::new(ExpBuckets::new());
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let b = Arc::clone(&b);
                std::thread::spawn(move || {
                    for i in 1..=1000u64 {
                        b.record(i);
                    }
                })
            })
            .collect();

        for t in threads {
            t.join().expect("thread panicked");
        }

        assert_eq!(b.get_count(), 8000);
        assert_eq!(b.get_sum(), 8 * (1000 * 1001 / 2)); // 8 * sum(1..=1000)
    }
}
