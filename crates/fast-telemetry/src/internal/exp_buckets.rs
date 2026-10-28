// Modified for EDEN-3278 (2026-10-28): upper-inclusive distribution buckets.
//! Sharded exponential histogram bucket storage.
//!
//! Uses base-2 exponential boundaries (scale 0 in OTLP terms) for lock-free,
//! fixed-memory recording. Static distributions preallocate shared shards;
//! dynamic series allocate per-thread buffers. Recording uses a bucket-or-zero
//! increment, sum add and count add (3 relaxed atomic updates, no CAS).

use std::sync::atomic::{AtomicU64, Ordering};

/// Positive storage slots for the full `u64` range at OTLP scale zero.
///
/// Slot 0 contains only 1. Slot `s` in 1..=64 contains integers in
/// `(2^(s-1), 2^s]`, clipped to `u64::MAX`; its OTLP bucket index is `s - 1`.
pub(crate) const POSITIVE_BUCKETS: usize = 65;

/// Snapshot of merged bucket data across all threads.
///
/// Produced by `Distribution::buckets_snapshot()` for export. Concurrent
/// recording is observed through independent relaxed loads, not an atomic cut.
pub struct ExpBucketsSnapshot {
    /// Positive counts in 65 storage slots; OTLP index is the slot minus one.
    ///
    /// Slot 0 contains 1; slot `s` > 0 contains `(2^(s-1), 2^s]`, clipped
    /// to `u64::MAX`. Zero is counted separately in `zero_count`.
    pub positive: [u64; POSITIVE_BUCKETS],
    /// Count of zero-valued recordings.
    pub zero_count: u64,
    /// Cumulative sum of all recorded values.
    pub sum: u64,
    /// Cumulative count of all recorded values.
    pub count: u64,
}

impl ExpBucketsSnapshot {
    /// Representative integer midpoint for a positive storage slot.
    ///
    /// Slots 0 and 1 contain only 1 and 2 respectively. Higher slots use
    /// `3 * 2^(i-2)`, which fits in `u64` even for the final slot 64.
    /// This is an approximation for sample export, not an observed extremum.
    ///
    /// # Panics
    /// Panics if `i` is outside the positive slot range 0..=64.
    pub fn bucket_midpoint(i: usize) -> u64 {
        assert!(i < POSITIVE_BUCKETS, "positive bucket slot out of range");
        match i {
            0 => 1,
            1 => 2,
            _ => 3u64 << (i - 2),
        }
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

    /// Smallest integer permitted by the lowest occupied bucket.
    ///
    /// This lower bound need not have been recorded. Returns zero if any
    /// zeros were recorded, and `None` for an empty snapshot.
    pub fn min(&self) -> Option<u64> {
        if self.zero_count > 0 {
            return Some(0);
        }
        for (i, &c) in self.positive.iter().enumerate() {
            if c > 0 {
                return Some(if i == 0 { 1 } else { (1u64 << (i - 1)) + 1 });
            }
        }
        None
    }

    /// Largest integer permitted by the highest occupied bucket.
    ///
    /// This upper bound need not have been recorded. Returns zero for an
    /// all-zero snapshot and `None` for an empty snapshot.
    pub fn max(&self) -> Option<u64> {
        for i in (0..POSITIVE_BUCKETS).rev() {
            if self.positive[i] > 0 {
                // The final mathematical upper bound is 2^64, outside u64.
                if i == 64 {
                    return Some(u64::MAX);
                }
                return Some(1u64 << i);
            }
        }
        if self.zero_count > 0 {
            return Some(0);
        }
        None
    }
}

/// Exponential histogram storage for one shard or dynamic-series thread buffer.
///
/// Positive storage slot `s` maps to the upper-inclusive OTLP scale-zero
/// bucket `s - 1`. Zero-valued samples go in `zero_count`.
///
/// All fields use `Relaxed` ordering. Writers may share a static shard;
/// export reads independent approximate snapshots while recording continues.
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

/// Map a nonzero `u64` to a positive storage slot, `ceil(log2(value))`.
///
/// The caller handles zero separately. Subtraction cannot underflow for a
/// nonzero value; `leading_zeros(0) == 64` makes value 1 map to slot 0.
/// The result is in 0..=64 and its OTLP scale-zero index is one less.
/// Uses integer arithmetic without logarithms, allocation, or extra atomics.
#[inline]
fn bucket_index(value: u64) -> usize {
    debug_assert!(value > 0, "bucket_index called with zero");
    64 - (value - 1).leading_zeros() as usize
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
        assert_eq!(bucket_index(3), 2); // ceil(log2(3)) = 2
        assert_eq!(bucket_index(5), 3); // ceil(log2(5)) = 3
        assert_eq!(bucket_index(7), 3);
        assert_eq!(bucket_index(255), 8);
        assert_eq!(bucket_index(256), 8);
        assert_eq!(bucket_index(u64::MAX), 64);
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
        b.record(3); // slot 2
        b.record(100); // slot 7

        assert_eq!(b.get_count(), 4);
        assert_eq!(b.get_sum(), 106);
        assert_eq!(b.get_zero_count(), 0);

        let buckets = b.get_positive_buckets();
        assert_eq!(buckets[0], 1); // value 1
        assert_eq!(buckets[1], 1); // value 2
        assert_eq!(buckets[2], 1); // value 3
        assert_eq!(buckets[7], 1); // value 100
    }

    #[test]
    fn snapshot_min_max() {
        let snap = ExpBucketsSnapshot {
            positive: {
                let mut a = [0u64; POSITIVE_BUCKETS];
                a[3] = 5; // values in (4, 8]
                a[7] = 2; // values in (64, 128]
                a
            },
            zero_count: 0,
            sum: 500,
            count: 7,
        };

        assert_eq!(snap.min(), Some(5)); // smallest integer greater than 2^2
        assert_eq!(snap.max(), Some(128)); // 2^7
    }

    #[test]
    fn snapshot_min_max_with_zero() {
        let snap = ExpBucketsSnapshot {
            positive: [0u64; POSITIVE_BUCKETS],
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
            positive: [0u64; POSITIVE_BUCKETS],
            zero_count: 0,
            sum: 0,
            count: 0,
        };

        assert_eq!(snap.min(), None);
        assert_eq!(snap.max(), None);
    }

    #[test]
    fn final_positive_slot_keeps_u64_max_and_zero_separate() {
        let buckets = ExpBuckets::new();
        buckets.record(u64::MAX);
        buckets.record(0);
        let positive = buckets.get_positive_buckets();
        assert_eq!(positive.len(), 65);
        assert_eq!(positive[64], 1);
        assert_eq!(positive.iter().sum::<u64>(), 1);
        assert_eq!(buckets.get_zero_count(), 1);
        assert_eq!(buckets.get_count(), 2);
        assert_eq!(buckets.get_sum(), u64::MAX);
        let snapshot = ExpBucketsSnapshot {
            positive,
            zero_count: 0,
            sum: u64::MAX,
            count: 1,
        };
        assert_eq!(snapshot.min(), Some((1u64 << 63) + 1));
        assert_eq!(snapshot.max(), Some(u64::MAX));
        assert_eq!(
            snapshot.iter_samples().collect::<Vec<_>>(),
            vec![(3u64 << 62, 1)]
        );
    }

    #[test]
    fn integer_representatives_and_bounds_cover_every_positive_slot() {
        for slot in 0..POSITIVE_BUCKETS {
            let mut positive = [0u64; POSITIVE_BUCKETS];
            positive[slot] = 1;
            let snapshot = ExpBucketsSnapshot {
                positive,
                zero_count: 0,
                sum: 0,
                count: 1,
            };
            let representative = ExpBucketsSnapshot::bucket_midpoint(slot);
            let lower = snapshot.min().expect("occupied bucket");
            let upper = snapshot.max().expect("occupied bucket");
            assert!(lower <= representative && representative <= upper);
            assert_eq!(bucket_index(lower), slot);
            assert_eq!(bucket_index(upper), slot);
            assert_eq!(bucket_index(representative), slot);
        }
        assert_eq!(ExpBucketsSnapshot::bucket_midpoint(0), 1);
        assert_eq!(ExpBucketsSnapshot::bucket_midpoint(1), 2);
        assert_eq!(ExpBucketsSnapshot::bucket_midpoint(2), 3);
        assert_eq!(ExpBucketsSnapshot::bucket_midpoint(8), 192);
        assert_eq!(ExpBucketsSnapshot::bucket_midpoint(64), 3u64 << 62);
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
