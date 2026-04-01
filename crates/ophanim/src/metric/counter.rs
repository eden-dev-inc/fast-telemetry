//! Thread-sharded atomic counter.
//!
//! Forked from https://crates.io/crates/fast-counter (MIT/Apache licensed)
//! Originally authored by https://crates.io/users/JackThomson2
//! Modified to use crossbeam's CachePadded for more correct cache line sizing,
//! and to support swap operations and export operations.

use crate::thread_id::thread_id;
use crossbeam_utils::CachePadded;
use std::fmt;
use std::sync::atomic::{AtomicIsize, Ordering};

fn make_padded_counter() -> CachePadded<AtomicIsize> {
    CachePadded::new(AtomicIsize::new(0))
}

/// A sharded atomic counter.
///
/// Shards cache-line aligned AtomicIsize values across a vector for faster
/// updates in high contention scenarios.
pub struct Counter {
    cells: Vec<CachePadded<AtomicIsize>>,
}

impl fmt::Debug for Counter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Counter")
            .field("sum", &self.sum())
            .field("cells", &self.cells.len())
            .finish()
    }
}

impl Counter {
    /// Creates a new Counter with at least `count` cells.
    ///
    /// The count is rounded up to the next power of two for fast modulo.
    #[inline]
    pub fn new(count: usize) -> Self {
        let count = count.next_power_of_two();
        Self {
            cells: (0..count).map(|_| make_padded_counter()).collect(),
        }
    }

    /// Adds a value to the counter using relaxed ordering.
    #[inline]
    pub fn add(&self, value: isize) {
        self.add_with_ordering(value, Ordering::Relaxed)
    }

    /// Increments the counter by 1.
    #[inline]
    pub fn inc(&self) {
        self.add(1)
    }

    /// Adds a value to the counter with the specified ordering.
    #[inline]
    pub fn add_with_ordering(&self, value: isize, ordering: Ordering) {
        let idx = thread_id() & (self.cells.len() - 1);
        // SAFETY: idx is always < cells.len() due to power-of-two masking
        let cell = if cfg!(debug_assertions) {
            self.cells.get(idx).expect("index out of bounds")
        } else {
            unsafe { self.cells.get_unchecked(idx) }
        };
        cell.fetch_add(value, ordering);
    }

    /// Returns the sum of all shards using relaxed ordering.
    ///
    /// # Eventual Consistency
    ///
    /// Due to sharding, this may be slightly inaccurate under heavy concurrent
    /// modification - writes to already-summed shards won't be reflected until
    /// the next call. The total is eventually consistent.
    #[inline]
    pub fn sum(&self) -> isize {
        self.sum_with_ordering(Ordering::Relaxed)
    }

    /// Returns the sum of all shards with the specified ordering.
    #[inline]
    pub fn sum_with_ordering(&self, ordering: Ordering) -> isize {
        self.cells.iter().map(|c| c.load(ordering)).sum()
    }

    /// Resets all shards to zero and returns the previous sum.
    ///
    /// Useful for delta-style metrics export.
    ///
    /// # Eventual Consistency
    ///
    /// Writes that occur concurrently with `swap()` may be attributed to the
    /// next window rather than the current one. This is because shards are
    /// swapped sequentially - a write landing on an already-swapped shard
    /// will be picked up by the next `swap()` call. No counts are lost; they
    /// simply shift to the next export window. For telemetry purposes with
    /// multi-second export intervals, this timing skew is negligible.
    #[inline]
    pub fn swap(&self) -> isize {
        self.cells
            .iter()
            .map(|c| c.swap(0, Ordering::Relaxed))
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_test() {
        let counter = Counter::new(1);
        counter.add(1);
        assert_eq!(counter.sum(), 1);
    }

    #[test]
    fn increment_multiple_times() {
        let counter = Counter::new(1);
        counter.add(1);
        counter.add(1);
        counter.add(1);
        assert_eq!(counter.sum(), 3);
    }

    #[test]
    fn test_inc() {
        let counter = Counter::new(4);
        counter.inc();
        counter.inc();
        assert_eq!(counter.sum(), 2);
    }

    #[test]
    fn test_swap() {
        let counter = Counter::new(4);
        counter.add(100);
        let val = counter.swap();
        assert_eq!(val, 100);
        assert_eq!(counter.sum(), 0);
    }

    #[test]
    fn two_threads_incrementing_concurrently() {
        let counter = Counter::new(2);

        std::thread::scope(|s| {
            for _ in 0..2 {
                s.spawn(|| {
                    counter.add(1);
                });
            }
        });

        assert_eq!(counter.sum(), 2);
    }

    #[test]
    fn multiple_threads_incrementing_many_times() {
        const WRITE_COUNT: isize = 1_000_000;
        const THREAD_COUNT: isize = 8;

        let counter = Counter::new(THREAD_COUNT as usize);

        std::thread::scope(|s| {
            for _ in 0..THREAD_COUNT {
                s.spawn(|| {
                    for _ in 0..WRITE_COUNT {
                        counter.add(1);
                    }
                });
            }
        });

        assert_eq!(counter.sum(), THREAD_COUNT * WRITE_COUNT);
    }

    #[test]
    fn debug_format() {
        let counter = Counter::new(8);
        counter.add(42);
        let debug = format!("{counter:?}");
        assert!(debug.contains("sum: 42"));
        assert!(debug.contains("cells: 8"));
    }
}
