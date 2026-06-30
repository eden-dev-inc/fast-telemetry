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

#[cfg(feature = "bench-tools")]
fn make_counter_cell() -> AtomicIsize {
    AtomicIsize::new(0)
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

    #[inline]
    fn add_with_thread_id(&self, thread_id: usize, value: isize, ordering: Ordering) {
        let idx = thread_id & (self.cells.len() - 1);
        // SAFETY: idx is always < cells.len() due to power-of-two masking
        let cell = if cfg!(debug_assertions) {
            self.cells.get(idx).expect("index out of bounds")
        } else {
            unsafe { self.cells.get_unchecked(idx) }
        };
        cell.fetch_add(value, ordering);
    }

    /// Adds a value to the counter with the specified ordering.
    #[inline]
    pub fn add_with_ordering(&self, value: isize, ordering: Ordering) {
        self.add_with_thread_id(thread_id(), value, ordering);
    }

    /// Benchmark-only prototype for batching increments across multiple counters.
    #[cfg(feature = "bench-tools")]
    #[doc(hidden)]
    #[inline]
    pub fn inc_many(counters: &[Counter]) {
        Self::add_many(counters, 1);
    }

    /// Benchmark-only prototype for batching additions across multiple counters.
    #[cfg(feature = "bench-tools")]
    #[doc(hidden)]
    #[inline]
    pub fn add_many(counters: &[Counter], value: isize) {
        Self::add_many_with_ordering(counters, value, Ordering::Relaxed);
    }

    /// Benchmark-only prototype for batching additions across multiple counters.
    #[cfg(feature = "bench-tools")]
    #[doc(hidden)]
    #[inline]
    pub fn add_many_with_ordering(counters: &[Counter], value: isize, ordering: Ordering) {
        let thread_id = thread_id();
        for counter in counters {
            counter.add_with_thread_id(thread_id, value, ordering);
        }
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

/// Benchmark-only prototype for grouping related counters in one sharded layout.
#[cfg(feature = "bench-tools")]
#[doc(hidden)]
pub struct CounterSet {
    cells: Vec<AtomicIsize>,
    counters: usize,
    stride: usize,
    shard_mask: usize,
}

#[cfg(feature = "bench-tools")]
impl CounterSet {
    /// Creates a grouped counter set with `counters` counters per shard.
    ///
    /// Shards are padded by row instead of by cell, so related counters updated
    /// by the same thread sit contiguously while adjacent shard rows remain
    /// separated enough to avoid false sharing.
    pub fn new(shards: usize, counters: usize) -> Self {
        assert!(counters >= 1, "counters must be >= 1");
        let shards = shards.next_power_of_two();
        let cells_per_padded_counter =
            std::mem::size_of::<CachePadded<AtomicIsize>>() / std::mem::size_of::<AtomicIsize>();
        let row_padding = cells_per_padded_counter.max(1);
        let stride = counters.div_ceil(row_padding) * row_padding;
        let cells = (0..(shards * stride))
            .map(|_| make_counter_cell())
            .collect();

        Self {
            cells,
            counters,
            stride,
            shard_mask: shards - 1,
        }
    }

    /// Returns the number of counters in each shard row.
    #[inline]
    pub fn len(&self) -> usize {
        self.counters
    }

    /// Returns true if there are no counters in the set.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.counters == 0
    }

    #[inline]
    fn current_shard_offset(&self) -> usize {
        (thread_id() & self.shard_mask) * self.stride
    }

    #[inline]
    fn cell_at(&self, index: usize) -> &AtomicIsize {
        if cfg!(debug_assertions) {
            self.cells.get(index).expect("index out of bounds")
        } else {
            // SAFETY: callers compute indexes from checked counter indexes and
            // row offsets derived from the shard mask.
            unsafe { self.cells.get_unchecked(index) }
        }
    }

    /// Increments all counters in the current thread's shard row.
    #[inline]
    pub fn inc_all(&self) {
        self.add_all(1);
    }

    /// Adds the same value to all counters in the current thread's shard row.
    #[inline]
    pub fn add_all(&self, value: isize) {
        let offset = self.current_shard_offset();
        let row = if cfg!(debug_assertions) {
            self.cells
                .get(offset..offset + self.counters)
                .expect("row index out of bounds")
        } else {
            // SAFETY: current_shard_offset derives from shard_mask and stride,
            // and counters <= stride by construction.
            unsafe { std::slice::from_raw_parts(self.cells.as_ptr().add(offset), self.counters) }
        };
        for cell in row {
            cell.fetch_add(value, Ordering::Relaxed);
        }
    }

    /// Adds the same value to selected counters in the current thread's shard row.
    #[inline]
    pub fn add_indices(&self, indexes: &[usize], value: isize) {
        let offset = self.current_shard_offset();
        if cfg!(debug_assertions) {
            for index in indexes {
                assert!(*index < self.counters, "counter index out of bounds");
            }
        }
        for index in indexes {
            self.cell_at(offset + *index)
                .fetch_add(value, Ordering::Relaxed);
        }
    }

    /// Adds one value per selected counter in the current thread's shard row.
    #[inline]
    pub fn add_index_values(&self, updates: &[(usize, isize)]) {
        let offset = self.current_shard_offset();
        if cfg!(debug_assertions) {
            for (index, _) in updates {
                assert!(*index < self.counters, "counter index out of bounds");
            }
        }
        for (index, value) in updates {
            self.cell_at(offset + *index)
                .fetch_add(*value, Ordering::Relaxed);
        }
    }

    /// Adds the same value to all counters in the current thread's shard row.
    #[inline]
    pub fn add_all_indexed(&self, value: isize) {
        let offset = self.current_shard_offset();
        for counter_idx in 0..self.counters {
            self.cell_at(offset + counter_idx)
                .fetch_add(value, Ordering::Relaxed);
        }
    }

    /// Adds one value per counter in the current thread's shard row.
    #[inline]
    pub fn add_values(&self, values: &[isize]) {
        assert_eq!(values.len(), self.counters, "values must match counters");
        let offset = self.current_shard_offset();
        let row = if cfg!(debug_assertions) {
            self.cells
                .get(offset..offset + self.counters)
                .expect("row index out of bounds")
        } else {
            // SAFETY: current_shard_offset derives from shard_mask and stride,
            // and counters <= stride by construction.
            unsafe { std::slice::from_raw_parts(self.cells.as_ptr().add(offset), self.counters) }
        };
        for (cell, value) in row.iter().zip(values.iter().copied()) {
            cell.fetch_add(value, Ordering::Relaxed);
        }
    }

    /// Returns the sum for one counter across all shards.
    #[inline]
    pub fn sum(&self, counter_idx: usize) -> isize {
        assert!(counter_idx < self.counters, "counter index out of bounds");
        let shards = self.cells.len() / self.stride;
        (0..shards)
            .map(|shard| {
                self.cell_at((shard * self.stride) + counter_idx)
                    .load(Ordering::Relaxed)
            })
            .sum()
    }

    /// Returns the sum of all counters across all shards.
    #[inline]
    pub fn sum_all(&self) -> isize {
        let shards = self.cells.len() / self.stride;
        let mut total = 0isize;
        for shard in 0..shards {
            let offset = shard * self.stride;
            for counter_idx in 0..self.counters {
                total += self.cell_at(offset + counter_idx).load(Ordering::Relaxed);
            }
        }
        total
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

    #[cfg(feature = "bench-tools")]
    #[test]
    fn inc_many_updates_all_counters() {
        let counters = vec![Counter::new(4), Counter::new(4), Counter::new(4)];

        Counter::inc_many(&counters);
        Counter::add_many(&counters, 2);

        for counter in &counters {
            assert_eq!(counter.sum(), 3);
        }
    }

    #[cfg(feature = "bench-tools")]
    #[test]
    fn counter_set_updates_grouped_counters() {
        let counters = CounterSet::new(4, 3);

        counters.inc_all();
        counters.add_values(&[2, 3, 4]);
        counters.add_indices(&[0, 2], 1);
        counters.add_index_values(&[(1, 2), (2, 3)]);

        assert_eq!(counters.len(), 3);
        assert!(!counters.is_empty());
        assert_eq!(counters.sum(0), 4);
        assert_eq!(counters.sum(1), 6);
        assert_eq!(counters.sum(2), 9);
        assert_eq!(counters.sum_all(), 19);
    }
}
