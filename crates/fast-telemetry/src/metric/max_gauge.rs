//! Thread-sharded maximum gauge.
//!
//! Records the maximum observed value without introducing a single contended
//! atomic on the hot path. Each thread updates its own shard with `fetch_max`,
//! and reads aggregate by taking the maximum across shards.

use crate::thread_id::thread_id;
use crossbeam_utils::CachePadded;
use std::fmt;
use std::sync::atomic::{AtomicI64, Ordering};

fn make_padded_cell(initial: i64) -> CachePadded<AtomicI64> {
    CachePadded::new(AtomicI64::new(initial))
}

/// A thread-sharded maximum tracker exported as a gauge.
///
/// This is useful for recording peaks such as maximum queue depth or the
/// highest number of in-flight requests seen during an export interval.
///
/// Unlike [`crate::Gauge`], this is not a point-in-time `set()` value.
/// Writers call [`observe`](Self::observe), and readers aggregate by taking
/// the maximum across shards.
pub struct MaxGauge {
    cells: Vec<CachePadded<AtomicI64>>,
    reset_value: i64,
}

impl MaxGauge {
    /// Create a new max gauge with all shards initialized to zero.
    ///
    /// This is appropriate for metrics that are naturally non-negative.
    pub fn new(shard_count: usize) -> Self {
        Self::with_value(shard_count, 0)
    }

    /// Create a new max gauge with all shards initialized to `initial`.
    pub fn with_value(shard_count: usize, initial: i64) -> Self {
        let shard_count = shard_count.next_power_of_two();
        Self {
            cells: (0..shard_count)
                .map(|_| make_padded_cell(initial))
                .collect(),
            reset_value: initial,
        }
    }

    /// Record a candidate value for the maximum.
    #[inline]
    pub fn observe(&self, value: i64) {
        let idx = thread_id() & (self.cells.len() - 1);
        let cell = if cfg!(debug_assertions) {
            self.cells.get(idx).expect("index out of bounds")
        } else {
            unsafe { self.cells.get_unchecked(idx) }
        };
        cell.fetch_max(value, Ordering::Relaxed);
    }

    /// Return the current maximum across all shards.
    #[inline]
    pub fn get(&self) -> i64 {
        self.cells
            .iter()
            .map(|cell| cell.load(Ordering::Relaxed))
            .max()
            .unwrap_or(self.reset_value)
    }

    /// Reset all shards to the original value configured at construction.
    ///
    /// This is intended for export/sampling code, not the hot path.
    pub fn reset(&self) {
        for cell in &self.cells {
            cell.store(self.reset_value, Ordering::Relaxed);
        }
    }

    /// Reset all shards and return the previous maximum.
    ///
    /// Concurrent observations that land on already-reset shards may be
    /// attributed to the next window rather than the current one. No maxima
    /// are lost, but timing near the reset boundary is eventually consistent
    /// in the same way as [`crate::Counter::swap`].
    pub fn swap_reset(&self) -> i64 {
        self.cells
            .iter()
            .map(|cell| cell.swap(self.reset_value, Ordering::Relaxed))
            .max()
            .unwrap_or(self.reset_value)
    }
}

impl Default for MaxGauge {
    fn default() -> Self {
        Self::new(4)
    }
}

impl fmt::Debug for MaxGauge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MaxGauge")
            .field("max", &self.get())
            .field("cells", &self.cells.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_observe() {
        let gauge = MaxGauge::new(4);
        gauge.observe(3);
        gauge.observe(7);
        gauge.observe(5);
        assert_eq!(gauge.get(), 7);
    }

    #[test]
    fn initial_value_is_respected() {
        let gauge = MaxGauge::with_value(4, -10);
        assert_eq!(gauge.get(), -10);
        gauge.observe(-3);
        assert_eq!(gauge.get(), -3);
    }

    #[test]
    fn reset_and_swap_reset() {
        let gauge = MaxGauge::with_value(4, 0);
        gauge.observe(9);
        gauge.observe(4);
        assert_eq!(gauge.swap_reset(), 9);
        assert_eq!(gauge.get(), 0);
        gauge.observe(2);
        gauge.reset();
        assert_eq!(gauge.get(), 0);
    }

    #[test]
    fn concurrent_observe() {
        let gauge = MaxGauge::with_value(8, 0);
        std::thread::scope(|s| {
            for value in [10, 30, 20, 40, 15, 25, 35, 5] {
                let gauge = &gauge;
                s.spawn(move || {
                    gauge.observe(value);
                });
            }
        });
        assert_eq!(gauge.get(), 40);
    }
}
