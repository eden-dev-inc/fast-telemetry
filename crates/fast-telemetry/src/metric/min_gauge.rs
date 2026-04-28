//! Thread-sharded minimum gauge.

use crate::thread_id::thread_id;
use crossbeam_utils::CachePadded;
use std::fmt;
use std::sync::atomic::{AtomicI64, Ordering};

fn make_padded_cell(initial: i64) -> CachePadded<AtomicI64> {
    CachePadded::new(AtomicI64::new(initial))
}

/// A thread-sharded minimum tracker exported as a gauge.
pub struct MinGauge {
    cells: Vec<CachePadded<AtomicI64>>,
    reset_value: i64,
}

impl MinGauge {
    /// Create a new min gauge with all shards initialized to [`i64::MAX`],
    /// so any observation displaces the initial value.
    ///
    /// `get()` on a gauge that has never been observed returns [`i64::MAX`];
    /// callers that need a different sentinel should use [`Self::with_value`].
    pub fn new(shard_count: usize) -> Self {
        Self::with_value(shard_count, i64::MAX)
    }

    /// Create a new min gauge with all shards initialized to `initial`.
    pub fn with_value(shard_count: usize, initial: i64) -> Self {
        let shard_count = shard_count.next_power_of_two();
        Self {
            cells: (0..shard_count)
                .map(|_| make_padded_cell(initial))
                .collect(),
            reset_value: initial,
        }
    }

    /// Record a candidate value for the minimum.
    #[inline]
    pub fn observe(&self, value: i64) {
        let idx = thread_id() & (self.cells.len() - 1);
        let cell = if cfg!(debug_assertions) {
            self.cells.get(idx).expect("index out of bounds")
        } else {
            unsafe { self.cells.get_unchecked(idx) }
        };
        cell.fetch_min(value, Ordering::Relaxed);
    }

    /// Return the current minimum across all shards.
    #[inline]
    pub fn get(&self) -> i64 {
        self.cells
            .iter()
            .map(|cell| cell.load(Ordering::Relaxed))
            .min()
            .unwrap_or(self.reset_value)
    }

    /// Reset all shards to the original value configured at construction.
    pub fn reset(&self) {
        for cell in &self.cells {
            cell.store(self.reset_value, Ordering::Relaxed);
        }
    }

    /// Reset all shards and return the previous minimum.
    pub fn swap_reset(&self) -> i64 {
        self.cells
            .iter()
            .map(|cell| cell.swap(self.reset_value, Ordering::Relaxed))
            .min()
            .unwrap_or(self.reset_value)
    }
}

impl Default for MinGauge {
    fn default() -> Self {
        Self::new(4)
    }
}

impl fmt::Debug for MinGauge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MinGauge")
            .field("min", &self.get())
            .field("cells", &self.cells.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_observe() {
        let gauge = MinGauge::new(4);
        gauge.observe(3);
        gauge.observe(-7);
        gauge.observe(5);
        assert_eq!(gauge.get(), -7);
    }

    #[test]
    fn initial_value_is_respected() {
        let gauge = MinGauge::with_value(4, 10);
        assert_eq!(gauge.get(), 10);
        gauge.observe(3);
        assert_eq!(gauge.get(), 3);
    }

    #[test]
    fn new_tracks_minimum_of_positive_observations() {
        let gauge = MinGauge::new(4);
        assert_eq!(gauge.get(), i64::MAX);
        gauge.observe(8);
        gauge.observe(3);
        gauge.observe(5);
        assert_eq!(gauge.get(), 3);
    }
}
