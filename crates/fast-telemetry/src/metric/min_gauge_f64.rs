//! Thread-sharded minimum gauge for `f64` values.

use crate::thread_id::thread_id;
use crossbeam_utils::CachePadded;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

const SIGN_MASK: u64 = 1u64 << 63;

#[inline]
fn encode_ordered_f64(value: f64) -> u64 {
    let bits = value.to_bits();
    if bits & SIGN_MASK != 0 {
        !bits
    } else {
        bits ^ SIGN_MASK
    }
}

#[inline]
fn decode_ordered_f64(encoded: u64) -> f64 {
    let bits = if encoded & SIGN_MASK != 0 {
        encoded ^ SIGN_MASK
    } else {
        !encoded
    };
    f64::from_bits(bits)
}

/// A thread-sharded minimum tracker for floating-point values.
///
/// `NaN` observations are ignored.
pub struct MinGaugeF64 {
    cells: Vec<CachePadded<AtomicU64>>,
    reset_value: u64,
}

impl MinGaugeF64 {
    /// Create a new min gauge with all shards initialized to [`f64::INFINITY`],
    /// so any (non-NaN) observation displaces the initial value.
    ///
    /// `get()` on a gauge that has never been observed returns [`f64::INFINITY`];
    /// callers that need a different sentinel should use [`Self::with_value`].
    pub fn new(shard_count: usize) -> Self {
        Self::with_value(shard_count, f64::INFINITY)
    }

    /// Create a new min gauge with all shards initialized to `initial`.
    pub fn with_value(shard_count: usize, initial: f64) -> Self {
        let shard_count = shard_count.next_power_of_two();
        let reset_value = encode_ordered_f64(initial);
        Self {
            cells: (0..shard_count)
                .map(|_| CachePadded::new(AtomicU64::new(reset_value)))
                .collect(),
            reset_value,
        }
    }

    /// Record a candidate value for the minimum.
    ///
    /// `NaN` is ignored.
    #[inline]
    pub fn observe(&self, value: f64) {
        if value.is_nan() {
            return;
        }
        let idx = thread_id() & (self.cells.len() - 1);
        let cell = if cfg!(debug_assertions) {
            self.cells.get(idx).expect("index out of bounds")
        } else {
            unsafe { self.cells.get_unchecked(idx) }
        };
        cell.fetch_min(encode_ordered_f64(value), Ordering::Relaxed);
    }

    /// Return the current minimum across all shards.
    #[inline]
    pub fn get(&self) -> f64 {
        decode_ordered_f64(
            self.cells
                .iter()
                .map(|cell| cell.load(Ordering::Relaxed))
                .min()
                .unwrap_or(self.reset_value),
        )
    }

    /// Reset all shards to the original value configured at construction.
    pub fn reset(&self) {
        for cell in &self.cells {
            cell.store(self.reset_value, Ordering::Relaxed);
        }
    }

    /// Reset all shards and return the previous minimum.
    pub fn swap_reset(&self) -> f64 {
        decode_ordered_f64(
            self.cells
                .iter()
                .map(|cell| cell.swap(self.reset_value, Ordering::Relaxed))
                .min()
                .unwrap_or(self.reset_value),
        )
    }
}

impl Default for MinGaugeF64 {
    fn default() -> Self {
        Self::new(4)
    }
}

impl fmt::Debug for MinGaugeF64 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MinGaugeF64")
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
        let gauge = MinGaugeF64::new(4);
        gauge.observe(3.5);
        gauge.observe(-7.25);
        gauge.observe(5.0);
        assert!((gauge.get() - (-7.25)).abs() < f64::EPSILON);
    }

    #[test]
    fn nan_is_ignored() {
        let gauge = MinGaugeF64::with_value(4, 1.0);
        gauge.observe(f64::NAN);
        assert!((gauge.get() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn new_tracks_minimum_of_positive_observations() {
        let gauge = MinGaugeF64::new(4);
        assert!(gauge.get().is_infinite() && gauge.get().is_sign_positive());
        gauge.observe(8.0);
        gauge.observe(3.25);
        gauge.observe(5.5);
        assert!((gauge.get() - 3.25).abs() < f64::EPSILON);
    }
}
