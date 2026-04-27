//! Thread-sharded maximum gauge for `f64` values.
//!
//! Values are encoded into a sortable `u64` representation so the hot path can
//! use atomic min/max operations while preserving numeric ordering, including
//! for negative values.

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

/// A thread-sharded maximum tracker for floating-point values.
///
/// `NaN` observations are ignored.
pub struct MaxGaugeF64 {
    cells: Vec<CachePadded<AtomicU64>>,
    reset_value: u64,
}

impl MaxGaugeF64 {
    /// Create a new max gauge with all shards initialized to zero.
    pub fn new(shard_count: usize) -> Self {
        Self::with_value(shard_count, 0.0)
    }

    /// Create a new max gauge with all shards initialized to `initial`.
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

    /// Record a candidate value for the maximum.
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
        cell.fetch_max(encode_ordered_f64(value), Ordering::Relaxed);
    }

    /// Return the current maximum across all shards.
    #[inline]
    pub fn get(&self) -> f64 {
        decode_ordered_f64(
            self.cells
                .iter()
                .map(|cell| cell.load(Ordering::Relaxed))
                .max()
                .unwrap_or(self.reset_value),
        )
    }

    /// Reset all shards to the original value configured at construction.
    pub fn reset(&self) {
        for cell in &self.cells {
            cell.store(self.reset_value, Ordering::Relaxed);
        }
    }

    /// Reset all shards and return the previous maximum.
    pub fn swap_reset(&self) -> f64 {
        decode_ordered_f64(
            self.cells
                .iter()
                .map(|cell| cell.swap(self.reset_value, Ordering::Relaxed))
                .max()
                .unwrap_or(self.reset_value),
        )
    }
}

impl Default for MaxGaugeF64 {
    fn default() -> Self {
        Self::new(4)
    }
}

impl fmt::Debug for MaxGaugeF64 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MaxGaugeF64")
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
        let gauge = MaxGaugeF64::new(4);
        gauge.observe(3.5);
        gauge.observe(7.25);
        gauge.observe(5.0);
        assert!((gauge.get() - 7.25).abs() < f64::EPSILON);
    }

    #[test]
    fn negative_values_order_correctly() {
        let gauge = MaxGaugeF64::with_value(4, -10.0);
        gauge.observe(-3.25);
        gauge.observe(-8.0);
        assert!((gauge.get() - (-3.25)).abs() < f64::EPSILON);
    }

    #[test]
    fn nan_is_ignored() {
        let gauge = MaxGaugeF64::with_value(4, 1.0);
        gauge.observe(f64::NAN);
        assert!((gauge.get() - 1.0).abs() < f64::EPSILON);
    }
}
