//! Atomic gauge for floating-point values.

use crossbeam_utils::CachePadded;
use std::sync::atomic::{AtomicU64, Ordering};

/// A cache-padded atomic gauge for floating-point point-in-time measurements.
///
/// Uses `AtomicU64` to store the bit representation of an `f64`, enabling
/// atomic operations on floating-point values.
///
/// # Usage
///
/// Use this for metrics that require decimal precision, such as:
/// - Memory usage in MB/GB
/// - CPU utilization percentages
/// - Ratios and rates
///
/// For integer values, prefer [`Gauge`] which uses `i64` directly.
///
/// [`Gauge`]: crate::Gauge
pub struct GaugeF64 {
    bits: CachePadded<AtomicU64>,
}

impl GaugeF64 {
    /// Create a new gauge initialized to zero.
    pub fn new() -> Self {
        Self {
            bits: CachePadded::new(AtomicU64::new(0.0_f64.to_bits())),
        }
    }

    /// Set the gauge to a value.
    #[inline]
    pub fn set(&self, value: f64) {
        self.bits.store(value.to_bits(), Ordering::Relaxed);
    }

    /// Get the current value.
    #[inline]
    pub fn get(&self) -> f64 {
        f64::from_bits(self.bits.load(Ordering::Relaxed))
    }
}

impl Default for GaugeF64 {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_operations() {
        let gauge = GaugeF64::new();
        assert_eq!(gauge.get(), 0.0);

        gauge.set(44331.8);
        assert!((gauge.get() - 44331.8).abs() < f64::EPSILON);

        gauge.set(-273.15);
        assert!((gauge.get() - (-273.15)).abs() < f64::EPSILON);
    }

    #[test]
    fn test_small_decimals() {
        let gauge = GaugeF64::new();
        gauge.set(0.00001);
        assert!((gauge.get() - 0.00001).abs() < f64::EPSILON);
    }
}
