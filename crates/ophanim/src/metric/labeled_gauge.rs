//! Enum-indexed gauge for dimensional point-in-time metrics.

use crate::Gauge;
use crate::label::LabelEnum;
use std::marker::PhantomData;

/// A gauge indexed by an enum label, providing O(1) dimensional metrics.
///
/// Each label variant has its own gauge for independent point-in-time values.
///
/// # Example
///
/// ```ignore
/// use ophanim::{LabeledGauge, LabelEnum};
///
/// #[derive(Copy, Clone, Debug)]
/// enum CacheType { Memory, Disk, Network }
///
/// impl LabelEnum for CacheType {
///     const CARDINALITY: usize = 3;
///     const LABEL_NAME: &'static str = "cache_type";
///
///     fn as_index(self) -> usize { self as usize }
///     fn from_index(index: usize) -> Self {
///         match index {
///             0 => Self::Memory, 1 => Self::Disk, _ => Self::Network,
///         }
///     }
///     fn variant_name(self) -> &'static str {
///         match self {
///             Self::Memory => "memory", Self::Disk => "disk", Self::Network => "network",
///         }
///     }
/// }
///
/// let gauge: LabeledGauge<CacheType> = LabeledGauge::new();
/// gauge.set(CacheType::Memory, 1024);
/// gauge.set(CacheType::Disk, 4096);
///
/// // Iteration for export
/// for (label, value) in gauge.iter() {
///     println!("{}={}: {}", CacheType::LABEL_NAME, label.variant_name(), value);
/// }
/// ```
pub struct LabeledGauge<L: LabelEnum> {
    gauges: Vec<Gauge>,
    _phantom: PhantomData<L>,
}

impl<L: LabelEnum> LabeledGauge<L> {
    /// Create a new labeled gauge with all values initialized to zero.
    pub fn new() -> Self {
        let gauges = (0..L::CARDINALITY).map(|_| Gauge::new()).collect();
        Self {
            gauges,
            _phantom: PhantomData,
        }
    }

    /// Set the gauge value for the given label.
    #[inline]
    pub fn set(&self, label: L, value: i64) {
        let idx = label.as_index();
        debug_assert!(idx < self.gauges.len(), "label index out of bounds");
        if cfg!(debug_assertions) {
            self.gauges[idx].set(value);
        } else {
            unsafe { self.gauges.get_unchecked(idx) }.set(value);
        }
    }

    /// Get the current gauge value for a specific label.
    #[inline]
    pub fn get(&self, label: L) -> i64 {
        let idx = label.as_index();
        if cfg!(debug_assertions) {
            self.gauges[idx].get()
        } else {
            unsafe { self.gauges.get_unchecked(idx) }.get()
        }
    }

    /// Iterate over all label/value pairs.
    ///
    /// Used for Prometheus export.
    pub fn iter(&self) -> impl Iterator<Item = (L, i64)> + '_ {
        self.gauges
            .iter()
            .enumerate()
            .map(|(idx, gauge)| (L::from_index(idx), gauge.get()))
    }
}

impl<L: LabelEnum> Default for LabeledGauge<L> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Copy, Clone, Debug, PartialEq)]
    enum TestLabel {
        X,
        Y,
        Z,
    }

    impl LabelEnum for TestLabel {
        const CARDINALITY: usize = 3;
        const LABEL_NAME: &'static str = "test";

        fn as_index(self) -> usize {
            self as usize
        }

        fn from_index(index: usize) -> Self {
            match index {
                0 => Self::X,
                1 => Self::Y,
                _ => Self::Z,
            }
        }

        fn variant_name(self) -> &'static str {
            match self {
                Self::X => "x",
                Self::Y => "y",
                Self::Z => "z",
            }
        }
    }

    #[test]
    fn test_basic_operations() {
        let gauge: LabeledGauge<TestLabel> = LabeledGauge::new();

        gauge.set(TestLabel::X, 100);
        gauge.set(TestLabel::Y, 200);

        assert_eq!(gauge.get(TestLabel::X), 100);
        assert_eq!(gauge.get(TestLabel::Y), 200);
        assert_eq!(gauge.get(TestLabel::Z), 0);
    }

    #[test]
    fn test_overwrite() {
        let gauge: LabeledGauge<TestLabel> = LabeledGauge::new();

        gauge.set(TestLabel::X, 100);
        gauge.set(TestLabel::X, 50);

        assert_eq!(gauge.get(TestLabel::X), 50);
    }

    #[test]
    fn test_iteration() {
        let gauge: LabeledGauge<TestLabel> = LabeledGauge::new();

        gauge.set(TestLabel::X, 1);
        gauge.set(TestLabel::Y, 2);
        gauge.set(TestLabel::Z, 3);

        let pairs: Vec<_> = gauge.iter().collect();
        assert_eq!(pairs.len(), 3);
        assert_eq!(pairs[0], (TestLabel::X, 1));
        assert_eq!(pairs[1], (TestLabel::Y, 2));
        assert_eq!(pairs[2], (TestLabel::Z, 3));
    }
}
