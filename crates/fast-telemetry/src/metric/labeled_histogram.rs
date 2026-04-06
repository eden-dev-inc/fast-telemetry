//! Enum-indexed histogram for dimensional latency/distribution metrics.

use crate::Histogram;
use crate::label::LabelEnum;
use std::marker::PhantomData;

/// A histogram indexed by an enum label, providing O(1) dimensional metrics.
///
/// Each label variant has its own histogram with independent buckets.
///
/// # Example
///
/// ```ignore
/// use fast_telemetry::{LabeledHistogram, LabelEnum};
///
/// #[derive(Copy, Clone, Debug)]
/// enum Endpoint { Api, Auth, Static }
///
/// impl LabelEnum for Endpoint {
///     const CARDINALITY: usize = 3;
///     const LABEL_NAME: &'static str = "endpoint";
///
///     fn as_index(self) -> usize { self as usize }
///     fn from_index(index: usize) -> Self {
///         match index {
///             0 => Self::Api, 1 => Self::Auth, _ => Self::Static,
///         }
///     }
///     fn variant_name(self) -> &'static str {
///         match self {
///             Self::Api => "api", Self::Auth => "auth", Self::Static => "static",
///         }
///     }
/// }
///
/// let histogram: LabeledHistogram<Endpoint> = LabeledHistogram::with_latency_buckets(4);
/// histogram.record(Endpoint::Api, 150);    // 150µs
/// histogram.record(Endpoint::Auth, 2000);  // 2ms
///
/// // Iteration for export
/// for (label, buckets, sum, count) in histogram.iter() {
///     println!("{}={}: count={}", Endpoint::LABEL_NAME, label.variant_name(), count);
/// }
/// ```
pub struct LabeledHistogram<L: LabelEnum> {
    histograms: Vec<Histogram>,
    _phantom: PhantomData<L>,
}

impl<L: LabelEnum> LabeledHistogram<L> {
    /// Create a labeled histogram with custom bucket boundaries.
    ///
    /// Each label variant gets its own histogram with these boundaries.
    pub fn new(bounds: &[u64], shard_count: usize) -> Self {
        let histograms = (0..L::CARDINALITY)
            .map(|_| Histogram::new(bounds, shard_count))
            .collect();
        Self {
            histograms,
            _phantom: PhantomData,
        }
    }

    /// Create a labeled histogram with default latency buckets (microseconds).
    ///
    /// Buckets: 10µs, 50µs, 100µs, 500µs, 1ms, 5ms, 10ms, 50ms, 100ms, 500ms, 1s, 5s, 10s
    pub fn with_latency_buckets(shard_count: usize) -> Self {
        let histograms = (0..L::CARDINALITY)
            .map(|_| Histogram::with_latency_buckets(shard_count))
            .collect();
        Self {
            histograms,
            _phantom: PhantomData,
        }
    }

    /// Record a value in the histogram for the given label.
    #[inline]
    pub fn record(&self, label: L, value: u64) {
        let idx = label.as_index();
        debug_assert!(idx < self.histograms.len(), "label index out of bounds");
        if cfg!(debug_assertions) {
            self.histograms[idx].record(value);
        } else {
            unsafe { self.histograms.get_unchecked(idx) }.record(value);
        }
    }

    /// Get the histogram for a specific label (for detailed inspection).
    pub fn get(&self, label: L) -> &Histogram {
        let idx = label.as_index();
        if cfg!(debug_assertions) {
            &self.histograms[idx]
        } else {
            unsafe { self.histograms.get_unchecked(idx) }
        }
    }

    /// Iterate over all label/histogram data for export.
    ///
    /// Returns (label, cumulative_buckets, sum, count) for each variant.
    pub fn iter(&self) -> impl Iterator<Item = (L, Vec<(u64, u64)>, u64, u64)> + '_ {
        self.histograms.iter().enumerate().map(|(idx, histogram)| {
            (
                L::from_index(idx),
                histogram.buckets_cumulative(),
                histogram.sum(),
                histogram.count(),
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Copy, Clone, Debug, PartialEq)]
    enum TestLabel {
        Fast,
        Medium,
        Slow,
    }

    impl LabelEnum for TestLabel {
        const CARDINALITY: usize = 3;
        const LABEL_NAME: &'static str = "speed";

        fn as_index(self) -> usize {
            self as usize
        }

        fn from_index(index: usize) -> Self {
            match index {
                0 => Self::Fast,
                1 => Self::Medium,
                _ => Self::Slow,
            }
        }

        fn variant_name(self) -> &'static str {
            match self {
                Self::Fast => "fast",
                Self::Medium => "medium",
                Self::Slow => "slow",
            }
        }
    }

    #[test]
    fn test_basic_recording() {
        let histogram: LabeledHistogram<TestLabel> = LabeledHistogram::new(&[10, 100, 1000], 4);

        histogram.record(TestLabel::Fast, 5);
        histogram.record(TestLabel::Fast, 8);
        histogram.record(TestLabel::Medium, 50);
        histogram.record(TestLabel::Slow, 5000);

        assert_eq!(histogram.get(TestLabel::Fast).count(), 2);
        assert_eq!(histogram.get(TestLabel::Medium).count(), 1);
        assert_eq!(histogram.get(TestLabel::Slow).count(), 1);
    }

    #[test]
    fn test_iteration() {
        let histogram: LabeledHistogram<TestLabel> = LabeledHistogram::new(&[100], 4);

        histogram.record(TestLabel::Fast, 50);
        histogram.record(TestLabel::Medium, 150);

        let data: Vec<_> = histogram.iter().collect();
        assert_eq!(data.len(), 3);

        // Fast: 1 value in bucket 0
        assert_eq!(data[0].0, TestLabel::Fast);
        assert_eq!(data[0].3, 1); // count

        // Medium: 1 value in +Inf bucket
        assert_eq!(data[1].0, TestLabel::Medium);
        assert_eq!(data[1].3, 1); // count

        // Slow: no values
        assert_eq!(data[2].0, TestLabel::Slow);
        assert_eq!(data[2].3, 0); // count
    }

    #[test]
    fn test_latency_buckets() {
        let histogram: LabeledHistogram<TestLabel> = LabeledHistogram::with_latency_buckets(4);

        histogram.record(TestLabel::Fast, 5); // 5µs
        histogram.record(TestLabel::Fast, 100); // 100µs
        histogram.record(TestLabel::Medium, 1_000); // 1ms

        assert_eq!(histogram.get(TestLabel::Fast).count(), 2);
        assert_eq!(histogram.get(TestLabel::Medium).count(), 1);
    }

    #[test]
    fn test_concurrent_recording() {
        let histogram: LabeledHistogram<TestLabel> = LabeledHistogram::new(&[100], 4);

        std::thread::scope(|s| {
            for _ in 0..4 {
                s.spawn(|| {
                    for i in 0..1000 {
                        histogram.record(TestLabel::Fast, i);
                    }
                });
            }
        });

        assert_eq!(histogram.get(TestLabel::Fast).count(), 4000);
    }
}
