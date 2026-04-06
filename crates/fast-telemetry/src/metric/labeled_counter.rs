//! Enum-indexed counter for dimensional metrics with O(1) lookup.

use crate::Counter;
use crate::label::LabelEnum;
use std::marker::PhantomData;

/// A counter indexed by an enum label, providing O(1) dimensional metrics.
///
/// Instead of using a HashMap to look up counters by label values, this type
/// uses the enum variant's index to directly access an array of counters.
/// This eliminates hashing overhead on the hot path.
///
/// # Example
///
/// ```ignore
/// use fast_telemetry::{LabeledCounter, LabelEnum};
///
/// #[derive(Copy, Clone, Debug)]
/// enum HttpMethod { Get, Post, Put, Delete, Other }
///
/// impl LabelEnum for HttpMethod {
///     const CARDINALITY: usize = 5;
///     const LABEL_NAME: &'static str = "method";
///
///     fn as_index(self) -> usize { self as usize }
///     fn from_index(index: usize) -> Self {
///         match index {
///             0 => Self::Get, 1 => Self::Post, 2 => Self::Put,
///             3 => Self::Delete, _ => Self::Other,
///         }
///     }
///     fn variant_name(self) -> &'static str {
///         match self {
///             Self::Get => "get", Self::Post => "post", Self::Put => "put",
///             Self::Delete => "delete", Self::Other => "other",
///         }
///     }
/// }
///
/// let counter: LabeledCounter<HttpMethod> = LabeledCounter::new(4);
/// counter.inc(HttpMethod::Get);
/// counter.add(HttpMethod::Post, 5);
///
/// // Iteration for export
/// for (label, count) in counter.iter() {
///     println!("{}={}: {}", HttpMethod::LABEL_NAME, label.variant_name(), count);
/// }
/// ```
pub struct LabeledCounter<L: LabelEnum> {
    counters: Vec<Counter>,
    _phantom: PhantomData<L>,
}

impl<L: LabelEnum> LabeledCounter<L> {
    /// Create a new labeled counter.
    ///
    /// `shard_count` is passed to each underlying `Counter` for thread-sharding.
    pub fn new(shard_count: usize) -> Self {
        let counters = (0..L::CARDINALITY)
            .map(|_| Counter::new(shard_count))
            .collect();
        Self {
            counters,
            _phantom: PhantomData,
        }
    }

    /// Increment the counter for the given label by 1.
    #[inline]
    pub fn inc(&self, label: L) {
        self.add(label, 1);
    }

    /// Add a value to the counter for the given label.
    #[inline]
    pub fn add(&self, label: L, value: isize) {
        let idx = label.as_index();
        debug_assert!(idx < self.counters.len(), "label index out of bounds");
        // SAFETY: LabelEnum::CARDINALITY guarantees idx < counters.len()
        if cfg!(debug_assertions) {
            self.counters[idx].add(value);
        } else {
            unsafe { self.counters.get_unchecked(idx) }.add(value);
        }
    }

    /// Get the current count for a specific label.
    #[inline]
    pub fn get(&self, label: L) -> isize {
        let idx = label.as_index();
        if cfg!(debug_assertions) {
            self.counters[idx].sum()
        } else {
            unsafe { self.counters.get_unchecked(idx) }.sum()
        }
    }

    /// Get the sum across all labels.
    pub fn sum_all(&self) -> isize {
        self.counters.iter().map(|c| c.sum()).sum()
    }

    /// Iterate over all label/count pairs.
    ///
    /// Used for Prometheus export.
    pub fn iter(&self) -> impl Iterator<Item = (L, isize)> + '_ {
        self.counters
            .iter()
            .enumerate()
            .map(|(idx, counter)| (L::from_index(idx), counter.sum()))
    }

    /// Swap all counters and return label/count pairs.
    ///
    /// Useful for delta-style metrics export.
    pub fn swap_all(&self) -> impl Iterator<Item = (L, isize)> + '_ {
        self.counters
            .iter()
            .enumerate()
            .map(|(idx, counter)| (L::from_index(idx), counter.swap()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Copy, Clone, Debug, PartialEq)]
    enum TestLabel {
        A,
        B,
        C,
    }

    impl LabelEnum for TestLabel {
        const CARDINALITY: usize = 3;
        const LABEL_NAME: &'static str = "test";

        fn as_index(self) -> usize {
            self as usize
        }

        fn from_index(index: usize) -> Self {
            match index {
                0 => Self::A,
                1 => Self::B,
                _ => Self::C,
            }
        }

        fn variant_name(self) -> &'static str {
            match self {
                Self::A => "a",
                Self::B => "b",
                Self::C => "c",
            }
        }
    }

    #[test]
    fn test_basic_operations() {
        let counter: LabeledCounter<TestLabel> = LabeledCounter::new(4);

        counter.inc(TestLabel::A);
        counter.add(TestLabel::B, 5);
        counter.inc(TestLabel::A);

        assert_eq!(counter.get(TestLabel::A), 2);
        assert_eq!(counter.get(TestLabel::B), 5);
        assert_eq!(counter.get(TestLabel::C), 0);
    }

    #[test]
    fn test_sum_all() {
        let counter: LabeledCounter<TestLabel> = LabeledCounter::new(4);

        counter.add(TestLabel::A, 10);
        counter.add(TestLabel::B, 20);
        counter.add(TestLabel::C, 30);

        assert_eq!(counter.sum_all(), 60);
    }

    #[test]
    fn test_iteration() {
        let counter: LabeledCounter<TestLabel> = LabeledCounter::new(4);

        counter.add(TestLabel::A, 1);
        counter.add(TestLabel::B, 2);
        counter.add(TestLabel::C, 3);

        let pairs: Vec<_> = counter.iter().collect();
        assert_eq!(pairs.len(), 3);
        assert_eq!(pairs[0], (TestLabel::A, 1));
        assert_eq!(pairs[1], (TestLabel::B, 2));
        assert_eq!(pairs[2], (TestLabel::C, 3));
    }

    #[test]
    fn test_swap_all() {
        let counter: LabeledCounter<TestLabel> = LabeledCounter::new(4);

        counter.add(TestLabel::A, 10);
        counter.add(TestLabel::B, 20);

        let swapped: Vec<_> = counter.swap_all().collect();
        assert_eq!(swapped[0], (TestLabel::A, 10));
        assert_eq!(swapped[1], (TestLabel::B, 20));

        // After swap, counters should be zero
        assert_eq!(counter.get(TestLabel::A), 0);
        assert_eq!(counter.get(TestLabel::B), 0);
    }

    #[test]
    fn test_concurrent_access() {
        let counter: LabeledCounter<TestLabel> = LabeledCounter::new(4);

        std::thread::scope(|s| {
            for _ in 0..4 {
                s.spawn(|| {
                    for _ in 0..1000 {
                        counter.inc(TestLabel::A);
                        counter.inc(TestLabel::B);
                    }
                });
            }
        });

        assert_eq!(counter.get(TestLabel::A), 4000);
        assert_eq!(counter.get(TestLabel::B), 4000);
        assert_eq!(counter.get(TestLabel::C), 0);
    }
}
