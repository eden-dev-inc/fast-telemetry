//! Sampled elapsed-time recording for hot paths.
//!
//! `SampledTimer` counts every call but only records elapsed time for one call
//! per configured stride. This keeps hot-path instrumentation cheap while still
//! giving useful phase latency samples.

use std::cell::Cell;
use std::marker::PhantomData;
use std::time::{Duration, Instant};

use crate::{Counter, Histogram, LabelEnum};

thread_local! {
    static SAMPLE_SEQ: Cell<u64> = const { Cell::new(0) };
}

/// Counts every timed operation and samples elapsed latency into a histogram.
///
/// Durations are recorded in nanoseconds.
pub struct SampledTimer {
    inner: SampledTimerInner,
}

/// Label-indexed sampled timer with O(1) lookup.
pub struct LabeledSampledTimer<L: LabelEnum> {
    timers: Vec<SampledTimerInner>,
    _phantom: PhantomData<L>,
}

/// RAII guard returned by [`SampledTimer::start`] and [`LabeledSampledTimer::start`].
///
/// Dropping the guard records elapsed time when the operation was selected for
/// sampling. Call [`finish`](Self::finish) when an explicit endpoint is clearer.
pub struct SampledTimerGuard<'a> {
    timer: &'a SampledTimerInner,
    start: Option<Instant>,
    finished: bool,
}

struct SampledTimerInner {
    calls: Counter,
    samples: Histogram,
    stride_mask: u64,
}

impl SampledTimer {
    /// Create a sampled timer with custom histogram bounds in nanoseconds.
    ///
    /// `sample_stride` is rounded up to a power of two. A stride of `1` records
    /// every call.
    pub fn new(bounds_nanos: &[u64], shard_count: usize, sample_stride: u64) -> Self {
        Self {
            inner: SampledTimerInner::new(bounds_nanos, shard_count, sample_stride),
        }
    }

    /// Create a sampled timer with default latency buckets in nanoseconds.
    pub fn with_latency_buckets(shard_count: usize, sample_stride: u64) -> Self {
        Self {
            inner: SampledTimerInner::with_latency_buckets(shard_count, sample_stride),
        }
    }

    /// Start timing one operation.
    #[inline]
    pub fn start(&self) -> SampledTimerGuard<'_> {
        self.inner.start()
    }

    /// Record an already-measured duration if this call should be sampled.
    #[inline]
    pub fn record_elapsed(&self, elapsed: Duration) {
        self.inner.record_elapsed(elapsed);
    }

    /// Total operation calls, sampled or not.
    #[inline]
    pub fn calls(&self) -> u64 {
        self.inner.calls()
    }

    /// Number of latency samples recorded.
    #[inline]
    pub fn sample_count(&self) -> u64 {
        self.inner.sample_count()
    }

    /// Sum of sampled latency values in nanoseconds.
    #[inline]
    pub fn sample_sum_nanos(&self) -> u64 {
        self.inner.sample_sum_nanos()
    }

    /// Average sampled latency in nanoseconds.
    pub fn avg_sample_nanos(&self) -> Option<f64> {
        self.inner.avg_sample_nanos()
    }

    /// Access the underlying call counter for export.
    #[inline]
    pub fn calls_metric(&self) -> &Counter {
        &self.inner.calls
    }

    /// Access the underlying sampled histogram for export.
    #[inline]
    pub fn histogram(&self) -> &Histogram {
        &self.inner.samples
    }
}

impl<L: LabelEnum> LabeledSampledTimer<L> {
    /// Create a labeled sampled timer with custom histogram bounds in nanoseconds.
    pub fn new(bounds_nanos: &[u64], shard_count: usize, sample_stride: u64) -> Self {
        let timers = (0..L::CARDINALITY)
            .map(|_| SampledTimerInner::new(bounds_nanos, shard_count, sample_stride))
            .collect();
        Self {
            timers,
            _phantom: PhantomData,
        }
    }

    /// Create a labeled sampled timer with default latency buckets in nanoseconds.
    pub fn with_latency_buckets(shard_count: usize, sample_stride: u64) -> Self {
        let timers = (0..L::CARDINALITY)
            .map(|_| SampledTimerInner::with_latency_buckets(shard_count, sample_stride))
            .collect();
        Self {
            timers,
            _phantom: PhantomData,
        }
    }

    /// Start timing one labeled operation.
    #[inline]
    pub fn start(&self, label: L) -> SampledTimerGuard<'_> {
        self.timer(label).start()
    }

    /// Record an already-measured duration for the given label if it should be sampled.
    #[inline]
    pub fn record_elapsed(&self, label: L, elapsed: Duration) {
        self.timer(label).record_elapsed(elapsed);
    }

    /// Total calls for a label, sampled or not.
    #[inline]
    pub fn calls(&self, label: L) -> u64 {
        self.timer(label).calls()
    }

    /// Number of latency samples recorded for a label.
    #[inline]
    pub fn sample_count(&self, label: L) -> u64 {
        self.timer(label).sample_count()
    }

    /// Sum of sampled latency values in nanoseconds for a label.
    #[inline]
    pub fn sample_sum_nanos(&self, label: L) -> u64 {
        self.timer(label).sample_sum_nanos()
    }

    /// Average sampled latency in nanoseconds for a label.
    pub fn avg_sample_nanos(&self, label: L) -> Option<f64> {
        self.timer(label).avg_sample_nanos()
    }

    /// Access the underlying call counter for a label.
    #[inline]
    pub fn calls_metric(&self, label: L) -> &Counter {
        &self.timer(label).calls
    }

    /// Access the underlying sampled histogram for a label.
    #[inline]
    pub fn histogram(&self, label: L) -> &Histogram {
        &self.timer(label).samples
    }

    /// Iterate over all label/timer pairs for export.
    pub fn iter(&self) -> impl Iterator<Item = (L, &Counter, &Histogram)> + '_ {
        self.timers
            .iter()
            .enumerate()
            .map(|(idx, timer)| (L::from_index(idx), &timer.calls, &timer.samples))
    }

    #[inline]
    fn timer(&self, label: L) -> &SampledTimerInner {
        let idx = label.as_index();
        debug_assert!(idx < self.timers.len(), "label index out of bounds");
        if cfg!(debug_assertions) {
            &self.timers[idx]
        } else {
            unsafe { self.timers.get_unchecked(idx) }
        }
    }
}

impl SampledTimerGuard<'_> {
    /// Finish timing now.
    #[inline]
    pub fn finish(mut self) {
        self.record();
        self.finished = true;
    }

    #[inline]
    fn record(&mut self) {
        let Some(start) = self.start.take() else {
            return;
        };
        self.timer.samples.record(duration_nanos(start.elapsed()));
    }
}

impl Drop for SampledTimerGuard<'_> {
    #[inline]
    fn drop(&mut self) {
        if !self.finished {
            self.record();
        }
    }
}

impl SampledTimerInner {
    fn new(bounds_nanos: &[u64], shard_count: usize, sample_stride: u64) -> Self {
        Self {
            calls: Counter::new(shard_count),
            samples: Histogram::new(bounds_nanos, shard_count),
            stride_mask: stride_mask(sample_stride),
        }
    }

    fn with_latency_buckets(shard_count: usize, sample_stride: u64) -> Self {
        Self::new(
            &[
                10_000,         // 10µs
                50_000,         // 50µs
                100_000,        // 100µs
                500_000,        // 500µs
                1_000_000,      // 1ms
                5_000_000,      // 5ms
                10_000_000,     // 10ms
                50_000_000,     // 50ms
                100_000_000,    // 100ms
                500_000_000,    // 500ms
                1_000_000_000,  // 1s
                5_000_000_000,  // 5s
                10_000_000_000, // 10s
            ],
            shard_count,
            sample_stride,
        )
    }

    #[inline]
    fn start(&self) -> SampledTimerGuard<'_> {
        self.calls.inc();
        let sampled = should_sample(self.stride_mask);
        SampledTimerGuard {
            timer: self,
            start: sampled.then(Instant::now),
            finished: false,
        }
    }

    #[inline]
    fn record_elapsed(&self, elapsed: Duration) {
        self.calls.inc();
        if should_sample(self.stride_mask) {
            self.samples.record(duration_nanos(elapsed));
        }
    }

    #[inline]
    fn calls(&self) -> u64 {
        self.calls.sum() as u64
    }

    #[inline]
    fn sample_count(&self) -> u64 {
        self.samples.count()
    }

    #[inline]
    fn sample_sum_nanos(&self) -> u64 {
        self.samples.sum()
    }

    fn avg_sample_nanos(&self) -> Option<f64> {
        let count = self.sample_count();
        if count == 0 {
            return None;
        }
        Some(self.sample_sum_nanos() as f64 / count as f64)
    }
}

fn should_sample(stride_mask: u64) -> bool {
    SAMPLE_SEQ.with(|seq| {
        let next = seq.get().wrapping_add(1);
        seq.set(next);
        next & stride_mask == 0
    })
}

fn stride_mask(sample_stride: u64) -> u64 {
    sample_stride.max(1).next_power_of_two() - 1
}

fn duration_nanos(elapsed: Duration) -> u64 {
    elapsed.as_nanos().min(u64::MAX as u128) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Copy, Clone, Debug, PartialEq)]
    enum TestLabel {
        A,
        B,
    }

    impl LabelEnum for TestLabel {
        const CARDINALITY: usize = 2;
        const LABEL_NAME: &'static str = "label";

        fn as_index(self) -> usize {
            self as usize
        }

        fn from_index(index: usize) -> Self {
            match index {
                0 => Self::A,
                _ => Self::B,
            }
        }

        fn variant_name(self) -> &'static str {
            match self {
                Self::A => "a",
                Self::B => "b",
            }
        }
    }

    #[test]
    fn stride_one_records_every_call() {
        let timer = SampledTimer::with_latency_buckets(4, 1);

        timer.record_elapsed(Duration::from_nanos(10));
        timer.record_elapsed(Duration::from_nanos(20));

        assert_eq!(timer.calls(), 2);
        assert_eq!(timer.sample_count(), 2);
        assert_eq!(timer.sample_sum_nanos(), 30);
        assert_eq!(timer.avg_sample_nanos(), Some(15.0));
    }

    #[test]
    fn stride_samples_subset() {
        let timer = SampledTimer::with_latency_buckets(4, 4);

        for _ in 0..8 {
            timer.record_elapsed(Duration::from_nanos(10));
        }

        assert_eq!(timer.calls(), 8);
        assert_eq!(timer.sample_count(), 2);
    }

    #[test]
    fn guard_records_on_drop() {
        let timer = SampledTimer::with_latency_buckets(4, 1);

        {
            let _guard = timer.start();
        }

        assert_eq!(timer.calls(), 1);
        assert_eq!(timer.sample_count(), 1);
    }

    #[test]
    fn explicit_finish_records_once() {
        let timer = SampledTimer::with_latency_buckets(4, 1);

        timer.start().finish();

        assert_eq!(timer.calls(), 1);
        assert_eq!(timer.sample_count(), 1);
    }

    #[test]
    fn labeled_timer_tracks_labels_independently() {
        let timer: LabeledSampledTimer<TestLabel> = LabeledSampledTimer::with_latency_buckets(4, 1);

        timer.record_elapsed(TestLabel::A, Duration::from_nanos(15));
        timer.record_elapsed(TestLabel::B, Duration::from_nanos(25));
        timer.record_elapsed(TestLabel::A, Duration::from_nanos(35));

        assert_eq!(timer.calls(TestLabel::A), 2);
        assert_eq!(timer.calls(TestLabel::B), 1);
        assert_eq!(timer.sample_count(TestLabel::A), 2);
        assert_eq!(timer.sample_sum_nanos(TestLabel::A), 50);
        assert_eq!(timer.sample_sum_nanos(TestLabel::B), 25);
    }
}
