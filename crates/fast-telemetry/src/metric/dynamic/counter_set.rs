//! Runtime-labeled grouped counters for dynamic dimensions.

use super::cache::{CacheableSeries, LabelCache, SERIES_CACHE_SIZE};
#[cfg(feature = "eviction")]
use super::current_cycle;
use super::{COUNTER_IDS, DynamicIndexMap, DynamicLabelSet, dynamic_index_map, thread_id};
use crossbeam_utils::CachePadded;
use parking_lot::RwLock;
use std::cell::RefCell;
use std::collections::BTreeMap;
#[cfg(feature = "eviction")]
use std::sync::atomic::AtomicU32;
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

const DEFAULT_MAX_SERIES: usize = 2000;
const OVERFLOW_LABEL_KEY: &str = "__ft_overflow";
const OVERFLOW_LABEL_VALUE: &str = "true";

struct CounterSetSeries {
    cells: Vec<AtomicIsize>,
    counters: usize,
    stride: usize,
    evicted: AtomicBool,
    #[cfg(feature = "eviction")]
    last_accessed_cycle: AtomicU32,
}

type CounterSetIndexShard = CachePadded<RwLock<DynamicIndexMap<Arc<CounterSetSeries>>>>;

impl CounterSetSeries {
    #[cfg(feature = "eviction")]
    fn new(shards: usize, counters: usize, current_cycle: u32) -> Self {
        let stride = counter_row_stride(counters);
        Self {
            cells: (0..(shards * stride))
                .map(|_| AtomicIsize::new(0))
                .collect(),
            counters,
            stride,
            evicted: AtomicBool::new(false),
            last_accessed_cycle: AtomicU32::new(current_cycle),
        }
    }

    #[cfg(not(feature = "eviction"))]
    fn new(shards: usize, counters: usize) -> Self {
        let stride = counter_row_stride(counters);
        Self {
            cells: (0..(shards * stride))
                .map(|_| AtomicIsize::new(0))
                .collect(),
            counters,
            stride,
            evicted: AtomicBool::new(false),
        }
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

    #[inline]
    fn row_offset(&self, shard_idx: usize) -> usize {
        shard_idx * self.stride
    }

    #[inline]
    fn add_at(&self, shard_idx: usize, counter_idx: usize, value: isize) {
        assert!(counter_idx < self.counters, "counter index out of bounds");
        let offset = self.row_offset(shard_idx);
        self.cell_at(offset + counter_idx)
            .fetch_add(value, Ordering::Relaxed);
    }

    #[inline]
    fn add_index_values_at(&self, shard_idx: usize, updates: &[(usize, isize)]) {
        let offset = self.row_offset(shard_idx);
        for (counter_idx, value) in updates {
            assert!(*counter_idx < self.counters, "counter index out of bounds");
            self.cell_at(offset + *counter_idx)
                .fetch_add(*value, Ordering::Relaxed);
        }
    }

    #[inline(always)]
    fn add_values_at(&self, shard_idx: usize, values: &[isize]) {
        assert_eq!(values.len(), self.counters, "values must match counters");
        let offset = self.row_offset(shard_idx);
        match values {
            [a] => {
                self.cell_at(offset).fetch_add(*a, Ordering::Relaxed);
            }
            [a, b] => {
                self.cell_at(offset).fetch_add(*a, Ordering::Relaxed);
                self.cell_at(offset + 1).fetch_add(*b, Ordering::Relaxed);
            }
            [a, b, c] => {
                self.cell_at(offset).fetch_add(*a, Ordering::Relaxed);
                self.cell_at(offset + 1).fetch_add(*b, Ordering::Relaxed);
                self.cell_at(offset + 2).fetch_add(*c, Ordering::Relaxed);
            }
            [a, b, c, d] => {
                self.cell_at(offset).fetch_add(*a, Ordering::Relaxed);
                self.cell_at(offset + 1).fetch_add(*b, Ordering::Relaxed);
                self.cell_at(offset + 2).fetch_add(*c, Ordering::Relaxed);
                self.cell_at(offset + 3).fetch_add(*d, Ordering::Relaxed);
            }
            [a, b, c, d, e] => {
                self.cell_at(offset).fetch_add(*a, Ordering::Relaxed);
                self.cell_at(offset + 1).fetch_add(*b, Ordering::Relaxed);
                self.cell_at(offset + 2).fetch_add(*c, Ordering::Relaxed);
                self.cell_at(offset + 3).fetch_add(*d, Ordering::Relaxed);
                self.cell_at(offset + 4).fetch_add(*e, Ordering::Relaxed);
            }
            [a, b, c, d, e, f] => {
                self.cell_at(offset).fetch_add(*a, Ordering::Relaxed);
                self.cell_at(offset + 1).fetch_add(*b, Ordering::Relaxed);
                self.cell_at(offset + 2).fetch_add(*c, Ordering::Relaxed);
                self.cell_at(offset + 3).fetch_add(*d, Ordering::Relaxed);
                self.cell_at(offset + 4).fetch_add(*e, Ordering::Relaxed);
                self.cell_at(offset + 5).fetch_add(*f, Ordering::Relaxed);
            }
            [a, b, c, d, e, f, g] => {
                self.cell_at(offset).fetch_add(*a, Ordering::Relaxed);
                self.cell_at(offset + 1).fetch_add(*b, Ordering::Relaxed);
                self.cell_at(offset + 2).fetch_add(*c, Ordering::Relaxed);
                self.cell_at(offset + 3).fetch_add(*d, Ordering::Relaxed);
                self.cell_at(offset + 4).fetch_add(*e, Ordering::Relaxed);
                self.cell_at(offset + 5).fetch_add(*f, Ordering::Relaxed);
                self.cell_at(offset + 6).fetch_add(*g, Ordering::Relaxed);
            }
            [a, b, c, d, e, f, g, h] => {
                self.cell_at(offset).fetch_add(*a, Ordering::Relaxed);
                self.cell_at(offset + 1).fetch_add(*b, Ordering::Relaxed);
                self.cell_at(offset + 2).fetch_add(*c, Ordering::Relaxed);
                self.cell_at(offset + 3).fetch_add(*d, Ordering::Relaxed);
                self.cell_at(offset + 4).fetch_add(*e, Ordering::Relaxed);
                self.cell_at(offset + 5).fetch_add(*f, Ordering::Relaxed);
                self.cell_at(offset + 6).fetch_add(*g, Ordering::Relaxed);
                self.cell_at(offset + 7).fetch_add(*h, Ordering::Relaxed);
            }
            values => {
                let row = if cfg!(debug_assertions) {
                    self.cells
                        .get(offset..offset + self.counters)
                        .expect("row index out of bounds")
                } else {
                    // SAFETY: row_offset derives from a shard index masked by
                    // the owning metric, and counters <= stride by construction.
                    unsafe {
                        std::slice::from_raw_parts(self.cells.as_ptr().add(offset), self.counters)
                    }
                };
                for (cell, value) in row.iter().zip(values.iter().copied()) {
                    cell.fetch_add(value, Ordering::Relaxed);
                }
            }
        }
    }

    #[inline]
    fn sum(&self, counter_idx: usize) -> isize {
        assert!(counter_idx < self.counters, "counter index out of bounds");
        let shards = self.cells.len() / self.stride;
        (0..shards)
            .map(|shard| {
                self.cell_at((shard * self.stride) + counter_idx)
                    .load(Ordering::Relaxed)
            })
            .sum()
    }

    #[inline]
    fn values(&self) -> Vec<isize> {
        (0..self.counters).map(|idx| self.sum(idx)).collect()
    }

    #[inline]
    fn sum_all(&self) -> isize {
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

    #[inline]
    fn sum_and_reset(&self, counter_idx: usize) -> isize {
        assert!(counter_idx < self.counters, "counter index out of bounds");
        let shards = self.cells.len() / self.stride;
        (0..shards)
            .map(|shard| {
                self.cell_at((shard * self.stride) + counter_idx)
                    .swap(0, Ordering::Relaxed)
            })
            .sum()
    }

    #[inline]
    fn snapshot_and_reset(&self) -> Vec<isize> {
        let mut values = vec![0; self.counters];
        let shards = self.cells.len() / self.stride;
        for shard in 0..shards {
            let offset = shard * self.stride;
            for (counter_idx, value) in values.iter_mut().enumerate() {
                *value += self
                    .cell_at(offset + counter_idx)
                    .swap(0, Ordering::Relaxed);
            }
        }
        values
    }

    #[cfg(feature = "eviction")]
    #[inline]
    fn touch(&self, cycle: u32) {
        self.last_accessed_cycle.store(cycle, Ordering::Relaxed);
    }

    #[inline]
    fn is_evicted(&self) -> bool {
        self.evicted.load(Ordering::Relaxed)
    }

    #[cfg(feature = "eviction")]
    fn mark_evicted(&self) {
        self.evicted.store(true, Ordering::Relaxed);
    }
}

impl CacheableSeries for CounterSetSeries {
    fn is_evicted(&self) -> bool {
        self.is_evicted()
    }
}

/// A reusable handle to one dynamic-label grouped counter series.
///
/// Resolve this once with [`DynamicCounterSet::get_or_create`] or
/// [`DynamicCounterSet::series`], then use index-based methods on hot paths.
#[derive(Clone)]
pub struct DynamicCounterSetSeries {
    series: Arc<CounterSetSeries>,
    names: Arc<Vec<String>>,
    indexes: Arc<BTreeMap<String, usize>>,
    shard_mask: usize,
}

impl DynamicCounterSetSeries {
    /// Increment one named counter by 1.
    #[inline]
    pub fn inc(&self, counter: &str) {
        self.add(counter, 1);
    }

    /// Add `value` to one named counter.
    #[inline]
    pub fn add(&self, counter: &str, value: isize) {
        let counter_idx = self
            .counter_index(counter)
            .unwrap_or_else(|| panic!("unknown counter name: {counter}"));
        self.add_index(counter_idx, value);
    }

    /// Add `value` to one pre-resolved counter index.
    #[inline]
    pub fn add_index(&self, counter_idx: usize, value: isize) {
        let shard_idx = thread_id() & self.shard_mask;
        self.series.add_at(shard_idx, counter_idx, value);
    }

    /// Add named counter values in one grouped call.
    #[inline]
    pub fn add_values(&self, updates: &[(&str, isize)]) {
        let shard_idx = thread_id() & self.shard_mask;
        for (counter, value) in updates {
            let counter_idx = self
                .counter_index(counter)
                .unwrap_or_else(|| panic!("unknown counter name: {counter}"));
            self.series.add_at(shard_idx, counter_idx, *value);
        }
    }

    /// Add pre-resolved `(counter_idx, value)` updates in one grouped call.
    #[inline]
    pub fn add_index_values(&self, updates: &[(usize, isize)]) {
        let shard_idx = thread_id() & self.shard_mask;
        self.series.add_index_values_at(shard_idx, updates);
    }

    /// Add one value for every counter in index order.
    #[inline]
    pub fn add_all_values(&self, values: &[isize]) {
        let shard_idx = thread_id() & self.shard_mask;
        self.series.add_values_at(shard_idx, values);
    }

    /// Return the current total for one named counter.
    #[inline]
    pub fn get(&self, counter: &str) -> isize {
        let counter_idx = self
            .counter_index(counter)
            .unwrap_or_else(|| panic!("unknown counter name: {counter}"));
        self.get_index(counter_idx)
    }

    /// Return the current total for one pre-resolved counter index.
    #[inline]
    pub fn get_index(&self, counter_idx: usize) -> isize {
        self.series.sum(counter_idx)
    }

    /// Return all current values in counter index order.
    #[inline]
    pub fn values(&self) -> Vec<isize> {
        self.series.values()
    }

    /// Return the index for `counter`, if it exists in this grouped set.
    #[inline]
    pub fn counter_index(&self, counter: &str) -> Option<usize> {
        self.indexes.get(counter).copied()
    }

    /// Return counter names in index order.
    #[inline]
    pub fn counter_names(&self) -> &[String] {
        &self.names
    }

    /// Check if this series handle has been evicted.
    ///
    /// If true, writes go to a detached series that is no longer exported.
    /// Callers holding long-lived handles can check this and re-resolve via
    /// [`DynamicCounterSet::series`] if needed.
    #[inline]
    pub fn is_evicted(&self) -> bool {
        self.series.is_evicted()
    }
}

thread_local! {
    static SERIES_CACHE: RefCell<LabelCache<Weak<CounterSetSeries>, SERIES_CACHE_SIZE>> =
        RefCell::new(LabelCache::new());
}

/// Runtime-label grouped counters.
///
/// Use `DynamicCounterSet` when label values are discovered at runtime and one
/// logical operation updates several related counters for the same label set.
/// Resolve and cache a [`DynamicCounterSetSeries`] once per active label set,
/// then use pre-resolved counter indexes on the hot path.
///
/// ```
/// use fast_telemetry::DynamicCounterSet;
///
/// let counters = DynamicCounterSet::with_shards(
///     4,
///     ["requests", "bytes", "errors"],
/// );
/// let requests = counters.counter_index("requests").unwrap();
/// let bytes = counters.counter_index("bytes").unwrap();
///
/// let series = counters.get_or_create(&[
///     ("org", "eden"),
///     ("endpoint", "ingest"),
/// ]);
/// series.add_index_values(&[(requests, 1), (bytes, 4096)]);
///
/// assert_eq!(series.get("requests"), 1);
/// assert_eq!(series.get("bytes"), 4096);
/// ```
pub struct DynamicCounterSet {
    id: usize,
    shard_count: usize,
    max_series: usize,
    shard_mask: usize,
    names: Arc<Vec<String>>,
    indexes: Arc<BTreeMap<String, usize>>,
    index_shards: Vec<CounterSetIndexShard>,
    series_count: AtomicUsize,
    overflow_count: AtomicU64,
}

impl DynamicCounterSet {
    /// Creates a new dynamic grouped counter with a default shard count.
    ///
    /// Prefer [`DynamicCounterSet::with_shards`] in services that already know
    /// the desired shard count.
    pub fn new<I, S>(counter_names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        Self::with_shards(default_shard_count(), counter_names)
    }

    /// Creates a new dynamic grouped counter with `shard_count` shards.
    pub fn with_shards<I, S>(shard_count: usize, counter_names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        Self::with_max_series(shard_count, DEFAULT_MAX_SERIES, counter_names)
    }

    /// Creates a new dynamic grouped counter with a series cardinality cap.
    ///
    /// When the number of unique label sets approximately reaches
    /// `max_series`, new label sets are redirected into a single overflow
    /// series (`__ft_overflow=true`). A `max_series` of 0 disables the cap.
    pub fn with_max_series<I, S>(shard_count: usize, max_series: usize, counter_names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let shard_count = shard_count.next_power_of_two();
        let (names, indexes) = build_counter_index(counter_names);
        let id = COUNTER_IDS.fetch_add(1, Ordering::Relaxed);
        Self {
            id,
            shard_count,
            max_series,
            shard_mask: shard_count - 1,
            names: Arc::new(names),
            indexes: Arc::new(indexes),
            index_shards: (0..shard_count)
                .map(|_| CachePadded::new(RwLock::new(dynamic_index_map())))
                .collect(),
            series_count: AtomicUsize::new(0),
            overflow_count: AtomicU64::new(0),
        }
    }

    /// Resolve a reusable grouped series handle for `labels`.
    pub fn series(&self, labels: &[(&str, &str)]) -> DynamicCounterSetSeries {
        self.get_or_create(labels)
    }

    /// Resolve a reusable grouped series handle for `labels`.
    pub fn get_or_create(&self, labels: &[(&str, &str)]) -> DynamicCounterSetSeries {
        self.series_handle(self.cached_or_create_series(labels))
    }

    /// Increment one named counter for the series identified by `labels`.
    #[inline]
    pub fn inc(&self, labels: &[(&str, &str)], counter: &str) {
        self.add(labels, counter, 1);
    }

    /// Add `value` to one named counter for the series identified by `labels`.
    #[inline]
    pub fn add(&self, labels: &[(&str, &str)], counter: &str, value: isize) {
        let counter_idx = self
            .counter_index(counter)
            .unwrap_or_else(|| panic!("unknown counter name: {counter}"));
        self.add_index(labels, counter_idx, value);
    }

    /// Add `value` to one pre-resolved counter index for `labels`.
    #[inline]
    pub fn add_index(&self, labels: &[(&str, &str)], counter_idx: usize, value: isize) {
        let series = self.cached_or_create_series(labels);
        let shard_idx = thread_id() & self.shard_mask;
        series.add_at(shard_idx, counter_idx, value);
    }

    /// Add named counter values in one grouped call.
    #[inline]
    pub fn add_values(&self, labels: &[(&str, &str)], updates: &[(&str, isize)]) {
        let series = self.cached_or_create_series(labels);
        let shard_idx = thread_id() & self.shard_mask;
        for (counter, value) in updates {
            let counter_idx = self
                .counter_index(counter)
                .unwrap_or_else(|| panic!("unknown counter name: {counter}"));
            series.add_at(shard_idx, counter_idx, *value);
        }
    }

    /// Add pre-resolved `(counter_idx, value)` updates in one grouped call.
    #[inline]
    pub fn add_index_values(&self, labels: &[(&str, &str)], updates: &[(usize, isize)]) {
        let series = self.cached_or_create_series(labels);
        let shard_idx = thread_id() & self.shard_mask;
        series.add_index_values_at(shard_idx, updates);
    }

    /// Add one value for every counter in index order.
    #[inline]
    pub fn add_all_values(&self, labels: &[(&str, &str)], values: &[isize]) {
        let series = self.cached_or_create_series(labels);
        let shard_idx = thread_id() & self.shard_mask;
        series.add_values_at(shard_idx, values);
    }

    /// Get one named counter for `labels`.
    pub fn get(&self, labels: &[(&str, &str)], counter: &str) -> isize {
        let counter_idx = self
            .counter_index(counter)
            .unwrap_or_else(|| panic!("unknown counter name: {counter}"));
        self.get_index(labels, counter_idx)
    }

    /// Get one pre-resolved counter index for `labels`.
    pub fn get_index(&self, labels: &[(&str, &str)], counter_idx: usize) -> isize {
        assert!(
            counter_idx < self.names.len(),
            "counter index out of bounds"
        );
        let key = DynamicLabelSet::from_pairs(labels);
        let index_shard = self.index_shard_for(&key);
        self.index_shards[index_shard]
            .read()
            .get(&key)
            .map(|series| series.sum(counter_idx))
            .unwrap_or(0)
    }

    /// Get all values for `labels` in counter index order.
    pub fn values(&self, labels: &[(&str, &str)]) -> Vec<isize> {
        let key = DynamicLabelSet::from_pairs(labels);
        let index_shard = self.index_shard_for(&key);
        self.index_shards[index_shard]
            .read()
            .get(&key)
            .map(|series| series.values())
            .unwrap_or_else(|| vec![0; self.names.len()])
    }

    /// Sums all counters across all dynamic series.
    pub fn sum_all(&self) -> isize {
        self.index_shards
            .iter()
            .map(|shard| {
                let guard = shard.read();
                guard.values().map(|series| series.sum_all()).sum::<isize>()
            })
            .sum()
    }

    /// Return a cumulative snapshot of all dynamic label sets.
    pub fn snapshot(&self) -> Vec<(DynamicLabelSet, Vec<isize>)> {
        let mut out = Vec::new();
        for shard in &self.index_shards {
            let guard = shard.read();
            for (labels, series) in guard.iter() {
                out.push((labels.clone(), series.values()));
            }
        }
        out
    }

    /// Return a snapshot of all dynamic label sets and reset counters to zero.
    ///
    /// Writes that occur concurrently with this method may be attributed to the
    /// next snapshot window rather than the current one. No counts are lost.
    pub fn snapshot_and_reset(&self) -> Vec<(DynamicLabelSet, Vec<isize>)> {
        let mut out = Vec::new();
        for shard in &self.index_shards {
            let guard = shard.read();
            for (labels, series) in guard.iter() {
                out.push((labels.clone(), series.snapshot_and_reset()));
            }
        }
        out
    }

    /// Reset one counter index for all dynamic label sets and return its total.
    pub fn sum_and_reset(&self, counter_idx: usize) -> isize {
        assert!(
            counter_idx < self.names.len(),
            "counter index out of bounds"
        );
        self.index_shards
            .iter()
            .map(|shard| {
                let guard = shard.read();
                guard
                    .values()
                    .map(|series| series.sum_and_reset(counter_idx))
                    .sum::<isize>()
            })
            .sum()
    }

    /// Iterate all counter values without cloning label sets.
    ///
    /// Calls `f(counter_name, labels, value)` for every counter in every
    /// dynamic label set. The callback runs while a shard read lock is held; it
    /// must be fast and must not call back into this metric.
    pub fn visit_series(&self, mut f: impl FnMut(&str, &[(String, String)], isize)) {
        for shard in &self.index_shards {
            let guard = shard.read();
            for (labels, series) in guard.iter() {
                for (counter_idx, counter_name) in self.names.iter().enumerate() {
                    f(
                        counter_name.as_str(),
                        labels.pairs(),
                        series.sum(counter_idx),
                    );
                }
            }
        }
    }

    /// Returns counter names in index order.
    #[inline]
    pub fn counter_names(&self) -> &[String] {
        &self.names
    }

    /// Return the index for `counter`, if it exists.
    #[inline]
    pub fn counter_index(&self, counter: &str) -> Option<usize> {
        self.indexes.get(counter).copied()
    }

    /// Returns the current number of distinct dynamic label sets.
    pub fn cardinality(&self) -> usize {
        self.index_shards
            .iter()
            .map(|shard| shard.read().len())
            .sum()
    }

    /// Returns the number of records routed to the overflow bucket.
    pub fn overflow_count(&self) -> u64 {
        self.overflow_count.load(Ordering::Relaxed)
    }

    /// Evict series that haven't been accessed for `max_staleness` cycles.
    ///
    /// Protected series (Arc::strong_count > 1) are never evicted because a
    /// caller still holds a [`DynamicCounterSetSeries`] handle.
    #[cfg(feature = "eviction")]
    pub fn evict_stale(&self, max_staleness: u32) -> usize {
        let cycle = current_cycle();
        let mut removed = 0;

        for shard in &self.index_shards {
            let mut guard = shard.write();
            guard.retain(|_labels, series| {
                if Arc::strong_count(series) > 1 {
                    return true;
                }
                let last = series.last_accessed_cycle.load(Ordering::Relaxed);
                let stale = cycle.saturating_sub(last) > max_staleness;
                if stale {
                    series.mark_evicted();
                    removed += 1;
                    self.series_count.fetch_sub(1, Ordering::Relaxed);
                }
                !stale
            });
        }

        removed
    }

    fn series_handle(&self, series: Arc<CounterSetSeries>) -> DynamicCounterSetSeries {
        DynamicCounterSetSeries {
            series,
            names: Arc::clone(&self.names),
            indexes: Arc::clone(&self.indexes),
            shard_mask: self.shard_mask,
        }
    }

    fn cached_or_create_series(&self, labels: &[(&str, &str)]) -> Arc<CounterSetSeries> {
        if let Some(series) = self.cached_series(labels) {
            return series;
        }
        let series = self.lookup_or_create(labels);
        self.update_cache(labels, &series);
        series
    }

    fn lookup_or_create(&self, labels: &[(&str, &str)]) -> Arc<CounterSetSeries> {
        let requested_key = DynamicLabelSet::from_pairs(labels);
        let requested_shard = self.index_shard_for(&requested_key);
        #[cfg(feature = "eviction")]
        let cycle = current_cycle();

        if let Some(series) = self.index_shards[requested_shard]
            .read()
            .get(&requested_key)
        {
            #[cfg(feature = "eviction")]
            series.touch(cycle);
            return Arc::clone(series);
        }

        let key = if self.max_series > 0
            && self.series_count.load(Ordering::Relaxed) >= self.max_series
        {
            self.overflow_count.fetch_add(1, Ordering::Relaxed);
            DynamicLabelSet::from_pairs(&[(OVERFLOW_LABEL_KEY, OVERFLOW_LABEL_VALUE)])
        } else {
            requested_key
        };
        let shard = self.index_shard_for(&key);

        if let Some(series) = self.index_shards[shard].read().get(&key) {
            #[cfg(feature = "eviction")]
            series.touch(cycle);
            return Arc::clone(series);
        }

        let mut guard = self.index_shards[shard].write();
        if let Some(series) = guard.get(&key) {
            #[cfg(feature = "eviction")]
            series.touch(cycle);
            return Arc::clone(series);
        }
        #[cfg(feature = "eviction")]
        let series = Arc::new(CounterSetSeries::new(
            self.shard_count,
            self.names.len(),
            cycle,
        ));
        #[cfg(not(feature = "eviction"))]
        let series = Arc::new(CounterSetSeries::new(self.shard_count, self.names.len()));
        guard.insert(key, Arc::clone(&series));
        self.series_count.fetch_add(1, Ordering::Relaxed);
        series
    }

    fn index_shard_for(&self, key: &DynamicLabelSet) -> usize {
        key.shard_index(self.shard_mask)
    }

    fn cached_series(&self, labels: &[(&str, &str)]) -> Option<Arc<CounterSetSeries>> {
        SERIES_CACHE.with(|cache| {
            let series = cache.borrow_mut().get(self.id, labels)?;
            #[cfg(feature = "eviction")]
            series.touch(current_cycle());
            Some(series)
        })
    }

    fn update_cache(&self, labels: &[(&str, &str)], series: &Arc<CounterSetSeries>) {
        SERIES_CACHE.with(|cache| {
            cache
                .borrow_mut()
                .insert(self.id, labels, Arc::downgrade(series));
        });
    }
}

fn build_counter_index<I, S>(counter_names: I) -> (Vec<String>, BTreeMap<String, usize>)
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut names = Vec::new();
    let mut indexes = BTreeMap::new();
    for name in counter_names {
        let name = name.as_ref();
        assert!(!name.is_empty(), "counter names must not be empty");
        let index = names.len();
        let inserted = indexes.insert(name.to_string(), index);
        assert!(inserted.is_none(), "duplicate counter name: {name}");
        names.push(name.to_string());
    }
    assert!(!names.is_empty(), "counter_names must not be empty");
    (names, indexes)
}

#[inline]
fn counter_row_stride(counters: usize) -> usize {
    let cells_per_padded_counter =
        std::mem::size_of::<CachePadded<AtomicIsize>>() / std::mem::size_of::<AtomicIsize>();
    let row_padding = cells_per_padded_counter.max(1);
    counters.div_ceil(row_padding) * row_padding
}

#[inline]
fn default_shard_count() -> usize {
    std::thread::available_parallelism().map_or(4, usize::from)
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "eviction")]
    use super::super::{advance_cycle, lock_eviction_cycle_for_test};
    use super::*;

    #[test]
    fn dynamic_counter_set_updates_named_counters() {
        let counters = DynamicCounterSet::with_shards(4, ["requests", "bytes", "errors"]);
        let labels = &[("org", "eden"), ("endpoint", "ingest")];

        counters.add_values(labels, &[("requests", 1), ("bytes", 4096)]);
        counters.inc(labels, "errors");

        assert_eq!(counters.get(labels, "requests"), 1);
        assert_eq!(counters.get(labels, "bytes"), 4096);
        assert_eq!(counters.get(labels, "errors"), 1);
        assert_eq!(counters.values(labels), vec![1, 4096, 1]);
        assert_eq!(counters.sum_all(), 4098);
    }

    #[test]
    fn dynamic_counter_set_label_order_is_canonicalized() {
        let counters = DynamicCounterSet::with_shards(4, ["requests"]);

        counters.inc(&[("org", "eden"), ("endpoint", "ingest")], "requests");

        assert_eq!(
            counters.get(&[("endpoint", "ingest"), ("org", "eden")], "requests"),
            1
        );
    }

    #[test]
    fn dynamic_counter_set_series_handle_supports_index_updates() {
        let counters = DynamicCounterSet::with_shards(4, ["requests", "bytes", "errors"]);
        let requests = counters.counter_index("requests").unwrap();
        let bytes = counters.counter_index("bytes").unwrap();
        let errors = counters.counter_index("errors").unwrap();
        let series = counters.series(&[("org", "eden")]);

        series.add_index_values(&[(requests, 2), (bytes, 512)]);
        series.add_index(errors, 1);

        assert_eq!(series.values(), vec![2, 512, 1]);
        assert_eq!(counters.values(&[("org", "eden")]), vec![2, 512, 1]);
    }

    #[test]
    fn dynamic_counter_set_add_all_values_uses_index_order() {
        let counters = DynamicCounterSet::with_shards(4, ["requests", "bytes", "errors"]);

        counters.add_all_values(&[("org", "eden")], &[1, 1024, 0]);

        assert_eq!(counters.values(&[("org", "eden")]), vec![1, 1024, 0]);
    }

    #[test]
    fn dynamic_counter_set_snapshot_and_reset_returns_all_series() {
        let counters = DynamicCounterSet::with_shards(4, ["requests", "bytes"]);

        counters.add_all_values(&[("org", "eden")], &[2, 10]);
        counters.add_all_values(&[("org", "acme")], &[3, 15]);

        let mut snapshot = counters.snapshot_and_reset();
        snapshot.sort_by(|(left, _), (right, _)| left.cmp(right));

        assert_eq!(snapshot.len(), 2);
        assert_eq!(
            snapshot[0].0.pairs()[0],
            ("org".to_string(), "acme".to_string())
        );
        assert_eq!(snapshot[0].1, vec![3, 15]);
        assert_eq!(
            snapshot[1].0.pairs()[0],
            ("org".to_string(), "eden".to_string())
        );
        assert_eq!(snapshot[1].1, vec![2, 10]);
        assert_eq!(counters.sum_all(), 0);
    }

    #[test]
    fn dynamic_counter_set_sum_and_reset_resets_one_counter_across_series() {
        let counters = DynamicCounterSet::with_shards(4, ["requests", "bytes"]);
        let requests = counters.counter_index("requests").unwrap();

        counters.add_all_values(&[("org", "eden")], &[2, 10]);
        counters.add_all_values(&[("org", "acme")], &[3, 15]);

        assert_eq!(counters.sum_and_reset(requests), 5);
        assert_eq!(counters.values(&[("org", "eden")]), vec![0, 10]);
        assert_eq!(counters.values(&[("org", "acme")]), vec![0, 15]);
    }

    #[test]
    fn dynamic_counter_set_visit_series_exposes_counter_names() {
        let counters = DynamicCounterSet::with_shards(4, ["requests", "bytes"]);
        counters.add_all_values(&[("org", "eden")], &[2, 10]);

        let mut visited = Vec::new();
        counters.visit_series(|counter, labels, value| {
            visited.push((counter.to_string(), labels.to_vec(), value));
        });
        visited.sort_by(|left, right| left.0.cmp(&right.0));

        assert_eq!(visited.len(), 2);
        assert_eq!(visited[0].0, "bytes");
        assert_eq!(visited[0].2, 10);
        assert_eq!(visited[1].0, "requests");
        assert_eq!(visited[1].2, 2);
    }

    #[test]
    fn dynamic_counter_set_overflow_bucket_routes_new_series_at_capacity() {
        let counters = DynamicCounterSet::with_max_series(4, 2, ["requests"]);

        counters.inc(&[("org", "1")], "requests");
        counters.inc(&[("org", "2")], "requests");
        counters.inc(&[("org", "3")], "requests");

        assert_eq!(counters.cardinality(), 3);
        assert_eq!(
            counters.get(&[(OVERFLOW_LABEL_KEY, OVERFLOW_LABEL_VALUE)], "requests"),
            1
        );
        assert!(counters.overflow_count() > 0);
    }

    #[test]
    fn dynamic_counter_set_concurrent_index_updates() {
        let counters = Arc::new(DynamicCounterSet::with_shards(
            8,
            ["requests", "bytes", "errors"],
        ));
        let requests = counters.counter_index("requests").unwrap();
        let bytes = counters.counter_index("bytes").unwrap();

        std::thread::scope(|scope| {
            for _ in 0..8 {
                let counters = Arc::clone(&counters);
                scope.spawn(move || {
                    let series = counters.series(&[("org", "eden")]);
                    for _ in 0..10_000 {
                        series.add_index_values(&[(requests, 1), (bytes, 2)]);
                    }
                });
            }
        });

        assert_eq!(counters.get(&[("org", "eden")], "requests"), 80_000);
        assert_eq!(counters.get(&[("org", "eden")], "bytes"), 160_000);
    }

    #[cfg(feature = "eviction")]
    #[test]
    fn dynamic_counter_set_eviction_tombstone_invalidates_cache() {
        let _cycle_guard = lock_eviction_cycle_for_test();
        let counters = DynamicCounterSet::with_shards(4, ["requests"]);
        let labels = &[("org", "eden")];

        counters.inc(labels, "requests");
        counters.inc(labels, "requests");
        assert_eq!(counters.get(labels, "requests"), 2);

        advance_cycle();
        advance_cycle();
        counters.inc(&[("flush", "cache")], "requests");
        assert_eq!(counters.evict_stale(1), 1);

        counters.inc(labels, "requests");
        assert_eq!(counters.get(labels, "requests"), 1);
    }
}
