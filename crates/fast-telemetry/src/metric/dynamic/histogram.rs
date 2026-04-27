//! Runtime-labeled histogram for dynamic dimensions.

use super::cache::{CacheableSeries, LabelCache, SERIES_CACHE_SIZE};
#[cfg(feature = "eviction")]
use super::current_cycle;
use super::{DynamicLabelSet, HISTOGRAM_IDS, thread_id};
use crossbeam_utils::CachePadded;
use parking_lot::RwLock;
use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
#[cfg(feature = "eviction")]
use std::sync::atomic::AtomicU32;
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

const DEFAULT_MAX_SERIES: usize = 2000;
const OVERFLOW_LABEL_KEY: &str = "__ft_overflow";
const OVERFLOW_LABEL_VALUE: &str = "true";

type HistogramIndexShard = CachePadded<RwLock<HashMap<DynamicLabelSet, Arc<HistogramSeries>>>>;
type HistogramSnapshotEntry = (DynamicLabelSet, Vec<(u64, u64)>, u64, u64);

struct ShardedCounter {
    cells: Vec<CachePadded<AtomicIsize>>,
}

impl ShardedCounter {
    fn new(shard_count: usize) -> Self {
        Self {
            cells: (0..shard_count)
                .map(|_| CachePadded::new(AtomicIsize::new(0)))
                .collect(),
        }
    }

    #[inline]
    fn add_at(&self, shard_idx: usize, value: isize) {
        self.cells[shard_idx].fetch_add(value, Ordering::Relaxed);
    }

    #[inline]
    fn inc_at(&self, shard_idx: usize) {
        self.add_at(shard_idx, 1);
    }

    #[inline]
    fn sum(&self) -> isize {
        self.cells
            .iter()
            .map(|cell| cell.load(Ordering::Relaxed))
            .sum()
    }
}

struct HistogramSeries {
    bounds: Arc<Vec<u64>>,
    buckets: Vec<ShardedCounter>,
    sum: ShardedCounter,
    count: ShardedCounter,
    /// Tombstone flag set by exporter before removing from map.
    evicted: AtomicBool,
    /// Last export cycle when this series was accessed.
    #[cfg(feature = "eviction")]
    last_accessed_cycle: AtomicU32,
}

impl HistogramSeries {
    #[cfg(feature = "eviction")]
    fn new(bounds: Arc<Vec<u64>>, shard_count: usize, cycle: u32) -> Self {
        let buckets = (0..=bounds.len())
            .map(|_| ShardedCounter::new(shard_count))
            .collect();
        Self {
            bounds,
            buckets,
            sum: ShardedCounter::new(shard_count),
            count: ShardedCounter::new(shard_count),
            evicted: AtomicBool::new(false),
            last_accessed_cycle: AtomicU32::new(cycle),
        }
    }

    #[cfg(not(feature = "eviction"))]
    fn new(bounds: Arc<Vec<u64>>, shard_count: usize) -> Self {
        let buckets = (0..=bounds.len())
            .map(|_| ShardedCounter::new(shard_count))
            .collect();
        Self {
            bounds,
            buckets,
            sum: ShardedCounter::new(shard_count),
            count: ShardedCounter::new(shard_count),
            evicted: AtomicBool::new(false),
        }
    }

    #[inline]
    fn is_evicted(&self) -> bool {
        self.evicted.load(Ordering::Relaxed)
    }

    #[cfg(feature = "eviction")]
    fn mark_evicted(&self) {
        self.evicted.store(true, Ordering::Relaxed);
    }

    #[inline]
    fn record_at(&self, shard_idx: usize, value: u64) {
        let bucket_idx = self
            .bounds
            .iter()
            .position(|&bound| value <= bound)
            .unwrap_or(self.bounds.len());
        self.buckets[bucket_idx].inc_at(shard_idx);
        self.sum.add_at(shard_idx, value as isize);
        self.count.inc_at(shard_idx);
        // Note: timestamp updated on slow path (lookup/cache miss) to avoid
        // global atomic read on every record.
    }

    /// Touch the series timestamp. Called on slow path only.
    #[cfg(feature = "eviction")]
    #[inline]
    fn touch(&self, cycle: u32) {
        self.last_accessed_cycle.store(cycle, Ordering::Relaxed);
    }

    fn buckets_cumulative(&self) -> Vec<(u64, u64)> {
        let mut result = Vec::with_capacity(self.buckets.len());
        for (bound, cumulative) in self.buckets_cumulative_iter() {
            result.push((bound, cumulative));
        }
        result
    }

    fn buckets_cumulative_iter(&self) -> impl Iterator<Item = (u64, u64)> + '_ {
        let mut cumulative = 0i64;
        self.buckets.iter().enumerate().map(move |(i, counter)| {
            cumulative += counter.sum() as i64;
            let bound = if i < self.bounds.len() {
                self.bounds[i]
            } else {
                u64::MAX
            };
            (bound, cumulative as u64)
        })
    }

    fn sum(&self) -> u64 {
        self.sum.sum() as u64
    }

    fn count(&self) -> u64 {
        self.count.sum() as u64
    }
}

impl CacheableSeries for HistogramSeries {
    fn is_evicted(&self) -> bool {
        self.is_evicted()
    }
}

/// A reusable handle to a dynamic-label histogram series.
///
/// Use this for hot paths to avoid per-update label canonicalization and map
/// lookups. Resolve once with `DynamicHistogram::series(...)`, then call
/// `record()` on the handle.
#[derive(Clone)]
pub struct DynamicHistogramSeries {
    series: Arc<HistogramSeries>,
    shard_mask: usize,
}

/// Borrowed read-only view of a dynamic histogram series.
#[doc(hidden)]
pub struct DynamicHistogramSeriesView<'a> {
    series: &'a HistogramSeries,
}

impl<'a> DynamicHistogramSeriesView<'a> {
    /// Iterate cumulative `(bound, count)` buckets without allocating.
    #[doc(hidden)]
    pub fn buckets_cumulative_iter(&self) -> impl Iterator<Item = (u64, u64)> + '_ {
        self.series.buckets_cumulative_iter()
    }

    #[doc(hidden)]
    pub fn sum(&self) -> u64 {
        self.series.sum()
    }

    #[doc(hidden)]
    pub fn count(&self) -> u64 {
        self.series.count()
    }
}

impl DynamicHistogramSeries {
    /// Record a value in this histogram series.
    #[inline]
    pub fn record(&self, value: u64) {
        let shard_idx = thread_id() & self.shard_mask;
        self.series.record_at(shard_idx, value);
    }

    /// Get cumulative bucket counts.
    pub fn buckets_cumulative(&self) -> Vec<(u64, u64)> {
        self.series.buckets_cumulative()
    }

    /// Get the sum of all recorded values.
    pub fn sum(&self) -> u64 {
        self.series.sum()
    }

    /// Get the count of all recorded values.
    pub fn count(&self) -> u64 {
        self.series.count()
    }

    /// Check if this series handle has been evicted.
    #[inline]
    pub fn is_evicted(&self) -> bool {
        self.series.is_evicted()
    }
}

thread_local! {
    static SERIES_CACHE: RefCell<LabelCache<Weak<HistogramSeries>, SERIES_CACHE_SIZE>> =
        RefCell::new(LabelCache::new());
}

/// Histogram keyed by runtime label sets.
///
/// Uses sharded index for key->series lookup and per-series sharded counters
/// for fast updates.
pub struct DynamicHistogram {
    id: usize,
    bounds: Arc<Vec<u64>>,
    shard_count: usize,
    max_series: usize,
    shard_mask: usize,
    index_shards: Vec<HistogramIndexShard>,
    /// Approximate number of live series (incremented on insert, decremented on evict).
    series_count: AtomicUsize,
    /// Count of records routed to overflow bucket due to cardinality cap.
    overflow_count: AtomicU64,
}

impl DynamicHistogram {
    /// Creates a new runtime-labeled histogram with given bucket boundaries.
    pub fn new(bounds: &[u64], shard_count: usize) -> Self {
        Self::with_limits(bounds, shard_count, DEFAULT_MAX_SERIES)
    }

    /// Creates a new runtime-labeled histogram with a series cardinality cap.
    ///
    /// When the number of unique label sets approximately reaches `max_series`,
    /// new label sets are redirected into a single overflow series
    /// (`__ft_overflow=true`). The cap is checked via a lock-free atomic counter,
    /// so concurrent inserts may briefly overshoot by the number of in-flight
    /// writers before the overflow kicks in.
    pub fn with_limits(bounds: &[u64], shard_count: usize, max_series: usize) -> Self {
        let shard_count = shard_count.next_power_of_two();
        let id = HISTOGRAM_IDS.fetch_add(1, Ordering::Relaxed);
        Self {
            id,
            bounds: Arc::new(bounds.to_vec()),
            shard_count,
            max_series,
            shard_mask: shard_count - 1,
            index_shards: (0..shard_count)
                .map(|_| CachePadded::new(RwLock::new(HashMap::new())))
                .collect(),
            series_count: AtomicUsize::new(0),
            overflow_count: AtomicU64::new(0),
        }
    }

    /// Creates a histogram with default latency buckets (in microseconds).
    pub fn with_latency_buckets(shard_count: usize) -> Self {
        Self::with_limits(
            &[
                10,         // 10µs
                50,         // 50µs
                100,        // 100µs
                500,        // 500µs
                1_000,      // 1ms
                5_000,      // 5ms
                10_000,     // 10ms
                50_000,     // 50ms
                100_000,    // 100ms
                500_000,    // 500ms
                1_000_000,  // 1s
                5_000_000,  // 5s
                10_000_000, // 10s
            ],
            shard_count,
            DEFAULT_MAX_SERIES,
        )
    }

    /// Resolve a reusable series handle for `labels`.
    ///
    /// Preferred for hot paths when labels come from a finite active set.
    pub fn series(&self, labels: &[(&str, &str)]) -> DynamicHistogramSeries {
        if let Some(series) = self.cached_series(labels) {
            return DynamicHistogramSeries {
                series,
                shard_mask: self.shard_mask,
            };
        }
        let series = self.lookup_or_create(labels);
        self.update_cache(labels, &series);
        DynamicHistogramSeries {
            series,
            shard_mask: self.shard_mask,
        }
    }

    /// Record a value for the series identified by `labels`.
    #[inline]
    pub fn record(&self, labels: &[(&str, &str)], value: u64) {
        if let Some(series) = self.cached_series(labels) {
            let shard_idx = thread_id() & self.shard_mask;
            series.record_at(shard_idx, value);
            return;
        }

        let series = self.lookup_or_create(labels);
        self.update_cache(labels, &series);
        let shard_idx = thread_id() & self.shard_mask;
        series.record_at(shard_idx, value);
    }

    /// Get cumulative bucket counts for the series identified by `labels`.
    pub fn buckets_cumulative(&self, labels: &[(&str, &str)]) -> Vec<(u64, u64)> {
        let key = DynamicLabelSet::from_pairs(labels);
        let index_shard = self.index_shard_for(&key);
        self.index_shards[index_shard]
            .read()
            .get(&key)
            .map(|series| series.buckets_cumulative())
            .unwrap_or_default()
    }

    /// Get sum for the series identified by `labels`.
    pub fn sum(&self, labels: &[(&str, &str)]) -> u64 {
        let key = DynamicLabelSet::from_pairs(labels);
        let index_shard = self.index_shard_for(&key);
        self.index_shards[index_shard]
            .read()
            .get(&key)
            .map(|series| series.sum())
            .unwrap_or(0)
    }

    /// Get count for the series identified by `labels`.
    pub fn count(&self, labels: &[(&str, &str)]) -> u64 {
        let key = DynamicLabelSet::from_pairs(labels);
        let index_shard = self.index_shard_for(&key);
        self.index_shards[index_shard]
            .read()
            .get(&key)
            .map(|series| series.count())
            .unwrap_or(0)
    }

    /// Returns a snapshot of all label-set with their histogram data.
    pub fn snapshot(&self) -> Vec<HistogramSnapshotEntry> {
        let mut out = Vec::new();
        for shard in &self.index_shards {
            let guard = shard.read();
            for (labels, series) in guard.iter() {
                out.push((
                    labels.clone(),
                    series.buckets_cumulative(),
                    series.sum(),
                    series.count(),
                ));
            }
        }
        out
    }

    /// Returns the current number of distinct label sets.
    pub fn cardinality(&self) -> usize {
        self.index_shards
            .iter()
            .map(|shard| shard.read().len())
            .sum()
    }

    /// Returns the number of records routed to the overflow bucket.
    ///
    /// A non-zero value indicates the cardinality cap was hit and label
    /// fidelity is being lost. Use this to alert on cardinality pressure.
    pub fn overflow_count(&self) -> u64 {
        self.overflow_count.load(Ordering::Relaxed)
    }

    /// Iterate all series without cloning label sets.
    ///
    /// Calls `f` with borrowed label pairs and a borrowed series view.
    /// Used by exporters/macros to avoid `snapshot()` and bucket vec allocations.
    #[doc(hidden)]
    pub fn visit_series<F>(&self, mut f: F)
    where
        F: for<'a> FnMut(&'a [(String, String)], DynamicHistogramSeriesView<'a>),
    {
        for shard in &self.index_shards {
            let guard = shard.read();
            for (labels, series) in guard.iter() {
                f(labels.pairs(), DynamicHistogramSeriesView { series });
            }
        }
    }

    /// Evict series that haven't been accessed for `max_staleness` cycles.
    ///
    /// Call this after `advance_cycle()` in your exporter task.
    /// Series are marked as evicted (so cached handles see the tombstone),
    /// then removed from the index.
    ///
    /// Protected series (Arc::strong_count > 1) are never evicted - someone
    /// holds a DynamicHistogramSeries handle to them.
    ///
    /// Returns the number of series evicted.
    #[cfg(feature = "eviction")]
    pub fn evict_stale(&self, max_staleness: u32) -> usize {
        let cycle = current_cycle();
        let mut removed = 0;

        for shard in &self.index_shards {
            let mut guard = shard.write();
            guard.retain(|_labels, series| {
                // Protected if someone holds a handle (strong_count > 1 means
                // both the map and at least one DynamicHistogramSeries hold refs)
                if Arc::strong_count(series) > 1 {
                    return true;
                }
                // Otherwise check timestamp staleness
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

    fn lookup_or_create(&self, labels: &[(&str, &str)]) -> Arc<HistogramSeries> {
        let requested_key = DynamicLabelSet::from_pairs(labels);
        let requested_shard = self.index_shard_for(&requested_key);
        #[cfg(feature = "eviction")]
        let cycle = current_cycle();

        // Fast path: read lock only.
        if let Some(series) = self.index_shards[requested_shard]
            .read()
            .get(&requested_key)
        {
            #[cfg(feature = "eviction")]
            series.touch(cycle);
            return Arc::clone(series);
        }

        // Check cardinality cap BEFORE taking any write lock (lock-free).
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
        let series = Arc::new(HistogramSeries::new(
            Arc::clone(&self.bounds),
            self.shard_count,
            cycle,
        ));
        #[cfg(not(feature = "eviction"))]
        let series = Arc::new(HistogramSeries::new(
            Arc::clone(&self.bounds),
            self.shard_count,
        ));
        guard.insert(key, Arc::clone(&series));
        self.series_count.fetch_add(1, Ordering::Relaxed);
        series
    }

    fn index_shard_for(&self, key: &DynamicLabelSet) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) & self.shard_mask
    }

    fn cached_series(&self, labels: &[(&str, &str)]) -> Option<Arc<HistogramSeries>> {
        SERIES_CACHE.with(|cache| {
            let series = cache.borrow_mut().get(self.id, labels)?;
            #[cfg(feature = "eviction")]
            series.touch(current_cycle());
            Some(series)
        })
    }

    fn update_cache(&self, labels: &[(&str, &str)], series: &Arc<HistogramSeries>) {
        SERIES_CACHE.with(|cache| {
            cache
                .borrow_mut()
                .insert(self.id, labels, Arc::downgrade(series));
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_recording() {
        let h = DynamicHistogram::new(&[10, 100, 1000], 4);
        let labels = &[("org_id", "42")];

        h.record(labels, 5); // bucket 0 (≤10)
        h.record(labels, 50); // bucket 1 (≤100)
        h.record(labels, 500); // bucket 2 (≤1000)
        h.record(labels, 5000); // bucket 3 (+Inf)

        let buckets = h.buckets_cumulative(labels);
        assert_eq!(buckets.len(), 4);
        assert_eq!(buckets[0], (10, 1));
        assert_eq!(buckets[1], (100, 2));
        assert_eq!(buckets[2], (1000, 3));
        assert_eq!(buckets[3], (u64::MAX, 4));

        assert_eq!(h.count(labels), 4);
        assert_eq!(h.sum(labels), 5 + 50 + 500 + 5000);
    }

    #[test]
    fn test_label_order_is_canonicalized() {
        let h = DynamicHistogram::new(&[10, 100], 4);

        h.record(&[("org_id", "42"), ("endpoint", "abc")], 5);

        assert_eq!(h.count(&[("endpoint", "abc"), ("org_id", "42")]), 1);
    }

    #[test]
    fn test_series_handle() {
        let h = DynamicHistogram::new(&[10, 100, 1000], 4);
        let series = h.series(&[("org_id", "42")]);

        series.record(5);
        series.record(50);
        series.record(500);

        assert_eq!(series.count(), 3);
        assert_eq!(series.sum(), 555);
        assert_eq!(h.count(&[("org_id", "42")]), 3);
    }

    #[test]
    fn test_multiple_label_sets() {
        let h = DynamicHistogram::new(&[100], 4);

        h.record(&[("org_id", "1")], 50);
        h.record(&[("org_id", "2")], 150);

        assert_eq!(h.count(&[("org_id", "1")]), 1);
        assert_eq!(h.count(&[("org_id", "2")]), 1);

        let snap = h.snapshot();
        assert_eq!(snap.len(), 2);
    }

    #[test]
    fn test_overflow_bucket_routes_new_series_at_capacity() {
        let h = DynamicHistogram::with_limits(&[100], 4, 1);

        h.record(&[("org_id", "1")], 50);
        h.record(&[("org_id", "2")], 150);

        assert_eq!(h.cardinality(), 2);
        assert_eq!(h.count(&[(OVERFLOW_LABEL_KEY, OVERFLOW_LABEL_VALUE)]), 1);
        assert_eq!(h.sum(&[(OVERFLOW_LABEL_KEY, OVERFLOW_LABEL_VALUE)]), 150);
    }

    #[test]
    fn test_concurrent_cap_bounded_overshoot() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        let cap = 10;
        let threads = 16;
        let h = Arc::new(DynamicHistogram::with_limits(&[100, 1000], 4, cap));
        let barrier = Arc::new(Barrier::new(threads));

        let handles: Vec<_> = (0..threads)
            .map(|t| {
                let h = Arc::clone(&h);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    for i in 0..5 {
                        let label = format!("t{t}_s{i}");
                        h.record(&[("key", &label)], 42);
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        let card = h.cardinality();
        assert!(
            card <= cap + threads + 1,
            "cardinality {card} exceeded bounded overshoot (cap={cap}, threads={threads})"
        );
        assert!(h.overflow_count() > 0, "overflow should have triggered");
    }
}
