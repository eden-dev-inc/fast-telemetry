//! Runtime-labeled signed integer gauge for dynamic dimensions.
//!
//! Use this for metrics like "active connections" or "in-flight requests"
//! that need atomic add/sub semantics but should export as absolute gauge values
//! (not counter deltas).

use super::cache::{CacheableSeries, LabelCache, SERIES_CACHE_SIZE};
#[cfg(feature = "eviction")]
use super::current_cycle;
use super::{DynamicLabelSet, thread_id};
use crossbeam_utils::CachePadded;
use parking_lot::RwLock;
use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
#[cfg(feature = "eviction")]
use std::sync::atomic::AtomicU32;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

static GAUGE_I64_IDS: AtomicUsize = AtomicUsize::new(1);
const DEFAULT_MAX_SERIES: usize = 2000;
const OVERFLOW_LABEL_KEY: &str = "__ft_overflow";
const OVERFLOW_LABEL_VALUE: &str = "true";
type GaugeI64IndexShard = CachePadded<RwLock<HashMap<DynamicLabelSet, Arc<GaugeI64Series>>>>;

struct GaugeI64Series {
    cells: Vec<CachePadded<AtomicI64>>,
    /// Tombstone flag set by exporter before removing from map.
    evicted: AtomicBool,
    /// Last export cycle when this series was accessed.
    #[cfg(feature = "eviction")]
    last_accessed_cycle: AtomicU32,
}

impl GaugeI64Series {
    #[cfg(feature = "eviction")]
    fn new(shard_count: usize, cycle: u32) -> Self {
        Self {
            cells: (0..shard_count)
                .map(|_| CachePadded::new(AtomicI64::new(0)))
                .collect(),
            evicted: AtomicBool::new(false),
            last_accessed_cycle: AtomicU32::new(cycle),
        }
    }

    #[cfg(not(feature = "eviction"))]
    fn new(shard_count: usize) -> Self {
        Self {
            cells: (0..shard_count)
                .map(|_| CachePadded::new(AtomicI64::new(0)))
                .collect(),
            evicted: AtomicBool::new(false),
        }
    }

    #[inline]
    fn add_at(&self, shard_idx: usize, value: i64) {
        self.cells[shard_idx].fetch_add(value, Ordering::Relaxed);
        // Note: timestamp updated on slow path (lookup/cache miss) to avoid
        // global atomic read on every add.
    }

    #[inline]
    fn set_at(&self, shard_idx: usize, value: i64) {
        // For set, we need to clear other shards and set the target shard
        // This is inherently racy but acceptable for gauge semantics
        for (i, cell) in self.cells.iter().enumerate() {
            if i == shard_idx {
                cell.store(value, Ordering::Relaxed);
            } else {
                cell.store(0, Ordering::Relaxed);
            }
        }
        // Note: timestamp updated on slow path (lookup/cache miss) to avoid
        // global atomic read on every set.
    }

    /// Touch the series timestamp. Called on slow path only.
    #[cfg(feature = "eviction")]
    #[inline]
    fn touch(&self, cycle: u32) {
        self.last_accessed_cycle.store(cycle, Ordering::Relaxed);
    }

    #[inline]
    fn sum(&self) -> i64 {
        self.cells
            .iter()
            .map(|cell| cell.load(Ordering::Relaxed))
            .sum()
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

impl CacheableSeries for GaugeI64Series {
    fn is_evicted(&self) -> bool {
        self.is_evicted()
    }
}

/// A reusable handle to a dynamic-label i64 gauge series.
///
/// Use this for hot paths to avoid per-update label canonicalization and map
/// lookups. Resolve once with `DynamicGaugeI64::series(...)`, then call `add()`
/// / `set()` on the handle.
#[derive(Clone)]
pub struct DynamicGaugeI64Series {
    series: Arc<GaugeI64Series>,
    shard_mask: usize,
}

impl DynamicGaugeI64Series {
    /// Increment this gauge by 1.
    #[inline]
    pub fn inc(&self) {
        self.add(1);
    }

    /// Decrement this gauge by 1.
    #[inline]
    pub fn dec(&self) {
        self.add(-1);
    }

    /// Add `value` to this gauge (can be negative).
    #[inline]
    pub fn add(&self, value: i64) {
        let shard_idx = thread_id() & self.shard_mask;
        self.series.add_at(shard_idx, value);
    }

    /// Set this gauge to an absolute value.
    #[inline]
    pub fn set(&self, value: i64) {
        let shard_idx = thread_id() & self.shard_mask;
        self.series.set_at(shard_idx, value);
    }

    /// Get this gauge's total across shards.
    #[inline]
    pub fn get(&self) -> i64 {
        self.series.sum()
    }

    /// Check if this series handle has been evicted.
    #[inline]
    pub fn is_evicted(&self) -> bool {
        self.series.is_evicted()
    }
}

thread_local! {
    static SERIES_CACHE: RefCell<LabelCache<Weak<GaugeI64Series>, SERIES_CACHE_SIZE>> =
        RefCell::new(LabelCache::new());
}

/// Signed integer gauge keyed by runtime label sets.
///
/// Unlike `DynamicCounter`, this exports as a gauge (absolute value) rather than
/// a counter (delta). Use for metrics like "active connections" that go up and down.
///
/// Uses sharded atomics internally for fast concurrent updates.
pub struct DynamicGaugeI64 {
    id: usize,
    shard_count: usize,
    max_series: usize,
    shard_mask: usize,
    index_shards: Vec<GaugeI64IndexShard>,
    /// Approximate number of live series (incremented on insert, decremented on evict).
    series_count: AtomicUsize,
    /// Count of records routed to overflow bucket due to cardinality cap.
    overflow_count: AtomicU64,
}

impl DynamicGaugeI64 {
    /// Creates a new runtime-labeled i64 gauge.
    pub fn new(shard_count: usize) -> Self {
        Self::with_max_series(shard_count, DEFAULT_MAX_SERIES)
    }

    /// Creates a new runtime-labeled i64 gauge with a series cardinality cap.
    ///
    /// When the number of unique label sets approximately reaches `max_series`,
    /// new label sets are redirected into a single overflow series
    /// (`__ft_overflow=true`). The cap is checked via a lock-free atomic counter,
    /// so concurrent inserts may briefly overshoot by the number of in-flight
    /// writers before the overflow kicks in.
    pub fn with_max_series(shard_count: usize, max_series: usize) -> Self {
        let shard_count = shard_count.next_power_of_two();
        let id = GAUGE_I64_IDS.fetch_add(1, Ordering::Relaxed);
        Self {
            id,
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

    /// Resolve a reusable series handle for `labels`.
    pub fn series(&self, labels: &[(&str, &str)]) -> DynamicGaugeI64Series {
        if let Some(series) = self.cached_series(labels) {
            return DynamicGaugeI64Series {
                series,
                shard_mask: self.shard_mask,
            };
        }
        let series = self.lookup_or_create(labels);
        self.update_cache(labels, &series);
        DynamicGaugeI64Series {
            series,
            shard_mask: self.shard_mask,
        }
    }

    /// Increments the gauge identified by `labels` by 1.
    #[inline]
    pub fn inc(&self, labels: &[(&str, &str)]) {
        self.add(labels, 1);
    }

    /// Decrements the gauge identified by `labels` by 1.
    #[inline]
    pub fn dec(&self, labels: &[(&str, &str)]) {
        self.add(labels, -1);
    }

    /// Adds `value` to the gauge identified by `labels` (can be negative).
    #[inline]
    pub fn add(&self, labels: &[(&str, &str)], value: i64) {
        if let Some(series) = self.cached_series(labels) {
            let shard_idx = thread_id() & self.shard_mask;
            series.add_at(shard_idx, value);
            return;
        }

        let series = self.lookup_or_create(labels);
        self.update_cache(labels, &series);
        let shard_idx = thread_id() & self.shard_mask;
        series.add_at(shard_idx, value);
    }

    /// Sets the gauge identified by `labels` to an absolute value.
    #[inline]
    pub fn set(&self, labels: &[(&str, &str)], value: i64) {
        if let Some(series) = self.cached_series(labels) {
            let shard_idx = thread_id() & self.shard_mask;
            series.set_at(shard_idx, value);
            return;
        }

        let series = self.lookup_or_create(labels);
        self.update_cache(labels, &series);
        let shard_idx = thread_id() & self.shard_mask;
        series.set_at(shard_idx, value);
    }

    /// Gets the current value for the gauge identified by `labels`.
    pub fn get(&self, labels: &[(&str, &str)]) -> i64 {
        let key = DynamicLabelSet::from_pairs(labels);
        let index_shard = self.index_shard_for(&key);
        self.index_shards[index_shard]
            .read()
            .get(&key)
            .map(|series| series.sum())
            .unwrap_or(0)
    }

    /// Sums all series.
    pub fn sum_all(&self) -> i64 {
        self.snapshot().into_iter().map(|(_, value)| value).sum()
    }

    /// Returns a snapshot of all label-set/value pairs.
    pub fn snapshot(&self) -> Vec<(DynamicLabelSet, i64)> {
        let mut out = Vec::new();
        for shard in &self.index_shards {
            let guard = shard.read();
            for (labels, series) in guard.iter() {
                out.push((labels.clone(), series.sum()));
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
    /// Calls `f` with borrowed label pairs and the current value for each series.
    /// Used by exporters to avoid the intermediate `snapshot()` allocation.
    pub(crate) fn visit_series(&self, mut f: impl FnMut(&[(String, String)], i64)) {
        for shard in &self.index_shards {
            let guard = shard.read();
            for (labels, series) in guard.iter() {
                f(labels.pairs(), series.sum());
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
    /// holds a DynamicGaugeI64Series handle to them.
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
                // both the map and at least one DynamicGaugeI64Series hold refs)
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

    fn lookup_or_create(&self, labels: &[(&str, &str)]) -> Arc<GaugeI64Series> {
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
        let series = Arc::new(GaugeI64Series::new(self.shard_count, cycle));
        #[cfg(not(feature = "eviction"))]
        let series = Arc::new(GaugeI64Series::new(self.shard_count));
        guard.insert(key, Arc::clone(&series));
        self.series_count.fetch_add(1, Ordering::Relaxed);
        series
    }

    fn index_shard_for(&self, key: &DynamicLabelSet) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) & self.shard_mask
    }

    fn cached_series(&self, labels: &[(&str, &str)]) -> Option<Arc<GaugeI64Series>> {
        SERIES_CACHE.with(|cache| {
            let series = cache.borrow_mut().get(self.id, labels)?;
            #[cfg(feature = "eviction")]
            series.touch(current_cycle());
            Some(series)
        })
    }

    fn update_cache(&self, labels: &[(&str, &str)], series: &Arc<GaugeI64Series>) {
        SERIES_CACHE.with(|cache| {
            cache
                .borrow_mut()
                .insert(self.id, labels, Arc::downgrade(series));
        });
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "eviction")]
    use super::super::advance_cycle;
    use super::*;

    #[test]
    fn test_basic_operations() {
        let gauge = DynamicGaugeI64::new(4);
        gauge.inc(&[("endpoint_id", "ep1")]);
        gauge.add(&[("endpoint_id", "ep1")], 2);

        assert_eq!(gauge.get(&[("endpoint_id", "ep1")]), 3);

        gauge.dec(&[("endpoint_id", "ep1")]);
        assert_eq!(gauge.get(&[("endpoint_id", "ep1")]), 2);

        gauge.add(&[("endpoint_id", "ep1")], -2);
        assert_eq!(gauge.get(&[("endpoint_id", "ep1")]), 0);
    }

    #[test]
    fn test_series_handle() {
        let gauge = DynamicGaugeI64::new(4);
        let series = gauge.series(&[("endpoint_id", "ep1")]);
        series.inc();
        series.inc();
        series.dec();

        assert_eq!(series.get(), 1);
        assert_eq!(gauge.get(&[("endpoint_id", "ep1")]), 1);
    }

    #[test]
    fn test_snapshot() {
        let gauge = DynamicGaugeI64::new(4);
        gauge.add(&[("endpoint_id", "ep1")], 10);
        gauge.add(&[("endpoint_id", "ep2")], 20);

        let snap = gauge.snapshot();
        assert_eq!(snap.len(), 2);

        let total: i64 = snap.iter().map(|(_, v)| v).sum();
        assert_eq!(total, 30);
    }

    #[cfg(feature = "eviction")]
    #[test]
    fn test_evict_stale() {
        let gauge = DynamicGaugeI64::new(4);
        let labels = &[("endpoint_id", "evict_i64")];

        gauge.add(labels, 5);
        assert_eq!(gauge.cardinality(), 1);

        // Advance cycles past staleness threshold
        advance_cycle();
        advance_cycle();

        // Flush thread-local cache by accessing a different label set
        gauge.add(&[("flush", "cache")], 1);

        let removed = gauge.evict_stale(1);
        assert_eq!(removed, 1);
        assert_eq!(gauge.cardinality(), 1); // flush series remains
        assert_eq!(gauge.get(labels), 0);
    }

    #[cfg(feature = "eviction")]
    #[test]
    fn test_series_handle_protects_from_eviction() {
        let gauge = DynamicGaugeI64::new(4);
        let labels = &[("endpoint_id", "tombstone_i64")];

        let series = gauge.series(labels);
        series.add(5);
        assert!(!series.is_evicted());

        // Try to evict - but handle protects the series
        advance_cycle();
        advance_cycle();
        let removed = gauge.evict_stale(1);

        // Handle protects series from eviction (Arc::strong_count > 1)
        assert_eq!(removed, 0);
        assert!(!series.is_evicted());
        assert_eq!(gauge.get(labels), 5);
    }

    #[test]
    fn test_overflow_bucket_routes_new_series_at_capacity() {
        let gauge = DynamicGaugeI64::with_max_series(4, 1);
        gauge.add(&[("endpoint_id", "1")], 1);
        gauge.add(&[("endpoint_id", "2")], 2);

        assert_eq!(gauge.cardinality(), 2);
        assert_eq!(gauge.get(&[(OVERFLOW_LABEL_KEY, OVERFLOW_LABEL_VALUE)]), 2);
    }
}
