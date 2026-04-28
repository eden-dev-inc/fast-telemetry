//! Runtime-labeled gauge for dynamic dimensions.

use super::cache::{CacheableSeries, LabelCache, SERIES_CACHE_SIZE};
#[cfg(feature = "eviction")]
use super::current_cycle;
use super::{DynamicLabelSet, GAUGE_IDS};
use crossbeam_utils::CachePadded;
use parking_lot::RwLock;
use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
#[cfg(feature = "eviction")]
use std::sync::atomic::AtomicU32;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

const DEFAULT_MAX_SERIES: usize = 2000;
const OVERFLOW_LABEL_KEY: &str = "__ft_overflow";
const OVERFLOW_LABEL_VALUE: &str = "true";

type GaugeIndexShard = CachePadded<RwLock<HashMap<DynamicLabelSet, Arc<GaugeSeries>>>>;

struct GaugeSeries {
    bits: CachePadded<AtomicU64>,
    /// Tombstone flag set by exporter before removing from map.
    evicted: AtomicBool,
    /// Last export cycle when this series was accessed.
    #[cfg(feature = "eviction")]
    last_accessed_cycle: AtomicU32,
}

impl GaugeSeries {
    #[cfg(feature = "eviction")]
    fn new(cycle: u32) -> Self {
        Self {
            bits: CachePadded::new(AtomicU64::new(0.0_f64.to_bits())),
            evicted: AtomicBool::new(false),
            last_accessed_cycle: AtomicU32::new(cycle),
        }
    }

    #[cfg(not(feature = "eviction"))]
    fn new() -> Self {
        Self {
            bits: CachePadded::new(AtomicU64::new(0.0_f64.to_bits())),
            evicted: AtomicBool::new(false),
        }
    }

    #[inline]
    fn set(&self, value: f64) {
        self.bits.store(value.to_bits(), Ordering::Relaxed);
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
    fn get(&self) -> f64 {
        f64::from_bits(self.bits.load(Ordering::Relaxed))
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

impl CacheableSeries for GaugeSeries {
    fn is_evicted(&self) -> bool {
        self.is_evicted()
    }
}

/// A reusable handle to a dynamic-label gauge series.
///
/// Use this for hot paths to avoid per-update label canonicalization and map
/// lookups. Resolve once with `DynamicGauge::series(...)`, then call `set()`
/// / `get()` on the handle.
#[derive(Clone)]
pub struct DynamicGaugeSeries {
    series: Arc<GaugeSeries>,
}

impl DynamicGaugeSeries {
    /// Set the gauge value.
    #[inline]
    pub fn set(&self, value: f64) {
        self.series.set(value);
    }

    /// Get the current value.
    #[inline]
    pub fn get(&self) -> f64 {
        self.series.get()
    }

    /// Check if this series handle has been evicted.
    #[inline]
    pub fn is_evicted(&self) -> bool {
        self.series.is_evicted()
    }
}

thread_local! {
    static SERIES_CACHE: RefCell<LabelCache<Weak<GaugeSeries>, SERIES_CACHE_SIZE>> =
        RefCell::new(LabelCache::new());
}

/// Gauge keyed by runtime label sets.
///
/// Uses sharded index for key->series lookup for concurrent access.
pub struct DynamicGauge {
    id: usize,
    max_series: usize,
    shard_mask: usize,
    index_shards: Vec<GaugeIndexShard>,
    /// Approximate number of live series (incremented on insert, decremented on evict).
    series_count: AtomicUsize,
    /// Count of records routed to overflow bucket due to cardinality cap.
    overflow_count: AtomicU64,
}

impl DynamicGauge {
    /// Creates a new runtime-labeled gauge.
    pub fn new(shard_count: usize) -> Self {
        Self::with_max_series(shard_count, DEFAULT_MAX_SERIES)
    }

    /// Creates a new runtime-labeled gauge with a series cardinality cap.
    ///
    /// When the number of unique label sets approximately reaches `max_series`,
    /// new label sets are redirected into a single overflow series
    /// (`__ft_overflow=true`). The cap is checked via a lock-free atomic counter,
    /// so concurrent inserts may briefly overshoot by the number of in-flight
    /// writers before the overflow kicks in.
    pub fn with_max_series(shard_count: usize, max_series: usize) -> Self {
        let shard_count = shard_count.next_power_of_two();
        let id = GAUGE_IDS.fetch_add(1, Ordering::Relaxed);
        Self {
            id,
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
    ///
    /// Preferred for hot paths when labels come from a finite active set.
    pub fn series(&self, labels: &[(&str, &str)]) -> DynamicGaugeSeries {
        if let Some(series) = self.cached_series(labels) {
            return DynamicGaugeSeries { series };
        }
        let series = self.lookup_or_create(labels);
        self.update_cache(labels, &series);
        DynamicGaugeSeries { series }
    }

    /// Set the gauge value for the series identified by `labels`.
    #[inline]
    pub fn set(&self, labels: &[(&str, &str)], value: f64) {
        if let Some(series) = self.cached_series(labels) {
            series.set(value);
            return;
        }

        let series = self.lookup_or_create(labels);
        self.update_cache(labels, &series);
        series.set(value);
    }

    /// Get the current value for the series identified by `labels`.
    pub fn get(&self, labels: &[(&str, &str)]) -> f64 {
        let key = DynamicLabelSet::from_pairs(labels);
        let index_shard = self.index_shard_for(&key);
        self.index_shards[index_shard]
            .read()
            .get(&key)
            .map(|series| series.get())
            .unwrap_or(0.0)
    }

    /// Returns a snapshot of all label-set/value pairs.
    pub fn snapshot(&self) -> Vec<(DynamicLabelSet, f64)> {
        let mut out = Vec::new();
        for shard in &self.index_shards {
            let guard = shard.read();
            for (labels, series) in guard.iter() {
                out.push((labels.clone(), series.get()));
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

    /// Iterate all series without doing heavy work under the read lock.
    ///
    /// Snapshots under the lock, then invokes `f` outside; see [`DynamicCounter::visit_series`].
    pub(crate) fn visit_series(&self, mut f: impl FnMut(&[(String, String)], f64)) {
        for shard in &self.index_shards {
            let snapshot: Vec<(DynamicLabelSet, f64)> = {
                let guard = shard.read();
                guard
                    .iter()
                    .map(|(labels, series)| (labels.clone(), series.get()))
                    .collect()
            };
            for (labels, value) in &snapshot {
                f(labels.pairs(), *value);
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
    /// holds a DynamicGaugeSeries handle to them.
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
                // both the map and at least one DynamicGaugeSeries hold refs)
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

    fn lookup_or_create(&self, labels: &[(&str, &str)]) -> Arc<GaugeSeries> {
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
        let series = Arc::new(GaugeSeries::new(cycle));
        #[cfg(not(feature = "eviction"))]
        let series = Arc::new(GaugeSeries::new());
        guard.insert(key, Arc::clone(&series));
        self.series_count.fetch_add(1, Ordering::Relaxed);
        series
    }

    fn index_shard_for(&self, key: &DynamicLabelSet) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) & self.shard_mask
    }

    fn cached_series(&self, labels: &[(&str, &str)]) -> Option<Arc<GaugeSeries>> {
        SERIES_CACHE.with(|cache| {
            let series = cache.borrow_mut().get(self.id, labels)?;
            #[cfg(feature = "eviction")]
            series.touch(current_cycle());
            Some(series)
        })
    }

    fn update_cache(&self, labels: &[(&str, &str)], series: &Arc<GaugeSeries>) {
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
    fn test_basic_operations() {
        let gauge = DynamicGauge::new(4);
        gauge.set(&[("org_id", "42"), ("endpoint_uuid", "abc")], 100.5);

        assert!(
            (gauge.get(&[("org_id", "42"), ("endpoint_uuid", "abc")]) - 100.5).abs() < f64::EPSILON
        );
    }

    #[test]
    fn test_label_order_is_canonicalized() {
        let gauge = DynamicGauge::new(4);
        gauge.set(&[("org_id", "42"), ("endpoint_uuid", "abc")], 50.0);

        assert!(
            (gauge.get(&[("endpoint_uuid", "abc"), ("org_id", "42")]) - 50.0).abs() < f64::EPSILON
        );
    }

    #[test]
    fn test_series_handle() {
        let gauge = DynamicGauge::new(4);
        let series = gauge.series(&[("org_id", "42"), ("endpoint_uuid", "abc")]);
        series.set(123.456);

        assert!((series.get() - 123.456).abs() < f64::EPSILON);
        assert!(
            (gauge.get(&[("org_id", "42"), ("endpoint_uuid", "abc")]) - 123.456).abs()
                < f64::EPSILON
        );
    }

    #[test]
    fn test_snapshot() {
        let gauge = DynamicGauge::new(4);
        gauge.set(&[("org_id", "1")], 10.0);
        gauge.set(&[("org_id", "2")], 20.0);

        let snap = gauge.snapshot();
        assert_eq!(snap.len(), 2);

        let total: f64 = snap.iter().map(|(_, v)| v).sum();
        assert!((total - 30.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_overflow_bucket_routes_new_series_at_capacity() {
        let gauge = DynamicGauge::with_max_series(4, 1);
        gauge.set(&[("org_id", "1")], 1.0);
        gauge.set(&[("org_id", "2")], 2.0);

        assert_eq!(gauge.cardinality(), 2);
        assert!(
            (gauge.get(&[(OVERFLOW_LABEL_KEY, OVERFLOW_LABEL_VALUE)]) - 2.0).abs() < f64::EPSILON
        );
    }
}
