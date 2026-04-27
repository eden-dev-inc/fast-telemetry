//! Runtime-labeled counter for dynamic dimensions.

use super::cache::{CacheableSeries, LabelCache, SERIES_CACHE_SIZE};
#[cfg(feature = "eviction")]
use super::current_cycle;
use super::{COUNTER_IDS, DynamicLabelSet, thread_id};
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

struct CounterSeries {
    cells: Vec<CachePadded<AtomicIsize>>,
    /// Tombstone flag set by exporter before removing from map.
    /// Checked in cached_series() to invalidate stale cache entries.
    evicted: AtomicBool,
    /// Last export cycle when this series was accessed.
    /// Used for staleness-based eviction.
    #[cfg(feature = "eviction")]
    last_accessed_cycle: AtomicU32,
}

type CounterIndexShard = CachePadded<RwLock<HashMap<DynamicLabelSet, Arc<CounterSeries>>>>;

impl CounterSeries {
    #[cfg(feature = "eviction")]
    fn new(shard_count: usize, current_cycle: u32) -> Self {
        Self {
            cells: (0..shard_count)
                .map(|_| CachePadded::new(AtomicIsize::new(0)))
                .collect(),
            evicted: AtomicBool::new(false),
            last_accessed_cycle: AtomicU32::new(current_cycle),
        }
    }

    #[cfg(not(feature = "eviction"))]
    fn new(shard_count: usize) -> Self {
        Self {
            cells: (0..shard_count)
                .map(|_| CachePadded::new(AtomicIsize::new(0)))
                .collect(),
            evicted: AtomicBool::new(false),
        }
    }

    #[inline]
    fn add_at(&self, shard_idx: usize, value: isize) {
        self.cells[shard_idx].fetch_add(value, Ordering::Relaxed);
        // Note: timestamp updated on slow path (lookup/cache miss) to avoid
        // global atomic read on every increment.
    }

    /// Touch the series timestamp. Called on slow path only.
    #[cfg(feature = "eviction")]
    #[inline]
    fn touch(&self, cycle: u32) {
        self.last_accessed_cycle.store(cycle, Ordering::Relaxed);
    }

    #[inline]
    fn sum(&self) -> isize {
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

impl CacheableSeries for CounterSeries {
    fn is_evicted(&self) -> bool {
        self.is_evicted()
    }
}

/// A reusable handle to a dynamic-label counter series.
///
/// Use this for hot paths to avoid per-update label canonicalization and map
/// lookups. Resolve once with `DynamicCounter::series(...)`, then call `inc()`
/// / `add()` on the handle.
#[derive(Clone)]
pub struct DynamicCounterSeries {
    series: Arc<CounterSeries>,
    shard_mask: usize,
}

impl DynamicCounterSeries {
    /// Increment this series by 1.
    #[inline]
    pub fn inc(&self) {
        self.add(1);
    }

    /// Add `value` to this series.
    #[inline]
    pub fn add(&self, value: isize) {
        let shard_idx = thread_id() & self.shard_mask;
        self.series.add_at(shard_idx, value);
    }

    /// Get this series total across shards.
    #[inline]
    pub fn get(&self) -> isize {
        self.series.sum()
    }

    /// Check if this series handle has been evicted.
    ///
    /// If true, writes go to a detached series that is no longer exported.
    /// Callers holding long-lived handles can check this and re-resolve
    /// via `DynamicCounter::series()` if needed.
    #[inline]
    pub fn is_evicted(&self) -> bool {
        self.series.is_evicted()
    }
}

thread_local! {
    static SERIES_CACHE: RefCell<LabelCache<Weak<CounterSeries>, SERIES_CACHE_SIZE>> =
        RefCell::new(LabelCache::new());
}

/// Counter keyed by runtime label sets.
///
/// Uses a sharded index for key->series lookup and per-series sharded atomics
/// for fast updates.
pub struct DynamicCounter {
    id: usize,
    shard_count: usize,
    max_series: usize,
    shard_mask: usize,
    index_shards: Vec<CounterIndexShard>,
    /// Approximate total series count across all shards for fast cap checks.
    series_count: AtomicUsize,
    /// Count of records routed to overflow bucket due to cardinality cap.
    overflow_count: AtomicU64,
}

impl DynamicCounter {
    /// Creates a new runtime-labeled counter.
    pub fn new(shard_count: usize) -> Self {
        Self::with_max_series(shard_count, DEFAULT_MAX_SERIES)
    }

    /// Creates a new runtime-labeled counter with a series cardinality cap.
    ///
    /// When the number of unique label sets approximately reaches `max_series`,
    /// new label sets are redirected into a single overflow series
    /// (`__ft_overflow=true`). The cap is checked via a lock-free atomic counter,
    /// so concurrent inserts may briefly overshoot by the number of in-flight
    /// writers before the overflow kicks in.
    pub fn with_max_series(shard_count: usize, max_series: usize) -> Self {
        let shard_count = shard_count.next_power_of_two();
        let id = COUNTER_IDS.fetch_add(1, Ordering::Relaxed);
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
    ///
    /// Preferred for hot paths when labels come from a finite active set.
    pub fn series(&self, labels: &[(&str, &str)]) -> DynamicCounterSeries {
        if let Some(series) = self.cached_series(labels) {
            return DynamicCounterSeries {
                series,
                shard_mask: self.shard_mask,
            };
        }
        let series = self.lookup_or_create(labels);
        self.update_cache(labels, &series);
        DynamicCounterSeries {
            series,
            shard_mask: self.shard_mask,
        }
    }

    /// Increments the series identified by `labels` by 1.
    #[inline]
    pub fn inc(&self, labels: &[(&str, &str)]) {
        self.add(labels, 1);
    }

    /// Adds `value` to the series identified by `labels`.
    #[inline]
    pub fn add(&self, labels: &[(&str, &str)], value: isize) {
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

    /// Gets the current value for the series identified by `labels`.
    pub fn get(&self, labels: &[(&str, &str)]) -> isize {
        let key = DynamicLabelSet::from_pairs(labels);
        let index_shard = self.index_shard_for(&key);
        self.index_shards[index_shard]
            .read()
            .get(&key)
            .map(|series| series.sum())
            .unwrap_or(0)
    }

    /// Sums all series.
    pub fn sum_all(&self) -> isize {
        self.snapshot().into_iter().map(|(_, value)| value).sum()
    }

    /// Returns a snapshot of all label-set/count pairs.
    pub fn snapshot(&self) -> Vec<(DynamicLabelSet, isize)> {
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
    /// Calls `f` with borrowed label pairs and the current sum for each series.
    /// Used by exporters/macros to avoid the intermediate `snapshot()` allocation.
    #[doc(hidden)]
    pub fn visit_series(&self, mut f: impl FnMut(&[(String, String)], isize)) {
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
    /// holds a DynamicCounterSeries handle to them.
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
                // both the map and at least one DynamicCounterSeries hold refs)
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

    fn lookup_or_create(&self, labels: &[(&str, &str)]) -> Arc<CounterSeries> {
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
        // series_count is approximate — concurrent inserts may briefly exceed the
        // cap by the number of in-flight writers, but it cannot deadlock and the
        // overshoot is bounded by thread count, not by workload cardinality.
        let key = if self.max_series > 0
            && self.series_count.load(Ordering::Relaxed) >= self.max_series
        {
            self.overflow_count.fetch_add(1, Ordering::Relaxed);
            DynamicLabelSet::from_pairs(&[(OVERFLOW_LABEL_KEY, OVERFLOW_LABEL_VALUE)])
        } else {
            requested_key
        };
        let shard = self.index_shard_for(&key);

        // Check read lock on the (possibly redirected) shard.
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
        let series = Arc::new(CounterSeries::new(self.shard_count, cycle));
        #[cfg(not(feature = "eviction"))]
        let series = Arc::new(CounterSeries::new(self.shard_count));
        guard.insert(key, Arc::clone(&series));
        self.series_count.fetch_add(1, Ordering::Relaxed);
        series
    }

    fn index_shard_for(&self, key: &DynamicLabelSet) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) & self.shard_mask
    }

    fn cached_series(&self, labels: &[(&str, &str)]) -> Option<Arc<CounterSeries>> {
        SERIES_CACHE.with(|cache| {
            let series = cache.borrow_mut().get(self.id, labels)?;
            #[cfg(feature = "eviction")]
            series.touch(current_cycle());
            Some(series)
        })
    }

    fn update_cache(&self, labels: &[(&str, &str)], series: &Arc<CounterSeries>) {
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
        let counter = DynamicCounter::new(4);
        counter.inc(&[("org_id", "42"), ("endpoint_uuid", "abc")]);
        counter.add(&[("org_id", "42"), ("endpoint_uuid", "abc")], 2);

        assert_eq!(
            counter.get(&[("org_id", "42"), ("endpoint_uuid", "abc")]),
            3
        );
        assert_eq!(counter.sum_all(), 3);
    }

    #[test]
    fn test_label_order_is_canonicalized() {
        let counter = DynamicCounter::new(4);
        counter.inc(&[("org_id", "42"), ("endpoint_uuid", "abc")]);

        assert_eq!(
            counter.get(&[("endpoint_uuid", "abc"), ("org_id", "42")]),
            1
        );
    }

    #[test]
    fn test_series_handle() {
        let counter = DynamicCounter::new(4);
        let series = counter.series(&[("org_id", "42"), ("endpoint_uuid", "abc")]);
        series.inc();
        series.add(9);

        assert_eq!(series.get(), 10);
        assert_eq!(
            counter.get(&[("org_id", "42"), ("endpoint_uuid", "abc")]),
            10
        );
    }

    #[test]
    fn test_concurrent_adds() {
        let counter = DynamicCounter::new(8);
        let series = counter.series(&[("org_id", "42"), ("endpoint_uuid", "abc")]);

        std::thread::scope(|s| {
            for _ in 0..8 {
                let series = series.clone();
                s.spawn(move || {
                    for _ in 0..10_000 {
                        series.inc();
                    }
                });
            }
        });

        assert_eq!(
            counter.get(&[("org_id", "42"), ("endpoint_uuid", "abc")]),
            80_000
        );
    }

    #[cfg(feature = "eviction")]
    #[test]
    fn test_evict_stale() {
        let counter = DynamicCounter::new(4);
        let labels = &[("org_id", "42")];

        // Create series and increment
        counter.inc(labels);
        assert_eq!(counter.cardinality(), 1);
        assert_eq!(counter.get(labels), 1);

        // Advance cycle past staleness threshold
        advance_cycle();
        advance_cycle();

        // Flush thread-local cache by accessing a different label set
        counter.inc(&[("flush", "cache")]);

        // Evict series not accessed in last 1 cycle
        let removed = counter.evict_stale(1);
        assert_eq!(removed, 1); // Only the original label set, not the flush one
        assert_eq!(counter.cardinality(), 1); // flush series remains

        // Series is gone - get returns 0
        assert_eq!(counter.get(labels), 0);

        // New inc creates fresh series
        counter.inc(labels);
        assert_eq!(counter.cardinality(), 2);
        assert_eq!(counter.get(labels), 1);
    }

    #[cfg(feature = "eviction")]
    #[test]
    fn test_evict_stale_keeps_active() {
        let counter = DynamicCounter::new(4);
        let active = &[("status", "active")];
        let stale = &[("status", "stale")];

        // Create both series
        counter.inc(active);
        counter.inc(stale);
        assert_eq!(counter.cardinality(), 2);

        // Advance cycle
        advance_cycle();

        // Touch only the active series
        counter.inc(active);

        // Advance again
        advance_cycle();

        // Evict with staleness of 1 - should only evict 'stale'
        let removed = counter.evict_stale(1);
        assert_eq!(removed, 1);
        assert_eq!(counter.cardinality(), 1);
        assert_eq!(counter.get(active), 2);
        assert_eq!(counter.get(stale), 0);
    }

    #[cfg(feature = "eviction")]
    #[test]
    fn test_eviction_tombstone_invalidates_cache() {
        let counter = DynamicCounter::new(4);
        let labels = &[("org_id", "evict_test")];

        // Populate the thread-local cache
        counter.inc(labels);
        counter.inc(labels); // Second call uses cached series
        assert_eq!(counter.get(labels), 2);

        // Force eviction by advancing cycles
        advance_cycle();
        advance_cycle();

        // Flush thread-local cache by accessing a different label set
        counter.inc(&[("flush", "cache")]);

        counter.evict_stale(1);

        // Next inc should create fresh series (tombstone invalidates cache)
        counter.inc(labels);
        assert_eq!(counter.get(labels), 1); // Fresh series starts at 1, not 3
    }

    #[cfg(feature = "eviction")]
    #[test]
    fn test_series_handle_protects_from_eviction() {
        let counter = DynamicCounter::new(4);
        let labels = &[("org_id", "handle_test")];

        // Get a long-lived handle
        let series = counter.series(labels);
        series.inc();
        assert!(!series.is_evicted());

        // Try to evict - but handle protects the series
        advance_cycle();
        advance_cycle();
        let removed = counter.evict_stale(1);

        // Handle protects series from eviction (Arc::strong_count > 1)
        assert_eq!(removed, 0);
        assert!(!series.is_evicted());
        assert_eq!(counter.cardinality(), 1);
        assert_eq!(counter.get(labels), 1);

        // Writes still work
        series.inc();
        assert_eq!(counter.get(labels), 2);
    }

    #[cfg(feature = "eviction")]
    #[test]
    fn test_series_evicted_after_handle_dropped() {
        let counter = DynamicCounter::new(4);
        let labels = &[("org_id", "handle_drop_test")];

        // Create series via handle, then drop it
        {
            let series = counter.series(labels);
            series.inc();
        }
        // Handle dropped, but thread-local cache still holds reference

        assert_eq!(counter.cardinality(), 1);
        assert_eq!(counter.get(labels), 1);

        // Advance cycles
        advance_cycle();
        advance_cycle();

        // Flush thread-local cache by accessing a different label set
        counter.inc(&[("flush", "cache")]);

        // Now eviction should work
        let removed = counter.evict_stale(1);
        assert_eq!(removed, 1);
        assert_eq!(counter.get(labels), 0);
    }

    #[test]
    fn test_overflow_bucket_routes_new_series_at_capacity() {
        let counter = DynamicCounter::with_max_series(4, 2);

        counter.inc(&[("org_id", "1")]);
        counter.inc(&[("org_id", "2")]);
        counter.inc(&[("org_id", "3")]);

        assert_eq!(counter.cardinality(), 3);
        assert_eq!(
            counter.get(&[(OVERFLOW_LABEL_KEY, OVERFLOW_LABEL_VALUE)]),
            1
        );
    }

    #[test]
    fn test_concurrent_cap_bounded_overshoot() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        let cap = 10;
        let threads = 16;
        let counter = Arc::new(DynamicCounter::with_max_series(4, cap));
        let barrier = Arc::new(Barrier::new(threads));

        let handles: Vec<_> = (0..threads)
            .map(|t| {
                let counter = Arc::clone(&counter);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    // Each thread creates a unique label set
                    for i in 0..5 {
                        let label = format!("t{t}_s{i}");
                        counter.inc(&[("key", &label)]);
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        let card = counter.cardinality();
        // Cap is approximate: may overshoot by at most thread count, but must
        // not grow unboundedly (80 distinct labels were attempted).
        assert!(
            card <= cap + threads + 1, // +1 for the overflow bucket
            "cardinality {card} exceeded bounded overshoot (cap={cap}, threads={threads})"
        );
        // Must have hit overflow at least once
        assert!(
            counter.overflow_count() > 0,
            "overflow should have triggered"
        );
    }

    #[cfg(feature = "eviction")]
    #[test]
    fn test_eviction_and_reinsertion_bookkeeping() {
        let counter = DynamicCounter::with_max_series(4, 3);

        counter.inc(&[("k", "a")]);
        counter.inc(&[("k", "b")]);
        counter.inc(&[("k", "c")]);
        assert_eq!(counter.cardinality(), 3);

        counter.inc(&[("k", "d")]);
        assert!(counter.overflow_count() > 0);
        let card_after_overflow = counter.cardinality();
        assert!(card_after_overflow <= 4);

        advance_cycle();
        advance_cycle();
        advance_cycle();
        counter.inc(&[("flush", "cache")]);
        let evicted = counter.evict_stale(1);
        assert!(evicted > 0);

        let card_after_evict = counter.cardinality();
        assert!(
            card_after_evict < card_after_overflow,
            "cardinality should decrease after eviction: before={card_after_overflow} after={card_after_evict}"
        );

        let overflow_before = counter.overflow_count();
        counter.inc(&[("k", "new1")]);
        counter.inc(&[("k", "new2")]);

        assert!(counter.cardinality() <= 5);

        let overflow_after = counter.overflow_count();
        assert!(
            overflow_after - overflow_before <= 1,
            "unexpected overflow after eviction freed space"
        );
    }
}
