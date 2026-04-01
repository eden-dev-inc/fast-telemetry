//! Runtime-labeled distribution for dynamic dimensions.
//!
//! Uses base-2 exponential histogram buckets per label set, matching the
//! `Distribution` implementation.  Each label set × thread gets its own
//! fixed-size bucket array.

use super::{DISTRIBUTION_IDS, DynamicLabelSet, current_cycle};
use crate::exp_buckets::{ExpBuckets, ExpBucketsSnapshot};
use crossbeam_utils::CachePadded;
use parking_lot::{Mutex, RwLock};
use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::Weak;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};

const DEFAULT_MAX_SERIES: usize = 2000;
const OVERFLOW_LABEL_KEY: &str = "__ft_overflow";
const OVERFLOW_LABEL_VALUE: &str = "true";
type DistributionIndexShard =
    CachePadded<RwLock<HashMap<DynamicLabelSet, Arc<DistributionSeries>>>>;
type DistributionSnapshotEntry = (DynamicLabelSet, u64, u64, ExpBucketsSnapshot);
static SERIES_IDS: AtomicUsize = AtomicUsize::new(1);

struct DistributionSeries {
    id: usize,
    registry: Mutex<Vec<Arc<ExpBuckets>>>,
    /// Tombstone flag set by exporter before removing from map.
    evicted: AtomicBool,
    /// Last export cycle when this series was accessed.
    last_accessed_cycle: AtomicU32,
}

impl DistributionSeries {
    fn new(cycle: u32) -> Self {
        Self {
            id: SERIES_IDS.fetch_add(1, Ordering::Relaxed),
            registry: Mutex::new(Vec::new()),
            evicted: AtomicBool::new(false),
            last_accessed_cycle: AtomicU32::new(cycle),
        }
    }

    /// Touch the series timestamp. Called on slow path only.
    #[inline]
    fn touch(&self, cycle: u32) {
        self.last_accessed_cycle.store(cycle, Ordering::Relaxed);
    }

    #[inline]
    fn is_evicted(&self) -> bool {
        self.evicted.load(Ordering::Relaxed)
    }

    fn mark_evicted(&self) {
        self.evicted.store(true, Ordering::Relaxed);
    }

    fn get_or_create_buf(&self) -> Arc<ExpBuckets> {
        let buf = Arc::new(ExpBuckets::new());
        self.registry.lock().push(Arc::clone(&buf));
        buf
    }

    fn count(&self) -> u64 {
        self.registry.lock().iter().map(|buf| buf.get_count()).sum()
    }

    fn sum(&self) -> u64 {
        self.registry.lock().iter().map(|buf| buf.get_sum()).sum()
    }

    fn buckets_snapshot(&self) -> ExpBucketsSnapshot {
        let mut positive = [0u64; 64];
        let mut zero_count = 0u64;
        let mut sum = 0u64;
        let mut count = 0u64;

        let registry = self.registry.lock();
        for buf in registry.iter() {
            let thread_buckets = buf.get_positive_buckets();
            for (i, &c) in thread_buckets.iter().enumerate() {
                positive[i] += c;
            }
            zero_count += buf.get_zero_count();
            sum += buf.get_sum();
            count += buf.get_count();
        }

        ExpBucketsSnapshot {
            positive,
            zero_count,
            sum,
            count,
        }
    }
}

/// A reusable handle to a dynamic-label distribution series.
///
/// Use this for hot paths to avoid per-update label canonicalization and map
/// lookups. Resolve once with `DynamicDistribution::series(...)`, then call
/// `record()` on the handle.
#[derive(Clone)]
pub struct DynamicDistributionSeries {
    series: Arc<DistributionSeries>,
    buf: Arc<ExpBuckets>,
}

impl DynamicDistributionSeries {
    /// Record a value.
    #[inline]
    pub fn record(&self, value: u64) {
        self.buf.record(value);
    }

    /// Get the count across all threads for this series.
    pub fn count(&self) -> u64 {
        self.series.count()
    }

    /// Get the sum across all threads for this series.
    pub fn sum(&self) -> u64 {
        self.series.sum()
    }

    /// Check if this series handle has been evicted.
    #[inline]
    pub fn is_evicted(&self) -> bool {
        self.series.is_evicted()
    }
}

struct SeriesCacheEntry {
    distribution_id: usize,
    ordered_labels: Vec<(String, String)>,
    series: Weak<DistributionSeries>,
    buf: Arc<ExpBuckets>,
}

thread_local! {
    static SERIES_CACHE: RefCell<Option<SeriesCacheEntry>> = const { RefCell::new(None) };
    static SERIES_BUF_CACHE: RefCell<Vec<(usize, usize, Weak<ExpBuckets>)>> = const { RefCell::new(Vec::new()) };
}

/// Distribution keyed by runtime label sets.
///
/// Each label set gets its own set of thread-local exponential histogram buckets.
pub struct DynamicDistribution {
    id: usize,
    max_series: usize,
    shard_mask: usize,
    index_shards: Vec<DistributionIndexShard>,
    /// Approximate number of live series (incremented on insert, decremented on evict).
    series_count: AtomicUsize,
    /// Count of records routed to overflow bucket due to cardinality cap.
    overflow_count: AtomicU64,
}

impl DynamicDistribution {
    /// Creates a new runtime-labeled distribution with default cardinality cap.
    pub fn new(shard_count: usize) -> Self {
        Self::with_max_series(shard_count, DEFAULT_MAX_SERIES)
    }

    /// Creates a new runtime-labeled distribution with a custom cardinality cap.
    pub fn with_max_series(shard_count: usize, max_series: usize) -> Self {
        let shard_count = shard_count.next_power_of_two();
        let id = DISTRIBUTION_IDS.fetch_add(1, Ordering::Relaxed);
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
    pub fn series(&self, labels: &[(&str, &str)]) -> DynamicDistributionSeries {
        if let Some((series, buf)) = self.cached_series(labels) {
            return DynamicDistributionSeries { series, buf };
        }
        let series = self.lookup_or_create(labels);
        let buf = self.get_or_create_thread_buf(&series);
        self.update_cache(labels, Arc::clone(&series), Arc::clone(&buf));
        DynamicDistributionSeries { series, buf }
    }

    /// Record a value for the series identified by `labels`.
    #[inline]
    pub fn record(&self, labels: &[(&str, &str)], value: u64) {
        if let Some((_series, buf)) = self.cached_series(labels) {
            buf.record(value);
            return;
        }

        let series = self.lookup_or_create(labels);
        let buf = self.get_or_create_thread_buf(&series);
        self.update_cache(labels, Arc::clone(&series), Arc::clone(&buf));
        buf.record(value);
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

    /// Returns a snapshot of all label-sets with their stats.
    pub fn snapshot(&self) -> Vec<DistributionSnapshotEntry> {
        let mut out = Vec::new();
        for shard in &self.index_shards {
            let guard = shard.read();
            for (labels, series) in guard.iter() {
                let snap = series.buckets_snapshot();
                out.push((labels.clone(), snap.count, snap.sum, snap));
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
    /// Calls `f` with borrowed label pairs, count, sum, and bucket snapshot
    /// for each series. Used by exporters/macros to avoid `snapshot()` cloning.
    #[doc(hidden)]
    pub fn visit_series(
        &self,
        mut f: impl FnMut(&[(String, String)], u64, u64, ExpBucketsSnapshot),
    ) {
        for shard in &self.index_shards {
            let guard = shard.read();
            for (labels, series) in guard.iter() {
                let snap = series.buckets_snapshot();
                f(labels.pairs(), snap.count, snap.sum, snap);
            }
        }
    }

    /// Evict series that haven't been accessed for `max_staleness` cycles.
    ///
    /// Call this after `advance_cycle()` in your sweeper task.
    /// Series are marked as evicted (so cached handles see the tombstone),
    /// then removed from the index.
    ///
    /// Protected series (Arc::strong_count > 1) are never evicted — someone
    /// holds a DynamicDistributionSeries handle to them.
    ///
    /// Returns the number of series evicted.
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

    fn lookup_or_create(&self, labels: &[(&str, &str)]) -> Arc<DistributionSeries> {
        let requested_key = DynamicLabelSet::from_pairs(labels);
        let requested_shard = self.index_shard_for(&requested_key);
        let cycle = current_cycle();

        // Fast path: read lock only.
        if let Some(series) = self.index_shards[requested_shard]
            .read()
            .get(&requested_key)
        {
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
            series.touch(cycle);
            return Arc::clone(series);
        }

        let mut guard = self.index_shards[shard].write();
        if let Some(series) = guard.get(&key) {
            series.touch(cycle);
            return Arc::clone(series);
        }
        let series = Arc::new(DistributionSeries::new(cycle));
        guard.insert(key, Arc::clone(&series));
        self.series_count.fetch_add(1, Ordering::Relaxed);
        series
    }

    fn index_shard_for(&self, key: &DynamicLabelSet) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) & self.shard_mask
    }

    fn cached_series(
        &self,
        labels: &[(&str, &str)],
    ) -> Option<(Arc<DistributionSeries>, Arc<ExpBuckets>)> {
        SERIES_CACHE.with(|cache| {
            let cache_ref = cache.borrow();
            let entry = cache_ref.as_ref()?;
            if entry.distribution_id != self.id {
                return None;
            }
            if entry.ordered_labels.len() != labels.len() {
                return None;
            }
            for (idx, (k, v)) in labels.iter().enumerate() {
                let (ek, ev) = &entry.ordered_labels[idx];
                if ek != k || ev != v {
                    return None;
                }
            }
            let series = entry.series.upgrade()?;
            // Check tombstone - forces re-lookup if series was evicted
            if series.is_evicted() {
                return None;
            }
            series.touch(current_cycle());
            Some((series, Arc::clone(&entry.buf)))
        })
    }

    fn update_cache(
        &self,
        labels: &[(&str, &str)],
        series: Arc<DistributionSeries>,
        buf: Arc<ExpBuckets>,
    ) {
        SERIES_CACHE.with(|cache| {
            let ordered_labels = labels
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect();
            *cache.borrow_mut() = Some(SeriesCacheEntry {
                distribution_id: self.id,
                ordered_labels,
                series: Arc::downgrade(&series),
                buf,
            });
        });
    }

    fn get_or_create_thread_buf(&self, series: &Arc<DistributionSeries>) -> Arc<ExpBuckets> {
        let dist_id = self.id;
        let series_id = series.id;

        SERIES_BUF_CACHE.with(|cache| {
            let mut entries = cache.borrow_mut();
            entries.retain(|(_id, _ptr, weak)| weak.strong_count() > 0);

            for (id, ptr, weak) in entries.iter() {
                if *id == dist_id
                    && *ptr == series_id
                    && let Some(buf) = weak.upgrade()
                {
                    return buf;
                }
            }

            let buf = series.get_or_create_buf();
            entries.push((dist_id, series_id, Arc::downgrade(&buf)));
            buf
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_recording() {
        let dist = DynamicDistribution::new(4);
        let labels = &[("org_id", "42")];

        dist.record(labels, 100);
        dist.record(labels, 200);
        dist.record(labels, 300);

        assert_eq!(dist.count(labels), 3);
        assert_eq!(dist.sum(labels), 600);
    }

    #[test]
    fn test_label_order_is_canonicalized() {
        let dist = DynamicDistribution::new(4);

        dist.record(&[("org_id", "42"), ("endpoint", "abc")], 100);

        assert_eq!(dist.count(&[("endpoint", "abc"), ("org_id", "42")]), 1);
    }

    #[test]
    fn test_series_handle() {
        let dist = DynamicDistribution::new(4);
        let series = dist.series(&[("org_id", "42")]);

        series.record(100);
        series.record(200);

        assert_eq!(series.count(), 2);
        assert_eq!(series.sum(), 300);
        assert_eq!(dist.count(&[("org_id", "42")]), 2);
    }

    #[test]
    fn test_multiple_label_sets() {
        let dist = DynamicDistribution::new(4);

        dist.record(&[("org_id", "1")], 100);
        dist.record(&[("org_id", "2")], 200);

        assert_eq!(dist.count(&[("org_id", "1")]), 1);
        assert_eq!(dist.count(&[("org_id", "2")]), 1);

        let snap = dist.snapshot();
        assert_eq!(snap.len(), 2);
    }

    #[test]
    fn test_overflow_bucket_routes_new_series_at_capacity() {
        let dist = DynamicDistribution::with_max_series(4, 2);

        dist.record(&[("org_id", "1")], 100);
        dist.record(&[("org_id", "2")], 200);
        // Third label set should overflow
        dist.record(&[("org_id", "3")], 300);

        assert_eq!(dist.cardinality(), 3); // 2 real + 1 overflow
        assert!(dist.overflow_count() > 0);
        assert_eq!(dist.count(&[(OVERFLOW_LABEL_KEY, OVERFLOW_LABEL_VALUE)]), 1);
        assert_eq!(dist.sum(&[(OVERFLOW_LABEL_KEY, OVERFLOW_LABEL_VALUE)]), 300);
    }

    #[test]
    fn test_snapshot_includes_buckets() {
        let dist = DynamicDistribution::new(4);
        dist.record(&[("org_id", "1")], 100);
        dist.record(&[("org_id", "1")], 200);

        let snap = dist.snapshot();
        assert_eq!(snap.len(), 1);
        let (_, count, sum, bucket_snap) = &snap[0];
        assert_eq!(*count, 2);
        assert_eq!(*sum, 300);
        // Both 100 and 200 land in bucket 6 and 7 respectively
        assert!(bucket_snap.positive[6] > 0 || bucket_snap.positive[7] > 0);
    }
}
