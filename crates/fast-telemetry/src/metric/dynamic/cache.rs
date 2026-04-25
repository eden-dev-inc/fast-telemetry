//! Shared thread-local label-cache machinery for dynamic metrics.
//!
//! This module owns the small fixed-size cache used by the dynamic metric
//! types to avoid repeated label canonicalization and index lookups on hot
//! paths. The cache is intentionally narrow in scope:
//!
//! - it caches only exact ordered input label sequences
//! - it stores weak references so cached entries do not keep series alive
//! - it validates the upgraded series on read so eviction tombstones are
//!   respected
//!
//! The metric-specific code still owns slow-path lookup, overflow handling,
//! series construction, and update semantics.

use std::sync::Arc;

/// Number of per-thread cache slots used by dynamic metrics.
///
/// This must remain a power of two because slot selection is mask-based.
pub(crate) const SERIES_CACHE_SIZE: usize = 16;

/// Payload contract for entries stored in [`LabelCache`].
///
/// A cache entry may hold just a `Weak<Series>` or a richer metric-specific
/// payload. On lookup, the cache upgrades the entry into a strong value and
/// then asks the payload whether the upgraded value is still valid.
pub(crate) trait CacheValue {
    /// Strong value returned to the caller on a cache hit.
    type Strong;

    /// Upgrade the cached payload into a strong value, if it is still live.
    fn upgrade(&self) -> Option<Self::Strong>;

    /// Check whether the upgraded value is still valid for hot-path use.
    fn is_valid(strong: &Self::Strong) -> bool;
}

/// Exact-match cache entry for one metric id plus one ordered label sequence.
pub(crate) struct LabelCacheEntry<T> {
    metric_id: usize,
    fingerprint: u64,
    ordered_labels: Vec<(String, String)>,
    value: T,
}

impl<T> LabelCacheEntry<T> {
    fn matches_ordered(&self, metric_id: usize, labels: &[(&str, &str)]) -> bool {
        if self.metric_id != metric_id || self.ordered_labels.len() != labels.len() {
            return false;
        }

        labels.iter().enumerate().all(|(idx, (k, v))| {
            let (ek, ev) = &self.ordered_labels[idx];
            ek == k && ev == v
        })
    }

    fn matches(&self, metric_id: usize, fingerprint: u64, labels: &[(&str, &str)]) -> bool {
        if self.metric_id != metric_id
            || self.fingerprint != fingerprint
            || self.ordered_labels.len() != labels.len()
        {
            return false;
        }

        labels.iter().enumerate().all(|(idx, (k, v))| {
            let (ek, ev) = &self.ordered_labels[idx];
            ek == k && ev == v
        })
    }
}

/// Small fixed-size direct-mapped cache for dynamic metric label lookups.
///
/// The cache keeps a single "last hit" fast path and then falls back to a
/// direct-mapped probe keyed by a lightweight fingerprint of the ordered input
/// labels. This preserves the previous best case for repeated identical calls
/// while substantially improving workloads that rotate across a small hot set.
pub(crate) struct LabelCache<T, const N: usize> {
    last: Option<usize>,
    entries: [Option<LabelCacheEntry<T>>; N],
}

impl<T, const N: usize> LabelCache<T, N>
where
    T: CacheValue,
{
    /// Create an empty cache.
    pub(crate) fn new() -> Self {
        debug_assert!(N.is_power_of_two());
        Self {
            last: None,
            entries: std::array::from_fn(|_| None),
        }
    }

    /// Look up a cached payload for `metric_id` and the exact ordered `labels`.
    pub(crate) fn get(
        &mut self,
        metric_id: usize,
        labels: &[(&str, &str)],
    ) -> Option<<T as CacheValue>::Strong> {
        if let Some(index) = self.last {
            if let Some(value) = self.get_at(index, metric_id, labels) {
                return Some(value);
            }
        }

        let fingerprint = label_fingerprint(labels);
        let index = cache_index::<N>(fingerprint);
        let value = self.get_at_with_fingerprint(index, metric_id, fingerprint, labels)?;
        self.last = Some(index);
        Some(value)
    }

    /// Replace the cached entry for the slot selected by `labels`.
    pub(crate) fn insert(&mut self, metric_id: usize, labels: &[(&str, &str)], value: T) {
        let fingerprint = label_fingerprint(labels);
        let index = cache_index::<N>(fingerprint);
        let ordered_labels = labels
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        self.entries[index] = Some(LabelCacheEntry {
            metric_id,
            fingerprint,
            ordered_labels,
            value,
        });
        self.last = Some(index);
    }

    fn get_at(
        &self,
        index: usize,
        metric_id: usize,
        labels: &[(&str, &str)],
    ) -> Option<<T as CacheValue>::Strong> {
        let entry = self.entries[index].as_ref()?;
        if !entry.matches_ordered(metric_id, labels) {
            return None;
        }
        let strong = entry.value.upgrade()?;
        if !T::is_valid(&strong) {
            return None;
        }
        Some(strong)
    }

    fn get_at_with_fingerprint(
        &self,
        index: usize,
        metric_id: usize,
        fingerprint: u64,
        labels: &[(&str, &str)],
    ) -> Option<<T as CacheValue>::Strong> {
        let entry = self.entries[index].as_ref()?;
        if !entry.matches(metric_id, fingerprint, labels) {
            return None;
        }
        let strong = entry.value.upgrade()?;
        if !T::is_valid(&strong) {
            return None;
        }
        Some(strong)
    }
}

fn cache_index<const N: usize>(fingerprint: u64) -> usize {
    debug_assert!(N.is_power_of_two());
    (fingerprint as usize) & (N - 1)
}

/// Fingerprint an ordered borrowed label slice for direct-mapped slot lookup.
///
/// This is deliberately cheaper than full canonicalization and is only used to
/// select a cache slot. Exact key/value matching still happens before a hit is
/// accepted.
pub(crate) fn label_fingerprint(labels: &[(&str, &str)]) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

    let mut hash = FNV_OFFSET;
    for (k, v) in labels {
        for byte in k.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash ^= 0xff;
        hash = hash.wrapping_mul(FNV_PRIME);
        for byte in v.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash ^= 0xfe;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

impl<T> CacheValue for std::sync::Weak<T>
where
    T: CacheableSeries,
{
    type Strong = Arc<T>;

    fn upgrade(&self) -> Option<Self::Strong> {
        self.upgrade()
    }

    fn is_valid(strong: &Self::Strong) -> bool {
        !strong.is_evicted()
    }
}

/// Shared validity hook for series types that expose an eviction tombstone.
pub(crate) trait CacheableSeries {
    /// Return `true` once the series has been evicted from its metric index.
    fn is_evicted(&self) -> bool;
}
