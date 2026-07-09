//! Runtime-labeled metrics for dynamic dimensions.
//!
//! Use these types when label values are not known at compile time
//! (e.g. `endpoint_uuid`, `org_id`, `user_id`).
//!
//! Each type provides:
//! - Thread-local caching for hot paths
//! - Sharded index for concurrent access
//! - Label canonicalization (order-independent)
//! - Series handles for zero-lookup repeated access
//! - Access-timestamp eviction for bounded cardinality

mod cache;
mod counter;
mod counter_set;
mod distribution;
mod gauge;
mod gauge_i64;
mod histogram;

pub use counter::{DynamicCounter, DynamicCounterSeries};
pub use counter_set::{DynamicCounterSet, DynamicCounterSetSeries};
pub use distribution::{DynamicDistribution, DynamicDistributionSeries};
pub use gauge::{DynamicGauge, DynamicGaugeSeries};
pub use gauge_i64::{DynamicGaugeI64, DynamicGaugeI64Series};
pub use histogram::{DynamicHistogram, DynamicHistogramSeries, DynamicHistogramSeriesView};

use std::collections::BTreeMap;
use std::sync::atomic::AtomicUsize;
#[cfg(feature = "eviction")]
use std::sync::atomic::{AtomicU32, Ordering};

pub(crate) use crate::thread_id::thread_id;

#[cfg(feature = "eviction")]
static EVICTION_CYCLE: AtomicU32 = AtomicU32::new(0);

#[cfg(all(test, feature = "eviction"))]
static EVICTION_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

const VEC_CANONICALIZATION_MAX_LABELS: usize = 16;

/// Get the current eviction cycle.
#[cfg(feature = "eviction")]
#[inline]
pub fn current_cycle() -> u32 {
    EVICTION_CYCLE.load(Ordering::Relaxed)
}

/// Advance the eviction cycle by 1 and return the new value.
///
/// Call this from your exporter task before calling `evict_stale()` on metrics.
#[cfg(feature = "eviction")]
#[inline]
pub fn advance_cycle() -> u32 {
    EVICTION_CYCLE.fetch_add(1, Ordering::Relaxed) + 1
}

#[cfg(all(test, feature = "eviction"))]
pub(crate) fn lock_eviction_cycle_for_test() -> std::sync::MutexGuard<'static, ()> {
    let guard = EVICTION_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    EVICTION_CYCLE.store(0, Ordering::Relaxed);
    guard
}

/// Canonicalized runtime label set.
///
/// Labels are deduplicated by key (last value wins) and sorted by key to ensure
/// stable identity regardless of input order.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct DynamicLabelSet {
    labels: Vec<(String, String)>,
}

impl DynamicLabelSet {
    /// Build a canonical label set from borrowed key/value pairs.
    #[inline]
    pub fn from_pairs(labels: &[(&str, &str)]) -> Self {
        Self {
            labels: canonicalize_labels(labels),
        }
    }

    /// Build from already-canonicalized owned pairs.
    ///
    /// The input must already be sorted by key and deduplicated.
    #[doc(hidden)]
    pub fn from_canonical_pairs(labels: &[(String, String)]) -> Self {
        Self {
            labels: labels.to_vec(),
        }
    }

    /// Returns labels as ordered `(key, value)` pairs.
    pub fn pairs(&self) -> &[(String, String)] {
        &self.labels
    }

    #[inline]
    pub(crate) fn shard_index(&self, shard_mask: usize) -> usize {
        let hash = label_set_fingerprint(&self.labels);
        ((hash ^ (hash >> 32)) as usize) & shard_mask
    }
}

pub(crate) type DynamicIndexMap<T> = BTreeMap<DynamicLabelSet, T>;

#[inline]
pub(crate) fn dynamic_index_map<T>() -> DynamicIndexMap<T> {
    BTreeMap::new()
}

#[inline]
fn canonicalize_labels(labels: &[(&str, &str)]) -> Vec<(String, String)> {
    match labels {
        [] => Vec::new(),
        [(key, value)] => vec![(key.to_string(), value.to_string())],
        labels if labels.len() <= VEC_CANONICALIZATION_MAX_LABELS => {
            canonicalize_labels_vec(labels)
        }
        labels => canonicalize_labels_btree(labels),
    }
}

#[inline]
fn canonicalize_labels_vec(labels: &[(&str, &str)]) -> Vec<(String, String)> {
    let mut ordered: Vec<(String, String)> = Vec::with_capacity(labels.len());
    for (k, v) in labels {
        match ordered.binary_search_by(|(key, _)| key.as_str().cmp(k)) {
            Ok(index) => ordered[index].1 = (*v).to_string(),
            Err(index) => ordered.insert(index, ((*k).to_string(), (*v).to_string())),
        }
    }
    ordered
}

#[inline]
fn canonicalize_labels_btree(labels: &[(&str, &str)]) -> Vec<(String, String)> {
    let mut map = BTreeMap::new();
    for (k, v) in labels {
        map.insert((*k).to_string(), (*v).to_string());
    }
    map.into_iter().collect()
}

#[inline]
fn label_set_fingerprint(labels: &[(String, String)]) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    let mut hash = FNV_OFFSET;
    for (key, value) in labels {
        for byte in key.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash ^= 0xff;
        hash = hash.wrapping_mul(FNV_PRIME);
        for byte in value.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash ^= 0xfe;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

// Shared ID generators
pub(crate) static COUNTER_IDS: AtomicUsize = AtomicUsize::new(1);
pub(crate) static GAUGE_IDS: AtomicUsize = AtomicUsize::new(1);
pub(crate) static HISTOGRAM_IDS: AtomicUsize = AtomicUsize::new(1);
pub(crate) static DISTRIBUTION_IDS: AtomicUsize = AtomicUsize::new(1);

#[cfg(test)]
mod tests {
    use super::DynamicLabelSet;

    #[test]
    fn dynamic_label_set_is_order_independent() {
        let left = DynamicLabelSet::from_pairs(&[("b", "2"), ("a", "1")]);
        let right = DynamicLabelSet::from_pairs(&[("a", "1"), ("b", "2")]);

        assert_eq!(left, right);
        assert_eq!(
            left.pairs(),
            &[
                ("a".to_string(), "1".to_string()),
                ("b".to_string(), "2".to_string())
            ]
        );
    }

    #[test]
    fn dynamic_label_set_duplicate_key_last_value_wins() {
        let labels = DynamicLabelSet::from_pairs(&[("b", "1"), ("a", "1"), ("b", "2")]);

        assert_eq!(
            labels.pairs(),
            &[
                ("a".to_string(), "1".to_string()),
                ("b".to_string(), "2".to_string())
            ]
        );
    }

    #[test]
    fn dynamic_label_set_canonicalizes_large_label_sets() {
        let labels = DynamicLabelSet::from_pairs(&[
            ("m", "13"),
            ("l", "12"),
            ("k", "11"),
            ("j", "10"),
            ("i", "9"),
            ("h", "8"),
            ("g", "7"),
            ("f", "6"),
            ("e", "5"),
            ("d", "4"),
            ("c", "3"),
            ("b", "2"),
            ("a", "1"),
        ]);

        assert_eq!(
            labels
                .pairs()
                .first()
                .map(|(k, v)| (k.as_str(), v.as_str())),
            Some(("a", "1"))
        );
        assert_eq!(
            labels.pairs().last().map(|(k, v)| (k.as_str(), v.as_str())),
            Some(("m", "13"))
        );
    }
}
