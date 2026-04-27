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
mod distribution;
mod gauge;
mod gauge_i64;
mod histogram;

pub use counter::{DynamicCounter, DynamicCounterSeries};
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

/// Canonicalized runtime label set.
///
/// Labels are deduplicated by key (last value wins) and sorted by key to ensure
/// stable identity regardless of input order.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DynamicLabelSet {
    labels: Vec<(String, String)>,
}

impl DynamicLabelSet {
    /// Build a canonical label set from borrowed key/value pairs.
    pub fn from_pairs(labels: &[(&str, &str)]) -> Self {
        let mut map = BTreeMap::new();
        for (k, v) in labels {
            map.insert((*k).to_string(), (*v).to_string());
        }
        Self {
            labels: map.into_iter().collect(),
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
}

// Shared ID generators
pub(crate) static COUNTER_IDS: AtomicUsize = AtomicUsize::new(1);
pub(crate) static GAUGE_IDS: AtomicUsize = AtomicUsize::new(1);
pub(crate) static HISTOGRAM_IDS: AtomicUsize = AtomicUsize::new(1);
pub(crate) static DISTRIBUTION_IDS: AtomicUsize = AtomicUsize::new(1);
