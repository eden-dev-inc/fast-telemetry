//! Atomic gauge for point-in-time values.

use crossbeam_utils::CachePadded;
use std::sync::atomic::{AtomicI64, Ordering};

/// A cache-padded atomic gauge for point-in-time measurements.
///
/// Gauges represent a current value that is periodically sampled and set,
/// such as memory usage, queue depth, or progress percentage.
///
/// # Why Only `set()` and `get()`?
///
/// This type intentionally omits `add()`/`inc()`/`dec()` methods. If you need
/// to increment or decrement a value from multiple threads (e.g., tracking
/// active connections), use [`Counter`] instead - it's thread-sharded for
/// contention-free concurrent increments.
///
/// Providing increment methods here would create a footgun: the API would look
/// convenient for counting, but the single-atomic implementation would cause
/// cache-line contention under concurrent writes. By limiting the API to
/// `set()`/`get()`, we make the intended usage pattern clear: periodic
/// point-in-time snapshots from a single writer.
///
/// # Why Not Thread-Sharded?
///
/// Thread-sharding works for `Counter` because addition is commutative — you
/// can sum the shards to get the total. This includes subtraction (adding
/// negative values): the shard sums still produce the correct aggregate.
/// `set()` is not commutative; there's no meaningful way to aggregate "last
/// value written" across shards.
///
/// The cache padding prevents false sharing if this gauge is stored adjacent
/// to frequently-accessed data.
///
/// [`Counter`]: crate::Counter
pub struct Gauge {
    value: CachePadded<AtomicI64>,
}

impl Gauge {
    /// Create a new gauge initialized to zero.
    pub fn new() -> Self {
        Self {
            value: CachePadded::new(AtomicI64::new(0)),
        }
    }

    /// Create a new gauge with an initial value.
    pub fn with_value(initial: i64) -> Self {
        Self {
            value: CachePadded::new(AtomicI64::new(initial)),
        }
    }

    /// Set the gauge to a value.
    #[inline]
    pub fn set(&self, value: i64) {
        self.value.store(value, Ordering::Relaxed);
    }

    /// Get the current value.
    #[inline]
    pub fn get(&self) -> i64 {
        self.value.load(Ordering::Relaxed)
    }
}

impl Default for Gauge {
    fn default() -> Self {
        Self::new()
    }
}
