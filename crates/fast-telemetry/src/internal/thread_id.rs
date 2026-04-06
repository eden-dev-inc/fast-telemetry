//! Shared thread ID for shard indexing.
//!
//! Each thread is assigned a unique, monotonically increasing ID on first use.
//! Metric types use `thread_id() & mask` for lock-free shard selection.

use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};

static THREAD_COUNTER: AtomicUsize = AtomicUsize::new(1);

thread_local! {
    static THREAD_ID: Cell<usize> = Cell::new(THREAD_COUNTER.fetch_add(1, Ordering::SeqCst));
}

#[inline]
pub(crate) fn thread_id() -> usize {
    THREAD_ID.with(|id| id.get())
}
