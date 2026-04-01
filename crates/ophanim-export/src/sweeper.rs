//! Periodic sweeper for stale dynamic metric series.
//!
//! Advances the global eviction cycle and evicts series that have been inactive
//! for longer than the configured threshold. This bounds memory usage from
//! dynamic labels regardless of which exporters are active.
//!
//! The actual eviction logic is provided by the caller via a closure,
//! making this work with any metrics struct that has dynamic series.

use std::time::Duration;

use tokio_util::sync::CancellationToken;

/// Default sweep interval.
const DEFAULT_SWEEP_INTERVAL: Duration = Duration::from_secs(10);

/// Default eviction threshold: series inactive for this many sweep cycles are
/// evicted. With the default 10s interval this equals ~5 minutes of inactivity.
const DEFAULT_EVICTION_THRESHOLD: u32 = 30;

/// Configuration for the stale-series sweeper.
#[derive(Clone)]
pub struct SweepConfig {
    /// How often to run the sweep (default: 10s).
    pub interval: Duration,
    /// Number of consecutive idle cycles before a series is evicted (default: 30).
    pub eviction_threshold: u32,
}

impl Default for SweepConfig {
    fn default() -> Self {
        Self {
            interval: DEFAULT_SWEEP_INTERVAL,
            eviction_threshold: DEFAULT_EVICTION_THRESHOLD,
        }
    }
}

impl SweepConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    pub fn with_eviction_threshold(mut self, threshold: u32) -> Self {
        self.eviction_threshold = threshold;
        self
    }
}

/// Run the stale-series sweep loop.
///
/// `sweep_fn` is called each cycle with the eviction threshold. It should
/// advance the eviction cycle and evict stale series, returning the number
/// of series evicted.
///
/// Runs until `cancel` is triggered.
///
/// # Example
///
/// ```ignore
/// use ophanim_export::sweeper::{SweepConfig, run};
/// use tokio_util::sync::CancellationToken;
///
/// let metrics = Arc::new(MyMetrics::new());
/// let cancel = CancellationToken::new();
///
/// let m = metrics.clone();
/// tokio::spawn(run(SweepConfig::default(), cancel, move |threshold| {
///     m.evict_stale_series(threshold)
/// }));
/// ```
pub async fn run<F>(config: SweepConfig, cancel: CancellationToken, mut sweep_fn: F)
where
    F: FnMut(u32) -> usize,
{
    use tokio::time::MissedTickBehavior;

    log::info!(
        "Starting stale-series sweeper, interval={}s, eviction_threshold={}",
        config.interval.as_secs(),
        config.eviction_threshold
    );

    let mut interval = tokio::time::interval(config.interval);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    interval.tick().await;

    loop {
        tokio::select! {
            _ = interval.tick() => {}
            _ = cancel.cancelled() => {
                log::info!("Stale-series sweeper shutting down");
                return;
            }
        }

        let evicted = sweep_fn(config.eviction_threshold);

        if evicted > 0 {
            log::debug!("Evicted {evicted} stale metric series");
        }
    }
}
