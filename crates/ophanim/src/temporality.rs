//! Export temporality for metrics backends.

/// Aggregation temporality used during export.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Temporality {
    /// Export totals since process start.
    Cumulative,
    /// Export values for the current export window.
    Delta,
}
