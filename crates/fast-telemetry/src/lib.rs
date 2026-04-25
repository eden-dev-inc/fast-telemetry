//! Fast, thread-sharded counters and histograms for high-performance telemetry.
//!
//! OpenTelemetry counters use atomic operations with global synchronization,
//! which causes cache-line bouncing across cores. This crate provides
//! thread-local sharded counters that:
//!
//! - Increment with no cross-thread contention (just a thread-local write)
//! - Are cache-line padded to avoid false sharing (using crossbeam's CachePadded)
//! - Aggregate on read (sum all shards)
//!
//! # Labeled Metrics
//!
//! For dimensional metrics (counters/gauges/histograms broken down by label),
//! use the `Labeled*` types with a `LabelEnum` implementation. These provide
//! O(1) lookup via array indexing instead of HashMap lookups.
//!
//! ```ignore
//! use fast_telemetry::{LabeledCounter, LabelEnum};
//!
//! #[derive(Copy, Clone, Debug)]
//! enum HttpMethod { Get, Post, Put, Delete }
//!
//! impl LabelEnum for HttpMethod { /* ... */ }
//!
//! let counter: LabeledCounter<HttpMethod> = LabeledCounter::new(4);
//! counter.inc(HttpMethod::Get);  // O(1) array index, no hashing
//! ```

mod export;
pub(crate) mod internal;
mod metric;
pub mod span;
mod temporality;

pub use export::text::{DogStatsDExport, PrometheusExport};

// These helpers are public only because the ExportMetrics proc macro generates
// code that calls them. They are not part of the public API and may change
// without notice.
#[doc(hidden)]
pub mod __macro_support {
    pub use crate::export::text::{
        __write_dogstatsd, __write_dogstatsd_distribution, __write_dogstatsd_distribution_delta,
        __write_dogstatsd_distribution_delta_dynamic,
        __write_dogstatsd_distribution_delta_dynamic_pairs, __write_dogstatsd_distribution_dynamic,
        __write_dogstatsd_dynamic, __write_dogstatsd_dynamic_pairs, __write_dogstatsd_with_label,
    };
}
pub use metric::{
    Counter, Distribution, DynamicCounter, DynamicCounterSeries, DynamicDistribution,
    DynamicDistributionSeries, DynamicGauge, DynamicGaugeI64, DynamicGaugeI64Series,
    DynamicGaugeSeries, DynamicHistogram, DynamicHistogramSeries, DynamicHistogramSeriesView,
    DynamicLabelSet, Gauge, GaugeF64, Histogram, LabelEnum, LabeledCounter, LabeledGauge,
    LabeledHistogram, MaxGauge, MaxGaugeF64, MinGauge, MinGaugeF64,
};
#[cfg(feature = "eviction")]
pub use metric::{advance_cycle, current_cycle};
pub use span::{
    CompletedSpan, Span, SpanAttribute, SpanCollector, SpanEvent, SpanId, SpanKind, SpanStatus,
    SpanValue, TraceId, current_span_id, current_trace_id,
};
pub use temporality::Temporality;

#[cfg(feature = "otlp")]
pub use export::otlp::OtlpExport;

#[cfg(feature = "macros")]
pub use fast_telemetry_macros::{ExportMetrics, LabelEnum as DeriveLabel};

// Internal compatibility aliases for moved modules.
pub(crate) use internal::exp_buckets;
pub(crate) use internal::thread_id;
pub(crate) use metric::label;

#[cfg(feature = "otlp")]
pub mod otlp {
    pub use crate::export::otlp::*;
}
