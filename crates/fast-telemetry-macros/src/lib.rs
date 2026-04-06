//! Derive macros for fast-telemetry.
//!
//! Provides:
//! - `#[derive(ExportMetrics)]` to auto-generate Prometheus, DogStatsD, and
//!   optional OTLP export code
//! - `#[derive(LabelEnum)]` to auto-generate `LabelEnum` trait implementations

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::spanned::Spanned;
use syn::{
    Data, DeriveInput, Expr, Fields, GenericArgument, Lit, Meta, PathArguments, Type,
    parse_macro_input,
};

enum MetricKind {
    Counter,
    Distribution,
    DynamicCounter,
    DynamicDistribution,
    DynamicGauge,
    DynamicGaugeI64,
    DynamicHistogram,
    Gauge,
    GaugeF64,
    Histogram,
    LabeledCounter(Type),
    LabeledGauge,
    LabeledHistogram(Type),
}

// Output reserve heuristics for derive-generated exporters.
const PROM_BASE_FIELD_OVERHEAD_BYTES: usize = 48;
const PROM_COMPLEX_METRIC_OVERHEAD_BYTES: usize = 128;
const DOGSTATSD_SIMPLE_LINE_OVERHEAD_BYTES: usize = 24;
const DOGSTATSD_HISTOGRAM_LINE_OVERHEAD_BYTES: usize = 30;
const DOGSTATSD_HISTOGRAM_LINES: usize = 2;
const DOGSTATSD_TAG_PREFIX_BYTES: usize = 2; // "|#"
const DOGSTATSD_TAG_PAIR_OVERHEAD_BYTES: usize = 2; // ":" plus separator/comma budget
const DYNAMIC_LABELS_PER_SERIES_ESTIMATE: usize = 10;
const DYNAMIC_LABEL_PAIR_ESTIMATE_BYTES: usize = 16;
const PROM_DYNAMIC_SIMPLE_SERIES_OVERHEAD_BYTES: usize = 64;
const PROM_DYNAMIC_COMPLEX_SERIES_OVERHEAD_BYTES: usize = 160;
const DOGSTATSD_DYNAMIC_SIMPLE_SERIES_OVERHEAD_BYTES: usize = 64;
const DOGSTATSD_DYNAMIC_COMPLEX_SERIES_OVERHEAD_BYTES: usize = 160;

fn metric_kind(ty: &Type) -> Option<MetricKind> {
    let Type::Path(type_path) = ty else {
        return None;
    };
    let segment = type_path.path.segments.last()?;
    match segment.ident.to_string().as_str() {
        "Counter" => Some(MetricKind::Counter),
        "Distribution" => Some(MetricKind::Distribution),
        "DynamicCounter" => Some(MetricKind::DynamicCounter),
        "DynamicDistribution" => Some(MetricKind::DynamicDistribution),
        "DynamicGauge" => Some(MetricKind::DynamicGauge),
        "DynamicGaugeI64" => Some(MetricKind::DynamicGaugeI64),
        "DynamicHistogram" => Some(MetricKind::DynamicHistogram),
        "Gauge" => Some(MetricKind::Gauge),
        "GaugeF64" => Some(MetricKind::GaugeF64),
        "Histogram" => Some(MetricKind::Histogram),
        "LabeledCounter" => {
            let PathArguments::AngleBracketed(args) = &segment.arguments else {
                return None;
            };
            let arg = args.args.first()?;
            let GenericArgument::Type(label_ty) = arg else {
                return None;
            };
            Some(MetricKind::LabeledCounter(label_ty.clone()))
        }
        "LabeledGauge" => {
            let PathArguments::AngleBracketed(args) = &segment.arguments else {
                return None;
            };
            let arg = args.args.first()?;
            let GenericArgument::Type(_label_ty) = arg else {
                return None;
            };
            Some(MetricKind::LabeledGauge)
        }
        "LabeledHistogram" => {
            let PathArguments::AngleBracketed(args) = &segment.arguments else {
                return None;
            };
            let arg = args.args.first()?;
            let GenericArgument::Type(label_ty) = arg else {
                return None;
            };
            Some(MetricKind::LabeledHistogram(label_ty.clone()))
        }
        _ => None,
    }
}

/// Derive macro for exporting metrics in Prometheus, DogStatsD, and OTLP formats.
///
/// Generates methods:
/// - `export_prometheus(&self, output: &mut String)` — Prometheus text format
/// - `export_dogstatsd(&self, output: &mut String, tags: &[(&str, &str)])` — DogStatsD format
/// - `export_dogstatsd_delta(...)` — DogStatsD with per-sink delta temporality
/// - `export_dogstatsd_with_temporality(...)` — runtime-selectable cumulative or delta export
/// - `export_otlp(...)` — OTLP protobuf (only when `#[otlp]` attribute is present)
///
/// Supports unlabeled metrics (`Counter`, `Gauge`, `GaugeF64`, `Histogram`,
/// `Distribution`), compile-time labeled metrics (`LabeledCounter<L>`,
/// `LabeledGauge<L>`, `LabeledHistogram<L>`), and runtime-labeled metrics
/// (`DynamicCounter`, `DynamicGauge`, `DynamicGaugeI64`, `DynamicHistogram`,
/// `DynamicDistribution`).
///
/// # Example
///
/// ```ignore
/// use fast_telemetry::{Counter, Histogram, Gauge, LabeledCounter, DeriveLabel};
///
/// #[derive(Copy, Clone, Debug, DeriveLabel)]
/// #[label_name = "method"]
/// enum HttpMethod { Get, Post, Put, Delete }
///
/// #[derive(ExportMetrics)]
/// #[metric_prefix = "proxy"]
/// pub struct ProxyMetrics {
///     #[help = "Total requests proxied"]
///     pub requests: Counter,
///
///     #[help = "Requests by HTTP method"]
///     pub requests_by_method: LabeledCounter<HttpMethod>,
///
///     #[help = "Request latency in microseconds"]
///     pub latency: Histogram,
///
///     #[help = "Current memory usage"]
///     pub memory_mb: Gauge,
/// }
///
/// let metrics = ProxyMetrics::new();
///
/// // Prometheus export
/// let mut prom_output = String::new();
/// metrics.export_prometheus(&mut prom_output);
///
/// // DogStatsD export (with optional tags)
/// let mut statsd_output = String::new();
/// metrics.export_dogstatsd(&mut statsd_output, &[("env", "prod")]);
/// ```
#[proc_macro_derive(ExportMetrics, attributes(metric_prefix, help, otlp))]
pub fn derive_export_metrics(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match derive_export_metrics_impl(input) {
        Ok(ts) => ts,
        Err(err) => err.to_compile_error().into(),
    }
}

fn derive_export_metrics_impl(input: DeriveInput) -> syn::Result<TokenStream> {
    let name = &input.ident;
    let vis = &input.vis;
    let state_name = format_ident!("{}DogStatsDState", name);

    // Extract metric_prefix from struct attributes
    let prefix = extract_metric_prefix(&input.attrs).unwrap_or_default();

    // Check for #[otlp] attribute to enable OTLP export generation
    let enable_otlp = input.attrs.iter().any(|attr| attr.path().is_ident("otlp"));

    // Get struct fields
    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            _ => {
                return Err(syn::Error::new_spanned(
                    &data.fields,
                    "ExportMetrics only supports structs with named fields",
                ));
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                &input,
                "ExportMetrics only supports structs",
            ));
        }
    };

    let mut prometheus_exports = Vec::new();
    let mut dogstatsd_exports = Vec::new();
    let mut delta_exports = Vec::new();
    let mut otlp_exports = Vec::new();
    let mut state_fields = Vec::new();
    let mut state_inits = Vec::new();
    let mut state_label_count_exprs = Vec::new();
    let mut prom_reserve_hint = 0usize;
    let mut dogstatsd_reserve_hint = 0usize;
    let mut dogstatsd_delta_reserve_hint = 0usize;
    let mut dogstatsd_tag_line_hint = 0usize;
    let mut dogstatsd_delta_tag_line_hint = 0usize;
    let mut prom_dynamic_reserve_exprs = Vec::new();
    let mut dogstatsd_dynamic_reserve_exprs = Vec::new();
    let mut dogstatsd_delta_dynamic_reserve_exprs = Vec::new();
    let mut dogstatsd_dynamic_tag_line_exprs = Vec::new();
    let mut dogstatsd_delta_dynamic_tag_line_exprs = Vec::new();

    for field in fields.iter() {
        let field_name = field.ident.as_ref().ok_or_else(|| {
            syn::Error::new(field.span(), "ExportMetrics only supports named fields")
        })?;
        let field_name_str = field_name.to_string();
        let prom_metric_name = if prefix.is_empty() {
            field_name_str.clone()
        } else {
            format!("{}_{}", prefix, field_name_str)
        };
        let statsd_metric_name = if prefix.is_empty() {
            field_name_str.clone()
        } else {
            format!("{}.{}", prefix, field_name_str)
        };
        let help = extract_help(&field.attrs).unwrap_or_else(|| field_name_str.clone());

        prometheus_exports.push(quote! {
            fast_telemetry::PrometheusExport::export_prometheus(&self.#field_name, output, #prom_metric_name, #help);
        });

        dogstatsd_exports.push(quote! {
            fast_telemetry::DogStatsDExport::export_dogstatsd(&self.#field_name, output, #statsd_metric_name, tags);
        });

        otlp_exports.push(quote! {
            fast_telemetry::OtlpExport::export_otlp(&self.#field_name, metrics, #prom_metric_name, #help, time_unix_nano);
        });

        let metric_kind = metric_kind(&field.ty).ok_or_else(|| {
            syn::Error::new_spanned(
                &field.ty,
                format!(
                    "ExportMetrics does not support field '{}' with this type",
                    field_name_str
                ),
            )
        })?;

        prom_reserve_hint += prom_metric_name.len() + help.len() + PROM_BASE_FIELD_OVERHEAD_BYTES;
        match &metric_kind {
            MetricKind::Counter
            | MetricKind::Gauge
            | MetricKind::GaugeF64
            | MetricKind::Distribution
            | MetricKind::DynamicCounter
            | MetricKind::DynamicGauge
            | MetricKind::DynamicGaugeI64
            | MetricKind::LabeledCounter(_)
            | MetricKind::LabeledGauge => {
                prom_reserve_hint += PROM_BASE_FIELD_OVERHEAD_BYTES;
            }
            MetricKind::Histogram
            | MetricKind::DynamicHistogram
            | MetricKind::DynamicDistribution
            | MetricKind::LabeledHistogram(_) => {
                prom_reserve_hint += PROM_COMPLEX_METRIC_OVERHEAD_BYTES;
            }
        }

        match &metric_kind {
            MetricKind::Counter | MetricKind::Gauge | MetricKind::GaugeF64 => {
                dogstatsd_reserve_hint +=
                    statsd_metric_name.len() + DOGSTATSD_SIMPLE_LINE_OVERHEAD_BYTES;
                dogstatsd_delta_reserve_hint +=
                    statsd_metric_name.len() + DOGSTATSD_SIMPLE_LINE_OVERHEAD_BYTES;
                dogstatsd_tag_line_hint += 1;
                dogstatsd_delta_tag_line_hint += 1;
            }
            MetricKind::Histogram => {
                dogstatsd_reserve_hint += (statsd_metric_name.len()
                    + DOGSTATSD_HISTOGRAM_LINE_OVERHEAD_BYTES)
                    * DOGSTATSD_HISTOGRAM_LINES;
                dogstatsd_delta_reserve_hint += (statsd_metric_name.len()
                    + DOGSTATSD_HISTOGRAM_LINE_OVERHEAD_BYTES)
                    * DOGSTATSD_HISTOGRAM_LINES;
                dogstatsd_tag_line_hint += DOGSTATSD_HISTOGRAM_LINES;
                dogstatsd_delta_tag_line_hint += DOGSTATSD_HISTOGRAM_LINES;
            }
            MetricKind::Distribution => {
                dogstatsd_reserve_hint +=
                    statsd_metric_name.len() + DOGSTATSD_SIMPLE_LINE_OVERHEAD_BYTES;
                dogstatsd_delta_reserve_hint +=
                    statsd_metric_name.len() + DOGSTATSD_SIMPLE_LINE_OVERHEAD_BYTES;
                dogstatsd_tag_line_hint += 1;
                dogstatsd_delta_tag_line_hint += 1;
            }
            MetricKind::DynamicCounter
            | MetricKind::DynamicGauge
            | MetricKind::DynamicGaugeI64
            | MetricKind::DynamicDistribution => {
                dogstatsd_reserve_hint +=
                    statsd_metric_name.len() + DOGSTATSD_SIMPLE_LINE_OVERHEAD_BYTES;
                dogstatsd_delta_reserve_hint +=
                    statsd_metric_name.len() + DOGSTATSD_SIMPLE_LINE_OVERHEAD_BYTES;
            }
            MetricKind::DynamicHistogram => {
                dogstatsd_reserve_hint += (statsd_metric_name.len()
                    + DOGSTATSD_HISTOGRAM_LINE_OVERHEAD_BYTES)
                    * DOGSTATSD_HISTOGRAM_LINES;
                dogstatsd_delta_reserve_hint += (statsd_metric_name.len()
                    + DOGSTATSD_HISTOGRAM_LINE_OVERHEAD_BYTES)
                    * DOGSTATSD_HISTOGRAM_LINES;
            }
            MetricKind::LabeledCounter(_) | MetricKind::LabeledGauge => {
                dogstatsd_reserve_hint +=
                    statsd_metric_name.len() + DOGSTATSD_SIMPLE_LINE_OVERHEAD_BYTES;
                dogstatsd_delta_reserve_hint +=
                    statsd_metric_name.len() + DOGSTATSD_SIMPLE_LINE_OVERHEAD_BYTES;
            }
            MetricKind::LabeledHistogram(_) => {
                dogstatsd_reserve_hint += (statsd_metric_name.len()
                    + DOGSTATSD_HISTOGRAM_LINE_OVERHEAD_BYTES)
                    * DOGSTATSD_HISTOGRAM_LINES;
                dogstatsd_delta_reserve_hint += (statsd_metric_name.len()
                    + DOGSTATSD_HISTOGRAM_LINE_OVERHEAD_BYTES)
                    * DOGSTATSD_HISTOGRAM_LINES;
            }
        }

        match metric_kind {
            MetricKind::Counter => {
                state_label_count_exprs.push(quote! { 0usize });
                state_fields.push(quote! { #field_name: isize, });
                state_inits.push(quote! { #field_name: 0, });
                delta_exports.push(quote! {
                    let current = self.#field_name.sum();
                    let delta = current - state.#field_name;
                    state.#field_name = current;
                    // Use counter type - in Datadog use .as_count() to see raw values
                    fast_telemetry::__macro_support::__write_dogstatsd(output, #statsd_metric_name, delta, "c", tags);
                });
            }
            MetricKind::Distribution => {
                let buckets_state_field = format_ident!("{}_buckets", field_name);
                state_label_count_exprs.push(quote! { 0usize });
                state_fields.push(quote! { #buckets_state_field: [u64; 65], });
                state_inits.push(quote! { #buckets_state_field: [0u64; 65], });
                delta_exports.push(quote! {
                    let snap = self.#field_name.buckets_snapshot();
                    fast_telemetry::__macro_support::__write_dogstatsd_distribution_delta(
                        output, #statsd_metric_name, &snap, &mut state.#buckets_state_field, tags
                    );
                });
            }
            MetricKind::DynamicCounter => {
                prom_dynamic_reserve_exprs.push(quote! {
                    self.#field_name.cardinality().saturating_mul(
                        #prom_metric_name.len()
                            + #PROM_DYNAMIC_SIMPLE_SERIES_OVERHEAD_BYTES
                            + (#DYNAMIC_LABELS_PER_SERIES_ESTIMATE * #DYNAMIC_LABEL_PAIR_ESTIMATE_BYTES)
                    )
                });
                dogstatsd_dynamic_reserve_exprs.push(quote! {
                    self.#field_name.cardinality().saturating_mul(
                        #statsd_metric_name.len()
                            + #DOGSTATSD_DYNAMIC_SIMPLE_SERIES_OVERHEAD_BYTES
                            + (#DYNAMIC_LABELS_PER_SERIES_ESTIMATE * #DYNAMIC_LABEL_PAIR_ESTIMATE_BYTES)
                    )
                });
                dogstatsd_delta_dynamic_reserve_exprs.push(quote! {
                    self.#field_name.cardinality().saturating_mul(
                        #statsd_metric_name.len()
                            + #DOGSTATSD_DYNAMIC_SIMPLE_SERIES_OVERHEAD_BYTES
                            + (#DYNAMIC_LABELS_PER_SERIES_ESTIMATE * #DYNAMIC_LABEL_PAIR_ESTIMATE_BYTES)
                    )
                });
                dogstatsd_dynamic_tag_line_exprs.push(quote! { self.#field_name.cardinality() });
                dogstatsd_delta_dynamic_tag_line_exprs
                    .push(quote! { self.#field_name.cardinality() });
                state_label_count_exprs.push(quote! { self.#field_name.len() });
                state_fields.push(quote! { #field_name: std::collections::HashMap<fast_telemetry::DynamicLabelSet, isize>, });
                state_inits.push(quote! { #field_name: std::collections::HashMap::new(), });
                delta_exports.push(quote! {
                    let overflow = self.#field_name.overflow_count();
                    if overflow > 0 {
                        log::warn!(
                            "fast-telemetry: {} hit cardinality cap, {} records routed to overflow",
                            #statsd_metric_name,
                            overflow
                        );
                    }
                    let mut current_keys = std::collections::HashSet::new();
                    self.#field_name.visit_series(|labels, current| {
                        let key = fast_telemetry::DynamicLabelSet::from_canonical_pairs(labels);
                        current_keys.insert(key.clone());
                        let previous = state.#field_name.get(&key).copied().unwrap_or(0);
                        let delta = current - previous;
                        state.#field_name.insert(key, current);
                        fast_telemetry::__macro_support::__write_dogstatsd_dynamic_pairs(
                            output,
                            #statsd_metric_name,
                            delta,
                            "c",
                            labels,
                            tags,
                        );
                    });
                    // Prune state entries for evicted label sets
                    state.#field_name.retain(|k, _| current_keys.contains(k));
                });
            }
            MetricKind::DynamicDistribution => {
                let buckets_state_field = format_ident!("{}_buckets", field_name);
                prom_dynamic_reserve_exprs.push(quote! {
                    self.#field_name.cardinality().saturating_mul(
                        #prom_metric_name.len()
                            + #PROM_DYNAMIC_COMPLEX_SERIES_OVERHEAD_BYTES
                            + (#DYNAMIC_LABELS_PER_SERIES_ESTIMATE * #DYNAMIC_LABEL_PAIR_ESTIMATE_BYTES)
                    )
                });
                dogstatsd_dynamic_reserve_exprs.push(quote! {
                    self.#field_name.cardinality().saturating_mul(
                        #statsd_metric_name.len()
                            + #DOGSTATSD_DYNAMIC_COMPLEX_SERIES_OVERHEAD_BYTES
                            + (#DYNAMIC_LABELS_PER_SERIES_ESTIMATE * #DYNAMIC_LABEL_PAIR_ESTIMATE_BYTES)
                    )
                });
                dogstatsd_delta_dynamic_reserve_exprs.push(quote! {
                    self.#field_name.cardinality().saturating_mul(
                        #statsd_metric_name.len()
                            + #DOGSTATSD_DYNAMIC_COMPLEX_SERIES_OVERHEAD_BYTES
                            + (#DYNAMIC_LABELS_PER_SERIES_ESTIMATE * #DYNAMIC_LABEL_PAIR_ESTIMATE_BYTES)
                    )
                });
                dogstatsd_dynamic_tag_line_exprs.push(quote! { self.#field_name.cardinality() });
                dogstatsd_delta_dynamic_tag_line_exprs
                    .push(quote! { self.#field_name.cardinality() });
                state_label_count_exprs.push(quote! {
                    self.#buckets_state_field.len()
                });
                state_fields.push(quote! { #buckets_state_field: std::collections::HashMap<fast_telemetry::DynamicLabelSet, [u64; 65]>, });
                state_inits
                    .push(quote! { #buckets_state_field: std::collections::HashMap::new(), });
                delta_exports.push(quote! {
                    let overflow = self.#field_name.overflow_count();
                    if overflow > 0 {
                        log::warn!(
                            "fast-telemetry: {} hit cardinality cap, {} records routed to overflow",
                            #statsd_metric_name,
                            overflow
                        );
                    }
                    let mut current_keys = std::collections::HashSet::new();
                    self.#field_name.visit_series(|labels, _count, _sum, snap| {
                        let key = fast_telemetry::DynamicLabelSet::from_canonical_pairs(labels);
                        current_keys.insert(key.clone());
                        let prev = state.#buckets_state_field.entry(key).or_insert([0u64; 65]);
                        fast_telemetry::__macro_support::__write_dogstatsd_distribution_delta_dynamic_pairs(
                            output, #statsd_metric_name, &snap, prev, labels, tags
                        );
                    });
                    // Prune state entries for evicted label sets
                    state.#buckets_state_field.retain(|k, _| current_keys.contains(k));
                });
            }
            MetricKind::DynamicGauge => {
                prom_dynamic_reserve_exprs.push(quote! {
                    self.#field_name.cardinality().saturating_mul(
                        #prom_metric_name.len()
                            + #PROM_DYNAMIC_SIMPLE_SERIES_OVERHEAD_BYTES
                            + (#DYNAMIC_LABELS_PER_SERIES_ESTIMATE * #DYNAMIC_LABEL_PAIR_ESTIMATE_BYTES)
                    )
                });
                dogstatsd_dynamic_reserve_exprs.push(quote! {
                    self.#field_name.cardinality().saturating_mul(
                        #statsd_metric_name.len()
                            + #DOGSTATSD_DYNAMIC_SIMPLE_SERIES_OVERHEAD_BYTES
                            + (#DYNAMIC_LABELS_PER_SERIES_ESTIMATE * #DYNAMIC_LABEL_PAIR_ESTIMATE_BYTES)
                    )
                });
                dogstatsd_delta_dynamic_reserve_exprs.push(quote! {
                    self.#field_name.cardinality().saturating_mul(
                        #statsd_metric_name.len()
                            + #DOGSTATSD_DYNAMIC_SIMPLE_SERIES_OVERHEAD_BYTES
                            + (#DYNAMIC_LABELS_PER_SERIES_ESTIMATE * #DYNAMIC_LABEL_PAIR_ESTIMATE_BYTES)
                    )
                });
                dogstatsd_dynamic_tag_line_exprs.push(quote! { self.#field_name.cardinality() });
                dogstatsd_delta_dynamic_tag_line_exprs
                    .push(quote! { self.#field_name.cardinality() });
                state_label_count_exprs.push(quote! { 0usize });
                // Gauges are point-in-time, no delta tracking needed (always export current value)
                delta_exports.push(quote! {
                    let overflow = self.#field_name.overflow_count();
                    if overflow > 0 {
                        log::warn!(
                            "fast-telemetry: {} hit cardinality cap, {} records routed to overflow",
                            #statsd_metric_name,
                            overflow
                        );
                    }
                    fast_telemetry::DogStatsDExport::export_dogstatsd(&self.#field_name, output, #statsd_metric_name, tags);
                });
            }
            MetricKind::DynamicGaugeI64 => {
                prom_dynamic_reserve_exprs.push(quote! {
                    self.#field_name.cardinality().saturating_mul(
                        #prom_metric_name.len()
                            + #PROM_DYNAMIC_SIMPLE_SERIES_OVERHEAD_BYTES
                            + (#DYNAMIC_LABELS_PER_SERIES_ESTIMATE * #DYNAMIC_LABEL_PAIR_ESTIMATE_BYTES)
                    )
                });
                dogstatsd_dynamic_reserve_exprs.push(quote! {
                    self.#field_name.cardinality().saturating_mul(
                        #statsd_metric_name.len()
                            + #DOGSTATSD_DYNAMIC_SIMPLE_SERIES_OVERHEAD_BYTES
                            + (#DYNAMIC_LABELS_PER_SERIES_ESTIMATE * #DYNAMIC_LABEL_PAIR_ESTIMATE_BYTES)
                    )
                });
                dogstatsd_delta_dynamic_reserve_exprs.push(quote! {
                    self.#field_name.cardinality().saturating_mul(
                        #statsd_metric_name.len()
                            + #DOGSTATSD_DYNAMIC_SIMPLE_SERIES_OVERHEAD_BYTES
                            + (#DYNAMIC_LABELS_PER_SERIES_ESTIMATE * #DYNAMIC_LABEL_PAIR_ESTIMATE_BYTES)
                    )
                });
                dogstatsd_dynamic_tag_line_exprs.push(quote! { self.#field_name.cardinality() });
                dogstatsd_delta_dynamic_tag_line_exprs
                    .push(quote! { self.#field_name.cardinality() });
                state_label_count_exprs.push(quote! { 0usize });
                // i64 Gauges are point-in-time, no delta tracking needed (always export current value)
                delta_exports.push(quote! {
                    let overflow = self.#field_name.overflow_count();
                    if overflow > 0 {
                        log::warn!(
                            "fast-telemetry: {} hit cardinality cap, {} records routed to overflow",
                            #statsd_metric_name,
                            overflow
                        );
                    }
                    fast_telemetry::DogStatsDExport::export_dogstatsd(&self.#field_name, output, #statsd_metric_name, tags);
                });
            }
            MetricKind::DynamicHistogram => {
                let count_state_field = format_ident!("{}_count", field_name);
                let sum_state_field = format_ident!("{}_sum", field_name);
                let count_metric_name = format!("{}.count", statsd_metric_name);
                let sum_metric_name = format!("{}.sum", statsd_metric_name);
                prom_dynamic_reserve_exprs.push(quote! {
                    self.#field_name.cardinality().saturating_mul(
                        #prom_metric_name.len()
                            + #PROM_DYNAMIC_COMPLEX_SERIES_OVERHEAD_BYTES
                            + (#DYNAMIC_LABELS_PER_SERIES_ESTIMATE * #DYNAMIC_LABEL_PAIR_ESTIMATE_BYTES)
                    )
                });
                dogstatsd_dynamic_reserve_exprs.push(quote! {
                    self.#field_name.cardinality().saturating_mul(
                        #statsd_metric_name.len()
                            + #DOGSTATSD_DYNAMIC_COMPLEX_SERIES_OVERHEAD_BYTES
                            + (#DYNAMIC_LABELS_PER_SERIES_ESTIMATE * #DYNAMIC_LABEL_PAIR_ESTIMATE_BYTES)
                    )
                });
                dogstatsd_delta_dynamic_reserve_exprs.push(quote! {
                    self.#field_name.cardinality().saturating_mul(
                        #statsd_metric_name.len()
                            + #DOGSTATSD_DYNAMIC_COMPLEX_SERIES_OVERHEAD_BYTES
                            + (#DYNAMIC_LABELS_PER_SERIES_ESTIMATE * #DYNAMIC_LABEL_PAIR_ESTIMATE_BYTES)
                    )
                });
                dogstatsd_dynamic_tag_line_exprs.push(quote! { self.#field_name.cardinality() });
                dogstatsd_delta_dynamic_tag_line_exprs
                    .push(quote! { self.#field_name.cardinality() });
                state_label_count_exprs.push(quote! {
                    core::cmp::max(self.#count_state_field.len(), self.#sum_state_field.len())
                });
                state_fields.push(quote! { #count_state_field: std::collections::HashMap<fast_telemetry::DynamicLabelSet, u64>, });
                state_fields.push(quote! { #sum_state_field: std::collections::HashMap<fast_telemetry::DynamicLabelSet, u64>, });
                state_inits.push(quote! { #count_state_field: std::collections::HashMap::new(), });
                state_inits.push(quote! { #sum_state_field: std::collections::HashMap::new(), });
                delta_exports.push(quote! {
                    let overflow = self.#field_name.overflow_count();
                    if overflow > 0 {
                        log::warn!(
                            "fast-telemetry: {} hit cardinality cap, {} records routed to overflow",
                            #statsd_metric_name,
                            overflow
                        );
                    }
                    let mut current_keys = std::collections::HashSet::new();
                    self.#field_name.visit_series(|labels, series| {
                        let key = fast_telemetry::DynamicLabelSet::from_canonical_pairs(labels);
                        current_keys.insert(key.clone());
                        let current_count = series.count();
                        let current_sum = series.sum();
                        let previous_count = state.#count_state_field.get(&key).copied().unwrap_or(0);
                        let previous_sum = state.#sum_state_field.get(&key).copied().unwrap_or(0);
                        let delta_count = if current_count >= previous_count {
                            current_count - previous_count
                        } else {
                            current_count
                        };
                        let delta_sum = if current_sum >= previous_sum {
                            current_sum - previous_sum
                        } else {
                            current_sum
                        };
                        state.#count_state_field.insert(key.clone(), current_count);
                        state.#sum_state_field.insert(key, current_sum);
                        fast_telemetry::__macro_support::__write_dogstatsd_dynamic_pairs(
                            output,
                            #count_metric_name,
                            delta_count,
                            "c",
                            labels,
                            tags,
                        );
                        fast_telemetry::__macro_support::__write_dogstatsd_dynamic_pairs(
                            output,
                            #sum_metric_name,
                            delta_sum,
                            "c",
                            labels,
                            tags,
                        );
                    });
                    // Prune state entries for evicted label sets
                    state.#count_state_field.retain(|k, _| current_keys.contains(k));
                    state.#sum_state_field.retain(|k, _| current_keys.contains(k));
                });
            }
            MetricKind::Gauge | MetricKind::GaugeF64 => {
                state_label_count_exprs.push(quote! { 0usize });
                // Gauges are point-in-time, no delta tracking needed (always export current value)
                delta_exports.push(quote! {
                    fast_telemetry::DogStatsDExport::export_dogstatsd(&self.#field_name, output, #statsd_metric_name, tags);
                });
            }
            MetricKind::Histogram => {
                let count_state_field = format_ident!("{}_count", field_name);
                let sum_state_field = format_ident!("{}_sum", field_name);
                let count_metric_name = format!("{}.count", statsd_metric_name);
                let sum_metric_name = format!("{}.sum", statsd_metric_name);
                state_label_count_exprs.push(quote! { 0usize });
                state_fields.push(quote! { #count_state_field: u64, });
                state_fields.push(quote! { #sum_state_field: u64, });
                state_inits.push(quote! { #count_state_field: 0, });
                state_inits.push(quote! { #sum_state_field: 0, });
                delta_exports.push(quote! {
                    let current_count = self.#field_name.count();
                    let current_sum = self.#field_name.sum();
                    let delta_count = if current_count >= state.#count_state_field {
                        current_count - state.#count_state_field
                    } else {
                        current_count
                    };
                    let delta_sum = if current_sum >= state.#sum_state_field {
                        current_sum - state.#sum_state_field
                    } else {
                        current_sum
                    };
                    state.#count_state_field = current_count;
                    state.#sum_state_field = current_sum;
                    fast_telemetry::__macro_support::__write_dogstatsd(output, #count_metric_name, delta_count, "c", tags);
                    fast_telemetry::__macro_support::__write_dogstatsd(output, #sum_metric_name, delta_sum, "c", tags);
                });
            }
            MetricKind::LabeledCounter(label_ty) => {
                state_label_count_exprs.push(quote! { 0usize });
                state_fields.push(quote! { #field_name: Vec<isize>, });
                state_inits.push(quote! {
                    #field_name: vec![0; <#label_ty as fast_telemetry::LabelEnum>::CARDINALITY],
                });
                delta_exports.push(quote! {
                    for idx in 0..<#label_ty as fast_telemetry::LabelEnum>::CARDINALITY {
                        let label = <#label_ty as fast_telemetry::LabelEnum>::from_index(idx);
                        let current = self.#field_name.get(label);
                        let delta = current - state.#field_name[idx];
                        state.#field_name[idx] = current;
                        fast_telemetry::__macro_support::__write_dogstatsd_with_label(
                            output,
                            #statsd_metric_name,
                            delta,
                            "c",
                            <#label_ty as fast_telemetry::LabelEnum>::LABEL_NAME,
                            label.variant_name(),
                            tags,
                        );
                    }
                });
            }
            MetricKind::LabeledGauge => {
                state_label_count_exprs.push(quote! { 0usize });
                delta_exports.push(quote! {
                    fast_telemetry::DogStatsDExport::export_dogstatsd(&self.#field_name, output, #statsd_metric_name, tags);
                });
            }
            MetricKind::LabeledHistogram(label_ty) => {
                let count_state_field = format_ident!("{}_count", field_name);
                let sum_state_field = format_ident!("{}_sum", field_name);
                let count_metric_name = format!("{}.count", statsd_metric_name);
                let sum_metric_name = format!("{}.sum", statsd_metric_name);
                state_label_count_exprs.push(quote! { 0usize });
                state_fields.push(quote! { #count_state_field: Vec<u64>, });
                state_fields.push(quote! { #sum_state_field: Vec<u64>, });
                state_inits.push(quote! {
                    #count_state_field: vec![0; <#label_ty as fast_telemetry::LabelEnum>::CARDINALITY],
                });
                state_inits.push(quote! {
                    #sum_state_field: vec![0; <#label_ty as fast_telemetry::LabelEnum>::CARDINALITY],
                });
                delta_exports.push(quote! {
                    for idx in 0..<#label_ty as fast_telemetry::LabelEnum>::CARDINALITY {
                        let label = <#label_ty as fast_telemetry::LabelEnum>::from_index(idx);
                        let current_count = self.#field_name.get(label).count();
                        let current_sum = self.#field_name.get(label).sum();
                        let delta_count = if current_count >= state.#count_state_field[idx] {
                            current_count - state.#count_state_field[idx]
                        } else {
                            current_count
                        };
                        let delta_sum = if current_sum >= state.#sum_state_field[idx] {
                            current_sum - state.#sum_state_field[idx]
                        } else {
                            current_sum
                        };
                        state.#count_state_field[idx] = current_count;
                        state.#sum_state_field[idx] = current_sum;
                        fast_telemetry::__macro_support::__write_dogstatsd_with_label(
                            output,
                            #count_metric_name,
                            delta_count,
                            "c",
                            <#label_ty as fast_telemetry::LabelEnum>::LABEL_NAME,
                            label.variant_name(),
                            tags,
                        );
                        fast_telemetry::__macro_support::__write_dogstatsd_with_label(
                            output,
                            #sum_metric_name,
                            delta_sum,
                            "c",
                            <#label_ty as fast_telemetry::LabelEnum>::LABEL_NAME,
                            label.variant_name(),
                            tags,
                        );
                    }
                });
            }
        }
    }

    let otlp_method = if enable_otlp {
        quote! {
            /// Export all metrics as OTLP protobuf `Metric` messages (cumulative temporality).
            ///
            /// `time_unix_nano` is a shared timestamp for all data points in this export cycle.
            /// Use `fast_telemetry::otlp::now_nanos()` to get the current time.
            ///
            /// Requires the `otlp` feature on the `fast-telemetry` dependency.
            pub fn export_otlp(&self, metrics: &mut Vec<fast_telemetry::otlp::pb::Metric>, time_unix_nano: u64) {
                #(#otlp_exports)*
            }
        }
    } else {
        quote! {}
    };

    let expanded = quote! {
        /// State for tracking DogStatsD delta values.
        #vis struct #state_name {
            #(#state_fields)*
        }

        impl #state_name {
            pub fn new() -> Self {
                Self {
                    #(#state_inits)*
                }
            }

            /// Total number of dynamic label sets currently retained in delta state maps.
            pub fn tracked_label_sets(&self) -> usize {
                0usize #(+ #state_label_count_exprs)*
            }
        }

        impl Default for #state_name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl #name {
            /// Export all metrics in Prometheus text exposition format.
            pub fn export_prometheus(&self, output: &mut String) {
                let __ft_prom_dynamic_reserve = 0usize #(+ #prom_dynamic_reserve_exprs)*;
                output.reserve(#prom_reserve_hint + __ft_prom_dynamic_reserve);
                #(#prometheus_exports)*
            }

            /// Export all metrics in DogStatsD format (cumulative).
            ///
            /// - `output`: String buffer to append to
            /// - `tags`: Additional tags to include (e.g., `&[("env", "prod")]`)
            pub fn export_dogstatsd(&self, output: &mut String, tags: &[(&str, &str)]) {
                let __ft_tag_bytes = if tags.is_empty() {
                    0usize
                } else {
                    #DOGSTATSD_TAG_PREFIX_BYTES
                        + tags.iter().map(|(k, v)| k.len() + v.len() + #DOGSTATSD_TAG_PAIR_OVERHEAD_BYTES).sum::<usize>()
                };
                let __ft_dynamic_reserve = 0usize #(+ #dogstatsd_dynamic_reserve_exprs)*;
                let __ft_dynamic_tag_lines = 0usize #(+ #dogstatsd_dynamic_tag_line_exprs)*;
                output.reserve(
                    #dogstatsd_reserve_hint
                        + __ft_dynamic_reserve
                        + __ft_tag_bytes.saturating_mul(#dogstatsd_tag_line_hint + __ft_dynamic_tag_lines)
                );
                #(#dogstatsd_exports)*
            }

            /// Export all metrics in DogStatsD format using per-sink delta temporality.
            ///
            /// Requires a mutable state object to track previous values.
            pub fn export_dogstatsd_delta(
                &self,
                output: &mut String,
                tags: &[(&str, &str)],
                state: &mut #state_name,
            ) {
                let __ft_tag_bytes = if tags.is_empty() {
                    0usize
                } else {
                    #DOGSTATSD_TAG_PREFIX_BYTES
                        + tags.iter().map(|(k, v)| k.len() + v.len() + #DOGSTATSD_TAG_PAIR_OVERHEAD_BYTES).sum::<usize>()
                };
                let __ft_dynamic_reserve = 0usize #(+ #dogstatsd_delta_dynamic_reserve_exprs)*;
                let __ft_dynamic_tag_lines = 0usize #(+ #dogstatsd_delta_dynamic_tag_line_exprs)*;
                output.reserve(
                    #dogstatsd_delta_reserve_hint
                        + __ft_dynamic_reserve
                        + __ft_tag_bytes.saturating_mul(#dogstatsd_delta_tag_line_hint + __ft_dynamic_tag_lines)
                );
                #(#delta_exports)*
            }

            /// Export all metrics in DogStatsD format with configurable temporality.
            pub fn export_dogstatsd_with_temporality(
                &self,
                output: &mut String,
                tags: &[(&str, &str)],
                temporality: fast_telemetry::Temporality,
                state: &mut #state_name,
            ) {
                match temporality {
                    fast_telemetry::Temporality::Cumulative => self.export_dogstatsd(output, tags),
                    fast_telemetry::Temporality::Delta => self.export_dogstatsd_delta(output, tags, state),
                }
            }

            #otlp_method
        }
    };

    Ok(TokenStream::from(expanded))
}

/// Derive macro for implementing `LabelEnum` on enums.
///
/// Automatically generates all required trait methods from the enum definition.
/// Converts variant names to snake_case for Prometheus label values.
///
/// # Attributes
///
/// - `#[label_name = "..."]` (required on enum): The Prometheus label name
/// - `#[label = "..."]` (optional on variant): Override the snake_case variant name
///
/// # Example
///
/// ```ignore
/// use fast_telemetry_macros::LabelEnum;
///
/// #[derive(LabelEnum)]
/// #[label_name = "method"]
/// enum HttpMethod {
///     Get,
///     Post,
///     Put,
///     Delete,
///     #[label = "other"]
///     Unknown,
/// }
///
/// // Generates:
/// // - CARDINALITY = 5
/// // - LABEL_NAME = "method"
/// // - as_index() returns 0, 1, 2, 3, 4
/// // - from_index() returns Get, Post, Put, Delete, Unknown
/// // - variant_name() returns "get", "post", "put", "delete", "other"
/// // ```
#[proc_macro_derive(LabelEnum, attributes(label_name, label))]
pub fn derive_label_enum(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match derive_label_enum_impl(input) {
        Ok(ts) => ts,
        Err(err) => err.to_compile_error().into(),
    }
}

fn derive_label_enum_impl(input: DeriveInput) -> syn::Result<TokenStream> {
    let name = &input.ident;

    // Extract label_name from enum attributes (required)
    let label_name = extract_label_name(&input.attrs).ok_or_else(|| {
        syn::Error::new_spanned(
            name,
            "LabelEnum requires #[label_name = \"...\"] attribute on the enum",
        )
    })?;

    // Get enum variants
    let variants = match &input.data {
        Data::Enum(data) => &data.variants,
        _ => {
            return Err(syn::Error::new_spanned(
                &input,
                "LabelEnum can only be derived for enums",
            ));
        }
    };
    if variants.is_empty() {
        return Err(syn::Error::new_spanned(
            name,
            "LabelEnum requires at least one variant",
        ));
    }

    let cardinality = variants.len();

    // Generate as_index match arms
    let as_index_arms: Vec<_> = variants
        .iter()
        .enumerate()
        .map(|(idx, variant)| {
            let variant_ident = &variant.ident;
            quote! { Self::#variant_ident => #idx, }
        })
        .collect();

    // Generate from_index match arms
    let from_index_arms: Vec<_> = variants
        .iter()
        .enumerate()
        .map(|(idx, variant)| {
            let variant_ident = &variant.ident;
            quote! { #idx => Self::#variant_ident, }
        })
        .collect();

    // Get the last variant for the default case
    let last_variant = &variants[variants.len() - 1].ident;

    // Generate variant_name match arms
    let variant_name_arms: Vec<_> = variants
        .iter()
        .map(|variant| {
            let variant_ident = &variant.ident;
            let label_value = extract_label_override(&variant.attrs)
                .unwrap_or_else(|| to_snake_case(&variant_ident.to_string()));
            quote! { Self::#variant_ident => #label_value, }
        })
        .collect();

    let expanded = quote! {
        impl fast_telemetry::LabelEnum for #name {
            const CARDINALITY: usize = #cardinality;
            const LABEL_NAME: &'static str = #label_name;

            fn as_index(self) -> usize {
                match self {
                    #(#as_index_arms)*
                }
            }

            fn from_index(index: usize) -> Self {
                match index {
                    #(#from_index_arms)*
                    _ => Self::#last_variant,
                }
            }

            fn variant_name(self) -> &'static str {
                match self {
                    #(#variant_name_arms)*
                }
            }
        }
    };

    Ok(TokenStream::from(expanded))
}

/// Extract #[label_name = "..."] from enum attributes.
fn extract_label_name(attrs: &[syn::Attribute]) -> Option<String> {
    for attr in attrs {
        if attr.path().is_ident("label_name")
            && let Meta::NameValue(nv) = &attr.meta
            && let Expr::Lit(expr_lit) = &nv.value
            && let Lit::Str(lit) = &expr_lit.lit
        {
            return Some(lit.value());
        }
    }
    None
}

/// Extract #[label = "..."] from variant attributes.
fn extract_label_override(attrs: &[syn::Attribute]) -> Option<String> {
    for attr in attrs {
        if attr.path().is_ident("label")
            && let Meta::NameValue(nv) = &attr.meta
            && let Expr::Lit(expr_lit) = &nv.value
            && let Lit::Str(lit) = &expr_lit.lit
        {
            return Some(lit.value());
        }
    }
    None
}

/// Convert PascalCase to snake_case.
fn to_snake_case(s: &str) -> String {
    let mut result = String::new();
    for (i, c) in s.chars().enumerate() {
        if c.is_uppercase() {
            if i > 0 {
                result.push('_');
            }
            for lower in c.to_lowercase() {
                result.push(lower);
            }
        } else {
            result.push(c);
        }
    }
    result
}

fn extract_metric_prefix(attrs: &[syn::Attribute]) -> Option<String> {
    for attr in attrs {
        if attr.path().is_ident("metric_prefix")
            && let Meta::NameValue(nv) = &attr.meta
            && let Expr::Lit(expr_lit) = &nv.value
            && let Lit::Str(lit) = &expr_lit.lit
        {
            return Some(lit.value());
        }
    }
    None
}

fn extract_help(attrs: &[syn::Attribute]) -> Option<String> {
    for attr in attrs {
        if attr.path().is_ident("help")
            && let Meta::NameValue(nv) = &attr.meta
            && let Expr::Lit(expr_lit) = &nv.value
            && let Lit::Str(lit) = &expr_lit.lit
        {
            return Some(lit.value());
        }
    }
    None
}
