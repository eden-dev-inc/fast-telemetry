use crate::{
    Counter, Distribution, DynamicCounter, DynamicDistribution, DynamicGauge, DynamicGaugeI64,
    DynamicHistogram, DynamicLabelSet, Gauge, GaugeF64, Histogram, LabelEnum, LabeledCounter,
    LabeledGauge, LabeledHistogram, LabeledSampledTimer, MaxGauge, MaxGaugeF64, MinGauge,
    MinGaugeF64, SampledTimer, exp_buckets::ExpBucketsSnapshot,
};

use super::fast_format::{FastFormat, push_f64_compact};

/// Trait for exporting a metric in DogStatsD format.
///
/// Format: `metric.name:value|type|#tag1:value1,tag2:value2`
///
/// Types:
/// - `c` - counter (increment)
/// - `g` - gauge (point-in-time value)
/// - `d` - distribution (percentile-capable: p50/p95/p99 in Datadog)
pub trait DogStatsDExport {
    /// Export this metric to the output string in DogStatsD format.
    ///
    /// - `output`: String buffer to append to (one line per metric, newline-terminated)
    /// - `name`: The metric name (with prefix already applied)
    /// - `tags`: Additional tags to append (e.g., `&[("env", "prod")]`)
    fn export_dogstatsd(&self, output: &mut String, name: &str, tags: &[(&str, &str)]);
}

#[inline]
fn push_display<T: FastFormat>(output: &mut String, value: T) {
    value.fast_push(output);
}

#[inline]
fn write_gauge_f64_value(output: &mut String, value: f64) {
    push_f64_compact(output, value);
}

/// Helper to append tags in DogStatsD format: `|#tag1:value1,tag2:value2`
fn append_tags(output: &mut String, tags: &[(&str, &str)]) {
    if !tags.is_empty() {
        output.push_str("|#");
        for (i, (k, v)) in tags.iter().enumerate() {
            if i > 0 {
                output.push(',');
            }
            output.push_str(k);
            output.push(':');
            output.push_str(v);
        }
    }
}

/// Helper to append tags with an additional label prepended.
fn append_tags_with_label(
    output: &mut String,
    label_name: &str,
    label_value: &str,
    tags: &[(&str, &str)],
) {
    output.push_str("|#");
    output.push_str(label_name);
    output.push(':');
    output.push_str(label_value);
    for (k, v) in tags {
        output.push(',');
        output.push_str(k);
        output.push(':');
        output.push_str(v);
    }
}

fn append_tags_with_dynamic_label_pairs(
    output: &mut String,
    labels: &[(String, String)],
    tags: &[(&str, &str)],
) {
    output.push_str("|#");
    let mut first = true;
    for (k, v) in labels {
        if !first {
            output.push(',');
        }
        first = false;
        output.push_str(k);
        output.push(':');
        output.push_str(v);
    }
    for (k, v) in tags {
        if !first {
            output.push(',');
        }
        first = false;
        output.push_str(k);
        output.push(':');
        output.push_str(v);
    }
}

fn append_tags_with_dynamic_labels(
    output: &mut String,
    labels: &DynamicLabelSet,
    tags: &[(&str, &str)],
) {
    append_tags_with_dynamic_label_pairs(output, labels.pairs(), tags);
}

#[doc(hidden)]
pub fn __write_dogstatsd(
    output: &mut String,
    name: &str,
    value: impl FastFormat,
    metric_type: &str,
    tags: &[(&str, &str)],
) {
    output.push_str(name);
    output.push(':');
    push_display(output, value);
    output.push('|');
    output.push_str(metric_type);
    append_tags(output, tags);
    output.push('\n');
}

#[doc(hidden)]
pub fn __write_dogstatsd_with_label(
    output: &mut String,
    name: &str,
    value: impl FastFormat,
    metric_type: &str,
    label_name: &str,
    label_value: &str,
    tags: &[(&str, &str)],
) {
    output.push_str(name);
    output.push(':');
    push_display(output, value);
    output.push('|');
    output.push_str(metric_type);
    append_tags_with_label(output, label_name, label_value, tags);
    output.push('\n');
}

#[doc(hidden)]
pub fn __write_dogstatsd_dynamic(
    output: &mut String,
    name: &str,
    value: impl FastFormat,
    metric_type: &str,
    labels: &DynamicLabelSet,
    tags: &[(&str, &str)],
) {
    output.push_str(name);
    output.push(':');
    push_display(output, value);
    output.push('|');
    output.push_str(metric_type);
    append_tags_with_dynamic_labels(output, labels, tags);
    output.push('\n');
}

#[doc(hidden)]
pub fn __write_dogstatsd_dynamic_pairs(
    output: &mut String,
    name: &str,
    value: impl FastFormat,
    metric_type: &str,
    labels: &[(String, String)],
    tags: &[(&str, &str)],
) {
    output.push_str(name);
    output.push(':');
    push_display(output, value);
    output.push('|');
    output.push_str(metric_type);
    append_tags_with_dynamic_label_pairs(output, labels, tags);
    output.push('\n');
}

/// Append a `|d` sample with optional sample_rate.
///
/// Format: `name:value|d[|@rate]` — tags appended by the caller afterward.
fn write_distribution_sample(output: &mut String, name: &str, value: u64, count: u64) {
    output.push_str(name);
    output.push(':');
    push_display(output, value);
    output.push_str("|d");
    if count > 1 {
        // sample_rate = 1/count tells the agent to multiply this sample by count
        output.push_str("|@");
        (1.0_f64 / count as f64).fast_push(output);
    }
}

/// Write DogStatsD `|d` distribution lines from an [`ExpBucketsSnapshot`].
///
/// Emits one line per non-zero bucket using the bucket midpoint as the
/// representative value and `@sample_rate` to encode the bucket count.
/// This allows Datadog to compute p50/p95/p99 percentiles.
#[doc(hidden)]
pub fn __write_dogstatsd_distribution(
    output: &mut String,
    name: &str,
    snap: &ExpBucketsSnapshot,
    tags: &[(&str, &str)],
) {
    for (value, count) in snap.iter_samples() {
        write_distribution_sample(output, name, value, count);
        append_tags(output, tags);
        output.push('\n');
    }
}

/// Write DogStatsD `|d` distribution lines with dynamic labels.
#[doc(hidden)]
pub fn __write_dogstatsd_distribution_dynamic(
    output: &mut String,
    name: &str,
    snap: &ExpBucketsSnapshot,
    labels: &DynamicLabelSet,
    tags: &[(&str, &str)],
) {
    for (value, count) in snap.iter_samples() {
        write_distribution_sample(output, name, value, count);
        append_tags_with_dynamic_labels(output, labels, tags);
        output.push('\n');
    }
}

fn write_dogstatsd_distribution_dynamic_pairs(
    output: &mut String,
    name: &str,
    snap: &ExpBucketsSnapshot,
    labels: &[(String, String)],
    tags: &[(&str, &str)],
) {
    for (value, count) in snap.iter_samples() {
        write_distribution_sample(output, name, value, count);
        append_tags_with_dynamic_label_pairs(output, labels, tags);
        output.push('\n');
    }
}

/// Write DogStatsD `|d` distribution lines for the **delta** between the current
/// snapshot and previously-stored bucket counts.
///
/// `previous` is a `[u64; 65]` array: indices 0..64 are positive buckets,
/// index 64 is the zero-count.  Updated in-place to the current values.
#[doc(hidden)]
pub fn __write_dogstatsd_distribution_delta(
    output: &mut String,
    name: &str,
    current: &ExpBucketsSnapshot,
    previous: &mut [u64; 65],
    tags: &[(&str, &str)],
) {
    for (i, &cur) in current.positive.iter().enumerate() {
        let delta = cur.saturating_sub(previous[i]);
        previous[i] = cur;
        if delta > 0 {
            let value = ExpBucketsSnapshot::bucket_midpoint(i);
            write_distribution_sample(output, name, value, delta);
            append_tags(output, tags);
            output.push('\n');
        }
    }
    let zero_delta = current.zero_count.saturating_sub(previous[64]);
    previous[64] = current.zero_count;
    if zero_delta > 0 {
        write_distribution_sample(output, name, 0, zero_delta);
        append_tags(output, tags);
        output.push('\n');
    }
}

/// Write DogStatsD `|d` distribution delta lines with dynamic labels.
#[doc(hidden)]
pub fn __write_dogstatsd_distribution_delta_dynamic(
    output: &mut String,
    name: &str,
    current: &ExpBucketsSnapshot,
    previous: &mut [u64; 65],
    labels: &DynamicLabelSet,
    tags: &[(&str, &str)],
) {
    for (i, &cur) in current.positive.iter().enumerate() {
        let delta = cur.saturating_sub(previous[i]);
        previous[i] = cur;
        if delta > 0 {
            let value = ExpBucketsSnapshot::bucket_midpoint(i);
            write_distribution_sample(output, name, value, delta);
            append_tags_with_dynamic_labels(output, labels, tags);
            output.push('\n');
        }
    }
    let zero_delta = current.zero_count.saturating_sub(previous[64]);
    previous[64] = current.zero_count;
    if zero_delta > 0 {
        write_distribution_sample(output, name, 0, zero_delta);
        append_tags_with_dynamic_labels(output, labels, tags);
        output.push('\n');
    }
}

/// Write DogStatsD `|d` distribution delta lines with dynamic labels as borrowed pairs.
#[doc(hidden)]
pub fn __write_dogstatsd_distribution_delta_dynamic_pairs(
    output: &mut String,
    name: &str,
    current: &ExpBucketsSnapshot,
    previous: &mut [u64; 65],
    labels: &[(String, String)],
    tags: &[(&str, &str)],
) {
    for (i, &cur) in current.positive.iter().enumerate() {
        let delta = cur.saturating_sub(previous[i]);
        previous[i] = cur;
        if delta > 0 {
            let value = ExpBucketsSnapshot::bucket_midpoint(i);
            write_distribution_sample(output, name, value, delta);
            append_tags_with_dynamic_label_pairs(output, labels, tags);
            output.push('\n');
        }
    }
    let zero_delta = current.zero_count.saturating_sub(previous[64]);
    previous[64] = current.zero_count;
    if zero_delta > 0 {
        write_distribution_sample(output, name, 0, zero_delta);
        append_tags_with_dynamic_label_pairs(output, labels, tags);
        output.push('\n');
    }
}

impl DogStatsDExport for Counter {
    fn export_dogstatsd(&self, output: &mut String, name: &str, tags: &[(&str, &str)]) {
        output.push_str(name);
        output.push(':');
        push_display(output, self.sum());
        output.push_str("|c");
        append_tags(output, tags);
        output.push('\n');
    }
}

impl DogStatsDExport for Gauge {
    fn export_dogstatsd(&self, output: &mut String, name: &str, tags: &[(&str, &str)]) {
        output.push_str(name);
        output.push(':');
        push_display(output, self.get());
        output.push_str("|g");
        append_tags(output, tags);
        output.push('\n');
    }
}

impl DogStatsDExport for MaxGauge {
    fn export_dogstatsd(&self, output: &mut String, name: &str, tags: &[(&str, &str)]) {
        output.push_str(name);
        output.push(':');
        push_display(output, self.get());
        output.push_str("|g");
        append_tags(output, tags);
        output.push('\n');
    }
}

impl DogStatsDExport for MaxGaugeF64 {
    fn export_dogstatsd(&self, output: &mut String, name: &str, tags: &[(&str, &str)]) {
        output.push_str(name);
        output.push(':');
        write_gauge_f64_value(output, self.get());
        output.push_str("|g");
        append_tags(output, tags);
        output.push('\n');
    }
}

impl DogStatsDExport for MinGauge {
    fn export_dogstatsd(&self, output: &mut String, name: &str, tags: &[(&str, &str)]) {
        output.push_str(name);
        output.push(':');
        push_display(output, self.get());
        output.push_str("|g");
        append_tags(output, tags);
        output.push('\n');
    }
}

impl DogStatsDExport for MinGaugeF64 {
    fn export_dogstatsd(&self, output: &mut String, name: &str, tags: &[(&str, &str)]) {
        output.push_str(name);
        output.push(':');
        write_gauge_f64_value(output, self.get());
        output.push_str("|g");
        append_tags(output, tags);
        output.push('\n');
    }
}

impl DogStatsDExport for GaugeF64 {
    fn export_dogstatsd(&self, output: &mut String, name: &str, tags: &[(&str, &str)]) {
        output.push_str(name);
        output.push(':');
        // Format with up to 6 decimal places, trimming trailing zeros.
        write_gauge_f64_value(output, self.get());
        output.push_str("|g");
        append_tags(output, tags);
        output.push('\n');
    }
}

impl DogStatsDExport for Histogram {
    /// Export histogram as count + sum metrics.
    ///
    /// DogStatsD distributions expect raw samples, not pre-aggregated buckets.
    /// We export `name.count` and `name.sum` as counters instead.
    fn export_dogstatsd(&self, output: &mut String, name: &str, tags: &[(&str, &str)]) {
        output.push_str(name);
        output.push_str(".count:");
        push_display(output, self.count());
        output.push_str("|c");
        append_tags(output, tags);
        output.push('\n');

        output.push_str(name);
        output.push_str(".sum:");
        push_display(output, self.sum());
        output.push_str("|c");
        append_tags(output, tags);
        output.push('\n');
    }
}

impl DogStatsDExport for SampledTimer {
    fn export_dogstatsd(&self, output: &mut String, name: &str, tags: &[(&str, &str)]) {
        let calls_name = concat_two(name, ".calls");
        let samples_name = concat_two(name, ".samples");
        self.calls_metric()
            .export_dogstatsd(output, &calls_name, tags);
        self.histogram()
            .export_dogstatsd(output, &samples_name, tags);
    }
}

#[inline]
fn concat_two(a: &str, b: &str) -> String {
    let mut s = String::with_capacity(a.len() + b.len());
    s.push_str(a);
    s.push_str(b);
    s
}

impl DogStatsDExport for Distribution {
    /// Export distribution as `|d` samples for Datadog percentile computation.
    fn export_dogstatsd(&self, output: &mut String, name: &str, tags: &[(&str, &str)]) {
        let snap = self.buckets_snapshot();
        __write_dogstatsd_distribution(output, name, &snap, tags);
    }
}

impl<L: LabelEnum> DogStatsDExport for LabeledCounter<L> {
    fn export_dogstatsd(&self, output: &mut String, name: &str, tags: &[(&str, &str)]) {
        for (label, count) in self.iter() {
            output.push_str(name);
            output.push(':');
            push_display(output, count);
            output.push_str("|c");
            append_tags_with_label(output, L::LABEL_NAME, label.variant_name(), tags);
            output.push('\n');
        }
    }
}

impl<L: LabelEnum> DogStatsDExport for LabeledGauge<L> {
    fn export_dogstatsd(&self, output: &mut String, name: &str, tags: &[(&str, &str)]) {
        for (label, value) in self.iter() {
            output.push_str(name);
            output.push(':');
            push_display(output, value);
            output.push_str("|g");
            append_tags_with_label(output, L::LABEL_NAME, label.variant_name(), tags);
            output.push('\n');
        }
    }
}

impl<L: LabelEnum> DogStatsDExport for LabeledHistogram<L> {
    fn export_dogstatsd(&self, output: &mut String, name: &str, tags: &[(&str, &str)]) {
        for (label, _buckets, sum, count) in self.iter() {
            let variant = label.variant_name();

            output.push_str(name);
            output.push_str(".count:");
            push_display(output, count);
            output.push_str("|c");
            append_tags_with_label(output, L::LABEL_NAME, variant, tags);
            output.push('\n');

            output.push_str(name);
            output.push_str(".sum:");
            push_display(output, sum);
            output.push_str("|c");
            append_tags_with_label(output, L::LABEL_NAME, variant, tags);
            output.push('\n');
        }
    }
}

impl<L: LabelEnum> DogStatsDExport for LabeledSampledTimer<L> {
    fn export_dogstatsd(&self, output: &mut String, name: &str, tags: &[(&str, &str)]) {
        let calls_name = concat_two(name, ".calls");
        let samples_count_name = concat_three(name, ".samples", ".count");
        let samples_sum_name = concat_three(name, ".samples", ".sum");

        for (label, calls, histogram) in self.iter() {
            let variant = label.variant_name();

            __write_dogstatsd_with_label(
                output,
                &calls_name,
                calls.sum(),
                "c",
                L::LABEL_NAME,
                variant,
                tags,
            );

            __write_dogstatsd_with_label(
                output,
                &samples_count_name,
                histogram.count(),
                "c",
                L::LABEL_NAME,
                variant,
                tags,
            );

            __write_dogstatsd_with_label(
                output,
                &samples_sum_name,
                histogram.sum(),
                "c",
                L::LABEL_NAME,
                variant,
                tags,
            );
        }
    }
}

#[inline]
fn concat_three(a: &str, b: &str, c: &str) -> String {
    let mut s = String::with_capacity(a.len() + b.len() + c.len());
    s.push_str(a);
    s.push_str(b);
    s.push_str(c);
    s
}

impl DogStatsDExport for DynamicCounter {
    fn export_dogstatsd(&self, output: &mut String, name: &str, tags: &[(&str, &str)]) {
        self.visit_series(|labels, count| {
            __write_dogstatsd_dynamic_pairs(output, name, count, "c", labels, tags);
        });
    }
}

impl DogStatsDExport for DynamicGauge {
    fn export_dogstatsd(&self, output: &mut String, name: &str, tags: &[(&str, &str)]) {
        self.visit_series(|labels, value| {
            output.push_str(name);
            output.push(':');
            write_gauge_f64_value(output, value);
            output.push_str("|g");
            append_tags_with_dynamic_label_pairs(output, labels, tags);
            output.push('\n');
        });
    }
}

impl DogStatsDExport for DynamicGaugeI64 {
    fn export_dogstatsd(&self, output: &mut String, name: &str, tags: &[(&str, &str)]) {
        self.visit_series(|labels, value| {
            __write_dogstatsd_dynamic_pairs(output, name, value, "g", labels, tags);
        });
    }
}

impl DogStatsDExport for DynamicHistogram {
    fn export_dogstatsd(&self, output: &mut String, name: &str, tags: &[(&str, &str)]) {
        let count_name = concat_two(name, ".count");
        let sum_name = concat_two(name, ".sum");
        self.visit_series(|labels, series| {
            __write_dogstatsd_dynamic_pairs(output, &count_name, series.count(), "c", labels, tags);
            __write_dogstatsd_dynamic_pairs(output, &sum_name, series.sum(), "c", labels, tags);
        });
    }
}

impl DogStatsDExport for DynamicDistribution {
    /// Export distribution as `|d` samples per label set for Datadog percentile computation.
    fn export_dogstatsd(&self, output: &mut String, name: &str, tags: &[(&str, &str)]) {
        self.visit_series(|labels, _count, _sum, snap| {
            write_dogstatsd_distribution_dynamic_pairs(output, name, &snap, labels, tags);
        });
    }
}

#[cfg(test)]
mod tests {
    use super::DogStatsDExport;
    use crate::{Counter, Distribution, DynamicCounter, DynamicDistribution, Gauge, Histogram};

    #[test]
    fn test_dogstatsd_counter() {
        let counter = Counter::new(4);
        counter.inc();
        counter.inc();

        let mut output = String::new();
        counter.export_dogstatsd(&mut output, "test.counter", &[]);

        assert_eq!(output, "test.counter:2|c\n");
    }

    #[test]
    fn test_dogstatsd_counter_with_tags() {
        let counter = Counter::new(4);
        counter.add(100);

        let mut output = String::new();
        counter.export_dogstatsd(
            &mut output,
            "test.counter",
            &[("env", "prod"), ("host", "web01")],
        );

        assert_eq!(output, "test.counter:100|c|#env:prod,host:web01\n");
    }

    #[test]
    fn test_dogstatsd_gauge() {
        let gauge = Gauge::new();
        gauge.set(42);

        let mut output = String::new();
        gauge.export_dogstatsd(&mut output, "test.gauge", &[]);

        assert_eq!(output, "test.gauge:42|g\n");
    }

    #[test]
    fn test_dogstatsd_gauge_with_tags() {
        let gauge = Gauge::new();
        gauge.set(-10);

        let mut output = String::new();
        gauge.export_dogstatsd(&mut output, "memory.used", &[("region", "us-east")]);

        assert_eq!(output, "memory.used:-10|g|#region:us-east\n");
    }

    #[test]
    fn test_dogstatsd_histogram() {
        let histogram = Histogram::new(&[10, 100], 4);
        histogram.record(5);
        histogram.record(50);
        histogram.record(500);

        let mut output = String::new();
        histogram.export_dogstatsd(&mut output, "latency", &[]);

        assert!(output.contains("latency.count:3|c\n"));
        assert!(output.contains("latency.sum:555|c\n"));
    }

    #[test]
    fn test_dogstatsd_histogram_with_tags() {
        let histogram = Histogram::new(&[100], 4);
        histogram.record(50);
        histogram.record(150);

        let mut output = String::new();
        histogram.export_dogstatsd(&mut output, "latency", &[("service", "api")]);

        assert!(output.contains("latency.count:2|c|#service:api\n"));
        assert!(output.contains("latency.sum:200|c|#service:api\n"));
    }

    #[test]
    fn test_dogstatsd_distribution() {
        let dist = Distribution::new(4);
        dist.record(100);
        dist.record(200);
        dist.record(300);

        let mut output = String::new();
        dist.export_dogstatsd(&mut output, "latency", &[]);

        assert!(output.contains("latency:96|d\n"));
        assert!(output.contains("latency:192|d\n"));
        assert!(output.contains("latency:384|d\n"));
    }

    #[test]
    fn test_dogstatsd_distribution_with_tags() {
        let dist = Distribution::new(4);
        dist.record(50);
        dist.record(150);

        let mut output = String::new();
        dist.export_dogstatsd(&mut output, "latency", &[("service", "api")]);

        assert!(output.contains("latency:48|d|#service:api\n"));
        assert!(output.contains("latency:192|d|#service:api\n"));
    }

    #[test]
    fn test_dogstatsd_distribution_empty() {
        let dist = Distribution::new(4);

        let mut output = String::new();
        dist.export_dogstatsd(&mut output, "latency", &[]);

        assert!(output.is_empty());
    }

    #[test]
    fn test_dogstatsd_distribution_sample_rate() {
        let dist = Distribution::new(4);
        dist.record(100);
        dist.record(100);
        dist.record(100);

        let mut output = String::new();
        dist.export_dogstatsd(&mut output, "latency", &[]);

        let line = output.lines().next().expect("should have a line");
        assert!(line.starts_with("latency:96|d|@"));
        assert!(line.contains("|@0.3333333333333333"));
    }

    #[test]
    fn test_dogstatsd_dynamic_counter() {
        let counter = DynamicCounter::new(4);
        counter.add(&[("endpoint", "ep1"), ("method", "GET")], 3);

        let mut output = String::new();
        counter.export_dogstatsd(&mut output, "requests", &[("env", "prod")]);

        assert!(output.contains("requests:3|c|#"));
        assert!(output.contains("endpoint:ep1"));
        assert!(output.contains("method:GET"));
        assert!(output.contains("env:prod"));
    }

    #[test]
    fn test_dogstatsd_dynamic_distribution() {
        let dist = DynamicDistribution::new(4);
        dist.record(&[("endpoint", "ep1")], 100);
        dist.record(&[("endpoint", "ep1")], 100);

        let mut output = String::new();
        dist.export_dogstatsd(&mut output, "latency", &[("env", "prod")]);

        assert!(output.contains("latency:96|d|@0.5|#endpoint:ep1,env:prod"));
    }
}
