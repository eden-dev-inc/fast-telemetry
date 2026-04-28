use super::fast_format::FastFormat;
use crate::{
    Counter, Distribution, DynamicCounter, DynamicDistribution, DynamicGauge, DynamicGaugeI64,
    DynamicHistogram, Gauge, GaugeF64, Histogram, LabelEnum, LabeledCounter, LabeledGauge,
    LabeledHistogram, LabeledSampledTimer, MaxGauge, MaxGaugeF64, MinGauge, MinGaugeF64,
    SampledTimer,
};

/// Trait for exporting a metric in Prometheus text exposition format.
pub trait PrometheusExport {
    /// Export this metric to the output string.
    ///
    /// - `output`: String buffer to append to
    /// - `name`: The metric name (with prefix already applied)
    /// - `help`: The help text for this metric
    fn export_prometheus(&self, output: &mut String, name: &str, help: &str);
}

#[inline]
fn push_display<T: FastFormat>(output: &mut String, value: T) {
    value.fast_push(output);
}

fn write_dynamic_labels(output: &mut String, labels: &[(String, String)]) {
    for (idx, (k, v)) in labels.iter().enumerate() {
        if idx > 0 {
            output.push(',');
        }
        output.push_str(k);
        output.push_str("=\"");
        output.push_str(v);
        output.push('"');
    }
}

fn write_labeled_counter_series<L, I>(output: &mut String, name: &str, help: &str, series: I)
where
    L: LabelEnum,
    I: IntoIterator<Item = (L, u64)>,
{
    output.push_str("# HELP ");
    output.push_str(name);
    output.push(' ');
    output.push_str(help);
    output.push_str("\n# TYPE ");
    output.push_str(name);
    output.push_str(" counter\n");

    for (label, count) in series {
        output.push_str(name);
        output.push('{');
        output.push_str(L::LABEL_NAME);
        output.push_str("=\"");
        output.push_str(label.variant_name());
        output.push_str("\"} ");
        push_display(output, count);
        output.push('\n');
    }
}

fn write_labeled_histogram_series<L, I>(output: &mut String, name: &str, help: &str, series: I)
where
    L: LabelEnum,
    I: IntoIterator<Item = (L, Vec<(u64, u64)>, u64, u64)>,
{
    output.push_str("# HELP ");
    output.push_str(name);
    output.push(' ');
    output.push_str(help);
    output.push_str("\n# TYPE ");
    output.push_str(name);
    output.push_str(" histogram\n");

    for (label, buckets, sum, count) in series {
        let variant = label.variant_name();

        for (bound, bucket_count) in buckets {
            output.push_str(name);
            output.push_str("_bucket{");
            output.push_str(L::LABEL_NAME);
            output.push_str("=\"");
            output.push_str(variant);
            output.push_str("\",le=\"");
            if bound == u64::MAX {
                output.push_str("+Inf");
            } else {
                push_display(output, bound);
            }
            output.push_str("\"} ");
            push_display(output, bucket_count);
            output.push('\n');
        }

        output.push_str(name);
        output.push_str("_sum{");
        output.push_str(L::LABEL_NAME);
        output.push_str("=\"");
        output.push_str(variant);
        output.push_str("\"} ");
        push_display(output, sum);
        output.push('\n');

        output.push_str(name);
        output.push_str("_count{");
        output.push_str(L::LABEL_NAME);
        output.push_str("=\"");
        output.push_str(variant);
        output.push_str("\"} ");
        push_display(output, count);
        output.push('\n');
    }
}

impl PrometheusExport for Counter {
    fn export_prometheus(&self, output: &mut String, name: &str, help: &str) {
        output.push_str("# HELP ");
        output.push_str(name);
        output.push(' ');
        output.push_str(help);
        output.push_str("\n# TYPE ");
        output.push_str(name);
        output.push_str(" counter\n");
        output.push_str(name);
        output.push(' ');
        push_display(output, self.sum());
        output.push('\n');
    }
}

impl PrometheusExport for Gauge {
    fn export_prometheus(&self, output: &mut String, name: &str, help: &str) {
        output.push_str("# HELP ");
        output.push_str(name);
        output.push(' ');
        output.push_str(help);
        output.push_str("\n# TYPE ");
        output.push_str(name);
        output.push_str(" gauge\n");
        output.push_str(name);
        output.push(' ');
        push_display(output, self.get());
        output.push('\n');
    }
}

impl PrometheusExport for GaugeF64 {
    fn export_prometheus(&self, output: &mut String, name: &str, help: &str) {
        output.push_str("# HELP ");
        output.push_str(name);
        output.push(' ');
        output.push_str(help);
        output.push_str("\n# TYPE ");
        output.push_str(name);
        output.push_str(" gauge\n");
        output.push_str(name);
        output.push(' ');
        push_display(output, self.get());
        output.push('\n');
    }
}

impl PrometheusExport for MaxGauge {
    fn export_prometheus(&self, output: &mut String, name: &str, help: &str) {
        output.push_str("# HELP ");
        output.push_str(name);
        output.push(' ');
        output.push_str(help);
        output.push_str("\n# TYPE ");
        output.push_str(name);
        output.push_str(" gauge\n");
        output.push_str(name);
        output.push(' ');
        push_display(output, self.get());
        output.push('\n');
    }
}

impl PrometheusExport for MaxGaugeF64 {
    fn export_prometheus(&self, output: &mut String, name: &str, help: &str) {
        output.push_str("# HELP ");
        output.push_str(name);
        output.push(' ');
        output.push_str(help);
        output.push_str("\n# TYPE ");
        output.push_str(name);
        output.push_str(" gauge\n");
        output.push_str(name);
        output.push(' ');
        push_display(output, self.get());
        output.push('\n');
    }
}

impl PrometheusExport for MinGauge {
    fn export_prometheus(&self, output: &mut String, name: &str, help: &str) {
        output.push_str("# HELP ");
        output.push_str(name);
        output.push(' ');
        output.push_str(help);
        output.push_str("\n# TYPE ");
        output.push_str(name);
        output.push_str(" gauge\n");
        output.push_str(name);
        output.push(' ');
        push_display(output, self.get());
        output.push('\n');
    }
}

impl PrometheusExport for MinGaugeF64 {
    fn export_prometheus(&self, output: &mut String, name: &str, help: &str) {
        output.push_str("# HELP ");
        output.push_str(name);
        output.push(' ');
        output.push_str(help);
        output.push_str("\n# TYPE ");
        output.push_str(name);
        output.push_str(" gauge\n");
        output.push_str(name);
        output.push(' ');
        push_display(output, self.get());
        output.push('\n');
    }
}

impl PrometheusExport for Histogram {
    fn export_prometheus(&self, output: &mut String, name: &str, help: &str) {
        output.push_str("# HELP ");
        output.push_str(name);
        output.push(' ');
        output.push_str(help);
        output.push_str("\n# TYPE ");
        output.push_str(name);
        output.push_str(" histogram\n");

        for (bound, count) in self.buckets_cumulative() {
            output.push_str(name);
            output.push_str("_bucket{le=\"");
            if bound == u64::MAX {
                output.push_str("+Inf");
            } else {
                push_display(output, bound);
            }
            output.push_str("\"} ");
            push_display(output, count);
            output.push('\n');
        }

        output.push_str(name);
        output.push_str("_sum ");
        push_display(output, self.sum());
        output.push('\n');

        output.push_str(name);
        output.push_str("_count ");
        push_display(output, self.count());
        output.push('\n');
    }
}

impl PrometheusExport for SampledTimer {
    fn export_prometheus(&self, output: &mut String, name: &str, help: &str) {
        let calls_name = format!("{name}_calls");
        let samples_name = format!("{name}_samples");
        let calls_help = format!("{help} total calls");
        let samples_help = format!("{help} sampled latency in nanoseconds");
        self.calls_metric()
            .export_prometheus(output, &calls_name, &calls_help);
        self.histogram()
            .export_prometheus(output, &samples_name, &samples_help);
    }
}

impl PrometheusExport for Distribution {
    /// Export distribution as summary (count + sum only, no quantiles).
    fn export_prometheus(&self, output: &mut String, name: &str, help: &str) {
        output.push_str("# HELP ");
        output.push_str(name);
        output.push(' ');
        output.push_str(help);
        output.push_str("\n# TYPE ");
        output.push_str(name);
        output.push_str(" summary\n");

        let (sum, count) = self.sum_and_count();

        output.push_str(name);
        output.push_str("_sum ");
        push_display(output, sum);
        output.push('\n');

        output.push_str(name);
        output.push_str("_count ");
        push_display(output, count);
        output.push('\n');
    }
}

impl<L: LabelEnum> PrometheusExport for LabeledCounter<L> {
    fn export_prometheus(&self, output: &mut String, name: &str, help: &str) {
        write_labeled_counter_series::<L, _>(
            output,
            name,
            help,
            self.iter().map(|(label, count)| (label, count as u64)),
        );
    }
}

impl<L: LabelEnum> PrometheusExport for LabeledGauge<L> {
    fn export_prometheus(&self, output: &mut String, name: &str, help: &str) {
        output.push_str("# HELP ");
        output.push_str(name);
        output.push(' ');
        output.push_str(help);
        output.push_str("\n# TYPE ");
        output.push_str(name);
        output.push_str(" gauge\n");

        for (label, value) in self.iter() {
            output.push_str(name);
            output.push('{');
            output.push_str(L::LABEL_NAME);
            output.push_str("=\"");
            output.push_str(label.variant_name());
            output.push_str("\"} ");
            push_display(output, value);
            output.push('\n');
        }
    }
}

impl<L: LabelEnum> PrometheusExport for LabeledHistogram<L> {
    fn export_prometheus(&self, output: &mut String, name: &str, help: &str) {
        write_labeled_histogram_series::<L, _>(output, name, help, self.iter());
    }
}

impl<L: LabelEnum> PrometheusExport for LabeledSampledTimer<L> {
    fn export_prometheus(&self, output: &mut String, name: &str, help: &str) {
        let calls_name = format!("{name}_calls");
        let samples_name = format!("{name}_samples");
        let calls_help = format!("{help} total calls");
        let samples_help = format!("{help} sampled latency in nanoseconds");

        write_labeled_counter_series::<L, _>(
            output,
            &calls_name,
            &calls_help,
            self.iter()
                .map(|(label, calls, _)| (label, calls.sum() as u64)),
        );
        write_labeled_histogram_series::<L, _>(
            output,
            &samples_name,
            &samples_help,
            self.iter().map(|(label, _, histogram)| {
                (
                    label,
                    histogram.buckets_cumulative(),
                    histogram.sum(),
                    histogram.count(),
                )
            }),
        );
    }
}

impl PrometheusExport for DynamicCounter {
    fn export_prometheus(&self, output: &mut String, name: &str, help: &str) {
        output.push_str("# HELP ");
        output.push_str(name);
        output.push(' ');
        output.push_str(help);
        output.push_str("\n# TYPE ");
        output.push_str(name);
        output.push_str(" counter\n");

        self.visit_series(|labels, count| {
            output.push_str(name);
            output.push('{');
            write_dynamic_labels(output, labels);
            output.push_str("} ");
            push_display(output, count);
            output.push('\n');
        });
    }
}

impl PrometheusExport for DynamicGauge {
    fn export_prometheus(&self, output: &mut String, name: &str, help: &str) {
        output.push_str("# HELP ");
        output.push_str(name);
        output.push(' ');
        output.push_str(help);
        output.push_str("\n# TYPE ");
        output.push_str(name);
        output.push_str(" gauge\n");

        self.visit_series(|labels, value| {
            output.push_str(name);
            output.push('{');
            write_dynamic_labels(output, labels);
            output.push_str("} ");
            push_display(output, value);
            output.push('\n');
        });
    }
}

impl PrometheusExport for DynamicGaugeI64 {
    fn export_prometheus(&self, output: &mut String, name: &str, help: &str) {
        output.push_str("# HELP ");
        output.push_str(name);
        output.push(' ');
        output.push_str(help);
        output.push_str("\n# TYPE ");
        output.push_str(name);
        output.push_str(" gauge\n");

        self.visit_series(|labels, value| {
            output.push_str(name);
            output.push('{');
            write_dynamic_labels(output, labels);
            output.push_str("} ");
            push_display(output, value);
            output.push('\n');
        });
    }
}

impl PrometheusExport for DynamicHistogram {
    fn export_prometheus(&self, output: &mut String, name: &str, help: &str) {
        output.push_str("# HELP ");
        output.push_str(name);
        output.push(' ');
        output.push_str(help);
        output.push_str("\n# TYPE ");
        output.push_str(name);
        output.push_str(" histogram\n");

        self.visit_series(|labels, series| {
            for (bound, bucket_count) in series.buckets_cumulative_iter() {
                output.push_str(name);
                output.push_str("_bucket{");
                write_dynamic_labels(output, labels);
                if !labels.is_empty() {
                    output.push(',');
                }
                output.push_str("le=\"");
                if bound == u64::MAX {
                    output.push_str("+Inf");
                } else {
                    push_display(output, bound);
                }
                output.push_str("\"} ");
                push_display(output, bucket_count);
                output.push('\n');
            }

            output.push_str(name);
            output.push_str("_sum{");
            write_dynamic_labels(output, labels);
            output.push_str("} ");
            push_display(output, series.sum());
            output.push('\n');

            output.push_str(name);
            output.push_str("_count{");
            write_dynamic_labels(output, labels);
            output.push_str("} ");
            push_display(output, series.count());
            output.push('\n');
        });
    }
}

impl PrometheusExport for DynamicDistribution {
    /// Export distribution as summary (count + sum only, no quantiles).
    fn export_prometheus(&self, output: &mut String, name: &str, help: &str) {
        output.push_str("# HELP ");
        output.push_str(name);
        output.push(' ');
        output.push_str(help);
        output.push_str("\n# TYPE ");
        output.push_str(name);
        output.push_str(" summary\n");

        self.visit_series(|labels, count, sum, _snap| {
            output.push_str(name);
            output.push_str("_sum{");
            write_dynamic_labels(output, labels);
            output.push_str("} ");
            push_display(output, sum);
            output.push('\n');

            output.push_str(name);
            output.push_str("_count{");
            write_dynamic_labels(output, labels);
            output.push_str("} ");
            push_display(output, count);
            output.push('\n');
        });
    }
}

#[cfg(test)]
mod tests {
    use super::PrometheusExport;
    use crate::{Counter, Distribution, DynamicCounter, DynamicHistogram, Gauge, Histogram};

    #[test]
    fn test_prometheus_counter() {
        let counter = Counter::new(4);
        counter.inc();
        counter.inc();

        let mut output = String::new();
        counter.export_prometheus(&mut output, "test_counter", "A test counter");

        assert!(output.contains("# HELP test_counter A test counter"));
        assert!(output.contains("# TYPE test_counter counter"));
        assert!(output.contains("test_counter 2"));
    }

    #[test]
    fn test_prometheus_gauge() {
        let gauge = Gauge::new();
        gauge.set(42);

        let mut output = String::new();
        gauge.export_prometheus(&mut output, "test_gauge", "A test gauge");

        assert!(output.contains("# HELP test_gauge A test gauge"));
        assert!(output.contains("# TYPE test_gauge gauge"));
        assert!(output.contains("test_gauge 42"));
    }

    #[test]
    fn test_prometheus_histogram() {
        let histogram = Histogram::new(&[10, 100], 4);
        histogram.record(5);
        histogram.record(50);
        histogram.record(500);

        let mut output = String::new();
        histogram.export_prometheus(&mut output, "test_hist", "A test histogram");

        assert!(output.contains("# HELP test_hist A test histogram"));
        assert!(output.contains("# TYPE test_hist histogram"));
        assert!(output.contains("test_hist_bucket{le=\"10\"} 1"));
        assert!(output.contains("test_hist_bucket{le=\"100\"} 2"));
        assert!(output.contains("test_hist_bucket{le=\"+Inf\"} 3"));
        assert!(output.contains("test_hist_count 3"));
    }

    #[test]
    fn test_prometheus_distribution() {
        let dist = Distribution::new(4);
        dist.record(100);
        dist.record(200);
        dist.record(300);

        let mut output = String::new();
        dist.export_prometheus(&mut output, "latency", "Request latency");

        assert!(output.contains("# HELP latency Request latency"));
        assert!(output.contains("# TYPE latency summary"));
        assert!(output.contains("latency_sum 600"));
        assert!(output.contains("latency_count 3"));
    }

    #[test]
    fn test_prometheus_dynamic_counter() {
        let counter = DynamicCounter::new(4);
        counter.add(&[("endpoint", "ep1"), ("method", "GET")], 3);

        let mut output = String::new();
        counter.export_prometheus(&mut output, "requests", "Requests by endpoint");

        assert!(output.contains("# HELP requests Requests by endpoint"));
        assert!(output.contains("# TYPE requests counter"));
        assert!(output.contains("requests{endpoint=\"ep1\",method=\"GET\"} 3"));
    }

    #[test]
    fn test_prometheus_dynamic_histogram() {
        let h = DynamicHistogram::new(&[100], 4);
        h.record(&[("endpoint", "ep1")], 50);
        h.record(&[("endpoint", "ep1")], 150);

        let mut output = String::new();
        h.export_prometheus(&mut output, "latency", "Latency by endpoint");

        assert!(output.contains("# TYPE latency histogram"));
        assert!(output.contains("latency_bucket{endpoint=\"ep1\",le=\"100\"} 1"));
        assert!(output.contains("latency_bucket{endpoint=\"ep1\",le=\"+Inf\"} 2"));
        assert!(output.contains("latency_sum{endpoint=\"ep1\"} 200"));
        assert!(output.contains("latency_count{endpoint=\"ep1\"} 2"));
    }
}
