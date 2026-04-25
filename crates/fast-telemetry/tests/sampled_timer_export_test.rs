use std::time::Duration;

use fast_telemetry::{DeriveLabel, ExportMetrics, LabeledSampledTimer, SampledTimer, Temporality};

#[derive(Copy, Clone, Debug, DeriveLabel)]
#[label_name = "phase"]
enum Phase {
    Parse,
    Execute,
}

#[derive(ExportMetrics)]
#[metric_prefix = "test"]
#[cfg_attr(feature = "otlp", otlp)]
struct SampledMetrics {
    #[help = "Request latency"]
    latency: SampledTimer,

    #[help = "Phase latency"]
    phase_latency: LabeledSampledTimer<Phase>,
}

impl SampledMetrics {
    fn new() -> Self {
        Self {
            latency: SampledTimer::with_latency_buckets(4, 1),
            phase_latency: LabeledSampledTimer::with_latency_buckets(4, 1),
        }
    }
}

#[test]
fn test_sampled_timer_prometheus_export() {
    let metrics = SampledMetrics::new();
    metrics.latency.record_elapsed(Duration::from_nanos(50_000));

    let mut output = String::new();
    metrics.export_prometheus(&mut output);

    assert!(output.contains("# HELP test_latency_calls Request latency total calls"));
    assert!(output.contains("test_latency_calls 1"));
    assert!(output.contains("# TYPE test_latency_samples histogram"));
    assert!(output.contains("test_latency_samples_count 1"));
}

#[test]
fn test_labeled_sampled_timer_prometheus_export() {
    let metrics = SampledMetrics::new();
    metrics
        .phase_latency
        .record_elapsed(Phase::Parse, Duration::from_nanos(100_000));

    let mut output = String::new();
    metrics.export_prometheus(&mut output);

    assert!(output.contains("test_phase_latency_calls{phase=\"parse\"} 1"));
    assert!(output.contains("test_phase_latency_samples_count{phase=\"parse\"} 1"));
    assert!(output.contains("test_phase_latency_calls{phase=\"execute\"} 0"));
}

#[test]
fn test_sampled_timer_dogstatsd_export() {
    let metrics = SampledMetrics::new();
    metrics.latency.record_elapsed(Duration::from_nanos(50_000));

    let mut output = String::new();
    metrics.export_dogstatsd(&mut output, &[]);

    assert!(output.contains("test.latency.calls:1|c\n"));
    assert!(output.contains("test.latency.samples.count:1|c\n"));
    assert!(output.contains("test.latency.samples.sum:50000|c\n"));
}

#[test]
fn test_sampled_timer_dogstatsd_delta_export() {
    let metrics = SampledMetrics::new();
    let mut state = SampledMetricsDogStatsDState::new();

    metrics.latency.record_elapsed(Duration::from_nanos(50_000));

    let mut output = String::new();
    metrics.export_dogstatsd_with_temporality(&mut output, &[], Temporality::Delta, &mut state);
    assert!(output.contains("test.latency.calls:1|c\n"));
    assert!(output.contains("test.latency.samples.count:1|c\n"));
    assert!(output.contains("test.latency.samples.sum:50000|c\n"));

    metrics.latency.record_elapsed(Duration::from_nanos(70_000));

    let mut output = String::new();
    metrics.export_dogstatsd_with_temporality(&mut output, &[], Temporality::Delta, &mut state);
    assert!(output.contains("test.latency.calls:1|c\n"));
    assert!(output.contains("test.latency.samples.count:1|c\n"));
    assert!(output.contains("test.latency.samples.sum:70000|c\n"));
}

#[cfg(feature = "otlp")]
#[test]
fn test_sampled_timer_otlp_export() {
    let metrics = SampledMetrics::new();
    metrics.latency.record_elapsed(Duration::from_nanos(50_000));

    let mut exported = Vec::new();
    metrics.export_otlp(&mut exported, 123);

    assert_eq!(exported.len(), 4);
    assert_eq!(exported[0].name, "test_latency.calls");
    assert_eq!(exported[1].name, "test_latency.samples");
    assert_eq!(exported[2].name, "test_phase_latency.calls");
    assert_eq!(exported[3].name, "test_phase_latency.samples");
}
