//! Integration test for ExportMetrics derive macro with Distribution fields.

use fast_telemetry::{Distribution, ExportMetrics};

#[derive(ExportMetrics)]
#[metric_prefix = "test"]
struct DistributionMetrics {
    #[help = "Request latency samples"]
    latency: Distribution,
}

impl DistributionMetrics {
    fn new() -> Self {
        Self {
            latency: Distribution::new(4),
        }
    }
}

#[test]
fn test_distribution_derive_prometheus_export() {
    let metrics = DistributionMetrics::new();
    metrics.latency.record(100);
    metrics.latency.record(200);
    metrics.latency.record(300);

    let mut output = String::new();
    metrics.export_prometheus(&mut output);

    assert!(output.contains("# HELP test_latency Request latency samples"));
    assert!(output.contains("# TYPE test_latency summary"));
    assert!(output.contains("test_latency_sum 600"));
    assert!(output.contains("test_latency_count 3"));
}

#[test]
fn test_distribution_derive_dogstatsd_delta() {
    let metrics = DistributionMetrics::new();
    metrics.latency.record(7); // bucket 2: [4, 8) → midpoint 6
    metrics.latency.record(9); // bucket 3: [8, 16) → midpoint 12

    let mut output = String::new();
    let mut state = DistributionMetricsDogStatsDState::new();
    metrics.export_dogstatsd_delta(&mut output, &[], &mut state);

    // Exports as |d distribution samples (one per non-zero bucket)
    assert!(output.contains("test.latency:6|d\n"));
    assert!(output.contains("test.latency:12|d\n"));

    // Second export with no new recordings should produce nothing
    let mut output2 = String::new();
    metrics.export_dogstatsd_delta(&mut output2, &[], &mut state);
    assert!(output2.is_empty());

    // Record more and verify delta tracking
    metrics.latency.record(100); // bucket 6: [64, 128) → midpoint 96
    let mut output3 = String::new();
    metrics.export_dogstatsd_delta(&mut output3, &[], &mut state);
    assert!(output3.contains("test.latency:96|d\n"));
}
