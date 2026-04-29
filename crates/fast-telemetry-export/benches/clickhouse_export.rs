//! ClickHouse export cost benchmarks.
//!
//! This separates format cost from network cost:
//! - DogStatsD text is the Datadog-compatible local serialization path.
//! - OTLP build/build+encode is the OpenTelemetry protobuf path.
//! - ClickHouse `otlp_then_rows` is today's built-in exporter shape.
//! - ClickHouse `first_party_direct_rows` uses `fast-telemetry/clickhouse` to
//!   build rows directly from primitives, skipping `pb::Metric`.

use criterion::{Criterion, criterion_group, criterion_main};
use fast_telemetry::otlp::{OtlpExport, build_export_request, build_resource, now_nanos};
use fast_telemetry::{
    ClickHouseExport, ClickHouseMetricBatch, Counter, DogStatsDExport, DynamicCounter, Gauge,
    Histogram,
};
use prost::Message;
use std::hint::black_box;

struct Fixture {
    counters: Vec<(String, Counter)>,
    gauges: Vec<(String, Gauge)>,
    histograms: Vec<(String, Histogram)>,
    dynamic_counters: Vec<(String, DynamicCounter)>,
}

fn fixture() -> Fixture {
    let counters = (0..5)
        .map(|i| {
            let c = Counter::new(4);
            c.add(1000);
            (format!("counter_{i}"), c)
        })
        .collect();

    let gauges = (0..5)
        .map(|i| {
            let g = Gauge::new();
            g.set(i * 100);
            (format!("gauge_{i}"), g)
        })
        .collect();

    let histograms = (0..3)
        .map(|i| {
            let h = Histogram::with_latency_buckets(4);
            for v in 0..500u64 {
                h.record(v * 10);
            }
            (format!("histogram_{i}"), h)
        })
        .collect();

    let dynamic_counters = (0..2)
        .map(|i| {
            let dc = DynamicCounter::new(4);
            for endpoint in 0..20 {
                dc.add(&[("endpoint", &format!("ep{endpoint}"))], 100);
            }
            (format!("dynamic_counter_{i}"), dc)
        })
        .collect();

    Fixture {
        counters,
        gauges,
        histograms,
        dynamic_counters,
    }
}

fn build_otlp_metrics(fixture: &Fixture, ts: u64) -> Vec<fast_telemetry::otlp::pb::Metric> {
    let mut metrics = Vec::with_capacity(55);
    for (name, counter) in &fixture.counters {
        counter.export_otlp(&mut metrics, name, "", ts);
    }
    for (name, gauge) in &fixture.gauges {
        gauge.export_otlp(&mut metrics, name, "", ts);
    }
    for (name, histogram) in &fixture.histograms {
        histogram.export_otlp(&mut metrics, name, "", ts);
    }
    for (name, dynamic_counter) in &fixture.dynamic_counters {
        dynamic_counter.export_otlp(&mut metrics, name, "", ts);
    }
    metrics
}

fn export_dogstatsd(fixture: &Fixture) -> usize {
    let mut output = String::with_capacity(16 * 1024);
    for (name, counter) in &fixture.counters {
        counter.export_dogstatsd(&mut output, name, &[("env", "prod")]);
    }
    for (name, gauge) in &fixture.gauges {
        gauge.export_dogstatsd(&mut output, name, &[("env", "prod")]);
    }
    for (name, histogram) in &fixture.histograms {
        histogram.export_dogstatsd(&mut output, name, &[("env", "prod")]);
    }
    for (name, dynamic_counter) in &fixture.dynamic_counters {
        dynamic_counter.export_dogstatsd(&mut output, name, &[("env", "prod")]);
    }
    output.len()
}

fn direct_clickhouse_rows(fixture: &Fixture, ts: u64) -> usize {
    let mut batch = ClickHouseMetricBatch::new("bench").with_resource_attribute("env", "prod");
    for (name, counter) in &fixture.counters {
        counter.export_clickhouse(&mut batch, name, "", ts);
    }
    for (name, gauge) in &fixture.gauges {
        gauge.export_clickhouse(&mut batch, name, "", ts);
    }
    for (name, histogram) in &fixture.histograms {
        histogram.export_clickhouse(&mut batch, name, "", ts);
    }
    for (name, dynamic_counter) in &fixture.dynamic_counters {
        dynamic_counter.export_clickhouse(&mut batch, name, "", ts);
    }
    let rows = batch.total_rows();
    black_box(batch);
    rows
}

fn bench_clickhouse_export(c: &mut Criterion) {
    let fixture = fixture();
    let resource = build_resource("bench", &[("env", "prod")]);
    let ts = now_nanos();

    let mut group = c.benchmark_group("export/full_clickhouse_cycle");

    group.bench_function("datadog/dogstatsd_text", |b| {
        b.iter(|| black_box(export_dogstatsd(&fixture)));
    });

    group.bench_function("otel/build", |b| {
        b.iter(|| {
            let metrics = build_otlp_metrics(&fixture, ts);
            black_box(metrics.len());
        });
    });

    group.bench_function("otel/build+protobuf_encode", |b| {
        b.iter(|| {
            let metrics = build_otlp_metrics(&fixture, ts);
            let request = build_export_request(&resource, "fast-telemetry", metrics);
            let bytes = request.encode_to_vec();
            black_box(bytes.len());
        });
    });

    group.bench_function("clickhouse/otlp_then_rows", |b| {
        b.iter(|| {
            let metrics = build_otlp_metrics(&fixture, ts);
            let rows =
                fast_telemetry_export::clickhouse::otel_standard::benchmark_translate_row_count(
                    &metrics,
                );
            black_box(rows);
        });
    });

    group.bench_function("clickhouse/first_party_direct_rows", |b| {
        b.iter(|| black_box(direct_clickhouse_rows(&fixture, ts)));
    });

    group.finish();
}

criterion_group!(clickhouse_export, bench_clickhouse_export);
criterion_main!(clickhouse_export);
