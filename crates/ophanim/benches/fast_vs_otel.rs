//! Criterion benchmarks: ophanim vs OpenTelemetry SDK.
//!
//! Covers:
//! - Single-thread recording hot path (counter, gauge, histogram)
//! - Labeled/dynamic recording with varying cardinality
//! - Export cost: OTLP protobuf vs DogStatsD vs Prometheus text
//! - OTLP protobuf encoding overhead
//! - Multi-thread contention (counter, histogram)
//!
//! Run:
//!   cargo bench -p ophanim --features otlp --bench fast_vs_otel

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use opentelemetry::metrics::MeterProvider;
use opentelemetry::{KeyValue, metrics as otel_metrics};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};
use ophanim::{
    Counter, Distribution, DogStatsDExport, DynamicCounter, DynamicDistribution, DynamicHistogram,
    Gauge, GaugeF64, Histogram, LabelEnum, LabeledCounter, LabeledHistogram, PrometheusExport,
};
use std::hint::black_box;
use std::sync::Arc;

// ============================================================================
// Helpers
// ============================================================================

fn otel_provider() -> (SdkMeterProvider, InMemoryMetricExporter) {
    let exporter = InMemoryMetricExporter::default();
    let provider = SdkMeterProvider::builder()
        .with_periodic_exporter(exporter.clone())
        .build();
    (provider, exporter)
}

#[derive(Copy, Clone, Debug)]
struct BenchLabel(usize);

impl LabelEnum for BenchLabel {
    const CARDINALITY: usize = 256;
    const LABEL_NAME: &'static str = "label";
    fn as_index(self) -> usize {
        self.0
    }
    fn from_index(index: usize) -> Self {
        Self(index)
    }
    fn variant_name(self) -> &'static str {
        // Static strings for each variant — just reuse a few
        match self.0 % 8 {
            0 => "v0",
            1 => "v1",
            2 => "v2",
            3 => "v3",
            4 => "v4",
            5 => "v5",
            6 => "v6",
            _ => "v7",
        }
    }
}

// ============================================================================
// 1. Recording hot path — single-threaded, no labels
// ============================================================================

fn bench_record_counter(c: &mut Criterion) {
    let mut group = c.benchmark_group("record/counter");

    // ophanim
    let ft_counter = Counter::new(4);
    group.bench_function("ophanim", |b| {
        b.iter(|| ft_counter.inc());
    });

    // OTel SDK
    let (provider, _exporter) = otel_provider();
    let meter = provider.meter("bench");
    let otel_counter: otel_metrics::Counter<u64> = meter.u64_counter("bench_counter").build();
    group.bench_function("otel-sdk", |b| {
        b.iter(|| otel_counter.add(black_box(1), &[]));
    });

    group.finish();
}

fn bench_record_gauge(c: &mut Criterion) {
    let mut group = c.benchmark_group("record/gauge");

    let ft_gauge = Gauge::new();
    group.bench_function("ophanim/i64", |b| {
        b.iter(|| ft_gauge.set(black_box(42)));
    });

    let ft_gauge_f64 = GaugeF64::new();
    group.bench_function("ophanim/f64", |b| {
        b.iter(|| ft_gauge_f64.set(black_box(3.125)));
    });

    let (provider, _exporter) = otel_provider();
    let meter = provider.meter("bench");
    let otel_gauge: otel_metrics::Gauge<i64> = meter.i64_gauge("bench_gauge").build();
    group.bench_function("otel-sdk/i64", |b| {
        b.iter(|| otel_gauge.record(black_box(42), &[]));
    });

    let otel_gauge_f64: otel_metrics::Gauge<f64> = meter.f64_gauge("bench_gauge_f64").build();
    group.bench_function("otel-sdk/f64", |b| {
        b.iter(|| otel_gauge_f64.record(black_box(3.125), &[]));
    });

    group.finish();
}

fn bench_record_histogram(c: &mut Criterion) {
    let mut group = c.benchmark_group("record/histogram");

    let ft_hist = Histogram::with_latency_buckets(4);
    group.bench_function("ophanim", |b| {
        b.iter(|| ft_hist.record(black_box(1234)));
    });

    let (provider, _exporter) = otel_provider();
    let meter = provider.meter("bench");
    let otel_hist: otel_metrics::Histogram<u64> = meter.u64_histogram("bench_hist").build();
    group.bench_function("otel-sdk", |b| {
        b.iter(|| otel_hist.record(black_box(1234), &[]));
    });

    group.finish();
}

fn bench_record_distribution(c: &mut Criterion) {
    let mut group = c.benchmark_group("record/distribution");

    let ft_dist = Distribution::new(4);
    group.bench_function("ophanim", |b| {
        b.iter(|| ft_dist.record(black_box(1234)));
    });

    group.finish();
}

// ============================================================================
// 2. Labeled recording — enum labels vs OTel KeyValue
// ============================================================================

fn bench_record_labeled_counter(c: &mut Criterion) {
    let mut group = c.benchmark_group("record/labeled_counter");

    // ophanim: LabeledCounter with enum
    let ft_lc = LabeledCounter::<BenchLabel>::new(4);
    group.bench_function("ophanim", |b| {
        let mut i = 0usize;
        b.iter(|| {
            ft_lc.inc(BenchLabel(i % 16));
            i += 1;
        });
    });

    // OTel: counter with 1 KeyValue
    let (provider, _exporter) = otel_provider();
    let meter = provider.meter("bench");
    let otel_counter = meter.u64_counter("bench_labeled").build();
    let labels: Vec<KeyValue> = (0..16)
        .map(|i| KeyValue::new("label", format!("v{i}")))
        .collect();
    group.bench_function("otel-sdk", |b| {
        let mut i = 0usize;
        b.iter(|| {
            otel_counter.add(1, &[labels[i % 16].clone()]);
            i += 1;
        });
    });

    group.finish();
}

fn bench_record_labeled_histogram(c: &mut Criterion) {
    let mut group = c.benchmark_group("record/labeled_histogram");

    let ft_lh = LabeledHistogram::<BenchLabel>::with_latency_buckets(4);
    group.bench_function("ophanim", |b| {
        let mut i = 0usize;
        b.iter(|| {
            ft_lh.record(BenchLabel(i % 16), black_box(1234));
            i += 1;
        });
    });

    let (provider, _exporter) = otel_provider();
    let meter = provider.meter("bench");
    let otel_hist = meter.u64_histogram("bench_labeled_hist").build();
    let labels: Vec<KeyValue> = (0..16)
        .map(|i| KeyValue::new("label", format!("v{i}")))
        .collect();
    group.bench_function("otel-sdk", |b| {
        let mut i = 0usize;
        b.iter(|| {
            otel_hist.record(black_box(1234), &[labels[i % 16].clone()]);
            i += 1;
        });
    });

    group.finish();
}

// ============================================================================
// 3. Dynamic labels — series handle vs OTel KeyValue slice
// ============================================================================

fn bench_record_dynamic_counter(c: &mut Criterion) {
    let mut group = c.benchmark_group("record/dynamic_counter");

    // ophanim: with series handle (cache-friendly hot path)
    let ft_dc = DynamicCounter::new(4);
    let handles: Vec<_> = (0..16)
        .map(|i| ft_dc.series(&[("endpoint", &format!("ep{i}"))]))
        .collect();
    group.bench_function("ophanim/series_handle", |b| {
        let mut i = 0usize;
        b.iter(|| {
            handles[i % 16].inc();
            i += 1;
        });
    });

    // ophanim: without series handle (label lookup each time)
    let ft_dc2 = DynamicCounter::new(4);
    // Pre-populate so lookup doesn't create new entries
    for i in 0..16 {
        ft_dc2.inc(&[("endpoint", &format!("ep{i}"))]);
    }
    let label_strs: Vec<String> = (0..16).map(|i| format!("ep{i}")).collect();
    group.bench_function("ophanim/label_lookup", |b| {
        let mut i = 0usize;
        b.iter(|| {
            ft_dc2.inc(&[("endpoint", label_strs[i % 16].as_str())]);
            i += 1;
        });
    });

    // OTel
    let (provider, _exporter) = otel_provider();
    let meter = provider.meter("bench");
    let otel_counter = meter.u64_counter("bench_dynamic").build();
    let kv_sets: Vec<Vec<KeyValue>> = (0..16)
        .map(|i| vec![KeyValue::new("endpoint", format!("ep{i}"))])
        .collect();
    group.bench_function("otel-sdk", |b| {
        let mut i = 0usize;
        b.iter(|| {
            otel_counter.add(1, &kv_sets[i % 16]);
            i += 1;
        });
    });

    group.finish();
}

fn bench_record_dynamic_distribution(c: &mut Criterion) {
    let mut group = c.benchmark_group("record/dynamic_distribution");

    // ophanim: with series handle
    let ft_dd = DynamicDistribution::new(4);
    let handles: Vec<_> = (0..16)
        .map(|i| ft_dd.series(&[("endpoint", &format!("ep{i}"))]))
        .collect();
    group.bench_function("ophanim/series_handle", |b| {
        let mut i = 0usize;
        b.iter(|| {
            handles[i % 16].record(black_box(1234));
            i += 1;
        });
    });

    // ophanim: label lookup each time
    let ft_dd2 = DynamicDistribution::new(4);
    for i in 0..16 {
        ft_dd2.record(&[("endpoint", &format!("ep{i}"))], 1);
    }
    let label_strs: Vec<String> = (0..16).map(|i| format!("ep{i}")).collect();
    group.bench_function("ophanim/label_lookup", |b| {
        let mut i = 0usize;
        b.iter(|| {
            ft_dd2.record(
                &[("endpoint", label_strs[i % 16].as_str())],
                black_box(1234),
            );
            i += 1;
        });
    });

    group.finish();
}

// ============================================================================
// 4. Multi-thread contention — counter and histogram
// ============================================================================

fn bench_contention_counter(c: &mut Criterion) {
    let mut group = c.benchmark_group("contention/counter");
    group.sample_size(20);

    for threads in [2, 4, 8] {
        // ophanim
        let ft_counter = Arc::new(Counter::new(threads));
        group.bench_with_input(BenchmarkId::new("ophanim", threads), &threads, |b, &t| {
            b.iter(|| {
                let barrier = Arc::new(std::sync::Barrier::new(t));
                let handles: Vec<_> = (0..t)
                    .map(|_| {
                        let counter = Arc::clone(&ft_counter);
                        let barrier = Arc::clone(&barrier);
                        std::thread::spawn(move || {
                            barrier.wait();
                            for _ in 0..10_000 {
                                counter.inc();
                            }
                        })
                    })
                    .collect();
                for h in handles {
                    h.join().expect("thread panicked");
                }
            });
        });

        // OTel
        let (provider, _exporter) = otel_provider();
        let meter = provider.meter("bench");
        let otel_counter = Arc::new(meter.u64_counter("contention_counter").build());
        group.bench_with_input(BenchmarkId::new("otel-sdk", threads), &threads, |b, &t| {
            b.iter(|| {
                let barrier = Arc::new(std::sync::Barrier::new(t));
                let handles: Vec<_> = (0..t)
                    .map(|_| {
                        let counter = Arc::clone(&otel_counter);
                        let barrier = Arc::clone(&barrier);
                        std::thread::spawn(move || {
                            barrier.wait();
                            for _ in 0..10_000 {
                                counter.add(1, &[]);
                            }
                        })
                    })
                    .collect();
                for h in handles {
                    h.join().expect("thread panicked");
                }
            });
        });
    }

    group.finish();
}

fn bench_contention_histogram(c: &mut Criterion) {
    let mut group = c.benchmark_group("contention/histogram");
    group.sample_size(20);

    for threads in [2, 4, 8] {
        let ft_hist = Arc::new(Histogram::with_latency_buckets(threads));
        group.bench_with_input(BenchmarkId::new("ophanim", threads), &threads, |b, &t| {
            b.iter(|| {
                let barrier = Arc::new(std::sync::Barrier::new(t));
                let handles: Vec<_> = (0..t)
                    .map(|_| {
                        let hist = Arc::clone(&ft_hist);
                        let barrier = Arc::clone(&barrier);
                        std::thread::spawn(move || {
                            barrier.wait();
                            for i in 0..10_000u64 {
                                hist.record(i % 5000);
                            }
                        })
                    })
                    .collect();
                for h in handles {
                    h.join().expect("thread panicked");
                }
            });
        });

        let (provider, _exporter) = otel_provider();
        let meter = provider.meter("bench");
        let otel_hist = Arc::new(meter.u64_histogram("contention_hist").build());
        group.bench_with_input(BenchmarkId::new("otel-sdk", threads), &threads, |b, &t| {
            b.iter(|| {
                let barrier = Arc::new(std::sync::Barrier::new(t));
                let handles: Vec<_> = (0..t)
                    .map(|_| {
                        let hist = Arc::clone(&otel_hist);
                        let barrier = Arc::clone(&barrier);
                        std::thread::spawn(move || {
                            barrier.wait();
                            for i in 0..10_000u64 {
                                hist.record(i % 5000, &[]);
                            }
                        })
                    })
                    .collect();
                for h in handles {
                    h.join().expect("thread panicked");
                }
            });
        });
    }

    group.finish();
}

fn bench_contention_distribution(c: &mut Criterion) {
    let mut group = c.benchmark_group("contention/distribution");
    group.sample_size(20);

    for threads in [2, 4, 8] {
        let ft_dist = Arc::new(Distribution::new(threads));
        group.bench_with_input(BenchmarkId::new("ophanim", threads), &threads, |b, &t| {
            b.iter(|| {
                let barrier = Arc::new(std::sync::Barrier::new(t));
                let handles: Vec<_> = (0..t)
                    .map(|_| {
                        let dist = Arc::clone(&ft_dist);
                        let barrier = Arc::clone(&barrier);
                        std::thread::spawn(move || {
                            barrier.wait();
                            for i in 0..10_000u64 {
                                dist.record(i % 5000);
                            }
                        })
                    })
                    .collect();
                for h in handles {
                    h.join().expect("thread panicked");
                }
            });
        });
    }

    group.finish();
}

// ============================================================================
// 5. Export cost — DogStatsD vs Prometheus vs OTLP
// ============================================================================

#[cfg(feature = "otlp")]
fn bench_export_formats(c: &mut Criterion) {
    use ophanim::otlp::{OtlpExport, build_export_request, build_resource, now_nanos};
    use prost::Message;

    let mut group = c.benchmark_group("export/counter");

    let counter = Counter::new(4);
    counter.add(123_456);

    // DogStatsD
    group.bench_function("dogstatsd", |b| {
        let mut output = String::with_capacity(256);
        b.iter(|| {
            output.clear();
            counter.export_dogstatsd(&mut output, "request_count", &[("env", "prod")]);
            black_box(&output);
        });
    });

    // Prometheus
    group.bench_function("prometheus", |b| {
        let mut output = String::with_capacity(256);
        b.iter(|| {
            output.clear();
            counter.export_prometheus(&mut output, "request_count", "Total requests");
            black_box(&output);
        });
    });

    // OTLP (struct building only)
    group.bench_function("otlp/build", |b| {
        let ts = now_nanos();
        b.iter(|| {
            let mut metrics = Vec::new();
            counter.export_otlp(&mut metrics, "request_count", "Total requests", ts);
            black_box(&metrics);
        });
    });

    // OTLP (build + protobuf encode)
    group.bench_function("otlp/build+encode", |b| {
        let resource = build_resource("eden", &[("env", "prod")]);
        let ts = now_nanos();
        b.iter(|| {
            let mut metrics = Vec::new();
            counter.export_otlp(&mut metrics, "request_count", "Total requests", ts);
            let request = build_export_request(&resource, "ophanim", metrics);
            let bytes = request.encode_to_vec();
            black_box(&bytes);
        });
    });

    group.finish();
}

#[cfg(feature = "otlp")]
fn bench_export_histogram_formats(c: &mut Criterion) {
    use ophanim::otlp::{OtlpExport, build_export_request, build_resource, now_nanos};
    use prost::Message;

    let mut group = c.benchmark_group("export/histogram");

    let hist = Histogram::with_latency_buckets(4);
    for i in 0..1000u64 {
        hist.record(i * 10);
    }

    group.bench_function("dogstatsd", |b| {
        let mut output = String::with_capacity(512);
        b.iter(|| {
            output.clear();
            hist.export_dogstatsd(&mut output, "latency", &[]);
            black_box(&output);
        });
    });

    group.bench_function("prometheus", |b| {
        let mut output = String::with_capacity(2048);
        b.iter(|| {
            output.clear();
            hist.export_prometheus(&mut output, "latency", "Request latency");
            black_box(&output);
        });
    });

    group.bench_function("otlp/build", |b| {
        let ts = now_nanos();
        b.iter(|| {
            let mut metrics = Vec::new();
            hist.export_otlp(&mut metrics, "latency", "Request latency", ts);
            black_box(&metrics);
        });
    });

    group.bench_function("otlp/build+encode", |b| {
        let resource = build_resource("eden", &[]);
        let ts = now_nanos();
        b.iter(|| {
            let mut metrics = Vec::new();
            hist.export_otlp(&mut metrics, "latency", "Request latency", ts);
            let request = build_export_request(&resource, "ophanim", metrics);
            let bytes = request.encode_to_vec();
            black_box(&bytes);
        });
    });

    group.finish();
}

#[cfg(feature = "otlp")]
fn bench_export_distribution_formats(c: &mut Criterion) {
    use ophanim::otlp::{OtlpExport, build_export_request, build_resource, now_nanos};
    use prost::Message;

    let mut group = c.benchmark_group("export/distribution");

    let dist = Distribution::new(4);
    for i in 0..1000u64 {
        dist.record(i * 10);
    }

    group.bench_function("dogstatsd", |b| {
        let mut output = String::with_capacity(512);
        b.iter(|| {
            output.clear();
            dist.export_dogstatsd(&mut output, "latency", &[]);
            black_box(&output);
        });
    });

    group.bench_function("prometheus", |b| {
        let mut output = String::with_capacity(2048);
        b.iter(|| {
            output.clear();
            dist.export_prometheus(&mut output, "latency", "Request latency");
            black_box(&output);
        });
    });

    group.bench_function("otlp/build", |b| {
        let ts = now_nanos();
        b.iter(|| {
            let mut metrics = Vec::new();
            dist.export_otlp(&mut metrics, "latency", "Request latency", ts);
            black_box(&metrics);
        });
    });

    group.bench_function("otlp/build+encode", |b| {
        let resource = build_resource("eden", &[]);
        let ts = now_nanos();
        b.iter(|| {
            let mut metrics = Vec::new();
            dist.export_otlp(&mut metrics, "latency", "Request latency", ts);
            let request = build_export_request(&resource, "ophanim", metrics);
            let bytes = request.encode_to_vec();
            black_box(&bytes);
        });
    });

    group.finish();
}

// ============================================================================
// 6. Label cardinality scaling — dynamic metrics export
// ============================================================================

#[cfg(feature = "otlp")]
fn bench_dynamic_cardinality(c: &mut Criterion) {
    use ophanim::otlp::{OtlpExport, now_nanos};

    let mut group = c.benchmark_group("export/dynamic_counter_cardinality");

    for cardinality in [10, 50, 200] {
        let dc = DynamicCounter::new(4);
        for i in 0..cardinality {
            dc.add(&[("endpoint", &format!("ep{i}"))], (i + 1) as isize);
        }

        group.bench_with_input(
            BenchmarkId::new("otlp", cardinality),
            &cardinality,
            |b, _| {
                let ts = now_nanos();
                b.iter(|| {
                    let mut metrics = Vec::new();
                    dc.export_otlp(&mut metrics, "requests", "Request count", ts);
                    black_box(&metrics);
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("dogstatsd", cardinality),
            &cardinality,
            |b, _| {
                let mut output = String::with_capacity(4096);
                b.iter(|| {
                    output.clear();
                    dc.export_dogstatsd(&mut output, "requests", &[]);
                    black_box(&output);
                });
            },
        );
    }

    group.finish();
}

#[cfg(feature = "otlp")]
fn bench_dynamic_histogram_cardinality(c: &mut Criterion) {
    use ophanim::otlp::{OtlpExport, now_nanos};

    let mut group = c.benchmark_group("export/dynamic_histogram_cardinality");

    for cardinality in [10, 50, 200] {
        let dh = DynamicHistogram::with_latency_buckets(4);
        for i in 0..cardinality {
            for v in 0..100u64 {
                dh.record(&[("endpoint", &format!("ep{i}"))], v * 10);
            }
        }

        group.bench_with_input(
            BenchmarkId::new("otlp", cardinality),
            &cardinality,
            |b, _| {
                let ts = now_nanos();
                b.iter(|| {
                    let mut metrics = Vec::new();
                    dh.export_otlp(&mut metrics, "latency", "Request latency", ts);
                    black_box(&metrics);
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("dogstatsd", cardinality),
            &cardinality,
            |b, _| {
                let mut output = String::with_capacity(16384);
                b.iter(|| {
                    output.clear();
                    dh.export_dogstatsd(&mut output, "latency", &[]);
                    black_box(&output);
                });
            },
        );
    }

    group.finish();
}

#[cfg(feature = "otlp")]
fn bench_dynamic_distribution_cardinality(c: &mut Criterion) {
    use ophanim::otlp::{OtlpExport, now_nanos};

    let mut group = c.benchmark_group("export/dynamic_distribution_cardinality");

    for cardinality in [10, 50, 200] {
        let dd = DynamicDistribution::new(4);
        for i in 0..cardinality {
            for v in 0..100u64 {
                dd.record(&[("endpoint", &format!("ep{i}"))], v * 10);
            }
        }

        group.bench_with_input(
            BenchmarkId::new("otlp", cardinality),
            &cardinality,
            |b, _| {
                let ts = now_nanos();
                b.iter(|| {
                    let mut metrics = Vec::new();
                    dd.export_otlp(&mut metrics, "latency", "Request latency", ts);
                    black_box(&metrics);
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("dogstatsd", cardinality),
            &cardinality,
            |b, _| {
                let mut output = String::with_capacity(16384);
                b.iter(|| {
                    output.clear();
                    dd.export_dogstatsd(&mut output, "latency", &[]);
                    black_box(&output);
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// 7. Full OTLP export cycle — simulate AllMetrics.export_otlp() + encode
// ============================================================================

#[cfg(feature = "otlp")]
fn bench_full_otlp_export(c: &mut Criterion) {
    use ophanim::otlp::{OtlpExport, build_export_request, build_resource, now_nanos};
    use prost::Message;

    let mut group = c.benchmark_group("export/full_otlp_cycle");

    // Simulate a realistic metrics set: 5 counters, 5 gauges, 3 histograms, 2 dynamic counters
    let counters: Vec<Counter> = (0..5)
        .map(|_| {
            let c = Counter::new(4);
            c.add(1000);
            c
        })
        .collect();

    let gauges: Vec<Gauge> = (0..5)
        .map(|i| {
            let g = Gauge::new();
            g.set(i * 100);
            g
        })
        .collect();

    let histograms: Vec<Histogram> = (0..3)
        .map(|_| {
            let h = Histogram::with_latency_buckets(4);
            for v in 0..500u64 {
                h.record(v * 10);
            }
            h
        })
        .collect();

    let dynamic_counters: Vec<DynamicCounter> = (0..2)
        .map(|_| {
            let dc = DynamicCounter::new(4);
            for i in 0..20 {
                dc.add(&[("endpoint", &format!("ep{i}"))], 100);
            }
            dc
        })
        .collect();

    let resource = build_resource("eden", &[("env", "prod"), ("version", "1.0")]);

    group.bench_function("build_all", |b| {
        let ts = now_nanos();
        b.iter(|| {
            let mut metrics = Vec::with_capacity(20);
            for (i, c) in counters.iter().enumerate() {
                c.export_otlp(&mut metrics, &format!("counter_{i}"), "", ts);
            }
            for (i, g) in gauges.iter().enumerate() {
                g.export_otlp(&mut metrics, &format!("gauge_{i}"), "", ts);
            }
            for (i, h) in histograms.iter().enumerate() {
                h.export_otlp(&mut metrics, &format!("histogram_{i}"), "", ts);
            }
            for (i, dc) in dynamic_counters.iter().enumerate() {
                dc.export_otlp(&mut metrics, &format!("dynamic_counter_{i}"), "", ts);
            }
            black_box(&metrics);
        });
    });

    group.bench_function("build_all+encode", |b| {
        let ts = now_nanos();
        b.iter(|| {
            let mut metrics = Vec::with_capacity(20);
            for (i, c) in counters.iter().enumerate() {
                c.export_otlp(&mut metrics, &format!("counter_{i}"), "", ts);
            }
            for (i, g) in gauges.iter().enumerate() {
                g.export_otlp(&mut metrics, &format!("gauge_{i}"), "", ts);
            }
            for (i, h) in histograms.iter().enumerate() {
                h.export_otlp(&mut metrics, &format!("histogram_{i}"), "", ts);
            }
            for (i, dc) in dynamic_counters.iter().enumerate() {
                dc.export_otlp(&mut metrics, &format!("dynamic_counter_{i}"), "", ts);
            }
            let request = build_export_request(&resource, "ophanim", metrics);
            let bytes = request.encode_to_vec();
            black_box(&bytes);
        });
    });

    group.finish();
}

// ============================================================================
// Criterion groups
// ============================================================================

#[cfg(feature = "otlp")]
criterion_group!(
    recording,
    bench_record_counter,
    bench_record_gauge,
    bench_record_histogram,
    bench_record_distribution,
    bench_record_labeled_counter,
    bench_record_labeled_histogram,
    bench_record_dynamic_counter,
    bench_record_dynamic_distribution,
);

#[cfg(feature = "otlp")]
criterion_group!(
    contention,
    bench_contention_counter,
    bench_contention_histogram,
    bench_contention_distribution,
);

#[cfg(feature = "otlp")]
criterion_group!(
    export,
    bench_export_formats,
    bench_export_histogram_formats,
    bench_export_distribution_formats,
    bench_dynamic_cardinality,
    bench_dynamic_histogram_cardinality,
    bench_dynamic_distribution_cardinality,
    bench_full_otlp_export,
);

#[cfg(feature = "otlp")]
criterion_main!(recording, contention, export);
