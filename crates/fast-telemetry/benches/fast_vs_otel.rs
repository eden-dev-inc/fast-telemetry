//! Criterion benchmarks: fast-telemetry vs OpenTelemetry SDK.
//!
//! Covers:
//! - Single-thread recording hot path (counter, gauge, histogram)
//! - Labeled/dynamic recording with varying cardinality
//! - First-touch dynamic series creation and overflow behavior
//! - Export cost: OTLP protobuf vs DogStatsD vs Prometheus text
//! - OTLP protobuf encoding overhead
//! - Multi-thread contention (counter, histogram)
//! - Concurrent write+export overlap under load
//! - Span OTLP drain/build/encode/gzip cost
//!
//! Run:
//!   cargo bench -p fast-telemetry --features otlp --bench fast_vs_otel

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use fast_telemetry::{
    Counter, Distribution, DogStatsDExport, DynamicCounter, DynamicDistribution, DynamicHistogram,
    Gauge, GaugeF64, Histogram, LabelEnum, LabeledCounter, LabeledHistogram, PrometheusExport,
    SpanAttribute, SpanCollector, SpanKind, SpanStatus,
};
use opentelemetry::metrics::MeterProvider;
use opentelemetry::{KeyValue, metrics as otel_metrics};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};
use std::hint::black_box;
use std::sync::{Arc, Barrier, mpsc};
use std::time::{Duration, Instant};

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

fn run_with_periodic_export<W, X, E>(
    threads: usize,
    iters: usize,
    export_interval: Duration,
    worker: W,
    exporter: X,
) -> (Duration, u64)
where
    W: Fn(usize, usize) + Send + Sync + 'static,
    X: Fn() -> E + Send + Sync + 'static,
    E: Send + 'static,
{
    let barrier = Arc::new(Barrier::new(threads + 2));
    let (stop_tx, stop_rx) = mpsc::channel::<()>();

    let exporter_barrier = Arc::clone(&barrier);
    let exporter = std::thread::spawn(move || {
        exporter_barrier.wait();
        let mut export_count = 0u64;
        loop {
            let _ = exporter();
            export_count += 1;
            if stop_rx.recv_timeout(export_interval).is_ok() {
                break;
            }
        }
        export_count
    });

    let worker = Arc::new(worker);
    let mut workers = Vec::with_capacity(threads);
    for thread_idx in 0..threads {
        let worker_fn = Arc::clone(&worker);
        let worker_barrier = Arc::clone(&barrier);
        workers.push(std::thread::spawn(move || {
            worker_barrier.wait();
            worker_fn(thread_idx, iters);
        }));
    }

    barrier.wait();
    let start = Instant::now();
    for worker_thread in workers {
        worker_thread.join().expect("worker thread panicked");
    }
    let _ = stop_tx.send(());
    let export_count = exporter.join().expect("exporter thread panicked");

    (start.elapsed(), export_count)
}

#[derive(Copy, Clone)]
enum SpanExportScenario {
    Root,
    Lifecycle,
    Pipeline,
}

impl SpanExportScenario {
    fn name(self) -> &'static str {
        match self {
            Self::Root => "root",
            Self::Lifecycle => "lifecycle",
            Self::Pipeline => "pipeline",
        }
    }

    fn spans_per_root(self) -> usize {
        match self {
            Self::Root => 1,
            Self::Lifecycle => 2,
            Self::Pipeline => 4,
        }
    }
}

fn build_span_export_collector(scenario: SpanExportScenario, roots: usize) -> Arc<SpanCollector> {
    let collector = Arc::new(SpanCollector::new(
        1,
        roots * scenario.spans_per_root() * 2 + 16,
    ));

    match scenario {
        SpanExportScenario::Root => {
            for _ in 0..roots {
                let mut root = collector.start_span("handle_request", SpanKind::Server);
                root.set_status(SpanStatus::Ok);
            }
        }
        SpanExportScenario::Lifecycle => {
            for _ in 0..roots {
                let mut root = collector.start_span("handle_request", SpanKind::Server);
                root.set_attribute("http.method", "GET");
                root.set_attribute("http.url", "/api/v1/users");
                root.add_event("auth_check", vec![SpanAttribute::new("result", "pass")]);
                {
                    let mut child = root.child("db_query", SpanKind::Client);
                    child.set_attribute("db.statement", "SELECT * FROM users");
                    child.set_status(SpanStatus::Ok);
                }
                root.set_status(SpanStatus::Ok);
            }
        }
        SpanExportScenario::Pipeline => {
            for _ in 0..roots {
                let mut root = collector.start_span("handle_request", SpanKind::Server);
                root.set_attribute("http.method", "POST");
                {
                    let mut validate = root.child("validate", SpanKind::Internal);
                    validate.set_attribute("valid", true);
                    validate.set_status(SpanStatus::Ok);
                }
                {
                    let mut db = root.child("db_write", SpanKind::Client);
                    db.set_attribute("db.statement", "INSERT INTO orders");
                    db.set_status(SpanStatus::Ok);
                }
                {
                    let mut notify = root.child("notify", SpanKind::Producer);
                    notify.set_attribute("topic", "order_events");
                    notify.set_status(SpanStatus::Ok);
                }
                root.set_status(SpanStatus::Ok);
            }
        }
    }

    collector.flush_local();
    collector
}

// ============================================================================
// 1. Recording hot path — single-threaded, no labels
// ============================================================================

fn bench_record_counter(c: &mut Criterion) {
    let mut group = c.benchmark_group("record/counter");

    // fast-telemetry
    let ft_counter = Counter::new(4);
    group.bench_function("fast-telemetry", |b| {
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
    group.bench_function("fast-telemetry/i64", |b| {
        b.iter(|| ft_gauge.set(black_box(42)));
    });

    let ft_gauge_f64 = GaugeF64::new();
    group.bench_function("fast-telemetry/f64", |b| {
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
    group.bench_function("fast-telemetry", |b| {
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
    group.bench_function("fast-telemetry", |b| {
        b.iter(|| ft_dist.record(black_box(1234)));
    });

    group.finish();
}

// ============================================================================
// 2. Labeled recording — enum labels vs OTel KeyValue
// ============================================================================

fn bench_record_labeled_counter(c: &mut Criterion) {
    let mut group = c.benchmark_group("record/labeled_counter");

    // fast-telemetry: LabeledCounter with enum
    let ft_lc = LabeledCounter::<BenchLabel>::new(4);
    group.bench_function("fast-telemetry", |b| {
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
    group.bench_function("fast-telemetry", |b| {
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

    // fast-telemetry: with series handle (cache-friendly hot path)
    let ft_dc = DynamicCounter::new(4);
    let handles: Vec<_> = (0..16)
        .map(|i| ft_dc.series(&[("endpoint", &format!("ep{i}"))]))
        .collect();
    group.bench_function("fast-telemetry/series_handle", |b| {
        let mut i = 0usize;
        b.iter(|| {
            handles[i % 16].inc();
            i += 1;
        });
    });

    // fast-telemetry: without series handle (label lookup each time)
    let ft_dc2 = DynamicCounter::new(4);
    // Pre-populate so lookup doesn't create new entries
    for i in 0..16 {
        ft_dc2.inc(&[("endpoint", &format!("ep{i}"))]);
    }
    let label_strs: Vec<String> = (0..16).map(|i| format!("ep{i}")).collect();
    group.bench_function("fast-telemetry/label_lookup", |b| {
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

    // fast-telemetry: with series handle
    let ft_dd = DynamicDistribution::new(4);
    let handles: Vec<_> = (0..16)
        .map(|i| ft_dd.series(&[("endpoint", &format!("ep{i}"))]))
        .collect();
    group.bench_function("fast-telemetry/series_handle", |b| {
        let mut i = 0usize;
        b.iter(|| {
            handles[i % 16].record(black_box(1234));
            i += 1;
        });
    });

    // fast-telemetry: label lookup each time
    let ft_dd2 = DynamicDistribution::new(4);
    for i in 0..16 {
        ft_dd2.record(&[("endpoint", &format!("ep{i}"))], 1);
    }
    let label_strs: Vec<String> = (0..16).map(|i| format!("ep{i}")).collect();
    group.bench_function("fast-telemetry/label_lookup", |b| {
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

fn bench_record_dynamic_counter_first_touch(c: &mut Criterion) {
    let mut group = c.benchmark_group("record/dynamic_counter_first_touch");
    group.sample_size(20);

    for cardinality in [16usize, 256, 1024] {
        group.throughput(Throughput::Elements(cardinality as u64));

        let labels: Vec<String> = (0..cardinality).map(|i| format!("ep{i}")).collect();
        let otel_attrs: Vec<Vec<KeyValue>> = (0..cardinality)
            .map(|i| vec![KeyValue::new("endpoint", format!("ep{i}"))])
            .collect();

        group.bench_with_input(
            BenchmarkId::new("fast-telemetry", cardinality),
            &cardinality,
            |b, &n| {
                b.iter_batched(
                    || DynamicCounter::with_max_series(8, n * 2),
                    |counter| {
                        for label in &labels {
                            counter.inc(&[("endpoint", label.as_str())]);
                        }
                        black_box(counter.cardinality());
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("fast-telemetry_overflow", cardinality),
            &cardinality,
            |b, &n| {
                let cap = usize::max(1, n / 4);
                b.iter_batched(
                    || DynamicCounter::with_max_series(8, cap),
                    |counter| {
                        for label in &labels {
                            counter.inc(&[("endpoint", label.as_str())]);
                        }
                        black_box((counter.cardinality(), counter.overflow_count()));
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("otel-sdk", cardinality),
            &cardinality,
            |b, _| {
                b.iter_batched(
                    || {
                        let (provider, _exporter) = otel_provider();
                        let meter = provider.meter("bench");
                        let counter = meter.u64_counter("bench_dynamic_first_touch").build();
                        (provider, counter)
                    },
                    |(_provider, counter)| {
                        for attrs in &otel_attrs {
                            counter.add(1, attrs);
                        }
                        black_box(());
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

// ============================================================================
// 4. Multi-thread contention — counter and histogram
// ============================================================================

fn bench_contention_counter(c: &mut Criterion) {
    let mut group = c.benchmark_group("contention/counter");
    group.sample_size(20);

    for threads in [2, 4, 8] {
        // fast-telemetry
        let ft_counter = Arc::new(Counter::new(threads));
        group.bench_with_input(
            BenchmarkId::new("fast-telemetry", threads),
            &threads,
            |b, &t| {
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
            },
        );

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
        group.bench_with_input(
            BenchmarkId::new("fast-telemetry", threads),
            &threads,
            |b, &t| {
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
            },
        );

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
        group.bench_with_input(
            BenchmarkId::new("fast-telemetry", threads),
            &threads,
            |b, &t| {
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
            },
        );
    }

    group.finish();
}

#[cfg(feature = "otlp")]
fn bench_counter_write_export_overlap(c: &mut Criterion) {
    use fast_telemetry::otlp::{OtlpExport, build_export_request, build_resource, now_nanos};
    use prost::Message;

    let mut group = c.benchmark_group("contention/counter_write_export_overlap");
    group.sample_size(10);

    let export_interval = Duration::from_millis(1);
    let ops_per_thread = 50_000usize;
    let resource = Arc::new(build_resource("bench", &[("env", "test")]));

    for threads in [4usize, 8] {
        group.throughput(Throughput::Elements((threads * ops_per_thread) as u64));

        group.bench_with_input(
            BenchmarkId::new("fast-telemetry_prometheus", threads),
            &threads,
            |b, &t| {
                b.iter_batched(
                    || Arc::new(Counter::new(t)),
                    |counter| {
                        let worker_counter = Arc::clone(&counter);
                        let exporter_counter = Arc::clone(&counter);
                        let (elapsed, export_count) = run_with_periodic_export(
                            t,
                            ops_per_thread,
                            export_interval,
                            move |_, n| {
                                for _ in 0..n {
                                    worker_counter.inc();
                                }
                            },
                            move || {
                                let mut output = String::with_capacity(256);
                                exporter_counter.export_prometheus(
                                    &mut output,
                                    "requests",
                                    "Total requests",
                                );
                                black_box(output.len())
                            },
                        );
                        black_box((elapsed, export_count, counter.sum()));
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("fast-telemetry_otlp_build+encode", threads),
            &threads,
            |b, &t| {
                let resource = Arc::clone(&resource);
                b.iter_batched(
                    || Arc::new(Counter::new(t)),
                    |counter| {
                        let worker_counter = Arc::clone(&counter);
                        let exporter_counter = Arc::clone(&counter);
                        let export_resource = Arc::clone(&resource);
                        let (elapsed, export_count) = run_with_periodic_export(
                            t,
                            ops_per_thread,
                            export_interval,
                            move |_, n| {
                                for _ in 0..n {
                                    worker_counter.inc();
                                }
                            },
                            move || {
                                let mut metrics = Vec::new();
                                exporter_counter.export_otlp(
                                    &mut metrics,
                                    "requests",
                                    "Total requests",
                                    now_nanos(),
                                );
                                let request = build_export_request(
                                    &export_resource,
                                    "fast-telemetry",
                                    metrics,
                                );
                                let bytes = request.encode_to_vec();
                                black_box(bytes.len())
                            },
                        );
                        black_box((elapsed, export_count, counter.sum()));
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("otel-sdk_flush", threads),
            &threads,
            |b, &t| {
                b.iter_batched(
                    || {
                        let exporter = InMemoryMetricExporter::default();
                        let provider = Arc::new(
                            SdkMeterProvider::builder()
                                .with_periodic_exporter(exporter.clone())
                                .build(),
                        );
                        let meter = provider.meter("bench");
                        let counter = Arc::new(meter.u64_counter("overlap_counter").build());
                        (provider, exporter, counter)
                    },
                    |(provider, exporter, counter)| {
                        let worker_counter = Arc::clone(&counter);
                        let exporter_provider = Arc::clone(&provider);
                        let exporter_exporter = exporter.clone();
                        let (elapsed, export_count) = run_with_periodic_export(
                            t,
                            ops_per_thread,
                            export_interval,
                            move |_, n| {
                                for _ in 0..n {
                                    worker_counter.add(1, &[]);
                                }
                            },
                            move || {
                                let _ = exporter_provider.force_flush();
                                let _ = exporter_exporter.get_finished_metrics();
                                exporter_exporter.reset();
                                0usize
                            },
                        );
                        black_box((elapsed, export_count));
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

// ============================================================================
// 5. Export cost — DogStatsD vs Prometheus vs OTLP
// ============================================================================

#[cfg(feature = "otlp")]
fn bench_export_formats(c: &mut Criterion) {
    use fast_telemetry::otlp::{OtlpExport, build_export_request, build_resource, now_nanos};
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
            let request = build_export_request(&resource, "fast-telemetry", metrics);
            let bytes = request.encode_to_vec();
            black_box(&bytes);
        });
    });

    group.finish();
}

#[cfg(feature = "otlp")]
fn bench_export_histogram_formats(c: &mut Criterion) {
    use fast_telemetry::otlp::{OtlpExport, build_export_request, build_resource, now_nanos};
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
            let request = build_export_request(&resource, "fast-telemetry", metrics);
            let bytes = request.encode_to_vec();
            black_box(&bytes);
        });
    });

    group.finish();
}

#[cfg(feature = "otlp")]
fn bench_export_distribution_formats(c: &mut Criterion) {
    use fast_telemetry::otlp::{OtlpExport, build_export_request, build_resource, now_nanos};
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
            let request = build_export_request(&resource, "fast-telemetry", metrics);
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
    use fast_telemetry::otlp::{OtlpExport, now_nanos};

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
    use fast_telemetry::otlp::{OtlpExport, now_nanos};

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
    use fast_telemetry::otlp::{OtlpExport, now_nanos};

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
    use fast_telemetry::otlp::{OtlpExport, build_export_request, build_resource, now_nanos};
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
            let request = build_export_request(&resource, "fast-telemetry", metrics);
            let bytes = request.encode_to_vec();
            black_box(&bytes);
        });
    });

    group.finish();
}

#[cfg(feature = "otlp")]
fn bench_span_otlp_export(c: &mut Criterion) {
    use fast_telemetry::otlp::{build_resource, build_trace_export_request};
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use prost::Message;
    use std::io::Write;

    let mut group = c.benchmark_group("export/span_otlp_cycle");
    let resource = build_resource("bench", &[("env", "test")]);
    let roots = 256usize;

    for scenario in [
        SpanExportScenario::Root,
        SpanExportScenario::Lifecycle,
        SpanExportScenario::Pipeline,
    ] {
        let span_count = roots * scenario.spans_per_root();
        group.throughput(Throughput::Elements(span_count as u64));

        group.bench_with_input(
            BenchmarkId::new("drain+build", scenario.name()),
            &scenario,
            |b, &scenario| {
                b.iter_batched_ref(
                    || build_span_export_collector(scenario, roots),
                    |collector| {
                        let mut completed = Vec::with_capacity(span_count);
                        collector.drain_into(&mut completed);
                        let otlp_spans: Vec<_> =
                            completed.iter().map(|span| span.to_otlp()).collect();
                        let request =
                            build_trace_export_request(&resource, "fast-telemetry", otlp_spans);
                        black_box(request);
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("drain+build+encode", scenario.name()),
            &scenario,
            |b, &scenario| {
                b.iter_batched_ref(
                    || build_span_export_collector(scenario, roots),
                    |collector| {
                        let mut completed = Vec::with_capacity(span_count);
                        collector.drain_into(&mut completed);
                        let otlp_spans: Vec<_> =
                            completed.iter().map(|span| span.to_otlp()).collect();
                        let request =
                            build_trace_export_request(&resource, "fast-telemetry", otlp_spans);
                        let bytes = request.encode_to_vec();
                        black_box(bytes.len());
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("drain+build+encode+gzip", scenario.name()),
            &scenario,
            |b, &scenario| {
                b.iter_batched_ref(
                    || build_span_export_collector(scenario, roots),
                    |collector| {
                        let mut completed = Vec::with_capacity(span_count);
                        collector.drain_into(&mut completed);
                        let otlp_spans: Vec<_> =
                            completed.iter().map(|span| span.to_otlp()).collect();
                        let request =
                            build_trace_export_request(&resource, "fast-telemetry", otlp_spans);
                        let bytes = request.encode_to_vec();
                        let mut compressed = Vec::new();
                        let mut encoder = GzEncoder::new(&mut compressed, Compression::fast());
                        encoder.write_all(&bytes).expect("gzip write");
                        encoder.finish().expect("gzip finish");
                        black_box(compressed.len());
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

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
    bench_record_dynamic_counter_first_touch,
);

#[cfg(feature = "otlp")]
criterion_group!(
    contention,
    bench_contention_counter,
    bench_contention_histogram,
    bench_contention_distribution,
    bench_counter_write_export_overlap,
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
    bench_span_otlp_export,
);

#[cfg(feature = "otlp")]
criterion_main!(recording, contention, export);
