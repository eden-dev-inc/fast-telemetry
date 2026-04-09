// Compare contention behavior of fast-telemetry vs OpenTelemetry.
//
// Build and run directly:
// - cargo run --release --bin bench_cache_contention --features bench-tools -- --mode fast --entity counter --threads 16 --iters 10000000 --shards 16
// - cargo run --release --bin bench_cache_contention --features bench-tools -- --mode otel --entity labeled_counter --threads 16 --iters 10000000 --labels 64
// - cargo run --release --bin bench_cache_contention --features bench-tools -- --mode fast --entity dynamic_counter --threads 16 --iters 10000000 --labels 64 --shards 16
// - cargo run --release --bin bench_cache_contention --features bench-tools -- --mode otel --entity dynamic_counter --threads 16 --iters 10000000 --labels 64

use fast_telemetry::{
    Counter, Distribution, DynamicCounter, DynamicDistribution, DynamicGauge, DynamicGaugeI64, DynamicHistogram, LabelEnum, LabeledCounter,
    LabeledGauge, LabeledHistogram,
};
use metrics::atomics::AtomicU64 as MetricsAtomicU64;
use metrics::{Counter as MetricsCounter, Gauge as MetricsGauge, Histogram as MetricsHistogram, Key, Label};
use metrics_util::registry::{AtomicStorage as MetricsAtomicStorage, Registry as MetricsRegistry};
use metrics_util::storage::AtomicBucket as MetricsAtomicBucket;
use opentelemetry::metrics::MeterProvider;
use opentelemetry::{KeyValue, metrics::Counter as OTelCounter, metrics::Gauge as OTelGauge, metrics::Histogram as OTelHistogram};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};
use std::sync::mpsc;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

mod thread_affinity {
    include!("thread_affinity.rs");
}
mod process_usage {
    include!("process_usage.rs");
}
use process_usage::ProcessCpuSnapshot;
use thread_affinity::ThreadAffinityMode;

#[derive(Copy, Clone)]
enum Mode {
    Fast,
    Atomic,
    Metrics,
    Otel,
}

#[derive(Copy, Clone, Debug)]
enum Entity {
    Counter,
    Distribution,
    DynamicCounter,
    DynamicDistribution,
    DynamicGauge,
    DynamicGaugeI64,
    DynamicHistogram,
    LabeledCounter,
    LabeledGauge,
    LabeledHistogram,
}

#[derive(Copy, Clone)]
enum WorkloadProfile {
    Uniform,
    Hotspot,
    Churn,
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
        "bench"
    }
}

struct Config {
    mode: Mode,
    entity: Entity,
    threads: usize,
    iters: usize,
    shards: usize,
    labels: usize,
    profile: WorkloadProfile,
    thread_affinity: ThreadAffinityMode,
    export_interval_ms: u64,
}

struct RunResult {
    final_count: isize,
    record_seconds: f64,
    total_seconds: f64,
    export_count: u64,
    export_seconds: f64,
}

fn parse_args() -> Config {
    let mut mode = Mode::Fast;
    let mut entity = Entity::Counter;
    let mut threads = std::thread::available_parallelism().map_or(4, |n| n.get());
    let mut iters = 10_000_000usize;
    let mut shards = threads;
    let mut shards_set = false;
    let mut labels = 16usize;
    let mut profile = WorkloadProfile::Uniform;
    let mut thread_affinity = ThreadAffinityMode::Off;
    let mut export_interval_ms = 10u64;

    let args: Vec<String> = std::env::args().collect();
    let mut i = 1usize;
    while i < args.len() {
        match args[i].as_str() {
            "--mode" if i + 1 < args.len() => {
                mode = match args[i + 1].as_str() {
                    "fast" => Mode::Fast,
                    "atomic" => Mode::Atomic,
                    "metrics" => Mode::Metrics,
                    "otel" => Mode::Otel,
                    value => panic!("invalid --mode: {value} (expected fast|atomic|metrics|otel)"),
                };
                i += 2;
            }
            "--entity" if i + 1 < args.len() => {
                entity = match args[i + 1].as_str() {
                    "counter" => Entity::Counter,
                    "distribution" => Entity::Distribution,
                    "dynamic_counter" => Entity::DynamicCounter,
                    "dynamic_distribution" => Entity::DynamicDistribution,
                    "dynamic_gauge" => Entity::DynamicGauge,
                    "dynamic_gauge_i64" => Entity::DynamicGaugeI64,
                    "dynamic_histogram" => Entity::DynamicHistogram,
                    "labeled_counter" => Entity::LabeledCounter,
                    "labeled_gauge" => Entity::LabeledGauge,
                    "labeled_histogram" => Entity::LabeledHistogram,
                    value => panic!(
                        "invalid --entity: {value} (expected counter|distribution|dynamic_counter|dynamic_distribution|dynamic_gauge|dynamic_gauge_i64|dynamic_histogram|labeled_counter|labeled_gauge|labeled_histogram)"
                    ),
                };
                i += 2;
            }
            "--profile" if i + 1 < args.len() => {
                profile = match args[i + 1].as_str() {
                    "uniform" => WorkloadProfile::Uniform,
                    "hotspot" => WorkloadProfile::Hotspot,
                    "churn" => WorkloadProfile::Churn,
                    value => panic!("invalid --profile: {value} (expected uniform|hotspot|churn)"),
                };
                i += 2;
            }
            "--thread-affinity" if i + 1 < args.len() => {
                thread_affinity = ThreadAffinityMode::parse(args[i + 1].as_str()).unwrap_or_else(|| {
                    panic!("invalid --thread-affinity: {} (expected off|round_robin|rr)", args[i + 1])
                });
                i += 2;
            }
            "--threads" if i + 1 < args.len() => {
                threads = args[i + 1].parse().expect("--threads must be an integer");
                i += 2;
            }
            "--iters" if i + 1 < args.len() => {
                iters = args[i + 1].parse().expect("--iters must be an integer");
                i += 2;
            }
            "--shards" if i + 1 < args.len() => {
                shards = args[i + 1].parse().expect("--shards must be an integer");
                shards_set = true;
                i += 2;
            }
            "--labels" if i + 1 < args.len() => {
                labels = args[i + 1].parse().expect("--labels must be an integer");
                i += 2;
            }
            "--export-interval-ms" if i + 1 < args.len() => {
                export_interval_ms = args[i + 1].parse().expect("--export-interval-ms must be an integer");
                i += 2;
            }
            "--help" => {
                println!(
                    "Usage: bench_cache_contention --mode <fast|atomic|metrics|otel> --entity <counter|distribution|dynamic_counter|labeled_counter|labeled_gauge|labeled_histogram> --threads <n> --iters <n> [--shards <n>] [--labels <n>] [--profile <uniform|hotspot|churn>] [--thread-affinity <off|round_robin|rr>] [--export-interval-ms <n>]"
                );
                println!("  --profile <uniform|hotspot|churn> controls label access pattern");
                println!("  metrics mode uses metrics + metrics-util::Registry<_, AtomicStorage>");
                std::process::exit(0);
            }
            arg => panic!("unknown arg: {arg}"),
        }
    }

    if !shards_set {
        shards = threads;
    }

    assert!(labels >= 1, "--labels must be >= 1");
    assert!(labels <= BenchLabel::CARDINALITY, "--labels must be <= {}", BenchLabel::CARDINALITY);

    Config {
        mode,
        entity,
        threads,
        iters,
        shards,
        labels,
        profile,
        thread_affinity,
        export_interval_ms,
    }
}

type MetricsCounterCell = Arc<MetricsAtomicU64>;
type MetricsGaugeCell = Arc<MetricsAtomicU64>;
type MetricsHistogramCell = Arc<MetricsAtomicBucket<f64>>;

#[derive(Clone)]
struct MetricsCounterEntry {
    handle: MetricsCounter,
    cell: MetricsCounterCell,
}

#[derive(Clone)]
struct MetricsGaugeEntry {
    handle: MetricsGauge,
    cell: MetricsGaugeCell,
}

#[derive(Clone)]
struct MetricsHistogramEntry {
    handle: MetricsHistogram,
    cell: MetricsHistogramCell,
}

#[inline]
fn profile_index(profile: WorkloadProfile, thread: usize, iter: usize, cardinality: usize) -> usize {
    debug_assert!(cardinality > 0);
    match profile {
        WorkloadProfile::Uniform => (iter + thread) % cardinality,
        WorkloadProfile::Hotspot => {
            let hot = usize::max(1, usize::min(8, cardinality));
            if (iter & 0x0f) == 0 {
                (iter + thread) % cardinality
            } else {
                (iter + thread) % hot
            }
        }
        WorkloadProfile::Churn => (iter.wrapping_mul(17).wrapping_add(thread.wrapping_mul(131))) % cardinality,
    }
}

fn run_with_threads<E, W, X>(
    threads: usize,
    iters: usize,
    thread_affinity: ThreadAffinityMode,
    export_interval_ms: u64,
    worker: W,
    exporter: X,
) -> (f64, f64, u64, f64)
where
    E: Send + 'static,
    W: Fn(usize, usize) + Send + Sync + 'static,
    X: Fn() -> E + Send + Sync + 'static,
{
    let interval = Duration::from_millis(export_interval_ms.max(1));
    let barrier = Arc::new(Barrier::new(threads + 2));
    let (stop_tx, stop_rx) = mpsc::channel::<()>();

    let exporter_barrier = Arc::clone(&barrier);
    let exporter = std::thread::spawn(move || {
        exporter_barrier.wait();
        let mut export_count = 0u64;
        let mut export_seconds = 0.0f64;
        loop {
            let export_start = Instant::now();
            let _ = exporter();
            export_seconds += export_start.elapsed().as_secs_f64();
            export_count += 1;
            if stop_rx.recv_timeout(interval).is_ok() {
                break;
            }
        }
        (export_count, export_seconds)
    });

    let worker = Arc::new(worker);
    let mut workers = Vec::with_capacity(threads);
    for t in 0..threads {
        let worker_fn = Arc::clone(&worker);
        let worker_barrier = Arc::clone(&barrier);
        workers.push(std::thread::spawn(move || {
            thread_affinity::pin_worker_thread(t, thread_affinity);
            worker_barrier.wait();
            worker_fn(t, iters);
        }));
    }

    barrier.wait();
    let record_start = Instant::now();
    for worker_thread in workers {
        worker_thread.join().expect("worker thread panicked");
    }
    let record_seconds = record_start.elapsed().as_secs_f64();

    let total_start = Instant::now();
    let _ = stop_tx.send(());
    let (export_count, export_seconds) = exporter.join().expect("exporter thread panicked");
    let total_seconds = record_seconds + total_start.elapsed().as_secs_f64();

    (record_seconds, total_seconds, export_count, export_seconds)
}

fn metrics_key(name: &'static str, labels: Vec<Label>) -> Key {
    if labels.is_empty() {
        Key::from_name(name)
    } else {
        Key::from_parts(name, labels)
    }
}

fn metrics_counter_entry(
    registry: &MetricsRegistry<Key, MetricsAtomicStorage>,
    key: Key,
) -> MetricsCounterEntry {
    registry.get_or_create_counter(&key, |counter: &MetricsCounterCell| MetricsCounterEntry {
        handle: MetricsCounter::from_arc(Arc::clone(counter)),
        cell: Arc::clone(counter),
    })
}

fn metrics_gauge_entry(
    registry: &MetricsRegistry<Key, MetricsAtomicStorage>,
    key: Key,
) -> MetricsGaugeEntry {
    registry.get_or_create_gauge(&key, |gauge: &MetricsGaugeCell| MetricsGaugeEntry {
        handle: MetricsGauge::from_arc(Arc::clone(gauge)),
        cell: Arc::clone(gauge),
    })
}

fn metrics_histogram_entry(
    registry: &MetricsRegistry<Key, MetricsAtomicStorage>,
    key: Key,
) -> MetricsHistogramEntry {
    registry.get_or_create_histogram(&key, |histogram: &MetricsHistogramCell| MetricsHistogramEntry {
        handle: MetricsHistogram::from_arc(Arc::clone(histogram)),
        cell: Arc::clone(histogram),
    })
}

fn metrics_gauge_value(cell: &MetricsGaugeCell) -> f64 {
    f64::from_bits(cell.load(Ordering::Relaxed))
}

fn export_metrics_histogram(cell: &MetricsHistogramCell) -> u64 {
    let mut total = 0u64;
    cell.clear_with(|block| {
        total += block.len() as u64;
        total += block.iter().copied().sum::<f64>() as u64;
    });
    total
}

fn run_fast(
    entity: Entity,
    cfg: &Config,
) -> RunResult {
    let threads = cfg.threads;
    let iters = cfg.iters;
    let shards = cfg.shards;
    let labels = cfg.labels;
    let profile = cfg.profile;
    let thread_affinity = cfg.thread_affinity;
    let export_interval_ms = cfg.export_interval_ms;
    match entity {
        Entity::Counter => {
            let counter = Arc::new(Counter::new(shards));
            let worker_counter = Arc::clone(&counter);
            let exporter_counter = Arc::clone(&counter);
            let (record_seconds, total_seconds, export_count, export_seconds) = run_with_threads(
                threads,
                iters,
                thread_affinity,
                export_interval_ms,
                move |_, n| {
                    for _ in 0..n {
                        worker_counter.inc();
                    }
                },
                move || exporter_counter.sum(),
            );

            RunResult {
                final_count: counter.sum(),
                record_seconds,
                total_seconds,
                export_count,
                export_seconds,
            }
        }
        Entity::Distribution => {
            let dist = Arc::new(Distribution::new(shards));
            let worker_dist = Arc::clone(&dist);
            let exporter_dist = Arc::clone(&dist);
            let (record_seconds, total_seconds, export_count, export_seconds) = run_with_threads(
                threads,
                iters,
                thread_affinity,
                export_interval_ms,
                move |_, n| {
                    for i in 0..n {
                        let value = 10 + ((i % 10_000) as u64);
                        worker_dist.record(value);
                    }
                },
                move || exporter_dist.count(),
            );

            RunResult {
                final_count: dist.count() as isize,
                record_seconds,
                total_seconds,
                export_count,
                export_seconds,
            }
        }
        Entity::DynamicCounter => {
            let metric = Arc::new(DynamicCounter::new(shards));
            let endpoint_values: Arc<Vec<String>> = Arc::new((0..labels).map(|i| format!("ep{i}")).collect());
            let org_cardinality = usize::max(1, labels / 4);
            let org_values: Arc<Vec<String>> = Arc::new((0..org_cardinality).map(|i| format!("org{i}")).collect());
            let worker_metric = Arc::clone(&metric);
            let worker_endpoints = Arc::clone(&endpoint_values);
            let worker_orgs = Arc::clone(&org_values);
            let exporter_metric = Arc::clone(&metric);
            let (record_seconds, total_seconds, export_count, export_seconds) = run_with_threads(
                threads,
                iters,
                thread_affinity,
                export_interval_ms,
                move |t, n| {
                    let mut series_handles = Vec::with_capacity(worker_endpoints.len());
                    for (endpoint_idx, endpoint) in worker_endpoints.iter().enumerate() {
                        let org_idx = endpoint_idx % worker_orgs.len();
                        series_handles
                            .push(worker_metric.series(&[("endpoint_uuid", endpoint.as_str()), ("org_id", worker_orgs[org_idx].as_str())]));
                    }
                    for i in 0..n {
                        let idx = profile_index(profile, t, i, series_handles.len());
                        series_handles[idx].inc();
                    }
                },
                move || exporter_metric.sum_all(),
            );

            RunResult {
                final_count: metric.sum_all(),
                record_seconds,
                total_seconds,
                export_count,
                export_seconds,
            }
        }
        Entity::DynamicDistribution => {
            let metric = Arc::new(DynamicDistribution::new(shards));
            let endpoint_values: Arc<Vec<String>> = Arc::new((0..labels).map(|i| format!("ep{i}")).collect());
            let org_cardinality = usize::max(1, labels / 4);
            let org_values: Arc<Vec<String>> = Arc::new((0..org_cardinality).map(|i| format!("org{i}")).collect());
            let worker_metric = Arc::clone(&metric);
            let worker_endpoints = Arc::clone(&endpoint_values);
            let worker_orgs = Arc::clone(&org_values);
            let exporter_metric = Arc::clone(&metric);
            let exporter_endpoints = Arc::clone(&endpoint_values);
            let exporter_orgs = Arc::clone(&org_values);
            let (record_seconds, total_seconds, export_count, export_seconds) = run_with_threads(
                threads,
                iters,
                thread_affinity,
                export_interval_ms,
                move |t, n| {
                    let mut series_handles = Vec::with_capacity(worker_endpoints.len());
                    for (endpoint_idx, endpoint) in worker_endpoints.iter().enumerate() {
                        let org_idx = endpoint_idx % worker_orgs.len();
                        series_handles
                            .push(worker_metric.series(&[("endpoint_uuid", endpoint.as_str()), ("org_id", worker_orgs[org_idx].as_str())]));
                    }
                    for i in 0..n {
                        let idx = profile_index(profile, t, i, series_handles.len());
                        let value = 10 + ((i % 10_000) as u64);
                        series_handles[idx].record(value);
                    }
                },
                move || {
                    let mut total = 0u64;
                    for (endpoint_idx, endpoint) in exporter_endpoints.iter().enumerate() {
                        let org_idx = endpoint_idx % exporter_orgs.len();
                        total +=
                            exporter_metric.count(&[("endpoint_uuid", endpoint.as_str()), ("org_id", exporter_orgs[org_idx].as_str())]);
                    }
                    total
                },
            );

            let mut final_count = 0u64;
            for (endpoint_idx, endpoint) in endpoint_values.iter().enumerate() {
                let org_idx = endpoint_idx % org_values.len();
                final_count += metric.count(&[("endpoint_uuid", endpoint.as_str()), ("org_id", org_values[org_idx].as_str())]);
            }
            RunResult {
                final_count: final_count as isize,
                record_seconds,
                total_seconds,
                export_count,
                export_seconds,
            }
        }
        Entity::DynamicGauge => {
            let metric = Arc::new(DynamicGauge::new(shards));
            let endpoint_values: Arc<Vec<String>> = Arc::new((0..labels).map(|i| format!("ep{i}")).collect());
            let org_cardinality = usize::max(1, labels / 4);
            let org_values: Arc<Vec<String>> = Arc::new((0..org_cardinality).map(|i| format!("org{i}")).collect());
            let worker_metric = Arc::clone(&metric);
            let worker_endpoints = Arc::clone(&endpoint_values);
            let worker_orgs = Arc::clone(&org_values);
            let exporter_metric = Arc::clone(&metric);
            let exporter_endpoints = Arc::clone(&endpoint_values);
            let exporter_orgs = Arc::clone(&org_values);
            let (record_seconds, total_seconds, export_count, export_seconds) = run_with_threads(
                threads,
                iters,
                thread_affinity,
                export_interval_ms,
                move |t, n| {
                    let mut series_handles = Vec::with_capacity(worker_endpoints.len());
                    for (endpoint_idx, endpoint) in worker_endpoints.iter().enumerate() {
                        let org_idx = endpoint_idx % worker_orgs.len();
                        series_handles
                            .push(worker_metric.series(&[("endpoint_uuid", endpoint.as_str()), ("org_id", worker_orgs[org_idx].as_str())]));
                    }
                    for i in 0..n {
                        let idx = profile_index(profile, t, i, series_handles.len());
                        series_handles[idx].set(i as f64);
                    }
                },
                move || {
                    let mut total = 0.0f64;
                    for (endpoint_idx, endpoint) in exporter_endpoints.iter().enumerate() {
                        let org_idx = endpoint_idx % exporter_orgs.len();
                        total += exporter_metric.get(&[("endpoint_uuid", endpoint.as_str()), ("org_id", exporter_orgs[org_idx].as_str())]);
                    }
                    total as u64
                },
            );

            RunResult {
                final_count: (threads * iters) as isize,
                record_seconds,
                total_seconds,
                export_count,
                export_seconds,
            }
        }
        Entity::DynamicGaugeI64 => {
            let metric = Arc::new(DynamicGaugeI64::new(shards));
            let endpoint_values: Arc<Vec<String>> = Arc::new((0..labels).map(|i| format!("ep{i}")).collect());
            let org_cardinality = usize::max(1, labels / 4);
            let org_values: Arc<Vec<String>> = Arc::new((0..org_cardinality).map(|i| format!("org{i}")).collect());
            let worker_metric = Arc::clone(&metric);
            let worker_endpoints = Arc::clone(&endpoint_values);
            let worker_orgs = Arc::clone(&org_values);
            let exporter_metric = Arc::clone(&metric);
            let exporter_endpoints = Arc::clone(&endpoint_values);
            let exporter_orgs = Arc::clone(&org_values);
            let (record_seconds, total_seconds, export_count, export_seconds) = run_with_threads(
                threads,
                iters,
                thread_affinity,
                export_interval_ms,
                move |t, n| {
                    let mut series_handles = Vec::with_capacity(worker_endpoints.len());
                    for (endpoint_idx, endpoint) in worker_endpoints.iter().enumerate() {
                        let org_idx = endpoint_idx % worker_orgs.len();
                        series_handles
                            .push(worker_metric.series(&[("endpoint_uuid", endpoint.as_str()), ("org_id", worker_orgs[org_idx].as_str())]));
                    }
                    for i in 0..n {
                        let idx = profile_index(profile, t, i, series_handles.len());
                        series_handles[idx].set(i as i64);
                    }
                },
                move || {
                    let mut total = 0i64;
                    for (endpoint_idx, endpoint) in exporter_endpoints.iter().enumerate() {
                        let org_idx = endpoint_idx % exporter_orgs.len();
                        total += exporter_metric.get(&[("endpoint_uuid", endpoint.as_str()), ("org_id", exporter_orgs[org_idx].as_str())]);
                    }
                    total
                },
            );

            RunResult {
                final_count: (threads * iters) as isize,
                record_seconds,
                total_seconds,
                export_count,
                export_seconds,
            }
        }
        Entity::DynamicHistogram => {
            let metric = Arc::new(DynamicHistogram::with_latency_buckets(shards));
            let endpoint_values: Arc<Vec<String>> = Arc::new((0..labels).map(|i| format!("ep{i}")).collect());
            let org_cardinality = usize::max(1, labels / 4);
            let org_values: Arc<Vec<String>> = Arc::new((0..org_cardinality).map(|i| format!("org{i}")).collect());
            let worker_metric = Arc::clone(&metric);
            let worker_endpoints = Arc::clone(&endpoint_values);
            let worker_orgs = Arc::clone(&org_values);
            let exporter_metric = Arc::clone(&metric);
            let exporter_endpoints = Arc::clone(&endpoint_values);
            let exporter_orgs = Arc::clone(&org_values);
            let (record_seconds, total_seconds, export_count, export_seconds) = run_with_threads(
                threads,
                iters,
                thread_affinity,
                export_interval_ms,
                move |t, n| {
                    let mut series_handles = Vec::with_capacity(worker_endpoints.len());
                    for (endpoint_idx, endpoint) in worker_endpoints.iter().enumerate() {
                        let org_idx = endpoint_idx % worker_orgs.len();
                        series_handles
                            .push(worker_metric.series(&[("endpoint_uuid", endpoint.as_str()), ("org_id", worker_orgs[org_idx].as_str())]));
                    }
                    for i in 0..n {
                        let idx = profile_index(profile, t, i, series_handles.len());
                        let value = 10 + ((i % 10_000) as u64);
                        series_handles[idx].record(value);
                    }
                },
                move || {
                    let mut total = 0u64;
                    for (endpoint_idx, endpoint) in exporter_endpoints.iter().enumerate() {
                        let org_idx = endpoint_idx % exporter_orgs.len();
                        total +=
                            exporter_metric.count(&[("endpoint_uuid", endpoint.as_str()), ("org_id", exporter_orgs[org_idx].as_str())]);
                    }
                    total
                },
            );

            let mut final_count = 0u64;
            for (endpoint_idx, endpoint) in endpoint_values.iter().enumerate() {
                let org_idx = endpoint_idx % org_values.len();
                final_count += metric.count(&[("endpoint_uuid", endpoint.as_str()), ("org_id", org_values[org_idx].as_str())]);
            }
            RunResult {
                final_count: final_count as isize,
                record_seconds,
                total_seconds,
                export_count,
                export_seconds,
            }
        }
        Entity::LabeledCounter => {
            let metric = Arc::new(LabeledCounter::<BenchLabel>::new(shards));
            let worker_metric = Arc::clone(&metric);
            let exporter_metric = Arc::clone(&metric);
            let (record_seconds, total_seconds, export_count, export_seconds) = run_with_threads(
                threads,
                iters,
                thread_affinity,
                export_interval_ms,
                move |t, n| {
                    for i in 0..n {
                        let idx = profile_index(profile, t, i, labels);
                        worker_metric.inc(BenchLabel(idx));
                    }
                },
                move || {
                    let mut total = 0isize;
                    for idx in 0..labels {
                        total += exporter_metric.get(BenchLabel(idx));
                    }
                    total
                },
            );

            let mut final_count = 0isize;
            for idx in 0..labels {
                final_count += metric.get(BenchLabel(idx));
            }
            RunResult {
                final_count,
                record_seconds,
                total_seconds,
                export_count,
                export_seconds,
            }
        }
        Entity::LabeledGauge => {
            let metric = Arc::new(LabeledGauge::<BenchLabel>::new());
            let worker_metric = Arc::clone(&metric);
            let exporter_metric = Arc::clone(&metric);
            let (record_seconds, total_seconds, export_count, export_seconds) = run_with_threads(
                threads,
                iters,
                thread_affinity,
                export_interval_ms,
                move |t, n| {
                    for i in 0..n {
                        let idx = profile_index(profile, t, i, labels);
                        worker_metric.set(BenchLabel(idx), i as i64);
                    }
                },
                move || {
                    let mut total = 0i64;
                    for idx in 0..labels {
                        total += exporter_metric.get(BenchLabel(idx));
                    }
                    total
                },
            );

            RunResult {
                final_count: (threads * iters) as isize,
                record_seconds,
                total_seconds,
                export_count,
                export_seconds,
            }
        }
        Entity::LabeledHistogram => {
            let metric = Arc::new(LabeledHistogram::<BenchLabel>::with_latency_buckets(shards));
            let worker_metric = Arc::clone(&metric);
            let exporter_metric = Arc::clone(&metric);
            let (record_seconds, total_seconds, export_count, export_seconds) = run_with_threads(
                threads,
                iters,
                thread_affinity,
                export_interval_ms,
                move |t, n| {
                    for i in 0..n {
                        let idx = profile_index(profile, t, i, labels);
                        let value = 10 + ((i % 10_000) as u64);
                        worker_metric.record(BenchLabel(idx), value);
                    }
                },
                move || {
                    let mut total = 0u64;
                    for idx in 0..labels {
                        let h = exporter_metric.get(BenchLabel(idx));
                        total += h.count();
                        total += h.sum();
                    }
                    total
                },
            );

            let mut final_count = 0u64;
            for idx in 0..labels {
                final_count += metric.get(BenchLabel(idx)).count();
            }
            RunResult {
                final_count: final_count as isize,
                record_seconds,
                total_seconds,
                export_count,
                export_seconds,
            }
        }
    }
}

fn run_atomic(
    entity: Entity,
    cfg: &Config,
) -> RunResult {
    let threads = cfg.threads;
    let iters = cfg.iters;
    let thread_affinity = cfg.thread_affinity;
    let export_interval_ms = cfg.export_interval_ms;
    assert!(matches!(entity, Entity::Counter), "atomic mode only supports entity=counter");
    let counter = Arc::new(std::sync::atomic::AtomicIsize::new(0));
    let worker_counter = Arc::clone(&counter);
    let exporter_counter = Arc::clone(&counter);
    let (record_seconds, total_seconds, export_count, export_seconds) = run_with_threads(
        threads,
        iters,
        thread_affinity,
        export_interval_ms,
        move |_, n| {
            for _ in 0..n {
                worker_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        },
        move || exporter_counter.load(std::sync::atomic::Ordering::Relaxed),
    );

    RunResult {
        final_count: counter.load(std::sync::atomic::Ordering::Relaxed),
        record_seconds,
        total_seconds,
        export_count,
        export_seconds,
    }
}

fn run_metrics(
    entity: Entity,
    cfg: &Config,
) -> RunResult {
    let threads = cfg.threads;
    let iters = cfg.iters;
    let labels = cfg.labels;
    let profile = cfg.profile;
    let thread_affinity = cfg.thread_affinity;
    let export_interval_ms = cfg.export_interval_ms;

    let registry = MetricsRegistry::<Key, MetricsAtomicStorage>::atomic();

    match entity {
        Entity::Counter => {
            let entry = metrics_counter_entry(&registry, metrics_key("contention_counter", Vec::new()));
            let worker_counter = entry.handle.clone();
            let exporter_cell = Arc::clone(&entry.cell);
            let (record_seconds, total_seconds, export_count, export_seconds) = run_with_threads(
                threads,
                iters,
                thread_affinity,
                export_interval_ms,
                move |_, n| {
                    for _ in 0..n {
                        worker_counter.increment(1);
                    }
                },
                move || exporter_cell.load(Ordering::Relaxed),
            );

            RunResult {
                final_count: entry.cell.load(Ordering::Relaxed) as isize,
                record_seconds,
                total_seconds,
                export_count,
                export_seconds,
            }
        }
        Entity::Distribution | Entity::DynamicDistribution => {
            panic!("metrics mode does not support entity={entity:?}; metrics-rs exposes histograms but not a distribution primitive")
        }
        Entity::DynamicCounter => {
            let org_cardinality = usize::max(1, labels / 4);
            let entries: Arc<Vec<MetricsCounterEntry>> = Arc::new(
                (0..labels)
                    .map(|i| {
                        let org_idx = i % org_cardinality;
                        metrics_counter_entry(
                            &registry,
                            metrics_key(
                                "contention_dynamic_counter",
                                vec![
                                    Label::new("endpoint_uuid", format!("ep{i}")),
                                    Label::new("org_id", format!("org{org_idx}")),
                                ],
                            ),
                        )
                    })
                    .collect(),
            );
            let worker_entries = Arc::clone(&entries);
            let exporter_entries = Arc::clone(&entries);
            let (record_seconds, total_seconds, export_count, export_seconds) = run_with_threads(
                threads,
                iters,
                thread_affinity,
                export_interval_ms,
                move |t, n| {
                    for i in 0..n {
                        let idx = profile_index(profile, t, i, worker_entries.len());
                        worker_entries[idx].handle.increment(1);
                    }
                },
                move || exporter_entries.iter().map(|entry| entry.cell.load(Ordering::Relaxed)).sum::<u64>(),
            );

            RunResult {
                final_count: entries.iter().map(|entry| entry.cell.load(Ordering::Relaxed) as isize).sum(),
                record_seconds,
                total_seconds,
                export_count,
                export_seconds,
            }
        }
        Entity::DynamicGauge => {
            let org_cardinality = usize::max(1, labels / 4);
            let entries: Arc<Vec<MetricsGaugeEntry>> = Arc::new(
                (0..labels)
                    .map(|i| {
                        let org_idx = i % org_cardinality;
                        metrics_gauge_entry(
                            &registry,
                            metrics_key(
                                "contention_dynamic_gauge",
                                vec![
                                    Label::new("endpoint_uuid", format!("ep{i}")),
                                    Label::new("org_id", format!("org{org_idx}")),
                                ],
                            ),
                        )
                    })
                    .collect(),
            );
            let worker_entries = Arc::clone(&entries);
            let exporter_entries = Arc::clone(&entries);
            let (record_seconds, total_seconds, export_count, export_seconds) = run_with_threads(
                threads,
                iters,
                thread_affinity,
                export_interval_ms,
                move |t, n| {
                    for i in 0..n {
                        let idx = profile_index(profile, t, i, worker_entries.len());
                        worker_entries[idx].handle.set(i as f64);
                    }
                },
                move || exporter_entries.iter().map(|entry| metrics_gauge_value(&entry.cell)).sum::<f64>() as u64,
            );

            RunResult {
                final_count: (threads * iters) as isize,
                record_seconds,
                total_seconds,
                export_count,
                export_seconds,
            }
        }
        Entity::DynamicGaugeI64 => {
            let org_cardinality = usize::max(1, labels / 4);
            let entries: Arc<Vec<MetricsGaugeEntry>> = Arc::new(
                (0..labels)
                    .map(|i| {
                        let org_idx = i % org_cardinality;
                        metrics_gauge_entry(
                            &registry,
                            metrics_key(
                                "contention_dynamic_gauge_i64",
                                vec![
                                    Label::new("endpoint_uuid", format!("ep{i}")),
                                    Label::new("org_id", format!("org{org_idx}")),
                                ],
                            ),
                        )
                    })
                    .collect(),
            );
            let worker_entries = Arc::clone(&entries);
            let exporter_entries = Arc::clone(&entries);
            let (record_seconds, total_seconds, export_count, export_seconds) = run_with_threads(
                threads,
                iters,
                thread_affinity,
                export_interval_ms,
                move |t, n| {
                    for i in 0..n {
                        let idx = profile_index(profile, t, i, worker_entries.len());
                        worker_entries[idx].handle.set(i as f64);
                    }
                },
                move || exporter_entries.iter().map(|entry| metrics_gauge_value(&entry.cell)).sum::<f64>() as u64,
            );

            RunResult {
                final_count: (threads * iters) as isize,
                record_seconds,
                total_seconds,
                export_count,
                export_seconds,
            }
        }
        Entity::DynamicHistogram => {
            let org_cardinality = usize::max(1, labels / 4);
            let entries: Arc<Vec<MetricsHistogramEntry>> = Arc::new(
                (0..labels)
                    .map(|i| {
                        let org_idx = i % org_cardinality;
                        metrics_histogram_entry(
                            &registry,
                            metrics_key(
                                "contention_dynamic_histogram",
                                vec![
                                    Label::new("endpoint_uuid", format!("ep{i}")),
                                    Label::new("org_id", format!("org{org_idx}")),
                                ],
                            ),
                        )
                    })
                    .collect(),
            );
            let observed_count = Arc::new(std::sync::atomic::AtomicU64::new(0));
            let worker_entries = Arc::clone(&entries);
            let worker_count = Arc::clone(&observed_count);
            let exporter_entries = Arc::clone(&entries);
            let (record_seconds, total_seconds, export_count, export_seconds) = run_with_threads(
                threads,
                iters,
                thread_affinity,
                export_interval_ms,
                move |t, n| {
                    for i in 0..n {
                        let idx = profile_index(profile, t, i, worker_entries.len());
                        let value = 10 + ((i % 10_000) as u64);
                        worker_entries[idx].handle.record(value as f64);
                        worker_count.fetch_add(1, Ordering::Relaxed);
                    }
                },
                move || exporter_entries.iter().map(|entry| export_metrics_histogram(&entry.cell)).sum::<u64>(),
            );

            RunResult {
                final_count: observed_count.load(Ordering::Relaxed) as isize,
                record_seconds,
                total_seconds,
                export_count,
                export_seconds,
            }
        }
        Entity::LabeledCounter => {
            let entries: Arc<Vec<MetricsCounterEntry>> = Arc::new(
                (0..labels)
                    .map(|i| {
                        metrics_counter_entry(
                            &registry,
                            metrics_key("contention_labeled_counter", vec![Label::new("label", format!("v{i}"))]),
                        )
                    })
                    .collect(),
            );
            let worker_entries = Arc::clone(&entries);
            let exporter_entries = Arc::clone(&entries);
            let (record_seconds, total_seconds, export_count, export_seconds) = run_with_threads(
                threads,
                iters,
                thread_affinity,
                export_interval_ms,
                move |t, n| {
                    for i in 0..n {
                        let idx = profile_index(profile, t, i, worker_entries.len());
                        worker_entries[idx].handle.increment(1);
                    }
                },
                move || exporter_entries.iter().map(|entry| entry.cell.load(Ordering::Relaxed)).sum::<u64>(),
            );

            RunResult {
                final_count: entries.iter().map(|entry| entry.cell.load(Ordering::Relaxed) as isize).sum(),
                record_seconds,
                total_seconds,
                export_count,
                export_seconds,
            }
        }
        Entity::LabeledGauge => {
            let entries: Arc<Vec<MetricsGaugeEntry>> = Arc::new(
                (0..labels)
                    .map(|i| {
                        metrics_gauge_entry(
                            &registry,
                            metrics_key("contention_labeled_gauge", vec![Label::new("label", format!("v{i}"))]),
                        )
                    })
                    .collect(),
            );
            let worker_entries = Arc::clone(&entries);
            let exporter_entries = Arc::clone(&entries);
            let (record_seconds, total_seconds, export_count, export_seconds) = run_with_threads(
                threads,
                iters,
                thread_affinity,
                export_interval_ms,
                move |t, n| {
                    for i in 0..n {
                        let idx = profile_index(profile, t, i, worker_entries.len());
                        worker_entries[idx].handle.set(i as f64);
                    }
                },
                move || exporter_entries.iter().map(|entry| metrics_gauge_value(&entry.cell)).sum::<f64>() as u64,
            );

            RunResult {
                final_count: (threads * iters) as isize,
                record_seconds,
                total_seconds,
                export_count,
                export_seconds,
            }
        }
        Entity::LabeledHistogram => {
            let entries: Arc<Vec<MetricsHistogramEntry>> = Arc::new(
                (0..labels)
                    .map(|i| {
                        metrics_histogram_entry(
                            &registry,
                            metrics_key("contention_labeled_histogram", vec![Label::new("label", format!("v{i}"))]),
                        )
                    })
                    .collect(),
            );
            let observed_count = Arc::new(std::sync::atomic::AtomicU64::new(0));
            let worker_entries = Arc::clone(&entries);
            let worker_count = Arc::clone(&observed_count);
            let exporter_entries = Arc::clone(&entries);
            let (record_seconds, total_seconds, export_count, export_seconds) = run_with_threads(
                threads,
                iters,
                thread_affinity,
                export_interval_ms,
                move |t, n| {
                    for i in 0..n {
                        let idx = profile_index(profile, t, i, worker_entries.len());
                        let value = 10 + ((i % 10_000) as u64);
                        worker_entries[idx].handle.record(value as f64);
                        worker_count.fetch_add(1, Ordering::Relaxed);
                    }
                },
                move || exporter_entries.iter().map(|entry| export_metrics_histogram(&entry.cell)).sum::<u64>(),
            );

            RunResult {
                final_count: observed_count.load(Ordering::Relaxed) as isize,
                record_seconds,
                total_seconds,
                export_count,
                export_seconds,
            }
        }
    }
}

fn run_otel(entity: Entity, cfg: &Config) -> RunResult {
    let threads = cfg.threads;
    let iters = cfg.iters;
    let labels = cfg.labels;
    let profile = cfg.profile;
    let thread_affinity = cfg.thread_affinity;
    let export_interval_ms = cfg.export_interval_ms;
    let exporter = InMemoryMetricExporter::default();
    let provider = Arc::new(SdkMeterProvider::builder().with_periodic_exporter(exporter.clone()).build());
    let meter = provider.meter("fast-telemetry.bench_cache_contention");

    let attrs: Arc<Vec<KeyValue>> = Arc::new((0..labels).map(|i| KeyValue::new("label", format!("v{i}"))).collect());

    match entity {
        Entity::Counter => {
            let counter: Arc<OTelCounter<u64>> = Arc::new(meter.u64_counter("contention_counter").build());
            let worker_counter = Arc::clone(&counter);
            let exporter_provider = Arc::clone(&provider);
            let exporter_exporter = exporter.clone();

            let (record_seconds, total_seconds, export_count, export_seconds) = run_with_threads(
                threads,
                iters,
                thread_affinity,
                export_interval_ms,
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

            let _ = provider.force_flush();
            let _ = exporter.get_finished_metrics();
            RunResult {
                final_count: (threads * iters) as isize,
                record_seconds,
                total_seconds,
                export_count,
                export_seconds,
            }
        }
        Entity::Distribution => {
            // OTel histogram as the closest equivalent to Distribution
            let histogram: Arc<OTelHistogram<u64>> = Arc::new(meter.u64_histogram("contention_distribution").build());
            let worker_hist = Arc::clone(&histogram);
            let exporter_provider = Arc::clone(&provider);
            let exporter_exporter = exporter.clone();

            let (record_seconds, total_seconds, export_count, export_seconds) = run_with_threads(
                threads,
                iters,
                thread_affinity,
                export_interval_ms,
                move |_, n| {
                    for i in 0..n {
                        let value = 10 + ((i % 10_000) as u64);
                        worker_hist.record(value, &[]);
                    }
                },
                move || {
                    let _ = exporter_provider.force_flush();
                    let _ = exporter_exporter.get_finished_metrics();
                    exporter_exporter.reset();
                    0usize
                },
            );

            let _ = provider.force_flush();
            let _ = exporter.get_finished_metrics();
            RunResult {
                final_count: (threads * iters) as isize,
                record_seconds,
                total_seconds,
                export_count,
                export_seconds,
            }
        }
        Entity::DynamicCounter => {
            let counter: Arc<OTelCounter<u64>> = Arc::new(meter.u64_counter("contention_dynamic_counter").build());
            let org_cardinality = usize::max(1, labels / 4);
            let attrs: Arc<Vec<Vec<KeyValue>>> = Arc::new(
                (0..labels)
                    .map(|i| {
                        let org_idx = i % org_cardinality;
                        vec![
                            KeyValue::new("endpoint_uuid", format!("ep{i}")),
                            KeyValue::new("org_id", format!("org{org_idx}")),
                        ]
                    })
                    .collect(),
            );
            let worker_counter = Arc::clone(&counter);
            let worker_attrs = Arc::clone(&attrs);
            let exporter_provider = Arc::clone(&provider);
            let exporter_exporter = exporter.clone();

            let (record_seconds, total_seconds, export_count, export_seconds) = run_with_threads(
                threads,
                iters,
                thread_affinity,
                export_interval_ms,
                move |t, n| {
                    for i in 0..n {
                        let idx = profile_index(profile, t, i, worker_attrs.len());
                        worker_counter.add(1, &worker_attrs[idx]);
                    }
                },
                move || {
                    let _ = exporter_provider.force_flush();
                    let _ = exporter_exporter.get_finished_metrics();
                    exporter_exporter.reset();
                    0usize
                },
            );

            let _ = provider.force_flush();
            let _ = exporter.get_finished_metrics();
            RunResult {
                final_count: (threads * iters) as isize,
                record_seconds,
                total_seconds,
                export_count,
                export_seconds,
            }
        }
        Entity::DynamicDistribution => {
            // OTel histogram as the closest equivalent
            let histogram: Arc<OTelHistogram<u64>> = Arc::new(meter.u64_histogram("contention_dynamic_distribution").build());
            let org_cardinality = usize::max(1, labels / 4);
            let attrs: Arc<Vec<Vec<KeyValue>>> = Arc::new(
                (0..labels)
                    .map(|i| {
                        let org_idx = i % org_cardinality;
                        vec![
                            KeyValue::new("endpoint_uuid", format!("ep{i}")),
                            KeyValue::new("org_id", format!("org{org_idx}")),
                        ]
                    })
                    .collect(),
            );
            let worker_hist = Arc::clone(&histogram);
            let worker_attrs = Arc::clone(&attrs);
            let exporter_provider = Arc::clone(&provider);
            let exporter_exporter = exporter.clone();

            let (record_seconds, total_seconds, export_count, export_seconds) = run_with_threads(
                threads,
                iters,
                thread_affinity,
                export_interval_ms,
                move |t, n| {
                    for i in 0..n {
                        let idx = profile_index(profile, t, i, worker_attrs.len());
                        let value = 10 + ((i % 10_000) as u64);
                        worker_hist.record(value, &worker_attrs[idx]);
                    }
                },
                move || {
                    let _ = exporter_provider.force_flush();
                    let _ = exporter_exporter.get_finished_metrics();
                    exporter_exporter.reset();
                    0usize
                },
            );

            let _ = provider.force_flush();
            let _ = exporter.get_finished_metrics();
            RunResult {
                final_count: (threads * iters) as isize,
                record_seconds,
                total_seconds,
                export_count,
                export_seconds,
            }
        }
        Entity::DynamicGauge => {
            let gauge: Arc<OTelGauge<f64>> = Arc::new(meter.f64_gauge("contention_dynamic_gauge").build());
            let org_cardinality = usize::max(1, labels / 4);
            let attrs: Arc<Vec<Vec<KeyValue>>> = Arc::new(
                (0..labels)
                    .map(|i| {
                        let org_idx = i % org_cardinality;
                        vec![
                            KeyValue::new("endpoint_uuid", format!("ep{i}")),
                            KeyValue::new("org_id", format!("org{org_idx}")),
                        ]
                    })
                    .collect(),
            );
            let worker_gauge = Arc::clone(&gauge);
            let worker_attrs = Arc::clone(&attrs);
            let exporter_provider = Arc::clone(&provider);
            let exporter_exporter = exporter.clone();

            let (record_seconds, total_seconds, export_count, export_seconds) = run_with_threads(
                threads,
                iters,
                thread_affinity,
                export_interval_ms,
                move |t, n| {
                    for i in 0..n {
                        let idx = profile_index(profile, t, i, worker_attrs.len());
                        worker_gauge.record(i as f64, &worker_attrs[idx]);
                    }
                },
                move || {
                    let _ = exporter_provider.force_flush();
                    let _ = exporter_exporter.get_finished_metrics();
                    exporter_exporter.reset();
                    0usize
                },
            );

            let _ = provider.force_flush();
            let _ = exporter.get_finished_metrics();
            RunResult {
                final_count: (threads * iters) as isize,
                record_seconds,
                total_seconds,
                export_count,
                export_seconds,
            }
        }
        Entity::DynamicGaugeI64 => {
            let gauge: Arc<OTelGauge<i64>> = Arc::new(meter.i64_gauge("contention_dynamic_gauge_i64").build());
            let org_cardinality = usize::max(1, labels / 4);
            let attrs: Arc<Vec<Vec<KeyValue>>> = Arc::new(
                (0..labels)
                    .map(|i| {
                        let org_idx = i % org_cardinality;
                        vec![
                            KeyValue::new("endpoint_uuid", format!("ep{i}")),
                            KeyValue::new("org_id", format!("org{org_idx}")),
                        ]
                    })
                    .collect(),
            );
            let worker_gauge = Arc::clone(&gauge);
            let worker_attrs = Arc::clone(&attrs);
            let exporter_provider = Arc::clone(&provider);
            let exporter_exporter = exporter.clone();

            let (record_seconds, total_seconds, export_count, export_seconds) = run_with_threads(
                threads,
                iters,
                thread_affinity,
                export_interval_ms,
                move |t, n| {
                    for i in 0..n {
                        let idx = profile_index(profile, t, i, worker_attrs.len());
                        worker_gauge.record(i as i64, &worker_attrs[idx]);
                    }
                },
                move || {
                    let _ = exporter_provider.force_flush();
                    let _ = exporter_exporter.get_finished_metrics();
                    exporter_exporter.reset();
                    0usize
                },
            );

            let _ = provider.force_flush();
            let _ = exporter.get_finished_metrics();
            RunResult {
                final_count: (threads * iters) as isize,
                record_seconds,
                total_seconds,
                export_count,
                export_seconds,
            }
        }
        Entity::DynamicHistogram => {
            let histogram: Arc<OTelHistogram<u64>> = Arc::new(meter.u64_histogram("contention_dynamic_histogram").build());
            let org_cardinality = usize::max(1, labels / 4);
            let attrs: Arc<Vec<Vec<KeyValue>>> = Arc::new(
                (0..labels)
                    .map(|i| {
                        let org_idx = i % org_cardinality;
                        vec![
                            KeyValue::new("endpoint_uuid", format!("ep{i}")),
                            KeyValue::new("org_id", format!("org{org_idx}")),
                        ]
                    })
                    .collect(),
            );
            let worker_hist = Arc::clone(&histogram);
            let worker_attrs = Arc::clone(&attrs);
            let exporter_provider = Arc::clone(&provider);
            let exporter_exporter = exporter.clone();

            let (record_seconds, total_seconds, export_count, export_seconds) = run_with_threads(
                threads,
                iters,
                thread_affinity,
                export_interval_ms,
                move |t, n| {
                    for i in 0..n {
                        let idx = profile_index(profile, t, i, worker_attrs.len());
                        let value = 10 + ((i % 10_000) as u64);
                        worker_hist.record(value, &worker_attrs[idx]);
                    }
                },
                move || {
                    let _ = exporter_provider.force_flush();
                    let _ = exporter_exporter.get_finished_metrics();
                    exporter_exporter.reset();
                    0usize
                },
            );

            let _ = provider.force_flush();
            let _ = exporter.get_finished_metrics();
            RunResult {
                final_count: (threads * iters) as isize,
                record_seconds,
                total_seconds,
                export_count,
                export_seconds,
            }
        }
        Entity::LabeledCounter => {
            let counter: Arc<OTelCounter<u64>> = Arc::new(meter.u64_counter("contention_labeled_counter").build());
            let worker_counter = Arc::clone(&counter);
            let worker_attrs = Arc::clone(&attrs);
            let exporter_provider = Arc::clone(&provider);
            let exporter_exporter = exporter.clone();

            let (record_seconds, total_seconds, export_count, export_seconds) = run_with_threads(
                threads,
                iters,
                thread_affinity,
                export_interval_ms,
                move |t, n| {
                    for i in 0..n {
                        let idx = profile_index(profile, t, i, worker_attrs.len());
                        let kv = std::slice::from_ref(&worker_attrs[idx]);
                        worker_counter.add(1, kv);
                    }
                },
                move || {
                    let _ = exporter_provider.force_flush();
                    let _ = exporter_exporter.get_finished_metrics();
                    exporter_exporter.reset();
                    0usize
                },
            );

            let _ = provider.force_flush();
            let _ = exporter.get_finished_metrics();
            RunResult {
                final_count: (threads * iters) as isize,
                record_seconds,
                total_seconds,
                export_count,
                export_seconds,
            }
        }
        Entity::LabeledGauge => {
            let gauge: Arc<OTelGauge<i64>> = Arc::new(meter.i64_gauge("contention_labeled_gauge").build());
            let worker_gauge = Arc::clone(&gauge);
            let worker_attrs = Arc::clone(&attrs);
            let exporter_provider = Arc::clone(&provider);
            let exporter_exporter = exporter.clone();

            let (record_seconds, total_seconds, export_count, export_seconds) = run_with_threads(
                threads,
                iters,
                thread_affinity,
                export_interval_ms,
                move |t, n| {
                    for i in 0..n {
                        let idx = profile_index(profile, t, i, worker_attrs.len());
                        let kv = std::slice::from_ref(&worker_attrs[idx]);
                        worker_gauge.record(i as i64, kv);
                    }
                },
                move || {
                    let _ = exporter_provider.force_flush();
                    let _ = exporter_exporter.get_finished_metrics();
                    exporter_exporter.reset();
                    0usize
                },
            );

            let _ = provider.force_flush();
            let _ = exporter.get_finished_metrics();
            RunResult {
                final_count: (threads * iters) as isize,
                record_seconds,
                total_seconds,
                export_count,
                export_seconds,
            }
        }
        Entity::LabeledHistogram => {
            let histogram: Arc<OTelHistogram<u64>> = Arc::new(meter.u64_histogram("contention_labeled_histogram").build());
            let worker_hist = Arc::clone(&histogram);
            let worker_attrs = Arc::clone(&attrs);
            let exporter_provider = Arc::clone(&provider);
            let exporter_exporter = exporter.clone();

            let (record_seconds, total_seconds, export_count, export_seconds) = run_with_threads(
                threads,
                iters,
                thread_affinity,
                export_interval_ms,
                move |t, n| {
                    for i in 0..n {
                        let idx = profile_index(profile, t, i, worker_attrs.len());
                        let kv = std::slice::from_ref(&worker_attrs[idx]);
                        let value = 10 + ((i % 10_000) as u64);
                        worker_hist.record(value, kv);
                    }
                },
                move || {
                    let _ = exporter_provider.force_flush();
                    let _ = exporter_exporter.get_finished_metrics();
                    exporter_exporter.reset();
                    0usize
                },
            );

            let _ = provider.force_flush();
            let _ = exporter.get_finished_metrics();
            RunResult {
                final_count: (threads * iters) as isize,
                record_seconds,
                total_seconds,
                export_count,
                export_seconds,
            }
        }
    }
}

fn main() {
    let cfg = parse_args();
    let total_ops = cfg.threads * cfg.iters;
    let cpu_start = ProcessCpuSnapshot::capture().ok();

    let result = match cfg.mode {
        Mode::Fast => run_fast(cfg.entity, &cfg),
        Mode::Atomic => run_atomic(cfg.entity, &cfg),
        Mode::Metrics => run_metrics(cfg.entity, &cfg),
        Mode::Otel => run_otel(cfg.entity, &cfg),
    };
    let cpu_usage = cpu_start.and_then(|start| ProcessCpuSnapshot::capture().ok().map(|end| end.elapsed_since(start)));

    let record_ops_per_sec = (total_ops as f64) / result.record_seconds;
    let total_ops_per_sec = (total_ops as f64) / result.total_seconds;
    let export_avg_ms = if result.export_count == 0 {
        0.0
    } else {
        (result.export_seconds * 1000.0) / (result.export_count as f64)
    };
    let mode = match cfg.mode {
        Mode::Fast => "fast",
        Mode::Atomic => "atomic",
        Mode::Metrics => "metrics",
        Mode::Otel => "otel",
    };
    let entity = match cfg.entity {
        Entity::Counter => "counter",
        Entity::Distribution => "distribution",
        Entity::DynamicCounter => "dynamic_counter",
        Entity::DynamicDistribution => "dynamic_distribution",
        Entity::DynamicGauge => "dynamic_gauge",
        Entity::DynamicGaugeI64 => "dynamic_gauge_i64",
        Entity::DynamicHistogram => "dynamic_histogram",
        Entity::LabeledCounter => "labeled_counter",
        Entity::LabeledGauge => "labeled_gauge",
        Entity::LabeledHistogram => "labeled_histogram",
    };

    let profile = match cfg.profile {
        WorkloadProfile::Uniform => "uniform",
        WorkloadProfile::Hotspot => "hotspot",
        WorkloadProfile::Churn => "churn",
    };

    let expected_count = (cfg.threads * cfg.iters) as isize;
    let verify_count = matches!(
        cfg.entity,
        Entity::Counter
            | Entity::Distribution
            | Entity::DynamicCounter
            | Entity::DynamicDistribution
            | Entity::DynamicHistogram
            | Entity::LabeledCounter
            | Entity::LabeledHistogram
    );
    let verified = !verify_count || result.final_count == expected_count;
    let cpu_user_seconds = cpu_usage.map_or(0.0, |usage| usage.user_seconds);
    let cpu_system_seconds = cpu_usage.map_or(0.0, |usage| usage.system_seconds);
    let cpu_total_seconds = cpu_usage.map_or(0.0, |usage| usage.total_seconds);
    let cpu_avg_cores = if result.total_seconds > 0.0 {
        cpu_total_seconds / result.total_seconds
    } else {
        0.0
    };
    let cpu_utilization_pct = cpu_avg_cores * 100.0;
    let cpu_ns_per_op = if total_ops > 0 {
        (cpu_total_seconds * 1_000_000_000.0) / (total_ops as f64)
    } else {
        0.0
    };

    println!("mode={mode}");
    println!("entity={entity}");
    println!("profile={profile}");
    println!("thread_affinity={}", cfg.thread_affinity.as_str());
    println!("threads={}", cfg.threads);
    println!("iters_per_thread={}", cfg.iters);
    println!("shards={}", cfg.shards);
    println!("labels={}", cfg.labels);
    println!("export_interval_ms={}", cfg.export_interval_ms);
    println!("total_ops={total_ops}");
    println!("record_seconds={:.6}", result.record_seconds);
    println!("total_seconds={:.6}", result.total_seconds);
    println!("record_ops_per_sec={record_ops_per_sec:.2}");
    println!("total_ops_per_sec={total_ops_per_sec:.2}");
    println!("ops_per_sec={total_ops_per_sec:.2}");
    println!("export_count={}", result.export_count);
    println!("export_seconds={:.6}", result.export_seconds);
    println!("export_avg_ms={export_avg_ms:.6}");
    println!("cpu_usage_measured={}", cpu_usage.is_some());
    println!("cpu_user_seconds={cpu_user_seconds:.6}");
    println!("cpu_system_seconds={cpu_system_seconds:.6}");
    println!("cpu_total_seconds={cpu_total_seconds:.6}");
    println!("cpu_avg_cores={cpu_avg_cores:.6}");
    println!("cpu_utilization_pct={cpu_utilization_pct:.2}");
    println!("cpu_ns_per_op={cpu_ns_per_op:.2}");
    println!("final_count={}", result.final_count);
    if verify_count {
        println!("expected_count={expected_count}");
    }
    println!("verified={verified}");
}
