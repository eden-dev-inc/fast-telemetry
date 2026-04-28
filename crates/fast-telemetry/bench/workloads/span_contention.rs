// Compare contention behavior of fast-telemetry spans vs OpenTelemetry SDK spans.
//
// Each worker thread creates spans in a tight loop. An exporter thread drains
// completed spans periodically. Measures both recording throughput and export cost.
//
// Build and run directly:
// - cargo run --release --bin bench_span_contention --features bench-tools -- --mode fast --threads 8 --iters 1000000
// - cargo run --release --bin bench_span_contention --features bench-tools -- --mode otel --threads 8 --iters 1000000
// - cargo run --release --bin bench_span_contention --features bench-tools -- --mode fast --scenario lifecycle --threads 8 --iters 500000

use std::sync::mpsc;
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

// ============================================================================
// Config
// ============================================================================

#[derive(Copy, Clone)]
enum Mode {
    Fast,
    Otel,
}

#[derive(Copy, Clone)]
enum Scenario {
    /// Root span: create + drop (minimal).
    RootOnly,
    /// Root + child span with attributes and events (realistic handler).
    Lifecycle,
    /// Root span + 3 sequential children (typical request pipeline).
    Pipeline,
}

struct Config {
    mode: Mode,
    scenario: Scenario,
    threads: usize,
    iters: usize,
    thread_affinity: ThreadAffinityMode,
    export_interval_ms: u64,
}

struct RunResult {
    span_count: u64,
    record_seconds: f64,
    total_seconds: f64,
    export_count: u64,
    export_seconds: f64,
}

// ============================================================================
// Args
// ============================================================================

fn parse_args() -> Config {
    let mut mode = Mode::Fast;
    let mut scenario = Scenario::RootOnly;
    let mut threads = std::thread::available_parallelism().map_or(4, |n| n.get());
    let mut iters = 1_000_000usize;
    let mut thread_affinity = ThreadAffinityMode::Off;
    let mut export_interval_ms = 10u64;

    let args: Vec<String> = std::env::args().collect();
    let mut i = 1usize;
    while i < args.len() {
        match args[i].as_str() {
            "--mode" if i + 1 < args.len() => {
                mode = match args[i + 1].as_str() {
                    "fast" => Mode::Fast,
                    "otel" => Mode::Otel,
                    value => panic!("invalid --mode: {value} (expected fast|otel)"),
                };
                i += 2;
            }
            "--scenario" if i + 1 < args.len() => {
                scenario = match args[i + 1].as_str() {
                    "root" => Scenario::RootOnly,
                    "lifecycle" => Scenario::Lifecycle,
                    "pipeline" => Scenario::Pipeline,
                    value => panic!("invalid --scenario: {value} (expected root|lifecycle|pipeline)"),
                };
                i += 2;
            }
            "--threads" if i + 1 < args.len() => {
                threads = args[i + 1].parse().expect("--threads must be an integer");
                i += 2;
            }
            "--thread-affinity" if i + 1 < args.len() => {
                thread_affinity = ThreadAffinityMode::parse(args[i + 1].as_str()).unwrap_or_else(|| {
                    panic!("invalid --thread-affinity: {} (expected off|round_robin|rr)", args[i + 1])
                });
                i += 2;
            }
            "--iters" if i + 1 < args.len() => {
                iters = args[i + 1].parse().expect("--iters must be an integer");
                i += 2;
            }
            "--export-interval-ms" if i + 1 < args.len() => {
                export_interval_ms = args[i + 1].parse().expect("--export-interval-ms must be an integer");
                i += 2;
            }
            "--help" => {
                println!(
                    "Usage: bench_span_contention --mode <fast|otel> [--scenario <root|lifecycle|pipeline>] --threads <n> --iters <n> [--thread-affinity <off|round_robin|rr>] [--export-interval-ms <n>]"
                );
                std::process::exit(0);
            }
            arg => panic!("unknown arg: {arg}"),
        }
    }

    Config { mode, scenario, threads, iters, thread_affinity, export_interval_ms }
}

// ============================================================================
// Threading harness (same pattern as bench_cache_contention)
// ============================================================================

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
    let exporter_handle = std::thread::spawn(move || {
        exporter_barrier.wait();
        let mut export_count = 0u64;
        let mut export_seconds = 0.0f64;
        loop {
            let t0 = Instant::now();
            let _ = exporter();
            export_seconds += t0.elapsed().as_secs_f64();
            export_count += 1;
            if stop_rx.recv_timeout(interval).is_ok() {
                break;
            }
        }
        (export_count, export_seconds)
    });

    let worker = Arc::new(worker);
    let warmup_iters = (iters / 10).max(1);
    let mut workers = Vec::with_capacity(threads);
    for t in 0..threads {
        let worker_fn = Arc::clone(&worker);
        let worker_barrier = Arc::clone(&barrier);
        workers.push(std::thread::spawn(move || {
            thread_affinity::pin_worker_thread(t, thread_affinity);
            // Warmup pass: discarded from measurement because it runs before
            // the barrier release.
            worker_fn(t, warmup_iters);
            worker_barrier.wait();
            worker_fn(t, iters);
        }));
    }

    barrier.wait();
    let record_start = Instant::now();
    for w in workers {
        w.join().expect("worker thread panicked");
    }
    let record_seconds = record_start.elapsed().as_secs_f64();

    let flush_start = Instant::now();
    let _ = stop_tx.send(());
    let (export_count, export_seconds) = exporter_handle.join().expect("exporter panicked");
    let total_seconds = record_seconds + flush_start.elapsed().as_secs_f64();

    (record_seconds, total_seconds, export_count, export_seconds)
}

// ============================================================================
// fast-telemetry spans
// ============================================================================

fn run_fast(
    scenario: Scenario,
    threads: usize,
    iters: usize,
    thread_affinity: ThreadAffinityMode,
    export_interval_ms: u64,
) -> RunResult {
    use fast_telemetry::{SpanAttribute, SpanCollector, SpanKind, SpanStatus};

    let collector = Arc::new(SpanCollector::new(threads, 4096));

    // Spans per iteration depends on scenario.
    let spans_per_iter: u64 = match scenario {
        Scenario::RootOnly => 1,
        Scenario::Lifecycle => 2, // root + child
        Scenario::Pipeline => 4,  // root + 3 children
    };

    let worker_collector = Arc::clone(&collector);
    let exporter_collector = Arc::clone(&collector);

    let (record_seconds, total_seconds, export_count, export_seconds) = run_with_threads(
        threads,
        iters,
        thread_affinity,
        export_interval_ms,
        move |_t, n| match scenario {
            Scenario::RootOnly => {
                for _ in 0..n {
                    let _span = worker_collector.start_span("handle_request", SpanKind::Server);
                }
            }
            Scenario::Lifecycle => {
                for _ in 0..n {
                    let mut span = worker_collector.start_span("handle_request", SpanKind::Server);
                    span.set_attribute("http.method", "GET");
                    span.set_attribute("http.url", "/api/v1/users");
                    span.add_event("auth_check", vec![SpanAttribute::new("result", "pass")]);
                    {
                        let mut child = span.child("db_query", SpanKind::Client);
                        child.set_attribute("db.statement", "SELECT * FROM users");
                        child.set_status(SpanStatus::Ok);
                    }
                    span.set_status(SpanStatus::Ok);
                }
            }
            Scenario::Pipeline => {
                for _ in 0..n {
                    let mut root = worker_collector.start_span("handle_request", SpanKind::Server);
                    root.set_attribute("http.method", "POST");
                    {
                        let mut c1 = root.child("validate", SpanKind::Internal);
                        c1.set_attribute("valid", true);
                        c1.set_status(SpanStatus::Ok);
                    }
                    {
                        let mut c2 = root.child("db_write", SpanKind::Client);
                        c2.set_attribute("db.statement", "INSERT INTO orders");
                        c2.set_status(SpanStatus::Ok);
                    }
                    {
                        let mut c3 = root.child("notify", SpanKind::Producer);
                        c3.set_attribute("topic", "order_events");
                        c3.set_status(SpanStatus::Ok);
                    }
                    root.set_status(SpanStatus::Ok);
                }
            }
        },
        move || {
            let mut buf = Vec::new();
            exporter_collector.drain_into(&mut buf);
            buf.len()
        },
    );

    RunResult {
        span_count: (threads as u64) * (iters as u64) * spans_per_iter,
        record_seconds,
        total_seconds,
        export_count,
        export_seconds,
    }
}

// ============================================================================
// OTel SDK spans
// ============================================================================

fn run_otel(
    scenario: Scenario,
    threads: usize,
    iters: usize,
    thread_affinity: ThreadAffinityMode,
    export_interval_ms: u64,
) -> RunResult {
    use opentelemetry::KeyValue;
    use opentelemetry::trace::{Span, SpanKind as OtelSpanKind, Status as OtelStatus, TraceContextExt, Tracer, TracerProvider};
    use opentelemetry_sdk::trace::{InMemorySpanExporterBuilder, SdkTracerProvider};

    let exporter = InMemorySpanExporterBuilder::new().build();
    let provider = Arc::new(SdkTracerProvider::builder().with_simple_exporter(exporter.clone()).build());
    let tracer = Arc::new(provider.tracer("bench"));

    let spans_per_iter: u64 = match scenario {
        Scenario::RootOnly => 1,
        Scenario::Lifecycle => 2,
        Scenario::Pipeline => 4,
    };

    let worker_tracer = Arc::clone(&tracer);
    let exporter_provider = Arc::clone(&provider);

    let (record_seconds, total_seconds, export_count, export_seconds) = run_with_threads(
        threads,
        iters,
        thread_affinity,
        export_interval_ms,
        move |_t, n| {
            match scenario {
                Scenario::RootOnly => {
                    for _ in 0..n {
                        let span = worker_tracer.span_builder("handle_request").with_kind(OtelSpanKind::Server).start(&*worker_tracer);
                        drop(span);
                    }
                }
                Scenario::Lifecycle => {
                    for _ in 0..n {
                        let mut root = worker_tracer.span_builder("handle_request").with_kind(OtelSpanKind::Server).start(&*worker_tracer);
                        root.set_attribute(KeyValue::new("http.method", "GET"));
                        root.set_attribute(KeyValue::new("http.url", "/api/v1/users"));
                        root.add_event("auth_check", vec![KeyValue::new("result", "pass")]);
                        root.set_status(OtelStatus::Ok);
                        let parent_cx = opentelemetry::Context::current_with_span(root);
                        let mut child = worker_tracer
                            .span_builder("db_query")
                            .with_kind(OtelSpanKind::Client)
                            .start_with_context(&*worker_tracer, &parent_cx);
                        child.set_attribute(KeyValue::new("db.statement", "SELECT * FROM users"));
                        child.set_status(OtelStatus::Ok);
                        child.end();
                        // parent_cx drops, ending root.
                    }
                }
                Scenario::Pipeline => {
                    for _ in 0..n {
                        let mut root = worker_tracer.span_builder("handle_request").with_kind(OtelSpanKind::Server).start(&*worker_tracer);
                        root.set_attribute(KeyValue::new("http.method", "POST"));
                        root.set_status(OtelStatus::Ok);
                        let parent_cx = opentelemetry::Context::current_with_span(root);
                        {
                            let mut c1 = worker_tracer
                                .span_builder("validate")
                                .with_kind(OtelSpanKind::Internal)
                                .start_with_context(&*worker_tracer, &parent_cx);
                            c1.set_attribute(KeyValue::new("valid", true));
                            c1.set_status(OtelStatus::Ok);
                            c1.end();
                        }
                        {
                            let mut c2 = worker_tracer
                                .span_builder("db_write")
                                .with_kind(OtelSpanKind::Client)
                                .start_with_context(&*worker_tracer, &parent_cx);
                            c2.set_attribute(KeyValue::new("db.statement", "INSERT INTO orders"));
                            c2.set_status(OtelStatus::Ok);
                            c2.end();
                        }
                        {
                            let mut c3 = worker_tracer
                                .span_builder("notify")
                                .with_kind(OtelSpanKind::Producer)
                                .start_with_context(&*worker_tracer, &parent_cx);
                            c3.set_attribute(KeyValue::new("topic", "order_events"));
                            c3.set_status(OtelStatus::Ok);
                            c3.end();
                        }
                        // parent_cx drops, ending root.
                    }
                }
            }
        },
        move || {
            let _ = exporter_provider.force_flush();
            exporter.reset();
            0usize
        },
    );

    RunResult {
        span_count: (threads as u64) * (iters as u64) * spans_per_iter,
        record_seconds,
        total_seconds,
        export_count,
        export_seconds,
    }
}

// ============================================================================
// Main
// ============================================================================

fn main() {
    let cfg = parse_args();
    let total_ops = cfg.threads * cfg.iters;
    let cpu_start = ProcessCpuSnapshot::capture().ok();

    let result = match cfg.mode {
        Mode::Fast => run_fast(cfg.scenario, cfg.threads, cfg.iters, cfg.thread_affinity, cfg.export_interval_ms),
        Mode::Otel => run_otel(cfg.scenario, cfg.threads, cfg.iters, cfg.thread_affinity, cfg.export_interval_ms),
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
        Mode::Otel => "otel",
    };
    let scenario = match cfg.scenario {
        Scenario::RootOnly => "root",
        Scenario::Lifecycle => "lifecycle",
        Scenario::Pipeline => "pipeline",
    };
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
    println!("scenario={scenario}");
    println!("threads={}", cfg.threads);
    println!("iters_per_thread={}", cfg.iters);
    println!("thread_affinity={}", cfg.thread_affinity.as_str());
    println!("export_interval_ms={}", cfg.export_interval_ms);
    println!("total_ops={total_ops}");
    println!("total_spans={}", result.span_count);
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
}
