// Stress test for dynamic series lifecycle under concurrent load.
//
// Exercises: concurrent series creation at/above cardinality cap, eviction
// sweeps running in parallel with hot-path writes, thread-local cache
// invalidation after eviction, series handle survival across evictions,
// and series_count bookkeeping consistency.
//
// Run:
//   cargo run --release --bin bench_dynamic_series -- --threads 16 --iters 1000000 --cap 100
//   cargo run --release --bin bench_dynamic_series -- --threads 8 --iters 500000 --cap 50 --sweep-ms 5

use fast_telemetry::{DynamicCounter, advance_cycle};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

struct Config {
    threads: usize,
    iters: u64,
    cap: usize,
    shards: usize,
    labels_per_thread: usize,
    sweep_interval_ms: u64,
    sweep_threshold: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            threads: 8,
            iters: 500_000,
            cap: 100,
            shards: 8,
            labels_per_thread: 20,
            sweep_interval_ms: 10,
            sweep_threshold: 3,
        }
    }
}

fn parse_args() -> Config {
    let mut config = Config::default();
    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--threads" => { i += 1; config.threads = args[i].parse().expect("--threads N"); }
            "--iters" => { i += 1; config.iters = args[i].parse().expect("--iters N"); }
            "--cap" => { i += 1; config.cap = args[i].parse().expect("--cap N"); }
            "--shards" => { i += 1; config.shards = args[i].parse().expect("--shards N"); }
            "--labels" => { i += 1; config.labels_per_thread = args[i].parse().expect("--labels N"); }
            "--sweep-ms" => { i += 1; config.sweep_interval_ms = args[i].parse().expect("--sweep-ms N"); }
            "--sweep-threshold" => { i += 1; config.sweep_threshold = args[i].parse().expect("--sweep-threshold N"); }
            "--help" | "-h" => {
                eprintln!("Usage: bench_dynamic_series [OPTIONS]");
                eprintln!("  --threads N          Worker threads (default: 8)");
                eprintln!("  --iters N            Iterations per thread (default: 500000)");
                eprintln!("  --cap N              Series cardinality cap (default: 100)");
                eprintln!("  --shards N           Index shard count (default: 8)");
                eprintln!("  --labels N           Distinct labels per thread (default: 20)");
                eprintln!("  --sweep-ms N         Sweep interval in ms (default: 10)");
                eprintln!("  --sweep-threshold N  Cycles before eviction (default: 3)");
                std::process::exit(0);
            }
            other => {
                eprintln!("Unknown argument: {other}");
                std::process::exit(1);
            }
        }
        i += 1;
    }
    config
}

fn main() {
    let config = parse_args();
    let total_possible_labels = config.threads * config.labels_per_thread;

    eprintln!(
        "Dynamic series stress test: {} threads, {} iters/thread, cap={}, shards={}, labels/thread={} ({} total distinct)",
        config.threads, config.iters, config.cap, config.shards, config.labels_per_thread, total_possible_labels
    );

    let counter = Arc::new(DynamicCounter::with_max_series(config.shards, config.cap));
    let stop = Arc::new(AtomicBool::new(false));
    let total_evicted = Arc::new(AtomicU64::new(0));
    let total_sweeps = Arc::new(AtomicU64::new(0));

    // Sweeper thread — runs concurrently with writers
    let sweep_counter = Arc::clone(&counter);
    let sweep_stop = Arc::clone(&stop);
    let sweep_evicted = Arc::clone(&total_evicted);
    let sweep_count = Arc::clone(&total_sweeps);
    let sweep_threshold = config.sweep_threshold;
    let sweep_interval = Duration::from_millis(config.sweep_interval_ms);

    let sweeper = std::thread::Builder::new()
        .name("sweeper".to_string())
        .spawn(move || {
            while !sweep_stop.load(Ordering::Relaxed) {
                std::thread::sleep(sweep_interval);
                advance_cycle();
                let evicted = sweep_counter.evict_stale(sweep_threshold);
                sweep_evicted.fetch_add(evicted as u64, Ordering::Relaxed);
                sweep_count.fetch_add(1, Ordering::Relaxed);
            }
        })
        .expect("spawn sweeper");

    // Worker threads — create series, use handles, do label lookups
    let barrier = Arc::new(Barrier::new(config.threads));
    let start = Instant::now();

    let workers: Vec<_> = (0..config.threads)
        .map(|t| {
            let counter = Arc::clone(&counter);
            let barrier = Arc::clone(&barrier);
            let iters = config.iters;
            let labels_per_thread = config.labels_per_thread;

            std::thread::Builder::new()
                .name(format!("worker-{t}"))
                .spawn(move || {
                    // Pre-generate label strings
                    let labels: Vec<String> = (0..labels_per_thread)
                        .map(|i| format!("t{t}_s{i}"))
                        .collect();

                    barrier.wait();

                    let mut handle_ops = 0u64;
                    let mut lookup_ops = 0u64;

                    for i in 0..iters {
                        let label_idx = (i as usize) % labels.len();
                        let label = &labels[label_idx];

                        if i % 3 == 0 {
                            // Use series handle (cached path)
                            let series = counter.series(&[("key", label)]);
                            series.inc();
                            handle_ops += 1;
                        } else {
                            // Direct label lookup (exercises cache + map)
                            counter.inc(&[("key", label)]);
                            lookup_ops += 1;
                        }
                    }

                    (handle_ops, lookup_ops)
                })
                .expect("spawn worker")
        })
        .collect();

    let mut total_handle_ops = 0u64;
    let mut total_lookup_ops = 0u64;

    for w in workers {
        let (h, l) = w.join().expect("worker panicked");
        total_handle_ops += h;
        total_lookup_ops += l;
    }

    let elapsed = start.elapsed();

    // Stop sweeper
    stop.store(true, Ordering::Relaxed);
    sweeper.join().expect("sweeper panicked");

    let total_ops = total_handle_ops + total_lookup_ops;
    let ops_per_sec = total_ops as f64 / elapsed.as_secs_f64();
    let evicted = total_evicted.load(Ordering::Relaxed);
    let sweeps = total_sweeps.load(Ordering::Relaxed);
    let final_cardinality = counter.cardinality();
    let overflow = counter.overflow_count();

    eprintln!();
    eprintln!("Results:");
    eprintln!("  elapsed:          {elapsed:.2?}");
    eprintln!("  total ops:        {total_ops}");
    eprintln!("  ops/sec:          {ops_per_sec:.0}");
    eprintln!("  handle ops:       {total_handle_ops}");
    eprintln!("  lookup ops:       {total_lookup_ops}");
    eprintln!("  sweeps:           {sweeps}");
    eprintln!("  total evicted:    {evicted}");
    eprintln!("  final cardinality: {final_cardinality}");
    eprintln!("  overflow count:   {overflow}");
    eprintln!("  sum_all:          {}", counter.sum_all());

    // Validation
    // Note: sum_all < total_ops is expected — evicted series lose their counts.
    // We validate that no counts appear from nowhere (sum_all <= total_ops).
    let actual_sum = counter.sum_all();
    assert!(
        actual_sum <= total_ops as isize,
        "sum_all={actual_sum} exceeds total_ops={total_ops} (phantom counts)"
    );

    if config.cap < total_possible_labels {
        assert!(
            overflow > 0,
            "Expected overflow with cap={} and {} possible labels, but got 0",
            config.cap, total_possible_labels
        );
        // Cardinality should be bounded: cap + bounded overshoot + overflow bucket
        let max_expected = config.cap + config.threads + 1;
        assert!(
            final_cardinality <= max_expected,
            "Cardinality {final_cardinality} exceeds bounded max {max_expected} (cap={}, threads={})",
            config.cap, config.threads
        );
    }

    eprintln!("  validation:       PASS");

    // Machine-readable output
    println!(
        "ops={total_ops},ops_per_sec={ops_per_sec:.0},elapsed_ms={},threads={},cap={},labels_per_thread={},cardinality={final_cardinality},overflow={overflow},evicted={evicted},sweeps={sweeps}",
        elapsed.as_millis(), config.threads, config.cap, config.labels_per_thread
    );
}
