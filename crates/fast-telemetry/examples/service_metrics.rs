//! Realistic service metrics example.
//!
//! Shows a pattern for defining metrics that might be used in a production service,
//! with both high-frequency unlabeled counters and dimensional breakdowns.
//!
//! Run with: cargo run --example service_metrics

use fast_telemetry::{
    Counter, DeriveLabel, ExportMetrics, Gauge, LabelEnum, LabeledCounter, LabeledHistogram,
};
use std::sync::Arc;
use std::thread;

// Label for Redis command types
#[derive(Copy, Clone, Debug, DeriveLabel)]
#[label_name = "command"]
enum RedisCommand {
    Get,
    Set,
    Del,
    Mget,
    Mset,
    #[label = "other"]
    Unknown,
}

// Label for error categories
#[derive(Copy, Clone, Debug, DeriveLabel)]
#[label_name = "error"]
enum ErrorType {
    Timeout,
    ConnectionRefused,
    InvalidResponse,
    #[label = "other"]
    Unknown,
}

#[derive(ExportMetrics)]
#[metric_prefix = "proxy"]
struct ProxyMetrics {
    // === Hot path metrics (unlabeled for max speed) ===
    #[help = "Total commands processed"]
    commands_total: Counter,

    #[help = "Total bytes read from clients"]
    bytes_read: Counter,

    #[help = "Total bytes written to clients"]
    bytes_written: Counter,

    // === Dimensional breakdowns (still fast, O(1) lookup) ===
    #[help = "Commands by type"]
    commands_by_type: LabeledCounter<RedisCommand>,

    #[help = "Command latency by type (microseconds)"]
    latency_by_command: LabeledHistogram<RedisCommand>,

    #[help = "Errors by type"]
    errors: LabeledCounter<ErrorType>,

    // === Point-in-time gauges ===
    #[help = "Current active connections"]
    active_connections: Gauge,

    #[help = "Current memory usage in bytes"]
    memory_bytes: Gauge,
}

impl ProxyMetrics {
    fn new(shard_count: usize) -> Self {
        Self {
            commands_total: Counter::new(shard_count),
            bytes_read: Counter::new(shard_count),
            bytes_written: Counter::new(shard_count),
            commands_by_type: LabeledCounter::new(shard_count),
            latency_by_command: LabeledHistogram::with_latency_buckets(shard_count),
            errors: LabeledCounter::new(shard_count),
            active_connections: Gauge::new(),
            memory_bytes: Gauge::new(),
        }
    }
}

fn main() {
    // Shared metrics across threads
    let metrics = Arc::new(ProxyMetrics::new(8));

    // Spawn worker threads that increment metrics
    let mut handles = vec![];
    for thread_id in 0..4 {
        let m = Arc::clone(&metrics);
        handles.push(thread::spawn(move || {
            for i in 0..10_000 {
                // Hot path: just increment
                m.commands_total.inc();
                m.bytes_read.add(64);
                m.bytes_written.add(128);

                // Dimensional: still O(1)
                let cmd = match i % 5 {
                    0 => RedisCommand::Get,
                    1 => RedisCommand::Set,
                    2 => RedisCommand::Mget,
                    3 => RedisCommand::Del,
                    _ => RedisCommand::Unknown,
                };
                m.commands_by_type.inc(cmd);
                m.latency_by_command.record(cmd, 50 + (i % 100) as u64);

                // Occasional error
                if i % 1000 == 0 {
                    m.errors.inc(ErrorType::Timeout);
                }
            }
            println!("Thread {} done", thread_id);
        }));
    }

    // Wait for workers
    for h in handles {
        h.join().unwrap();
    }

    // Update gauges (typically done periodically)
    metrics.active_connections.set(42);
    metrics.memory_bytes.set(1024 * 1024 * 256); // 256MB

    // Export
    println!("\n=== Prometheus Export ===\n");
    let mut output = String::new();
    metrics.export_prometheus(&mut output);
    println!("{}", output);

    // In production, you'd send this to Datadog Agent via UDP:
    // let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
    // let mut statsd = String::new();
    // metrics.export_dogstatsd(&mut statsd, &[("env", "prod")]);
    // socket.send_to(statsd.as_bytes(), "127.0.0.1:8125").unwrap();
}
