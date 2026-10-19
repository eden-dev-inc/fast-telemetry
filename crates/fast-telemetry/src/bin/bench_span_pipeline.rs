//! Combined native tracing pipeline benchmark; requires Linux CPU accounting.
//! Charges recording/context entry, required flush/drain, optional encoding and
//! actual native/protobuf/body cleanup. `record` skips request encoding. Setup,
//! warmup, semantic/wire verification, compression and network export are outside
//! the CPU intervals.

#[cfg(target_os = "linux")]
mod benchmark {
    use std::collections::HashSet;
    use std::hint::black_box;
    use std::sync::Arc;

    use fast_telemetry::otlp::{
        build_resource, build_trace_export_request, encode_native_trace_export_request_batches,
    };
    use fast_telemetry::{
        CompletedSpan, SpanAttribute, SpanBatch, SpanCollector, SpanId, SpanKind, SpanStatus,
        SpanValue,
    };
    use prost::Message;

    fn cpu_ns() -> u64 {
        let mut time = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // SAFETY: initialized writable timespec, no alias or retained pointer.
        assert_eq!(
            unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &mut time) },
            0
        );
        time.tv_sec as u64 * 1_000_000_000 + time.tv_nsec as u64
    }

    fn cycle(
        collector: &Arc<SpanCollector>,
        resource: &fast_telemetry::otlp::pb::Resource,
        spans: &mut SpanBatch,
        encoding: &str,
        workload: &str,
        batch: usize,
    ) -> (u64, usize) {
        let before = collector.recorded_count();
        let start = cpu_ns();
        black_box(&collector);
        for index in 0..batch {
            let mut root = if workload == "attributes" {
                collector.start_span(format!("operation-{index}"), SpanKind::Server)
            } else {
                collector.start_span("operation", SpanKind::Server)
            };
            root.enter();
            if workload == "attributes" {
                for key in ["a", "b", "c", "d"] {
                    root.set_attribute(key, format!("value-{index}-{key}"));
                }
                root.set_status(SpanStatus::Error {
                    message: format!("export-error-{index}").into(),
                });
                root.add_event(
                    "event",
                    vec![
                        SpanAttribute::new("text", format!("event-{index}")),
                        SpanAttribute::new("number", index as i64),
                    ],
                );
            } else if workload == "children" {
                for _ in 0..3 {
                    let mut child = root.child("child", SpanKind::Client);
                    child.enter();
                }
            }
        }
        collector.flush_local();
        collector.drain_batches(spans);
        let protobuf = if encoding == "protobuf" {
            Some(build_trace_export_request(
                resource,
                "scope",
                spans.iter().map(CompletedSpan::to_otlp).collect(),
            ))
        } else {
            None
        };
        let (body, span_count) = if encoding == "record" {
            (Vec::new(), spans.len() as u64)
        } else if let Some(request) = &protobuf {
            (request.encode_to_vec(), spans.len() as u64)
        } else {
            let request =
                encode_native_trace_export_request_batches(resource, "scope", spans, usize::MAX);
            let count = request.span_count();
            (request.into_body(), count)
        };
        black_box(&body);
        let mut elapsed = cpu_ns() - start;

        let per_trace = if workload == "children" { 4 } else { 1 };
        assert_eq!(spans.len(), batch * per_trace);
        assert_eq!(span_count, spans.len() as u64);
        assert!(fast_telemetry::current_trace_id().is_none());
        assert!(fast_telemetry::current_span_id().is_none());
        let mut span_ids = HashSet::new();
        let mut trace_ids = HashSet::new();
        assert_eq!(collector.recorded_count() - before, batch as u64);
        assert_eq!(collector.sampled_out_count(), 0);
        assert_eq!(collector.dropped_count(), 0);
        assert!(collector.is_empty());
        let borrowed = spans.iter().collect::<Vec<_>>();
        for group in borrowed.chunks_exact(per_trace) {
            let root = &group[per_trace - 1];
            assert_eq!(root.parent_span_id, SpanId::INVALID);
            assert_eq!(root.kind, SpanKind::Server);
            assert!(trace_ids.insert(root.trace_id), "duplicate root trace ID");
            if workload != "attributes" {
                assert_eq!(root.name, "operation");
            }
            for span in group {
                assert!(!span.trace_id.is_invalid() && !span.span_id.is_invalid());
                assert!(span_ids.insert(span.span_id), "duplicate span ID");
                assert!(span.start_time_ns > 0 && span.end_time_ns >= span.start_time_ns);
                assert_eq!(span.trace_id, root.trace_id);
                if workload != "attributes" {
                    assert!(span.attributes.is_empty() && span.events.is_empty());
                    assert!(matches!(span.status, SpanStatus::Unset));
                }
            }
            for child in &group[..per_trace - 1] {
                assert_eq!(child.parent_span_id, root.span_id);
                assert_eq!(child.name, "child");
                assert_eq!(child.kind, SpanKind::Client);
                assert_ne!(child.span_id, root.span_id);
                assert!(
                    child.start_time_ns >= root.start_time_ns
                        && child.end_time_ns <= root.end_time_ns
                );
            }
        }
        if workload == "attributes" {
            for (index, span) in spans.iter().enumerate() {
                assert_eq!(span.name, format!("operation-{index}"));
                assert_eq!(span.attributes.len(), 4);
                for (attribute, key) in span.attributes.iter().zip(["a", "b", "c", "d"]) {
                    assert_eq!(attribute.key, key);
                    let expected = format!("value-{index}-{key}");
                    assert!(
                        matches!(&attribute.value, SpanValue::String(value) if value.as_ref() == expected.as_str())
                    );
                }
                assert_eq!(span.events.len(), 1);
                assert_eq!(span.events[0].name, "event");
                assert!(
                    span.events[0].time_ns >= span.start_time_ns
                        && span.events[0].time_ns <= span.end_time_ns
                );
                assert_eq!(span.events[0].attributes.len(), 2);
                assert_eq!(span.events[0].attributes[0].key, "text");
                let expected = format!("event-{index}");
                assert!(
                    matches!(&span.events[0].attributes[0].value, SpanValue::String(value) if value.as_ref() == expected.as_str())
                );
                assert_eq!(span.events[0].attributes[1].key, "number");
                assert!(
                    matches!(span.events[0].attributes[1].value, SpanValue::I64(value) if value == index as i64)
                );
                assert!(
                    matches!(&span.status,SpanStatus::Error {message} if message==&format!("export-error-{index}"))
                );
            }
        }
        if encoding != "record" {
            let expected = build_trace_export_request(
                resource,
                "scope",
                spans.iter().map(CompletedSpan::to_otlp).collect(),
            )
            .encode_to_vec();
            assert_eq!(body, expected);
        }
        drop(borrowed);
        let bytes = body.len();
        let start = cpu_ns();
        spans.clear();
        drop(body);
        drop(protobuf);
        elapsed += cpu_ns() - start;
        (elapsed, bytes)
    }

    pub(super) fn run() -> Result<(), Box<dyn std::error::Error>> {
        let args: Vec<_> = std::env::args().collect();
        if args.len() != 5 {
            return Err(
                "usage: bench_span_pipeline record|native|protobuf root|attributes|children BATCH CYCLES"
                    .into(),
            );
        }
        let encoding = args[1].as_str();
        let workload = args[2].as_str();
        if !matches!(encoding, "record" | "native" | "protobuf") {
            return Err("unknown encoding".into());
        }
        if !matches!(workload, "root" | "attributes" | "children") {
            return Err("unknown pipeline workload".into());
        }
        let batch = args[3].parse::<usize>()?;
        let cycles = args[4].parse::<usize>()?;
        let per_trace = if workload == "children" { 4 } else { 1 };
        let burst = batch
            .checked_mul(per_trace)
            .ok_or("span burst overflows usize")?;
        let traces = batch
            .checked_mul(cycles)
            .ok_or("trace count overflows usize")?;
        let span_total = traces
            .checked_mul(per_trace)
            .ok_or("span count overflows usize")?;
        if batch == 0 || cycles == 0 || burst > 1024 {
            return Err("batch/cycles must be positive and each burst must fit the 1024-span native retention limit".into());
        }
        let collector = Arc::new(SpanCollector::new(1, 4096));
        let resource = build_resource("native-pipeline-benchmark", &[("version", "1")]);
        let mut spans = SpanBatch::new();
        for _ in 0..4 {
            cycle(&collector, &resource, &mut spans, encoding, workload, batch);
        }
        let mut elapsed = 0;
        let mut bytes = 0;
        for _ in 0..cycles {
            let result = cycle(&collector, &resource, &mut spans, encoding, workload, batch);
            elapsed += result.0;
            bytes += result.1;
        }
        println!(
            "encoding,workload,batch,cycles,verified_traces,verified_spans,sampled_out,dropped,wire_bytes,cpu_ns_per_trace,cpu_ns_per_span"
        );
        println!(
            "{encoding},{workload},{batch},{cycles},{traces},{span_total},0,0,{bytes},{:.3},{:.3}",
            elapsed as f64 / traces as f64,
            elapsed as f64 / span_total as f64
        );
        Ok(())
    }
}

#[cfg(target_os = "linux")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    benchmark::run()
}

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("bench_span_pipeline requires Linux process CPU accounting");
    std::process::exit(2);
}
