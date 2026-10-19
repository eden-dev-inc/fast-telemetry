//! Isolate encoding and wire-body destruction; native payloads stay borrowed.
use std::hint::black_box;
use std::sync::Arc;

use fast_telemetry::otlp::{
    EncodedTraceRequest, build_resource, build_trace_export_request,
    encode_native_trace_export_request_batches, encode_native_trace_export_request_chunks,
};
use fast_telemetry::{
    CompletedSpan, SpanAttribute, SpanBatch, SpanCollector, SpanEvent, SpanId, SpanKind,
    SpanStatus, TraceId,
};
use prost::Message;

fn cpu_ns() -> u64 {
    let mut time = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: writable initialized timespec; the call retains no pointer.
    assert_eq!(
        unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &mut time) },
        0
    );
    time.tv_sec as u64 * 1_000_000_000 + time.tv_nsec as u64
}

fn fixture(workload: &str, count: usize) -> SpanBatch {
    let collector = Arc::new(SpanCollector::new(1, count));
    let sink = collector.span_sink(count);
    for index in 0..count {
        let span = CompletedSpan {
            trace_id: TraceId::random(),
            span_id: SpanId::random(),
            parent_span_id: SpanId::INVALID,
            name: if workload == "attributes" {
                format!("operation-{index}").into()
            } else {
                "operation".into()
            },
            kind: SpanKind::Server,
            start_time_ns: 1_000_000,
            end_time_ns: 2_000_000,
            status: if workload == "attributes" {
                SpanStatus::Error {
                    message: format!("export-error-{index}").into(),
                }
            } else {
                SpanStatus::Unset
            },
            attributes: if workload == "attributes" {
                ["a", "b", "c", "d"]
                    .into_iter()
                    .map(|key| SpanAttribute::new(key, format!("value-{index}-{key}")))
                    .collect()
            } else {
                Vec::new()
            },
            events: if workload == "attributes" {
                vec![SpanEvent {
                    name: "event".into(),
                    time_ns: 1_500_000,
                    attributes: vec![
                        SpanAttribute::new("text", format!("event-{index}")),
                        SpanAttribute::new("number", index as i64),
                    ],
                }]
            } else {
                Vec::new()
            },
        };
        assert!(sink.publish(span).is_ok());
    }
    let mut batch = SpanBatch::new();
    collector.drain_batches(&mut batch);
    assert_eq!(batch.len(), count);
    assert_eq!(sink.rejected_count(), 0);
    batch
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 4 {
        return Err("usage: single|chunks static|attributes SPANS CYCLES".into());
    }
    let mode = &args[0];
    let workload = &args[1];
    let count: usize = args[2].parse()?;
    let cycles: usize = args[3].parse()?;
    if !matches!(mode.as_str(), "single" | "chunks")
        || !matches!(workload.as_str(), "static" | "attributes")
        || count == 0
        || cycles == 0
    {
        return Err("invalid probe options".into());
    }
    let cap = if mode == "single" { usize::MAX } else { 512 };
    let batch = fixture(workload, count);
    let resource = build_resource("chunk-probe", &[("version", "1")]);
    let mut expected = Vec::new();
    let mut offset = 0;
    while offset < count {
        let n = cap.min(count - offset);
        expected.push(
            build_trace_export_request(
                &resource,
                "scope",
                batch
                    .iter()
                    .skip(offset)
                    .take(n)
                    .map(CompletedSpan::to_otlp)
                    .collect(),
            )
            .encode_to_vec(),
        );
        offset += n;
    }
    let mut requests: Vec<EncodedTraceRequest> = Vec::new();
    let mut elapsed = 0;
    let mut bytes = 0;
    let mut verified = 0;
    for cycle in 0..cycles + 8 {
        let start = cpu_ns();
        requests.reserve(count.div_ceil(cap));
        if mode == "single" {
            requests.push(encode_native_trace_export_request_batches(
                &resource,
                "scope",
                black_box(&batch),
                usize::MAX,
            ));
        } else {
            requests.extend(encode_native_trace_export_request_chunks(
                &resource,
                "scope",
                black_box(&batch),
                cap,
            ));
        }
        black_box(&requests);
        let mut duration = cpu_ns() - start;
        assert_eq!(requests.len(), expected.len());
        let mut spans = 0;
        let mut wire_bytes = 0;
        for (request, golden) in requests.iter().zip(&expected) {
            assert_eq!(request.as_bytes(), golden);
            spans += request.span_count() as usize;
            wire_bytes += request.as_bytes().len();
        }
        assert_eq!(spans, count);
        let cleanup = cpu_ns();
        requests.clear();
        duration += cpu_ns() - cleanup;
        if cycle >= 8 {
            elapsed += duration;
            bytes += wire_bytes;
            verified += spans;
        }
    }
    assert_eq!(batch.len(), count);
    println!(
        "mode,workload,batch,cycles,verified_spans,requests_per_cycle,wire_bytes,cpu_ns_per_span"
    );
    println!(
        "{mode},{workload},{count},{cycles},{verified},{},{bytes},{:.3}",
        expected.len(),
        elapsed as f64 / verified as f64
    );
    Ok(())
}
