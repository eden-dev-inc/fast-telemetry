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

pub(crate) fn fixture(workload: &str, count: usize) -> SpanBatch {
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
    if !(4..=6).contains(&args.len()) {
        return Err(
            "usage: single|chunks static|attributes SPANS CYCLES [CAP] [original|matched]".into(),
        );
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
    let cap = if mode == "single" {
        usize::MAX
    } else {
        args.get(4)
            .map(|s| s.parse::<usize>())
            .transpose()?
            .unwrap_or(512)
    };
    let setup = args.get(5).map(String::as_str).unwrap_or("matched");
    if cap == 0
        || !matches!(setup, "original" | "matched")
        || cycles.checked_add(8).is_none()
        || count.checked_mul(cycles).is_none()
    {
        return Err("invalid cap/setup or overflowing sample count".into());
    }
    let batch = fixture(workload, count);
    let resource = build_resource("chunk-probe", &[("version", "1")]);
    let caps = if setup == "original" {
        vec![cap]
    } else {
        vec![usize::MAX, 256, 512, 1024, 2048]
    };
    if !caps.contains(&cap) {
        return Err("matched setup supports caps 256/512/1024/2048 or single".into());
    }
    // Every matched process creates and retains the same golden buffers in the
    // same order, irrespective of the selected encoding strategy.
    let golden_sets = caps
        .iter()
        .map(|limit| {
            let mut expected = Vec::new();
            let mut offset = 0;
            while offset < count {
                let n = (*limit).min(count - offset);
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
            expected
        })
        .collect::<Vec<_>>();
    let expected = &golden_sets[caps
        .iter()
        .position(|&limit| limit == cap)
        .expect("validated cap")];
    let mut requests: Vec<EncodedTraceRequest> = Vec::new();
    let mut elapsed = 0;
    let mut encoding = 0;
    let mut cleanup_time = 0;
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
        let duration = cpu_ns() - start;
        assert_eq!(requests.len(), expected.len());
        let mut spans = 0;
        let mut wire_bytes = 0;
        for (request, golden) in requests.iter().zip(expected) {
            assert_eq!(request.as_bytes(), golden);
            spans += request.span_count() as usize;
            wire_bytes += request.as_bytes().len();
        }
        assert_eq!(spans, count);
        let cleanup = cpu_ns();
        requests.clear();
        let cleanup_duration = cpu_ns() - cleanup;
        if cycle >= 8 {
            elapsed += duration + cleanup_duration;
            encoding += duration;
            cleanup_time += cleanup_duration;
            bytes += wire_bytes;
            verified += spans;
        }
    }
    assert_eq!(batch.len(), count);
    println!(
        "mode,workload,batch,cap,setup,cycles,verified_spans,requests_per_cycle,wire_bytes,cpu_ns_per_span,encode_cpu_ns_per_span,cleanup_cpu_ns_per_span"
    );
    println!(
        "{mode},{workload},{count},{cap},{setup},{cycles},{verified},{},{bytes},{:.3},{:.3},{:.3}",
        expected.len(),
        elapsed as f64 / verified as f64,
        encoding as f64 / verified as f64,
        cleanup_time as f64 / verified as f64
    );
    Ok(())
}
