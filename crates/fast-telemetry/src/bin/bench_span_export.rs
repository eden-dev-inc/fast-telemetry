//! Native OTLP conversion/encoding benchmark; requires Linux CPU accounting.
//! Native fixture creation and byte verification are outside timing. Conversion,
//! protobuf encoding and actual native/protobuf payload destruction are charged.

#[cfg(target_os = "linux")]
mod benchmark {
    use std::hint::black_box;

    use fast_telemetry::otlp::{
        build_resource, build_trace_export_request, encode_native_trace_export_request, pb,
    };
    use fast_telemetry::{
        CompletedSpan, SpanAttribute, SpanEvent, SpanId, SpanKind, SpanStatus, TraceId,
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

    fn fixture(workload: &str, count: usize) -> Vec<CompletedSpan> {
        (0..count)
            .map(|index| CompletedSpan {
                trace_id: TraceId::random(),
                span_id: SpanId::random(),
                parent_span_id: SpanId::INVALID,
                name: if workload == "owned" {
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
            })
            .collect()
    }

    fn cycle(mode: &str, workload: &str, count: usize) -> (u64, usize) {
        let resource = build_resource("export-benchmark", &[("version", "1")]);
        let mut spans = fixture(workload, count);
        let expected = build_trace_export_request(
            &resource,
            "scope",
            spans.iter().map(CompletedSpan::to_otlp).collect(),
        )
        .encode_to_vec();
        let pointer = spans.as_ptr();
        let capacity = spans.capacity();
        let start = cpu_ns();
        black_box(&spans);
        let request = if mode == "native" {
            None
        } else {
            let converted = if mode == "consume" {
                spans.drain(..).map(CompletedSpan::into_otlp).collect()
            } else {
                spans.iter().map(CompletedSpan::to_otlp).collect()
            };
            Some(build_trace_export_request(&resource, "scope", converted))
        };
        let (body, encoded_count) = if let Some(request) = &request {
            (request.encode_to_vec(), count as u64)
        } else {
            let encoded = encode_native_trace_export_request(&resource, "scope", &spans);
            let encoded_count = encoded.span_count();
            (encoded.into_body(), encoded_count)
        };
        black_box(&body);
        let mut elapsed = cpu_ns() - start;
        assert_eq!(encoded_count, count as u64);
        assert_eq!(body, expected);
        let decoded =
            pb::ExportTraceServiceRequest::decode(body.as_slice()).expect("decode exported batch");
        assert_eq!(decoded.resource_spans.len(), 1);
        assert_eq!(decoded.resource_spans[0].scope_spans.len(), 1);
        assert_eq!(decoded.resource_spans[0].scope_spans[0].spans.len(), count);
        assert_eq!(decoded.encode_to_vec(), expected);
        assert_eq!(spans.as_ptr(), pointer);
        assert_eq!(spans.capacity(), capacity);
        if mode == "consume" {
            assert!(spans.is_empty());
        }
        let bytes = body.len();
        let start = cpu_ns();
        drop(body);
        drop(request);
        spans.clear(); // Charge original payload cleanup in all paths.
        elapsed += cpu_ns() - start;
        (elapsed, bytes)
    }

    pub(super) fn run() -> Result<(), Box<dyn std::error::Error>> {
        let args: Vec<_> = std::env::args().collect();
        if args.len() != 5 {
            return Err(
                "usage: bench_span_export borrow|consume|native static|owned|attributes BATCH CYCLES"
                    .into(),
            );
        }
        let mode = args[1].as_str();
        let workload = args[2].as_str();
        let batch = args[3].parse::<usize>()?;
        let cycles = args[4].parse::<usize>()?;
        if !matches!(mode, "borrow" | "consume" | "native") {
            return Err("mode must be borrow, consume or native".into());
        }
        if !matches!(workload, "static" | "owned" | "attributes") {
            return Err("unknown export workload".into());
        }
        if batch == 0 || cycles == 0 || batch.checked_mul(cycles).is_none() {
            return Err(
                "batch and cycles must be positive and their product must fit usize".into(),
            );
        }
        for _ in 0..4 {
            cycle(mode, workload, batch);
        }
        let mut elapsed = 0;
        let mut bytes = 0;
        for _ in 0..cycles {
            let result = cycle(mode, workload, batch);
            elapsed += result.0;
            bytes += result.1;
        }
        println!("mode,workload,batch,cycles,verified_spans,wire_bytes,cpu_ns_per_span");
        println!(
            "{mode},{workload},{batch},{cycles},{},{bytes},{:.3}",
            batch * cycles,
            elapsed as f64 / (batch * cycles) as f64
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
    eprintln!("bench_span_export requires Linux process CPU accounting");
    std::process::exit(2);
}
