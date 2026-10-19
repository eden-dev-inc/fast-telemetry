//! Native propagation benchmark with verified headers and retained spans.
//! Requires Linux process CPU accounting. Verification is outside timing;
//! required native flush, drain and actual payload/header destruction are charged.

#[cfg(target_os = "linux")]
mod benchmark {
    use std::fmt::Write;
    use std::hint::black_box;
    use std::sync::Arc;

    use fast_telemetry::{CompletedSpan, SpanCollector, SpanContext, SpanId, SpanKind, TraceId};

    const BATCH: usize = 256;

    fn cpu_ns() -> u64 {
        let mut time = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // SAFETY: initialized writable timespec, no retained pointer or alias.
        assert_eq!(
            unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &mut time) },
            0
        );
        time.tv_sec as u64 * 1_000_000_000 + time.tv_nsec as u64
    }

    fn fixture() -> SpanContext {
        SpanContext {
            trace_id: TraceId::from_hex("4bf92f3577b34da6a3ce929d0e0e4736").expect("fixture trace"),
            span_id: SpanId::from_hex("00f067aa0ba902b7").expect("fixture span"),
            trace_flags: 0xa7,
        }
    }

    fn reference(context: SpanContext) -> String {
        let trace: String = context
            .trace_id
            .as_bytes()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        let span: String = context
            .span_id
            .as_bytes()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        format!("00-{trace}-{span}-{:02x}", context.trace_flags)
    }

    fn parsing(iterations: usize) -> u64 {
        let contexts: Vec<_> = (0..64)
            .map(|index| SpanContext {
                trace_id: TraceId::random(),
                span_id: SpanId::random(),
                trace_flags: index as u8 ^ 0xa7,
            })
            .collect();
        let headers: Vec<_> = contexts
            .iter()
            .enumerate()
            .map(|(index, &context)| {
                let mut header = reference(context);
                if index % 2 != 0 {
                    header.make_ascii_uppercase();
                }
                header
            })
            .collect();
        for (header, expected) in headers.iter().zip(&contexts) {
            let actual = SpanContext::from_traceparent(header).expect("valid parse fixture");
            assert_eq!(
                (actual.trace_id, actual.span_id, actual.trace_flags),
                (expected.trace_id, expected.span_id, expected.trace_flags)
            );
        }
        let mut last = None;
        let start = cpu_ns();
        for index in 0..iterations {
            last = black_box(SpanContext::from_traceparent(black_box(
                headers[index & 63].as_str(),
            )));
        }
        let elapsed = cpu_ns() - start;
        let actual = last.expect("parsed header");
        let expected = contexts[(iterations - 1) & 63];
        assert_eq!(
            (actual.trace_id, actual.span_id, actual.trace_flags),
            (expected.trace_id, expected.span_id, expected.trace_flags)
        );
        elapsed
    }

    fn headers(iterations: usize) -> u64 {
        let context = fixture();
        let expected = reference(context);
        let mut values = Vec::with_capacity(BATCH);
        let mut elapsed = 0;
        let mut remaining = iterations;
        while remaining > 0 {
            let count = remaining.min(BATCH);
            let start = cpu_ns();
            for _ in 0..count {
                values.push(black_box(context).to_traceparent());
            }
            elapsed += cpu_ns() - start;
            assert!(values.iter().all(|value| value == &expected));
            let start = cpu_ns();
            values.clear(); // Actual header destruction is charged.
            elapsed += cpu_ns() - start;
            remaining -= count;
        }
        elapsed
    }

    fn display(iterations: usize, trace: bool) -> u64 {
        let context = fixture();
        let expected: String = if trace {
            context.trace_id.as_bytes().as_slice()
        } else {
            context.span_id.as_bytes().as_slice()
        }
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
        let mut value = String::with_capacity(64);
        let start = cpu_ns();
        for _ in 0..iterations {
            value.clear();
            if trace {
                write!(value, "{}", black_box(context.trace_id)).expect("format trace");
            } else {
                write!(value, "{}", black_box(context.span_id)).expect("format span");
            }
            black_box(value.as_str());
        }
        let elapsed = cpu_ns() - start;
        assert_eq!(value, expected);
        elapsed
    }

    fn spans(collector: &Arc<SpanCollector>, iterations: usize, remote: bool) -> u64 {
        let context = fixture();
        let incoming = reference(context);
        let mut headers = Vec::with_capacity(BATCH);
        let mut completed: Vec<CompletedSpan> = Vec::with_capacity(BATCH);
        let initial_count = collector.recorded_count();
        let mut remaining = iterations;
        let mut elapsed = 0;
        while remaining > 0 {
            let count = remaining.min(BATCH);
            let start = cpu_ns();
            for _ in 0..count {
                let mut span = if remote {
                    collector.start_span_from_traceparent(
                        Some(black_box(&incoming)),
                        "propagation",
                        SpanKind::Server,
                    )
                } else {
                    collector.start_span("propagation", SpanKind::Server)
                };
                let span = span.enter();
                headers.push(span.traceparent());
            }
            collector.flush_local();
            collector.drain_into(&mut completed);
            elapsed += cpu_ns() - start;
            assert_eq!(completed.len(), count);
            assert_eq!(headers.len(), count);
            for (span, header) in completed.iter().zip(&headers) {
                let decoded = SpanContext::from_traceparent(header).expect("valid emitted header");
                assert_eq!(decoded.trace_id, span.trace_id);
                assert_eq!(decoded.span_id, span.span_id);
                assert_eq!(decoded.trace_flags, 1);
                assert!(!span.trace_id.is_invalid() && !span.span_id.is_invalid());
                assert!(span.end_time_ns >= span.start_time_ns && span.start_time_ns != 0);
                assert_eq!(span.name, "propagation");
                assert!(span.attributes.is_empty() && span.events.is_empty());
                assert_eq!(
                    span.parent_span_id,
                    if remote {
                        context.span_id
                    } else {
                        SpanId::INVALID
                    }
                );
                if remote {
                    assert_eq!(span.trace_id, context.trace_id);
                }
            }
            let start = cpu_ns();
            headers.clear();
            completed.clear(); // Actual header and span destruction are charged.
            elapsed += cpu_ns() - start;
            remaining -= count;
        }
        assert_eq!(
            collector.recorded_count(),
            initial_count + iterations as u64
        );
        assert_eq!(collector.sampled_out_count(), 0);
        assert_eq!(collector.dropped_count(), 0);
        assert!(collector.is_empty());
        elapsed
    }

    pub(super) fn run() -> Result<(), Box<dyn std::error::Error>> {
        let args: Vec<_> = std::env::args().collect();
        if args.len() != 3 {
            return Err("usage: bench_span_propagation MODE ITERATIONS".into());
        }
        let iterations = args[2].parse::<usize>()?;
        if iterations == 0 {
            return Err("iterations must be positive".into());
        }
        let mode = args[1].as_str();
        let (elapsed, retained) = match mode {
            "parse" => {
                parsing(1024);
                (parsing(iterations), 0)
            }
            "header" => {
                headers(1024);
                (headers(iterations), 0)
            }
            "trace_display" => {
                display(1024, true);
                (display(iterations, true), 0)
            }
            "span_display" => {
                display(1024, false);
                (display(iterations, false), 0)
            }
            "root_header" | "remote_header" => {
                let collector = Arc::new(SpanCollector::new(1, 64));
                let remote = mode == "remote_header";
                spans(&collector, 1024, remote);
                (spans(&collector, iterations, remote), iterations)
            }
            _ => return Err(
                "mode must be parse, header, trace_display, span_display, root_header or remote_header"
                    .into(),
            ),
        };
        println!("mode,iterations,cpu_ns_per_operation,retained_spans,sampled_out,dropped");
        println!(
            "{mode},{iterations},{:.3},{retained},0,0",
            elapsed as f64 / iterations as f64
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
    eprintln!("bench_span_propagation requires Linux process CPU accounting");
    std::process::exit(2);
}
