//! Matched prepared spans through each production OTLP/HTTP exporter.
use std::sync::Arc;
use std::time::{Duration, Instant, UNIX_EPOCH};

use fast_telemetry::otlp::{
    build_resource, build_trace_export_request, encode_native_trace_export_request_chunks, pb,
};
use fast_telemetry::{
    CompletedSpan, SpanAttribute, SpanBatch, SpanCollector, SpanEvent, SpanId, SpanKind,
    SpanStatus, TraceId,
};
use opentelemetry::trace::{Event, SpanContext, TraceFlags, TraceState};
use opentelemetry::{InstrumentationScope, KeyValue};
use opentelemetry_otlp::{WithExportConfig, WithHttpConfig};
use opentelemetry_proto::transform::trace::tonic::group_spans_by_resource_and_scope;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::{SpanData, SpanEvents, SpanExporter as _};
use prost::Message;

mod receiver;
#[allow(dead_code)]
#[rustfmt::skip]
#[path = "../../span-export-tradeoff/src/transport.rs"]
mod transport;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

const SERVICE: &str = "http-head-to-head";
const SCOPE: &str = "scope";
const CAPS: [usize; 3] = [512, 2048, usize::MAX];
const WARMUPS: usize = 8;

fn cpu_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: initialized writable timespec; no pointer is retained.
    assert_eq!(
        unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) },
        0
    );
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

fn native_span(attributed: bool, index: usize) -> CompletedSpan {
    CompletedSpan {
        trace_id: TraceId::from_hex(&format!("{:032x}", index + 1)).expect("trace ID"),
        span_id: SpanId::from_hex(&format!("{:016x}", index + 1)).expect("span ID"),
        parent_span_id: SpanId::INVALID,
        name: if attributed {
            format!("operation-{index}").into()
        } else {
            "operation".into()
        },
        kind: SpanKind::Server,
        start_time_ns: 1_000_000,
        end_time_ns: 2_000_000,
        status: if attributed {
            SpanStatus::Error {
                message: format!("export-error-{index}").into(),
            }
        } else {
            SpanStatus::Unset
        },
        attributes: if attributed {
            ["a", "b", "c", "d"]
                .into_iter()
                .map(|key| SpanAttribute::new(key, format!("value-{index}-{key}")))
                .collect()
        } else {
            Vec::new()
        },
        events: if attributed {
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
    }
}

fn sdk_span(attributed: bool, index: usize, scope: &InstrumentationScope) -> SpanData {
    let mut events = SpanEvents::default();
    if attributed {
        events.events.push(Event::new(
            "event",
            UNIX_EPOCH + Duration::from_nanos(1_500_000),
            vec![
                KeyValue::new("text", format!("event-{index}")),
                KeyValue::new("number", index as i64),
            ],
            0,
        ));
    }
    SpanData {
        span_context: SpanContext::new(
            opentelemetry::trace::TraceId::from_bytes(((index + 1) as u128).to_be_bytes()),
            opentelemetry::trace::SpanId::from_bytes(((index + 1) as u64).to_be_bytes()),
            TraceFlags::SAMPLED,
            false,
            TraceState::NONE,
        ),
        parent_span_id: opentelemetry::trace::SpanId::INVALID,
        span_kind: opentelemetry::trace::SpanKind::Server,
        name: if attributed {
            format!("operation-{index}").into()
        } else {
            "operation".into()
        },
        start_time: UNIX_EPOCH + Duration::from_nanos(1_000_000),
        end_time: UNIX_EPOCH + Duration::from_nanos(2_000_000),
        attributes: if attributed {
            ["a", "b", "c", "d"]
                .into_iter()
                .map(|key| KeyValue::new(key, format!("value-{index}-{key}")))
                .collect()
        } else {
            Vec::new()
        },
        dropped_attributes_count: 0,
        events,
        links: Default::default(),
        status: if attributed {
            opentelemetry::trace::Status::error(format!("export-error-{index}"))
        } else {
            opentelemetry::trace::Status::Unset
        },
        instrumentation_scope: scope.clone(),
    }
}

fn sdk_chunks(
    attributed: bool,
    count: usize,
    cap: usize,
    scope: &InstrumentationScope,
) -> Vec<Vec<SpanData>> {
    (0..count)
        .step_by(cap.min(count))
        .map(|offset| {
            (offset..(offset + cap.min(count - offset)))
                .map(|index| sdk_span(attributed, index, scope))
                .collect()
        })
        .collect()
}

fn verify_equivalent(native: &[u8], sdk: &[u8], attributed: bool) -> Result<()> {
    let expected = pb::ExportTraceServiceRequest::decode(native)?;
    let mut actual = pb::ExportTraceServiceRequest::decode(sdk)?;
    assert_eq!(expected.resource_spans.len(), 1);
    assert_eq!(actual.resource_spans.len(), 1);
    let spans = &mut actual.resource_spans[0].scope_spans[0].spans;
    for span in spans {
        // Preserve each production encoder's metadata in transmitted bytes.
        // Normalize only the known sampled flag and explicit unset status here.
        assert_eq!(span.flags, 1);
        span.flags = 0;
        if !attributed {
            let status = span.status.take().expect("SDK explicit unset status");
            assert_eq!(status.code, 0);
            assert!(status.message.is_empty());
        }
    }
    assert_eq!(actual, expected, "complete payload and identity parity");
    Ok(())
}

async fn run() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 6 {
        return Err(
            "usage: native|otel static|attributes SPANS CAP|all CYCLES ACK_DELAY_US".into(),
        );
    }
    let mode = &args[0];
    let workload = &args[1];
    let count: usize = args[2].parse()?;
    let cap = if args[3] == "all" {
        usize::MAX
    } else {
        args[3].parse()?
    };
    let cycles: usize = args[4].parse()?;
    let delay: u64 = args[5].parse()?;
    if !matches!(mode.as_str(), "native" | "otel")
        || !matches!(workload.as_str(), "static" | "attributes")
        || count == 0
        || count > 8192
        || !CAPS.contains(&cap)
        || cycles == 0
        || cycles > 1024
        || delay > 10_000
        || count.checked_mul(cycles).is_none()
    {
        return Err("invalid probe options".into());
    }
    let attributed = workload == "attributes";
    let scope = InstrumentationScope::builder(SCOPE).build();
    let resource = build_resource(SERVICE, &[]);
    let sdk_resource = Resource::builder_empty()
        .with_attribute(KeyValue::new("service.name", SERVICE))
        .build();
    let collector = Arc::new(SpanCollector::new(1, count));
    let sink = collector.span_sink(count);
    let mut batch = SpanBatch::new();
    for index in 0..count {
        assert!(sink.publish(native_span(attributed, index)).is_ok());
    }
    collector.drain_batches(&mut batch);
    assert_eq!(batch.len(), count);

    // Every process constructs and retains both exporters' goldens, for all
    // request caps, in the same order. This avoids strategy-specific setup.
    let mut goldens = Vec::new();
    for limit in CAPS {
        let native = (0..count)
            .step_by(limit.min(count))
            .map(|offset| {
                build_trace_export_request(
                    &resource,
                    SCOPE,
                    batch
                        .iter()
                        .skip(offset)
                        .take(limit.min(count - offset))
                        .map(CompletedSpan::to_otlp)
                        .collect(),
                )
                .encode_to_vec()
            })
            .collect::<Vec<_>>();
        let sdk = sdk_chunks(attributed, count, limit, &scope)
            .into_iter()
            .map(|spans| {
                opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest {
                    resource_spans: group_spans_by_resource_and_scope(
                        spans,
                        &(&sdk_resource).into(),
                    ),
                }
                .encode_to_vec()
            })
            .collect::<Vec<_>>();
        assert_eq!(native.len(), sdk.len());
        for (left, right) in native.iter().zip(&sdk) {
            verify_equivalent(left, right, attributed)?;
        }
        goldens.push((native, sdk));
    }
    batch.clear();
    let selected = &goldens[CAPS.iter().position(|&c| c == cap).expect("cap")];
    let expected = if mode == "native" {
        &selected.0
    } else {
        &selected.1
    };
    let server = receiver::Server::start(cycles + WARMUPS, delay, expected)?;

    // Both real clients are constructed in every process; only the selected one
    // sends traffic. The official opentelemetry-http reqwest adapter is used.
    let native = transport::OtlpHttpClient::new(
        transport::OtlpHttpConfig::new(&server.endpoint).with_gzip_threshold(usize::MAX),
    )?;
    let mut otel = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(format!("{}/v1/traces", server.endpoint))
        .with_protocol(opentelemetry_otlp::Protocol::HttpBinary)
        .with_http_client(
            reqwest12::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()?,
        )
        .build()?;
    otel.set_resource(&sdk_resource);
    let mut requests = Vec::new();
    let (mut total_cpu, mut total_wall) = (0, 0);
    for cycle in 0..cycles + WARMUPS {
        // Generate both representations outside timing, in the same order for
        // both modes. Keep unused input alive until after the timed interval.
        for index in 0..count {
            assert!(sink.publish(native_span(attributed, index)).is_ok());
        }
        collector.drain_batches(&mut batch);
        assert_eq!(batch.len(), count);
        let mut sdk = sdk_chunks(attributed, count, cap, &scope);
        let wall_start = Instant::now();
        let cpu_start = cpu_ns();
        if mode == "native" {
            requests.extend(encode_native_trace_export_request_chunks(
                &resource, SCOPE, &batch, cap,
            ));
            // Same order as the actual native exporter: encode, release native
            // payloads, then sequentially send all requests.
            batch.clear();
            let mut accepted = 0;
            for request in requests.drain(..) {
                let outcome = native.export_native_traces(request).await?;
                assert_eq!(outcome.rejected, 0);
                accepted += outcome.accepted;
            }
            assert_eq!(accepted, count as u64);
        } else {
            for spans in sdk.drain(..) {
                otel.export(spans).await?;
            }
        }
        let cpu = cpu_ns() - cpu_start;
        let wall = wall_start.elapsed().as_nanos();
        if cycle >= WARMUPS {
            total_cpu += cpu;
            total_wall += wall;
        }
        // Unselected input destruction is excluded. Selected native and SDK
        // payload destruction already occurred inside their respective paths.
        batch.clear();
        drop(sdk);
    }
    assert_eq!(sink.rejected_count(), 0);
    let wire = server.finish((cycles + WARMUPS) * expected.len())?;
    let protobuf: usize = expected.iter().map(Vec::len).sum();
    assert_eq!(wire, protobuf * cycles);
    let verified = count * cycles;
    println!(
        "mode,workload,batch,cap,cycles,ack_delay_us,verified_spans,requests_per_cycle,protobuf_bytes_per_cycle,wire_bytes,client_cpu_ns_per_span,wall_ns_per_span"
    );
    println!(
        "{mode},{workload},{count},{cap},{cycles},{delay},{verified},{},{protobuf},{wire},{:.3},{:.3}",
        expected.len(),
        total_cpu as f64 / verified as f64,
        total_wall as f64 / verified as f64
    );
    Ok(())
}

fn main() -> Result<()> {
    if std::env::args().nth(1).as_deref() == Some("serve") {
        return receiver::serve();
    }
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(run())
}
