use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use fast_telemetry::otlp::{
    build_export_request, build_log_export_request, build_resource, build_trace_export_request,
    encode_native_trace_export_request, pb,
};
use fast_telemetry_export::otlp::{OtlpHttpClient, OtlpHttpConfig, OtlpHttpErrorKind};
use fast_telemetry_export::spans::{SpanExportConfig, run as run_span_exporter};
use flate2::read::GzDecoder;
use prost::Message;
use tokio_util::sync::CancellationToken;

struct CapturedRequest {
    head: String,
    body: Vec<u8>,
}

fn collector(
    requests: usize,
) -> (
    String,
    mpsc::Receiver<CapturedRequest>,
    thread::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind collector");
    let address = listener.local_addr().expect("collector address");
    let (captured_tx, captured_rx) = mpsc::channel();
    let task = thread::spawn(move || {
        for _ in 0..requests {
            let (mut stream, _) = listener.accept().expect("accept request");
            captured_tx
                .send(read_request(&mut stream))
                .expect("capture request");
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/x-protobuf\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .expect("write response");
        }
    });
    (format!("http://{address}"), captured_rx, task)
}

fn collector_response(
    status: &str,
    body: Vec<u8>,
) -> (
    String,
    mpsc::Receiver<CapturedRequest>,
    thread::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind collector");
    let address = listener.local_addr().expect("collector address");
    let (captured_tx, captured_rx) = mpsc::channel();
    let status = status.to_string();
    let task = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept request");
        captured_tx
            .send(read_request(&mut stream))
            .expect("capture request");
        let head = format!(
            "HTTP/1.1 {status}\r\nContent-Type: application/x-protobuf\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        let _ = stream.write_all(head.as_bytes());
        let _ = stream.write_all(&body);
    });
    (format!("http://{address}"), captured_rx, task)
}

fn read_request(stream: &mut TcpStream) -> CapturedRequest {
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("read timeout");
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 4096];
    let header_end = loop {
        let read = stream.read(&mut buffer).expect("read headers");
        assert!(read > 0, "request closed before headers");
        bytes.extend_from_slice(&buffer[..read]);
        if let Some(position) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break position + 4;
        }
    };
    let head = String::from_utf8(bytes[..header_end].to_vec()).expect("ASCII headers");
    let content_length = head
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().expect("content length"))
        })
        .expect("content-length header");
    while bytes.len() < header_end + content_length {
        let read = stream.read(&mut buffer).expect("read body");
        assert!(read > 0, "request closed before body");
        bytes.extend_from_slice(&buffer[..read]);
    }
    CapturedRequest {
        head,
        body: bytes[header_end..header_end + content_length].to_vec(),
    }
}

fn decoded_body(request: &CapturedRequest) -> Vec<u8> {
    if !request
        .head
        .to_ascii_lowercase()
        .contains("content-encoding: gzip")
    {
        return request.body.clone();
    }
    let mut body = Vec::new();
    GzDecoder::new(request.body.as_slice())
        .read_to_end(&mut body)
        .expect("decode gzip request");
    body
}

#[tokio::test]
async fn exports_logs_metrics_and_traces_through_the_shared_client() {
    let (endpoint, captured, server) = collector(4);
    let client = OtlpHttpClient::new(
        OtlpHttpConfig::new(endpoint)
            .with_header("x-tenant", "tenant-a")
            .with_gzip_threshold(0),
    )
    .expect("HTTP client");
    let resource = build_resource("checkout", &[("service.instance.id", "checkout-1")]);

    let logs = build_log_export_request(
        &resource,
        "integration",
        vec![pb::LogRecord {
            body: Some(pb::AnyValue {
                value: Some(pb::any_value::Value::StringValue("ready".to_string())),
            }),
            ..Default::default()
        }],
    );
    let log_outcome = client.export_logs(&logs).await.expect("export logs");
    assert_eq!((log_outcome.accepted, log_outcome.rejected), (1, 0));

    let metrics = build_export_request(
        &resource,
        "integration",
        vec![pb::Metric {
            data: Some(pb::metric::Data::Gauge(pb::OtlpGauge {
                data_points: vec![pb::NumberDataPoint::default(); 2],
            })),
            ..Default::default()
        }],
    );
    let metric_outcome = client
        .export_metrics(&metrics)
        .await
        .expect("export metrics");
    assert_eq!((metric_outcome.accepted, metric_outcome.rejected), (2, 0));

    let traces =
        build_trace_export_request(&resource, "integration", vec![pb::OtlpSpan::default()]);
    let trace_outcome = client.export_traces(&traces).await.expect("export traces");
    assert_eq!((trace_outcome.accepted, trace_outcome.rejected), (1, 0));

    let spans = native_spans();
    let native = encode_native_trace_export_request(&resource, "integration", &spans);
    let expected = native.as_bytes().to_vec();
    let outcome = client
        .export_native_traces(native)
        .await
        .expect("export native traces");
    assert_eq!((outcome.accepted, outcome.rejected), (2, 0));

    let log_request = captured
        .recv_timeout(Duration::from_secs(1))
        .expect("log request");
    let metric_request = captured
        .recv_timeout(Duration::from_secs(1))
        .expect("metric request");
    let trace_request = captured
        .recv_timeout(Duration::from_secs(1))
        .expect("trace request");
    let native_request = captured
        .recv_timeout(Duration::from_secs(1))
        .expect("native trace request");
    assert!(native_request.head.starts_with("POST /v1/traces "));
    assert_eq!(decoded_body(&native_request), expected);
    for request in [
        &log_request,
        &metric_request,
        &trace_request,
        &native_request,
    ] {
        let head = request.head.to_ascii_lowercase();
        assert!(head.contains("x-tenant: tenant-a"));
        assert!(head.contains("content-encoding: gzip"));
        assert!(head.contains("user-agent: otel-otlp-exporter-rust/fast-telemetry-export-"));
    }
    assert!(log_request.head.starts_with("POST /v1/logs "));
    assert!(metric_request.head.starts_with("POST /v1/metrics "));
    assert!(trace_request.head.starts_with("POST /v1/traces "));
    pb::ExportLogsServiceRequest::decode(decoded_body(&log_request).as_slice())
        .expect("decode logs");
    pb::ExportMetricsServiceRequest::decode(decoded_body(&metric_request).as_slice())
        .expect("decode metrics");
    pb::ExportTraceServiceRequest::decode(decoded_body(&trace_request).as_slice())
        .expect("decode traces");

    server.join().expect("collector thread");
}

#[tokio::test]
async fn span_exporter_drains_sharded_sink_and_reopens_its_capacity() {
    use std::borrow::Cow;
    use std::sync::Arc;

    use fast_telemetry::{CompletedSpan, SpanCollector, SpanId, SpanKind, SpanStatus, TraceId};

    let (endpoint, captured, server) = collector(2);
    let span_collector = Arc::new(SpanCollector::new(1, 1));
    let sink = span_collector.span_sink(8);
    for _ in 0..2 {
        thread::scope(|scope| {
            for _ in 0..4 {
                let sink = sink.clone();
                scope.spawn(move || {
                    for _ in 0..2 {
                        assert!(
                            sink.publish(CompletedSpan {
                                trace_id: TraceId::random(),
                                span_id: SpanId::random(),
                                parent_span_id: SpanId::INVALID,
                                name: Cow::Borrowed("adapter span"),
                                kind: SpanKind::Internal,
                                start_time_ns: 1,
                                end_time_ns: 2,
                                status: SpanStatus::Unset,
                                attributes: Vec::new(),
                                events: Vec::new(),
                            })
                            .is_ok()
                        );
                    }
                });
            }
        });
        let cancel = CancellationToken::new();
        cancel.cancel();
        tokio::time::timeout(
            Duration::from_secs(5),
            run_span_exporter(
                Arc::clone(&span_collector),
                SpanExportConfig::new(&endpoint)
                    .with_interval(Duration::from_secs(60))
                    .with_max_batch_size(8),
                cancel,
            ),
        )
        .await
        .expect("final span export");
        assert!(span_collector.is_empty());
        let request = captured
            .recv_timeout(Duration::from_secs(1))
            .expect("trace request");
        assert!(request.head.starts_with("POST /v1/traces "));
        let traces = pb::ExportTraceServiceRequest::decode(decoded_body(&request).as_slice())
            .expect("decode traces");
        let spans: Vec<_> = traces
            .resource_spans
            .iter()
            .flat_map(|resource| &resource.scope_spans)
            .flat_map(|scope| &scope.spans)
            .collect();
        assert_eq!(spans.len(), 8);
        assert!(spans.iter().all(|span| span.name == "adapter span"));
    }
    assert_eq!(sink.accepted_count(), 16);
    assert_eq!(sink.rejected_count(), 0);
    server.join().expect("collector thread");
}

#[tokio::test]
async fn bounds_success_and_error_response_bodies() {
    let resource = build_resource("checkout", &[]);
    let logs = build_log_export_request(&resource, "integration", vec![pb::LogRecord::default()]);

    let (endpoint, captured, server) = collector_response("200 OK", vec![b'x'; 4 * 1024]);
    let client = OtlpHttpClient::new(OtlpHttpConfig::new(endpoint).with_max_response_bytes(128))
        .expect("HTTP client");
    let error = client
        .export_logs(&logs)
        .await
        .expect_err("oversized success response");
    assert_eq!(error.kind, OtlpHttpErrorKind::Decode);
    captured
        .recv_timeout(Duration::from_secs(1))
        .expect("captured success request");
    server.join().expect("collector thread");

    let (endpoint, captured, server) =
        collector_response("503 Service Unavailable", vec![b'y'; 4 * 1024]);
    let client = OtlpHttpClient::new(OtlpHttpConfig::new(endpoint).with_max_response_bytes(128))
        .expect("HTTP client");
    let error = client
        .export_logs(&logs)
        .await
        .expect_err("bounded retryable response");
    assert_eq!(error.kind, OtlpHttpErrorKind::RetryableStatus);
    assert!(error.message.len() <= 128 + '…'.len_utf8());
    assert!(error.message.ends_with('…'));
    captured
        .recv_timeout(Duration::from_secs(1))
        .expect("captured error request");
    server.join().expect("collector thread");
}

fn native_spans() -> Vec<fast_telemetry::CompletedSpan> {
    use fast_telemetry::{CompletedSpan, SpanId, SpanKind, SpanStatus, TraceId};
    (0..2)
        .map(|index| CompletedSpan {
            trace_id: TraceId::random(),
            span_id: SpanId::random(),
            parent_span_id: SpanId::INVALID,
            name: format!("native-{index}").into(),
            kind: SpanKind::Server,
            start_time_ns: 1,
            end_time_ns: 2,
            status: SpanStatus::Unset,
            attributes: Vec::new(),
            events: Vec::new(),
        })
        .collect()
}

#[tokio::test]
async fn native_trace_acknowledgements_use_the_encoded_count() {
    let resource = build_resource("native", &[]);
    for threshold in [0, usize::MAX] {
        for rejected in [-1_i64, 1, 99] {
            // Encode the response using the publicly re-exported response's
            // inferred partial-success type.
            let mut response = pb::ExportTraceServiceResponse::default();
            let partial = response.partial_success.get_or_insert_default();
            partial.rejected_spans = rejected;
            partial.error_message = "warning".into();
            let (endpoint, captured, server) =
                collector_response("200 OK", response.encode_to_vec());
            let client =
                OtlpHttpClient::new(OtlpHttpConfig::new(endpoint).with_gzip_threshold(threshold))
                    .expect("HTTP client");
            let spans = native_spans();
            let request = encode_native_trace_export_request(&resource, "scope", &spans);
            let expected = request.as_bytes().to_vec();
            let outcome = client
                .export_native_traces(request)
                .await
                .expect("native response");
            let expected_rejected = u64::try_from(rejected).unwrap_or(0).min(2);
            assert_eq!(outcome.accepted, 2 - expected_rejected);
            assert_eq!(outcome.rejected, expected_rejected);
            assert_eq!(outcome.message.as_deref(), Some("warning"));
            let captured = captured
                .recv_timeout(Duration::from_secs(1))
                .expect("native request");
            assert_eq!(decoded_body(&captured), expected);
            assert_eq!(
                captured
                    .head
                    .to_ascii_lowercase()
                    .contains("content-encoding: gzip"),
                threshold == 0
            );
            server.join().expect("collector thread");
        }
    }
}
