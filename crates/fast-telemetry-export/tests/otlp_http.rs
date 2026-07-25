use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use fast_telemetry::otlp::{
    build_export_request, build_log_export_request, build_resource, build_trace_export_request, pb,
};
use fast_telemetry_export::otlp::{OtlpHttpClient, OtlpHttpConfig};
use flate2::read::GzDecoder;
use prost::Message;

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
    let (endpoint, captured, server) = collector(3);
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

    let log_request = captured
        .recv_timeout(Duration::from_secs(1))
        .expect("log request");
    let metric_request = captured
        .recv_timeout(Duration::from_secs(1))
        .expect("metric request");
    let trace_request = captured
        .recv_timeout(Duration::from_secs(1))
        .expect("trace request");
    for request in [&log_request, &metric_request, &trace_request] {
        let head = request.head.to_ascii_lowercase();
        assert!(head.contains("x-tenant: tenant-a"));
        assert!(head.contains("content-encoding: gzip"));
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
