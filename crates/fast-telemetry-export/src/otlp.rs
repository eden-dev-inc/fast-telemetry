//! OTLP HTTP/protobuf metrics exporter.
//!
//! Exports metrics to an OTLP-compatible collector (OpenTelemetry Collector,
//! Grafana Alloy, Datadog OTLP intake, etc.) via HTTP POST of protobuf-encoded
//! `ExportMetricsServiceRequest` to `/v1/metrics`.
//!
//! Uses cumulative temporality — no state tracking needed between export cycles.
//! Larger payloads are gzip-compressed automatically, and failed exports retry
//! with exponential backoff.
//!
//! The actual metric collection is provided by the caller via a closure,
//! making this exporter work with any metrics struct.

use std::time::Duration;

use fast_telemetry::otlp::{build_export_request, build_resource, pb};
use prost::Message;
use tokio::time::{MissedTickBehavior, interval};
use tokio_util::sync::CancellationToken;

/// Configuration for the OTLP HTTP metrics exporter.
#[derive(Clone)]
pub struct OtlpConfig {
    /// OTLP collector endpoint (scheme + host + port), e.g. `"http://localhost:4318"`.
    /// The path `/v1/metrics` is appended automatically.
    pub endpoint: String,
    /// Export interval (default: 60s).
    pub interval: Duration,
    /// `service.name` resource attribute.
    pub service_name: String,
    /// Instrumentation scope name (default: "fast-telemetry").
    pub scope_name: String,
    /// Additional resource attributes (e.g. `("service.version", "1.0")`).
    pub resource_attributes: Vec<(String, String)>,
    /// Request timeout (default: 10s).
    pub timeout: Duration,
    /// Extra HTTP headers sent with every export request.
    ///
    /// Use this for collector authentication, e.g.:
    /// - `("Authorization", "Bearer <token>")`
    /// - `("x-api-key", "<key>")`
    pub headers: Vec<(String, String)>,
}

impl Default for OtlpConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://localhost:4318".to_string(),
            interval: Duration::from_secs(60),
            service_name: "unknown_service".to_string(),
            scope_name: "fast-telemetry".to_string(),
            resource_attributes: Vec::new(),
            timeout: Duration::from_secs(10),
            headers: Vec::new(),
        }
    }
}

impl OtlpConfig {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            ..Default::default()
        }
    }

    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    pub fn with_service_name(mut self, name: impl Into<String>) -> Self {
        self.service_name = name.into();
        self
    }

    pub fn with_scope_name(mut self, name: impl Into<String>) -> Self {
        self.scope_name = name.into();
        self
    }

    pub fn with_attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.resource_attributes.push((key.into(), value.into()));
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }
}

/// Maximum backoff delay between retries after export failures.
const MAX_BACKOFF: Duration = Duration::from_secs(300);

/// Base backoff delay after the first failure.
const BASE_BACKOFF: Duration = Duration::from_secs(5);

/// Minimum payload size (bytes) before gzip compression is applied.
/// Below this threshold, compression overhead exceeds savings.
const GZIP_THRESHOLD: usize = 1024;

/// Gzip-compress `data` into `out` using fast compression (level 1).
///
/// Returns `true` if compression was applied, `false` if the payload was below
/// the threshold (in which case `out` is untouched).
fn gzip_compress(data: &[u8], out: &mut Vec<u8>) -> bool {
    if data.len() < GZIP_THRESHOLD {
        return false;
    }
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use std::io::Write;

    out.clear();
    let mut encoder = GzEncoder::new(out, Compression::fast());
    let _ = encoder.write_all(data);
    let _ = encoder.finish();
    true
}

/// Send a protobuf-encoded body, applying gzip when beneficial.
async fn send_otlp(
    client: &reqwest::Client,
    url: &str,
    body: &[u8],
    gzip_buf: &mut Vec<u8>,
    extra_headers: &[(String, String)],
) -> Result<reqwest::Response, reqwest::Error> {
    let mut req = client
        .post(url)
        .header("Content-Type", "application/x-protobuf");

    for (name, value) in extra_headers {
        req = req.header(name, value);
    }

    if gzip_compress(body, gzip_buf) {
        req.header("Content-Encoding", "gzip")
            .body(gzip_buf.clone())
            .send()
            .await
    } else {
        req.body(body.to_vec()).send().await
    }
}

/// Run the OTLP metrics export loop.
///
/// `collect_fn` is called each cycle with a `&mut Vec<pb::Metric>`. The closure
/// should append OTLP metric messages (typically via `ExportMetrics::export_otlp`).
/// The exporter handles protobuf encoding, gzip compression, HTTP transport, and
/// exponential backoff on failures.
///
/// On cancellation, a final export is performed to flush pending metrics.
///
/// # Example
///
/// ```ignore
/// use std::sync::Arc;
/// use std::time::Duration;
///
/// use fast_telemetry_export::otlp::{OtlpConfig, run};
/// use tokio_util::sync::CancellationToken;
///
/// let metrics = Arc::new(MyMetrics::new());
/// let cancel = CancellationToken::new();
/// let config = OtlpConfig::new("http://otel-collector:4318")
///     .with_service_name("myapp")
///     .with_scope_name("proxy")
///     .with_attribute("service.version", "1.0")
///     .with_header("Authorization", "Bearer <token>")
///     .with_timeout(Duration::from_secs(5));
///
/// let m = metrics.clone();
/// tokio::spawn(run(config, cancel, move |out| {
///     m.export_otlp(out);
/// }));
/// ```
pub async fn run<F>(config: OtlpConfig, cancel: CancellationToken, mut collect_fn: F)
where
    F: FnMut(&mut Vec<pb::Metric>),
{
    let url = format!("{}/v1/metrics", config.endpoint.trim_end_matches('/'));

    crate::logging::log_info!(
        "Starting OTLP metrics exporter, endpoint={url}, interval={}s",
        config.interval.as_secs()
    );

    let attr_refs: Vec<(&str, &str)> = config
        .resource_attributes
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let resource = build_resource(&config.service_name, &attr_refs);

    let client = match reqwest::Client::builder().timeout(config.timeout).build() {
        Ok(c) => c,
        Err(e) => {
            crate::logging::log_error!("Failed to build HTTP client for OTLP exporter: {e}");
            return;
        }
    };

    let mut interval = interval(config.interval);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    interval.tick().await;

    let mut consecutive_failures: u32 = 0;
    let mut bufs = ExportBufs::default();

    let ctx = ExportContext {
        client: &client,
        url: &url,
        resource: &resource,
        scope_name: &config.scope_name,
        extra_headers: &config.headers,
    };

    loop {
        tokio::select! {
            _ = interval.tick() => {}
            _ = cancel.cancelled() => {
                crate::logging::log_info!("OTLP metrics exporter shutting down, performing final export");
                export_once(&ctx, &mut collect_fn, &mut bufs).await;
                return;
            }
        }

        if consecutive_failures > 0 {
            let backoff = backoff_with_jitter(consecutive_failures);
            crate::logging::log_debug!(
                "OTLP export backing off {}ms (failures={consecutive_failures})",
                backoff.as_millis()
            );
            tokio::select! {
                _ = tokio::time::sleep(backoff) => {}
                _ = cancel.cancelled() => {
                    export_once(&ctx, &mut collect_fn, &mut bufs).await;
                    return;
                }
            }
        }

        let mut metric_messages = Vec::new();
        collect_fn(&mut metric_messages);

        if metric_messages.is_empty() {
            continue;
        }

        let metric_count = metric_messages.len();
        let request = build_export_request(&resource, &config.scope_name, metric_messages);

        bufs.encode.clear();
        if let Err(e) = request.encode(&mut bufs.encode) {
            crate::logging::log_warn!("OTLP protobuf encode failed: {e}");
            continue;
        }
        let body_len = bufs.encode.len();

        match send_otlp(&client, &url, &bufs.encode, &mut bufs.gzip, &config.headers).await {
            Ok(resp) if resp.status().is_success() => {
                consecutive_failures = 0;
                crate::logging::log_debug!(
                    "Exported {metric_count} OTLP metrics ({body_len} bytes)"
                );
            }
            Ok(resp) => {
                consecutive_failures = consecutive_failures.saturating_add(1);
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                crate::logging::log_warn!("OTLP export failed: status={status}, body={body}");
            }
            Err(e) => {
                consecutive_failures = consecutive_failures.saturating_add(1);
                crate::logging::log_warn!("OTLP export request failed: {e}");
            }
        }
    }
}

/// Run the OTLP metrics export loop on a monoio runtime.
///
/// This is a monoio-native alternative to [`run`] for applications that do not
/// want their metrics exporter to run on Tokio. It sends OTLP HTTP/protobuf over
/// a fresh monoio TCP connection per export attempt.
///
/// Currently this supports plaintext `http://` collector endpoints only. Keep
/// using [`run`] for `https://` endpoints until a monoio TLS transport is added.
/// The caller must run this inside a monoio runtime with timers enabled.
#[cfg(feature = "monoio")]
pub async fn run_monoio<F>(config: OtlpConfig, cancel: CancellationToken, mut collect_fn: F)
where
    F: FnMut(&mut Vec<pb::Metric>),
{
    use monoio::time::MissedTickBehavior;

    let url = format!("{}/v1/metrics", config.endpoint.trim_end_matches('/'));
    let target = match MonoioHttpTarget::parse(&url) {
        Ok(target) => target,
        Err(e) => {
            crate::logging::log_error!("Invalid monoio OTLP endpoint {url}: {e}");
            return;
        }
    };

    crate::logging::log_info!(
        "Starting monoio OTLP metrics exporter, endpoint={url}, interval={}s",
        config.interval.as_secs()
    );

    let attr_refs: Vec<(&str, &str)> = config
        .resource_attributes
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let resource = build_resource(&config.service_name, &attr_refs);

    let mut interval = monoio::time::interval(config.interval);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    interval.tick().await;

    let mut consecutive_failures: u32 = 0;
    let mut bufs = ExportBufs::default();

    let ctx = MonoioExportContext {
        target: &target,
        resource: &resource,
        scope_name: &config.scope_name,
        extra_headers: &config.headers,
        timeout: config.timeout,
    };

    loop {
        monoio::select! {
            _ = interval.tick() => {}
            _ = cancel.cancelled() => {
                crate::logging::log_info!("monoio OTLP metrics exporter shutting down, performing final export");
                export_once_monoio(&ctx, &mut collect_fn, &mut bufs).await;
                return;
            }
        }

        if consecutive_failures > 0 {
            let backoff = backoff_with_jitter(consecutive_failures);
            crate::logging::log_debug!(
                "monoio OTLP export backing off {}ms (failures={consecutive_failures})",
                backoff.as_millis()
            );
            monoio::select! {
                _ = monoio::time::sleep(backoff) => {}
                _ = cancel.cancelled() => {
                    export_once_monoio(&ctx, &mut collect_fn, &mut bufs).await;
                    return;
                }
            }
        }

        let mut metric_messages = Vec::new();
        collect_fn(&mut metric_messages);

        if metric_messages.is_empty() {
            continue;
        }

        let metric_count = metric_messages.len();
        let request = build_export_request(&resource, &config.scope_name, metric_messages);

        bufs.encode.clear();
        if let Err(e) = request.encode(&mut bufs.encode) {
            crate::logging::log_warn!("monoio OTLP protobuf encode failed: {e}");
            continue;
        }
        let body_len = bufs.encode.len();

        match send_otlp_monoio(
            &target,
            &bufs.encode,
            &mut bufs.gzip,
            &config.headers,
            config.timeout,
        )
        .await
        {
            Ok(resp) if resp.is_success() => {
                consecutive_failures = 0;
                crate::logging::log_debug!(
                    "Exported {metric_count} monoio OTLP metrics ({body_len} bytes)"
                );
            }
            Ok(resp) => {
                consecutive_failures = consecutive_failures.saturating_add(1);
                crate::logging::log_warn!(
                    "monoio OTLP export failed: status={}, body={}",
                    resp.status,
                    resp.body_text_lossy()
                );
            }
            Err(e) => {
                consecutive_failures = consecutive_failures.saturating_add(1);
                crate::logging::log_warn!("monoio OTLP export request failed: {e}");
            }
        }
    }
}

struct ExportContext<'a> {
    client: &'a reqwest::Client,
    url: &'a str,
    resource: &'a pb::Resource,
    scope_name: &'a str,
    extra_headers: &'a [(String, String)],
}

#[derive(Default)]
struct ExportBufs {
    encode: Vec<u8>,
    gzip: Vec<u8>,
}

async fn export_once<F>(ctx: &ExportContext<'_>, collect_fn: &mut F, bufs: &mut ExportBufs)
where
    F: FnMut(&mut Vec<pb::Metric>),
{
    let mut metric_messages = Vec::new();
    collect_fn(&mut metric_messages);

    if metric_messages.is_empty() {
        return;
    }

    let request = build_export_request(ctx.resource, ctx.scope_name, metric_messages);

    bufs.encode.clear();
    if let Err(e) = request.encode(&mut bufs.encode) {
        crate::logging::log_warn!("Final OTLP protobuf encode failed: {e}");
        return;
    }

    match send_otlp(
        ctx.client,
        ctx.url,
        &bufs.encode,
        &mut bufs.gzip,
        ctx.extra_headers,
    )
    .await
    {
        Ok(resp) if !resp.status().is_success() => {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            crate::logging::log_warn!("Final OTLP export returned {status}: {body}");
        }
        Err(e) => crate::logging::log_warn!("Final OTLP export failed: {e}"),
        _ => {}
    }
}

#[cfg(feature = "monoio")]
struct MonoioExportContext<'a> {
    target: &'a MonoioHttpTarget,
    resource: &'a pb::Resource,
    scope_name: &'a str,
    extra_headers: &'a [(String, String)],
    timeout: Duration,
}

#[cfg(feature = "monoio")]
async fn export_once_monoio<F>(
    ctx: &MonoioExportContext<'_>,
    collect_fn: &mut F,
    bufs: &mut ExportBufs,
) where
    F: FnMut(&mut Vec<pb::Metric>),
{
    let mut metric_messages = Vec::new();
    collect_fn(&mut metric_messages);

    if metric_messages.is_empty() {
        return;
    }

    let request = build_export_request(ctx.resource, ctx.scope_name, metric_messages);

    bufs.encode.clear();
    if let Err(e) = request.encode(&mut bufs.encode) {
        crate::logging::log_warn!("Final monoio OTLP protobuf encode failed: {e}");
        return;
    }

    match send_otlp_monoio(
        ctx.target,
        &bufs.encode,
        &mut bufs.gzip,
        ctx.extra_headers,
        ctx.timeout,
    )
    .await
    {
        Ok(resp) if !resp.is_success() => {
            crate::logging::log_warn!(
                "Final monoio OTLP export returned {}: {}",
                resp.status,
                resp.body_text_lossy()
            );
        }
        Err(e) => crate::logging::log_warn!("Final monoio OTLP export failed: {e}"),
        _ => {}
    }
}

#[cfg(feature = "monoio")]
#[derive(Debug)]
struct MonoioHttpTarget {
    connect_addr: String,
    host_header: String,
    path: String,
}

#[cfg(feature = "monoio")]
impl MonoioHttpTarget {
    fn parse(url: &str) -> Result<Self, MonoioHttpError> {
        let rest = url
            .strip_prefix("http://")
            .ok_or_else(|| MonoioHttpError::InvalidEndpoint("only http:// is supported".into()))?;
        let (authority, path) = match rest.find('/') {
            Some(idx) => (&rest[..idx], &rest[idx..]),
            None => (rest, "/"),
        };

        if authority.is_empty() {
            return Err(MonoioHttpError::InvalidEndpoint(
                "missing host in endpoint".into(),
            ));
        }

        let connect_addr = connect_addr_for_authority(authority)?;

        Ok(Self {
            connect_addr,
            host_header: authority.to_string(),
            path: path.to_string(),
        })
    }
}

#[cfg(feature = "monoio")]
fn connect_addr_for_authority(authority: &str) -> Result<String, MonoioHttpError> {
    if let Some(rest) = authority.strip_prefix('[') {
        let end = rest.find(']').ok_or_else(|| {
            MonoioHttpError::InvalidEndpoint("IPv6 hosts must use [addr] syntax".into())
        })?;
        let suffix = &rest[end + 1..];
        return if suffix.is_empty() {
            Ok(format!("{authority}:80"))
        } else if let Some(port) = suffix.strip_prefix(':') {
            validate_port(port)?;
            Ok(authority.to_string())
        } else {
            Err(MonoioHttpError::InvalidEndpoint(
                "unexpected characters after IPv6 host".into(),
            ))
        };
    }

    match authority.rsplit_once(':') {
        Some((_host, port)) => {
            validate_port(port)?;
            Ok(authority.to_string())
        }
        None => Ok(format!("{authority}:80")),
    }
}

#[cfg(feature = "monoio")]
fn validate_port(port: &str) -> Result<(), MonoioHttpError> {
    if port.parse::<u16>().is_ok() {
        Ok(())
    } else {
        Err(MonoioHttpError::InvalidEndpoint(
            "invalid port in endpoint".into(),
        ))
    }
}

#[cfg(feature = "monoio")]
#[derive(Debug)]
struct MonoioHttpResponse {
    status: u16,
    body: Vec<u8>,
}

#[cfg(feature = "monoio")]
impl MonoioHttpResponse {
    fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }

    fn body_text_lossy(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
}

#[cfg(feature = "monoio")]
#[derive(Debug)]
enum MonoioHttpError {
    InvalidEndpoint(String),
    InvalidHeader(String),
    Io(std::io::Error),
    Timeout,
    InvalidResponse,
}

#[cfg(feature = "monoio")]
impl std::fmt::Display for MonoioHttpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidEndpoint(msg) => write!(f, "{msg}"),
            Self::InvalidHeader(name) => write!(f, "invalid HTTP header {name:?}"),
            Self::Io(e) => write!(f, "{e}"),
            Self::Timeout => f.write_str("request timed out"),
            Self::InvalidResponse => f.write_str("invalid HTTP response"),
        }
    }
}

#[cfg(feature = "monoio")]
impl From<std::io::Error> for MonoioHttpError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

#[cfg(feature = "monoio")]
async fn send_otlp_monoio(
    target: &MonoioHttpTarget,
    body: &[u8],
    gzip_buf: &mut Vec<u8>,
    extra_headers: &[(String, String)],
    timeout: Duration,
) -> Result<MonoioHttpResponse, MonoioHttpError> {
    let compressed = gzip_compress(body, gzip_buf);
    let request_body = if compressed {
        gzip_buf.as_slice()
    } else {
        body
    };
    let request = build_monoio_http_request(target, request_body, compressed, extra_headers)?;

    match monoio::time::timeout(timeout, send_monoio_http_request(target, request)).await {
        Ok(result) => result,
        Err(_) => Err(MonoioHttpError::Timeout),
    }
}

#[cfg(feature = "monoio")]
fn build_monoio_http_request(
    target: &MonoioHttpTarget,
    body: &[u8],
    compressed: bool,
    extra_headers: &[(String, String)],
) -> Result<Vec<u8>, MonoioHttpError> {
    use std::fmt::Write as _;

    let mut head = String::with_capacity(512 + extra_headers.len() * 64);
    write!(
        &mut head,
        "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/x-protobuf\r\nContent-Length: {}\r\nConnection: close\r\n",
        target.path,
        target.host_header,
        body.len()
    )
    .expect("writing to String cannot fail");

    if compressed {
        head.push_str("Content-Encoding: gzip\r\n");
    }

    for (name, value) in extra_headers {
        validate_header(name, value)?;
        write!(&mut head, "{name}: {value}\r\n").expect("writing to String cannot fail");
    }

    head.push_str("\r\n");
    let mut request = head.into_bytes();
    request.extend_from_slice(body);
    Ok(request)
}

#[cfg(feature = "monoio")]
fn validate_header(name: &str, value: &str) -> Result<(), MonoioHttpError> {
    let invalid_name = name.is_empty()
        || name
            .bytes()
            .any(|b| b <= b' ' || b == b':' || b == b'\r' || b == b'\n');
    let invalid_value = value.bytes().any(|b| b == b'\r' || b == b'\n');

    if invalid_name || invalid_value {
        Err(MonoioHttpError::InvalidHeader(name.to_string()))
    } else {
        Ok(())
    }
}

#[cfg(feature = "monoio")]
async fn send_monoio_http_request(
    target: &MonoioHttpTarget,
    request: Vec<u8>,
) -> Result<MonoioHttpResponse, MonoioHttpError> {
    use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
    use monoio::net::TcpStream;

    let mut stream = TcpStream::connect(target.connect_addr.as_str()).await?;
    let (write_result, _request) = stream.write_all(request).await;
    write_result?;

    let mut response = Vec::with_capacity(8192);
    loop {
        let buf = Vec::with_capacity(8192);
        let (read_result, buf) = stream.read(buf).await;
        let n = read_result?;
        if n == 0 {
            break;
        }
        let available = buf.len().min(n);
        response.extend_from_slice(&buf[..available]);
        if response.len() >= 64 * 1024 {
            break;
        }
    }

    parse_monoio_http_response(response)
}

#[cfg(feature = "monoio")]
fn parse_monoio_http_response(response: Vec<u8>) -> Result<MonoioHttpResponse, MonoioHttpError> {
    let header_end = response
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|idx| idx + 4)
        .ok_or(MonoioHttpError::InvalidResponse)?;

    let status_line_end = response[..header_end]
        .windows(2)
        .position(|w| w == b"\r\n")
        .ok_or(MonoioHttpError::InvalidResponse)?;
    let status_line = std::str::from_utf8(&response[..status_line_end])
        .map_err(|_| MonoioHttpError::InvalidResponse)?;
    let status = parse_status_code(status_line).ok_or(MonoioHttpError::InvalidResponse)?;
    let body = response[header_end..].to_vec();

    Ok(MonoioHttpResponse { status, body })
}

#[cfg(feature = "monoio")]
fn parse_status_code(status_line: &str) -> Option<u16> {
    let mut parts = status_line.split_whitespace();
    let version = parts.next()?;
    if !version.starts_with("HTTP/") {
        return None;
    }
    parts.next()?.parse().ok()
}

/// Compute backoff with jitter: min(MAX_BACKOFF, BASE_BACKOFF * 2^failures) +/- 25% jitter.
fn backoff_with_jitter(consecutive_failures: u32) -> Duration {
    let exp = consecutive_failures.min(10);
    let base_ms = BASE_BACKOFF.as_millis() as u64;
    let backoff_ms = base_ms
        .saturating_mul(1u64 << exp)
        .min(MAX_BACKOFF.as_millis() as u64);

    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    let jitter_range = (backoff_ms / 4).max(1);
    let jitter = (nanos as u64 % (jitter_range * 2 + 1)).saturating_sub(jitter_range);
    let final_ms = backoff_ms.saturating_add(jitter);

    Duration::from_millis(final_ms)
}

#[cfg(all(test, feature = "monoio"))]
mod monoio_tests {
    use super::*;

    #[test]
    fn parses_http_target_with_port() {
        let target = MonoioHttpTarget::parse("http://localhost:4318/v1/metrics").unwrap();
        assert_eq!(target.connect_addr, "localhost:4318");
        assert_eq!(target.host_header, "localhost:4318");
        assert_eq!(target.path, "/v1/metrics");
    }

    #[test]
    fn parses_http_target_without_port() {
        let target = MonoioHttpTarget::parse("http://collector/v1/metrics").unwrap();
        assert_eq!(target.connect_addr, "collector:80");
        assert_eq!(target.host_header, "collector");
        assert_eq!(target.path, "/v1/metrics");
    }

    #[test]
    fn rejects_https_target() {
        let err = MonoioHttpTarget::parse("https://collector:4318/v1/metrics").unwrap_err();
        assert!(matches!(err, MonoioHttpError::InvalidEndpoint(_)));
    }

    #[test]
    fn rejects_invalid_port() {
        let err = MonoioHttpTarget::parse("http://collector:nope/v1/metrics").unwrap_err();
        assert!(matches!(err, MonoioHttpError::InvalidEndpoint(_)));
    }

    #[test]
    fn builds_http_request() {
        let target = MonoioHttpTarget::parse("http://localhost:4318/v1/metrics").unwrap();
        let request = build_monoio_http_request(
            &target,
            b"abc",
            true,
            &[("Authorization".to_string(), "Bearer token".to_string())],
        )
        .unwrap();
        let request = String::from_utf8(request).unwrap();
        assert!(request.starts_with("POST /v1/metrics HTTP/1.1\r\n"));
        assert!(request.contains("Host: localhost:4318\r\n"));
        assert!(request.contains("Content-Length: 3\r\n"));
        assert!(request.contains("Content-Encoding: gzip\r\n"));
        assert!(request.ends_with("\r\n\r\nabc"));
    }

    #[test]
    fn parses_http_status() {
        let response = parse_monoio_http_response(
            b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n".to_vec(),
        )
        .unwrap();
        assert_eq!(response.status, 204);
        assert!(response.body.is_empty());
    }
}
