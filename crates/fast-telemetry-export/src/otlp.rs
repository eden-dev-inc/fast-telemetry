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

    log::info!(
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
            log::error!("Failed to build HTTP client for OTLP exporter: {e}");
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
                log::info!("OTLP metrics exporter shutting down, performing final export");
                export_once(&ctx, &mut collect_fn, &mut bufs).await;
                return;
            }
        }

        if consecutive_failures > 0 {
            let backoff = backoff_with_jitter(consecutive_failures);
            log::debug!(
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
            log::warn!("OTLP protobuf encode failed: {e}");
            continue;
        }
        let body_len = bufs.encode.len();

        match send_otlp(&client, &url, &bufs.encode, &mut bufs.gzip, &config.headers).await {
            Ok(resp) if resp.status().is_success() => {
                consecutive_failures = 0;
                log::debug!("Exported {metric_count} OTLP metrics ({body_len} bytes)");
            }
            Ok(resp) => {
                consecutive_failures = consecutive_failures.saturating_add(1);
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                log::warn!("OTLP export failed: status={status}, body={body}");
            }
            Err(e) => {
                consecutive_failures = consecutive_failures.saturating_add(1);
                log::warn!("OTLP export request failed: {e}");
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
        log::warn!("Final OTLP protobuf encode failed: {e}");
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
            log::warn!("Final OTLP export returned {status}: {body}");
        }
        Err(e) => log::warn!("Final OTLP export failed: {e}"),
        _ => {}
    }
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
