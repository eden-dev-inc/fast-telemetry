//! OTLP HTTP/protobuf span exporter.
//!
//! Exports completed spans from an [`ophanim::span::SpanCollector`] to an
//! OTLP-compatible collector via HTTP POST to `/v1/traces`.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use ophanim::otlp::{build_resource, build_trace_export_request, pb};
use ophanim::span::SpanCollector;
use prost::Message;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

/// Configuration for the OTLP span exporter.
#[derive(Clone)]
pub struct SpanExportConfig {
    /// OTLP collector endpoint (scheme + host + port), e.g. `"http://localhost:4318"`.
    /// The path `/v1/traces` is appended automatically.
    pub endpoint: String,
    /// Export interval (default: 10s).
    pub interval: Duration,
    /// `service.name` resource attribute.
    pub service_name: String,
    /// Instrumentation scope name (default: "ophanim").
    pub scope_name: String,
    /// Additional resource attributes.
    pub resource_attributes: Vec<(String, String)>,
    /// Request timeout (default: 10s).
    pub timeout: Duration,
    /// Extra HTTP headers sent with every export request.
    pub headers: Vec<(String, String)>,
    /// Maximum number of spans per export batch (default: 512).
    pub max_batch_size: usize,
}

impl Default for SpanExportConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://localhost:4318".to_string(),
            interval: Duration::from_secs(10),
            service_name: "unknown_service".to_string(),
            scope_name: "ophanim".to_string(),
            resource_attributes: Vec::new(),
            timeout: Duration::from_secs(10),
            headers: Vec::new(),
            max_batch_size: 512,
        }
    }
}

impl SpanExportConfig {
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

    pub fn with_max_batch_size(mut self, size: usize) -> Self {
        self.max_batch_size = size;
        self
    }
}

/// Maximum backoff delay between retries after export failures.
const MAX_BACKOFF: Duration = Duration::from_secs(300);

/// Base backoff delay after the first failure.
const BASE_BACKOFF: Duration = Duration::from_secs(5);

/// Minimum payload size (bytes) before gzip compression is applied.
const GZIP_THRESHOLD: usize = 1024;

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

/// Spawn the span exporter on a dedicated thread with its own single-threaded
/// tokio runtime, matching the original design that avoids contending with the
/// application's async runtime.
pub fn spawn(
    collector: Arc<SpanCollector>,
    config: SpanExportConfig,
    cancel: CancellationToken,
) -> Option<std::thread::JoinHandle<()>> {
    std::thread::Builder::new()
        .name("span-exporter".to_string())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("span exporter runtime");
            rt.block_on(run(collector, config, cancel));
        })
        .ok()
}

/// Run the OTLP span export loop.
///
/// Drains completed spans from the collector in batches and sends them to
/// `/v1/traces`. On cancellation, a final drain+export is performed.
pub async fn run(
    collector: Arc<SpanCollector>,
    config: SpanExportConfig,
    cancel: CancellationToken,
) {
    let url = format!("{}/v1/traces", config.endpoint.trim_end_matches('/'));

    log::info!(
        "Starting OTLP span exporter, endpoint={url}, service={}",
        config.service_name
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
            log::error!("Failed to build HTTP client for span exporter: {e}");
            return;
        }
    };

    let mut interval = tokio::time::interval(config.interval);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    interval.tick().await;

    let mut consecutive_failures: u32 = 0;
    let mut bufs = SpanExportBufs {
        spans: Vec::with_capacity(config.max_batch_size),
        encode: Vec::new(),
        gzip: Vec::new(),
    };

    let ctx = SpanExportContext {
        client: &client,
        url: &url,
        collector: &collector,
        resource: &resource,
        config: &config,
    };

    loop {
        tokio::select! {
            _ = interval.tick() => {}
            _ = cancel.cancelled() => {
                log::info!("Span exporter shutting down, performing final export");
                export_once(&ctx, &mut bufs).await;
                return;
            }
        }

        if consecutive_failures > 0 {
            let backoff = backoff_with_jitter(consecutive_failures);
            log::debug!(
                "Span export backing off {}ms (failures={consecutive_failures})",
                backoff.as_millis()
            );
            tokio::select! {
                _ = tokio::time::sleep(backoff) => {}
                _ = cancel.cancelled() => {
                    export_once(&ctx, &mut bufs).await;
                    return;
                }
            }
        }

        bufs.spans.clear();
        collector.drain_into(&mut bufs.spans);

        if bufs.spans.is_empty() {
            continue;
        }

        let total_drained = bufs.spans.len();
        let dropped = total_drained.saturating_sub(config.max_batch_size);
        bufs.spans.truncate(config.max_batch_size);
        let span_count = bufs.spans.len();

        if dropped > 0 {
            log::debug!("Span export dropped {dropped} excess spans (exported {span_count})");
        }

        let otlp_spans: Vec<_> = bufs.spans.iter().map(|s| s.to_otlp()).collect();
        let request = build_trace_export_request(&resource, &config.scope_name, otlp_spans);

        bufs.encode.clear();
        if let Err(e) = request.encode(&mut bufs.encode) {
            log::warn!("Span protobuf encode failed: {e}");
            continue;
        }

        let body_len = bufs.encode.len();

        match send_otlp(&client, &url, &bufs.encode, &mut bufs.gzip, &config.headers).await {
            Ok(resp) if resp.status().is_success() => {
                consecutive_failures = 0;
                log::debug!("Exported {span_count} spans ({body_len} bytes)");
            }
            Ok(resp) => {
                consecutive_failures = consecutive_failures.saturating_add(1);
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                log::warn!("Span export failed: status={status}, body={body}");
            }
            Err(e) => {
                consecutive_failures = consecutive_failures.saturating_add(1);
                log::warn!("Span export request failed: {e}");
            }
        }
    }
}

struct SpanExportContext<'a> {
    client: &'a reqwest::Client,
    url: &'a str,
    collector: &'a SpanCollector,
    resource: &'a pb::Resource,
    config: &'a SpanExportConfig,
}

struct SpanExportBufs {
    spans: Vec<ophanim::span::CompletedSpan>,
    encode: Vec<u8>,
    gzip: Vec<u8>,
}

async fn export_once(ctx: &SpanExportContext<'_>, bufs: &mut SpanExportBufs) {
    bufs.spans.clear();
    ctx.collector.drain_into(&mut bufs.spans);

    if bufs.spans.is_empty() {
        return;
    }

    let otlp_spans: Vec<_> = bufs.spans.iter().map(|s| s.to_otlp()).collect();
    let request = build_trace_export_request(ctx.resource, &ctx.config.scope_name, otlp_spans);

    bufs.encode.clear();
    if let Err(e) = request.encode(&mut bufs.encode) {
        log::warn!("Final span protobuf encode failed: {e}");
        return;
    }

    match send_otlp(
        ctx.client,
        ctx.url,
        &bufs.encode,
        &mut bufs.gzip,
        &ctx.config.headers,
    )
    .await
    {
        Ok(resp) if !resp.status().is_success() => {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            log::warn!("Final span export returned {status}: {body}");
        }
        Err(e) => log::warn!("Final span export failed: {e}"),
        _ => {}
    }
}

fn backoff_with_jitter(consecutive_failures: u32) -> Duration {
    let exp = consecutive_failures.min(10);
    let base_ms = BASE_BACKOFF.as_millis() as u64;
    let backoff_ms = base_ms
        .saturating_mul(1u64 << exp)
        .min(MAX_BACKOFF.as_millis() as u64);

    let nanos = SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    let jitter_range = (backoff_ms / 4).max(1);
    let jitter = (nanos as u64 % (jitter_range * 2 + 1)).saturating_sub(jitter_range);
    let final_ms = backoff_ms.saturating_add(jitter);

    Duration::from_millis(final_ms)
}
