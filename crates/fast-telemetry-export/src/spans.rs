//! OTLP HTTP/protobuf span exporter.
//!
//! Exports completed spans from an [`fast_telemetry::span::SpanCollector`] to an
//! OTLP-compatible collector via HTTP POST to `/v1/traces`.
//! Larger payloads are gzip-compressed automatically. Consecutive failures delay
//! later export cycles with exponential backoff.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use fast_telemetry::otlp::{
    EncodedTraceRequest, build_resource, encode_native_trace_export_request_chunks, pb,
};
use fast_telemetry::span::{SpanBatch, SpanCollector};
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
    /// Instrumentation scope name (default: "fast-telemetry").
    pub scope_name: String,
    /// Additional resource attributes.
    pub resource_attributes: Vec<(String, String)>,
    /// Request timeout (default: 10s).
    pub timeout: Duration,
    /// Extra HTTP headers sent with every export request.
    pub headers: Vec<(String, String)>,
    /// Maximum spans per HTTP request (default: 512, must be positive).
    /// Larger drains are split into multiple requests without truncation.
    pub max_batch_size: usize,
}

impl Default for SpanExportConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://localhost:4318".to_string(),
            interval: Duration::from_secs(10),
            service_name: "unknown_service".to_string(),
            scope_name: "fast-telemetry".to_string(),
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

    /// Set the export interval.
    ///
    /// # Panics
    /// Panics if `interval` is zero.
    pub fn with_interval(mut self, interval: Duration) -> Self {
        assert!(!interval.is_zero(), "span export interval must be positive");
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

    /// Limit spans in each request, including the final shutdown drain.
    ///
    /// Every drained span is attempted once; this does not bound total work in
    /// an export cycle or retry failed requests.
    ///
    /// # Panics
    /// Panics if `size` is zero.
    pub fn with_max_batch_size(mut self, size: usize) -> Self {
        assert!(size > 0, "span request limit must be positive");
        self.max_batch_size = size;
        self
    }
}

/// Maximum delay before a later export cycle after failures.
const MAX_BACKOFF: Duration = Duration::from_secs(300);

/// Base delay before applying the failure exponent.
const BASE_BACKOFF: Duration = Duration::from_secs(5);

/// Spawn the span exporter on a parent-owned Tokio runtime.
///
/// This is the preferred hosted-service entry point. The parent retains
/// control of executor sizing and shutdown, while the returned task can be
/// awaited after cancelling `cancel` to guarantee the final drain attempt has
/// completed.
pub fn spawn_on(
    handle: &tokio::runtime::Handle,
    collector: Arc<SpanCollector>,
    config: SpanExportConfig,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    handle.spawn(run(collector, config, cancel))
}

/// Spawn the span exporter on a dedicated thread with its own single-threaded
/// Tokio runtime.
///
/// Prefer [`spawn_on`] when fast-telemetry is embedded in an application that
/// already owns a Tokio runtime. This standalone path is useful when telemetry
/// isolation is intentional or no parent executor exists.
pub fn spawn_standalone(
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

/// Backwards-compatible alias for [`spawn_standalone`].
///
/// New hosted applications should use [`spawn_on`] so runtime ownership stays
/// with the parent application.
pub fn spawn(
    collector: Arc<SpanCollector>,
    config: SpanExportConfig,
    cancel: CancellationToken,
) -> Option<std::thread::JoinHandle<()>> {
    spawn_standalone(collector, config, cancel)
}

/// Periodically flush this monoio worker's thread-local span buffer.
///
/// [`SpanCollector::drain_into`] can only drain spans that have already moved
/// from a worker's thread-local buffer into the shared outbox. On long-lived
/// monoio workers, low-volume spans may sit below the collector's automatic
/// flush threshold for a long time, so run one of these tasks on each monoio
/// worker that records spans.
///
/// The actual OTLP span exporter can run on a parent Tokio executor with
/// [`spawn_on`] or remain isolated with [`spawn_standalone`]; either path
/// drains the shared outboxes.
#[cfg(feature = "monoio")]
pub async fn run_local_flusher_monoio(
    collector: Arc<SpanCollector>,
    interval: Duration,
    cancel: CancellationToken,
) {
    use monoio::time::MissedTickBehavior;

    let mut interval = monoio::time::interval(interval);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    interval.tick().await;

    loop {
        monoio::select! {
            _ = interval.tick() => {
                collector.flush_local();
            }
            _ = cancel.cancelled() => {
                collector.flush_local();
                return;
            }
        }
    }
}

/// Periodically flush this compio worker's thread-local span buffer.
///
/// This is the compio-native counterpart to `run_local_flusher_monoio`. Run
/// one task on each compio worker that records spans; the OTLP span exporter
/// can use [`spawn_on`] or [`spawn_standalone`]. `cancel` may be any future
/// that completes when the flusher should shut down.
#[cfg(feature = "compio")]
pub async fn run_local_flusher_compio(
    collector: Arc<SpanCollector>,
    interval: Duration,
    cancel: impl std::future::Future<Output = ()>,
) {
    use futures_util::{FutureExt as _, select};

    let mut interval = compio::time::interval(interval);
    interval.tick().await;
    let cancel = cancel.fuse();
    let mut cancel = std::pin::pin!(cancel);

    loop {
        let tick = interval.tick();
        let tick = std::pin::pin!(tick);

        select! {
            _ = tick.fuse() => {
                collector.flush_local();
            },
            _ = cancel.as_mut() => {
                collector.flush_local();
                return;
            }
        }
    }
}

/// Run the OTLP span export loop.
///
/// Drains completed spans from the collector in batches and sends them to
/// `/v1/traces`. On cancellation, a final drain+export is performed.
/// Each drain is split at [`SpanExportConfig::max_batch_size`], with every
/// request attempted once even if an earlier request fails. Requests are not
/// replayed. Invalid zero intervals or request limits are logged and return
/// before draining the collector.
///
/// # Example
///
/// ```no_run
/// use std::sync::Arc;
/// use std::time::Duration;
///
/// use fast_telemetry::SpanCollector;
/// use fast_telemetry_export::spans::{SpanExportConfig, spawn_on};
/// use tokio_util::sync::CancellationToken;
///
/// # async fn example() {
/// let collector = Arc::new(SpanCollector::new(4, 4096));
/// let cancel = CancellationToken::new();
/// let config = SpanExportConfig::new("http://otel-collector:4318")
///     .with_service_name("myapp")
///     .with_scope_name("proxy")
///     .with_header("Authorization", "Bearer <token>")
///     .with_timeout(Duration::from_secs(5))
///     .with_max_batch_size(1024);
///
/// let exporter = spawn_on(
///     &tokio::runtime::Handle::current(),
///     collector,
///     config,
///     cancel.clone(),
/// );
/// cancel.cancel();
/// exporter.await.expect("span exporter task");
/// # }
/// ```
pub async fn run(
    collector: Arc<SpanCollector>,
    config: SpanExportConfig,
    cancel: CancellationToken,
) {
    if config.max_batch_size == 0 || config.interval.is_zero() {
        crate::logging::log_error!("Span export interval and request limit must be positive");
        return;
    }
    let url = format!("{}/v1/traces", config.endpoint.trim_end_matches('/'));

    crate::logging::log_info!(
        "Starting OTLP span exporter, endpoint={url}, service={}",
        config.service_name
    );

    let attr_refs: Vec<(&str, &str)> = config
        .resource_attributes
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let resource = build_resource(&config.service_name, &attr_refs);

    let mut http_config =
        crate::otlp::OtlpHttpConfig::new(&config.endpoint).with_timeout(config.timeout);
    for (name, value) in &config.headers {
        http_config = http_config.with_header(name, value);
    }
    let client = match crate::otlp::OtlpHttpClient::new(http_config) {
        Ok(c) => c,
        Err(e) => {
            crate::logging::log_error!("Failed to build HTTP client for span exporter: {e}");
            return;
        }
    };

    let mut interval = tokio::time::interval(config.interval);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    interval.tick().await;

    let mut consecutive_failures: u32 = 0;
    let mut bufs = SpanExportBufs {
        spans: SpanBatch::new(),
        requests: Vec::new(),
    };

    let ctx = SpanExportContext {
        client: &client,
        collector: &collector,
        resource: &resource,
        config: &config,
    };

    loop {
        tokio::select! {
            _ = interval.tick() => {}
            _ = cancel.cancelled() => {
                crate::logging::log_info!("Span exporter shutting down, performing final export");
                let _ = export_once(&ctx, &mut bufs).await;
                return;
            }
        }

        if consecutive_failures > 0 {
            let backoff = backoff_with_jitter(consecutive_failures);
            crate::logging::log_debug!(
                "Span export backing off {}ms (failures={consecutive_failures})",
                backoff.as_millis()
            );
            tokio::select! {
                _ = tokio::time::sleep(backoff) => {}
                _ = cancel.cancelled() => {
                    let _ = export_once(&ctx, &mut bufs).await;
                    return;
                }
            }
        }

        match export_once(&ctx, &mut bufs).await {
            Some(true) => consecutive_failures = 0,
            Some(false) => consecutive_failures = consecutive_failures.saturating_add(1),
            None => {}
        }
    }
}

struct SpanExportContext<'a> {
    client: &'a crate::otlp::OtlpHttpClient,
    collector: &'a SpanCollector,
    resource: &'a pb::Resource,
    config: &'a SpanExportConfig,
}

struct SpanExportBufs {
    spans: SpanBatch,
    requests: Vec<EncodedTraceRequest>,
}

// None denotes an empty drain: it must not reset a previous cycle's failure.
async fn export_once(ctx: &SpanExportContext<'_>, bufs: &mut SpanExportBufs) -> Option<bool> {
    bufs.spans.clear();
    ctx.collector.drain_batches(&mut bufs.spans);

    if bufs.spans.is_empty() {
        return None;
    }

    encode_trace_requests(
        ctx.resource,
        &ctx.config.scope_name,
        &mut bufs.spans,
        ctx.config.max_batch_size,
        &mut bufs.requests,
    );
    let mut success = true;
    for request in bufs.requests.drain(..) {
        let span_count = request.span_count();
        let body_len = request.as_bytes().len();
        match ctx.client.export_native_traces(request).await {
            Ok(outcome) if outcome.rejected == 0 => {
                crate::logging::log_debug!("Exported {span_count} spans ({body_len} bytes)");
            }
            Ok(outcome) => {
                success = false;
                crate::logging::log_warn!(
                    "Span export partially rejected {} spans: {}",
                    outcome.rejected,
                    outcome
                        .message
                        .as_deref()
                        .unwrap_or("collector did not provide a reason")
                );
            }
            Err(e) => {
                success = false;
                crate::logging::log_warn!("Span export request failed: {e}");
            }
        }
    }
    Some(success)
}

fn encode_trace_requests(
    resource: &pb::Resource,
    scope_name: &str,
    spans: &mut SpanBatch,
    max_spans: usize,
    requests: &mut Vec<EncodedTraceRequest>,
) {
    requests.clear();
    requests.reserve(spans.len().div_ceil(max_spans));
    requests.extend(encode_native_trace_export_request_chunks(
        resource, scope_name, spans, max_spans,
    ));
    // Release the actual payload before awaiting HTTP; keep the drain allocation.
    spans.clear();
}

fn backoff_with_jitter(consecutive_failures: u32) -> Duration {
    let nanos = SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    backoff_for_sample(consecutive_failures, u64::from(nanos))
}

fn backoff_for_sample(consecutive_failures: u32, sample: u64) -> Duration {
    let exp = consecutive_failures.min(10);
    let maximum = MAX_BACKOFF.as_millis() as u64;
    let backoff_ms = (BASE_BACKOFF.as_millis() as u64)
        .saturating_mul(1u64 << exp)
        .min(maximum);
    let jitter_range = (backoff_ms / 4).max(1);
    // Add before subtracting so the lower half of the sample range produces
    // negative jitter. Clamp the final delay, including positive jitter.
    let final_ms = (backoff_ms + sample % (jitter_range * 2 + 1) - jitter_range).min(maximum);

    Duration::from_millis(final_ms)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fast_telemetry::CompletedSpan;
    use fast_telemetry::otlp::build_trace_export_request;
    use prost::Message;

    #[test]
    #[should_panic(expected = "span request limit must be positive")]
    fn span_request_limit_rejects_zero() {
        let _ = SpanExportConfig::default().with_max_batch_size(0);
    }

    #[test]
    #[should_panic(expected = "span export interval must be positive")]
    fn span_export_interval_rejects_zero() {
        let _ = SpanExportConfig::default().with_interval(Duration::ZERO);
    }

    #[tokio::test]
    async fn invalid_public_config_does_not_drain_spans() {
        for config in [
            SpanExportConfig {
                max_batch_size: 0,
                ..Default::default()
            },
            SpanExportConfig {
                interval: Duration::ZERO,
                ..Default::default()
            },
        ] {
            let collector = Arc::new(SpanCollector::new(1, 64));
            drop(collector.start_span("retained", fast_telemetry::SpanKind::Internal));
            collector.flush_local();
            let cancel = CancellationToken::new();
            cancel.cancel();
            run(Arc::clone(&collector), config, cancel).await;
            assert_eq!(collector.len(), 1);
        }
    }

    #[test]
    fn backoff_jitter_varies_in_both_directions_and_respects_maximum() {
        assert_eq!(backoff_for_sample(1, 0), Duration::from_millis(7_500));
        assert_eq!(backoff_for_sample(1, 2_500), Duration::from_millis(10_000));
        assert_eq!(backoff_for_sample(1, 5_000), Duration::from_millis(12_500));
        for failures in [0, 1, 2, 4, 8, 32, u32::MAX] {
            for sample in [0, 1, 75_000, 150_000, u64::MAX] {
                let delay = backoff_for_sample(failures, sample);
                assert!(delay > Duration::ZERO);
                assert!(delay <= MAX_BACKOFF);
            }
        }
    }

    #[test]
    fn native_trace_requests_preserve_batch_storage_order_and_wire_bytes() {
        let collector = Arc::new(SpanCollector::new(1, 64));
        let resource = build_resource("owned export", &[("version", "1")]);
        let mut spans = SpanBatch::new();
        let mut requests = Vec::new();
        for cycle in 0..4 {
            for index in 0..2 {
                let mut span = collector.start_span(
                    format!("owned-{cycle}-{index}"),
                    fast_telemetry::SpanKind::Server,
                );
                span.set_attribute(format!("key-{index}"), format!("value-{cycle}-{index}"));
            }
            collector.flush_local();
            collector.drain_batches(&mut spans);
            let expected = build_trace_export_request(
                &resource,
                "scope",
                spans.iter().map(CompletedSpan::to_otlp).collect(),
            );
            let pointers = spans.buffers().map(|b| b.as_ptr()).collect::<Vec<_>>();
            encode_trace_requests(&resource, "scope", &mut spans, usize::MAX, &mut requests);
            assert_eq!(requests.len(), 1);
            let request = requests.pop().expect("encoded request");
            assert!(spans.is_empty());
            assert!(!pointers.is_empty());
            assert_eq!(request.span_count(), 2);
            assert_eq!(request.as_bytes(), expected.encode_to_vec());
            let decoded = pb::ExportTraceServiceRequest::decode(request.as_bytes())
                .expect("decode native export");
            let exported = &decoded.resource_spans[0].scope_spans[0].spans;
            assert_eq!(exported[0].name, format!("owned-{cycle}-0"));
            assert_eq!(exported[1].name, format!("owned-{cycle}-1"));
        }
    }

    #[test]
    fn span_exporter_can_run_on_parent_tokio_runtime() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("parent Tokio runtime");
        let collector = Arc::new(SpanCollector::new(1, 16));
        let cancel = CancellationToken::new();
        cancel.cancel();

        let task = spawn_on(
            runtime.handle(),
            collector,
            SpanExportConfig::new("http://127.0.0.1:4318"),
            cancel,
        );
        runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(1), task)
                .await
                .expect("span exporter should stop promptly after cancellation")
                .expect("span exporter task should exit cleanly");
        });
    }
}
