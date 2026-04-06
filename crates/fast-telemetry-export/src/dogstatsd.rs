//! DogStatsD metrics exporter.
//!
//! Exports metrics to a DogStatsD-compatible agent (Datadog, StatsD) over UDP.
//! The exporter handles UDP socket management, packet batching (respecting MTU),
//! and newline-delimited packet splitting via `memchr`.
//!
//! The actual metric serialization is provided by the caller via a closure,
//! making this exporter work with any metrics struct.

use std::time::Duration;

/// Configuration for the DogStatsD exporter.
#[derive(Clone)]
pub struct DogStatsDConfig {
    /// DogStatsD endpoint (host:port), e.g. "127.0.0.1:8125"
    pub endpoint: String,
    /// Export interval
    pub interval: Duration,
    /// Maximum UDP packet size (default: 8000 bytes)
    pub max_packet_size: usize,
}

impl Default for DogStatsDConfig {
    fn default() -> Self {
        Self {
            endpoint: "127.0.0.1:8125".to_string(),
            interval: Duration::from_secs(10),
            max_packet_size: 8000,
        }
    }
}

impl DogStatsDConfig {
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

    pub fn with_max_packet_size(mut self, size: usize) -> Self {
        self.max_packet_size = size;
        self
    }
}

/// Run the DogStatsD export loop.
///
/// `export_fn` is called each cycle with a `&mut String` buffer. The closure
/// should append newline-delimited DogStatsD metric lines (typically via
/// `ExportMetrics::export_dogstatsd_delta`). The exporter handles UDP batching
/// and sending.
///
/// Runs until `cancel` is triggered. On cancellation, returns immediately
/// without a final export (DogStatsD is fire-and-forget).
///
/// # Example
///
/// ```ignore
/// use std::sync::Arc;
///
/// use fast_telemetry_export::dogstatsd::{DogStatsDConfig, run};
/// use tokio_util::sync::CancellationToken;
///
/// let metrics = Arc::new(MyMetrics::new());
/// let mut state = MyMetricsExportState::new();
/// let tags: Vec<(&str, &str)> = vec![("service", "myapp")];
/// let cancel = CancellationToken::new();
/// let config = DogStatsDConfig::new("127.0.0.1:8125");
///
/// let m = metrics.clone();
/// tokio::spawn(run(config, cancel, move |output| {
///     m.export_dogstatsd_delta(output, &tags, &mut state);
/// }));
/// ```
///
/// `MyMetricsExportState` is the derive-generated state type for delta
/// DogStatsD export. Keep one state instance per export sink.
pub async fn run<F>(
    config: DogStatsDConfig,
    cancel: tokio_util::sync::CancellationToken,
    mut export_fn: F,
) where
    F: FnMut(&mut String),
{
    use tokio::net::UdpSocket;
    use tokio::time::MissedTickBehavior;

    log::info!("Starting DogStatsD exporter, endpoint={}", config.endpoint);

    let socket = match UdpSocket::bind("0.0.0.0:0").await {
        Ok(s) => s,
        Err(e) => {
            log::error!("Failed to bind UDP socket for DogStatsD export: {e}");
            return;
        }
    };

    if let Err(e) = socket.connect(&config.endpoint).await {
        log::error!("Failed to connect UDP socket to {}: {e}", config.endpoint);
        return;
    }

    let max_packet_size = config.max_packet_size;
    let mut output = String::with_capacity(16384);
    let mut batch = Vec::<u8>::with_capacity(max_packet_size);

    let mut interval = tokio::time::interval(config.interval);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    interval.tick().await;

    loop {
        tokio::select! {
            _ = interval.tick() => {}
            _ = cancel.cancelled() => {
                log::info!("DogStatsD exporter shutting down");
                return;
            }
        }

        output.clear();
        export_fn(&mut output);

        if output.is_empty() {
            continue;
        }

        let output_bytes = output.as_bytes();
        batch.clear();

        let mut total_sent = 0usize;
        let mut batch_count = 0usize;
        let mut metric_count = 0usize;
        let mut start = 0usize;

        for nl in memchr::memchr_iter(b'\n', output_bytes) {
            let end = nl + 1;
            let line = &output_bytes[start..end];
            let line_len = line.len();
            metric_count += 1;

            if line_len > max_packet_size {
                log::warn!(
                    "Dropping oversized metric line ({line_len} bytes, max {max_packet_size})"
                );
                start = end;
                continue;
            }

            if !batch.is_empty() && batch.len() + line_len > max_packet_size {
                match socket.send(&batch).await {
                    Ok(n) => {
                        total_sent += n;
                        batch_count += 1;
                    }
                    Err(e) => log::warn!("Failed to send DogStatsD batch: {e}"),
                }
                batch.clear();
            }

            batch.extend_from_slice(line);
            start = end;
        }

        // Handle trailing bytes if output didn't end with '\n'
        if start < output_bytes.len() {
            let line = &output_bytes[start..];
            let line_len = line.len();
            metric_count += 1;

            if line_len <= max_packet_size {
                if !batch.is_empty() && batch.len() + line_len > max_packet_size {
                    match socket.send(&batch).await {
                        Ok(n) => {
                            total_sent += n;
                            batch_count += 1;
                        }
                        Err(e) => log::warn!("Failed to send DogStatsD batch: {e}"),
                    }
                    batch.clear();
                }
                batch.extend_from_slice(line);
            } else {
                log::warn!("Dropping oversized trailing metric ({line_len} bytes)");
            }
        }

        if !batch.is_empty() {
            match socket.send(&batch).await {
                Ok(n) => {
                    total_sent += n;
                    batch_count += 1;
                }
                Err(e) => log::warn!("Failed to send final DogStatsD batch: {e}"),
            }
        }

        log::debug!(
            "DogStatsD export: {metric_count} metrics, {batch_count} batches, {total_sent} bytes"
        );
    }
}
