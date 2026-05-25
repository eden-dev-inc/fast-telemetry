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
#[cfg(feature = "tokio-runtime")]
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

/// Run the DogStatsD export loop on a monoio runtime.
///
/// This is the monoio-native counterpart to [`run`]. It uses
/// [`monoio::net::udp::UdpSocket`] and [`monoio::time::interval`], so the
/// caller must run it inside a monoio runtime with timers enabled.
#[cfg(feature = "monoio")]
pub async fn run_monoio<F>(
    config: DogStatsDConfig,
    cancel: tokio_util::sync::CancellationToken,
    mut export_fn: F,
) where
    F: FnMut(&mut String),
{
    use std::net::{SocketAddr, ToSocketAddrs};

    use monoio::net::udp::UdpSocket;
    use monoio::time::MissedTickBehavior;

    log::info!(
        "Starting monoio DogStatsD exporter, endpoint={}",
        config.endpoint
    );

    let endpoint = match config.endpoint.to_socket_addrs() {
        Ok(mut addrs) => match addrs.next() {
            Some(addr) => addr,
            None => {
                log::error!("DogStatsD endpoint resolved to no addresses");
                return;
            }
        },
        Err(e) => {
            log::error!(
                "Failed to resolve DogStatsD endpoint {}: {e}",
                config.endpoint
            );
            return;
        }
    };

    let bind_addr: SocketAddr = if endpoint.is_ipv4() {
        "0.0.0.0:0"
    } else {
        "[::]:0"
    }
    .parse()
    .expect("valid UDP bind address");

    let socket = match UdpSocket::bind(bind_addr) {
        Ok(s) => s,
        Err(e) => {
            log::error!("Failed to bind monoio UDP socket for DogStatsD export: {e}");
            return;
        }
    };

    if let Err(e) = socket.connect(endpoint).await {
        log::error!("Failed to connect monoio UDP socket to {endpoint}: {e}");
        return;
    }

    let max_packet_size = config.max_packet_size;
    let mut output = String::with_capacity(16384);
    let mut batch = Vec::<u8>::with_capacity(max_packet_size);

    let mut interval = monoio::time::interval(config.interval);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    interval.tick().await;

    loop {
        monoio::select! {
            _ = interval.tick() => {}
            _ = cancel.cancelled() => {
                log::info!("monoio DogStatsD exporter shutting down");
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

            if !batch.is_empty()
                && batch.len() + line_len > max_packet_size
                && let Some(n) = send_monoio_batch(&socket, &mut batch, "DogStatsD batch").await
            {
                total_sent += n;
                batch_count += 1;
            }

            batch.extend_from_slice(line);
            start = end;
        }

        if start < output_bytes.len() {
            let line = &output_bytes[start..];
            let line_len = line.len();
            metric_count += 1;

            if line_len <= max_packet_size {
                if !batch.is_empty()
                    && batch.len() + line_len > max_packet_size
                    && let Some(n) = send_monoio_batch(&socket, &mut batch, "DogStatsD batch").await
                {
                    total_sent += n;
                    batch_count += 1;
                }
                batch.extend_from_slice(line);
            } else {
                log::warn!("Dropping oversized trailing metric ({line_len} bytes)");
            }
        }

        if !batch.is_empty()
            && let Some(n) = send_monoio_batch(&socket, &mut batch, "final DogStatsD batch").await
        {
            total_sent += n;
            batch_count += 1;
        }

        log::debug!(
            "monoio DogStatsD export: {metric_count} metrics, {batch_count} batches, {total_sent} bytes"
        );
    }
}

/// Run the DogStatsD export loop on a compio runtime.
///
/// This is the compio-native counterpart to `run`. It uses
/// [`compio::net::UdpSocket`] and [`compio::time`], so the caller must run it
/// inside a compio runtime. `cancel` may be any future that completes when the
/// exporter should shut down.
#[cfg(feature = "compio")]
pub async fn run_compio<F>(
    config: DogStatsDConfig,
    cancel: impl std::future::Future<Output = ()>,
    mut export_fn: F,
) where
    F: FnMut(&mut String),
{
    use std::net::{SocketAddr, ToSocketAddrs};

    use compio::net::UdpSocket;
    use futures_util::{FutureExt as _, select};

    log::info!(
        "Starting compio DogStatsD exporter, endpoint={}",
        config.endpoint
    );

    let endpoint = match config.endpoint.to_socket_addrs() {
        Ok(mut addrs) => match addrs.next() {
            Some(addr) => addr,
            None => {
                log::error!("DogStatsD endpoint resolved to no addresses");
                return;
            }
        },
        Err(e) => {
            log::error!(
                "Failed to resolve DogStatsD endpoint {}: {e}",
                config.endpoint
            );
            return;
        }
    };

    let bind_addr: SocketAddr = if endpoint.is_ipv4() {
        "0.0.0.0:0"
    } else {
        "[::]:0"
    }
    .parse()
    .expect("valid UDP bind address");

    let socket = match UdpSocket::bind(bind_addr).await {
        Ok(s) => s,
        Err(e) => {
            log::error!("Failed to bind compio UDP socket for DogStatsD export: {e}");
            return;
        }
    };

    if let Err(e) = socket.connect(endpoint).await {
        log::error!("Failed to connect compio UDP socket to {endpoint}: {e}");
        return;
    }

    let max_packet_size = config.max_packet_size;
    let mut output = String::with_capacity(16384);
    let mut batch = Vec::<u8>::with_capacity(max_packet_size);
    let mut interval = compio::time::interval(config.interval);
    interval.tick().await;
    let cancel = cancel.fuse();
    let mut cancel = std::pin::pin!(cancel);

    loop {
        let tick = interval.tick();
        let tick = std::pin::pin!(tick);

        select! {
            _ = tick.fuse() => {},
            _ = cancel.as_mut() => {
                log::info!("compio DogStatsD exporter shutting down");
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

            if !batch.is_empty()
                && batch.len() + line_len > max_packet_size
                && let Some(n) = send_compio_batch(&socket, &mut batch, "DogStatsD batch").await
            {
                total_sent += n;
                batch_count += 1;
            }

            batch.extend_from_slice(line);
            start = end;
        }

        if start < output_bytes.len() {
            let line = &output_bytes[start..];
            let line_len = line.len();
            metric_count += 1;

            if line_len <= max_packet_size {
                if !batch.is_empty()
                    && batch.len() + line_len > max_packet_size
                    && let Some(n) = send_compio_batch(&socket, &mut batch, "DogStatsD batch").await
                {
                    total_sent += n;
                    batch_count += 1;
                }
                batch.extend_from_slice(line);
            } else {
                log::warn!("Dropping oversized trailing metric ({line_len} bytes)");
            }
        }

        if !batch.is_empty()
            && let Some(n) = send_compio_batch(&socket, &mut batch, "final DogStatsD batch").await
        {
            total_sent += n;
            batch_count += 1;
        }

        log::debug!(
            "compio DogStatsD export: {metric_count} metrics, {batch_count} batches, {total_sent} bytes"
        );
    }
}

#[cfg(feature = "monoio")]
async fn send_monoio_batch(
    socket: &monoio::net::udp::UdpSocket,
    batch: &mut Vec<u8>,
    context: &str,
) -> Option<usize> {
    let send_buf = std::mem::take(batch);
    let (result, mut send_buf) = socket.send(send_buf).await;
    send_buf.clear();
    *batch = send_buf;

    match result {
        Ok(n) => Some(n),
        Err(e) => {
            log::warn!("Failed to send monoio {context}: {e}");
            None
        }
    }
}

#[cfg(feature = "compio")]
async fn send_compio_batch(
    socket: &compio::net::UdpSocket,
    batch: &mut Vec<u8>,
    context: &str,
) -> Option<usize> {
    let send_buf = std::mem::take(batch);
    let compio::BufResult(result, mut send_buf) = socket.send(send_buf).await;
    send_buf.clear();
    *batch = send_buf;

    match result {
        Ok(n) => Some(n),
        Err(e) => {
            log::warn!("Failed to send compio {context}: {e}");
            None
        }
    }
}
