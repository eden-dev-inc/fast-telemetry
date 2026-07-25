//! Reusable acknowledged OTLP/HTTP protobuf transport.
//!
//! This module deliberately does not log or retry. Callers decide how to
//! surface diagnostics and when a request is safe to retry.

use std::fmt;
use std::io::Write;
use std::time::{Duration, SystemTime};

use flate2::Compression;
use flate2::write::GzEncoder;
use prost::Message;
use reqwest::header::{
    CONTENT_ENCODING, CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue, RETRY_AFTER,
};

use super::pb;

const DEFAULT_GZIP_THRESHOLD: usize = 1024;
const DEFAULT_MAX_RESPONSE_BYTES: usize = 64 * 1024;
const MAX_ERROR_BODY_BYTES: usize = 8 * 1024;
const USER_AGENT: &str = concat!(
    "OTel-OTLP-Exporter-Rust/fast-telemetry-export-",
    env!("CARGO_PKG_VERSION")
);

/// TLS material for an OTLP HTTP client.
#[derive(Clone, Default)]
pub struct OtlpTlsConfig {
    /// Additional PEM-encoded CA certificate bundles.
    pub ca_certificates_pem: Vec<Vec<u8>>,
    /// PEM containing a client certificate chain and private key.
    pub client_identity_pem: Option<Vec<u8>>,
}

/// Shared configuration for acknowledged OTLP HTTP requests.
#[derive(Clone)]
pub struct OtlpHttpConfig {
    /// Collector base endpoint. Signal paths are appended automatically.
    pub endpoint: String,
    /// Request timeout.
    pub timeout: Duration,
    /// Headers applied to every request.
    pub headers: Vec<(String, String)>,
    /// Minimum uncompressed protobuf size before gzip is used.
    pub gzip_threshold: usize,
    /// Maximum response body retained in memory.
    pub max_response_bytes: usize,
    /// Optional additional trust roots and mTLS identity.
    pub tls: OtlpTlsConfig,
}

impl Default for OtlpHttpConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://localhost:4318".to_string(),
            timeout: Duration::from_secs(10),
            headers: Vec::new(),
            gzip_threshold: DEFAULT_GZIP_THRESHOLD,
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
            tls: OtlpTlsConfig::default(),
        }
    }
}

impl OtlpHttpConfig {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            ..Self::default()
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    pub fn with_gzip_threshold(mut self, threshold: usize) -> Self {
        self.gzip_threshold = threshold;
        self
    }

    pub fn with_max_response_bytes(mut self, maximum: usize) -> Self {
        self.max_response_bytes = maximum;
        self
    }

    pub fn with_ca_certificate_pem(mut self, pem: impl Into<Vec<u8>>) -> Self {
        self.tls.ca_certificates_pem.push(pem.into());
        self
    }

    pub fn with_client_identity_pem(mut self, pem: impl Into<Vec<u8>>) -> Self {
        self.tls.client_identity_pem = Some(pem.into());
        self
    }
}

/// Classification used by retrying exporters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OtlpHttpErrorKind {
    /// The endpoint, header, timeout, or TLS configuration is invalid.
    Configuration,
    /// The protobuf request or gzip body could not be encoded.
    Encode,
    /// The request could not be sent or its response body could not be read.
    Transport,
    /// The collector returned a status that callers may safely retry.
    RetryableStatus,
    /// The collector rejected the request payload as invalid or too large.
    InvalidPayload,
    /// The collector returned a permanent HTTP status.
    TerminalStatus,
    /// A successful response contained invalid protobuf.
    Decode,
}

/// A structured OTLP request failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OtlpHttpError {
    /// Stable error classification for retry and health decisions.
    pub kind: OtlpHttpErrorKind,
    /// Human-readable diagnostic that is safe to surface outside logging.
    pub message: String,
    /// HTTP status when the collector returned a non-success response.
    pub status: Option<u16>,
    /// Collector-provided retry delay, when present and valid.
    pub retry_after: Option<Duration>,
}

impl OtlpHttpError {
    fn new(kind: OtlpHttpErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            status: None,
            retry_after: None,
        }
    }

    pub const fn is_retryable(&self) -> bool {
        matches!(
            self.kind,
            OtlpHttpErrorKind::Transport | OtlpHttpErrorKind::RetryableStatus
        )
    }

    pub const fn is_invalid_payload(&self) -> bool {
        matches!(self.kind, OtlpHttpErrorKind::InvalidPayload)
    }

    pub const fn is_terminal(&self) -> bool {
        matches!(
            self.kind,
            OtlpHttpErrorKind::Configuration
                | OtlpHttpErrorKind::Encode
                | OtlpHttpErrorKind::TerminalStatus
                | OtlpHttpErrorKind::Decode
        )
    }
}

impl fmt::Display for OtlpHttpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(status) = self.status {
            write!(f, "OTLP HTTP {status}: {}", self.message)
        } else {
            f.write_str(&self.message)
        }
    }
}

impl std::error::Error for OtlpHttpError {}

/// Acknowledged result from an OTLP request.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OtlpExportOutcome {
    /// Signal items accepted by the collector.
    pub accepted: u64,
    /// Signal items rejected by the collector.
    pub rejected: u64,
    /// Optional collector warning or partial-rejection diagnostic.
    pub message: Option<String>,
}

/// Cloneable OTLP HTTP client shared by logs, metrics, and traces.
#[derive(Clone)]
pub struct OtlpHttpClient {
    client: reqwest::Client,
    endpoint: String,
    headers: HeaderMap,
    gzip_threshold: usize,
    max_response_bytes: usize,
}

impl OtlpHttpClient {
    pub fn new(config: OtlpHttpConfig) -> Result<Self, OtlpHttpError> {
        if config.timeout.is_zero() {
            return Err(OtlpHttpError::new(
                OtlpHttpErrorKind::Configuration,
                "OTLP request timeout must be greater than zero",
            ));
        }
        if config.max_response_bytes == 0 {
            return Err(OtlpHttpError::new(
                OtlpHttpErrorKind::Configuration,
                "OTLP maximum response bytes must be greater than zero",
            ));
        }
        let parsed =
            reqwest::Url::parse(config.endpoint.trim_end_matches('/')).map_err(|error| {
                OtlpHttpError::new(
                    OtlpHttpErrorKind::Configuration,
                    format!("invalid OTLP endpoint: {error}"),
                )
            })?;
        if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
            return Err(OtlpHttpError::new(
                OtlpHttpErrorKind::Configuration,
                "OTLP endpoint must be an absolute http:// or https:// URL",
            ));
        }
        if parsed.query().is_some() || parsed.fragment().is_some() {
            return Err(OtlpHttpError::new(
                OtlpHttpErrorKind::Configuration,
                "OTLP endpoint must not contain a query or fragment",
            ));
        }

        let mut headers = HeaderMap::new();
        for (name, value) in config.headers {
            let name = HeaderName::from_bytes(name.as_bytes()).map_err(|error| {
                OtlpHttpError::new(
                    OtlpHttpErrorKind::Configuration,
                    format!("invalid OTLP header name: {error}"),
                )
            })?;
            let value = HeaderValue::from_str(&value).map_err(|error| {
                OtlpHttpError::new(
                    OtlpHttpErrorKind::Configuration,
                    format!("invalid OTLP header value: {error}"),
                )
            })?;
            headers.append(name, value);
        }

        let mut builder = reqwest::Client::builder()
            .timeout(config.timeout)
            .user_agent(USER_AGENT);
        for bundle in config.tls.ca_certificates_pem {
            let certificates = reqwest::Certificate::from_pem_bundle(&bundle).map_err(|error| {
                OtlpHttpError::new(
                    OtlpHttpErrorKind::Configuration,
                    format!("invalid OTLP CA certificate bundle: {error}"),
                )
            })?;
            builder = builder.tls_certs_merge(certificates);
        }
        if let Some(identity_pem) = config.tls.client_identity_pem {
            let identity = reqwest::Identity::from_pem(&identity_pem).map_err(|error| {
                OtlpHttpError::new(
                    OtlpHttpErrorKind::Configuration,
                    format!("invalid OTLP client identity: {error}"),
                )
            })?;
            builder = builder.identity(identity);
        }
        let client = builder.build().map_err(|error| {
            OtlpHttpError::new(
                OtlpHttpErrorKind::Configuration,
                format!("failed to build OTLP HTTP client: {error}"),
            )
        })?;

        Ok(Self {
            client,
            endpoint: parsed.as_str().trim_end_matches('/').to_string(),
            headers,
            gzip_threshold: config.gzip_threshold,
            max_response_bytes: config.max_response_bytes,
        })
    }

    #[cfg(feature = "otlp-logs")]
    pub async fn export_logs(
        &self,
        request: &pb::ExportLogsServiceRequest,
    ) -> Result<OtlpExportOutcome, OtlpHttpError> {
        let sent: u64 = request
            .resource_logs
            .iter()
            .flat_map(|resource| &resource.scope_logs)
            .map(|scope| scope.log_records.len() as u64)
            .sum();
        let response: pb::ExportLogsServiceResponse =
            self.send_protobuf("/v1/logs", request).await?;
        let (rejected, message) = response
            .partial_success
            .map(|partial| (partial.rejected_log_records, partial.error_message))
            .unwrap_or_default();
        Ok(export_outcome(sent, rejected, message))
    }

    pub async fn export_metrics(
        &self,
        request: &pb::ExportMetricsServiceRequest,
    ) -> Result<OtlpExportOutcome, OtlpHttpError> {
        let sent: u64 = request
            .resource_metrics
            .iter()
            .flat_map(|resource| &resource.scope_metrics)
            .flat_map(|scope| &scope.metrics)
            .map(metric_data_point_count)
            .sum();
        let response: pb::ExportMetricsServiceResponse =
            self.send_protobuf("/v1/metrics", request).await?;
        let (rejected, message) = response
            .partial_success
            .map(|partial| (partial.rejected_data_points, partial.error_message))
            .unwrap_or_default();
        Ok(export_outcome(sent, rejected, message))
    }

    pub async fn export_traces(
        &self,
        request: &pb::ExportTraceServiceRequest,
    ) -> Result<OtlpExportOutcome, OtlpHttpError> {
        let sent: u64 = request
            .resource_spans
            .iter()
            .flat_map(|resource| &resource.scope_spans)
            .map(|scope| scope.spans.len() as u64)
            .sum();
        let response: pb::ExportTraceServiceResponse =
            self.send_protobuf("/v1/traces", request).await?;
        let (rejected, message) = response
            .partial_success
            .map(|partial| (partial.rejected_spans, partial.error_message))
            .unwrap_or_default();
        Ok(export_outcome(sent, rejected, message))
    }

    async fn send_protobuf<M, R>(&self, path: &str, message: &M) -> Result<R, OtlpHttpError>
    where
        M: Message,
        R: Message + Default,
    {
        let mut body = Vec::with_capacity(message.encoded_len());
        message.encode(&mut body).map_err(|error| {
            OtlpHttpError::new(
                OtlpHttpErrorKind::Encode,
                format!("failed to encode OTLP protobuf: {error}"),
            )
        })?;

        let (body, compressed) = gzip(&body, self.gzip_threshold)?;
        let mut request = self
            .client
            .post(format!("{}{path}", self.endpoint))
            .headers(self.headers.clone())
            .header(CONTENT_TYPE, "application/x-protobuf");
        if compressed {
            request = request.header(CONTENT_ENCODING, "gzip");
        }

        let mut response = request.body(body).send().await.map_err(|error| {
            OtlpHttpError::new(
                OtlpHttpErrorKind::Transport,
                format!("OTLP request failed: {error}"),
            )
        })?;
        let status = response.status();
        let retry_after = parse_retry_after(response.headers().get(RETRY_AFTER));
        let (response_body, response_truncated) =
            read_bounded_body(&mut response, self.max_response_bytes, status.is_success()).await?;

        if !status.is_success() {
            let kind = classify_status(status.as_u16());
            let mut error = OtlpHttpError::new(
                kind,
                body_text_with_truncation(&response_body, response_truncated),
            );
            error.status = Some(status.as_u16());
            error.retry_after = retry_after;
            return Err(error);
        }

        R::decode(response_body.as_ref()).map_err(|error| {
            OtlpHttpError::new(
                OtlpHttpErrorKind::Decode,
                format!("failed to decode OTLP response: {error}"),
            )
        })
    }
}

async fn read_bounded_body(
    response: &mut reqwest::Response,
    maximum: usize,
    success: bool,
) -> Result<(Vec<u8>, bool), OtlpHttpError> {
    if success
        && response
            .content_length()
            .is_some_and(|length| length > maximum as u64)
    {
        return Err(OtlpHttpError::new(
            OtlpHttpErrorKind::Decode,
            format!("OTLP response exceeded the configured {maximum}-byte limit"),
        ));
    }

    let capacity = response
        .content_length()
        .and_then(|length| usize::try_from(length).ok())
        .unwrap_or_default()
        .min(maximum);
    let mut body = Vec::with_capacity(capacity);
    let mut truncated = false;
    while let Some(chunk) = response.chunk().await.map_err(|error| {
        OtlpHttpError::new(
            OtlpHttpErrorKind::Transport,
            format!("failed to read OTLP response: {error}"),
        )
    })? {
        let remaining = maximum.saturating_sub(body.len());
        if chunk.len() > remaining {
            body.extend_from_slice(&chunk[..remaining]);
            truncated = true;
            break;
        }
        body.extend_from_slice(&chunk);
    }

    if success && truncated {
        return Err(OtlpHttpError::new(
            OtlpHttpErrorKind::Decode,
            format!("OTLP response exceeded the configured {maximum}-byte limit"),
        ));
    }
    Ok((body, truncated))
}

fn metric_data_point_count(metric: &pb::Metric) -> u64 {
    let count = match metric.data.as_ref() {
        Some(pb::metric::Data::Gauge(data)) => data.data_points.len(),
        Some(pb::metric::Data::Sum(data)) => data.data_points.len(),
        Some(pb::metric::Data::Histogram(data)) => data.data_points.len(),
        Some(pb::metric::Data::ExponentialHistogram(data)) => data.data_points.len(),
        Some(pb::metric::Data::Summary(data)) => data.data_points.len(),
        None => 0,
    };
    count.try_into().unwrap_or(u64::MAX)
}

fn export_outcome(sent: u64, reported_rejected: i64, message: String) -> OtlpExportOutcome {
    let rejected = u64::try_from(reported_rejected).unwrap_or(0).min(sent);
    OtlpExportOutcome {
        accepted: sent - rejected,
        rejected,
        message: non_empty(message),
    }
}

fn non_empty(value: String) -> Option<String> {
    (!value.is_empty()).then_some(value)
}

fn gzip(body: &[u8], threshold: usize) -> Result<(Vec<u8>, bool), OtlpHttpError> {
    if body.len() < threshold {
        return Ok((body.to_vec(), false));
    }
    let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
    encoder.write_all(body).map_err(|error| {
        OtlpHttpError::new(
            OtlpHttpErrorKind::Encode,
            format!("failed to gzip OTLP request: {error}"),
        )
    })?;
    let compressed = encoder.finish().map_err(|error| {
        OtlpHttpError::new(
            OtlpHttpErrorKind::Encode,
            format!("failed to finish OTLP gzip stream: {error}"),
        )
    })?;
    Ok((compressed, true))
}

fn classify_status(status: u16) -> OtlpHttpErrorKind {
    match status {
        400 | 413 => OtlpHttpErrorKind::InvalidPayload,
        429 | 502..=504 => OtlpHttpErrorKind::RetryableStatus,
        _ => OtlpHttpErrorKind::TerminalStatus,
    }
}

fn parse_retry_after(value: Option<&HeaderValue>) -> Option<Duration> {
    let value = value?.to_str().ok()?;
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    let deadline = httpdate::parse_http_date(value).ok()?;
    deadline.duration_since(SystemTime::now()).ok()
}

#[cfg(test)]
fn body_text(body: &[u8]) -> String {
    body_text_with_truncation(body, false)
}

fn body_text_with_truncation(body: &[u8], response_truncated: bool) -> String {
    let truncated = &body[..body.len().min(MAX_ERROR_BODY_BYTES)];
    let mut text = String::from_utf8_lossy(truncated).into_owned();
    if text.is_empty() {
        text = "collector rejected OTLP request".to_string();
    } else if response_truncated || truncated.len() != body.len() {
        text.push('…');
    }
    text
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use hegel::TestCase;
    use hegel::generators as gs;

    use super::*;

    #[test]
    fn validates_endpoint_and_headers() {
        assert!(OtlpHttpClient::new(OtlpHttpConfig::new("not a url")).is_err());
        assert!(
            OtlpHttpClient::new(
                OtlpHttpConfig::new("http://localhost:4318").with_timeout(Duration::ZERO)
            )
            .is_err()
        );
        assert!(
            OtlpHttpClient::new(
                OtlpHttpConfig::new("http://localhost:4318").with_max_response_bytes(0)
            )
            .is_err()
        );
        assert!(
            OtlpHttpClient::new(
                OtlpHttpConfig::new("http://localhost:4318").with_header("bad\nname", "value")
            )
            .is_err()
        );
    }

    #[test]
    fn validates_tls_material_at_construction() {
        let ca_error = OtlpHttpClient::new(
            OtlpHttpConfig::new("https://localhost:4318").with_ca_certificate_pem(
                b"-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n".to_vec(),
            ),
        )
        .err()
        .expect("invalid CA material must fail");
        assert_eq!(ca_error.kind, OtlpHttpErrorKind::Configuration);

        let identity_error = OtlpHttpClient::new(
            OtlpHttpConfig::new("https://localhost:4318")
                .with_client_identity_pem(b"not an identity".to_vec()),
        )
        .err()
        .expect("invalid client identity must fail");
        assert_eq!(identity_error.kind, OtlpHttpErrorKind::Configuration);
    }

    #[test]
    fn classifies_statuses() {
        assert_eq!(classify_status(400), OtlpHttpErrorKind::InvalidPayload);
        assert_eq!(classify_status(413), OtlpHttpErrorKind::InvalidPayload);
        assert_eq!(classify_status(429), OtlpHttpErrorKind::RetryableStatus);
        assert_eq!(classify_status(502), OtlpHttpErrorKind::RetryableStatus);
        assert_eq!(classify_status(503), OtlpHttpErrorKind::RetryableStatus);
        assert_eq!(classify_status(504), OtlpHttpErrorKind::RetryableStatus);
        assert_eq!(classify_status(408), OtlpHttpErrorKind::TerminalStatus);
        assert_eq!(classify_status(425), OtlpHttpErrorKind::TerminalStatus);
        assert_eq!(classify_status(500), OtlpHttpErrorKind::TerminalStatus);
        assert_eq!(classify_status(505), OtlpHttpErrorKind::TerminalStatus);
        assert_eq!(classify_status(401), OtlpHttpErrorKind::TerminalStatus);
    }

    #[test]
    fn parses_retry_after_seconds() {
        let value = HeaderValue::from_static("12");
        assert_eq!(
            parse_retry_after(Some(&value)),
            Some(Duration::from_secs(12))
        );
    }

    #[hegel::test(test_cases = 500)]
    fn generated_statuses_follow_retry_and_payload_policy(tc: TestCase) {
        let status = tc.draw(gs::integers::<u16>());
        let expected = match status {
            400 | 413 => OtlpHttpErrorKind::InvalidPayload,
            429 | 502..=504 => OtlpHttpErrorKind::RetryableStatus,
            _ => OtlpHttpErrorKind::TerminalStatus,
        };
        let kind = classify_status(status);
        let error = OtlpHttpError::new(kind, "generated status");

        assert_eq!(kind, expected);
        assert_eq!(
            error.is_retryable(),
            kind == OtlpHttpErrorKind::RetryableStatus
        );
        assert_eq!(
            error.is_invalid_payload(),
            kind == OtlpHttpErrorKind::InvalidPayload
        );
        assert_eq!(
            error.is_terminal(),
            kind == OtlpHttpErrorKind::TerminalStatus
        );
    }

    #[hegel::test(test_cases = 250)]
    fn generated_gzip_payloads_round_trip_at_every_threshold(tc: TestCase) {
        let body = tc.draw(gs::binary().max_size(16 * 1024));
        let threshold = tc.draw(gs::integers::<u16>().max_value(16 * 1024)) as usize;
        let (encoded, compressed) = gzip(&body, threshold).expect("in-memory gzip");

        assert_eq!(compressed, body.len() >= threshold);
        if compressed {
            let mut decoded = Vec::new();
            flate2::read::GzDecoder::new(encoded.as_slice())
                .read_to_end(&mut decoded)
                .expect("decode generated gzip payload");
            assert_eq!(decoded, body);
        } else {
            assert_eq!(encoded, body);
        }
    }

    #[hegel::test(test_cases = 250)]
    fn generated_retry_after_seconds_are_preserved(tc: TestCase) {
        let seconds = tc.draw(gs::integers::<u32>()) as u64;
        let value = HeaderValue::from_str(&seconds.to_string()).expect("numeric header value");

        assert_eq!(
            parse_retry_after(Some(&value)),
            Some(Duration::from_secs(seconds))
        );
    }

    #[hegel::test(test_cases = 250)]
    fn generated_error_bodies_are_bounded_and_mark_truncation(tc: TestCase) {
        let body = tc.draw(gs::binary().max_size(MAX_ERROR_BODY_BYTES * 2));
        let text = body_text(&body);

        assert!(!text.is_empty());
        assert!(
            text.len()
                <= MAX_ERROR_BODY_BYTES
                    .saturating_mul(3)
                    .saturating_add('…'.len_utf8())
        );
        assert_eq!(text.ends_with('…'), body.len() > MAX_ERROR_BODY_BYTES);
    }

    #[hegel::test(test_cases = 300)]
    fn generated_outcomes_conserve_the_sent_signal_count(tc: TestCase) {
        let sent = tc.draw(gs::integers::<u32>()) as u64;
        let reported_rejected = tc.draw(gs::integers::<i32>()) as i64;
        let include_message = tc.draw(gs::booleans());
        let message = if include_message {
            "collector warning".to_string()
        } else {
            String::new()
        };
        let outcome = export_outcome(sent, reported_rejected, message);

        assert_eq!(outcome.accepted.saturating_add(outcome.rejected), sent);
        assert!(outcome.rejected <= sent);
        assert_eq!(outcome.message.is_some(), include_message);
    }

    #[test]
    fn counts_metric_data_points_instead_of_metric_messages() {
        let metrics = [
            pb::Metric {
                data: Some(pb::metric::Data::Gauge(pb::OtlpGauge {
                    data_points: vec![pb::NumberDataPoint::default(); 2],
                })),
                ..Default::default()
            },
            pb::Metric {
                data: Some(pb::metric::Data::Sum(pb::Sum {
                    data_points: vec![pb::NumberDataPoint::default(); 3],
                    ..Default::default()
                })),
                ..Default::default()
            },
            pb::Metric {
                data: Some(pb::metric::Data::Histogram(pb::OtlpHistogram {
                    data_points: vec![pb::HistogramDataPoint::default(); 4],
                    ..Default::default()
                })),
                ..Default::default()
            },
            pb::Metric {
                data: Some(pb::metric::Data::ExponentialHistogram(
                    pb::OtlpExpHistogram {
                        data_points: vec![pb::ExponentialHistogramDataPoint::default(); 5],
                        ..Default::default()
                    },
                )),
                ..Default::default()
            },
            pb::Metric {
                data: Some(pb::metric::Data::Summary(pb::Summary {
                    data_points: vec![pb::SummaryDataPoint::default(); 6],
                })),
                ..Default::default()
            },
        ];

        assert_eq!(metrics.iter().map(metric_data_point_count).sum::<u64>(), 20);
    }

    #[test]
    fn every_error_kind_has_one_exporter_action() {
        for kind in [
            OtlpHttpErrorKind::Configuration,
            OtlpHttpErrorKind::Encode,
            OtlpHttpErrorKind::Transport,
            OtlpHttpErrorKind::RetryableStatus,
            OtlpHttpErrorKind::InvalidPayload,
            OtlpHttpErrorKind::TerminalStatus,
            OtlpHttpErrorKind::Decode,
        ] {
            let error = OtlpHttpError::new(kind, "classification");
            let actions = [
                error.is_retryable(),
                error.is_invalid_payload(),
                error.is_terminal(),
            ];
            assert_eq!(actions.into_iter().filter(|selected| *selected).count(), 1);
        }
    }
}
