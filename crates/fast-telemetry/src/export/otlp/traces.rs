//! Encode native spans without allocating intermediate protobuf objects.
//!
//! Field tags and default/presence behavior mirror OTLP0.30 and the existing
//! `CompletedSpan::to_otlp` conversion. Resource serialization stays with prost.
//! Golden tests compare complete request bytes, including nested oneof defaults.

use prost::Message;
use prost::bytes::BufMut;
use prost::encoding::{encode_varint, encoded_len_varint};

use super::pb;
use crate::{CompletedSpan, SpanAttribute, SpanEvent, SpanKind, SpanStatus, SpanValue};

/// An owned native trace request with its verified input span count.
///
/// Created by [`encode_native_trace_export_request`]. The payload is a complete
/// OTLP `ExportTraceServiceRequest`, independent of the source spans' lifetime.
pub struct EncodedTraceRequest {
    body: Vec<u8>,
    spans: u64,
}

impl EncodedTraceRequest {
    /// Encoded HTTP/protobuf payload, without compression or HTTP framing.
    pub fn as_bytes(&self) -> &[u8] {
        &self.body
    }

    /// Number of native spans included in this request.
    pub fn span_count(&self) -> u64 {
        self.spans
    }

    /// Transfer the encoded allocation to a transport.
    pub fn into_body(self) -> Vec<u8> {
        self.body
    }
}

/// Encode one resource/scope batch directly from borrowed native span data.
///
/// Produces the same wire bytes as `build_trace_export_request` with
/// `CompletedSpan::to_otlp`. Only the returned body allocates; native string,
/// ID and attribute storage is read in place. No source data is consumed.
/// Batches of 16–512 spans cache lengths in a bounded stack buffer (4 KiB on
/// 64-bit targets); other batch sizes recompute lengths without that buffer.
pub fn encode_native_trace_export_request(
    resource: &pb::Resource,
    scope_name: &str,
    spans: &[CompletedSpan],
) -> EncodedTraceRequest {
    encode_trace_iter(resource, scope_name, spans.iter(), spans.len())
}

/// Encode contiguous producer buckets directly, without flattening span data.
/// `max_spans` caps the request in drain order; the batch remains unchanged.
pub fn encode_native_trace_export_request_batches(
    resource: &pb::Resource,
    scope_name: &str,
    batch: &crate::SpanBatch,
    max_spans: usize,
) -> EncodedTraceRequest {
    encode_trace_iter(
        resource,
        scope_name,
        batch.iter().take(max_spans),
        batch.len().min(max_spans),
    )
}

/// Encode every span in a batch as bounded requests, preserving drain order.
///
/// Requests are encoded lazily, with one owned body allocation per request.
/// Native payloads remain borrowed and unchanged; no span records are flattened
/// or cloned. An empty batch yields no requests. Collect the requests before
/// clearing the batch if native payloads must be released before network I/O.
///
/// # Panics
/// Panics if `max_spans_per_request` is zero.
pub fn encode_native_trace_export_request_chunks<'a>(
    resource: &'a pb::Resource,
    scope_name: &'a str,
    batch: &'a crate::SpanBatch,
    max_spans_per_request: usize,
) -> impl Iterator<Item = EncodedTraceRequest> + 'a {
    assert!(
        max_spans_per_request > 0,
        "request span limit must be positive"
    );
    let mut spans = batch.iter();
    let mut remaining = batch.len();
    std::iter::from_fn(move || {
        if remaining == 0 {
            return None;
        }
        let count = remaining.min(max_spans_per_request);
        let request = encode_trace_iter(resource, scope_name, spans.clone().take(count), count);
        // Advance through the existing producer slices, retaining their backing
        // storage. The count came from the same immutable batch as the cursor.
        let last = spans.nth(count - 1);
        debug_assert!(last.is_some());
        remaining -= count;
        Some(request)
    })
}

fn encode_trace_iter<'a>(
    resource: &pb::Resource,
    scope_name: &str,
    spans: impl Iterator<Item = &'a CompletedSpan> + Clone,
    count: usize,
) -> EncodedTraceRequest {
    if (MIN_CACHED_SPANS..=MAX_CACHED_SPANS).contains(&count) {
        encode_cached_trace_request(resource, scope_name, spans, count)
    } else {
        encode_uncached_trace_request(resource, scope_name, spans, count)
    }
}

// Cache the top-level length pass for common batches, keeping one body allocation.
// Tiny batches avoid the fixed cache cost; large batches keep bounded stack use.
const MIN_CACHED_SPANS: usize = 16;
const MAX_CACHED_SPANS: usize = 512;

// Keep the cache frame (4 KiB on 64-bit targets) off the uncached path.
#[inline(never)]
fn encode_cached_trace_request<'a>(
    resource: &pb::Resource,
    scope_name: &str,
    spans: impl Iterator<Item = &'a CompletedSpan> + Clone,
    count: usize,
) -> EncodedTraceRequest {
    let mut lengths = [0_usize; MAX_CACHED_SPANS];
    let lengths = &mut lengths[..count];
    let mut spans_len = 0;
    for (span, length) in spans.clone().zip(lengths.iter_mut()) {
        *length = span_len(span);
        spans_len += delimited_len(*length);
    }
    let (mut body, total) = trace_request_buffer(resource, scope_name, spans_len);
    for (span, length) in spans.zip(lengths.iter()) {
        prefix(2, *length, &mut body);
        encode_span(span, &mut body);
    }
    debug_assert_eq!(body.len(), total);
    EncodedTraceRequest {
        body,
        spans: count as u64,
    }
}

#[inline(never)]
fn encode_uncached_trace_request<'a>(
    resource: &pb::Resource,
    scope_name: &str,
    spans: impl Iterator<Item = &'a CompletedSpan> + Clone,
    count: usize,
) -> EncodedTraceRequest {
    let spans_len: usize = spans
        .clone()
        .map(|span| delimited_len(span_len(span)))
        .sum();
    let (mut body, total) = trace_request_buffer(resource, scope_name, spans_len);
    for span in spans {
        prefix(2, span_len(span), &mut body);
        encode_span(span, &mut body);
    }
    debug_assert_eq!(body.len(), total);
    EncodedTraceRequest {
        body,
        spans: count as u64,
    }
}

#[inline(always)]
fn trace_request_buffer(
    resource: &pb::Resource,
    scope_name: &str,
    spans_len: usize,
) -> (Vec<u8>, usize) {
    let resource_len = resource.encoded_len();
    let scope_len = text_len(scope_name);
    let scope_spans_len = delimited_len(scope_len) + spans_len;
    let resource_spans_len = delimited_len(resource_len) + delimited_len(scope_spans_len);
    let total = delimited_len(resource_spans_len);
    let mut body = Vec::with_capacity(total);
    prefix(1, resource_spans_len, &mut body);
    prefix(1, resource_len, &mut body);
    resource.encode_raw(&mut body);
    prefix(2, scope_spans_len, &mut body);
    prefix(1, scope_len, &mut body);
    put_text(1, scope_name, &mut body);
    (body, total)
}

// Native lengths and positive integer attributes commonly fit in two bytes.
#[inline]
fn put_varint(value: u64, output: &mut Vec<u8>) {
    if value < 0x80 {
        output.put_u8(value as u8);
    } else if value < 0x4000 {
        output.put_slice(&[(value as u8) | 0x80, (value >> 7) as u8]);
    } else {
        encode_varint(value, output);
    }
}

// Small lengths and positive scalar values are the usual native case.
#[inline]
fn varint_len(value: u64) -> usize {
    if value < 0x80 {
        1
    } else if value < 0x4000 {
        2
    } else {
        encoded_len_varint(value)
    }
}

// Every native field emitted below has a tag in1..=15, hence a one-byte key.
#[inline]
fn delimited_len(length: usize) -> usize {
    1 + varint_len(length as u64) + length
}

#[inline]
fn prefix(tag: u32, length: usize, output: &mut Vec<u8>) {
    debug_assert!((1..=15).contains(&tag));
    output.put_u8(((tag << 3) | 2) as u8);
    put_varint(length as u64, output);
}

#[inline]
fn put_bytes(tag: u32, value: &[u8], output: &mut Vec<u8>) {
    prefix(tag, value.len(), output);
    output.put_slice(value);
}

#[inline]
fn text_len(value: &str) -> usize {
    if value.is_empty() {
        0
    } else {
        delimited_len(value.len())
    }
}

#[inline]
fn put_text(tag: u32, value: &str, output: &mut Vec<u8>) {
    if !value.is_empty() {
        put_bytes(tag, value.as_bytes(), output);
    }
}

#[inline]
fn timestamp_len(value: u64) -> usize {
    if value == 0 { 0 } else { 9 }
}

#[inline]
fn put_timestamp(tag: u32, value: u64, output: &mut Vec<u8>) {
    if value != 0 {
        output.put_u8(((tag << 3) | 1) as u8);
        output.put_u64_le(value);
    }
}

#[inline]
fn kind(kind: SpanKind) -> u64 {
    match kind {
        SpanKind::Internal => pb::OtlpSpanKind::Internal as u64,
        SpanKind::Server => pb::OtlpSpanKind::Server as u64,
        SpanKind::Client => pb::OtlpSpanKind::Client as u64,
        SpanKind::Producer => pb::OtlpSpanKind::Producer as u64,
        SpanKind::Consumer => pb::OtlpSpanKind::Consumer as u64,
    }
}

#[inline]
fn status_len(status: &SpanStatus) -> usize {
    match status {
        SpanStatus::Unset => 0,
        SpanStatus::Ok => 2,
        SpanStatus::Error { message } => 2 + text_len(message),
    }
}

#[inline]
fn span_len(span: &CompletedSpan) -> usize {
    let mut length = delimited_len(16)
        + delimited_len(8)
        + text_len(&span.name)
        + 1
        + varint_len(kind(span.kind))
        + timestamp_len(span.start_time_ns)
        + timestamp_len(span.end_time_ns);
    if !span.parent_span_id.is_invalid() {
        length += delimited_len(8);
    }
    length += span
        .attributes
        .iter()
        .map(|a| delimited_len(attribute_len(a)))
        .sum::<usize>();
    length += span
        .events
        .iter()
        .map(|e| delimited_len(event_len(e)))
        .sum::<usize>();
    if !matches!(span.status, SpanStatus::Unset) {
        length += delimited_len(status_len(&span.status));
    }
    length
}

#[inline(always)]
fn encode_span(span: &CompletedSpan, output: &mut Vec<u8>) {
    // IDs have mandatory fixed widths, including all-zero IDs.
    let mut ids = [0_u8; 28];
    ids[..2].copy_from_slice(&[10, 16]);
    ids[2..18].copy_from_slice(span.trace_id.as_bytes());
    ids[18..20].copy_from_slice(&[18, 8]);
    ids[20..].copy_from_slice(span.span_id.as_bytes());
    output.put_slice(&ids);
    if !span.parent_span_id.is_invalid() {
        put_bytes(4, span.parent_span_id.as_bytes(), output);
    }
    put_text(5, &span.name, output);
    if span.start_time_ns != 0 && span.end_time_ns != 0 {
        // The usual span emits kind and both fixed64 timestamps together.
        let mut tail = [0_u8; 20];
        tail[..3].copy_from_slice(&[6 << 3, kind(span.kind) as u8, (7 << 3) | 1]);
        tail[3..11].copy_from_slice(&span.start_time_ns.to_le_bytes());
        tail[11] = (8 << 3) | 1;
        tail[12..].copy_from_slice(&span.end_time_ns.to_le_bytes());
        output.put_slice(&tail);
    } else {
        output.put_u8(6 << 3);
        output.put_u8(kind(span.kind) as u8);
        put_timestamp(7, span.start_time_ns, output);
        put_timestamp(8, span.end_time_ns, output);
    }
    for attribute in &span.attributes {
        encode_attribute(9, attribute, output);
    }
    for event in &span.events {
        prefix(11, event_len(event), output);
        put_timestamp(1, event.time_ns, output);
        put_text(2, &event.name, output);
        for attribute in &event.attributes {
            encode_attribute(3, attribute, output);
        }
    }
    let code = match &span.status {
        SpanStatus::Unset => return,
        SpanStatus::Ok => pb::OtlpStatusCode::Ok,
        SpanStatus::Error { .. } => pb::OtlpStatusCode::Error,
    };
    prefix(15, status_len(&span.status), output);
    if let SpanStatus::Error { message } = &span.status {
        put_text(2, message, output);
    }
    output.put_u8(3 << 3);
    output.put_u8(code as u8);
}

#[inline]
fn event_len(event: &SpanEvent) -> usize {
    timestamp_len(event.time_ns)
        + text_len(&event.name)
        + event
            .attributes
            .iter()
            .map(|a| delimited_len(attribute_len(a)))
            .sum::<usize>()
}

#[inline]
fn value_len(value: &SpanValue) -> usize {
    match value {
        SpanValue::String(value) => delimited_len(value.len()),
        SpanValue::I64(value) => 1 + varint_len(*value as u64),
        SpanValue::F64(_) => 9,
        SpanValue::Bool(_) => 2,
        SpanValue::Uuid(_) => delimited_len(36),
    }
}

#[inline]
fn attribute_len(attribute: &SpanAttribute) -> usize {
    text_len(&attribute.key) + delimited_len(value_len(&attribute.value))
}

fn encode_attribute(tag: u32, attribute: &SpanAttribute, output: &mut Vec<u8>) {
    prefix(tag, attribute_len(attribute), output);
    put_text(1, &attribute.key, output);
    prefix(2, value_len(&attribute.value), output);
    match &attribute.value {
        // Oneof values remain present even when their scalar/string is empty.
        SpanValue::String(value) => put_bytes(1, value.as_bytes(), output),
        SpanValue::I64(value) => {
            output.put_u8(3 << 3);
            put_varint(*value as u64, output);
        }
        SpanValue::F64(value) => {
            output.put_u8((4 << 3) | 1);
            output.put_u64_le(value.to_bits());
        }
        SpanValue::Bool(value) => {
            output.put_u8(2 << 3);
            output.put_u8(u8::from(*value));
        }
        SpanValue::Uuid(value) => {
            let mut text = [0; 36];
            put_bytes(
                1,
                value.hyphenated().encode_lower(&mut text).as_bytes(),
                output,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::{build_resource, build_trace_export_request};
    use super::*;

    #[test]
    fn producer_buckets_encode_without_flattening_with_exact_limits() {
        let collector = std::sync::Arc::new(crate::SpanCollector::new(1, 4096));
        std::thread::scope(|scope| {
            for _ in 0..2 {
                let collector = &collector;
                scope.spawn(move || {
                    for index in 0..384 {
                        let mut span =
                            collector.start_span(format!("owned-{index}"), SpanKind::Server);
                        span.set_attribute("index", index as i64);
                    }
                    collector.flush_local();
                });
            }
        });
        let mut batch = crate::SpanBatch::new();
        collector.drain_batches(&mut batch);
        assert_eq!(batch.len(), 768);
        assert!(batch.buffers().count() >= 2);
        let pointers = batch.buffers().map(|b| b.as_ptr()).collect::<Vec<_>>();
        let resource = build_resource("bucket export", &[]);
        for limit in [0, 1, 15, 16, 64, 128, 512, 513, usize::MAX] {
            let expected = build_trace_export_request(
                &resource,
                "scope",
                batch
                    .iter()
                    .take(limit)
                    .map(CompletedSpan::to_otlp)
                    .collect(),
            )
            .encode_to_vec();
            let encoded =
                encode_native_trace_export_request_batches(&resource, "scope", &batch, limit);
            assert_eq!(encoded.as_bytes(), expected);
            assert_eq!(encoded.span_count(), 768.min(limit) as u64);
            assert_eq!(batch.len(), 768);
            assert_eq!(
                batch.buffers().map(|b| b.as_ptr()).collect::<Vec<_>>(),
                pointers
            );
        }
        for limit in [1, 15, 16, 17, 384, 511, 512, 513, 768, 1024, usize::MAX] {
            let mut offset = 0;
            for request in
                encode_native_trace_export_request_chunks(&resource, "scope", &batch, limit)
            {
                let count = limit.min(batch.len() - offset);
                let expected = build_trace_export_request(
                    &resource,
                    "scope",
                    batch
                        .iter()
                        .skip(offset)
                        .take(count)
                        .map(CompletedSpan::to_otlp)
                        .collect(),
                )
                .encode_to_vec();
                assert_eq!(
                    request.as_bytes(),
                    expected,
                    "limit={limit}, offset={offset}"
                );
                assert_eq!(request.span_count(), count as u64);
                offset += count;
            }
            assert_eq!(offset, 768);
            assert_eq!(batch.len(), 768);
            assert_eq!(
                batch.buffers().map(|b| b.as_ptr()).collect::<Vec<_>>(),
                pointers
            );
        }
    }

    #[test]
    fn empty_batch_yields_no_chunk_requests() {
        let batch = crate::SpanBatch::new();
        let resource = pb::Resource::default();
        assert_eq!(
            encode_native_trace_export_request_chunks(&resource, "", &batch, 512).count(),
            0
        );
    }

    #[test]
    fn chunk_requests_preserve_mixed_owned_payloads_across_sink_buckets() {
        let collector = std::sync::Arc::new(crate::SpanCollector::new(1, 2048));
        let sink = collector.span_sink(1025);
        for index in 0..1025 {
            let span = super::super::tests::ownership_span(
                index % 2 == 0,
                SpanKind::Client,
                crate::SpanId::INVALID,
                index % 3,
            );
            sink.publish(span).expect("fixture fits sink");
        }
        let mut batch = crate::SpanBatch::new();
        collector.drain_batches(&mut batch);
        assert_eq!(batch.len(), 1025);
        let resource = build_resource("mixed chunks Σ", &[("version", "1")]);
        for limit in [1, 15, 16, 17, 384, 512, 513, usize::MAX] {
            let mut offset = 0;
            for request in
                encode_native_trace_export_request_chunks(&resource, "scope", &batch, limit)
            {
                let count = (batch.len() - offset).min(limit);
                let expected = build_trace_export_request(
                    &resource,
                    "scope",
                    batch
                        .iter()
                        .skip(offset)
                        .take(count)
                        .map(CompletedSpan::to_otlp)
                        .collect(),
                )
                .encode_to_vec();
                assert_eq!(
                    request.as_bytes(),
                    expected,
                    "limit={limit}, offset={offset}"
                );
                assert_eq!(request.span_count(), count as u64);
                offset += count;
            }
            assert_eq!(offset, batch.len());
        }
    }

    #[test]
    #[should_panic(expected = "request span limit must be positive")]
    fn chunk_requests_reject_zero_limit() {
        let batch = crate::SpanBatch::new();
        let resource = pb::Resource::default();
        let _ = encode_native_trace_export_request_chunks(&resource, "", &batch, 0);
    }

    fn equivalent(resource: &pb::Resource, scope: &str, spans: &[CompletedSpan]) {
        let expected = build_trace_export_request(
            resource,
            scope,
            spans.iter().map(CompletedSpan::to_otlp).collect(),
        )
        .encode_to_vec();
        let encoded = encode_native_trace_export_request(resource, scope, spans);
        assert_eq!(encoded.span_count(), spans.len() as u64);
        assert_eq!(encoded.as_bytes(), expected);
        let body = encoded.into_body();
        let decoded = pb::ExportTraceServiceRequest::decode(body.as_slice())
            .expect("decode native wire batch");
        assert_eq!(decoded.encode_to_vec(), expected);
    }

    #[test]
    fn direct_requests_preserve_every_native_value_and_status() {
        let resource = build_resource("native Σ\0resource", &[("version", "1")]);
        let parent = crate::SpanId::from_hex("1234567890abcdef").expect("parent fixture");
        for kind in [
            SpanKind::Internal,
            SpanKind::Server,
            SpanKind::Client,
            SpanKind::Producer,
            SpanKind::Consumer,
        ] {
            for parent in [crate::SpanId::INVALID, parent] {
                for owned in [false, true] {
                    for status in 0..3 {
                        let span = super::super::tests::ownership_span(owned, kind, parent, status);
                        equivalent(&resource, "scope Σ\0", &[span]);
                    }
                }
            }
        }
    }

    #[test]
    fn direct_requests_preserve_empty_fields_and_presence() {
        equivalent(&pb::Resource::default(), "", &[]);
        let mut span = super::super::tests::ownership_span(
            false,
            SpanKind::Internal,
            crate::SpanId::INVALID,
            0,
        );
        span.trace_id = crate::TraceId::INVALID;
        span.span_id = crate::SpanId::INVALID;
        span.name = "".into();
        span.start_time_ns = 0;
        span.end_time_ns = 0;
        span.attributes = vec![
            SpanAttribute::new("", ""),
            SpanAttribute::new("", false),
            SpanAttribute::new("", 0_i64),
            SpanAttribute::new("", 0.0_f64),
            SpanAttribute::new("", -0.0_f64),
        ];
        span.events = vec![SpanEvent {
            name: "".into(),
            time_ns: 0,
            attributes: Vec::new(),
        }];
        equivalent(&pb::Resource::default(), "", &[span]);
    }

    #[test]
    fn direct_requests_preserve_partial_zero_and_maximum_timestamps() {
        for start in [0, 1, u64::MAX] {
            for end in [0, 1, u64::MAX] {
                let mut span = super::super::tests::ownership_span(
                    false,
                    SpanKind::Consumer,
                    crate::SpanId::INVALID,
                    0,
                );
                span.start_time_ns = start;
                span.end_time_ns = end;
                span.attributes.clear();
                span.events.clear();
                equivalent(&pb::Resource::default(), "", &[span]);
            }
        }
    }

    #[test]
    fn direct_batches_preserve_length_cache_boundaries_and_span_order() {
        let resource = build_resource("length-cache-boundary", &[]);
        for batch in [
            0, 1, 8, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129, 255, 256, 257, 511, 512,
            513, 1024, 4096,
        ] {
            let spans: Vec<_> = (0..batch)
                .map(|index| {
                    let mut span = super::super::tests::ownership_span(
                        index % 2 == 0,
                        SpanKind::Client,
                        crate::SpanId::INVALID,
                        index % 3,
                    );
                    span.name = format!("span-{index}-{}", "x".repeat(index % 131)).into();
                    span.attributes
                        .push(SpanAttribute::new("index", index as i64));
                    span
                })
                .collect();
            equivalent(&resource, "scope", &spans);
        }
    }

    #[test]
    fn direct_requests_preserve_varint_and_nested_message_boundaries() {
        for length in [0, 1, 126, 127, 128, 129, 254, 255, 16382, 16383, 16384] {
            let text = "x".repeat(length);
            let resource = build_resource(&text, &[("key", &text)]);
            let mut span = super::super::tests::ownership_span(
                true,
                SpanKind::Server,
                crate::SpanId::INVALID,
                2,
            );
            span.name = text.clone().into();
            span.status = SpanStatus::Error {
                message: text.clone().into(),
            };
            span.attributes
                .push(SpanAttribute::new(text.clone(), text.clone()));
            span.events[0].name = text.clone().into();
            equivalent(&resource, &text, &[span]);
        }
        let spans: Vec<_> = (0..512)
            .map(|index| {
                super::super::tests::ownership_span(
                    index % 2 == 0,
                    SpanKind::Client,
                    crate::SpanId::INVALID,
                    index % 3,
                )
            })
            .collect();
        equivalent(&build_resource("batch", &[]), "scope", &spans);
    }

    #[test]
    fn generated_batches_preserve_wire_bytes_and_arbitrary_float_bits() {
        use opentelemetry_proto::tonic::common::v1::{ArrayValue, KeyValueList};

        let mut state = 0x95d4_31ba_f012_6789_u64;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for batch in 0..256 {
            let text = format!("Σ\0{}", "x".repeat((next() % 260) as usize));
            let mut resource = build_resource(&text, &[("resource", &text)]);
            resource.dropped_attributes_count = next() as u32;
            resource.attributes.push(pb::KeyValue {
                key: text.clone(),
                value: Some(pb::AnyValue {
                    value: Some(pb::any_value::Value::ArrayValue(ArrayValue {
                        values: vec![pb::AnyValue {
                            value: Some(pb::any_value::Value::KvlistValue(KeyValueList {
                                values: vec![pb::KeyValue {
                                    key: text.clone(),
                                    value: Some(pb::AnyValue {
                                        value: Some(pb::any_value::Value::DoubleValue(
                                            f64::from_bits(next()),
                                        )),
                                    }),
                                }],
                            })),
                        }],
                    })),
                }),
            });
            let spans: Vec<_> = (0..batch % 9)
                .map(|index| {
                    let mut span = super::super::tests::ownership_span(
                        index % 2 == 0,
                        SpanKind::Producer,
                        crate::SpanId(next().to_le_bytes()),
                        index % 3,
                    );
                    let mut trace = [0; 16];
                    trace[..8].copy_from_slice(&next().to_le_bytes());
                    trace[8..].copy_from_slice(&next().to_le_bytes());
                    span.trace_id = crate::TraceId(trace);
                    span.span_id = crate::SpanId(next().to_le_bytes());
                    span.start_time_ns = next();
                    span.end_time_ns = next();
                    span.name = text.clone().into();
                    span.attributes.extend([
                        SpanAttribute::new(text.clone(), next() as i64),
                        SpanAttribute::new("float", f64::from_bits(next())),
                        SpanAttribute::new("bool", next() & 1 != 0),
                    ]);
                    span.events[0].time_ns = next();
                    span.events[0].name = text.clone().into();
                    span
                })
                .collect();
            equivalent(&resource, &text, &spans);
        }
    }

    #[test]
    fn integer_varint_boundaries_preserve_wire_bytes() {
        let mut span = super::super::tests::ownership_span(
            false,
            SpanKind::Internal,
            crate::SpanId::INVALID,
            0,
        );
        span.attributes.clear();
        for bits in 0..64 {
            let value = 1_u64 << bits;
            for value in [value.wrapping_sub(1), value, value.wrapping_add(1)] {
                span.attributes
                    .push(SpanAttribute::new("integer", value as i64));
            }
        }
        equivalent(&pb::Resource::default(), "", &[span]);
    }

    #[test]
    fn common_varints_match_prost_for_all_one_and_two_byte_values() {
        let mut expected = Vec::new();
        let mut actual = Vec::new();
        for value in 0..=0x4000 {
            expected.clear();
            actual.clear();
            encode_varint(value, &mut expected);
            put_varint(value, &mut actual);
            assert_eq!(actual, expected, "value {value}");
        }
        for bits in 0..64 {
            let value = 1_u64 << bits;
            for value in [
                value.wrapping_sub(1),
                value,
                value.wrapping_add(1),
                u64::MAX,
            ] {
                expected.clear();
                actual.clear();
                encode_varint(value, &mut expected);
                put_varint(value, &mut actual);
                assert_eq!(actual, expected, "value {value}");
            }
        }
    }
}
