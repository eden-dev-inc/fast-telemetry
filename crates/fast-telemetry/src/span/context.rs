//! Span context for W3C trace propagation and thread-local current span.
//!
//! `SpanContext` is a propagation value used at two boundaries:
//! 1. **Cross-service propagation** — parsing/encoding W3C `traceparent` headers
//! 2. **Logging integration** — thread-local "current span" surfaced via
//!    [`current_trace_id()`] and [`current_span_id()`]
//!
//! It is NOT passed between functions for parent-child creation — use
//! [`Span::child()`](super::Span::child) instead.

use std::cell::Cell;

use super::ids::{SpanId, TraceId, encode_hex};

/// Lightweight span context for W3C propagation and logging integration.
#[derive(Clone, Copy, Debug)]
pub struct SpanContext {
    pub trace_id: TraceId,
    pub span_id: SpanId,
    pub trace_flags: u8,
}

impl SpanContext {
    pub const INVALID: Self = Self {
        trace_id: TraceId::INVALID,
        span_id: SpanId::INVALID,
        trace_flags: 0,
    };

    /// Parse a W3C `traceparent` header value.
    ///
    /// Format: `{version:2}-{trace_id:32}-{span_id:16}-{flags:2}`
    /// Example: `00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01`
    pub fn from_traceparent(header: &str) -> Option<Self> {
        let bytes = header.as_bytes();
        if bytes.len() != 55
            || bytes[0] != b'0'
            || bytes[1] != b'0'
            || bytes[2] != b'-'
            || bytes[35] != b'-'
            || bytes[52] != b'-'
        {
            return None;
        }
        // Slice bytes so malformed multibyte text cannot cross a str boundary.
        // Every hex byte is checked by the decoder; no ASCII prepass is needed.
        let trace_id = TraceId(decode_hex(&bytes[3..35])?);
        let span_id = SpanId(decode_hex(&bytes[36..52])?);
        let trace_flags = decode_hex::<1>(&bytes[53..55])?[0];
        if trace_id.is_invalid() || span_id.is_invalid() {
            return None;
        }
        Some(Self {
            trace_id,
            span_id,
            trace_flags,
        })
    }

    /// Encode as a W3C `traceparent` header value.
    pub fn to_traceparent(self) -> String {
        let mut encoded = *b"00-00000000000000000000000000000000-0000000000000000-00";
        encode_hex(self.trace_id.as_bytes(), &mut encoded[3..35]);
        encode_hex(self.span_id.as_bytes(), &mut encoded[36..52]);
        encode_hex(&[self.trace_flags], &mut encoded[53..55]);
        String::from_utf8(encoded.to_vec()).expect("traceparent encoding is ASCII")
    }
}

#[inline]
fn decode_digit(value: u8) -> u8 {
    let decimal = value.wrapping_sub(b'0');
    let alpha = (value | 0x20).wrapping_sub(b'a');
    if decimal < 10 {
        decimal
    } else if alpha < 6 {
        alpha + 10
    } else {
        u8::MAX
    }
}

#[inline]
fn decode_hex<const N: usize>(encoded: &[u8]) -> Option<[u8; N]> {
    if encoded.len() != N * 2 {
        return None;
    }
    let mut decoded = [0; N];
    let mut invalid = 0;
    for (slot, pair) in decoded.iter_mut().zip(encoded.chunks_exact(2)) {
        let hi = decode_digit(pair[0]);
        let lo = decode_digit(pair[1]);
        invalid |= hi | lo;
        *slot = (hi << 4) | lo;
    }
    (invalid < 16).then_some(decoded)
}

// ---------------------------------------------------------------------------
// Thread-local current span context (for logging integration)
// ---------------------------------------------------------------------------

thread_local! {
    static CURRENT: Cell<SpanContext> = const { Cell::new(SpanContext::INVALID) };
}

/// Get the trace ID of the currently entered span on this thread, if any.
///
/// Returns `None` if no span is currently entered or the trace ID is invalid.
/// Used by logging integration to correlate log entries with traces.
pub fn current_trace_id() -> Option<TraceId> {
    CURRENT.with(|cell| {
        let ctx = cell.get();
        if ctx.trace_id.is_invalid() {
            None
        } else {
            Some(ctx.trace_id)
        }
    })
}

/// Get the span ID of the currently entered span on this thread, if any.
///
/// Returns `None` if no span is currently entered or the span ID is invalid.
pub fn current_span_id() -> Option<SpanId> {
    CURRENT.with(|cell| {
        let ctx = cell.get();
        if ctx.span_id.is_invalid() {
            None
        } else {
            Some(ctx.span_id)
        }
    })
}

/// RAII guard that sets the thread-local current span context on creation
/// and restores the previous value on drop.
pub(crate) struct SpanEnterGuard {
    prev: SpanContext,
}

impl SpanEnterGuard {
    /// Enter a span context: set it as current and return a guard that
    /// restores the previous context on drop.
    #[inline]
    pub fn enter(ctx: SpanContext) -> Self {
        let prev = CURRENT.with(|cell| cell.replace(ctx));
        Self { prev }
    }
}

impl Drop for SpanEnterGuard {
    #[inline]
    fn drop(&mut self) {
        CURRENT.with(|cell| cell.set(self.prev));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn traceparent_encoding_matches_bytewise_reference_for_all_flags() {
        for value in 0..=u8::MAX {
            let context = SpanContext {
                trace_id: TraceId(std::array::from_fn(|i| value.wrapping_add(i as u8))),
                span_id: SpanId(std::array::from_fn(|i| value.wrapping_sub(i as u8))),
                trace_flags: value,
            };
            let trace_hex: String = context
                .trace_id
                .0
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect();
            let span_hex: String = context
                .span_id
                .0
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect();
            let expected = format!("00-{trace_hex}-{span_hex}-{value:02x}");
            let encoded = context.to_traceparent();
            assert_eq!(encoded, expected);
            assert_eq!(encoded.len(), 55);
            let decoded = SpanContext::from_traceparent(&encoded).expect("decode encoded context");
            assert_eq!(decoded.trace_id, context.trace_id);
            assert_eq!(decoded.span_id, context.span_id);
            assert_eq!(decoded.trace_flags, value);
        }
        assert_eq!(
            SpanContext::INVALID.to_traceparent(),
            "00-00000000000000000000000000000000-0000000000000000-00"
        );
    }

    #[test]
    fn traceparent_roundtrip() {
        let ctx = SpanContext {
            trace_id: TraceId::from_hex("4bf92f3577b34da6a3ce929d0e0e4736").expect("valid"),
            span_id: SpanId::from_hex("00f067aa0ba902b7").expect("valid"),
            trace_flags: 0x01,
        };
        let header = ctx.to_traceparent();
        assert_eq!(
            header,
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        );

        let parsed = SpanContext::from_traceparent(&header).expect("should parse");
        assert_eq!(parsed.trace_id, ctx.trace_id);
        assert_eq!(parsed.span_id, ctx.span_id);
        assert_eq!(parsed.trace_flags, ctx.trace_flags);
    }

    #[test]
    fn traceparent_flags_zero() {
        let ctx = SpanContext {
            trace_id: TraceId::from_hex("aaaabbbbccccdddd1111222233334444").expect("valid"),
            span_id: SpanId::from_hex("1234567890abcdef").expect("valid"),
            trace_flags: 0x00,
        };
        let header = ctx.to_traceparent();
        assert!(header.ends_with("-00"));

        let parsed = SpanContext::from_traceparent(&header).expect("should parse");
        assert_eq!(parsed.trace_flags, 0x00);
    }

    #[test]
    fn traceparent_rejects_invalid() {
        // Too short.
        assert!(SpanContext::from_traceparent("00-abc-def-01").is_none());
        // Wrong version.
        assert!(
            SpanContext::from_traceparent(
                "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
            )
            .is_none()
        );
        // Missing dashes.
        assert!(
            SpanContext::from_traceparent(
                "00x4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
            )
            .is_none()
        );
        // All-zero trace ID.
        assert!(
            SpanContext::from_traceparent(
                "00-00000000000000000000000000000000-00f067aa0ba902b7-01"
            )
            .is_none()
        );
        // All-zero span ID.
        assert!(
            SpanContext::from_traceparent(
                "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01"
            )
            .is_none()
        );
    }

    #[test]
    fn version_zero_rejects_suffixes_and_non_ascii_without_panicking() {
        let valid = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        assert!(SpanContext::from_traceparent(&format!("{valid}-extra")).is_none());
        let mut unicode = valid.to_owned();
        unicode.replace_range(34..36, "é");
        assert_eq!(unicode.len(), 55);
        assert!(SpanContext::from_traceparent(&unicode).is_none());
    }

    #[test]
    fn thread_local_enter_exit() {
        assert!(current_trace_id().is_none());
        assert!(current_span_id().is_none());

        let ctx = SpanContext {
            trace_id: TraceId::random(),
            span_id: SpanId::random(),
            trace_flags: 1,
        };

        {
            let _guard = SpanEnterGuard::enter(ctx);
            assert_eq!(current_trace_id(), Some(ctx.trace_id));
            assert_eq!(current_span_id(), Some(ctx.span_id));

            // Nested enter.
            let inner_ctx = SpanContext {
                trace_id: ctx.trace_id,
                span_id: SpanId::random(),
                trace_flags: 1,
            };
            {
                let _inner_guard = SpanEnterGuard::enter(inner_ctx);
                assert_eq!(current_span_id(), Some(inner_ctx.span_id));
            }
            // Restored after inner guard drops.
            assert_eq!(current_span_id(), Some(ctx.span_id));
        }

        // Restored to invalid after outer guard drops.
        assert!(current_trace_id().is_none());
        assert!(current_span_id().is_none());
    }
    fn reference_parse(header: &str) -> Option<SpanContext> {
        let bytes = header.as_bytes();

        // Version 00 has exactly 2 + 1 + 32 + 1 + 16 + 1 + 2 = 55 ASCII bytes.
        if bytes.len() != 55 || !header.is_ascii() {
            return None;
        }

        // Version check (only support version 00)
        if bytes[0] != b'0' || bytes[1] != b'0' {
            return None;
        }
        if bytes[2] != b'-' || bytes[35] != b'-' || bytes[52] != b'-' {
            return None;
        }

        let trace_id = TraceId::from_hex(&header[3..35])?;
        let span_id = SpanId::from_hex(&header[36..52])?;

        let flags_hi = legacy_hex_digit(bytes[53])?;
        let flags_lo = legacy_hex_digit(bytes[54])?;
        let trace_flags = (flags_hi << 4) | flags_lo;

        // Reject all-zero IDs per W3C spec.
        if trace_id.is_invalid() || span_id.is_invalid() {
            return None;
        }

        Some(SpanContext {
            trace_id,
            span_id,
            trace_flags,
        })
    }

    fn legacy_hex_digit(c: u8) -> Option<u8> {
        match c {
            b'0'..=b'9' => Some(c - b'0'),
            b'a'..=b'f' => Some(c - b'a' + 10),
            b'A'..=b'F' => Some(c - b'A' + 10),
            _ => None,
        }
    }

    #[test]
    fn incoming_header_ascii_mutations_match_the_legacy_parser() {
        let header = b"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-a7";
        for position in 0..header.len() {
            for value in 0..=127 {
                let mut bytes = *header;
                bytes[position] = value;
                let text = std::str::from_utf8(&bytes).expect("ASCII fixture");
                let fields = |value: Option<SpanContext>| {
                    value.map(|v| (v.trace_id, v.span_id, v.trace_flags))
                };
                assert_eq!(
                    fields(SpanContext::from_traceparent(text)),
                    fields(reference_parse(text)),
                    "position {position}, byte {value}"
                );
            }
        }
    }

    #[test]
    fn incoming_header_equal_length_unicode_is_rejected_without_slicing_panics() {
        let header = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-a7";
        for replacement in ["é", "界", "🦀"] {
            for position in 0..=55 - replacement.len() {
                let mut text = header.to_string();
                text.replace_range(position..position + replacement.len(), replacement);
                assert_eq!(text.len(), 55);
                assert!(reference_parse(&text).is_none());
                assert!(SpanContext::from_traceparent(&text).is_none());
            }
        }
    }

    #[test]
    fn arithmetic_hex_digit_matches_the_legacy_classification_for_all_bytes() {
        for value in 0..=u8::MAX {
            let decoded = decode_digit(value);
            assert_eq!(
                (decoded < 16).then_some(decoded),
                legacy_hex_digit(value),
                "byte {value}"
            );
        }
    }
}
