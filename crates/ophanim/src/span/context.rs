//! Span context for W3C trace propagation and thread-local current span.
//!
//! `SpanContext` is an internal type used at two boundaries:
//! 1. **Cross-service propagation** — parsing/encoding W3C `traceparent` headers
//! 2. **Logging integration** — thread-local "current span" for `extract_trace_context()`
//!
//! It is NOT passed between functions for parent-child creation — use
//! [`Span::child()`](super::Span::child) instead.

use std::cell::Cell;

use super::ids::{SpanId, TraceId};

/// Lightweight span context for W3C propagation and logging integration.
#[derive(Clone, Copy, Debug)]
pub(crate) struct SpanContext {
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

        // Minimum length: 2 + 1 + 32 + 1 + 16 + 1 + 2 = 55
        if bytes.len() < 55 {
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

        let flags_hi = hex_digit(bytes[53])?;
        let flags_lo = hex_digit(bytes[54])?;
        let trace_flags = (flags_hi << 4) | flags_lo;

        // Reject all-zero IDs per W3C spec.
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
        format!(
            "00-{}-{}-{:02x}",
            self.trace_id, self.span_id, self.trace_flags
        )
    }
}

fn hex_digit(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
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
    pub fn enter(ctx: SpanContext) -> Self {
        let prev = CURRENT.with(|cell| cell.replace(ctx));
        Self { prev }
    }
}

impl Drop for SpanEnterGuard {
    fn drop(&mut self) {
        CURRENT.with(|cell| cell.set(self.prev));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
