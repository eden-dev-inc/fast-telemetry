use fast_telemetry::SpanValue;
use fast_telemetry::otlp::pb;
use tracing::Level;

use crate::RecordedEvent;

impl RecordedEvent {
    /// Convert a standalone event into an OTLP log record. Drain with
    /// `LayerHandle::drain_events`, wrap with `build_log_export_request`, and
    /// send with `fast_telemetry_export::otlp::OtlpHttpClient::export_logs`.
    pub fn to_otlp(&self) -> pb::LogRecord {
        let severity_number = match self.level {
            Level::TRACE => 1,
            Level::DEBUG => 5,
            Level::INFO => 9,
            Level::WARN => 13,
            Level::ERROR => 17,
        };
        let mut attributes: Vec<pb::KeyValue> = self
            .event
            .attributes
            .iter()
            .map(|attribute| {
                let value = value(&attribute.value);
                pb::KeyValue {
                    key: attribute.key.to_string(),
                    value: Some(pb::AnyValue { value: Some(value) }),
                }
            })
            .collect();
        attributes.push(pb::KeyValue {
            key: "fast_telemetry.tracing.event_name".to_owned(),
            value: Some(pb::AnyValue {
                value: Some(pb::any_value::Value::StringValue(
                    self.event.name.to_string(),
                )),
            }),
        });
        let body = self
            .event
            .attributes
            .iter()
            .find(|a| a.key == "message")
            .map_or_else(
                || pb::any_value::Value::StringValue(self.event.name.to_string()),
                |a| value(&a.value),
            );
        pb::LogRecord {
            time_unix_nano: self.event.time_ns,
            observed_time_unix_nano: self.event.time_ns,
            severity_number,
            severity_text: self.level.to_string(),
            body: Some(pb::AnyValue { value: Some(body) }),
            attributes,
            trace_id: self
                .context
                .map_or_else(Vec::new, |ctx| ctx.trace_id.as_bytes().to_vec()),
            span_id: self
                .context
                .map_or_else(Vec::new, |ctx| ctx.span_id.as_bytes().to_vec()),
            flags: self.context.map_or(0, |ctx| u32::from(ctx.trace_flags)),
            ..Default::default()
        }
    }
}

fn value(value: &SpanValue) -> pb::any_value::Value {
    match value {
        SpanValue::String(v) => pb::any_value::Value::StringValue(v.to_string()),
        SpanValue::I64(v) => pb::any_value::Value::IntValue(*v),
        SpanValue::F64(v) => pb::any_value::Value::DoubleValue(*v),
        SpanValue::Bool(v) => pb::any_value::Value::BoolValue(*v),
        SpanValue::Uuid(v) => pb::any_value::Value::StringValue(v.to_string()),
    }
}
