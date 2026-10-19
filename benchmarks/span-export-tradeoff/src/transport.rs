pub use fast_telemetry::otlp::pb;
#[path = "../../../crates/fast-telemetry-export/src/otlp/http.rs"]
mod implementation;
pub use implementation::*;
