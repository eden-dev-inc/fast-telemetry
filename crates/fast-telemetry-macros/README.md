# fast-telemetry-macros

Derive macros for the `fast-telemetry` crate.

## Macros

### `#[derive(LabelEnum)]`

Auto-generates `LabelEnum` trait implementation for enums used as metric labels.

```rust
use fast_telemetry::{LabeledCounter, DeriveLabel, LabelEnum};

#[derive(Copy, Clone, Debug, DeriveLabel)]
#[label_name = "method"]
enum HttpMethod {
    Get,
    Post,
    Put,
    Delete,
}

// Generates:
// impl LabelEnum for HttpMethod {
//     const CARDINALITY: usize = 4;
//     const LABEL_NAME: &'static str = "method";
//     fn as_index(self) -> usize { ... }
//     fn from_index(index: usize) -> Self { ... }
//     fn variant_name(self) -> &'static str { ... }
// }

let counter: LabeledCounter<HttpMethod> = LabeledCounter::new(4);
counter.inc(HttpMethod::Get);
```

#### Attributes

| Attribute | Target | Required | Description |
|-----------|--------|----------|-------------|
| `#[label_name = "..."]` | enum | Yes | Prometheus label name (e.g., `"method"`, `"error_type"`) |
| `#[label = "..."]` | variant | No | Override auto-generated variant name |

#### Snake Case Conversion

Variant names are automatically converted to snake_case:

```rust
#[derive(Copy, Clone, Debug, DeriveLabel)]
#[label_name = "error"]
enum ErrorType {
    NotFound,            // -> "not_found"
    InternalServerError, // -> "internal_server_error"
    BadRequest,          // -> "bad_request"
    #[label = "unknown"]
    Other,               // -> "unknown" (overridden)
}
```

---

### `#[derive(ExportMetrics)]`

Auto-generates Prometheus, DogStatsD, and optional OTLP export methods for
metric structs.

```rust
use fast_telemetry::{Counter, Gauge, Histogram, ExportMetrics};

#[derive(ExportMetrics)]
#[metric_prefix = "api"]
struct ApiMetrics {
    #[help = "Total requests processed"]
    requests_total: Counter,

    #[help = "Current active connections"]
    connections: Gauge,

    #[help = "Request latency in microseconds"]
    latency: Histogram,
}

let metrics = ApiMetrics { /* ... */ };
let mut output = String::new();
metrics.export_prometheus(&mut output);

// Output:
// # HELP api_requests_total Total requests processed
// # TYPE api_requests_total counter
// api_requests_total 1234
// # HELP api_connections Current active connections
// # TYPE api_connections gauge
// api_connections 42
// # HELP api_latency Request latency in microseconds
// # TYPE api_latency histogram
// api_latency_bucket{le="10"} 100
// ...
```

#### Attributes

| Attribute | Target | Required | Description |
|-----------|--------|----------|-------------|
| `#[metric_prefix = "..."]` | struct | No | Prefix for all metric names |
| `#[otlp]` | struct | No | Generate `export_otlp()` method (requires `fast-telemetry/otlp` feature) |
| `#[help = "..."]` | field | No | Help text (defaults to field name) |

Generated methods:

- `export_prometheus(&self, output: &mut String)`
- `export_dogstatsd(&self, output: &mut String, tags: &[(&str, &str)])`
- `export_dogstatsd_delta(&self, output: &mut String, tags: &[(&str, &str)], state: &mut ...State)`
- `export_dogstatsd_with_temporality(&self, output: &mut String, tags: &[(&str, &str)], temporality: fast_telemetry::Temporality, state: &mut ...State)`
- `export_otlp(&self, output: &mut Vec<fast_telemetry::otlp::pb::Metric>)` when `#[otlp]` is present

The delta-capable methods use a generated state type named
`<YourStructName>DogStatsDState`.

#### Supported Field Types

| Type | Prometheus Type |
|------|-----------------|
| `Counter` | counter |
| `Distribution` | histogram |
| `Gauge` | gauge |
| `GaugeF64` | gauge |
| `Histogram` | histogram |
| `LabeledCounter<L>` | counter (with labels) |
| `LabeledGauge<L>` | gauge (with labels) |
| `LabeledHistogram<L>` | histogram (with labels) |
| `DynamicCounter` | counter (with runtime labels) |
| `DynamicDistribution` | histogram (with runtime labels) |
| `DynamicGauge` | gauge (with runtime labels) |
| `DynamicGaugeI64` | gauge (with runtime labels) |
| `DynamicHistogram` | histogram (with runtime labels) |

---

## Complete Example

```rust
use fast_telemetry::{
    Counter, Gauge, Histogram,
    LabeledCounter, LabeledHistogram,
    ExportMetrics, DeriveLabel,
};

// Define label enums
#[derive(Copy, Clone, Debug, DeriveLabel)]
#[label_name = "method"]
enum HttpMethod {
    Get,
    Post,
    Put,
    Delete,
}

#[derive(Copy, Clone, Debug, DeriveLabel)]
#[label_name = "status"]
enum StatusClass {
    Success2xx,
    Redirect3xx,
    ClientError4xx,
    ServerError5xx,
}

// Define metrics struct
#[derive(ExportMetrics)]
#[metric_prefix = "http"]
struct HttpMetrics {
    #[help = "Total HTTP requests"]
    requests_total: Counter,

    #[help = "HTTP requests by method"]
    requests_by_method: LabeledCounter<HttpMethod>,

    #[help = "HTTP requests by status class"]
    requests_by_status: LabeledCounter<StatusClass>,

    #[help = "Request latency by method (microseconds)"]
    latency_by_method: LabeledHistogram<HttpMethod>,

    #[help = "Current in-flight requests"]
    in_flight: Gauge,
}

impl HttpMetrics {
    fn new() -> Self {
        Self {
            requests_total: Counter::new(4),
            requests_by_method: LabeledCounter::new(4),
            requests_by_status: LabeledCounter::new(4),
            latency_by_method: LabeledHistogram::with_latency_buckets(4),
            in_flight: Gauge::new(),
        }
    }
}

fn main() {
    let metrics = HttpMetrics::new();

    // Record some metrics
    metrics.requests_total.inc();
    metrics.requests_by_method.inc(HttpMethod::Get);
    metrics.requests_by_status.inc(StatusClass::Success2xx);
    metrics.latency_by_method.record(HttpMethod::Get, 150);
    metrics.in_flight.set(5);

    // Export to Prometheus format
    let mut output = String::new();
    metrics.export_prometheus(&mut output);
    println!("{}", output);
}
```

Output:
```
# HELP http_requests_total Total HTTP requests
# TYPE http_requests_total counter
http_requests_total 1
# HELP http_requests_by_method HTTP requests by method
# TYPE http_requests_by_method counter
http_requests_by_method{method="get"} 1
http_requests_by_method{method="post"} 0
http_requests_by_method{method="put"} 0
http_requests_by_method{method="delete"} 0
# HELP http_requests_by_status HTTP requests by status class
# TYPE http_requests_by_status counter
http_requests_by_status{status="success2xx"} 1
http_requests_by_status{status="redirect3xx"} 0
http_requests_by_status{status="client_error4xx"} 0
http_requests_by_status{status="server_error5xx"} 0
# HELP http_latency_by_method Request latency by method (microseconds)
# TYPE http_latency_by_method histogram
http_latency_by_method_bucket{method="get",le="10"} 0
http_latency_by_method_bucket{method="get",le="50"} 0
http_latency_by_method_bucket{method="get",le="100"} 0
http_latency_by_method_bucket{method="get",le="500"} 1
...
# HELP http_in_flight Current in-flight requests
# TYPE http_in_flight gauge
http_in_flight 5
```
