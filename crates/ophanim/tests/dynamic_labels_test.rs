use ophanim::{DynamicCounter, ExportMetrics};
use statsd_parser::{Metric, parse};
use std::collections::BTreeMap;

#[derive(ExportMetrics)]
#[metric_prefix = "api"]
struct DynamicMetrics {
    #[help = "Requests by dynamic dimensions"]
    requests: DynamicCounter,
}

impl DynamicMetrics {
    fn new() -> Self {
        Self {
            requests: DynamicCounter::new(4),
        }
    }
}

#[test]
fn test_dynamic_counter_prometheus_export() {
    let metrics = DynamicMetrics::new();
    metrics
        .requests
        .inc(&[("endpoint_uuid", "ep-1"), ("org_id", "org-a")]);
    metrics
        .requests
        .add(&[("org_id", "org-a"), ("endpoint_uuid", "ep-1")], 2);

    let mut output = String::new();
    metrics.export_prometheus(&mut output);

    let parsed = parse_prometheus(&output);

    assert_eq!(
        parsed.help.get("api_requests").map(String::as_str),
        Some("Requests by dynamic dimensions")
    );
    assert_eq!(
        parsed.metric_type.get("api_requests").map(String::as_str),
        Some("counter")
    );

    let sample = parsed
        .samples
        .iter()
        .find(|s| s.name == "api_requests")
        .expect("expected api_requests sample");
    assert_eq!(
        sample.labels.get("endpoint_uuid").map(String::as_str),
        Some("ep-1")
    );
    assert_eq!(
        sample.labels.get("org_id").map(String::as_str),
        Some("org-a")
    );
    assert_eq!(sample.value, 3.0);
}

#[test]
fn test_dynamic_counter_dogstatsd_export_and_parse() {
    let metrics = DynamicMetrics::new();
    metrics
        .requests
        .add(&[("org_id", "org-a"), ("endpoint_uuid", "ep-1")], 7);

    let mut output = String::new();
    metrics.export_dogstatsd(&mut output, &[("env", "prod")]);

    let line = output.lines().next().expect("expected a DogStatsD line");
    let msg = parse(line).unwrap_or_else(|e| panic!("failed to parse line '{line}': {e:?}"));

    assert_eq!(msg.name, "api.requests");
    assert!(matches!(msg.metric, Metric::Counter(_)));

    let tags = msg.tags.expect("expected tags");
    assert_eq!(tags.get("org_id").map(String::as_str), Some("org-a"));
    assert_eq!(tags.get("endpoint_uuid").map(String::as_str), Some("ep-1"));
    assert_eq!(tags.get("env").map(String::as_str), Some("prod"));
}

#[test]
fn test_dynamic_counter_delta_export() {
    let metrics = DynamicMetrics::new();
    let mut state = DynamicMetricsDogStatsDState::new();

    metrics
        .requests
        .add(&[("org_id", "org-a"), ("endpoint_uuid", "ep-1")], 5);

    let mut output = String::new();
    metrics.export_dogstatsd_delta(&mut output, &[], &mut state);
    assert!(output.contains("api.requests:5|c|#endpoint_uuid:ep-1,org_id:org-a\n"));

    metrics
        .requests
        .add(&[("endpoint_uuid", "ep-1"), ("org_id", "org-a")], 3);

    let mut output = String::new();
    metrics.export_dogstatsd_delta(&mut output, &[], &mut state);
    assert!(output.contains("api.requests:3|c|#endpoint_uuid:ep-1,org_id:org-a\n"));
}

struct ParsedPrometheus {
    help: BTreeMap<String, String>,
    metric_type: BTreeMap<String, String>,
    samples: Vec<PromSample>,
}

struct PromSample {
    name: String,
    labels: BTreeMap<String, String>,
    value: f64,
}

fn parse_prometheus(input: &str) -> ParsedPrometheus {
    let mut help = BTreeMap::new();
    let mut metric_type = BTreeMap::new();
    let mut samples = Vec::new();

    for raw_line in input.lines() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }

        if let Some(rest) = line.strip_prefix("# HELP ") {
            let (name, text) = rest.split_once(' ').expect("invalid HELP line");
            help.insert(name.to_string(), text.to_string());
            continue;
        }
        if let Some(rest) = line.strip_prefix("# TYPE ") {
            let (name, kind) = rest.split_once(' ').expect("invalid TYPE line");
            metric_type.insert(name.to_string(), kind.to_string());
            continue;
        }

        samples.push(parse_sample(line));
    }

    ParsedPrometheus {
        help,
        metric_type,
        samples,
    }
}

fn parse_sample(line: &str) -> PromSample {
    let (metric_part, value_part) = line.split_once(' ').expect("invalid sample line");
    let value = value_part.parse::<f64>().expect("invalid sample value");

    let Some((name, rest)) = metric_part.split_once('{') else {
        return PromSample {
            name: metric_part.to_string(),
            labels: BTreeMap::new(),
            value,
        };
    };

    let labels_str = rest.strip_suffix('}').expect("invalid label block");
    let mut labels = BTreeMap::new();
    if !labels_str.is_empty() {
        for pair in labels_str.split(',') {
            let (key, quoted_value) = pair.split_once('=').expect("invalid label pair");
            let label_value = quoted_value
                .strip_prefix('"')
                .and_then(|v| v.strip_suffix('"'))
                .expect("invalid label quoting");
            labels.insert(key.to_string(), label_value.to_string());
        }
    }

    PromSample {
        name: name.to_string(),
        labels,
        value,
    }
}
