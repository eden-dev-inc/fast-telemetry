use ophanim::{DogStatsDExport, LabelEnum, LabeledCounter, LabeledGauge, LabeledHistogram};
use statsd_parser::{Metric, parse};

#[derive(Copy, Clone, Debug, PartialEq)]
enum HttpMethod {
    Get,
    Post,
    Put,
}

impl LabelEnum for HttpMethod {
    const CARDINALITY: usize = 3;
    const LABEL_NAME: &'static str = "method";

    fn as_index(self) -> usize {
        self as usize
    }

    fn from_index(index: usize) -> Self {
        match index {
            0 => Self::Get,
            1 => Self::Post,
            _ => Self::Put,
        }
    }

    fn variant_name(self) -> &'static str {
        match self {
            Self::Get => "get",
            Self::Post => "post",
            Self::Put => "put",
        }
    }
}

#[test]
fn test_dogstatsd_lines_are_parseable_and_typed() {
    let counter = LabeledCounter::<HttpMethod>::new(4);
    let gauge = LabeledGauge::<HttpMethod>::new();
    let histogram = LabeledHistogram::<HttpMethod>::new(&[100, 1000], 4);

    counter.inc(HttpMethod::Get);
    counter.add(HttpMethod::Post, 5);
    gauge.set(HttpMethod::Get, 10);
    histogram.record(HttpMethod::Get, 50);
    histogram.record(HttpMethod::Get, 200);

    let mut output = String::new();
    counter.export_dogstatsd(&mut output, "api.requests_by_method", &[("env", "prod")]);
    gauge.export_dogstatsd(&mut output, "api.queue_depth", &[("env", "prod")]);
    histogram.export_dogstatsd(&mut output, "api.latency_by_method", &[("env", "prod")]);

    let mut saw_counter = false;
    let mut saw_gauge = false;
    let mut saw_hist_count = false;
    let mut saw_hist_sum = false;

    for line in output.lines() {
        let msg = parse(line).unwrap_or_else(|e| panic!("failed to parse line '{line}': {e:?}"));

        let tags = msg.tags.unwrap_or_default();
        assert_eq!(tags.get("env").map(String::as_str), Some("prod"));
        assert!(tags.contains_key("method"));

        if msg.name == "api.requests_by_method" {
            assert!(matches!(msg.metric, Metric::Counter(_)));
            saw_counter = true;
        }
        if msg.name == "api.queue_depth" {
            assert!(matches!(msg.metric, Metric::Gauge(_)));
            saw_gauge = true;
        }
        if msg.name == "api.latency_by_method.count" {
            assert!(matches!(msg.metric, Metric::Counter(_)));
            saw_hist_count = true;
        }
        if msg.name == "api.latency_by_method.sum" {
            assert!(matches!(msg.metric, Metric::Counter(_)));
            saw_hist_sum = true;
        }
    }

    assert!(saw_counter);
    assert!(saw_gauge);
    assert!(saw_hist_count);
    assert!(saw_hist_sum);
}
