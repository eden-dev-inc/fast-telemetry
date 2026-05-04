//! Structured visitor API for custom metric exporters.

/// Coarse semantic kind for a metric observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetricKind {
    Counter,
    Gauge,
    Histogram,
    Distribution,
    SampledTimer,
}

/// Immutable metadata for one metric observation.
#[derive(Clone, Copy, Debug)]
pub struct MetricMeta<'a> {
    pub name: &'a str,
    pub help: &'a str,
    pub kind: MetricKind,
    pub unit: Option<&'a str>,
}

/// One borrowed metric label pair.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetricLabel<'a> {
    pub name: &'a str,
    pub value: &'a str,
}

/// Borrowed labels for one metric observation.
#[derive(Clone, Copy, Debug)]
pub struct MetricLabels<'a> {
    #[allow(dead_code)]
    inner: MetricLabelsInner<'a>,
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug)]
enum MetricLabelsInner<'a> {
    None,
    One(MetricLabel<'a>),
    Slice(&'a [MetricLabel<'a>]),
    DynamicPairs(&'a [(String, String)]),
}

/// Iterator over borrowed metric labels.
#[derive(Debug)]
pub struct MetricLabelsIter<'a> {
    #[allow(dead_code)]
    inner: MetricLabelsIterInner<'a>,
}

#[allow(dead_code)]
#[derive(Debug)]
enum MetricLabelsIterInner<'a> {
    None,
    One(Option<MetricLabel<'a>>),
    Slice(core::slice::Iter<'a, MetricLabel<'a>>),
    DynamicPairs(core::slice::Iter<'a, (String, String)>),
}

/// Borrowed snapshot view for fixed-bucket histograms.
pub trait HistogramSnapshot {
    fn count(&self) -> u64;
    fn sum(&self) -> u64;
    fn visit_buckets(&self, visitor: &mut dyn FnMut(u64, u64));
}

/// Borrowed snapshot view for exponential-bucket distributions.
pub trait DistributionSnapshot {
    fn count(&self) -> u64;
    fn sum(&self) -> u64;
    fn min(&self) -> Option<u64>;
    fn max(&self) -> Option<u64>;
    fn zero_count(&self) -> u64;
    fn visit_positive_buckets(&self, visitor: &mut dyn FnMut(i32, u64));
}

/// Visitor for structured cumulative metric observations.
///
/// Implementations should keep callbacks fast. Dynamic metric traversal may call
/// visitor methods while holding an internal series read lock so it can borrow
/// canonical label pairs without allocating. Visitor methods must not call back
/// into the same dynamic metric or block on work that could need that metric's
/// locks.
pub trait MetricVisitor {
    fn counter(&mut self, meta: MetricMeta<'_>, labels: MetricLabels<'_>, value: i64);

    fn gauge_i64(&mut self, meta: MetricMeta<'_>, labels: MetricLabels<'_>, value: i64);

    fn gauge_f64(&mut self, meta: MetricMeta<'_>, labels: MetricLabels<'_>, value: f64);

    fn histogram(
        &mut self,
        meta: MetricMeta<'_>,
        labels: MetricLabels<'_>,
        histogram: &dyn HistogramSnapshot,
    );

    fn distribution(
        &mut self,
        meta: MetricMeta<'_>,
        labels: MetricLabels<'_>,
        distribution: &dyn DistributionSnapshot,
    ) {
        let _ = (meta, labels, distribution);
    }

    fn dynamic_overflow(&mut self, meta: MetricMeta<'_>, overflow_count: u64) {
        let _ = (meta, overflow_count);
    }
}
