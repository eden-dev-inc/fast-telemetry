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
    inner: MetricLabelsInner<'a>,
}

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
    inner: MetricLabelsIterInner<'a>,
}

#[derive(Debug)]
enum MetricLabelsIterInner<'a> {
    None,
    One(Option<MetricLabel<'a>>),
    Slice(core::slice::Iter<'a, MetricLabel<'a>>),
    DynamicPairs(core::slice::Iter<'a, (String, String)>),
}

impl<'a> MetricLabels<'a> {
    pub const fn none() -> Self {
        Self {
            inner: MetricLabelsInner::None,
        }
    }

    pub const fn one(label: MetricLabel<'a>) -> Self {
        Self {
            inner: MetricLabelsInner::One(label),
        }
    }

    pub const fn slice(labels: &'a [MetricLabel<'a>]) -> Self {
        Self {
            inner: MetricLabelsInner::Slice(labels),
        }
    }

    pub const fn dynamic_pairs(labels: &'a [(String, String)]) -> Self {
        Self {
            inner: MetricLabelsInner::DynamicPairs(labels),
        }
    }

    pub fn iter(self) -> MetricLabelsIter<'a> {
        let inner = match self.inner {
            MetricLabelsInner::None => MetricLabelsIterInner::None,
            MetricLabelsInner::One(label) => MetricLabelsIterInner::One(Some(label)),
            MetricLabelsInner::Slice(labels) => MetricLabelsIterInner::Slice(labels.iter()),
            MetricLabelsInner::DynamicPairs(labels) => {
                MetricLabelsIterInner::DynamicPairs(labels.iter())
            }
        };
        MetricLabelsIter { inner }
    }
}

impl<'a> Iterator for MetricLabelsIter<'a> {
    type Item = MetricLabel<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.inner {
            MetricLabelsIterInner::None => None,
            MetricLabelsIterInner::One(label) => label.take(),
            MetricLabelsIterInner::Slice(labels) => labels.next().copied(),
            MetricLabelsIterInner::DynamicPairs(labels) => labels.next().map(|(name, value)| {
                MetricLabel {
                    name: name.as_str(),
                    value: value.as_str(),
                }
            }),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match &self.inner {
            MetricLabelsIterInner::None => (0, Some(0)),
            MetricLabelsIterInner::One(Some(_)) => (1, Some(1)),
            MetricLabelsIterInner::One(None) => (0, Some(0)),
            MetricLabelsIterInner::Slice(labels) => labels.size_hint(),
            MetricLabelsIterInner::DynamicPairs(labels) => labels.size_hint(),
        }
    }
}

impl ExactSizeIterator for MetricLabelsIter<'_> {}

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
