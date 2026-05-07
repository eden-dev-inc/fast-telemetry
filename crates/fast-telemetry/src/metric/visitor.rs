//! Structured visitor API for custom metric exporters.

use crate::{Distribution, DynamicHistogramSeriesView, Histogram, exp_buckets::ExpBucketsSnapshot};

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
            MetricLabelsIterInner::DynamicPairs(labels) => {
                labels.next().map(|(name, value)| MetricLabel {
                    name: name.as_str(),
                    value: value.as_str(),
                })
            }
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

impl HistogramSnapshot for Histogram {
    fn count(&self) -> u64 {
        self.count()
    }

    fn sum(&self) -> u64 {
        self.sum()
    }

    fn visit_buckets(&self, visitor: &mut dyn FnMut(u64, u64)) {
        for (upper_bound, cumulative_count) in self.buckets_cumulative_iter() {
            visitor(upper_bound, cumulative_count);
        }
    }
}

impl HistogramSnapshot for DynamicHistogramSeriesView<'_> {
    fn count(&self) -> u64 {
        self.count()
    }

    fn sum(&self) -> u64 {
        self.sum()
    }

    fn visit_buckets(&self, visitor: &mut dyn FnMut(u64, u64)) {
        for (upper_bound, cumulative_count) in self.buckets_cumulative_iter() {
            visitor(upper_bound, cumulative_count);
        }
    }
}

impl DistributionSnapshot for Distribution {
    fn count(&self) -> u64 {
        self.count()
    }

    fn sum(&self) -> u64 {
        self.sum()
    }

    fn min(&self) -> Option<u64> {
        self.min()
    }

    fn max(&self) -> Option<u64> {
        self.max()
    }

    fn zero_count(&self) -> u64 {
        self.buckets_snapshot().zero_count
    }

    fn visit_positive_buckets(&self, visitor: &mut dyn FnMut(i32, u64)) {
        visit_positive_buckets(&self.buckets_snapshot(), visitor);
    }
}

impl DistributionSnapshot for ExpBucketsSnapshot {
    fn count(&self) -> u64 {
        self.count
    }

    fn sum(&self) -> u64 {
        self.sum
    }

    fn min(&self) -> Option<u64> {
        self.min()
    }

    fn max(&self) -> Option<u64> {
        self.max()
    }

    fn zero_count(&self) -> u64 {
        self.zero_count
    }

    fn visit_positive_buckets(&self, visitor: &mut dyn FnMut(i32, u64)) {
        visit_positive_buckets(self, visitor);
    }
}

fn visit_positive_buckets(snapshot: &ExpBucketsSnapshot, visitor: &mut dyn FnMut(i32, u64)) {
    for (index, count) in snapshot.positive.iter().copied().enumerate() {
        if count > 0 {
            visitor(index as i32, count);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{MetricLabel, MetricLabels};

    #[test]
    fn metric_labels_none_iterates_empty() {
        let labels: Vec<_> = MetricLabels::none().iter().collect();
        assert!(labels.is_empty());
    }

    #[test]
    fn metric_labels_one_iterates_once() {
        let label = MetricLabel {
            name: "method",
            value: "get",
        };

        let labels: Vec<_> = MetricLabels::one(label).iter().collect();
        assert_eq!(labels, vec![label]);
    }

    #[test]
    fn metric_labels_slice_iterates_borrowed_labels() {
        let source = [
            MetricLabel {
                name: "method",
                value: "get",
            },
            MetricLabel {
                name: "status",
                value: "ok",
            },
        ];

        let labels: Vec<_> = MetricLabels::slice(&source).iter().collect();
        assert_eq!(labels, source);
    }

    #[test]
    fn metric_labels_dynamic_pairs_iterates_borrowed_strings() {
        let source = vec![
            ("endpoint_uuid".to_string(), "ep-1".to_string()),
            ("org_id".to_string(), "org-a".to_string()),
        ];

        let labels: Vec<_> = MetricLabels::dynamic_pairs(&source).iter().collect();
        assert_eq!(
            labels,
            vec![
                MetricLabel {
                    name: "endpoint_uuid",
                    value: "ep-1",
                },
                MetricLabel {
                    name: "org_id",
                    value: "org-a",
                },
            ]
        );
    }

    #[test]
    fn metric_labels_iterator_reports_exact_len() {
        let source = vec![
            ("a".to_string(), "1".to_string()),
            ("b".to_string(), "2".to_string()),
        ];

        let mut labels = MetricLabels::dynamic_pairs(&source).iter();
        assert_eq!(labels.len(), 2);
        assert_eq!(labels.next().map(|label| label.name), Some("a"));
        assert_eq!(labels.len(), 1);
    }
}
