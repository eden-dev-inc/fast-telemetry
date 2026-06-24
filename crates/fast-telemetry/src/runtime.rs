//! Runtime registry for sharing pre-built metric groups.
//!
//! The runtime owns metric groups for export/snapshot traversal. Recording
//! remains a direct call on metric handles owned by the registered metric type.

use crate::{ExportMetrics, MetricVisitor};
use parking_lot::RwLock;
use std::ops::Deref;
use std::sync::Arc;

/// Configuration for [`Runtime`].
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct RuntimeConfig {}

/// Logical scope for a registered metric group.
///
/// A scope is registry metadata for grouping/filtering registered metric
/// groups. It does not rewrite metric names emitted by the metric group.
///
/// Downstream crates should expose their accepted runtime type and scope names
/// rather than asking callers to guess the exact dependency edge.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct MetricScope {
    name: Arc<str>,
}

impl MetricScope {
    /// Create a metric scope from a stable logical name.
    pub fn new(name: impl Into<Arc<str>>) -> Self {
        Self { name: name.into() }
    }

    /// Return the scope name.
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl From<&'static str> for MetricScope {
    fn from(name: &'static str) -> Self {
        Self::new(name)
    }
}

impl From<String> for MetricScope {
    fn from(name: String) -> Self {
        Self::new(Arc::<str>::from(name))
    }
}

/// Shared telemetry runtime.
///
/// Register metric groups once during construction, then keep updating the
/// returned metric handles directly on hot paths.
pub struct Runtime {
    config: RuntimeConfig,
    registry: RwLock<Vec<RegisteredMetricGroup>>,
}

impl Runtime {
    /// Create a runtime wrapped in [`Arc`] for parent/child sharing.
    pub fn new(config: RuntimeConfig) -> Arc<Self> {
        Arc::new(Self {
            config,
            registry: RwLock::new(Vec::new()),
        })
    }

    /// Return this runtime's configuration.
    pub fn config(&self) -> &RuntimeConfig {
        &self.config
    }

    /// Register a metric group and return direct handles to it.
    ///
    /// Registry work happens here. Recording should use the returned
    /// [`RegisteredMetrics`] value, or handles pre-resolved inside `metrics`.
    pub fn register_metrics<M>(&self, scope: MetricScope, metrics: M) -> RegisteredMetrics<M>
    where
        M: ExportMetrics + Send + Sync + 'static,
    {
        let metrics = Arc::new(metrics);
        let erased_metrics: Arc<dyn ErasedExportMetrics> = metrics.clone();
        self.registry.write().push(RegisteredMetricGroup {
            scope: scope.clone(),
            metrics: erased_metrics,
        });

        RegisteredMetrics { scope, metrics }
    }

    /// Visit all registered metric groups as structured cumulative metrics.
    pub fn visit_metrics<V>(&self, visitor: &mut V)
    where
        V: MetricVisitor,
    {
        for group in self.registry.read().iter() {
            group.metrics.visit_metrics(visitor);
        }
    }

    /// Visit registered metric groups that match `scope`.
    pub fn visit_metrics_for_scope<V>(&self, scope: &MetricScope, visitor: &mut V)
    where
        V: MetricVisitor,
    {
        for group in self.registry.read().iter() {
            if &group.scope == scope {
                group.metrics.visit_metrics(visitor);
            }
        }
    }

    /// Return the number of registered metric groups.
    pub fn registered_metrics_len(&self) -> usize {
        self.registry.read().len()
    }

    /// Return the registered metric scopes.
    pub fn scopes(&self) -> Vec<MetricScope> {
        self.registry
            .read()
            .iter()
            .map(|group| group.scope.clone())
            .collect()
    }
}

/// Direct handles to a metric group registered with a [`Runtime`].
#[must_use = "keep RegisteredMetrics so hot paths can update direct metric handles"]
pub struct RegisteredMetrics<M> {
    scope: MetricScope,
    metrics: Arc<M>,
}

impl<M> RegisteredMetrics<M> {
    /// Return this metric group's scope.
    pub fn scope(&self) -> &MetricScope {
        &self.scope
    }

    /// Return the shared metric group.
    pub fn metrics(&self) -> &Arc<M> {
        &self.metrics
    }

    /// Convert into the shared metric group.
    pub fn into_metrics(self) -> Arc<M> {
        self.metrics
    }
}

impl<M> Clone for RegisteredMetrics<M> {
    fn clone(&self) -> Self {
        Self {
            scope: self.scope.clone(),
            metrics: Arc::clone(&self.metrics),
        }
    }
}

impl<M> Deref for RegisteredMetrics<M> {
    type Target = M;

    fn deref(&self) -> &Self::Target {
        &self.metrics
    }
}

struct RegisteredMetricGroup {
    scope: MetricScope,
    metrics: Arc<dyn ErasedExportMetrics>,
}

trait ErasedExportMetrics: Send + Sync {
    fn visit_metrics(&self, visitor: &mut dyn MetricVisitor);
}

impl<M> ErasedExportMetrics for M
where
    M: ExportMetrics + Send + Sync + 'static,
{
    fn visit_metrics(&self, visitor: &mut dyn MetricVisitor) {
        ExportMetrics::visit_metrics(self, visitor);
    }
}
