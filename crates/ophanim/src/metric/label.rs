//! Label trait for enum-indexed metrics.
//!
//! Labels provide O(1) dimensional metrics without HashMap lookups.
//! Each label enum variant maps directly to an array index.

use std::fmt::Debug;

/// Trait for label enums used with `LabeledCounter`, `LabeledGauge`, etc.
///
/// Implementing this trait allows an enum to be used as a dimension for metrics.
/// Each variant maps to an array index, enabling O(1) metric lookup and update.
///
/// # Example
///
/// ```ignore
/// #[derive(Copy, Clone, Debug)]
/// enum HttpMethod {
///     Get,
///     Post,
///     Put,
///     Delete,
///     Other,
/// }
///
/// impl LabelEnum for HttpMethod {
///     const CARDINALITY: usize = 5;
///     const LABEL_NAME: &'static str = "method";
///
///     fn as_index(self) -> usize {
///         self as usize
///     }
///
///     fn from_index(index: usize) -> Self {
///         match index {
///             0 => Self::Get,
///             1 => Self::Post,
///             2 => Self::Put,
///             3 => Self::Delete,
///             _ => Self::Other,
///         }
///     }
///
///     fn variant_name(self) -> &'static str {
///         match self {
///             Self::Get => "get",
///             Self::Post => "post",
///             Self::Put => "put",
///             Self::Delete => "delete",
///             Self::Other => "other",
///         }
///     }
/// }
/// ```
pub trait LabelEnum: Copy + Debug + 'static {
    /// Number of variants in this enum.
    const CARDINALITY: usize;

    /// The Prometheus label name for this dimension.
    ///
    /// E.g., "method" for HttpMethod, "command" for RedisCommand.
    const LABEL_NAME: &'static str;

    /// Convert this variant to its array index.
    fn as_index(self) -> usize;

    /// Convert an array index back to a variant.
    ///
    /// Used for iteration during export. Should handle out-of-bounds
    /// by returning a sensible default (e.g., an "Other" variant).
    fn from_index(index: usize) -> Self;

    /// Get the Prometheus label value for this variant.
    ///
    /// Should be lowercase, snake_case for Prometheus compatibility.
    fn variant_name(self) -> &'static str;

    /// Get the label name (convenience method for accessing LABEL_NAME).
    #[inline]
    fn label_name(&self) -> &'static str {
        Self::LABEL_NAME
    }
}
