//! Canonical JSON representation and Arrow RecordBatch adapter for Bolt/graph values.
//!
//! All Arrow [`RecordBatch`] construction delegates to the single unified builder in
//! [`crate::value::arrow`].

use arrow_array::RecordBatch;

use crate::bolt::packstream::{BoltNode, BoltPath, BoltRelationship, BoltValue};
use crate::error::Result;
use crate::value::arrow::{batch_needs_raw_rows, values_to_record_batch};
use crate::value::result::{ResultNode, ResultPath, ResultRelationship, ResultValue};

/// Converts a [`BoltNode`] into the canonical kind-tagged JSON object.
pub fn bolt_node_to_canonical_json(node: &BoltNode) -> serde_json::Value {
    ResultNode::from(node).to_canonical_json()
}

/// Converts a [`BoltRelationship`] into the canonical kind-tagged JSON object.
pub fn bolt_relationship_to_canonical_json(rel: &BoltRelationship) -> serde_json::Value {
    ResultRelationship::from(rel).to_canonical_json()
}

/// Converts a [`BoltPath`] into the canonical kind-tagged JSON object.
pub fn bolt_path_to_canonical_json(path: &BoltPath) -> serde_json::Value {
    ResultPath::from(path).to_canonical_json()
}

/// Converts any [`BoltValue`] into canonical JSON without serde enum wrapper tags.
pub fn bolt_value_to_canonical_json(value: &BoltValue) -> serde_json::Value {
    ResultValue::from(value).to_canonical_json()
}

/// Returns `true` when `batch` contains any JSON-encoded column that relies on
/// raw [`BoltValue`] rows (`bolt_rows`) for lossless Python conversion.
pub fn batch_needs_bolt_rows(batch: &RecordBatch) -> bool {
    batch_needs_raw_rows(batch)
}

/// Converts tabular [`BoltValue`] records into an Apache Arrow [`RecordBatch`]
/// via the unified [`crate::value::arrow::values_to_record_batch`] builder.
pub fn build_bolt_record_batch(
    columns: &[String],
    records: &[Vec<BoltValue>],
) -> Result<RecordBatch> {
    let result_rows: Vec<Vec<ResultValue>> = records
        .iter()
        .map(|row| row.iter().map(ResultValue::from).collect())
        .collect();
    values_to_record_batch(columns, &result_rows)
}
