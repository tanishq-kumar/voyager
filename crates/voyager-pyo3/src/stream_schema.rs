//! Schema locking and per-chunk reconciliation for streaming Arrow C Data Interface exports.

use std::fmt;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Decimal128Type, DurationMicrosecondType, DurationNanosecondType, Float32Type,
    Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, Time64MicrosecondType,
    Time64NanosecondType, TimestampMicrosecondType, TimestampNanosecondType, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type,
};
use arrow_array::{
    Array, ArrayRef, BinaryArray, Decimal128Array, Float64Array, LargeBinaryArray, RecordBatch,
    builder::StringBuilder,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use voyager_drivers::value::{
    META_ENCODING_JSON, META_TYPE_JSON, VOYAGER_META_ENCODING, VOYAGER_META_KIND,
    VOYAGER_META_TYPE, days_to_ymd, format_datetime_iso, format_iso_date, format_iso_duration,
    format_iso_time, format_uuid_bytes, nanos_to_hmsn,
};

/// Error raised when a streaming chunk introduces an incompatible Arrow schema change
/// after the C Data Interface stream schema has already been locked from the first chunk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaDriftError {
    pub column: String,
    pub locked_type: String,
    pub chunk_type: String,
    pub reason: String,
}

impl SchemaDriftError {
    pub fn new(
        column: impl Into<String>,
        locked_type: impl Into<String>,
        chunk_type: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            column: column.into(),
            locked_type: locked_type.into(),
            chunk_type: chunk_type.into(),
            reason: reason.into(),
        }
    }
}

impl fmt::Display for SchemaDriftError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SchemaDriftError on column '{}': locked Arrow type {} cannot accept chunk type {} ({}). \
             Use as_format=\"dicts\", add an explicit CAST(...) in your query, or increase fetch_size.",
            self.column, self.locked_type, self.chunk_type, self.reason
        )
    }
}

impl std::error::Error for SchemaDriftError {}

fn format_field_type(field: &Field) -> String {
    if field.metadata().is_empty() {
        format!("{:?}", field.data_type())
    } else {
        let mut pairs: Vec<(&String, &String)> = field.metadata().iter().collect();
        pairs.sort_by_key(|(a, _)| *a);
        let meta = pairs
            .into_iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(", ");
        format!("{:?} {{{meta}}}", field.data_type())
    }
}

fn is_all_null_dynamic_column(col: &dyn Array, field: &Field, num_rows: usize) -> bool {
    matches!(field.data_type(), DataType::Null)
        || (col.null_count() == num_rows
            && matches!(field.data_type(), DataType::Utf8)
            && field.metadata().is_empty())
}

fn is_generic_json_utf8_field(field: &Field) -> bool {
    if field.data_type() != &DataType::Utf8 {
        return false;
    }
    let meta = field.metadata();
    meta.get(VOYAGER_META_ENCODING).map(String::as_str) == Some(META_ENCODING_JSON)
        && !meta.contains_key(VOYAGER_META_KIND)
        && meta
            .get(VOYAGER_META_TYPE)
            .is_none_or(|t| t == META_TYPE_JSON)
}

fn is_json_encoded_utf8_field(field: &Field) -> bool {
    field.data_type() == &DataType::Utf8
        && field
            .metadata()
            .get(VOYAGER_META_ENCODING)
            .map(String::as_str)
            == Some(META_ENCODING_JSON)
}

fn format_decimal128_str(val: i128, scale: i8) -> String {
    if scale <= 0 {
        let factor = 10_i128.checked_pow(scale.unsigned_abs() as u32);
        return match factor.and_then(|f| val.checked_mul(f)) {
            Some(v) => v.to_string(),
            None => val.to_string(),
        };
    }
    let scale_u = scale as usize;
    let neg = val < 0;
    let digits = val.unsigned_abs().to_string();
    let sign = if neg { "-" } else { "" };
    if digits.len() <= scale_u {
        let zeros = "0".repeat(scale_u - digits.len());
        format!("{sign}0.{zeros}{digits}")
    } else {
        let split = digits.len() - scale_u;
        format!("{sign}{}.{}", &digits[..split], &digits[split..])
    }
}

fn arrow_cell_to_json(
    array: &dyn Array,
    field: &Field,
    row: usize,
    column_name: &str,
    locked_type: &str,
) -> Result<serde_json::Value, SchemaDriftError> {
    if array.is_null(row) {
        return Ok(serde_json::Value::Null);
    }

    match field.data_type() {
        DataType::Null => Ok(serde_json::Value::Null),
        DataType::Boolean => Ok(serde_json::Value::Bool(array.as_boolean().value(row))),
        DataType::Int8 => Ok(serde_json::Value::from(
            array.as_primitive::<Int8Type>().value(row),
        )),
        DataType::Int16 => Ok(serde_json::Value::from(
            array.as_primitive::<Int16Type>().value(row),
        )),
        DataType::Int32 => Ok(serde_json::Value::from(
            array.as_primitive::<Int32Type>().value(row),
        )),
        DataType::Int64 => Ok(serde_json::Value::from(
            array.as_primitive::<Int64Type>().value(row),
        )),
        DataType::UInt8 => Ok(serde_json::Value::from(
            array.as_primitive::<UInt8Type>().value(row),
        )),
        DataType::UInt16 => Ok(serde_json::Value::from(
            array.as_primitive::<UInt16Type>().value(row),
        )),
        DataType::UInt32 => Ok(serde_json::Value::from(
            array.as_primitive::<UInt32Type>().value(row),
        )),
        DataType::UInt64 => Ok(serde_json::Value::from(
            array.as_primitive::<UInt64Type>().value(row),
        )),
        DataType::Float32 => {
            let v = array.as_primitive::<Float32Type>().value(row) as f64;
            Ok(serde_json::Number::from_f64(v)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null))
        }
        DataType::Float64 => {
            let v = array.as_primitive::<Float64Type>().value(row);
            Ok(serde_json::Number::from_f64(v)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null))
        }
        DataType::Utf8 => {
            let s = array.as_string::<i32>().value(row);
            if is_json_encoded_utf8_field(field) {
                if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(s) {
                    return Ok(parsed);
                }
            }
            Ok(serde_json::Value::String(s.to_string()))
        }
        DataType::LargeUtf8 => Ok(serde_json::Value::String(
            array.as_string::<i64>().value(row).to_string(),
        )),
        DataType::Date32 => {
            let days = array.as_primitive::<Date32Type>().value(row) as i64;
            let (y, m, d) = days_to_ymd(days);
            Ok(serde_json::Value::String(format_iso_date(
                y, m as u8, d as u8,
            )))
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let us = array.as_primitive::<Time64MicrosecondType>().value(row);
            let (h, m, s, ns) = nanos_to_hmsn(us.saturating_mul(1_000));
            Ok(serde_json::Value::String(format_iso_time(
                h as u8, m as u8, s as u8, ns, None,
            )))
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            let nanos = array.as_primitive::<Time64NanosecondType>().value(row);
            let (h, m, s, sub) = nanos_to_hmsn(nanos);
            Ok(serde_json::Value::String(format_iso_time(
                h as u8, m as u8, s as u8, sub, None,
            )))
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let us = array.as_primitive::<TimestampMicrosecondType>().value(row);
            let secs = us.div_euclid(1_000_000);
            let sub_ns = (us.rem_euclid(1_000_000) * 1_000) as u32;
            let s = if tz.is_some() {
                format_datetime_iso(secs, sub_ns, Some(0))
            } else {
                let (y, m, d) = days_to_ymd(secs.div_euclid(86_400));
                let sod = secs.rem_euclid(86_400);
                format!(
                    "{}T{}",
                    format_iso_date(y, m as u8, d as u8),
                    format_iso_time(
                        (sod / 3_600) as u8,
                        ((sod % 3_600) / 60) as u8,
                        (sod % 60) as u8,
                        sub_ns,
                        None,
                    )
                )
            };
            Ok(serde_json::Value::String(s))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
            let ns = array.as_primitive::<TimestampNanosecondType>().value(row);
            let secs = ns.div_euclid(1_000_000_000);
            let sub_ns = ns.rem_euclid(1_000_000_000) as u32;
            let s = if tz.is_some() {
                format_datetime_iso(secs, sub_ns, Some(0))
            } else {
                let (y, m, d) = days_to_ymd(secs.div_euclid(86_400));
                let sod = secs.rem_euclid(86_400);
                format!(
                    "{}T{}",
                    format_iso_date(y, m as u8, d as u8),
                    format_iso_time(
                        (sod / 3_600) as u8,
                        ((sod % 3_600) / 60) as u8,
                        (sod % 60) as u8,
                        sub_ns,
                        None,
                    )
                )
            };
            Ok(serde_json::Value::String(s))
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            let us = array.as_primitive::<DurationMicrosecondType>().value(row);
            let secs = us / 1_000_000;
            let ns = (us % 1_000_000) * 1_000;
            Ok(serde_json::Value::String(format_iso_duration(
                0, 0, secs, ns,
            )))
        }
        DataType::Duration(TimeUnit::Nanosecond) => {
            let ns_total = array.as_primitive::<DurationNanosecondType>().value(row);
            let secs = ns_total / 1_000_000_000;
            let ns = ns_total % 1_000_000_000;
            Ok(serde_json::Value::String(format_iso_duration(
                0, 0, secs, ns,
            )))
        }
        DataType::Decimal128(_, scale) => {
            let val = array.as_primitive::<Decimal128Type>().value(row);
            Ok(serde_json::Value::String(format_decimal128_str(
                val, *scale,
            )))
        }
        DataType::Binary => {
            let bytes = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    SchemaDriftError::new(
                        column_name,
                        locked_type,
                        format_field_type(field),
                        "expected BinaryArray",
                    )
                })?;
            Ok(serde_json::Value::Array(
                bytes
                    .value(row)
                    .iter()
                    .map(|b| serde_json::Value::Number((*b).into()))
                    .collect(),
            ))
        }
        DataType::LargeBinary => {
            let bytes = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| {
                    SchemaDriftError::new(
                        column_name,
                        locked_type,
                        format_field_type(field),
                        "expected LargeBinaryArray",
                    )
                })?;
            Ok(serde_json::Value::Array(
                bytes
                    .value(row)
                    .iter()
                    .map(|b| serde_json::Value::Number((*b).into()))
                    .collect(),
            ))
        }
        DataType::FixedSizeBinary(16) => {
            let bytes = array.as_fixed_size_binary().value(row);
            let fixed: [u8; 16] = bytes.try_into().map_err(|_| {
                SchemaDriftError::new(
                    column_name,
                    locked_type,
                    format_field_type(field),
                    "invalid 16-byte FixedSizeBinary length",
                )
            })?;
            Ok(serde_json::Value::String(format_uuid_bytes(&fixed)))
        }
        DataType::List(item_field) => {
            let list = array.as_list::<i32>();
            let values = list.value(row);
            let mut out = Vec::with_capacity(values.len());
            for i in 0..values.len() {
                out.push(arrow_cell_to_json(
                    values.as_ref(),
                    item_field.as_ref(),
                    i,
                    column_name,
                    locked_type,
                )?);
            }
            Ok(serde_json::Value::Array(out))
        }
        DataType::LargeList(item_field) => {
            let list = array.as_list::<i64>();
            let values = list.value(row);
            let mut out = Vec::with_capacity(values.len());
            for i in 0..values.len() {
                out.push(arrow_cell_to_json(
                    values.as_ref(),
                    item_field.as_ref(),
                    i,
                    column_name,
                    locked_type,
                )?);
            }
            Ok(serde_json::Value::Array(out))
        }
        DataType::Struct(children) => {
            let st = array.as_struct();
            let mut map = serde_json::Map::with_capacity(children.len());
            for (idx, child_field) in children.iter().enumerate() {
                let child_arr = st.column(idx);
                let v = arrow_cell_to_json(
                    child_arr.as_ref(),
                    child_field.as_ref(),
                    row,
                    column_name,
                    locked_type,
                )?;
                map.insert(child_field.name().clone(), v);
            }
            Ok(serde_json::Value::Object(map))
        }
        _ => Err(SchemaDriftError::new(
            column_name,
            locked_type,
            format_field_type(field),
            "cannot widen column to JSON-encoded Utf8",
        )),
    }
}

fn widen_column_to_json_utf8(
    col: &dyn Array,
    actual_field: &Field,
    locked_field: &Field,
    num_rows: usize,
) -> Result<ArrayRef, SchemaDriftError> {
    if is_json_encoded_utf8_field(actual_field) {
        return Ok(Arc::new(col.as_string::<i32>().clone()));
    }
    let locked_type = format_field_type(locked_field);
    let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 16);
    let mut buf = Vec::with_capacity(64);
    for row in 0..num_rows {
        if col.is_null(row) {
            builder.append_null();
            continue;
        }
        let val = arrow_cell_to_json(col, actual_field, row, locked_field.name(), &locked_type)?;
        buf.clear();
        serde_json::to_writer(&mut buf, &val).map_err(|e| {
            SchemaDriftError::new(
                locked_field.name(),
                &locked_type,
                format_field_type(actual_field),
                format!("JSON serialization failed: {e}"),
            )
        })?;
        let s = std::str::from_utf8(&buf).unwrap_or("null");
        builder.append_value(s);
    }
    Ok(Arc::new(builder.finish()))
}

fn decimal128_max_abs(precision: u8) -> Option<i128> {
    if precision == 0 || precision > 38 {
        return None;
    }
    if precision == 38 {
        Some(99_999_999_999_999_999_999_999_999_999_999_999_999_i128)
    } else {
        10_i128.checked_pow(precision as u32).map(|p| p - 1)
    }
}

fn reconcile_column(
    col: &ArrayRef,
    actual_field: &Field,
    locked_field: &Field,
    num_rows: usize,
) -> Result<ArrayRef, SchemaDriftError> {
    if !locked_field.is_nullable() && col.null_count() > 0 {
        return Err(SchemaDriftError::new(
            locked_field.name(),
            format_field_type(locked_field),
            format_field_type(actual_field),
            "non-nullable locked column received NULL values",
        ));
    }

    if actual_field.data_type() == locked_field.data_type()
        && actual_field.metadata() == locked_field.metadata()
    {
        return Ok(col.clone());
    }

    if is_all_null_dynamic_column(col.as_ref(), actual_field, num_rows) {
        return Ok(arrow_array::new_null_array(
            locked_field.data_type(),
            num_rows,
        ));
    }

    if locked_field.data_type() == &DataType::Float64
        && locked_field.metadata().is_empty()
        && actual_field.data_type() == &DataType::Int64
        && actual_field.metadata().is_empty()
    {
        let ints = col.as_primitive::<Int64Type>();
        let out: Float64Array = ints.iter().map(|opt| opt.map(|v| v as f64)).collect();
        return Ok(Arc::new(out));
    }

    if let (DataType::Decimal128(lp, ls), DataType::Decimal128(ap, as_)) =
        (locked_field.data_type(), actual_field.data_type())
        && locked_field.metadata() == actual_field.metadata()
    {
        if *ap <= *lp && *as_ >= 0 && *ls >= *as_ {
            let diff = (*ls - *as_) as u32;
            let factor = 10_i128.checked_pow(diff).ok_or_else(|| {
                SchemaDriftError::new(
                    locked_field.name(),
                    format_field_type(locked_field),
                    format_field_type(actual_field),
                    "decimal scale widening overflowed i128",
                )
            })?;
            let max_abs = decimal128_max_abs(*lp).ok_or_else(|| {
                SchemaDriftError::new(
                    locked_field.name(),
                    format_field_type(locked_field),
                    format_field_type(actual_field),
                    "unsupported decimal precision",
                )
            })?;
            let src = col.as_primitive::<Decimal128Type>();
            let mut scaled = Vec::with_capacity(src.len());
            for opt in src.iter() {
                match opt {
                    None => scaled.push(None),
                    Some(v) => {
                        let widened = v.checked_mul(factor).ok_or_else(|| {
                            SchemaDriftError::new(
                                locked_field.name(),
                                format_field_type(locked_field),
                                format_field_type(actual_field),
                                "decimal scale widening overflowed Decimal128",
                            )
                        })?;
                        if widened.saturating_abs() > max_abs {
                            return Err(SchemaDriftError::new(
                                locked_field.name(),
                                format_field_type(locked_field),
                                format_field_type(actual_field),
                                "decimal scale widening exceeded locked precision",
                            ));
                        }
                        scaled.push(Some(widened));
                    }
                }
            }
            let arr = Decimal128Array::from(scaled)
                .with_precision_and_scale(*lp, *ls)
                .map_err(|e| {
                    SchemaDriftError::new(
                        locked_field.name(),
                        format_field_type(locked_field),
                        format_field_type(actual_field),
                        format!("invalid Decimal128 precision/scale: {e}"),
                    )
                })?;
            return Ok(Arc::new(arr));
        }
        return Err(SchemaDriftError::new(
            locked_field.name(),
            format_field_type(locked_field),
            format_field_type(actual_field),
            "cannot narrow Decimal128 scale or precision across chunks",
        ));
    }

    if is_generic_json_utf8_field(locked_field) {
        return widen_column_to_json_utf8(col.as_ref(), actual_field, locked_field, num_rows);
    }

    Err(SchemaDriftError::new(
        locked_field.name(),
        format_field_type(locked_field),
        format_field_type(actual_field),
        "incompatible Arrow column type across streaming chunks",
    ))
}

/// Reconciles a streaming chunk's `RecordBatch` to the `Schema` locked from the first chunk.
///
/// Exact schema matches pass through with zero copy. Safe widenings (all-null columns to any
/// locked type, `Int64` to `Float64`, `Decimal128` scale widening `s2 <= s1`, and homogeneous
/// values into generic `Utf8` + `voyager.encoding=json`) are converted cleanly. Incompatible
/// schema drift fails loudly with [`SchemaDriftError`].
pub fn reconcile_batch_to_schema(
    batch: &RecordBatch,
    locked_schema: &Arc<Schema>,
) -> Result<RecordBatch, SchemaDriftError> {
    if batch.schema().as_ref() == locked_schema.as_ref() {
        return Ok(batch.clone());
    }

    let actual_schema = batch.schema();
    if actual_schema.fields().len() != locked_schema.fields().len() {
        return Err(SchemaDriftError::new(
            "<schema>",
            format!("{} column(s)", locked_schema.fields().len()),
            format!("{} column(s)", actual_schema.fields().len()),
            "column count changed across streaming chunks",
        ));
    }

    let num_rows = batch.num_rows();
    let mut out_cols = Vec::with_capacity(locked_schema.fields().len());
    for (idx, locked_field) in locked_schema.fields().iter().enumerate() {
        let actual_field = actual_schema.field(idx);
        if actual_field.name() != locked_field.name() {
            return Err(SchemaDriftError::new(
                locked_field.name(),
                format_field_type(locked_field),
                format!(
                    "{} ({})",
                    actual_field.name(),
                    format_field_type(actual_field)
                ),
                "column name changed across streaming chunks",
            ));
        }
        let col = reconcile_column(batch.column(idx), actual_field, locked_field, num_rows)?;
        out_cols.push(col);
    }

    RecordBatch::try_new(locked_schema.clone(), out_cols).map_err(|e| {
        SchemaDriftError::new(
            "<schema>",
            format!("{locked_schema:?}"),
            format!("{actual_schema:?}"),
            format!("failed to construct reconciled RecordBatch: {e}"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{BooleanArray, Int64Array, NullArray, StringArray};
    use std::collections::HashMap;
    use voyager_drivers::ResultValue;
    use voyager_drivers::value::result::{ResultMap, ResultNode};
    use voyager_drivers::value::{META_KIND_NODE, values_to_record_batch};

    #[test]
    fn exact_schema_match_is_zero_copy() {
        let cols = vec!["n".to_string(), "s".to_string()];
        let b1 = values_to_record_batch(
            &cols,
            &[vec![ResultValue::Int(1), ResultValue::Str("a".into())]],
        )
        .unwrap();
        let b2 = values_to_record_batch(
            &cols,
            &[vec![ResultValue::Int(2), ResultValue::Str("b".into())]],
        )
        .unwrap();
        let locked = b1.schema();
        let out = reconcile_batch_to_schema(&b2, &locked).unwrap();
        assert_eq!(out.schema(), locked);
        assert!(Arc::ptr_eq(out.column(0), b2.column(0)));
        assert!(Arc::ptr_eq(out.column(1), b2.column(1)));
    }

    #[test]
    fn null_and_dynamic_all_null_columns_widen_to_locked_type() {
        let locked = Arc::new(Schema::new(vec![
            Field::new("i", DataType::Int64, true),
            Field::new("b", DataType::Boolean, true),
        ]));

        // Explicit Arrow NullArray -> Int64, dynamic all-null Utf8 -> Boolean.
        let chunk = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("i", DataType::Null, true),
                Field::new("b", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(NullArray::new(2)),
                Arc::new(StringArray::from(vec![None::<&str>, None::<&str>])),
            ],
        )
        .unwrap();

        let out = reconcile_batch_to_schema(&chunk, &locked).unwrap();
        assert_eq!(out.schema(), locked);
        assert_eq!(out.column(0).data_type(), &DataType::Int64);
        assert_eq!(out.column(0).null_count(), 2);
        assert_eq!(out.column(1).data_type(), &DataType::Boolean);
        assert_eq!(out.column(1).null_count(), 2);
    }

    #[test]
    fn int64_widens_to_float64() {
        let locked = Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, true)]));
        let chunk = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)])),
            vec![Arc::new(Int64Array::from(vec![Some(10), None, Some(-3)]))],
        )
        .unwrap();

        let out = reconcile_batch_to_schema(&chunk, &locked).unwrap();
        let floats = out.column(0).as_primitive::<Float64Type>();
        assert_eq!(floats.value(0), 10.0);
        assert!(floats.is_null(1));
        assert_eq!(floats.value(2), -3.0);
    }

    #[test]
    fn decimal128_widens_smaller_scale_and_rejects_larger_scale_or_overflow() {
        let locked = Arc::new(Schema::new(vec![Field::new(
            "d",
            DataType::Decimal128(38, 3),
            true,
        )]));
        let smaller = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "d",
                DataType::Decimal128(38, 1),
                true,
            )])),
            vec![Arc::new(
                Decimal128Array::from(vec![Some(125), None, Some(-4)])
                    .with_precision_and_scale(38, 1)
                    .unwrap(),
            )],
        )
        .unwrap();

        let out = reconcile_batch_to_schema(&smaller, &locked).unwrap();
        let decs = out.column(0).as_primitive::<Decimal128Type>();
        assert_eq!(decs.value(0), 12_500);
        assert!(decs.is_null(1));
        assert_eq!(decs.value(2), -400);

        // Larger scale s2=4 > s1=3 fails loudly.
        let larger = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "d",
                DataType::Decimal128(38, 4),
                true,
            )])),
            vec![Arc::new(
                Decimal128Array::from(vec![Some(1)])
                    .with_precision_and_scale(38, 4)
                    .unwrap(),
            )],
        )
        .unwrap();
        let err = reconcile_batch_to_schema(&larger, &locked).unwrap_err();
        assert!(err.to_string().contains("SchemaDriftError"));
        assert!(err.to_string().contains("'d'"));

        // Overflow on scale-up fails loudly.
        let overflow_chunk = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "d",
                DataType::Decimal128(38, 1),
                true,
            )])),
            vec![Arc::new(
                Decimal128Array::from(vec![Some(i128::MAX)])
                    .with_precision_and_scale(38, 1)
                    .unwrap(),
            )],
        )
        .unwrap();
        assert!(reconcile_batch_to_schema(&overflow_chunk, &locked).is_err());
    }

    #[test]
    fn homogeneous_values_widen_into_locked_json_utf8() {
        let cols = vec!["x".to_string()];
        // Chunk 1 has mixed Int + Str -> Utf8 with voyager.encoding=json (no voyager.kind).
        let b1 = values_to_record_batch(
            &cols,
            &[
                vec![ResultValue::Int(1)],
                vec![ResultValue::Str("hi".into())],
            ],
        )
        .unwrap();
        let locked = b1.schema();

        // Chunk 2 has homogeneous Int64 -> serialized into JSON strings.
        let b2 = values_to_record_batch(
            &cols,
            &[vec![ResultValue::Int(42)], vec![ResultValue::Null]],
        )
        .unwrap();
        let out2 = reconcile_batch_to_schema(&b2, &locked).unwrap();
        let strs2 = out2.column(0).as_string::<i32>();
        assert_eq!(strs2.value(0), "42");
        assert!(strs2.is_null(1));

        // Chunk 3 has homogeneous Node (Utf8 with voyager.encoding=json, voyager.kind=node) -> passes through.
        let b3 = values_to_record_batch(
            &cols,
            &[vec![ResultValue::Node(Box::new(ResultNode {
                id: 7,
                labels: vec![Arc::from("Person")],
                properties: ResultMap::new(),
                element_id: None,
            }))]],
        )
        .unwrap();
        let out3 = reconcile_batch_to_schema(&b3, &locked).unwrap();
        assert_eq!(out3.schema(), locked);
        let strs3 = out3.column(0).as_string::<i32>();
        assert!(strs3.value(0).contains("\"kind\":\"node\""));
    }

    #[test]
    fn incompatible_schema_drift_fails_loudly_with_actionable_message() {
        let int_schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Int64, true)]));
        let float_chunk = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "val",
                DataType::Float64,
                true,
            )])),
            vec![Arc::new(Float64Array::from(vec![1.5]))],
        )
        .unwrap();
        let err = reconcile_batch_to_schema(&float_chunk, &int_schema).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("val"), "{msg}");
        assert!(msg.contains("Int64"), "{msg}");
        assert!(msg.contains("Float64"), "{msg}");
        assert!(msg.contains("as_format=\"dicts\""), "{msg}");
        assert!(msg.contains("CAST("), "{msg}");
        assert!(msg.contains("fetch_size"), "{msg}");

        // Locked Null cannot widen to Int64.
        let null_schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Null, true)]));
        let int_chunk = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("val", DataType::Int64, true)])),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();
        assert!(reconcile_batch_to_schema(&int_chunk, &null_schema).is_err());

        // Locked Node (voyager.kind=node) rejects Int64.
        let mut node_meta = HashMap::new();
        node_meta.insert(
            VOYAGER_META_ENCODING.to_string(),
            META_ENCODING_JSON.to_string(),
        );
        node_meta.insert(VOYAGER_META_KIND.to_string(), META_KIND_NODE.to_string());
        let node_schema = Arc::new(Schema::new(vec![
            Field::new("val", DataType::Utf8, true).with_metadata(node_meta),
        ]));
        assert!(reconcile_batch_to_schema(&int_chunk, &node_schema).is_err());

        // Locked Int64 rejects Boolean.
        let bool_chunk = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "val",
                DataType::Boolean,
                true,
            )])),
            vec![Arc::new(BooleanArray::from(vec![true]))],
        )
        .unwrap();
        assert!(reconcile_batch_to_schema(&bool_chunk, &int_schema).is_err());
    }
}
