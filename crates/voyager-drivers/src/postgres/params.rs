//! PostgreSQL / Apache AGE parameter binding preparation.
//!
//! The native client must never splice parameter values into SQL text. This
//! module rewrites statement placeholders into PostgreSQL extended-protocol
//! positional placeholders and encodes each value for `Bind` (text format).
//!
//! Two placeholder styles are supported:
//!
//! * Apache AGE compiled statements keep a single driver `%s` placeholder for
//!   the agtype parameter map outside their `$$ ... $$` Cypher body. It becomes
//!   `$1` and the entire parameter map is bound as one JSON text value.
//! * Everything else uses the compiled `$name` placeholders (`$p0`, `$batch`,
//!   ...). They become `$1`, `$2`, ... in order of first appearance. Repeated
//!   names reuse the same positional placeholder.
//!
//! Scanning skips PostgreSQL string literals, quoted identifiers, dollar-quoted
//! bodies (`$$ ... $$`), and comments, so `%s` and `$name` text inside a Cypher
//! body is left untouched.

use std::collections::{BTreeMap, HashMap};

use serde_json::Value;

use crate::error::{NetError, Result};
use crate::params::{ParamValue, format_datetime_iso, format_iso_date, format_iso_time};

const MAX_NESTING_DEPTH: usize = 64;

/// Placeholder convention detected in a compiled statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParameterPlaceholderStyle {
    /// No parameters were bound.
    None,
    /// Apache AGE `%s` placeholder for the agtype parameter map.
    AgeParameterMap,
    /// Named `$name` placeholders bound positionally.
    Named,
}

/// A statement ready for a PostgreSQL extended-query `Parse` / `Bind` cycle.
#[derive(Debug, Clone, PartialEq)]
pub struct PreparedPostgresStatement {
    /// SQL text with placeholders rewritten to `$1`, `$2`, ...
    pub statement: String,
    /// Parameter type OIDs supplied to `Parse` (0 lets the server infer).
    pub parameter_oids: Vec<i32>,
    /// Text-format parameter values supplied to `Bind` (`None` is SQL NULL).
    pub parameter_values: Vec<Option<Vec<u8>>>,
    /// Placeholder convention that was detected.
    pub placeholder_style: ParameterPlaceholderStyle,
}

impl PreparedPostgresStatement {
    /// Returns `true` when at least one parameter must be sent through `Bind`.
    pub fn is_parameterized(&self) -> bool {
        !self.parameter_values.is_empty()
    }
}

fn unparameterized(statement: &str) -> PreparedPostgresStatement {
    PreparedPostgresStatement {
        statement: statement.to_string(),
        parameter_oids: Vec::new(),
        parameter_values: Vec::new(),
        placeholder_style: ParameterPlaceholderStyle::None,
    }
}

/// Prepares a PostgreSQL / Apache AGE statement for the extended query protocol.
///
/// The returned SQL text never contains a caller-supplied parameter value.
pub fn prepare_postgres_statement(
    query: &str,
    params: &HashMap<String, ParamValue>,
) -> Result<PreparedPostgresStatement> {
    if params.is_empty() {
        return Ok(unparameterized(query));
    }

    if looks_like_age_statement(query) {
        let (rewritten, age_placeholders) = rewrite_age_parameter_placeholders(query);
        if age_placeholders > 0 {
            return Ok(PreparedPostgresStatement {
                statement: rewritten,
                // OID 0 lets PostgreSQL resolve the third `cypher()` argument to
                // agtype, exactly as the psycopg bridge's unknown text value does.
                parameter_oids: vec![0],
                parameter_values: vec![Some(age_parameter_map(params)?)],
                placeholder_style: ParameterPlaceholderStyle::AgeParameterMap,
            });
        }
    }

    let (statement, bound) = rewrite_named_placeholders(query, params)?;
    if bound.is_empty() {
        return Ok(unparameterized(query));
    }

    let (parameter_values, parameter_oids) = bound.into_iter().unzip();
    Ok(PreparedPostgresStatement {
        statement,
        parameter_oids,
        parameter_values,
        placeholder_style: ParameterPlaceholderStyle::Named,
    })
}

/// PostgreSQL type OID constants needed to type parameter values.
mod parameter_oids {
    pub(super) const BOOL: i32 = 16;
    pub(super) const BYTEA: i32 = 17;
    pub(super) const INT8: i32 = 20;
    pub(super) const FLOAT8: i32 = 701;
    pub(super) const DATE: i32 = 1082;
    pub(super) const TIME: i32 = 1083;
    pub(super) const TIMESTAMP: i32 = 1114;
    pub(super) const TIMESTAMPTZ: i32 = 1184;
    pub(super) const INTERVAL: i32 = 1186;
    pub(super) const TIMETZ: i32 = 1266;
    pub(super) const NUMERIC: i32 = 1700;
    pub(super) const UUID: i32 = 2950;
    pub(super) const JSONB: i32 = 3802;

    pub(super) const BOOL_ARRAY: i32 = 1000;
    pub(super) const BYTEA_ARRAY: i32 = 1001;
    pub(super) const INT8_ARRAY: i32 = 1016;
    pub(super) const FLOAT8_ARRAY: i32 = 1022;
    pub(super) const DATE_ARRAY: i32 = 1182;
    pub(super) const TIME_ARRAY: i32 = 1183;
    pub(super) const TIMESTAMP_ARRAY: i32 = 1115;
    pub(super) const TIMESTAMPTZ_ARRAY: i32 = 1185;
    pub(super) const INTERVAL_ARRAY: i32 = 1187;
    pub(super) const TIMETZ_ARRAY: i32 = 1270;
    pub(super) const NUMERIC_ARRAY: i32 = 1231;
    pub(super) const UUID_ARRAY: i32 = 2951;
    pub(super) const JSONB_ARRAY: i32 = 3807;
}

/// Chooses a PostgreSQL type OID for a parameter value.
///
/// `0` means "untyped, let the server infer". Typed values (booleans, numbers,
/// temporal values, UUID, numeric, bytea, and homogeneous arrays of them) carry
/// their PostgreSQL type OID. Genuine strings (`ParamValue::Json(Value::String(_))`),
/// SQL NULL, empty lists, and mixed lists stay untyped (`0`) so the server
/// resolves them from column context; maps are `jsonb` and lists of maps are
/// `jsonb[]`.
fn value_oid(value: &ParamValue) -> i32 {
    match value {
        ParamValue::Json(v) => json_value_oid(v),
        ParamValue::Jsonb(_) | ParamValue::Map(_) => parameter_oids::JSONB,
        ParamValue::List(items) => param_array_oid(items).unwrap_or(0),
        other => scalar_param_oid(other).unwrap_or(0),
    }
}

/// Selects the wire text and type OID for one bound parameter.
///
/// `force_json` is set when the placeholder is followed by an explicit `::json` /
/// `::jsonb` cast. Maps and `Json(...)` wrappers always encode as JSON text with
/// the `jsonb` OID. Lists keep the array behaviour psycopg has: scalar lists are
/// typed PostgreSQL arrays, lists of maps become `jsonb[]` (each map is one JSON
/// element), and nested lists become multidimensional arrays (`{{1,2},{3,4}}`).
/// With an explicit JSON cast (or a `Json(...)` wrapper) every list, including
/// `[]` and `[[]]`, is encoded as one JSON document, so `[]` is never sent as a
/// JSON object.
fn bind_value(value: &ParamValue, force_json: bool) -> Result<BoundParameter> {
    if let ParamValue::Jsonb(inner) = value {
        return Ok((Some(json_param_text(inner)?), parameter_oids::JSONB));
    }
    if force_json || value_requires_json(value) {
        return Ok((Some(json_param_text(value)?), parameter_oids::JSONB));
    }
    Ok((value_to_text(value)?, value_oid(value)))
}

/// Returns `true` when a value must be encoded as a JSON document rather than a
/// PostgreSQL array/scalar, even without an explicit JSON cast.
fn value_requires_json(value: &ParamValue) -> bool {
    matches!(
        value,
        ParamValue::Jsonb(_) | ParamValue::Map(_) | ParamValue::Json(Value::Object(_))
    )
}

fn json_value_oid(value: &Value) -> i32 {
    match value {
        Value::Null | Value::String(_) | Value::Object(_) => 0,
        Value::Array(items) => json_array_oid(items).unwrap_or(0),
        other => scalar_json_oid(other).unwrap_or(0),
    }
}

fn element_to_array_oid(element: i32) -> i32 {
    use parameter_oids as oid;

    match element {
        oid::BOOL => oid::BOOL_ARRAY,
        oid::BYTEA => oid::BYTEA_ARRAY,
        oid::INT8 => oid::INT8_ARRAY,
        oid::FLOAT8 => oid::FLOAT8_ARRAY,
        oid::DATE => oid::DATE_ARRAY,
        oid::TIME => oid::TIME_ARRAY,
        oid::TIMESTAMP => oid::TIMESTAMP_ARRAY,
        oid::TIMESTAMPTZ => oid::TIMESTAMPTZ_ARRAY,
        oid::INTERVAL => oid::INTERVAL_ARRAY,
        oid::TIMETZ => oid::TIMETZ_ARRAY,
        oid::NUMERIC => oid::NUMERIC_ARRAY,
        oid::UUID => oid::UUID_ARRAY,
        oid::JSONB => oid::JSONB_ARRAY,
        _ => element,
    }
}

/// Chooses the array type OID for a typed list, or `None` when ambiguous.
fn param_array_oid(items: &[ParamValue]) -> Option<i32> {
    let mut element_oid: Option<i32> = None;
    for item in items {
        let candidate = match item {
            ParamValue::Json(Value::Null) => continue,
            ParamValue::List(nested) => base_param_array_element_oid(nested, 1)?,
            ParamValue::Json(Value::Array(nested)) => base_json_array_element_oid(nested, 1)?,
            other => param_element_oid(other)?,
        };
        match element_oid {
            None => element_oid = Some(candidate),
            Some(existing) if existing == candidate => {}
            Some(_) => return None,
        }
    }

    element_oid.map(element_to_array_oid)
}

/// Returns the PostgreSQL element OID for one item of a SQL array.
///
/// Maps and `Json(...)` wrappers are elements of a `jsonb[]` array, matching
/// psycopg's adaptation of a list of dicts.
fn param_element_oid(value: &ParamValue) -> Option<i32> {
    match value {
        ParamValue::Map(_) | ParamValue::Jsonb(_) | ParamValue::Json(Value::Object(_)) => {
            Some(parameter_oids::JSONB)
        }
        other => scalar_param_oid(other),
    }
}

fn base_param_array_element_oid(items: &[ParamValue], depth: usize) -> Option<i32> {
    if depth > MAX_NESTING_DEPTH {
        return None;
    }
    for item in items {
        match item {
            ParamValue::Json(Value::Null) => continue,
            ParamValue::List(nested) => return base_param_array_element_oid(nested, depth + 1),
            ParamValue::Json(Value::Array(nested)) => {
                return base_json_array_element_oid(nested, depth + 1);
            }
            other => return param_element_oid(other),
        }
    }
    None
}

fn scalar_param_oid(value: &ParamValue) -> Option<i32> {
    use parameter_oids as oid;

    match value {
        ParamValue::Json(v) => scalar_json_oid(v),
        ParamValue::Float(_) => Some(oid::FLOAT8),
        ParamValue::Bytes(_) => Some(oid::BYTEA),
        ParamValue::Date { .. } => Some(oid::DATE),
        ParamValue::Time {
            tz_offset_seconds: None,
            ..
        } => Some(oid::TIME),
        ParamValue::Time {
            tz_offset_seconds: Some(_),
            ..
        } => Some(oid::TIMETZ),
        ParamValue::LocalDateTime { .. } => Some(oid::TIMESTAMP),
        ParamValue::DateTime { .. } => Some(oid::TIMESTAMPTZ),
        ParamValue::Duration { .. } => Some(oid::INTERVAL),
        ParamValue::Uuid(_) => Some(oid::UUID),
        ParamValue::Decimal(_) => Some(oid::NUMERIC),
        ParamValue::Point2D { .. } | ParamValue::Point3D { .. } => None,
        ParamValue::List(_) | ParamValue::Map(_) | ParamValue::Jsonb(_) => None,
    }
}

/// Chooses the array type OID for a JSON list, or `None` when ambiguous.
fn json_array_oid(items: &[Value]) -> Option<i32> {
    let mut element_oid: Option<i32> = None;
    for item in items {
        let candidate = match item {
            Value::Null => continue,
            Value::Array(nested) => base_json_array_element_oid(nested, 1)?,
            Value::Object(_) => parameter_oids::JSONB,
            other => scalar_json_oid(other)?,
        };
        match element_oid {
            None => element_oid = Some(candidate),
            Some(existing) if existing == candidate => {}
            Some(_) => return None,
        }
    }

    element_oid.map(element_to_array_oid)
}

/// Returns the scalar element OID nested inside a JSON list.
fn base_json_array_element_oid(items: &[Value], depth: usize) -> Option<i32> {
    if depth > MAX_NESTING_DEPTH {
        return None;
    }
    for item in items {
        match item {
            Value::Null => continue,
            Value::Array(nested) => return base_json_array_element_oid(nested, depth + 1),
            Value::Object(_) => return Some(parameter_oids::JSONB),
            other => return scalar_json_oid(other),
        }
    }
    None
}

/// Returns the PostgreSQL type OID for a non-null scalar JSON value.
fn scalar_json_oid(value: &Value) -> Option<i32> {
    use parameter_oids as oid;

    match value {
        Value::Bool(_) => Some(oid::BOOL),
        Value::Number(number) => {
            if number.as_i64().is_some() || number.as_u64().is_some() {
                Some(oid::INT8)
            } else {
                Some(oid::FLOAT8)
            }
        }
        // Strings and maps keep the whole list untyped (see `value_oid`).
        Value::String(_) | Value::Object(_) | Value::Null | Value::Array(_) => None,
    }
}

/// Returns `true` for statements emitted by [`voyager_ogm::emitters::AgeEmitter`].
fn looks_like_age_statement(query: &str) -> bool {
    let lower = query.to_ascii_lowercase();
    lower.contains("cypher(") || lower.contains("ag_catalog.cypher")
}

/// Serializes the full AGE parameter map into a deterministic JSON object.
fn age_parameter_map(params: &HashMap<String, ParamValue>) -> Result<Vec<u8>> {
    let sorted: BTreeMap<&str, &ParamValue> = params
        .iter()
        .map(|(key, value)| (key.as_str(), value))
        .collect();
    let mut out = String::new();
    out.push('{');
    for (i, (key, value)) in sorted.into_iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        push_json_string(key, &mut out);
        out.push(':');
        write_json_param(value, &mut out, 0)?;
    }
    out.push('}');
    Ok(out.into_bytes())
}

/// Rewrites the AGE `%s` driver placeholder (outside `$$ ... $$`) to `$1`.
///
/// AGE text is left untouched. Returns the statement and the number of
/// placeholders rewritten.
fn rewrite_age_parameter_placeholders(query: &str) -> (String, usize) {
    let bytes = query.as_bytes();
    let mut out = String::with_capacity(query.len());
    let mut count = 0usize;
    let mut i = 0usize;

    while i < bytes.len() {
        match bytes[i] {
            b'\'' => {
                let end = skip_single_quoted(bytes, i);
                push_slice(&mut out, query, i, end);
                i = end;
            }
            b'"' => {
                let end = skip_double_quoted(bytes, i);
                push_slice(&mut out, query, i, end);
                i = end;
            }
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                let end = skip_line_comment(bytes, i);
                push_slice(&mut out, query, i, end);
                i = end;
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                let end = skip_block_comment(bytes, i);
                push_slice(&mut out, query, i, end);
                i = end;
            }
            b'$' => {
                if let Some((opener_len, delimiter)) = dollar_quote_opener(bytes, i) {
                    let end = dollar_quote_end(query, i + opener_len, &delimiter);
                    push_slice(&mut out, query, i, end);
                    i = end;
                } else {
                    out.push('$');
                    i += 1;
                }
            }
            b'%' if bytes.get(i + 1) == Some(&b's') => {
                out.push_str("$1");
                count += 1;
                i += 2;
            }
            _ => {
                push_char(&mut out, query, i);
                i += char_len(query, i);
            }
        }
    }

    (out, count)
}

/// A bound value and its chosen PostgreSQL type OID (`0` means untyped).
type BoundParameter = (Option<Vec<u8>>, i32);

/// Rewrites named `$name` placeholders to positional `$1`, `$2`, ... values.
fn rewrite_named_placeholders(
    query: &str,
    params: &HashMap<String, ParamValue>,
) -> Result<(String, Vec<BoundParameter>)> {
    let bytes = query.as_bytes();
    let mut out = String::with_capacity(query.len());
    let mut bound: Vec<BoundParameter> = Vec::new();
    let mut indices: HashMap<String, usize> = HashMap::new();
    let mut i = 0usize;

    while i < bytes.len() {
        match bytes[i] {
            b'\'' => {
                let end = skip_single_quoted(bytes, i);
                push_slice(&mut out, query, i, end);
                i = end;
            }
            b'"' => {
                let end = skip_double_quoted(bytes, i);
                push_slice(&mut out, query, i, end);
                i = end;
            }
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                let end = skip_line_comment(bytes, i);
                push_slice(&mut out, query, i, end);
                i = end;
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                let end = skip_block_comment(bytes, i);
                push_slice(&mut out, query, i, end);
                i = end;
            }
            b'$' => {
                if let Some((opener_len, delimiter)) = dollar_quote_opener(bytes, i) {
                    let end = dollar_quote_end(query, i + opener_len, &delimiter);
                    push_slice(&mut out, query, i, end);
                    i = end;
                    continue;
                }

                // A `$` inside an unquoted identifier (PostgreSQL permits
                // `col$name`) is not a parameter placeholder.
                if i > 0 && (is_placeholder_name_byte(bytes[i - 1]) || bytes[i - 1] == b'$') {
                    out.push('$');
                    i += 1;
                    continue;
                }

                let name_start = i + 1;
                let mut name_end = name_start;
                while name_end < bytes.len() && is_placeholder_name_byte(bytes[name_end]) {
                    name_end += 1;
                }

                if name_end > name_start {
                    let name = &query[name_start..name_end];
                    if let Some(value) = params.get(name) {
                        let index = match indices.get(name) {
                            Some(index) => *index,
                            None => {
                                let index = bound.len() + 1;
                                indices.insert(name.to_string(), index);
                                let force_json = placeholder_forces_json(bytes, name_end);
                                bound.push(bind_value(value, force_json)?);
                                index
                            }
                        };
                        out.push('$');
                        out.push_str(&index.to_string());
                    } else {
                        push_slice(&mut out, query, i, name_end);
                    }
                    i = name_end;
                } else {
                    out.push('$');
                    i += 1;
                }
            }
            _ => {
                push_char(&mut out, query, i);
                i += char_len(query, i);
            }
        }
    }

    Ok((out, bound))
}

/// Returns `true` when the placeholder ending at `pos` is explicitly cast to
/// `json` or `jsonb` (`$v::jsonb`, `$v :: json`, `$v::pg_catalog.jsonb`).
fn placeholder_forces_json(bytes: &[u8], pos: usize) -> bool {
    let mut i = pos;
    while bytes.get(i).is_some_and(u8::is_ascii_whitespace) {
        i += 1;
    }
    if bytes.get(i) != Some(&b':') || bytes.get(i + 1) != Some(&b':') {
        return false;
    }
    i += 2;
    while bytes.get(i).is_some_and(u8::is_ascii_whitespace) {
        i += 1;
    }
    let start = i;
    while bytes
        .get(i)
        .is_some_and(|b| b.is_ascii_alphanumeric() || *b == b'_' || *b == b'.')
    {
        i += 1;
    }
    let name = std::str::from_utf8(&bytes[start..i])
        .unwrap_or("")
        .to_ascii_lowercase();
    let last = name.rsplit('.').next().unwrap_or("");
    last == "json" || last == "jsonb"
}

/// Formats an `f64` for PostgreSQL text-format `Bind` bytes (preserving `NaN`/`Infinity`/`-Infinity`).
fn format_pg_float(f: f64) -> String {
    if f.is_nan() {
        "NaN".to_string()
    } else if f.is_infinite() {
        if f.is_sign_positive() {
            "Infinity".to_string()
        } else {
            "-Infinity".to_string()
        }
    } else {
        f.to_string()
    }
}

/// Formats a `ParamValue::Duration` in PostgreSQL `interval` text input syntax.
fn format_pg_interval(months: i64, days: i64, seconds: i64, nanoseconds: i64) -> String {
    let total_us = (seconds as i128) * 1_000_000 + (nanoseconds as i128) / 1_000;
    if months == 0 && days == 0 && total_us == 0 {
        return "0 seconds".to_string();
    }
    let sign = if total_us < 0 { "-" } else { "" };
    let abs_us = total_us.unsigned_abs();
    let s = abs_us / 1_000_000;
    let us = abs_us % 1_000_000;
    format!("{months} mons {days} days {sign}{s}.{us:06} seconds")
}

/// Encodes a value as compact JSON text for a `json` / `jsonb` parameter.
///
/// Finite floats use the shortest round-trip representation. Non-finite floats
/// are written as `NaN` / `Infinity` / `-Infinity`, which PostgreSQL `jsonb`
/// (and Apache AGE `agtype`) accept even though they are not standard JSON.
fn json_param_text(value: &ParamValue) -> Result<Vec<u8>> {
    let mut out = String::new();
    write_json_param(value, &mut out, 0)?;
    Ok(out.into_bytes())
}

fn write_json_param(value: &ParamValue, out: &mut String, depth: usize) -> Result<()> {
    if depth > MAX_NESTING_DEPTH {
        return Err(NetError::ProtocolError(format!(
            "PostgreSQL JSON parameter exceeded maximum nesting depth of {MAX_NESTING_DEPTH}"
        )));
    }
    match value {
        ParamValue::Json(v) => write_json_value(v, out, depth)?,
        ParamValue::Jsonb(inner) => write_json_param(inner, out, depth + 1)?,
        ParamValue::Float(f) => write_json_f64(*f, out),
        ParamValue::List(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_json_param(item, out, depth + 1)?;
            }
            out.push(']');
        }
        ParamValue::Map(map) => {
            let sorted: BTreeMap<&str, &ParamValue> =
                map.iter().map(|(k, v)| (k.as_str(), v)).collect();
            out.push('{');
            for (i, (key, item)) in sorted.into_iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                push_json_string(key, out);
                out.push(':');
                write_json_param(item, out, depth + 1)?;
            }
            out.push('}');
        }
        other => write_json_value(&other.to_json_value(), out, depth)?,
    }
    Ok(())
}

fn write_json_value(value: &Value, out: &mut String, depth: usize) -> Result<()> {
    if depth > MAX_NESTING_DEPTH {
        return Err(NetError::ProtocolError(format!(
            "PostgreSQL JSON parameter exceeded maximum nesting depth of {MAX_NESTING_DEPTH}"
        )));
    }
    match value {
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_json_value(item, out, depth + 1)?;
            }
            out.push(']');
        }
        Value::Object(map) => {
            out.push('{');
            for (i, (key, item)) in map.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                push_json_string(key, out);
                out.push(':');
                write_json_value(item, out, depth + 1)?;
            }
            out.push('}');
        }
        other => match serde_json::to_string(other) {
            Ok(text) => out.push_str(&text),
            Err(_) => out.push_str("null"),
        },
    }
    Ok(())
}

fn write_json_f64(value: f64, out: &mut String) {
    if value.is_nan() {
        out.push_str("NaN");
    } else if value.is_infinite() {
        out.push_str(if value.is_sign_positive() {
            "Infinity"
        } else {
            "-Infinity"
        });
    } else {
        match serde_json::Number::from_f64(value) {
            Some(number) => out.push_str(&number.to_string()),
            None => out.push_str("null"),
        }
    }
}

fn push_json_string(value: &str, out: &mut String) {
    match serde_json::to_string(value) {
        Ok(text) => out.push_str(&text),
        Err(_) => out.push_str("\"\""),
    }
}

/// Encodes a typed parameter as PostgreSQL text-format `Bind` bytes.
fn value_to_text(value: &ParamValue) -> Result<Option<Vec<u8>>> {
    Ok(match value {
        ParamValue::Json(v) => json_to_text(v)?,
        ParamValue::Jsonb(inner) => Some(json_param_text(inner)?),
        ParamValue::Float(f) => Some(format_pg_float(*f).into_bytes()),
        ParamValue::Bytes(b) => Some(format!("\\x{}", hex::encode(b)).into_bytes()),
        ParamValue::Date { year, month, day } => {
            Some(format_iso_date(*year, *month, *day).into_bytes())
        }
        ParamValue::Time {
            hour,
            minute,
            second,
            nanosecond,
            tz_offset_seconds,
        } => Some(
            format_iso_time(*hour, *minute, *second, *nanosecond, *tz_offset_seconds).into_bytes(),
        ),
        ParamValue::LocalDateTime {
            year,
            month,
            day,
            hour,
            minute,
            second,
            nanosecond,
        } => Some(
            format!(
                "{}T{}",
                format_iso_date(*year, *month, *day),
                format_iso_time(*hour, *minute, *second, *nanosecond, None)
            )
            .into_bytes(),
        ),
        ParamValue::DateTime {
            utc_seconds,
            nanosecond,
            ..
        } => Some(format_datetime_iso(*utc_seconds, *nanosecond, None).into_bytes()),
        ParamValue::Duration {
            months,
            days,
            seconds,
            nanoseconds,
        } => Some(format_pg_interval(*months, *days, *seconds, *nanoseconds).into_bytes()),
        ParamValue::Uuid(s) | ParamValue::Decimal(s) => Some(s.as_bytes().to_vec()),
        ParamValue::Point2D { .. } | ParamValue::Point3D { .. } => {
            serde_json::to_vec(&value.to_json_value()).ok()
        }
        ParamValue::List(items) => {
            let mut out = String::new();
            encode_pg_param_array(items, &mut out, 0)?;
            Some(out.into_bytes())
        }
        ParamValue::Map(_) => serde_json::to_vec(&value.to_json_value()).ok(),
    })
}

fn json_to_text(value: &Value) -> Result<Option<Vec<u8>>> {
    Ok(match value {
        Value::Null => None,
        Value::String(s) => Some(s.as_bytes().to_vec()),
        Value::Bool(b) => Some(if *b {
            b"true".to_vec()
        } else {
            b"false".to_vec()
        }),
        Value::Number(n) => Some(n.to_string().into_bytes()),
        Value::Array(items) => {
            let mut out = String::new();
            encode_pg_array(items, &mut out, 0)?;
            Some(out.into_bytes())
        }
        // Top-level maps are sent as JSON text.
        Value::Object(_) => serde_json::to_vec(value).ok(),
    })
}

/// Encodes a typed parameter array as PostgreSQL array literal text (`{elem1,elem2}`).
fn encode_pg_param_array(items: &[ParamValue], out: &mut String, depth: usize) -> Result<()> {
    if depth > MAX_NESTING_DEPTH {
        return Err(NetError::ProtocolError(format!(
            "PostgreSQL array parameter exceeded maximum nesting depth of {MAX_NESTING_DEPTH}"
        )));
    }
    out.push('{');
    for (i, item) in items.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        encode_pg_param_array_element(item, out, depth + 1)?;
    }
    out.push('}');
    Ok(())
}

fn encode_pg_param_array_element(value: &ParamValue, out: &mut String, depth: usize) -> Result<()> {
    match value {
        ParamValue::Json(v) => encode_pg_array_element(v, out, depth)?,
        ParamValue::Float(f) => escape_and_quote_element(&format_pg_float(*f), out),
        ParamValue::Bytes(b) => escape_and_quote_element(&format!("\\x{}", hex::encode(b)), out),
        ParamValue::Date { year, month, day } => {
            escape_and_quote_element(&format_iso_date(*year, *month, *day), out);
        }
        ParamValue::Time {
            hour,
            minute,
            second,
            nanosecond,
            tz_offset_seconds,
        } => {
            escape_and_quote_element(
                &format_iso_time(*hour, *minute, *second, *nanosecond, *tz_offset_seconds),
                out,
            );
        }
        ParamValue::LocalDateTime {
            year,
            month,
            day,
            hour,
            minute,
            second,
            nanosecond,
        } => {
            let text = format!(
                "{}T{}",
                format_iso_date(*year, *month, *day),
                format_iso_time(*hour, *minute, *second, *nanosecond, None)
            );
            escape_and_quote_element(&text, out);
        }
        ParamValue::DateTime {
            utc_seconds,
            nanosecond,
            ..
        } => {
            escape_and_quote_element(&format_datetime_iso(*utc_seconds, *nanosecond, None), out);
        }
        ParamValue::Duration {
            months,
            days,
            seconds,
            nanoseconds,
        } => {
            escape_and_quote_element(
                &format_pg_interval(*months, *days, *seconds, *nanoseconds),
                out,
            );
        }
        ParamValue::Uuid(s) => escape_and_quote_element(s, out),
        ParamValue::Decimal(s) => out.push_str(s),
        ParamValue::List(nested) => encode_pg_param_array(nested, out, depth)?,
        ParamValue::Jsonb(inner) => {
            let mut json_buf = String::new();
            write_json_param(inner, &mut json_buf, depth)?;
            escape_and_quote_element(&json_buf, out);
        }
        ParamValue::Map(_) | ParamValue::Point2D { .. } | ParamValue::Point3D { .. } => {
            let mut json_buf = String::new();
            write_json_param(value, &mut json_buf, depth)?;
            escape_and_quote_element(&json_buf, out);
        }
    }
    Ok(())
}

/// Encodes a JSON array as PostgreSQL array literal text (`{elem1,elem2}`).
fn encode_pg_array(items: &[Value], out: &mut String, depth: usize) -> Result<()> {
    if depth > MAX_NESTING_DEPTH {
        return Err(NetError::ProtocolError(format!(
            "PostgreSQL array parameter exceeded maximum nesting depth of {MAX_NESTING_DEPTH}"
        )));
    }
    out.push('{');
    for (i, item) in items.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        encode_pg_array_element(item, out, depth + 1)?;
    }
    out.push('}');
    Ok(())
}

/// Encodes an element inside a PostgreSQL array literal.
fn encode_pg_array_element(value: &Value, out: &mut String, depth: usize) -> Result<()> {
    match value {
        Value::Null => out.push_str("NULL"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Number(n) => out.push_str(&n.to_string()),
        Value::String(s) => escape_and_quote_element(s, out),
        Value::Array(nested) => encode_pg_array(nested, out, depth)?,
        Value::Object(_) => {
            let mut json_buf = String::new();
            write_json_value(value, &mut json_buf, depth)?;
            escape_and_quote_element(&json_buf, out);
        }
    }
    Ok(())
}

/// Double-quotes an array element, escaping `\` as `\\` and `"` as `\"`.
fn escape_and_quote_element(s: &str, out: &mut String) {
    out.push('"');
    for ch in s.chars() {
        match ch {
            '\\' => out.push_str(r"\\"),
            '"' => out.push_str(r#"\""#),
            c => out.push(c),
        }
    }
    out.push('"');
}

fn is_placeholder_name_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn push_slice(out: &mut String, query: &str, start: usize, end: usize) {
    if let Some(slice) = query.get(start..end) {
        out.push_str(slice);
    }
}

fn char_len(query: &str, index: usize) -> usize {
    query[index..].chars().next().map_or(1, char::len_utf8)
}

fn push_char(out: &mut String, query: &str, index: usize) {
    if let Some(ch) = query[index..].chars().next() {
        out.push(ch);
    }
}

fn skip_single_quoted(bytes: &[u8], start: usize) -> usize {
    let mut i = start + 1;
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            if bytes.get(i + 1) == Some(&b'\'') {
                i += 2;
                continue;
            }
            return i + 1;
        }
        i += 1;
    }
    bytes.len()
}

fn skip_double_quoted(bytes: &[u8], start: usize) -> usize {
    let mut i = start + 1;
    while i < bytes.len() {
        if bytes[i] == b'"' {
            if bytes.get(i + 1) == Some(&b'"') {
                i += 2;
                continue;
            }
            return i + 1;
        }
        i += 1;
    }
    bytes.len()
}

fn skip_line_comment(bytes: &[u8], start: usize) -> usize {
    let mut i = start + 2;
    while i < bytes.len() {
        if bytes[i] == b'\n' {
            return i + 1;
        }
        i += 1;
    }
    bytes.len()
}

fn skip_block_comment(bytes: &[u8], start: usize) -> usize {
    let mut depth = 1usize;
    let mut i = start + 2;
    while i < bytes.len() {
        if bytes[i] == b'/' && bytes.get(i + 1) == Some(&b'*') {
            depth += 1;
            i += 2;
        } else if bytes[i] == b'*' && bytes.get(i + 1) == Some(&b'/') {
            depth -= 1;
            i += 2;
            if depth == 0 {
                return i;
            }
        } else {
            i += 1;
        }
    }
    bytes.len()
}

/// Detects a PostgreSQL dollar-quote opener (`$$` or `$tag$`).
fn dollar_quote_opener(bytes: &[u8], start: usize) -> Option<(usize, String)> {
    if bytes.get(start) != Some(&b'$') {
        return None;
    }
    let mut i = start + 1;
    if bytes.get(i) == Some(&b'$') {
        return Some((2, "$$".to_string()));
    }
    if !bytes
        .get(i)
        .is_some_and(|b| b.is_ascii_alphabetic() || *b == b'_')
    {
        return None;
    }
    i += 1;
    while i < bytes.len() && is_placeholder_name_byte(bytes[i]) {
        i += 1;
    }
    if bytes.get(i) == Some(&b'$') {
        let delimiter = String::from_utf8_lossy(&bytes[start..=i]).into_owned();
        return Some((i + 1 - start, delimiter));
    }
    None
}

/// Returns the byte index just past the closing dollar-quote delimiter.
fn dollar_quote_end(query: &str, body_start: usize, delimiter: &str) -> usize {
    match query[body_start..].find(delimiter) {
        Some(offset) => body_start + offset + delimiter.len(),
        None => query.len(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn params(entries: &[(&str, Value)]) -> HashMap<String, ParamValue> {
        entries
            .iter()
            .map(|(key, value)| ((*key).to_string(), ParamValue::from(value.clone())))
            .collect()
    }

    #[test]
    fn age_placeholder_becomes_positional_and_binds_json_map() {
        let query = "SELECT * FROM cypher('g', $$ MATCH (n) WHERE n.name = $p0 RETURN n $$, %s) AS (n agtype)";
        let prepared = prepare_postgres_statement(query, &params(&[("p0", json!("Alice"))]))
            .expect("prepare should succeed");

        assert_eq!(
            prepared.statement,
            "SELECT * FROM cypher('g', $$ MATCH (n) WHERE n.name = $p0 RETURN n $$, $1) AS (n agtype)"
        );
        assert_eq!(
            prepared.placeholder_style,
            ParameterPlaceholderStyle::AgeParameterMap
        );
        assert_eq!(prepared.parameter_oids, vec![0]);
        let value = prepared.parameter_values[0].as_ref().expect("bound value");
        assert_eq!(value, br#"{"p0":"Alice"}"#);
    }

    #[test]
    fn age_rewrite_ignores_percent_s_inside_body_strings_and_comments() {
        let query = "SELECT * FROM cypher('g', $$ RETURN '$p0 %s' $$, %s) AS (v agtype) -- %s\n";
        let prepared = prepare_postgres_statement(query, &params(&[("p0", json!("x"))]))
            .expect("prepare should succeed");

        assert!(prepared.statement.contains("RETURN '$p0 %s'"));
        assert!(prepared.statement.contains(", $1)"));
        assert!(prepared.statement.ends_with("-- %s\n"));
        assert!(!prepared.statement.contains(", %s)"));
    }

    #[test]
    fn age_parameter_map_supports_quotes_dollars_backslashes_unicode_and_nulls() {
        let query = "SELECT * FROM cypher('g', $$ RETURN $p0 $$, %s) AS (v agtype)";
        let prepared = prepare_postgres_statement(
            query,
            &params(&[
                ("p0", json!("x') ; DROP TABLE t; --")),
                ("p1", json!("$$ \\ ünïcode")),
                ("p2", json!({"nested": {"list": [1, null, "a'b"]}})),
            ]),
        )
        .expect("prepare should succeed");

        let value = prepared.parameter_values[0].as_ref().expect("bound value");
        let decoded: Value = serde_json::from_slice(value).expect("valid JSON");
        assert_eq!(
            decoded,
            json!({
                "p0": "x') ; DROP TABLE t; --",
                "p1": "$$ \\ ünïcode",
                "p2": {"nested": {"list": [1, null, "a'b"]}},
            })
        );
        // The raw value must never appear in the statement text.
        assert!(!prepared.statement.contains("DROP TABLE"));
        assert!(!prepared.statement.contains("ünïcode"));
    }

    #[test]
    fn named_placeholders_bind_positionally_without_prefix_collisions() {
        let query = "SELECT * FROM t WHERE a = $p10 AND b = $p1 AND c = $p10 AND d = $p2";
        let prepared = prepare_postgres_statement(
            query,
            &params(&[
                ("p1", json!(42)),
                ("p10", json!(100)),
                ("p2", json!("O'Reilly")),
            ]),
        )
        .expect("prepare should succeed");

        assert_eq!(
            prepared.statement,
            "SELECT * FROM t WHERE a = $1 AND b = $2 AND c = $1 AND d = $3"
        );
        assert_eq!(
            prepared.parameter_values,
            vec![
                Some(b"100".to_vec()),
                Some(b"42".to_vec()),
                Some(b"O'Reilly".to_vec()),
            ]
        );
        assert_eq!(prepared.parameter_oids, vec![20, 20, 0]);
        assert_eq!(prepared.placeholder_style, ParameterPlaceholderStyle::Named);
    }

    #[test]
    fn named_rewrite_skips_dollar_quoted_bodies_strings_and_comments() {
        let query = "SELECT $$ $p0 $$, '$p0', \"$p0\", /* $p0 */ $p0, my$p0 -- $p0\n";
        let prepared = prepare_postgres_statement(query, &params(&[("p0", json!("bound"))]))
            .expect("prepare should succeed");

        assert_eq!(
            prepared.statement,
            "SELECT $$ $p0 $$, '$p0', \"$p0\", /* $p0 */ $1, my$p0 -- $p0\n"
        );
        assert_eq!(prepared.parameter_values, vec![Some(b"bound".to_vec())]);
    }

    #[test]
    fn null_maps_and_lists_are_encoded_for_bind() {
        let query = "SELECT $a, $b, $c";
        let prepared = prepare_postgres_statement(
            query,
            &params(&[
                ("a", Value::Null),
                ("b", json!({"k": "v"})),
                ("c", json!([1, 2, 3])),
            ]),
        )
        .expect("prepare should succeed");

        assert_eq!(
            prepared.parameter_values,
            vec![
                None,
                Some(br#"{"k":"v"}"#.to_vec()),
                Some(b"{1,2,3}".to_vec()),
            ]
        );
        // NULL stays untyped, maps are jsonb, and integer lists are int8[].
        assert_eq!(prepared.parameter_oids, vec![0, 3802, 1016]);
    }

    #[test]
    fn parameter_oids_are_derived_from_values() {
        let query = "SELECT $bool, $int, $float, $text, $null, $map, $list, $empty, $mixed";
        let prepared = prepare_postgres_statement(
            query,
            &params(&[
                ("bool", json!(false)),
                ("int", json!(42)),
                ("float", json!(3.5)),
                ("text", json!("hello")),
                ("null", Value::Null),
                ("map", json!({"k": [1, 2]})),
                ("list", json!(["a", "b"])),
                ("empty", json!([])),
                ("mixed", json!([1, "two"])),
            ]),
        )
        .expect("prepare should succeed");

        assert_eq!(
            prepared.parameter_oids,
            vec![16, 20, 701, 0, 0, 3802, 0, 0, 0]
        );
    }

    #[test]
    fn parameter_oids_cover_arrays_of_each_scalar_type() {
        let query = "SELECT $bools, $ints, $floats, $objects, $nested, $nulls";
        let prepared = prepare_postgres_statement(
            query,
            &params(&[
                ("bools", json!([true, false])),
                ("ints", json!([1, 2, null])),
                ("floats", json!([1.5, 2.5])),
                ("objects", json!([{"a": 1}])),
                ("nested", json!([[1, 2], [3, 4]])),
                ("nulls", json!([null, null])),
            ]),
        )
        .expect("prepare should succeed");

        assert_eq!(
            prepared.parameter_oids,
            vec![1000, 1016, 1022, 3807, 1016, 0]
        );
    }

    #[test]
    fn age_parameter_map_remains_untyped_for_agtype_resolution() {
        let query = "SELECT * FROM cypher('g', $$ RETURN $p0 $$, %s) AS (v agtype)";
        let prepared = prepare_postgres_statement(query, &params(&[("p0", json!(true))]))
            .expect("prepare should succeed");

        assert_eq!(
            prepared.placeholder_style,
            ParameterPlaceholderStyle::AgeParameterMap
        );
        assert_eq!(prepared.parameter_oids, vec![0]);
        assert_eq!(
            prepared.parameter_values,
            vec![Some(br#"{"p0":true}"#.to_vec())]
        );
    }

    #[test]
    fn pg_array_parameter_encoding_supports_all_types() {
        let query = "SELECT $empty, $strings, $numbers, $bools, $nulls, $nested, $maps";
        let prepared = prepare_postgres_statement(
            query,
            &params(&[
                ("empty", json!([])),
                (
                    "strings",
                    json!(["foo\"bar", "foo\\bar", "a,b", "{brace}", "NULL"]),
                ),
                ("numbers", json!([1, -42, 3.5])),
                ("bools", json!([true, false])),
                ("nulls", json!([null, null])),
                ("nested", json!([[1, 2], [3, 4]])),
                ("maps", json!([{"a": 1}])),
            ]),
        )
        .expect("prepare should succeed");

        assert_eq!(
            prepared.parameter_values[0].as_deref(),
            Some(b"{}".as_slice())
        );
        assert_eq!(
            prepared.parameter_values[1].as_deref(),
            Some(br#"{"foo\"bar","foo\\bar","a,b","{brace}","NULL"}"#.as_slice())
        );
        assert_eq!(
            prepared.parameter_values[2].as_deref(),
            Some(b"{1,-42,3.5}".as_slice())
        );
        assert_eq!(
            prepared.parameter_values[3].as_deref(),
            Some(b"{true,false}".as_slice())
        );
        assert_eq!(
            prepared.parameter_values[4].as_deref(),
            Some(b"{NULL,NULL}".as_slice())
        );
        assert_eq!(
            prepared.parameter_values[5].as_deref(),
            Some(b"{{1,2},{3,4}}".as_slice())
        );
        assert_eq!(
            prepared.parameter_values[6].as_deref(),
            Some(br#"{"{\"a\":1}"}"#.as_slice())
        );
        assert_eq!(
            &prepared.parameter_oids[5..7],
            &[1016, parameter_oids::JSONB_ARRAY]
        );
    }

    #[test]
    fn explicit_jsonb_cast_forces_json_for_empty_and_scalar_lists() {
        let query = "SELECT $empty::jsonb, $ints::jsonb, $plain :: jsonb, $array_cast::bigint[], \
                     $maps::jsonb, $nested::jsonb";
        let prepared = prepare_postgres_statement(
            query,
            &params(&[
                ("empty", json!([])),
                ("ints", json!([1, 2])),
                ("plain", json!({"k": 1})),
                ("array_cast", json!([])),
                ("maps", json!([{}])),
                ("nested", json!([[]])),
            ]),
        )
        .expect("prepare should succeed");

        assert_eq!(
            prepared.parameter_oids,
            vec![3802, 3802, 3802, 0, 3802, 3802]
        );
        assert_eq!(
            prepared.parameter_values,
            vec![
                Some(b"[]".to_vec()),
                Some(b"[1,2]".to_vec()),
                Some(br#"{"k":1}"#.to_vec()),
                Some(b"{}".to_vec()),
                Some(b"[{}]".to_vec()),
                Some(b"[[]]".to_vec()),
            ]
        );
    }

    #[test]
    fn explicit_json_wrapper_binds_json_null_and_non_finite_floats() {
        let query = "SELECT $null::jsonb, $inf, $map";
        let mut p = HashMap::new();
        p.insert(
            "null".to_string(),
            ParamValue::Jsonb(Box::new(ParamValue::Json(Value::Null))),
        );
        p.insert(
            "inf".to_string(),
            ParamValue::Jsonb(Box::new(ParamValue::Float(f64::INFINITY))),
        );
        p.insert(
            "map".to_string(),
            ParamValue::Jsonb(Box::new(ParamValue::Map(HashMap::from([
                ("nan".to_string(), ParamValue::Float(f64::NAN)),
                (
                    "huge".to_string(),
                    ParamValue::Json(serde_json::Value::Number(
                        serde_json::Number::from_f64(1.7976931348623157e308).unwrap(),
                    )),
                ),
            ])))),
        );

        let prepared = prepare_postgres_statement(query, &p).expect("prepare should succeed");
        assert_eq!(prepared.parameter_oids, vec![3802, 3802, 3802]);
        assert_eq!(
            prepared.parameter_values,
            vec![
                Some(b"null".to_vec()),
                Some(b"Infinity".to_vec()),
                Some(br#"{"huge":1.7976931348623157e+308,"nan":NaN}"#.to_vec()),
            ]
        );
    }

    #[test]
    fn typed_nested_lists_bind_as_multidimensional_arrays_without_a_cast() {
        let mut p = HashMap::new();
        p.insert(
            "v".to_string(),
            ParamValue::List(vec![
                ParamValue::List(vec![ParamValue::from(1), ParamValue::from(2)]),
                ParamValue::List(vec![ParamValue::from(3), ParamValue::from(4)]),
            ]),
        );
        assert!(!value_requires_json(&p["v"]));
        let prepared = prepare_postgres_statement("SELECT $v", &p).expect("prepare should succeed");
        assert_eq!(prepared.parameter_oids, vec![1016]);
        assert_eq!(
            prepared.parameter_values,
            vec![Some(b"{{1,2},{3,4}}".to_vec())]
        );
    }

    #[test]
    fn typed_list_of_maps_binds_as_jsonb_array_without_a_cast() {
        let mut p = HashMap::new();
        p.insert(
            "v".to_string(),
            ParamValue::List(vec![
                ParamValue::Map(HashMap::from([("a".to_string(), ParamValue::from(1))])),
                ParamValue::Map(HashMap::from([("b".to_string(), ParamValue::from(2))])),
            ]),
        );
        assert!(!value_requires_json(&p["v"]));
        let prepared = prepare_postgres_statement("SELECT $v", &p).expect("prepare should succeed");
        assert_eq!(prepared.parameter_oids, vec![parameter_oids::JSONB_ARRAY]);
        assert_eq!(
            prepared.parameter_values,
            vec![Some(br#"{"{\"a\":1}","{\"b\":2}"}"#.to_vec())]
        );
    }

    #[test]
    fn ragged_nested_lists_stay_an_untyped_array_literal() {
        // PostgreSQL cannot represent ragged multidimensional arrays; the value
        // is still sent as an array literal, so the server rejects it exactly
        // like the psycopg adaptation does. Use `Json(...)`/`::jsonb` for ragged JSON.
        let mut p = HashMap::new();
        p.insert(
            "v".to_string(),
            ParamValue::List(vec![
                ParamValue::List(vec![ParamValue::from(1), ParamValue::from(2)]),
                ParamValue::List(vec![]),
            ]),
        );
        let prepared = prepare_postgres_statement("SELECT $v", &p).expect("prepare should succeed");
        assert_eq!(prepared.parameter_oids, vec![0]);
        assert_eq!(
            prepared.parameter_values,
            vec![Some(b"{{1,2},{}}".to_vec())]
        );
    }

    #[test]
    fn json_arrays_of_scalars_stay_arrays_without_a_json_cast() {
        let prepared = prepare_postgres_statement("SELECT $v", &params(&[("v", json!([1, 2, 3]))]))
            .expect("prepare should succeed");
        assert_eq!(prepared.parameter_oids, vec![1016]);
        assert_eq!(prepared.parameter_values, vec![Some(b"{1,2,3}".to_vec())]);
    }

    #[test]
    fn empty_parameter_map_leaves_statement_untouched() {
        let query = "SELECT * FROM cypher('g', $$ RETURN 1 $$) AS (v agtype)";
        let prepared =
            prepare_postgres_statement(query, &HashMap::new()).expect("prepare should succeed");
        assert_eq!(prepared.statement, query);
        assert!(!prepared.is_parameterized());
        assert_eq!(prepared.placeholder_style, ParameterPlaceholderStyle::None);
    }

    #[test]
    fn typed_temporal_uuid_numeric_and_bytea_oids_and_text_encodings() {
        use crate::params::ParamTimezone;

        let mut p = HashMap::new();
        p.insert(
            "d".to_string(),
            ParamValue::Date {
                year: 2024,
                month: 1,
                day: 2,
            },
        );
        p.insert(
            "t".to_string(),
            ParamValue::Time {
                hour: 14,
                minute: 30,
                second: 15,
                nanosecond: 123_456_000,
                tz_offset_seconds: None,
            },
        );
        p.insert(
            "tz".to_string(),
            ParamValue::Time {
                hour: 14,
                minute: 30,
                second: 15,
                nanosecond: 0,
                tz_offset_seconds: Some(7200),
            },
        );
        p.insert(
            "ts".to_string(),
            ParamValue::LocalDateTime {
                year: 2024,
                month: 1,
                day: 2,
                hour: 3,
                minute: 4,
                second: 5,
                nanosecond: 123_456_000,
            },
        );
        p.insert(
            "tstz".to_string(),
            ParamValue::DateTime {
                utc_seconds: 1_704_164_645,
                nanosecond: 123_456_000,
                tz: ParamTimezone::OffsetSeconds(0),
            },
        );
        p.insert(
            "iv".to_string(),
            ParamValue::Duration {
                months: 0,
                days: 2,
                seconds: 3661,
                nanoseconds: 500_000_000,
            },
        );
        p.insert(
            "neg_iv".to_string(),
            ParamValue::Duration {
                months: 0,
                days: -1,
                seconds: 86395,
                nanoseconds: 0,
            },
        );
        p.insert(
            "u".to_string(),
            ParamValue::Uuid("12345678-1234-5678-1234-567812345678".to_string()),
        );
        p.insert("num".to_string(), ParamValue::Decimal("123.45".to_string()));
        p.insert("b".to_string(), ParamValue::Bytes(vec![0x01, 0xAB, 0xFF]));
        p.insert("raw_str".to_string(), ParamValue::from("2024-01-02"));

        let query = "SELECT $d, $t, $tz, $ts, $tstz, $iv, $neg_iv, $u, $num, $b, $raw_str";
        let prepared = prepare_postgres_statement(query, &p).expect("prepare should succeed");

        assert_eq!(
            prepared.parameter_oids,
            vec![1082, 1083, 1266, 1114, 1184, 1186, 1186, 2950, 1700, 17, 0]
        );
        assert_eq!(
            prepared.parameter_values,
            vec![
                Some(b"2024-01-02".to_vec()),
                Some(b"14:30:15.123456".to_vec()),
                Some(b"14:30:15+02:00".to_vec()),
                Some(b"2024-01-02T03:04:05.123456".to_vec()),
                Some(b"2024-01-02T03:04:05.123456+00:00".to_vec()),
                Some(b"0 mons 2 days 3661.500000 seconds".to_vec()),
                Some(b"0 mons -1 days 86395.000000 seconds".to_vec()),
                Some(b"12345678-1234-5678-1234-567812345678".to_vec()),
                Some(b"123.45".to_vec()),
                Some(br"\x01abff".to_vec()),
                Some(b"2024-01-02".to_vec()),
            ]
        );
    }

    #[test]
    fn typed_arrays_use_matching_array_oids_and_mixed_stay_untyped() {
        use crate::params::ParamTimezone;

        let mut p = HashMap::new();
        p.insert(
            "dates".to_string(),
            ParamValue::List(vec![
                ParamValue::Date {
                    year: 2024,
                    month: 1,
                    day: 2,
                },
                ParamValue::Json(Value::Null),
            ]),
        );
        p.insert(
            "times".to_string(),
            ParamValue::List(vec![ParamValue::Time {
                hour: 1,
                minute: 2,
                second: 3,
                nanosecond: 0,
                tz_offset_seconds: None,
            }]),
        );
        p.insert(
            "timetzs".to_string(),
            ParamValue::List(vec![ParamValue::Time {
                hour: 1,
                minute: 2,
                second: 3,
                nanosecond: 0,
                tz_offset_seconds: Some(0),
            }]),
        );
        p.insert(
            "tss".to_string(),
            ParamValue::List(vec![ParamValue::LocalDateTime {
                year: 2024,
                month: 1,
                day: 2,
                hour: 3,
                minute: 4,
                second: 5,
                nanosecond: 0,
            }]),
        );
        p.insert(
            "tstzs".to_string(),
            ParamValue::List(vec![ParamValue::DateTime {
                utc_seconds: 1_704_164_645,
                nanosecond: 0,
                tz: ParamTimezone::OffsetSeconds(0),
            }]),
        );
        p.insert(
            "ivs".to_string(),
            ParamValue::List(vec![ParamValue::Duration {
                months: 0,
                days: 1,
                seconds: 0,
                nanoseconds: 0,
            }]),
        );
        p.insert(
            "uuids".to_string(),
            ParamValue::List(vec![ParamValue::Uuid(
                "12345678-1234-5678-1234-567812345678".to_string(),
            )]),
        );
        p.insert(
            "nums".to_string(),
            ParamValue::List(vec![ParamValue::Decimal("12.34".to_string())]),
        );
        p.insert(
            "byteas".to_string(),
            ParamValue::List(vec![ParamValue::Bytes(vec![0x01, 0x02])]),
        );
        p.insert(
            "mixed".to_string(),
            ParamValue::List(vec![
                ParamValue::Date {
                    year: 2024,
                    month: 1,
                    day: 2,
                },
                ParamValue::from("2024-01-03"),
            ]),
        );

        let query =
            "SELECT $dates, $times, $timetzs, $tss, $tstzs, $ivs, $uuids, $nums, $byteas, $mixed";
        let prepared = prepare_postgres_statement(query, &p).expect("prepare should succeed");
        assert_eq!(
            prepared.parameter_oids,
            vec![1182, 1183, 1270, 1115, 1185, 1187, 2951, 1231, 1001, 0]
        );
        assert_eq!(
            prepared.parameter_values[0].as_deref(),
            Some(br#"{"2024-01-02",NULL}"#.as_slice())
        );
        assert_eq!(
            prepared.parameter_values[8].as_deref(),
            Some(br#"{"\\x0102"}"#.as_slice())
        );
    }

    #[test]
    fn age_parameter_map_handles_typed_params_deterministically() {
        let mut p = HashMap::new();
        p.insert(
            "d".to_string(),
            ParamValue::Date {
                year: 2024,
                month: 1,
                day: 2,
            },
        );
        p.insert("dec".to_string(), ParamValue::Decimal("12.5".to_string()));
        p.insert(
            "u".to_string(),
            ParamValue::Uuid("12345678-1234-5678-1234-567812345678".to_string()),
        );

        let query = "SELECT * FROM cypher('g', $$ RETURN $d, $dec, $u $$, %s) AS (v agtype)";
        let prepared = prepare_postgres_statement(query, &p).expect("prepare should succeed");
        assert_eq!(prepared.parameter_oids, vec![0]);
        let decoded: Value =
            serde_json::from_slice(prepared.parameter_values[0].as_ref().unwrap()).unwrap();
        assert_eq!(
            decoded,
            json!({
                "d": "2024-01-02",
                "dec": 12.5,
                "u": "12345678-1234-5678-1234-567812345678",
            })
        );
    }
}
