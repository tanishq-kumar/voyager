//! Python hydration for `ResultValue` and Arrow values, plus parameter conversion.
//!
//! All value decoding lives here so the PyO3 surface has one conversation with
//! the value contract: [`voyager_drivers::ResultValue`] for row-oriented results,
//! canonical Arrow child-field metadata for columnar results, and
//! [`voyager_drivers::ParamValue`] for query inputs.

use std::collections::HashMap;
use std::sync::Mutex;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{
    PyBool, PyByteArray, PyBytes, PyDict, PyFloat, PyFrozenSet, PyInt, PyList, PyString, PyTuple,
};

use voyager_drivers::value::result::{
    ResultNode, ResultPath, ResultRelationship, ResultTimezone, ResultValue,
};
use voyager_drivers::value::{
    ARROW_EXTENSION_NAME, ARROW_UUID_EXTENSION, META_ENCODING_JSON, META_TYPE_DATE,
    META_TYPE_DATETIME_TZ, META_TYPE_DECIMAL, META_TYPE_DURATION, META_TYPE_JSON,
    META_TYPE_LOCAL_DATETIME, META_TYPE_POINT, META_TYPE_TIME, META_TYPE_TIME_TZ, META_TYPE_UUID,
    VOYAGER_META_ENCODING, VOYAGER_META_KIND, VOYAGER_META_TYPE,
};

struct EntityConstructors {
    node_from_trusted: Py<PyAny>,
    rel_from_trusted: Py<PyAny>,
    path_from_trusted: Py<PyAny>,
    datetime_cls: Py<PyAny>,
    date_cls: Py<PyAny>,
    time_cls: Py<PyAny>,
    timedelta_cls: Py<PyAny>,
    timezone_cls: Py<PyAny>,
    utc_obj: Py<PyAny>,
    uuid_cls: Py<PyAny>,
    decimal_cls: Py<PyAny>,
    zoneinfo_cls: Py<PyAny>,
    zoneinfo_cache: Mutex<HashMap<String, Py<PyAny>>>,
    duration_cls: Py<PyAny>,
    point_cls: Py<PyAny>,
    parse_pg_interval_fn: Py<PyAny>,
}

/// Upper bound on cached `ZoneInfo` objects for a process.
const ZONEINFO_CACHE_CAPACITY: usize = 64;

static ENTITY_CONSTRUCTORS: PyOnceLock<EntityConstructors> = PyOnceLock::new();

fn entity_constructors<'py>(py: Python<'py>) -> PyResult<&'py EntityConstructors> {
    ENTITY_CONSTRUCTORS.get_or_try_init(py, || {
        let mod_entities = py.import("voyager_ogm.entities")?;
        let node_cls = mod_entities.getattr("GraphNode")?;
        let rel_cls = mod_entities.getattr("GraphRelationship")?;
        let path_cls = mod_entities.getattr("GraphPath")?;
        let dt_mod = py.import("datetime")?;
        let timezone_cls = dt_mod.getattr("timezone")?;
        let utc_obj = timezone_cls.getattr("utc")?.unbind();
        let uuid_mod = py.import("uuid")?;
        let dec_mod = py.import("decimal")?;
        let zi_mod = py.import("zoneinfo")?;
        let types_mod = py.import("voyager_ogm.types")?;
        Ok(EntityConstructors {
            node_from_trusted: node_cls.getattr("_from_trusted")?.unbind(),
            rel_from_trusted: rel_cls.getattr("_from_trusted")?.unbind(),
            path_from_trusted: path_cls.getattr("_from_trusted")?.unbind(),
            datetime_cls: dt_mod.getattr("datetime")?.unbind(),
            date_cls: dt_mod.getattr("date")?.unbind(),
            time_cls: dt_mod.getattr("time")?.unbind(),
            timedelta_cls: dt_mod.getattr("timedelta")?.unbind(),
            timezone_cls: timezone_cls.unbind(),
            utc_obj,
            uuid_cls: uuid_mod.getattr("UUID")?.unbind(),
            decimal_cls: dec_mod.getattr("Decimal")?.unbind(),
            zoneinfo_cls: zi_mod.getattr("ZoneInfo")?.unbind(),
            zoneinfo_cache: Mutex::new(HashMap::new()),
            duration_cls: types_mod.getattr("Duration")?.unbind(),
            point_cls: types_mod.getattr("Point")?.unbind(),
            parse_pg_interval_fn: types_mod.getattr("parse_pg_interval_text")?.unbind(),
        })
    })
}

/// Returns a cached `ZoneInfo` for `zone_id`, constructing it on first use.
///
/// Hydrating a zoned datetime otherwise builds a fresh `ZoneInfo` per cell.
/// The cache is bounded so a long-lived process cannot accumulate unbounded
/// zone objects; when it fills it is cleared and rebuilt.
fn zoneinfo_cached<'py>(
    py: Python<'py>,
    ctors: &EntityConstructors,
    zone_id: &str,
) -> PyResult<Bound<'py, PyAny>> {
    {
        let cache = ctors
            .zoneinfo_cache
            .lock()
            .map_err(|_| PyValueError::new_err("ZoneInfo cache lock is poisoned"))?;
        if let Some(zi) = cache.get(zone_id) {
            return Ok(zi.bind(py).clone());
        }
    }
    let zi = ctors.zoneinfo_cls.bind(py).call1((zone_id,))?;
    if let Ok(mut cache) = ctors.zoneinfo_cache.lock() {
        if cache.len() >= ZONEINFO_CACHE_CAPACITY {
            cache.clear();
        }
        cache.insert(zone_id.to_string(), zi.clone().unbind());
    }
    Ok(zi)
}

fn is_entity_node_obj(map: &serde_json::Map<String, serde_json::Value>) -> bool {
    map.get("kind").and_then(|v| v.as_str()) == Some("node")
        && map.contains_key("id")
        && map.get("labels").and_then(|v| v.as_array()).is_some()
        && map.get("properties").and_then(|v| v.as_object()).is_some()
        && map.keys().all(|k| {
            matches!(
                k.as_str(),
                "kind" | "id" | "element_id" | "labels" | "properties"
            )
        })
}

fn is_entity_rel_obj(map: &serde_json::Map<String, serde_json::Value>) -> bool {
    map.get("kind").and_then(|v| v.as_str()) == Some("relationship")
        && map.contains_key("id")
        && map.contains_key("start_id")
        && map.contains_key("end_id")
        && map.get("type").and_then(|v| v.as_str()).is_some()
        && map.get("properties").and_then(|v| v.as_object()).is_some()
        && map.keys().all(|k| {
            matches!(
                k.as_str(),
                "kind"
                    | "id"
                    | "element_id"
                    | "type"
                    | "start_id"
                    | "end_id"
                    | "start_element_id"
                    | "end_element_id"
                    | "properties"
            )
        })
}

fn is_entity_path_obj(map: &serde_json::Map<String, serde_json::Value>) -> bool {
    map.get("kind").and_then(|v| v.as_str()) == Some("path")
        && map.get("nodes").and_then(|v| v.as_array()).is_some()
        && map
            .get("relationships")
            .and_then(|v| v.as_array())
            .is_some()
        && map
            .keys()
            .all(|k| matches!(k.as_str(), "kind" | "nodes" | "relationships" | "sequence"))
}

/// Sentinel strings used to carry JSON numbers `serde_json` cannot represent
/// through parsing.
///
/// PostgreSQL `jsonb` cannot contain a NUL character, so these values can never
/// collide with a real stored string. PostgreSQL/AGE emit `NaN`, `Infinity` and
/// `-Infinity` as bare tokens (and Python's `json` accepts them), which
/// `serde_json` rejects; out-of-range negative/positive integers are preserved
/// as arbitrary-precision Python integers the way psycopg's `json.loads` does.
const JSON_NAN_SENTINEL: &str = "\u{0}voyager:json:nan";
const JSON_INF_SENTINEL: &str = "\u{0}voyager:json:infinity";
const JSON_NEG_INF_SENTINEL: &str = "\u{0}voyager:json:-infinity";
const JSON_INT_SENTINEL_PREFIX: &str = "\u{0}voyager:json:int:";

fn parse_json_result_text(text: &str) -> std::result::Result<serde_json::Value, String> {
    let normalized = normalize_json_numbers(text);
    serde_json::from_str(&normalized).map_err(|error| error.to_string())
}

/// Rewrites bare `NaN`/`Infinity` tokens, out-of-range floats and integers that
/// do not fit `i64`/`u64` into quoted sentinel strings that `serde_json` can
/// parse without losing precision.
fn normalize_json_numbers(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len() + 16);
    let mut i = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    while i < bytes.len() {
        let byte = bytes[i];
        if in_string {
            out.push(byte);
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            i += 1;
            continue;
        }
        if byte == b'"' {
            in_string = true;
            out.push(byte);
            i += 1;
            continue;
        }
        if let Some((consumed, replacement)) = json_non_finite_token(&text[i..]) {
            out.extend_from_slice(replacement.as_bytes());
            i += consumed;
            continue;
        }
        if byte == b'-' || byte.is_ascii_digit() {
            let start = i;
            let mut end = i;
            if bytes[end] == b'-' {
                end += 1;
            }
            let integer_start = end;
            while end < bytes.len() && bytes[end].is_ascii_digit() {
                end += 1;
            }
            if end == integer_start {
                out.push(byte);
                i += 1;
                continue;
            }
            let mut is_float = false;
            if end < bytes.len() && bytes[end] == b'.' {
                is_float = true;
                end += 1;
                while end < bytes.len() && bytes[end].is_ascii_digit() {
                    end += 1;
                }
            }
            if end < bytes.len() && (bytes[end] == b'e' || bytes[end] == b'E') {
                let mut exponent = end + 1;
                if exponent < bytes.len() && (bytes[exponent] == b'+' || bytes[exponent] == b'-') {
                    exponent += 1;
                }
                let exponent_start = exponent;
                while exponent < bytes.len() && bytes[exponent].is_ascii_digit() {
                    exponent += 1;
                }
                if exponent > exponent_start {
                    is_float = true;
                    end = exponent;
                }
            }
            let token = &text[start..end];
            if is_float {
                match token.parse::<f64>() {
                    Ok(value) if value.is_infinite() => {
                        out.extend_from_slice(if value.is_sign_positive() {
                            br#""\u0000voyager:json:infinity""#
                        } else {
                            br#""\u0000voyager:json:-infinity""#
                        });
                    }
                    _ => out.extend_from_slice(token.as_bytes()),
                }
            } else if token.parse::<i64>().is_ok() || token.parse::<u64>().is_ok() {
                out.extend_from_slice(token.as_bytes());
            } else {
                out.extend_from_slice(br#""\u0000voyager:json:int:"#);
                out.extend_from_slice(token.as_bytes());
                out.push(b'"');
            }
            i = end;
            continue;
        }
        out.push(byte);
        i += 1;
    }
    // All inserted bytes are ASCII; copying the input bytes preserves valid UTF-8.
    String::from_utf8(out).unwrap_or_else(|_| text.to_string())
}

/// Returns the number of bytes consumed and the replacement JSON text for a
/// bare non-finite token at the start of `text`.
fn json_non_finite_token(text: &str) -> Option<(usize, &'static str)> {
    for (token, replacement) in [
        ("-Infinity", r#""\u0000voyager:json:-infinity""#),
        ("+Infinity", r#""\u0000voyager:json:infinity""#),
        ("Infinity", r#""\u0000voyager:json:infinity""#),
        ("NaN", r#""\u0000voyager:json:nan""#),
    ] {
        if text.starts_with(token) {
            let next = text.as_bytes().get(token.len()).copied();
            if next.is_none_or(|b| !(b.is_ascii_alphanumeric() || b == b'_')) {
                return Some((token.len(), replacement));
            }
        }
    }
    None
}

/// Converts a canonical JSON value to Python objects, optionally hydrating entity objects
/// into `GraphNode`, `GraphRelationship`, and `GraphPath` instances.
fn json_value_to_py<'py>(
    py: Python<'py>,
    value: &serde_json::Value,
    hydrate_entities: bool,
) -> PyResult<Bound<'py, PyAny>> {
    match value {
        serde_json::Value::Null => Ok(py.None().into_bound(py)),
        serde_json::Value::Bool(value) => Ok(value.into_pyobject(py)?.to_owned().into_any()),
        serde_json::Value::Number(value) => {
            if let Some(integer) = value.as_i64() {
                Ok(integer.into_pyobject(py)?.to_owned().into_any())
            } else if let Some(unsigned) = value.as_u64() {
                Ok(unsigned.into_pyobject(py)?.to_owned().into_any())
            } else {
                Ok(value
                    .as_f64()
                    .unwrap_or(f64::NAN)
                    .into_pyobject(py)?
                    .into_any())
            }
        }
        serde_json::Value::String(value) if value.starts_with(JSON_INT_SENTINEL_PREFIX) => {
            let digits = &value[JSON_INT_SENTINEL_PREFIX.len()..];
            let builtins = py.import("builtins")?;
            Ok(builtins.getattr("int")?.call1((digits,))?)
        }
        serde_json::Value::String(value) if value == JSON_NAN_SENTINEL => {
            Ok(f64::NAN.into_pyobject(py)?.into_any())
        }
        serde_json::Value::String(value) if value == JSON_INF_SENTINEL => {
            Ok(f64::INFINITY.into_pyobject(py)?.into_any())
        }
        serde_json::Value::String(value) if value == JSON_NEG_INF_SENTINEL => {
            Ok(f64::NEG_INFINITY.into_pyobject(py)?.into_any())
        }
        serde_json::Value::String(value) => Ok(value.into_pyobject(py)?.into_any()),
        serde_json::Value::Array(values) => {
            let list = PyList::empty(py);
            for value in values {
                list.append(json_value_to_py(py, value, hydrate_entities)?)?;
            }
            Ok(list.into_any())
        }
        serde_json::Value::Object(values) => {
            if hydrate_entities && is_entity_node_obj(values) {
                let ctors = entity_constructors(py)?;
                let id_py = values
                    .get("id")
                    .map(|v| json_value_to_py(py, v, true))
                    .transpose()?
                    .unwrap_or_else(|| py.None().into_bound(py));
                let eid_py = values
                    .get("element_id")
                    .map(|v| json_value_to_py(py, v, true))
                    .transpose()?
                    .unwrap_or_else(|| py.None().into_bound(py));
                let labels_arr = values["labels"].as_array().expect("checked");
                let mut label_objs = Vec::with_capacity(labels_arr.len());
                for lbl in labels_arr {
                    if let Some(s) = lbl.as_str() {
                        label_objs.push(s.into_pyobject(py)?.into_any());
                    } else {
                        label_objs.push(json_value_to_py(py, lbl, true)?);
                    }
                }
                let labels_fs = PyFrozenSet::new(py, &label_objs)?;
                let props_dict = PyDict::new(py);
                for (pk, pv) in values["properties"].as_object().expect("checked") {
                    props_dict.set_item(pk, json_value_to_py(py, pv, true)?)?;
                }
                return ctors
                    .node_from_trusted
                    .bind(py)
                    .call1((id_py, eid_py, labels_fs, props_dict));
            }

            if hydrate_entities && is_entity_rel_obj(values) {
                let ctors = entity_constructors(py)?;
                let id_py = json_value_to_py(py, &values["id"], true)?;
                let eid_py = values
                    .get("element_id")
                    .map(|v| json_value_to_py(py, v, true))
                    .transpose()?
                    .unwrap_or_else(|| py.None().into_bound(py));
                let rel_type = values["type"].as_str().expect("checked");
                let start_id_py = json_value_to_py(py, &values["start_id"], true)?;
                let end_id_py = json_value_to_py(py, &values["end_id"], true)?;
                let start_eid_py = values
                    .get("start_element_id")
                    .map(|v| json_value_to_py(py, v, true))
                    .transpose()?
                    .unwrap_or_else(|| py.None().into_bound(py));
                let end_eid_py = values
                    .get("end_element_id")
                    .map(|v| json_value_to_py(py, v, true))
                    .transpose()?
                    .unwrap_or_else(|| py.None().into_bound(py));
                let props_dict = PyDict::new(py);
                for (pk, pv) in values["properties"].as_object().expect("checked") {
                    props_dict.set_item(pk, json_value_to_py(py, pv, true)?)?;
                }
                return ctors.rel_from_trusted.bind(py).call1((
                    id_py,
                    eid_py,
                    rel_type,
                    start_id_py,
                    end_id_py,
                    props_dict,
                    start_eid_py,
                    end_eid_py,
                ));
            }

            if hydrate_entities && is_entity_path_obj(values) {
                let ctors = entity_constructors(py)?;
                let nodes_arr = values["nodes"].as_array().expect("checked");
                let rels_arr = values["relationships"].as_array().expect("checked");
                let mut node_objs = Vec::with_capacity(nodes_arr.len());
                for n in nodes_arr {
                    node_objs.push(json_value_to_py(py, n, true)?);
                }
                let mut rel_objs = Vec::with_capacity(rels_arr.len());
                for r in rels_arr {
                    rel_objs.push(json_value_to_py(py, r, true)?);
                }
                let nodes_tuple = PyTuple::new(py, &node_objs)?;
                let rels_tuple = PyTuple::new(py, &rel_objs)?;
                return ctors
                    .path_from_trusted
                    .bind(py)
                    .call1((nodes_tuple, rels_tuple));
            }

            let dict = PyDict::new(py);
            for (key, value) in values {
                dict.set_item(key, json_value_to_py(py, value, hydrate_entities)?)?;
            }
            Ok(dict.into_any())
        }
    }
}

/// Converts a JSON fallback value whose leaf scalars need a typed Python representation.
///
/// Mixed-depth PostgreSQL array columns fall back to JSON text but their elements
/// keep the column's declared type, so `numeric` leaves become `Decimal` and
/// `uuid` leaves become `uuid.UUID` exactly like the typed scalar decoders.
fn json_value_to_py_typed<'py>(
    py: Python<'py>,
    value: &serde_json::Value,
    kind: &str,
) -> PyResult<Bound<'py, PyAny>> {
    match value {
        serde_json::Value::Null => Ok(py.None().into_bound(py)),
        serde_json::Value::Array(values) => {
            let list = PyList::empty(py);
            for value in values {
                list.append(json_value_to_py_typed(py, value, kind)?)?;
            }
            Ok(list.into_any())
        }
        serde_json::Value::String(text) => parse_voyager_typed_utf8(py, kind, text),
        serde_json::Value::Number(number) => {
            parse_voyager_typed_utf8(py, kind, &number.to_string())
        }
        other => json_value_to_py(py, other, false),
    }
}

#[derive(Clone, Copy)]
enum PyTemporalTz<'a> {
    Offset(i64),
    Zone(&'a str),
    LocalZone(&'a str),
}

fn make_py_offset_tz<'py>(
    py: Python<'py>,
    offset_secs: i64,
) -> PyResult<Option<Bound<'py, PyAny>>> {
    let ctors = entity_constructors(py)?;
    if offset_secs == 0 {
        return Ok(Some(ctors.utc_obj.bind(py).clone()));
    }
    if offset_secs.abs() >= 86_400 {
        return Ok(None);
    }
    let td = ctors.timedelta_cls.bind(py).call1((0, offset_secs))?;
    Ok(Some(ctors.timezone_cls.bind(py).call1((td,))?))
}

fn days_to_py_date<'py>(
    py: Python<'py>,
    days: i64,
    fallback_iso: Option<&str>,
) -> PyResult<Bound<'py, PyAny>> {
    let (year, month, day) = voyager_drivers::bolt::packstream::days_to_ymd(days);
    if !(1..=9999).contains(&year) {
        let iso = fallback_iso
            .map(ToString::to_string)
            .unwrap_or_else(|| format!("{year:04}-{month:02}-{day:02}"));
        return Ok(iso.into_pyobject(py)?.into_any());
    }
    let ctors = entity_constructors(py)?;
    ctors.date_cls.bind(py).call1((year, month, day))
}

fn nanos_to_py_time<'py, S: AsRef<str>>(
    py: Python<'py>,
    nanos_of_day: i64,
    tz_offset_seconds: Option<i64>,
    fallback_iso: impl FnOnce() -> S,
) -> PyResult<Bound<'py, PyAny>> {
    if !(0..86_400_000_000_000).contains(&nanos_of_day) || nanos_of_day % 1_000 != 0 {
        return Ok(fallback_iso().as_ref().into_pyobject(py)?.into_any());
    }
    let (h, m, s, ns) = voyager_drivers::bolt::packstream::nanos_to_hmsn(nanos_of_day);
    let us = ns / 1_000;
    let ctors = entity_constructors(py)?;
    match tz_offset_seconds {
        None => ctors.time_cls.bind(py).call1((h, m, s, us)),
        Some(offset) => match make_py_offset_tz(py, offset)? {
            Some(tz) => ctors.time_cls.bind(py).call1((h, m, s, us, tz)),
            None => Ok(fallback_iso().as_ref().into_pyobject(py)?.into_any()),
        },
    }
}

fn epoch_nanos_to_py_datetime<'py, S: AsRef<str>>(
    py: Python<'py>,
    epoch_seconds: i64,
    nanoseconds: i64,
    tz: Option<PyTemporalTz<'_>>,
    fallback_iso: impl FnOnce() -> S,
) -> PyResult<Bound<'py, PyAny>> {
    if !(0..1_000_000_000).contains(&nanoseconds) || nanoseconds % 1_000 != 0 {
        return Ok(fallback_iso().as_ref().into_pyobject(py)?.into_any());
    }
    let us = nanoseconds / 1_000;
    let ctors = entity_constructors(py)?;
    let split_secs = |secs: i64| -> (i32, u32, u32, i64, i64, i64) {
        let days = secs.div_euclid(86_400);
        let rem = secs.rem_euclid(86_400);
        let (y, m, d) = voyager_drivers::bolt::packstream::days_to_ymd(days);
        (y, m, d, rem / 3_600, (rem % 3_600) / 60, rem % 60)
    };

    match tz {
        None => {
            let (y, m, d, hh, mm, ss) = split_secs(epoch_seconds);
            if !(1..=9999).contains(&y) {
                return Ok(fallback_iso().as_ref().into_pyobject(py)?.into_any());
            }
            ctors.datetime_cls.bind(py).call1((y, m, d, hh, mm, ss, us))
        }
        Some(PyTemporalTz::Offset(offset)) => {
            let Some(local_secs) = epoch_seconds.checked_add(offset) else {
                return Ok(fallback_iso().as_ref().into_pyobject(py)?.into_any());
            };
            let (y, m, d, hh, mm, ss) = split_secs(local_secs);
            if !(1..=9999).contains(&y) {
                return Ok(fallback_iso().as_ref().into_pyobject(py)?.into_any());
            }
            let Some(tz_obj) = make_py_offset_tz(py, offset)? else {
                return Ok(fallback_iso().as_ref().into_pyobject(py)?.into_any());
            };
            ctors
                .datetime_cls
                .bind(py)
                .call1((y, m, d, hh, mm, ss, us, tz_obj))
        }
        Some(PyTemporalTz::Zone(zone_id)) => {
            let (y, m, d, hh, mm, ss) = split_secs(epoch_seconds);
            if !(1..=9999).contains(&y) {
                return Ok(fallback_iso().as_ref().into_pyobject(py)?.into_any());
            }
            let Ok(zi) = zoneinfo_cached(py, ctors, zone_id) else {
                return Ok(fallback_iso().as_ref().into_pyobject(py)?.into_any());
            };
            let utc = ctors.utc_obj.bind(py);
            let dt_utc = ctors
                .datetime_cls
                .bind(py)
                .call1((y, m, d, hh, mm, ss, us, utc))?;
            match dt_utc.call_method1("astimezone", (zi,)) {
                Ok(dt_zoned) => Ok(dt_zoned),
                Err(_) => Ok(fallback_iso().as_ref().into_pyobject(py)?.into_any()),
            }
        }
        Some(PyTemporalTz::LocalZone(zone_id)) => {
            let (y, m, d, hh, mm, ss) = split_secs(epoch_seconds);
            if !(1..=9999).contains(&y) {
                return Ok(fallback_iso().as_ref().into_pyobject(py)?.into_any());
            }
            let Ok(zi) = zoneinfo_cached(py, ctors, zone_id) else {
                return Ok(fallback_iso().as_ref().into_pyobject(py)?.into_any());
            };
            ctors
                .datetime_cls
                .bind(py)
                .call1((y, m, d, hh, mm, ss, us, zi))
        }
    }
}

fn decimal128_to_py<'py>(py: Python<'py>, raw: i128, scale: i8) -> PyResult<Bound<'py, PyAny>> {
    let ctors = entity_constructors(py)?;
    let s = if scale == 0 {
        raw.to_string()
    } else if scale < 0 {
        if raw == 0 {
            "0".to_string()
        } else {
            let zeros = "0".repeat((-scale) as usize);
            format!("{raw}{zeros}")
        }
    } else {
        let neg = raw < 0;
        let digits = raw.unsigned_abs().to_string();
        let scale_u = scale as usize;
        let sign = if neg { "-" } else { "" };
        if digits.len() <= scale_u {
            let pad = "0".repeat(scale_u - digits.len());
            format!("{sign}0.{pad}{digits}")
        } else {
            let split = digits.len() - scale_u;
            format!("{sign}{}.{}", &digits[..split], &digits[split..])
        }
    };
    ctors.decimal_cls.bind(py).call1((s,))
}

fn parse_hms_str(raw: &str) -> Option<(u32, u32, u32, i64)> {
    let (hms, frac_opt) = match raw.split_once('.') {
        Some((l, r)) => (l, Some(r)),
        None => (raw, None),
    };
    let mut parts = hms.split(':');
    let h: u32 = parts.next()?.parse().ok()?;
    let m: u32 = parts.next()?.parse().ok()?;
    let s: u32 = parts.next()?.parse().ok()?;
    if parts.next().is_some() || h > 23 || m > 59 || s > 59 {
        return None;
    }
    let nanos = if let Some(frac) = frac_opt {
        if frac.is_empty() || frac.len() > 9 || !frac.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        let mut padded = [b'0'; 9];
        padded[..frac.len()].copy_from_slice(frac.as_bytes());
        std::str::from_utf8(&padded).ok()?.parse::<i64>().ok()?
    } else {
        0
    };
    Some((h, m, s, nanos))
}

fn parse_tz_suffix_seconds(raw: &str) -> Option<i64> {
    if raw.eq_ignore_ascii_case("z") {
        return Some(0);
    }
    let (sign, rest) = if let Some(r) = raw.strip_prefix('+') {
        (1i64, r)
    } else {
        let r = raw.strip_prefix('-')?;
        (-1i64, r)
    };
    let parts: Vec<&str> = rest.split(':').collect();
    if parts.is_empty() || parts.len() > 3 {
        return None;
    }
    let oh: i64 = parts[0].parse().ok()?;
    let om: i64 = if parts.len() >= 2 {
        parts[1].parse().ok()?
    } else {
        0
    };
    let os: i64 = if parts.len() == 3 {
        parts[2].parse().ok()?
    } else {
        0
    };
    if !(0..=23).contains(&oh) || !(0..=59).contains(&om) || !(0..=59).contains(&os) {
        return None;
    }
    Some(sign * (oh * 3_600 + om * 60 + os))
}

fn parse_voyager_typed_utf8<'py>(
    py: Python<'py>,
    kind: &str,
    s: &str,
) -> PyResult<Bound<'py, PyAny>> {
    let ctors = entity_constructors(py)?;
    let trimmed = s.trim();
    match kind {
        META_TYPE_DECIMAL => ctors.decimal_cls.bind(py).call1((trimmed,)),
        META_TYPE_UUID => ctors.uuid_cls.bind(py).call1((trimmed,)),
        META_TYPE_DURATION => {
            if trimmed.starts_with('P') || trimmed.starts_with("-P") {
                match ctors
                    .duration_cls
                    .bind(py)
                    .call_method1("from_iso", (trimmed,))
                {
                    Ok(v) => Ok(v),
                    Err(_) => Ok(s.into_pyobject(py)?.into_any()),
                }
            } else {
                match ctors.parse_pg_interval_fn.bind(py).call1((trimmed,)) {
                    Ok(v) => Ok(v),
                    Err(_) => Ok(s.into_pyobject(py)?.into_any()),
                }
            }
        }
        META_TYPE_POINT => {
            let Ok(parsed) = serde_json::from_str::<serde_json::Value>(trimmed) else {
                return Ok(s.into_pyobject(py)?.into_any());
            };
            let Some(map) = parsed.as_object() else {
                return Ok(s.into_pyobject(py)?.into_any());
            };
            let coordinate = |key: &str| map.get(key).and_then(|value| value.as_f64());
            let (Some(x), Some(y)) = (coordinate("x"), coordinate("y")) else {
                return Ok(s.into_pyobject(py)?.into_any());
            };
            let kwargs = PyDict::new(py);
            if let Some(srid) = map.get("srid").and_then(|value| value.as_i64()) {
                kwargs.set_item("srid", srid)?;
            }
            kwargs.set_item("x", x)?;
            kwargs.set_item("y", y)?;
            if let Some(z) = coordinate("z") {
                kwargs.set_item("z", z)?;
            }
            match ctors.point_cls.bind(py).call((), Some(&kwargs)) {
                Ok(point) => Ok(point),
                Err(_) => Ok(s.into_pyobject(py)?.into_any()),
            }
        }
        META_TYPE_DATE => {
            let mut parts = trimmed.split('-');
            if let (Some(ys), Some(ms), Some(ds), None) =
                (parts.next(), parts.next(), parts.next(), parts.next())
                && let (Ok(y), Ok(m), Ok(d)) =
                    (ys.parse::<i32>(), ms.parse::<u32>(), ds.parse::<u32>())
                && (1..=9999).contains(&y)
                && (1..=12).contains(&m)
                && (1..=31).contains(&d)
            {
                return ctors.date_cls.bind(py).call1((y, m, d));
            }
            Ok(s.into_pyobject(py)?.into_any())
        }
        META_TYPE_TIME | META_TYPE_TIME_TZ => {
            let bytes = trimmed.as_bytes();
            let tz_start = if bytes.ends_with(b"Z") || bytes.ends_with(b"z") {
                Some(trimmed.len() - 1)
            } else {
                bytes.iter().rposition(|&b| b == b'+' || b == b'-')
            };
            let (hms_part, tz_offset) = match tz_start {
                Some(idx) => (&trimmed[..idx], parse_tz_suffix_seconds(&trimmed[idx..])),
                None => (trimmed, None),
            };
            if (tz_start.is_some() || kind == META_TYPE_TIME_TZ) && tz_offset.is_none() {
                return Ok(s.into_pyobject(py)?.into_any());
            }
            if let Some((h, m, sec, nanos)) = parse_hms_str(hms_part.trim()) {
                let total_ns =
                    ((h as i64) * 3_600 + (m as i64) * 60 + (sec as i64)) * 1_000_000_000 + nanos;
                return nanos_to_py_time(py, total_ns, tz_offset, || trimmed);
            }
            Ok(s.into_pyobject(py)?.into_any())
        }
        META_TYPE_LOCAL_DATETIME | META_TYPE_DATETIME_TZ => {
            let (main, bracket_zone) = if let Some(b_start) = trimmed.find('[')
                && trimmed.ends_with(']')
            {
                (
                    &trimmed[..b_start],
                    Some(&trimmed[b_start + 1..trimmed.len() - 1]),
                )
            } else {
                (trimmed, None)
            };
            let Some((date_part, time_tz_part)) =
                main.split_once('T').or_else(|| main.split_once(' '))
            else {
                return Ok(s.into_pyobject(py)?.into_any());
            };
            let mut dparts = date_part.split('-');
            let (Some(ys), Some(ms), Some(ds), None) =
                (dparts.next(), dparts.next(), dparts.next(), dparts.next())
            else {
                return Ok(s.into_pyobject(py)?.into_any());
            };
            let (Ok(y), Ok(mo), Ok(da)) = (ys.parse::<i32>(), ms.parse::<u32>(), ds.parse::<u32>())
            else {
                return Ok(s.into_pyobject(py)?.into_any());
            };
            if !(1..=9999).contains(&y) || !(1..=12).contains(&mo) || !(1..=31).contains(&da) {
                return Ok(s.into_pyobject(py)?.into_any());
            }
            let bytes = time_tz_part.as_bytes();
            let tz_idx = if bytes.ends_with(b"Z") || bytes.ends_with(b"z") {
                Some(time_tz_part.len() - 1)
            } else {
                bytes.iter().rposition(|&b| b == b'+' || b == b'-')
            };
            let (hms_part, offset_opt) = match tz_idx {
                Some(idx) => (
                    &time_tz_part[..idx],
                    parse_tz_suffix_seconds(&time_tz_part[idx..]),
                ),
                None => (time_tz_part, None),
            };
            if tz_idx.is_some() && offset_opt.is_none() {
                return Ok(s.into_pyobject(py)?.into_any());
            }
            if kind == META_TYPE_DATETIME_TZ && bracket_zone.is_none() && offset_opt.is_none() {
                return Ok(s.into_pyobject(py)?.into_any());
            }
            let Some((h, mi, sec, nanos)) = parse_hms_str(hms_part.trim()) else {
                return Ok(s.into_pyobject(py)?.into_any());
            };
            if nanos % 1_000 != 0 {
                return Ok(s.into_pyobject(py)?.into_any());
            }
            let us = nanos / 1_000;
            if let Some(zone_id) = bracket_zone {
                let Ok(zi) = zoneinfo_cached(py, ctors, zone_id) else {
                    return Ok(s.into_pyobject(py)?.into_any());
                };
                if let Some(offset) = offset_opt {
                    let days = voyager_drivers::bolt::packstream::ymd_to_days(y, mo, da);
                    let local_secs =
                        days * 86_400 + (h as i64) * 3_600 + (mi as i64) * 60 + (sec as i64);
                    let utc_secs = local_secs - offset;
                    return epoch_nanos_to_py_datetime(
                        py,
                        utc_secs,
                        nanos,
                        Some(PyTemporalTz::Zone(zone_id)),
                        || trimmed,
                    );
                }
                return ctors
                    .datetime_cls
                    .bind(py)
                    .call1((y, mo, da, h, mi, sec, us, zi));
            }
            if let Some(offset) = offset_opt {
                let Some(tz_obj) = make_py_offset_tz(py, offset)? else {
                    return Ok(s.into_pyobject(py)?.into_any());
                };
                return ctors
                    .datetime_cls
                    .bind(py)
                    .call1((y, mo, da, h, mi, sec, us, tz_obj));
            }
            ctors
                .datetime_cls
                .bind(py)
                .call1((y, mo, da, h, mi, sec, us))
        }
        _ => Ok(s.into_pyobject(py)?.into_any()),
    }
}

fn result_node_to_py<'py>(py: Python<'py>, node: &ResultNode) -> PyResult<Bound<'py, PyAny>> {
    let ctors = entity_constructors(py)?;
    let eid_py = match &node.element_id {
        Some(eid) => eid.as_ref().into_pyobject(py)?.into_any(),
        None => py.None().into_bound(py),
    };
    let mut labels: Vec<Bound<'py, PyAny>> = Vec::with_capacity(node.labels.len());
    for lbl in &node.labels {
        labels.push(lbl.as_ref().into_pyobject(py)?.into_any());
    }
    let labels_fs = PyFrozenSet::new(py, &labels)?;
    let props = PyDict::new(py);
    for (k, v) in &node.properties {
        props.set_item(k.as_ref(), result_value_to_py(py, v)?)?;
    }
    ctors
        .node_from_trusted
        .bind(py)
        .call1((node.id, eid_py, labels_fs, props))
}

fn result_rel_to_py<'py>(py: Python<'py>, rel: &ResultRelationship) -> PyResult<Bound<'py, PyAny>> {
    let ctors = entity_constructors(py)?;
    let eid_py = match &rel.element_id {
        Some(eid) => eid.as_ref().into_pyobject(py)?.into_any(),
        None => py.None().into_bound(py),
    };
    let start_eid_py = match &rel.start_element_id {
        Some(eid) => eid.as_ref().into_pyobject(py)?.into_any(),
        None => py.None().into_bound(py),
    };
    let end_eid_py = match &rel.end_element_id {
        Some(eid) => eid.as_ref().into_pyobject(py)?.into_any(),
        None => py.None().into_bound(py),
    };
    let props = PyDict::new(py);
    for (k, v) in &rel.properties {
        props.set_item(k.as_ref(), result_value_to_py(py, v)?)?;
    }
    ctors.rel_from_trusted.bind(py).call1((
        rel.id,
        eid_py,
        rel.rel_type.as_ref(),
        rel.start_node_id,
        rel.end_node_id,
        props,
        start_eid_py,
        end_eid_py,
    ))
}

fn result_path_to_py<'py>(py: Python<'py>, path: &ResultPath) -> PyResult<Bound<'py, PyAny>> {
    let ctors = entity_constructors(py)?;
    let mut node_objs = Vec::with_capacity(path.nodes.len());
    for node in &path.nodes {
        node_objs.push(result_node_to_py(py, node)?);
    }
    let mut rel_objs = Vec::with_capacity(path.relationships.len());
    for rel in &path.relationships {
        rel_objs.push(result_rel_to_py(py, rel)?);
    }
    let nodes_tuple = PyTuple::new(py, &node_objs)?;
    let rels_tuple = PyTuple::new(py, &rel_objs)?;
    ctors
        .path_from_trusted
        .bind(py)
        .call1((nodes_tuple, rels_tuple))
}

fn result_point_to_py<'py>(
    py: Python<'py>,
    srid: i64,
    x: f64,
    y: f64,
    z_opt: Option<f64>,
) -> PyResult<Bound<'py, PyAny>> {
    let ctors = entity_constructors(py)?;
    let kwargs = PyDict::new(py);
    kwargs.set_item("srid", srid)?;
    kwargs.set_item("x", x)?;
    kwargs.set_item("y", y)?;
    if let Some(z) = z_opt {
        kwargs.set_item("z", z)?;
    }
    match ctors.point_cls.bind(py).call((), Some(&kwargs)) {
        Ok(pt) => Ok(pt),
        Err(_) => Ok(kwargs.into_any()),
    }
}

/// Hydrates one [`ResultValue`] into its rich Python representation.
fn result_value_to_py<'py>(py: Python<'py>, value: &ResultValue) -> PyResult<Bound<'py, PyAny>> {
    match value {
        ResultValue::Null => Ok(py.None().into_bound(py)),
        ResultValue::Bool(b) => Ok(b.into_pyobject(py)?.to_owned().into_any()),
        ResultValue::Int(i) => Ok(i.into_pyobject(py)?.to_owned().into_any()),
        ResultValue::Float(f) => Ok(f.into_pyobject(py)?.into_any()),
        ResultValue::Bytes(bytes) => Ok(PyBytes::new(py, bytes).into_any()),
        ResultValue::Str(s) => Ok(s.as_ref().into_pyobject(py)?.into_any()),
        ResultValue::Decimal(s) => parse_voyager_typed_utf8(py, META_TYPE_DECIMAL, s),
        ResultValue::Uuid(bytes) => {
            let ctors = entity_constructors(py)?;
            ctors
                .uuid_cls
                .bind(py)
                .call1((py.None(), PyBytes::new(py, bytes)))
        }
        ResultValue::Json(raw) => {
            let parsed = parse_json_result_text(raw).map_err(PyValueError::new_err)?;
            json_value_to_py(py, &parsed, false)
        }
        ResultValue::Date(days) => days_to_py_date(py, *days, None),
        ResultValue::LocalTime(nanos) => nanos_to_py_time(py, *nanos, None, || {
            ResultValue::format_local_time_nanos(*nanos)
        }),
        ResultValue::Time {
            nanoseconds,
            tz_offset_seconds,
        } => nanos_to_py_time(py, *nanoseconds, Some(*tz_offset_seconds as i64), || {
            ResultValue::format_time_offset(*nanoseconds, *tz_offset_seconds)
        }),
        ResultValue::LocalDateTime {
            seconds,
            nanoseconds,
        } => epoch_nanos_to_py_datetime(py, *seconds, *nanoseconds as i64, None, || {
            ResultValue::format_local_datetime(*seconds, *nanoseconds)
        }),
        ResultValue::DateTime(dt) => {
            let tz = match &dt.tz {
                ResultTimezone::Offset(offset) => PyTemporalTz::Offset(*offset as i64),
                ResultTimezone::Zone { id, is_local } => {
                    if *is_local {
                        PyTemporalTz::LocalZone(id)
                    } else {
                        PyTemporalTz::Zone(id)
                    }
                }
            };
            epoch_nanos_to_py_datetime(py, dt.seconds, dt.nanoseconds, Some(tz), || {
                dt.to_iso_string()
            })
        }
        ResultValue::Duration(dur) => {
            let ctors = entity_constructors(py)?;
            ctors
                .duration_cls
                .bind(py)
                .call1((dur.months, dur.days, dur.seconds, dur.nanoseconds))
        }
        ResultValue::Point2D(pt) => result_point_to_py(py, pt.srid, pt.x, pt.y, None),
        ResultValue::Point3D(pt) => result_point_to_py(py, pt.srid, pt.x, pt.y, Some(pt.z)),
        ResultValue::List(items) => {
            let list = PyList::empty(py);
            for item in items.iter() {
                list.append(result_value_to_py(py, item)?)?;
            }
            Ok(list.into_any())
        }
        ResultValue::Map(map) => {
            let dict = PyDict::new(py);
            for (k, v) in map.iter() {
                dict.set_item(k.as_ref(), result_value_to_py(py, v)?)?;
            }
            Ok(dict.into_any())
        }
        ResultValue::Node(node) => result_node_to_py(py, node),
        ResultValue::Relationship(rel) => result_rel_to_py(py, rel),
        ResultValue::Path(path) => result_path_to_py(py, path),
        ResultValue::Structure(structure) => {
            let dict = PyDict::new(py);
            dict.set_item("tag", structure.tag)?;
            let list = PyList::empty(py);
            for field in &structure.fields {
                list.append(result_value_to_py(py, field)?)?;
            }
            dict.set_item("fields", list)?;
            Ok(dict.into_any())
        }
        ResultValue::TypedText(text) => {
            parse_voyager_typed_utf8(py, text.meta_type, text.text.as_ref())
        }
    }
}

/// Converts a result value whose leaf strings need a typed Python representation.
fn result_value_to_py_typed<'py>(
    py: Python<'py>,
    value: &ResultValue,
    kind: &str,
) -> PyResult<Bound<'py, PyAny>> {
    match value {
        ResultValue::Null => Ok(py.None().into_bound(py)),
        ResultValue::Str(text) => parse_voyager_typed_utf8(py, kind, text.as_ref()),
        ResultValue::TypedText(text) => parse_voyager_typed_utf8(py, kind, text.text.as_ref()),
        ResultValue::List(items) => {
            let list = PyList::empty(py);
            for item in items.iter() {
                list.append(result_value_to_py_typed(py, item, kind)?)?;
            }
            Ok(list.into_any())
        }
        other => result_value_to_py(py, other),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Utf8ColMode {
    Plain,
    JsonEntities,
    JsonPure,
    JsonTyped(&'static str),
    Typed(&'static str),
}

fn field_meta<'a>(field: &'a arrow_schema::Field, key: &str) -> Option<&'a str> {
    field.metadata().get(key).map(String::as_str)
}

fn utf8_col_mode_for_field(field: &arrow_schema::Field) -> Utf8ColMode {
    let meta_type = field_meta(field, VOYAGER_META_TYPE);
    if field_meta(field, VOYAGER_META_ENCODING) == Some(META_ENCODING_JSON) {
        return match meta_type {
            Some(META_TYPE_JSON) => Utf8ColMode::JsonPure,
            Some(META_TYPE_DECIMAL) => Utf8ColMode::JsonTyped(META_TYPE_DECIMAL),
            Some(META_TYPE_UUID) => Utf8ColMode::JsonTyped(META_TYPE_UUID),
            _ => Utf8ColMode::JsonEntities,
        };
    }
    if field.metadata().contains_key(VOYAGER_META_KIND) {
        return Utf8ColMode::JsonEntities;
    }
    match meta_type {
        Some(META_TYPE_DATE) => Utf8ColMode::Typed(META_TYPE_DATE),
        Some(META_TYPE_TIME) => Utf8ColMode::Typed(META_TYPE_TIME),
        Some(META_TYPE_TIME_TZ) => Utf8ColMode::Typed(META_TYPE_TIME_TZ),
        Some(META_TYPE_LOCAL_DATETIME) => Utf8ColMode::Typed(META_TYPE_LOCAL_DATETIME),
        Some(META_TYPE_DATETIME_TZ) => Utf8ColMode::Typed(META_TYPE_DATETIME_TZ),
        Some(META_TYPE_DECIMAL) => Utf8ColMode::Typed(META_TYPE_DECIMAL),
        Some(META_TYPE_UUID) => Utf8ColMode::Typed(META_TYPE_UUID),
        Some(META_TYPE_DURATION) => Utf8ColMode::Typed(META_TYPE_DURATION),
        Some(META_TYPE_POINT) => Utf8ColMode::Typed(META_TYPE_POINT),
        _ => Utf8ColMode::Plain,
    }
}

/// Pre-resolved per-column Arrow-to-Python extractor.
///
/// Built once per column (or child array) so downcasting, field-metadata lookups,
/// and `Utf8ColMode` resolution happen O(columns) instead of O(rows * columns).
enum ColumnExtractor<'a> {
    Int64(&'a arrow_array::Int64Array),
    Int32(&'a arrow_array::Int32Array),
    Int16(&'a arrow_array::Int16Array),
    Int8(&'a arrow_array::Int8Array),
    UInt64(&'a arrow_array::UInt64Array),
    UInt32(&'a arrow_array::UInt32Array),
    UInt16(&'a arrow_array::UInt16Array),
    UInt8(&'a arrow_array::UInt8Array),
    Float64(&'a arrow_array::Float64Array),
    Float32(&'a arrow_array::Float32Array),
    Boolean(&'a arrow_array::BooleanArray),
    Binary(&'a arrow_array::BinaryArray),
    LargeBinary(&'a arrow_array::LargeBinaryArray),
    UuidBinary(&'a arrow_array::FixedSizeBinaryArray),
    FixedSizeBinary(&'a arrow_array::FixedSizeBinaryArray),
    Decimal128(&'a arrow_array::Decimal128Array, i8),
    Date32(&'a arrow_array::Date32Array),
    Date64(&'a arrow_array::Date64Array),
    Time32Second(&'a arrow_array::Time32SecondArray),
    Time32Millisecond(&'a arrow_array::Time32MillisecondArray),
    Time64Microsecond(&'a arrow_array::Time64MicrosecondArray),
    Time64Nanosecond(&'a arrow_array::Time64NanosecondArray),
    TimestampSecond(&'a arrow_array::TimestampSecondArray, Option<&'a str>),
    TimestampMillisecond(&'a arrow_array::TimestampMillisecondArray, Option<&'a str>),
    TimestampMicrosecond(&'a arrow_array::TimestampMicrosecondArray, Option<&'a str>),
    TimestampNanosecond(&'a arrow_array::TimestampNanosecondArray, Option<&'a str>),
    DurationSecond(&'a arrow_array::DurationSecondArray),
    DurationMillisecond(&'a arrow_array::DurationMillisecondArray),
    DurationMicrosecond(&'a arrow_array::DurationMicrosecondArray),
    DurationNanosecond(&'a arrow_array::DurationNanosecondArray),
    DurationStruct {
        months: &'a arrow_array::Int64Array,
        days: &'a arrow_array::Int64Array,
        seconds: &'a arrow_array::Int64Array,
        nanoseconds: &'a arrow_array::Int64Array,
    },
    PointStruct {
        srid: &'a arrow_array::Int64Array,
        x: &'a arrow_array::Float64Array,
        y: &'a arrow_array::Float64Array,
        z: &'a arrow_array::Float64Array,
    },
    GenericStruct {
        fields: Vec<(&'a str, &'a dyn arrow_array::Array, ColumnExtractor<'a>)>,
    },
    Utf8Plain(&'a arrow_array::StringArray),
    Utf8(&'a arrow_array::StringArray, Utf8ColMode),
    LargeUtf8Plain(&'a arrow_array::LargeStringArray),
    LargeUtf8(&'a arrow_array::LargeStringArray, Utf8ColMode),
    List(&'a arrow_array::ListArray, &'a arrow_schema::Field),
    LargeList(&'a arrow_array::LargeListArray, &'a arrow_schema::Field),
    FixedSizeList(&'a arrow_array::FixedSizeListArray, &'a arrow_schema::Field),
    Unsupported,
}

impl<'a> ColumnExtractor<'a> {
    fn new(field: &'a arrow_schema::Field, array: &'a dyn arrow_array::Array) -> Self {
        use arrow_array::*;
        use arrow_schema::{DataType, TimeUnit};

        match array.data_type() {
            DataType::Int64 => Self::Int64(array.as_any().downcast_ref::<Int64Array>().unwrap()),
            DataType::Int32 => Self::Int32(array.as_any().downcast_ref::<Int32Array>().unwrap()),
            DataType::Int16 => Self::Int16(array.as_any().downcast_ref::<Int16Array>().unwrap()),
            DataType::Int8 => Self::Int8(array.as_any().downcast_ref::<Int8Array>().unwrap()),
            DataType::UInt64 => Self::UInt64(array.as_any().downcast_ref::<UInt64Array>().unwrap()),
            DataType::UInt32 => Self::UInt32(array.as_any().downcast_ref::<UInt32Array>().unwrap()),
            DataType::UInt16 => Self::UInt16(array.as_any().downcast_ref::<UInt16Array>().unwrap()),
            DataType::UInt8 => Self::UInt8(array.as_any().downcast_ref::<UInt8Array>().unwrap()),
            DataType::Float64 => {
                Self::Float64(array.as_any().downcast_ref::<Float64Array>().unwrap())
            }
            DataType::Float32 => {
                Self::Float32(array.as_any().downcast_ref::<Float32Array>().unwrap())
            }
            DataType::Boolean => {
                Self::Boolean(array.as_any().downcast_ref::<BooleanArray>().unwrap())
            }
            DataType::Binary => Self::Binary(array.as_any().downcast_ref::<BinaryArray>().unwrap()),
            DataType::LargeBinary => {
                Self::LargeBinary(array.as_any().downcast_ref::<LargeBinaryArray>().unwrap())
            }
            DataType::FixedSizeBinary(16) => {
                let values = array
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .unwrap();
                if field_meta(field, ARROW_EXTENSION_NAME) == Some(ARROW_UUID_EXTENSION) {
                    Self::UuidBinary(values)
                } else {
                    Self::FixedSizeBinary(values)
                }
            }
            DataType::FixedSizeBinary(_) => Self::FixedSizeBinary(
                array
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .unwrap(),
            ),
            DataType::Decimal128(_, scale) => Self::Decimal128(
                array.as_any().downcast_ref::<Decimal128Array>().unwrap(),
                *scale,
            ),
            DataType::Date32 => Self::Date32(array.as_any().downcast_ref::<Date32Array>().unwrap()),
            DataType::Date64 => Self::Date64(array.as_any().downcast_ref::<Date64Array>().unwrap()),
            DataType::Time32(TimeUnit::Second) => {
                Self::Time32Second(array.as_any().downcast_ref::<Time32SecondArray>().unwrap())
            }
            DataType::Time32(TimeUnit::Millisecond) => Self::Time32Millisecond(
                array
                    .as_any()
                    .downcast_ref::<Time32MillisecondArray>()
                    .unwrap(),
            ),
            DataType::Time64(TimeUnit::Microsecond) => Self::Time64Microsecond(
                array
                    .as_any()
                    .downcast_ref::<Time64MicrosecondArray>()
                    .unwrap(),
            ),
            DataType::Time64(TimeUnit::Nanosecond) => Self::Time64Nanosecond(
                array
                    .as_any()
                    .downcast_ref::<Time64NanosecondArray>()
                    .unwrap(),
            ),
            DataType::Timestamp(TimeUnit::Second, tz_opt) => Self::TimestampSecond(
                array
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .unwrap(),
                tz_opt.as_deref(),
            ),
            DataType::Timestamp(TimeUnit::Millisecond, tz_opt) => Self::TimestampMillisecond(
                array
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap(),
                tz_opt.as_deref(),
            ),
            DataType::Timestamp(TimeUnit::Microsecond, tz_opt) => Self::TimestampMicrosecond(
                array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap(),
                tz_opt.as_deref(),
            ),
            DataType::Timestamp(TimeUnit::Nanosecond, tz_opt) => Self::TimestampNanosecond(
                array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap(),
                tz_opt.as_deref(),
            ),
            DataType::Duration(TimeUnit::Second) => Self::DurationSecond(
                array
                    .as_any()
                    .downcast_ref::<DurationSecondArray>()
                    .unwrap(),
            ),
            DataType::Duration(TimeUnit::Millisecond) => Self::DurationMillisecond(
                array
                    .as_any()
                    .downcast_ref::<DurationMillisecondArray>()
                    .unwrap(),
            ),
            DataType::Duration(TimeUnit::Microsecond) => Self::DurationMicrosecond(
                array
                    .as_any()
                    .downcast_ref::<DurationMicrosecondArray>()
                    .unwrap(),
            ),
            DataType::Duration(TimeUnit::Nanosecond) => Self::DurationNanosecond(
                array
                    .as_any()
                    .downcast_ref::<DurationNanosecondArray>()
                    .unwrap(),
            ),
            DataType::Struct(sub_fields) => {
                let values = array.as_any().downcast_ref::<StructArray>().unwrap();
                if field_meta(field, VOYAGER_META_TYPE) == Some(META_TYPE_DURATION)
                    && sub_fields.len() == 4
                {
                    return Self::DurationStruct {
                        months: values
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap(),
                        days: values
                            .column(1)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap(),
                        seconds: values
                            .column(2)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap(),
                        nanoseconds: values
                            .column(3)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap(),
                    };
                }
                if field_meta(field, VOYAGER_META_TYPE) == Some(META_TYPE_POINT)
                    && sub_fields.len() == 4
                {
                    return Self::PointStruct {
                        srid: values
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap(),
                        x: values
                            .column(1)
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .unwrap(),
                        y: values
                            .column(2)
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .unwrap(),
                        z: values
                            .column(3)
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .unwrap(),
                    };
                }
                let fields = sub_fields
                    .iter()
                    .enumerate()
                    .map(|(col_idx, sub_field)| {
                        let sub_arr = values.column(col_idx).as_ref();
                        (
                            sub_field.name().as_str(),
                            sub_arr,
                            Self::new(sub_field.as_ref(), sub_arr),
                        )
                    })
                    .collect();
                Self::GenericStruct { fields }
            }
            DataType::Utf8 => {
                let values = array.as_any().downcast_ref::<StringArray>().unwrap();
                match utf8_col_mode_for_field(field) {
                    Utf8ColMode::Plain => Self::Utf8Plain(values),
                    mode => Self::Utf8(values, mode),
                }
            }
            DataType::LargeUtf8 => {
                let values = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
                match utf8_col_mode_for_field(field) {
                    Utf8ColMode::Plain => Self::LargeUtf8Plain(values),
                    mode => Self::LargeUtf8(values, mode),
                }
            }
            DataType::List(item_field) => Self::List(
                array.as_any().downcast_ref::<ListArray>().unwrap(),
                item_field.as_ref(),
            ),
            DataType::LargeList(item_field) => Self::LargeList(
                array.as_any().downcast_ref::<LargeListArray>().unwrap(),
                item_field.as_ref(),
            ),
            DataType::FixedSizeList(item_field, _) => Self::FixedSizeList(
                array.as_any().downcast_ref::<FixedSizeListArray>().unwrap(),
                item_field.as_ref(),
            ),
            _ => Self::Unsupported,
        }
    }

    fn extract<'py>(
        &self,
        index: usize,
        py: Python<'py>,
        ctors: &EntityConstructors,
    ) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Int64(values) => Ok(values.value(index).into_pyobject(py)?.to_owned().into_any()),
            Self::Int32(values) => Ok(values.value(index).into_pyobject(py)?.to_owned().into_any()),
            Self::Int16(values) => Ok(values.value(index).into_pyobject(py)?.to_owned().into_any()),
            Self::Int8(values) => Ok(values.value(index).into_pyobject(py)?.to_owned().into_any()),
            Self::UInt64(values) => {
                Ok(values.value(index).into_pyobject(py)?.to_owned().into_any())
            }
            Self::UInt32(values) => {
                Ok(values.value(index).into_pyobject(py)?.to_owned().into_any())
            }
            Self::UInt16(values) => {
                Ok(values.value(index).into_pyobject(py)?.to_owned().into_any())
            }
            Self::UInt8(values) => Ok(values.value(index).into_pyobject(py)?.to_owned().into_any()),
            Self::Float64(values) => {
                Ok(values.value(index).into_pyobject(py)?.to_owned().into_any())
            }
            Self::Float32(values) => Ok((values.value(index) as f64)
                .into_pyobject(py)?
                .to_owned()
                .into_any()),
            Self::Boolean(values) => {
                Ok(values.value(index).into_pyobject(py)?.to_owned().into_any())
            }
            Self::Binary(values) => Ok(PyBytes::new(py, values.value(index)).into_any()),
            Self::LargeBinary(values) => Ok(PyBytes::new(py, values.value(index)).into_any()),
            Self::UuidBinary(values) => ctors
                .uuid_cls
                .bind(py)
                .call1((py.None(), PyBytes::new(py, values.value(index)))),
            Self::FixedSizeBinary(values) => Ok(PyBytes::new(py, values.value(index)).into_any()),
            Self::Decimal128(values, scale) => decimal128_to_py(py, values.value(index), *scale),
            Self::Date32(values) => days_to_py_date(py, values.value(index) as i64, None),
            Self::Date64(values) => {
                days_to_py_date(py, values.value(index).div_euclid(86_400_000), None)
            }
            Self::Time32Second(values) => {
                let s = values.value(index) as i64;
                nanos_to_py_time(py, s * 1_000_000_000, None, || {
                    format!("{:02}:{:02}:{:02}", s / 3_600, (s % 3_600) / 60, s % 60)
                })
            }
            Self::Time32Millisecond(values) => {
                let ns = (values.value(index) as i64) * 1_000_000;
                nanos_to_py_time(py, ns, None, || {
                    voyager_drivers::bolt::packstream::BoltLocalTime { nanoseconds: ns }
                        .to_iso_string()
                })
            }
            Self::Time64Microsecond(values) => {
                let ns = values.value(index) * 1_000;
                nanos_to_py_time(py, ns, None, || {
                    voyager_drivers::bolt::packstream::BoltLocalTime { nanoseconds: ns }
                        .to_iso_string()
                })
            }
            Self::Time64Nanosecond(values) => {
                let ns = values.value(index);
                nanos_to_py_time(py, ns, None, || {
                    voyager_drivers::bolt::packstream::BoltLocalTime { nanoseconds: ns }
                        .to_iso_string()
                })
            }
            Self::TimestampSecond(values, tz_opt) => {
                extract_timestamp_to_py(py, values.value(index), 0, *tz_opt)
            }
            Self::TimestampMillisecond(values, tz_opt) => {
                let v = values.value(index);
                extract_timestamp_to_py(
                    py,
                    v.div_euclid(1_000),
                    v.rem_euclid(1_000) * 1_000_000,
                    *tz_opt,
                )
            }
            Self::TimestampMicrosecond(values, tz_opt) => {
                let v = values.value(index);
                extract_timestamp_to_py(
                    py,
                    v.div_euclid(1_000_000),
                    v.rem_euclid(1_000_000) * 1_000,
                    *tz_opt,
                )
            }
            Self::TimestampNanosecond(values, tz_opt) => {
                let v = values.value(index);
                extract_timestamp_to_py(
                    py,
                    v.div_euclid(1_000_000_000),
                    v.rem_euclid(1_000_000_000),
                    *tz_opt,
                )
            }
            Self::DurationSecond(values) => {
                extract_duration_ns_to_py(py, ctors, values.value(index) as i128 * 1_000_000_000)
            }
            Self::DurationMillisecond(values) => {
                extract_duration_ns_to_py(py, ctors, values.value(index) as i128 * 1_000_000)
            }
            Self::DurationMicrosecond(values) => {
                extract_duration_ns_to_py(py, ctors, values.value(index) as i128 * 1_000)
            }
            Self::DurationNanosecond(values) => {
                extract_duration_ns_to_py(py, ctors, values.value(index) as i128)
            }
            Self::DurationStruct {
                months,
                days,
                seconds,
                nanoseconds,
            } => ctors.duration_cls.bind(py).call1((
                months.value(index),
                days.value(index),
                seconds.value(index),
                nanoseconds.value(index),
            )),
            Self::PointStruct { srid, x, y, z } => {
                use arrow_array::Array;
                let z_val = (!z.is_null(index)).then(|| z.value(index));
                result_point_to_py(py, srid.value(index), x.value(index), y.value(index), z_val)
            }
            Self::GenericStruct { fields } => {
                let dict = PyDict::new(py);
                for (name, sub_arr, sub_ext) in fields {
                    let val = if sub_arr.is_null(index) {
                        py.None().into_bound(py)
                    } else {
                        sub_ext.extract(index, py, ctors)?
                    };
                    dict.set_item(*name, val)?;
                }
                Ok(dict.into_any())
            }
            Self::Utf8Plain(values) => Ok(values.value(index).into_pyobject(py)?.into_any()),
            Self::Utf8(values, mode) => extract_utf8_with_mode(py, values.value(index), *mode),
            Self::LargeUtf8Plain(values) => Ok(values.value(index).into_pyobject(py)?.into_any()),
            Self::LargeUtf8(values, mode) => extract_utf8_with_mode(py, values.value(index), *mode),
            Self::List(values, item_field) => {
                let child = values.value(index);
                arrow_array_values_to_py(item_field, child.as_ref(), py, ctors)
            }
            Self::LargeList(values, item_field) => {
                let child = values.value(index);
                arrow_array_values_to_py(item_field, child.as_ref(), py, ctors)
            }
            Self::FixedSizeList(values, item_field) => {
                let child = values.value(index);
                arrow_array_values_to_py(item_field, child.as_ref(), py, ctors)
            }
            Self::Unsupported => Ok(py.None().into_bound(py)),
        }
    }
}

fn extract_timestamp_to_py<'py>(
    py: Python<'py>,
    secs: i64,
    nanos: i64,
    tz_opt: Option<&str>,
) -> PyResult<Bound<'py, PyAny>> {
    let tz = tz_opt.map(|z| {
        if z == "UTC" {
            PyTemporalTz::Offset(0)
        } else {
            PyTemporalTz::Zone(z)
        }
    });
    epoch_nanos_to_py_datetime(py, secs, nanos, tz, || match tz_opt {
        None => voyager_drivers::bolt::packstream::BoltLocalDateTime {
            seconds: secs,
            nanoseconds: nanos,
        }
        .to_iso_string(),
        Some("UTC") => voyager_drivers::bolt::packstream::BoltDateTime {
            seconds: secs,
            nanoseconds: nanos,
            tz_offset_seconds: 0,
        }
        .to_iso_string(),
        Some(z) => voyager_drivers::bolt::packstream::BoltDateTimeZoneId {
            seconds: secs,
            nanoseconds: nanos,
            zone_id: z.to_string(),
            is_local: false,
        }
        .to_iso_string(),
    })
}

fn extract_duration_ns_to_py<'py>(
    py: Python<'py>,
    ctors: &EntityConstructors,
    total_ns: i128,
) -> PyResult<Bound<'py, PyAny>> {
    let total_secs = (total_ns / 1_000_000_000) as i64;
    let ns = (total_ns % 1_000_000_000) as i64;
    ctors
        .duration_cls
        .bind(py)
        .call1((0i64, 0i64, total_secs, ns))
}

fn extract_utf8_with_mode<'py>(
    py: Python<'py>,
    s: &str,
    mode: Utf8ColMode,
) -> PyResult<Bound<'py, PyAny>> {
    match mode {
        Utf8ColMode::JsonEntities => {
            let parsed: serde_json::Value =
                parse_json_result_text(s).map_err(PyValueError::new_err)?;
            json_value_to_py(py, &parsed, true)
        }
        Utf8ColMode::JsonPure => {
            let parsed: serde_json::Value =
                parse_json_result_text(s).map_err(PyValueError::new_err)?;
            json_value_to_py(py, &parsed, false)
        }
        Utf8ColMode::JsonTyped(kind) => {
            let parsed: serde_json::Value =
                parse_json_result_text(s).map_err(PyValueError::new_err)?;
            json_value_to_py_typed(py, &parsed, kind)
        }
        Utf8ColMode::Typed(kind) => parse_voyager_typed_utf8(py, kind, s),
        Utf8ColMode::Plain => Ok(s.into_pyobject(py)?.into_any()),
    }
}

/// Converts a whole Arrow array into a Python list.
fn arrow_array_values_to_py<'py>(
    field: &arrow_schema::Field,
    array: &dyn arrow_array::Array,
    py: Python<'py>,
    ctors: &EntityConstructors,
) -> PyResult<Bound<'py, PyAny>> {
    let extractor = ColumnExtractor::new(field, array);
    let list = PyList::empty(py);
    for index in 0..array.len() {
        let val = if array.is_null(index) {
            py.None().into_bound(py)
        } else {
            extractor.extract(index, py, ctors)?
        };
        list.append(val)?;
    }
    Ok(list.into_any())
}

struct ColumnPlan<'a, 'py> {
    name: Bound<'py, PyString>,
    array: &'a dyn arrow_array::Array,
    extractor: ColumnExtractor<'a>,
    bolt_kind: Option<Option<&'static str>>,
}

pub(crate) fn record_batch_to_py_dicts<'py>(
    batch: &arrow_array::RecordBatch,
    _age_columns: &[bool],
    bolt_rows: Option<&[Vec<voyager_drivers::BoltValue>]>,
    result_rows: Option<&[Vec<ResultValue>]>,
    row_offset: usize,
    py: Python<'py>,
) -> PyResult<Bound<'py, PyList>> {
    let num_rows = batch.num_rows();
    let num_cols = batch.num_columns();
    let schema = batch.schema();
    let fields = schema.fields();
    let ctors = entity_constructors(py)?;

    let mut plans: Vec<ColumnPlan<'_, 'py>> = Vec::with_capacity(num_cols);
    for (col_idx, field) in fields.iter().enumerate() {
        let array = batch.column(col_idx).as_ref();
        let mode = utf8_col_mode_for_field(field.as_ref());
        let bolt_kind = match mode {
            Utf8ColMode::JsonEntities => Some(None),
            Utf8ColMode::JsonTyped(kind) => Some(Some(kind)),
            _ => None,
        };
        plans.push(ColumnPlan {
            name: PyString::intern(py, field.name().as_str()),
            array,
            extractor: ColumnExtractor::new(field.as_ref(), array),
            bolt_kind,
        });
    }

    let list = PyList::empty(py);

    for row in 0..num_rows {
        let dict = PyDict::new(py);
        let result_row = result_rows.and_then(|rows| rows.get(row_offset + row));
        let bolt_row = if result_row.is_none() {
            bolt_rows.and_then(|rows| rows.get(row_offset + row))
        } else {
            None
        };
        for (col_idx, plan) in plans.iter().enumerate() {
            if plan.array.is_null(row) {
                dict.set_item(&plan.name, py.None())?;
                continue;
            }
            if let Some(kind) = plan.bolt_kind {
                if let Some(rv) = result_row.and_then(|r| r.get(col_idx))
                    && !rv.is_null()
                {
                    let py_value = match kind {
                        Some(kind) => result_value_to_py_typed(py, rv, kind)?,
                        None => result_value_to_py(py, rv)?,
                    };
                    dict.set_item(&plan.name, py_value)?;
                    continue;
                }
                if let Some(bolt_val) = bolt_row.and_then(|r| r.get(col_idx)) {
                    let converted = ResultValue::from(bolt_val);
                    if !converted.is_null() {
                        let py_value = match kind {
                            Some(kind) => result_value_to_py_typed(py, &converted, kind)?,
                            None => result_value_to_py(py, &converted)?,
                        };
                        dict.set_item(&plan.name, py_value)?;
                        continue;
                    }
                }
            }
            dict.set_item(&plan.name, plan.extractor.extract(row, py, ctors)?)?;
        }
        list.append(dict)?;
    }
    Ok(list)
}

/// Converts decoded [`ResultValue`] rows directly into Python dictionaries.
///
/// This is the row-only counterpart to [`record_batch_to_py_dicts`]: column
/// names are interned once and every cell is hydrated with
/// [`result_value_to_py`], so no Apache Arrow `RecordBatch` is built or read.
pub(crate) fn result_rows_to_py_dicts<'py>(
    columns: &[String],
    rows: &[Vec<ResultValue>],
    py: Python<'py>,
) -> PyResult<Bound<'py, PyList>> {
    let names: Vec<Bound<'py, PyString>> = columns
        .iter()
        .map(|name| PyString::intern(py, name))
        .collect();
    let list = PyList::empty(py);
    for row in rows {
        let dict = PyDict::new(py);
        for (col_idx, name) in names.iter().enumerate() {
            match row.get(col_idx) {
                Some(value) => dict.set_item(name, result_value_to_py(py, value)?)?,
                None => dict.set_item(name, py.None())?,
            }
        }
        list.append(dict)?;
    }
    Ok(list)
}

struct ParamPyTypes {
    datetime_cls: Py<PyAny>,
    date_cls: Py<PyAny>,
    time_cls: Py<PyAny>,
    timedelta_cls: Py<PyAny>,
    uuid_cls: Py<PyAny>,
    decimal_cls: Py<PyAny>,
    duration_cls: Py<PyAny>,
    point_cls: Py<PyAny>,
}

static PARAM_PY_TYPES: PyOnceLock<ParamPyTypes> = PyOnceLock::new();

fn param_py_types<'py>(py: Python<'py>) -> PyResult<&'py ParamPyTypes> {
    PARAM_PY_TYPES.get_or_try_init(py, || {
        let dt_mod = py.import("datetime")?;
        let uuid_mod = py.import("uuid")?;
        let dec_mod = py.import("decimal")?;
        let types_mod = py.import("voyager_ogm.types")?;
        Ok(ParamPyTypes {
            datetime_cls: dt_mod.getattr("datetime")?.unbind(),
            date_cls: dt_mod.getattr("date")?.unbind(),
            time_cls: dt_mod.getattr("time")?.unbind(),
            timedelta_cls: dt_mod.getattr("timedelta")?.unbind(),
            uuid_cls: uuid_mod.getattr("UUID")?.unbind(),
            decimal_cls: dec_mod.getattr("Decimal")?.unbind(),
            duration_cls: types_mod.getattr("Duration")?.unbind(),
            point_cls: types_mod.getattr("Point")?.unbind(),
        })
    })
}

fn extract_non_bool_f64(obj: &Bound<'_, PyAny>) -> Option<f64> {
    if obj.is_none() || obj.is_instance_of::<PyBool>() {
        return None;
    }
    obj.extract::<f64>().ok()
}

fn srid_from_crs(crs: &str, is_3d: bool) -> Option<i64> {
    let norm = crs.trim().to_ascii_lowercase();
    match (norm.as_str(), is_3d) {
        ("wgs-84" | "wgs84" | "epsg:4326", false) => Some(4326),
        ("wgs-84" | "wgs84" | "wgs-84-3d" | "wgs84-3d" | "epsg:4979", true) => Some(4979),
        ("wgs-84-3d" | "wgs84-3d" | "epsg:4979", false) => Some(4979),
        ("cartesian" | "cartesian-2d" | "epsg:7203", false) => Some(7203),
        ("cartesian" | "cartesian-3d" | "epsg:9157", true) => Some(9157),
        ("cartesian-3d" | "epsg:9157", false) => Some(9157),
        _ => None,
    }
}

fn extract_point_param_from_mapping(
    mapping: &Bound<'_, PyAny>,
) -> Option<voyager_drivers::ParamValue> {
    use voyager_drivers::ParamValue;

    let dict = mapping.cast::<PyDict>().ok()?;
    let srid_opt = match dict.get_item("srid").ok().flatten() {
        Some(v) if !v.is_none() && !v.is_instance_of::<PyBool>() => Some(v.extract::<i64>().ok()?),
        Some(v) if v.is_instance_of::<PyBool>() => return None,
        _ => None,
    };
    let crs_opt = match dict.get_item("crs").ok().flatten() {
        Some(v) if !v.is_none() => Some(v.extract::<String>().ok()?),
        _ => None,
    };

    let lat_obj = dict.get_item("latitude").ok().flatten();
    let lon_obj = dict.get_item("longitude").ok().flatten();
    let height_obj = dict.get_item("height").ok().flatten();
    let x_obj = dict.get_item("x").ok().flatten();
    let y_obj = dict.get_item("y").ok().flatten();
    let z_obj = dict.get_item("z").ok().flatten();

    let height_or_z = height_obj
        .as_ref()
        .filter(|v| !v.is_none())
        .or_else(|| z_obj.as_ref().filter(|v| !v.is_none()));
    let has_3d = height_or_z.is_some();
    let z_val = match height_or_z {
        Some(v) => Some(extract_non_bool_f64(v)?),
        None => None,
    };

    let is_geo_by_srid = srid_opt.map(|s| s == 4326 || s == 4979);
    let is_geo_by_keys = lat_obj.as_ref().is_some_and(|v| !v.is_none())
        || lon_obj.as_ref().is_some_and(|v| !v.is_none())
        || height_obj.as_ref().is_some_and(|v| !v.is_none());
    let is_geo_by_crs = crs_opt
        .as_deref()
        .and_then(|c| srid_from_crs(c, has_3d))
        .map(|s| s == 4326 || s == 4979);

    let is_geo = is_geo_by_srid.or(is_geo_by_crs).unwrap_or(is_geo_by_keys);

    let (x, y) = if is_geo {
        let lon = lon_obj
            .as_ref()
            .filter(|v| !v.is_none())
            .or_else(|| x_obj.as_ref().filter(|v| !v.is_none()))
            .and_then(extract_non_bool_f64)?;
        let lat = lat_obj
            .as_ref()
            .filter(|v| !v.is_none())
            .or_else(|| y_obj.as_ref().filter(|v| !v.is_none()))
            .and_then(extract_non_bool_f64)?;
        (lon, lat)
    } else {
        let x = x_obj.as_ref().and_then(extract_non_bool_f64)?;
        let y = y_obj.as_ref().and_then(extract_non_bool_f64)?;
        (x, y)
    };

    let srid = match srid_opt {
        Some(s) => s,
        None => match crs_opt.as_deref().and_then(|c| srid_from_crs(c, has_3d)) {
            Some(s) => s,
            None if is_geo => {
                if z_val.is_some() {
                    4979
                } else {
                    4326
                }
            }
            None => {
                if z_val.is_some() {
                    9157
                } else {
                    7203
                }
            }
        },
    };

    if let Some(z) = z_val {
        Some(ParamValue::Point3D { srid, x, y, z })
    } else {
        Some(ParamValue::Point2D { srid, x, y })
    }
}

fn extract_timedelta_offset_seconds(offset_obj: &Bound<'_, PyAny>) -> PyResult<i32> {
    let days: i64 = offset_obj.getattr("days")?.extract()?;
    let secs: i64 = offset_obj.getattr("seconds")?.extract()?;
    let micros: i64 = offset_obj.getattr("microseconds")?.extract()?;
    if micros != 0 {
        return Err(PyValueError::new_err(
            "Sub-second UTC offsets are not supported by native drivers",
        ));
    }
    i32::try_from(days * 86_400 + secs)
        .map_err(|_| PyValueError::new_err("UTC offset seconds out of 32-bit range"))
}

fn extract_zone_id(tzinfo: &Bound<'_, PyAny>) -> PyResult<Option<String>> {
    if let Ok(key) = tzinfo.getattr("key")
        && !key.is_none()
        && let Ok(s) = key.extract::<String>()
        && !s.is_empty()
    {
        return Ok(Some(s));
    }
    if let Ok(zone) = tzinfo.getattr("zone")
        && !zone.is_none()
        && let Ok(s) = zone.extract::<String>()
        && !s.is_empty()
    {
        return Ok(Some(s));
    }
    Ok(None)
}

fn py_any_to_param_value(val: &Bound<'_, PyAny>) -> PyResult<voyager_drivers::ParamValue> {
    use voyager_drivers::{ParamTimezone, ParamValue};

    if val.is_none() {
        Ok(ParamValue::Json(serde_json::Value::Null))
    } else if let Ok(b) = val.cast::<PyBool>() {
        Ok(ParamValue::Json(serde_json::Value::Bool(b.is_true())))
    } else if let Ok(i) = val.cast::<PyInt>() {
        let val_i: i64 = i.extract()?;
        Ok(ParamValue::Json(serde_json::Value::Number(val_i.into())))
    } else if let Ok(f) = val.cast::<PyFloat>() {
        let fv = f.value();
        if let Some(n) = serde_json::Number::from_f64(fv) {
            Ok(ParamValue::Json(serde_json::Value::Number(n)))
        } else {
            Ok(ParamValue::Float(fv))
        }
    } else if let Ok(s) = val.cast::<PyString>() {
        Ok(ParamValue::Json(serde_json::Value::String(
            s.to_string_lossy().into_owned(),
        )))
    } else if val.hasattr("__voyager_json__")? {
        let inner = val.getattr("value")?;
        let converted = py_any_to_param_value(&inner)?;
        Ok(ParamValue::Jsonb(Box::new(converted)))
    } else if let Ok(b) = val.cast::<PyBytes>() {
        Ok(ParamValue::Bytes(b.as_bytes().to_vec()))
    } else if let Ok(ba) = val.cast::<PyByteArray>() {
        Ok(ParamValue::Bytes(ba.to_vec()))
    } else if let Ok(list) = val.cast::<PyList>() {
        let mut items = Vec::with_capacity(list.len());
        for item in list {
            items.push(py_any_to_param_value(&item)?);
        }
        Ok(ParamValue::List(items))
    } else if let Ok(tuple) = val.cast::<PyTuple>() {
        let mut items = Vec::with_capacity(tuple.len());
        for item in tuple {
            items.push(py_any_to_param_value(&item)?);
        }
        Ok(ParamValue::List(items))
    } else if let Ok(dict) = val.cast::<PyDict>() {
        if dict.len() == 1
            && let Ok(Some(inner)) = dict.get_item("__voyager_spatial__")
            && let Some(pt) = extract_point_param_from_mapping(&inner)
        {
            return Ok(pt);
        }
        let mut map = HashMap::with_capacity(dict.len());
        for (k, v) in dict {
            let key = if let Ok(s) = k.cast::<PyString>() {
                s.to_string_lossy().into_owned()
            } else {
                k.extract::<String>()?
            };
            map.insert(key, py_any_to_param_value(&v)?);
        }
        Ok(ParamValue::Map(map))
    } else {
        let py = val.py();
        let types = param_py_types(py)?;
        if val.is_instance(types.datetime_cls.bind(py))? {
            let year: i32 = val.getattr("year")?.extract()?;
            let month: u8 = val.getattr("month")?.extract()?;
            let day: u8 = val.getattr("day")?.extract()?;
            let hour: u8 = val.getattr("hour")?.extract()?;
            let minute: u8 = val.getattr("minute")?.extract()?;
            let second: u8 = val.getattr("second")?.extract()?;
            let microsecond: u32 = val.getattr("microsecond")?.extract()?;
            let nanosecond = microsecond.saturating_mul(1_000);

            let tzinfo = val.getattr("tzinfo")?;
            if tzinfo.is_none() {
                return Ok(ParamValue::LocalDateTime {
                    year,
                    month,
                    day,
                    hour,
                    minute,
                    second,
                    nanosecond,
                });
            }
            let offset_obj = val.call_method0("utcoffset")?;
            if offset_obj.is_none() {
                return Ok(ParamValue::LocalDateTime {
                    year,
                    month,
                    day,
                    hour,
                    minute,
                    second,
                    nanosecond,
                });
            }
            let offset_seconds = extract_timedelta_offset_seconds(&offset_obj)?;
            let local_days =
                voyager_drivers::bolt::packstream::ymd_to_days(year, month as u32, day as u32);
            let local_seconds = local_days * 86_400
                + (hour as i64) * 3_600
                + (minute as i64) * 60
                + (second as i64);
            let utc_seconds = local_seconds - (offset_seconds as i64);
            let tz = match extract_zone_id(&tzinfo)? {
                Some(id) => ParamTimezone::ZoneId { id, offset_seconds },
                None => ParamTimezone::OffsetSeconds(offset_seconds),
            };
            Ok(ParamValue::DateTime {
                utc_seconds,
                nanosecond,
                tz,
            })
        } else if val.is_instance(types.date_cls.bind(py))? {
            let year: i32 = val.getattr("year")?.extract()?;
            let month: u8 = val.getattr("month")?.extract()?;
            let day: u8 = val.getattr("day")?.extract()?;
            Ok(ParamValue::Date { year, month, day })
        } else if val.is_instance(types.time_cls.bind(py))? {
            let hour: u8 = val.getattr("hour")?.extract()?;
            let minute: u8 = val.getattr("minute")?.extract()?;
            let second: u8 = val.getattr("second")?.extract()?;
            let microsecond: u32 = val.getattr("microsecond")?.extract()?;
            let nanosecond = microsecond.saturating_mul(1_000);
            let offset_obj = val.call_method0("utcoffset")?;
            let tz_offset_seconds = if offset_obj.is_none() {
                None
            } else {
                Some(extract_timedelta_offset_seconds(&offset_obj)?)
            };
            Ok(ParamValue::Time {
                hour,
                minute,
                second,
                nanosecond,
                tz_offset_seconds,
            })
        } else if val.is_instance(types.timedelta_cls.bind(py))? {
            let days: i64 = val.getattr("days")?.extract()?;
            let seconds: i64 = val.getattr("seconds")?.extract()?;
            let microseconds: i64 = val.getattr("microseconds")?.extract()?;
            Ok(ParamValue::Duration {
                months: 0,
                days,
                seconds,
                nanoseconds: microseconds.saturating_mul(1_000),
            })
        } else if val.is_instance(types.duration_cls.bind(py))? {
            let months: i64 = val.getattr("months")?.extract()?;
            let days: i64 = val.getattr("days")?.extract()?;
            let seconds: i64 = val.getattr("seconds")?.extract()?;
            let nanoseconds: i64 = val.getattr("nanoseconds")?.extract()?;
            Ok(ParamValue::Duration {
                months,
                days,
                seconds,
                nanoseconds,
            })
        } else if val.is_instance(types.point_cls.bind(py))? {
            let srid: i64 = val.getattr("srid")?.extract()?;
            let x: f64 = val.getattr("x")?.extract()?;
            let y: f64 = val.getattr("y")?.extract()?;
            let z_obj = val.getattr("z")?;
            if !z_obj.is_none() {
                let z: f64 = z_obj.extract()?;
                Ok(ParamValue::Point3D { srid, x, y, z })
            } else {
                Ok(ParamValue::Point2D { srid, x, y })
            }
        } else if val.is_instance(types.uuid_cls.bind(py))? {
            let s: String = val.str()?.extract()?;
            Ok(ParamValue::Uuid(s))
        } else if val.is_instance(types.decimal_cls.bind(py))? {
            let s: String = val.str()?.extract()?;
            Ok(ParamValue::Decimal(s))
        } else if val.hasattr("to_cypher_dict")? {
            let cypher_dict = val.call_method0("to_cypher_dict")?;
            py_any_to_param_value(&cypher_dict)
        } else {
            let s: String = val.str()?.extract()?;
            Ok(ParamValue::Json(serde_json::Value::String(s)))
        }
    }
}

pub(crate) fn py_dict_to_params(
    dict_opt: Option<Bound<'_, PyDict>>,
) -> PyResult<HashMap<String, voyager_drivers::ParamValue>> {
    let mut params = HashMap::new();
    if let Some(dict) = dict_opt {
        params.reserve(dict.len());
        for (k, v) in dict {
            let key = if let Ok(s) = k.cast::<PyString>() {
                s.to_string_lossy().into_owned()
            } else {
                k.extract::<String>()?
            };
            let val = py_any_to_param_value(&v)?;
            params.insert(key, val);
        }
    }
    Ok(params)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_result_parser_keeps_extreme_floats_exact() {
        let parsed = parse_json_result_text(r#"{"k":2.332717393807508e-41}"#).unwrap();
        assert_eq!(
            parsed["k"].as_f64().unwrap().to_bits(),
            2.332717393807508e-41_f64.to_bits()
        );

        let max = parse_json_result_text(r#"{"k":1.7976931348623157e+308}"#).unwrap();
        assert_eq!(max["k"].as_f64().unwrap(), f64::MAX);
    }

    #[test]
    fn json_result_parser_accepts_non_finite_and_out_of_range_numbers() {
        let parsed = parse_json_result_text(r#"{"a":NaN,"b":Infinity,"c":-Infinity}"#).unwrap();
        assert_eq!(parsed["a"], JSON_NAN_SENTINEL);
        assert_eq!(parsed["b"], JSON_INF_SENTINEL);
        assert_eq!(parsed["c"], JSON_NEG_INF_SENTINEL);

        let huge = parse_json_result_text(r#"{"k":1e400}"#).unwrap();
        assert_eq!(huge["k"], JSON_INF_SENTINEL);
        let tiny = parse_json_result_text(r#"{"k":-1e400}"#).unwrap();
        assert_eq!(tiny["k"], JSON_NEG_INF_SENTINEL);
    }

    #[test]
    fn json_result_parser_preserves_integers_beyond_u64() {
        let parsed = parse_json_result_text(r#"{"k":13884398771378935000000000}"#).unwrap();
        assert_eq!(
            parsed["k"],
            format!("{JSON_INT_SENTINEL_PREFIX}13884398771378935000000000")
        );
        let negative = parse_json_result_text(r#"{"k":-17976931348623157000000000}"#).unwrap();
        assert_eq!(
            negative["k"],
            format!("{JSON_INT_SENTINEL_PREFIX}-17976931348623157000000000")
        );
        let small = parse_json_result_text(r#"{"k":9223372036854775807}"#).unwrap();
        assert_eq!(small["k"].as_i64(), Some(i64::MAX));
    }

    #[test]
    fn json_result_parser_leaves_strings_and_finite_numbers_alone() {
        let parsed = parse_json_result_text(r#"{"s":"Infinity NaN 1e400","n":0.5}"#).unwrap();
        assert_eq!(parsed["s"], "Infinity NaN 1e400");
        assert_eq!(parsed["n"].as_f64(), Some(0.5));
    }

    #[test]
    fn utf8_col_mode_maps_voyager_point_to_typed_decoder() {
        let mut metadata = HashMap::new();
        metadata.insert(VOYAGER_META_TYPE.to_string(), META_TYPE_POINT.to_string());
        let field = arrow_schema::Field::new("p", arrow_schema::DataType::Utf8, true)
            .with_metadata(metadata);
        assert_eq!(
            utf8_col_mode_for_field(&field),
            Utf8ColMode::Typed(META_TYPE_POINT)
        );
    }
}
