//! Python PyO3 bindings and Apache Arrow bridge for Voyager OGM.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};

use pyo3::exceptions::{
    PyNotImplementedError, PyRuntimeError, PyTypeError, PyUserWarning, PyValueError,
};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{
    PyBool, PyByteArray, PyBytes, PyDict, PyFloat, PyFrozenSet, PyInt, PyList, PyString, PyTuple,
};

pyo3::create_exception!(
    voyager_ogm._voyager_rs,
    VoyagerError,
    pyo3::exceptions::PyValueError
);

fn make_voyager_err(code: voyager_ogm::ErrorCode, msg: impl Into<String>) -> PyErr {
    Python::attach(|py| {
        let py_err = VoyagerError::new_err(msg.into());
        let _ = py_err.value(py).setattr("code", code.as_str());
        py_err
    })
}

/// Emits a Python `UserWarning` with the given message.
fn emit_user_warning(py: Python<'_>, message: &str) -> PyResult<()> {
    let warnings = py.import("warnings")?;
    let user_warning = py.get_type::<PyUserWarning>();
    warnings.call_method1("warn", (message, user_warning))?;
    Ok(())
}

/// Maps a Query Spec consumer error onto the Python `VoyagerError`, preserving
/// the code, JSON Pointer path and message, and emitting any warnings the
/// consumer produced before failing.
fn spec_error_to_py(error: voyager_ogm::spec::SpecError) -> PyErr {
    Python::attach(|py| {
        for warning in error.warnings() {
            let _ = emit_user_warning(py, warning);
        }
        let py_err = VoyagerError::new_err(error.message().to_string());
        let _ = py_err.value(py).setattr("code", error.code().as_str());
        if let Some(path) = error.path() {
            let _ = py_err.value(py).setattr("path", path);
        }
        py_err
    })
}

/// Path to an element of a Python builder argument.
///
/// Paths are built while descending into the argument so an error does not have
/// to reconstruct its location: [`SpecPath::key`] appends an object member and
/// [`SpecPath::index`] appends an array index. `/` and `~` are escaped as `~1`
/// and `~0`, matching JSON Pointer. The root is the empty string.
#[derive(Clone, Debug, PartialEq, Eq)]
struct SpecPath {
    pointer: String,
}

impl SpecPath {
    fn fluent() -> Self {
        Self {
            pointer: String::new(),
        }
    }

    fn key(&self, key: &str) -> Self {
        let mut pointer = self.pointer.clone();
        pointer.push('/');
        for ch in key.chars() {
            match ch {
                '~' => pointer.push_str("~0"),
                '/' => pointer.push_str("~1"),
                other => pointer.push(other),
            }
        }
        Self { pointer }
    }

    fn index(&self, index: usize) -> Self {
        let mut pointer = self.pointer.clone();
        pointer.push('/');
        pointer.push_str(&index.to_string());
        Self { pointer }
    }
}

/// JSON type name for a Python value, used in spec error messages.
///
/// Spec authors write JSON, so messages say `number`, `string`, `array`, and so
/// on instead of leaking a Python type repr such as `<class 'int'>`.
fn json_type_name(val: &Bound<'_, PyAny>) -> &'static str {
    if val.is_none() {
        "null"
    } else if val.is_instance_of::<PyBool>() {
        "boolean"
    } else if val.is_instance_of::<PyInt>() || val.is_instance_of::<PyFloat>() {
        "number"
    } else if val.is_instance_of::<PyString>() {
        "string"
    } else if val.is_instance_of::<PyList>() || val.is_instance_of::<PyTuple>() {
        "array"
    } else if val.is_instance_of::<PyDict>() {
        "object"
    } else {
        "value"
    }
}

/// Renders a scalar Python value the way JSON spells it, for error messages.
///
/// Containers and arbitrary objects fall back to their JSON type name, so a
/// Python `repr` (single quotes, tuple parentheses) never leaks into a spec
/// message.
fn render_json_scalar(val: &Bound<'_, PyAny>) -> Option<String> {
    if val.is_none() {
        Some("null".to_string())
    } else if let Ok(boolean) = val.extract::<bool>() {
        Some(boolean.to_string())
    } else if let Ok(integer) = val.extract::<i64>() {
        Some(integer.to_string())
    } else if let Ok(float) = val.extract::<f64>() {
        Some(float.to_string())
    } else if val.is_instance_of::<PyString>() {
        val.extract::<String>()
            .ok()
            .map(|s| format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\"")))
    } else {
        None
    }
}

/// Structural error for a fluent builder argument.
fn path_error(_path: &SpecPath, msg: impl Into<String>) -> PyErr {
    make_voyager_err(voyager_ogm::ErrorCode::InvalidClause, msg)
}

/// Value error for a fluent builder argument.
fn path_value_error(_path: &SpecPath, msg: impl Into<String>) -> PyErr {
    PyValueError::new_err(msg.into())
}

/// Extracts a Python value for a fluent builder argument.
fn extract_required<'a, 'py, T>(
    val: &'a Bound<'py, PyAny>,
    _path: &SpecPath,
    expected: &str,
) -> PyResult<T>
where
    T: pyo3::FromPyObject<'a, 'py>,
{
    val.extract::<T>().map_err(|_| {
        let msg = format!("expected {expected}, got {}", json_type_name(val));
        PyTypeError::new_err(msg)
    })
}

fn to_py_schema_err(err: voyager_ogm::Error) -> PyErr {
    Python::attach(|py| match err.code() {
        voyager_ogm::ErrorCode::UnsupportedDialect | voyager_ogm::ErrorCode::UnsupportedFeature => {
            let py_err = PyNotImplementedError::new_err(err.to_string());
            let _ = py_err.value(py).setattr("code", err.code().as_str());
            py_err
        }
        _ => make_voyager_err(err.code(), err.to_string()),
    })
}
use voyager_ogm::ast::{
    AggregationFunc, BinaryOp, Direction, LiteralValue, NodeHandle, QueryAstArena, UnaryOp,
};
use voyager_ogm::builder::{
    MergeRelationshipProperties, QueryBuilder, RelationshipBatchSpec, RelationshipConnectSpec,
    RelationshipDisconnectSpec, RelationshipEndpoint, RelationshipNavigationSpec,
};
use voyager_ogm::emitters::{AgeEmitter, CypherEmitter, IsoGqlEmitter, SqlPgqEmitter};
use voyager_ogm::optimizer::{AstOptimizer, OptimizationLevel};
use voyager_ogm::schema::{
    ConformanceReport, DEFAULT_VECTOR_DIMENSIONS, DdlDialect, FieldDescriptor, FieldType,
    FullTextIndexConfig, IndexType, NodeSchema, RelationshipDeclaration, RelationshipSchema,
    SchemaRegistry, VectorIndexConfig, VectorSimilarity,
    adapt_tantivy_query as core_adapt_tantivy_query,
    canonical_fulltext_index_name as core_canonical_fulltext_index_name,
    canonical_vector_index_name as core_canonical_vector_index_name, global_schema_registry,
    resolve_fulltext_search as core_resolve_fulltext_search,
};
use voyager_ogm::spec_export::{SpecExportError, SpecValue, spec_from_ast};
use voyager_ogm::topology::GraphTopology;
use voyager_ogm::visitor::{AstVisitor, CompiledQuery};

fn compiled_query_to_py_dict<'py>(
    py: Python<'py>,
    compiled: &CompiledQuery,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("statement", &compiled.statement)?;

    let params_dict = PyDict::new(py);
    for (k, v) in &compiled.parameters {
        params_dict.set_item(k, literal_to_py(v, py)?)?;
    }
    dict.set_item("parameters", params_dict)?;
    dict.set_item("execution_mode", compiled.execution_mode.as_str())?;

    let cols_list = PyList::empty(py);
    for col in &compiled.columns {
        let col_dict = PyDict::new(py);
        col_dict.set_item("name", &col.name)?;
        col_dict.set_item("alias", &col.alias)?;
        cols_list.append(col_dict)?;
    }
    dict.set_item("columns", cols_list)?;

    Ok(dict)
}

fn py_to_literal(val: &Bound<'_, PyAny>) -> PyResult<LiteralValue> {
    if val.is_none() {
        Ok(LiteralValue::Null)
    } else if let Ok(b) = val.cast::<PyBool>() {
        Ok(LiteralValue::Bool(b.is_true()))
    } else if let Ok(i) = val.cast::<PyInt>() {
        Ok(LiteralValue::Int64(i.extract()?))
    } else if let Ok(f) = val.cast::<PyFloat>() {
        Ok(LiteralValue::Float64(f.value()))
    } else if let Ok(s) = val.cast::<PyString>() {
        Ok(LiteralValue::String(s.to_string_lossy().into_owned()))
    } else if let Ok(list) = val.cast::<PyList>() {
        let mut items = Vec::with_capacity(list.len());
        for item in list {
            items.push(py_to_literal(&item)?);
        }
        Ok(LiteralValue::List(items))
    } else if let Ok(tuple) = val.cast::<PyTuple>() {
        let mut items = Vec::with_capacity(tuple.len());
        for item in tuple {
            items.push(py_to_literal(&item)?);
        }
        Ok(LiteralValue::List(items))
    } else if let Ok(dict) = val.cast::<PyDict>() {
        let mut entries = Vec::with_capacity(dict.len());
        for (k, v) in dict {
            let key_str: String = if let Ok(s) = k.cast::<PyString>() {
                s.to_string_lossy().into_owned()
            } else {
                k.extract()?
            };
            let val_lit = py_to_literal(&v)?;
            entries.push((key_str, val_lit));
        }
        Ok(LiteralValue::Map(entries))
    } else if val.hasattr("isoformat")? {
        let iso: String = val.call_method0("isoformat")?.extract()?;
        Ok(LiteralValue::String(iso))
    } else if val.hasattr("hex")? && val.hasattr("urn")? {
        let s: String = val.str()?.extract()?;
        Ok(LiteralValue::String(s))
    } else if val.get_type().name()? == "Decimal" {
        if let Ok(f) = val
            .call_method0("__float__")
            .and_then(|v| v.extract::<f64>())
        {
            Ok(LiteralValue::Float64(f))
        } else {
            let s: String = val.str()?.extract()?;
            Ok(LiteralValue::String(s))
        }
    } else {
        Err(PyValueError::new_err(format!(
            "Unsupported parameter type: {}",
            json_type_name(val)
        )))
    }
}

/// Converts the right-hand side of a fluent `IN` / `NOT IN` comparison. A
/// Python list is the membership collection, so it is always a literal list;
/// anything else goes through the normal expression converter.
fn fluent_membership_operand(
    builder: &mut QueryBuilder,
    other: &Bound<'_, PyAny>,
    path: &SpecPath,
) -> PyResult<NodeHandle> {
    if other.cast::<PyList>().is_ok() {
        let lit = py_to_literal(other).map_err(|e| path_value_error(path, e.to_string()))?;
        return Ok(builder.literal(lit));
    }
    py_to_node_handle_depth(builder, other, 0, path)
}

fn property_list_to_literals(val: &Bound<'_, PyAny>) -> PyResult<Vec<(String, LiteralValue)>> {
    let list = val.cast::<PyList>().map_err(|_| {
        PyTypeError::new_err("relationship properties must be a list of (name, value) pairs")
    })?;
    let mut properties = Vec::with_capacity(list.len());
    for item in list {
        let (name, value): (String, Bound<'_, PyAny>) = item.extract()?;
        properties.push((name, py_to_literal(&value)?));
    }
    Ok(properties)
}

fn optional_property_list_to_literals(
    properties: Option<Vec<(String, Bound<'_, PyAny>)>>,
) -> PyResult<Vec<(String, LiteralValue)>> {
    match properties {
        Some(items) => {
            let mut literals = Vec::with_capacity(items.len());
            for (name, value) in items {
                literals.push((name, py_to_literal(&value)?));
            }
            Ok(literals)
        }
        None => Ok(Vec::new()),
    }
}

fn parse_binary_op(s: &str) -> PyResult<BinaryOp> {
    match s.to_ascii_lowercase().as_str() {
        "=" | "eq" => Ok(BinaryOp::Eq),
        "!=" | "ne" | "neq" | "<>" => Ok(BinaryOp::Neq),
        "<" | "lt" => Ok(BinaryOp::Lt),
        "<=" | "lte" | "le" => Ok(BinaryOp::Lte),
        ">" | "gt" => Ok(BinaryOp::Gt),
        ">=" | "gte" | "ge" => Ok(BinaryOp::Gte),
        "in" => Ok(BinaryOp::In),
        "not_in" | "not in" => Ok(BinaryOp::NotIn),
        "contains" => Ok(BinaryOp::Contains),
        "starts_with" | "startswith" => Ok(BinaryOp::StartsWith),
        "ends_with" | "endswith" => Ok(BinaryOp::EndsWith),
        "=~" | "regex" => Ok(BinaryOp::RegexMatch),
        "and" | "&" => Ok(BinaryOp::And),
        "or" | "|" => Ok(BinaryOp::Or),
        "xor" | "^" => Ok(BinaryOp::Xor),
        "+" | "add" => Ok(BinaryOp::Add),
        "||" | "concat" => Ok(BinaryOp::Concat),
        "-" | "sub" => Ok(BinaryOp::Sub),
        "*" | "mul" => Ok(BinaryOp::Mul),
        "/" | "div" => Ok(BinaryOp::Div),
        "%" | "mod" => Ok(BinaryOp::Mod),
        other => Err(PyValueError::new_err(format!(
            "Unknown binary operator: '{other}'"
        ))),
    }
}

fn parse_unary_op(s: &str) -> PyResult<UnaryOp> {
    match s.to_ascii_lowercase().as_str() {
        "not" | "~" => Ok(UnaryOp::Not),
        "-" | "neg" => Ok(UnaryOp::Neg),
        "is_null" | "is null" => Ok(UnaryOp::IsNull),
        "is_not_null" | "is not null" => Ok(UnaryOp::IsNotNull),
        other => Err(PyValueError::new_err(format!(
            "Unknown unary operator: '{other}'"
        ))),
    }
}

/// Native AST Expression node handle wrapping a lightweight AST arena.
#[pyclass(name = "AstExpr", from_py_object)]
#[derive(Clone)]
pub struct PyAstExpr {
    pub(crate) arena: QueryAstArena,
    pub(crate) handle: NodeHandle,
}

#[pymethods]
impl PyAstExpr {
    #[staticmethod]
    fn prop(var: String, prop: String) -> Self {
        let mut b = QueryBuilder::new();
        let h = b.prop(var, prop);
        Self {
            arena: b.into_arena(),
            handle: h,
        }
    }

    #[staticmethod]
    fn ident(name: String) -> Self {
        let mut b = QueryBuilder::new();
        let h = b.ident(name);
        Self {
            arena: b.into_arena(),
            handle: h,
        }
    }

    #[staticmethod]
    fn label(variable: String, labels: Vec<String>) -> Self {
        let mut b = QueryBuilder::new();
        let h = b.label_predicate(variable, labels);
        Self {
            arena: b.into_arena(),
            handle: h,
        }
    }

    #[staticmethod]
    fn literal(val: &Bound<'_, PyAny>) -> PyResult<Self> {
        let lit = py_to_literal(val)?;
        let mut b = QueryBuilder::new();
        let h = b.literal(lit);
        Ok(Self {
            arena: b.into_arena(),
            handle: h,
        })
    }

    #[staticmethod]
    fn binary(left: &Bound<'_, PyAny>, op: String, right: &Bound<'_, PyAny>) -> PyResult<Self> {
        let op_parsed = parse_binary_op(&op)?;
        let mut b = QueryBuilder::new();
        let left_h = py_to_node_handle_depth(&mut b, left, 0, &SpecPath::fluent())?;
        let right_h = py_to_node_handle_depth(&mut b, right, 0, &SpecPath::fluent())?;
        let h = b.binary_expr(left_h, op_parsed, right_h);
        Ok(Self {
            arena: b.into_arena(),
            handle: h,
        })
    }

    #[staticmethod]
    fn unary(op: String, operand: &Bound<'_, PyAny>) -> PyResult<Self> {
        let op_parsed = parse_unary_op(&op)?;
        let mut b = QueryBuilder::new();
        let op_h = py_to_node_handle_depth(&mut b, operand, 0, &SpecPath::fluent())?;
        let h = b.unary_expr(op_parsed, op_h);
        Ok(Self {
            arena: b.into_arena(),
            handle: h,
        })
    }

    #[staticmethod]
    fn function(name: String, args: Vec<Bound<'_, PyAny>>) -> PyResult<Self> {
        let mut b = QueryBuilder::new();
        let mut arg_handles = Vec::with_capacity(args.len());
        for arg in &args {
            arg_handles.push(py_to_node_handle_depth(
                &mut b,
                arg,
                0,
                &SpecPath::fluent(),
            )?);
        }
        let h = b.function(name, arg_handles);
        Ok(Self {
            arena: b.into_arena(),
            handle: h,
        })
    }

    fn in_(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        let mut b = QueryBuilder::new();
        let left_h = b.import_subarena(self.arena.clone(), self.handle);
        let right_h = fluent_membership_operand(&mut b, other, &SpecPath::fluent())?;
        let h = b.binary_expr(left_h, BinaryOp::In, right_h);
        Ok(Self {
            arena: b.into_arena(),
            handle: h,
        })
    }

    fn not_in(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        let mut b = QueryBuilder::new();
        let left_h = b.import_subarena(self.arena.clone(), self.handle);
        let right_h = fluent_membership_operand(&mut b, other, &SpecPath::fluent())?;
        let h = b.binary_expr(left_h, BinaryOp::NotIn, right_h);
        Ok(Self {
            arena: b.into_arena(),
            handle: h,
        })
    }

    fn contains(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        let mut b = QueryBuilder::new();
        let left_h = b.import_subarena(self.arena.clone(), self.handle);
        let right_h = py_to_node_handle_depth(&mut b, other, 0, &SpecPath::fluent())?;
        let h = b.binary_expr(left_h, BinaryOp::Contains, right_h);
        Ok(Self {
            arena: b.into_arena(),
            handle: h,
        })
    }

    fn startswith(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        let mut b = QueryBuilder::new();
        let left_h = b.import_subarena(self.arena.clone(), self.handle);
        let right_h = py_to_node_handle_depth(&mut b, other, 0, &SpecPath::fluent())?;
        let h = b.binary_expr(left_h, BinaryOp::StartsWith, right_h);
        Ok(Self {
            arena: b.into_arena(),
            handle: h,
        })
    }

    fn endswith(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        let mut b = QueryBuilder::new();
        let left_h = b.import_subarena(self.arena.clone(), self.handle);
        let right_h = py_to_node_handle_depth(&mut b, other, 0, &SpecPath::fluent())?;
        let h = b.binary_expr(left_h, BinaryOp::EndsWith, right_h);
        Ok(Self {
            arena: b.into_arena(),
            handle: h,
        })
    }

    fn is_null(&self) -> Self {
        let mut b = QueryBuilder::new();
        let op_h = b.import_subarena(self.arena.clone(), self.handle);
        let h = b.unary_expr(UnaryOp::IsNull, op_h);
        Self {
            arena: b.into_arena(),
            handle: h,
        }
    }

    fn is_not_null(&self) -> Self {
        let mut b = QueryBuilder::new();
        let op_h = b.import_subarena(self.arena.clone(), self.handle);
        let h = b.unary_expr(UnaryOp::IsNotNull, op_h);
        Self {
            arena: b.into_arena(),
            handle: h,
        }
    }

    fn __add__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        let mut b = QueryBuilder::new();
        let left_h = b.import_subarena(self.arena.clone(), self.handle);
        let right_h = py_to_node_handle_depth(&mut b, other, 0, &SpecPath::fluent())?;
        let h = b.binary_expr(left_h, BinaryOp::Add, right_h);
        Ok(Self {
            arena: b.into_arena(),
            handle: h,
        })
    }

    fn __radd__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        let mut b = QueryBuilder::new();
        let left_h = py_to_node_handle_depth(&mut b, other, 0, &SpecPath::fluent())?;
        let right_h = b.import_subarena(self.arena.clone(), self.handle);
        let h = b.binary_expr(left_h, BinaryOp::Add, right_h);
        Ok(Self {
            arena: b.into_arena(),
            handle: h,
        })
    }

    fn __sub__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        let mut b = QueryBuilder::new();
        let left_h = b.import_subarena(self.arena.clone(), self.handle);
        let right_h = py_to_node_handle_depth(&mut b, other, 0, &SpecPath::fluent())?;
        let h = b.binary_expr(left_h, BinaryOp::Sub, right_h);
        Ok(Self {
            arena: b.into_arena(),
            handle: h,
        })
    }

    fn __rsub__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        let mut b = QueryBuilder::new();
        let left_h = py_to_node_handle_depth(&mut b, other, 0, &SpecPath::fluent())?;
        let right_h = b.import_subarena(self.arena.clone(), self.handle);
        let h = b.binary_expr(left_h, BinaryOp::Sub, right_h);
        Ok(Self {
            arena: b.into_arena(),
            handle: h,
        })
    }

    fn __mul__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        let mut b = QueryBuilder::new();
        let left_h = b.import_subarena(self.arena.clone(), self.handle);
        let right_h = py_to_node_handle_depth(&mut b, other, 0, &SpecPath::fluent())?;
        let h = b.binary_expr(left_h, BinaryOp::Mul, right_h);
        Ok(Self {
            arena: b.into_arena(),
            handle: h,
        })
    }

    fn __rmul__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        let mut b = QueryBuilder::new();
        let left_h = py_to_node_handle_depth(&mut b, other, 0, &SpecPath::fluent())?;
        let right_h = b.import_subarena(self.arena.clone(), self.handle);
        let h = b.binary_expr(left_h, BinaryOp::Mul, right_h);
        Ok(Self {
            arena: b.into_arena(),
            handle: h,
        })
    }

    fn __truediv__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        let mut b = QueryBuilder::new();
        let left_h = b.import_subarena(self.arena.clone(), self.handle);
        let right_h = py_to_node_handle_depth(&mut b, other, 0, &SpecPath::fluent())?;
        let h = b.binary_expr(left_h, BinaryOp::Div, right_h);
        Ok(Self {
            arena: b.into_arena(),
            handle: h,
        })
    }

    fn __rtruediv__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        let mut b = QueryBuilder::new();
        let left_h = py_to_node_handle_depth(&mut b, other, 0, &SpecPath::fluent())?;
        let right_h = b.import_subarena(self.arena.clone(), self.handle);
        let h = b.binary_expr(left_h, BinaryOp::Div, right_h);
        Ok(Self {
            arena: b.into_arena(),
            handle: h,
        })
    }

    fn __mod__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        let mut b = QueryBuilder::new();
        let left_h = b.import_subarena(self.arena.clone(), self.handle);
        let right_h = py_to_node_handle_depth(&mut b, other, 0, &SpecPath::fluent())?;
        let h = b.binary_expr(left_h, BinaryOp::Mod, right_h);
        Ok(Self {
            arena: b.into_arena(),
            handle: h,
        })
    }

    fn __rmod__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        let mut b = QueryBuilder::new();
        let left_h = py_to_node_handle_depth(&mut b, other, 0, &SpecPath::fluent())?;
        let right_h = b.import_subarena(self.arena.clone(), self.handle);
        let h = b.binary_expr(left_h, BinaryOp::Mod, right_h);
        Ok(Self {
            arena: b.into_arena(),
            handle: h,
        })
    }

    fn __neg__(&self) -> Self {
        let mut b = QueryBuilder::new();
        let op_h = b.import_subarena(self.arena.clone(), self.handle);
        let h = b.unary_expr(UnaryOp::Neg, op_h);
        Self {
            arena: b.into_arena(),
            handle: h,
        }
    }

    fn __and__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        let mut b = QueryBuilder::new();
        let left_h = b.import_subarena(self.arena.clone(), self.handle);
        let right_h = py_to_node_handle_depth(&mut b, other, 0, &SpecPath::fluent())?;
        let h = b.binary_expr(left_h, BinaryOp::And, right_h);
        Ok(Self {
            arena: b.into_arena(),
            handle: h,
        })
    }

    fn __rand__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        let mut b = QueryBuilder::new();
        let left_h = py_to_node_handle_depth(&mut b, other, 0, &SpecPath::fluent())?;
        let right_h = b.import_subarena(self.arena.clone(), self.handle);
        let h = b.binary_expr(left_h, BinaryOp::And, right_h);
        Ok(Self {
            arena: b.into_arena(),
            handle: h,
        })
    }

    fn __or__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        let mut b = QueryBuilder::new();
        let left_h = b.import_subarena(self.arena.clone(), self.handle);
        let right_h = py_to_node_handle_depth(&mut b, other, 0, &SpecPath::fluent())?;
        let h = b.binary_expr(left_h, BinaryOp::Or, right_h);
        Ok(Self {
            arena: b.into_arena(),
            handle: h,
        })
    }

    fn __ror__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        let mut b = QueryBuilder::new();
        let left_h = py_to_node_handle_depth(&mut b, other, 0, &SpecPath::fluent())?;
        let right_h = b.import_subarena(self.arena.clone(), self.handle);
        let h = b.binary_expr(left_h, BinaryOp::Or, right_h);
        Ok(Self {
            arena: b.into_arena(),
            handle: h,
        })
    }

    fn __xor__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        let mut b = QueryBuilder::new();
        let left_h = b.import_subarena(self.arena.clone(), self.handle);
        let right_h = py_to_node_handle_depth(&mut b, other, 0, &SpecPath::fluent())?;
        let h = b.binary_expr(left_h, BinaryOp::Xor, right_h);
        Ok(Self {
            arena: b.into_arena(),
            handle: h,
        })
    }

    fn __rxor__(&self, other: &Bound<'_, PyAny>) -> PyResult<Self> {
        let mut b = QueryBuilder::new();
        let left_h = py_to_node_handle_depth(&mut b, other, 0, &SpecPath::fluent())?;
        let right_h = b.import_subarena(self.arena.clone(), self.handle);
        let h = b.binary_expr(left_h, BinaryOp::Xor, right_h);
        Ok(Self {
            arena: b.into_arena(),
            handle: h,
        })
    }

    fn __invert__(&self) -> Self {
        let mut b = QueryBuilder::new();
        let op_h = b.import_subarena(self.arena.clone(), self.handle);
        let h = b.unary_expr(UnaryOp::Not, op_h);
        Self {
            arena: b.into_arena(),
            handle: h,
        }
    }

    fn __bool__(&self) -> PyResult<bool> {
        Err(PyTypeError::new_err(
            "Evaluating a Voyager AstExpr in a boolean context is not supported.",
        ))
    }

    fn __richcmp__(
        &self,
        other: &Bound<'_, PyAny>,
        op: pyo3::pyclass::CompareOp,
    ) -> PyResult<Self> {
        let bin_op = match op {
            pyo3::pyclass::CompareOp::Eq => BinaryOp::Eq,
            pyo3::pyclass::CompareOp::Ne => BinaryOp::Neq,
            pyo3::pyclass::CompareOp::Lt => BinaryOp::Lt,
            pyo3::pyclass::CompareOp::Le => BinaryOp::Lte,
            pyo3::pyclass::CompareOp::Gt => BinaryOp::Gt,
            pyo3::pyclass::CompareOp::Ge => BinaryOp::Gte,
        };
        let mut b = QueryBuilder::new();
        let left_h = b.import_subarena(self.arena.clone(), self.handle);
        let right_h = py_to_node_handle_depth(&mut b, other, 0, &SpecPath::fluent())?;
        let h = b.binary_expr(left_h, bin_op, right_h);
        Ok(Self {
            arena: b.into_arena(),
            handle: h,
        })
    }

    fn __repr__(&self) -> String {
        format!("<AstExpr handle={:?}>", self.handle)
    }
}

/// Maximum recursion depth allowed when converting Python expression trees to AST node handles.
const MAX_AST_DEPTH: usize = 256;

fn is_known_expr_tag(tag: &str) -> bool {
    matches!(
        tag,
        "prop"
            | "ident"
            | "param"
            | "lit"
            | "bin"
            | "unary"
            | "fn"
            | "case"
            | "list_comp"
            | "pattern_comp"
            | "exists"
            | "count"
            | "list"
            | "alias"
    )
}

fn py_to_node_handle(builder: &mut QueryBuilder, val: &Bound<'_, PyAny>) -> PyResult<NodeHandle> {
    py_to_node_handle_depth(builder, val, 0, &SpecPath::fluent())
}

#[inline(never)]
fn try_import_native_ast(builder: &mut QueryBuilder, val: &Bound<'_, PyAny>) -> Option<NodeHandle> {
    if let Ok(py_expr) = val.cast::<PyAstExpr>() {
        let borrowed = py_expr.borrow();
        return Some(builder.import_subarena(borrowed.arena.clone(), borrowed.handle));
    }
    if let Ok(native_attr) = val.getattr("_native_expr")
        && let Ok(py_expr) = native_attr.cast::<PyAstExpr>()
    {
        let borrowed = py_expr.borrow();
        return Some(builder.import_subarena(borrowed.arena.clone(), borrowed.handle));
    }
    None
}

#[inline(never)]
fn handle_dict_expr(
    builder: &mut QueryBuilder,
    dict: &Bound<'_, PyDict>,
    depth: usize,
    path: &SpecPath,
) -> PyResult<NodeHandle> {
    if let Some(ver_item) = dict.get_item("spec_version")? {
        let ver_path = path.key("spec_version");
        let ver: u32 = extract_required(&ver_item, &ver_path, "an integer major version")?;
        if ver != voyager_ogm::spec::SPEC_VERSION {
            return Err(make_voyager_err(
                voyager_ogm::ErrorCode::InvalidSpec,
                format!(
                    "Unsupported expression spec_version: {ver}. Supported major versions: {}.",
                    voyager_ogm::spec::SPEC_VERSION
                ),
            ));
        }
    }
    if let Some(rev_item) = dict.get_item("spec_revision")?
        && !rev_item.is_none()
    {
        let rev_path = path.key("spec_revision");
        let rev: u32 = extract_required(&rev_item, &rev_path, "an integer spec revision")?;
        if rev < 1 {
            return Err(path_error(
                &rev_path,
                format!("Invalid expression spec_revision: {rev}. The minimum revision is 1."),
            ));
        }
        if rev > voyager_ogm::spec::SPEC_REVISION {
            return Err(path_error(
                &rev_path,
                format!(
                    "The expression requires Query Spec revision {rev}; this Voyager supports up to revision {}.",
                    voyager_ogm::spec::SPEC_REVISION
                ),
            ));
        }
    }
    if let Some(inner_expr) = dict.get_item("expr")? {
        return py_to_node_handle_depth(builder, &inner_expr, depth + 1, &path.key("expr"));
    }
    // A dict without an `expr` key is a nested query spec in the fluent API.
    // Convert it to JSON and build it through the Rust consumer so the parsing
    // logic exists in one place.
    let nested = py_spec_to_json_value(dict.as_any())?;
    let nested_builder =
        voyager_ogm::spec::build_nested_query(&nested).map_err(spec_error_to_py)?;
    let (nested_arena, nested_handle) = nested_builder.build();
    Ok(builder.import_subarena(nested_arena, nested_handle))
}

/// Reads a Query Spec v1 array position as a sequence of items, accepting both
/// spellings the protocol allows: a Python `list` (the JSON array form, e.g.
/// after `json.loads`/`json.dumps`) and a Python `tuple` (the form the Python
/// SDK emits). The two are interchangeable, so every array position goes
/// through here instead of ad-hoc `PyTuple`/`PyList` casts.
fn as_spec_sequence<'a>(val: &Bound<'a, PyAny>) -> Option<Bound<'a, PyTuple>> {
    if let Ok(tuple) = val.cast::<PyTuple>() {
        Some(tuple.clone())
    } else if let Ok(list) = val.cast::<PyList>() {
        Some(list.to_tuple())
    } else {
        None
    }
}

/// Reads a spec array position whose shape is part of the v1 contract. A
/// non-sequence value fails with `INVALID_SPEC` at `path` (or `INVALID_CLAUSE`
/// for a fluent caller) instead of a bare PyO3 `TypeError`.
fn expect_spec_array<'a>(
    val: &Bound<'a, PyAny>,
    path: &SpecPath,
    field: &str,
) -> PyResult<Bound<'a, PyTuple>> {
    as_spec_sequence(val).ok_or_else(|| {
        path_error(
            path,
            format!(
                "Invalid '{field}': expected a JSON array (list or tuple), got {}.",
                json_type_name(val)
            ),
        )
    })
}

/// Validates the item count of a fixed-arity spec array so a too-short
/// sequence fails with a clear, field-named error instead of the bare
/// `IndexError` a later `get_item` would raise.
fn check_spec_arity(
    tuple: &Bound<'_, PyTuple>,
    path: &SpecPath,
    field: &str,
    min_items: usize,
    shape: &str,
) -> PyResult<()> {
    let len = tuple.len();
    if len < min_items {
        return Err(path_error(
            path,
            format!("Invalid '{field}': expected at least {min_items} item(s) {shape}, got {len}."),
        ));
    }
    Ok(())
}

/// Validates the item count of a fixed-arity spec array against both bounds.
///
/// Every fixed shape in the v1 schema carries `maxItems`; an item past the
/// documented shape is a spec error at the extra item's own path instead of
/// being read and silently dropped. Path steps use the narrower
/// [`reject_extra_step_items`] tolerance, which still accepts trailing `null`
/// placeholders.
fn check_spec_arity_bounded(
    tuple: &Bound<'_, PyTuple>,
    path: &SpecPath,
    field: &str,
    min_items: usize,
    max_items: usize,
    shape: &str,
) -> PyResult<()> {
    check_spec_arity(tuple, path, field, min_items, shape)?;
    if tuple.len() > max_items {
        let extra = tuple.get_item(max_items)?;
        return Err(path_error(
            &path.index(max_items),
            format!(
                "Invalid '{field}': unexpected {} after the documented shape {shape}. This position accepts at most {max_items} item(s).",
                json_type_name(&extra)
            ),
        ));
    }
    Ok(())
}

/// Strips one leading `$` from a parameter name, matching the Python SDK's
/// `param()` helper (`param("$batch")` and `param("batch")` are the same
/// parameter). Exactly one `$` is removed; `"$$x"` still names `"$x"`.
fn strip_leading_dollar(name: String) -> String {
    match name.strip_prefix('$') {
        Some(stripped) => stripped.to_string(),
        None => name,
    }
}

/// Rejects items past a path step's documented shape unless they are `null`.
///
/// A node step documents `[node, variable, labels, properties]` and an edge
/// step documents `[edge, direction, types, variable, min_hops, max_hops,
/// properties]`. Trailing `null` placeholders stay accepted; any other extra
/// item is a spec error rather than being dropped.
fn reject_extra_step_items(
    tuple: &Bound<'_, PyTuple>,
    path: &SpecPath,
    field: &str,
    documented_items: usize,
    shape: &str,
) -> PyResult<()> {
    for idx in documented_items..tuple.len() {
        let item = tuple.get_item(idx)?;
        if !item.is_none() {
            return Err(path_error(
                &path.index(idx),
                format!(
                    "Invalid {field}: unexpected {} after the documented shape {shape}. This path step accepts no further items.",
                    json_type_name(&item)
                ),
            ));
        }
    }
    Ok(())
}

/// Parses a path step's optional properties item into `property -> expression`
/// pairs.
///
/// Omitted and `null` both mean "no properties"; an empty object is treated the
/// same way. A non-object is rejected at the properties item's path, and each
/// value is parsed with the strict tagged-expression rule, so a bare scalar
/// fails at `/.../properties/<name>` with the `["lit", value]` hint.
fn parse_step_properties(
    builder: &mut QueryBuilder,
    tuple: &Bound<'_, PyTuple>,
    idx: usize,
    depth: usize,
    step_path: &SpecPath,
    field: &str,
) -> PyResult<Vec<(String, NodeHandle)>> {
    let Some(item) = optional_step_item(tuple, idx)? else {
        return Ok(Vec::new());
    };
    if item.is_none() {
        return Ok(Vec::new());
    }
    let properties_path = step_path.index(idx);
    let dict = item.cast::<PyDict>().map_err(|_| {
        path_error(
            &properties_path,
            format!(
                "Invalid {field} properties: expected a JSON object mapping property names to expressions, or null, got {}.",
                json_type_name(&item)
            ),
        )
    })?;
    let mut properties = Vec::with_capacity(dict.len());
    for (key, value) in dict.iter() {
        let name: String = key.extract().map_err(|_| {
            path_error(
                &properties_path,
                format!(
                    "Invalid {field} properties: property names must be strings, got {}.",
                    json_type_name(&key)
                ),
            )
        })?;
        let value_path = properties_path.key(&name);
        let value_handle = py_to_node_handle_depth(builder, &value, depth, &value_path)?;
        properties.push((name, value_handle));
    }
    Ok(properties)
}

/// Returns item `idx` of a path step, or `None` when the step is shorter.
fn optional_step_item<'py>(
    tuple: &Bound<'py, PyTuple>,
    idx: usize,
) -> PyResult<Option<Bound<'py, PyAny>>> {
    if idx < tuple.len() {
        Ok(Some(tuple.get_item(idx)?))
    } else {
        Ok(None)
    }
}

#[inline(never)]
fn handle_prop_tuple(
    builder: &mut QueryBuilder,
    tuple: &Bound<'_, PyTuple>,
    path: &SpecPath,
) -> PyResult<NodeHandle> {
    check_spec_arity_bounded(
        tuple,
        path,
        "expression 'prop'",
        3,
        3,
        "[prop, variable, property]",
    )?;
    let var: String = extract_required(&tuple.get_item(1)?, &path.index(1), "a string variable")?;
    if var.is_empty() {
        return Err(path_error(
            &path.index(1),
            "Invalid 'prop': the variable must be a non-empty string.",
        ));
    }
    let prop: String = extract_required(&tuple.get_item(2)?, &path.index(2), "a string property")?;
    Ok(builder.prop(var, prop))
}

#[inline(never)]
fn handle_ident_tuple(
    builder: &mut QueryBuilder,
    tuple: &Bound<'_, PyTuple>,
    path: &SpecPath,
) -> PyResult<NodeHandle> {
    check_spec_arity_bounded(tuple, path, "expression 'ident'", 2, 2, "[ident, name]")?;
    let name: String = extract_required(&tuple.get_item(1)?, &path.index(1), "a string name")?;
    if name.is_empty() {
        return Err(path_error(
            &path.index(1),
            "Invalid 'ident': the identifier must be a non-empty string.",
        ));
    }
    Ok(builder.ident(name))
}

#[inline(never)]
fn handle_param_tuple(
    builder: &mut QueryBuilder,
    tuple: &Bound<'_, PyTuple>,
    path: &SpecPath,
) -> PyResult<NodeHandle> {
    check_spec_arity_bounded(tuple, path, "expression 'param'", 2, 2, "[param, name]")?;
    let name: String = extract_required(&tuple.get_item(1)?, &path.index(1), "a string name")?;
    let name = strip_leading_dollar(name);
    if name.is_empty() {
        return Err(path_error(
            &path.index(1),
            "Invalid 'param': the parameter name must be a non-empty string.",
        ));
    }
    Ok(builder.param(name))
}

#[inline(never)]
fn handle_lit_tuple(
    builder: &mut QueryBuilder,
    tuple: &Bound<'_, PyTuple>,
    path: &SpecPath,
) -> PyResult<NodeHandle> {
    check_spec_arity_bounded(tuple, path, "expression 'lit'", 2, 2, "[lit, value]")?;
    let value_path = path.index(1);
    let lit = py_to_literal(&tuple.get_item(1)?)
        .map_err(|e| path_value_error(&value_path, e.to_string()))?;
    Ok(builder.literal(lit))
}

#[inline(never)]
fn handle_fn_tuple(
    builder: &mut QueryBuilder,
    tuple: &Bound<'_, PyTuple>,
    depth: usize,
    path: &SpecPath,
) -> PyResult<NodeHandle> {
    check_spec_arity_bounded(tuple, path, "expression 'fn'", 3, 3, "[fn, name, args]")?;
    let name: String = extract_required(&tuple.get_item(1)?, &path.index(1), "a string name")?;
    let args_path = path.index(2);
    let args_seq = expect_spec_array(&tuple.get_item(2)?, &args_path, "fn args")?;
    let mut arg_handles = Vec::with_capacity(args_seq.len());
    for (idx, arg) in args_seq.iter().enumerate() {
        arg_handles.push(py_to_node_handle_depth(
            builder,
            &arg,
            depth + 1,
            &args_path.index(idx),
        )?);
    }
    Ok(builder.function(name, arg_handles))
}

#[inline(never)]
fn handle_case_tuple(
    builder: &mut QueryBuilder,
    tuple: &Bound<'_, PyTuple>,
    depth: usize,
    path: &SpecPath,
) -> PyResult<NodeHandle> {
    check_spec_arity_bounded(
        tuple,
        path,
        "expression 'case'",
        4,
        4,
        "[case, operand, branches, else_branch]",
    )?;
    let op_item = tuple.get_item(1)?;
    let operand = if op_item.is_none() {
        None
    } else {
        Some(py_to_node_handle_depth(
            builder,
            &op_item,
            depth + 1,
            &path.index(1),
        )?)
    };
    let branches_path = path.index(2);
    let branches_seq = expect_spec_array(&tuple.get_item(2)?, &branches_path, "case branches")?;
    let mut branches = Vec::with_capacity(branches_seq.len());
    for (idx, item) in branches_seq.iter().enumerate() {
        let branch_path = branches_path.index(idx);
        let branch = expect_spec_array(&item, &branch_path, "case branch")?;
        check_spec_arity_bounded(&branch, &branch_path, "case branch", 2, 2, "[when, then]")?;
        let w = py_to_node_handle_depth(
            builder,
            &branch.get_item(0)?,
            depth + 1,
            &branch_path.index(0),
        )?;
        let t = py_to_node_handle_depth(
            builder,
            &branch.get_item(1)?,
            depth + 1,
            &branch_path.index(1),
        )?;
        branches.push((w, t));
    }
    let else_item = tuple.get_item(3)?;
    let else_branch = if else_item.is_none() {
        None
    } else {
        Some(py_to_node_handle_depth(
            builder,
            &else_item,
            depth + 1,
            &path.index(3),
        )?)
    };
    Ok(builder.case_when(operand, branches, else_branch))
}

#[inline(never)]
fn handle_list_comp_tuple(
    builder: &mut QueryBuilder,
    tuple: &Bound<'_, PyTuple>,
    depth: usize,
    path: &SpecPath,
) -> PyResult<NodeHandle> {
    check_spec_arity_bounded(
        tuple,
        path,
        "expression 'list_comp'",
        5,
        5,
        "[list_comp, variable, list_expr, where_filter, map_expr]",
    )?;
    let var: String = extract_required(&tuple.get_item(1)?, &path.index(1), "a string variable")?;
    let list_h = py_to_node_handle_depth(builder, &tuple.get_item(2)?, depth + 1, &path.index(2))?;
    let wh_item = tuple.get_item(3)?;
    let where_filter = if wh_item.is_none() {
        None
    } else {
        Some(py_to_node_handle_depth(
            builder,
            &wh_item,
            depth + 1,
            &path.index(3),
        )?)
    };
    let map_item = tuple.get_item(4)?;
    let map_expr = if map_item.is_none() {
        None
    } else {
        Some(py_to_node_handle_depth(
            builder,
            &map_item,
            depth + 1,
            &path.index(4),
        )?)
    };
    Ok(builder.list_comprehension(var, list_h, where_filter, map_expr))
}

#[inline(never)]
fn handle_pattern_comp_tuple(
    builder: &mut QueryBuilder,
    tuple: &Bound<'_, PyTuple>,
    depth: usize,
    path: &SpecPath,
) -> PyResult<NodeHandle> {
    check_spec_arity_bounded(
        tuple,
        path,
        "expression 'pattern_comp'",
        4,
        4,
        "[pattern_comp, path_spec, where_filter, proj_expr]",
    )?;
    let path_spec = tuple.get_item(1)?;
    let path_h = build_path_from_py(builder, &path_spec, depth + 1, &path.index(1))?;
    let wh_item = tuple.get_item(2)?;
    let where_filter = if wh_item.is_none() {
        None
    } else {
        Some(py_to_node_handle_depth(
            builder,
            &wh_item,
            depth + 1,
            &path.index(2),
        )?)
    };
    let proj_h = py_to_node_handle_depth(builder, &tuple.get_item(3)?, depth + 1, &path.index(3))?;
    Ok(builder.pattern_comprehension(path_h, where_filter, proj_h))
}

#[inline(never)]
fn handle_exists_tuple(
    builder: &mut QueryBuilder,
    tuple: &Bound<'_, PyTuple>,
    depth: usize,
    path: &SpecPath,
) -> PyResult<NodeHandle> {
    check_spec_arity_bounded(
        tuple,
        path,
        "expression 'exists'",
        2,
        2,
        "[exists, subquery_spec]",
    )?;
    let sub_spec = tuple.get_item(1)?;
    let sub_h = build_subquery_from_py(builder, &sub_spec, depth + 1, &path.index(1))?;
    Ok(builder.exists_subquery(sub_h))
}

#[inline(never)]
fn handle_count_tuple(
    builder: &mut QueryBuilder,
    tuple: &Bound<'_, PyTuple>,
    depth: usize,
    path: &SpecPath,
) -> PyResult<NodeHandle> {
    check_spec_arity_bounded(
        tuple,
        path,
        "expression 'count'",
        2,
        2,
        "[count, subquery_spec]",
    )?;
    let sub_spec = tuple.get_item(1)?;
    let sub_h = build_subquery_from_py(builder, &sub_spec, depth + 1, &path.index(1))?;
    Ok(builder.count_subquery(sub_h))
}

#[inline(never)]
fn handle_list_literal_tuple(
    builder: &mut QueryBuilder,
    tuple: &Bound<'_, PyTuple>,
    depth: usize,
    path: &SpecPath,
) -> PyResult<NodeHandle> {
    check_spec_arity_bounded(tuple, path, "expression 'list'", 2, 2, "[list, items]")?;
    let items_path = path.index(1);
    let items_seq = expect_spec_array(&tuple.get_item(1)?, &items_path, "list items")?;
    let mut item_handles = Vec::with_capacity(items_seq.len());
    for (idx, item) in items_seq.iter().enumerate() {
        item_handles.push(py_to_node_handle_depth(
            builder,
            &item,
            depth + 1,
            &items_path.index(idx),
        )?);
    }
    Ok(builder.list_literal(item_handles))
}

#[inline(never)]
fn handle_literal_fallback(
    builder: &mut QueryBuilder,
    val: &Bound<'_, PyAny>,
    _path: &SpecPath,
) -> PyResult<NodeHandle> {
    if let Ok(lit) = py_to_literal(val) {
        Ok(builder.literal(lit))
    } else {
        let msg = format!(
            "Cannot convert Python object to AST node handle: {}",
            json_type_name(val)
        );
        Err(make_voyager_err(voyager_ogm::ErrorCode::InvalidSpec, msg))
    }
}

fn py_to_node_handle_depth(
    builder: &mut QueryBuilder,
    val: &Bound<'_, PyAny>,
    depth: usize,
    path: &SpecPath,
) -> PyResult<NodeHandle> {
    if depth > MAX_AST_DEPTH {
        return Err(path_value_error(
            path,
            format!(
                "Maximum AST expression recursion depth exceeded ({MAX_AST_DEPTH}). Query is too deeply nested.",
            ),
        ));
    }
    // Direct Native AST Expression fast-path:
    if let Some(h) = try_import_native_ast(builder, val) {
        return Ok(h);
    }
    if let Ok(dict) = val.cast::<PyDict>() {
        return handle_dict_expr(builder, dict, depth, path);
    }
    // In an expression position a sequence is always a tagged expression: there
    // is no implicit literal-list spelling. Lists and tuples are accepted
    // interchangeably, but the first item must be a known expression tag.
    if let Some(tuple) = as_spec_sequence(val) {
        return handle_expr_sequence(builder, tuple, depth, path);
    }
    handle_literal_fallback(builder, val, path)
}

/// Suggestion appended to expression-array errors, naming the two explicit
/// spellings for a list value.
const EXPRESSION_ARRAY_HINT: &str =
    "Use [\"lit\", [...]] for a literal list or [\"list\", [...]] for a list of expressions.";

/// Validates the first item of an expression sequence and returns its tag.
///
/// A JSON array in an expression position is never a literal list: it must be a
/// tagged expression descriptor, so the first item must be a known expression
/// tag. Empty sequences, non-string first items and unknown tags are rejected at
/// the sequence's own JSON Pointer.
fn expression_tag(tuple: &Bound<'_, PyTuple>, path: &SpecPath) -> PyResult<String> {
    if tuple.is_empty() {
        return Err(path_error(
            path,
            format!(
                "Empty expression array: an expression array must start with a known expression tag. {}",
                EXPRESSION_ARRAY_HINT
            ),
        ));
    }
    let first = tuple.get_item(0)?;
    let tag = match first.extract::<String>() {
        Ok(tag) => tag,
        Err(_) => {
            let type_name = json_type_name(&first);
            let rendered = match render_json_scalar(&first) {
                Some(value) => format!("got {value} ({type_name})"),
                None => format!("got {type_name}"),
            };
            return Err(path_error(
                path,
                format!(
                    "Invalid expression array: the first element must be a known expression tag string, {rendered}. {}",
                    EXPRESSION_ARRAY_HINT
                ),
            ));
        }
    };
    if tag == "agg" {
        return Err(path_error(
            path,
            "Invalid expression 'agg': aggregations are only valid as projection items in 'projections' or 'with_clauses[i].projections', not in where, order_by, set, function arguments or other expression positions. Write the aggregate as [\"agg\", variable, property, func, alias] in a projection list instead.",
        ));
    }
    if !is_known_expr_tag(&tag) {
        return Err(path_error(
            path,
            format!(
                "Unknown expression tag '{tag}': the first element of an expression array must be a known expression tag. {}",
                EXPRESSION_ARRAY_HINT
            ),
        ));
    }
    Ok(tag)
}

#[inline(never)]
fn handle_expr_sequence(
    builder: &mut QueryBuilder,
    tuple: Bound<'_, PyTuple>,
    depth: usize,
    path: &SpecPath,
) -> PyResult<NodeHandle> {
    let tag = expression_tag(&tuple, path)?;
    match tag.as_str() {
        "prop" => handle_prop_tuple(builder, &tuple, path),
        "ident" => handle_ident_tuple(builder, &tuple, path),
        "param" => handle_param_tuple(builder, &tuple, path),
        "lit" => handle_lit_tuple(builder, &tuple, path),
        "bin" => {
            check_spec_arity_bounded(
                &tuple,
                path,
                "expression 'bin'",
                4,
                4,
                "[bin, operator, left, right]",
            )?;
            let op_path = path.index(1);
            let op_str: String =
                extract_required(&tuple.get_item(1)?, &op_path, "a string operator")?;
            let op =
                parse_binary_op(&op_str).map_err(|e| path_value_error(&op_path, e.to_string()))?;
            let left =
                py_to_node_handle_depth(builder, &tuple.get_item(2)?, depth + 1, &path.index(2))?;
            let right =
                py_to_node_handle_depth(builder, &tuple.get_item(3)?, depth + 1, &path.index(3))?;
            Ok(builder.binary_expr(left, op, right))
        }
        "unary" => {
            check_spec_arity_bounded(
                &tuple,
                path,
                "expression 'unary'",
                3,
                3,
                "[unary, operator, operand]",
            )?;
            let op_path = path.index(1);
            let op_str: String =
                extract_required(&tuple.get_item(1)?, &op_path, "a string operator")?;
            let op =
                parse_unary_op(&op_str).map_err(|e| path_value_error(&op_path, e.to_string()))?;
            let operand =
                py_to_node_handle_depth(builder, &tuple.get_item(2)?, depth + 1, &path.index(2))?;
            Ok(builder.unary_expr(op, operand))
        }
        "fn" => handle_fn_tuple(builder, &tuple, depth, path),
        "case" => handle_case_tuple(builder, &tuple, depth, path),
        "list_comp" => handle_list_comp_tuple(builder, &tuple, depth, path),
        "pattern_comp" => handle_pattern_comp_tuple(builder, &tuple, depth, path),
        "exists" => handle_exists_tuple(builder, &tuple, depth, path),
        "count" => handle_count_tuple(builder, &tuple, depth, path),
        "list" => handle_list_literal_tuple(builder, &tuple, depth, path),
        "alias" => {
            check_spec_arity_bounded(
                &tuple,
                path,
                "expression 'alias'",
                3,
                3,
                "[alias, expression, name]",
            )?;
            let _: String =
                extract_required(&tuple.get_item(2)?, &path.index(2), "a string alias name")?;
            py_to_node_handle_depth(builder, &tuple.get_item(1)?, depth + 1, &path.index(1))
        }
        other => Err(path_error(
            path,
            format!(
                "Unknown expression tag '{other}': the first element of an expression array must be a known expression tag. {}",
                EXPRESSION_ARRAY_HINT
            ),
        )),
    }
}

fn parse_direction(dir_str: &str) -> PyResult<Direction> {
    match dir_str {
        "out" | "outgoing" | "->" => Ok(Direction::Outgoing),
        "in" | "incoming" | "<-" => Ok(Direction::Incoming),
        "undirected" | "both" | "-" | "--" => Ok(Direction::Undirected),
        other => Err(make_voyager_err(
            voyager_ogm::ErrorCode::InvalidClause,
            format!(
                "Invalid edge direction '{other}'. Expected 'outgoing' ('out', '->'), 'incoming' ('in', '<-'), or 'undirected' ('both', '-', '--')."
            ),
        )),
    }
}

fn relationship_spec_string(dict: &Bound<'_, PyDict>, key: &str) -> PyResult<String> {
    dict.get_item(key)?
        .ok_or_else(|| PyValueError::new_err(format!("Missing required key '{key}'")))?
        .extract()
}

fn relationship_spec_strings(dict: &Bound<'_, PyDict>, key: &str) -> PyResult<Vec<String>> {
    dict.get_item(key)?
        .ok_or_else(|| PyValueError::new_err(format!("Missing required key '{key}'")))?
        .extract()
}

fn relationship_spec_optional_string(
    dict: &Bound<'_, PyDict>,
    key: &str,
) -> PyResult<Option<String>> {
    Ok(dict.get_item(key)?.and_then(|value| {
        if value.is_none() {
            None
        } else {
            value.extract().ok()
        }
    }))
}

fn relationship_spec_endpoint(
    dict: &Bound<'_, PyDict>,
    prefix: &str,
    require_key_value: bool,
) -> PyResult<RelationshipEndpoint> {
    let variable = relationship_spec_string(dict, prefix)?;
    let labels = relationship_spec_strings(dict, &format!("{prefix}_labels"))?;
    let key = if require_key_value {
        relationship_spec_string(dict, &format!("{prefix}_key"))?
    } else {
        relationship_spec_optional_string(dict, &format!("{prefix}_key"))?.unwrap_or_default()
    };
    let key_value = if require_key_value {
        let raw = dict
            .get_item(format!("{prefix}_key_value"))?
            .ok_or_else(|| {
                PyValueError::new_err(format!("Missing required key '{prefix}_key_value'"))
            })?;
        py_to_literal(&raw)?
    } else {
        LiteralValue::Null
    };
    Ok(RelationshipEndpoint {
        variable,
        labels,
        key,
        key_value,
    })
}

fn relationship_spec_properties(dict: &Bound<'_, PyDict>) -> PyResult<Vec<(String, LiteralValue)>> {
    let Some(properties) = dict.get_item("properties")? else {
        return Ok(Vec::new());
    };
    if properties.is_none() {
        return Ok(Vec::new());
    }
    let properties = properties.extract::<Vec<(String, Bound<'_, PyAny>)>>()?;
    optional_property_list_to_literals(Some(properties))
}

fn build_path_steps_into_builder(
    builder: &mut QueryBuilder,
    steps: &Bound<'_, PyTuple>,
    depth: usize,
    path: &SpecPath,
) -> PyResult<()> {
    for (idx, step) in steps.iter().enumerate() {
        let step_path = path.index(idx);
        let tuple = expect_spec_array(&step, &step_path, "path step")?;
        let tag_path = step_path.index(0);
        let tag: String =
            extract_required(&tuple.get_item(0)?, &tag_path, "a string path step tag")?;
        match tag.as_str() {
            "node" => {
                check_spec_arity(
                    &tuple,
                    &step_path,
                    "path step 'node'",
                    3,
                    "[node, variable, labels, properties?]",
                )?;
                reject_extra_step_items(
                    &tuple,
                    &step_path,
                    "path step 'node'",
                    4,
                    "[node, variable, labels, properties]",
                )?;
                let var: Option<String> = extract_required(
                    &tuple.get_item(1)?,
                    &step_path.index(1),
                    "a string or null variable",
                )?;
                let labels: Vec<String> = extract_required(
                    &tuple.get_item(2)?,
                    &step_path.index(2),
                    "an array of string labels",
                )?;
                let properties = parse_step_properties(
                    builder,
                    &tuple,
                    3,
                    depth,
                    &step_path,
                    "path step 'node'",
                )?;
                if properties.is_empty() {
                    builder.node(var, labels);
                } else {
                    builder.node_with_prop_exprs(var, labels, properties);
                }
            }
            "edge" => {
                check_spec_arity(
                    &tuple,
                    &step_path,
                    "path step 'edge'",
                    3,
                    "[edge, direction, types, variable?, min_hops?, max_hops?, properties?]",
                )?;
                reject_extra_step_items(
                    &tuple,
                    &step_path,
                    "path step 'edge'",
                    7,
                    "[edge, direction, types, variable, min_hops, max_hops, properties]",
                )?;
                let dir_path = step_path.index(1);
                let dir_str: String =
                    extract_required(&tuple.get_item(1)?, &dir_path, "a string edge direction")?;
                let dir =
                    parse_direction(&dir_str).map_err(|e| path_error(&dir_path, e.to_string()))?;
                let edge_types: Vec<String> = extract_required(
                    &tuple.get_item(2)?,
                    &step_path.index(2),
                    "an array of string edge types",
                )?;
                let var: Option<String> = match optional_step_item(&tuple, 3)? {
                    Some(item) => {
                        extract_required(&item, &step_path.index(3), "a string or null variable")?
                    }
                    None => None,
                };
                let min_hops: Option<u32> = match optional_step_item(&tuple, 4)? {
                    Some(item) => extract_required(
                        &item,
                        &step_path.index(4),
                        "a non-negative integer or null",
                    )?,
                    None => None,
                };
                let max_hops: Option<Result<Option<u32>, ()>> = match optional_step_item(&tuple, 5)?
                {
                    Some(item) => {
                        if item.is_none() {
                            Some(Ok(None))
                        } else if let Ok(s) = item.extract::<String>() {
                            if s == "unbounded" {
                                Some(Err(()))
                            } else {
                                return Err(PyTypeError::new_err(format!(
                                    "expected a non-negative integer, null, or 'unbounded', got {}",
                                    json_type_name(&item)
                                )));
                            }
                        } else if let Ok(n) = item.extract::<u32>() {
                            Some(Ok(Some(n)))
                        } else {
                            return Err(PyTypeError::new_err(format!(
                                "expected a non-negative integer, null, or 'unbounded', got {}",
                                json_type_name(&item)
                            )));
                        }
                    }
                    None => None,
                };
                let properties = parse_step_properties(
                    builder,
                    &tuple,
                    6,
                    depth,
                    &step_path,
                    "path step 'edge'",
                )?;
                match (dir, properties.is_empty()) {
                    (Direction::Outgoing, true) => builder.to(edge_types, var),
                    (Direction::Incoming, true) => builder.from(edge_types, var),
                    (Direction::Undirected, true) => builder.edge(edge_types, var),
                    (Direction::Outgoing, false) => {
                        builder.to_with_prop_exprs(edge_types, var, properties)
                    }
                    (Direction::Incoming, false) => {
                        builder.from_with_prop_exprs(edge_types, var, properties)
                    }
                    (Direction::Undirected, false) => {
                        builder.edge_with_prop_exprs(edge_types, var, properties)
                    }
                };
                match (min_hops, max_hops) {
                    (Some(min), Some(Ok(Some(max)))) => {
                        builder.hops_opt(Some(min), Some(max));
                    }
                    (Some(min), Some(Err(()))) => {
                        builder.hops_opt(Some(min), None);
                    }
                    (Some(min), Some(Ok(None)) | None) => {
                        builder.hops_opt(Some(min), Some(min));
                    }
                    (None, Some(Ok(Some(max)))) => {
                        builder.hops_opt(Some(1), Some(max));
                    }
                    (None, Some(Err(()))) => {
                        builder.hops_opt(Some(1), None);
                    }
                    (None, Some(Ok(None)) | None) => {}
                }
            }
            other => {
                return Err(path_error(
                    &tag_path,
                    format!("Unknown path step tag: '{other}'. Expected 'node' or 'edge'."),
                ));
            }
        }
    }
    Ok(())
}

fn build_path_from_py(
    builder: &mut QueryBuilder,
    spec: &Bound<'_, PyAny>,
    depth: usize,
    path: &SpecPath,
) -> PyResult<NodeHandle> {
    let target = if let Ok(native) = spec.getattr("_native") {
        native
    } else {
        spec.clone()
    };
    if let Ok(py_builder) = target.extract::<PyRef<PyQueryBuilder>>() {
        let sub = py_builder.inner.clone();
        let (sub_arena, sub_handle) = sub.build();
        let sub_h = builder.import_subarena(sub_arena, sub_handle);
        return Ok(sub_h);
    }
    if let Some(steps) = as_spec_sequence(spec) {
        let mut err: Option<PyErr> = None;
        let sub_h = builder.subquery(|q| {
            if let Err(e) = build_path_steps_into_builder(q, &steps, depth, path) {
                err = Some(e);
            }
        });
        if let Some(e) = err {
            return Err(e);
        }
        Ok(sub_h)
    } else {
        py_to_node_handle_depth(builder, spec, depth, path)
    }
}

fn build_subquery_from_py(
    builder: &mut QueryBuilder,
    spec: &Bound<'_, PyAny>,
    depth: usize,
    path: &SpecPath,
) -> PyResult<NodeHandle> {
    let target = if let Ok(native) = spec.getattr("_native") {
        native
    } else {
        spec.clone()
    };
    if let Ok(py_builder) = target.extract::<PyRef<PyQueryBuilder>>() {
        let sub = py_builder.inner.clone();
        let (sub_arena, sub_handle) = sub.build();
        let sub_h = builder.import_subarena(sub_arena, sub_handle);
        return Ok(sub_h);
    }
    if let Ok(dict) = spec.cast::<PyDict>() {
        let nested = py_spec_to_json_value(dict.as_any())?;
        let nested_builder =
            voyager_ogm::spec::build_nested_query(&nested).map_err(spec_error_to_py)?;
        let (nested_arena, nested_handle) = nested_builder.build();
        Ok(builder.import_subarena(nested_arena, nested_handle))
    } else if let Some(steps) = as_spec_sequence(spec) {
        let mut err: Option<PyErr> = None;
        let sub_h = builder.subquery(|q| {
            q.r#match();
            if let Err(e) = build_path_steps_into_builder(q, &steps, depth, path) {
                err = Some(e);
            }
        });
        if let Some(e) = err {
            return Err(e);
        }
        Ok(sub_h)
    } else {
        py_to_node_handle_depth(builder, spec, depth, path)
    }
}

fn literal_to_py<'py>(lit: &LiteralValue, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
    match lit {
        LiteralValue::Null => Ok(py.None().into_bound(py)),
        LiteralValue::Bool(b) => Ok(b.into_pyobject(py)?.to_owned().into_any()),
        LiteralValue::Int64(i) => Ok(i.into_pyobject(py)?.to_owned().into_any()),
        LiteralValue::Float64(f) => Ok(f.into_pyobject(py)?.to_owned().into_any()),
        LiteralValue::String(s) => Ok(s.into_pyobject(py)?.to_owned().into_any()),
        LiteralValue::ParameterRef(p) => Ok(p.into_pyobject(py)?.to_owned().into_any()),
        LiteralValue::List(l) => {
            let list = PyList::empty(py);
            for item in l {
                list.append(literal_to_py(item, py)?)?;
            }
            Ok(list.into_any())
        }
        LiteralValue::Map(m) => {
            let dict = PyDict::new(py);
            for (k, v) in m {
                dict.set_item(k, literal_to_py(v, py)?)?;
            }
            Ok(dict.into_any())
        }
    }
}

/// Converts a Query Spec value into its Python representation.
///
/// Objects are inserted in order so a pattern property map keeps the order the
/// emitters use when they assign parameter names.
fn spec_value_to_py<'py>(py: Python<'py>, value: &SpecValue) -> PyResult<Bound<'py, PyAny>> {
    match value {
        SpecValue::Null => Ok(py.None().into_bound(py)),
        SpecValue::Bool(value) => Ok(value.into_pyobject(py)?.to_owned().into_any()),
        SpecValue::Int(value) => Ok(value.into_pyobject(py)?.to_owned().into_any()),
        SpecValue::Float(value) => Ok(value.into_pyobject(py)?.to_owned().into_any()),
        SpecValue::String(value) => Ok(value.into_pyobject(py)?.to_owned().into_any()),
        SpecValue::Array(items) => {
            let list = PyList::empty(py);
            for item in items {
                list.append(spec_value_to_py(py, item)?)?;
            }
            Ok(list.into_any())
        }
        SpecValue::Object(members) => {
            let dict = PyDict::new(py);
            for (key, value) in members {
                dict.set_item(key, spec_value_to_py(py, value)?)?;
            }
            Ok(dict.into_any())
        }
    }
}

/// Maps a Query Spec serialisation failure onto a `VoyagerError`.
fn map_spec_export_error(error: SpecExportError) -> PyErr {
    match error {
        SpecExportError::Unsupported { construct } => make_voyager_err(
            voyager_ogm::ErrorCode::UnsupportedFeature,
            format!("Query Spec v1 cannot represent {construct} without changing the query"),
        ),
        SpecExportError::Invalid(message) => {
            make_voyager_err(voyager_ogm::ErrorCode::InvalidClause, message)
        }
    }
}

fn check_warn_cypher_path_mode(
    py: Python<'_>,
    dialect: &str,
    path_mode: Option<&str>,
) -> PyResult<()> {
    if matches!(
        dialect.trim().to_ascii_lowercase().as_str(),
        "cypher" | "opencypher" | "neo4j" | "memgraph"
    ) && let Some(mode) = path_mode
        && !mode.is_empty()
        && !mode.eq_ignore_ascii_case("none")
    {
        let msg = format!(
            "Dialect '{dialect}' does not support explicit path search modes ('{mode}'). Cypher default traversal semantics (trail) will be used and the search mode keyword was omitted."
        );
        let warnings = py.import("warnings")?;
        let user_warning = py.get_type::<PyUserWarning>();
        warnings.call_method1("warn", (msg, user_warning))?;
    }
    Ok(())
}

fn topology_to_py_dict<'py>(
    topology: &GraphTopology,
    py: Python<'py>,
) -> PyResult<Bound<'py, PyDict>> {
    let result = PyDict::new(py);
    result.set_item("version", topology.version)?;
    let nodes_list = PyList::empty(py);
    let edges_list = PyList::empty(py);

    for node in &topology.nodes {
        let node_dict = PyDict::new(py);
        node_dict.set_item("id", &node.id)?;
        node_dict.set_item("label", &node.label)?;
        let labels_list = PyList::empty(py);
        for l in &node.labels {
            labels_list.append(l)?;
        }
        node_dict.set_item("labels", labels_list)?;
        node_dict.set_item("group", &node.group)?;
        node_dict.set_item("size", node.size)?;

        let props_dict = PyDict::new(py);
        for (k, v) in &node.properties {
            props_dict.set_item(k, literal_to_py(v, py)?)?;
        }
        node_dict.set_item("properties", props_dict)?;

        let data_dict = PyDict::new(py);
        for (k, v) in &node.data {
            data_dict.set_item(k, literal_to_py(v, py)?)?;
        }
        node_dict.set_item("data", data_dict)?;

        nodes_list.append(node_dict)?;
    }

    for edge in &topology.edges {
        let edge_dict = PyDict::new(py);
        edge_dict.set_item("id", &edge.id)?;
        edge_dict.set_item("source", &edge.source)?;
        edge_dict.set_item("target", &edge.target)?;
        edge_dict.set_item("label", &edge.label)?;
        let types_list = PyList::empty(py);
        for t in &edge.types {
            types_list.append(t)?;
        }
        edge_dict.set_item("types", types_list)?;
        let dir_str = match edge.direction {
            Direction::Outgoing => "outgoing",
            Direction::Incoming => "incoming",
            Direction::Undirected => "undirected",
        };
        edge_dict.set_item("direction", dir_str)?;
        edge_dict.set_item("color", &edge.color)?;
        edge_dict.set_item("min_hops", edge.min_hops)?;
        edge_dict.set_item("max_hops", edge.max_hops)?;

        let props_dict = PyDict::new(py);
        for (k, v) in &edge.properties {
            props_dict.set_item(k, literal_to_py(v, py)?)?;
        }
        edge_dict.set_item("properties", props_dict)?;

        let data_dict = PyDict::new(py);
        for (k, v) in &edge.data {
            data_dict.set_item(k, literal_to_py(v, py)?)?;
        }
        edge_dict.set_item("data", data_dict)?;

        edges_list.append(edge_dict)?;
    }

    result.set_item("nodes", nodes_list)?;
    result.set_item("edges", edges_list)?;
    Ok(result)
}

/// Extracts graph node and edge topology from a raw query string without external parsers.
#[pyfunction]
fn extract_topology_from_query<'py>(query: &str, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
    let topology = GraphTopology::from_query_str(query);
    topology_to_py_dict(&topology, py)
}

fn conformance_report_to_py_dict<'py>(
    report: &ConformanceReport,
    py: Python<'py>,
) -> PyResult<Bound<'py, PyDict>> {
    let result = PyDict::new(py);
    result.set_item("is_valid", report.is_valid)?;

    let diags_list = PyList::empty(py);
    for diag in &report.diagnostics {
        let d = PyDict::new(py);
        d.set_item("severity", diag.severity.as_str())?;
        d.set_item("code", &diag.code)?;
        d.set_item("message", &diag.message)?;
        d.set_item("entity_id", &diag.entity_id)?;
        d.set_item("suggestion", &diag.suggestion)?;
        diags_list.append(d)?;
    }
    result.set_item("diagnostics", diags_list)?;

    let errors_list = PyList::empty(py);
    for err in report.error_messages() {
        errors_list.append(err)?;
    }
    result.set_item("errors", errors_list)?;

    let warnings_list = PyList::empty(py);
    for warn in report.warning_messages() {
        warnings_list.append(warn)?;
    }
    result.set_item("warnings", warnings_list)?;

    Ok(result)
}

/// Validates a raw query string against the schema registry.
#[pyfunction]
#[pyo3(signature = (query, registry=None))]
fn validate_query_against_schema<'py>(
    query: &str,
    registry: Option<&PyNativeSchemaRegistry>,
    py: Python<'py>,
) -> PyResult<Bound<'py, PyDict>> {
    let topology = GraphTopology::from_query_str(query);
    let reg = match registry {
        Some(r) => r.registry(),
        None => global_schema_registry(),
    };
    let report = reg.validate_topology(&topology);
    conformance_report_to_py_dict(&report, py)
}

/// Native Rust Query Builder exposed to Python.
#[pyclass(name = "NativeQueryBuilder", from_py_object)]
#[derive(Default, Clone)]
pub struct PyQueryBuilder {
    inner: QueryBuilder,
}

#[pymethods]
impl PyQueryBuilder {
    #[new]
    fn new() -> Self {
        Self {
            inner: QueryBuilder::new(),
        }
    }

    fn r#match(&mut self) {
        self.inner.r#match();
    }

    fn optional_match(&mut self) {
        self.inner.optional_match();
    }

    fn pattern(&mut self) {
        self.inner.pattern();
    }

    fn has_mutations(&self) -> bool {
        self.inner.has_mutations()
    }

    fn explain(&mut self) {
        self.inner.explain();
    }

    fn profile(&mut self) {
        self.inner.profile();
    }

    fn execution_mode(&self) -> &'static str {
        self.inner.execution_mode().as_str()
    }

    fn clause_mode(&self) -> Option<&'static str> {
        self.inner.clause_mode()
    }

    fn last_merged_variable(&self) -> Option<String> {
        self.inner.last_merged_variable()
    }

    fn is_variable_declared(&self, var: &str) -> bool {
        self.inner.is_variable_declared(var)
    }

    fn find_variables_by_labels(&self, labels: Vec<String>) -> Vec<String> {
        self.inner.find_variables_by_labels(&labels)
    }

    fn clone_builder(&self) -> Self {
        self.clone()
    }

    fn __copy__(&self) -> Self {
        self.clone()
    }

    fn __deepcopy__(&self, _memo: &Bound<'_, PyAny>) -> Self {
        self.clone()
    }

    #[pyo3(signature = (other, seen_vars=None))]
    fn import_match_patterns(
        &mut self,
        other: &Bound<'_, PyQueryBuilder>,
        seen_vars: Option<Vec<String>>,
    ) -> PyResult<Vec<String>> {
        let mut other_ref = other.borrow_mut();
        let mut vars_set: std::collections::HashSet<String> =
            seen_vars.unwrap_or_default().into_iter().collect();
        self.inner
            .import_match_patterns(&mut other_ref.inner, &mut vars_set);
        Ok(vars_set.into_iter().collect())
    }

    #[pyo3(signature = (variable=None, labels=vec![], properties=None))]
    fn node(
        &mut self,
        variable: Option<String>,
        labels: Vec<String>,
        properties: Option<Vec<(String, Bound<'_, PyAny>)>>,
    ) -> PyResult<()> {
        if let Some(props) = properties {
            let mut props_lit = Vec::with_capacity(props.len());
            for (k, v) in props {
                let lit = py_to_literal(&v)?;
                props_lit.push((k, lit));
            }
            self.inner.node_with_props(variable, labels, props_lit);
        } else {
            self.inner.node(variable, labels);
        }
        Ok(())
    }

    #[pyo3(signature = (edge_types=vec![], variable=None, properties=None))]
    fn to(
        &mut self,
        edge_types: Vec<String>,
        variable: Option<String>,
        properties: Option<Vec<(String, Bound<'_, PyAny>)>>,
    ) -> PyResult<()> {
        if let Some(props) = properties {
            let mut props_lit = Vec::with_capacity(props.len());
            for (k, v) in props {
                let lit = py_to_literal(&v)?;
                props_lit.push((k, lit));
            }
            self.inner.to_with_props(edge_types, variable, props_lit);
        } else {
            self.inner.to(edge_types, variable);
        }
        Ok(())
    }

    #[allow(clippy::wrong_self_convention)]
    #[pyo3(signature = (edge_types=vec![], variable=None, properties=None))]
    fn from_edge(
        &mut self,
        edge_types: Vec<String>,
        variable: Option<String>,
        properties: Option<Vec<(String, Bound<'_, PyAny>)>>,
    ) -> PyResult<()> {
        if let Some(props) = properties {
            let mut props_lit = Vec::with_capacity(props.len());
            for (k, v) in props {
                let lit = py_to_literal(&v)?;
                props_lit.push((k, lit));
            }
            self.inner.from_with_props(edge_types, variable, props_lit);
        } else {
            self.inner.from(edge_types, variable);
        }
        Ok(())
    }

    #[pyo3(signature = (edge_types=vec![], variable=None, properties=None))]
    fn edge(
        &mut self,
        edge_types: Vec<String>,
        variable: Option<String>,
        properties: Option<Vec<(String, Bound<'_, PyAny>)>>,
    ) -> PyResult<()> {
        if let Some(props) = properties {
            let mut props_lit = Vec::with_capacity(props.len());
            for (k, v) in props {
                let lit = py_to_literal(&v)?;
                props_lit.push((k, lit));
            }
            self.inner.edge_with_props(edge_types, variable, props_lit);
        } else {
            self.inner.edge(edge_types, variable);
        }
        Ok(())
    }

    #[pyo3(signature = (min=None, max=None))]
    fn hops(&mut self, min: Option<u32>, max: Option<u32>) {
        let min = if min.is_none() && max.is_some() {
            Some(1)
        } else {
            min
        };
        self.inner.hops_opt(min, max);
    }

    #[pyo3(signature = (variable=None))]
    fn trail(&mut self, variable: Option<String>) {
        self.inner.trail();
        if let Some(v) = variable {
            self.inner.path_variable(v);
        }
    }

    #[pyo3(signature = (variable=None))]
    fn simple(&mut self, variable: Option<String>) {
        self.inner.simple();
        if let Some(v) = variable {
            self.inner.path_variable(v);
        }
    }

    #[pyo3(signature = (variable=None))]
    fn acyclic(&mut self, variable: Option<String>) {
        self.inner.acyclic();
        if let Some(v) = variable {
            self.inner.path_variable(v);
        }
    }

    #[pyo3(signature = (variable=None))]
    fn walk(&mut self, variable: Option<String>) {
        self.inner.walk();
        if let Some(v) = variable {
            self.inner.path_variable(v);
        }
    }

    fn path_variable(&mut self, variable: String) {
        self.inner.path_variable(variable);
    }

    fn path_mode(&mut self, mode: String) -> PyResult<()> {
        let m = match mode.to_ascii_lowercase().as_str() {
            "trail" => voyager_ogm::ast::PathMode::Trail,
            "simple" => voyager_ogm::ast::PathMode::Simple,
            "acyclic" => voyager_ogm::ast::PathMode::Acyclic,
            "walk" => voyager_ogm::ast::PathMode::Walk,
            "none" | "" => voyager_ogm::ast::PathMode::None,
            other => {
                return Err(PyValueError::new_err(format!(
                    "Unknown path mode '{other}'. Expected trail, simple, acyclic, walk, or none."
                )));
            }
        };
        self.inner.path_mode(m);
        Ok(())
    }

    fn get_path_mode(&self) -> Option<String> {
        self.inner.get_path_mode()
    }

    fn where_expr(&mut self, expr: &Bound<'_, PyAny>) -> PyResult<()> {
        let h = py_to_node_handle(&mut self.inner, expr)?;
        self.inner.where_expr(h);
        Ok(())
    }

    #[pyo3(signature = (expr, alias=None))]
    fn select_expr(&mut self, expr: &Bound<'_, PyAny>, alias: Option<String>) -> PyResult<()> {
        let (h, resolved_alias) = if let Ok(tuple) = expr.cast::<PyTuple>() {
            if !tuple.is_empty()
                && tuple
                    .get_item(0)?
                    .extract::<String>()
                    .map(|t| t == "alias")
                    .unwrap_or(false)
            {
                let inner = tuple.get_item(1)?;
                let tuple_alias: Option<String> = tuple.get_item(2)?.extract().ok();
                let inner_h = py_to_node_handle(&mut self.inner, &inner)?;
                (inner_h, alias.or(tuple_alias))
            } else {
                (py_to_node_handle(&mut self.inner, expr)?, alias)
            }
        } else {
            (py_to_node_handle(&mut self.inner, expr)?, alias)
        };
        self.inner.select_expr(h, resolved_alias);
        Ok(())
    }

    #[pyo3(signature = (expr, alias=None))]
    fn custom_expr(&mut self, expr: &Bound<'_, PyAny>, alias: Option<String>) -> PyResult<()> {
        self.select_expr(expr, alias)
    }

    fn where_property(
        &mut self,
        var: String,
        prop: String,
        op: String,
        val: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let op_parsed = parse_binary_op(&op)?;
        let lit = py_to_literal(val)?;
        self.inner.where_property(var, prop, op_parsed, lit);
        Ok(())
    }

    fn where_eq(&mut self, var: String, prop: String, val: &Bound<'_, PyAny>) -> PyResult<()> {
        let lit = py_to_literal(val)?;
        self.inner.where_property(var, prop, BinaryOp::Eq, lit);
        Ok(())
    }

    fn where_gt(&mut self, var: String, prop: String, val: &Bound<'_, PyAny>) -> PyResult<()> {
        let lit = py_to_literal(val)?;
        self.inner.where_property(var, prop, BinaryOp::Gt, lit);
        Ok(())
    }

    fn where_gte(&mut self, var: String, prop: String, val: &Bound<'_, PyAny>) -> PyResult<()> {
        let lit = py_to_literal(val)?;
        self.inner.where_property(var, prop, BinaryOp::Gte, lit);
        Ok(())
    }

    fn where_lt(&mut self, var: String, prop: String, val: &Bound<'_, PyAny>) -> PyResult<()> {
        let lit = py_to_literal(val)?;
        self.inner.where_property(var, prop, BinaryOp::Lt, lit);
        Ok(())
    }

    fn where_lte(&mut self, var: String, prop: String, val: &Bound<'_, PyAny>) -> PyResult<()> {
        let lit = py_to_literal(val)?;
        self.inner.where_property(var, prop, BinaryOp::Lte, lit);
        Ok(())
    }

    fn where_ne(&mut self, var: String, prop: String, val: &Bound<'_, PyAny>) -> PyResult<()> {
        let lit = py_to_literal(val)?;
        self.inner.where_property(var, prop, BinaryOp::Neq, lit);
        Ok(())
    }

    fn where_in(&mut self, var: String, prop: String, val: &Bound<'_, PyAny>) -> PyResult<()> {
        let lit = py_to_literal(val)?;
        self.inner.where_property(var, prop, BinaryOp::In, lit);
        Ok(())
    }

    fn where_not_in(&mut self, var: String, prop: String, val: &Bound<'_, PyAny>) -> PyResult<()> {
        let lit = py_to_literal(val)?;
        self.inner.where_property(var, prop, BinaryOp::NotIn, lit);
        Ok(())
    }

    fn where_contains(&mut self, var: String, prop: String, val: String) {
        self.inner.where_contains(var, prop, val);
    }

    fn where_starts_with(&mut self, var: String, prop: String, val: String) {
        self.inner
            .where_property(var, prop, BinaryOp::StartsWith, LiteralValue::String(val));
    }

    fn where_ends_with(&mut self, var: String, prop: String, val: String) {
        self.inner
            .where_property(var, prop, BinaryOp::EndsWith, LiteralValue::String(val));
    }

    fn r#with(&mut self) {
        self.inner.r#with();
    }

    #[pyo3(name = "with_")]
    fn with_py(&mut self) {
        self.inner.r#with();
    }

    fn r#return(&mut self) {
        self.inner.r#return();
    }

    #[pyo3(name = "return_")]
    fn return_py(&mut self) {
        self.inner.r#return();
    }

    fn create(&mut self) {
        self.inner.create();
    }

    fn merge(&mut self) {
        self.inner.merge();
    }

    #[pyo3(signature = (source, rel_type, target, direction="outgoing", variable=None, properties=None))]
    fn create_relationship(
        &mut self,
        source: String,
        rel_type: String,
        target: String,
        direction: &str,
        variable: Option<String>,
        properties: Option<Vec<(String, Bound<'_, PyAny>)>>,
    ) -> PyResult<()> {
        let direction = parse_direction(direction)?;
        let properties = optional_property_list_to_literals(properties)?;
        self.inner
            .create_relationship(source, rel_type, direction, variable, target, properties)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(())
    }

    #[pyo3(signature = (source, rel_type, target, direction="outgoing", variable=None, properties=None))]
    fn merge_relationship(
        &mut self,
        source: String,
        rel_type: String,
        target: String,
        direction: &str,
        variable: Option<String>,
        properties: Option<Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let direction = parse_direction(direction)?;
        let mut parts = MergeRelationshipProperties::new();
        if let Some(bundle) = properties {
            let bundle = bundle.cast::<PyTuple>().map_err(|_| {
                PyTypeError::new_err(
                    "merge_relationship properties must be a (key, shared, create_only) tuple of property lists",
                )
            })?;
            if bundle.len() != 3 {
                return Err(PyTypeError::new_err(
                    "merge_relationship properties must be a (key, shared, create_only) tuple of property lists",
                ));
            }
            parts.key = property_list_to_literals(&bundle.get_item(0)?)?;
            parts.shared = property_list_to_literals(&bundle.get_item(1)?)?;
            parts.create_only = property_list_to_literals(&bundle.get_item(2)?)?;
        }
        self.inner
            .merge_relationship(source, rel_type, direction, variable, target, parts)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(())
    }

    fn navigate_relationship(&mut self, spec: &Bound<'_, PyDict>) -> PyResult<()> {
        let source = relationship_spec_endpoint(spec, "source", true)?;
        let target = relationship_spec_endpoint(spec, "target", false)?;
        let direction = parse_direction(&relationship_spec_string(spec, "direction")?)?;
        self.inner
            .navigate_relationship(&RelationshipNavigationSpec {
                source,
                target,
                rel_type: relationship_spec_string(spec, "rel_type")?,
                direction,
                rel_variable: relationship_spec_optional_string(spec, "rel_variable")?,
            });
        Ok(())
    }

    fn connect_relationship(&mut self, spec: &Bound<'_, PyDict>) -> PyResult<()> {
        let source = relationship_spec_endpoint(spec, "source", true)?;
        let target = relationship_spec_endpoint(spec, "target", true)?;
        let direction = parse_direction(&relationship_spec_string(spec, "direction")?)?;
        self.inner
            .connect_relationship(&RelationshipConnectSpec {
                source,
                target,
                rel_type: relationship_spec_string(spec, "rel_type")?,
                direction,
                properties: relationship_spec_properties(spec)?,
            })
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(())
    }

    fn disconnect_relationship(&mut self, spec: &Bound<'_, PyDict>) -> PyResult<()> {
        let source = relationship_spec_endpoint(spec, "source", true)?;
        let target = relationship_spec_endpoint(spec, "target", true)?;
        let direction = parse_direction(&relationship_spec_string(spec, "direction")?)?;
        self.inner
            .disconnect_relationship(&RelationshipDisconnectSpec {
                source,
                target,
                rel_type: relationship_spec_string(spec, "rel_type")?,
                direction,
                rel_variable: relationship_spec_string(spec, "rel_variable")?,
            })
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(())
    }

    fn relationship_batch(&mut self, spec: &Bound<'_, PyDict>) -> PyResult<()> {
        let source = relationship_spec_endpoint(spec, "source", true)?;
        let target = relationship_spec_endpoint(spec, "target", false)?;
        let direction = parse_direction(&relationship_spec_string(spec, "direction")?)?;
        self.inner
            .relationship_batch(&RelationshipBatchSpec {
                source,
                target,
                rel_type: relationship_spec_string(spec, "rel_type")?,
                direction,
                rel_variable: relationship_spec_optional_string(spec, "rel_variable")?,
                properties: relationship_spec_properties(spec)?,
                batch_param: relationship_spec_optional_string(spec, "batch_param")?
                    .unwrap_or_else(|| "targets".to_string()),
                row_alias: relationship_spec_optional_string(spec, "row_alias")?
                    .unwrap_or_else(|| "target_key".to_string()),
            })
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(())
    }

    fn on_create_set(&mut self, var: String, prop: String, val: &Bound<'_, PyAny>) -> PyResult<()> {
        let lit = py_to_literal(val)?;
        self.inner.on_create_set(var, prop, lit);
        Ok(())
    }

    fn on_match_set(&mut self, var: String, prop: String, val: &Bound<'_, PyAny>) -> PyResult<()> {
        let lit = py_to_literal(val)?;
        self.inner.on_match_set(var, prop, lit);
        Ok(())
    }

    fn on_create_and_match_set(
        &mut self,
        var: String,
        prop: String,
        val: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let lit = py_to_literal(val)?;
        self.inner.on_create_and_match_set(var, prop, lit);
        Ok(())
    }

    fn fold_merge_eq(
        &mut self,
        variable: String,
        property: String,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let lit = py_to_literal(value)?;
        self.inner
            .fold_merge_eq(&variable, &property, lit)
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }

    fn set_property(&mut self, var: String, prop: String, val: &Bound<'_, PyAny>) -> PyResult<()> {
        let lit = py_to_literal(val)?;
        self.inner.set_property(var, prop, lit);
        Ok(())
    }

    fn set_merge(&mut self, var: String, val: &Bound<'_, PyAny>) -> PyResult<()> {
        let lit = py_to_literal(val)?;
        self.inner.set_merge(var, lit);
        Ok(())
    }

    fn delete(&mut self, targets: Vec<String>) {
        self.inner.delete(targets);
    }

    fn detach_delete(&mut self, targets: Vec<String>) {
        self.inner.detach_delete(targets);
    }

    fn unwind(&mut self, param_name: String, alias: String) {
        self.inner.unwind_param(param_name, alias);
    }

    #[pyo3(signature = (url, with_headers=true, alias="row".to_string()))]
    fn load_csv(&mut self, url: String, with_headers: bool, alias: String) {
        self.inner.load_csv(url, with_headers, alias);
    }

    fn remove_property(&mut self, var: String, prop: String) {
        self.inner.remove_property(var, prop);
    }

    #[pyo3(signature = (var, prop, alias=None))]
    fn field(&mut self, var: String, prop: String, alias: Option<String>) {
        self.inner.field(var, prop, alias);
    }

    #[pyo3(signature = (var, prop, func, alias=None))]
    fn aggregate(
        &mut self,
        var: String,
        prop: String,
        func: String,
        alias: Option<String>,
    ) -> PyResult<()> {
        let agg = match func.to_lowercase().as_str() {
            "count" => AggregationFunc::Count,
            "count_distinct" => AggregationFunc::CountDistinct,
            "sum" => AggregationFunc::Sum,
            "avg" => AggregationFunc::Avg,
            "min" => AggregationFunc::Min,
            "max" => AggregationFunc::Max,
            "collect" => AggregationFunc::Collect,
            other => {
                return Err(PyValueError::new_err(format!(
                    "Unknown aggregation function: {other}"
                )));
            }
        };
        if prop == "*" || prop.is_empty() {
            let expr = self.inner.ident(var);
            self.inner.select_aggregate(expr, agg, alias);
        } else {
            self.inner.select_property_aggregate(var, prop, agg, alias);
        }
        Ok(())
    }

    fn order_by(&mut self, var: String, prop: String, ascending: bool) {
        if ascending {
            self.inner.order_by_asc(var, prop);
        } else {
            self.inner.order_by_desc(var, prop);
        }
    }

    fn order_by_expr(&mut self, expr: &Bound<'_, PyAny>, ascending: bool) -> PyResult<()> {
        let h = py_to_node_handle(&mut self.inner, expr)?;
        self.inner.order_by(h, ascending);
        Ok(())
    }

    fn distinct(&mut self) {
        self.inner.distinct(true);
    }

    fn limit(&mut self, limit: u64) {
        self.inner.limit(limit);
    }

    fn limit_param(&mut self, name: String) -> PyResult<()> {
        let name = strip_leading_dollar(name);
        if name.is_empty() {
            return Err(PyValueError::new_err(
                "Pagination parameter name must be a non-empty string",
            ));
        }
        self.inner.limit_param(name);
        Ok(())
    }

    fn skip(&mut self, skip: u64) {
        self.inner.skip(skip);
    }

    fn skip_param(&mut self, name: String) -> PyResult<()> {
        let name = strip_leading_dollar(name);
        if name.is_empty() {
            return Err(PyValueError::new_err(
                "Pagination parameter name must be a non-empty string",
            ));
        }
        self.inner.skip_param(name);
        Ok(())
    }

    fn offset(&mut self, offset: u64) {
        self.inner.offset(offset);
    }

    fn offset_param(&mut self, name: String) -> PyResult<()> {
        let name = strip_leading_dollar(name);
        if name.is_empty() {
            return Err(PyValueError::new_err(
                "Pagination parameter name must be a non-empty string",
            ));
        }
        self.inner.offset_param(name);
        Ok(())
    }

    fn let_(&mut self, variable: String, expr: &Bound<'_, PyAny>) -> PyResult<()> {
        let h = py_to_node_handle(&mut self.inner, expr)?;
        self.inner.let_(variable, h);
        Ok(())
    }

    fn filter_(&mut self, predicate: &Bound<'_, PyAny>) -> PyResult<()> {
        let h = py_to_node_handle(&mut self.inner, predicate)?;
        self.inner.filter_(h);
        Ok(())
    }

    fn linear_filter(&mut self, predicate: &Bound<'_, PyAny>) -> PyResult<()> {
        self.filter_(predicate)
    }

    #[pyo3(signature = (procedure_name, args=vec![], kwargs=std::collections::HashMap::new()))]
    fn call_procedure(
        &mut self,
        procedure_name: String,
        args: Vec<Bound<'_, PyAny>>,
        kwargs: std::collections::HashMap<String, Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let mut arg_handles = Vec::with_capacity(args.len() + kwargs.len());
        for arg in args {
            let lit = py_to_literal(&arg)?;
            let h = self.inner.literal(lit);
            arg_handles.push(h);
        }
        for (_k, v) in kwargs {
            let lit = py_to_literal(&v)?;
            let h = self.inner.literal(lit);
            arg_handles.push(h);
        }
        self.inner.call_procedure(procedure_name, arg_handles);
        Ok(())
    }

    fn yield_items(&mut self, items: Vec<String>) {
        self.inner.yield_items(items);
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (dialect, is_relationship, target_name, falkordb_target, query_string, properties=vec![], yield_node="node", yield_score="score"))]
    fn fulltext_search(
        &mut self,
        dialect: &str,
        is_relationship: bool,
        target_name: &str,
        falkordb_target: &str,
        query_string: &str,
        properties: Vec<String>,
        yield_node: &str,
        yield_score: &str,
    ) -> PyResult<()> {
        let d = DdlDialect::parse_str(dialect).unwrap_or(DdlDialect::Cypher);
        self.inner.fulltext_search(
            d,
            is_relationship,
            target_name,
            falkordb_target,
            query_string,
            &properties,
            Some(yield_node),
            Some(yield_score),
        );
        Ok(())
    }

    /// Serialises this builder into a Query Spec v1 dictionary.
    ///
    /// Equivalent constructs compile from the returned document exactly like the
    /// fluent query. A construct Query Spec cannot express raises `VoyagerError`
    /// with code `UNSUPPORTED_FEATURE` naming it.
    fn to_spec_dict<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let (arena, root) = self.inner.clone().try_build().map_err(|error| {
            make_voyager_err(voyager_ogm::ErrorCode::InvalidClause, error.to_string())
        })?;
        let spec = spec_from_ast(&arena, root).map_err(map_spec_export_error)?;
        spec_value_to_py(py, &spec)
    }

    #[pyo3(signature = (dialect="cypher", graph_name=None, optimize=false, optimization_level="standard"))]
    fn compile<'py>(
        &self,
        dialect: &str,
        graph_name: Option<String>,
        optimize: bool,
        optimization_level: &str,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyDict>> {
        check_warn_cypher_path_mode(py, dialect, self.inner.get_path_mode().as_deref())?;

        let (mut arena, root) = self.inner.clone().try_build().map_err(|e| match e.code() {
            voyager_ogm::ErrorCode::InvalidClause => {
                make_voyager_err(voyager_ogm::ErrorCode::InvalidClause, e.to_string())
            }
            _ => PyValueError::new_err(e.to_string()),
        })?;

        if optimize {
            let opt_level = OptimizationLevel::from_str_opt(optimization_level);
            let optimizer = AstOptimizer::new(opt_level);
            optimizer
                .optimize(&mut arena, root)
                .map_err(|e| PyValueError::new_err(e.to_string()))?;
        }

        let compiled = match dialect.to_lowercase().as_str() {
            "cypher" | "opencypher" | "neo4j" | "memgraph" | "falkordb" | "falkor" => {
                let mut emitter = CypherEmitter::new();
                emitter
                    .visit_query(&arena, root)
                    .map_err(|e| PyValueError::new_err(e.to_string()))?
            }
            "sql_pgq" | "pgq" | "sql" => {
                let name = graph_name.unwrap_or_else(|| "graph_table".into());
                let mut emitter = SqlPgqEmitter::new(name);
                emitter
                    .visit_query(&arena, root)
                    .map_err(|e| PyValueError::new_err(e.to_string()))?
            }
            "duckpgq" | "duck_pgq" => {
                let name = graph_name.unwrap_or_else(|| "graph_table".into());
                let mut emitter = SqlPgqEmitter::new(name).for_duckpgq();
                emitter
                    .visit_query(&arena, root)
                    .map_err(|e| PyValueError::new_err(e.to_string()))?
            }
            "iso_gql" | "gql" => {
                let mut emitter = IsoGqlEmitter::new();
                emitter
                    .visit_query(&arena, root)
                    .map_err(|e| PyValueError::new_err(e.to_string()))?
            }
            "age" | "apache_age" | "postgres_age" => {
                let name = graph_name
                    .unwrap_or_else(|| voyager_ogm::emitters::age::DEFAULT_AGE_GRAPH.into());
                let mut emitter = AgeEmitter::new(name);
                emitter
                    .visit_query(&arena, root)
                    .map_err(|e| PyValueError::new_err(e.to_string()))?
            }
            other => {
                return Err(make_voyager_err(
                    voyager_ogm::ErrorCode::UnsupportedDialect,
                    format!(
                        "Unsupported query dialect: '{other}'. Choose from 'cypher', 'sql_pgq', 'iso_gql', or 'apache_age'."
                    ),
                ));
            }
        };

        compiled_query_to_py_dict(py, &compiled)
    }

    /// Extracts native graph topology (nodes and edges) directly from the AST arena.
    fn extract_topology<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let (arena, root) = self
            .inner
            .clone()
            .try_build()
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        let topology = GraphTopology::from_arena(&arena, Some(root));
        topology_to_py_dict(&topology, py)
    }

    /// Validates the query AST against the schema registry.
    #[pyo3(signature = (registry=None))]
    fn validate<'py>(
        &self,
        registry: Option<&PyNativeSchemaRegistry>,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let (arena, root) = self
            .inner
            .clone()
            .try_build()
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        let topology = GraphTopology::from_arena(&arena, Some(root));
        let reg = match registry {
            Some(r) => r.registry(),
            None => global_schema_registry(),
        };
        let report = reg.validate_topology(&topology);
        conformance_report_to_py_dict(&report, py)
    }
}

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
    duration_cls: Py<PyAny>,
    parse_pg_interval_fn: Py<PyAny>,
}

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
            duration_cls: types_mod.getattr("Duration")?.unbind(),
            parse_pg_interval_fn: types_mod.getattr("parse_pg_interval_text")?.unbind(),
        })
    })
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

fn nanos_to_py_time<'py>(
    py: Python<'py>,
    nanos_of_day: i64,
    tz_offset_seconds: Option<i64>,
    fallback_iso: &str,
) -> PyResult<Bound<'py, PyAny>> {
    if !(0..86_400_000_000_000).contains(&nanos_of_day) || nanos_of_day % 1_000 != 0 {
        return Ok(fallback_iso.into_pyobject(py)?.into_any());
    }
    let (h, m, s, ns) = voyager_drivers::bolt::packstream::nanos_to_hmsn(nanos_of_day);
    let us = ns / 1_000;
    let ctors = entity_constructors(py)?;
    match tz_offset_seconds {
        None => ctors.time_cls.bind(py).call1((h, m, s, us)),
        Some(offset) => match make_py_offset_tz(py, offset)? {
            Some(tz) => ctors.time_cls.bind(py).call1((h, m, s, us, tz)),
            None => Ok(fallback_iso.into_pyobject(py)?.into_any()),
        },
    }
}

fn epoch_nanos_to_py_datetime<'py>(
    py: Python<'py>,
    epoch_seconds: i64,
    nanoseconds: i64,
    tz: Option<PyTemporalTz<'_>>,
    fallback_iso: &str,
) -> PyResult<Bound<'py, PyAny>> {
    if !(0..1_000_000_000).contains(&nanoseconds) || nanoseconds % 1_000 != 0 {
        return Ok(fallback_iso.into_pyobject(py)?.into_any());
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
                return Ok(fallback_iso.into_pyobject(py)?.into_any());
            }
            ctors.datetime_cls.bind(py).call1((y, m, d, hh, mm, ss, us))
        }
        Some(PyTemporalTz::Offset(offset)) => {
            let Some(local_secs) = epoch_seconds.checked_add(offset) else {
                return Ok(fallback_iso.into_pyobject(py)?.into_any());
            };
            let (y, m, d, hh, mm, ss) = split_secs(local_secs);
            if !(1..=9999).contains(&y) {
                return Ok(fallback_iso.into_pyobject(py)?.into_any());
            }
            let Some(tz_obj) = make_py_offset_tz(py, offset)? else {
                return Ok(fallback_iso.into_pyobject(py)?.into_any());
            };
            ctors
                .datetime_cls
                .bind(py)
                .call1((y, m, d, hh, mm, ss, us, tz_obj))
        }
        Some(PyTemporalTz::Zone(zone_id)) => {
            let (y, m, d, hh, mm, ss) = split_secs(epoch_seconds);
            if !(1..=9999).contains(&y) {
                return Ok(fallback_iso.into_pyobject(py)?.into_any());
            }
            let Ok(zi) = ctors.zoneinfo_cls.bind(py).call1((zone_id,)) else {
                return Ok(fallback_iso.into_pyobject(py)?.into_any());
            };
            let utc = ctors.utc_obj.bind(py);
            let dt_utc = ctors
                .datetime_cls
                .bind(py)
                .call1((y, m, d, hh, mm, ss, us, utc))?;
            match dt_utc.call_method1("astimezone", (zi,)) {
                Ok(dt_zoned) => Ok(dt_zoned),
                Err(_) => Ok(fallback_iso.into_pyobject(py)?.into_any()),
            }
        }
        Some(PyTemporalTz::LocalZone(zone_id)) => {
            let (y, m, d, hh, mm, ss) = split_secs(epoch_seconds);
            if !(1..=9999).contains(&y) {
                return Ok(fallback_iso.into_pyobject(py)?.into_any());
            }
            let Ok(zi) = ctors.zoneinfo_cls.bind(py).call1((zone_id,)) else {
                return Ok(fallback_iso.into_pyobject(py)?.into_any());
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
        "decimal" => ctors.decimal_cls.bind(py).call1((trimmed,)),
        "uuid" => ctors.uuid_cls.bind(py).call1((trimmed,)),
        "duration" => {
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
        "date" => {
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
        "time" | "time_tz" => {
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
            if (tz_start.is_some() || kind == "time_tz") && tz_offset.is_none() {
                return Ok(s.into_pyobject(py)?.into_any());
            }
            if let Some((h, m, sec, nanos)) = parse_hms_str(hms_part.trim()) {
                let total_ns =
                    ((h as i64) * 3_600 + (m as i64) * 60 + (sec as i64)) * 1_000_000_000 + nanos;
                return nanos_to_py_time(py, total_ns, tz_offset, trimmed);
            }
            Ok(s.into_pyobject(py)?.into_any())
        }
        "local_datetime" | "datetime_tz" => {
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
            if kind == "datetime_tz" && bracket_zone.is_none() && offset_opt.is_none() {
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
                let Ok(zi) = ctors.zoneinfo_cls.bind(py).call1((zone_id,)) else {
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
                        trimmed,
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

fn bolt_node_to_py<'py>(
    py: Python<'py>,
    node: &voyager_drivers::BoltNode,
) -> PyResult<Bound<'py, PyAny>> {
    let ctors = entity_constructors(py)?;
    let eid_py = match &node.element_id {
        Some(eid) => eid.into_pyobject(py)?.into_any(),
        None => py.None().into_bound(py),
    };
    let mut labels: Vec<Bound<'py, PyAny>> = Vec::with_capacity(node.labels.len());
    for lbl in &node.labels {
        labels.push(lbl.into_pyobject(py)?.into_any());
    }
    let labels_fs = PyFrozenSet::new(py, &labels)?;
    let props = PyDict::new(py);
    for (k, v) in &node.properties {
        props.set_item(k, bolt_value_to_py(py, v)?)?;
    }
    ctors
        .node_from_trusted
        .bind(py)
        .call1((node.id, eid_py, labels_fs, props))
}

fn bolt_rel_to_py<'py>(
    py: Python<'py>,
    rel: &voyager_drivers::BoltRelationship,
) -> PyResult<Bound<'py, PyAny>> {
    let ctors = entity_constructors(py)?;
    let eid_py = match &rel.element_id {
        Some(eid) => eid.into_pyobject(py)?.into_any(),
        None => py.None().into_bound(py),
    };
    let start_eid_py = match &rel.start_element_id {
        Some(eid) => eid.into_pyobject(py)?.into_any(),
        None => py.None().into_bound(py),
    };
    let end_eid_py = match &rel.end_element_id {
        Some(eid) => eid.into_pyobject(py)?.into_any(),
        None => py.None().into_bound(py),
    };
    let props = PyDict::new(py);
    for (k, v) in &rel.properties {
        props.set_item(k, bolt_value_to_py(py, v)?)?;
    }
    ctors.rel_from_trusted.bind(py).call1((
        rel.id,
        eid_py,
        rel.rel_type.as_str(),
        rel.start_node_id,
        rel.end_node_id,
        props,
        start_eid_py,
        end_eid_py,
    ))
}

fn bolt_value_to_py<'py>(
    py: Python<'py>,
    value: &voyager_drivers::BoltValue,
) -> PyResult<Bound<'py, PyAny>> {
    use voyager_drivers::{BoltRelationship, BoltValue};

    match value {
        BoltValue::Null => Ok(py.None().into_bound(py)),
        BoltValue::Boolean(b) => Ok(b.into_pyobject(py)?.to_owned().into_any()),
        BoltValue::Integer(i) => Ok(i.into_pyobject(py)?.to_owned().into_any()),
        BoltValue::Float(f) => Ok(f.into_pyobject(py)?.into_any()),
        BoltValue::Bytes(bytes) => Ok(PyBytes::new(py, bytes).into_any()),
        BoltValue::String(s) => Ok(s.into_pyobject(py)?.into_any()),
        BoltValue::List(items) => {
            let list = PyList::empty(py);
            for item in items {
                list.append(bolt_value_to_py(py, item)?)?;
            }
            Ok(list.into_any())
        }
        BoltValue::Map(map) => {
            let dict = PyDict::new(py);
            for (k, v) in map {
                dict.set_item(k, bolt_value_to_py(py, v)?)?;
            }
            Ok(dict.into_any())
        }
        BoltValue::Structure { tag, fields } => {
            let dict = PyDict::new(py);
            dict.set_item("tag", *tag)?;
            let list = PyList::empty(py);
            for f in fields {
                list.append(bolt_value_to_py(py, f)?)?;
            }
            dict.set_item("fields", list)?;
            Ok(dict.into_any())
        }
        BoltValue::Node(node) => bolt_node_to_py(py, node),
        BoltValue::Relationship(rel) => bolt_rel_to_py(py, rel),
        BoltValue::Path(path) => {
            let ctors = entity_constructors(py)?;
            let mut node_objs = Vec::new();
            let mut rel_objs = Vec::new();
            if path.sequence.is_empty() {
                for node in &path.nodes {
                    node_objs.push(bolt_node_to_py(py, node)?);
                }
            } else {
                if let Some(first) = path.nodes.first() {
                    node_objs.push(bolt_node_to_py(py, first)?);
                }
                let mut current_node_idx = 0usize;
                for step_idx in 0..(path.sequence.len() / 2) {
                    let rel_code = path.sequence[2 * step_idx];
                    let next_node_raw = path.sequence[2 * step_idx + 1];
                    let next_node_idx = usize::try_from(next_node_raw).unwrap_or(0);
                    let rel_idx = rel_code.unsigned_abs().saturating_sub(1) as usize;
                    if let (Some(prev_node), Some(next_node), Some(unbound_rel)) = (
                        path.nodes.get(current_node_idx),
                        path.nodes.get(next_node_idx),
                        path.relationships.get(rel_idx),
                    ) {
                        node_objs.push(bolt_node_to_py(py, next_node)?);
                        let (start_node, end_node) = if rel_code > 0 {
                            (prev_node, next_node)
                        } else {
                            (next_node, prev_node)
                        };
                        let bound = BoltRelationship {
                            id: unbound_rel.id,
                            start_node_id: start_node.id,
                            end_node_id: end_node.id,
                            rel_type: unbound_rel.rel_type.clone(),
                            properties: unbound_rel.properties.clone(),
                            element_id: unbound_rel.element_id.clone(),
                            start_element_id: start_node.element_id.clone(),
                            end_element_id: end_node.element_id.clone(),
                        };
                        rel_objs.push(bolt_rel_to_py(py, &bound)?);
                        current_node_idx = next_node_idx;
                    }
                }
            }
            let nodes_tuple = PyTuple::new(py, &node_objs)?;
            let rels_tuple = PyTuple::new(py, &rel_objs)?;
            ctors
                .path_from_trusted
                .bind(py)
                .call1((nodes_tuple, rels_tuple))
        }
        BoltValue::Point2D(pt) => {
            let dict = PyDict::new(py);
            dict.set_item("srid", pt.srid)?;
            dict.set_item("x", pt.x)?;
            dict.set_item("y", pt.y)?;
            Ok(dict.into_any())
        }
        BoltValue::Point3D(pt) => {
            let dict = PyDict::new(py);
            dict.set_item("srid", pt.srid)?;
            dict.set_item("x", pt.x)?;
            dict.set_item("y", pt.y)?;
            dict.set_item("z", pt.z)?;
            Ok(dict.into_any())
        }
        BoltValue::Date(d) => days_to_py_date(py, d.days, Some(&d.to_iso_string())),
        BoltValue::LocalTime(lt) => nanos_to_py_time(py, lt.nanoseconds, None, &lt.to_iso_string()),
        BoltValue::Time(t) => nanos_to_py_time(
            py,
            t.nanoseconds,
            Some(t.tz_offset_seconds),
            &t.to_iso_string(),
        ),
        BoltValue::LocalDateTime(ldt) => {
            epoch_nanos_to_py_datetime(py, ldt.seconds, ldt.nanoseconds, None, &ldt.to_iso_string())
        }
        BoltValue::DateTime(dt) => epoch_nanos_to_py_datetime(
            py,
            dt.seconds,
            dt.nanoseconds,
            Some(PyTemporalTz::Offset(dt.tz_offset_seconds)),
            &dt.to_iso_string(),
        ),
        BoltValue::DateTimeZoneId(dtz) => epoch_nanos_to_py_datetime(
            py,
            dtz.seconds,
            dtz.nanoseconds,
            Some(if dtz.is_local {
                PyTemporalTz::LocalZone(&dtz.zone_id)
            } else {
                PyTemporalTz::Zone(&dtz.zone_id)
            }),
            &dtz.to_iso_string(),
        ),
        BoltValue::Duration(dur) => {
            let ctors = entity_constructors(py)?;
            ctors
                .duration_cls
                .bind(py)
                .call1((dur.months, dur.days, dur.seconds, dur.nanoseconds))
        }
    }
}

#[derive(Clone, Copy)]
enum Utf8ColMode {
    Plain,
    JsonEntities,
    JsonPure,
    Typed(&'static str),
}

fn utf8_col_mode_for_field(field: &arrow_schema::Field) -> Utf8ColMode {
    let meta = field.metadata();
    if meta.get("voyager.encoding").is_some_and(|v| v == "json") {
        if meta.get("voyager.type").is_some_and(|v| v == "json") {
            return Utf8ColMode::JsonPure;
        }
        return Utf8ColMode::JsonEntities;
    }
    if meta.contains_key("voyager.kind") {
        return Utf8ColMode::JsonEntities;
    }
    match meta.get("voyager.type").map(String::as_str) {
        Some("date") => Utf8ColMode::Typed("date"),
        Some("time") => Utf8ColMode::Typed("time"),
        Some("time_tz") => Utf8ColMode::Typed("time_tz"),
        Some("local_datetime") => Utf8ColMode::Typed("local_datetime"),
        Some("datetime_tz") => Utf8ColMode::Typed("datetime_tz"),
        Some("decimal") => Utf8ColMode::Typed("decimal"),
        Some("uuid") => Utf8ColMode::Typed("uuid"),
        Some("duration") => Utf8ColMode::Typed("duration"),
        _ => Utf8ColMode::Plain,
    }
}

/// Converts one Arrow array element to the corresponding Python value.
///
/// Handles all Arrow scalar, temporal, binary, uuid extension, decimal,
/// duration/struct, JSON metadata, and nested `List` types.
fn arrow_array_value_to_py<'py>(
    field: &arrow_schema::Field,
    array: &dyn arrow_array::Array,
    index: usize,
    py: Python<'py>,
) -> PyResult<Bound<'py, PyAny>> {
    use arrow_array::*;
    use arrow_schema::{DataType, TimeUnit};

    if array.is_null(index) {
        return Ok(py.None().into_bound(py));
    }

    match array.data_type() {
        DataType::Int64 => {
            let values = array.as_any().downcast_ref::<Int64Array>().unwrap();
            Ok(values.value(index).into_pyobject(py)?.to_owned().into_any())
        }
        DataType::Int32 => {
            let values = array.as_any().downcast_ref::<Int32Array>().unwrap();
            Ok(values.value(index).into_pyobject(py)?.to_owned().into_any())
        }
        DataType::Int16 => {
            let values = array.as_any().downcast_ref::<Int16Array>().unwrap();
            Ok(values.value(index).into_pyobject(py)?.to_owned().into_any())
        }
        DataType::Int8 => {
            let values = array.as_any().downcast_ref::<Int8Array>().unwrap();
            Ok(values.value(index).into_pyobject(py)?.to_owned().into_any())
        }
        DataType::UInt64 => {
            let values = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            Ok(values.value(index).into_pyobject(py)?.to_owned().into_any())
        }
        DataType::UInt32 => {
            let values = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            Ok(values.value(index).into_pyobject(py)?.to_owned().into_any())
        }
        DataType::UInt16 => {
            let values = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            Ok(values.value(index).into_pyobject(py)?.to_owned().into_any())
        }
        DataType::UInt8 => {
            let values = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            Ok(values.value(index).into_pyobject(py)?.to_owned().into_any())
        }
        DataType::Float64 => {
            let values = array.as_any().downcast_ref::<Float64Array>().unwrap();
            Ok(values.value(index).into_pyobject(py)?.to_owned().into_any())
        }
        DataType::Float32 => {
            let values = array.as_any().downcast_ref::<Float32Array>().unwrap();
            Ok((values.value(index) as f64)
                .into_pyobject(py)?
                .to_owned()
                .into_any())
        }
        DataType::Boolean => {
            let values = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            Ok(values.value(index).into_pyobject(py)?.to_owned().into_any())
        }
        DataType::Binary => {
            let values = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            Ok(PyBytes::new(py, values.value(index)).into_any())
        }
        DataType::LargeBinary => {
            let values = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            Ok(PyBytes::new(py, values.value(index)).into_any())
        }
        DataType::FixedSizeBinary(16) => {
            let values = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            let bytes = values.value(index);
            if field
                .metadata()
                .get("ARROW:extension:name")
                .is_some_and(|v| v == "arrow.uuid")
            {
                let ctors = entity_constructors(py)?;
                ctors
                    .uuid_cls
                    .bind(py)
                    .call1((py.None(), PyBytes::new(py, bytes)))
            } else {
                Ok(PyBytes::new(py, bytes).into_any())
            }
        }
        DataType::FixedSizeBinary(_) => {
            let values = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            Ok(PyBytes::new(py, values.value(index)).into_any())
        }
        DataType::Decimal128(_, scale) => {
            let values = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
            decimal128_to_py(py, values.value(index), *scale)
        }
        DataType::Date32 => {
            let values = array.as_any().downcast_ref::<Date32Array>().unwrap();
            days_to_py_date(py, values.value(index) as i64, None)
        }
        DataType::Date64 => {
            let values = array.as_any().downcast_ref::<Date64Array>().unwrap();
            days_to_py_date(py, values.value(index).div_euclid(86_400_000), None)
        }
        DataType::Time32(TimeUnit::Second) => {
            let values = array.as_any().downcast_ref::<Time32SecondArray>().unwrap();
            let s = values.value(index) as i64;
            let iso = format!("{:02}:{:02}:{:02}", s / 3_600, (s % 3_600) / 60, s % 60);
            nanos_to_py_time(py, s * 1_000_000_000, None, &iso)
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            let values = array
                .as_any()
                .downcast_ref::<Time32MillisecondArray>()
                .unwrap();
            let ns = (values.value(index) as i64) * 1_000_000;
            let iso = voyager_drivers::bolt::packstream::BoltLocalTime { nanoseconds: ns }
                .to_iso_string();
            nanos_to_py_time(py, ns, None, &iso)
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let values = array
                .as_any()
                .downcast_ref::<Time64MicrosecondArray>()
                .unwrap();
            let ns = values.value(index) * 1_000;
            let iso = voyager_drivers::bolt::packstream::BoltLocalTime { nanoseconds: ns }
                .to_iso_string();
            nanos_to_py_time(py, ns, None, &iso)
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            let values = array
                .as_any()
                .downcast_ref::<Time64NanosecondArray>()
                .unwrap();
            let ns = values.value(index);
            let iso = voyager_drivers::bolt::packstream::BoltLocalTime { nanoseconds: ns }
                .to_iso_string();
            nanos_to_py_time(py, ns, None, &iso)
        }
        DataType::Timestamp(unit, tz_opt) => {
            let (secs, nanos) = match unit {
                TimeUnit::Second => {
                    let v = array
                        .as_any()
                        .downcast_ref::<TimestampSecondArray>()
                        .unwrap()
                        .value(index);
                    (v, 0i64)
                }
                TimeUnit::Millisecond => {
                    let v = array
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .unwrap()
                        .value(index);
                    (v.div_euclid(1_000), v.rem_euclid(1_000) * 1_000_000)
                }
                TimeUnit::Microsecond => {
                    let v = array
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap()
                        .value(index);
                    (v.div_euclid(1_000_000), v.rem_euclid(1_000_000) * 1_000)
                }
                TimeUnit::Nanosecond => {
                    let v = array
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .unwrap()
                        .value(index);
                    (v.div_euclid(1_000_000_000), v.rem_euclid(1_000_000_000))
                }
            };
            let tz = tz_opt.as_deref().map(|z| {
                if z == "UTC" {
                    PyTemporalTz::Offset(0)
                } else {
                    PyTemporalTz::Zone(z)
                }
            });
            let iso = match tz_opt.as_deref() {
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
            };
            epoch_nanos_to_py_datetime(py, secs, nanos, tz, &iso)
        }
        DataType::Duration(unit) => {
            let total_ns: i128 = match unit {
                TimeUnit::Second => {
                    array
                        .as_any()
                        .downcast_ref::<DurationSecondArray>()
                        .unwrap()
                        .value(index) as i128
                        * 1_000_000_000
                }
                TimeUnit::Millisecond => {
                    array
                        .as_any()
                        .downcast_ref::<DurationMillisecondArray>()
                        .unwrap()
                        .value(index) as i128
                        * 1_000_000
                }
                TimeUnit::Microsecond => {
                    array
                        .as_any()
                        .downcast_ref::<DurationMicrosecondArray>()
                        .unwrap()
                        .value(index) as i128
                        * 1_000
                }
                TimeUnit::Nanosecond => array
                    .as_any()
                    .downcast_ref::<DurationNanosecondArray>()
                    .unwrap()
                    .value(index) as i128,
            };
            let total_secs = (total_ns / 1_000_000_000) as i64;
            let ns = (total_ns % 1_000_000_000) as i64;
            let ctors = entity_constructors(py)?;
            ctors
                .duration_cls
                .bind(py)
                .call1((0i64, 0i64, total_secs, ns))
        }
        DataType::Struct(sub_fields) => {
            let values = array.as_any().downcast_ref::<StructArray>().unwrap();
            if field
                .metadata()
                .get("voyager.type")
                .is_some_and(|v| v == "duration")
                && sub_fields.len() == 4
            {
                let months = values
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(index);
                let days = values
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(index);
                let seconds = values
                    .column(2)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(index);
                let nanoseconds = values
                    .column(3)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(index);
                let ctors = entity_constructors(py)?;
                return ctors
                    .duration_cls
                    .bind(py)
                    .call1((months, days, seconds, nanoseconds));
            }
            let dict = PyDict::new(py);
            for (col_idx, sub_field) in sub_fields.iter().enumerate() {
                let sub_arr = values.column(col_idx);
                dict.set_item(
                    sub_field.name(),
                    arrow_array_value_to_py(sub_field.as_ref(), sub_arr.as_ref(), index, py)?,
                )?;
            }
            Ok(dict.into_any())
        }
        DataType::Utf8 => {
            let values = array.as_any().downcast_ref::<StringArray>().unwrap();
            let s = values.value(index);
            match utf8_col_mode_for_field(field) {
                Utf8ColMode::JsonEntities => {
                    let parsed: serde_json::Value = serde_json::from_str(s)
                        .map_err(|e| PyValueError::new_err(e.to_string()))?;
                    json_value_to_py(py, &parsed, true)
                }
                Utf8ColMode::JsonPure => {
                    let parsed: serde_json::Value = serde_json::from_str(s)
                        .map_err(|e| PyValueError::new_err(e.to_string()))?;
                    json_value_to_py(py, &parsed, false)
                }
                Utf8ColMode::Typed(kind) => parse_voyager_typed_utf8(py, kind, s),
                Utf8ColMode::Plain => Ok(s.into_pyobject(py)?.into_any()),
            }
        }
        DataType::LargeUtf8 => {
            let values = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            let s = values.value(index);
            match utf8_col_mode_for_field(field) {
                Utf8ColMode::JsonEntities => {
                    let parsed: serde_json::Value = serde_json::from_str(s)
                        .map_err(|e| PyValueError::new_err(e.to_string()))?;
                    json_value_to_py(py, &parsed, true)
                }
                Utf8ColMode::JsonPure => {
                    let parsed: serde_json::Value = serde_json::from_str(s)
                        .map_err(|e| PyValueError::new_err(e.to_string()))?;
                    json_value_to_py(py, &parsed, false)
                }
                Utf8ColMode::Typed(kind) => parse_voyager_typed_utf8(py, kind, s),
                Utf8ColMode::Plain => Ok(s.into_pyobject(py)?.into_any()),
            }
        }
        DataType::List(item_field) => {
            let values = array.as_any().downcast_ref::<ListArray>().unwrap();
            let child = values.value(index);
            arrow_array_values_to_py(item_field.as_ref(), child.as_ref(), py)
        }
        DataType::LargeList(item_field) => {
            let values = array.as_any().downcast_ref::<LargeListArray>().unwrap();
            let child = values.value(index);
            arrow_array_values_to_py(item_field.as_ref(), child.as_ref(), py)
        }
        _ => Ok(py.None().into_bound(py)),
    }
}

/// Converts a whole Arrow array into a Python list.
fn arrow_array_values_to_py<'py>(
    field: &arrow_schema::Field,
    array: &dyn arrow_array::Array,
    py: Python<'py>,
) -> PyResult<Bound<'py, PyAny>> {
    let list = PyList::empty(py);
    for index in 0..array.len() {
        list.append(arrow_array_value_to_py(field, array, index, py)?)?;
    }
    Ok(list.into_any())
}

fn record_batch_to_py_dicts<'py>(
    batch: &arrow_array::RecordBatch,
    _age_columns: &[bool],
    bolt_rows: Option<&[Vec<voyager_drivers::BoltValue>]>,
    row_offset: usize,
    py: Python<'py>,
) -> PyResult<Bound<'py, PyList>> {
    let num_rows = batch.num_rows();
    let num_cols = batch.num_columns();
    let schema = batch.schema();
    let fields = schema.fields();

    let mut col_names = Vec::with_capacity(num_cols);
    let mut col_modes = Vec::with_capacity(num_cols);
    for field in fields.iter() {
        col_names.push(PyString::intern(py, field.name().as_str()));
        col_modes.push(utf8_col_mode_for_field(field.as_ref()));
    }

    let list = PyList::empty(py);

    for row in 0..num_rows {
        let dict = PyDict::new(py);
        let bolt_row = bolt_rows.and_then(|rows| rows.get(row_offset + row));
        for col_idx in 0..num_cols {
            let col = batch.column(col_idx);
            let field_name = &col_names[col_idx];
            if col.is_null(row) {
                dict.set_item(field_name, py.None())?;
                continue;
            }
            if matches!(col_modes[col_idx], Utf8ColMode::JsonEntities)
                && let Some(bolt_val) = bolt_row.and_then(|r| r.get(col_idx))
            {
                dict.set_item(field_name, bolt_value_to_py(py, bolt_val)?)?;
                continue;
            }
            dict.set_item(
                field_name,
                arrow_array_value_to_py(fields[col_idx].as_ref(), col.as_ref(), row, py)?,
            )?;
        }
        list.append(dict)?;
    }
    Ok(list)
}

/// Zero-copy Arrow Stream implementing the Python Arrow PyCapsule Protocol (`__arrow_c_stream__`).
#[pyclass(name = "ArrowStream", from_py_object)]
#[derive(Clone)]
pub struct PyArrowStream {
    batch: arrow_array::RecordBatch,
    consumed: Arc<AtomicBool>,
    bolt_rows: Option<Arc<Vec<Vec<voyager_drivers::BoltValue>>>>,
    row_offset: usize,
}

impl PyArrowStream {
    /// Creates a new `PyArrowStream` wrapping an Arrow `RecordBatch`.
    pub fn from_batch(batch: arrow_array::RecordBatch) -> Self {
        Self {
            batch,
            consumed: Arc::new(AtomicBool::new(false)),
            bolt_rows: None,
            row_offset: 0,
        }
    }

    /// Creates a `PyArrowStream` preserving raw Bolt records for nested `Session.execute()` fidelity.
    pub fn from_batch_with_bolt(
        batch: arrow_array::RecordBatch,
        bolt_rows: Option<Arc<Vec<Vec<voyager_drivers::BoltValue>>>>,
    ) -> Self {
        Self {
            batch,
            consumed: Arc::new(AtomicBool::new(false)),
            bolt_rows,
            row_offset: 0,
        }
    }
}

#[pymethods]
impl PyArrowStream {
    /// Number of rows in the batch.
    #[getter]
    fn num_rows(&self) -> usize {
        self.batch.num_rows()
    }

    /// Number of columns in the batch.
    #[getter]
    fn num_columns(&self) -> usize {
        self.batch.num_columns()
    }

    /// Ordered list of column names from the Arrow batch schema.
    #[getter]
    fn columns(&self) -> Vec<String> {
        self.batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    }

    /// Returns whether this stream has already been consumed via `__arrow_c_stream__`.
    #[getter]
    fn is_consumed(&self) -> bool {
        self.consumed.load(Ordering::SeqCst)
    }

    /// Official Python Arrow PyCapsule Protocol for zero-copy streaming into Polars / PyArrow.
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, pyo3::types::PyCapsule>> {
        let _ = requested_schema;
        if self.consumed.swap(true, Ordering::SeqCst) {
            return Err(PyRuntimeError::new_err(
                "ArrowArrayStream has already been consumed: the Apache Arrow C Data Interface specification enforces single-pass stream consumption.",
            ));
        }
        let ffi_stream = voyager_ogm::arrow::export_batch_to_c_stream(self.batch.clone());
        pyo3::types::PyCapsule::new_with_value(py, ffi_stream, c"arrow_array_stream")
    }

    /// Exports rows as a list of Python dictionaries.
    fn to_dicts<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        record_batch_to_py_dicts(
            &self.batch,
            &[],
            self.bolt_rows.as_deref().map(Vec::as_slice),
            self.row_offset,
            py,
        )
    }

    /// Slices the record batch into chunked batches of `batch_size` rows each, returning PyArrowStreams.
    #[pyo3(signature = (batch_size=10_000))]
    fn iter_batches(&self, batch_size: usize) -> PyResult<Vec<PyArrowStream>> {
        let chunk_size = batch_size.max(1);
        let num_rows = self.batch.num_rows();
        let mut chunks = Vec::new();
        let mut offset = 0;
        while offset < num_rows {
            let length = chunk_size.min(num_rows - offset);
            let sliced = self.batch.slice(offset, length);
            chunks.push(PyArrowStream {
                batch: sliced,
                consumed: Arc::new(AtomicBool::new(false)),
                bolt_rows: self.bolt_rows.clone(),
                row_offset: self.row_offset + offset,
            });
            offset += length;
        }
        Ok(chunks)
    }
}

/// Generates a synthetic Arrow RecordBatch stream for microbenchmarks.
#[pyfunction]
fn generate_synthetic_stream(count: usize) -> PyResult<PyArrowStream> {
    let batch = voyager_ogm::arrow::GraphBatchBuilder::generate_synthetic_nodes(count);
    Ok(PyArrowStream::from_batch(batch))
}

/// Replaces a Rust `Debug` rendering of an AST node in a compile error message
/// with the node kind alone.
///
/// Emitters report failures such as `Unsupported expression node:
/// QueryStatement(..)`; a spec author cannot act on the arena indexes and field
/// values inside that dump, so only the variant name survives.
/// Compiles a query spec dictionary directly into a target dialect query with parameters in a single FFI call.
///
/// The spec must be a Query Spec v1 object (`spec_version: 1`). Any malformed
/// structure raises `VoyagerError` with `code == "INVALID_SPEC"`; the JSON
/// Pointer to the offending element is available as `err.path` and also appears
/// in the message (for example `/matches/0/paths/1/2`). In an expression
/// position every array must be a tagged expression: bare or untagged arrays
/// (an empty array, a non-string first element, or an unknown tag) are rejected,
/// so a literal list is written as `["lit", [...]]` and a list of expressions as
/// `["list", [...]]`. A `where` array whose first element is a string is one
/// expression, while an array whose first element is an array or object is a
/// list of predicates. Node and edge path steps accept an optional properties
/// object (the 4th node item and the 7th edge item) whose values are
/// expressions; omitted, `null` and `{}` mean no properties. Error messages use
/// JSON type names (`string`, `number`, `array`, ...) rather than Python type
/// reprs.
#[pyfunction]
#[pyo3(signature = (spec, dialect="cypher", graph_name=None, optimize=false, optimization_level="standard", use_cache=true))]
fn compile_query_from_spec<'py>(
    py: Python<'py>,
    spec: &Bound<'py, PyAny>,
    dialect: &str,
    graph_name: Option<String>,
    optimize: bool,
    optimization_level: &str,
    use_cache: bool,
) -> PyResult<Bound<'py, PyDict>> {
    let spec = py_spec_to_json_value(spec)?;
    let options = voyager_ogm::spec::SpecCompileOptions {
        graph_name,
        optimize,
        optimization_level: optimization_level.to_string(),
        use_cache,
    };
    let result = voyager_ogm::spec::compile(&spec, dialect, &options).map_err(spec_error_to_py)?;
    for warning in &result.warnings {
        emit_user_warning(py, warning)?;
    }
    compiled_query_to_py_dict(py, &result.compiled)
}

/// Returns telemetry metrics for the global compiled query cache.
#[pyfunction]
fn get_query_cache_stats<'py>(py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
    let m = voyager_ogm::global_query_cache().metrics();
    let dict = PyDict::new(py);
    dict.set_item("hits", m.hits)?;
    dict.set_item("misses", m.misses)?;
    dict.set_item("len", m.len)?;
    dict.set_item("capacity", m.capacity)?;
    dict.set_item("evictions", m.evictions)?;
    dict.set_item("hit_ratio", m.hit_ratio())?;
    Ok(dict)
}

/// Clears all entries from the global compiled query cache.
#[pyfunction]
fn clear_query_cache() {
    voyager_ogm::global_query_cache().clear();
}

/// Returns the native Voyager OGM engine version string.
#[pyfunction]
fn version() -> &'static str {
    voyager_ogm::VERSION
}

/// Native In-Memory Unit of Work for dirty entity state management.
#[pyclass(name = "NativeUnitOfWork")]
pub struct PyUnitOfWork {
    inner: voyager_ogm::transaction::UnitOfWork,
}

#[pymethods]
impl PyUnitOfWork {
    #[new]
    fn new() -> Self {
        Self {
            inner: voyager_ogm::transaction::UnitOfWork::new(),
        }
    }

    #[getter]
    fn len(&self) -> usize {
        self.inner.len()
    }

    #[getter]
    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn clear(&mut self) {
        self.inner.clear();
    }
}

/// Native in-memory Unit of Work scope with savepoints and dirty-state rollback.
#[pyclass(name = "NativeUnitOfWorkScope")]
pub struct PyUnitOfWorkScope {
    inner: voyager_ogm::transaction::Transaction,
    uow: voyager_ogm::transaction::UnitOfWork,
    arena: voyager_ogm::ast::QueryAstArena,
}

#[pymethods]
impl PyUnitOfWorkScope {
    #[new]
    fn new(id: u64) -> Self {
        let uow = voyager_ogm::transaction::UnitOfWork::new();
        let arena = voyager_ogm::ast::QueryAstArena::new();
        let inner = voyager_ogm::transaction::Transaction::new(id, &uow, &arena);
        Self { inner, uow, arena }
    }

    #[getter]
    fn id(&self) -> u64 {
        self.inner.id()
    }

    #[getter]
    fn state(&self) -> String {
        match self.inner.state() {
            voyager_ogm::transaction::TransactionState::Active => "ACTIVE".into(),
            voyager_ogm::transaction::TransactionState::Committed => "COMMITTED".into(),
            voyager_ogm::transaction::TransactionState::RolledBack => "ROLLED_BACK".into(),
        }
    }

    #[getter]
    fn is_active(&self) -> bool {
        self.inner.is_active()
    }

    fn savepoint(&mut self, name: String) -> PyResult<()> {
        self.inner
            .savepoint(name, &self.uow, &self.arena)
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }

    fn rollback_to_savepoint(&mut self, name: String) -> PyResult<()> {
        self.inner
            .rollback_to_savepoint(&name, &mut self.uow, &mut self.arena)
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }

    fn release_savepoint(&mut self, name: String) -> PyResult<()> {
        self.inner
            .release_savepoint(&name)
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }

    fn commit(&mut self) -> PyResult<()> {
        self.inner
            .commit(&mut self.uow)
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }

    fn rollback(&mut self) -> PyResult<()> {
        self.inner
            .rollback(&mut self.uow, &mut self.arena)
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }
}

/// Compiles a high-speed bulk create Cypher/GQL query: `UNWIND $batch AS row CREATE ...`.
#[pyfunction]
#[pyo3(signature = (label, properties, batch_param="batch", row_alias="row", dialect="cypher", graph_name=None))]
fn compile_bulk_create<'py>(
    py: Python<'py>,
    label: String,
    properties: Vec<String>,
    batch_param: &str,
    row_alias: &str,
    dialect: &str,
    graph_name: Option<String>,
) -> PyResult<Bound<'py, PyDict>> {
    let prop_refs: Vec<&str> = properties.iter().map(|s| s.as_str()).collect();
    let compiled = voyager_ogm::bulk::compile_bulk_create_with_graph(
        &label,
        &prop_refs,
        batch_param,
        row_alias,
        dialect,
        graph_name.as_deref(),
    )
    .map_err(|e| make_voyager_err(e.code(), e.to_string()))?;

    let dict = PyDict::new(py);
    dict.set_item("statement", compiled.statement)?;
    let params_dict = PyDict::new(py);
    for (k, v) in compiled.parameters {
        params_dict.set_item(k, literal_to_py(&v, py)?)?;
    }
    dict.set_item("parameters", params_dict)?;
    Ok(dict)
}

/// Compiles a high-speed bulk upsert / MERGE Cypher/GQL query: `UNWIND $batch AS row MERGE ...`.
#[pyfunction]
#[pyo3(signature = (label, key_property, properties, batch_param="batch", row_alias="row", dialect="cypher", graph_name=None))]
fn compile_bulk_merge<'py>(
    label: String,
    key_property: String,
    properties: Bound<'py, PyAny>,
    batch_param: &str,
    row_alias: &str,
    dialect: &str,
    graph_name: Option<String>,
) -> PyResult<Bound<'py, PyDict>> {
    let py = properties.py();
    let prop_strings: Vec<String> = properties.extract()?;
    let prop_refs: Vec<&str> = prop_strings.iter().map(|s| s.as_str()).collect();
    let compiled = voyager_ogm::bulk::compile_bulk_merge_with_graph(
        &label,
        &key_property,
        &prop_refs,
        batch_param,
        row_alias,
        dialect,
        graph_name.as_deref(),
    )
    .map_err(|e| make_voyager_err(e.code(), e.to_string()))?;

    let dict = PyDict::new(py);
    dict.set_item("statement", compiled.statement)?;
    let params_dict = PyDict::new(py);
    for (k, v) in compiled.parameters {
        params_dict.set_item(k, literal_to_py(&v, py)?)?;
    }
    dict.set_item("parameters", params_dict)?;
    Ok(dict)
}

/// Compiles a bulk upsert with properties that are assigned only on creation.
#[pyfunction]
#[pyo3(signature = (label, key_property, properties, create_only_properties, dialect="cypher", graph_name=None))]
fn compile_bulk_merge_create_only<'py>(
    py: Python<'py>,
    label: String,
    key_property: String,
    properties: Vec<String>,
    create_only_properties: Vec<String>,
    dialect: &str,
    graph_name: Option<String>,
) -> PyResult<Bound<'py, PyDict>> {
    let prop_refs: Vec<&str> = properties.iter().map(|s| s.as_str()).collect();
    let create_only_refs: Vec<&str> = create_only_properties.iter().map(|s| s.as_str()).collect();
    let compiled =
        voyager_ogm::bulk::compile_bulk_merge_split(&voyager_ogm::bulk::BulkMergeConfig {
            label: &label,
            key_property: &key_property,
            properties: &prop_refs,
            create_only_properties: &create_only_refs,
            batch_param: "batch",
            row_alias: "row",
            dialect,
            graph_name: graph_name.as_deref(),
        })
        .map_err(|e| make_voyager_err(e.code(), e.to_string()))?;

    let dict = PyDict::new(py);
    dict.set_item("statement", compiled.statement)?;
    let params_dict = PyDict::new(py);
    for (k, v) in compiled.parameters {
        params_dict.set_item(k, literal_to_py(&v, py)?)?;
    }
    dict.set_item("parameters", params_dict)?;
    Ok(dict)
}

/// Compiles a high-speed bulk relationship creation query.
#[pyfunction]
#[allow(clippy::too_many_arguments)]
#[pyo3(signature = (rel_type, properties, from_label, from_key, to_label, to_key, batch_param="batch", row_alias="row", dialect="cypher", graph_name=None))]
fn compile_bulk_create_rel<'py>(
    py: Python<'py>,
    rel_type: String,
    properties: Vec<String>,
    from_label: String,
    from_key: String,
    to_label: String,
    to_key: String,
    batch_param: &str,
    row_alias: &str,
    dialect: &str,
    graph_name: Option<String>,
) -> PyResult<Bound<'py, PyDict>> {
    let prop_refs: Vec<&str> = properties.iter().map(|s| s.as_str()).collect();
    let config = voyager_ogm::bulk::BulkRelConfig {
        rel_type: &rel_type,
        properties: &prop_refs,
        from_label: &from_label,
        from_key: &from_key,
        to_label: &to_label,
        to_key: &to_key,
        batch_param,
        row_alias,
        dialect,
        graph_name: graph_name.as_deref(),
    };
    let compiled = voyager_ogm::bulk::compile_bulk_create_rel_with_config(&config)
        .map_err(|e| make_voyager_err(e.code(), e.to_string()))?;

    let dict = PyDict::new(py);
    dict.set_item("statement", compiled.statement)?;
    let params_dict = PyDict::new(py);
    for (k, v) in compiled.parameters {
        params_dict.set_item(k, literal_to_py(&v, py)?)?;
    }
    dict.set_item("parameters", params_dict)?;
    Ok(dict)
}

/// Converts a Python Query Spec value into `serde_json::Value`.
///
/// Tuples and lists both become JSON arrays. Dicts keep their insertion order
/// (the workspace enables `serde_json`'s `preserve_order` feature, and pattern
/// property order decides parameter numbering). A non-string key is encoded
/// with [`voyager_ogm::spec::encode_non_string_key`] so the Rust consumer can
/// report the original JSON type at the containing object, exactly as the
/// Python-only consumer did.
fn py_spec_to_json_value(val: &Bound<'_, PyAny>) -> PyResult<serde_json::Value> {
    if val.is_none() {
        Ok(serde_json::Value::Null)
    } else if let Ok(b) = val.cast::<PyBool>() {
        Ok(serde_json::Value::Bool(b.is_true()))
    } else if let Ok(i) = val.cast::<PyInt>() {
        let val_i: i64 = i.extract()?;
        Ok(serde_json::Value::Number(val_i.into()))
    } else if let Ok(f) = val.cast::<PyFloat>() {
        if let Some(n) = serde_json::Number::from_f64(f.value()) {
            Ok(serde_json::Value::Number(n))
        } else {
            Ok(serde_json::Value::Null)
        }
    } else if let Ok(s) = val.cast::<PyString>() {
        Ok(serde_json::Value::String(s.to_string_lossy().into_owned()))
    } else if let Ok(list) = val.cast::<PyList>() {
        let mut items = Vec::with_capacity(list.len());
        for item in list {
            items.push(py_spec_to_json_value(&item)?);
        }
        Ok(serde_json::Value::Array(items))
    } else if let Ok(tuple) = val.cast::<PyTuple>() {
        let mut items = Vec::with_capacity(tuple.len());
        for item in tuple {
            items.push(py_spec_to_json_value(&item)?);
        }
        Ok(serde_json::Value::Array(items))
    } else if let Ok(dict) = val.cast::<PyDict>() {
        let mut map = serde_json::Map::with_capacity(dict.len());
        for (k, v) in dict {
            let key = if let Ok(s) = k.cast::<PyString>() {
                s.to_string_lossy().into_owned()
            } else {
                voyager_ogm::spec::encode_non_string_key(json_type_name(&k))
            };
            map.insert(key, py_spec_to_json_value(&v)?);
        }
        Ok(serde_json::Value::Object(map))
    } else if val.hasattr("to_spec_dict")? {
        // A Query, NativeQueryBuilder or Expression object in a nested spec
        // position: serialise it through its own Query Spec representation so
        // the Rust consumer sees a plain JSON document.
        let spec = val.call_method0("to_spec_dict")?;
        py_spec_to_json_value(&spec)
    } else if val.hasattr("isoformat")? {
        let iso: String = val.call_method0("isoformat")?.extract()?;
        Ok(serde_json::Value::String(iso))
    } else if val.hasattr("hex")? && val.hasattr("urn")? {
        let s: String = val.str()?.extract()?;
        Ok(serde_json::Value::String(s))
    } else if val.get_type().name()? == "Decimal" {
        if let Ok(f) = val
            .call_method0("__float__")
            .and_then(|v| v.extract::<f64>())
        {
            if let Some(n) = serde_json::Number::from_f64(f) {
                Ok(serde_json::Value::Number(n))
            } else {
                Ok(serde_json::Value::Null)
            }
        } else {
            let s: String = val.str()?.extract()?;
            Ok(serde_json::Value::String(s))
        }
    } else if val.hasattr("to_cypher_dict")? {
        let cypher_dict = val.call_method0("to_cypher_dict")?;
        py_spec_to_json_value(&cypher_dict)
    } else {
        let s: String = val.str()?.extract()?;
        Ok(serde_json::Value::String(s))
    }
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

fn py_dict_to_params(
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

/// Inspects the typed [`ParamValue`] representations produced for a Python parameter dict (for testing).
#[pyfunction]
#[pyo3(signature = (params=None))]
fn inspect_native_params(params: Option<Bound<'_, PyDict>>) -> PyResult<String> {
    let converted = py_dict_to_params(params)?;
    let deterministic: std::collections::BTreeMap<String, voyager_drivers::ParamValue> =
        converted.into_iter().collect();
    serde_json::to_string(&deterministic).map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Native query result containing columnar Apache Arrow record batch and execution summary counters.
#[pyclass(name = "NativeQueryResult", from_py_object)]
#[derive(Clone)]
pub struct PyNativeQueryResult {
    stream: PyArrowStream,
    columns: Vec<String>,
    summary: voyager_drivers::QuerySummary,
    age_columns: Vec<bool>,
}

#[pymethods]
impl PyNativeQueryResult {
    /// Ordered list of projected return column names.
    #[getter]
    fn columns(&self) -> Vec<String> {
        self.columns.clone()
    }

    /// Number of rows in the result batch.
    #[getter]
    fn num_rows(&self) -> usize {
        self.stream.num_rows()
    }

    /// Number of columns in the result batch.
    #[getter]
    fn num_columns(&self) -> usize {
        self.stream.num_columns()
    }

    /// Returns whether this stream has already been consumed.
    #[getter]
    fn is_consumed(&self) -> bool {
        self.stream.is_consumed()
    }

    /// Number of graph nodes created by the query.
    #[getter]
    fn nodes_created(&self) -> u64 {
        self.summary.nodes_created
    }

    /// Number of graph nodes deleted by the query.
    #[getter]
    fn nodes_deleted(&self) -> u64 {
        self.summary.nodes_deleted
    }

    /// Number of graph relationships created by the query.
    #[getter]
    fn relationships_created(&self) -> u64 {
        self.summary.relationships_created
    }

    /// Number of graph relationships deleted by the query.
    #[getter]
    fn relationships_deleted(&self) -> u64 {
        self.summary.relationships_deleted
    }

    /// Number of properties set or updated on nodes/relationships.
    #[getter]
    fn properties_set(&self) -> u64 {
        self.summary.properties_set
    }

    /// Total server-side execution duration in milliseconds.
    #[getter]
    fn execution_time_ms(&self) -> u64 {
        self.summary.execution_time_ms
    }

    /// Whether any projected column is Apache AGE `agtype` and needs decoding.
    #[getter]
    fn has_age_columns(&self) -> bool {
        self.age_columns.iter().any(|is_age| *is_age)
    }

    /// Access to the underlying ArrowStream.
    #[getter]
    fn stream(&self) -> PyArrowStream {
        self.stream.clone()
    }

    /// Official Python Arrow PyCapsule Protocol for zero-copy streaming into Polars / PyArrow.
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, pyo3::types::PyCapsule>> {
        self.stream.__arrow_c_stream__(py, requested_schema)
    }

    /// Exports rows as a list of Python dictionaries.
    fn to_dicts<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        record_batch_to_py_dicts(
            &self.stream.batch,
            &self.age_columns,
            self.stream.bolt_rows.as_deref().map(Vec::as_slice),
            self.stream.row_offset,
            py,
        )
    }

    /// Access to result records as a list of Python dictionaries.
    #[getter]
    fn records<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        self.to_dicts(py)
    }

    /// Access to this result's execution summary metrics.
    #[getter]
    fn summary(slf: Bound<'_, Self>) -> Bound<'_, Self> {
        slf
    }
}

fn empty_batch_for_columns(columns: &[String]) -> arrow_array::RecordBatch {
    if columns.is_empty() {
        return arrow_array::RecordBatch::new_empty(std::sync::Arc::new(
            arrow_schema::Schema::empty(),
        ));
    }
    let fields: Vec<arrow_schema::Field> = columns
        .iter()
        .map(|name| arrow_schema::Field::new(name, arrow_schema::DataType::Utf8, true))
        .collect();
    arrow_array::RecordBatch::new_empty(std::sync::Arc::new(arrow_schema::Schema::new(fields)))
}

fn query_result_to_py(query_result: voyager_drivers::QueryResult) -> PyResult<PyNativeQueryResult> {
    let columns = query_result.columns.clone();
    let summary = query_result.summary.clone();
    let age_columns = query_result.age_columns.clone();
    let bolt_rows = query_result.bolt_rows.clone();
    let batch = query_result
        .into_single_batch()
        .map_err(|e| PyValueError::new_err(e.to_string()))?
        .unwrap_or_else(|| empty_batch_for_columns(&columns));

    Ok(PyNativeQueryResult {
        stream: PyArrowStream::from_batch_with_bolt(batch, bolt_rows),
        columns,
        summary,
        age_columns,
    })
}

use std::sync::atomic::{AtomicU32, Ordering as AtomicOrdering};

static RUNTIME_PID: AtomicU32 = AtomicU32::new(0);
static ACTIVE_RUNTIME: parking_lot::RwLock<Option<Arc<tokio::runtime::Runtime>>> =
    parking_lot::RwLock::new(None);

/// Returns a shared `Arc<tokio::runtime::Runtime>` guaranteed healthy for the current OS process.
///
/// In multi-process async web environments (`gunicorn -w 4 -k uvicorn.workers.UvicornWorker`),
/// worker processes are spawned via Unix `os.fork()`. Forking copies address space but terminates
/// all background OS worker threads and epoll/IOCP event loop state.
///
/// This accessor compares the stored process PID against `std::process::id()`.
/// If a fork is detected, it cleanly creates a fresh multi-threaded Tokio runtime for the child.
pub fn get_tokio_runtime() -> Arc<tokio::runtime::Runtime> {
    let current_pid = std::process::id();
    let stored_pid = RUNTIME_PID.load(AtomicOrdering::Acquire);

    if stored_pid == current_pid
        && let Some(ref rt) = *ACTIVE_RUNTIME.read()
    {
        return rt.clone();
    }

    let mut guard = ACTIVE_RUNTIME.write();
    let stored_pid = RUNTIME_PID.load(AtomicOrdering::Acquire);
    if stored_pid == current_pid
        && let Some(ref rt) = *guard
    {
        return rt.clone();
    }

    let new_rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Failed to initialize fork-safe Tokio runtime for Voyager native client"),
    );

    *guard = Some(new_rt.clone());
    RUNTIME_PID.store(current_pid, AtomicOrdering::Release);
    new_rt
}

/// Returns the process ID of the active Tokio runtime for fork-safety diagnostics.
#[pyfunction]
fn get_runtime_pid() -> u32 {
    let _ = get_tokio_runtime();
    RUNTIME_PID.load(AtomicOrdering::Acquire)
}

/// Native asynchronous Rust network client managing connection pooling and wire protocols.
#[pyclass(name = "NativeClient")]
pub struct PyNativeClient {
    client: voyager_drivers::NativeClient,
}

#[pymethods]
impl PyNativeClient {
    /// Creates a new `NativeClient` connecting to the given URI with connection pooling.
    ///
    /// TLS is chosen by the URI: `bolt+s://`, `rediss://`, PostgreSQL `?sslmode=...`, with
    /// `tls_ca_file` / `sslrootcert`, `tls_cert_file` / `sslcert`, `tls_key_file` / `sslkey`
    /// and `tls_server_name` parameters.
    ///
    /// `age=True` runs Apache AGE setup on every PostgreSQL connection even when the URI
    /// does not name AGE, so a Session can select AGE through its dialect or graph name.
    #[new]
    #[pyo3(signature = (uri, min_idle=1, max_size=10, age=false))]
    fn new(uri: &str, min_idle: usize, max_size: usize, age: bool) -> PyResult<Self> {
        let pool_config = voyager_drivers::PoolConfig {
            min_idle,
            max_size,
            ..Default::default()
        };
        let client = voyager_drivers::NativeClient::new_with_options(
            uri,
            Some(pool_config),
            voyager_drivers::ConnectOptions { age },
        )
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(Self { client })
    }

    /// Returns the target connection URI.
    #[getter]
    fn uri(&self) -> &str {
        self.client.uri()
    }

    /// Returns the active database protocol name.
    #[getter]
    fn protocol(&self) -> String {
        self.client.protocol().to_string()
    }

    /// Synchronously executes a query releasing the Python GIL during network I/O.
    #[pyo3(signature = (query, params=None))]
    fn execute_sync<'py>(
        &self,
        py: Python<'py>,
        query: String,
        params: Option<Bound<'py, PyDict>>,
    ) -> PyResult<PyNativeQueryResult> {
        let params_map = py_dict_to_params(params)?;
        let client = self.client.clone();
        let query_result = py
            .detach(|| {
                get_tokio_runtime()
                    .block_on(async move { client.execute(&query, &params_map).await })
            })
            .map_err(|e| PyValueError::new_err(e.to_string()))?;

        query_result_to_py(query_result)
    }

    /// Asynchronously executes a query returning a Python asyncio Future.
    #[pyo3(signature = (query, params=None))]
    fn execute<'py>(
        &self,
        py: Python<'py>,
        query: String,
        params: Option<Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let params_map = py_dict_to_params(params)?;
        let client = self.client.clone();
        let _ = get_tokio_runtime();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let query_result = client
                .execute(&query, &params_map)
                .await
                .map_err(|e| PyValueError::new_err(e.to_string()))?;

            query_result_to_py(query_result)
        })
    }

    /// Synchronously checks database connectivity.
    fn ping_sync(&self, py: Python<'_>) -> PyResult<bool> {
        let client = self.client.clone();
        py.detach(|| get_tokio_runtime().block_on(async move { client.ping().await }))
            .map(|_| true)
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }

    /// Asynchronously checks database connectivity.
    fn ping<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        let _ = get_tokio_runtime();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .ping()
                .await
                .map(|_| true)
                .map_err(|e| PyValueError::new_err(e.to_string()))
        })
    }

    /// Synchronously leases one pooled connection and begins a native transaction.
    fn begin_transaction_sync(&self, py: Python<'_>) -> PyResult<PyNativeTransaction> {
        let client = self.client.clone();
        let tx = py
            .detach(|| {
                get_tokio_runtime().block_on(async move { client.begin_transaction().await })
            })
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(PyNativeTransaction::from_handle(tx))
    }

    /// Asynchronously leases one pooled connection and begins a native transaction.
    fn begin_transaction<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        let _ = get_tokio_runtime();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let tx = client
                .begin_transaction()
                .await
                .map_err(|e| PyValueError::new_err(e.to_string()))?;
            Ok(PyNativeTransaction::from_handle(tx))
        })
    }

    /// Closes the connection pool.
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        let client = self.client.clone();
        py.detach(|| get_tokio_runtime().block_on(async move { client.close().await }));
        Ok(())
    }
}

type NativeTransactionHandle =
    voyager_drivers::NativeTransaction<Box<dyn voyager_drivers::AsyncConnection>>;

const TX_STATE_ACTIVE: u8 = 1;
const TX_STATE_COMMITTED: u8 = 2;
const TX_STATE_ROLLED_BACK: u8 = 3;
const TX_STATE_FAILED: u8 = 4;

/// Marks a PyO3 transaction failed if its future is cancelled mid-await.
///
/// The taken [`NativeTransaction`] is dropped on cancellation, which marks its
/// pooled connection broken (never RESET/reused with in-flight frames). The
/// guard additionally records the FAILED lifecycle state so Python sees an
/// aborted transaction instead of a stale ACTIVE handle with no connection.
struct TxFutureGuard {
    inner: Arc<parking_lot::Mutex<Option<NativeTransactionHandle>>>,
    state: Arc<AtomicU8>,
    armed: bool,
}

impl TxFutureGuard {
    fn new(
        inner: Arc<parking_lot::Mutex<Option<NativeTransactionHandle>>>,
        state: Arc<AtomicU8>,
    ) -> Self {
        Self {
            inner,
            state,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for TxFutureGuard {
    fn drop(&mut self) {
        if self.armed {
            self.state.store(TX_STATE_FAILED, Ordering::Release);
            self.inner.lock().take();
        }
    }
}

fn native_transaction_state_code(state: voyager_drivers::NativeTransactionState) -> u8 {
    match state {
        voyager_drivers::NativeTransactionState::Active => TX_STATE_ACTIVE,
        voyager_drivers::NativeTransactionState::Failed => TX_STATE_FAILED,
        voyager_drivers::NativeTransactionState::Committed => TX_STATE_COMMITTED,
        voyager_drivers::NativeTransactionState::RolledBack => TX_STATE_ROLLED_BACK,
    }
}

/// Native explicit database transaction holding one pooled connection.
#[pyclass(name = "NativeTransaction")]
pub struct PyNativeTransaction {
    inner: Arc<parking_lot::Mutex<Option<NativeTransactionHandle>>>,
    state: Arc<AtomicU8>,
    supports_savepoints: bool,
}

impl PyNativeTransaction {
    fn from_handle(tx: NativeTransactionHandle) -> Self {
        let supports_savepoints = tx.supports_savepoints();
        Self {
            inner: Arc::new(parking_lot::Mutex::new(Some(tx))),
            state: Arc::new(AtomicU8::new(TX_STATE_ACTIVE)),
            supports_savepoints,
        }
    }

    fn ensure_active(&self) -> PyResult<()> {
        match self.state.load(Ordering::Acquire) {
            TX_STATE_ACTIVE => Ok(()),
            TX_STATE_FAILED => Err(PyRuntimeError::new_err(
                "transaction is aborted; roll back before executing or committing more statements",
            )),
            _ => Err(PyRuntimeError::new_err(
                "Transaction is not active; it was already committed or rolled back",
            )),
        }
    }

    fn ensure_rollback_allowed(&self) -> PyResult<()> {
        match self.state.load(Ordering::Acquire) {
            TX_STATE_ACTIVE | TX_STATE_FAILED => Ok(()),
            _ => Err(PyRuntimeError::new_err(
                "Transaction is not active; it was already committed or rolled back",
            )),
        }
    }

    fn sync_state_from_handle(&self, tx: &NativeTransactionHandle) {
        self.state
            .store(native_transaction_state_code(tx.state()), Ordering::Release);
    }

    fn take(&self) -> PyResult<NativeTransactionHandle> {
        self.inner.lock().take().ok_or_else(|| {
            PyRuntimeError::new_err("Transaction is not active; no pooled connection is leased")
        })
    }

    fn put(&self, tx: NativeTransactionHandle) {
        *self.inner.lock() = Some(tx);
    }

    fn ensure_savepoints(&self) -> PyResult<()> {
        if self.supports_savepoints {
            Ok(())
        } else {
            Err(PyRuntimeError::new_err(
                "Savepoints are not supported by this backend (Bolt/Neo4j/Memgraph has no savepoints)",
            ))
        }
    }
}

#[pymethods]
impl PyNativeTransaction {
    /// Returns the lifecycle state ('ACTIVE', 'COMMITTED', or 'ROLLED_BACK').
    #[getter]
    fn state(&self) -> &'static str {
        match self.state.load(Ordering::Acquire) {
            TX_STATE_COMMITTED => "COMMITTED",
            TX_STATE_ROLLED_BACK => "ROLLED_BACK",
            TX_STATE_FAILED => "FAILED",
            _ => "ACTIVE",
        }
    }

    /// Returns `true` while the transaction accepts statements.
    #[getter]
    fn is_active(&self) -> bool {
        self.state.load(Ordering::Acquire) == TX_STATE_ACTIVE
    }

    /// Returns `true` when this transaction supports real savepoints.
    #[getter]
    fn supports_savepoints(&self) -> bool {
        self.supports_savepoints
    }

    /// Synchronously executes a statement inside the transaction.
    #[pyo3(signature = (query, params=None))]
    fn execute_sync(
        &self,
        py: Python<'_>,
        query: String,
        params: Option<Bound<'_, PyDict>>,
    ) -> PyResult<PyNativeQueryResult> {
        self.ensure_active()?;
        let params_map = py_dict_to_params(params)?;
        let mut tx = self.take()?;
        let (tx, result) = py.detach(|| {
            let result =
                get_tokio_runtime().block_on(async { tx.execute(&query, &params_map).await });
            (tx, result)
        });
        self.sync_state_from_handle(&tx);
        self.put(tx);
        query_result_to_py(result.map_err(|e| PyValueError::new_err(e.to_string()))?)
    }

    /// Asynchronously executes a statement inside the transaction.
    #[pyo3(signature = (query, params=None))]
    fn execute<'py>(
        &self,
        py: Python<'py>,
        query: String,
        params: Option<Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_active()?;
        let params_map = py_dict_to_params(params)?;
        let inner = self.inner.clone();
        let state = self.state.clone();
        let _ = get_tokio_runtime();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = TxFutureGuard::new(inner.clone(), state.clone());
            let mut tx = match inner.lock().take() {
                Some(tx) => tx,
                None => {
                    guard.disarm();
                    return Err(PyRuntimeError::new_err("Transaction is not active"));
                }
            };
            let result = tx.execute(&query, &params_map).await;
            let tx_state = tx.state();
            *inner.lock() = Some(tx);
            state.store(native_transaction_state_code(tx_state), Ordering::Release);
            guard.disarm();
            query_result_to_py(result.map_err(|e| PyValueError::new_err(e.to_string()))?)
        })
    }

    /// Synchronously commits the transaction.
    fn commit_sync(&self, py: Python<'_>) -> PyResult<()> {
        self.ensure_active()?;
        let mut tx = self.take()?;
        let (tx, result) = py.detach(|| {
            let result = get_tokio_runtime().block_on(async { tx.commit().await });
            (tx, result)
        });
        match result {
            Ok(_) => {
                self.state.store(TX_STATE_COMMITTED, Ordering::Release);
                Ok(())
            }
            Err(err) => {
                self.sync_state_from_handle(&tx);
                self.put(tx);
                Err(PyValueError::new_err(err.to_string()))
            }
        }
    }

    /// Asynchronously commits the transaction.
    fn commit<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_active()?;
        let inner = self.inner.clone();
        let state = self.state.clone();
        let _ = get_tokio_runtime();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = TxFutureGuard::new(inner.clone(), state.clone());
            let mut tx = match inner.lock().take() {
                Some(tx) => tx,
                None => {
                    guard.disarm();
                    return Err(PyRuntimeError::new_err("Transaction is not active"));
                }
            };
            match tx.commit().await {
                Ok(_) => {
                    state.store(TX_STATE_COMMITTED, Ordering::Release);
                    guard.disarm();
                    Ok(())
                }
                Err(err) => {
                    let tx_state = tx.state();
                    *inner.lock() = Some(tx);
                    state.store(native_transaction_state_code(tx_state), Ordering::Release);
                    guard.disarm();
                    Err(PyValueError::new_err(err.to_string()))
                }
            }
        })
    }

    /// Synchronously rolls the transaction back.
    fn rollback_sync(&self, py: Python<'_>) -> PyResult<()> {
        self.ensure_rollback_allowed()?;
        let mut tx = self.take()?;
        let (tx, result) = py.detach(|| {
            let result = get_tokio_runtime().block_on(async { tx.rollback().await });
            (tx, result)
        });
        match result {
            Ok(()) => {
                self.state.store(TX_STATE_ROLLED_BACK, Ordering::Release);
                Ok(())
            }
            Err(err) => {
                self.sync_state_from_handle(&tx);
                self.put(tx);
                Err(PyValueError::new_err(err.to_string()))
            }
        }
    }

    /// Asynchronously rolls the transaction back.
    fn rollback<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_rollback_allowed()?;
        let inner = self.inner.clone();
        let state = self.state.clone();
        let _ = get_tokio_runtime();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = TxFutureGuard::new(inner.clone(), state.clone());
            let mut tx = match inner.lock().take() {
                Some(tx) => tx,
                None => {
                    guard.disarm();
                    return Err(PyRuntimeError::new_err("Transaction is not active"));
                }
            };
            match tx.rollback().await {
                Ok(()) => {
                    state.store(TX_STATE_ROLLED_BACK, Ordering::Release);
                    guard.disarm();
                    Ok(())
                }
                Err(err) => {
                    let tx_state = tx.state();
                    *inner.lock() = Some(tx);
                    state.store(native_transaction_state_code(tx_state), Ordering::Release);
                    guard.disarm();
                    Err(PyValueError::new_err(err.to_string()))
                }
            }
        })
    }

    /// Synchronously creates a real database savepoint.
    fn savepoint_sync(&self, py: Python<'_>, name: String) -> PyResult<()> {
        self.ensure_active()?;
        self.ensure_savepoints()?;
        let mut tx = self.take()?;
        let (tx, result) = py.detach(|| {
            let result = get_tokio_runtime().block_on(async { tx.savepoint(&name).await });
            (tx, result)
        });
        self.sync_state_from_handle(&tx);
        self.put(tx);
        result.map_err(|e| PyValueError::new_err(e.to_string()))
    }

    /// Asynchronously creates a real database savepoint.
    fn savepoint<'py>(&self, py: Python<'py>, name: String) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_active()?;
        self.ensure_savepoints()?;
        let inner = self.inner.clone();
        let state = self.state.clone();
        let _ = get_tokio_runtime();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = TxFutureGuard::new(inner.clone(), state.clone());
            let mut tx = match inner.lock().take() {
                Some(tx) => tx,
                None => {
                    guard.disarm();
                    return Err(PyRuntimeError::new_err("Transaction is not active"));
                }
            };
            let result = tx.savepoint(&name).await;
            let tx_state = tx.state();
            *inner.lock() = Some(tx);
            state.store(native_transaction_state_code(tx_state), Ordering::Release);
            guard.disarm();
            result.map_err(|e| PyValueError::new_err(e.to_string()))
        })
    }

    /// Synchronously rolls the transaction back to a savepoint.
    fn rollback_to_savepoint_sync(&self, py: Python<'_>, name: String) -> PyResult<()> {
        self.ensure_active()?;
        self.ensure_savepoints()?;
        let mut tx = self.take()?;
        let (tx, result) = py.detach(|| {
            let result =
                get_tokio_runtime().block_on(async { tx.rollback_to_savepoint(&name).await });
            (tx, result)
        });
        self.sync_state_from_handle(&tx);
        self.put(tx);
        result.map_err(|e| PyValueError::new_err(e.to_string()))
    }

    /// Asynchronously rolls the transaction back to a savepoint.
    fn rollback_to_savepoint<'py>(
        &self,
        py: Python<'py>,
        name: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_active()?;
        self.ensure_savepoints()?;
        let inner = self.inner.clone();
        let state = self.state.clone();
        let _ = get_tokio_runtime();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = TxFutureGuard::new(inner.clone(), state.clone());
            let mut tx = match inner.lock().take() {
                Some(tx) => tx,
                None => {
                    guard.disarm();
                    return Err(PyRuntimeError::new_err("Transaction is not active"));
                }
            };
            let result = tx.rollback_to_savepoint(&name).await;
            let tx_state = tx.state();
            *inner.lock() = Some(tx);
            state.store(native_transaction_state_code(tx_state), Ordering::Release);
            guard.disarm();
            result.map_err(|e| PyValueError::new_err(e.to_string()))
        })
    }

    /// Synchronously releases a database savepoint.
    fn release_savepoint_sync(&self, py: Python<'_>, name: String) -> PyResult<()> {
        self.ensure_active()?;
        self.ensure_savepoints()?;
        let mut tx = self.take()?;
        let (tx, result) = py.detach(|| {
            let result = get_tokio_runtime().block_on(async { tx.release_savepoint(&name).await });
            (tx, result)
        });
        self.sync_state_from_handle(&tx);
        self.put(tx);
        result.map_err(|e| PyValueError::new_err(e.to_string()))
    }

    /// Asynchronously releases a database savepoint.
    fn release_savepoint<'py>(&self, py: Python<'py>, name: String) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_active()?;
        self.ensure_savepoints()?;
        let inner = self.inner.clone();
        let state = self.state.clone();
        let _ = get_tokio_runtime();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = TxFutureGuard::new(inner.clone(), state.clone());
            let mut tx = match inner.lock().take() {
                Some(tx) => tx,
                None => {
                    guard.disarm();
                    return Err(PyRuntimeError::new_err("Transaction is not active"));
                }
            };
            let result = tx.release_savepoint(&name).await;
            let tx_state = tx.state();
            *inner.lock() = Some(tx);
            state.store(native_transaction_state_code(tx_state), Ordering::Release);
            guard.disarm();
            result.map_err(|e| PyValueError::new_err(e.to_string()))
        })
    }
}

/// Native thread-safe circuit breaker and backend router for Voyager sessions.
#[pyclass(name = "NativeCircuitRouter")]
pub struct PyNativeCircuitRouter {
    router: voyager_drivers::CircuitRouter,
}

#[pymethods]
impl PyNativeCircuitRouter {
    /// Creates a new `NativeCircuitRouter` with configurable failure threshold, cooldown, and success threshold.
    #[new]
    #[pyo3(signature = (failure_threshold=1, cooldown_seconds=30.0, success_threshold=1))]
    fn new(failure_threshold: u32, cooldown_seconds: f64, success_threshold: u32) -> Self {
        let config = voyager_drivers::CircuitConfig::new(
            failure_threshold,
            std::time::Duration::from_secs_f64(cooldown_seconds.max(0.0)),
        )
        .with_success_threshold(success_threshold);
        Self {
            router: voyager_drivers::CircuitRouter::new(config),
        }
    }

    /// Returns the current state of the circuit breaker ("closed", "open", "half_open").
    #[getter]
    fn state(&self) -> &'static str {
        self.router.state().as_str()
    }

    /// Determines the routing destination for an incoming query ("native", "fallback", "probe").
    fn route(&self) -> &'static str {
        self.router.route().as_str()
    }

    /// Returns `true` if the cooldown has elapsed and a trial probe should be executed.
    fn should_probe(&self) -> bool {
        self.router.should_probe()
    }

    /// Returns `true` if the query should be routed to the native engine or probed.
    fn should_route_native(&self) -> bool {
        self.router.should_route_native()
    }

    /// Records a query failure, tripping the circuit if transient and threshold reached.
    #[pyo3(signature = (is_transient=true))]
    fn record_failure(&self, is_transient: bool) {
        self.router.record_failure(is_transient);
    }

    /// Records a successful query execution, resetting failures or closing a half-open circuit.
    fn record_success(&self) {
        self.router.record_success();
    }

    /// Classifies an error message and records it, returning `true` if counted as transient.
    fn record_error(&self, error_msg: &str) -> bool {
        self.router.record_error(error_msg)
    }

    /// Manually trips the circuit breaker to `Open`.
    fn trip(&self) {
        self.router.trip();
    }

    /// Manually resets the circuit breaker to `Closed`.
    fn reset(&self) {
        self.router.reset();
    }

    /// Returns the current count of consecutive transient failures.
    #[getter]
    fn consecutive_failures(&self) -> u32 {
        self.router.consecutive_failures()
    }

    /// Returns the current count of consecutive successful probes.
    #[getter]
    fn consecutive_successes(&self) -> u32 {
        self.router.consecutive_successes()
    }

    /// Returns the configured failure threshold.
    #[getter]
    fn failure_threshold(&self) -> u32 {
        self.router.failure_threshold()
    }

    /// Returns the configured cooldown duration in seconds.
    #[getter]
    fn cooldown_seconds(&self) -> f64 {
        self.router.cooldown_seconds()
    }

    /// Returns the remaining cooldown duration in seconds if currently Open, or `None`.
    fn remaining_cooldown(&self) -> Option<f64> {
        self.router.remaining_cooldown().map(|d| d.as_secs_f64())
    }

    /// Returns an immutable telemetry snapshot dictionary of the circuit breaker metrics.
    fn snapshot<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let snap = self.router.snapshot();
        let dict = PyDict::new(py);
        dict.set_item("state", snap.state.as_str())?;
        dict.set_item("consecutive_failures", snap.consecutive_failures)?;
        dict.set_item("consecutive_successes", snap.consecutive_successes)?;
        dict.set_item("failure_threshold", snap.failure_threshold)?;
        dict.set_item("cooldown_seconds", snap.cooldown_seconds)?;
        dict.set_item(
            "remaining_cooldown_seconds",
            snap.remaining_cooldown_seconds,
        )?;
        Ok(dict)
    }

    /// Checks if an error string represents a query syntax, semantic, or constraint error.
    #[staticmethod]
    fn is_semantic_error(error_msg: &str) -> bool {
        voyager_drivers::is_semantic_error(error_msg)
    }

    /// Classifies an error string as "semantic" or "transient".
    #[staticmethod]
    fn classify_error(error_msg: &str) -> &'static str {
        voyager_drivers::classify_error(error_msg).as_str()
    }

    fn __repr__(&self) -> String {
        format!(
            "NativeCircuitRouter(state='{}', failures={}, threshold={}, cooldown={:.2}s)",
            self.router.state().as_str(),
            self.router.consecutive_failures(),
            self.router.failure_threshold(),
            self.router.cooldown_seconds(),
        )
    }
}

/// Maps a Python field type annotation to a [`FieldType`].
///
/// `Optional[T]` and `T | None` map to the type of `T`; any other union maps to
/// [`FieldType::Any`]. Other annotations are named by their `__name__`.
fn annotation_field_type(ann: &Bound<'_, PyAny>) -> PyResult<FieldType> {
    if ann.is_none() {
        return Ok(FieldType::Any);
    }
    if let Ok(s) = ann.extract::<String>() {
        return Ok(FieldType::parse_str(&s));
    }
    let py = ann.py();
    let typing = py.import("typing")?;
    let origin = typing.call_method1("get_origin", (ann,))?;
    let is_union = !origin.is_none()
        && (origin.is(&typing.getattr("Union")?)
            || origin.is(&py.import("types")?.getattr("UnionType")?));
    if is_union {
        let none_type = py.None().bind(py).get_type();
        let args = typing.call_method1("get_args", (ann,))?;
        let mut members = Vec::new();
        for arg in args.try_iter()? {
            let arg = arg?;
            if !arg.is(&none_type) {
                members.push(arg);
            }
        }
        return match members.as_slice() {
            [single] => annotation_field_type(single),
            _ => Ok(FieldType::Any),
        };
    }
    if ann.hasattr("__name__")? {
        let s: String = ann.getattr("__name__")?.extract()?;
        return Ok(FieldType::parse_str(&s));
    }
    Ok(FieldType::Any)
}

fn parse_field_descriptor(name: &str, obj: &Bound<'_, PyAny>) -> PyResult<FieldDescriptor> {
    if let Ok(dict) = obj.cast::<PyDict>() {
        let field_name = if let Some(n) = dict.get_item("name")? {
            if n.is_none() {
                name.to_string()
            } else {
                let s: String = n.extract()?;
                if s.is_empty() { name.to_string() } else { s }
            }
        } else {
            name.to_string()
        };

        if field_name.is_empty() {
            return Err(PyValueError::new_err(
                "Field descriptor missing property name (key or 'name' attribute)",
            ));
        }

        let primary_key = dict
            .get_item("primary_key")?
            .and_then(|v| v.extract::<bool>().ok())
            .unwrap_or(false);

        let unique = dict
            .get_item("unique")?
            .and_then(|v| v.extract::<bool>().ok())
            .unwrap_or(primary_key);

        let nullable = dict
            .get_item("nullable")?
            .and_then(|v| v.extract::<bool>().ok())
            .unwrap_or(!primary_key);

        let nullable_explicit = match dict.get_item("nullable_explicit")? {
            Some(value) => value.extract::<Option<bool>>().unwrap_or(None),
            // Dictionaries written before inference only had `nullable`; a value
            // there was always an explicit request.
            None => dict
                .get_item("nullable")?
                .and_then(|v| v.extract::<bool>().ok()),
        };
        let nullable_inferred = nullable_explicit.is_none() && !nullable && !primary_key;

        let mut indexed = dict
            .get_item("indexed")?
            .or(dict.get_item("index")?)
            .and_then(|v| v.extract::<bool>().ok())
            .unwrap_or(false);

        let mut index_type = if let Some(it) = dict.get_item("index_type")? {
            if it.is_none() {
                if indexed {
                    Some(IndexType::BTree)
                } else {
                    None
                }
            } else {
                let it_str: String = it.extract()?;
                indexed = true;
                IndexType::parse_str(&it_str)
            }
        } else if indexed {
            Some(IndexType::BTree)
        } else {
            None
        };

        let field_type = if let Some(t) = dict.get_item("type")?.or(dict.get_item("field_type")?) {
            if let Ok(s) = t.extract::<String>() {
                FieldType::parse_str(&s)
            } else if t.hasattr("__name__")? {
                let s: String = t.getattr("__name__")?.extract()?;
                FieldType::parse_str(&s)
            } else {
                FieldType::Any
            }
        } else {
            FieldType::Any
        };

        let default_value = if let Some(dv) = dict
            .get_item("default_value")?
            .or(dict.get_item("default")?)
        {
            if dv.is_none() {
                None
            } else {
                py_to_literal(&dv).ok()
            }
        } else {
            None
        };

        let mut vector_config = None;
        if index_type == Some(IndexType::Vector)
            || dict.contains("dimensions")?
            || dict.contains("vector_config")?
        {
            let dims: usize = if let Some(d) = dict.get_item("dimensions")? {
                if d.is_none() {
                    DEFAULT_VECTOR_DIMENSIONS
                } else {
                    match d.extract::<usize>() {
                        Ok(v) if v > 0 => v,
                        Ok(_) => {
                            return Err(PyValueError::new_err(
                                "Vector dimensions must be a positive integer greater than 0",
                            ));
                        }
                        Err(e) => {
                            return Err(PyValueError::new_err(format!(
                                "Invalid vector dimensions: expected positive integer, got: {e}"
                            )));
                        }
                    }
                }
            } else {
                DEFAULT_VECTOR_DIMENSIONS
            };
            let sim_str: String = if let Some(s) = dict.get_item("similarity")? {
                s.extract().unwrap_or_else(|_| "cosine".to_string())
            } else {
                "cosine".to_string()
            };
            let similarity =
                VectorSimilarity::parse_str(&sim_str).unwrap_or(VectorSimilarity::Cosine);
            let idx_name: Option<String> = if let Some(n) = dict.get_item("index_name")? {
                if n.is_none() { None } else { n.extract().ok() }
            } else {
                None
            };
            indexed = true;
            index_type = Some(IndexType::Vector);
            vector_config = Some(VectorIndexConfig {
                dimensions: dims,
                similarity,
                index_name: idx_name,
            });
        }

        let description = if let Some(d) = dict.get_item("description")? {
            if d.is_none() {
                None
            } else {
                d.extract::<String>().ok()
            }
        } else {
            None
        };

        Ok(FieldDescriptor {
            name: field_name,
            field_type,
            nullable,
            nullable_explicit,
            nullable_inferred,
            primary_key,
            unique,
            indexed,
            index_type,
            default_value,
            vector_config,
            description,
        })
    } else {
        let field_name = if obj.hasattr("name")? {
            let n = obj.getattr("name")?;
            if n.is_none() {
                name.to_string()
            } else {
                let s: String = n.extract()?;
                if s.is_empty() { name.to_string() } else { s }
            }
        } else {
            name.to_string()
        };

        if field_name.is_empty() {
            return Err(PyValueError::new_err(
                "Field descriptor missing property name (key or 'name' attribute)",
            ));
        }

        let primary_key = if obj.hasattr("primary_key")? {
            obj.getattr("primary_key")?
                .extract::<bool>()
                .unwrap_or(false)
        } else {
            false
        };

        let unique = if obj.hasattr("unique")? {
            obj.getattr("unique")?.extract::<bool>().unwrap_or(false) || primary_key
        } else {
            primary_key
        };

        let mut indexed = if obj.hasattr("index")? {
            obj.getattr("index")?.extract::<bool>().unwrap_or(false)
        } else if obj.hasattr("indexed")? {
            obj.getattr("indexed")?.extract::<bool>().unwrap_or(false)
        } else {
            false
        };

        let nullable = if obj.hasattr("nullable")? {
            obj.getattr("nullable")?
                .extract::<bool>()
                .unwrap_or(!primary_key)
        } else {
            !primary_key
        };

        let nullable_explicit = if obj.hasattr("nullable_explicit")? {
            obj.getattr("nullable_explicit")?
                .extract::<Option<bool>>()
                .unwrap_or(None)
        } else if obj.hasattr("nullable")? {
            // Objects written before inference only had `nullable`; a value
            // there was always an explicit request.
            obj.getattr("nullable")?.extract::<bool>().ok()
        } else {
            None
        };
        let nullable_inferred = nullable_explicit.is_none() && !nullable && !primary_key;

        let mut index_type = if obj.hasattr("index_type")? {
            let it = obj.getattr("index_type")?;
            if it.is_none() {
                if indexed {
                    Some(IndexType::BTree)
                } else {
                    None
                }
            } else {
                let it_str: String = it.extract()?;
                indexed = true;
                IndexType::parse_str(&it_str)
            }
        } else if indexed {
            Some(IndexType::BTree)
        } else {
            None
        };

        let field_type = if obj.hasattr("type_annotation")? {
            annotation_field_type(&obj.getattr("type_annotation")?)?
        } else if obj.hasattr("type")? {
            let t = obj.getattr("type")?;
            if let Ok(s) = t.extract::<String>() {
                FieldType::parse_str(&s)
            } else {
                FieldType::Any
            }
        } else {
            FieldType::Any
        };

        let default_value = if obj.hasattr("default")? {
            let dv = obj.getattr("default")?;
            if dv.is_none() {
                None
            } else {
                py_to_literal(&dv).ok()
            }
        } else {
            None
        };

        let mut vector_config = None;
        if index_type == Some(IndexType::Vector)
            || obj.hasattr("dimensions")?
            || obj.hasattr("vector_config")?
        {
            let dims: usize = if obj.hasattr("dimensions")? {
                let d = obj.getattr("dimensions")?;
                if d.is_none() {
                    DEFAULT_VECTOR_DIMENSIONS
                } else {
                    match d.extract::<usize>() {
                        Ok(v) if v > 0 => v,
                        Ok(_) => {
                            return Err(PyValueError::new_err(
                                "Vector dimensions must be a positive integer greater than 0",
                            ));
                        }
                        Err(e) => {
                            return Err(PyValueError::new_err(format!(
                                "Invalid vector dimensions: expected positive integer, got: {e}"
                            )));
                        }
                    }
                }
            } else {
                DEFAULT_VECTOR_DIMENSIONS
            };
            let sim_str: String = if obj.hasattr("similarity")? {
                let s = obj.getattr("similarity")?;
                if s.is_none() {
                    "cosine".to_string()
                } else {
                    s.extract::<String>()
                        .unwrap_or_else(|_| "cosine".to_string())
                }
            } else {
                "cosine".to_string()
            };
            let similarity =
                VectorSimilarity::parse_str(&sim_str).unwrap_or(VectorSimilarity::Cosine);
            let idx_name: Option<String> = if obj.hasattr("index_name")? {
                let n = obj.getattr("index_name")?;
                if n.is_none() { None } else { n.extract().ok() }
            } else {
                None
            };
            indexed = true;
            index_type = Some(IndexType::Vector);
            vector_config = Some(VectorIndexConfig {
                dimensions: dims,
                similarity,
                index_name: idx_name,
            });
        }

        let description = if obj.hasattr("description")? {
            let d = obj.getattr("description")?;
            if d.is_none() {
                None
            } else {
                d.extract::<String>().ok()
            }
        } else {
            None
        };

        Ok(FieldDescriptor {
            name: field_name,
            field_type,
            nullable,
            nullable_explicit,
            nullable_inferred,
            primary_key,
            unique,
            indexed,
            index_type,
            default_value,
            vector_config,
            description,
        })
    }
}

fn field_descriptor_to_py<'py>(
    py: Python<'py>,
    field: &FieldDescriptor,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("name", &field.name)?;
    dict.set_item("type", field.field_type.as_str())?;
    dict.set_item("nullable", field.nullable)?;
    dict.set_item("primary_key", field.primary_key)?;
    dict.set_item("unique", field.unique)?;
    dict.set_item("indexed", field.indexed)?;
    if let Some(it) = field.index_type {
        dict.set_item("index_type", it.as_str())?;
    } else {
        dict.set_item("index_type", py.None())?;
    }
    if let Some(ref dv) = field.default_value {
        dict.set_item("default_value", literal_to_py(dv, py)?)?;
    } else {
        dict.set_item("default_value", py.None())?;
    }
    if let Some(ref vc) = field.vector_config {
        dict.set_item("dimensions", vc.dimensions)?;
        dict.set_item("similarity", vc.similarity.as_str())?;
        if let Some(ref iname) = vc.index_name {
            dict.set_item("index_name", iname)?;
        } else {
            dict.set_item("index_name", py.None())?;
        }
    }
    if let Some(ref d) = field.description {
        dict.set_item("description", d)?;
    } else {
        dict.set_item("description", py.None())?;
    }
    Ok(dict)
}

fn node_schema_to_py<'py>(py: Python<'py>, node: &NodeSchema) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("name", &node.name)?;
    dict.set_item("labels", &node.labels)?;
    if let Some(ref d) = node.description {
        dict.set_item("description", d)?;
    } else {
        dict.set_item("description", py.None())?;
    }
    if let Some(ref pk) = node.primary_key {
        dict.set_item("primary_key", pk)?;
    } else {
        dict.set_item("primary_key", py.None())?;
    }
    let fields_dict = PyDict::new(py);
    for (k, v) in &node.fields {
        fields_dict.set_item(k, field_descriptor_to_py(py, v)?)?;
    }
    dict.set_item("fields", fields_dict)?;
    let ft_list = PyList::empty(py);
    for ft in &node.fulltext_indexes {
        let ft_dict = PyDict::new(py);
        ft_dict.set_item("name", &ft.name)?;
        ft_dict.set_item("properties", &ft.properties)?;
        if let Some(ref a) = ft.analyzer {
            ft_dict.set_item("analyzer", a)?;
        } else {
            ft_dict.set_item("analyzer", py.None())?;
        }
        ft_list.append(ft_dict)?;
    }
    dict.set_item("fulltext_indexes", ft_list)?;
    let declaration_list = PyList::empty(py);
    for declaration in &node.relationship_declarations {
        let item = PyDict::new(py);
        item.set_item("name", &declaration.name)?;
        item.set_item("rel_type", &declaration.rel_type)?;
        item.set_item(
            "direction",
            match declaration.direction {
                Direction::Outgoing => "outgoing",
                Direction::Incoming => "incoming",
                Direction::Undirected => "undirected",
            },
        )?;
        item.set_item("target_model", &declaration.target_model)?;
        if let Some(ref model) = declaration.relationship_model {
            item.set_item("relationship_model", model)?;
        } else {
            item.set_item("relationship_model", py.None())?;
        }
        declaration_list.append(item)?;
    }
    dict.set_item("relationship_declarations", declaration_list)?;
    Ok(dict)
}

fn parse_relationship_declaration(val: &Bound<'_, PyAny>) -> PyResult<RelationshipDeclaration> {
    if let Ok(dict) = val.cast::<PyDict>() {
        let name: String = dict
            .get_item("name")?
            .ok_or_else(|| {
                PyValueError::new_err("Missing required key 'name' in relationship declaration")
            })?
            .extract()?;
        let rel_type: String = dict
            .get_item("rel_type")?
            .or(dict.get_item("type_name")?)
            .or(dict.get_item("type")?)
            .ok_or_else(|| {
                PyValueError::new_err("Missing required key 'rel_type' in relationship declaration")
            })?
            .extract()?;
        let direction: String = dict
            .get_item("direction")?
            .and_then(|v| if v.is_none() { None } else { v.extract().ok() })
            .unwrap_or_else(|| "outgoing".to_string());
        let direction = parse_direction(&direction)?;
        let target_model: String = dict
            .get_item("target_model")?
            .or(dict.get_item("target")?)
            .ok_or_else(|| {
                PyValueError::new_err(
                    "Missing required key 'target_model' in relationship declaration",
                )
            })?
            .extract()?;
        let relationship_model: Option<String> = dict
            .get_item("relationship_model")?
            .or(dict.get_item("model")?)
            .and_then(|v| if v.is_none() { None } else { v.extract().ok() });
        let mut declaration = RelationshipDeclaration::new(name, rel_type, direction, target_model);
        if let Some(model) = relationship_model {
            declaration = declaration.with_relationship_model(model);
        }
        Ok(declaration)
    } else {
        let name: String = val.getattr("name")?.extract()?;
        let rel_type: String = if val.hasattr("rel_type")? {
            val.getattr("rel_type")?.extract()?
        } else {
            val.getattr("type_name")?.extract()?
        };
        let direction: String = if val.hasattr("direction")? {
            val.getattr("direction")?.extract()?
        } else {
            "outgoing".to_string()
        };
        let direction = parse_direction(&direction)?;
        let target_model: String = if val.hasattr("target_model")? {
            val.getattr("target_model")?.extract()?
        } else {
            val.getattr("target")?.extract()?
        };
        let relationship_model: Option<String> = if val.hasattr("relationship_model")? {
            val.getattr("relationship_model")?
                .extract::<Option<String>>()?
        } else if val.hasattr("model")? {
            val.getattr("model")?.extract::<Option<String>>()?
        } else {
            None
        };
        let mut declaration = RelationshipDeclaration::new(name, rel_type, direction, target_model);
        if let Some(model) = relationship_model {
            declaration = declaration.with_relationship_model(model);
        }
        Ok(declaration)
    }
}

fn extract_relationship_declarations(
    obj: &Bound<'_, PyAny>,
) -> PyResult<Vec<RelationshipDeclaration>> {
    let mut out = Vec::new();
    if let Ok(list) = obj.cast::<PyList>() {
        for item in list {
            out.push(parse_relationship_declaration(&item)?);
        }
    } else if let Ok(tuple) = obj.cast::<PyTuple>() {
        for item in tuple {
            out.push(parse_relationship_declaration(&item)?);
        }
    }
    Ok(out)
}

fn relationship_schema_to_py<'py>(
    py: Python<'py>,
    rel: &RelationshipSchema,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("name", &rel.name)?;
    dict.set_item("type_name", &rel.type_name)?;
    if let Some(ref d) = rel.description {
        dict.set_item("description", d)?;
    } else {
        dict.set_item("description", py.None())?;
    }
    dict.set_item("source_labels", &rel.source_labels)?;
    dict.set_item("target_labels", &rel.target_labels)?;
    dict.set_item("directed", rel.directed)?;
    let fields_dict = PyDict::new(py);
    for (k, v) in &rel.fields {
        fields_dict.set_item(k, field_descriptor_to_py(py, v)?)?;
    }
    dict.set_item("fields", fields_dict)?;
    let ft_list = PyList::empty(py);
    for ft in &rel.fulltext_indexes {
        let ft_dict = PyDict::new(py);
        ft_dict.set_item("name", &ft.name)?;
        ft_dict.set_item("properties", &ft.properties)?;
        if let Some(ref a) = ft.analyzer {
            ft_dict.set_item("analyzer", a)?;
        } else {
            ft_dict.set_item("analyzer", py.None())?;
        }
        ft_list.append(ft_dict)?;
    }
    dict.set_item("fulltext_indexes", ft_list)?;
    Ok(dict)
}

fn parse_fulltext_index_config(val: &Bound<'_, PyAny>) -> PyResult<FullTextIndexConfig> {
    if let Ok(dict) = val.cast::<PyDict>() {
        let name: String = dict
            .get_item("name")?
            .and_then(|v| if v.is_none() { None } else { v.extract().ok() })
            .unwrap_or_default();
        let properties: Vec<String> = if let Some(p) = dict.get_item("properties")? {
            if let Ok(vec) = p.extract::<Vec<String>>() {
                vec
            } else if let Ok(s) = p.extract::<String>() {
                vec![s]
            } else {
                vec![]
            }
        } else {
            vec![]
        };
        let analyzer: Option<String> = dict
            .get_item("analyzer")?
            .and_then(|v| if v.is_none() { None } else { v.extract().ok() });
        let mut cfg = FullTextIndexConfig::new(name, properties);
        if let Some(a) = analyzer {
            cfg = cfg.with_analyzer(a);
        }
        Ok(cfg)
    } else {
        let name: String = if val.hasattr("name")? {
            val.getattr("name")?
                .extract::<Option<String>>()?
                .unwrap_or_default()
        } else {
            String::new()
        };
        let properties: Vec<String> = if val.hasattr("properties")? {
            let p = val.getattr("properties")?;
            if let Ok(vec) = p.extract::<Vec<String>>() {
                vec
            } else if let Ok(s) = p.extract::<String>() {
                vec![s]
            } else {
                vec![]
            }
        } else {
            vec![]
        };
        let analyzer: Option<String> = if val.hasattr("analyzer")? {
            val.getattr("analyzer")?.extract::<Option<String>>()?
        } else {
            None
        };
        let mut cfg = FullTextIndexConfig::new(name, properties);
        if let Some(a) = analyzer {
            cfg = cfg.with_analyzer(a);
        }
        Ok(cfg)
    }
}

fn extract_fulltext_configs(obj: &Bound<'_, PyAny>) -> PyResult<Vec<FullTextIndexConfig>> {
    let mut out = Vec::new();
    if let Ok(list) = obj.cast::<PyList>() {
        for item in list {
            out.push(parse_fulltext_index_config(&item)?);
        }
    } else if let Ok(tuple) = obj.cast::<PyTuple>() {
        for item in tuple {
            out.push(parse_fulltext_index_config(&item)?);
        }
    }
    Ok(out)
}

fn extract_node_schema(obj: &Bound<'_, PyAny>) -> PyResult<NodeSchema> {
    if let Ok(dict) = obj.cast::<PyDict>() {
        let name: String = dict
            .get_item("name")?
            .ok_or_else(|| PyValueError::new_err("Missing required key 'name' in node schema"))?
            .extract()?;
        let labels: Vec<String> = if let Some(l) = dict.get_item("labels")? {
            l.extract()?
        } else {
            vec![name.clone()]
        };
        let pk: Option<String> = dict
            .get_item("primary_key")?
            .and_then(|v| if v.is_none() { None } else { v.extract().ok() });
        let mut node = NodeSchema::new(name, labels);
        if let Some(f_obj) = dict.get_item("fields")? {
            if let Ok(f_dict) = f_obj.cast::<PyDict>() {
                for (k, v) in f_dict {
                    let field_name: String = if let Ok(s) = k.cast::<PyString>() {
                        s.to_string_lossy().into_owned()
                    } else {
                        k.extract()?
                    };
                    let desc = parse_field_descriptor(&field_name, &v)?;
                    node = node.with_field(desc);
                }
            } else if let Ok(f_list) = f_obj.cast::<PyList>() {
                for item in f_list {
                    let desc = parse_field_descriptor("", &item)?;
                    node = node.with_field(desc);
                }
            }
        }
        if let Some(pk_name) = pk {
            if let Some(f) = node.fields.get_mut(&pk_name) {
                f.primary_key = true;
                f.unique = true;
                f.nullable = false;
            }
            node.primary_key = Some(pk_name);
        }
        if let Some(ft_obj) = dict.get_item("fulltext_indexes")? {
            for cfg in extract_fulltext_configs(&ft_obj)? {
                node = node.with_fulltext_index(cfg);
            }
        }
        if let Some(decl_obj) = dict.get_item("relationship_declarations")? {
            for declaration in extract_relationship_declarations(&decl_obj)? {
                node = node.with_relationship_declaration(declaration);
            }
        }
        Ok(node)
    } else {
        let name: String = if obj.hasattr("__name__")? {
            obj.getattr("__name__")?.extract()?
        } else {
            return Err(PyTypeError::new_err(
                "Expected node class or schema dict with '__name__'",
            ));
        };
        let labels: Vec<String> = if obj.hasattr("__labels__")? {
            let l = obj.getattr("__labels__")?;
            if let Ok(vec) = l.extract::<Vec<String>>() {
                if vec.is_empty() {
                    vec![name.clone()]
                } else {
                    vec
                }
            } else {
                vec![name.clone()]
            }
        } else {
            vec![name.clone()]
        };
        let mut node = NodeSchema::new(name, labels);
        if obj.hasattr("_schema_fields")? {
            let fields_any = obj.getattr("_schema_fields")?;
            if let Ok(fields_dict) = fields_any.cast::<PyDict>() {
                for (k, v) in fields_dict {
                    let field_name: String = if let Ok(s) = k.cast::<PyString>() {
                        s.to_string_lossy().into_owned()
                    } else {
                        k.extract()?
                    };
                    let desc = parse_field_descriptor(&field_name, &v)?;
                    node = node.with_field(desc);
                }
            }
        }
        if obj.hasattr("__fulltext_indexes__")? {
            let ft_obj = obj.getattr("__fulltext_indexes__")?;
            for cfg in extract_fulltext_configs(&ft_obj)? {
                node = node.with_fulltext_index(cfg);
            }
        }
        if obj.hasattr("_schema_relationships")? {
            let decl_obj = obj.getattr("_schema_relationships")?;
            if let Ok(values) = decl_obj.call_method0("values") {
                if let Ok(iterator) = values.try_iter() {
                    for item in iterator.flatten() {
                        // Relationship descriptors expose ``to_declaration()``;
                        // plain schema dicts are parsed directly.
                        let declaration = if item.hasattr("to_declaration")? {
                            item.call_method0("to_declaration")?
                        } else {
                            item
                        };
                        node = node.with_relationship_declaration(parse_relationship_declaration(
                            &declaration,
                        )?);
                    }
                }
            }
        }
        Ok(node)
    }
}

fn extract_rel_schema(obj: &Bound<'_, PyAny>) -> PyResult<RelationshipSchema> {
    if let Ok(dict) = obj.cast::<PyDict>() {
        let name: String = dict
            .get_item("name")?
            .ok_or_else(|| {
                PyValueError::new_err("Missing required key 'name' in relationship schema")
            })?
            .extract()?;
        let type_name: String = if let Some(t) = dict
            .get_item("type_name")?
            .or(dict.get_item("type_")?)
            .or(dict.get_item("type")?)
        {
            t.extract()?
        } else {
            name.to_ascii_uppercase()
        };
        let source_labels: Vec<String> = dict
            .get_item("source_labels")?
            .or(dict.get_item("from_labels")?)
            .and_then(|v| if v.is_none() { None } else { v.extract().ok() })
            .unwrap_or_default();
        let target_labels: Vec<String> = dict
            .get_item("target_labels")?
            .or(dict.get_item("to_labels")?)
            .and_then(|v| if v.is_none() { None } else { v.extract().ok() })
            .unwrap_or_default();
        let directed: bool = dict
            .get_item("directed")?
            .and_then(|v| v.extract::<bool>().ok())
            .unwrap_or(true);
        let mut rel = RelationshipSchema::new(name, type_name)
            .with_endpoints(source_labels, target_labels)
            .directed(directed);
        if let Some(f_obj) = dict.get_item("fields")? {
            if let Ok(f_dict) = f_obj.cast::<PyDict>() {
                for (k, v) in f_dict {
                    let field_name: String = if let Ok(s) = k.cast::<PyString>() {
                        s.to_string_lossy().into_owned()
                    } else {
                        k.extract()?
                    };
                    let desc = parse_field_descriptor(&field_name, &v)?;
                    rel = rel.with_field(desc);
                }
            } else if let Ok(f_list) = f_obj.cast::<PyList>() {
                for item in f_list {
                    let desc = parse_field_descriptor("", &item)?;
                    rel = rel.with_field(desc);
                }
            }
        }
        if let Some(ft_obj) = dict.get_item("fulltext_indexes")? {
            for cfg in extract_fulltext_configs(&ft_obj)? {
                rel = rel.with_fulltext_index(cfg);
            }
        }
        Ok(rel)
    } else {
        let name: String = if obj.hasattr("__name__")? {
            obj.getattr("__name__")?.extract()?
        } else {
            return Err(PyTypeError::new_err(
                "Expected relationship class or schema dict with '__name__'",
            ));
        };
        let type_name: String = if obj.hasattr("__type__")? {
            let t: String = obj.getattr("__type__")?.extract()?;
            if t.is_empty() {
                name.to_ascii_uppercase()
            } else {
                t
            }
        } else {
            name.to_ascii_uppercase()
        };
        let source_labels: Vec<String> = if obj.hasattr("__source_labels__")? {
            obj.getattr("__source_labels__")?
                .extract::<Vec<String>>()
                .unwrap_or_default()
        } else {
            vec![]
        };
        let target_labels: Vec<String> = if obj.hasattr("__target_labels__")? {
            obj.getattr("__target_labels__")?
                .extract::<Vec<String>>()
                .unwrap_or_default()
        } else {
            vec![]
        };
        let mut rel =
            RelationshipSchema::new(name, type_name).with_endpoints(source_labels, target_labels);
        if obj.hasattr("_schema_fields")? {
            let fields_any = obj.getattr("_schema_fields")?;
            if let Ok(fields_dict) = fields_any.cast::<PyDict>() {
                for (k, v) in fields_dict {
                    let field_name: String = if let Ok(s) = k.cast::<PyString>() {
                        s.to_string_lossy().into_owned()
                    } else {
                        k.extract()?
                    };
                    let desc = parse_field_descriptor(&field_name, &v)?;
                    rel = rel.with_field(desc);
                }
            }
        }
        if obj.hasattr("__fulltext_indexes__")? {
            let ft_obj = obj.getattr("__fulltext_indexes__")?;
            for cfg in extract_fulltext_configs(&ft_obj)? {
                rel = rel.with_fulltext_index(cfg);
            }
        }
        Ok(rel)
    }
}

enum RegistryStorage {
    Owned(Arc<SchemaRegistry>),
    Global,
}

/// Centralized, thread-safe Native Schema Registry and metadata storage.
#[pyclass(name = "NativeSchemaRegistry")]
pub struct PyNativeSchemaRegistry {
    storage: RegistryStorage,
}

impl PyNativeSchemaRegistry {
    fn registry(&self) -> &SchemaRegistry {
        match &self.storage {
            RegistryStorage::Owned(r) => r.as_ref(),
            RegistryStorage::Global => global_schema_registry(),
        }
    }

    fn build_node_schema(
        name: String,
        labels: Vec<String>,
        fields: &Bound<'_, PyAny>,
        primary_key: Option<String>,
        fulltext_indexes: Option<&Bound<'_, PyAny>>,
        relationship_declarations: Option<&Bound<'_, PyAny>>,
        description: Option<String>,
    ) -> PyResult<NodeSchema> {
        let mut node_schema = NodeSchema::new(name, labels);
        if let Some(desc) = description {
            node_schema = node_schema.with_description(desc);
        }
        if let Ok(dict) = fields.cast::<PyDict>() {
            for (k, v) in dict {
                let field_name: String = if let Ok(s) = k.cast::<PyString>() {
                    s.to_string_lossy().into_owned()
                } else {
                    k.extract()?
                };
                let desc = parse_field_descriptor(&field_name, &v)?;
                node_schema = node_schema.with_field(desc);
            }
        } else if let Ok(list) = fields.cast::<PyList>() {
            for item in list {
                let desc = parse_field_descriptor("", &item)?;
                node_schema = node_schema.with_field(desc);
            }
        } else {
            return Err(PyTypeError::new_err("fields must be a dict or list"));
        }

        if let Some(pk) = primary_key {
            if let Some(f) = node_schema.fields.get_mut(&pk) {
                f.primary_key = true;
                f.unique = true;
                f.nullable = false;
            }
            node_schema.primary_key = Some(pk);
        }

        if let Some(ft_any) = fulltext_indexes {
            for cfg in extract_fulltext_configs(ft_any)? {
                node_schema = node_schema.with_fulltext_index(cfg);
            }
        }

        if let Some(decl_any) = relationship_declarations {
            for declaration in extract_relationship_declarations(decl_any)? {
                node_schema = node_schema.with_relationship_declaration(declaration);
            }
        }

        Ok(node_schema)
    }

    fn build_relationship_schema(
        name: String,
        type_name: String,
        endpoints: (Option<Vec<String>>, Option<Vec<String>>),
        fields: Option<&Bound<'_, PyAny>>,
        directed: bool,
        fulltext_indexes: Option<&Bound<'_, PyAny>>,
        description: Option<String>,
    ) -> PyResult<RelationshipSchema> {
        let mut rel_schema = RelationshipSchema::new(name, type_name).directed(directed);
        if let Some(desc) = description {
            rel_schema = rel_schema.with_description(desc);
        }
        if let (Some(src), Some(tgt)) = endpoints {
            rel_schema = rel_schema.with_endpoints(src, tgt);
        }
        if let Some(fields_any) = fields {
            if let Ok(dict) = fields_any.cast::<PyDict>() {
                for (k, v) in dict {
                    let field_name: String = if let Ok(s) = k.cast::<PyString>() {
                        s.to_string_lossy().into_owned()
                    } else {
                        k.extract()?
                    };
                    let desc = parse_field_descriptor(&field_name, &v)?;
                    rel_schema = rel_schema.with_field(desc);
                }
            } else if let Ok(list) = fields_any.cast::<PyList>() {
                for item in list {
                    let desc = parse_field_descriptor("", &item)?;
                    rel_schema = rel_schema.with_field(desc);
                }
            }
        }

        if let Some(ft_any) = fulltext_indexes {
            for cfg in extract_fulltext_configs(ft_any)? {
                rel_schema = rel_schema.with_fulltext_index(cfg);
            }
        }

        Ok(rel_schema)
    }
}

#[pymethods]
impl PyNativeSchemaRegistry {
    #[new]
    fn new() -> Self {
        Self {
            storage: RegistryStorage::Owned(Arc::new(SchemaRegistry::new())),
        }
    }

    /// Accesses the global singleton schema registry.
    #[staticmethod]
    fn global_registry() -> Self {
        Self {
            storage: RegistryStorage::Global,
        }
    }

    /// Alias for `global_registry()`.
    #[staticmethod]
    fn global_() -> Self {
        Self::global_registry()
    }

    /// Registers a node schema with labels, fields, and optional primary key.
    #[pyo3(signature = (name, labels, fields, primary_key=None, fulltext_indexes=None, description=None))]
    fn register_node(
        &self,
        name: String,
        labels: Vec<String>,
        fields: &Bound<'_, PyAny>,
        primary_key: Option<String>,
        fulltext_indexes: Option<&Bound<'_, PyAny>>,
        description: Option<String>,
    ) -> PyResult<()> {
        let node_schema = Self::build_node_schema(
            name,
            labels,
            fields,
            primary_key,
            fulltext_indexes,
            None,
            description,
        )?;
        self.registry()
            .register_node(node_schema)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(())
    }

    /// Registers a node schema under a unique model identity.
    ///
    /// The primary key is derived from the field descriptors.
    #[pyo3(signature = (model_id, name, labels, fields, fulltext_indexes=None, description=None))]
    fn register_node_with_identity(
        &self,
        model_id: String,
        name: String,
        labels: Vec<String>,
        fields: &Bound<'_, PyAny>,
        fulltext_indexes: Option<&Bound<'_, PyAny>>,
        description: Option<String>,
    ) -> PyResult<()> {
        let node_schema = Self::build_node_schema(
            name,
            labels,
            fields,
            None,
            fulltext_indexes,
            None,
            description,
        )?;
        self.registry()
            .register_node_with_id(model_id, node_schema)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(())
    }

    /// Adds declarative relationship navigation attributes to a registered node.
    ///
    /// Node models register first and then attach their relationship
    /// declarations, so the node registration argument list stays unchanged.
    fn register_relationship_declarations(
        &self,
        model_name_or_id: String,
        declarations: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let declarations = extract_relationship_declarations(declarations)?;
        self.registry()
            .add_relationship_declarations(&model_name_or_id, declarations)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(())
    }

    /// Registers a node schema from a dictionary specification.
    fn register_node_schema(&self, schema: &Bound<'_, PyAny>) -> PyResult<()> {
        if let Ok(dict) = schema.cast::<PyDict>() {
            let name: String = dict
                .get_item("name")?
                .ok_or_else(|| PyValueError::new_err("Missing required key 'name' in node schema"))?
                .extract()?;
            let labels: Vec<String> = if let Some(l) = dict.get_item("labels")? {
                l.extract()?
            } else {
                vec![name.clone()]
            };
            let pk: Option<String> = dict
                .get_item("primary_key")?
                .and_then(|v| if v.is_none() { None } else { v.extract().ok() });
            let empty_dict = PyDict::new(schema.py());
            let fields_obj = if let Some(f) = dict.get_item("fields")? {
                f
            } else {
                empty_dict.into_any()
            };
            let ft_obj = dict.get_item("fulltext_indexes")?;
            let description: Option<String> = dict
                .get_item("description")?
                .and_then(|v| if v.is_none() { None } else { v.extract().ok() });
            let decl_obj = dict.get_item("relationship_declarations")?;
            let node_schema = Self::build_node_schema(
                name,
                labels,
                &fields_obj,
                pk,
                ft_obj.as_ref(),
                decl_obj.as_ref(),
                description,
            )?;
            self.registry()
                .register_node(node_schema)
                .map_err(|e| PyValueError::new_err(e.to_string()))?;
            Ok(())
        } else {
            Err(PyTypeError::new_err("schema must be a dict"))
        }
    }

    /// Registers a relationship schema with endpoints, fields, and direction.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (name, type_name, source_labels=None, target_labels=None, fields=None, directed=true, fulltext_indexes=None, model_id=None, description=None))]
    fn register_relationship(
        &self,
        name: String,
        type_name: String,
        source_labels: Option<Vec<String>>,
        target_labels: Option<Vec<String>>,
        fields: Option<&Bound<'_, PyAny>>,
        directed: bool,
        fulltext_indexes: Option<&Bound<'_, PyAny>>,
        model_id: Option<String>,
        description: Option<String>,
    ) -> PyResult<()> {
        let rel_schema = Self::build_relationship_schema(
            name,
            type_name,
            (source_labels, target_labels),
            fields,
            directed,
            fulltext_indexes,
            description,
        )?;
        let registry = self.registry();
        if let Some(model_id) = model_id {
            registry.register_relationship_with_id(model_id, rel_schema)
        } else {
            registry.register_relationship(rel_schema)
        }
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(())
    }

    /// Registers a relationship schema from a dictionary specification.
    fn register_relationship_schema(&self, schema: &Bound<'_, PyAny>) -> PyResult<()> {
        if let Ok(dict) = schema.cast::<PyDict>() {
            let name: String = dict
                .get_item("name")?
                .ok_or_else(|| {
                    PyValueError::new_err("Missing required key 'name' in relationship schema")
                })?
                .extract()?;
            let type_name: String = if let Some(t) = dict
                .get_item("type_name")?
                .or(dict.get_item("type_")?)
                .or(dict.get_item("type")?)
            {
                t.extract()?
            } else {
                name.to_ascii_uppercase()
            };
            let source_labels: Option<Vec<String>> = dict
                .get_item("source_labels")?
                .or(dict.get_item("from_labels")?)
                .and_then(|v| if v.is_none() { None } else { v.extract().ok() });
            let target_labels: Option<Vec<String>> = dict
                .get_item("target_labels")?
                .or(dict.get_item("to_labels")?)
                .and_then(|v| if v.is_none() { None } else { v.extract().ok() });
            let directed: bool = dict
                .get_item("directed")?
                .and_then(|v| v.extract::<bool>().ok())
                .unwrap_or(true);
            let fields_obj = dict.get_item("fields")?;
            let ft_obj = dict.get_item("fulltext_indexes")?;
            let model_id: Option<String> = dict
                .get_item("model_id")?
                .and_then(|v| if v.is_none() { None } else { v.extract().ok() });
            let description: Option<String> = dict
                .get_item("description")?
                .and_then(|v| if v.is_none() { None } else { v.extract().ok() });
            self.register_relationship(
                name,
                type_name,
                source_labels,
                target_labels,
                fields_obj.as_ref(),
                directed,
                ft_obj.as_ref(),
                model_id,
                description,
            )
        } else {
            Err(PyTypeError::new_err("schema must be a dict"))
        }
    }

    /// Retrieves a node schema by model identity or bare class name.
    ///
    /// When a bare class name is shared by several registrations, the most
    /// recently registered schema is returned.
    fn get_node<'py>(&self, py: Python<'py>, name: &str) -> PyResult<Option<Bound<'py, PyDict>>> {
        if let Some(node) = self.registry().get_node(name) {
            Ok(Some(node_schema_to_py(py, &node)?))
        } else {
            Ok(None)
        }
    }

    /// Returns the primary key property name for a node or relationship model.
    ///
    /// The value is derived by the native schema from the field descriptors.
    fn primary_key_for(&self, model_id: &str) -> Option<String> {
        self.registry().primary_key_for(model_id)
    }

    /// Resolves the MERGE key property names for a node or relationship model.
    ///
    /// `supplied` lists the property names present on the instance and
    /// `defaulted` lists the subset that only carries a declared default.
    fn merge_key_for(
        &self,
        model_id: &str,
        supplied: Vec<String>,
        defaulted: Vec<String>,
    ) -> Option<Vec<String>> {
        self.registry()
            .merge_key_for(model_id, &supplied, &defaulted)
    }

    /// Retrieves a node schema by primary label.
    fn get_node_by_label<'py>(
        &self,
        py: Python<'py>,
        label: &str,
    ) -> PyResult<Option<Bound<'py, PyDict>>> {
        if let Some(node) = self.registry().get_node_by_label(label) {
            Ok(Some(node_schema_to_py(py, &node)?))
        } else {
            Ok(None)
        }
    }

    /// Retrieves a relationship schema by model name.
    fn get_relationship<'py>(
        &self,
        py: Python<'py>,
        name: &str,
    ) -> PyResult<Option<Bound<'py, PyDict>>> {
        if let Some(rel) = self.registry().get_relationship(name) {
            Ok(Some(relationship_schema_to_py(py, &rel)?))
        } else {
            Ok(None)
        }
    }

    /// Retrieves a relationship schema by graph relationship type (case-insensitive).
    fn get_relationship_by_type<'py>(
        &self,
        py: Python<'py>,
        type_name: &str,
    ) -> PyResult<Option<Bound<'py, PyDict>>> {
        if let Some(rel) = self.registry().get_relationship_by_type(type_name) {
            Ok(Some(relationship_schema_to_py(py, &rel)?))
        } else {
            Ok(None)
        }
    }

    /// Checks if a node schema is registered by name.
    fn has_node(&self, name: &str) -> bool {
        self.registry().has_node(name)
    }

    /// Checks if a relationship schema is registered by name.
    fn has_relationship(&self, name: &str) -> bool {
        self.registry().has_relationship(name)
    }

    /// Removes a node schema by model name, returning it if present.
    fn remove_node<'py>(
        &self,
        py: Python<'py>,
        name: &str,
    ) -> PyResult<Option<Bound<'py, PyDict>>> {
        if let Some(node) = self.registry().remove_node(name) {
            Ok(Some(node_schema_to_py(py, &node)?))
        } else {
            Ok(None)
        }
    }

    /// Removes a relationship schema by model name, returning it if present.
    fn remove_relationship<'py>(
        &self,
        py: Python<'py>,
        name: &str,
    ) -> PyResult<Option<Bound<'py, PyDict>>> {
        if let Some(rel) = self.registry().remove_relationship(name) {
            Ok(Some(relationship_schema_to_py(py, &rel)?))
        } else {
            Ok(None)
        }
    }

    /// Returns a list of all registered node schemas as dictionaries.
    fn node_schemas<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let list = PyList::empty(py);
        for n in self.registry().node_schemas() {
            list.append(node_schema_to_py(py, &n)?)?;
        }
        Ok(list)
    }

    /// Returns a list of all registered relationship schemas as dictionaries.
    fn relationship_schemas<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let list = PyList::empty(py);
        for r in self.registry().relationship_schemas() {
            list.append(relationship_schema_to_py(py, &r)?)?;
        }
        Ok(list)
    }

    /// Clears all schemas from the registry.
    fn clear(&self) {
        self.registry().clear();
    }

    /// Serializes all registered schemas to a JSON string.
    fn to_json(&self) -> PyResult<String> {
        self.registry()
            .to_json()
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }

    /// Deserializes and restores schemas from a JSON string into this registry.
    #[allow(clippy::wrong_self_convention)]
    fn from_json(&self, json_str: &str) -> PyResult<()> {
        self.registry()
            .from_json(json_str)
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }

    /// Deserializes and restores schemas from a JSON string into this registry.
    fn load_json(&self, json_str: &str) -> PyResult<()> {
        self.from_json(json_str)
    }

    /// Returns the total count of registered schemas.
    fn __len__(&self) -> usize {
        self.registry().len()
    }

    /// Generates openCypher constraint and index creation statements.
    #[pyo3(signature = (include_type_constraints=false, model_name=None))]
    fn generate_cypher_ddl(
        &self,
        include_type_constraints: bool,
        model_name: Option<&str>,
    ) -> PyResult<Vec<String>> {
        if let Some(name) = model_name {
            if let Some(stmts) = self
                .registry()
                .generate_node_cypher_ddl(name, include_type_constraints)
            {
                return Ok(stmts);
            }
            if let Some(stmts) = self
                .registry()
                .generate_rel_cypher_ddl(name, include_type_constraints)
            {
                return Ok(stmts);
            }
            Err(PyValueError::new_err(format!(
                "No node or relationship registered with name '{name}'"
            )))
        } else {
            Ok(self
                .registry()
                .generate_cypher_ddl(include_type_constraints))
        }
    }

    /// Generates openCypher DROP statements.
    #[pyo3(signature = (include_type_constraints=false, model_name=None))]
    fn generate_cypher_drop_ddl(
        &self,
        include_type_constraints: bool,
        model_name: Option<&str>,
    ) -> PyResult<Vec<String>> {
        if let Some(name) = model_name {
            if let Some(stmts) = self
                .registry()
                .generate_node_cypher_drop_ddl(name, include_type_constraints)
            {
                return Ok(stmts);
            }
            if let Some(stmts) = self
                .registry()
                .generate_rel_cypher_drop_ddl(name, include_type_constraints)
            {
                return Ok(stmts);
            }
            Err(PyValueError::new_err(format!(
                "No node or relationship registered with name '{name}'"
            )))
        } else {
            Ok(self
                .registry()
                .generate_cypher_drop_ddl(include_type_constraints))
        }
    }

    /// Generates native Memgraph DDL statements (constraints and indexes).
    #[pyo3(signature = (model_name=None))]
    fn generate_memgraph_ddl(&self, model_name: Option<&str>) -> PyResult<Vec<String>> {
        if let Some(name) = model_name {
            if let Some(stmts) = self.registry().generate_node_memgraph_ddl(name) {
                return Ok(stmts);
            }
            if let Some(stmts) = self.registry().generate_rel_memgraph_ddl(name) {
                return Ok(stmts);
            }
            Err(PyValueError::new_err(format!(
                "No node or relationship registered with name '{name}'"
            )))
        } else {
            Ok(self.registry().generate_memgraph_ddl())
        }
    }

    /// Generates native Memgraph DROP statements (constraints and indexes).
    #[pyo3(signature = (model_name=None))]
    fn generate_memgraph_drop_ddl(&self, model_name: Option<&str>) -> PyResult<Vec<String>> {
        if let Some(name) = model_name {
            if let Some(stmts) = self.registry().generate_node_memgraph_drop_ddl(name) {
                return Ok(stmts);
            }
            if let Some(stmts) = self.registry().generate_rel_memgraph_drop_ddl(name) {
                return Ok(stmts);
            }
            Err(PyValueError::new_err(format!(
                "No node or relationship registered with name '{name}'"
            )))
        } else {
            Ok(self.registry().generate_memgraph_drop_ddl())
        }
    }

    /// Emits an ISO GQL `CREATE GRAPH TYPE <name> AS { ... }` DDL statement.
    #[pyo3(signature = (graph_type_name, model_names=None))]
    fn generate_gql_graph_type_ddl(
        &self,
        graph_type_name: &str,
        model_names: Option<Vec<String>>,
    ) -> PyResult<String> {
        if let Some(names) = model_names {
            let str_names: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
            Ok(self
                .registry()
                .generate_gql_graph_type_ddl_for(graph_type_name, &str_names))
        } else {
            Ok(self.registry().generate_gql_graph_type_ddl(graph_type_name))
        }
    }

    /// Emits an experimental ISO GQL `ALTER CURRENT GRAPH TYPE ADD NODE TYPE` DDL statement.
    fn generate_gql_alter_node_ddl(&self, node_name: &str) -> PyResult<Option<String>> {
        Ok(self.registry().generate_gql_alter_node_ddl(node_name))
    }

    /// Emits an experimental ISO GQL `ALTER CURRENT GRAPH TYPE ADD RELATIONSHIP TYPE` DDL statement.
    #[pyo3(signature = (rel_name, source_label=None, target_label=None))]
    fn generate_gql_alter_rel_ddl(
        &self,
        rel_name: &str,
        source_label: Option<&str>,
        target_label: Option<&str>,
    ) -> PyResult<Option<String>> {
        Ok(self
            .registry()
            .generate_gql_alter_rel_ddl(rel_name, source_label, target_label))
    }

    /// Emits an ISO GQL `DROP GRAPH TYPE` DDL statement.
    fn generate_gql_drop_graph_type_ddl(&self, graph_type_name: &str) -> PyResult<String> {
        Ok(self
            .registry()
            .generate_gql_drop_graph_type_ddl(graph_type_name))
    }

    /// Emits a SQL:2023 PGQ / DuckPGQ `CREATE PROPERTY GRAPH` DDL statement.
    #[pyo3(signature = (graph_name, model_names=None, dialect="postgres"))]
    fn generate_pgq_ddl(
        &self,
        graph_name: &str,
        model_names: Option<Vec<String>>,
        dialect: &str,
    ) -> PyResult<String> {
        if let Some(names) = model_names {
            let str_names: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
            self.registry()
                .generate_pgq_ddl_for_with_dialect(graph_name, &str_names, dialect)
                .map_err(to_py_schema_err)
        } else {
            self.registry()
                .generate_pgq_ddl_with_dialect(graph_name, dialect)
                .map_err(to_py_schema_err)
        }
    }

    /// Emits relational table DDL (`CREATE TABLE IF NOT EXISTS`) for registered models.
    #[pyo3(signature = (model_names=None, dialect="postgres"))]
    fn generate_table_ddl(
        &self,
        model_names: Option<Vec<String>>,
        dialect: &str,
    ) -> PyResult<Vec<String>> {
        if let Some(names) = model_names {
            let str_names: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
            self.registry()
                .generate_table_ddl_for(&str_names, dialect)
                .map_err(to_py_schema_err)
        } else {
            self.registry()
                .generate_table_ddl(dialect)
                .map_err(to_py_schema_err)
        }
    }

    /// Emits full SQL/PGQ schema DDL in run order (tables, constraints, indexes, property graph).
    #[pyo3(signature = (graph_name, model_names=None, dialect="postgres"))]
    fn generate_pgq_schema_ddl(
        &self,
        graph_name: &str,
        model_names: Option<Vec<String>>,
        dialect: &str,
    ) -> PyResult<Vec<String>> {
        if let Some(names) = model_names {
            let str_names: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
            self.registry()
                .generate_pgq_schema_ddl_for(graph_name, &str_names, dialect)
                .map_err(to_py_schema_err)
        } else {
            self.registry()
                .generate_pgq_schema_ddl(graph_name, dialect)
                .map_err(to_py_schema_err)
        }
    }

    /// Emits a SQL:2023 PGQ / DuckPGQ `DROP PROPERTY GRAPH` DDL statement.
    fn generate_pgq_drop_ddl(&self, graph_name: &str) -> PyResult<String> {
        Ok(self.registry().generate_pgq_drop_ddl(graph_name))
    }

    /// Emits a Neo4j Cypher 25 `ALTER CURRENT GRAPH TYPE SET { ... }` DDL statement.
    #[pyo3(signature = (model_names=None))]
    fn generate_cypher25_graph_type_ddl(
        &self,
        model_names: Option<Vec<String>>,
    ) -> PyResult<String> {
        if let Some(names) = model_names {
            let str_names: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
            Ok(self
                .registry()
                .generate_cypher25_graph_type_ddl_for(&str_names))
        } else {
            Ok(self.registry().generate_cypher25_graph_type_ddl())
        }
    }

    /// Emits a Neo4j Cypher 25 `ALTER CURRENT GRAPH TYPE SET {}` statement to reset the graph type.
    fn generate_cypher25_drop_graph_type_ddl(&self) -> PyResult<String> {
        Ok(self.registry().generate_cypher25_drop_graph_type_ddl())
    }

    /// Generates multi-dialect CREATE INDEX DDL statements for registered models.
    #[pyo3(signature = (dialect="cypher", model_names=None, graph_name=None))]
    fn generate_index_ddl(
        &self,
        dialect: &str,
        model_names: Option<Vec<String>>,
        graph_name: Option<&str>,
    ) -> PyResult<Vec<String>> {
        let str_names: Option<Vec<&str>> = model_names
            .as_ref()
            .map(|v| v.iter().map(|s| s.as_str()).collect());
        self.registry()
            .generate_index_ddl_with_graph(dialect, str_names.as_deref(), graph_name)
            .map_err(to_py_schema_err)
    }

    /// Generates multi-dialect CREATE CONSTRAINT DDL statements for registered models.
    #[pyo3(signature = (dialect="cypher", model_names=None, include_type_constraints=false, graph_name=None))]
    fn generate_constraint_ddl(
        &self,
        dialect: &str,
        model_names: Option<Vec<String>>,
        include_type_constraints: bool,
        graph_name: Option<&str>,
    ) -> PyResult<Vec<String>> {
        let str_names: Option<Vec<&str>> = model_names
            .as_ref()
            .map(|v| v.iter().map(|s| s.as_str()).collect());
        self.registry()
            .generate_constraint_ddl_with_graph(
                dialect,
                str_names.as_deref(),
                include_type_constraints,
                graph_name,
            )
            .map_err(to_py_schema_err)
    }

    /// Generates multi-dialect DROP INDEX DDL statements for registered models.
    #[pyo3(signature = (dialect="cypher", model_names=None, graph_name=None))]
    fn generate_drop_index_ddl(
        &self,
        dialect: &str,
        model_names: Option<Vec<String>>,
        graph_name: Option<&str>,
    ) -> PyResult<Vec<String>> {
        let str_names: Option<Vec<&str>> = model_names
            .as_ref()
            .map(|v| v.iter().map(|s| s.as_str()).collect());
        self.registry()
            .generate_drop_index_ddl_with_graph(dialect, str_names.as_deref(), graph_name)
            .map_err(to_py_schema_err)
    }

    /// Generates multi-dialect DROP CONSTRAINT DDL statements for registered models.
    #[pyo3(signature = (dialect="cypher", model_names=None, include_type_constraints=false, graph_name=None))]
    fn generate_drop_constraint_ddl(
        &self,
        dialect: &str,
        model_names: Option<Vec<String>>,
        include_type_constraints: bool,
        graph_name: Option<&str>,
    ) -> PyResult<Vec<String>> {
        let str_names: Option<Vec<&str>> = model_names
            .as_ref()
            .map(|v| v.iter().map(|s| s.as_str()).collect());
        self.registry()
            .generate_drop_constraint_ddl_with_graph(
                dialect,
                str_names.as_deref(),
                include_type_constraints,
                graph_name,
            )
            .map_err(to_py_schema_err)
    }

    /// Validates a query string against this schema registry instance.
    #[pyo3(signature = (query))]
    fn validate_query<'py>(&self, query: &str, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let topology = GraphTopology::from_query_str(query);
        let report = self.registry().validate_topology(&topology);
        conformance_report_to_py_dict(&report, py)
    }

    fn __repr__(&self) -> String {
        format!(
            "NativeSchemaRegistry(nodes={}, relationships={})",
            self.registry().node_schemas().len(),
            self.registry().relationship_schemas().len()
        )
    }
}

#[pyfunction]
#[pyo3(signature = (node, include_type_constraints=false))]
fn emit_cypher_node_ddl(
    node: &Bound<'_, PyAny>,
    include_type_constraints: bool,
) -> PyResult<Vec<String>> {
    let schema = extract_node_schema(node)?;
    Ok(voyager_ogm::emit_cypher_node_ddl(
        &schema,
        include_type_constraints,
    ))
}

#[pyfunction]
#[pyo3(signature = (rel, include_type_constraints=false))]
fn emit_cypher_rel_ddl(
    rel: &Bound<'_, PyAny>,
    include_type_constraints: bool,
) -> PyResult<Vec<String>> {
    let schema = extract_rel_schema(rel)?;
    Ok(voyager_ogm::emit_cypher_rel_ddl(
        &schema,
        include_type_constraints,
    ))
}

#[pyfunction]
#[pyo3(signature = (node, include_type_constraints=false))]
fn emit_cypher_drop_node_ddl(
    node: &Bound<'_, PyAny>,
    include_type_constraints: bool,
) -> PyResult<Vec<String>> {
    let schema = extract_node_schema(node)?;
    Ok(voyager_ogm::emit_cypher_drop_node_ddl(
        &schema,
        include_type_constraints,
    ))
}

#[pyfunction]
#[pyo3(signature = (rel, include_type_constraints=false))]
fn emit_cypher_drop_rel_ddl(
    rel: &Bound<'_, PyAny>,
    include_type_constraints: bool,
) -> PyResult<Vec<String>> {
    let schema = extract_rel_schema(rel)?;
    Ok(voyager_ogm::emit_cypher_drop_rel_ddl(
        &schema,
        include_type_constraints,
    ))
}

#[pyfunction]
fn emit_memgraph_node_ddl(node: &Bound<'_, PyAny>) -> PyResult<Vec<String>> {
    let schema = extract_node_schema(node)?;
    Ok(voyager_ogm::emit_memgraph_node_ddl(&schema))
}

#[pyfunction]
fn emit_memgraph_rel_ddl(rel: &Bound<'_, PyAny>) -> PyResult<Vec<String>> {
    let schema = extract_rel_schema(rel)?;
    Ok(voyager_ogm::emit_memgraph_rel_ddl(&schema))
}

#[pyfunction]
fn emit_memgraph_drop_node_ddl(node: &Bound<'_, PyAny>) -> PyResult<Vec<String>> {
    let schema = extract_node_schema(node)?;
    Ok(voyager_ogm::emit_memgraph_drop_node_ddl(&schema))
}

#[pyfunction]
fn emit_memgraph_drop_rel_ddl(rel: &Bound<'_, PyAny>) -> PyResult<Vec<String>> {
    let schema = extract_rel_schema(rel)?;
    Ok(voyager_ogm::emit_memgraph_drop_rel_ddl(&schema))
}

#[pyfunction]
fn emit_gql_alter_node_ddl(node: &Bound<'_, PyAny>) -> PyResult<String> {
    let schema = extract_node_schema(node)?;
    Ok(voyager_ogm::emit_gql_alter_node_ddl(&schema))
}

#[pyfunction]
#[pyo3(signature = (rel, source_label=None, target_label=None))]
fn emit_gql_alter_rel_ddl(
    rel: &Bound<'_, PyAny>,
    source_label: Option<&str>,
    target_label: Option<&str>,
) -> PyResult<String> {
    let schema = extract_rel_schema(rel)?;
    Ok(voyager_ogm::emit_gql_alter_rel_ddl(
        &schema,
        source_label,
        target_label,
    ))
}

#[pyfunction]
#[pyo3(signature = (graph_type_name, nodes=None, rels=None))]
fn emit_gql_graph_type_ddl(
    graph_type_name: &str,
    nodes: Option<Vec<Bound<'_, PyAny>>>,
    rels: Option<Vec<Bound<'_, PyAny>>>,
) -> PyResult<String> {
    let mut node_schemas = Vec::new();
    if let Some(n_list) = nodes {
        for n in &n_list {
            node_schemas.push(extract_node_schema(n)?);
        }
    }
    let mut rel_schemas = Vec::new();
    if let Some(r_list) = rels {
        for r in &r_list {
            rel_schemas.push(extract_rel_schema(r)?);
        }
    }
    let node_refs: Vec<&NodeSchema> = node_schemas.iter().collect();
    let rel_refs: Vec<&RelationshipSchema> = rel_schemas.iter().collect();
    Ok(voyager_ogm::emit_gql_graph_type_ddl(
        graph_type_name,
        &node_refs,
        &rel_refs,
    ))
}

#[pyfunction]
fn emit_gql_drop_graph_type_ddl(graph_type_name: &str) -> PyResult<String> {
    Ok(voyager_ogm::emit_gql_drop_graph_type_ddl(graph_type_name))
}

#[pyfunction]
#[pyo3(signature = (graph_name, nodes=None, rels=None, dialect="postgres"))]
fn emit_pgq_property_graph_ddl(
    graph_name: &str,
    nodes: Option<Vec<Bound<'_, PyAny>>>,
    rels: Option<Vec<Bound<'_, PyAny>>>,
    dialect: &str,
) -> PyResult<String> {
    let mut node_schemas = Vec::new();
    if let Some(n_list) = nodes {
        for n in &n_list {
            node_schemas.push(extract_node_schema(n)?);
        }
    }
    let mut rel_schemas = Vec::new();
    if let Some(r_list) = rels {
        for r in &r_list {
            rel_schemas.push(extract_rel_schema(r)?);
        }
    }
    let node_refs: Vec<&NodeSchema> = node_schemas.iter().collect();
    let rel_refs: Vec<&RelationshipSchema> = rel_schemas.iter().collect();
    voyager_ogm::emit_pgq_property_graph_ddl_with_dialect(
        graph_name, &node_refs, &rel_refs, dialect,
    )
    .map_err(to_py_schema_err)
}

#[pyfunction]
#[pyo3(signature = (nodes=None, rels=None, dialect="postgres"))]
fn emit_table_ddl(
    nodes: Option<Vec<Bound<'_, PyAny>>>,
    rels: Option<Vec<Bound<'_, PyAny>>>,
    dialect: &str,
) -> PyResult<Vec<String>> {
    let mut node_schemas = Vec::new();
    if let Some(n_list) = nodes {
        for n in &n_list {
            node_schemas.push(extract_node_schema(n)?);
        }
    }
    let mut rel_schemas = Vec::new();
    if let Some(r_list) = rels {
        for r in &r_list {
            rel_schemas.push(extract_rel_schema(r)?);
        }
    }
    let node_refs: Vec<&NodeSchema> = node_schemas.iter().collect();
    let rel_refs: Vec<&RelationshipSchema> = rel_schemas.iter().collect();
    voyager_ogm::emit_table_ddl(&node_refs, &rel_refs, dialect).map_err(to_py_schema_err)
}

#[pyfunction]
#[pyo3(signature = (graph_name, nodes=None, rels=None, dialect="postgres"))]
fn emit_pgq_schema_ddl(
    graph_name: &str,
    nodes: Option<Vec<Bound<'_, PyAny>>>,
    rels: Option<Vec<Bound<'_, PyAny>>>,
    dialect: &str,
) -> PyResult<Vec<String>> {
    let mut node_schemas = Vec::new();
    if let Some(n_list) = nodes {
        for n in &n_list {
            node_schemas.push(extract_node_schema(n)?);
        }
    }
    let mut rel_schemas = Vec::new();
    if let Some(r_list) = rels {
        for r in &r_list {
            rel_schemas.push(extract_rel_schema(r)?);
        }
    }
    let node_refs: Vec<&NodeSchema> = node_schemas.iter().collect();
    let rel_refs: Vec<&RelationshipSchema> = rel_schemas.iter().collect();
    voyager_ogm::emit_pgq_schema_ddl(graph_name, &node_refs, &rel_refs, dialect)
        .map_err(to_py_schema_err)
}

#[pyfunction]
fn emit_pgq_drop_property_graph_ddl(graph_name: &str) -> PyResult<String> {
    Ok(voyager_ogm::emit_pgq_drop_property_graph_ddl(graph_name))
}

#[pyfunction]
#[pyo3(signature = (nodes=None, rels=None))]
fn emit_cypher25_graph_type_ddl(
    nodes: Option<Vec<Bound<'_, PyAny>>>,
    rels: Option<Vec<Bound<'_, PyAny>>>,
) -> PyResult<String> {
    let mut node_schemas = Vec::new();
    if let Some(n_list) = nodes {
        for n in &n_list {
            node_schemas.push(extract_node_schema(n)?);
        }
    }
    let mut rel_schemas = Vec::new();
    if let Some(r_list) = rels {
        for r in &r_list {
            rel_schemas.push(extract_rel_schema(r)?);
        }
    }
    let node_refs: Vec<&NodeSchema> = node_schemas.iter().collect();
    let rel_refs: Vec<&RelationshipSchema> = rel_schemas.iter().collect();
    Ok(voyager_ogm::emit_cypher25_graph_type_ddl(
        &node_refs, &rel_refs,
    ))
}

#[pyfunction]
fn emit_cypher25_drop_graph_type_ddl() -> PyResult<String> {
    Ok(voyager_ogm::emit_cypher25_drop_graph_type_ddl())
}

#[pyfunction]
#[pyo3(signature = (node, dialect="cypher", graph_name=None))]
fn emit_node_index_ddl(
    node: &Bound<'_, PyAny>,
    dialect: &str,
    graph_name: Option<&str>,
) -> PyResult<Vec<String>> {
    let schema = extract_node_schema(node)?;
    voyager_ogm::emit_node_index_ddl_with_graph(&schema, dialect, graph_name)
        .map_err(to_py_schema_err)
}

#[pyfunction]
#[pyo3(signature = (rel, dialect="cypher", graph_name=None))]
fn emit_rel_index_ddl(
    rel: &Bound<'_, PyAny>,
    dialect: &str,
    graph_name: Option<&str>,
) -> PyResult<Vec<String>> {
    let schema = extract_rel_schema(rel)?;
    voyager_ogm::emit_rel_index_ddl_with_graph(&schema, dialect, graph_name)
        .map_err(to_py_schema_err)
}

#[pyfunction]
#[pyo3(signature = (node, dialect="cypher", include_type_constraints=false, graph_name=None))]
fn emit_node_constraint_ddl(
    node: &Bound<'_, PyAny>,
    dialect: &str,
    include_type_constraints: bool,
    graph_name: Option<&str>,
) -> PyResult<Vec<String>> {
    let schema = extract_node_schema(node)?;
    voyager_ogm::emit_node_constraint_ddl_with_graph(
        &schema,
        dialect,
        include_type_constraints,
        graph_name,
    )
    .map_err(to_py_schema_err)
}

#[pyfunction]
#[pyo3(signature = (rel, dialect="cypher", include_type_constraints=false, graph_name=None))]
fn emit_rel_constraint_ddl(
    rel: &Bound<'_, PyAny>,
    dialect: &str,
    include_type_constraints: bool,
    graph_name: Option<&str>,
) -> PyResult<Vec<String>> {
    let schema = extract_rel_schema(rel)?;
    voyager_ogm::emit_rel_constraint_ddl_with_graph(
        &schema,
        dialect,
        include_type_constraints,
        graph_name,
    )
    .map_err(to_py_schema_err)
}

#[pyfunction]
#[pyo3(signature = (node, dialect="cypher", graph_name=None))]
fn emit_node_drop_index_ddl(
    node: &Bound<'_, PyAny>,
    dialect: &str,
    graph_name: Option<&str>,
) -> PyResult<Vec<String>> {
    let schema = extract_node_schema(node)?;
    voyager_ogm::emit_node_drop_index_ddl_with_graph(&schema, dialect, graph_name)
        .map_err(to_py_schema_err)
}

#[pyfunction]
#[pyo3(signature = (rel, dialect="cypher", graph_name=None))]
fn emit_rel_drop_index_ddl(
    rel: &Bound<'_, PyAny>,
    dialect: &str,
    graph_name: Option<&str>,
) -> PyResult<Vec<String>> {
    let schema = extract_rel_schema(rel)?;
    voyager_ogm::emit_rel_drop_index_ddl_with_graph(&schema, dialect, graph_name)
        .map_err(to_py_schema_err)
}

#[pyfunction]
#[pyo3(signature = (node, dialect="cypher", include_type_constraints=false, graph_name=None))]
fn emit_node_drop_constraint_ddl(
    node: &Bound<'_, PyAny>,
    dialect: &str,
    include_type_constraints: bool,
    graph_name: Option<&str>,
) -> PyResult<Vec<String>> {
    let schema = extract_node_schema(node)?;
    voyager_ogm::emit_node_drop_constraint_ddl_with_graph(
        &schema,
        dialect,
        include_type_constraints,
        graph_name,
    )
    .map_err(to_py_schema_err)
}

#[pyfunction]
#[pyo3(signature = (rel, dialect="cypher", include_type_constraints=false, graph_name=None))]
fn emit_rel_drop_constraint_ddl(
    rel: &Bound<'_, PyAny>,
    dialect: &str,
    include_type_constraints: bool,
    graph_name: Option<&str>,
) -> PyResult<Vec<String>> {
    let schema = extract_rel_schema(rel)?;
    voyager_ogm::emit_rel_drop_constraint_ddl_with_graph(
        &schema,
        dialect,
        include_type_constraints,
        graph_name,
    )
    .map_err(to_py_schema_err)
}

/// Returns the canonical deterministic vector index name for a given label and property across dialects.
///
/// Note: Defaults to Cypher naming (`index_{label.to_lowercase()}_{prop.to_lowercase()}`)
/// if `dialect` is omitted or unrecognized.
#[pyfunction]
#[pyo3(signature = (label, prop, dialect=None))]
fn canonical_vector_index_name(label: &str, prop: &str, dialect: Option<&str>) -> PyResult<String> {
    let ddl_dialect = if let Some(d) = dialect {
        DdlDialect::parse_str(d).unwrap_or(DdlDialect::Cypher)
    } else {
        DdlDialect::Cypher
    };
    Ok(core_canonical_vector_index_name(label, prop, ddl_dialect))
}

/// Returns the canonical deterministic full-text index name for a given label and properties across dialects.
///
/// Note: Defaults to Cypher naming (`index_{label.to_lowercase()}_{props.join("_").to_lowercase()}`)
/// if `dialect` is omitted or unrecognized.
#[pyfunction]
#[pyo3(signature = (label, properties, dialect=None))]
fn canonical_fulltext_index_name(
    label: &str,
    properties: Vec<String>,
    dialect: Option<&str>,
) -> PyResult<String> {
    let ddl_dialect = if let Some(d) = dialect {
        DdlDialect::parse_str(d).unwrap_or(DdlDialect::Cypher)
    } else {
        DdlDialect::Cypher
    };
    Ok(core_canonical_fulltext_index_name(
        label,
        &properties,
        ddl_dialect,
    ))
}

/// Adapts a Memgraph MAGE Tantivy query using quote-aware lexical scanning.
#[pyfunction]
#[pyo3(signature = (query, properties=vec![]))]
fn adapt_tantivy_query(query: &str, properties: Vec<String>) -> PyResult<String> {
    Ok(core_adapt_tantivy_query(query, &properties))
}

/// Resolves cross-dialect full-text procedure name, target arguments, and query adaptation.
#[allow(clippy::too_many_arguments)]
#[pyfunction]
#[pyo3(signature = (dialect, is_relationship, target_name, falkordb_target, query_string, properties=vec![]))]
fn resolve_fulltext_search<'py>(
    py: Python<'py>,
    dialect: &str,
    is_relationship: bool,
    target_name: &str,
    falkordb_target: &str,
    query_string: &str,
    properties: Vec<String>,
) -> PyResult<Bound<'py, PyDict>> {
    let d = DdlDialect::parse_str(dialect).unwrap_or(DdlDialect::Cypher);
    let resolved = core_resolve_fulltext_search(
        d,
        is_relationship,
        target_name,
        falkordb_target,
        query_string,
        &properties,
    );
    let dict = PyDict::new(py);
    dict.set_item("procedure", resolved.procedure)?;
    dict.set_item("target_argument", resolved.target_argument)?;
    dict.set_item("query_string", resolved.query_string)?;
    dict.set_item("yield_entity", resolved.yield_entity)?;
    Ok(dict)
}

/// Connects to `uri` and returns its live graph schema as a JSON string.
///
/// The JSON is the `voyager_drivers::introspect` result, identical to
/// `voyager introspect --format json`. The GIL is released while the catalog
/// queries run. Connection and query failures raise `VoyagerError` with the
/// driver error code; Apache AGE and DuckDB raise `VoyagerError` with code
/// `UNSUPPORTED_FEATURE`.
#[pyfunction]
#[pyo3(signature = (uri, sample_size=voyager_drivers::introspect::DEFAULT_SAMPLE_SIZE))]
fn introspect_schema_json(py: Python<'_>, uri: &str, sample_size: usize) -> PyResult<String> {
    let parsed = voyager_drivers::ParsedUri::parse(uri)
        .map_err(|error| make_voyager_err(error.code(), error.to_string()))?;
    let options = voyager_drivers::IntrospectOptions { sample_size };
    let schema = py
        .detach(|| {
            get_tokio_runtime()
                .block_on(async move { voyager_drivers::introspect(&parsed, &options).await })
        })
        .map_err(|error| {
            make_voyager_err(
                error.code(),
                voyager_drivers::introspect::describe_error(&error),
            )
        })?;
    serde_json::to_string(&schema.to_json_value())
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))
}

/// Native Python module definition for `_voyager_rs`.
#[pymodule]
fn _voyager_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(version, m)?)?;
    m.add_function(wrap_pyfunction!(generate_synthetic_stream, m)?)?;
    m.add_function(wrap_pyfunction!(compile_query_from_spec, m)?)?;
    m.add_function(wrap_pyfunction!(extract_topology_from_query, m)?)?;
    m.add_function(wrap_pyfunction!(validate_query_against_schema, m)?)?;
    m.add_function(wrap_pyfunction!(compile_bulk_create, m)?)?;
    m.add_function(wrap_pyfunction!(compile_bulk_merge, m)?)?;
    m.add_function(wrap_pyfunction!(compile_bulk_merge_create_only, m)?)?;
    m.add_function(wrap_pyfunction!(compile_bulk_create_rel, m)?)?;
    m.add_function(wrap_pyfunction!(get_query_cache_stats, m)?)?;
    m.add_function(wrap_pyfunction!(clear_query_cache, m)?)?;
    m.add_function(wrap_pyfunction!(get_runtime_pid, m)?)?;
    m.add_function(wrap_pyfunction!(emit_cypher_node_ddl, m)?)?;
    m.add_function(wrap_pyfunction!(emit_cypher_rel_ddl, m)?)?;
    m.add_function(wrap_pyfunction!(emit_cypher_drop_node_ddl, m)?)?;
    m.add_function(wrap_pyfunction!(emit_cypher_drop_rel_ddl, m)?)?;
    m.add_function(wrap_pyfunction!(emit_memgraph_node_ddl, m)?)?;
    m.add_function(wrap_pyfunction!(emit_memgraph_rel_ddl, m)?)?;
    m.add_function(wrap_pyfunction!(emit_memgraph_drop_node_ddl, m)?)?;
    m.add_function(wrap_pyfunction!(emit_memgraph_drop_rel_ddl, m)?)?;
    m.add_function(wrap_pyfunction!(emit_gql_alter_node_ddl, m)?)?;
    m.add_function(wrap_pyfunction!(emit_gql_alter_rel_ddl, m)?)?;
    m.add_function(wrap_pyfunction!(emit_gql_graph_type_ddl, m)?)?;
    m.add_function(wrap_pyfunction!(emit_gql_drop_graph_type_ddl, m)?)?;
    m.add_function(wrap_pyfunction!(emit_pgq_property_graph_ddl, m)?)?;
    m.add_function(wrap_pyfunction!(emit_pgq_drop_property_graph_ddl, m)?)?;
    m.add_function(wrap_pyfunction!(emit_table_ddl, m)?)?;
    m.add_function(wrap_pyfunction!(emit_pgq_schema_ddl, m)?)?;
    m.add_function(wrap_pyfunction!(emit_cypher25_graph_type_ddl, m)?)?;
    m.add_function(wrap_pyfunction!(emit_cypher25_drop_graph_type_ddl, m)?)?;
    m.add_function(wrap_pyfunction!(emit_node_index_ddl, m)?)?;
    m.add_function(wrap_pyfunction!(emit_rel_index_ddl, m)?)?;
    m.add_function(wrap_pyfunction!(emit_node_constraint_ddl, m)?)?;
    m.add_function(wrap_pyfunction!(emit_rel_constraint_ddl, m)?)?;
    m.add_function(wrap_pyfunction!(emit_node_drop_index_ddl, m)?)?;
    m.add_function(wrap_pyfunction!(emit_rel_drop_index_ddl, m)?)?;
    m.add_function(wrap_pyfunction!(emit_node_drop_constraint_ddl, m)?)?;
    m.add_function(wrap_pyfunction!(emit_rel_drop_constraint_ddl, m)?)?;
    m.add_function(wrap_pyfunction!(canonical_vector_index_name, m)?)?;
    m.add_function(wrap_pyfunction!(canonical_fulltext_index_name, m)?)?;
    m.add_function(wrap_pyfunction!(adapt_tantivy_query, m)?)?;
    m.add_function(wrap_pyfunction!(resolve_fulltext_search, m)?)?;
    m.add_function(wrap_pyfunction!(introspect_schema_json, m)?)?;
    m.add_function(wrap_pyfunction!(inspect_native_params, m)?)?;
    m.add_class::<PyQueryBuilder>()?;
    m.add_class::<PyAstExpr>()?;
    m.add_class::<PyArrowStream>()?;
    m.add_class::<PyNativeQueryResult>()?;
    m.add_class::<PyNativeClient>()?;
    m.add_class::<PyNativeTransaction>()?;
    m.add_class::<PyNativeCircuitRouter>()?;
    m.add_class::<PyUnitOfWork>()?;
    m.add_class::<PyUnitOfWorkScope>()?;
    m.add_class::<PyNativeSchemaRegistry>()?;
    m.add("VoyagerError", m.py().get_type::<VoyagerError>())?;
    // pyo3::create_exception! generates a standard PyErr type whose C-level tp_init rejects keyword arguments.
    // We install a custom Python __init__ so callers can instantiate VoyagerError(message, code="...") ergonomics.
    // `skipped` is initialized to an empty list on every instance; schema helpers replace it with the
    // statements skipped on Neo4j Community. The class-level default also covers errors raised from Rust,
    // which bypass this Python __init__.
    m.py().run(
        c"def _init(self, message='', *args, code=None, **kwargs):\n    super(type(self), self).__init__(message, *args, **kwargs)\n    self.code = code\n    self.path = None\n    self.skipped = []\nVoyagerError.__init__ = _init\nVoyagerError.path = None\nVoyagerError.skipped = []\n",
        Some(&m.dict()),
        None,
    )?;
    m.add("__version__", voyager_ogm::VERSION)?;
    m.add("QUERY_SPEC_VERSION", voyager_ogm::spec::SPEC_VERSION)?;
    m.add("QUERY_SPEC_REVISION", voyager_ogm::spec::SPEC_REVISION)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_query_result_reports_age_columns() {
        let batch = arrow_array::RecordBatch::new_empty(Arc::new(arrow_schema::Schema::empty()));
        let result = PyNativeQueryResult {
            stream: PyArrowStream::from_batch(batch),
            columns: vec!["n".to_string(), "age".to_string()],
            summary: voyager_drivers::QuerySummary::empty(),
            age_columns: vec![false, true],
        };
        assert!(result.has_age_columns());
    }

    #[test]
    fn native_query_result_without_age_columns_is_plain_arrow() {
        let batch = arrow_array::RecordBatch::new_empty(Arc::new(arrow_schema::Schema::empty()));
        let result = PyNativeQueryResult {
            stream: PyArrowStream::from_batch(batch),
            columns: vec!["n".to_string()],
            summary: voyager_drivers::QuerySummary::empty(),
            age_columns: Vec::new(),
        };
        assert!(!result.has_age_columns());
    }
}
