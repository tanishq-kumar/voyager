//! Query Spec v1 consumer: JSON document to [`QueryBuilder`] and compiled query.
//!
//! The consumer accepts the same documents the Python bindings have always
//! accepted, including the historical error messages and JSON Pointer paths.
//! Python-specific spellings (tuples, ordered dictionaries and native scalar
//! values) are converted to [`serde_json::Value`] by the bindings before they
//! call this module, so the validation and build logic lives here once.
//!
//! # Example
//!
//! ```
//! use serde_json::json;
//!
//! let spec = json!({
//!     "spec_version": 1,
//!     "matches": [{"paths": [[["node", "p", ["Person"]]]]}],
//!     "projections": [["field", "p", "name", "n"]]
//! });
//!
//! let mut builder = voyager_ogm::spec::build_query(&spec).expect("valid spec");
//! let (arena, root) = builder.try_build().expect("buildable query");
//! assert!(!arena.get(root).is_err());
//!
//! let compiled = voyager_ogm::spec::compile(
//!     &spec,
//!     "cypher",
//!     &voyager_ogm::spec::SpecCompileOptions::default(),
//! )
//! .expect("compilable spec");
//! assert_eq!(compiled.compiled.statement, "MATCH (p:Person) RETURN p.name AS n");
//! ```

use std::collections::HashSet;
use std::fmt;

use serde_json::{Map, Value};

use crate::ast::{
    AggregationFunc, BinaryOp, Direction, LiteralValue, NodeHandle, PaginationValue, PathMode,
    UnaryOp,
};
use crate::builder::QueryBuilder;
use crate::cache::global_query_cache;
use crate::emitters::{AgeEmitter, CypherEmitter, DEFAULT_AGE_GRAPH, IsoGqlEmitter, SqlPgqEmitter};
use crate::error::ErrorCode;
use crate::optimizer::{AstOptimizer, OptimizationLevel};
use crate::visitor::{AstVisitor, CompiledQuery};

/// Maximum expression recursion depth accepted by the consumer.
pub const MAX_AST_DEPTH: usize = 256;

/// Prefix a foreign bridge uses to encode an object key that is not a JSON
/// string.
///
/// JSON object keys are always strings, while the Python bindings must keep the
/// consumer's "object keys must be strings, got number" diagnostics. The bridge
/// therefore encodes such a key with [`encode_non_string_key`]; the consumer
/// recognises it while walking an object and reports the original JSON type.
pub const NON_STRING_KEY_PREFIX: &str = "\u{0}voyager-non-string-key:";

/// Encodes an object key that could not be represented as a JSON string.
///
/// `json_type_name` is the JSON type name of the original key (`number`,
/// `boolean`, `null`, `array`, `object`, or `value`).
pub fn encode_non_string_key(json_type_name: &str) -> String {
    format!("{NON_STRING_KEY_PREFIX}{json_type_name}")
}

fn non_string_key_kind(key: &str) -> Option<&str> {
    key.strip_prefix(NON_STRING_KEY_PREFIX)
}

/// A Query Spec consumer error.
///
/// [`code`](Self::code) is the machine-readable Voyager error code,
/// [`path`](Self::path) is the JSON Pointer to the offending document element
/// (empty for the document root, and `None` when the underlying compiler did
/// not produce a spec path), and [`message`](Self::message) is the exact
/// user-facing message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpecError {
    code: ErrorCode,
    path: Option<String>,
    message: String,
    warnings: Vec<String>,
}

impl SpecError {
    /// Creates an error with an explicit code and optional JSON Pointer.
    pub fn new(code: ErrorCode, path: Option<String>, message: impl Into<String>) -> Self {
        Self {
            code,
            path,
            message: message.into(),
            warnings: Vec::new(),
        }
    }

    /// Returns the machine-readable error code.
    pub fn code(&self) -> ErrorCode {
        self.code
    }

    /// Returns the JSON Pointer to the offending element, when one applies.
    pub fn path(&self) -> Option<&str> {
        self.path.as_deref()
    }

    /// Returns the user-facing error message.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Returns warnings produced before the error was raised.
    ///
    /// The Python bindings emit these with the `warnings` module before raising
    /// the error, matching the historical consumer order.
    pub fn warnings(&self) -> &[String] {
        &self.warnings
    }

    fn with_warnings(mut self, warnings: Vec<String>) -> Self {
        self.warnings = warnings;
        self
    }
}

impl fmt::Display for SpecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for SpecError {}

/// Options for [`compile`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpecCompileOptions {
    /// Graph name used by the SQL/PGQ and Apache AGE emitters.
    pub graph_name: Option<String>,
    /// Whether to run the AST optimiser before emission.
    pub optimize: bool,
    /// Optimiser level name (`minimal`, `standard`, or `aggressive`).
    pub optimization_level: String,
    /// Whether to reuse the global compiled-query cache.
    pub use_cache: bool,
}

impl Default for SpecCompileOptions {
    fn default() -> Self {
        Self {
            graph_name: None,
            optimize: false,
            optimization_level: "standard".to_string(),
            use_cache: true,
        }
    }
}

/// The result of compiling a Query Spec document.
#[derive(Debug, Clone, PartialEq)]
pub struct SpecCompileResult {
    /// The emitted statement, parameters, execution mode and columns.
    pub compiled: CompiledQuery,
    /// Non-fatal warnings the caller should surface to the user.
    pub warnings: Vec<String>,
}

/// JSON Pointer to an element of a Query Spec v1 document.
///
/// `/` and `~` are escaped as `~1` and `~0`, as JSON Pointer requires. The
/// document root is the empty string.
#[derive(Clone, Debug, PartialEq, Eq)]
struct SpecPath {
    pointer: String,
}

impl SpecPath {
    fn root() -> Self {
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

    fn pointer(&self) -> &str {
        &self.pointer
    }
}

/// JSON type name for a value, used in spec error messages.
fn json_type_name(val: &Value) -> &'static str {
    match val {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Renders a scalar value the way JSON spells it, for error messages.
fn render_json_scalar(val: &Value) -> Option<String> {
    match val {
        Value::Null => Some("null".to_string()),
        Value::Bool(boolean) => Some(boolean.to_string()),
        Value::Number(number) => {
            if let Some(integer) = number.as_i64() {
                Some(integer.to_string())
            } else if let Some(integer) = number.as_u64() {
                Some(integer.to_string())
            } else {
                number.as_f64().map(|float| float.to_string())
            }
        }
        Value::String(string) => Some(format!(
            "\"{}\"",
            string.replace('\\', "\\\\").replace('"', "\\\"")
        )),
        Value::Array(_) | Value::Object(_) => None,
    }
}

/// Builds an `INVALID_SPEC` error with the JSON Pointer on the path and in the
/// message.
fn make_spec_err(path: &SpecPath, msg: impl Into<String>) -> SpecError {
    let pointer = path.pointer();
    let msg = msg.into();
    let message = if pointer.is_empty() {
        format!("Invalid query spec at the document root: {msg}")
    } else {
        format!("Invalid query spec at '{pointer}': {msg}")
    };
    SpecError::new(ErrorCode::InvalidSpec, Some(pointer.to_string()), message)
}

fn expected_error(val: &Value, path: &SpecPath, expected: &str) -> SpecError {
    make_spec_err(
        path,
        format!("expected {expected}, got {}", json_type_name(val)),
    )
}

/// Extracts a JSON string for a spec element.
fn extract_string(val: &Value, path: &SpecPath, expected: &str) -> Result<String, SpecError> {
    val.as_str()
        .map(str::to_owned)
        .ok_or_else(|| expected_error(val, path, expected))
}

/// Extracts a JSON boolean for a spec element.
fn extract_bool(val: &Value, path: &SpecPath, expected: &str) -> Result<bool, SpecError> {
    val.as_bool()
        .ok_or_else(|| expected_error(val, path, expected))
}

/// Extracts an unsigned 32-bit integer for a spec element.
fn extract_u32(val: &Value, path: &SpecPath, expected: &str) -> Result<u32, SpecError> {
    match val {
        Value::Number(number) if number.is_i64() || number.is_u64() => {
            if let Some(integer) = number.as_u64()
                && integer <= u64::from(u32::MAX)
            {
                return Ok(integer as u32);
            }
            Err(expected_error(val, path, expected))
        }
        _ => Err(expected_error(val, path, expected)),
    }
}

/// Extracts an optional JSON string (a string or `null`).
fn extract_optional_string(
    val: &Value,
    path: &SpecPath,
    expected: &str,
) -> Result<Option<String>, SpecError> {
    match val {
        Value::Null => Ok(None),
        Value::String(string) => Ok(Some(string.clone())),
        _ => Err(expected_error(val, path, expected)),
    }
}

/// Extracts an array of strings.
fn extract_string_array(
    val: &Value,
    path: &SpecPath,
    expected: &str,
) -> Result<Vec<String>, SpecError> {
    let Some(items) = val.as_array() else {
        return Err(expected_error(val, path, expected));
    };
    let mut strings = Vec::with_capacity(items.len());
    for item in items {
        let Some(string) = item.as_str() else {
            return Err(expected_error(val, path, expected));
        };
        strings.push(string.to_string());
    }
    Ok(strings)
}

/// Extracts an optional unsigned 32-bit integer (an integer or `null`).
fn extract_optional_u32(
    val: &Value,
    path: &SpecPath,
    expected: &str,
) -> Result<Option<u32>, SpecError> {
    match val {
        Value::Null => Ok(None),
        Value::Number(number) if number.is_i64() || number.is_u64() => {
            if let Some(integer) = number.as_u64()
                && integer <= u64::from(u32::MAX)
            {
                return Ok(Some(integer as u32));
            }
            Err(expected_error(val, path, expected))
        }
        _ => Err(expected_error(val, path, expected)),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SpecMaxHops {
    Null,
    Exact(u32),
    Unbounded,
}

fn extract_max_hops(val: &Value, path: &SpecPath) -> Result<SpecMaxHops, SpecError> {
    match val {
        Value::Null => Ok(SpecMaxHops::Null),
        Value::String(s) if s == "unbounded" => Ok(SpecMaxHops::Unbounded),
        Value::Number(number) if number.is_i64() || number.is_u64() => {
            if let Some(integer) = number.as_u64()
                && integer <= u64::from(u32::MAX)
            {
                return Ok(SpecMaxHops::Exact(integer as u32));
            }
            Err(expected_error(
                val,
                path,
                "a non-negative integer, null, or 'unbounded'",
            ))
        }
        _ => Err(expected_error(
            val,
            path,
            "a non-negative integer, null, or 'unbounded'",
        )),
    }
}

/// Converts a JSON literal value into an AST literal.
fn json_to_literal(value: &Value) -> Result<LiteralValue, String> {
    match value {
        Value::Null => Ok(LiteralValue::Null),
        Value::Bool(boolean) => Ok(LiteralValue::Bool(*boolean)),
        Value::Number(number) => {
            if let Some(integer) = number.as_i64() {
                Ok(LiteralValue::Int64(integer))
            } else if let Some(integer) = number.as_u64() {
                i64::try_from(integer)
                    .map(LiteralValue::Int64)
                    .map_err(|_| "Unsupported parameter type: number".to_string())
            } else if let Some(float) = number.as_f64() {
                Ok(LiteralValue::Float64(float))
            } else {
                Err("Unsupported parameter type: number".to_string())
            }
        }
        Value::String(string) => Ok(LiteralValue::String(string.clone())),
        Value::Array(items) => {
            let mut literals = Vec::with_capacity(items.len());
            for item in items {
                literals.push(json_to_literal(item)?);
            }
            Ok(LiteralValue::List(literals))
        }
        Value::Object(members) => {
            let mut entries = Vec::with_capacity(members.len());
            for (key, value) in members {
                if let Some(kind) = non_string_key_kind(key) {
                    return Err(format!("Unsupported parameter type: {kind}"));
                }
                entries.push((key.clone(), json_to_literal(value)?));
            }
            Ok(LiteralValue::Map(entries))
        }
    }
}

fn parse_binary_op(s: &str) -> Result<BinaryOp, String> {
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
        other => Err(format!("Unknown binary operator: '{other}'")),
    }
}

fn parse_unary_op(s: &str) -> Result<UnaryOp, String> {
    match s.to_ascii_lowercase().as_str() {
        "not" | "~" => Ok(UnaryOp::Not),
        "-" | "neg" => Ok(UnaryOp::Neg),
        "is_null" | "is null" => Ok(UnaryOp::IsNull),
        "is_not_null" | "is not null" => Ok(UnaryOp::IsNotNull),
        other => Err(format!("Unknown unary operator: '{other}'")),
    }
}

/// Parses the direction of an `order_by` entry.
fn parse_order_asc(val: &Value, path: &SpecPath, field: &str) -> Result<bool, SpecError> {
    if let Some(asc) = val.as_bool() {
        return Ok(asc);
    }
    if let Some(s) = val.as_str() {
        match s {
            "ASC" => return Ok(true),
            "DESC" => return Ok(false),
            _ => {}
        }
    }
    Err(make_spec_err(
        path,
        format!(
            "Invalid '{field}': expected a boolean or 'ASC'/'DESC', got {}.",
            json_type_name(val)
        ),
    ))
}

const EXPRESSION_ARRAY_HINT: &str =
    "Use [\"lit\", [...]] for a literal list or [\"list\", [...]] for a list of expressions.";

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
            | "label"
    )
}

/// Reads a spec array position, rejecting a non-array with `INVALID_SPEC`.
fn expect_spec_array<'a>(
    val: &'a Value,
    path: &SpecPath,
    field: &str,
) -> Result<&'a Vec<Value>, SpecError> {
    val.as_array().ok_or_else(|| {
        make_spec_err(
            path,
            format!(
                "Invalid '{field}': expected a JSON array (list or tuple), got {}.",
                json_type_name(val)
            ),
        )
    })
}

/// Validates the item count of a fixed-arity spec array.
fn check_spec_arity(
    array: &[Value],
    path: &SpecPath,
    field: &str,
    min_items: usize,
    shape: &str,
) -> Result<(), SpecError> {
    let len = array.len();
    if len < min_items {
        return Err(make_spec_err(
            path,
            format!("Invalid '{field}': expected at least {min_items} item(s) {shape}, got {len}."),
        ));
    }
    Ok(())
}

/// Validates the item count of a fixed-arity spec array against both bounds.
fn check_spec_arity_bounded(
    array: &[Value],
    path: &SpecPath,
    field: &str,
    min_items: usize,
    max_items: usize,
    shape: &str,
) -> Result<(), SpecError> {
    check_spec_arity(array, path, field, min_items, shape)?;
    if array.len() > max_items {
        let extra = &array[max_items];
        return Err(make_spec_err(
            &path.index(max_items),
            format!(
                "Invalid '{field}': unexpected {} after the documented shape {shape}. This position accepts at most {max_items} item(s).",
                json_type_name(extra)
            ),
        ));
    }
    Ok(())
}

/// Strips one leading `$` from a parameter name.
fn strip_leading_dollar(name: String) -> String {
    match name.strip_prefix('$') {
        Some(stripped) => stripped.to_string(),
        None => name,
    }
}

/// Rejects items past a path step's documented shape unless they are `null`.
fn reject_extra_step_items(
    array: &[Value],
    path: &SpecPath,
    field: &str,
    documented_items: usize,
    shape: &str,
) -> Result<(), SpecError> {
    for (idx, item) in array.iter().enumerate().skip(documented_items) {
        if !item.is_null() {
            return Err(make_spec_err(
                &path.index(idx),
                format!(
                    "Invalid {field}: unexpected {} after the documented shape {shape}. This path step accepts no further items.",
                    json_type_name(item)
                ),
            ));
        }
    }
    Ok(())
}

/// Parses a path step's optional properties item into `property -> expression`
/// pairs.
fn parse_step_properties(
    builder: &mut QueryBuilder,
    array: &[Value],
    idx: usize,
    depth: usize,
    step_path: &SpecPath,
    field: &str,
) -> Result<Vec<(String, NodeHandle)>, SpecError> {
    let Some(item) = array.get(idx) else {
        return Ok(Vec::new());
    };
    if item.is_null() {
        return Ok(Vec::new());
    }
    let properties_path = step_path.index(idx);
    let Some(dict) = item.as_object() else {
        return Err(make_spec_err(
            &properties_path,
            format!(
                "Invalid {field} properties: expected a JSON object mapping property names to expressions, or null, got {}.",
                json_type_name(item)
            ),
        ));
    };
    let mut properties = Vec::with_capacity(dict.len());
    for (key, value) in dict {
        if let Some(kind) = non_string_key_kind(key) {
            return Err(make_spec_err(
                &properties_path,
                format!("Invalid {field} properties: property names must be strings, got {kind}."),
            ));
        }
        let value_path = properties_path.key(key);
        let value_handle = value_to_node_handle(builder, value, depth, &value_path)?;
        properties.push((key.clone(), value_handle));
    }
    Ok(properties)
}

/// Recovers the alias of an `['alias', inner_expression, name]` expression spec.
fn extract_alias_from_expr_spec(
    expr_spec: &Value,
    path: &SpecPath,
) -> Result<Option<String>, SpecError> {
    if let Some(array) = expr_spec.as_array()
        && !array.is_empty()
        && array.first().and_then(Value::as_str) == Some("alias")
    {
        check_spec_arity_bounded(
            array,
            path,
            "expression 'alias'",
            3,
            3,
            "[alias, expression, name]",
        )?;
        let name = extract_string(&array[2], &path.index(2), "a string alias name")?;
        return Ok(Some(name));
    }
    Ok(None)
}

#[inline(never)]
fn handle_prop_tuple(
    builder: &mut QueryBuilder,
    array: &[Value],
    path: &SpecPath,
) -> Result<NodeHandle, SpecError> {
    check_spec_arity_bounded(
        array,
        path,
        "expression 'prop'",
        3,
        3,
        "[prop, variable, property]",
    )?;
    let var = extract_string(&array[1], &path.index(1), "a string variable")?;
    if var.is_empty() {
        return Err(make_spec_err(
            &path.index(1),
            "Invalid 'prop': the variable must be a non-empty string.",
        ));
    }
    let prop = extract_string(&array[2], &path.index(2), "a string property")?;
    Ok(builder.prop(var, prop))
}

#[inline(never)]
fn handle_label_tuple(
    builder: &mut QueryBuilder,
    array: &[Value],
    path: &SpecPath,
) -> Result<NodeHandle, SpecError> {
    check_spec_arity_bounded(
        array,
        path,
        "expression 'label'",
        3,
        3,
        "[label, variable, labels]",
    )?;
    let variable = extract_string(&array[1], &path.index(1), "a string variable")?;
    if variable.is_empty() {
        return Err(make_spec_err(
            &path.index(1),
            "Invalid 'label': the variable must be a non-empty string.",
        ));
    }
    let labels_path = path.index(2);
    let labels = expect_spec_array(&array[2], &labels_path, "label names")?;
    if labels.is_empty() {
        return Err(make_spec_err(
            &labels_path,
            "Invalid 'label': the labels list must contain at least one label name.",
        ));
    }
    let mut names = Vec::with_capacity(labels.len());
    for (index, label) in labels.iter().enumerate() {
        let label_path = labels_path.index(index);
        let name = extract_string(label, &label_path, "a string label name")?;
        if name.is_empty() {
            return Err(make_spec_err(
                &label_path,
                "Invalid 'label': label names must be non-empty strings.",
            ));
        }
        names.push(name);
    }
    Ok(builder.label_predicate(variable, names))
}

#[inline(never)]
fn handle_ident_tuple(
    builder: &mut QueryBuilder,
    array: &[Value],
    path: &SpecPath,
) -> Result<NodeHandle, SpecError> {
    check_spec_arity_bounded(array, path, "expression 'ident'", 2, 2, "[ident, name]")?;
    let name = extract_string(&array[1], &path.index(1), "a string name")?;
    if name.is_empty() {
        return Err(make_spec_err(
            &path.index(1),
            "Invalid 'ident': the identifier must be a non-empty string.",
        ));
    }
    Ok(builder.ident(name))
}

#[inline(never)]
fn handle_param_tuple(
    builder: &mut QueryBuilder,
    array: &[Value],
    path: &SpecPath,
) -> Result<NodeHandle, SpecError> {
    check_spec_arity_bounded(array, path, "expression 'param'", 2, 2, "[param, name]")?;
    let name = extract_string(&array[1], &path.index(1), "a string name")?;
    let name = strip_leading_dollar(name);
    if name.is_empty() {
        return Err(make_spec_err(
            &path.index(1),
            "Invalid 'param': the parameter name must be a non-empty string.",
        ));
    }
    Ok(builder.param(name))
}

#[inline(never)]
fn handle_lit_tuple(
    builder: &mut QueryBuilder,
    array: &[Value],
    path: &SpecPath,
) -> Result<NodeHandle, SpecError> {
    check_spec_arity_bounded(array, path, "expression 'lit'", 2, 2, "[lit, value]")?;
    let value_path = path.index(1);
    let lit = json_to_literal(&array[1]).map_err(|message| make_spec_err(&value_path, message))?;
    Ok(builder.literal(lit))
}

#[inline(never)]
fn handle_fn_tuple(
    builder: &mut QueryBuilder,
    array: &[Value],
    depth: usize,
    path: &SpecPath,
) -> Result<NodeHandle, SpecError> {
    check_spec_arity_bounded(array, path, "expression 'fn'", 3, 3, "[fn, name, args]")?;
    let name = extract_string(&array[1], &path.index(1), "a string name")?;
    let args_path = path.index(2);
    let args = expect_spec_array(&array[2], &args_path, "fn args")?;
    let mut arg_handles = Vec::with_capacity(args.len());
    for (idx, arg) in args.iter().enumerate() {
        arg_handles.push(value_to_node_handle(
            builder,
            arg,
            depth + 1,
            &args_path.index(idx),
        )?);
    }
    Ok(builder.function(name, arg_handles))
}

#[inline(never)]
fn handle_case_tuple(
    builder: &mut QueryBuilder,
    array: &[Value],
    depth: usize,
    path: &SpecPath,
) -> Result<NodeHandle, SpecError> {
    check_spec_arity_bounded(
        array,
        path,
        "expression 'case'",
        4,
        4,
        "[case, operand, branches, else_branch]",
    )?;
    let operand = if array[1].is_null() {
        None
    } else {
        Some(value_to_node_handle(
            builder,
            &array[1],
            depth + 1,
            &path.index(1),
        )?)
    };
    let branches_path = path.index(2);
    let branches = expect_spec_array(&array[2], &branches_path, "case branches")?;
    let mut parsed_branches = Vec::with_capacity(branches.len());
    for (idx, item) in branches.iter().enumerate() {
        let branch_path = branches_path.index(idx);
        let branch = expect_spec_array(item, &branch_path, "case branch")?;
        check_spec_arity_bounded(branch, &branch_path, "case branch", 2, 2, "[when, then]")?;
        let when = value_to_node_handle(builder, &branch[0], depth + 1, &branch_path.index(0))?;
        let then = value_to_node_handle(builder, &branch[1], depth + 1, &branch_path.index(1))?;
        parsed_branches.push((when, then));
    }
    let else_branch = if array[3].is_null() {
        None
    } else {
        Some(value_to_node_handle(
            builder,
            &array[3],
            depth + 1,
            &path.index(3),
        )?)
    };
    Ok(builder.case_when(operand, parsed_branches, else_branch))
}

#[inline(never)]
fn handle_list_comp_tuple(
    builder: &mut QueryBuilder,
    array: &[Value],
    depth: usize,
    path: &SpecPath,
) -> Result<NodeHandle, SpecError> {
    check_spec_arity_bounded(
        array,
        path,
        "expression 'list_comp'",
        5,
        5,
        "[list_comp, variable, list_expr, where_filter, map_expr]",
    )?;
    let var = extract_string(&array[1], &path.index(1), "a string variable")?;
    let list = value_to_node_handle(builder, &array[2], depth + 1, &path.index(2))?;
    let where_filter = if array[3].is_null() {
        None
    } else {
        Some(value_to_node_handle(
            builder,
            &array[3],
            depth + 1,
            &path.index(3),
        )?)
    };
    let map_expr = if array[4].is_null() {
        None
    } else {
        Some(value_to_node_handle(
            builder,
            &array[4],
            depth + 1,
            &path.index(4),
        )?)
    };
    Ok(builder.list_comprehension(var, list, where_filter, map_expr))
}

#[inline(never)]
fn handle_pattern_comp_tuple(
    builder: &mut QueryBuilder,
    array: &[Value],
    depth: usize,
    path: &SpecPath,
) -> Result<NodeHandle, SpecError> {
    check_spec_arity_bounded(
        array,
        path,
        "expression 'pattern_comp'",
        4,
        4,
        "[pattern_comp, path_spec, where_filter, proj_expr]",
    )?;
    let path_handle = build_path(builder, &array[1], depth + 1, &path.index(1))?;
    let where_filter = if array[2].is_null() {
        None
    } else {
        Some(value_to_node_handle(
            builder,
            &array[2],
            depth + 1,
            &path.index(2),
        )?)
    };
    let projection = value_to_node_handle(builder, &array[3], depth + 1, &path.index(3))?;
    Ok(builder.pattern_comprehension(path_handle, where_filter, projection))
}

#[inline(never)]
fn handle_exists_tuple(
    builder: &mut QueryBuilder,
    array: &[Value],
    depth: usize,
    path: &SpecPath,
) -> Result<NodeHandle, SpecError> {
    check_spec_arity_bounded(
        array,
        path,
        "expression 'exists'",
        2,
        2,
        "[exists, subquery_spec]",
    )?;
    let sub = build_subquery(builder, &array[1], depth + 1, &path.index(1))?;
    Ok(builder.exists_subquery(sub))
}

#[inline(never)]
fn handle_count_tuple(
    builder: &mut QueryBuilder,
    array: &[Value],
    depth: usize,
    path: &SpecPath,
) -> Result<NodeHandle, SpecError> {
    check_spec_arity_bounded(
        array,
        path,
        "expression 'count'",
        2,
        2,
        "[count, subquery_spec]",
    )?;
    let sub = build_subquery(builder, &array[1], depth + 1, &path.index(1))?;
    Ok(builder.count_subquery(sub))
}

#[inline(never)]
fn handle_list_literal_tuple(
    builder: &mut QueryBuilder,
    array: &[Value],
    depth: usize,
    path: &SpecPath,
) -> Result<NodeHandle, SpecError> {
    check_spec_arity_bounded(array, path, "expression 'list'", 2, 2, "[list, items]")?;
    let items_path = path.index(1);
    let items = expect_spec_array(&array[1], &items_path, "list items")?;
    let mut item_handles = Vec::with_capacity(items.len());
    for (idx, item) in items.iter().enumerate() {
        item_handles.push(value_to_node_handle(
            builder,
            item,
            depth + 1,
            &items_path.index(idx),
        )?);
    }
    Ok(builder.list_literal(item_handles))
}

/// Validates the first item of an expression array and returns its tag.
#[inline(never)]
fn expression_tag(array: &[Value], path: &SpecPath) -> Result<String, SpecError> {
    if array.is_empty() {
        return Err(make_spec_err(
            path,
            format!(
                "Empty expression array: an expression array must start with a known expression tag. {EXPRESSION_ARRAY_HINT}"
            ),
        ));
    }
    let first = &array[0];
    let Some(tag) = first.as_str() else {
        let type_name = json_type_name(first);
        let rendered = match render_json_scalar(first) {
            Some(value) => format!("got {value} ({type_name})"),
            None => format!("got {type_name}"),
        };
        return Err(make_spec_err(
            path,
            format!(
                "Invalid expression array: the first element must be a known expression tag string, {rendered}. {EXPRESSION_ARRAY_HINT}"
            ),
        ));
    };
    if tag == "agg" {
        return Err(make_spec_err(
            path,
            "Invalid expression 'agg': aggregations are only valid as projection items in 'projections' or 'with_clauses[i].projections', not in where, order_by, set, function arguments or other expression positions. Write the aggregate as [\"agg\", variable, property, func, alias] in a projection list instead.",
        ));
    }
    if !is_known_expr_tag(tag) {
        return Err(make_spec_err(
            path,
            format!(
                "Unknown expression tag '{tag}': the first element of an expression array must be a known expression tag. {EXPRESSION_ARRAY_HINT}"
            ),
        ));
    }
    Ok(tag.to_string())
}

#[inline(never)]
fn handle_bin_tuple(
    builder: &mut QueryBuilder,
    array: &[Value],
    depth: usize,
    path: &SpecPath,
) -> Result<NodeHandle, SpecError> {
    check_spec_arity_bounded(
        array,
        path,
        "expression 'bin'",
        4,
        4,
        "[bin, operator, left, right]",
    )?;
    let op_path = path.index(1);
    let op_str = extract_string(&array[1], &op_path, "a string operator")?;
    let op = parse_binary_op(&op_str).map_err(|message| make_spec_err(&op_path, message))?;
    let left = value_to_node_handle(builder, &array[2], depth + 1, &path.index(2))?;
    let right = value_to_node_handle(builder, &array[3], depth + 1, &path.index(3))?;
    Ok(builder.binary_expr(left, op, right))
}

#[inline(never)]
fn handle_unary_tuple(
    builder: &mut QueryBuilder,
    array: &[Value],
    depth: usize,
    path: &SpecPath,
) -> Result<NodeHandle, SpecError> {
    check_spec_arity_bounded(
        array,
        path,
        "expression 'unary'",
        3,
        3,
        "[unary, operator, operand]",
    )?;
    let op_path = path.index(1);
    let op_str = extract_string(&array[1], &op_path, "a string operator")?;
    let op = parse_unary_op(&op_str).map_err(|message| make_spec_err(&op_path, message))?;
    let operand = value_to_node_handle(builder, &array[2], depth + 1, &path.index(2))?;
    Ok(builder.unary_expr(op, operand))
}

#[inline(never)]
fn handle_expr_sequence(
    builder: &mut QueryBuilder,
    array: &[Value],
    depth: usize,
    path: &SpecPath,
) -> Result<NodeHandle, SpecError> {
    let tag = expression_tag(array, path)?;
    match tag.as_str() {
        "prop" => handle_prop_tuple(builder, array, path),
        "ident" => handle_ident_tuple(builder, array, path),
        "param" => handle_param_tuple(builder, array, path),
        "lit" => handle_lit_tuple(builder, array, path),
        "label" => handle_label_tuple(builder, array, path),
        "bin" => handle_bin_tuple(builder, array, depth, path),
        "unary" => handle_unary_tuple(builder, array, depth, path),
        "fn" => handle_fn_tuple(builder, array, depth, path),
        "case" => handle_case_tuple(builder, array, depth, path),
        "list_comp" => handle_list_comp_tuple(builder, array, depth, path),
        "pattern_comp" => handle_pattern_comp_tuple(builder, array, depth, path),
        "exists" => handle_exists_tuple(builder, array, depth, path),
        "count" => handle_count_tuple(builder, array, depth, path),
        "list" => handle_list_literal_tuple(builder, array, depth, path),
        "alias" => {
            check_spec_arity_bounded(
                array,
                path,
                "expression 'alias'",
                3,
                3,
                "[alias, expression, name]",
            )?;
            let _ = extract_string(&array[2], &path.index(2), "a string alias name")?;
            value_to_node_handle(builder, &array[1], depth + 1, &path.index(1))
        }
        other => Err(make_spec_err(
            path,
            format!(
                "Unknown expression tag '{other}': the first element of an expression array must be a known expression tag. {EXPRESSION_ARRAY_HINT}"
            ),
        )),
    }
}

#[inline(never)]
fn handle_dict_expr(
    builder: &mut QueryBuilder,
    dict: &Map<String, Value>,
    depth: usize,
    path: &SpecPath,
) -> Result<NodeHandle, SpecError> {
    if let Some(version) = dict.get("spec_version") {
        let version_path = path.key("spec_version");
        let version = extract_u32(version, &version_path, "an integer major version")?;
        if version != 1 {
            return Err(make_spec_err(
                &version_path,
                format!(
                    "Unsupported expression spec_version: {version}. Supported major versions: 1."
                ),
            ));
        }
    }
    if let Some(inner) = dict.get("expr") {
        reject_unknown_keys(dict, path, &["spec_version", "expr"], "expression object")?;
        return value_to_node_handle(builder, inner, depth + 1, &path.key("expr"));
    }
    Err(make_spec_err(
        path,
        "expected an object-wrapped expression with an 'expr' key; a nested query spec is only accepted by the 'exists', 'count', and 'pattern_comp' payloads",
    ))
}

#[inline(never)]
fn value_to_node_handle(
    builder: &mut QueryBuilder,
    val: &Value,
    depth: usize,
    path: &SpecPath,
) -> Result<NodeHandle, SpecError> {
    if depth > MAX_AST_DEPTH {
        return Err(make_spec_err(
            path,
            format!(
                "Maximum AST expression recursion depth exceeded ({MAX_AST_DEPTH}). Query is too deeply nested.",
            ),
        ));
    }
    if let Some(dict) = val.as_object() {
        return handle_dict_expr(builder, dict, depth, path);
    }
    // In an expression position a JSON array is always a tagged expression:
    // there is no implicit literal-list spelling. The binary and unary tags get
    // a direct path so a deep operator chain recurses through two frames per
    // level instead of three, keeping the documented depth limit inside a
    // 1 MB caller stack.
    if let Some(array) = val.as_array() {
        match array.first().and_then(Value::as_str) {
            Some("bin") => return handle_bin_tuple(builder, array, depth, path),
            Some("unary") => return handle_unary_tuple(builder, array, depth, path),
            _ => return handle_expr_sequence(builder, array, depth, path),
        }
    }
    Err(make_spec_err(
        path,
        format!(
            "expected an expression (a tagged JSON array or an object-wrapped expression), got {}; write a scalar literal as [\"lit\", value]",
            json_type_name(val)
        ),
    ))
}

fn parse_direction(dir_str: &str) -> Result<Direction, String> {
    match dir_str {
        "out" | "outgoing" | "->" => Ok(Direction::Outgoing),
        "in" | "incoming" | "<-" => Ok(Direction::Incoming),
        "undirected" | "both" | "-" | "--" => Ok(Direction::Undirected),
        other => Err(format!(
            "Invalid edge direction '{other}'. Expected 'outgoing' ('out', '->'), 'incoming' ('in', '<-'), or 'undirected' ('both', '-', '--')."
        )),
    }
}

fn build_path_steps_into_builder(
    builder: &mut QueryBuilder,
    steps: &[Value],
    depth: usize,
    path: &SpecPath,
) -> Result<(), SpecError> {
    for (idx, step) in steps.iter().enumerate() {
        let step_path = path.index(idx);
        let array = expect_spec_array(step, &step_path, "path step")?;
        let tag_path = step_path.index(0);
        let tag = extract_string(&array[0], &tag_path, "a string path step tag")?;
        match tag.as_str() {
            "node" => {
                check_spec_arity(
                    array,
                    &step_path,
                    "path step 'node'",
                    3,
                    "[node, variable, labels, properties?]",
                )?;
                reject_extra_step_items(
                    array,
                    &step_path,
                    "path step 'node'",
                    4,
                    "[node, variable, labels, properties]",
                )?;
                let var = extract_optional_string(
                    &array[1],
                    &step_path.index(1),
                    "a string or null variable",
                )?;
                let labels = extract_string_array(
                    &array[2],
                    &step_path.index(2),
                    "an array of string labels",
                )?;
                let properties = parse_step_properties(
                    builder,
                    array,
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
                    array,
                    &step_path,
                    "path step 'edge'",
                    3,
                    "[edge, direction, types, variable?, min_hops?, max_hops?, properties?]",
                )?;
                reject_extra_step_items(
                    array,
                    &step_path,
                    "path step 'edge'",
                    7,
                    "[edge, direction, types, variable, min_hops, max_hops, properties]",
                )?;
                let dir_path = step_path.index(1);
                let dir_str = extract_string(&array[1], &dir_path, "a string edge direction")?;
                let dir = parse_direction(&dir_str)
                    .map_err(|message| make_spec_err(&dir_path, message))?;
                let edge_types = extract_string_array(
                    &array[2],
                    &step_path.index(2),
                    "an array of string edge types",
                )?;
                let var = match array.get(3) {
                    Some(item) => extract_optional_string(
                        item,
                        &step_path.index(3),
                        "a string or null variable",
                    )?,
                    None => None,
                };
                let min_hops = match array.get(4) {
                    Some(item) => extract_optional_u32(
                        item,
                        &step_path.index(4),
                        "a non-negative integer or null",
                    )?,
                    None => None,
                };
                let max_hops = match array.get(5) {
                    Some(item) => extract_max_hops(item, &step_path.index(5))?,
                    None => SpecMaxHops::Null,
                };
                let properties = parse_step_properties(
                    builder,
                    array,
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
                    (Some(min), SpecMaxHops::Exact(max)) => {
                        builder.hops_opt(Some(min), Some(max));
                    }
                    (Some(min), SpecMaxHops::Unbounded) => {
                        builder.hops_opt(Some(min), None);
                    }
                    (Some(min), SpecMaxHops::Null) => {
                        builder.hops_opt(Some(min), Some(min));
                    }
                    (None, SpecMaxHops::Exact(max)) => {
                        builder.hops_opt(Some(1), Some(max));
                    }
                    (None, SpecMaxHops::Unbounded) => {
                        builder.hops_opt(Some(1), None);
                    }
                    (None, SpecMaxHops::Null) => {}
                }
            }
            other => {
                return Err(make_spec_err(
                    &tag_path,
                    format!("Unknown path step tag: '{other}'. Expected 'node' or 'edge'."),
                ));
            }
        }
    }
    Ok(())
}

fn build_order_by_entry(
    builder: &mut QueryBuilder,
    item: &Value,
    depth: usize,
    path: &SpecPath,
    field: &str,
) -> Result<(), SpecError> {
    let array = expect_spec_array(item, path, field)?;
    check_spec_arity(
        array,
        path,
        field,
        2,
        "[variable, property, direction] or [expression, direction]",
    )?;
    let first = &array[0];
    if let Some(var) = first.as_str() {
        check_spec_arity_bounded(array, path, field, 3, 3, "[variable, property, direction]")?;
        let prop = extract_string(&array[1], &path.index(1), "a string property")?;
        let asc = parse_order_asc(&array[2], &path.index(2), field)?;
        builder.order_by_property(var, prop, asc);
    } else {
        check_spec_arity_bounded(array, path, field, 2, 2, "[expression, direction]")?;
        let expr = value_to_node_handle(builder, first, depth, &path.index(0))?;
        let asc = parse_order_asc(&array[1], &path.index(1), field)?;
        builder.order_by(expr, asc);
    }
    Ok(())
}

fn build_where_predicates(
    builder: &mut QueryBuilder,
    where_item: &Value,
    depth: usize,
    path: &SpecPath,
) -> Result<(), SpecError> {
    if let Some(array) = where_item.as_array() {
        if array.is_empty() {
            return Ok(());
        }
        let first = &array[0];
        if first.as_str().is_some() {
            let handle = value_to_node_handle(builder, where_item, depth, path)?;
            builder.where_expr(handle);
            return Ok(());
        }
        if first.is_object() || first.is_array() {
            for (idx, predicate) in array.iter().enumerate() {
                let predicate_path = path.index(idx);
                let handle = value_to_node_handle(builder, predicate, depth, &predicate_path)?;
                builder.where_expr(handle);
            }
            return Ok(());
        }
        let type_name = json_type_name(first);
        let rendered = match render_json_scalar(first) {
            Some(value) => format!("first element {value} ({type_name})"),
            None => format!("first element of type {type_name}"),
        };
        return Err(make_spec_err(
            path,
            format!(
                "Invalid 'where': a 'where' array must be one expression (starting with a string tag) or an array of expressions (starting with an array or object), got {rendered}. {EXPRESSION_ARRAY_HINT}"
            ),
        ));
    }
    let handle = value_to_node_handle(builder, where_item, depth, path)?;
    builder.where_expr(handle);
    Ok(())
}

/// Variable names in scope at one point of the emitted statement.
#[derive(Clone)]
struct SpecScope {
    names: HashSet<String>,
    valid_identifiers: bool,
}

impl Default for SpecScope {
    fn default() -> Self {
        Self {
            names: HashSet::new(),
            valid_identifiers: true,
        }
    }
}

/// Mirrors the emitter's plain-identifier rule closely enough to defer invalid
/// names to the shared identifier validator.
fn is_plain_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(first) if first == '_' || first.is_alphabetic() => {}
        _ => return false,
    }
    chars.all(|ch| ch == '_' || ch.is_alphanumeric())
}

impl SpecScope {
    fn contains(&self, name: &str) -> bool {
        self.names.contains(name)
    }

    fn bind(&mut self, name: &str) {
        if name.is_empty() {
            return;
        }
        if !is_plain_identifier(name) {
            self.valid_identifiers = false;
            return;
        }
        self.names.insert(name.to_string());
    }

    fn include(&mut self, other: &SpecScope) {
        self.names.extend(other.names.iter().cloned());
    }
}

/// Where a variable reference sits in the fixed emission order.
#[derive(Clone, Copy, Debug)]
enum ScopeClause {
    Match,
    LinearLet,
    LinearFilter,
    WithProjection { index: usize },
    WithWhere { index: usize, only_projected: bool },
    WithOrderBy { index: usize, only_projected: bool },
    Mutation,
    Return,
    RootOrderBy { only_projected: bool },
}

/// Names defined by later clauses, used to explain a forward reference.
#[derive(Default)]
struct ScopeHints {
    with_names: Vec<(usize, String)>,
    let_names: HashSet<String>,
}

impl ScopeHints {
    fn with_index_of(&self, name: &str) -> Option<usize> {
        self.with_names
            .iter()
            .find(|(_, projected)| projected == name)
            .map(|(index, _)| *index)
    }
}

/// Builds the `INVALID_SPEC` error for a variable that is not in scope.
fn missing_scope_var(
    path: &SpecPath,
    name: &str,
    clause: ScopeClause,
    last_with: Option<usize>,
    hints: &ScopeHints,
) -> SpecError {
    let mut message = format!("variable '{name}' is not in scope here");
    match clause {
        ScopeClause::LinearLet | ScopeClause::LinearFilter => {
            if let Some(index) = hints.with_index_of(name) {
                message.push_str(&format!(
                    ": linear clauses are emitted before WITH clauses; move the reference into with_clauses[{index}].where or with_clauses[{index}].projections"
                ));
            } else if hints.let_names.contains(name) {
                message.push_str(": a linear 'let' binds its variable only from that entry onward");
            } else {
                message.push_str(
                    ": it is not bound by any preceding LOAD CSV, UNWIND, MATCH, linear let, or WITH projection",
                );
            }
        }
        ScopeClause::Match => {
            if hints.let_names.contains(name) {
                message.push_str(
                    ": MATCH clauses are emitted before linear clauses; bind the variable in a preceding WITH projection instead",
                );
            } else {
                message
                    .push_str(": it is not bound by any preceding LOAD CSV, UNWIND, or MATCH path");
            }
        }
        ScopeClause::WithProjection { index } => {
            message.push_str(&format!(
                ": with_clauses[{index}] projections can only read names bound by the preceding clauses; add it to an earlier WITH projection or a match path"
            ));
        }
        ScopeClause::WithWhere {
            index,
            only_projected,
        } => {
            if only_projected {
                message.push_str(&format!(
                    ": a WITH with DISTINCT or an aggregation only exposes the names it projects; add '{name}' to with_clauses[{index}].projections or reference a projected alias"
                ));
            } else {
                message.push_str(&format!(
                    ": with_clauses[{index}].where sees the names that clause projects and the names in scope before it; add '{name}' to with_clauses[{index}].projections or reference a projected alias"
                ));
            }
        }
        ScopeClause::WithOrderBy {
            index,
            only_projected,
        } => {
            if only_projected {
                message.push_str(&format!(
                    ": a WITH with DISTINCT or an aggregation only exposes the names it projects; add '{name}' to with_clauses[{index}].projections"
                ));
            } else {
                message.push_str(&format!(
                    ": with_clauses[{index}].order_by cannot see '{name}' because the WITH neither projects it nor keeps it from before the WITH; add it to with_clauses[{index}].projections"
                ));
            }
        }
        ScopeClause::Mutation => {
            if let Some(index) = last_with {
                message.push_str(&format!(
                    ": the last WITH clause (with_clauses[{index}]) does not project '{name}'; add it to that clause's projections"
                ));
            } else {
                message.push_str(": it is not bound by any preceding clause");
            }
        }
        ScopeClause::Return => {
            if let Some(index) = last_with {
                message.push_str(&format!(
                    ": the last WITH clause (with_clauses[{index}]) does not project '{name}'; add it to that clause's projections"
                ));
            } else {
                message.push_str(": it is not bound by any preceding clause");
            }
        }
        ScopeClause::RootOrderBy { only_projected } => {
            if only_projected {
                message.push_str(
                    ": a RETURN with DISTINCT or an aggregation only exposes the names it projects; reference a projected name",
                );
            } else if let Some(index) = last_with {
                message.push_str(&format!(
                    ": the last WITH clause (with_clauses[{index}]) does not project '{name}'; add it to that clause's projections"
                ));
            } else {
                message.push_str(": it is not bound by any preceding clause");
            }
        }
    }
    make_spec_err(path, message)
}

fn seq_tag(val: &Value) -> Option<(&Vec<Value>, &str)> {
    let sequence = val.as_array()?;
    let tag = sequence.first()?.as_str()?;
    Some((sequence, tag))
}

fn array_string(sequence: &[Value], index: usize) -> Option<String> {
    sequence
        .get(index)
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn array_optional_string(sequence: &[Value], index: usize) -> Option<String> {
    match sequence.get(index) {
        Some(Value::String(string)) => Some(string.clone()),
        _ => None,
    }
}

fn map_string(dict: &Map<String, Value>, key: &str) -> Option<String> {
    dict.get(key).and_then(Value::as_str).map(str::to_owned)
}

/// The names a projection introduces into the next clause.
#[derive(Default)]
struct ProjectionScope {
    projected: Vec<String>,
}

fn expression_projection_name(expression: &Value) -> Option<String> {
    let (sequence, tag) = seq_tag(expression)?;
    match tag {
        "alias" => array_string(sequence, 2),
        "ident" => array_string(sequence, 1),
        "prop" => array_string(sequence, 2),
        _ => None,
    }
}

fn projection_scope_outputs(item: &Value) -> ProjectionScope {
    let mut outputs = ProjectionScope::default();
    let Some((sequence, tag)) = seq_tag(item) else {
        return outputs;
    };
    match tag {
        "field" => match array_optional_string(sequence, 3) {
            Some(alias) => outputs.projected.push(alias),
            None => match array_string(sequence, 2) {
                Some(prop) if !prop.is_empty() => outputs.projected.push(prop),
                // `["field", var, ""]` projects the bare variable `var`.
                Some(_) => {
                    if let Some(variable) = array_string(sequence, 1) {
                        outputs.projected.push(variable);
                    }
                }
                None => {}
            },
        },
        "agg" => {
            if let Some(alias) = array_optional_string(sequence, 4) {
                outputs.projected.push(alias);
            }
        }
        "expr" => {
            if let Some(alias) = array_optional_string(sequence, 2) {
                outputs.projected.push(alias);
            }
            if let Some(expression) = sequence.get(1)
                && outputs.projected.is_empty()
                && let Some(name) = expression_projection_name(expression)
            {
                outputs.projected.push(name);
            }
        }
        _ => {}
    }
    outputs
}

/// True when a projection expression spec is exactly a bare variable.
fn expression_spec_is_bare_identifier(expression: &Value) -> bool {
    seq_tag(expression)
        .map(|(_, tag)| tag == "ident")
        .unwrap_or(false)
}

/// True when a projection item projects a bare variable rather than an expression.
fn with_projection_is_bare_variable(item: &Value) -> bool {
    let Some((sequence, tag)) = seq_tag(item) else {
        return false;
    };
    match tag {
        "expr" => sequence
            .get(1)
            .map(expression_spec_is_bare_identifier)
            .unwrap_or(false),
        // `["field", var, ""]` renders as the bare variable `var`.
        "field" => array_string(sequence, 2).is_some_and(|property| property.is_empty()),
        _ => false,
    }
}

/// True when a WITH projection carries an explicit alias or an alias wrapper.
fn with_projection_has_alias(item: &Value, path: &SpecPath) -> Result<bool, SpecError> {
    let Some((sequence, tag)) = seq_tag(item) else {
        return Ok(true);
    };
    match tag {
        "field" => Ok(array_optional_string(sequence, 3).is_some()),
        "agg" => Ok(array_optional_string(sequence, 4).is_some()),
        "expr" => {
            if array_optional_string(sequence, 2).is_some() {
                return Ok(true);
            }
            let Some(expression) = sequence.get(1) else {
                return Ok(true);
            };
            Ok(extract_alias_from_expr_spec(expression, path)?.is_some())
        }
        _ => Ok(true),
    }
}

fn render_expression_spec(expression: &Value) -> String {
    let Some((sequence, tag)) = seq_tag(expression) else {
        return "expression".to_string();
    };
    match tag {
        "ident" => array_string(sequence, 1).unwrap_or_else(|| "expression".to_string()),
        "prop" => match (array_string(sequence, 1), array_string(sequence, 2)) {
            (Some(variable), Some(property)) => format!("{variable}.{property}"),
            _ => "expression".to_string(),
        },
        "alias" => sequence
            .get(1)
            .map(render_expression_spec)
            .unwrap_or_else(|| "expression".to_string()),
        "fn" => match array_string(sequence, 1) {
            Some(name) => format!("{name}(...)"),
            None => "expression".to_string(),
        },
        "lit" => sequence
            .get(1)
            .and_then(render_json_scalar)
            .unwrap_or_else(|| "expression".to_string()),
        "label" => match array_string(sequence, 1) {
            Some(variable) => format!("{variable}:<label>"),
            None => "expression".to_string(),
        },
        _ => "expression".to_string(),
    }
}

fn render_with_projection(item: &Value) -> String {
    let Some((sequence, tag)) = seq_tag(item) else {
        return "expression".to_string();
    };
    match tag {
        "field" => match (array_string(sequence, 1), array_string(sequence, 2)) {
            (Some(variable), Some(property)) => format!("{variable}.{property}"),
            _ => "expression".to_string(),
        },
        "agg" => {
            let function = array_string(sequence, 3).unwrap_or_else(|| "agg".to_string());
            let target = match (array_string(sequence, 1), array_string(sequence, 2)) {
                (Some(variable), Some(property)) if !property.is_empty() && property != "*" => {
                    format!("{variable}.{property}")
                }
                (Some(variable), _) => variable,
                _ => "expression".to_string(),
            };
            format!("{function}({target})")
        }
        "expr" => sequence
            .get(1)
            .map(render_expression_spec)
            .unwrap_or_else(|| "expression".to_string()),
        _ => "expression".to_string(),
    }
}

/// A WITH projection that is not a bare variable must name its output column.
fn validate_with_projection_alias(item: &Value, path: &SpecPath) -> Result<(), SpecError> {
    if with_projection_is_bare_variable(item) || with_projection_has_alias(item, path)? {
        return Ok(());
    }
    let rendered = render_with_projection(item);
    Err(make_spec_err(
        path,
        format!(
            "WITH projection `{rendered}` needs an alias (for example `{rendered} AS name`); only a bare variable may be projected without one"
        ),
    ))
}

/// True when a spec object's boolean flag is present and true.
fn dict_flag_is_true(dict: &Map<String, Value>, key: &str) -> bool {
    dict.get(key).and_then(Value::as_bool).unwrap_or(false)
}

/// True when a projection list contains an aggregate projection item.
fn projections_contain_aggregation(projections: &Value) -> bool {
    let Some(items) = projections.as_array() else {
        return false;
    };
    items
        .iter()
        .any(|item| seq_tag(item).is_some_and(|(_, tag)| tag == "agg"))
}

/// Unwraps `{"expr": ...}` objects and `["alias", expression, name]` arrays so
/// two expression specs can be compared by their emitted expression.
fn unwrap_expression_spec(value: &Value) -> &Value {
    let mut current = value;
    loop {
        if let Some(dict) = current.as_object() {
            if let Some(inner) = dict.get("expr") {
                current = inner;
                continue;
            }
            break;
        }
        let Some((sequence, tag)) = seq_tag(current) else {
            break;
        };
        if tag != "alias" {
            break;
        }
        let Some(inner) = sequence.get(1) else {
            break;
        };
        current = inner;
    }
    current
}

fn expression_is_ident_named(expression: &Value, name: &str) -> bool {
    let expression = unwrap_expression_spec(expression);
    let Some((sequence, "ident")) = seq_tag(expression) else {
        return false;
    };
    array_string(sequence, 1).as_deref() == Some(name)
}

fn expression_is_property_named(expression: &Value, variable: &str, property: &str) -> bool {
    let expression = unwrap_expression_spec(expression);
    let Some((sequence, "prop")) = seq_tag(expression) else {
        return false;
    };
    array_string(sequence, 1).as_deref() == Some(variable)
        && array_string(sequence, 2).as_deref() == Some(property)
}

fn aggregate_target_matches(
    variable: &str,
    property: &str,
    argument: &Value,
    depth: usize,
) -> bool {
    if depth > MAX_AST_DEPTH {
        return false;
    }
    if property == "*" || property.is_empty() {
        expression_is_ident_named(argument, variable)
    } else {
        expression_is_property_named(argument, variable, property)
    }
}

/// Structural equality for expression specs, ignoring alias wrappers and
/// comparing function names without regard to case.
fn expression_specs_equivalent(left: &Value, right: &Value, depth: usize) -> bool {
    if depth > MAX_AST_DEPTH {
        return false;
    }
    let left = unwrap_expression_spec(left);
    let right = unwrap_expression_spec(right);
    if left == right {
        return true;
    }
    let (Some((left_sequence, left_tag)), Some((right_sequence, right_tag))) =
        (seq_tag(left), seq_tag(right))
    else {
        return false;
    };
    if left_tag != right_tag {
        return false;
    }
    match left_tag {
        "bin" => {
            array_string(left_sequence, 1) == array_string(right_sequence, 1)
                && [2, 3].into_iter().all(|index| {
                    match (left_sequence.get(index), right_sequence.get(index)) {
                        (Some(left_child), Some(right_child)) => {
                            expression_specs_equivalent(left_child, right_child, depth + 1)
                        }
                        _ => false,
                    }
                })
        }
        "unary" => {
            array_string(left_sequence, 1) == array_string(right_sequence, 1)
                && left_sequence.get(2).zip(right_sequence.get(2)).is_some_and(
                    |(left_operand, right_operand)| {
                        expression_specs_equivalent(left_operand, right_operand, depth + 1)
                    },
                )
        }
        "fn" => {
            let (Some(left_name), Some(right_name)) = (
                array_string(left_sequence, 1),
                array_string(right_sequence, 1),
            ) else {
                return false;
            };
            if !left_name.eq_ignore_ascii_case(&right_name) {
                return false;
            }
            let (Some(left_arguments), Some(right_arguments)) = (
                left_sequence.get(2).and_then(Value::as_array),
                right_sequence.get(2).and_then(Value::as_array),
            ) else {
                return false;
            };
            left_arguments.len() == right_arguments.len()
                && left_arguments.iter().zip(right_arguments.iter()).all(
                    |(left_argument, right_argument)| {
                        expression_specs_equivalent(left_argument, right_argument, depth + 1)
                    },
                )
        }
        "list" => {
            let (Some(left_items), Some(right_items)) = (
                left_sequence.get(1).and_then(Value::as_array),
                right_sequence.get(1).and_then(Value::as_array),
            ) else {
                return false;
            };
            left_items.len() == right_items.len()
                && left_items
                    .iter()
                    .zip(right_items.iter())
                    .all(|(left_item, right_item)| {
                        expression_specs_equivalent(left_item, right_item, depth + 1)
                    })
        }
        "label" => {
            left_sequence.get(1) == right_sequence.get(1)
                && left_sequence.get(2) == right_sequence.get(2)
        }
        _ => false,
    }
}

/// True when an ORDER BY/WHERE expression repeats a projection of a `DISTINCT`
/// or aggregating clause, either exactly or as a sub-expression.
fn projection_matches_expression(expression: &Value, item: &Value, depth: usize) -> bool {
    if depth > MAX_AST_DEPTH {
        return false;
    }
    let expression = unwrap_expression_spec(expression);
    let Some((sequence, tag)) = seq_tag(item) else {
        return false;
    };
    match tag {
        "field" => {
            let (Some(variable), Some(property)) =
                (array_string(sequence, 1), array_string(sequence, 2))
            else {
                return false;
            };
            if property.is_empty() {
                expression_is_ident_named(expression, &variable)
            } else {
                expression_is_property_named(expression, &variable, &property)
            }
        }
        "agg" => {
            let (Some(variable), Some(property), Some(function)) = (
                array_string(sequence, 1),
                array_string(sequence, 2),
                array_string(sequence, 3),
            ) else {
                return false;
            };
            // `COUNT(DISTINCT x)` has no function-call expression spelling, so
            // an ORDER BY cannot repeat it as an expression.
            if function == "count_distinct" {
                return false;
            }
            let Some((expression_sequence, "fn")) = seq_tag(expression) else {
                return false;
            };
            let Some(name) = array_string(expression_sequence, 1) else {
                return false;
            };
            if !name.eq_ignore_ascii_case(&function) {
                return false;
            }
            let Some(arguments) = expression_sequence.get(2).and_then(Value::as_array) else {
                return false;
            };
            arguments.len() == 1
                && aggregate_target_matches(&variable, &property, &arguments[0], depth + 1)
        }
        "expr" => sequence
            .get(1)
            .is_some_and(|inner| expression_specs_equivalent(expression, inner, depth + 1)),
        _ => false,
    }
}

/// True when an expression spec can be evaluated from the projected columns
/// alone. Mirrors `scope::ScopeValidator::expression_is_derivable`.
fn expression_is_derivable(
    expression: &Value,
    projections: &[Value],
    scope: &SpecScope,
    depth: usize,
) -> bool {
    if depth > MAX_AST_DEPTH {
        return true;
    }
    let expression = unwrap_expression_spec(expression);
    if projections
        .iter()
        .any(|item| projection_matches_expression(expression, item, depth))
    {
        return true;
    }
    let Some((sequence, tag)) = seq_tag(expression) else {
        return false;
    };
    match tag {
        "ident" => array_string(sequence, 1).is_some_and(|name| scope.contains(&name)),
        "prop" => array_string(sequence, 1).is_some_and(|variable| scope.contains(&variable)),
        "label" => array_string(sequence, 1).is_some_and(|variable| scope.contains(&variable)),
        "param" | "lit" => true,
        "bin" => sequence
            .get(2)
            .zip(sequence.get(3))
            .is_some_and(|(left, right)| {
                expression_is_derivable(left, projections, scope, depth + 1)
                    && expression_is_derivable(right, projections, scope, depth + 1)
            }),
        "unary" => sequence
            .get(2)
            .is_some_and(|operand| expression_is_derivable(operand, projections, scope, depth + 1)),
        "fn" => sequence
            .get(2)
            .and_then(Value::as_array)
            .is_none_or(|arguments| {
                arguments.iter().all(|argument| {
                    expression_is_derivable(argument, projections, scope, depth + 1)
                })
            }),
        "list" => sequence
            .get(1)
            .and_then(Value::as_array)
            .is_none_or(|items| {
                items
                    .iter()
                    .all(|item| expression_is_derivable(item, projections, scope, depth + 1))
            }),
        "case" => {
            let operand_ok = sequence.get(1).is_none_or(|operand| {
                operand.is_null() || expression_is_derivable(operand, projections, scope, depth + 1)
            });
            let branches_ok = sequence
                .get(2)
                .and_then(Value::as_array)
                .is_none_or(|branches| {
                    branches.iter().all(|branch| {
                        branch.as_array().is_none_or(|pair| {
                            pair.iter().all(|child| {
                                expression_is_derivable(child, projections, scope, depth + 1)
                            })
                        })
                    })
                });
            let else_ok = sequence.get(3).is_none_or(|else_branch| {
                else_branch.is_null()
                    || expression_is_derivable(else_branch, projections, scope, depth + 1)
            });
            operand_ok && branches_ok && else_ok
        }
        _ => false,
    }
}

/// True when an ORDER BY entry is derivable from the projected columns.
fn order_entry_is_derivable(
    entry: &Value,
    projections: &[Value],
    scope: &SpecScope,
    depth: usize,
) -> bool {
    let Some(sequence) = entry.as_array() else {
        return false;
    };
    let Some(first) = sequence.first() else {
        return false;
    };
    let Some(variable) = first.as_str() else {
        return expression_is_derivable(first, projections, scope, depth);
    };
    if scope.contains(variable) {
        return true;
    }
    let Some(property) = array_string(sequence, 1) else {
        return false;
    };
    projections.iter().any(|item| {
        let Some((item_sequence, tag)) = seq_tag(item) else {
            return false;
        };
        match tag {
            "field" => {
                array_string(item_sequence, 1).as_deref() == Some(variable)
                    && array_string(item_sequence, 2).as_deref() == Some(property.as_str())
            }
            "expr" => item_sequence
                .get(1)
                .is_some_and(|inner| expression_is_property_named(inner, variable, &property)),
            _ => false,
        }
    })
}

#[inline(never)]
fn collect_scope_hints(spec: &Map<String, Value>) -> ScopeHints {
    let mut hints = ScopeHints::default();
    if let Some(with_item) = spec.get("with_clauses")
        && let Some(with_sequence) = with_item.as_array()
    {
        for (index, item) in with_sequence.iter().enumerate() {
            let Some(with_dict) = item.as_object() else {
                continue;
            };
            let Some(projections) = with_dict.get("projections") else {
                continue;
            };
            let Some(projection_sequence) = projections.as_array() else {
                continue;
            };
            for projection in projection_sequence {
                for name in projection_scope_outputs(projection).projected {
                    hints.with_names.push((index, name));
                }
            }
        }
    }
    if let Some(linear_item) = spec.get("linear_clauses")
        && let Some(linear_sequence) = linear_item.as_array()
    {
        for item in linear_sequence {
            let Some((sequence, tag)) = seq_tag(item) else {
                continue;
            };
            if tag == "let"
                && let Some(name) = array_string(sequence, 1)
            {
                hints.let_names.insert(name);
            }
        }
    }
    hints
}

fn ensure_in_scope(
    name: &str,
    path: &SpecPath,
    scope: &SpecScope,
    clause: ScopeClause,
    last_with: Option<usize>,
    hints: &ScopeHints,
) -> Result<(), SpecError> {
    if name.is_empty()
        || !scope.valid_identifiers
        || !is_plain_identifier(name)
        || scope.contains(name)
    {
        return Ok(());
    }
    Err(missing_scope_var(path, name, clause, last_with, hints))
}

/// A scope gate for a WITH or RETURN that exposes only its projected names.
#[derive(Clone, Copy)]
struct ScopeGate<'a> {
    last_with: Option<usize>,
    projections: Option<&'a [Value]>,
}

#[inline(never)]
fn validate_where_scope(
    where_item: &Value,
    path: &SpecPath,
    scope: &SpecScope,
    clause: ScopeClause,
    hints: &ScopeHints,
    gate: ScopeGate<'_>,
    depth: usize,
) -> Result<(), SpecError> {
    let covered = |predicate: &Value| {
        gate.projections.is_some_and(|projections| {
            expression_is_derivable(predicate, projections, scope, depth)
        })
    };
    if let Some(sequence) = where_item.as_array() {
        if sequence.is_empty() {
            return Ok(());
        }
        let first = &sequence[0];
        if first.as_str().is_some() {
            if covered(where_item) {
                return Ok(());
            }
            return validate_expr_scope(
                where_item,
                path,
                scope,
                clause,
                gate.last_with,
                hints,
                depth,
            );
        }
        if first.is_object() || first.is_array() {
            for (index, predicate) in sequence.iter().enumerate() {
                if covered(predicate) {
                    continue;
                }
                validate_expr_scope(
                    predicate,
                    &path.index(index),
                    scope,
                    clause,
                    gate.last_with,
                    hints,
                    depth,
                )?;
            }
        }
        return Ok(());
    }
    if covered(where_item) {
        return Ok(());
    }
    validate_expr_scope(
        where_item,
        path,
        scope,
        clause,
        gate.last_with,
        hints,
        depth,
    )
}

#[inline(never)]
fn validate_order_by_scope(
    entry: &Value,
    path: &SpecPath,
    scope: &SpecScope,
    clause: ScopeClause,
    hints: &ScopeHints,
    gate: ScopeGate<'_>,
    depth: usize,
) -> Result<(), SpecError> {
    let Some(sequence) = entry.as_array() else {
        return Ok(());
    };
    let Some(first) = sequence.first() else {
        return Ok(());
    };
    if gate
        .projections
        .is_some_and(|projections| order_entry_is_derivable(entry, projections, scope, depth))
    {
        return Ok(());
    }
    if let Some(variable) = first.as_str() {
        return ensure_in_scope(
            variable,
            &path.index(0),
            scope,
            clause,
            gate.last_with,
            hints,
        );
    }
    validate_expr_scope(
        first,
        &path.index(0),
        scope,
        clause,
        gate.last_with,
        hints,
        depth,
    )
}

/// Validates one projection item and returns the names it introduces.
#[inline(never)]
fn validate_projection_item_scope(
    item: &Value,
    path: &SpecPath,
    scope: &SpecScope,
    clause: ScopeClause,
    last_with: Option<usize>,
    hints: &ScopeHints,
    depth: usize,
) -> Result<ProjectionScope, SpecError> {
    let outputs = projection_scope_outputs(item);
    let Some((sequence, tag)) = seq_tag(item) else {
        return Ok(outputs);
    };
    match tag {
        "field" | "agg" => {
            if let Some(variable) = array_string(sequence, 1) {
                ensure_in_scope(&variable, &path.index(1), scope, clause, last_with, hints)?;
            }
        }
        "expr" => {
            if let Some(expression) = sequence.get(1) {
                validate_expr_scope(
                    expression,
                    &path.index(1),
                    scope,
                    clause,
                    last_with,
                    hints,
                    depth,
                )?;
            }
        }
        _ => {}
    }
    Ok(outputs)
}

#[inline(never)]
fn validate_path_steps_scope(
    steps: &[Value],
    path: &SpecPath,
    scope: &SpecScope,
    clause: ScopeClause,
    last_with: Option<usize>,
    hints: &ScopeHints,
    depth: usize,
) -> Result<Vec<String>, SpecError> {
    let mut bound = Vec::new();
    for step in steps {
        let Some((sequence, tag)) = seq_tag(step) else {
            continue;
        };
        let variable = match tag {
            "node" => array_optional_string(sequence, 1),
            "edge" => array_optional_string(sequence, 3),
            _ => None,
        };
        if let Some(variable) = variable {
            bound.push(variable);
        }
    }

    let mut local = scope.clone();
    for name in &bound {
        local.bind(name);
    }

    for (index, step) in steps.iter().enumerate() {
        let step_path = path.index(index);
        let Some((sequence, tag)) = seq_tag(step) else {
            continue;
        };
        let properties_index = match tag {
            "node" => 3,
            "edge" => 6,
            _ => continue,
        };
        let Some(properties) = sequence.get(properties_index) else {
            continue;
        };
        let Some(properties) = properties.as_object() else {
            continue;
        };
        let properties_path = step_path.index(properties_index);
        for (key, value) in properties {
            validate_expr_scope(
                value,
                &properties_path.key(key),
                &local,
                clause,
                last_with,
                hints,
                depth,
            )?;
        }
    }
    Ok(bound)
}

#[inline(never)]
fn validate_subquery_scope(
    spec: &Map<String, Value>,
    path: &SpecPath,
    parent_scope: &SpecScope,
    depth: usize,
) -> Result<(), SpecError> {
    let mut child = parent_scope.clone();
    validate_spec_scope_inner(spec, path, &mut child, depth + 1)
}

#[inline(never)]
fn validate_expr_scope(
    val: &Value,
    path: &SpecPath,
    scope: &SpecScope,
    clause: ScopeClause,
    last_with: Option<usize>,
    hints: &ScopeHints,
    depth: usize,
) -> Result<(), SpecError> {
    if depth > MAX_AST_DEPTH {
        return Ok(());
    }
    if let Some(dict) = val.as_object() {
        if let Some(inner) = dict.get("expr") {
            return validate_expr_scope(
                inner,
                &path.key("expr"),
                scope,
                clause,
                last_with,
                hints,
                depth + 1,
            );
        }
        return Ok(());
    }
    let Some((sequence, tag)) = seq_tag(val) else {
        return Ok(());
    };
    match tag {
        "ident" | "prop" => {
            if let Some(name) = array_string(sequence, 1) {
                ensure_in_scope(&name, &path.index(1), scope, clause, last_with, hints)?;
            }
            Ok(())
        }
        "label" => {
            if let Some(name) = array_string(sequence, 1) {
                ensure_in_scope(&name, &path.index(1), scope, clause, last_with, hints)?;
            }
            Ok(())
        }
        "param" | "lit" => Ok(()),
        "bin" => validate_expr_scope_bin(sequence, path, scope, clause, last_with, hints, depth),
        "unary" => {
            validate_expr_scope_unary(sequence, path, scope, clause, last_with, hints, depth)
        }
        "fn" => validate_expr_scope_fn(sequence, path, scope, clause, last_with, hints, depth),
        "case" => validate_expr_scope_case(sequence, path, scope, clause, last_with, hints, depth),
        "list" => validate_expr_scope_list(sequence, path, scope, clause, last_with, hints, depth),
        "list_comp" => {
            validate_expr_scope_list_comp(sequence, path, scope, clause, last_with, hints, depth)
        }
        "pattern_comp" => {
            validate_expr_scope_pattern_comp(sequence, path, scope, clause, last_with, hints, depth)
        }
        "exists" | "count" => {
            validate_expr_scope_subquery(sequence, path, scope, clause, last_with, hints, depth)
        }
        "alias" => {
            validate_expr_scope_alias(sequence, path, scope, clause, last_with, hints, depth)
        }
        _ => Ok(()),
    }
}

#[inline(never)]
fn validate_expr_scope_bin(
    sequence: &[Value],
    path: &SpecPath,
    scope: &SpecScope,
    clause: ScopeClause,
    last_with: Option<usize>,
    hints: &ScopeHints,
    depth: usize,
) -> Result<(), SpecError> {
    for index in [2, 3] {
        if let Some(child) = sequence.get(index) {
            validate_expr_scope(
                child,
                &path.index(index),
                scope,
                clause,
                last_with,
                hints,
                depth + 1,
            )?;
        }
    }
    Ok(())
}

#[inline(never)]
fn validate_expr_scope_unary(
    sequence: &[Value],
    path: &SpecPath,
    scope: &SpecScope,
    clause: ScopeClause,
    last_with: Option<usize>,
    hints: &ScopeHints,
    depth: usize,
) -> Result<(), SpecError> {
    if let Some(operand) = sequence.get(2) {
        validate_expr_scope(
            operand,
            &path.index(2),
            scope,
            clause,
            last_with,
            hints,
            depth + 1,
        )?;
    }
    Ok(())
}

#[inline(never)]
fn validate_expr_scope_fn(
    sequence: &[Value],
    path: &SpecPath,
    scope: &SpecScope,
    clause: ScopeClause,
    last_with: Option<usize>,
    hints: &ScopeHints,
    depth: usize,
) -> Result<(), SpecError> {
    if let Some(arguments) = sequence.get(2)
        && let Some(arguments) = arguments.as_array()
    {
        let arguments_path = path.index(2);
        for (index, argument) in arguments.iter().enumerate() {
            validate_expr_scope(
                argument,
                &arguments_path.index(index),
                scope,
                clause,
                last_with,
                hints,
                depth + 1,
            )?;
        }
    }
    Ok(())
}

#[inline(never)]
fn validate_expr_scope_case(
    sequence: &[Value],
    path: &SpecPath,
    scope: &SpecScope,
    clause: ScopeClause,
    last_with: Option<usize>,
    hints: &ScopeHints,
    depth: usize,
) -> Result<(), SpecError> {
    for index in [1, 3] {
        if let Some(child) = sequence.get(index)
            && !child.is_null()
        {
            validate_expr_scope(
                child,
                &path.index(index),
                scope,
                clause,
                last_with,
                hints,
                depth + 1,
            )?;
        }
    }
    if let Some(branches) = sequence.get(2)
        && let Some(branches) = branches.as_array()
    {
        let branches_path = path.index(2);
        for (index, branch) in branches.iter().enumerate() {
            let branch_path = branches_path.index(index);
            let Some(branch) = branch.as_array() else {
                continue;
            };
            for position in [0, 1] {
                if let Some(child) = branch.get(position) {
                    validate_expr_scope(
                        child,
                        &branch_path.index(position),
                        scope,
                        clause,
                        last_with,
                        hints,
                        depth + 1,
                    )?;
                }
            }
        }
    }
    Ok(())
}

#[inline(never)]
fn validate_expr_scope_list(
    sequence: &[Value],
    path: &SpecPath,
    scope: &SpecScope,
    clause: ScopeClause,
    last_with: Option<usize>,
    hints: &ScopeHints,
    depth: usize,
) -> Result<(), SpecError> {
    if let Some(items) = sequence.get(1)
        && let Some(items) = items.as_array()
    {
        let items_path = path.index(1);
        for (index, item) in items.iter().enumerate() {
            validate_expr_scope(
                item,
                &items_path.index(index),
                scope,
                clause,
                last_with,
                hints,
                depth + 1,
            )?;
        }
    }
    Ok(())
}

#[inline(never)]
fn validate_expr_scope_list_comp(
    sequence: &[Value],
    path: &SpecPath,
    scope: &SpecScope,
    clause: ScopeClause,
    last_with: Option<usize>,
    hints: &ScopeHints,
    depth: usize,
) -> Result<(), SpecError> {
    if let Some(list_expression) = sequence.get(2) {
        validate_expr_scope(
            list_expression,
            &path.index(2),
            scope,
            clause,
            last_with,
            hints,
            depth + 1,
        )?;
    }
    let mut local = scope.clone();
    if let Some(variable) = array_string(sequence, 1) {
        local.bind(&variable);
    }
    for index in [3, 4] {
        if let Some(child) = sequence.get(index)
            && !child.is_null()
        {
            validate_expr_scope(
                child,
                &path.index(index),
                &local,
                clause,
                last_with,
                hints,
                depth + 1,
            )?;
        }
    }
    Ok(())
}

#[inline(never)]
fn validate_expr_scope_pattern_comp(
    sequence: &[Value],
    path: &SpecPath,
    scope: &SpecScope,
    clause: ScopeClause,
    last_with: Option<usize>,
    hints: &ScopeHints,
    depth: usize,
) -> Result<(), SpecError> {
    let mut local = scope.clone();
    if let Some(path_steps) = sequence.get(1)
        && let Some(path_steps) = path_steps.as_array()
    {
        let bound = validate_path_steps_scope(
            path_steps,
            &path.index(1),
            scope,
            clause,
            last_with,
            hints,
            depth + 1,
        )?;
        for name in bound {
            local.bind(&name);
        }
    }
    for index in [2, 3] {
        if let Some(child) = sequence.get(index)
            && !child.is_null()
        {
            validate_expr_scope(
                child,
                &path.index(index),
                &local,
                clause,
                last_with,
                hints,
                depth + 1,
            )?;
        }
    }
    Ok(())
}

#[inline(never)]
fn validate_expr_scope_subquery(
    sequence: &[Value],
    path: &SpecPath,
    scope: &SpecScope,
    clause: ScopeClause,
    last_with: Option<usize>,
    hints: &ScopeHints,
    depth: usize,
) -> Result<(), SpecError> {
    if let Some(payload) = sequence.get(1) {
        if let Some(dict) = payload.as_object() {
            validate_subquery_scope(dict, &path.index(1), scope, depth)?;
        } else if let Some(path_steps) = payload.as_array() {
            validate_path_steps_scope(
                path_steps,
                &path.index(1),
                scope,
                clause,
                last_with,
                hints,
                depth + 1,
            )?;
        }
    }
    Ok(())
}

#[inline(never)]
fn validate_expr_scope_alias(
    sequence: &[Value],
    path: &SpecPath,
    scope: &SpecScope,
    clause: ScopeClause,
    last_with: Option<usize>,
    hints: &ScopeHints,
    depth: usize,
) -> Result<(), SpecError> {
    if let Some(inner) = sequence.get(1) {
        validate_expr_scope(
            inner,
            &path.index(1),
            scope,
            clause,
            last_with,
            hints,
            depth + 1,
        )?;
    }
    Ok(())
}

/// Checks that every variable reference resolves at the point the consumer
/// emits it.
#[inline(never)]
fn validate_spec_scope_inner(
    spec: &Map<String, Value>,
    path: &SpecPath,
    scope: &mut SpecScope,
    depth: usize,
) -> Result<(), SpecError> {
    if depth > MAX_AST_DEPTH {
        return Ok(());
    }
    let hints = collect_scope_hints(spec);
    let mut last_with: Option<usize> = None;

    if let Some(load_item) = spec.get("load_csv")
        && !load_item.is_null()
        && let Some(sequence) = load_item.as_array()
        && let Some(alias) = array_string(sequence, 2)
    {
        scope.bind(&alias);
    }

    if let Some(unwinds_item) = spec.get("unwinds")
        && let Some(unwinds) = unwinds_item.as_array()
    {
        for entry in unwinds {
            if let Some((sequence, _)) = seq_tag(entry)
                && let Some(alias) = array_string(sequence, 1)
            {
                scope.bind(&alias);
            }
        }
    }

    if let Some(matches_item) = spec.get("matches")
        && let Some(matches) = matches_item.as_array()
    {
        let matches_path = path.key("matches");
        for (index, entry) in matches.iter().enumerate() {
            let Some(match_dict) = entry.as_object() else {
                continue;
            };
            let match_path = matches_path.index(index);
            if let Some(paths_item) = match_dict.get("paths")
                && let Some(paths) = paths_item.as_array()
            {
                let paths_path = match_path.key("paths");
                for (path_index, path_item) in paths.iter().enumerate() {
                    let Some(steps) = path_item.as_array() else {
                        continue;
                    };
                    let bound = validate_path_steps_scope(
                        steps,
                        &paths_path.index(path_index),
                        scope,
                        ScopeClause::Match,
                        last_with,
                        &hints,
                        depth,
                    )?;
                    for name in bound {
                        scope.bind(&name);
                    }
                }
            }
            if let Some(path_variable) = map_string(match_dict, "path_variable") {
                scope.bind(&path_variable);
            }
            if let Some(where_item) = match_dict.get("where")
                && !where_item.is_null()
            {
                validate_where_scope(
                    where_item,
                    &match_path.key("where"),
                    scope,
                    ScopeClause::Match,
                    &hints,
                    ScopeGate {
                        last_with,
                        projections: None,
                    },
                    depth,
                )?;
            }
        }
    }

    if let Some(linear_item) = spec.get("linear_clauses")
        && let Some(linear) = linear_item.as_array()
    {
        let linear_path = path.key("linear_clauses");
        for (index, entry) in linear.iter().enumerate() {
            let Some((sequence, tag)) = seq_tag(entry) else {
                continue;
            };
            let entry_path = linear_path.index(index);
            match tag {
                "let" => {
                    if let Some(expression) = sequence.get(2) {
                        validate_expr_scope(
                            expression,
                            &entry_path.index(2),
                            scope,
                            ScopeClause::LinearLet,
                            last_with,
                            &hints,
                            depth,
                        )?;
                    }
                    if let Some(variable) = array_string(sequence, 1) {
                        scope.bind(&variable);
                    }
                }
                "filter" => {
                    if let Some(predicate) = sequence.get(1) {
                        validate_expr_scope(
                            predicate,
                            &entry_path.index(1),
                            scope,
                            ScopeClause::LinearFilter,
                            last_with,
                            &hints,
                            depth,
                        )?;
                    }
                }
                _ => {}
            }
        }
    }

    if let Some(with_item) = spec.get("with_clauses")
        && let Some(with_clauses) = with_item.as_array()
    {
        let with_path = path.key("with_clauses");
        for (index, entry) in with_clauses.iter().enumerate() {
            let Some(with_dict) = entry.as_object() else {
                continue;
            };
            let entry_path = with_path.index(index);
            let before = scope.clone();
            let projections_item = with_dict.get("projections");
            let only_projected = dict_flag_is_true(with_dict, "distinct")
                || projections_item.is_some_and(projections_contain_aggregation);
            let mut projected = SpecScope {
                names: HashSet::new(),
                valid_identifiers: before.valid_identifiers,
            };
            if let Some(projections) = projections_item
                && let Some(projections) = projections.as_array()
            {
                let projections_path = entry_path.key("projections");
                for (projection_index, projection) in projections.iter().enumerate() {
                    let projection_path = projections_path.index(projection_index);
                    validate_with_projection_alias(projection, &projection_path)?;
                    let outputs = validate_projection_item_scope(
                        projection,
                        &projection_path,
                        &before,
                        ScopeClause::WithProjection { index },
                        last_with,
                        &hints,
                        depth,
                    )?;
                    for name in outputs.projected {
                        projected.bind(&name);
                    }
                }
            }
            if let Some(where_item) = with_dict.get("where")
                && !where_item.is_null()
            {
                let mut where_scope = projected.clone();
                if !only_projected {
                    where_scope.include(&before);
                }
                let projections_slice = projections_item.and_then(Value::as_array);
                validate_where_scope(
                    where_item,
                    &entry_path.key("where"),
                    &where_scope,
                    ScopeClause::WithWhere {
                        index,
                        only_projected,
                    },
                    &hints,
                    ScopeGate {
                        last_with,
                        projections: if only_projected {
                            projections_slice.map(Vec::as_slice)
                        } else {
                            None
                        },
                    },
                    depth,
                )?;
            }
            if let Some(order_item) = with_dict.get("order_by")
                && !order_item.is_null()
                && let Some(order_entries) = order_item.as_array()
            {
                let order_path = entry_path.key("order_by");
                let mut order_scope = projected.clone();
                if !only_projected {
                    order_scope.include(&before);
                }
                for (order_index, order_entry) in order_entries.iter().enumerate() {
                    validate_order_by_scope(
                        order_entry,
                        &order_path.index(order_index),
                        &order_scope,
                        ScopeClause::WithOrderBy {
                            index,
                            only_projected,
                        },
                        &hints,
                        ScopeGate {
                            last_with,
                            projections: if only_projected {
                                projections_item
                                    .and_then(Value::as_array)
                                    .map(Vec::as_slice)
                            } else {
                                None
                            },
                        },
                        depth,
                    )?;
                }
            }
            *scope = projected;
            last_with = Some(index);
        }
    }

    if let Some(mutations_item) = spec.get("mutations")
        && let Some(mutations) = mutations_item.as_array()
    {
        let mutations_path = path.key("mutations");
        for (index, entry) in mutations.iter().enumerate() {
            let Some((sequence, tag)) = seq_tag(entry) else {
                continue;
            };
            let entry_path = mutations_path.index(index);
            let clause = ScopeClause::Mutation;
            match tag {
                "create" => {
                    if let Some(paths) = sequence.get(1)
                        && let Some(paths) = paths.as_array()
                    {
                        let paths_path = entry_path.index(1);
                        for (path_index, path_item) in paths.iter().enumerate() {
                            let Some(steps) = path_item.as_array() else {
                                continue;
                            };
                            let bound = validate_path_steps_scope(
                                steps,
                                &paths_path.index(path_index),
                                scope,
                                clause,
                                last_with,
                                &hints,
                                depth,
                            )?;
                            for name in bound {
                                scope.bind(&name);
                            }
                        }
                    }
                }
                "merge" => {
                    if let Some(path_item) = sequence.get(1)
                        && let Some(steps) = path_item.as_array()
                    {
                        let bound = validate_path_steps_scope(
                            steps,
                            &entry_path.index(1),
                            scope,
                            clause,
                            last_with,
                            &hints,
                            depth,
                        )?;
                        for name in bound {
                            scope.bind(&name);
                        }
                    }
                    for list_index in [2, 3] {
                        if let Some(sets) = sequence.get(list_index)
                            && let Some(sets) = sets.as_array()
                        {
                            let sets_path = entry_path.index(list_index);
                            for (set_index, set_item) in sets.iter().enumerate() {
                                let Some((set, _)) = seq_tag(set_item) else {
                                    continue;
                                };
                                let set_path = sets_path.index(set_index);
                                if let Some(variable) = array_string(set, 0) {
                                    ensure_in_scope(
                                        &variable,
                                        &set_path.index(0),
                                        scope,
                                        clause,
                                        last_with,
                                        &hints,
                                    )?;
                                }
                                if let Some(value) = set.get(2) {
                                    validate_expr_scope(
                                        value,
                                        &set_path.index(2),
                                        scope,
                                        clause,
                                        last_with,
                                        &hints,
                                        depth,
                                    )?;
                                }
                            }
                        }
                    }
                }
                "set" => {
                    check_spec_arity_bounded(
                        sequence,
                        &entry_path,
                        "mutation 'set'",
                        3,
                        4,
                        "[set, variable, map] or [set, variable, property, value]",
                    )?;
                    if let Some(variable) = array_string(sequence, 1) {
                        ensure_in_scope(
                            &variable,
                            &entry_path.index(1),
                            scope,
                            clause,
                            last_with,
                            &hints,
                        )?;
                    }
                    let expr_idx = if sequence.len() == 3 { 2 } else { 3 };
                    if let Some(value) = sequence.get(expr_idx) {
                        validate_expr_scope(
                            value,
                            &entry_path.index(expr_idx),
                            scope,
                            clause,
                            last_with,
                            &hints,
                            depth,
                        )?;
                    }
                }
                "delete" => {
                    if let Some(targets) = sequence.get(2)
                        && let Some(targets) = targets.as_array()
                    {
                        let targets_path = entry_path.index(2);
                        for (target_index, target) in targets.iter().enumerate() {
                            if let Some(variable) = target.as_str() {
                                ensure_in_scope(
                                    variable,
                                    &targets_path.index(target_index),
                                    scope,
                                    clause,
                                    last_with,
                                    &hints,
                                )?;
                            }
                        }
                    }
                }
                "remove" => {
                    if let Some(variable) = array_string(sequence, 1) {
                        ensure_in_scope(
                            &variable,
                            &entry_path.index(1),
                            scope,
                            clause,
                            last_with,
                            &hints,
                        )?;
                    }
                }
                _ => {}
            }
        }
    }

    let mut return_names = Vec::new();
    if let Some(projections) = spec.get("projections")
        && let Some(projections) = projections.as_array()
    {
        let projections_path = path.key("projections");
        for (index, projection) in projections.iter().enumerate() {
            let outputs = validate_projection_item_scope(
                projection,
                &projections_path.index(index),
                scope,
                ScopeClause::Return,
                last_with,
                &hints,
                depth,
            )?;
            return_names.extend(outputs.projected);
        }
    }
    if let Some(order_item) = spec.get("order_by")
        && !order_item.is_null()
        && let Some(order_entries) = order_item.as_array()
    {
        let order_path = path.key("order_by");
        let only_projected = dict_flag_is_true(spec, "distinct")
            || spec
                .get("projections")
                .is_some_and(projections_contain_aggregation);
        let mut order_scope = if only_projected {
            SpecScope {
                names: HashSet::new(),
                valid_identifiers: scope.valid_identifiers,
            }
        } else {
            scope.clone()
        };
        for name in return_names {
            order_scope.bind(&name);
        }
        let root_projections = spec.get("projections").and_then(Value::as_array);
        for (index, order_entry) in order_entries.iter().enumerate() {
            validate_order_by_scope(
                order_entry,
                &order_path.index(index),
                &order_scope,
                ScopeClause::RootOrderBy { only_projected },
                &hints,
                ScopeGate {
                    last_with,
                    projections: if only_projected {
                        root_projections.map(Vec::as_slice)
                    } else {
                        None
                    },
                },
                depth,
            )?;
        }
    }

    Ok(())
}

fn validate_spec_scope(
    spec: &Map<String, Value>,
    path: &SpecPath,
    depth: usize,
) -> Result<(), SpecError> {
    let mut scope = SpecScope::default();
    validate_spec_scope_inner(spec, path, &mut scope, depth)
}

fn build_path(
    builder: &mut QueryBuilder,
    spec: &Value,
    depth: usize,
    path: &SpecPath,
) -> Result<NodeHandle, SpecError> {
    if let Some(steps) = spec.as_array() {
        let mut nested = QueryBuilder::new();
        build_path_steps_into_builder(&mut nested, steps, depth, path)?;
        let (nested_arena, nested_handle) = nested.build();
        Ok(builder.import_subarena(nested_arena, nested_handle))
    } else if let Some(dict) = spec.as_object()
        && !dict.contains_key("expr")
    {
        // A query object in a pattern-comprehension path position (the Python
        // SDK passes a Query here). An object-wrapped expression still goes
        // through the expression reader.
        let mut nested = QueryBuilder::new();
        build_query_internal(&mut nested, dict, depth, path, false)?;
        let (nested_arena, nested_handle) = nested.build();
        Ok(builder.import_subarena(nested_arena, nested_handle))
    } else {
        value_to_node_handle(builder, spec, depth, path)
    }
}

fn build_subquery(
    builder: &mut QueryBuilder,
    spec: &Value,
    depth: usize,
    path: &SpecPath,
) -> Result<NodeHandle, SpecError> {
    if let Some(dict) = spec.as_object() {
        // Build the nested query in its own builder and import it. Doing this
        // directly avoids a closure frame per nesting level, which matters for
        // the documented expression-depth limit.
        let mut nested = QueryBuilder::new();
        build_query_internal(&mut nested, dict, depth, path, false)?;
        let (nested_arena, nested_handle) = nested.build();
        Ok(builder.import_subarena(nested_arena, nested_handle))
    } else if let Some(steps) = spec.as_array() {
        let mut nested = QueryBuilder::new();
        nested.r#match();
        build_path_steps_into_builder(&mut nested, steps, depth, path)?;
        let (nested_arena, nested_handle) = nested.build();
        Ok(builder.import_subarena(nested_arena, nested_handle))
    } else {
        value_to_node_handle(builder, spec, depth, path)
    }
}

const ROOT_SPEC_KEYS: &[&str] = &[
    "spec_version",
    "load_csv",
    "unwinds",
    "matches",
    "mutations",
    "with_clauses",
    "projections",
    "distinct",
    "order_by",
    "skip",
    "offset",
    "limit",
    "execution_mode",
    // Supported by the consumer and exercised by the round-trip suite, but not
    // yet listed in the v1 schema (schema completeness is tracked separately).
    "linear_clauses",
];

const MATCH_SPEC_KEYS: &[&str] = &[
    "spec_version",
    "optional",
    "path_mode",
    "path_variable",
    "paths",
    "where",
];

const WITH_SPEC_KEYS: &[&str] = &[
    "spec_version",
    "distinct",
    "projections",
    "where",
    "order_by",
    "skip",
    "limit",
];

/// Rejects any object key that is not in the closed v1 key set for that object.
fn reject_unknown_keys(
    dict: &Map<String, Value>,
    path: &SpecPath,
    allowed: &[&str],
    what: &str,
) -> Result<(), SpecError> {
    for key in dict.keys() {
        if let Some(kind) = non_string_key_kind(key) {
            return Err(make_spec_err(
                path,
                format!("invalid {what}: object keys must be strings, got {kind}"),
            ));
        }
        if !allowed.contains(&key.as_str()) {
            return Err(make_spec_err(
                &path.key(key),
                format!(
                    "unknown {what} key '{key}'. Allowed keys: {}.",
                    allowed.join(", ")
                ),
            ));
        }
    }
    Ok(())
}

/// Parses a pagination position: a non-negative integer literal or a
/// `["param", name]` reference.
fn parse_pagination(
    val: &Value,
    path: &SpecPath,
    field: &str,
) -> Result<PaginationValue, SpecError> {
    if let Value::Array(array) = val {
        check_spec_arity_bounded(
            array,
            path,
            &format!("'{field}' parameter"),
            2,
            2,
            "[param, name]",
        )?;
        let tag = extract_string(&array[0], &path.index(0), "the string tag 'param'")?;
        if tag != "param" {
            return Err(make_spec_err(
                &path.index(0),
                format!(
                    "Invalid '{field}': expected a non-negative integer or [\"param\", name], got the tag '{tag}'."
                ),
            ));
        }
        let name = extract_string(&array[1], &path.index(1), "a string name")?;
        let name = strip_leading_dollar(name);
        if name.is_empty() {
            return Err(make_spec_err(
                &path.index(1),
                format!("Invalid '{field}': the parameter name must be a non-empty string."),
            ));
        }
        return Ok(PaginationValue::Parameter(name));
    }
    Ok(PaginationValue::Count(parse_non_negative(
        val, path, field,
    )?))
}

/// Parses a non-negative `skip` / `offset` / `limit` value.
fn parse_non_negative(val: &Value, path: &SpecPath, field: &str) -> Result<u64, SpecError> {
    if val.is_boolean() {
        return Err(make_spec_err(
            path,
            format!(
                "Invalid '{field}': expected a non-negative integer, got {}.",
                json_type_name(val)
            ),
        ));
    }
    let value: i64 = if matches!(val, Value::Number(number) if number.is_f64()) {
        let number = val
            .as_f64()
            .ok_or_else(|| expected_error(val, path, "a non-negative integer"))?;
        if !number.is_finite() || number.fract() != 0.0 {
            let rendered =
                render_json_scalar(val).unwrap_or_else(|| json_type_name(val).to_string());
            return Err(make_spec_err(
                path,
                format!(
                    "Invalid '{field}': expected a non-negative integer, got {rendered} (not an integral number)."
                ),
            ));
        }
        if number < 0.0 || number > i64::MAX as f64 {
            return Err(make_spec_err(
                path,
                format!("Invalid '{field}': expected a non-negative integer, got {number}."),
            ));
        }
        number as i64
    } else {
        match val {
            Value::Number(number) => number
                .as_i64()
                .ok_or_else(|| expected_error(val, path, "a non-negative integer"))?,
            _ => return Err(expected_error(val, path, "a non-negative integer")),
        }
    };
    if value < 0 {
        return Err(make_spec_err(
            path,
            format!("Invalid '{field}': expected a non-negative integer, got {value}."),
        ));
    }
    Ok(value as u64)
}

/// Builds one projection item (`field`, `agg`, or `expr`).
fn build_projection_item(
    builder: &mut QueryBuilder,
    item: &Value,
    depth: usize,
    path: &SpecPath,
) -> Result<(), SpecError> {
    let array = expect_spec_array(item, path, "projection")?;
    check_spec_arity(array, path, "projection", 1, "[tag, ...]")?;
    let tag_path = path.index(0);
    let tag = extract_string(&array[0], &tag_path, "a string projection tag")?;
    match tag.as_str() {
        "field" => {
            check_spec_arity_bounded(
                array,
                path,
                "projection 'field'",
                4,
                4,
                "[field, variable, property, alias]",
            )?;
            let var = extract_string(&array[1], &path.index(1), "a string variable")?;
            let prop = extract_string(&array[2], &path.index(2), "a string property")?;
            let alias =
                extract_optional_string(&array[3], &path.index(3), "a string or null alias")?;
            builder.field(var, prop, alias);
        }
        "agg" => {
            check_spec_arity_bounded(
                array,
                path,
                "projection 'agg'",
                5,
                5,
                "[agg, variable, property, func, alias]",
            )?;
            let var = extract_string(&array[1], &path.index(1), "a string variable")?;
            let prop = extract_string(&array[2], &path.index(2), "a string property")?;
            let func_path = path.index(3);
            let func_str = extract_string(&array[3], &func_path, "a string aggregation function")?;
            let alias =
                extract_optional_string(&array[4], &path.index(4), "a string or null alias")?;
            let agg = match func_str.as_str() {
                "count" => AggregationFunc::Count,
                "count_distinct" => AggregationFunc::CountDistinct,
                "sum" => AggregationFunc::Sum,
                "avg" => AggregationFunc::Avg,
                "min" => AggregationFunc::Min,
                "max" => AggregationFunc::Max,
                "collect" => AggregationFunc::Collect,
                other => {
                    return Err(make_spec_err(
                        &func_path,
                        format!(
                            "Unknown aggregation function '{other}'. Expected count, count_distinct, sum, avg, min, max, collect."
                        ),
                    ));
                }
            };
            if prop == "*" || prop.is_empty() {
                let expr = builder.ident(var);
                builder.select_aggregate(expr, agg, alias);
            } else {
                builder.select_property_aggregate(var, prop, agg, alias);
            }
        }
        "expr" => {
            check_spec_arity_bounded(
                array,
                path,
                "projection 'expr'",
                3,
                3,
                "[expr, expression, alias]",
            )?;
            let expr_path = path.index(1);
            let expr_spec = &array[1];
            let mut alias =
                extract_optional_string(&array[2], &path.index(2), "a string or null alias")?;
            if alias.is_none() {
                alias = extract_alias_from_expr_spec(expr_spec, &expr_path)?;
            }
            let handle = value_to_node_handle(builder, expr_spec, depth, &expr_path)?;
            builder.select_expr(handle, alias);
        }
        other => {
            return Err(make_spec_err(
                &tag_path,
                format!("Unknown projection tag '{other}'. Expected 'field', 'agg', or 'expr'."),
            ));
        }
    }
    Ok(())
}

fn build_query_internal(
    builder: &mut QueryBuilder,
    spec: &Map<String, Value>,
    depth: usize,
    path: &SpecPath,
    require_version: bool,
) -> Result<(), SpecError> {
    // Spec Versioning & Compatibility Check:
    // Requires top-level "spec_version": 1 in accordance with the Query Spec
    // Interchange Protocol v1.
    let version_path = path.key("spec_version");
    match spec.get("spec_version") {
        Some(ver_item) => {
            let ver = extract_u32(ver_item, &version_path, "an integer major version number")?;
            if ver != 1 {
                return Err(make_spec_err(
                    &version_path,
                    format!("Unsupported query spec_version: {ver}. Supported major versions: 1."),
                ));
            }
        }
        None => {
            if require_version {
                return Err(make_spec_err(
                    &version_path,
                    "Missing required 'spec_version' in query spec dictionary. Expected 'spec_version': 1.",
                ));
            }
        }
    }

    reject_unknown_keys(spec, path, ROOT_SPEC_KEYS, "top-level query spec")?;

    let mut produced_clause = false;
    let mut mutation_count = 0usize;
    let mut with_clause_count = 0usize;
    let mut has_root_projections = false;

    if let Some(load_csv_item) = spec.get("load_csv")
        && !load_csv_item.is_null()
    {
        let item_path = path.key("load_csv");
        let array = expect_spec_array(load_csv_item, &item_path, "load_csv")?;
        check_spec_arity_bounded(
            array,
            &item_path,
            "load_csv",
            3,
            3,
            "[url, with_headers, alias]",
        )?;
        let url = extract_string(&array[0], &item_path.index(0), "a string URL")?;
        let with_headers = extract_bool(&array[1], &item_path.index(1), "a boolean")?;
        let alias = extract_string(&array[2], &item_path.index(2), "a string alias")?;
        builder.load_csv(url, with_headers, alias);
        produced_clause = true;
    }

    if let Some(unwinds_item) = spec.get("unwinds") {
        let key_path = path.key("unwinds");
        let unwinds = expect_spec_array(unwinds_item, &key_path, "unwinds")?;
        for (idx, item) in unwinds.iter().enumerate() {
            let item_path = key_path.index(idx);
            let array = expect_spec_array(item, &item_path, "unwinds entry")?;
            check_spec_arity_bounded(array, &item_path, "unwinds entry", 2, 2, "[param, alias]")?;
            let param_name =
                extract_string(&array[0], &item_path.index(0), "a string parameter name")?;
            let param_name = strip_leading_dollar(param_name);
            if param_name.is_empty() {
                return Err(make_spec_err(
                    &item_path.index(0),
                    "Invalid 'unwinds entry': the parameter name must be a non-empty string.",
                ));
            }
            let alias = extract_string(&array[1], &item_path.index(1), "a string alias")?;
            builder.unwind_param(param_name, alias);
            produced_clause = true;
        }
    }

    if let Some(matches_item) = spec.get("matches") {
        let key_path = path.key("matches");
        let matches = expect_spec_array(matches_item, &key_path, "matches")?;
        for (idx, m) in matches.iter().enumerate() {
            let m_path = key_path.index(idx);
            let Some(m_dict) = m.as_object() else {
                return Err(make_spec_err(
                    &m_path,
                    format!("expected a match clause object, got {}", json_type_name(m)),
                ));
            };
            reject_unknown_keys(m_dict, &m_path, MATCH_SPEC_KEYS, "match clause")?;
            if let Some(m_ver_item) = m_dict.get("spec_version") {
                let version_path = m_path.key("spec_version");
                let m_ver =
                    extract_u32(m_ver_item, &version_path, "an integer major version number")?;
                if m_ver != 1 {
                    return Err(make_spec_err(
                        &version_path,
                        format!(
                            "Unsupported match clause spec_version: {m_ver}. Supported major versions: 1."
                        ),
                    ));
                }
            }
            let paths_item = m_dict.get("paths").ok_or_else(|| {
                make_spec_err(
                    &m_path.key("paths"),
                    "missing required 'paths' in match clause.",
                )
            })?;
            if paths_item.is_null() {
                return Err(make_spec_err(
                    &m_path.key("paths"),
                    "missing required 'paths' in match clause.",
                ));
            }
            let paths_path = m_path.key("paths");
            let paths = expect_spec_array(paths_item, &paths_path, "paths")?;

            let is_optional = if let Some(optional_item) = m_dict.get("optional") {
                if optional_item.is_null() {
                    false
                } else {
                    extract_bool(optional_item, &m_path.key("optional"), "a boolean")?
                }
            } else {
                false
            };
            if is_optional {
                builder.optional_match();
            } else {
                builder.r#match();
            }

            if let Some(mode_item) = m_dict.get("path_mode")
                && !mode_item.is_null()
            {
                let mode_path = m_path.key("path_mode");
                let mode_str = extract_string(mode_item, &mode_path, "a string path mode")?;
                let mode = match mode_str.as_str() {
                    "trail" => PathMode::Trail,
                    "simple" => PathMode::Simple,
                    "acyclic" => PathMode::Acyclic,
                    "walk" => PathMode::Walk,
                    "none" => PathMode::None,
                    other => {
                        return Err(make_spec_err(
                            &mode_path,
                            format!(
                                "unknown path_mode '{other}'. Expected 'trail', 'simple', 'acyclic', 'walk', or 'none'."
                            ),
                        ));
                    }
                };
                builder.path_mode(mode);
            }
            if let Some(var_item) = m_dict.get("path_variable")
                && !var_item.is_null()
            {
                let var_path = m_path.key("path_variable");
                let variable = extract_string(var_item, &var_path, "a string path variable")?;
                builder.path_variable(variable);
            }
            if paths.is_empty() {
                return Err(make_spec_err(
                    &paths_path,
                    "a match clause needs at least one path; an empty 'paths' list has no pattern and would leave its predicates unattached",
                ));
            }

            for (p_idx, p_steps) in paths.iter().enumerate() {
                if p_idx > 0 {
                    builder.pattern();
                }
                let p_path = paths_path.index(p_idx);
                let steps = expect_spec_array(p_steps, &p_path, "path")?;
                if !steps.is_empty() {
                    produced_clause = true;
                }
                build_path_steps_into_builder(builder, steps, depth, &p_path)?;
            }

            if let Some(where_item) = m_dict.get("where")
                && !where_item.is_null()
            {
                build_where_predicates(builder, where_item, depth, &m_path.key("where"))?;
            }
        }
    }

    if let Some(mutations_item) = spec.get("mutations") {
        let key_path = path.key("mutations");
        let mutations = expect_spec_array(mutations_item, &key_path, "mutations")?;
        mutation_count = mutations.len();
        for (idx, mut_item) in mutations.iter().enumerate() {
            let mut_path = key_path.index(idx);
            let array = expect_spec_array(mut_item, &mut_path, "mutation")?;
            check_spec_arity(array, &mut_path, "mutation", 1, "[tag, ...]")?;
            let tag_path = mut_path.index(0);
            let tag = extract_string(&array[0], &tag_path, "a string mutation tag")?;
            match tag.as_str() {
                "create" => {
                    check_spec_arity_bounded(
                        array,
                        &mut_path,
                        "mutation 'create'",
                        2,
                        2,
                        "[create, paths]",
                    )?;
                    builder.create();
                    let paths_path = mut_path.index(1);
                    let paths =
                        expect_spec_array(&array[1], &paths_path, "mutation 'create' paths")?;
                    for (p_idx, p_steps) in paths.iter().enumerate() {
                        if p_idx > 0 {
                            builder.pattern();
                        }
                        let p_path = paths_path.index(p_idx);
                        let steps = expect_spec_array(p_steps, &p_path, "mutation 'create' path")?;
                        build_path_steps_into_builder(builder, steps, depth, &p_path)?;
                    }
                }
                "merge" => {
                    check_spec_arity_bounded(
                        array,
                        &mut_path,
                        "mutation 'merge'",
                        4,
                        4,
                        "[merge, path, on_create_set, on_match_set]",
                    )?;
                    builder.merge();
                    let path_path = mut_path.index(1);
                    let path_steps =
                        expect_spec_array(&array[1], &path_path, "mutation 'merge' path")?;
                    build_path_steps_into_builder(builder, path_steps, depth, &path_path)?;

                    let on_create_path = mut_path.index(2);
                    let on_creates = expect_spec_array(
                        &array[2],
                        &on_create_path,
                        "merge 'on_create_set' entries",
                    )?;
                    for (s_idx, item) in on_creates.iter().enumerate() {
                        let item_path = on_create_path.index(s_idx);
                        let set_array =
                            expect_spec_array(item, &item_path, "merge 'on_create_set' entry")?;
                        check_spec_arity_bounded(
                            set_array,
                            &item_path,
                            "merge 'on_create_set' entry",
                            3,
                            3,
                            "[variable, property, value]",
                        )?;
                        let var = extract_string(
                            &set_array[0],
                            &item_path.index(0),
                            "a string variable",
                        )?;
                        let prop = extract_string(
                            &set_array[1],
                            &item_path.index(1),
                            "a string property",
                        )?;
                        let value = value_to_node_handle(
                            builder,
                            &set_array[2],
                            depth,
                            &item_path.index(2),
                        )?;
                        builder.on_create_set_expr(var, prop, value);
                    }

                    let on_match_path = mut_path.index(3);
                    let on_matches = expect_spec_array(
                        &array[3],
                        &on_match_path,
                        "merge 'on_match_set' entries",
                    )?;
                    for (s_idx, item) in on_matches.iter().enumerate() {
                        let item_path = on_match_path.index(s_idx);
                        let set_array =
                            expect_spec_array(item, &item_path, "merge 'on_match_set' entry")?;
                        check_spec_arity_bounded(
                            set_array,
                            &item_path,
                            "merge 'on_match_set' entry",
                            3,
                            3,
                            "[variable, property, value]",
                        )?;
                        let var = extract_string(
                            &set_array[0],
                            &item_path.index(0),
                            "a string variable",
                        )?;
                        let prop = extract_string(
                            &set_array[1],
                            &item_path.index(1),
                            "a string property",
                        )?;
                        let value = value_to_node_handle(
                            builder,
                            &set_array[2],
                            depth,
                            &item_path.index(2),
                        )?;
                        builder.on_match_set_expr(var, prop, value);
                    }
                }
                "set" => {
                    check_spec_arity_bounded(
                        array,
                        &mut_path,
                        "mutation 'set'",
                        3,
                        4,
                        "[set, variable, map] or [set, variable, property, value]",
                    )?;
                    let var = extract_string(&array[1], &mut_path.index(1), "a string variable")?;
                    if array.len() == 3 {
                        let value =
                            value_to_node_handle(builder, &array[2], depth, &mut_path.index(2))?;
                        builder.set_merge_expr(var, value);
                    } else {
                        let prop =
                            extract_string(&array[2], &mut_path.index(2), "a string property")?;
                        let value =
                            value_to_node_handle(builder, &array[3], depth, &mut_path.index(3))?;
                        builder.set_property_expr(var, prop, value);
                    }
                }
                "delete" => {
                    check_spec_arity_bounded(
                        array,
                        &mut_path,
                        "mutation 'delete'",
                        3,
                        3,
                        "[delete, detach, targets]",
                    )?;
                    let detach = extract_bool(&array[1], &mut_path.index(1), "a boolean")?;
                    let targets = extract_string_array(
                        &array[2],
                        &mut_path.index(2),
                        "an array of string variables",
                    )?;
                    if detach {
                        builder.detach_delete(targets);
                    } else {
                        builder.delete(targets);
                    }
                }
                "remove" => {
                    check_spec_arity_bounded(
                        array,
                        &mut_path,
                        "mutation 'remove'",
                        3,
                        3,
                        "[remove, variable, property]",
                    )?;
                    let var = extract_string(&array[1], &mut_path.index(1), "a string variable")?;
                    let prop = extract_string(&array[2], &mut_path.index(2), "a string property")?;
                    builder.remove_property(var, prop);
                }
                other => {
                    return Err(make_spec_err(
                        &tag_path,
                        format!(
                            "Unknown mutation tag: '{other}'. Expected 'create', 'merge', 'set', 'delete', or 'remove'."
                        ),
                    ));
                }
            }
            produced_clause = true;
        }
    }

    if let Some(with_item) = spec.get("with_clauses") {
        let key_path = path.key("with_clauses");
        let with_sequence = expect_spec_array(with_item, &key_path, "with_clauses")?;
        with_clause_count = with_sequence.len();
        for (idx, item) in with_sequence.iter().enumerate() {
            let item_path = key_path.index(idx);
            let Some(with_dict) = item.as_object() else {
                return Err(make_spec_err(
                    &item_path,
                    format!(
                        "expected a with clause object, got {}",
                        json_type_name(item)
                    ),
                ));
            };
            reject_unknown_keys(with_dict, &item_path, WITH_SPEC_KEYS, "with clause")?;
            if let Some(w_ver_item) = with_dict.get("spec_version") {
                let version_path = item_path.key("spec_version");
                let w_ver =
                    extract_u32(w_ver_item, &version_path, "an integer major version number")?;
                if w_ver != 1 {
                    return Err(make_spec_err(
                        &version_path,
                        format!(
                            "Unsupported with clause spec_version: {w_ver}. Supported major versions: 1."
                        ),
                    ));
                }
            }
            let proj_item = with_dict.get("projections").ok_or_else(|| {
                make_spec_err(
                    &item_path.key("projections"),
                    "missing required 'projections' in with clause.",
                )
            })?;
            if proj_item.is_null() {
                return Err(make_spec_err(
                    &item_path.key("projections"),
                    "missing required 'projections' in with clause.",
                ));
            }
            let proj_path = item_path.key("projections");
            let projections =
                expect_spec_array(proj_item, &proj_path, "with_clauses 'projections'")?;
            if projections.is_empty() {
                return Err(make_spec_err(
                    &proj_path,
                    "a with clause needs at least one projection; an empty projection list would drop its where, order_by, skip and limit",
                ));
            }
            builder.r#with();
            produced_clause = true;
            if let Some(distinct_item) = with_dict.get("distinct")
                && !distinct_item.is_null()
            {
                let distinct =
                    extract_bool(distinct_item, &item_path.key("distinct"), "a boolean")?;
                if distinct {
                    builder.distinct(true);
                }
            }
            for (p_idx, p) in projections.iter().enumerate() {
                build_projection_item(builder, p, depth, &proj_path.index(p_idx))?;
            }
            if let Some(where_item) = with_dict.get("where")
                && !where_item.is_null()
            {
                build_where_predicates(builder, where_item, depth, &item_path.key("where"))?;
            }
            if let Some(order_item) = with_dict.get("order_by")
                && !order_item.is_null()
            {
                let order_path = item_path.key("order_by");
                let order_entries =
                    expect_spec_array(order_item, &order_path, "with_clauses 'order_by'")?;
                for (o_idx, order_entry) in order_entries.iter().enumerate() {
                    build_order_by_entry(
                        builder,
                        order_entry,
                        depth,
                        &order_path.index(o_idx),
                        "with_clauses 'order_by' entry",
                    )?;
                }
            }
            if let Some(skip_item) = with_dict.get("skip")
                && !skip_item.is_null()
            {
                match parse_pagination(skip_item, &item_path.key("skip"), "skip")? {
                    PaginationValue::Count(skip) => {
                        builder.skip(skip);
                    }
                    PaginationValue::Parameter(name) => {
                        builder.skip_param(name);
                    }
                }
            }
            if let Some(limit_item) = with_dict.get("limit")
                && !limit_item.is_null()
            {
                match parse_pagination(limit_item, &item_path.key("limit"), "limit")? {
                    PaginationValue::Count(limit) => {
                        builder.limit(limit);
                    }
                    PaginationValue::Parameter(name) => {
                        builder.limit_param(name);
                    }
                }
            }
        }
    }

    if let Some(proj_item) = spec.get("projections")
        && !proj_item.is_null()
    {
        let proj_path = path.key("projections");
        let projections = expect_spec_array(proj_item, &proj_path, "projections")?;
        if !projections.is_empty() {
            builder.r#return();
            produced_clause = true;
            has_root_projections = true;
        }
        for (idx, item) in projections.iter().enumerate() {
            build_projection_item(builder, item, depth, &proj_path.index(idx))?;
        }
    }

    // Root pagination and DISTINCT belong to the final RETURN. Without one they
    // would either be dropped or fold into the last pending WITH, silently
    // changing the query's meaning, so reject them at the first offending key.
    if !has_root_projections {
        for key in ["distinct", "order_by", "skip", "offset", "limit"] {
            if let Some(item) = spec.get(key)
                && !item.is_null()
            {
                return Err(make_spec_err(
                    &path.key(key),
                    format!(
                        "root-level '{key}' requires 'projections'; add a non-empty root projection list (a RETURN clause) or move it into a with clause"
                    ),
                ));
            }
        }
        // A statement that ends in WITH is not a complete query. Mutations emit
        // after WITH, so a mutation-only tail is fine.
        if with_clause_count > 0 && mutation_count == 0 {
            return Err(make_spec_err(
                &path.key("projections"),
                "a query ending in WITH needs root projections; add a non-empty root 'projections' list to close the statement with RETURN",
            ));
        }
    }

    if let Some(distinct_item) = spec.get("distinct")
        && !distinct_item.is_null()
    {
        let distinct = extract_bool(distinct_item, &path.key("distinct"), "a boolean")?;
        if distinct {
            builder.distinct(true);
        }
    }

    if let Some(order_item) = spec.get("order_by")
        && !order_item.is_null()
    {
        let order_path = path.key("order_by");
        let order_entries = expect_spec_array(order_item, &order_path, "order_by")?;
        for (idx, order_entry) in order_entries.iter().enumerate() {
            build_order_by_entry(
                builder,
                order_entry,
                depth,
                &order_path.index(idx),
                "order_by entry",
            )?;
        }
    }

    // `skip` and `offset` are aliases of the same pagination position. Accepting
    // both used to let the later one win silently, so a document that sets both
    // is rejected at the second key in document order.
    if let (Some(skip_item), Some(offset_item)) = (spec.get("skip"), spec.get("offset"))
        && !skip_item.is_null()
        && !offset_item.is_null()
    {
        let mut seen = 0usize;
        let mut second_key: Option<&str> = None;
        for key in spec.keys() {
            if key == "skip" || key == "offset" {
                seen += 1;
                if seen == 2 {
                    second_key = Some(key);
                }
            }
        }
        let second_key = second_key.unwrap_or("offset");
        return Err(make_spec_err(
            &path.key(second_key),
            "conflicting pagination keys: 'skip' and 'offset' are aliases; provide only one of them.",
        ));
    }

    if let Some(skip_item) = spec.get("skip")
        && !skip_item.is_null()
    {
        match parse_pagination(skip_item, &path.key("skip"), "skip")? {
            PaginationValue::Count(skip) => {
                builder.skip(skip);
            }
            PaginationValue::Parameter(name) => {
                builder.skip_param(name);
            }
        }
    }

    if let Some(offset_item) = spec.get("offset")
        && !offset_item.is_null()
    {
        match parse_pagination(offset_item, &path.key("offset"), "offset")? {
            PaginationValue::Count(offset) => {
                builder.offset(offset);
            }
            PaginationValue::Parameter(name) => {
                builder.offset_param(name);
            }
        }
    }

    if let Some(limit_item) = spec.get("limit")
        && !limit_item.is_null()
    {
        match parse_pagination(limit_item, &path.key("limit"), "limit")? {
            PaginationValue::Count(limit) => {
                builder.limit(limit);
            }
            PaginationValue::Parameter(name) => {
                builder.limit_param(name);
            }
        }
    }

    if let Some(linear_item) = spec.get("linear_clauses") {
        let key_path = path.key("linear_clauses");
        let linear = expect_spec_array(linear_item, &key_path, "linear_clauses")?;
        for (idx, item) in linear.iter().enumerate() {
            let item_path = key_path.index(idx);
            let array = expect_spec_array(item, &item_path, "linear_clauses entry")?;
            check_spec_arity(array, &item_path, "linear_clauses entry", 1, "[tag, ...]")?;
            let tag_path = item_path.index(0);
            let tag = extract_string(&array[0], &tag_path, "a string linear clause tag")?;
            match tag.as_str() {
                "let" => {
                    check_spec_arity_bounded(
                        array,
                        &item_path,
                        "linear clause 'let'",
                        3,
                        3,
                        "[let, variable, expression]",
                    )?;
                    let var = extract_string(&array[1], &item_path.index(1), "a string variable")?;
                    let expr =
                        value_to_node_handle(builder, &array[2], depth, &item_path.index(2))?;
                    builder.let_(var, expr);
                }
                "filter" => {
                    check_spec_arity_bounded(
                        array,
                        &item_path,
                        "linear clause 'filter'",
                        2,
                        2,
                        "[filter, predicate]",
                    )?;
                    let predicate =
                        value_to_node_handle(builder, &array[1], depth, &item_path.index(1))?;
                    builder.filter_(predicate);
                }
                other => {
                    return Err(make_spec_err(
                        &tag_path,
                        format!(
                            "Unknown linear clause tag: '{other}'. Expected 'let' or 'filter'."
                        ),
                    ));
                }
            }
            produced_clause = true;
        }
    }

    if let Some(mode_item) = spec.get("execution_mode")
        && !mode_item.is_null()
    {
        let mode_path = path.key("execution_mode");
        let mode_str = extract_string(mode_item, &mode_path, "a string execution mode")?;
        match mode_str.as_str() {
            "normal" => {}
            "explain" => {
                builder.explain();
            }
            "profile" => {
                builder.profile();
            }
            "explain_and_profile" => {
                builder.explain();
                builder.profile();
            }
            other => {
                return Err(make_spec_err(
                    &mode_path,
                    format!(
                        "unknown execution_mode '{other}'. Expected 'normal', 'explain', 'profile', or 'explain_and_profile'."
                    ),
                ));
            }
        }
    }

    if !produced_clause {
        return Err(make_spec_err(
            path,
            "empty query spec: expected at least one of 'load_csv', 'unwinds', 'matches', 'mutations', 'with_clauses', 'projections', or 'linear_clauses'.",
        ));
    }

    // Scope is checked once for the document root. Nested `exists`/`count`
    // specs are reached from the expression walker with the enclosing scope,
    // so re-checking them from their own build entry point would lose the
    // correlated variables they are allowed to read.
    if depth == 0 {
        validate_spec_scope(spec, path, depth)?;
    }

    Ok(())
}

/// Rejects excessive expression nesting before the recursive builder runs.
///
/// The depth counter mirrors the recursive consumer: every tagged expression
/// array and every object-wrapped expression adds one level, while structural
/// arrays (path steps, spec containers) do not. This keeps the documented
/// depth guard from overflowing the caller's stack on an absurdly deep
/// document. A cheap pass runs first; only an over-limit document pays for the
/// second pass that reconstructs the offending JSON Pointer.
fn check_expression_depth(spec: &Value) -> Result<(), SpecError> {
    if !expression_depth_exceeds(spec) {
        return Ok(());
    }
    let path = first_expression_depth_violation(spec).unwrap_or_else(SpecPath::root);
    Err(make_spec_err(
        &path,
        format!(
            "Maximum AST expression recursion depth exceeded ({MAX_AST_DEPTH}). Query is too deeply nested.",
        ),
    ))
}

fn expression_depth_exceeds(spec: &Value) -> bool {
    let limit = MAX_AST_DEPTH + 1;
    let mut stack = vec![(spec, 0usize)];
    while let Some((value, depth)) = stack.pop() {
        match value {
            Value::Array(items) => {
                let expression = items
                    .first()
                    .and_then(Value::as_str)
                    .is_some_and(is_known_expr_tag);
                let child_depth = if expression { depth + 1 } else { depth };
                if child_depth > limit {
                    return true;
                }
                for item in items {
                    stack.push((item, child_depth));
                }
            }
            Value::Object(members) => {
                let child_depth = if let Some(inner) = members.get("expr") {
                    let child_depth = depth + 1;
                    if child_depth > limit {
                        return true;
                    }
                    stack.push((inner, child_depth));
                    child_depth
                } else {
                    depth
                };
                for (key, value) in members {
                    if key != "expr" {
                        stack.push((value, child_depth));
                    }
                }
            }
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
        }
    }
    false
}

fn first_expression_depth_violation(spec: &Value) -> Option<SpecPath> {
    let limit = MAX_AST_DEPTH + 1;
    let mut stack = vec![(spec, 0usize, SpecPath::root())];
    while let Some((value, depth, path)) = stack.pop() {
        match value {
            Value::Array(items) => {
                let expression = items
                    .first()
                    .and_then(Value::as_str)
                    .is_some_and(is_known_expr_tag);
                let child_depth = if expression { depth + 1 } else { depth };
                if child_depth > limit {
                    return Some(path);
                }
                for (index, item) in items.iter().enumerate() {
                    stack.push((item, child_depth, path.index(index)));
                }
            }
            Value::Object(members) => {
                let child_depth = if let Some(inner) = members.get("expr") {
                    let child_depth = depth + 1;
                    if child_depth > limit {
                        return Some(path);
                    }
                    stack.push((inner, child_depth, path.key("expr")));
                    child_depth
                } else {
                    depth
                };
                for (key, value) in members {
                    if key != "expr" {
                        stack.push((value, child_depth, path.key(key)));
                    }
                }
            }
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
        }
    }
    None
}

/// Builds a [`QueryBuilder`] from a Query Spec v1 JSON document.
///
/// The top-level value must be an object with `spec_version: 1`. The returned
/// builder has not been validated by an emitter yet; call
/// [`QueryBuilder::try_build`] to finish the AST. Structural failures return a
/// [`SpecError`] with code [`ErrorCode::InvalidSpec`], the JSON Pointer to the
/// offending element and the same message the Python bindings expose.
///
/// # Example
///
/// ```
/// use serde_json::json;
///
/// let spec = json!({
///     "spec_version": 1,
///     "matches": [{"paths": [[["node", "p", ["Person"]]]]}],
///     "projections": [["field", "p", "name", null]]
/// });
/// let builder = voyager_ogm::spec::build_query(&spec).unwrap();
/// assert!(builder.try_build().is_ok());
/// ```
pub fn build_query(spec: &Value) -> Result<QueryBuilder, SpecError> {
    let Some(dict) = spec.as_object() else {
        return Err(make_spec_err(
            &SpecPath::root(),
            format!(
                "expected a query spec object (dict), got {}",
                json_type_name(spec)
            ),
        ));
    };
    check_expression_depth(spec)?;
    let mut builder = QueryBuilder::new();
    build_query_internal(&mut builder, dict, 0, &SpecPath::root(), true)?;
    Ok(builder)
}

/// Builds a Query Spec document that is nested inside a fluent expression.
///
/// Nested specs may omit `spec_version`, matching the historical fluent-API
/// behaviour for a dictionary passed in an expression position. The Python
/// bindings use this for such a dictionary; top-level documents should call
/// [`build_query`] instead, which requires `spec_version: 1`.
pub fn build_nested_query(spec: &Value) -> Result<QueryBuilder, SpecError> {
    let Some(dict) = spec.as_object() else {
        return Err(make_spec_err(
            &SpecPath::root(),
            format!(
                "expected a query spec object (dict), got {}",
                json_type_name(spec)
            ),
        ));
    };
    check_expression_depth(spec)?;
    let mut builder = QueryBuilder::new();
    build_query_internal(&mut builder, dict, 0, &SpecPath::root(), false)?;
    Ok(builder)
}

fn sanitize_compile_message(message: &str) -> String {
    fn node_kind(text: &str) -> Option<&str> {
        let mut end = 0;
        for (idx, ch) in text.char_indices() {
            let allowed = if idx == 0 {
                ch.is_ascii_uppercase()
            } else {
                ch.is_ascii_alphanumeric() || ch == '_'
            };
            if allowed {
                end = idx + ch.len_utf8();
            } else {
                break;
            }
        }
        if end == 0 {
            return None;
        }
        let rest = &text[end..];
        if rest.starts_with('(') || rest.starts_with('{') || rest.starts_with(" {") {
            Some(&text[..end])
        } else {
            None
        }
    }
    for marker in [": ", " got "] {
        for (idx, _) in message.match_indices(marker) {
            let tail = &message[idx + marker.len()..];
            if let Some(kind) = node_kind(tail) {
                return format!("{}{kind} (AST node kind)", &message[..idx + marker.len()]);
            }
        }
    }
    message.to_string()
}

fn map_compile_error(error: crate::error::Error) -> SpecError {
    let message = sanitize_compile_message(&error.to_string());
    match error.code() {
        ErrorCode::UnsupportedFeature | ErrorCode::UnsupportedDialect => {
            SpecError::new(error.code(), Some(String::new()), message)
        }
        _ => make_spec_err(&SpecPath::root(), message),
    }
}

fn cypher_path_mode_warnings(dialect: &str, path_mode: Option<&str>) -> Vec<String> {
    let mut warnings = Vec::new();
    if matches!(
        dialect.trim().to_ascii_lowercase().as_str(),
        "cypher" | "opencypher" | "neo4j" | "memgraph"
    ) && let Some(mode) = path_mode
        && !mode.is_empty()
        && !mode.eq_ignore_ascii_case("none")
    {
        warnings.push(format!(
            "Dialect '{dialect}' does not support explicit path search modes ('{mode}'). Cypher default traversal semantics (trail) will be used and the search mode keyword was omitted."
        ));
    }
    warnings
}

/// Compiles a Query Spec v1 JSON document for a dialect.
///
/// `dialect` accepts the consumer aliases: `cypher`, `opencypher`, `neo4j`,
/// `memgraph`, `falkordb`, `falkor`, `sql_pgq`, `pgq`, `duckpgq`, `sql`,
/// `iso_gql`, `gql`, `age`, `apache_age`, and `postgres_age`. The returned
/// [`SpecCompileResult`] carries the compiled statement and any non-fatal
/// warning the caller should surface.
///
/// Structural failures carry [`ErrorCode::InvalidSpec`] and a JSON Pointer.
/// An unknown dialect carries [`ErrorCode::UnsupportedDialect`] without a
/// path. Emitter limitations keep their [`ErrorCode::UnsupportedFeature`] or
/// [`ErrorCode::UnsupportedDialect`] code.
///
/// # Example
///
/// ```
/// use serde_json::json;
///
/// let spec = json!({
///     "spec_version": 1,
///     "matches": [{"paths": [[["node", "p", ["Person"]]]]}],
///     "projections": [["field", "p", "name", null]]
/// });
/// let result = voyager_ogm::spec::compile(
///     &spec,
///     "cypher",
///     &voyager_ogm::spec::SpecCompileOptions::default(),
/// )
/// .unwrap();
/// assert_eq!(result.compiled.statement, "MATCH (p:Person) RETURN p.name");
/// ```
pub fn compile(
    spec: &Value,
    dialect: &str,
    options: &SpecCompileOptions,
) -> Result<SpecCompileResult, SpecError> {
    let cache_key = if options.use_cache {
        Some(format!(
            "{}:{}:{}:{}:{}",
            dialect,
            options.graph_name.as_deref().unwrap_or(""),
            serde_json::to_string(spec).unwrap_or_default(),
            options.optimize,
            options.optimization_level
        ))
    } else {
        None
    };

    if let Some(key) = &cache_key
        && let Some(cached) = global_query_cache().get(key)
    {
        return Ok(SpecCompileResult {
            compiled: cached,
            warnings: Vec::new(),
        });
    }

    let builder = build_query(spec)?;
    let warnings = cypher_path_mode_warnings(dialect, builder.get_path_mode().as_deref());
    let (mut arena, root) = builder
        .try_build()
        .map_err(|error| map_compile_error(error).with_warnings(warnings.clone()))?;

    if options.optimize {
        let level = OptimizationLevel::from_str_opt(&options.optimization_level);
        let optimizer = AstOptimizer::new(level);
        optimizer
            .optimize(&mut arena, root)
            .map_err(|error| map_compile_error(error).with_warnings(warnings.clone()))?;
    }

    let compiled = match dialect.to_lowercase().as_str() {
        "cypher" | "opencypher" | "neo4j" | "memgraph" | "falkordb" | "falkor" => {
            let mut emitter = CypherEmitter::new();
            emitter
                .visit_query(&arena, root)
                .map_err(|error| map_compile_error(error).with_warnings(warnings.clone()))?
        }
        "sql_pgq" | "pgq" | "sql" => {
            let name = options
                .graph_name
                .clone()
                .unwrap_or_else(|| "graph_table".into());
            let mut emitter = SqlPgqEmitter::new(name);
            emitter
                .visit_query(&arena, root)
                .map_err(|error| map_compile_error(error).with_warnings(warnings.clone()))?
        }
        "duckpgq" | "duck_pgq" => {
            let name = options
                .graph_name
                .clone()
                .unwrap_or_else(|| "graph_table".into());
            let mut emitter = SqlPgqEmitter::new(name).for_duckpgq();
            emitter
                .visit_query(&arena, root)
                .map_err(|error| map_compile_error(error).with_warnings(warnings.clone()))?
        }
        "iso_gql" | "gql" => {
            let mut emitter = IsoGqlEmitter::new();
            emitter
                .visit_query(&arena, root)
                .map_err(|error| map_compile_error(error).with_warnings(warnings.clone()))?
        }
        "age" | "apache_age" | "postgres_age" => {
            let name = options
                .graph_name
                .clone()
                .unwrap_or_else(|| DEFAULT_AGE_GRAPH.into());
            let mut emitter = AgeEmitter::new(name);
            emitter
                .visit_query(&arena, root)
                .map_err(|error| map_compile_error(error).with_warnings(warnings.clone()))?
        }
        other => {
            return Err(SpecError::new(
                ErrorCode::UnsupportedDialect,
                None,
                format!(
                    "Unsupported query dialect: '{other}'. Choose from 'cypher', 'sql_pgq', 'iso_gql', or 'apache_age'."
                ),
            ));
        }
    };

    if let Some(key) = cache_key {
        global_query_cache().put(key, compiled.clone());
    }

    Ok(SpecCompileResult { compiled, warnings })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn compile_cypher(spec: &Value) -> Result<SpecCompileResult, SpecError> {
        compile(
            spec,
            "cypher",
            &SpecCompileOptions {
                use_cache: false,
                ..SpecCompileOptions::default()
            },
        )
    }

    fn base_spec() -> Value {
        json!({
            "spec_version": 1,
            "matches": [{"paths": [[["node", "p", ["Person"]]]]}],
            "projections": [["field", "p", "name", "n"]]
        })
    }

    #[test]
    fn label_predicate_compiles_in_where() {
        let spec = json!({
            "spec_version": 1,
            "matches": [{
                "paths": [[["node", "i", ["Node"]]]],
                "where": ["label", "i", ["TextNode"]]
            }],
            "projections": [["field", "i", "name", null]]
        });

        let cypher = compile_cypher(&spec).unwrap();
        assert_eq!(
            cypher.compiled.statement,
            "MATCH (i:Node) WHERE i:TextNode RETURN i.name"
        );

        let age = compile(
            &spec,
            "age",
            &SpecCompileOptions {
                use_cache: false,
                ..SpecCompileOptions::default()
            },
        )
        .unwrap();
        assert!(
            age.compiled
                .statement
                .contains("WHERE label(i) = 'TextNode' RETURN i.name")
        );

        let gql = compile(
            &spec,
            "iso_gql",
            &SpecCompileOptions {
                use_cache: false,
                ..SpecCompileOptions::default()
            },
        )
        .unwrap();
        assert_eq!(
            gql.compiled.statement,
            "MATCH (i:Node) WHERE i IS TextNode RETURN i.name"
        );

        let pgq_error = compile(
            &spec,
            "sql_pgq",
            &SpecCompileOptions {
                use_cache: false,
                ..SpecCompileOptions::default()
            },
        )
        .unwrap_err();
        assert_eq!(pgq_error.code(), ErrorCode::UnsupportedFeature);
    }

    #[test]
    fn label_predicate_multiple_labels_mean_all_of_them() {
        let spec = json!({
            "spec_version": 1,
            "matches": [{
                "paths": [[["node", "i", ["Node"]]]],
                "where": ["label", "i", ["A", "B"]]
            }],
            "projections": [["field", "i", "name", null]]
        });
        let cypher = compile_cypher(&spec).unwrap();
        assert_eq!(
            cypher.compiled.statement,
            "MATCH (i:Node) WHERE i:A:B RETURN i.name"
        );

        let age = compile(
            &spec,
            "age",
            &SpecCompileOptions {
                use_cache: false,
                ..SpecCompileOptions::default()
            },
        )
        .unwrap();
        assert!(
            age.compiled
                .statement
                .contains("WHERE label(i) = 'A' AND label(i) = 'B'")
        );

        let gql = compile(
            &spec,
            "iso_gql",
            &SpecCompileOptions {
                use_cache: false,
                ..SpecCompileOptions::default()
            },
        )
        .unwrap();
        assert_eq!(
            gql.compiled.statement,
            "MATCH (i:Node) WHERE i IS A&B RETURN i.name"
        );
    }

    #[test]
    fn label_predicate_rejects_malformed_labels() {
        let cases = [
            (json!(["label", "i", []]), "/matches/0/where/2"),
            (json!(["label", "i", [""]]), "/matches/0/where/2/0"),
            (json!(["label", "i", ["A", 1]]), "/matches/0/where/2/1"),
            (json!(["label", "i", "TextNode"]), "/matches/0/where/2"),
            (json!(["label", "i"]), "/matches/0/where"),
            (json!(["label", "", ["TextNode"]]), "/matches/0/where/1"),
        ];
        for (where_expression, expected_path) in cases {
            let spec = json!({
                "spec_version": 1,
                "matches": [{
                    "paths": [[["node", "i", ["Node"]]]],
                    "where": where_expression
                }],
                "projections": [["field", "i", "name", null]]
            });
            let error = compile_cypher(&spec).unwrap_err();
            assert_eq!(error.code(), ErrorCode::InvalidSpec);
            assert_eq!(error.path(), Some(expected_path));
        }
    }

    #[test]
    fn label_predicate_requires_a_bound_variable() {
        let spec = json!({
            "spec_version": 1,
            "matches": [{
                "paths": [[["node", "i", ["Node"]]]],
                "where": ["label", "unknown", ["TextNode"]]
            }],
            "projections": [["field", "i", "name", null]]
        });
        let error = compile_cypher(&spec).unwrap_err();
        assert_eq!(error.code(), ErrorCode::InvalidSpec);
        assert_eq!(error.path(), Some("/matches/0/where/1"));
        assert!(
            error
                .message()
                .contains("variable 'unknown' is not in scope")
        );
    }

    #[test]
    fn label_predicate_round_trips_through_spec_export() {
        let spec = json!({
            "spec_version": 1,
            "matches": [{
                "paths": [[["node", "i", ["Node"]]]],
                "where": ["label", "i", ["A", "B"]]
            }],
            "projections": [["field", "i", "name", null]]
        });
        let builder = build_query(&spec).unwrap();
        let (arena, root) = builder.try_build().unwrap();
        let exported = crate::spec_export::spec_from_ast(&arena, root).unwrap();
        let exported_json = exported.to_json_value();
        assert_eq!(
            exported_json["matches"][0]["where"],
            json!(["label", "i", ["A", "B"]])
        );
    }

    #[test]
    fn non_object_document_reports_the_root() {
        let error = build_query(&json!(["spec_version", 1])).unwrap_err();
        assert_eq!(error.code(), ErrorCode::InvalidSpec);
        assert_eq!(error.path(), Some(""));
        assert!(
            error
                .message()
                .contains("expected a query spec object (dict), got array")
        );
    }

    #[test]
    fn nested_error_keeps_the_json_pointer() {
        let mut spec = base_spec();
        spec["matches"][0]["paths"][0][0][0] = json!("vertex");
        let error = compile_cypher(&spec).unwrap_err();
        assert_eq!(error.code(), ErrorCode::InvalidSpec);
        assert_eq!(error.path(), Some("/matches/0/paths/0/0/0"));
        assert!(error.message().contains("Unknown path step tag: 'vertex'"));
    }

    #[test]
    fn non_string_key_is_reported_at_the_containing_object() {
        let mut spec = serde_json::Map::new();
        spec.insert("spec_version".to_string(), json!(1));
        spec.insert(encode_non_string_key("number"), json!("x"));
        let error = build_query(&Value::Object(spec)).unwrap_err();
        assert_eq!(error.path(), Some(""));
        assert!(error.message().contains("object keys must be strings"));
        assert!(error.message().contains("got number"));
    }

    #[test]
    fn unsupported_dialect_has_no_path() {
        let error = compile(
            &base_spec(),
            "sparql",
            &SpecCompileOptions {
                use_cache: false,
                ..SpecCompileOptions::default()
            },
        )
        .unwrap_err();
        assert_eq!(error.code(), ErrorCode::UnsupportedDialect);
        assert_eq!(error.path(), None);
        assert!(
            error
                .message()
                .contains("Unsupported query dialect: 'sparql'")
        );
    }

    #[test]
    fn cypher_path_mode_warning_is_returned() {
        let spec = json!({
            "spec_version": 1,
            "matches": [{
                "path_mode": "trail",
                "paths": [[["node", "p", ["Person"]]]]
            }],
            "projections": [["field", "p", "name", null]]
        });
        let result = compile_cypher(&spec).unwrap();
        assert_eq!(result.compiled.statement, "MATCH (p:Person) RETURN p.name");
        assert_eq!(result.warnings.len(), 1);
        assert!(
            result.warnings[0].contains("does not support explicit path search modes"),
            "{:?}",
            result.warnings
        );
    }

    fn compile_dialect(spec: &Value, dialect: &str) -> Result<SpecCompileResult, SpecError> {
        compile(
            spec,
            dialect,
            &SpecCompileOptions {
                use_cache: false,
                ..SpecCompileOptions::default()
            },
        )
    }

    #[test]
    fn pagination_parameters_compile_in_every_dialect() {
        let spec = json!({
            "spec_version": 1,
            "matches": [{"paths": [[["node", "p", ["Person"]]]]}],
            "projections": [["field", "p", "name", null]],
            "skip": ["param", "s"],
            "limit": ["param", "$l"]
        });
        let cases = [
            ("cypher", "MATCH (p:Person) RETURN p.name SKIP $s LIMIT $l"),
            (
                "age",
                "SELECT * FROM cypher('age_graph', $$ MATCH (p:Person) RETURN p.name SKIP $s LIMIT $l $$, %s) AS (name agtype)",
            ),
            (
                "iso_gql",
                "MATCH (p:Person) RETURN p.name OFFSET $s LIMIT $l",
            ),
            (
                "sql_pgq",
                "SELECT * FROM GRAPH_TABLE (graph_table MATCH (p IS \"Person\") COLUMNS (p.name)) LIMIT $l OFFSET $s",
            ),
            (
                "duckpgq",
                "SELECT * FROM GRAPH_TABLE (graph_table MATCH (p IS \"Person\") COLUMNS (p.name)) LIMIT $l OFFSET $s",
            ),
        ];
        for (dialect, statement) in cases {
            let compiled = compile_dialect(&spec, dialect).unwrap();
            assert_eq!(compiled.compiled.statement, statement, "{dialect}");
            assert!(compiled.compiled.parameters.is_empty(), "{dialect}");
        }
    }

    #[test]
    fn pagination_parameters_compile_inside_with() {
        let spec = json!({
            "spec_version": 1,
            "matches": [{"paths": [[["node", "p", ["Person"]]]]}],
            "with_clauses": [{
                "projections": [["field", "p", "name", "n"]],
                "skip": ["param", "ws"],
                "limit": ["param", "wl"]
            }],
            "projections": [["expr", ["ident", "n"], null]],
            "skip": ["param", "s"],
            "limit": ["param", "l"]
        });
        let compiled = compile_cypher(&spec).unwrap();
        assert_eq!(
            compiled.compiled.statement,
            "MATCH (p:Person) WITH p.name AS n SKIP $ws LIMIT $wl RETURN n SKIP $s LIMIT $l"
        );
    }

    #[test]
    fn malformed_pagination_parameter_reports_its_pointer() {
        let base = base_spec();
        let cases = [
            (json!(["param"]), "/limit"),
            (json!(["param", 1]), "/limit/1"),
            (json!(["param", ""]), "/limit/1"),
            (json!(["param", "l", null]), "/limit/2"),
            (json!(["lit", "l"]), "/limit/0"),
        ];
        for (value, pointer) in cases {
            let mut spec = base.clone();
            spec["limit"] = value.clone();
            let error = compile_cypher(&spec).unwrap_err();
            assert_eq!(error.code(), ErrorCode::InvalidSpec, "{value}");
            assert_eq!(error.path(), Some(pointer), "{value}");
        }

        let spec = json!({
            "spec_version": 1,
            "matches": [{"paths": [[["node", "p", ["Person"]]]]}],
            "with_clauses": [{
                "projections": [["field", "p", "name", "n"]],
                "skip": ["param"]
            }],
            "projections": [["expr", ["ident", "n"], null]]
        });
        let error = compile_cypher(&spec).unwrap_err();
        assert_eq!(error.path(), Some("/with_clauses/0/skip"));
    }

    #[test]
    fn depth_limit_is_enforced_for_expression_trees() {
        let mut expression = json!(["lit", 1]);
        for _ in 0..260 {
            expression = json!(["bin", "+", expression, ["lit", 1]]);
        }
        let spec = json!({
            "spec_version": 1,
            "matches": [{"paths": [[["node", "p", ["Person"]]]], "where": [expression]}],
            "projections": [["field", "p", "name", null]]
        });
        let error = compile_cypher(&spec).unwrap_err();
        assert!(
            error
                .message()
                .contains("Maximum AST expression recursion depth exceeded")
        );
    }

    #[test]
    fn distinct_return_order_by_repeats_a_projected_field() {
        let spec = json!({
            "spec_version": 1,
            "matches": [{"paths": [[["node", "p", ["Person"]]]]}],
            "projections": [["field", "p", "city", null]],
            "distinct": true,
            "order_by": [["p", "city", true]]
        });
        let result = compile_cypher(&spec).unwrap();
        assert_eq!(
            result.compiled.statement,
            "MATCH (p:Person) RETURN DISTINCT p.city ORDER BY p.city ASC"
        );
    }

    #[test]
    fn distinct_return_order_by_accepts_a_projected_subexpression() {
        let spec = json!({
            "spec_version": 1,
            "matches": [{"paths": [[["node", "p", ["Person"]]]]}],
            "projections": [["field", "p", "city", null]],
            "distinct": true,
            "order_by": [[["bin", "+", ["prop", "p", "city"], ["lit", 1]], true]]
        });
        let result = compile_cypher(&spec).unwrap();
        assert_eq!(
            result.compiled.statement,
            "MATCH (p:Person) RETURN DISTINCT p.city ORDER BY p.city + $p0 ASC"
        );
    }

    #[test]
    fn distinct_return_order_by_rejects_an_unprojected_field() {
        let spec = json!({
            "spec_version": 1,
            "matches": [{"paths": [[["node", "p", ["Person"]]]]}],
            "projections": [["field", "p", "city", null]],
            "distinct": true,
            "order_by": [["p", "age", true]]
        });
        let error = compile_cypher(&spec).unwrap_err();
        assert_eq!(error.code(), ErrorCode::InvalidSpec);
        assert_eq!(error.path(), Some("/order_by/0/0"));
    }

    #[test]
    fn aggregating_return_order_by_repeats_a_projected_field() {
        let spec = json!({
            "spec_version": 1,
            "matches": [{"paths": [[["node", "p", ["Person"]]]]}],
            "projections": [
                ["field", "p", "city", null],
                ["agg", "p", "*", "count", "c"]
            ],
            "order_by": [["p", "city", true]]
        });
        let result = compile_cypher(&spec).unwrap();
        assert_eq!(
            result.compiled.statement,
            "MATCH (p:Person) RETURN p.city, COUNT(p) AS c ORDER BY p.city ASC"
        );
    }

    #[test]
    fn aggregating_return_order_by_repeats_a_projected_aggregation() {
        let spec = json!({
            "spec_version": 1,
            "matches": [{"paths": [[["node", "p", ["Person"]]]]}],
            "projections": [["agg", "p", "*", "count", "c"]],
            "order_by": [[["fn", "count", [["ident", "p"]]], true]]
        });
        let result = compile_cypher(&spec).unwrap();
        assert_eq!(
            result.compiled.statement,
            "MATCH (p:Person) RETURN COUNT(p) AS c ORDER BY count(p) ASC"
        );
    }

    #[test]
    fn distinct_with_order_by_and_where_repeat_a_projected_field() {
        let order_spec = json!({
            "spec_version": 1,
            "matches": [{"paths": [[["node", "p", ["Person"]]]]}],
            "with_clauses": [{
                "distinct": true,
                "projections": [["field", "p", "city", "city"]],
                "order_by": [["p", "city", true]]
            }],
            "projections": [["expr", ["ident", "city"], null]]
        });
        let result = compile_cypher(&order_spec).unwrap();
        assert_eq!(
            result.compiled.statement,
            "MATCH (p:Person) WITH DISTINCT p.city AS city ORDER BY p.city ASC RETURN city"
        );

        let where_spec = json!({
            "spec_version": 1,
            "matches": [{"paths": [[["node", "p", ["Person"]]]]}],
            "with_clauses": [{
                "distinct": true,
                "projections": [["field", "p", "city", "city"]],
                "where": [["unary", "is_not_null", ["prop", "p", "city"]]]
            }],
            "projections": [["expr", ["ident", "city"], null]]
        });
        let result = compile_cypher(&where_spec).unwrap();
        assert_eq!(
            result.compiled.statement,
            "MATCH (p:Person) WITH DISTINCT p.city AS city WHERE p.city IS NOT NULL RETURN city"
        );
    }

    #[test]
    fn distinct_with_order_by_and_where_reject_an_unprojected_field() {
        let order_spec = json!({
            "spec_version": 1,
            "matches": [{"paths": [[["node", "p", ["Person"]]]]}],
            "with_clauses": [{
                "distinct": true,
                "projections": [["field", "p", "city", "city"]],
                "order_by": [["p", "age", true]]
            }],
            "projections": [["expr", ["ident", "city"], null]]
        });
        let error = compile_cypher(&order_spec).unwrap_err();
        assert_eq!(error.code(), ErrorCode::InvalidSpec);
        assert_eq!(error.path(), Some("/with_clauses/0/order_by/0/0"));

        let where_spec = json!({
            "spec_version": 1,
            "matches": [{"paths": [[["node", "p", ["Person"]]]]}],
            "with_clauses": [{
                "distinct": true,
                "projections": [["field", "p", "city", "city"]],
                "where": [["unary", "is_not_null", ["prop", "p", "age"]]]
            }],
            "projections": [["expr", ["ident", "city"], null]]
        });
        let error = compile_cypher(&where_spec).unwrap_err();
        assert_eq!(error.code(), ErrorCode::InvalidSpec);
        assert_eq!(error.path(), Some("/with_clauses/0/where/0/2/1"));
    }

    #[test]
    fn depth_limit_survives_subquery_reentry() {
        // The test thread's default stack is smaller than the recursion budget;
        // run on a normal application-sized stack, like the Python caller does.
        std::thread::Builder::new()
            .stack_size(16 * 1024 * 1024)
            .spawn(|| {
                let mut nested = json!({
                    "spec_version": 1,
                    "matches": [{"paths": [[["node", "n", ["Person"]]]]}]
                });
                for _ in 0..300 {
                    nested = json!({
                        "spec_version": 1,
                        "matches": [{
                            "paths": [[["node", "n", ["Person"]]]],
                            "where": [["exists", nested]]
                        }]
                    });
                }
                let spec = json!({
                    "spec_version": 1,
                    "matches": [{
                        "paths": [[["node", "n", ["Person"]]]],
                        "where": [["exists", nested]]
                    }]
                });
                let error = compile_cypher(&spec).unwrap_err();
                assert!(
                    error
                        .message()
                        .contains("Maximum AST expression recursion depth exceeded")
                );
            })
            .expect("spawn deep-spec test")
            .join()
            .expect("deep-spec test");
    }
}
