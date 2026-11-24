//! Query Spec v1 serialisation: native AST to a JSON-shaped specification value.
//!
//! The fluent builder is the source of truth for a query AST. This module walks
//! the finished arena and produces the equivalent Query Spec v1 document, so
//! SDKs and generators can reuse one native serialiser instead of reimplementing
//! the mapping. Objects are represented as ordered key/value pairs because a
//! pattern property map keeps its insertion order, which in turn decides the
//! parameter names the emitters assign.

use crate::ast::{
    AggregationFunc, AstNode, BinaryOp, Direction, ExecutionMode, LiteralValue, NodeHandle,
    PaginationValue, PathMode, ProjectionItem, QueryAstArena, UnaryOp,
};
use std::fmt;

/// The major Query Spec version produced by this serialiser.
pub const SPEC_VERSION: i64 = 1;

/// One JSON-shaped Query Spec v1 value.
///
/// Object members keep their insertion order (a JSON object is ordered for the
/// purpose of parameter numbering), which a `serde_json::Map` backed by a
/// sorted map would not preserve.
#[derive(Debug, Clone, PartialEq)]
pub enum SpecValue {
    /// JSON `null`.
    Null,
    /// JSON boolean.
    Bool(bool),
    /// JSON integer.
    Int(i64),
    /// JSON number with a fractional part.
    Float(f64),
    /// JSON string.
    String(String),
    /// JSON array.
    Array(Vec<SpecValue>),
    /// JSON object with ordered members.
    Object(Vec<(String, SpecValue)>),
}

impl SpecValue {
    /// Converts this value into a `serde_json::Value`.
    ///
    /// Object member order is not preserved by `serde_json` unless its
    /// `preserve_order` feature is enabled. Use [`Self::to_json_string`] when
    /// the serialised text must keep pattern property order.
    pub fn to_json_value(&self) -> serde_json::Value {
        match self {
            Self::Null => serde_json::Value::Null,
            Self::Bool(value) => serde_json::Value::Bool(*value),
            Self::Int(value) => serde_json::Value::Number((*value).into()),
            Self::Float(value) => serde_json::Number::from_f64(*value)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            Self::String(value) => serde_json::Value::String(value.clone()),
            Self::Array(items) => {
                serde_json::Value::Array(items.iter().map(Self::to_json_value).collect())
            }
            Self::Object(members) => serde_json::Value::Object(
                members
                    .iter()
                    .map(|(key, value)| (key.clone(), value.to_json_value()))
                    .collect(),
            ),
        }
    }

    /// Serialises this value to JSON text, preserving object member order.
    pub fn to_json_string(&self) -> serde_json::Result<String> {
        serde_json::to_string(self)
    }
}

impl serde::Serialize for SpecValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;

        match self {
            Self::Null => serializer.serialize_none(),
            Self::Bool(value) => serializer.serialize_bool(*value),
            Self::Int(value) => serializer.serialize_i64(*value),
            Self::Float(value) => serializer.serialize_f64(*value),
            Self::String(value) => serializer.serialize_str(value),
            Self::Array(items) => items.serialize(serializer),
            Self::Object(members) => {
                let mut map = serializer.serialize_map(Some(members.len()))?;
                for (key, value) in members {
                    map.serialize_entry(key, value)?;
                }
                map.end()
            }
        }
    }
}

/// Why a native AST could not be serialised to Query Spec v1.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpecExportError {
    /// The fluent query contains a construct that Query Spec v1 cannot express
    /// without changing its meaning.
    Unsupported {
        /// Human-readable name of the construct.
        construct: String,
    },
    /// The AST is structurally invalid or empty.
    Invalid(String),
}

impl fmt::Display for SpecExportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unsupported { construct } => write!(
                f,
                "Query Spec v1 cannot represent {construct} without changing the query"
            ),
            Self::Invalid(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for SpecExportError {}

/// Result alias for Query Spec serialisation.
pub type SpecExportResult<T> = Result<T, SpecExportError>;

fn unsupported(construct: impl Into<String>) -> SpecExportError {
    SpecExportError::Unsupported {
        construct: construct.into(),
    }
}

fn invalid(message: impl Into<String>) -> SpecExportError {
    SpecExportError::Invalid(message.into())
}

fn node(arena: &QueryAstArena, handle: NodeHandle) -> SpecExportResult<&AstNode> {
    arena
        .get(handle)
        .map_err(|error| invalid(error.to_string()))
}

fn text(value: impl Into<String>) -> SpecValue {
    SpecValue::String(value.into())
}

fn optional_text(value: &Option<String>) -> SpecValue {
    match value {
        Some(value) => text(value),
        None => SpecValue::Null,
    }
}

fn optional_u32(value: Option<u32>) -> SpecValue {
    match value {
        Some(value) => SpecValue::Int(i64::from(value)),
        None => SpecValue::Null,
    }
}

fn binary_op_name(op: BinaryOp) -> &'static str {
    match op {
        BinaryOp::Eq => "eq",
        BinaryOp::Neq => "neq",
        BinaryOp::Lt => "lt",
        BinaryOp::Lte => "lte",
        BinaryOp::Gt => "gt",
        BinaryOp::Gte => "gte",
        BinaryOp::In => "in",
        BinaryOp::NotIn => "not_in",
        BinaryOp::Contains => "contains",
        BinaryOp::StartsWith => "starts_with",
        BinaryOp::EndsWith => "ends_with",
        BinaryOp::RegexMatch => "regex",
        BinaryOp::And => "and",
        BinaryOp::Or => "or",
        BinaryOp::Xor => "xor",
        BinaryOp::Add => "add",
        BinaryOp::Concat => "concat",
        BinaryOp::Sub => "sub",
        BinaryOp::Mul => "mul",
        BinaryOp::Div => "div",
        BinaryOp::Mod => "mod",
    }
}

fn unary_op_name(op: UnaryOp) -> &'static str {
    match op {
        UnaryOp::Not => "not",
        UnaryOp::Neg => "-",
        UnaryOp::IsNull => "is_null",
        UnaryOp::IsNotNull => "is_not_null",
    }
}

fn direction_name(direction: Direction) -> &'static str {
    match direction {
        Direction::Outgoing => "outgoing",
        Direction::Incoming => "incoming",
        Direction::Undirected => "undirected",
    }
}

fn path_mode_name(mode: PathMode) -> &'static str {
    match mode {
        PathMode::None => "none",
        PathMode::Walk => "walk",
        PathMode::Trail => "trail",
        PathMode::Simple => "simple",
        PathMode::Acyclic => "acyclic",
    }
}

fn aggregation_name(func: AggregationFunc) -> &'static str {
    match func {
        AggregationFunc::Count => "count",
        AggregationFunc::CountDistinct => "count_distinct",
        AggregationFunc::Sum => "sum",
        AggregationFunc::Avg => "avg",
        AggregationFunc::Min => "min",
        AggregationFunc::Max => "max",
        AggregationFunc::Collect => "collect",
    }
}

fn execution_mode_name(mode: ExecutionMode) -> &'static str {
    mode.as_str()
}

fn literal_json_value(value: &LiteralValue) -> SpecExportResult<SpecValue> {
    match value {
        LiteralValue::Null => Ok(SpecValue::Null),
        LiteralValue::Bool(value) => Ok(SpecValue::Bool(*value)),
        LiteralValue::Int64(value) => Ok(SpecValue::Int(*value)),
        LiteralValue::Float64(value) => Ok(SpecValue::Float(*value)),
        LiteralValue::String(value) => Ok(text(value)),
        LiteralValue::ParameterRef(_) => Err(unsupported(
            "a parameter reference inside a literal list or object",
        )),
        LiteralValue::List(items) => Ok(SpecValue::Array(
            items
                .iter()
                .map(literal_json_value)
                .collect::<SpecExportResult<Vec<_>>>()?,
        )),
        LiteralValue::Map(entries) => Ok(SpecValue::Object(
            entries
                .iter()
                .map(|(key, value)| Ok((key.clone(), literal_json_value(value)?)))
                .collect::<SpecExportResult<Vec<_>>>()?,
        )),
    }
}

fn export_literal(value: &LiteralValue) -> SpecExportResult<SpecValue> {
    match value {
        LiteralValue::ParameterRef(name) => Ok(SpecValue::Array(vec![text("param"), text(name)])),
        other => Ok(SpecValue::Array(vec![
            text("lit"),
            literal_json_value(other)?,
        ])),
    }
}

/// Serialises a finished query arena and root statement into a Query Spec value.
pub fn spec_from_ast(arena: &QueryAstArena, root: NodeHandle) -> SpecExportResult<SpecValue> {
    let root_node = node(arena, root)?;
    let AstNode::QueryStatement {
        execution_mode,
        load_csv,
        unwinds,
        matches,
        linear_clauses,
        with_clauses,
        mutations,
        return_clause,
    } = root_node
    else {
        return Err(match root_node {
            AstNode::ProcedureCall { .. } => {
                unsupported("a procedure call (CALL ...), including vector and full-text search")
            }
            _ => unsupported("a query root that is not a complete query statement"),
        });
    };

    let mut members = vec![("spec_version".to_string(), SpecValue::Int(SPEC_VERSION))];
    let mut produced = false;

    if let Some(handle) = load_csv {
        members.push(("load_csv".to_string(), export_load_csv(arena, *handle)?));
        produced = true;
    }
    if !unwinds.is_empty() {
        members.push((
            "unwinds".to_string(),
            SpecValue::Array(
                unwinds
                    .iter()
                    .map(|handle| export_unwind(arena, *handle))
                    .collect::<SpecExportResult<Vec<_>>>()?,
            ),
        ));
        produced = true;
    }
    if !matches.is_empty() {
        members.push((
            "matches".to_string(),
            SpecValue::Array(
                matches
                    .iter()
                    .map(|handle| export_match(arena, *handle))
                    .collect::<SpecExportResult<Vec<_>>>()?,
            ),
        ));
        produced = true;
    }
    if !mutations.is_empty() {
        let mut entries = Vec::new();
        for &handle in mutations {
            entries.extend(export_mutation(arena, handle)?);
        }
        members.push(("mutations".to_string(), SpecValue::Array(entries)));
        produced = true;
    }
    if !with_clauses.is_empty() {
        members.push((
            "with_clauses".to_string(),
            SpecValue::Array(
                with_clauses
                    .iter()
                    .map(|handle| export_with(arena, *handle))
                    .collect::<SpecExportResult<Vec<_>>>()?,
            ),
        ));
        produced = true;
    }
    if let Some(handle) = return_clause {
        let mut return_members = export_return_members(arena, *handle)?;
        members.append(&mut return_members);
        produced = true;
    }
    if !linear_clauses.is_empty() {
        members.push((
            "linear_clauses".to_string(),
            SpecValue::Array(
                linear_clauses
                    .iter()
                    .map(|handle| export_linear(arena, *handle))
                    .collect::<SpecExportResult<Vec<_>>>()?,
            ),
        ));
        produced = true;
    }
    if *execution_mode != ExecutionMode::Normal {
        members.push((
            "execution_mode".to_string(),
            text(execution_mode_name(*execution_mode)),
        ));
    }

    if !produced {
        return Err(invalid(
            "the query has no clauses to serialise; build a MATCH, mutation, WITH, projection, LOAD CSV, UNWIND or linear clause first",
        ));
    }

    Ok(SpecValue::Object(members))
}

fn export_load_csv(arena: &QueryAstArena, handle: NodeHandle) -> SpecExportResult<SpecValue> {
    let AstNode::LoadCsvClause {
        url,
        with_headers,
        alias,
    } = node(arena, handle)?
    else {
        return Err(unsupported("a LOAD CSV clause that is not a load-csv node"));
    };
    let url = match node(arena, *url)? {
        AstNode::Literal(LiteralValue::String(value)) => value.clone(),
        _ => {
            return Err(unsupported("LOAD CSV with a non-string URL expression"));
        }
    };
    Ok(SpecValue::Array(vec![
        text(url),
        SpecValue::Bool(*with_headers),
        text(alias),
    ]))
}

fn export_unwind(arena: &QueryAstArena, handle: NodeHandle) -> SpecExportResult<SpecValue> {
    let AstNode::UnwindClause { expression, alias } = node(arena, handle)? else {
        return Err(unsupported("an UNWIND clause that is not an unwind node"));
    };
    let name = match node(arena, *expression)? {
        AstNode::Parameter(name) => name.clone(),
        AstNode::Literal(LiteralValue::ParameterRef(name)) => name.clone(),
        _ => {
            return Err(unsupported("UNWIND over a non-parameter expression"));
        }
    };
    Ok(SpecValue::Array(vec![text(name), text(alias)]))
}

fn path_metadata(
    arena: &QueryAstArena,
    handle: NodeHandle,
) -> SpecExportResult<(PathMode, Option<String>)> {
    match node(arena, handle)? {
        AstNode::PathChain {
            path_mode,
            path_variable,
            ..
        } => Ok((*path_mode, path_variable.clone())),
        AstNode::NodePattern { .. } => Ok((PathMode::None, None)),
        _ => Err(unsupported("a MATCH path that is not a node or path chain")),
    }
}

fn export_match(arena: &QueryAstArena, handle: NodeHandle) -> SpecExportResult<SpecValue> {
    let AstNode::MatchClause {
        optional,
        paths,
        where_clause,
    } = node(arena, handle)?
    else {
        return Err(unsupported("a MATCH clause that is not a match node"));
    };

    let mut mode: Option<PathMode> = None;
    let mut variable: Option<String> = None;
    for &path in paths {
        let (path_mode, path_variable) = path_metadata(arena, path)?;
        match (&mode, &variable) {
            (None, _) => {
                mode = Some(path_mode);
                variable = path_variable;
            }
            (Some(expected_mode), expected_variable)
                if *expected_mode != path_mode || *expected_variable != path_variable =>
            {
                return Err(unsupported(
                    "a MATCH clause whose paths use different path modes or path variables",
                ));
            }
            _ => {}
        }
    }

    let mut members = vec![(
        "paths".to_string(),
        SpecValue::Array(
            paths
                .iter()
                .map(|path| export_path(arena, *path))
                .collect::<SpecExportResult<Vec<_>>>()?,
        ),
    )];
    if *optional {
        members.push(("optional".to_string(), SpecValue::Bool(true)));
    }
    if let Some(path_mode) = mode
        && path_mode != PathMode::None
    {
        members.push(("path_mode".to_string(), text(path_mode_name(path_mode))));
    }
    if let Some(path_variable) = variable {
        members.push(("path_variable".to_string(), text(path_variable)));
    }
    if let Some(where_handle) = where_clause {
        members.push(("where".to_string(), export_where(arena, *where_handle)?));
    }
    Ok(SpecValue::Object(members))
}

fn export_where(arena: &QueryAstArena, handle: NodeHandle) -> SpecExportResult<SpecValue> {
    match node(arena, handle)? {
        AstNode::WhereClause { root_predicate } => export_expression(arena, *root_predicate),
        _ => export_expression(arena, handle),
    }
}

fn export_path(arena: &QueryAstArena, handle: NodeHandle) -> SpecExportResult<SpecValue> {
    match node(arena, handle)? {
        AstNode::NodePattern { .. } => Ok(SpecValue::Array(vec![export_node_step(arena, handle)?])),
        AstNode::PathChain {
            start_node, edges, ..
        } => {
            let mut steps = vec![export_node_step(arena, *start_node)?];
            for &edge in edges {
                steps.push(export_edge_step(arena, edge)?);
                let AstNode::EdgePattern { target_node, .. } = node(arena, edge)? else {
                    return Err(unsupported("a path edge that is not an edge pattern"));
                };
                steps.push(export_node_step(arena, *target_node)?);
            }
            Ok(SpecValue::Array(steps))
        }
        _ => Err(unsupported("a path that is not a node or path chain")),
    }
}

fn export_node_step(arena: &QueryAstArena, handle: NodeHandle) -> SpecExportResult<SpecValue> {
    let AstNode::NodePattern {
        variable,
        label_expression,
        predicates,
    } = node(arena, handle)?
    else {
        return Err(unsupported("a node step that is not a node pattern"));
    };
    let labels = match label_expression {
        Some(label_expression) => label_expression
            .to_conjunction_labels()
            .ok_or_else(|| unsupported("a node label expression using OR or NOT"))?,
        None => Vec::new(),
    };
    let properties = export_pattern_properties(arena, variable.as_deref(), predicates, "node")?;

    let mut items = vec![
        text("node"),
        optional_text(variable),
        SpecValue::Array(labels.into_iter().map(text).collect()),
    ];
    if !properties.is_empty() {
        items.push(SpecValue::Object(properties));
    }
    Ok(SpecValue::Array(items))
}

fn export_edge_step(arena: &QueryAstArena, handle: NodeHandle) -> SpecExportResult<SpecValue> {
    let AstNode::EdgePattern {
        variable,
        label_expression,
        direction,
        min_hops,
        max_hops,
        predicates,
        ..
    } = node(arena, handle)?
    else {
        return Err(unsupported("an edge step that is not an edge pattern"));
    };
    let types = match label_expression {
        Some(label_expression) => label_expression
            .to_disjunction_labels()
            .ok_or_else(|| unsupported("an edge type expression using AND or NOT"))?,
        None => Vec::new(),
    };
    let properties = export_pattern_properties(arena, variable.as_deref(), predicates, "edge")?;

    let mut items = vec![
        text("edge"),
        text(direction_name(*direction)),
        SpecValue::Array(types.into_iter().map(text).collect()),
    ];
    let has_hops = min_hops.is_some() || max_hops.is_some();
    if variable.is_some() || has_hops || !properties.is_empty() {
        items.push(optional_text(variable));
    }
    if has_hops || !properties.is_empty() {
        let spec_min = optional_u32(*min_hops);
        let spec_max = match (*min_hops, *max_hops) {
            (Some(_), None) => text("unbounded"),
            (_, Some(max)) => SpecValue::Int(i64::from(max)),
            (None, None) => SpecValue::Null,
        };
        items.push(spec_min);
        items.push(spec_max);
    }
    if !properties.is_empty() {
        items.push(SpecValue::Object(properties));
    }
    Ok(SpecValue::Array(items))
}

fn export_pattern_properties(
    arena: &QueryAstArena,
    variable: Option<&str>,
    predicates: &[NodeHandle],
    construct: &str,
) -> SpecExportResult<Vec<(String, SpecValue)>> {
    let mut properties = Vec::with_capacity(predicates.len());
    for &predicate in predicates {
        let AstNode::BinaryExpression {
            left,
            op: BinaryOp::Eq,
            right,
        } = node(arena, predicate)?
        else {
            return Err(unsupported(format!(
                "a {construct} pattern predicate that is not an equality property map entry"
            )));
        };
        match node(arena, *left)? {
            AstNode::PropertyAccess { target, property } => {
                let target_name = match node(arena, *target)? {
                    AstNode::Identifier(name) => Some(name.as_str()),
                    _ => None,
                };
                if target_name != variable {
                    return Err(unsupported(format!(
                        "a {construct} pattern property on a variable other than the pattern variable"
                    )));
                }
                properties.push((property.clone(), export_expression(arena, *right)?));
            }
            AstNode::Identifier(property) => {
                if variable.is_some() {
                    return Err(unsupported(format!(
                        "a {construct} pattern property without the pattern variable"
                    )));
                }
                properties.push((property.clone(), export_expression(arena, *right)?));
            }
            _ => {
                return Err(unsupported(format!(
                    "a {construct} pattern predicate that is not an equality property map entry"
                )));
            }
        }
    }
    Ok(properties)
}

fn export_projection_item(
    arena: &QueryAstArena,
    item: &ProjectionItem,
) -> SpecExportResult<SpecValue> {
    let alias = optional_text(&item.alias);
    if let Some(func) = item.aggregation {
        let func = aggregation_name(func);
        match node(arena, item.expression)? {
            AstNode::PropertyAccess { target, property } => {
                let variable = match node(arena, *target)? {
                    AstNode::Identifier(name) if !name.is_empty() => name.clone(),
                    _ => {
                        return Err(unsupported(
                            "an aggregation over a non-identifier expression",
                        ));
                    }
                };
                let property = if property == "*" || property.is_empty() {
                    "*".to_string()
                } else {
                    property.clone()
                };
                Ok(SpecValue::Array(vec![
                    text("agg"),
                    text(variable),
                    text(property),
                    text(func),
                    alias,
                ]))
            }
            AstNode::Identifier(name) if !name.is_empty() => Ok(SpecValue::Array(vec![
                text("agg"),
                text(name),
                text("*"),
                text(func),
                alias,
            ])),
            _ => Err(unsupported("an aggregation over a non-property expression")),
        }
    } else {
        match node(arena, item.expression)? {
            AstNode::PropertyAccess { target, property } if !property.is_empty() => {
                if let AstNode::Identifier(variable) = node(arena, *target)?
                    && !variable.is_empty()
                {
                    return Ok(SpecValue::Array(vec![
                        text("field"),
                        text(variable),
                        text(property),
                        alias,
                    ]));
                }
                Ok(SpecValue::Array(vec![
                    text("expr"),
                    export_expression(arena, item.expression)?,
                    alias,
                ]))
            }
            _ => Ok(SpecValue::Array(vec![
                text("expr"),
                export_expression(arena, item.expression)?,
                alias,
            ])),
        }
    }
}

fn export_projections(
    arena: &QueryAstArena,
    projections: &[ProjectionItem],
) -> SpecExportResult<SpecValue> {
    Ok(SpecValue::Array(
        projections
            .iter()
            .map(|item| export_projection_item(arena, item))
            .collect::<SpecExportResult<Vec<_>>>()?,
    ))
}

fn export_order_by(
    arena: &QueryAstArena,
    order_by: &[(NodeHandle, bool)],
) -> SpecExportResult<SpecValue> {
    Ok(SpecValue::Array(
        order_by
            .iter()
            .map(|(handle, ascending)| {
                Ok(SpecValue::Array(vec![
                    export_expression(arena, *handle)?,
                    SpecValue::Bool(*ascending),
                ]))
            })
            .collect::<SpecExportResult<Vec<_>>>()?,
    ))
}

fn push_common_projection_members(
    members: &mut Vec<(String, SpecValue)>,
    arena: &QueryAstArena,
    distinct: bool,
    order_by: &[(NodeHandle, bool)],
    skip: Option<PaginationValue>,
    limit: Option<PaginationValue>,
    where_clause: Option<NodeHandle>,
) -> SpecExportResult<()> {
    if distinct {
        members.push(("distinct".to_string(), SpecValue::Bool(true)));
    }
    if let Some(handle) = where_clause {
        members.push(("where".to_string(), export_where(arena, handle)?));
    }
    if !order_by.is_empty() {
        members.push(("order_by".to_string(), export_order_by(arena, order_by)?));
    }
    if let Some(skip) = skip {
        members.push(("skip".to_string(), export_pagination(&skip)));
    }
    if let Some(limit) = limit {
        members.push(("limit".to_string(), export_pagination(&limit)));
    }
    Ok(())
}

fn export_pagination(value: &PaginationValue) -> SpecValue {
    match value {
        PaginationValue::Count(count) => SpecValue::Int(*count as i64),
        PaginationValue::Parameter(name) => SpecValue::Array(vec![
            SpecValue::String("param".to_string()),
            SpecValue::String(name.clone()),
        ]),
    }
}

fn export_return_members(
    arena: &QueryAstArena,
    handle: NodeHandle,
) -> SpecExportResult<Vec<(String, SpecValue)>> {
    let AstNode::ReturnClause {
        distinct,
        projections,
        order_by,
        skip,
        limit,
    } = node(arena, handle)?
    else {
        return Err(unsupported("a RETURN clause that is not a return node"));
    };
    let mut members = vec![(
        "projections".to_string(),
        export_projections(arena, projections)?,
    )];
    push_common_projection_members(
        &mut members,
        arena,
        *distinct,
        order_by,
        skip.clone(),
        limit.clone(),
        None,
    )?;
    Ok(members)
}

fn export_with(arena: &QueryAstArena, handle: NodeHandle) -> SpecExportResult<SpecValue> {
    let AstNode::WithClause {
        distinct,
        projections,
        order_by,
        skip,
        limit,
        where_clause,
    } = node(arena, handle)?
    else {
        return Err(unsupported("a WITH clause that is not a with node"));
    };
    let mut members = vec![(
        "projections".to_string(),
        export_projections(arena, projections)?,
    )];
    push_common_projection_members(
        &mut members,
        arena,
        *distinct,
        order_by,
        skip.clone(),
        limit.clone(),
        *where_clause,
    )?;
    Ok(SpecValue::Object(members))
}

fn export_linear(arena: &QueryAstArena, handle: NodeHandle) -> SpecExportResult<SpecValue> {
    match node(arena, handle)? {
        AstNode::LetClause {
            variable,
            expression,
        } => Ok(SpecValue::Array(vec![
            text("let"),
            text(variable),
            export_expression(arena, *expression)?,
        ])),
        AstNode::FilterClause { predicate } => Ok(SpecValue::Array(vec![
            text("filter"),
            export_expression(arena, *predicate)?,
        ])),
        _ => Err(unsupported("a linear clause that is not LET or FILTER")),
    }
}

enum ExportSetMutation {
    Property(String, String, SpecValue),
    Merge(String, SpecValue),
}

fn export_set_clause_items(
    arena: &QueryAstArena,
    items: &[NodeHandle],
) -> SpecExportResult<Vec<ExportSetMutation>> {
    let mut entries = Vec::with_capacity(items.len());
    for &item in items {
        let AstNode::SetItem {
            target,
            value,
            is_merge,
        } = node(arena, item)?
        else {
            return Err(unsupported("a SET entry that is not a set item"));
        };
        if *is_merge {
            let AstNode::Identifier(variable) = node(arena, *target)? else {
                return Err(unsupported(
                    "a SET merge target that is not a bound variable",
                ));
            };
            entries.push(ExportSetMutation::Merge(
                variable.clone(),
                export_expression(arena, *value)?,
            ));
        } else {
            let AstNode::PropertyAccess { target, property } = node(arena, *target)? else {
                return Err(unsupported("a SET target that is not a property"));
            };
            let AstNode::Identifier(variable) = node(arena, *target)? else {
                return Err(unsupported("a SET target that is not a bound variable"));
            };
            entries.push(ExportSetMutation::Property(
                variable.clone(),
                property.clone(),
                export_expression(arena, *value)?,
            ));
        }
    }
    Ok(entries)
}

fn export_set_entries(
    arena: &QueryAstArena,
    items: &[NodeHandle],
) -> SpecExportResult<Vec<(String, String, SpecValue)>> {
    let mut entries = Vec::with_capacity(items.len());
    for &item in items {
        let AstNode::SetItem {
            target,
            value,
            is_merge,
        } = node(arena, item)?
        else {
            return Err(unsupported("a SET entry that is not a set item"));
        };
        if *is_merge {
            return Err(unsupported("a `SET variable += map` map merge"));
        }
        let AstNode::PropertyAccess { target, property } = node(arena, *target)? else {
            return Err(unsupported("a SET target that is not a property"));
        };
        let AstNode::Identifier(variable) = node(arena, *target)? else {
            return Err(unsupported("a SET target that is not a bound variable"));
        };
        entries.push((
            variable.clone(),
            property.clone(),
            export_expression(arena, *value)?,
        ));
    }
    Ok(entries)
}

fn export_mutation(arena: &QueryAstArena, handle: NodeHandle) -> SpecExportResult<Vec<SpecValue>> {
    match node(arena, handle)? {
        AstNode::CreateClause { paths } => {
            let paths = paths
                .iter()
                .map(|path| export_path(arena, *path))
                .collect::<SpecExportResult<Vec<_>>>()?;
            Ok(vec![SpecValue::Array(vec![
                text("create"),
                SpecValue::Array(paths),
            ])])
        }
        AstNode::MergeClause {
            path,
            on_create_set,
            on_match_set,
        } => {
            let path = export_path(arena, *path)?;
            let on_create = export_set_entries(arena, on_create_set)?;
            let on_match = export_set_entries(arena, on_match_set)?;
            let entries = |items: Vec<(String, String, SpecValue)>| {
                SpecValue::Array(
                    items
                        .into_iter()
                        .map(|(variable, property, value)| {
                            SpecValue::Array(vec![text(variable), text(property), value])
                        })
                        .collect(),
                )
            };
            Ok(vec![SpecValue::Array(vec![
                text("merge"),
                path,
                entries(on_create),
                entries(on_match),
            ])])
        }
        AstNode::SetClause { items } => Ok(export_set_clause_items(arena, items)?
            .into_iter()
            .map(|mutation| match mutation {
                ExportSetMutation::Property(variable, property, value) => {
                    SpecValue::Array(vec![text("set"), text(variable), text(property), value])
                }
                ExportSetMutation::Merge(variable, value) => {
                    SpecValue::Array(vec![text("set"), text(variable), value])
                }
            })
            .collect()),
        AstNode::DeleteClause { detach, targets } => {
            let mut names = Vec::with_capacity(targets.len());
            for &target in targets {
                match node(arena, target)? {
                    AstNode::Identifier(name) => names.push(text(name)),
                    _ => {
                        return Err(unsupported("a DELETE target that is not a bound variable"));
                    }
                }
            }
            Ok(vec![SpecValue::Array(vec![
                text("delete"),
                SpecValue::Bool(*detach),
                SpecValue::Array(names),
            ])])
        }
        AstNode::RemoveClause { items } => {
            let mut entries = Vec::with_capacity(items.len());
            for &item in items {
                let AstNode::PropertyAccess { target, property } = node(arena, item)? else {
                    return Err(unsupported("a REMOVE item that is not a property"));
                };
                let AstNode::Identifier(variable) = node(arena, *target)? else {
                    return Err(unsupported("a REMOVE target that is not a bound variable"));
                };
                entries.push(SpecValue::Array(vec![
                    text("remove"),
                    text(variable),
                    text(property),
                ]));
            }
            Ok(entries)
        }
        _ => Err(unsupported(
            "a mutation clause that is not CREATE, MERGE, SET, DELETE or REMOVE",
        )),
    }
}

fn export_expression(arena: &QueryAstArena, handle: NodeHandle) -> SpecExportResult<SpecValue> {
    match node(arena, handle)? {
        AstNode::BinaryExpression { left, op, right } => Ok(SpecValue::Array(vec![
            text("bin"),
            text(binary_op_name(*op)),
            export_expression(arena, *left)?,
            export_expression(arena, *right)?,
        ])),
        AstNode::UnaryExpression { op, operand } => Ok(SpecValue::Array(vec![
            text("unary"),
            text(unary_op_name(*op)),
            export_expression(arena, *operand)?,
        ])),
        AstNode::FunctionCall { name, arguments } => Ok(SpecValue::Array(vec![
            text("fn"),
            text(name),
            SpecValue::Array(
                arguments
                    .iter()
                    .map(|argument| export_expression(arena, *argument))
                    .collect::<SpecExportResult<Vec<_>>>()?,
            ),
        ])),
        AstNode::CaseExpression {
            operand,
            when_then_branches,
            else_branch,
        } => {
            let operand = match operand {
                Some(operand) => export_expression(arena, *operand)?,
                None => SpecValue::Null,
            };
            let branches = when_then_branches
                .iter()
                .map(|(when, then)| {
                    Ok(SpecValue::Array(vec![
                        export_expression(arena, *when)?,
                        export_expression(arena, *then)?,
                    ]))
                })
                .collect::<SpecExportResult<Vec<_>>>()?;
            let else_branch = match else_branch {
                Some(else_branch) => export_expression(arena, *else_branch)?,
                None => SpecValue::Null,
            };
            Ok(SpecValue::Array(vec![
                text("case"),
                operand,
                SpecValue::Array(branches),
                else_branch,
            ]))
        }
        AstNode::ListComprehension {
            variable,
            list_expression,
            where_filter,
            map_expression,
        } => {
            let where_filter = match where_filter {
                Some(where_filter) => export_expression(arena, *where_filter)?,
                None => SpecValue::Null,
            };
            let map_expression = match map_expression {
                Some(map_expression) => export_expression(arena, *map_expression)?,
                None => SpecValue::Null,
            };
            Ok(SpecValue::Array(vec![
                text("list_comp"),
                text(variable),
                export_expression(arena, *list_expression)?,
                where_filter,
                map_expression,
            ]))
        }
        AstNode::PatternComprehension {
            path,
            where_filter,
            projection,
        } => {
            let where_filter = match where_filter {
                Some(where_filter) => export_expression(arena, *where_filter)?,
                None => SpecValue::Null,
            };
            Ok(SpecValue::Array(vec![
                text("pattern_comp"),
                export_pattern_path(arena, *path)?,
                where_filter,
                export_expression(arena, *projection)?,
            ]))
        }
        AstNode::ExistsSubquery { subquery } => Ok(SpecValue::Array(vec![
            text("exists"),
            export_subquery_payload(arena, *subquery)?,
        ])),
        AstNode::CountSubquery { subquery } => Ok(SpecValue::Array(vec![
            text("count"),
            export_subquery_payload(arena, *subquery)?,
        ])),
        AstNode::ListLiteral(items) => Ok(SpecValue::Array(vec![
            text("list"),
            SpecValue::Array(
                items
                    .iter()
                    .map(|item| export_expression(arena, *item))
                    .collect::<SpecExportResult<Vec<_>>>()?,
            ),
        ])),
        AstNode::PropertyAccess { target, property } => {
            let AstNode::Identifier(variable) = node(arena, *target)? else {
                return Err(unsupported(
                    "property access on a target that is not an identifier",
                ));
            };
            if variable.is_empty() {
                return Err(unsupported("property access on an empty variable name"));
            }
            Ok(SpecValue::Array(vec![
                text("prop"),
                text(variable),
                text(property),
            ]))
        }
        AstNode::LabelPredicate { variable, labels } => Ok(SpecValue::Array(vec![
            text("label"),
            text(variable),
            SpecValue::Array(labels.iter().map(text).collect()),
        ])),
        AstNode::Literal(value) => export_literal(value),
        AstNode::Identifier(name) => Ok(SpecValue::Array(vec![text("ident"), text(name)])),
        AstNode::Parameter(name) => Ok(SpecValue::Array(vec![text("param"), text(name)])),
        AstNode::WhereClause { root_predicate } => export_expression(arena, *root_predicate),
        AstNode::NodePattern {
            variable: Some(variable),
            ..
        }
        | AstNode::EdgePattern {
            variable: Some(variable),
            ..
        } => Ok(SpecValue::Array(vec![text("ident"), text(variable)])),
        _ => Err(unsupported(
            "an AST expression that Query Spec v1 has no expression tag for",
        )),
    }
}

fn export_subquery_payload(
    arena: &QueryAstArena,
    handle: NodeHandle,
) -> SpecExportResult<SpecValue> {
    match node(arena, handle)? {
        AstNode::QueryStatement { .. } => spec_from_ast(arena, handle),
        AstNode::NodePattern { .. } | AstNode::PathChain { .. } => export_path(arena, handle),
        AstNode::MatchClause { .. } => Ok(SpecValue::Object(vec![
            ("spec_version".to_string(), SpecValue::Int(SPEC_VERSION)),
            (
                "matches".to_string(),
                SpecValue::Array(vec![export_match(arena, handle)?]),
            ),
        ])),
        _ => Err(unsupported(
            "an exists/count subquery that is not a query statement or path",
        )),
    }
}

fn export_pattern_path(arena: &QueryAstArena, handle: NodeHandle) -> SpecExportResult<SpecValue> {
    match node(arena, handle)? {
        AstNode::NodePattern { .. } | AstNode::PathChain { .. } => export_path(arena, handle),
        AstNode::MatchClause {
            optional: false,
            paths,
            where_clause: None,
        } if paths.len() == 1 => export_path(arena, paths[0]),
        AstNode::QueryStatement {
            load_csv: None,
            unwinds,
            matches,
            linear_clauses,
            with_clauses,
            mutations,
            return_clause: None,
            ..
        } if unwinds.is_empty()
            && linear_clauses.is_empty()
            && with_clauses.is_empty()
            && mutations.is_empty()
            && matches.len() == 1 =>
        {
            export_pattern_path(arena, matches[0])
        }
        _ => Err(unsupported(
            "a pattern comprehension path that is not one plain path pattern",
        )),
    }
}
