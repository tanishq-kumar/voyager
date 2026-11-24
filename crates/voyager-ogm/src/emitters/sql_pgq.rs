//! SQL:2023 Property Graph Queries (PGQ) & DuckPGQ Emitter.

use crate::ast::{
    AggregationFunc, AstNode, BinaryOp, Direction, ExecutionMode, LabelExpression, LiteralValue,
    NodeHandle, ProjectionItem, QueryAstArena, UnaryOp,
};
use crate::error::{Error, Result};
use crate::visitor::{AstVisitor, ColumnMeta, CompiledQuery};
use std::collections::HashMap;

/// Returns the AST node variant name without rendering its contents.
fn ast_node_kind(node: &AstNode) -> &'static str {
    match node {
        AstNode::NodePattern { .. } => "NodePattern",
        AstNode::EdgePattern { .. } => "EdgePattern",
        AstNode::PathChain { .. } => "PathChain",
        AstNode::BinaryExpression { .. } => "BinaryExpression",
        AstNode::UnaryExpression { .. } => "UnaryExpression",
        AstNode::FunctionCall { .. } => "FunctionCall",
        AstNode::CaseExpression { .. } => "CaseExpression",
        AstNode::ListComprehension { .. } => "ListComprehension",
        AstNode::PatternComprehension { .. } => "PatternComprehension",
        AstNode::ExistsSubquery { .. } => "ExistsSubquery",
        AstNode::CountSubquery { .. } => "CountSubquery",
        AstNode::ListLiteral(_) => "ListLiteral",
        AstNode::PropertyAccess { .. } => "PropertyAccess",
        AstNode::LabelPredicate { .. } => "LabelPredicate",
        AstNode::Literal(_) => "Literal",
        AstNode::Identifier(_) => "Identifier",
        AstNode::Parameter(_) => "Parameter",
        AstNode::UnwindClause { .. } => "UnwindClause",
        AstNode::WhereClause { .. } => "WhereClause",
        AstNode::MatchClause { .. } => "MatchClause",
        AstNode::ReturnClause { .. } => "ReturnClause",
        AstNode::ProcedureCall { .. } => "ProcedureCall",
        AstNode::CreateClause { .. } => "CreateClause",
        AstNode::MergeClause { .. } => "MergeClause",
        AstNode::SetClause { .. } => "SetClause",
        AstNode::SetItem { .. } => "SetItem",
        AstNode::DeleteClause { .. } => "DeleteClause",
        AstNode::RemoveClause { .. } => "RemoveClause",
        AstNode::LoadCsvClause { .. } => "LoadCsvClause",
        AstNode::WithClause { .. } => "WithClause",
        AstNode::LetClause { .. } => "LetClause",
        AstNode::FilterClause { .. } => "FilterClause",
        AstNode::QueryStatement { .. } => "QueryStatement",
    }
}

/// Emits standardized ISO/IEC 9075-16:2023 SQL:PGQ `GRAPH_TABLE` queries.
///
/// Labels in `IS` patterns are double-quoted (for example `IS "User"`), so
/// PostgreSQL 19 matches the declared label spelling exactly, the same
/// case-exact rule Cypher uses.
#[derive(Debug)]
pub struct SqlPgqEmitter {
    graph_name: String,
    param_counter: usize,
    parameters: HashMap<String, LiteralValue>,
    buffer: String,
}

impl Default for SqlPgqEmitter {
    fn default() -> Self {
        Self::new("graph_name")
    }
}

impl SqlPgqEmitter {
    /// Creates a fresh SQL:PGQ emitter targeting a specific property graph schema name.
    pub fn new(graph_name: impl Into<String>) -> Self {
        Self {
            graph_name: graph_name.into(),
            param_counter: 0,
            parameters: HashMap::new(),
            buffer: String::with_capacity(256),
        }
    }

    fn emit_node_pattern(&mut self, arena: &QueryAstArena, handle: NodeHandle) -> Result<()> {
        let node = arena.get(handle)?;
        if let AstNode::NodePattern {
            variable,
            label_expression,
            predicates,
        } = node
        {
            if let Some(expr) = label_expression {
                Self::validate_node_label_expression(expr)?;
            }
            self.buffer.push('(');
            let mut has_prefix = false;
            if let Some(var) = variable {
                self.buffer.push_str(var);
                has_prefix = true;
            }
            if label_expression.is_some() {
                crate::emitters::emit_pgq_node_labels(&mut self.buffer, label_expression.as_ref());
                has_prefix = true;
            }
            if !predicates.is_empty() {
                if has_prefix {
                    self.buffer.push(' ');
                }
                self.buffer.push_str("WHERE ");
                for (i, &pred_handle) in predicates.iter().enumerate() {
                    if i > 0 {
                        self.buffer.push_str(" AND ");
                    }
                    self.emit_expression(arena, pred_handle, false)?;
                }
            }
            self.buffer.push(')');
            Ok(())
        } else {
            Err(Error::AstInvariantViolation(format!(
                "Expected NodePattern, got {}",
                ast_node_kind(node)
            )))
        }
    }

    fn emit_edge_pattern(&mut self, arena: &QueryAstArena, handle: NodeHandle) -> Result<()> {
        let node = arena.get(handle)?;
        if let AstNode::EdgePattern {
            variable,
            label_expression,
            direction,
            min_hops,
            max_hops,
            predicates,
            target_node,
        } = node
        {
            match direction {
                Direction::Incoming => self.buffer.push_str(" <-["),
                Direction::Outgoing | Direction::Undirected => self.buffer.push_str(" -["),
            }

            if let Some(expr) = label_expression {
                Self::validate_edge_label_expression(expr)?;
            }

            if let Some(var) = variable {
                self.buffer.push_str(var);
            }

            crate::emitters::emit_pgq_edge_labels(&mut self.buffer, label_expression.as_ref());

            if !predicates.is_empty() {
                self.buffer.push_str(" WHERE ");
                for (i, &pred_handle) in predicates.iter().enumerate() {
                    if i > 0 {
                        self.buffer.push_str(" AND ");
                    }
                    self.emit_expression(arena, pred_handle, false)?;
                }
            }

            self.buffer.push(']');

            match direction {
                Direction::Outgoing => self.buffer.push_str("->"),
                Direction::Incoming | Direction::Undirected => self.buffer.push('-'),
            }

            if min_hops.is_some() || max_hops.is_some() {
                self.buffer.push('{');
                if let Some(min) = min_hops {
                    self.buffer.push_str(&min.to_string());
                } else {
                    self.buffer.push('1');
                }
                self.buffer.push(',');
                if let Some(max) = max_hops {
                    self.buffer.push_str(&max.to_string());
                }
                self.buffer.push('}');
            }

            self.buffer.push(' ');

            self.emit_node_pattern(arena, *target_node)?;
            Ok(())
        } else {
            Err(Error::AstInvariantViolation(format!(
                "Expected EdgePattern, got {}",
                ast_node_kind(node)
            )))
        }
    }

    fn emit_path(&mut self, arena: &QueryAstArena, handle: NodeHandle) -> Result<()> {
        let node = arena.get(handle)?;
        match node {
            AstNode::NodePattern { .. } => self.emit_node_pattern(arena, handle),
            AstNode::PathChain {
                path_variable,
                path_mode,
                start_node,
                edges,
            } => {
                if let Some(var) = path_variable {
                    return Err(Error::UnsupportedFeature {
                        dialect: "sql_pgq".to_string(),
                        feature: format!(
                            "Path variable assignment ('{var} = ...') - SQL:2023 PGQ GRAPH_TABLE does not support path variables"
                        ),
                    });
                }
                if *path_mode != crate::ast::PathMode::None {
                    return Err(Error::UnsupportedFeature {
                        dialect: "sql_pgq".to_string(),
                        feature: format!(
                            "Path search mode '{}' - SQL:2023 PGQ GRAPH_TABLE does not support traversal search modes",
                            path_mode.as_str()
                        ),
                    });
                }
                self.emit_node_pattern(arena, *start_node)?;
                for &edge_handle in edges {
                    self.emit_edge_pattern(arena, edge_handle)?;
                }
                Ok(())
            }
            other => Err(Error::AstInvariantViolation(format!(
                "Expected PathChain or NodePattern, got {}",
                ast_node_kind(other)
            ))),
        }
    }

    fn emit_expression(
        &mut self,
        arena: &QueryAstArena,
        handle: NodeHandle,
        nested: bool,
    ) -> Result<()> {
        let node = arena.get(handle)?;
        match node {
            AstNode::Identifier(id) => {
                self.buffer.push_str(id);
                Ok(())
            }
            AstNode::Parameter(name) => {
                self.buffer.push('$');
                self.buffer.push_str(name);
                Ok(())
            }
            AstNode::NodePattern {
                variable: Some(var),
                ..
            } => {
                self.buffer.push_str(var);
                Ok(())
            }
            AstNode::EdgePattern {
                variable: Some(var),
                ..
            } => {
                self.buffer.push_str(var);
                Ok(())
            }
            AstNode::PropertyAccess { target, property } => {
                self.emit_expression(arena, *target, false)?;
                if !property.is_empty() {
                    self.buffer.push('.');
                    self.buffer.push_str(property);
                }
                Ok(())
            }
            AstNode::LabelPredicate { .. } => Err(Error::UnsupportedFeature {
                dialect: "sql_pgq".to_string(),
                feature: "Label predicate expressions (label) - SQL:2023 PGQ GRAPH_TABLE only supports a label test inside an element pattern, not in a WHERE clause"
                    .to_string(),
            }),
            AstNode::Literal(lit) => {
                let param_name = format!("p{}", self.param_counter);
                self.param_counter += 1;
                self.buffer.push('$');
                self.buffer.push_str(&param_name);
                self.parameters.insert(param_name, lit.clone());
                Ok(())
            }
            AstNode::BinaryExpression { left, op, right } => {
                if matches!(op, BinaryOp::In | BinaryOp::NotIn) {
                    return self.emit_membership(arena, *left, *op, *right, nested);
                }
                if *op == BinaryOp::Xor {
                    return self.emit_xor(arena, *left, *right, nested);
                }
                if nested {
                    self.buffer.push('(');
                }
                if matches!(
                    op,
                    BinaryOp::Contains | BinaryOp::StartsWith | BinaryOp::EndsWith
                ) {
                    // Wildcard-free functions: LIKE would read `%` and `_` in the
                    // search value as wildcards, while Cypher matches it literally.
                    // A NULL operand still yields NULL.
                    let left_sql = self.render_expression_to_string(arena, *left)?;
                    let right_sql = self.render_expression_to_string(arena, *right)?;
                    match op {
                        BinaryOp::Contains => {
                            self.buffer
                                .push_str(&format!("strpos({left_sql}, {right_sql}) > 0"));
                        }
                        BinaryOp::StartsWith => {
                            self.buffer
                                .push_str(&format!("starts_with({left_sql}, {right_sql})"));
                        }
                        _ => {
                            self.buffer.push_str(&format!(
                                "right({left_sql}, length({right_sql})) = {right_sql}"
                            ));
                        }
                    }
                    if nested {
                        self.buffer.push(')');
                    }
                    return Ok(());
                }
                self.emit_expression(arena, *left, true)?;
                match op {
                    BinaryOp::Concat => {
                        self.buffer.push_str(" || ");
                        self.emit_expression(arena, *right, true)?;
                    }
                    BinaryOp::RegexMatch => {
                        return Err(Error::UnsupportedFeature {
                            dialect: "sql_pgq".to_string(),
                            feature: "Regular expression match operator ('=~', regex) - SQL:2023 PGQ GRAPH_TABLE does not define a regex match operator"
                                .to_string(),
                        });
                    }
                    other => {
                        self.buffer.push(' ');
                        self.buffer.push_str(&other.to_string());
                        self.buffer.push(' ');
                        self.emit_expression(arena, *right, true)?;
                    }
                }
                if nested {
                    self.buffer.push(')');
                }
                Ok(())
            }
            AstNode::UnaryExpression { op, operand } => match op {
                UnaryOp::Not => {
                    self.buffer.push_str("NOT (");
                    self.emit_expression(arena, *operand, false)?;
                    self.buffer.push(')');
                    Ok(())
                }
                UnaryOp::Neg => {
                    self.buffer.push('-');
                    self.emit_expression(arena, *operand, true)
                }
                UnaryOp::IsNull => {
                    self.emit_expression(arena, *operand, true)?;
                    self.buffer.push_str(" IS NULL");
                    Ok(())
                }
                UnaryOp::IsNotNull => {
                    self.emit_expression(arena, *operand, true)?;
                    self.buffer.push_str(" IS NOT NULL");
                    Ok(())
                }
            },
            AstNode::FunctionCall { name, arguments } => {
                let sql_func = match name.to_ascii_lowercase().as_str() {
                    "tolower" | "lower" => "LOWER",
                    "toupper" | "upper" => "UPPER",
                    "size" | "length" | "char_length" | "character_length" => "LENGTH",
                    "cardinality" => "CARDINALITY",
                    "distance" | "point.distance" | "point_distance" => "ST_Distance",
                    "coalesce" => "COALESCE",
                    "trim" => "TRIM",
                    "ltrim" => "LTRIM",
                    "rtrim" => "RTRIM",
                    "split" => "STRING_SPLIT",
                    "substring" => "SUBSTRING",
                    "replace" => "REPLACE",
                    "reverse" => "REVERSE",
                    "left" => "LEFT",
                    "right" => "RIGHT",
                    "abs" => "ABS",
                    "sqrt" => "SQRT",
                    "ceil" => "CEIL",
                    "floor" => "FLOOR",
                    "round" => "ROUND",
                    "log" => "LN",
                    "log10" => "LOG10",
                    "exp" => "EXP",
                    "sin" => "SIN",
                    "cos" => "COS",
                    "tan" => "TAN",
                    "asin" => "ASIN",
                    "acos" => "ACOS",
                    "atan" => "ATAN",
                    _ => name.as_str(),
                };
                self.buffer.push_str(sql_func);
                self.buffer.push('(');
                for (i, &arg) in arguments.iter().enumerate() {
                    if i > 0 {
                        self.buffer.push_str(", ");
                    }
                    self.emit_expression(arena, arg, false)?;
                }
                self.buffer.push(')');
                Ok(())
            }
            AstNode::CaseExpression {
                operand,
                when_then_branches,
                else_branch,
            } => {
                self.buffer.push_str("CASE");
                if let Some(op) = operand {
                    self.buffer.push(' ');
                    self.emit_expression(arena, *op, false)?;
                }
                for (when_expr, then_expr) in when_then_branches {
                    self.buffer.push_str(" WHEN ");
                    self.emit_expression(arena, *when_expr, false)?;
                    self.buffer.push_str(" THEN ");
                    self.emit_expression(arena, *then_expr, false)?;
                }
                if let Some(else_expr) = else_branch {
                    self.buffer.push_str(" ELSE ");
                    self.emit_expression(arena, *else_expr, false)?;
                }
                self.buffer.push_str(" END");
                Ok(())
            }
            AstNode::ExistsSubquery { .. } => Err(Error::UnsupportedFeature {
                dialect: "sql_pgq".to_string(),
                feature: "EXISTS subquery expressions (exists) - SQL:2023 PGQ GRAPH_TABLE does not support Cypher existential subqueries"
                    .to_string(),
            }),
            AstNode::CountSubquery { .. } => Err(Error::UnsupportedFeature {
                dialect: "sql_pgq".to_string(),
                feature: "COUNT subquery expressions (count) - SQL:2023 PGQ GRAPH_TABLE does not support Cypher count subqueries"
                    .to_string(),
            }),
            AstNode::ListComprehension { .. } => Err(Error::UnsupportedFeature {
                dialect: "sql_pgq".to_string(),
                feature: "List comprehension expressions (list_comp) - SQL:2023 PGQ GRAPH_TABLE does not support Cypher list comprehensions"
                    .to_string(),
            }),
            AstNode::PatternComprehension { .. } => Err(Error::UnsupportedFeature {
                dialect: "sql_pgq".to_string(),
                feature: "Pattern comprehension expressions (pattern_comp) - SQL:2023 PGQ GRAPH_TABLE does not support Cypher pattern comprehensions"
                    .to_string(),
            }),
            AstNode::ListLiteral(items) => {
                self.buffer.push('[');
                for (i, &item) in items.iter().enumerate() {
                    if i > 0 {
                        self.buffer.push_str(", ");
                    }
                    self.emit_expression(arena, item, false)?;
                }
                self.buffer.push(']');
                Ok(())
            }
            other => Err(Error::AstInvariantViolation(format!(
                "Unsupported expression node kind: {}",
                ast_node_kind(other)
            ))),
        }
    }

    fn render_expression_to_string(
        &mut self,
        arena: &QueryAstArena,
        handle: NodeHandle,
    ) -> Result<String> {
        let saved_buffer = std::mem::take(&mut self.buffer);
        let res = self.emit_expression(arena, handle, false);
        let expr_str = std::mem::replace(&mut self.buffer, saved_buffer);
        res.map(|_| expr_str)
    }

    /// Emits `IN` / `NOT IN` in the spellings PostgreSQL and DuckDB accept.
    ///
    /// A literal list becomes `x IN (a, b)` (and `x NOT IN (a, b)`); an empty
    /// literal list becomes `FALSE` (`TRUE` when negated), because `IN ()` is
    /// not valid SQL and Cypher's membership in an empty list is always false.
    /// Any other operand is a parameter or a list-valued expression, which is
    /// rendered as `x = ANY($p)` (`x <> ALL($p)` when negated).
    fn emit_membership(
        &mut self,
        arena: &QueryAstArena,
        left: NodeHandle,
        op: BinaryOp,
        right: NodeHandle,
        nested: bool,
    ) -> Result<()> {
        let negated = matches!(op, BinaryOp::NotIn);
        if let AstNode::ListLiteral(items) = arena.get(right)? {
            if items.is_empty() {
                self.buffer.push_str(if negated { "TRUE" } else { "FALSE" });
                return Ok(());
            }
            if nested {
                self.buffer.push('(');
            }
            self.emit_expression(arena, left, true)?;
            self.buffer
                .push_str(if negated { " NOT IN (" } else { " IN (" });
            for (i, &item) in items.iter().enumerate() {
                if i > 0 {
                    self.buffer.push_str(", ");
                }
                self.emit_expression(arena, item, true)?;
            }
            self.buffer.push(')');
            if nested {
                self.buffer.push(')');
            }
            return Ok(());
        }

        if nested {
            self.buffer.push('(');
        }
        self.emit_expression(arena, left, true)?;
        self.buffer
            .push_str(if negated { " <> ALL(" } else { " = ANY(" });
        self.emit_expression(arena, right, true)?;
        self.buffer.push(')');
        if nested {
            self.buffer.push(')');
        }
        Ok(())
    }

    /// Lowers `XOR` to SQL, which has no XOR operator.
    ///
    /// `(a OR b) AND NOT (a AND b)` has the same three-valued result as Cypher
    /// `XOR`, including when one or both operands are `NULL`. Each operand is
    /// rendered once, so a parameter used by the expansion keeps its name and
    /// the bridges bind it once (or once per occurrence, as they already do).
    fn emit_xor(
        &mut self,
        arena: &QueryAstArena,
        left: NodeHandle,
        right: NodeHandle,
        nested: bool,
    ) -> Result<()> {
        let left_sql = self.render_expression_to_string(arena, left)?;
        let right_sql = self.render_expression_to_string(arena, right)?;
        if nested {
            self.buffer.push('(');
        }
        self.buffer.push_str(&format!(
            "(({left_sql}) OR ({right_sql})) AND NOT (({left_sql}) AND ({right_sql}))"
        ));
        if nested {
            self.buffer.push(')');
        }
        Ok(())
    }

    fn emit_where(&mut self, arena: &QueryAstArena, handle: NodeHandle) -> Result<()> {
        let node = arena.get(handle)?;
        if let AstNode::WhereClause { root_predicate } = node {
            self.buffer.push_str(" WHERE ");
            self.emit_expression(arena, *root_predicate, false)?;
            Ok(())
        } else {
            Err(Error::AstInvariantViolation(format!(
                "Expected WhereClause, got {}",
                ast_node_kind(node)
            )))
        }
    }

    fn expressions_match(&self, arena: &QueryAstArena, a: NodeHandle, b: NodeHandle) -> bool {
        if a == b {
            return true;
        }
        match (arena.get(a), arena.get(b)) {
            (
                Ok(AstNode::PropertyAccess {
                    target: t1,
                    property: p1,
                }),
                Ok(AstNode::PropertyAccess {
                    target: t2,
                    property: p2,
                }),
            ) => p1 == p2 && self.expressions_match(arena, *t1, *t2),
            (Ok(AstNode::Identifier(id1)), Ok(AstNode::Identifier(id2))) => id1 == id2,
            (
                Ok(AstNode::NodePattern {
                    variable: Some(v1), ..
                }),
                Ok(AstNode::Identifier(id2)),
            ) => v1 == id2,
            (
                Ok(AstNode::Identifier(id1)),
                Ok(AstNode::NodePattern {
                    variable: Some(v2), ..
                }),
            ) => id1 == v2,
            (
                Ok(AstNode::NodePattern {
                    variable: Some(v1), ..
                }),
                Ok(AstNode::NodePattern {
                    variable: Some(v2), ..
                }),
            ) => v1 == v2,
            (
                Ok(AstNode::BinaryExpression {
                    left: l1,
                    op: op1,
                    right: r1,
                }),
                Ok(AstNode::BinaryExpression {
                    left: l2,
                    op: op2,
                    right: r2,
                }),
            ) => {
                op1 == op2
                    && self.expressions_match(arena, *l1, *l2)
                    && self.expressions_match(arena, *r1, *r2)
            }
            (
                Ok(AstNode::UnaryExpression {
                    op: op1,
                    operand: o1,
                }),
                Ok(AstNode::UnaryExpression {
                    op: op2,
                    operand: o2,
                }),
            ) => op1 == op2 && self.expressions_match(arena, *o1, *o2),
            (Ok(AstNode::Literal(lit1)), Ok(AstNode::Literal(lit2))) => lit1 == lit2,
            (Ok(AstNode::Parameter(p1)), Ok(AstNode::Parameter(p2))) => p1 == p2,
            _ => false,
        }
    }

    fn resolve_returned_column_name(
        &self,
        arena: &QueryAstArena,
        expr_handle: NodeHandle,
        projections: &[ProjectionItem],
    ) -> Option<String> {
        for proj in projections {
            if self.expressions_match(arena, proj.expression, expr_handle) {
                if let Some(alias) = &proj.alias {
                    return Some(alias.clone());
                } else if let Ok(AstNode::PropertyAccess { property, .. }) =
                    arena.get(proj.expression)
                {
                    return Some(property.clone());
                } else if let Ok(AstNode::Identifier(id)) = arena.get(proj.expression) {
                    return Some(id.clone());
                }
            }
        }
        for proj in projections {
            if let Some(func) = proj.aggregation {
                if let Ok(AstNode::FunctionCall { name, arguments }) = arena.get(expr_handle) {
                    let func_name_matches = match func {
                        AggregationFunc::Count => name.eq_ignore_ascii_case("count"),
                        AggregationFunc::CountDistinct => name.eq_ignore_ascii_case("count"),
                        AggregationFunc::Sum => name.eq_ignore_ascii_case("sum"),
                        AggregationFunc::Avg => name.eq_ignore_ascii_case("avg"),
                        AggregationFunc::Min => name.eq_ignore_ascii_case("min"),
                        AggregationFunc::Max => name.eq_ignore_ascii_case("max"),
                        AggregationFunc::Collect => name.eq_ignore_ascii_case("collect"),
                    };
                    if arguments.len() == 1
                        && func_name_matches
                        && self.expressions_match(arena, proj.expression, arguments[0])
                    {
                        if let Some(alias) = &proj.alias {
                            return Some(alias.clone());
                        } else {
                            let base = self.get_expr_leaf_name(arena, proj.expression);
                            return Some(format!("{func:?}({base})").to_ascii_lowercase());
                        }
                    }
                }
            }
        }
        if let Ok(AstNode::Identifier(id)) = arena.get(expr_handle) {
            for proj in projections {
                if let Some(alias) = &proj.alias {
                    if alias == id {
                        return Some(alias.clone());
                    }
                } else if let Ok(AstNode::PropertyAccess { property, .. }) =
                    arena.get(proj.expression)
                {
                    if property == id {
                        return Some(property.clone());
                    }
                } else if let Ok(AstNode::Identifier(proj_id)) = arena.get(proj.expression) {
                    if proj_id == id {
                        return Some(proj_id.clone());
                    }
                }
            }
        }
        if let Ok(AstNode::PropertyAccess { property, .. }) = arena.get(expr_handle) {
            for proj in projections {
                if let Some(alias) = &proj.alias {
                    if alias == property {
                        return Some(alias.clone());
                    }
                }
            }
        }
        None
    }

    fn get_projection_column_name(
        &self,
        arena: &QueryAstArena,
        proj: &ProjectionItem,
        idx: usize,
    ) -> String {
        if let Some(alias) = &proj.alias {
            alias.clone()
        } else if let Ok(AstNode::PropertyAccess { property, .. }) = arena.get(proj.expression) {
            property.clone()
        } else if let Ok(AstNode::Identifier(id)) = arena.get(proj.expression) {
            id.clone()
        } else {
            let leaf = self.get_expr_leaf_name(arena, proj.expression);
            if !leaf.is_empty() {
                leaf
            } else {
                format!("col_{idx}")
            }
        }
    }

    fn get_expr_leaf_name(&self, arena: &QueryAstArena, expr_handle: NodeHandle) -> String {
        match arena.get(expr_handle) {
            Ok(AstNode::PropertyAccess { property, .. }) => property.clone(),
            Ok(AstNode::Identifier(id)) => id.clone(),
            _ => String::new(),
        }
    }

    fn is_whole_entity_expression(
        &self,
        arena: &QueryAstArena,
        handle: NodeHandle,
    ) -> Result<Option<String>> {
        match arena.get(handle)? {
            AstNode::NodePattern { variable, .. } => {
                Ok(variable.clone().or_else(|| Some("entity".to_string())))
            }
            AstNode::EdgePattern { variable, .. } => {
                Ok(variable.clone().or_else(|| Some("entity".to_string())))
            }
            AstNode::PropertyAccess { target, property } => {
                if property.is_empty() {
                    match arena.get(*target)? {
                        AstNode::Identifier(id) => Ok(Some(id.clone())),
                        AstNode::NodePattern {
                            variable: Some(var),
                            ..
                        } => Ok(Some(var.clone())),
                        AstNode::EdgePattern {
                            variable: Some(var),
                            ..
                        } => Ok(Some(var.clone())),
                        _ => Ok(Some("entity".to_string())),
                    }
                } else {
                    Ok(None)
                }
            }
            AstNode::Identifier(id) => {
                if !id.contains('.') {
                    Ok(Some(id.clone()))
                } else {
                    Ok(None)
                }
            }
            _ => Ok(None),
        }
    }

    fn emit_matches_and_where(
        &mut self,
        arena: &QueryAstArena,
        matches: &[NodeHandle],
    ) -> Result<()> {
        for (m_idx, &match_handle) in matches.iter().enumerate() {
            if m_idx > 0 {
                self.buffer.push_str(", ");
            }
            let match_node = arena.get(match_handle)?;
            if let AstNode::MatchClause {
                optional,
                paths,
                where_clause,
            } = match_node
            {
                if *optional {
                    return Err(Error::UnsupportedFeature {
                        dialect: "sql_pgq".to_string(),
                        feature: "OPTIONAL MATCH clauses (optional) - SQL:2023 PGQ GRAPH_TABLE has no direct outer-join form for Cypher OPTIONAL MATCH"
                            .to_string(),
                    });
                }
                for (p_idx, &path_handle) in paths.iter().enumerate() {
                    if p_idx > 0 {
                        self.buffer.push_str(", ");
                    }
                    self.emit_path(arena, path_handle)?;
                }

                if let Some(wh) = where_clause {
                    self.emit_where(arena, *wh)?;
                }
            }
        }
        Ok(())
    }

    fn validate_node_label_expression(expr: &LabelExpression) -> Result<()> {
        if expr.is_simple_disjunction() {
            // Single label or plain disjunction: (A) or (A | B | C)
            Ok(())
        } else if let Some(labels) = expr.to_conjunction_labels() {
            if labels.len() > 1 {
                Err(Error::UnsupportedFeature {
                    dialect: "sql_pgq".to_string(),
                    feature: "Node pattern with multiple labels - SQL:2023 PGQ GRAPH_TABLE does not support multiple labels per node pattern".to_string(),
                })
            } else {
                Ok(())
            }
        } else {
            Err(Error::UnsupportedFeature {
                dialect: "sql_pgq".to_string(),
                feature: "Label expression with negation/conjunction/grouping or wildcard - SQL:2023 PGQ GRAPH_TABLE does not support label expressions using negation ('!'), conjunction ('&'), grouping, or wildcard ('%')".to_string(),
            })
        }
    }

    fn validate_edge_label_expression(expr: &LabelExpression) -> Result<()> {
        if expr.is_simple_disjunction() {
            // Single type or plain disjunction: -[r IS "KNOWS"]-> or -[r IS "KNOWS" | "HATES"]->
            Ok(())
        } else {
            Err(Error::UnsupportedFeature {
                dialect: "sql_pgq".to_string(),
                feature: "Relationship type expression with negation/conjunction/grouping or wildcard - SQL:2023 PGQ GRAPH_TABLE does not support relationship type expressions using negation ('!'), conjunction ('&'), grouping, or wildcard ('%')".to_string(),
            })
        }
    }

    fn validate_node_pattern(&self, arena: &QueryAstArena, node_handle: NodeHandle) -> Result<()> {
        if let AstNode::NodePattern {
            label_expression: Some(expr),
            ..
        } = arena.get(node_handle)?
        {
            Self::validate_node_label_expression(expr)?;
        }
        Ok(())
    }

    fn validate_edge_pattern(&self, arena: &QueryAstArena, edge_handle: NodeHandle) -> Result<()> {
        if let AstNode::EdgePattern {
            label_expression: Some(expr),
            ..
        } = arena.get(edge_handle)?
        {
            Self::validate_edge_label_expression(expr)?;
        }
        Ok(())
    }

    fn validate_matches(&self, arena: &QueryAstArena, matches: &[NodeHandle]) -> Result<()> {
        for &match_handle in matches {
            let match_node = arena.get(match_handle)?;
            if let AstNode::MatchClause {
                optional, paths, ..
            } = match_node
            {
                if *optional {
                    return Err(Error::UnsupportedFeature {
                        dialect: "sql_pgq".to_string(),
                        feature: "OPTIONAL MATCH clauses (optional) - SQL:2023 PGQ GRAPH_TABLE has no direct outer-join form for Cypher OPTIONAL MATCH"
                            .to_string(),
                    });
                }
                for &path_handle in paths {
                    let path_node = arena.get(path_handle)?;
                    match path_node {
                        AstNode::NodePattern { .. } => {
                            self.validate_node_pattern(arena, path_handle)?;
                        }
                        AstNode::PathChain {
                            path_variable,
                            path_mode,
                            start_node,
                            edges,
                        } => {
                            if let Some(var) = path_variable {
                                return Err(Error::UnsupportedFeature {
                                    dialect: "sql_pgq".to_string(),
                                    feature: format!(
                                        "Path variable assignment ('{var} = ...') - SQL:2023 PGQ GRAPH_TABLE does not support path variables"
                                    ),
                                });
                            }
                            if *path_mode != crate::ast::PathMode::None {
                                return Err(Error::UnsupportedFeature {
                                    dialect: "sql_pgq".to_string(),
                                    feature: format!(
                                        "Path search mode '{}' - SQL:2023 PGQ GRAPH_TABLE does not support traversal search modes",
                                        path_mode.as_str()
                                    ),
                                });
                            }
                            self.validate_node_pattern(arena, *start_node)?;
                            for &edge_handle in edges {
                                self.validate_edge_pattern(arena, edge_handle)?;
                                if let AstNode::EdgePattern { target_node, .. } =
                                    arena.get(edge_handle)?
                                {
                                    self.validate_node_pattern(arena, *target_node)?;
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
        Ok(())
    }

    fn emit_columns(
        &mut self,
        arena: &QueryAstArena,
        projections: &[ProjectionItem],
    ) -> Result<()> {
        self.buffer.push_str(" COLUMNS (");
        for (i, proj) in projections.iter().enumerate() {
            if i > 0 {
                self.buffer.push_str(", ");
            }
            if let Some(func) = proj.aggregation {
                match func {
                    AggregationFunc::Count => self.buffer.push_str("COUNT("),
                    AggregationFunc::CountDistinct => self.buffer.push_str("COUNT(DISTINCT "),
                    AggregationFunc::Sum => self.buffer.push_str("SUM("),
                    AggregationFunc::Avg => self.buffer.push_str("AVG("),
                    AggregationFunc::Min => self.buffer.push_str("MIN("),
                    AggregationFunc::Max => self.buffer.push_str("MAX("),
                    AggregationFunc::Collect => self.buffer.push_str("ARRAY_AGG("),
                }
                self.emit_expression(arena, proj.expression, false)?;
                self.buffer.push(')');
            } else {
                self.emit_expression(arena, proj.expression, false)?;
            }

            if let Some(alias) = &proj.alias {
                self.buffer.push_str(" AS ");
                self.buffer.push_str(alias);
            }
        }
        self.buffer.push(')');
        Ok(())
    }
}

impl AstVisitor for SqlPgqEmitter {
    fn visit_query(&mut self, arena: &QueryAstArena, root: NodeHandle) -> Result<CompiledQuery> {
        if !crate::emitters::is_valid_age_graph_name(&self.graph_name) {
            return Err(Error::EmissionError(format!(
                "Invalid graph name '{}' for SQL:PGQ queries. Graph names must be valid identifiers (ASCII letters, digits, and underscores, starting with a letter or underscore, 1 to 63 characters).",
                self.graph_name
            )));
        }

        crate::emitters::validate_query_identifiers(arena, root)?;

        self.buffer.clear();
        self.parameters.clear();
        self.param_counter = 0;

        let root_node = arena.get(root)?;
        if let AstNode::QueryStatement {
            execution_mode,
            load_csv,
            unwinds,
            matches,
            linear_clauses,
            with_clauses,
            mutations,
            return_clause,
        } = root_node
        {
            if !mutations.is_empty() {
                return Err(Error::UnsupportedFeature {
                    dialect: "sql_pgq".to_string(),
                    feature: "DML mutations (CREATE, MERGE, SET, DELETE) - GRAPH_TABLE is a read-only query operator".to_string(),
                });
            }
            if load_csv.is_some() {
                return Err(Error::UnsupportedFeature {
                    dialect: "sql_pgq".to_string(),
                    feature: "LOAD CSV clauses (load_csv) - GRAPH_TABLE is a read-only query operator over a registered property graph"
                        .to_string(),
                });
            }
            if !unwinds.is_empty() {
                return Err(Error::UnsupportedFeature {
                    dialect: "sql_pgq".to_string(),
                    feature: "UNWIND clauses - GRAPH_TABLE is a read-only query operator"
                        .to_string(),
                });
            }
            if !linear_clauses.is_empty() {
                return Err(Error::UnsupportedFeature {
                    dialect: "sql_pgq".to_string(),
                    feature: "Linear statements (LET, FILTER) - SQL:2023 PGQ GRAPH_TABLE does not support linear statements"
                        .to_string(),
                });
            }
            if !with_clauses.is_empty() {
                return Err(Error::UnsupportedFeature {
                    dialect: "sql_pgq".to_string(),
                    feature: "WITH clauses - SQL:PGQ GRAPH_TABLE operator does not support Cypher-style intermediate WITH projection pipelines"
                        .to_string(),
                });
            }

            let mut order_by_clause = None;
            let mut limit_clause = None;
            let mut skip_clause = None;
            let mut distinct = false;
            let mut projections_list: Vec<ProjectionItem> = Vec::new();

            if let Some(ret_handle) = return_clause {
                let ret_node = arena.get(*ret_handle)?;
                if let AstNode::ReturnClause {
                    distinct: return_distinct,
                    projections,
                    order_by,
                    skip,
                    limit,
                } = ret_node
                {
                    distinct = *return_distinct;
                    projections_list = projections.clone();
                    order_by_clause = Some(order_by);
                    limit_clause = limit.clone();
                    skip_clause = skip.clone();
                }
            }

            let prefix = match execution_mode {
                ExecutionMode::Normal => "",
                ExecutionMode::Explain => "EXPLAIN ",
                ExecutionMode::Profile | ExecutionMode::ExplainAndProfile => "EXPLAIN ANALYZE ",
            };

            self.validate_matches(arena, matches)?;

            if projections_list.is_empty() {
                return Err(Error::EmissionError(
                    "SQL:PGQ GRAPH_TABLE queries require at least one projection; add a RETURN clause with at least one field or expression."
                        .to_string(),
                ));
            }

            for proj in &projections_list {
                if proj.aggregation.is_none() {
                    if let Some(var) = self.is_whole_entity_expression(arena, proj.expression)? {
                        return Err(Error::UnsupportedFeature {
                            dialect: "sql_pgq".to_string(),
                            feature: format!(
                                "Whole entity column projection ('{var}') - SQL:2023 PGQ GRAPH_TABLE COLUMNS clause only supports scalar expressions and properties, not whole node or relationship variables"
                            ),
                        });
                    }
                }
            }

            let has_aggregates = projections_list.iter().any(|p| p.aggregation.is_some());

            let mut hidden_columns: Vec<(NodeHandle, String)> = Vec::new();
            let mut order_by_compiled: Vec<(String, bool)> = Vec::new();

            if let Some(order_by) = order_by_clause
                && !order_by.is_empty()
            {
                let mut returned_names: std::collections::HashSet<String> =
                    std::collections::HashSet::new();
                for (idx, proj) in projections_list.iter().enumerate() {
                    returned_names.insert(self.get_projection_column_name(arena, proj, idx));
                }

                let mut alias_counter = 0;
                for (order_expr, is_asc) in order_by {
                    if let Some(col_name) =
                        self.resolve_returned_column_name(arena, *order_expr, &projections_list)
                    {
                        order_by_compiled.push((col_name, *is_asc));
                    } else {
                        if let Some(var) = self.is_whole_entity_expression(arena, *order_expr)? {
                            return Err(Error::UnsupportedFeature {
                                dialect: "sql_pgq".to_string(),
                                feature: format!(
                                    "Whole entity column projection ('{var}') - SQL:2023 PGQ GRAPH_TABLE COLUMNS clause only supports scalar expressions and properties, not whole node or relationship variables"
                                ),
                            });
                        }
                        let existing = hidden_columns
                            .iter()
                            .find(|(expr, _)| self.expressions_match(arena, *expr, *order_expr));
                        let alias = if let Some((_, alias)) = existing {
                            alias.clone()
                        } else {
                            loop {
                                let candidate = format!("_ord_{alias_counter}");
                                alias_counter += 1;
                                if !returned_names.contains(&candidate) {
                                    hidden_columns.push((*order_expr, candidate.clone()));
                                    break candidate;
                                }
                            }
                        };
                        order_by_compiled.push((alias, *is_asc));
                    }
                }
            }

            if !hidden_columns.is_empty() {
                if distinct {
                    return Err(Error::UnsupportedFeature {
                        dialect: "sql_pgq".to_string(),
                        feature: "ORDER BY on non-projected expressions with DISTINCT - SQL/PGQ requires ordering expressions under DISTINCT to be projected columns to preserve DISTINCT semantics".to_string(),
                    });
                }
                if has_aggregates {
                    return Err(Error::UnsupportedFeature {
                        dialect: "sql_pgq".to_string(),
                        feature: "ORDER BY on unprojected expressions with aggregation - SQL/PGQ requires ordering expressions in aggregating queries to appear in the projection or GROUP BY".to_string(),
                    });
                }
            }

            if has_aggregates {
                let mut inner_columns: Vec<(String, String)> = Vec::new();
                let mut outer_select_items: Vec<String> = Vec::new();
                let mut group_by_keys: Vec<String> = Vec::new();

                for (idx, proj) in projections_list.iter().enumerate() {
                    let expr_sql = self.render_expression_to_string(arena, proj.expression)?;
                    let fallback_name = self.get_expr_leaf_name(arena, proj.expression);
                    if let Some(func) = proj.aggregation {
                        let inner_col_name = if let Some((_, existing_alias)) =
                            inner_columns.iter().find(|(e, _)| e == &expr_sql)
                        {
                            existing_alias.clone()
                        } else {
                            let base_name = if !fallback_name.is_empty() {
                                fallback_name
                            } else {
                                format!("col_{idx}")
                            };
                            let col_alias = if inner_columns.iter().any(|(_, a)| a == &base_name) {
                                format!("{base_name}_{idx}")
                            } else {
                                base_name
                            };
                            inner_columns.push((expr_sql, col_alias.clone()));
                            col_alias
                        };

                        let func_call = match func {
                            AggregationFunc::Count => format!("COUNT({inner_col_name})"),
                            AggregationFunc::CountDistinct => {
                                format!("COUNT(DISTINCT {inner_col_name})")
                            }
                            AggregationFunc::Sum => format!("SUM({inner_col_name})"),
                            AggregationFunc::Avg => format!("AVG({inner_col_name})"),
                            AggregationFunc::Min => format!("MIN({inner_col_name})"),
                            AggregationFunc::Max => format!("MAX({inner_col_name})"),
                            AggregationFunc::Collect => format!("ARRAY_AGG({inner_col_name})"),
                        };

                        if let Some(alias) = &proj.alias {
                            outer_select_items.push(format!("{func_call} AS {alias}"));
                        } else {
                            outer_select_items.push(func_call);
                        }
                    } else {
                        let col_alias = proj.alias.clone().unwrap_or_else(|| {
                            if !fallback_name.is_empty() {
                                fallback_name
                            } else {
                                format!("col_{idx}")
                            }
                        });

                        if let Some((_, existing_alias)) =
                            inner_columns.iter().find(|(e, _)| e == &expr_sql)
                        {
                            outer_select_items.push(existing_alias.clone());
                            group_by_keys.push(existing_alias.clone());
                        } else {
                            inner_columns.push((expr_sql, col_alias.clone()));
                            outer_select_items.push(col_alias.clone());
                            group_by_keys.push(col_alias);
                        }
                    }
                }

                self.buffer.push_str(prefix);
                self.buffer.push_str(if distinct {
                    "SELECT DISTINCT "
                } else {
                    "SELECT "
                });
                for (i, item) in outer_select_items.iter().enumerate() {
                    if i > 0 {
                        self.buffer.push_str(", ");
                    }
                    self.buffer.push_str(item);
                }
                self.buffer.push_str(" FROM GRAPH_TABLE (");
                self.buffer.push_str(&self.graph_name);
                self.buffer.push_str(" MATCH ");

                self.emit_matches_and_where(arena, matches)?;

                self.buffer.push_str(" COLUMNS (");
                for (i, (expr_sql, alias)) in inner_columns.iter().enumerate() {
                    if i > 0 {
                        self.buffer.push_str(", ");
                    }
                    self.buffer.push_str(expr_sql);
                    self.buffer.push_str(" AS ");
                    self.buffer.push_str(alias);
                }
                self.buffer.push_str("))");

                if !group_by_keys.is_empty() {
                    self.buffer.push_str(" GROUP BY ");
                    for (i, key) in group_by_keys.iter().enumerate() {
                        if i > 0 {
                            self.buffer.push_str(", ");
                        }
                        self.buffer.push_str(key);
                    }
                }
            } else if !hidden_columns.is_empty() {
                self.buffer.push_str(prefix);
                self.buffer.push_str("SELECT ");
                for (i, proj) in projections_list.iter().enumerate() {
                    if i > 0 {
                        self.buffer.push_str(", ");
                    }
                    self.buffer
                        .push_str(&self.get_projection_column_name(arena, proj, i));
                }
                self.buffer.push_str(" FROM GRAPH_TABLE (");
                self.buffer.push_str(&self.graph_name);
                self.buffer.push_str(" MATCH ");

                self.emit_matches_and_where(arena, matches)?;

                self.buffer.push_str(" COLUMNS (");
                for (i, proj) in projections_list.iter().enumerate() {
                    if i > 0 {
                        self.buffer.push_str(", ");
                    }
                    self.emit_expression(arena, proj.expression, false)?;
                    if let Some(alias) = &proj.alias {
                        self.buffer.push_str(" AS ");
                        self.buffer.push_str(alias);
                    }
                }
                for (expr, alias) in &hidden_columns {
                    self.buffer.push_str(", ");
                    self.emit_expression(arena, *expr, false)?;
                    self.buffer.push_str(" AS ");
                    self.buffer.push_str(alias);
                }
                self.buffer.push_str("))");
            } else {
                self.buffer.push_str(prefix);
                self.buffer.push_str(if distinct {
                    "SELECT DISTINCT * FROM GRAPH_TABLE ("
                } else {
                    "SELECT * FROM GRAPH_TABLE ("
                });
                self.buffer.push_str(&self.graph_name);
                self.buffer.push_str(" MATCH ");

                self.emit_matches_and_where(arena, matches)?;

                self.emit_columns(arena, &projections_list)?;
                self.buffer.push(')');
            }

            if !order_by_compiled.is_empty() {
                self.buffer.push_str(" ORDER BY ");
                for (i, (col, is_asc)) in order_by_compiled.iter().enumerate() {
                    if i > 0 {
                        self.buffer.push_str(", ");
                    }
                    self.buffer.push_str(col);
                    if *is_asc {
                        self.buffer.push_str(" ASC NULLS LAST");
                    } else {
                        self.buffer.push_str(" DESC NULLS FIRST");
                    }
                }
            }

            if let Some(limit) = limit_clause {
                self.buffer.push_str(&format!(" LIMIT {limit}"));
            }

            if let Some(skip) = skip_clause {
                self.buffer.push_str(&format!(" OFFSET {skip}"));
            }

            let columns = ColumnMeta::from_projections(arena, &projections_list);

            Ok(CompiledQuery::with_columns(
                std::mem::take(&mut self.buffer),
                std::mem::take(&mut self.parameters),
                *execution_mode,
                columns,
            ))
        } else {
            Err(Error::AstInvariantViolation(format!(
                "Invalid root AST node for SQL:PGQ: {}",
                ast_node_kind(root_node)
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::QueryBuilder;

    fn string_predicate_statement(op: BinaryOp, value: &str) -> (String, String) {
        let mut builder = QueryBuilder::new();
        builder
            .match_node(Some("p"), vec!["Person"])
            .where_property("p", "name", op, value)
            .field("p", "name", None::<String>);
        let (arena, root) = builder.build();
        let mut emitter = SqlPgqEmitter::new("g");
        let compiled = emitter.visit_query(&arena, root).unwrap();
        let p0 = compiled.parameters.get("p0").cloned().unwrap();
        (compiled.statement, format!("{p0:?}"))
    }

    #[test]
    fn sql_pgq_string_predicates_avoid_like_wildcards() {
        let cases = [
            (BinaryOp::StartsWith, "starts_with(p.name, $p0)"),
            (BinaryOp::Contains, "strpos(p.name, $p0) > 0"),
            (BinaryOp::EndsWith, "right(p.name, length($p0)) = $p0"),
        ];
        for (op, expected) in cases {
            let (statement, param) = string_predicate_statement(op, "10%_\\x");
            assert!(statement.contains(expected), "{statement}");
            assert!(!statement.contains("LIKE"), "{statement}");
            assert!(param.contains("10%_"), "{param}");
        }
    }

    #[test]
    fn sql_pgq_rejects_multi_label_node_patterns() {
        let mut builder = QueryBuilder::new();
        builder
            .r#match()
            .node(Some("p"), vec!["Person", "Employee"])
            .r#return()
            .field("p", "name", None::<String>);

        let (arena, root) = builder.build();
        let mut emitter = SqlPgqEmitter::new("test_graph");
        let err = emitter.visit_query(&arena, root).unwrap_err();

        match err {
            Error::UnsupportedFeature { dialect, feature } => {
                assert_eq!(dialect, "sql_pgq");
                assert!(feature.contains("Node pattern with multiple labels"));
            }
            other => panic!("Expected UnsupportedFeature, got {other:?}"),
        }
    }

    #[test]
    fn sql_pgq_rejects_whole_entity_columns() {
        let mut builder = QueryBuilder::new();
        builder
            .r#match()
            .node(Some("p"), vec!["Person"])
            .r#return()
            .field("p", "", None::<String>);

        let (arena, root) = builder.build();
        let mut emitter = SqlPgqEmitter::new("test_graph");
        let err = emitter.visit_query(&arena, root).unwrap_err();

        match err {
            Error::UnsupportedFeature { dialect, feature } => {
                assert_eq!(dialect, "sql_pgq");
                assert!(feature.contains("Whole entity column projection ('p')"));
            }
            other => panic!("Expected UnsupportedFeature, got {other:?}"),
        }
    }

    #[test]
    fn sql_pgq_accepts_single_label_and_property_columns() {
        let mut builder = QueryBuilder::new();
        builder
            .r#match()
            .node(Some("p"), vec!["Person"])
            .r#return()
            .field("p", "name", Some("actor"));

        let (arena, root) = builder.build();
        let mut emitter = SqlPgqEmitter::new("test_graph");
        let compiled = emitter.visit_query(&arena, root).unwrap();

        assert_eq!(
            compiled.statement,
            "SELECT * FROM GRAPH_TABLE (test_graph MATCH (p IS \"Person\") COLUMNS (p.name AS actor))"
        );
    }

    #[test]
    fn sql_pgq_accepts_alternative_relationship_types() {
        let mut builder = QueryBuilder::new();
        builder
            .r#match()
            .node(Some("a"), vec!["Person"])
            .to(vec!["KNOWS", "HATES"], Some("r"))
            .node(Some("b"), vec!["Person"])
            .r#return()
            .field("a", "name", None::<String>)
            .field("b", "name", None::<String>);

        let (arena, root) = builder.build();
        let mut emitter = SqlPgqEmitter::new("test_graph");
        let compiled = emitter.visit_query(&arena, root).unwrap();

        assert_eq!(
            compiled.statement,
            "SELECT * FROM GRAPH_TABLE (test_graph MATCH (a IS \"Person\") -[r IS \"KNOWS\" | \"HATES\"]-> (b IS \"Person\") COLUMNS (a.name, b.name))"
        );
    }

    #[test]
    fn sql_pgq_accepts_node_label_disjunction() {
        let mut builder = QueryBuilder::new();
        let expr = LabelExpression::parse("Person | Company").unwrap();
        builder
            .r#match()
            .node_expr(Some("n"), expr)
            .r#return()
            .field("n", "name", None::<String>);

        let (arena, root) = builder.build();
        let mut emitter = SqlPgqEmitter::new("test_graph");
        let compiled = emitter.visit_query(&arena, root).unwrap();

        assert_eq!(
            compiled.statement,
            "SELECT * FROM GRAPH_TABLE (test_graph MATCH (n IS \"Person\" | \"Company\") COLUMNS (n.name))"
        );
    }

    #[test]
    fn sql_pgq_rejects_node_label_negation_and_complex_expressions() {
        let mut builder = QueryBuilder::new();
        let expr = LabelExpression::parse("!Inactive").unwrap();
        builder
            .r#match()
            .node_expr(Some("n"), expr)
            .r#return()
            .field("n", "name", None::<String>);

        let (arena, root) = builder.build();
        let mut emitter = SqlPgqEmitter::new("test_graph");
        let err = emitter.visit_query(&arena, root).unwrap_err();

        match err {
            Error::UnsupportedFeature { dialect, feature } => {
                assert_eq!(dialect, "sql_pgq");
                assert!(feature.contains("Label expression with negation/conjunction/grouping"));
            }
            other => panic!("Expected UnsupportedFeature, got {other:?}"),
        }
    }

    fn compile_in_predicate(
        right: NodeHandle,
        op: BinaryOp,
        mut builder: QueryBuilder,
    ) -> CompiledQuery {
        let name = builder.prop("p", "name");
        let predicate = builder.binary_expr(name, op, right);
        builder
            .match_node(Some("p"), vec!["Person"])
            .where_predicate(predicate)
            .field("p", "name", None::<String>);
        let (arena, root) = builder.build();
        SqlPgqEmitter::new("g").visit_query(&arena, root).unwrap()
    }

    #[test]
    fn sql_pgq_in_literal_list_uses_parenthesised_values() {
        let mut builder = QueryBuilder::new();
        let alice = builder.literal(LiteralValue::String("Alice".into()));
        let bob = builder.literal(LiteralValue::String("Bob".into()));
        let list = builder.list_literal(vec![alice, bob]);
        let compiled = compile_in_predicate(list, BinaryOp::In, builder);

        assert_eq!(
            compiled.statement,
            "SELECT * FROM GRAPH_TABLE (g MATCH (p IS \"Person\") WHERE p.name IN ($p0, $p1) COLUMNS (p.name))"
        );
        assert_eq!(
            compiled.parameters.get("p0"),
            Some(&LiteralValue::String("Alice".into()))
        );
        assert_eq!(
            compiled.parameters.get("p1"),
            Some(&LiteralValue::String("Bob".into()))
        );
    }

    #[test]
    fn sql_pgq_not_in_literal_list_uses_parenthesised_values() {
        let mut builder = QueryBuilder::new();
        let alice = builder.literal(LiteralValue::String("Alice".into()));
        let list = builder.list_literal(vec![alice]);
        let compiled = compile_in_predicate(list, BinaryOp::NotIn, builder);

        assert!(
            compiled
                .statement
                .contains("WHERE p.name NOT IN ($p0) COLUMNS"),
            "statement was {}",
            compiled.statement
        );
    }

    #[test]
    fn sql_pgq_in_empty_literal_list_is_a_constant() {
        let mut builder = QueryBuilder::new();
        let list = builder.list_literal(vec![]);
        let compiled = compile_in_predicate(list, BinaryOp::In, builder);
        assert!(
            compiled.statement.contains("WHERE FALSE COLUMNS"),
            "statement was {}",
            compiled.statement
        );
        assert!(compiled.parameters.is_empty());

        let mut builder = QueryBuilder::new();
        let list = builder.list_literal(vec![]);
        let compiled = compile_in_predicate(list, BinaryOp::NotIn, builder);
        assert!(
            compiled.statement.contains("WHERE TRUE COLUMNS"),
            "statement was {}",
            compiled.statement
        );
        assert!(compiled.parameters.is_empty());
    }

    #[test]
    fn sql_pgq_in_parameter_uses_any_and_not_in_uses_all() {
        for (op, expected) in [
            (BinaryOp::In, "p.name = ANY($names)"),
            (BinaryOp::NotIn, "p.name <> ALL($names)"),
        ] {
            let mut builder = QueryBuilder::new();
            let param = builder.param("names");
            let compiled = compile_in_predicate(param, op, builder);
            assert!(
                compiled.statement.contains(expected),
                "statement was {}",
                compiled.statement
            );
            assert!(compiled.parameters.is_empty());
        }
    }

    #[test]
    fn sql_pgq_not_over_in_literal_list_stays_negated() {
        let mut builder = QueryBuilder::new();
        let name = builder.prop("p", "name");
        let alice = builder.literal(LiteralValue::String("Alice".into()));
        let list = builder.list_literal(vec![alice]);
        let predicate = builder.binary_expr(name, BinaryOp::In, list);
        let predicate = builder.unary_expr(UnaryOp::Not, predicate);
        builder
            .match_node(Some("p"), vec!["Person"])
            .where_predicate(predicate)
            .field("p", "name", None::<String>);
        let (arena, root) = builder.build();
        let compiled = SqlPgqEmitter::new("g").visit_query(&arena, root).unwrap();

        assert!(
            compiled
                .statement
                .contains("WHERE NOT (p.name IN ($p0)) COLUMNS"),
            "statement was {}",
            compiled.statement
        );
    }

    #[test]
    fn sql_pgq_xor_lowers_without_the_keyword_and_reuses_parameters() {
        let mut builder = QueryBuilder::new();
        let age = builder.prop("p", "age");
        let min = builder.param("min_age");
        let lower = builder.binary_expr(age, BinaryOp::Gt, min);
        let city = builder.prop("p", "city");
        let paris = builder.literal(LiteralValue::String("Paris".into()));
        let upper = builder.binary_expr(city, BinaryOp::Eq, paris);
        let xor = builder.binary_expr(lower, BinaryOp::Xor, upper);
        builder
            .match_node(Some("p"), vec!["Person"])
            .where_predicate(xor)
            .field("p", "name", None::<String>);
        let (arena, root) = builder.build();
        let compiled = SqlPgqEmitter::new("g").visit_query(&arena, root).unwrap();

        assert!(
            compiled.statement.contains(
                "WHERE ((p.age > $min_age) OR (p.city = $p0)) AND NOT ((p.age > $min_age) AND (p.city = $p0))"
            ),
            "statement was {}",
            compiled.statement
        );
        assert!(!compiled.statement.contains(" XOR "));
        assert_eq!(compiled.parameters.len(), 1);
        assert_eq!(
            compiled.parameters.get("p0"),
            Some(&LiteralValue::String("Paris".into()))
        );
    }

    #[test]
    fn sql_pgq_rejects_edge_label_expressions_with_negation_or_conjunction() {
        let mut builder = QueryBuilder::new();
        let expr = LabelExpression::parse("!KNOWS").unwrap();
        builder
            .r#match()
            .node(Some("a"), vec!["Person"])
            .to_expr(expr, Some("r"))
            .node(Some("b"), vec!["Person"])
            .r#return()
            .field("a", "name", None::<String>);

        let (arena, root) = builder.build();
        let mut emitter = SqlPgqEmitter::new("test_graph");
        let err = emitter.visit_query(&arena, root).unwrap_err();

        match err {
            Error::UnsupportedFeature { dialect, feature } => {
                assert_eq!(dialect, "sql_pgq");
                assert!(
                    feature.contains(
                        "Relationship type expression with negation/conjunction/grouping"
                    )
                );
            }
            other => panic!("Expected UnsupportedFeature, got {other:?}"),
        }
    }

    #[test]
    fn sql_pgq_order_by_expression_exposed_in_columns() {
        let mut builder = QueryBuilder::new();
        let age = builder.prop("p", "age");
        let one = builder.literal(LiteralValue::Int64(1));
        let age_plus_one = builder.binary_expr(age, BinaryOp::Add, one);
        builder
            .match_node(Some("p"), vec!["Person"])
            .field("p", "name", None::<String>)
            .field("p", "age", None::<String>)
            .order_by(age_plus_one, true);

        let (arena, root) = builder.build();
        let compiled = SqlPgqEmitter::new("g").visit_query(&arena, root).unwrap();

        assert_eq!(
            compiled.statement,
            "SELECT name, age FROM GRAPH_TABLE (g MATCH (p IS \"Person\") COLUMNS (p.name, p.age, p.age + $p0 AS _ord_0)) ORDER BY _ord_0 ASC NULLS LAST"
        );
        assert_eq!(compiled.parameters.get("p0"), Some(&LiteralValue::Int64(1)));
    }

    #[test]
    fn sql_pgq_order_by_non_returned_property_exposed_in_columns() {
        let mut builder = QueryBuilder::new();
        builder
            .match_node(Some("p"), vec!["Person"])
            .field("p", "name", None::<String>)
            .order_by_asc("p", "age");

        let (arena, root) = builder.build();
        let compiled = SqlPgqEmitter::new("g").visit_query(&arena, root).unwrap();

        assert_eq!(
            compiled.statement,
            "SELECT name FROM GRAPH_TABLE (g MATCH (p IS \"Person\") COLUMNS (p.name, p.age AS _ord_0)) ORDER BY _ord_0 ASC NULLS LAST"
        );
        assert!(compiled.parameters.is_empty());
    }

    #[test]
    fn sql_pgq_order_by_returned_property_stays_select_star() {
        let mut builder = QueryBuilder::new();
        builder
            .match_node(Some("p"), vec!["Person"])
            .field("p", "name", None::<String>)
            .field("p", "age", None::<String>)
            .order_by_asc("p", "age");

        let (arena, root) = builder.build();
        let compiled = SqlPgqEmitter::new("g").visit_query(&arena, root).unwrap();

        assert_eq!(
            compiled.statement,
            "SELECT * FROM GRAPH_TABLE (g MATCH (p IS \"Person\") COLUMNS (p.name, p.age)) ORDER BY age ASC NULLS LAST"
        );
        assert!(compiled.parameters.is_empty());
    }

    #[test]
    fn sql_pgq_order_by_multiple_mixed_keys() {
        let mut builder = QueryBuilder::new();
        let age = builder.prop("p", "age");
        let one = builder.literal(LiteralValue::Int64(1));
        let age_plus_one = builder.binary_expr(age, BinaryOp::Add, one);
        builder
            .match_node(Some("p"), vec!["Person"])
            .field("p", "name", None::<String>)
            .field("p", "city", None::<String>)
            .order_by_asc("p", "age")
            .order_by_desc("p", "city")
            .order_by(age_plus_one, true);

        let (arena, root) = builder.build();
        let compiled = SqlPgqEmitter::new("g").visit_query(&arena, root).unwrap();

        assert_eq!(
            compiled.statement,
            "SELECT name, city FROM GRAPH_TABLE (g MATCH (p IS \"Person\") COLUMNS (p.name, p.city, p.age AS _ord_0, p.age + $p0 AS _ord_1)) ORDER BY _ord_0 ASC NULLS LAST, city DESC NULLS FIRST, _ord_1 ASC NULLS LAST"
        );
        assert_eq!(compiled.parameters.get("p0"), Some(&LiteralValue::Int64(1)));
    }

    #[test]
    fn sql_pgq_order_by_duplicate_non_returned_key_reuses_alias() {
        let mut builder = QueryBuilder::new();
        builder
            .match_node(Some("p"), vec!["Person"])
            .field("p", "name", None::<String>)
            .order_by_asc("p", "age")
            .order_by_desc("p", "age");

        let (arena, root) = builder.build();
        let compiled = SqlPgqEmitter::new("g").visit_query(&arena, root).unwrap();

        assert_eq!(
            compiled.statement,
            "SELECT name FROM GRAPH_TABLE (g MATCH (p IS \"Person\") COLUMNS (p.name, p.age AS _ord_0)) ORDER BY _ord_0 ASC NULLS LAST, _ord_0 DESC NULLS FIRST"
        );
    }

    #[test]
    fn sql_pgq_order_by_asc_emits_nulls_last() {
        let mut builder = QueryBuilder::new();
        builder
            .match_node(Some("p"), vec!["Person"])
            .field("p", "name", None::<String>)
            .order_by_asc("p", "name");

        let (arena, root) = builder.build();
        let compiled = SqlPgqEmitter::new("g").visit_query(&arena, root).unwrap();

        assert_eq!(
            compiled.statement,
            "SELECT * FROM GRAPH_TABLE (g MATCH (p IS \"Person\") COLUMNS (p.name)) ORDER BY name ASC NULLS LAST"
        );
    }

    #[test]
    fn sql_pgq_order_by_desc_emits_nulls_first() {
        let mut builder = QueryBuilder::new();
        builder
            .match_node(Some("p"), vec!["Person"])
            .field("p", "name", None::<String>)
            .order_by_desc("p", "name");

        let (arena, root) = builder.build();
        let compiled = SqlPgqEmitter::new("g").visit_query(&arena, root).unwrap();

        assert_eq!(
            compiled.statement,
            "SELECT * FROM GRAPH_TABLE (g MATCH (p IS \"Person\") COLUMNS (p.name)) ORDER BY name DESC NULLS FIRST"
        );
    }

    #[test]
    fn sql_pgq_order_by_distinct_with_non_returned_key_rejected() {
        let mut builder = QueryBuilder::new();
        builder
            .match_node(Some("p"), vec!["Person"])
            .field("p", "name", None::<String>)
            .distinct(true)
            .order_by_asc("p", "age");

        let (arena, root) = builder.build();
        let err = SqlPgqEmitter::new("g")
            .visit_query(&arena, root)
            .unwrap_err();

        match err {
            Error::UnsupportedFeature { dialect, feature } => {
                assert_eq!(dialect, "sql_pgq");
                assert!(feature.contains("ORDER BY on non-projected expressions with DISTINCT"));
            }
            other => panic!("Expected UnsupportedFeature, got {other:?}"),
        }
    }

    #[test]
    fn sql_pgq_order_by_aggregation_with_non_returned_key_rejected() {
        let mut builder = QueryBuilder::new();
        builder
            .match_node(Some("p"), vec!["Person"])
            .field("p", "city", None::<String>)
            .select_property_aggregate("p", "name", AggregationFunc::Count, Some("total"))
            .order_by_asc("p", "age");

        let (arena, root) = builder.build();
        let err = SqlPgqEmitter::new("g")
            .visit_query(&arena, root)
            .unwrap_err();

        match err {
            Error::UnsupportedFeature { dialect, feature } => {
                assert_eq!(dialect, "sql_pgq");
                assert!(feature.contains("ORDER BY on unprojected expressions with aggregation"));
            }
            other => panic!("Expected UnsupportedFeature, got {other:?}"),
        }
    }
}
