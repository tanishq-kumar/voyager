//! AST-level variable scope validation for fluent queries.
//!
//! The Query Spec consumer validates variable scope against the document before
//! it builds an AST (see `spec::import`). The fluent builder compiles an AST
//! directly, so it needs the same rules applied to the finished statement. This
//! module walks the AST in the fixed emission order the emitters use and
//! rejects a variable reference that cannot resolve at that point with
//! [`ErrorCode::InvalidClause`](crate::error::ErrorCode::InvalidClause).
//!
//! The rules mirror the Query Spec walker:
//!
//! - `LOAD CSV` and `UNWIND` aliases are bound from their clause onward.
//! - a match path binds its node, edge and path variables; its `WHERE` sees
//!   those plus everything bound before the match;
//! - a linear `LET` binds its variable from that entry onward and `FILTER` sees
//!   the entries before it;
//! - a `WITH` keeps only the names it projects, but its own `WHERE`/`ORDER BY`
//!   also see the scope before the `WITH` unless the clause is `DISTINCT` or
//!   contains an aggregation;
//! - mutations and `RETURN` see the scope after the last `WITH` (plus names the
//!   earlier mutations created); root `ORDER BY` additionally sees the names
//!   `RETURN` projects;
//! - `EXISTS`/`COUNT` subqueries, pattern comprehensions and list
//!   comprehensions bind their own variables locally while still being allowed
//!   to read outer names;
//! - parameters and literals never need a binding.
//!
//! Names that are not plain identifiers (for example an opaque raw fragment a
//! caller smuggled in through a string) are never rejected; the shared
//! identifier validator or the server decides what they mean. Two fluent
//! spells cannot be resolved from the AST and are treated as opaque on
//! purpose: a bare raw projection string such as `return_("name")` (an
//! empty-property field) and a bare identifier passed directly to a function,
//! such as a raw path variable in `fn.nodes(path_var)`. A dotted raw string
//! (`return_("q.name")`) is a property reference and is resolved normally.

use std::collections::HashSet;

use crate::ast::{AstNode, NodeHandle, ProjectionItem, QueryAstArena};
use crate::error::{Error, Result};

/// Expression nesting budget mirrored from the Query Spec consumer.
const MAX_SCOPE_DEPTH: usize = 256;

/// Variable names visible at one point of the emitted statement.
#[derive(Clone)]
struct Scope {
    names: HashSet<String>,
    valid_identifiers: bool,
}

impl Scope {
    fn new() -> Self {
        Self {
            names: HashSet::new(),
            valid_identifiers: true,
        }
    }

    fn empty(valid_identifiers: bool) -> Self {
        Self {
            names: HashSet::new(),
            valid_identifiers,
        }
    }

    fn contains(&self, name: &str) -> bool {
        self.names.contains(name)
    }

    fn bind(&mut self, name: &str) {
        if name.is_empty() {
            return;
        }
        if !is_plain_identifier(name) {
            // Defer non-identifier names to the shared identifier validator,
            // exactly as the spec consumer does.
            self.valid_identifiers = false;
            return;
        }
        self.names.insert(name.to_string());
    }

    fn include(&mut self, other: &Scope) {
        self.names.extend(other.names.iter().cloned());
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

/// Where a variable reference sits in the fixed emission order.
#[derive(Clone, Copy)]
enum Clause {
    Unwind,
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

fn missing_variable(name: &str, clause: Clause, last_with: Option<usize>) -> Error {
    let reason = match clause {
        Clause::Unwind => {
            "the UNWIND expression can only read names bound by an earlier clause".to_string()
        }
        Clause::Match => {
            "MATCH clauses can only read variables bound by an earlier LOAD CSV, UNWIND or MATCH path"
                .to_string()
        }
        Clause::LinearLet => {
            "a linear LET binds its variable only from that entry onward".to_string()
        }
        Clause::LinearFilter => {
            "linear FILTER statements are emitted before WITH clauses and can only read names bound before them"
                .to_string()
        }
        Clause::WithProjection { index } => format!(
            "WITH clause {} can only read names bound by the preceding clauses; add '{name}' to an earlier WITH projection or a MATCH path",
            index + 1
        ),
        Clause::WithWhere {
            index,
            only_projected,
        } => {
            if only_projected {
                format!(
                    "WITH clause {} WHERE cannot read '{name}': a WITH with DISTINCT or an aggregation only exposes the names it projects; add '{name}' to that clause's projections or reference a projected alias",
                    index + 1
                )
            } else {
                format!(
                    "WITH clause {} WHERE sees the names that clause projects and the names in scope before it; add '{name}' to that clause's projections or reference a projected alias",
                    index + 1
                )
            }
        }
        Clause::WithOrderBy {
            index,
            only_projected,
        } => {
            if only_projected {
                format!(
                    "WITH clause {} ORDER BY cannot read '{name}': a WITH with DISTINCT or an aggregation only exposes the names it projects; add '{name}' to that clause's projections",
                    index + 1
                )
            } else {
                format!(
                    "WITH clause {} ORDER BY cannot see '{name}' because the WITH neither projects it nor keeps it from before the WITH; add it to that clause's projections",
                    index + 1
                )
            }
        }
        Clause::Mutation => match last_with {
            Some(index) => format!(
                "the mutation cannot read '{name}': the last WITH clause (with_clauses[{index}]) does not project it; add it to that clause's projections"
            ),
            None => "the mutation cannot read it because it is not bound by any preceding clause"
                .to_string(),
        },
        Clause::Return => match last_with {
            Some(index) => format!(
                "RETURN cannot read '{name}': the last WITH clause (with_clauses[{index}]) does not project it; add it to that clause's projections"
            ),
            None => {
                "RETURN cannot read it because it is not bound by any preceding clause".to_string()
            }
        },
        Clause::RootOrderBy { only_projected } => {
            if only_projected {
                "root ORDER BY cannot read it: a RETURN with DISTINCT or an aggregation only exposes the names it projects; reference a projected name".to_string()
            } else if let Some(index) = last_with {
                format!(
                    "root ORDER BY cannot read '{name}': the last WITH clause (with_clauses[{index}]) does not project it; add it to that clause's projections"
                )
            } else {
                "root ORDER BY cannot read it because it is not bound by any preceding clause"
                    .to_string()
            }
        }
    };
    Error::InvalidClause(format!("variable '{name}' is not in scope here: {reason}"))
}

struct ScopeValidator<'a> {
    arena: &'a QueryAstArena,
}

/// Validates every variable reference reachable from `root`.
///
/// The caller must invoke this on the root `QueryStatement`; nested statements
/// receive the enclosing scope while they are walked, so a subquery can read an
/// outer variable without redeclaring it.
pub(crate) fn validate_query_scope(arena: &QueryAstArena, root: NodeHandle) -> Result<()> {
    ScopeValidator { arena }.validate_statement(root, &Scope::new(), 0)
}

impl ScopeValidator<'_> {
    fn ensure_in_scope(
        &self,
        name: &str,
        scope: &Scope,
        clause: Clause,
        last_with: Option<usize>,
    ) -> Result<()> {
        if name.is_empty()
            || !scope.valid_identifiers
            || !is_plain_identifier(name)
            || scope.contains(name)
        {
            return Ok(());
        }
        Err(missing_variable(name, clause, last_with))
    }

    /// The names a projection introduces into the next clause.
    ///
    /// This mirrors the spec consumer: an alias wins, `field(var, "")` is the
    /// bare variable, an unaliased `field(var, prop)` is named `prop`, and an
    /// unaliased aggregation introduces no referenceable name.
    fn projection_output(&self, item: &ProjectionItem) -> Option<String> {
        if let Some(alias) = &item.alias {
            return Some(alias.clone());
        }
        if item.aggregation.is_some() {
            return None;
        }
        match self.arena.get(item.expression).ok()? {
            AstNode::Identifier(name) => Some(name.clone()),
            AstNode::PropertyAccess { target, property } => match self.arena.get(*target).ok()? {
                AstNode::Identifier(variable) if property.is_empty() => Some(variable.clone()),
                AstNode::Identifier(_) => Some(property.clone()),
                _ => None,
            },
            _ => None,
        }
    }

    /// True when `expression` repeats a non-aggregating RETURN projection.
    ///
    /// A `RETURN DISTINCT`/aggregating clause collapses the visible names to
    /// its projected columns, but Cypher still accepts `ORDER BY p.name` when
    /// `p.name` is itself one of those columns.
    fn matches_projection(&self, expression: NodeHandle, projections: &[ProjectionItem]) -> bool {
        projections.iter().any(|item| {
            item.aggregation.is_none()
                && self.expressions_equivalent(expression, item.expression, 0)
        })
    }

    /// Structural expression equality, ignoring arena handle positions.
    fn expressions_equivalent(&self, left: NodeHandle, right: NodeHandle, depth: usize) -> bool {
        if depth > MAX_SCOPE_DEPTH {
            return false;
        }
        let (Ok(left), Ok(right)) = (self.arena.get(left), self.arena.get(right)) else {
            return false;
        };
        match (left, right) {
            (AstNode::Identifier(a), AstNode::Identifier(b)) => a == b,
            (AstNode::Parameter(a), AstNode::Parameter(b)) => a == b,
            (AstNode::Literal(a), AstNode::Literal(b)) => a == b,
            (
                AstNode::PropertyAccess {
                    target: left_target,
                    property: left_property,
                },
                AstNode::PropertyAccess {
                    target: right_target,
                    property: right_property,
                },
            ) => {
                left_property == right_property
                    && self.expressions_equivalent(*left_target, *right_target, depth + 1)
            }
            (
                AstNode::BinaryExpression {
                    left: left_left,
                    op: left_op,
                    right: left_right,
                },
                AstNode::BinaryExpression {
                    left: right_left,
                    op: right_op,
                    right: right_right,
                },
            ) => {
                left_op == right_op
                    && self.expressions_equivalent(*left_left, *right_left, depth + 1)
                    && self.expressions_equivalent(*left_right, *right_right, depth + 1)
            }
            (
                AstNode::UnaryExpression {
                    op: left_op,
                    operand: left_operand,
                },
                AstNode::UnaryExpression {
                    op: right_op,
                    operand: right_operand,
                },
            ) => {
                left_op == right_op
                    && self.expressions_equivalent(*left_operand, *right_operand, depth + 1)
            }
            (
                AstNode::FunctionCall {
                    name: left_name,
                    arguments: left_arguments,
                },
                AstNode::FunctionCall {
                    name: right_name,
                    arguments: right_arguments,
                },
            ) => {
                left_name == right_name
                    && left_arguments.len() == right_arguments.len()
                    && left_arguments
                        .iter()
                        .zip(right_arguments.iter())
                        .all(|(&l, &r)| self.expressions_equivalent(l, r, depth + 1))
            }
            (AstNode::ListLiteral(left_items), AstNode::ListLiteral(right_items)) => {
                left_items.len() == right_items.len()
                    && left_items
                        .iter()
                        .zip(right_items.iter())
                        .all(|(&l, &r)| self.expressions_equivalent(l, r, depth + 1))
            }
            _ => false,
        }
    }

    fn validate_statement(&self, handle: NodeHandle, parent: &Scope, depth: usize) -> Result<()> {
        if depth > MAX_SCOPE_DEPTH {
            return Ok(());
        }
        let node = self.arena.get(handle)?;
        let AstNode::QueryStatement {
            load_csv,
            unwinds,
            matches,
            linear_clauses,
            with_clauses,
            mutations,
            return_clause,
            ..
        } = node
        else {
            return self.validate_subquery_node(handle, parent, depth);
        };

        let mut scope = parent.clone();

        if let Some(load_csv) = load_csv
            && let AstNode::LoadCsvClause { alias, .. } = self.arena.get(*load_csv)?
        {
            scope.bind(alias);
        }

        for &unwind in unwinds {
            if let AstNode::UnwindClause { expression, alias } = self.arena.get(unwind)? {
                self.validate_expr(*expression, &scope, Clause::Unwind, None, depth + 1)?;
                scope.bind(alias);
            }
        }

        for &match_clause in matches {
            self.validate_match_clause(match_clause, &mut scope, depth)?;
        }

        for &entry in linear_clauses {
            match self.arena.get(entry)? {
                AstNode::LetClause {
                    variable,
                    expression,
                } => {
                    self.validate_expr(*expression, &scope, Clause::LinearLet, None, depth + 1)?;
                    scope.bind(variable);
                }
                AstNode::FilterClause { predicate } => {
                    self.validate_expr(*predicate, &scope, Clause::LinearFilter, None, depth + 1)?;
                }
                _ => {}
            }
        }

        let mut last_with: Option<usize> = None;
        for (index, &with_clause) in with_clauses.iter().enumerate() {
            let AstNode::WithClause {
                distinct,
                projections,
                order_by,
                where_clause,
                ..
            } = self.arena.get(with_clause)?
            else {
                continue;
            };
            let before = scope.clone();
            let only_projected =
                *distinct || projections.iter().any(|item| item.aggregation.is_some());
            let mut projected = Scope::empty(before.valid_identifiers);
            for item in projections {
                self.validate_projection_item(
                    item,
                    &before,
                    Clause::WithProjection { index },
                    last_with,
                    depth,
                )?;
                if let Some(name) = self.projection_output(item) {
                    projected.bind(&name);
                }
            }
            if let Some(where_clause) = where_clause {
                let mut where_scope = projected.clone();
                if !only_projected {
                    where_scope.include(&before);
                }
                self.validate_where(
                    *where_clause,
                    &where_scope,
                    Clause::WithWhere {
                        index,
                        only_projected,
                    },
                    last_with,
                    depth,
                )?;
            }
            for &(expression, _) in order_by {
                let mut order_scope = projected.clone();
                if !only_projected {
                    order_scope.include(&before);
                }
                self.validate_expr(
                    expression,
                    &order_scope,
                    Clause::WithOrderBy {
                        index,
                        only_projected,
                    },
                    last_with,
                    depth + 1,
                )?;
            }
            scope = projected;
            last_with = Some(index);
        }

        for &mutation in mutations {
            self.validate_mutation(mutation, &mut scope, last_with, depth)?;
        }

        if let Some(return_clause) = return_clause
            && let AstNode::ReturnClause {
                distinct,
                projections,
                order_by,
                ..
            } = self.arena.get(*return_clause)?
        {
            let mut return_names = Vec::new();
            for item in projections {
                self.validate_projection_item(item, &scope, Clause::Return, last_with, depth)?;
                if let Some(name) = self.projection_output(item) {
                    return_names.push(name);
                }
            }
            let only_projected =
                *distinct || projections.iter().any(|item| item.aggregation.is_some());
            let mut order_scope = if only_projected {
                Scope::empty(scope.valid_identifiers)
            } else {
                scope.clone()
            };
            for name in return_names {
                order_scope.bind(&name);
            }
            for &(expression, _) in order_by {
                if only_projected && self.matches_projection(expression, projections) {
                    // A RETURN DISTINCT/aggregating clause exposes only the
                    // names it projects, but `ORDER BY p.name` is accepted when
                    // `p.name` itself is a projected column (Neo4j evaluates
                    // the repeated expression, not the pre-RETURN variable).
                    continue;
                }
                self.validate_expr(
                    expression,
                    &order_scope,
                    Clause::RootOrderBy { only_projected },
                    last_with,
                    depth + 1,
                )?;
            }
        }

        Ok(())
    }

    fn validate_match_clause(
        &self,
        handle: NodeHandle,
        scope: &mut Scope,
        depth: usize,
    ) -> Result<()> {
        let AstNode::MatchClause {
            paths,
            where_clause,
            ..
        } = self.arena.get(handle)?
        else {
            return Ok(());
        };
        for &path in paths {
            let mut names = Vec::new();
            self.collect_path_names(path, &mut names, depth)?;
            let mut local = scope.clone();
            for name in &names {
                local.bind(name);
            }
            self.validate_path_predicates(path, &local, Clause::Match, None, depth)?;
            for name in &names {
                scope.bind(name);
            }
        }
        if let Some(where_clause) = where_clause {
            self.validate_where(*where_clause, scope, Clause::Match, None, depth)?;
        }
        Ok(())
    }

    fn validate_mutation(
        &self,
        handle: NodeHandle,
        scope: &mut Scope,
        last_with: Option<usize>,
        depth: usize,
    ) -> Result<()> {
        match self.arena.get(handle)? {
            AstNode::CreateClause { paths } => {
                for &path in paths {
                    let mut names = Vec::new();
                    self.collect_path_names(path, &mut names, depth)?;
                    let mut local = scope.clone();
                    for name in &names {
                        local.bind(name);
                    }
                    self.validate_path_predicates(
                        path,
                        &local,
                        Clause::Mutation,
                        last_with,
                        depth,
                    )?;
                    for name in &names {
                        scope.bind(name);
                    }
                }
            }
            AstNode::MergeClause {
                path,
                on_create_set,
                on_match_set,
            } => {
                let mut names = Vec::new();
                self.collect_path_names(*path, &mut names, depth)?;
                let mut local = scope.clone();
                for name in &names {
                    local.bind(name);
                }
                self.validate_path_predicates(*path, &local, Clause::Mutation, last_with, depth)?;
                for name in &names {
                    scope.bind(name);
                }
                for &item in on_create_set.iter().chain(on_match_set.iter()) {
                    self.validate_set_item(item, scope, last_with, depth)?;
                }
            }
            AstNode::SetClause { items } => {
                for &item in items {
                    self.validate_set_item(item, scope, last_with, depth)?;
                }
            }
            AstNode::DeleteClause { targets, .. } => {
                for &target in targets {
                    self.validate_expr(target, scope, Clause::Mutation, last_with, depth + 1)?;
                }
            }
            AstNode::RemoveClause { items } => {
                for &item in items {
                    self.validate_expr(item, scope, Clause::Mutation, last_with, depth + 1)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn validate_set_item(
        &self,
        handle: NodeHandle,
        scope: &Scope,
        last_with: Option<usize>,
        depth: usize,
    ) -> Result<()> {
        let AstNode::SetItem { target, value, .. } = self.arena.get(handle)? else {
            return Ok(());
        };
        self.validate_expr(*target, scope, Clause::Mutation, last_with, depth + 1)?;
        self.validate_expr(*value, scope, Clause::Mutation, last_with, depth + 1)
    }

    fn validate_where(
        &self,
        handle: NodeHandle,
        scope: &Scope,
        clause: Clause,
        last_with: Option<usize>,
        depth: usize,
    ) -> Result<()> {
        if let AstNode::WhereClause { root_predicate } = self.arena.get(handle)? {
            self.validate_expr(*root_predicate, scope, clause, last_with, depth + 1)
        } else {
            self.validate_expr(handle, scope, clause, last_with, depth + 1)
        }
    }

    fn validate_projection_item(
        &self,
        item: &ProjectionItem,
        scope: &Scope,
        clause: Clause,
        last_with: Option<usize>,
        depth: usize,
    ) -> Result<()> {
        self.validate_expr(item.expression, scope, clause, last_with, depth + 1)
    }

    fn validate_subquery_node(
        &self,
        handle: NodeHandle,
        parent: &Scope,
        depth: usize,
    ) -> Result<()> {
        match self.arena.get(handle)? {
            AstNode::QueryStatement { .. } => self.validate_statement(handle, parent, depth + 1),
            AstNode::MatchClause { .. } => {
                let mut scope = parent.clone();
                self.validate_match_clause(handle, &mut scope, depth + 1)
            }
            AstNode::NodePattern { .. }
            | AstNode::EdgePattern { .. }
            | AstNode::PathChain { .. } => {
                let mut names = Vec::new();
                self.collect_path_names(handle, &mut names, depth + 1)?;
                let mut local = parent.clone();
                for name in &names {
                    local.bind(name);
                }
                self.validate_path_predicates(handle, &local, Clause::Match, None, depth + 1)
            }
            _ => Ok(()),
        }
    }

    fn validate_expr(
        &self,
        handle: NodeHandle,
        scope: &Scope,
        clause: Clause,
        last_with: Option<usize>,
        depth: usize,
    ) -> Result<()> {
        if depth > MAX_SCOPE_DEPTH {
            return Ok(());
        }
        match self.arena.get(handle)? {
            AstNode::Identifier(name) => self.ensure_in_scope(name, scope, clause, last_with),
            AstNode::PropertyAccess { target, property } => {
                // `field(var, "")` renders as the bare variable and is also the
                // shape a raw projection string such as `return_("name")`
                // produces. Such a spelling may refer to something the checker
                // cannot see, so it is left to the identifier validator.
                if property.is_empty() {
                    return Ok(());
                }
                self.validate_expr(*target, scope, clause, last_with, depth + 1)
            }
            AstNode::BinaryExpression { left, right, .. } => {
                self.validate_expr(*left, scope, clause, last_with, depth + 1)?;
                self.validate_expr(*right, scope, clause, last_with, depth + 1)
            }
            AstNode::UnaryExpression { operand, .. } => {
                self.validate_expr(*operand, scope, clause, last_with, depth + 1)
            }
            AstNode::FunctionCall { arguments, .. } => {
                for &argument in arguments {
                    // A bare identifier is the shape `ident("name")` and raw
                    // path-function arguments produce (for example
                    // `fn.nodes(path_var)`); the checker cannot know whether
                    // the caller binds it elsewhere, so never reject it.
                    if matches!(self.arena.get(argument)?, AstNode::Identifier(_)) {
                        continue;
                    }
                    self.validate_expr(argument, scope, clause, last_with, depth + 1)?;
                }
                Ok(())
            }
            AstNode::CaseExpression {
                operand,
                when_then_branches,
                else_branch,
            } => {
                if let Some(operand) = operand {
                    self.validate_expr(*operand, scope, clause, last_with, depth + 1)?;
                }
                for (when, then) in when_then_branches {
                    self.validate_expr(*when, scope, clause, last_with, depth + 1)?;
                    self.validate_expr(*then, scope, clause, last_with, depth + 1)?;
                }
                if let Some(else_branch) = else_branch {
                    self.validate_expr(*else_branch, scope, clause, last_with, depth + 1)?;
                }
                Ok(())
            }
            AstNode::ListComprehension {
                variable,
                list_expression,
                where_filter,
                map_expression,
            } => {
                self.validate_expr(*list_expression, scope, clause, last_with, depth + 1)?;
                let mut local = scope.clone();
                local.bind(variable);
                if let Some(where_filter) = where_filter {
                    self.validate_expr(*where_filter, &local, clause, last_with, depth + 1)?;
                }
                if let Some(map_expression) = map_expression {
                    self.validate_expr(*map_expression, &local, clause, last_with, depth + 1)?;
                }
                Ok(())
            }
            AstNode::PatternComprehension {
                path,
                where_filter,
                projection,
            } => {
                // The path is a pattern, not a subquery: its variables are
                // local to the comprehension and its own predicates were
                // already part of the path. Only the filter and projection
                // need checking here.
                let mut local = scope.clone();
                let mut names = Vec::new();
                self.collect_path_names(*path, &mut names, depth + 1)?;
                for name in &names {
                    local.bind(name);
                }
                if let Some(where_filter) = where_filter {
                    self.validate_expr(*where_filter, &local, clause, last_with, depth + 1)?;
                }
                self.validate_expr(*projection, &local, clause, last_with, depth + 1)
            }
            AstNode::ExistsSubquery { subquery } | AstNode::CountSubquery { subquery } => {
                self.validate_subquery_node(*subquery, scope, depth + 1)
            }
            AstNode::ListLiteral(items) => {
                for &item in items {
                    self.validate_expr(item, scope, clause, last_with, depth + 1)?;
                }
                Ok(())
            }
            AstNode::WhereClause { root_predicate } => {
                self.validate_expr(*root_predicate, scope, clause, last_with, depth + 1)
            }
            AstNode::QueryStatement { .. } => self.validate_statement(handle, scope, depth + 1),
            AstNode::MatchClause { .. } => self.validate_subquery_node(handle, scope, depth + 1),
            // Literals, parameters and structural nodes never need a binding.
            _ => Ok(()),
        }
    }

    /// Collects every variable a path binds, including destination nodes under
    /// edge patterns and nested match/statement paths.
    fn collect_path_names(
        &self,
        handle: NodeHandle,
        names: &mut Vec<String>,
        depth: usize,
    ) -> Result<()> {
        if depth > MAX_SCOPE_DEPTH {
            return Ok(());
        }
        match self.arena.get(handle)? {
            AstNode::NodePattern {
                variable: Some(variable),
                ..
            } => names.push(variable.clone()),
            AstNode::NodePattern { variable: None, .. } => {}
            AstNode::EdgePattern {
                variable,
                target_node,
                ..
            } => {
                if let Some(variable) = variable {
                    names.push(variable.clone());
                }
                self.collect_path_names(*target_node, names, depth + 1)?;
            }
            AstNode::PathChain {
                path_variable,
                start_node,
                edges,
                ..
            } => {
                if let Some(path_variable) = path_variable {
                    names.push(path_variable.clone());
                }
                self.collect_path_names(*start_node, names, depth + 1)?;
                for &edge in edges {
                    self.collect_path_names(edge, names, depth + 1)?;
                }
            }
            AstNode::MatchClause { paths, .. } => {
                for &path in paths {
                    self.collect_path_names(path, names, depth + 1)?;
                }
            }
            AstNode::QueryStatement { matches, .. } => {
                for &match_clause in matches {
                    self.collect_path_names(match_clause, names, depth + 1)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Validates the inline property predicates on a path.
    ///
    /// A property map predicate is `var.prop = value` (or `prop = value` on an
    /// anonymous node, where the left side is the property key, not a
    /// variable), so only the right-hand side is a variable reference.
    fn validate_path_predicates(
        &self,
        handle: NodeHandle,
        scope: &Scope,
        clause: Clause,
        last_with: Option<usize>,
        depth: usize,
    ) -> Result<()> {
        if depth > MAX_SCOPE_DEPTH {
            return Ok(());
        }
        match self.arena.get(handle)? {
            AstNode::NodePattern { predicates, .. } => {
                for &predicate in predicates {
                    self.validate_pattern_predicate(
                        predicate,
                        scope,
                        clause,
                        last_with,
                        depth + 1,
                    )?;
                }
            }
            AstNode::EdgePattern {
                predicates,
                target_node,
                ..
            } => {
                for &predicate in predicates {
                    self.validate_pattern_predicate(
                        predicate,
                        scope,
                        clause,
                        last_with,
                        depth + 1,
                    )?;
                }
                self.validate_path_predicates(*target_node, scope, clause, last_with, depth + 1)?;
            }
            AstNode::PathChain {
                start_node, edges, ..
            } => {
                self.validate_path_predicates(*start_node, scope, clause, last_with, depth + 1)?;
                for &edge in edges {
                    self.validate_path_predicates(edge, scope, clause, last_with, depth + 1)?;
                }
            }
            AstNode::MatchClause { paths, .. } => {
                for &path in paths {
                    self.validate_path_predicates(path, scope, clause, last_with, depth + 1)?;
                }
            }
            AstNode::QueryStatement { matches, .. } => {
                for &match_clause in matches {
                    self.validate_path_predicates(
                        match_clause,
                        scope,
                        clause,
                        last_with,
                        depth + 1,
                    )?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn validate_pattern_predicate(
        &self,
        handle: NodeHandle,
        scope: &Scope,
        clause: Clause,
        last_with: Option<usize>,
        depth: usize,
    ) -> Result<()> {
        if let AstNode::BinaryExpression { right, .. } = self.arena.get(handle)? {
            self.validate_expr(*right, scope, clause, last_with, depth + 1)
        } else {
            self.validate_expr(handle, scope, clause, last_with, depth + 1)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::QueryBuilder;
    use crate::error::ErrorCode;

    fn built(builder: QueryBuilder) -> Result<(QueryAstArena, NodeHandle)> {
        builder.try_build()
    }

    fn assert_accepted(builder: QueryBuilder) {
        let (arena, root) = built(builder).expect("query should compile");
        assert!(validate_query_scope(&arena, root).is_ok());
    }

    fn assert_rejected(builder: QueryBuilder, variable: &str, clause: &str) {
        let error = match built(builder) {
            Err(error) => error,
            Ok((arena, root)) => {
                validate_query_scope(&arena, root).expect_err("query should be rejected")
            }
        };
        assert_eq!(error.code(), ErrorCode::InvalidClause);
        let message = error.to_string();
        assert!(
            message.contains(&format!("variable '{variable}'")),
            "message should name the variable: {message}"
        );
        assert!(
            message.contains(clause),
            "message should name the clause {clause:?}: {message}"
        );
    }

    fn person_match() -> QueryBuilder {
        let mut builder = QueryBuilder::new();
        builder.r#match().node(Some("p"), vec!["Person"]);
        builder
    }

    #[test]
    fn with_where_may_read_a_pre_with_name() {
        let mut builder = person_match();
        builder.r#with().field("p", "name", Some("name"));
        builder.where_gt("p", "age", 1);
        builder.r#return().field("name", "", None::<String>);
        assert_accepted(builder);
    }

    #[test]
    fn with_order_by_may_read_a_pre_with_name() {
        let mut builder = person_match();
        builder.r#with().field("p", "name", Some("name"));
        builder.order_by_property("p", "age", true);
        builder.r#return().field("name", "", None::<String>);
        assert_accepted(builder);
    }

    #[test]
    fn with_replaces_scope_with_projected_names() {
        let mut builder = person_match();
        builder.r#with().field("p", "name", Some("name"));
        builder.r#return().field("p", "name", Some("name"));
        assert_rejected(builder, "p", "RETURN");
    }

    #[test]
    fn return_cannot_read_an_unbound_variable() {
        let mut builder = person_match();
        builder.r#return().field("q", "name", None::<String>);
        assert_rejected(builder, "q", "RETURN");
    }

    #[test]
    fn distinct_with_order_by_sees_only_projected_names() {
        let mut builder = person_match();
        builder.r#with().field("p", "name", Some("name"));
        builder.distinct(true);
        builder.order_by_property("p", "age", true);
        builder.r#return().field("p", "name", Some("name"));
        assert_rejected(builder, "p", "WITH clause 1 ORDER BY");
    }

    #[test]
    fn aggregating_with_where_sees_only_projected_names() {
        let mut builder = person_match();
        builder.r#with().field("p", "name", Some("name"));
        let target = builder.ident("p");
        builder.select_aggregate(target, crate::ast::AggregationFunc::Count, Some("c"));
        builder.where_gt("p", "age", 1);
        builder.r#return().field("p", "name", Some("name"));
        assert_rejected(builder, "p", "WITH clause 1 WHERE");
    }

    #[test]
    fn bare_variable_with_keeps_the_variable() {
        let mut builder = person_match();
        builder.r#with().field("p", "", None::<String>);
        builder.r#return().field("p", "name", None::<String>);
        assert_accepted(builder);
    }

    #[test]
    fn mutation_after_with_rejects_a_dropped_match_variable() {
        let mut builder = person_match();
        builder.r#match().node(Some("q"), vec!["Person"]);
        builder.r#with().field("p", "name", Some("name"));
        builder.set_property("q", "age", 30);
        builder.r#return().field("p", "name", Some("name"));
        assert_rejected(builder, "q", "mutation");
    }

    #[test]
    fn mutation_after_with_accepts_a_projected_name() {
        let mut builder = person_match();
        builder.r#with().field("p", "", None::<String>);
        builder.set_property("p", "age", 30);
        builder.r#return().field("p", "name", None::<String>);
        assert_accepted(builder);
    }

    #[test]
    fn variables_created_by_create_are_in_scope_for_return() {
        let mut builder = QueryBuilder::new();
        builder.create().node(Some("n"), vec!["Person"]);
        builder.r#return().field("n", "name", None::<String>);
        assert_accepted(builder);
    }

    #[test]
    fn root_order_by_sees_return_projections() {
        let mut builder = person_match();
        builder.r#return().field("p", "name", Some("name"));
        let projected = builder.ident("name");
        builder.order_by(projected, true);
        assert_accepted(builder);
    }

    #[test]
    fn distinct_return_order_by_accepts_a_projected_expression() {
        let mut builder = person_match();
        builder.r#return().field("p", "name", Some("name"));
        builder.distinct(true);
        builder.order_by_property("p", "name", true);
        assert_accepted(builder);
    }

    #[test]
    fn raw_projection_strings_are_not_rejected() {
        let mut builder = person_match();
        builder.r#return().field("name", "", Some("n"));
        assert_accepted(builder);
    }

    #[test]
    fn bare_identifiers_in_function_arguments_are_not_rejected() {
        let mut builder = person_match();
        let path = builder.ident("p_path");
        let call = builder.function("nodes", vec![path]);
        builder.select_expr(call, Some("nodes"));
        builder.r#return();
        assert_accepted(builder);
    }

    #[test]
    fn distinct_return_order_by_sees_only_projected_names() {
        let mut builder = person_match();
        builder.r#return().field("p", "name", Some("name"));
        builder.distinct(true);
        builder.order_by_property("p", "age", true);
        assert_rejected(builder, "p", "root ORDER BY");
    }

    #[test]
    fn linear_filter_cannot_read_a_later_let() {
        let mut builder = person_match();
        let limit = builder.ident("limit");
        let zero = builder.literal(0);
        let predicate = builder.binary_expr(limit, crate::ast::BinaryOp::Gt, zero);
        builder.filter_(predicate);
        let one = builder.literal(1);
        builder.let_("limit", one);
        builder.r#return().field("p", "name", None::<String>);
        assert_rejected(builder, "limit", "linear FILTER");
    }

    #[test]
    fn linear_let_binds_from_its_own_entry_onward() {
        let mut builder = person_match();
        let one = builder.literal(1);
        builder.let_("limit", one);
        let limit = builder.ident("limit");
        let zero = builder.literal(0);
        let predicate = builder.binary_expr(limit, crate::ast::BinaryOp::Gt, zero);
        builder.filter_(predicate);
        builder.r#return().field("p", "name", None::<String>);
        assert_accepted(builder);
    }

    #[test]
    fn list_comprehension_variable_is_local() {
        let mut builder = person_match();
        let source = builder.ident("p");
        let item = builder.ident("x");
        let list = builder.list_comprehension("x", source, None, Some(item));
        builder.select_expr(list, Some("items"));
        builder.r#return();
        assert_accepted(builder);
    }

    #[test]
    fn pattern_comprehension_variable_does_not_leak() {
        let mut builder = person_match();
        let friend = builder.subquery(|q| {
            q.r#match().node(Some("friend"), vec!["Person"]);
        });
        let projected = builder.ident("friend");
        let comprehension = builder.pattern_comprehension(friend, None, projected);
        builder.select_expr(comprehension, Some("friends"));
        builder.r#return();
        assert_accepted(builder);

        let mut builder = person_match();
        let friend = builder.subquery(|q| {
            q.r#match().node(Some("friend"), vec!["Person"]);
        });
        let projected = builder.ident("friend");
        let comprehension = builder.pattern_comprehension(friend, None, projected);
        builder.select_expr(comprehension, Some("friends"));
        builder.r#return();
        let leaked = builder.ident("friend");
        builder.order_by(leaked, true);
        assert_rejected(builder, "friend", "root ORDER BY");
    }

    #[test]
    fn exists_subquery_can_read_outer_variables() {
        let mut builder = person_match();
        let subquery = builder.subquery(|q| {
            q.r#match().node(Some("q"), vec!["Person"]);
            let left = q.prop("q", "name");
            let right = q.prop("p", "name");
            let predicate = q.binary_expr(left, crate::ast::BinaryOp::Eq, right);
            q.where_expr(predicate);
        });
        let exists = builder.exists_subquery(subquery);
        builder.where_expr(exists);
        builder.r#return().field("p", "name", None::<String>);
        assert_accepted(builder);
    }

    #[test]
    fn exists_subquery_variables_do_not_leak() {
        let mut builder = person_match();
        let subquery = builder.subquery(|q| {
            q.r#match().node(Some("q"), vec!["Person"]);
            q.r#return().field("q", "name", None::<String>);
        });
        let exists = builder.exists_subquery(subquery);
        builder.where_expr(exists);
        builder.r#return().field("q", "name", None::<String>);
        assert_rejected(builder, "q", "RETURN");
    }

    #[test]
    fn parameters_and_literals_are_always_in_scope() {
        let mut builder = person_match();
        let age = builder.prop("p", "age");
        let minimum = builder.param("min_age");
        let predicate = builder.binary_expr(age, crate::ast::BinaryOp::Gt, minimum);
        builder.where_expr(predicate);
        let one = builder.literal(1);
        builder.select_expr(one, Some("one"));
        builder.r#return();
        assert_accepted(builder);
    }

    #[test]
    fn opaque_identifiers_are_never_rejected() {
        let mut builder = person_match();
        builder.r#return().field("*", "", None::<String>);
        assert_accepted(builder);
    }
}
