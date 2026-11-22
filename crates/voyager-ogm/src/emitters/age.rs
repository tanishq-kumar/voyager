//! Apache AGE (PostgreSQL Embedded Cypher) Emitter.

use crate::ast::{AstNode, ExecutionMode, LabelExpression, NodeHandle, QueryAstArena};
use crate::emitters::cypher::CypherEmitter;
use crate::error::{Error, Result};
use crate::visitor::{AstVisitor, CompiledQuery};

/// Default graph name used by Apache AGE queries when none is explicitly configured.
pub const DEFAULT_AGE_GRAPH: &str = "age_graph";

/// Validates whether a graph name is a safe PostgreSQL / Apache AGE identifier.
pub fn is_valid_age_graph_name(name: &str) -> bool {
    if name.is_empty() || name.len() > 63 {
        return false;
    }
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Emits PostgreSQL Apache AGE wrapped Cypher queries:
/// `SELECT * FROM cypher('graph_name', $$ MATCH ... RETURN ... $$) AS (col1 agtype, col2 agtype)`
#[derive(Debug)]
pub struct AgeEmitter {
    graph_name: String,
}

impl AgeEmitter {
    /// Creates a fresh Apache AGE emitter targeting a specific graph name.
    pub fn new(graph_name: impl Into<String>) -> Self {
        Self {
            graph_name: graph_name.into(),
        }
    }

    /// Returns the target graph name.
    pub fn graph_name(&self) -> &str {
        &self.graph_name
    }
}

impl Default for AgeEmitter {
    fn default() -> Self {
        Self::new(DEFAULT_AGE_GRAPH)
    }
}

/// Returns `true` when Cypher text references a `$name` parameter outside
/// string literals, backtick identifiers, and comments.
///
/// Runtime bulk parameters (`UNWIND $batch AS row`) are not part of the compiled
/// literal parameter map, but AGE still needs the third `cypher()` argument to
/// receive them.
fn contains_cypher_parameter_reference(statement: &str) -> bool {
    let bytes = statement.as_bytes();
    let mut i = 0usize;

    while i < bytes.len() {
        match bytes[i] {
            b'\'' => {
                i += 1;
                while i < bytes.len() {
                    match bytes[i] {
                        b'\\' => i += 2,
                        b'\'' => {
                            i += 1;
                            break;
                        }
                        _ => i += 1,
                    }
                }
            }
            b'"' => {
                i += 1;
                while i < bytes.len() {
                    match bytes[i] {
                        b'\\' => i += 2,
                        b'"' => {
                            i += 1;
                            break;
                        }
                        _ => i += 1,
                    }
                }
            }
            b'`' => {
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'`' {
                        i += 1;
                        break;
                    }
                    i += 1;
                }
            }
            b'/' if bytes.get(i + 1) == Some(&b'/') => {
                i += 2;
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                i += 2;
                while i < bytes.len() {
                    if bytes[i] == b'*' && bytes.get(i + 1) == Some(&b'/') {
                        i += 2;
                        break;
                    }
                    i += 1;
                }
            }
            b'$' if bytes
                .get(i + 1)
                .is_some_and(|b| b.is_ascii_alphanumeric() || *b == b'_') =>
            {
                return true;
            }
            _ => i += 1,
        }
    }

    false
}

fn validate_age_ast(arena: &QueryAstArena, root: NodeHandle) -> Result<()> {
    let mut visited = std::collections::HashSet::new();
    validate_age_node(arena, root, &mut visited)
}

fn validate_age_node(
    arena: &QueryAstArena,
    handle: NodeHandle,
    visited: &mut std::collections::HashSet<NodeHandle>,
) -> Result<()> {
    if !visited.insert(handle) {
        return Ok(());
    }
    match arena.get(handle)? {
        AstNode::NodePattern {
            label_expression,
            predicates,
            ..
        } => {
            if let Some(expr) = label_expression {
                if !matches!(expr, LabelExpression::Label(_)) {
                    return Err(Error::UnsupportedFeature {
                        dialect: "age".to_string(),
                        feature: "Node pattern with multiple labels - Apache AGE does not support multiple labels per node pattern".to_string(),
                    });
                }
            }
            for &p in predicates {
                validate_age_node(arena, p, visited)?;
            }
        }
        AstNode::EdgePattern {
            label_expression,
            predicates,
            target_node,
            ..
        } => {
            if let Some(expr) = label_expression {
                if !matches!(expr, LabelExpression::Label(_)) {
                    return Err(Error::UnsupportedFeature {
                        dialect: "age".to_string(),
                        feature: "Relationship pattern with alternative types - Apache AGE does not support relationship type alternation ('|')".to_string(),
                    });
                }
            }
            for &p in predicates {
                validate_age_node(arena, p, visited)?;
            }
            validate_age_node(arena, *target_node, visited)?;
        }
        AstNode::PathChain {
            start_node, edges, ..
        } => {
            validate_age_node(arena, *start_node, visited)?;
            for &e in edges {
                validate_age_node(arena, e, visited)?;
            }
        }
        AstNode::MatchClause {
            paths,
            where_clause,
            ..
        } => {
            for &p in paths {
                validate_age_node(arena, p, visited)?;
            }
            if let Some(wh) = where_clause {
                validate_age_node(arena, *wh, visited)?;
            }
        }
        AstNode::WhereClause { root_predicate } => {
            validate_age_node(arena, *root_predicate, visited)?;
        }
        AstNode::BinaryExpression { left, right, .. } => {
            validate_age_node(arena, *left, visited)?;
            validate_age_node(arena, *right, visited)?;
        }
        AstNode::UnaryExpression { operand, .. } => {
            validate_age_node(arena, *operand, visited)?;
        }
        AstNode::FunctionCall { arguments, .. } => {
            for &arg in arguments {
                validate_age_node(arena, arg, visited)?;
            }
        }
        AstNode::CaseExpression {
            operand,
            when_then_branches,
            else_branch,
        } => {
            if let Some(op) = operand {
                validate_age_node(arena, *op, visited)?;
            }
            for (w, t) in when_then_branches {
                validate_age_node(arena, *w, visited)?;
                validate_age_node(arena, *t, visited)?;
            }
            if let Some(el) = else_branch {
                validate_age_node(arena, *el, visited)?;
            }
        }
        AstNode::ListComprehension {
            list_expression,
            where_filter,
            map_expression,
            ..
        } => {
            validate_age_node(arena, *list_expression, visited)?;
            if let Some(wh) = where_filter {
                validate_age_node(arena, *wh, visited)?;
            }
            if let Some(map) = map_expression {
                validate_age_node(arena, *map, visited)?;
            }
        }
        AstNode::PatternComprehension {
            path,
            where_filter,
            projection,
        } => {
            validate_age_node(arena, *path, visited)?;
            if let Some(wh) = where_filter {
                validate_age_node(arena, *wh, visited)?;
            }
            validate_age_node(arena, *projection, visited)?;
        }
        AstNode::ExistsSubquery { subquery } | AstNode::CountSubquery { subquery } => {
            validate_age_node(arena, *subquery, visited)?;
        }
        AstNode::ListLiteral(items) => {
            for &item in items {
                validate_age_node(arena, item, visited)?;
            }
        }
        AstNode::PropertyAccess { target, .. } => {
            validate_age_node(arena, *target, visited)?;
        }
        AstNode::UnwindClause { expression, .. } => {
            validate_age_node(arena, *expression, visited)?;
        }
        AstNode::CreateClause { paths } => {
            for &p in paths {
                validate_age_node(arena, p, visited)?;
            }
        }
        AstNode::MergeClause {
            path,
            on_create_set,
            on_match_set,
        } => {
            validate_age_node(arena, *path, visited)?;
            for &h in on_create_set {
                validate_age_node(arena, h, visited)?;
            }
            for &h in on_match_set {
                validate_age_node(arena, h, visited)?;
            }
        }
        AstNode::SetClause { items } => {
            for &item in items {
                validate_age_node(arena, item, visited)?;
            }
        }
        AstNode::SetItem { target, value, .. } => {
            validate_age_node(arena, *target, visited)?;
            validate_age_node(arena, *value, visited)?;
        }
        AstNode::DeleteClause { targets, .. } => {
            for &t in targets {
                validate_age_node(arena, t, visited)?;
            }
        }
        AstNode::RemoveClause { items } => {
            for &item in items {
                validate_age_node(arena, item, visited)?;
            }
        }
        AstNode::ReturnClause {
            projections,
            order_by,
            ..
        } => {
            for proj in projections {
                validate_age_node(arena, proj.expression, visited)?;
            }
            for (expr, _) in order_by {
                validate_age_node(arena, *expr, visited)?;
            }
        }
        AstNode::WithClause {
            projections,
            where_clause,
            order_by,
            ..
        } => {
            for proj in projections {
                validate_age_node(arena, proj.expression, visited)?;
            }
            if let Some(wh) = where_clause {
                validate_age_node(arena, *wh, visited)?;
            }
            for (expr, _) in order_by {
                validate_age_node(arena, *expr, visited)?;
            }
        }
        AstNode::ProcedureCall { arguments, .. } => {
            for &arg in arguments {
                validate_age_node(arena, arg, visited)?;
            }
        }
        AstNode::QueryStatement {
            unwinds,
            matches,
            mutations,
            with_clauses,
            return_clause,
            ..
        } => {
            for &u in unwinds {
                validate_age_node(arena, u, visited)?;
            }
            for &m in matches {
                validate_age_node(arena, m, visited)?;
            }
            for &m in mutations {
                validate_age_node(arena, m, visited)?;
            }
            for &w in with_clauses {
                validate_age_node(arena, w, visited)?;
            }
            if let Some(ret) = return_clause {
                validate_age_node(arena, *ret, visited)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn resolve_age_column_base_name(arena: &QueryAstArena, handle: NodeHandle) -> Result<String> {
    match arena.get(handle)? {
        AstNode::PropertyAccess { target, property } => {
            if property.is_empty() {
                match arena.get(*target)? {
                    AstNode::Identifier(id) => Ok(id.clone()),
                    AstNode::NodePattern {
                        variable: Some(var),
                        ..
                    } => Ok(var.clone()),
                    AstNode::EdgePattern {
                        variable: Some(var),
                        ..
                    } => Ok(var.clone()),
                    _ => Ok("entity".to_string()),
                }
            } else {
                Ok(property.clone())
            }
        }
        AstNode::Identifier(id) => {
            if let Some((_, prop)) = id.rsplit_once('.') {
                if !prop.is_empty() {
                    Ok(prop.to_string())
                } else {
                    Ok(id.clone())
                }
            } else {
                Ok(id.clone())
            }
        }
        AstNode::NodePattern {
            variable: Some(var),
            ..
        } => Ok(var.clone()),
        AstNode::EdgePattern {
            variable: Some(var),
            ..
        } => Ok(var.clone()),
        _ => Ok("col".to_string()),
    }
}

fn is_age_whole_entity(arena: &QueryAstArena, handle: NodeHandle) -> Result<bool> {
    Ok(
        matches!(arena.get(handle)?, AstNode::PropertyAccess { property, .. } if property.is_empty()),
    )
}

impl AstVisitor for AgeEmitter {
    fn visit_query(&mut self, arena: &QueryAstArena, root: NodeHandle) -> Result<CompiledQuery> {
        if !is_valid_age_graph_name(&self.graph_name) {
            return Err(crate::error::Error::EmissionError(format!(
                "Invalid Apache AGE graph name '{}'. Graph names must be valid identifiers (alphanumeric and underscores, starting with a letter or underscore, 1 to 63 characters).",
                self.graph_name
            )));
        }
        validate_age_ast(arena, root)?;

        let mut cypher_emitter = CypherEmitter::new().without_execution_mode();
        let cypher_compiled = cypher_emitter.visit_query(arena, root)?;

        let root_node = arena.get(root)?;
        let mut column_defs = Vec::new();
        let mut execution_mode = ExecutionMode::Normal;

        if let AstNode::QueryStatement {
            execution_mode: mode,
            return_clause: Some(ret_handle),
            ..
        } = root_node
        {
            execution_mode = *mode;
            let ret_node = arena.get(*ret_handle)?;
            if let AstNode::ReturnClause { projections, .. } = ret_node {
                // A single returned whole node or relationship keeps the
                // `entity` column key used by earlier 0.4 releases; several get
                // their variable names so the column list stays unique.
                let mut whole_entities = 0usize;
                for proj in projections {
                    if proj.alias.is_none() && is_age_whole_entity(arena, proj.expression)? {
                        whole_entities += 1;
                    }
                }
                let mut used_names = std::collections::HashSet::new();
                for proj in projections {
                    let base_name = if let Some(alias) = &proj.alias {
                        alias.clone()
                    } else if whole_entities == 1 && is_age_whole_entity(arena, proj.expression)? {
                        "entity".to_string()
                    } else {
                        resolve_age_column_base_name(arena, proj.expression)?
                    };
                    let base_name = if base_name.is_empty() {
                        "col".to_string()
                    } else {
                        base_name
                    };
                    let mut col_name = base_name.clone();
                    let mut counter = 1usize;
                    while !used_names.insert(col_name.clone()) {
                        col_name = format!("{base_name}_{counter}");
                        counter += 1;
                    }
                    column_defs.push(format!("{col_name} agtype"));
                }
            }
        } else if let AstNode::QueryStatement {
            execution_mode: mode,
            ..
        } = root_node
        {
            execution_mode = *mode;
        } else if let AstNode::ProcedureCall {
            execution_mode: mode,
            ..
        } = root_node
        {
            execution_mode = *mode;
        }

        let as_clause = if column_defs.is_empty() {
            "AS (result agtype)".to_string()
        } else {
            format!("AS ({})", column_defs.join(", "))
        };

        let params_arg = if cypher_compiled.parameters.is_empty()
            && !contains_cypher_parameter_reference(&cypher_compiled.statement)
        {
            String::new()
        } else {
            ", %s".to_string()
        };

        let prefix = match execution_mode {
            ExecutionMode::Normal => "",
            ExecutionMode::Explain => "EXPLAIN ",
            ExecutionMode::Profile | ExecutionMode::ExplainAndProfile => "EXPLAIN ANALYZE ",
        };

        let wrapped_statement = format!(
            "{}SELECT * FROM cypher('{}', $$ {} $${}) {}",
            prefix, self.graph_name, cypher_compiled.statement, params_arg, as_clause
        );

        Ok(CompiledQuery::with_columns(
            wrapped_statement,
            cypher_compiled.parameters,
            execution_mode,
            cypher_compiled.columns,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::QueryBuilder;
    use crate::visitor::AstVisitor;

    #[test]
    fn detects_runtime_parameters() {
        assert!(contains_cypher_parameter_reference(
            "UNWIND $batch AS row CREATE (n)"
        ));
        assert!(contains_cypher_parameter_reference("RETURN $name"));
        assert!(contains_cypher_parameter_reference("RETURN $_private"));
    }

    #[test]
    fn ignores_parameter_like_text_in_literals_and_comments() {
        assert!(!contains_cypher_parameter_reference("RETURN '$p0'"));
        assert!(!contains_cypher_parameter_reference(r#"RETURN "$p0""#));
        assert!(!contains_cypher_parameter_reference("RETURN `$p0`"));
        assert!(!contains_cypher_parameter_reference("RETURN 1 // $p0"));
        assert!(!contains_cypher_parameter_reference("RETURN 1 /* $p0 */"));
        assert!(!contains_cypher_parameter_reference("RETURN 'it\\'s $p0'"));
    }

    #[test]
    fn age_emitter_unique_column_names_for_multiple_entities() {
        let mut builder = QueryBuilder::new();
        builder
            .r#match()
            .node(Some("a"), vec!["Person"])
            .to(vec!["KNOWS"], Some("r"))
            .node(Some("b"), vec!["Person"])
            .r#return()
            .field("a", "", None::<String>)
            .field("r", "", None::<String>)
            .field("b", "", None::<String>);

        let (arena, root) = builder.build();
        let mut emitter = AgeEmitter::new("test_graph");
        let compiled = emitter.visit_query(&arena, root).unwrap();

        assert_eq!(
            compiled.statement,
            "SELECT * FROM cypher('test_graph', $$ MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a, r, b $$) AS (a agtype, r agtype, b agtype)"
        );
    }

    #[test]
    fn age_emitter_resolves_name_clashes_deterministically() {
        let mut builder = QueryBuilder::new();
        builder
            .r#match()
            .node(Some("a"), vec!["Person"])
            .r#return()
            .field("a", "", None::<String>)
            .field("a", "name", None::<String>)
            .field("a", "", None::<String>)
            .field("a", "name", None::<String>);

        let (arena, root) = builder.build();
        let mut emitter = AgeEmitter::new("test_graph");
        let compiled = emitter.visit_query(&arena, root).unwrap();

        assert_eq!(
            compiled.statement,
            "SELECT * FROM cypher('test_graph', $$ MATCH (a:Person) RETURN a, a.name, a, a.name $$) AS (a agtype, name agtype, a_1 agtype, name_1 agtype)"
        );
    }

    #[test]
    fn age_emitter_rejects_multi_label_node_patterns() {
        let mut builder = QueryBuilder::new();
        builder
            .r#match()
            .node(Some("p"), vec!["Person", "Employee"])
            .r#return()
            .field("p", "name", None::<String>);

        let (arena, root) = builder.build();
        let mut emitter = AgeEmitter::new("test_graph");
        let err = emitter.visit_query(&arena, root).unwrap_err();

        match err {
            Error::UnsupportedFeature { dialect, feature } => {
                assert_eq!(dialect, "age");
                assert!(feature.contains("Node pattern with multiple labels"));
            }
            other => panic!("Expected UnsupportedFeature, got {other:?}"),
        }
    }

    #[test]
    fn age_emitter_rejects_alternative_relationship_types() {
        let mut builder = QueryBuilder::new();
        builder
            .r#match()
            .node(Some("a"), vec!["Person"])
            .to(vec!["KNOWS", "HATES"], Some("r"))
            .node(Some("b"), vec!["Person"])
            .r#return()
            .field("r", "", None::<String>);

        let (arena, root) = builder.build();
        let mut emitter = AgeEmitter::new("test_graph");
        let err = emitter.visit_query(&arena, root).unwrap_err();

        match err {
            Error::UnsupportedFeature { dialect, feature } => {
                assert_eq!(dialect, "age");
                assert!(feature.contains("Relationship pattern with alternative types"));
            }
            other => panic!("Expected UnsupportedFeature, got {other:?}"),
        }
    }
}
