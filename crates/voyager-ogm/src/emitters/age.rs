//! Apache AGE (PostgreSQL Embedded Cypher) Emitter.

use crate::ast::{AstNode, ExecutionMode, NodeHandle, QueryAstArena};
use crate::emitters::cypher::CypherEmitter;
use crate::error::Result;
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

impl AstVisitor for AgeEmitter {
    fn visit_query(&mut self, arena: &QueryAstArena, root: NodeHandle) -> Result<CompiledQuery> {
        if !is_valid_age_graph_name(&self.graph_name) {
            return Err(crate::error::Error::EmissionError(format!(
                "Invalid Apache AGE graph name '{}'. Graph names must be valid identifiers (alphanumeric and underscores, starting with a letter or underscore, 1 to 63 characters).",
                self.graph_name
            )));
        }

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
                let mut used_names = std::collections::HashSet::new();
                for proj in projections {
                    let base_name = if let Some(alias) = &proj.alias {
                        alias.clone()
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
}
