//! Redis / FalkorDB transaction implementation using `MULTI`, `EXEC`, and `DISCARD`.

use async_trait::async_trait;
use std::collections::HashMap;

use crate::engine::{AsyncTransaction, QueryResult, QuerySummary};
use crate::error::{NetError, Result};
use crate::params::{
    ParamValue, format_datetime_iso, format_iso_date, format_iso_duration, format_iso_time,
    parse_decimal_f64,
};
use crate::redis::connection::{RedisConnection, falkordb_transaction_error};
use crate::redis::graph::FalkorQueryResult;
use crate::redis::resp::RespValue;

/// An active transaction over a Redis connection using `MULTI` / `EXEC` / `DISCARD`.
pub struct RedisTransaction<'a> {
    conn: &'a mut RedisConnection,
    is_active: bool,
    queued_count: usize,
    accumulated_summary: QuerySummary,
}

impl<'a> RedisTransaction<'a> {
    /// The legacy `MULTI` path is intentionally unusable as a transaction.
    pub async fn begin(_conn: &'a mut RedisConnection) -> Result<Self> {
        Err(falkordb_transaction_error())
    }

    /// Returns `true` if this transaction is active and has not been committed or rolled back.
    pub fn is_active(&self) -> bool {
        self.is_active
    }

    /// Executes a parameterized query within the transaction boundary.
    /// In Redis `MULTI` blocks, commands are queued until `EXEC`.
    pub async fn execute(
        &mut self,
        query: &str,
        params: &HashMap<String, ParamValue>,
    ) -> Result<QueryResult> {
        if !self.is_active {
            return Err(NetError::TransactionError(
                "Cannot execute query on inactive transaction".to_string(),
            ));
        }

        let formatted_query = format_cypher_with_params(query, params)?;
        let graph_name = self.conn.default_graph().to_string();

        let resp = self
            .conn
            .execute_raw(&["GRAPH.QUERY", &graph_name, &formatted_query])
            .await?;

        match resp {
            RespValue::SimpleString(s) if s == "QUEUED" => {
                self.queued_count += 1;
                // Since commands are queued until EXEC, return an empty QueryResult for intermediate steps
                Ok(QueryResult::empty(QuerySummary::default()))
            }
            RespValue::Error(err) => Err(NetError::ExecutionError(format!(
                "Failed to queue query in transaction: {}",
                err
            ))),
            other => Err(NetError::ProtocolError(format!(
                "Expected +QUEUED in transaction, got {:?}",
                other
            ))),
        }
    }

    /// Commits the active transaction using `EXEC` and aggregates summary statistics.
    pub async fn commit(mut self) -> Result<QuerySummary> {
        self.commit_internal().await
    }

    /// Aborts and rolls back the active transaction using `DISCARD`.
    pub async fn rollback(mut self) -> Result<()> {
        self.rollback_internal().await
    }

    async fn commit_internal(&mut self) -> Result<QuerySummary> {
        if !self.is_active {
            return Err(NetError::TransactionError(
                "Transaction is already completed".to_string(),
            ));
        }

        let resp = self.conn.execute_raw(&["EXEC"]).await?;
        self.conn.set_in_transaction(false);
        self.is_active = false;

        match resp {
            RespValue::Array(Some(results)) => {
                let mut total_summary = self.accumulated_summary.clone();

                for item in results {
                    if let Ok(res) = FalkorQueryResult::parse(item) {
                        let summary = res.statistics.to_query_summary();
                        total_summary.nodes_created += summary.nodes_created;
                        total_summary.nodes_deleted += summary.nodes_deleted;
                        total_summary.relationships_created += summary.relationships_created;
                        total_summary.relationships_deleted += summary.relationships_deleted;
                        total_summary.properties_set += summary.properties_set;
                        total_summary.labels_added += summary.labels_added;
                        total_summary.labels_removed += summary.labels_removed;
                        total_summary.indexes_added += summary.indexes_added;
                        total_summary.execution_time_ms += summary.execution_time_ms;
                    }
                }

                Ok(total_summary)
            }
            RespValue::Array(None) => Err(NetError::TransactionError(
                "Transaction was aborted (EXEC returned null)".to_string(),
            )),
            RespValue::Error(err) => Err(NetError::TransactionError(format!(
                "Transaction EXEC failed: {}",
                err
            ))),
            other => Err(NetError::ProtocolError(format!(
                "Unexpected response to EXEC: {:?}",
                other
            ))),
        }
    }

    async fn rollback_internal(&mut self) -> Result<()> {
        if !self.is_active {
            return Ok(());
        }

        let resp = self.conn.execute_raw(&["DISCARD"]).await?;
        self.conn.set_in_transaction(false);
        self.is_active = false;

        match resp {
            RespValue::SimpleString(s) if s == "OK" => Ok(()),
            RespValue::Error(err) => Err(NetError::TransactionError(format!(
                "Failed to discard transaction: {}",
                err
            ))),
            other => Err(NetError::ProtocolError(format!(
                "Unexpected response to DISCARD: {:?}",
                other
            ))),
        }
    }
}

#[async_trait]
impl<'a> AsyncTransaction for RedisTransaction<'a> {
    async fn execute(
        &mut self,
        query: &str,
        params: &HashMap<String, ParamValue>,
    ) -> Result<QueryResult> {
        self.execute(query, params).await
    }

    async fn commit(mut self: Box<Self>) -> Result<QuerySummary> {
        self.commit_internal().await
    }

    async fn rollback(mut self: Box<Self>) -> Result<()> {
        self.rollback_internal().await
    }
}

/// Helper function to determine if a string is a plain Cypher identifier
/// (starts with an ASCII letter or underscore, followed by ASCII alphanumeric or underscore).
fn is_plain_identifier(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Renders a map key or parameter name for FalkorDB Cypher syntax.
///
/// Plain identifiers are emitted unquoted. Non-plain identifiers are quoted in backticks.
/// Keys containing backticks or null bytes are rejected with an error, as FalkorDB parameter
/// parsing cannot escape backticks within backticked identifier tokens.
fn render_identifier_or_key(key: &str, out: &mut String) -> Result<()> {
    if key.contains('`') {
        return Err(NetError::ExecutionError(format!(
            "FalkorDB Cypher parameter key cannot contain backticks: {key:?}"
        )));
    }
    if key.contains('\0') {
        return Err(NetError::ExecutionError(
            "FalkorDB Cypher parameter key cannot contain null bytes".to_string(),
        ));
    }
    if is_plain_identifier(key) {
        out.push_str(key);
    } else {
        out.push('`');
        out.push_str(key);
        out.push('`');
    }
    Ok(())
}

/// Helper to emit a double-quoted Cypher string literal with backslash escaping.
fn push_cypher_string(s: &str, out: &mut String) {
    out.push('"');
    for ch in s.chars() {
        match ch {
            '"' => out.push_str(r#"\""#),
            '\\' => out.push_str(r"\\"),
            '\n' => out.push_str(r"\n"),
            '\r' => out.push_str(r"\r"),
            '\t' => out.push_str(r"\t"),
            c => out.push(c),
        }
    }
    out.push('"');
}

/// Recursively renders a JSON value as a Cypher literal for FalkorDB's `CYPHER name=value` prefix.
fn render_json_cypher_literal(val: &serde_json::Value, out: &mut String) -> Result<()> {
    match val {
        serde_json::Value::Null => {
            out.push_str("null");
            Ok(())
        }
        serde_json::Value::Bool(b) => {
            out.push_str(if *b { "true" } else { "false" });
            Ok(())
        }
        serde_json::Value::Number(n) => {
            out.push_str(&n.to_string());
            Ok(())
        }
        serde_json::Value::String(s) => {
            push_cypher_string(s, out);
            Ok(())
        }
        serde_json::Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                render_json_cypher_literal(item, out)?;
            }
            out.push(']');
            Ok(())
        }
        serde_json::Value::Object(map) => {
            out.push('{');
            let mut entries: Vec<(&String, &serde_json::Value)> = map.iter().collect();
            entries.sort_by(|a, b| a.0.cmp(b.0));
            for (i, (key, value)) in entries.into_iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                render_identifier_or_key(key, out)?;
                out.push_str(": ");
                render_json_cypher_literal(value, out)?;
            }
            out.push('}');
            Ok(())
        }
    }
}

/// Recursively renders a typed [`ParamValue`] as a Cypher literal for FalkorDB's `CYPHER name=value` prefix.
///
/// Matches [`FalkorDBBridge`]'s `_adapt_falkordb_value` for temporal types (ISO strings) and
/// decimals (numeric literals when finite).
fn render_cypher_literal(val: &ParamValue, out: &mut String) -> Result<()> {
    match val {
        ParamValue::Json(v) => render_json_cypher_literal(v, out),
        ParamValue::Float(f) => {
            if f.is_finite() {
                let s = f.to_string();
                out.push_str(&s);
                if !s.contains('.') && !s.contains('e') && !s.contains('E') {
                    out.push_str(".0");
                }
                Ok(())
            } else {
                Err(NetError::ExecutionError(
                    "FalkorDB Cypher parameter header does not support NaN or Infinity float literals"
                        .to_string(),
                ))
            }
        }
        ParamValue::Bytes(_) => Err(NetError::ExecutionError(
            "FalkorDB Cypher does not support binary bytes parameters".to_string(),
        )),
        ParamValue::Date { year, month, day } => {
            push_cypher_string(&format_iso_date(*year, *month, *day), out);
            Ok(())
        }
        ParamValue::Time {
            hour,
            minute,
            second,
            nanosecond,
            tz_offset_seconds,
        } => {
            push_cypher_string(
                &format_iso_time(*hour, *minute, *second, *nanosecond, *tz_offset_seconds),
                out,
            );
            Ok(())
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
            push_cypher_string(&text, out);
            Ok(())
        }
        ParamValue::DateTime {
            utc_seconds,
            nanosecond,
            tz,
        } => {
            let text = format_datetime_iso(*utc_seconds, *nanosecond, Some(tz.offset_seconds()));
            push_cypher_string(&text, out);
            Ok(())
        }
        ParamValue::Duration {
            months,
            days,
            seconds,
            nanoseconds,
        } => {
            let text = format_iso_duration(*months, *days, *seconds, *nanoseconds);
            push_cypher_string(&text, out);
            Ok(())
        }
        ParamValue::Uuid(s) => {
            push_cypher_string(s, out);
            Ok(())
        }
        ParamValue::Decimal(s) => {
            let f = parse_decimal_f64(s).ok_or_else(|| {
                NetError::ExecutionError(format!("Invalid decimal parameter literal: {s:?}"))
            })?;
            render_cypher_literal(&ParamValue::Float(f), out)
        }
        ParamValue::Point2D { srid, x, y } => {
            if *srid == 4326 {
                out.push_str(&format!("{{latitude: {y}, longitude: {x}}}"));
            } else {
                out.push_str(&format!("{{srid: {srid}, x: {x}, y: {y}}}"));
            }
            Ok(())
        }
        ParamValue::Point3D { srid, x, y, z } => {
            if *srid == 4979 {
                out.push_str(&format!("{{latitude: {y}, longitude: {x}, height: {z}}}"));
            } else {
                out.push_str(&format!("{{srid: {srid}, x: {x}, y: {y}, z: {z}}}"));
            }
            Ok(())
        }
        ParamValue::List(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                render_cypher_literal(item, out)?;
            }
            out.push(']');
            Ok(())
        }
        ParamValue::Map(map) => {
            out.push('{');
            let mut entries: Vec<(&String, &ParamValue)> = map.iter().collect();
            entries.sort_by(|a, b| a.0.cmp(b.0));
            for (i, (key, value)) in entries.into_iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                render_identifier_or_key(key, out)?;
                out.push_str(": ");
                render_cypher_literal(value, out)?;
            }
            out.push('}');
            Ok(())
        }
    }
}

/// Helper function to prefix a Cypher query with parameters in FalkorDB's native syntax.
pub fn format_cypher_with_params(
    query: &str,
    params: &HashMap<String, ParamValue>,
) -> Result<String> {
    if params.is_empty() {
        return Ok(query.to_string());
    }

    let mut keys: Vec<&String> = params.keys().collect();
    keys.sort();

    let mut parts = Vec::with_capacity(params.len());
    for k in keys {
        let param_name = k.strip_prefix('$').unwrap_or(k);
        let mut key_buf = String::new();
        render_identifier_or_key(param_name, &mut key_buf)?;
        let mut val_buf = String::new();
        render_cypher_literal(&params[k], &mut val_buf)?;
        parts.push(format!("{key_buf}={val_buf}"));
    }

    Ok(format!("CYPHER {} {}", parts.join(" "), query))
}
