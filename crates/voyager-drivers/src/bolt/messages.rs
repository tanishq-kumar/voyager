//! Bolt message definitions, framing, and request/response serialization.

use bytes::{Bytes, BytesMut};
use std::collections::HashMap;
use std::fmt;

use super::packstream::{BoltValue, PackStream};
use crate::config::Auth;
use crate::error::{NetError, Result};
use crate::params::ParamValue;
use crate::redact::is_sensitive_key;

/// Bolt protocol request messages sent from client to database server.
///
/// `Debug` redacts the `credentials` entry, so a logged request never prints the
/// password.
#[derive(Clone, PartialEq)]
pub enum BoltRequest {
    /// Handshake authentication and metadata initialization (`0x01`).
    Hello {
        /// Client metadata and authentication credentials.
        extra: HashMap<String, BoltValue>,
    },
    /// Bolt v5.1+ separate authentication message (`0x6A`).
    Logon {
        /// Authentication credentials dictionary.
        auth: HashMap<String, BoltValue>,
    },
    /// Bolt v5.1+ de-authentication message (`0x6B`).
    Logoff,
    /// Executes a parameterized query statement (`0x10`).
    Run {
        /// Parameterized Cypher or GQL query string.
        query: String,
        /// Named parameter map.
        params: HashMap<String, BoltValue>,
        /// Extra query configuration (e.g. `db`, `tx_timeout`, `mode`).
        extra: HashMap<String, BoltValue>,
    },
    /// Pulls streamed records resulting from a previously executed query (`0x3F`).
    Pull {
        /// Pull configuration (e.g. `{"n": -1}` for all records, or batch size).
        extra: HashMap<String, BoltValue>,
    },
    /// Discards remaining records from an active query stream (`0x2F`).
    Discard {
        /// Discard configuration.
        extra: HashMap<String, BoltValue>,
    },
    /// Begins an explicit transaction block (`0x11`).
    Begin {
        /// Transaction configuration (e.g. bookmarks, timeout, database).
        extra: HashMap<String, BoltValue>,
    },
    /// Commits an open explicit transaction (`0x12`).
    Commit,
    /// Aborts and rolls back an open explicit transaction (`0x13`).
    Rollback,
    /// Resets the session, interrupting any active query and returning to READY (`0x0F`).
    Reset,
    /// Gracefully closes the connection socket (`0x02`).
    Goodbye,
}

/// Credential dictionary with credential values replaced by `"***"`.
struct RedactedAuth<'a>(&'a HashMap<String, BoltValue>);

impl fmt::Debug for RedactedAuth<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut map = f.debug_map();
        for (key, value) in self.0 {
            if key == "credentials" || is_sensitive_key(key) {
                map.entry(key, &"***");
            } else {
                map.entry(key, value);
            }
        }
        map.finish()
    }
}

impl fmt::Debug for BoltRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Hello { extra } => f
                .debug_struct("Hello")
                .field("extra", &RedactedAuth(extra))
                .finish(),
            Self::Logon { auth } => f
                .debug_struct("Logon")
                .field("auth", &RedactedAuth(auth))
                .finish(),
            Self::Logoff => f.write_str("Logoff"),
            Self::Run {
                query,
                params,
                extra,
            } => f
                .debug_struct("Run")
                .field("query", query)
                .field("params", params)
                .field("extra", extra)
                .finish(),
            Self::Pull { extra } => f.debug_struct("Pull").field("extra", extra).finish(),
            Self::Discard { extra } => f.debug_struct("Discard").field("extra", extra).finish(),
            Self::Begin { extra } => f.debug_struct("Begin").field("extra", extra).finish(),
            Self::Commit => f.write_str("Commit"),
            Self::Rollback => f.write_str("Rollback"),
            Self::Reset => f.write_str("Reset"),
            Self::Goodbye => f.write_str("Goodbye"),
        }
    }
}

fn build_bolt_agent_map() -> HashMap<String, BoltValue> {
    let mut bolt_agent = HashMap::new();
    bolt_agent.insert(
        "product".to_string(),
        BoltValue::String(concat!("voyager-drivers/", env!("CARGO_PKG_VERSION")).to_string()),
    );
    bolt_agent.insert(
        "platform".to_string(),
        BoltValue::String(std::env::consts::OS.to_string()),
    );
    bolt_agent.insert(
        "language".to_string(),
        BoltValue::String("Rust".to_string()),
    );
    bolt_agent
}

fn insert_auth_fields(map: &mut HashMap<String, BoltValue>, auth: &Auth) {
    match auth {
        Auth::Basic {
            username,
            password,
            realm,
        } => {
            map.insert("scheme".to_string(), BoltValue::String("basic".to_string()));
            map.insert("principal".to_string(), BoltValue::String(username.clone()));
            map.insert(
                "credentials".to_string(),
                BoltValue::String(password.clone()),
            );
            if let Some(r) = realm {
                map.insert("realm".to_string(), BoltValue::String(r.clone()));
            }
        }
        Auth::Bearer { token } => {
            map.insert(
                "scheme".to_string(),
                BoltValue::String("bearer".to_string()),
            );
            map.insert("credentials".to_string(), BoltValue::String(token.clone()));
        }
        Auth::None => {
            map.insert("scheme".to_string(), BoltValue::String("none".to_string()));
        }
    }
}

impl BoltRequest {
    /// Creates a Bolt v5.1-v5.2 `HELLO` message (without inline auth or `bolt_agent`).
    pub fn hello_v51(user_agent: &str, routing: Option<HashMap<String, BoltValue>>) -> Self {
        let mut extra = HashMap::new();
        extra.insert(
            "user_agent".to_string(),
            BoltValue::String(user_agent.to_string()),
        );

        if let Some(r) = routing {
            extra.insert("routing".to_string(), BoltValue::Map(r));
        }

        Self::Hello { extra }
    }

    /// Creates a Bolt v5.3+ `HELLO` message (without inline authentication, including `bolt_agent`).
    pub fn hello_v53(user_agent: &str, routing: Option<HashMap<String, BoltValue>>) -> Self {
        let mut extra = HashMap::new();
        extra.insert(
            "user_agent".to_string(),
            BoltValue::String(user_agent.to_string()),
        );
        extra.insert(
            "bolt_agent".to_string(),
            BoltValue::Map(build_bolt_agent_map()),
        );

        if let Some(r) = routing {
            extra.insert("routing".to_string(), BoltValue::Map(r));
        }

        Self::Hello { extra }
    }

    /// Creates a legacy Bolt (<= v5.0) `HELLO` message (with inline authentication).
    pub fn hello_legacy(
        user_agent: &str,
        auth_scheme: Option<(&str, &str)>,
        database: Option<&str>,
    ) -> Self {
        Self::hello_legacy_with_patches(user_agent, auth_scheme, database, &[])
    }

    /// Creates a legacy Bolt (<= v5.0) `HELLO` message with optional `patch_bolt` entries.
    pub fn hello_legacy_with_patches(
        user_agent: &str,
        auth_scheme: Option<(&str, &str)>,
        database: Option<&str>,
        patch_bolt: &[&str],
    ) -> Self {
        let auth = match auth_scheme {
            Some((u, p)) => Auth::Basic {
                username: u.to_string(),
                password: p.to_string(),
                realm: None,
            },
            None => Auth::None,
        };
        Self::hello_legacy_with_auth(user_agent, &auth, database, patch_bolt)
    }

    /// Creates a legacy Bolt (<= v5.0) `HELLO` message supporting all [`Auth`] schemes and `patch_bolt`.
    pub fn hello_legacy_with_auth(
        user_agent: &str,
        auth: &Auth,
        database: Option<&str>,
        patch_bolt: &[&str],
    ) -> Self {
        let mut extra = HashMap::new();
        extra.insert(
            "user_agent".to_string(),
            BoltValue::String(user_agent.to_string()),
        );
        extra.insert(
            "bolt_agent".to_string(),
            BoltValue::Map(build_bolt_agent_map()),
        );

        if !patch_bolt.is_empty() {
            extra.insert(
                "patch_bolt".to_string(),
                BoltValue::List(
                    patch_bolt
                        .iter()
                        .map(|p| BoltValue::String((*p).to_string()))
                        .collect(),
                ),
            );
        }

        insert_auth_fields(&mut extra, auth);

        if let Some(db) = database {
            extra.insert("db".to_string(), BoltValue::String(db.to_string()));
        }

        Self::Hello { extra }
    }

    /// Creates a Bolt v5.1+ `LOGON` request message.
    pub fn logon(auth_scheme: Option<(&str, &str)>) -> Self {
        let auth = match auth_scheme {
            Some((u, p)) => Auth::Basic {
                username: u.to_string(),
                password: p.to_string(),
                realm: None,
            },
            None => Auth::None,
        };
        Self::logon_with_auth(&auth)
    }

    /// Creates a Bolt v5.1+ `LOGON` request message supporting all [`Auth`] schemes.
    pub fn logon_with_auth(auth_scheme: &Auth) -> Self {
        let mut auth = HashMap::new();
        insert_auth_fields(&mut auth, auth_scheme);
        Self::Logon { auth }
    }

    /// Creates a standard `RUN` request message.
    pub fn run(
        query: impl Into<String>,
        params: HashMap<String, BoltValue>,
        database: Option<&str>,
    ) -> Self {
        let mut extra = HashMap::new();
        if let Some(db) = database {
            extra.insert("db".to_string(), BoltValue::String(db.to_string()));
        }

        Self::Run {
            query: query.into(),
            params,
            extra,
        }
    }

    /// Creates a `PULL` message to fetch all remaining records (`n = -1`).
    pub fn pull_all() -> Self {
        let mut extra = HashMap::new();
        extra.insert("n".to_string(), BoltValue::Integer(-1));
        Self::Pull { extra }
    }

    /// Creates a `PULL` message with a specific batch limit.
    pub fn pull_batch(batch_size: i64) -> Self {
        let mut extra = HashMap::new();
        extra.insert("n".to_string(), BoltValue::Integer(batch_size));
        Self::Pull { extra }
    }

    /// Serializes the request into a PackStream binary byte buffer.
    pub fn encode(&self, buf: &mut BytesMut) {
        match self {
            Self::Hello { extra } => {
                PackStream::encode_struct_header(0x01, 1, buf);
                PackStream::encode_map(extra, buf);
            }
            Self::Logon { auth } => {
                PackStream::encode_struct_header(0x6A, 1, buf);
                PackStream::encode_map(auth, buf);
            }
            Self::Logoff => {
                PackStream::encode_struct_header(0x6B, 0, buf);
            }
            Self::Run {
                query,
                params,
                extra,
            } => {
                PackStream::encode_struct_header(0x10, 3, buf);
                PackStream::encode_string(query, buf);
                PackStream::encode_map(params, buf);
                PackStream::encode_map(extra, buf);
            }
            Self::Pull { extra } => {
                PackStream::encode_struct_header(0x3F, 1, buf);
                PackStream::encode_map(extra, buf);
            }
            Self::Discard { extra } => {
                PackStream::encode_struct_header(0x2F, 1, buf);
                PackStream::encode_map(extra, buf);
            }
            Self::Begin { extra } => {
                PackStream::encode_struct_header(0x11, 1, buf);
                PackStream::encode_map(extra, buf);
            }
            Self::Commit => {
                PackStream::encode_struct_header(0x12, 0, buf);
            }
            Self::Rollback => {
                PackStream::encode_struct_header(0x13, 0, buf);
            }
            Self::Reset => {
                PackStream::encode_struct_header(0x0F, 0, buf);
            }
            Self::Goodbye => {
                PackStream::encode_struct_header(0x02, 0, buf);
            }
        }
    }
}

/// Encodes a Bolt `RUN` message directly from borrowed inputs into `buf` without
/// allocating intermediate `String`, `HashMap`, or `BoltValue` copies.
pub(crate) fn encode_run_message(
    query: &str,
    params: &HashMap<String, ParamValue>,
    temporal_major: u8,
    db: Option<&str>,
    tx_timeout_ms: Option<i64>,
    buf: &mut BytesMut,
) -> Result<()> {
    PackStream::encode_struct_header(0x10, 3, buf);
    PackStream::encode_string(query, buf);
    PackStream::encode_param_map(params, temporal_major, buf)?;
    let extra_len = usize::from(db.is_some()) + usize::from(tx_timeout_ms.is_some());
    PackStream::encode_map_header(extra_len, buf);
    if let Some(db_name) = db {
        PackStream::encode_string("db", buf);
        PackStream::encode_string(db_name, buf);
    }
    if let Some(ms) = tx_timeout_ms {
        PackStream::encode_string("tx_timeout", buf);
        PackStream::encode_integer(ms, buf);
    }
    Ok(())
}

/// Encodes a Bolt `PULL {"n": -1}` message directly into `buf` without allocating.
#[inline]
pub(crate) fn encode_pull_all_message(buf: &mut BytesMut) {
    use bytes::BufMut;
    // TinyStruct(1) tag=0x3F, TinyMap(1), TinyString("n"), TinyInt(-1)
    buf.put_slice(&[0xB1, 0x3F, 0xA1, 0x81, b'n', 0xFF]);
}

#[inline]
fn encode_stream_control_message(tag: u8, n: i64, qid: Option<i64>, buf: &mut BytesMut) {
    PackStream::encode_struct_header(tag, 1, buf);
    let map_len = 1 + usize::from(qid.is_some());
    PackStream::encode_map_header(map_len, buf);
    PackStream::encode_string("n", buf);
    PackStream::encode_integer(n, buf);
    if let Some(qid) = qid {
        PackStream::encode_string("qid", buf);
        PackStream::encode_integer(qid, buf);
    }
}

/// Encodes a Bolt `PULL {"n": n, ["qid": qid]}` message directly into `buf` without allocating.
#[inline]
pub(crate) fn encode_pull_message(n: i64, qid: Option<i64>, buf: &mut BytesMut) {
    encode_stream_control_message(0x3F, n, qid, buf);
}

/// Encodes a Bolt `DISCARD {"n": n, ["qid": qid]}` message directly into `buf` without allocating.
#[inline]
pub(crate) fn encode_discard_message(n: i64, qid: Option<i64>, buf: &mut BytesMut) {
    encode_stream_control_message(0x2F, n, qid, buf);
}

/// Bolt protocol response messages returned by the database server.
#[derive(Debug, Clone, PartialEq)]
pub enum BoltResponse {
    /// Operation succeeded with metadata (`0x70`).
    Success {
        /// Metadata map (e.g. column `fields`, execution `stats`, server version).
        metadata: HashMap<String, BoltValue>,
    },
    /// A single record row returned by `PULL` (`0x71`).
    Record {
        /// Ordered field values matching columns declared in `SUCCESS.fields`.
        fields: Vec<BoltValue>,
    },
    /// Operation failed with error code and description (`0x7F`).
    Failure {
        /// Error metadata map (contains `"code"` and `"message"`).
        metadata: HashMap<String, BoltValue>,
    },
    /// Pipelined message was ignored due to an earlier error in the batch (`0x7E`).
    Ignored {
        /// Ignored metadata.
        metadata: HashMap<String, BoltValue>,
    },
}

impl BoltResponse {
    /// Decodes a `BoltResponse` from a PackStream byte buffer.
    pub fn decode(buf: &mut Bytes) -> Result<Self> {
        let value = PackStream::decode(buf)?;
        match value {
            BoltValue::Structure { tag, mut fields } => match tag {
                0x70 => {
                    let metadata = if let Some(BoltValue::Map(m)) = fields.pop() {
                        m
                    } else {
                        HashMap::new()
                    };
                    Ok(Self::Success { metadata })
                }
                0x71 => {
                    let record_fields = if let Some(BoltValue::List(l)) = fields.pop() {
                        l
                    } else {
                        fields
                    };
                    Ok(Self::Record {
                        fields: record_fields,
                    })
                }
                0x7F => {
                    let metadata = if let Some(BoltValue::Map(m)) = fields.pop() {
                        m
                    } else {
                        HashMap::new()
                    };
                    Ok(Self::Failure { metadata })
                }
                0x7E => {
                    let metadata = if let Some(BoltValue::Map(m)) = fields.pop() {
                        m
                    } else {
                        HashMap::new()
                    };
                    Ok(Self::Ignored { metadata })
                }
                unknown => Err(NetError::ProtocolError(format!(
                    "Unexpected Bolt response message structure tag 0x{:02X}",
                    unknown
                ))),
            },
            other => Err(NetError::ProtocolError(format!(
                "Expected Bolt response structure, got: {:?}",
                other
            ))),
        }
    }

    /// Returns the error message string if this is a `Failure` response.
    pub fn failure_message(&self) -> Option<(String, String)> {
        match self {
            Self::Failure { metadata } => {
                let code = metadata
                    .get("code")
                    .and_then(|v| v.as_str())
                    .unwrap_or("UnknownError")
                    .to_string();
                let message = metadata
                    .get("message")
                    .and_then(|v| v.as_str())
                    .unwrap_or("An unknown error occurred")
                    .to_string();
                Some((code, message))
            }
            _ => None,
        }
    }

    /// Returns the projected column names if this is a `SUCCESS` response declaring fields.
    pub fn fields(&self) -> Option<Vec<String>> {
        match self {
            Self::Success { metadata } => {
                metadata.get("fields").and_then(|v| v.as_list()).map(|l| {
                    l.iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect()
                })
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logon_debug_redacts_credentials() {
        let request = BoltRequest::logon(Some(("neo4j", "s3cr3t-326")));
        let rendered = format!("{request:?}");
        assert!(!rendered.contains("s3cr3t-326"), "{rendered}");
        assert!(rendered.contains("principal"), "{rendered}");
        assert!(rendered.contains("scheme"), "{rendered}");
        assert!(rendered.contains("\"credentials\": \"***\""), "{rendered}");
    }

    #[test]
    fn legacy_hello_debug_redacts_credentials() {
        let request = BoltRequest::hello_legacy(
            "Voyager-OGM/test",
            Some(("neo4j", "s3cr3t-326")),
            Some("neo4j"),
        );
        let rendered = format!("{request:?}");
        assert!(!rendered.contains("s3cr3t-326"), "{rendered}");
        assert!(rendered.contains("\"credentials\": \"***\""), "{rendered}");
        assert!(rendered.contains("\"db\": String(\"neo4j\")"), "{rendered}");
    }
}
