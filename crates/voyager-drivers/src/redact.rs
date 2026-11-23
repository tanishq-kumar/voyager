//! Credential redaction helpers for `Debug` output and user-facing messages.
//!
//! Nothing in this module ever renders a configured secret: a value is either
//! `None` (not set) or the literal `"***"`, so it stays clear whether a
//! credential is configured without disclosing it.

use std::collections::HashMap;
use std::fmt;

/// A credential rendered as `"***"`, or `None` when it is empty.
pub(crate) struct Secret<'a>(pub(crate) &'a str);

impl fmt::Debug for Secret<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.is_empty() {
            f.write_str("None")
        } else {
            f.write_str("\"***\"")
        }
    }
}

/// A credential byte string rendered as `"***"`, or `None` when it is empty.
pub(crate) struct SecretBytes<'a>(pub(crate) &'a [u8]);

impl fmt::Debug for SecretBytes<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.is_empty() {
            f.write_str("None")
        } else {
            f.write_str("\"***\"")
        }
    }
}

/// Query parameters with credential-looking values replaced by `"***"`.
pub(crate) struct RedactedParams<'a>(pub(crate) &'a HashMap<String, String>);

impl fmt::Debug for RedactedParams<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut map = f.debug_map();
        for (key, value) in self.0 {
            if is_sensitive_key(key) && !value.is_empty() {
                map.entry(key, &"***");
            } else {
                map.entry(key, value);
            }
        }
        map.finish()
    }
}

/// Whether a field or parameter name conventionally carries a credential.
pub(crate) fn is_sensitive_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    key.contains("password") || key.contains("token") || key.contains("secret")
}

/// Replaces a URI password (in the userinfo or a credential-named query
/// parameter) with `***`, keeping username, host, port, path, and every other
/// query parameter readable.
pub(crate) fn redact_uri(raw: &str) -> String {
    let mut redacted = redact_userinfo(raw);
    redact_query_secrets(&mut redacted);
    redacted
}

fn redact_userinfo(raw: &str) -> String {
    let Some(scheme_end) = raw.find("://") else {
        return raw.to_string();
    };
    let authority_start = scheme_end + 3;
    let rest = &raw[authority_start..];
    let authority_len = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let authority = &rest[..authority_len];
    let Some(at) = authority.rfind('@') else {
        return raw.to_string();
    };
    let userinfo = &authority[..at];
    let Some(colon) = userinfo.find(':') else {
        return raw.to_string();
    };

    format!(
        "{}{}:***{}",
        &raw[..authority_start],
        &userinfo[..colon],
        &raw[authority_start + at..]
    )
}

fn redact_query_secrets(raw: &mut String) {
    let Some(query_start) = raw.find('?') else {
        return;
    };
    let mut redacted = String::with_capacity(raw.len());
    redacted.push_str(&raw[..=query_start]);
    for (index, pair) in raw[query_start + 1..].split('&').enumerate() {
        if index > 0 {
            redacted.push('&');
        }
        match pair.split_once('=') {
            Some((key, value)) if is_sensitive_key(key) && !value.is_empty() => {
                redacted.push_str(key);
                redacted.push_str("=***");
            }
            _ => redacted.push_str(pair),
        }
    }
    *raw = redacted;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_debug_shows_none_when_absent() {
        assert_eq!(format!("{:?}", Secret("")), "None");
        assert_eq!(format!("{:?}", Secret("s3cr3t-326")), "\"***\"");
    }

    #[test]
    fn redact_uri_masks_userinfo_password_only() {
        assert_eq!(
            redact_uri("bolt://neo4j:s3cr3t-326@db:7687/movies"),
            "bolt://neo4j:***@db:7687/movies"
        );
        assert_eq!(
            redact_uri("voyager+bridge:postgresql://user:pw@[::1]:5456/graph?sslmode=require"),
            "voyager+bridge:postgresql://user:***@[::1]:5456/graph?sslmode=require"
        );
        assert_eq!(redact_uri("bolt://neo4j@db:7687"), "bolt://neo4j@db:7687");
        assert_eq!(redact_uri("duckdb://graph.db"), "duckdb://graph.db");
        assert_eq!(redact_uri("plain"), "plain");
    }

    #[test]
    fn redact_uri_masks_sensitive_query_parameters() {
        assert_eq!(
            redact_uri("bolt://db:7687?token=s3cr3t-326&database=movies"),
            "bolt://db:7687?token=***&database=movies"
        );
        assert_eq!(
            redact_uri("bolt://db:7687?password=&ssl=true"),
            "bolt://db:7687?password=&ssl=true"
        );
    }
}
