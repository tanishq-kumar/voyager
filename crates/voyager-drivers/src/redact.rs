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
#[cfg(feature = "postgres")]
pub(crate) struct SecretBytes<'a>(pub(crate) &'a [u8]);

#[cfg(feature = "postgres")]
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

pub(crate) fn redact_userinfo(raw: &str) -> String {
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

fn redact_kv_pairs(segment: &str, out: &mut String) {
    for (index, pair) in segment.split('&').enumerate() {
        if index > 0 {
            out.push('&');
        }
        match pair.split_once('=') {
            Some((key, value)) if is_sensitive_key(key) && !value.is_empty() => {
                out.push_str(key);
                out.push_str("=***");
            }
            _ => out.push_str(pair),
        }
    }
}

fn redact_query_secrets(raw: &mut String) {
    let (before_frag, frag_opt) = match raw.split_once('#') {
        Some((before, frag)) => (before, Some(frag)),
        None => (raw.as_str(), None),
    };
    if !before_frag.contains('?') && frag_opt.is_none() {
        return;
    }

    let mut redacted = String::with_capacity(raw.len());
    if let Some((prefix, query)) = before_frag.split_once('?') {
        redacted.push_str(prefix);
        redacted.push('?');
        redact_kv_pairs(query, &mut redacted);
    } else {
        redacted.push_str(before_frag);
    }

    if let Some(frag) = frag_opt {
        redacted.push('#');
        if let Some((frag_prefix, frag_query)) = frag.split_once('?') {
            redact_kv_pairs(frag_prefix, &mut redacted);
            redacted.push('?');
            redact_kv_pairs(frag_query, &mut redacted);
        } else {
            redact_kv_pairs(frag, &mut redacted);
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

    #[test]
    fn redact_uri_masks_sensitive_fragment_parameters() {
        assert_eq!(
            redact_uri("bolt://db:7687#token=s3cr3t-326&section=1"),
            "bolt://db:7687#token=***&section=1"
        );
        assert_eq!(
            redact_uri("bolt://db:7687?database=movies#password=s3cr3t-326"),
            "bolt://db:7687?database=movies#password=***"
        );
        assert_eq!(
            redact_uri("bolt://db:7687?token=s3cr3t-326#secret=topsecret"),
            "bolt://db:7687?token=***#secret=***"
        );
    }
}
