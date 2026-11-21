//! Query Spec v1 interchange.
//!
//! This module groups both directions of the Query Spec v1 interchange format:
//!
//! - [`export`] serialises a finished native AST into a Query Spec document
//!   (the same implementation exposed as [`crate::spec_export`]).
//! - [`import`] validates a JSON Query Spec document and builds a
//!   [`crate::QueryBuilder`] from it, or compiles it directly for a dialect.
//!
//! The import half is a pure Rust API over [`serde_json::Value`]. The Python
//! bindings convert their native values to JSON and call it, so the consumer
//! behaviour is shared by Rust and Python callers.

pub use crate::spec_export as export;
pub use import::{
    MAX_AST_DEPTH, NON_STRING_KEY_PREFIX, SpecCompileOptions, SpecCompileResult, SpecError,
    build_nested_query, build_query, compile, encode_non_string_key,
};

/// Query Spec v1 validation and compilation.
pub mod import;
