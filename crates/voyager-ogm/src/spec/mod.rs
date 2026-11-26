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

/// The major Query Spec version this build accepts and produces.
///
/// Spec documents carry it as `spec_version`. A different major version is
/// rejected because it may change how an existing document compiles.
pub const SPEC_VERSION: u32 = 1;

/// The highest Query Spec v1 revision this build accepts and produces.
///
/// Spec documents may carry it as `spec_revision`; a missing value means
/// revision 1. Each backwards-compatible addition to v1 bumps this number, and
/// a document that declares a newer revision is rejected with
/// [`crate::ErrorCode::InvalidSpec`] at `/spec_revision`.
pub const SPEC_REVISION: u32 = 1;

/// Query Spec v1 validation and compilation.
pub mod import;
