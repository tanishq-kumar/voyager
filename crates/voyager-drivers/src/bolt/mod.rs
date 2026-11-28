//! Native Bolt wire protocol implementation (PackStream framing, state machines, and connection).
//!
//! The PackStream value model is always compiled because PostgreSQL/AGE and Redis/FalkorDB
//! results reuse those types. The Bolt connection itself is gated behind the `bolt` feature,
//! and the stub server/TestKit backend behind `testkit`.

pub mod canonical;
#[cfg(feature = "bolt")]
pub mod connection;
pub mod decode_result;
#[cfg(feature = "bolt")]
pub mod messages;
pub mod packstream;
#[cfg(feature = "bolt")]
pub mod stream;
#[cfg(feature = "testkit")]
pub mod stub_server;
#[cfg(feature = "testkit")]
pub mod testkit;

pub use canonical::{
    bolt_node_to_canonical_json, bolt_path_to_canonical_json, bolt_relationship_to_canonical_json,
    bolt_value_to_canonical_json, build_bolt_record_batch,
};

#[cfg(feature = "bolt")]
pub use connection::{BOLT_MAGIC_PREAMBLE, BOLT_PROPOSED_VERSIONS, BoltConnection, BoltVersion};
pub use decode_result::{
    BoltResultDecoder, decode_bolt_record_frame, decode_result_value_slice, peek_bolt_message_tag,
};
#[cfg(feature = "bolt")]
pub use messages::{BoltRequest, BoltResponse};
pub use packstream::{
    BoltDate, BoltDateTime, BoltDateTimeZoneId, BoltDuration, BoltLocalDateTime, BoltLocalTime,
    BoltNode, BoltPath, BoltPoint2D, BoltPoint3D, BoltRelationship, BoltTime,
    BoltUnboundRelationship, BoltValue, PackStream, srid,
};
#[cfg(feature = "bolt")]
pub use stream::{
    BoltFrame, DEFAULT_CHUNK_SIZE, MAX_CHUNK_SIZE, encode_chunks, read_message_frame,
    read_message_frame_borrowed, read_message_frame_buffered, write_message_frame,
};
#[cfg(feature = "testkit")]
pub use stub_server::BoltStubServer;
#[cfg(feature = "testkit")]
pub use testkit::TestkitBackend;
