//! Direct PackStream-to-[`ResultValue`] decoder for Bolt result streams.
//!
//! Decodes PackStream binary payloads directly from a `&[u8]` slice into
//! [`ResultValue`] without constructing intermediate [`super::packstream::BoltValue`]
//! enums, temporary structure field `Vec`s, or per-string `Bytes::split_to`
//! refcount updates.
//!
//! [`BoltResultDecoder`] additionally interns repeated map keys, node labels,
//! relationship types, and IANA timezone IDs across rows of a single result via
//! a bounded `HashSet<Arc<str>>` and reuses path-assembly scratch buffers.
//!
//! Enforces the same security invariants as [`super::packstream::PackStream::decode`]:
//! - [`MAX_PACKSTREAM_DEPTH`] nesting depth cap
//! - Impossible claimed length rejection (`len > remaining` / `size * 2 > remaining`)
//! - Bounded pre-allocation (`len.min(1024)`)
//! - Strict UTF-8 validation and EOF checks on every read

use std::collections::{HashMap, HashSet};
use std::hash::RandomState;
use std::sync::Arc;

use crate::bolt::packstream::MAX_PACKSTREAM_DEPTH;
use crate::error::{NetError, Result};
use crate::value::result::{
    ResultDateTime, ResultMap, ResultNode, ResultPath, ResultRelationship, ResultStructure,
    ResultTimezone, ResultUnboundRelationship, ResultValue,
};
use crate::value::{DurationParts, Point2D, Point3D};

/// Maximum number of unique strings retained by [`BoltResultDecoder`]'s per-result
/// interner, preventing unbounded memory growth on high-cardinality map keys.
const MAX_INTERNED_STRINGS: usize = 4096;

/// Reusable scratch vectors for path endpoint binding (zero per-path temporary allocations).
#[derive(Default)]
struct PathScratch {
    raw_nodes: Vec<Option<ResultNode>>,
    raw_rels: Vec<Option<ResultUnboundRelationship>>,
    sequence: Vec<i64>,
    node_uses: Vec<usize>,
    rel_uses: Vec<usize>,
}

impl PathScratch {
    fn clear(&mut self) {
        self.raw_nodes.clear();
        self.raw_rels.clear();
        self.sequence.clear();
        self.node_uses.clear();
        self.rel_uses.clear();
    }
}

/// Stateful per-query Bolt PackStream decoder targeting [`ResultValue`] directly.
///
/// Reuses a bounded string interner for repeated map keys, node labels, relationship
/// types, and timezone names across rows of a single query result, per-query
/// [`RandomState`] (SipHash) hashers for the interner and decoded maps so crafted
/// colliding keys from a hostile server cannot degrade lookups, and scratch vectors
/// for path binding.
pub struct BoltResultDecoder {
    interner: Option<HashSet<Arc<str>, RandomState>>,
    map_hasher: RandomState,
    path_scratch: PathScratch,
}

impl Default for BoltResultDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl BoltResultDecoder {
    /// Creates a new per-result decoder with a lazily allocated interner and empty scratch buffers.
    pub fn new() -> Self {
        let map_hasher = RandomState::new();
        Self {
            interner: None,
            map_hasher,
            path_scratch: PathScratch::default(),
        }
    }

    /// Clears per-query interner entries and path scratch buffers for reuse across queries.
    ///
    /// Drops oversized interner tables (`capacity > 512`) so an unusually wide schema
    /// query does not pin memory on subsequent small queries.
    pub fn clear(&mut self) {
        if let Some(interner) = self.interner.as_mut() {
            if interner.capacity() > 512 {
                self.interner = None;
            } else {
                interner.clear();
            }
        }
        self.path_scratch.clear();
    }

    #[inline]
    fn intern(&mut self, s: &str) -> Arc<str> {
        let interner = self
            .interner
            .get_or_insert_with(|| HashSet::with_capacity_and_hasher(32, RandomState::new()));
        if let Some(existing) = interner.get(s) {
            return Arc::clone(existing);
        }
        let arc: Arc<str> = Arc::from(s);
        if interner.len() < MAX_INTERNED_STRINGS {
            interner.insert(Arc::clone(&arc));
        }
        arc
    }

    /// Decodes a single PackStream value directly into [`ResultValue`] from a byte slice,
    /// returning the decoded value and the number of bytes consumed.
    pub fn decode_value_slice(&mut self, data: &[u8]) -> Result<(ResultValue, usize)> {
        let mut cursor = SliceCursor::new(data);
        let val = self.decode_value(&mut cursor, 0)?;
        Ok((val, cursor.pos))
    }

    /// Decodes a Bolt `RECORD` (`0x71`) response frame directly into a row of [`ResultValue`]s.
    ///
    /// Enforces the same depth (`depth = 0` for the RECORD structure, `depth = 1` for the row
    /// list, `depth = 2` for each cell) and buffer bounds checks as `BoltResponse::decode`.
    pub fn decode_record_frame(&mut self, frame: &[u8]) -> Result<Vec<ResultValue>> {
        let mut cursor = SliceCursor::new(frame);
        let marker = cursor.read_u8("Unexpected EOF while decoding PackStream value")?;
        let num_fields = match marker {
            0xB0..=0xBF => (marker & 0x0F) as usize,
            0xDC => cursor.read_u8("Incomplete Struct8 length")? as usize,
            0xDD => cursor.read_u16("Incomplete Struct16 length")? as usize,
            other => {
                return Err(NetError::ProtocolError(format!(
                    "Expected Bolt RECORD structure marker, got 0x{:02X}",
                    other
                )));
            }
        };
        let tag = cursor.read_u8("Unexpected EOF reading structure signature tag")?;
        if tag != 0x71 {
            return Err(NetError::ProtocolError(format!(
                "Expected Bolt RECORD tag 0x71, got 0x{:02X}",
                tag
            )));
        }
        if num_fields > cursor.remaining() {
            return Err(NetError::ProtocolError(format!(
                "Structure field count {} exceeds available buffer bytes {} (minimum 1 byte per field required)",
                num_fields,
                cursor.remaining()
            )));
        }
        if num_fields == 0 {
            return Ok(Vec::new());
        }
        // Standard RECORD has 1 field: a List of column values (decoded at depth 1 -> cells at depth 2).
        if num_fields == 1 {
            if let Some(list_len) = try_read_list_header(&mut cursor, 1)? {
                let mut row = Vec::with_capacity(list_len.min(1024));
                for _ in 0..list_len {
                    row.push(self.decode_value(&mut cursor, 2)?);
                }
                return Ok(row);
            }
            let val = self.decode_value(&mut cursor, 1)?;
            return Ok(vec![val]);
        }
        // Non-standard multi-field RECORD: match BoltResponse::decode (last field if List, else fields)
        let mut fields = Vec::with_capacity(num_fields.min(1024));
        for _ in 0..num_fields {
            fields.push(self.decode_value(&mut cursor, 1)?);
        }
        if let Some(ResultValue::List(last)) = fields.pop() {
            Ok(last.into_vec())
        } else {
            let mut fallback = SliceCursor::new(frame);
            let _ = fallback.read_u8("eof")?;
            match marker {
                0xDC => {
                    let _ = fallback.read_u8("eof")?;
                }
                0xDD => {
                    let _ = fallback.read_u16("eof")?;
                }
                _ => {}
            }
            let _ = fallback.read_u8("eof")?;
            let mut out = Vec::with_capacity(num_fields.min(1024));
            for _ in 0..num_fields {
                out.push(self.decode_value(&mut fallback, 1)?);
            }
            Ok(out)
        }
    }

    fn decode_map_entries(
        &mut self,
        size: usize,
        cursor: &mut SliceCursor<'_>,
        depth: usize,
    ) -> Result<ResultMap> {
        let min_required = size.saturating_mul(2);
        if min_required > cursor.remaining() {
            return Err(NetError::ProtocolError(format!(
                "Map size {} requires at least {} bytes, which exceeds available buffer bytes {}",
                size,
                min_required,
                cursor.remaining()
            )));
        }
        if size == 0 {
            return Ok(ResultMap::new());
        }
        let mut entries: Vec<(Arc<str>, ResultValue)> = Vec::with_capacity(size.min(1024));
        if size <= 16 {
            for _ in 0..size {
                let key = match try_read_str(cursor, depth + 1)? {
                    Some(s) => self.intern(s),
                    None => {
                        let other = self.decode_value(cursor, depth + 1)?;
                        return Err(NetError::ProtocolError(format!(
                            "Map key must be a String, got: {:?}",
                            other
                        )));
                    }
                };
                let value = self.decode_value(cursor, depth + 1)?;
                if let Some((_, existing)) = entries
                    .iter_mut()
                    .find(|(ek, _)| Arc::ptr_eq(ek, &key) || ek.as_ref() == key.as_ref())
                {
                    *existing = value;
                } else {
                    entries.push((key, value));
                }
            }
        } else {
            let mut index =
                HashMap::with_capacity_and_hasher(size.min(1024), self.map_hasher.clone());
            for _ in 0..size {
                let key = match try_read_str(cursor, depth + 1)? {
                    Some(s) => self.intern(s),
                    None => {
                        let other = self.decode_value(cursor, depth + 1)?;
                        return Err(NetError::ProtocolError(format!(
                            "Map key must be a String, got: {:?}",
                            other
                        )));
                    }
                };
                let value = self.decode_value(cursor, depth + 1)?;
                if let Some(&pos) = index.get(&key)
                    && let Some((_, existing)) = entries.get_mut(pos)
                {
                    *existing = value;
                } else {
                    let pos = entries.len();
                    index.insert(Arc::clone(&key), pos);
                    entries.push((key, value));
                }
            }
        }
        Ok(ResultMap::from_entries(entries))
    }

    fn decode_value(&mut self, cursor: &mut SliceCursor<'_>, depth: usize) -> Result<ResultValue> {
        check_depth(depth)?;
        let marker = cursor.read_u8("Unexpected EOF while decoding PackStream value")?;

        match marker {
            // TinyInt positive (0..=127)
            0x00..=0x7F => Ok(ResultValue::Int(marker as i64)),
            // TinyInt negative (-16..=-1)
            0xF0..=0xFF => Ok(ResultValue::Int((marker as i8) as i64)),
            // TinyString (0..=15 bytes)
            0x80..=0x8F => {
                let len = (marker & 0x0F) as usize;
                let s = read_utf8_str(cursor, len)?;
                Ok(ResultValue::Str(s.into()))
            }
            // TinyList (0..=15 elements)
            0x90..=0x9F => {
                let len = (marker & 0x0F) as usize;
                self.decode_list(len, cursor, depth)
            }
            // TinyMap (0..=15 entries)
            0xA0..=0xAF => {
                let size = (marker & 0x0F) as usize;
                let map = self.decode_map_entries(size, cursor, depth)?;
                Ok(ResultValue::Map(map))
            }
            // TinyStruct (0..=15 fields)
            0xB0..=0xBF => {
                let num_fields = (marker & 0x0F) as usize;
                self.decode_structure(num_fields, cursor, depth)
            }
            // Null
            0xC0 => Ok(ResultValue::Null),
            // Float64
            0xC1 => Ok(ResultValue::Float(cursor.read_f64("Incomplete Float64")?)),
            // Boolean false / true
            0xC2 => Ok(ResultValue::Bool(false)),
            0xC3 => Ok(ResultValue::Bool(true)),
            // Int8 / Int16 / Int32 / Int64
            0xC8 => Ok(ResultValue::Int(cursor.read_i8("Incomplete Int8")? as i64)),
            0xC9 => Ok(ResultValue::Int(cursor.read_i16("Incomplete Int16")? as i64)),
            0xCA => Ok(ResultValue::Int(cursor.read_i32("Incomplete Int32")? as i64)),
            0xCB => Ok(ResultValue::Int(cursor.read_i64("Incomplete Int64")?)),
            // Bytes8 / Bytes16 / Bytes32
            0xCC => {
                let len = cursor.read_u8("Incomplete Bytes8 length")? as usize;
                decode_bytes(len, cursor)
            }
            0xCD => {
                let len = cursor.read_u16("Incomplete Bytes16 length")? as usize;
                decode_bytes(len, cursor)
            }
            0xCE => {
                let len = cursor.read_u32("Incomplete Bytes32 length")? as usize;
                decode_bytes(len, cursor)
            }
            // String8 / String16 / String32
            0xD0 => {
                let len = cursor.read_u8("Incomplete String8 length")? as usize;
                let s = read_utf8_str(cursor, len)?;
                Ok(ResultValue::Str(s.into()))
            }
            0xD1 => {
                let len = cursor.read_u16("Incomplete String16 length")? as usize;
                let s = read_utf8_str(cursor, len)?;
                Ok(ResultValue::Str(s.into()))
            }
            0xD2 => {
                let len = cursor.read_u32("Incomplete String32 length")? as usize;
                let s = read_utf8_str(cursor, len)?;
                Ok(ResultValue::Str(s.into()))
            }
            // List8 / List16 / List32
            0xD4 => {
                let len = cursor.read_u8("Incomplete List8 length")? as usize;
                self.decode_list(len, cursor, depth)
            }
            0xD5 => {
                let len = cursor.read_u16("Incomplete List16 length")? as usize;
                self.decode_list(len, cursor, depth)
            }
            0xD6 => {
                let len = cursor.read_u32("Incomplete List32 length")? as usize;
                self.decode_list(len, cursor, depth)
            }
            // Map8 / Map16 / Map32
            0xD8 => {
                let size = cursor.read_u8("Incomplete Map8 length")? as usize;
                let map = self.decode_map_entries(size, cursor, depth)?;
                Ok(ResultValue::Map(map))
            }
            0xD9 => {
                let size = cursor.read_u16("Incomplete Map16 length")? as usize;
                let map = self.decode_map_entries(size, cursor, depth)?;
                Ok(ResultValue::Map(map))
            }
            0xDA => {
                let size = cursor.read_u32("Incomplete Map32 length")? as usize;
                let map = self.decode_map_entries(size, cursor, depth)?;
                Ok(ResultValue::Map(map))
            }
            // Struct8 / Struct16
            0xDC => {
                let num_fields = cursor.read_u8("Incomplete Struct8 length")? as usize;
                self.decode_structure(num_fields, cursor, depth)
            }
            0xDD => {
                let num_fields = cursor.read_u16("Incomplete Struct16 length")? as usize;
                self.decode_structure(num_fields, cursor, depth)
            }
            unknown => Err(NetError::ProtocolError(format!(
                "Unknown PackStream marker byte 0x{:02X}",
                unknown
            ))),
        }
    }

    fn decode_list(
        &mut self,
        len: usize,
        cursor: &mut SliceCursor<'_>,
        depth: usize,
    ) -> Result<ResultValue> {
        if len > cursor.remaining() {
            return Err(NetError::ProtocolError(format!(
                "List length {} exceeds available buffer bytes {} (minimum 1 byte per element required)",
                len,
                cursor.remaining()
            )));
        }
        let mut list = Vec::with_capacity(len.min(1024));
        for _ in 0..len {
            list.push(self.decode_value(cursor, depth + 1)?);
        }
        Ok(ResultValue::List(list.into_boxed_slice()))
    }

    fn decode_generic_structure(
        &mut self,
        tag: u8,
        num_fields: usize,
        cursor: &mut SliceCursor<'_>,
        depth: usize,
    ) -> Result<ResultValue> {
        let mut fields = Vec::with_capacity(num_fields.min(1024));
        for _ in 0..num_fields {
            fields.push(self.decode_value(cursor, depth + 1)?);
        }
        Ok(ResultValue::Structure(Box::new(ResultStructure {
            tag,
            fields,
        })))
    }

    fn decode_structure(
        &mut self,
        num_fields: usize,
        cursor: &mut SliceCursor<'_>,
        depth: usize,
    ) -> Result<ResultValue> {
        let tag = cursor.read_u8("Unexpected EOF reading structure signature tag")?;
        if num_fields > cursor.remaining() {
            return Err(NetError::ProtocolError(format!(
                "Structure field count {} exceeds available buffer bytes {} (minimum 1 byte per field required)",
                num_fields,
                cursor.remaining()
            )));
        }

        let saved_pos = cursor.pos;
        match tag {
            0x4E if num_fields >= 3 => {
                if let Some(node) = self.decode_node_fields(num_fields, cursor, depth)? {
                    return Ok(ResultValue::Node(Box::new(node)));
                }
            }
            0x52 if num_fields >= 5 => {
                if let Some(rel) = self.decode_relationship_fields(num_fields, cursor, depth)? {
                    return Ok(ResultValue::Relationship(Box::new(rel)));
                }
            }
            0x50 if num_fields >= 3 => {
                let path = self.decode_path_fields(num_fields, cursor, depth)?;
                return Ok(ResultValue::Path(Box::new(path)));
            }
            0x58 if num_fields >= 3 => {
                if let Some(pt) = self.decode_point2d_fields(num_fields, cursor, depth)? {
                    return Ok(ResultValue::Point2D(Box::new(pt)));
                }
            }
            0x59 if num_fields >= 4 => {
                if let Some(pt) = self.decode_point3d_fields(num_fields, cursor, depth)? {
                    return Ok(ResultValue::Point3D(Box::new(pt)));
                }
            }
            0x44 if num_fields >= 1 => {
                if let Some(days) = try_read_i64(cursor, depth + 1)? {
                    for _ in 1..num_fields {
                        let _ = self.decode_value(cursor, depth + 1)?;
                    }
                    return Ok(ResultValue::Date(days));
                }
            }
            0x54 if num_fields >= 2 => {
                if let (Some(nanoseconds), Some(tz)) = (
                    try_read_i64(cursor, depth + 1)?,
                    try_read_i64(cursor, depth + 1)?,
                ) {
                    for _ in 2..num_fields {
                        let _ = self.decode_value(cursor, depth + 1)?;
                    }
                    return Ok(ResultValue::Time {
                        nanoseconds,
                        tz_offset_seconds: tz as i32,
                    });
                }
            }
            0x74 if num_fields >= 1 => {
                if let Some(nanoseconds) = try_read_i64(cursor, depth + 1)? {
                    for _ in 1..num_fields {
                        let _ = self.decode_value(cursor, depth + 1)?;
                    }
                    return Ok(ResultValue::LocalTime(nanoseconds));
                }
            }
            0x49 | 0x46 if num_fields >= 3 => {
                if let (Some(seconds), Some(nanoseconds), Some(tz)) = (
                    try_read_i64(cursor, depth + 1)?,
                    try_read_i64(cursor, depth + 1)?,
                    try_read_i64(cursor, depth + 1)?,
                ) {
                    for _ in 3..num_fields {
                        let _ = self.decode_value(cursor, depth + 1)?;
                    }
                    let utc_seconds = if tag == 0x46 {
                        seconds.saturating_sub(tz)
                    } else {
                        seconds
                    };
                    return Ok(ResultValue::DateTime(Box::new(ResultDateTime {
                        seconds: utc_seconds,
                        nanoseconds,
                        tz: ResultTimezone::Offset(tz as i32),
                    })));
                }
            }
            0x64 if num_fields >= 2 => {
                if let (Some(seconds), Some(nanoseconds)) = (
                    try_read_i64(cursor, depth + 1)?,
                    try_read_i64(cursor, depth + 1)?,
                ) {
                    for _ in 2..num_fields {
                        let _ = self.decode_value(cursor, depth + 1)?;
                    }
                    return Ok(ResultValue::LocalDateTime {
                        seconds,
                        nanoseconds: nanoseconds as u32,
                    });
                }
            }
            0x4A | 0x66 | 0x69 if num_fields >= 3 => {
                if let (Some(seconds), Some(nanoseconds), Some(zone_id)) = (
                    try_read_i64(cursor, depth + 1)?,
                    try_read_i64(cursor, depth + 1)?,
                    try_read_str(cursor, depth + 1)?,
                ) {
                    let id = self.intern(zone_id);
                    for _ in 3..num_fields {
                        let _ = self.decode_value(cursor, depth + 1)?;
                    }
                    return Ok(ResultValue::DateTime(Box::new(ResultDateTime {
                        seconds,
                        nanoseconds,
                        tz: ResultTimezone::Zone {
                            id,
                            is_local: tag == 0x66,
                        },
                    })));
                }
            }
            0x45 if num_fields >= 4 => {
                if let (Some(months), Some(days), Some(seconds), Some(nanoseconds)) = (
                    try_read_i64(cursor, depth + 1)?,
                    try_read_i64(cursor, depth + 1)?,
                    try_read_i64(cursor, depth + 1)?,
                    try_read_i64(cursor, depth + 1)?,
                ) {
                    for _ in 4..num_fields {
                        let _ = self.decode_value(cursor, depth + 1)?;
                    }
                    return Ok(ResultValue::Duration(Box::new(DurationParts {
                        months,
                        days,
                        seconds,
                        nanoseconds,
                    })));
                }
            }
            _ => {}
        }

        // Fallback: restore cursor to start of fields and decode generically.
        cursor.pos = saved_pos;
        self.decode_generic_structure(tag, num_fields, cursor, depth)
    }

    fn decode_node_fields(
        &mut self,
        num_fields: usize,
        cursor: &mut SliceCursor<'_>,
        depth: usize,
    ) -> Result<Option<ResultNode>> {
        let field_depth = depth + 1;
        let Some(id) = try_read_i64(cursor, field_depth)? else {
            return Ok(None);
        };

        // Field 1: labels (List of Strings)
        let labels = if let Some(len) = try_read_list_header(cursor, field_depth)? {
            let mut out = Vec::with_capacity(len.min(1024));
            for _ in 0..len {
                if let Some(s) = try_read_str(cursor, field_depth + 1)? {
                    out.push(self.intern(s));
                } else {
                    let _ = self.decode_value(cursor, field_depth + 1)?;
                }
            }
            out
        } else {
            let _ = self.decode_value(cursor, field_depth)?;
            Vec::new()
        };

        // Field 2: properties (Map)
        let properties = if let Some(size) = try_read_map_header(cursor, field_depth)? {
            self.decode_map_entries(size, cursor, field_depth)?
        } else {
            let _ = self.decode_value(cursor, field_depth)?;
            ResultMap::new()
        };

        // Field 3 (optional, Bolt v5+): element_id (String)
        let element_id = if num_fields >= 4 {
            if let Some(s) = try_read_str(cursor, field_depth)? {
                Some(Arc::from(s))
            } else {
                let _ = self.decode_value(cursor, field_depth)?;
                None
            }
        } else {
            None
        };

        for _ in 4..num_fields {
            let _ = self.decode_value(cursor, field_depth)?;
        }

        Ok(Some(ResultNode {
            id,
            labels,
            properties,
            element_id,
        }))
    }

    fn decode_relationship_fields(
        &mut self,
        num_fields: usize,
        cursor: &mut SliceCursor<'_>,
        depth: usize,
    ) -> Result<Option<ResultRelationship>> {
        let field_depth = depth + 1;
        let Some(id) = try_read_i64(cursor, field_depth)? else {
            return Ok(None);
        };

        let start_node_id = match try_read_i64(cursor, field_depth)? {
            Some(v) => v,
            None => {
                let _ = self.decode_value(cursor, field_depth)?;
                0
            }
        };
        let end_node_id = match try_read_i64(cursor, field_depth)? {
            Some(v) => v,
            None => {
                let _ = self.decode_value(cursor, field_depth)?;
                0
            }
        };
        let rel_type = match try_read_str(cursor, field_depth)? {
            Some(s) => self.intern(s),
            None => {
                let _ = self.decode_value(cursor, field_depth)?;
                self.intern("")
            }
        };
        let properties = if let Some(size) = try_read_map_header(cursor, field_depth)? {
            self.decode_map_entries(size, cursor, field_depth)?
        } else {
            let _ = self.decode_value(cursor, field_depth)?;
            ResultMap::new()
        };

        let (element_id, start_element_id, end_element_id) = if num_fields >= 8 {
            let e1 = match try_read_str(cursor, field_depth)? {
                Some(s) => Some(Arc::from(s)),
                None => {
                    let _ = self.decode_value(cursor, field_depth)?;
                    None
                }
            };
            let e2 = match try_read_str(cursor, field_depth)? {
                Some(s) => Some(Arc::from(s)),
                None => {
                    let _ = self.decode_value(cursor, field_depth)?;
                    None
                }
            };
            let e3 = match try_read_str(cursor, field_depth)? {
                Some(s) => Some(Arc::from(s)),
                None => {
                    let _ = self.decode_value(cursor, field_depth)?;
                    None
                }
            };
            for _ in 8..num_fields {
                let _ = self.decode_value(cursor, field_depth)?;
            }
            (e1, e2, e3)
        } else {
            for _ in 5..num_fields {
                let _ = self.decode_value(cursor, field_depth)?;
            }
            (None, None, None)
        };

        Ok(Some(ResultRelationship {
            id,
            start_node_id,
            end_node_id,
            rel_type,
            properties,
            element_id,
            start_element_id,
            end_element_id,
        }))
    }

    fn decode_path_fields(
        &mut self,
        num_fields: usize,
        cursor: &mut SliceCursor<'_>,
        depth: usize,
    ) -> Result<ResultPath> {
        let field_depth = depth + 1;
        // Take scratch temporarily so nested paths (if ever encountered) remain safe
        // while keeping allocated capacities across consecutive top-level paths.
        let mut scratch = std::mem::take(&mut self.path_scratch);
        scratch.clear();

        let result =
            self.decode_path_fields_with_scratch(num_fields, cursor, field_depth, &mut scratch);

        scratch.clear();
        self.path_scratch = scratch;
        result
    }

    fn decode_path_fields_with_scratch(
        &mut self,
        num_fields: usize,
        cursor: &mut SliceCursor<'_>,
        field_depth: usize,
        scratch: &mut PathScratch,
    ) -> Result<ResultPath> {
        // Field 0: nodes (List of Node structs 0x4E)
        if let Some(len) = try_read_list_header(cursor, field_depth)? {
            scratch.raw_nodes.reserve(len.min(1024));
            for _ in 0..len {
                let item_pos = cursor.pos;
                if let Some((nf, 0x4E)) = try_read_struct_header(cursor, field_depth + 1)?
                    && nf >= 3
                    && let Some(node) = self.decode_node_fields(nf, cursor, field_depth + 1)?
                {
                    scratch.raw_nodes.push(Some(node));
                    continue;
                }
                cursor.pos = item_pos;
                let _ = self.decode_value(cursor, field_depth + 1)?;
            }
        } else {
            let _ = self.decode_value(cursor, field_depth)?;
        }

        // Field 1: unbound relationships (List of 0x72 structs)
        if let Some(len) = try_read_list_header(cursor, field_depth)? {
            scratch.raw_rels.reserve(len.min(1024));
            for _ in 0..len {
                let item_pos = cursor.pos;
                if let Some((nf, 0x72)) = try_read_struct_header(cursor, field_depth + 1)?
                    && nf >= 3
                {
                    let rel_field_depth = field_depth + 2;
                    let id = match try_read_i64(cursor, rel_field_depth)? {
                        Some(v) => v,
                        None => {
                            let _ = self.decode_value(cursor, rel_field_depth)?;
                            0
                        }
                    };
                    let rel_type = match try_read_str(cursor, rel_field_depth)? {
                        Some(s) => self.intern(s),
                        None => {
                            let _ = self.decode_value(cursor, rel_field_depth)?;
                            self.intern("")
                        }
                    };
                    let properties =
                        if let Some(size) = try_read_map_header(cursor, rel_field_depth)? {
                            self.decode_map_entries(size, cursor, rel_field_depth)?
                        } else {
                            let _ = self.decode_value(cursor, rel_field_depth)?;
                            ResultMap::new()
                        };
                    let element_id = if nf >= 4 {
                        match try_read_str(cursor, rel_field_depth)? {
                            Some(s) => Some(Arc::from(s)),
                            None => {
                                let _ = self.decode_value(cursor, rel_field_depth)?;
                                None
                            }
                        }
                    } else {
                        None
                    };
                    for _ in 4..nf {
                        let _ = self.decode_value(cursor, rel_field_depth)?;
                    }
                    scratch.raw_rels.push(Some(ResultUnboundRelationship {
                        id,
                        rel_type,
                        properties,
                        element_id,
                    }));
                    continue;
                }
                cursor.pos = item_pos;
                let _ = self.decode_value(cursor, field_depth + 1)?;
            }
        } else {
            let _ = self.decode_value(cursor, field_depth)?;
        }

        // Field 2: sequence (List of Integers)
        if let Some(len) = try_read_list_header(cursor, field_depth)? {
            scratch.sequence.reserve(len.min(1024));
            for _ in 0..len {
                if let Some(i) = try_read_i64(cursor, field_depth + 1)? {
                    scratch.sequence.push(i);
                } else {
                    let _ = self.decode_value(cursor, field_depth + 1)?;
                }
            }
        } else {
            let _ = self.decode_value(cursor, field_depth)?;
        }

        for _ in 3..num_fields {
            let _ = self.decode_value(cursor, field_depth)?;
        }

        if scratch.sequence.is_empty() {
            let nodes = scratch.raw_nodes.drain(..).flatten().collect();
            return Ok(ResultPath {
                nodes,
                relationships: Vec::new(),
            });
        }

        // Count how many times each node and relationship is referenced so we can move
        // (zero-clone) on the last use and only clone when a path revisits a node/edge.
        let step_count = scratch.sequence.len() / 2;
        scratch.node_uses.resize(scratch.raw_nodes.len(), 0);
        scratch.rel_uses.resize(scratch.raw_rels.len(), 0);

        if !scratch.raw_nodes.is_empty()
            && let Some(slot) = scratch.node_uses.first_mut()
        {
            *slot += 1;
        }
        let mut cur_idx = 0usize;
        for step_idx in 0..step_count {
            let Some(&rel_code) = scratch.sequence.get(2 * step_idx) else {
                break;
            };
            let Some(&next_raw) = scratch.sequence.get(2 * step_idx + 1) else {
                break;
            };
            let next_idx = usize::try_from(next_raw).unwrap_or(0);
            let rel_idx = rel_code.unsigned_abs().saturating_sub(1) as usize;
            if cur_idx < scratch.raw_nodes.len()
                && next_idx < scratch.raw_nodes.len()
                && rel_idx < scratch.raw_rels.len()
            {
                if let Some(slot) = scratch.node_uses.get_mut(next_idx) {
                    *slot += 1;
                }
                if let Some(slot) = scratch.rel_uses.get_mut(rel_idx) {
                    *slot += 1;
                }
                cur_idx = next_idx;
            }
        }

        let mut nodes = Vec::with_capacity(step_count + 1);
        let mut relationships = Vec::with_capacity(step_count);

        let take_or_clone_node = |idx: usize,
                                  raw: &mut [Option<ResultNode>],
                                  uses: &mut [usize]|
         -> Option<ResultNode> {
            let count = uses.get_mut(idx)?;
            if *count <= 1 {
                *count = 0;
                raw.get_mut(idx)?.take()
            } else {
                *count -= 1;
                raw.get(idx)?.as_ref().cloned()
            }
        };

        if !scratch.raw_nodes.is_empty()
            && let Some(first) =
                take_or_clone_node(0, &mut scratch.raw_nodes, &mut scratch.node_uses)
        {
            nodes.push(first);
        }

        let mut current_node_idx = 0usize;
        for step_idx in 0..step_count {
            let Some(&rel_code) = scratch.sequence.get(2 * step_idx) else {
                break;
            };
            let Some(&next_node_raw) = scratch.sequence.get(2 * step_idx + 1) else {
                break;
            };
            let next_node_idx = usize::try_from(next_node_raw).unwrap_or(0);
            let rel_idx = rel_code.unsigned_abs().saturating_sub(1) as usize;

            if current_node_idx < scratch.raw_nodes.len()
                && next_node_idx < scratch.raw_nodes.len()
                && rel_idx < scratch.raw_rels.len()
            {
                let Some(next_node) = take_or_clone_node(
                    next_node_idx,
                    &mut scratch.raw_nodes,
                    &mut scratch.node_uses,
                ) else {
                    continue;
                };
                let prev_pos = nodes.len().saturating_sub(1);
                nodes.push(next_node);
                let next_pos = nodes.len().saturating_sub(1);

                let (start_pos, end_pos) = if rel_code > 0 {
                    (prev_pos, next_pos)
                } else {
                    (next_pos, prev_pos)
                };

                let (Some(start_node), Some(end_node)) = (nodes.get(start_pos), nodes.get(end_pos))
                else {
                    continue;
                };

                let start_node_id = start_node.id;
                let end_node_id = end_node.id;
                let start_element_id = start_node.element_id.clone();
                let end_element_id = end_node.element_id.clone();

                let urel = {
                    let count = scratch.rel_uses.get_mut(rel_idx);
                    match count {
                        Some(c) if *c <= 1 => {
                            *c = 0;
                            scratch.raw_rels.get_mut(rel_idx).and_then(Option::take)
                        }
                        Some(c) => {
                            *c -= 1;
                            scratch
                                .raw_rels
                                .get(rel_idx)
                                .and_then(|o| o.as_ref().cloned())
                        }
                        None => None,
                    }
                };

                if let Some(unbound_rel) = urel {
                    relationships.push(ResultRelationship {
                        id: unbound_rel.id,
                        start_node_id,
                        end_node_id,
                        rel_type: unbound_rel.rel_type,
                        properties: unbound_rel.properties,
                        element_id: unbound_rel.element_id,
                        start_element_id,
                        end_element_id,
                    });
                }
                current_node_idx = next_node_idx;
            }
        }

        Ok(ResultPath {
            nodes,
            relationships,
        })
    }

    fn decode_point2d_fields(
        &mut self,
        num_fields: usize,
        cursor: &mut SliceCursor<'_>,
        depth: usize,
    ) -> Result<Option<Point2D>> {
        let fd = depth + 1;
        let Some(srid) = try_read_i64(cursor, fd)? else {
            return Ok(None);
        };
        let Some(x) = try_read_f64_or_i64(cursor, fd)? else {
            return Ok(None);
        };
        let Some(y) = try_read_f64_or_i64(cursor, fd)? else {
            return Ok(None);
        };
        for _ in 3..num_fields {
            let _ = self.decode_value(cursor, fd)?;
        }
        Ok(Some(Point2D { srid, x, y }))
    }

    fn decode_point3d_fields(
        &mut self,
        num_fields: usize,
        cursor: &mut SliceCursor<'_>,
        depth: usize,
    ) -> Result<Option<Point3D>> {
        let fd = depth + 1;
        let Some(srid) = try_read_i64(cursor, fd)? else {
            return Ok(None);
        };
        let Some(x) = try_read_f64_or_i64(cursor, fd)? else {
            return Ok(None);
        };
        let Some(y) = try_read_f64_or_i64(cursor, fd)? else {
            return Ok(None);
        };
        let Some(z) = try_read_f64_or_i64(cursor, fd)? else {
            return Ok(None);
        };
        for _ in 4..num_fields {
            let _ = self.decode_value(cursor, fd)?;
        }
        Ok(Some(Point3D { srid, x, y, z }))
    }
}

/// Decodes a single PackStream value directly into [`ResultValue`] from a byte slice,
/// returning the decoded value and the number of bytes consumed.
pub fn decode_result_value_slice(data: &[u8]) -> Result<(ResultValue, usize)> {
    let mut decoder = BoltResultDecoder::default();
    decoder.decode_value_slice(data)
}

/// Peeks the Bolt response message tag from a raw frame slice without decoding the payload.
///
/// Returns `Some(tag)` when the frame starts with a valid PackStream structure header
/// (`0xB0..=0xBF`, `0xDC`, `0xDD`) followed by a tag byte.
#[inline]
pub fn peek_bolt_message_tag(frame: &[u8]) -> Option<u8> {
    let &marker = frame.first()?;
    match marker {
        0xB0..=0xBF => frame.get(1).copied(),
        0xDC => frame.get(2).copied(),
        0xDD => frame.get(3).copied(),
        _ => None,
    }
}

/// Decodes a single Bolt `RECORD` (`0x71`) response frame directly into a row of [`ResultValue`]s.
///
/// For decoding multiple rows of a query result, prefer [`BoltResultDecoder::decode_record_frame`]
/// to reuse the string interner and path scratch buffers across rows.
pub fn decode_bolt_record_frame(frame: &[u8]) -> Result<Vec<ResultValue>> {
    let mut decoder = BoltResultDecoder::default();
    decoder.decode_record_frame(frame)
}

#[cfg(feature = "bolt")]
fn open_success_metadata_map<'a>(frame: &'a [u8]) -> Result<Option<(SliceCursor<'a>, usize)>> {
    if peek_bolt_message_tag(frame) != Some(0x70) {
        return Ok(None);
    }
    let mut cursor = SliceCursor::new(frame);
    let Some((num_fields, 0x70)) = try_read_struct_header(&mut cursor, 0)? else {
        return Ok(None);
    };
    if num_fields != 1 {
        return Err(NetError::ProtocolError(format!(
            "SUCCESS message must have 1 field, got {}",
            num_fields
        )));
    }
    let Some(map_size) = try_read_map_header(&mut cursor, 1)? else {
        return Err(NetError::ProtocolError(
            "Expected metadata map in SUCCESS message".to_string(),
        ));
    };
    Ok(Some((cursor, map_size)))
}

#[cfg(feature = "bolt")]
fn skip_packstream_value(cursor: &mut SliceCursor<'_>, depth: usize) -> Result<()> {
    check_depth(depth)?;
    let marker = cursor.read_u8("Unexpected EOF while skipping PackStream value")?;
    match marker {
        0x00..=0x7F | 0xF0..=0xFF | 0xC0 | 0xC2 | 0xC3 => Ok(()),
        0xC1 => {
            cursor.read_exact_slice(8, "Incomplete Float64")?;
            Ok(())
        }
        0xC8 => {
            cursor.read_exact_slice(1, "Incomplete Int8")?;
            Ok(())
        }
        0xC9 => {
            cursor.read_exact_slice(2, "Incomplete Int16")?;
            Ok(())
        }
        0xCA => {
            cursor.read_exact_slice(4, "Incomplete Int32")?;
            Ok(())
        }
        0xCB => {
            cursor.read_exact_slice(8, "Incomplete Int64")?;
            Ok(())
        }
        0x80..=0x8F => {
            let _ = read_utf8_str(cursor, (marker & 0x0F) as usize)?;
            Ok(())
        }
        0xD0 => {
            let len = cursor.read_u8("Incomplete String8 length")? as usize;
            let _ = read_utf8_str(cursor, len)?;
            Ok(())
        }
        0xD1 => {
            let len = cursor.read_u16("Incomplete String16 length")? as usize;
            let _ = read_utf8_str(cursor, len)?;
            Ok(())
        }
        0xD2 => {
            let len = cursor.read_u32("Incomplete String32 length")? as usize;
            let _ = read_utf8_str(cursor, len)?;
            Ok(())
        }
        0xCC => {
            let len = cursor.read_u8("Incomplete Bytes8 length")? as usize;
            cursor.read_exact_slice(len, "Incomplete Bytes payload")?;
            Ok(())
        }
        0xCD => {
            let len = cursor.read_u16("Incomplete Bytes16 length")? as usize;
            cursor.read_exact_slice(len, "Incomplete Bytes payload")?;
            Ok(())
        }
        0xCE => {
            let len = cursor.read_u32("Incomplete Bytes32 length")? as usize;
            cursor.read_exact_slice(len, "Incomplete Bytes payload")?;
            Ok(())
        }
        0x90..=0x9F | 0xD4..=0xD6 => {
            let len = match marker {
                0x90..=0x9F => (marker & 0x0F) as usize,
                0xD4 => cursor.read_u8("Incomplete List8 length")? as usize,
                0xD5 => cursor.read_u16("Incomplete List16 length")? as usize,
                _ => cursor.read_u32("Incomplete List32 length")? as usize,
            };
            if len > cursor.remaining() {
                return Err(NetError::ProtocolError(format!(
                    "List length {} exceeds available buffer bytes {}",
                    len,
                    cursor.remaining()
                )));
            }
            for _ in 0..len {
                skip_packstream_value(cursor, depth + 1)?;
            }
            Ok(())
        }
        0xA0..=0xAF | 0xD8..=0xDA => {
            let size = match marker {
                0xA0..=0xAF => (marker & 0x0F) as usize,
                0xD8 => cursor.read_u8("Incomplete Map8 length")? as usize,
                0xD9 => cursor.read_u16("Incomplete Map16 length")? as usize,
                _ => cursor.read_u32("Incomplete Map32 length")? as usize,
            };
            let min_required = size.saturating_mul(2);
            if min_required > cursor.remaining() {
                return Err(NetError::ProtocolError(format!(
                    "Map size {} requires at least {} bytes, which exceeds available buffer bytes {}",
                    size,
                    min_required,
                    cursor.remaining()
                )));
            }
            for _ in 0..size {
                if try_read_str(cursor, depth + 1)?.is_none() {
                    return Err(NetError::ProtocolError(
                        "Map key must be a String".to_string(),
                    ));
                }
                skip_packstream_value(cursor, depth + 1)?;
            }
            Ok(())
        }
        0xB0..=0xBF | 0xDC | 0xDD => {
            let num_fields = match marker {
                0xB0..=0xBF => (marker & 0x0F) as usize,
                0xDC => cursor.read_u8("Incomplete Struct8 length")? as usize,
                _ => cursor.read_u16("Incomplete Struct16 length")? as usize,
            };
            let _tag = cursor.read_u8("Unexpected EOF reading structure signature tag")?;
            if num_fields > cursor.remaining() {
                return Err(NetError::ProtocolError(format!(
                    "Structure field count {} exceeds available buffer bytes {}",
                    num_fields,
                    cursor.remaining()
                )));
            }
            for _ in 0..num_fields {
                skip_packstream_value(cursor, depth + 1)?;
            }
            Ok(())
        }
        unknown => Err(NetError::ProtocolError(format!(
            "Unknown PackStream marker byte 0x{:02X}",
            unknown
        ))),
    }
}

/// Decodes the projected column names from a Bolt `RUN` `SUCCESS` (`0x70`) frame without
/// materializing an intermediate metadata `HashMap` or `BoltValue` tree.
///
/// Returns `Ok(None)` when the frame tag is not `0x70` (for example `0x7F FAILURE`).
#[cfg(feature = "bolt")]
pub(crate) fn decode_run_success_columns(frame: &[u8]) -> Result<Option<Vec<String>>> {
    let Some((mut cursor, map_size)) = open_success_metadata_map(frame)? else {
        return Ok(None);
    };
    let mut columns = Vec::new();
    for _ in 0..map_size {
        let Some(key) = try_read_str(&mut cursor, 2)? else {
            return Err(NetError::ProtocolError(
                "Map key must be a String".to_string(),
            ));
        };
        if key == "fields" {
            if let Some(len) = try_read_list_header(&mut cursor, 2)? {
                let mut cols = Vec::with_capacity(len.min(1024));
                for _ in 0..len {
                    if let Some(col) = try_read_str(&mut cursor, 3)? {
                        cols.push(col.to_string());
                    } else {
                        skip_packstream_value(&mut cursor, 3)?;
                    }
                }
                columns = cols;
            } else {
                skip_packstream_value(&mut cursor, 2)?;
            }
        } else {
            skip_packstream_value(&mut cursor, 2)?;
        }
    }
    Ok(Some(columns))
}

/// Decodes [`crate::engine::QuerySummary`] metrics from a Bolt `PULL` `SUCCESS` (`0x70`) frame
/// without allocating a metadata `HashMap`, nested `stats` `HashMap`, or `BoltValue` enum.
///
/// Returns `Ok(None)` when the frame tag is not `0x70` (for example `0x7F FAILURE`).
#[cfg(feature = "bolt")]
pub(crate) fn decode_pull_success_summary(
    frame: &[u8],
) -> Result<Option<crate::engine::QuerySummary>> {
    let Some((mut cursor, map_size)) = open_success_metadata_map(frame)? else {
        return Ok(None);
    };
    let mut summary = crate::engine::QuerySummary::empty();
    let mut t_last: Option<u64> = None;
    let mut t_first: Option<u64> = None;

    for _ in 0..map_size {
        let Some(key) = try_read_str(&mut cursor, 2)? else {
            return Err(NetError::ProtocolError(
                "Map key must be a String".to_string(),
            ));
        };
        match key {
            "stats" => {
                if let Some(stats_len) = try_read_map_header(&mut cursor, 2)? {
                    for _ in 0..stats_len {
                        let Some(stat_key) = try_read_str(&mut cursor, 3)? else {
                            return Err(NetError::ProtocolError(
                                "Map key must be a String".to_string(),
                            ));
                        };
                        if let Some(n) = try_read_i64(&mut cursor, 3)? {
                            let val = n as u64;
                            match stat_key {
                                "nodes-created" => summary.nodes_created = val,
                                "nodes-deleted" => summary.nodes_deleted = val,
                                "relationships-created" => summary.relationships_created = val,
                                "relationships-deleted" => summary.relationships_deleted = val,
                                "properties-set" => summary.properties_set = val,
                                "labels-added" => summary.labels_added = val,
                                "labels-removed" => summary.labels_removed = val,
                                "indexes-added" => summary.indexes_added = val,
                                "constraints-added" => summary.constraints_added = val,
                                _ => {}
                            }
                        } else {
                            skip_packstream_value(&mut cursor, 3)?;
                        }
                    }
                } else {
                    skip_packstream_value(&mut cursor, 2)?;
                }
            }
            "t_last" => {
                if let Some(n) = try_read_i64(&mut cursor, 2)? {
                    t_last = Some(n as u64);
                } else {
                    skip_packstream_value(&mut cursor, 2)?;
                }
            }
            "t_first" => {
                if let Some(n) = try_read_i64(&mut cursor, 2)? {
                    t_first = Some(n as u64);
                } else {
                    skip_packstream_value(&mut cursor, 2)?;
                }
            }
            _ => skip_packstream_value(&mut cursor, 2)?,
        }
    }

    if let Some(time) = t_last.or(t_first) {
        summary.execution_time_ms = time;
    }
    Ok(Some(summary))
}

/// Lightweight zero-copy cursor over a PackStream byte slice.
struct SliceCursor<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> SliceCursor<'a> {
    #[inline]
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    #[inline]
    fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.pos)
    }

    #[inline]
    fn read_u8(&mut self, eof_msg: &str) -> Result<u8> {
        let Some(&b) = self.data.get(self.pos) else {
            return Err(NetError::ProtocolError(eof_msg.to_string()));
        };
        self.pos += 1;
        Ok(b)
    }

    #[inline]
    fn read_exact_slice(&mut self, len: usize, eof_msg: &str) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or_else(|| NetError::ProtocolError(eof_msg.to_string()))?;
        let Some(slice) = self.data.get(self.pos..end) else {
            return Err(NetError::ProtocolError(eof_msg.to_string()));
        };
        self.pos = end;
        Ok(slice)
    }

    #[inline]
    fn read_i8(&mut self, eof_msg: &str) -> Result<i8> {
        Ok(self.read_u8(eof_msg)? as i8)
    }

    #[inline]
    fn read_u16(&mut self, eof_msg: &str) -> Result<u16> {
        let s = self.read_exact_slice(2, eof_msg)?;
        let arr: [u8; 2] = s
            .try_into()
            .map_err(|_| NetError::ProtocolError(eof_msg.to_string()))?;
        Ok(u16::from_be_bytes(arr))
    }

    #[inline]
    fn read_i16(&mut self, eof_msg: &str) -> Result<i16> {
        let s = self.read_exact_slice(2, eof_msg)?;
        let arr: [u8; 2] = s
            .try_into()
            .map_err(|_| NetError::ProtocolError(eof_msg.to_string()))?;
        Ok(i16::from_be_bytes(arr))
    }

    #[inline]
    fn read_u32(&mut self, eof_msg: &str) -> Result<u32> {
        let s = self.read_exact_slice(4, eof_msg)?;
        let arr: [u8; 4] = s
            .try_into()
            .map_err(|_| NetError::ProtocolError(eof_msg.to_string()))?;
        Ok(u32::from_be_bytes(arr))
    }

    #[inline]
    fn read_i32(&mut self, eof_msg: &str) -> Result<i32> {
        let s = self.read_exact_slice(4, eof_msg)?;
        let arr: [u8; 4] = s
            .try_into()
            .map_err(|_| NetError::ProtocolError(eof_msg.to_string()))?;
        Ok(i32::from_be_bytes(arr))
    }

    #[inline]
    fn read_i64(&mut self, eof_msg: &str) -> Result<i64> {
        let s = self.read_exact_slice(8, eof_msg)?;
        let arr: [u8; 8] = s
            .try_into()
            .map_err(|_| NetError::ProtocolError(eof_msg.to_string()))?;
        Ok(i64::from_be_bytes(arr))
    }

    #[inline]
    fn read_f64(&mut self, eof_msg: &str) -> Result<f64> {
        let s = self.read_exact_slice(8, eof_msg)?;
        let arr: [u8; 8] = s
            .try_into()
            .map_err(|_| NetError::ProtocolError(eof_msg.to_string()))?;
        Ok(f64::from_be_bytes(arr))
    }

    #[inline]
    fn peek_u8(&self) -> Option<u8> {
        self.data.get(self.pos).copied()
    }
}

#[inline]
fn check_depth(depth: usize) -> Result<()> {
    if depth > MAX_PACKSTREAM_DEPTH {
        return Err(NetError::ProtocolError(format!(
            "PackStream structure exceeded maximum nesting depth of {}",
            MAX_PACKSTREAM_DEPTH
        )));
    }
    Ok(())
}

#[inline]
fn read_utf8_str<'a>(cursor: &mut SliceCursor<'a>, len: usize) -> Result<&'a str> {
    if cursor.remaining() < len {
        return Err(NetError::ProtocolError(format!(
            "Unexpected EOF reading String of length {}",
            len
        )));
    }
    let bytes = cursor.read_exact_slice(len, "Unexpected EOF reading String")?;
    std::str::from_utf8(bytes)
        .map_err(|e| NetError::ProtocolError(format!("Invalid UTF-8 string: {}", e)))
}

#[inline]
fn decode_bytes(len: usize, cursor: &mut SliceCursor<'_>) -> Result<ResultValue> {
    if cursor.remaining() < len {
        return Err(NetError::ProtocolError(format!(
            "Unexpected EOF reading byte buffer of length {}",
            len
        )));
    }
    let slice = cursor.read_exact_slice(len, "Unexpected EOF reading byte buffer")?;
    Ok(ResultValue::Bytes(slice.into()))
}

#[inline]
fn try_read_str<'a>(cursor: &mut SliceCursor<'a>, depth: usize) -> Result<Option<&'a str>> {
    check_depth(depth)?;
    let Some(marker) = cursor.peek_u8() else {
        return Err(NetError::ProtocolError(
            "Unexpected EOF while decoding PackStream value".to_string(),
        ));
    };
    let len = match marker {
        0x80..=0x8F => {
            cursor.pos += 1;
            (marker & 0x0F) as usize
        }
        0xD0 => {
            cursor.pos += 1;
            cursor.read_u8("Incomplete String8 length")? as usize
        }
        0xD1 => {
            cursor.pos += 1;
            cursor.read_u16("Incomplete String16 length")? as usize
        }
        0xD2 => {
            cursor.pos += 1;
            cursor.read_u32("Incomplete String32 length")? as usize
        }
        _ => return Ok(None),
    };
    Ok(Some(read_utf8_str(cursor, len)?))
}

#[inline]
fn try_read_i64(cursor: &mut SliceCursor<'_>, depth: usize) -> Result<Option<i64>> {
    check_depth(depth)?;
    let Some(marker) = cursor.peek_u8() else {
        return Err(NetError::ProtocolError(
            "Unexpected EOF while decoding PackStream value".to_string(),
        ));
    };
    let val = match marker {
        0x00..=0x7F => {
            cursor.pos += 1;
            marker as i64
        }
        0xF0..=0xFF => {
            cursor.pos += 1;
            (marker as i8) as i64
        }
        0xC8 => {
            cursor.pos += 1;
            cursor.read_i8("Incomplete Int8")? as i64
        }
        0xC9 => {
            cursor.pos += 1;
            cursor.read_i16("Incomplete Int16")? as i64
        }
        0xCA => {
            cursor.pos += 1;
            cursor.read_i32("Incomplete Int32")? as i64
        }
        0xCB => {
            cursor.pos += 1;
            cursor.read_i64("Incomplete Int64")?
        }
        _ => return Ok(None),
    };
    Ok(Some(val))
}

#[inline]
fn try_read_f64_or_i64(cursor: &mut SliceCursor<'_>, depth: usize) -> Result<Option<f64>> {
    check_depth(depth)?;
    let Some(marker) = cursor.peek_u8() else {
        return Err(NetError::ProtocolError(
            "Unexpected EOF while decoding PackStream value".to_string(),
        ));
    };
    if marker == 0xC1 {
        cursor.pos += 1;
        return Ok(Some(cursor.read_f64("Incomplete Float64")?));
    }
    Ok(try_read_i64(cursor, depth)?.map(|i| i as f64))
}

#[inline]
fn try_read_list_header(cursor: &mut SliceCursor<'_>, depth: usize) -> Result<Option<usize>> {
    check_depth(depth)?;
    let Some(marker) = cursor.peek_u8() else {
        return Err(NetError::ProtocolError(
            "Unexpected EOF while decoding PackStream value".to_string(),
        ));
    };
    let len = match marker {
        0x90..=0x9F => {
            cursor.pos += 1;
            (marker & 0x0F) as usize
        }
        0xD4 => {
            cursor.pos += 1;
            cursor.read_u8("Incomplete List8 length")? as usize
        }
        0xD5 => {
            cursor.pos += 1;
            cursor.read_u16("Incomplete List16 length")? as usize
        }
        0xD6 => {
            cursor.pos += 1;
            cursor.read_u32("Incomplete List32 length")? as usize
        }
        _ => return Ok(None),
    };
    if len > cursor.remaining() {
        return Err(NetError::ProtocolError(format!(
            "List length {} exceeds available buffer bytes {} (minimum 1 byte per element required)",
            len,
            cursor.remaining()
        )));
    }
    Ok(Some(len))
}

#[inline]
fn try_read_map_header(cursor: &mut SliceCursor<'_>, depth: usize) -> Result<Option<usize>> {
    check_depth(depth)?;
    let Some(marker) = cursor.peek_u8() else {
        return Err(NetError::ProtocolError(
            "Unexpected EOF while decoding PackStream value".to_string(),
        ));
    };
    let size = match marker {
        0xA0..=0xAF => {
            cursor.pos += 1;
            (marker & 0x0F) as usize
        }
        0xD8 => {
            cursor.pos += 1;
            cursor.read_u8("Incomplete Map8 length")? as usize
        }
        0xD9 => {
            cursor.pos += 1;
            cursor.read_u16("Incomplete Map16 length")? as usize
        }
        0xDA => {
            cursor.pos += 1;
            cursor.read_u32("Incomplete Map32 length")? as usize
        }
        _ => return Ok(None),
    };
    let min_required = size.saturating_mul(2);
    if min_required > cursor.remaining() {
        return Err(NetError::ProtocolError(format!(
            "Map size {} requires at least {} bytes, which exceeds available buffer bytes {}",
            size,
            min_required,
            cursor.remaining()
        )));
    }
    Ok(Some(size))
}

#[inline]
fn try_read_struct_header(
    cursor: &mut SliceCursor<'_>,
    depth: usize,
) -> Result<Option<(usize, u8)>> {
    check_depth(depth)?;
    let Some(marker) = cursor.peek_u8() else {
        return Err(NetError::ProtocolError(
            "Unexpected EOF while decoding PackStream value".to_string(),
        ));
    };
    let num_fields = match marker {
        0xB0..=0xBF => {
            cursor.pos += 1;
            (marker & 0x0F) as usize
        }
        0xDC => {
            cursor.pos += 1;
            cursor.read_u8("Incomplete Struct8 length")? as usize
        }
        0xDD => {
            cursor.pos += 1;
            cursor.read_u16("Incomplete Struct16 length")? as usize
        }
        _ => return Ok(None),
    };
    let tag = cursor.read_u8("Unexpected EOF reading structure signature tag")?;
    if num_fields > cursor.remaining() {
        return Err(NetError::ProtocolError(format!(
            "Structure field count {} exceeds available buffer bytes {} (minimum 1 byte per field required)",
            num_fields,
            cursor.remaining()
        )));
    }
    Ok(Some((num_fields, tag)))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "bolt")]
    use crate::bolt::messages::BoltResponse;
    use crate::bolt::packstream::{
        BoltDate, BoltDateTime, BoltDateTimeZoneId, BoltDuration, BoltLocalDateTime, BoltLocalTime,
        BoltNode, BoltPath, BoltPoint2D, BoltPoint3D, BoltRelationship, BoltTime,
        BoltUnboundRelationship, BoltValue, PackStream,
    };
    use bytes::{Bytes, BytesMut};

    fn assert_equiv(val: &BoltValue) {
        let mut buf = BytesMut::new();
        PackStream::encode(val, &mut buf);
        let bytes = buf.freeze();

        let mut old_bytes = bytes.clone();
        let old_rv = ResultValue::from(PackStream::decode(&mut old_bytes).unwrap());

        let (new_rv, consumed) = decode_result_value_slice(&bytes).unwrap();
        assert_eq!(consumed, bytes.len(), "consumed bytes mismatch for {val:?}");
        assert_eq!(new_rv, old_rv, "ResultValue mismatch for {val:?}");
        let new_json: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&new_rv.as_canonical_json()).unwrap())
                .unwrap();
        let old_json: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&old_rv.as_canonical_json()).unwrap())
                .unwrap();
        assert_eq!(new_json, old_json, "canonical JSON mismatch for {val:?}");
    }

    #[test]
    fn direct_decoder_matches_packstream_decode_across_all_types() {
        let mut props = HashMap::new();
        props.insert(
            "name".to_string(),
            BoltValue::String("Alice 🚀".to_string()),
        );
        props.insert("age".to_string(), BoltValue::Integer(30));
        props.insert("neg".to_string(), BoltValue::Integer(-500_000));
        props.insert("big".to_string(), BoltValue::Integer(i64::MAX));
        props.insert("min".to_string(), BoltValue::Integer(i64::MIN));
        props.insert("score".to_string(), BoltValue::Float(3.125));
        props.insert("active".to_string(), BoltValue::Boolean(true));
        props.insert("empty".to_string(), BoltValue::Null);
        props.insert("raw".to_string(), BoltValue::Bytes(vec![0, 1, 255, 42]));

        // Large map (> 16 entries) to exercise the HashMap-indexed deduplication branch
        let mut large_map = HashMap::new();
        for i in 0..24 {
            large_map.insert(format!("key_{i}"), BoltValue::Integer(i));
        }

        let node_v5 = BoltNode {
            id: 42,
            labels: vec!["Person".to_string(), "Admin".to_string()],
            properties: props.clone(),
            element_id: Some("4:abc:42".to_string()),
        };
        let node_v4 = BoltNode {
            id: 43,
            labels: vec![],
            properties: HashMap::new(),
            element_id: None,
        };
        let rel_v5 = BoltRelationship {
            id: 100,
            start_node_id: 42,
            end_node_id: 43,
            rel_type: "KNOWS".to_string(),
            properties: props.clone(),
            element_id: Some("5:abc:100".to_string()),
            start_element_id: Some("4:abc:42".to_string()),
            end_element_id: Some("4:abc:43".to_string()),
        };
        let rel_v4 = BoltRelationship {
            id: 101,
            start_node_id: 43,
            end_node_id: 42,
            rel_type: "FOLLOWS".to_string(),
            properties: HashMap::new(),
            element_id: None,
            start_element_id: None,
            end_element_id: None,
        };

        let urel = BoltUnboundRelationship {
            id: 200,
            rel_type: "LINKS".to_string(),
            properties: props.clone(),
            element_id: Some("5:abc:200".to_string()),
        };
        // Linear path
        let path_linear = BoltPath {
            nodes: vec![node_v5.clone(), node_v4.clone()],
            relationships: vec![urel.clone()],
            sequence: vec![1, 1],
        };
        // Reversed edge path
        let path_reversed = BoltPath {
            nodes: vec![node_v5.clone(), node_v4.clone()],
            relationships: vec![urel.clone()],
            sequence: vec![-1, 1],
        };
        // Cyclic path revisiting node 0 and edge 1
        let path_cyclic = BoltPath {
            nodes: vec![node_v5.clone(), node_v4.clone()],
            relationships: vec![urel.clone()],
            sequence: vec![1, 1, -1, 0],
        };
        // Empty-sequence path
        let path_empty_seq = BoltPath {
            nodes: vec![node_v5.clone()],
            relationships: vec![],
            sequence: vec![],
        };

        let cases = vec![
            BoltValue::Null,
            BoltValue::Boolean(false),
            BoltValue::Boolean(true),
            BoltValue::Integer(0),
            BoltValue::Integer(127),
            BoltValue::Integer(-16),
            BoltValue::Integer(-17),
            BoltValue::Integer(1000),
            BoltValue::Integer(100_000),
            BoltValue::Integer(i64::MIN),
            BoltValue::Integer(i64::MAX),
            BoltValue::Float(0.0),
            BoltValue::Float(-42.5),
            BoltValue::Bytes(vec![]),
            BoltValue::Bytes(vec![0xAB; 300]),
            BoltValue::String(String::new()),
            BoltValue::String("short".to_string()),
            BoltValue::String("x".repeat(300)),
            BoltValue::List(vec![]),
            BoltValue::List(vec![
                BoltValue::Integer(1),
                BoltValue::String("a".to_string()),
                BoltValue::Null,
            ]),
            BoltValue::Map(HashMap::new()),
            BoltValue::Map(props),
            BoltValue::Map(large_map),
            BoltValue::Node(node_v5),
            BoltValue::Node(node_v4),
            BoltValue::Relationship(rel_v5),
            BoltValue::Relationship(rel_v4),
            BoltValue::Path(path_linear),
            BoltValue::Path(path_reversed),
            BoltValue::Path(path_cyclic),
            BoltValue::Path(path_empty_seq),
            BoltValue::Point2D(BoltPoint2D {
                srid: 4326,
                x: -122.4,
                y: 37.8,
            }),
            BoltValue::Point3D(BoltPoint3D {
                srid: 4979,
                x: 1.0,
                y: 2.0,
                z: 3.0,
            }),
            BoltValue::Date(BoltDate::new(19_723)),
            BoltValue::Time(BoltTime::new(12 * 3_600 * 1_000_000_000, 7200)),
            BoltValue::LocalTime(BoltLocalTime::new(9 * 3_600 * 1_000_000_000)),
            BoltValue::DateTime(BoltDateTime::new(1_700_000_000, 123_456, -18_000)),
            BoltValue::LocalDateTime(BoltLocalDateTime::new(1_700_000_000, 999)),
            BoltValue::DateTimeZoneId(BoltDateTimeZoneId::new(1_700_000_000, 42, "Europe/Berlin")),
            BoltValue::DateTimeZoneId(BoltDateTimeZoneId::new_local(1_700_000_000, 42, "UTC")),
            BoltValue::Duration(BoltDuration::new(14, 3, 45, 123_456_789)),
            // Legacy Bolt v4 DateTime offset (0x46)
            BoltValue::Structure {
                tag: 0x46,
                fields: vec![
                    BoltValue::Integer(1_700_007_200),
                    BoltValue::Integer(500),
                    BoltValue::Integer(7200),
                ],
            },
            // Unknown custom structure
            BoltValue::Structure {
                tag: 0x22,
                fields: vec![
                    BoltValue::Integer(1),
                    BoltValue::String("custom".to_string()),
                ],
            },
            // Malformed-field known structures that must fall back to Structure
            BoltValue::Structure {
                tag: 0x4E,
                fields: vec![BoltValue::String("not-an-int".to_string())],
            },
            BoltValue::Structure {
                tag: 0x44,
                fields: vec![BoltValue::String("not-a-day".to_string())],
            },
            BoltValue::Structure {
                tag: 0x58,
                fields: vec![
                    BoltValue::Integer(4326),
                    BoltValue::String("bad-x".to_string()),
                    BoltValue::Float(1.0),
                ],
            },
            BoltValue::Structure {
                tag: 0x69,
                fields: vec![
                    BoltValue::Integer(1),
                    BoltValue::Integer(2),
                    BoltValue::Integer(3),
                ],
            },
        ];

        for case in &cases {
            assert_equiv(case);
        }
    }

    #[test]
    fn duplicate_map_keys_last_value_wins_matching_hashmap() {
        // TinyMap(2): "a" -> 1, "a" -> 2
        let wire: &[u8] = &[0xA2, 0x81, b'a', 0x01, 0x81, b'a', 0x02];
        let mut old_bytes = Bytes::copy_from_slice(wire);
        let old_rv = ResultValue::from(PackStream::decode(&mut old_bytes).unwrap());
        let (new_rv, consumed) = decode_result_value_slice(wire).unwrap();
        assert_eq!(consumed, wire.len());
        assert_eq!(new_rv, old_rv);
    }

    #[cfg(feature = "bolt")]
    #[test]
    fn record_frame_decoder_matches_bolt_response_and_interns_across_rows() {
        let mut decoder = BoltResultDecoder::new();
        for i in 0..10i64 {
            let mut props = HashMap::new();
            props.insert("name".to_string(), BoltValue::String(format!("u_{i}")));
            props.insert("age".to_string(), BoltValue::Integer(20 + i));
            let row = vec![
                BoltValue::Node(BoltNode {
                    id: i,
                    labels: vec!["Person".to_string()],
                    properties: props,
                    element_id: Some(format!("4:x:{i}")),
                }),
                BoltValue::DateTimeZoneId(BoltDateTimeZoneId::new(1_700_000_000 + i, 0, "UTC")),
            ];
            let rec = BoltValue::Structure {
                tag: 0x71,
                fields: vec![BoltValue::List(row)],
            };
            let mut buf = BytesMut::new();
            PackStream::encode(&rec, &mut buf);
            let frame = buf.freeze();

            assert_eq!(peek_bolt_message_tag(&frame), Some(0x71));
            let new_row = decoder.decode_record_frame(&frame).unwrap();

            let mut old_buf = frame.clone();
            let BoltResponse::Record { fields } = BoltResponse::decode(&mut old_buf).unwrap()
            else {
                panic!("expected RECORD");
            };
            let old_row: Vec<ResultValue> = fields.into_iter().map(ResultValue::from).collect();
            assert_eq!(new_row, old_row);
        }
        // Interner holds only the 4 distinct keys/labels/zones: "Person", "name", "age", "UTC"
        assert_eq!(decoder.interner.as_ref().unwrap().len(), 4);
    }

    #[test]
    fn lazy_interner_stays_unallocated_for_scalar_rows_and_clears_cleanly() {
        let mut decoder = BoltResultDecoder::new();
        assert!(decoder.interner.is_none());

        let scalar_rec = BoltValue::Structure {
            tag: 0x71,
            fields: vec![BoltValue::List(vec![
                BoltValue::Integer(42),
                BoltValue::String("hello".to_string()),
                BoltValue::Boolean(true),
                BoltValue::Float(1.5),
            ])],
        };
        let mut buf = BytesMut::new();
        PackStream::encode(&scalar_rec, &mut buf);
        let frame = buf.freeze();

        let row = decoder.decode_record_frame(&frame).unwrap();
        assert_eq!(row.len(), 4);
        assert!(
            decoder.interner.is_none(),
            "scalar-only rows must not allocate the interner HashSet"
        );

        // Now decode a map: interner is allocated lazily
        let mut map = HashMap::new();
        map.insert("k".to_string(), BoltValue::Integer(1));
        let map_rec = BoltValue::Structure {
            tag: 0x71,
            fields: vec![BoltValue::List(vec![BoltValue::Map(map)])],
        };
        let mut buf2 = BytesMut::new();
        PackStream::encode(&map_rec, &mut buf2);
        let frame2 = buf2.freeze();
        let _ = decoder.decode_record_frame(&frame2).unwrap();
        assert_eq!(decoder.interner.as_ref().unwrap().len(), 1);

        decoder.clear();
        assert_eq!(decoder.interner.as_ref().unwrap().len(), 0);
    }

    #[test]
    fn interner_respects_max_cap_on_high_cardinality_keys() {
        let mut decoder = BoltResultDecoder::new();
        let mut map = HashMap::new();
        for i in 0..(MAX_INTERNED_STRINGS + 100) {
            map.insert(format!("k_{i}"), BoltValue::Integer(i as i64));
        }
        let mut buf = BytesMut::new();
        PackStream::encode(&BoltValue::Map(map), &mut buf);
        let bytes = buf.freeze();

        let (rv, consumed) = decoder.decode_value_slice(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert!(matches!(rv, ResultValue::Map(m) if m.len() == MAX_INTERNED_STRINGS + 100));
        assert!(decoder.interner.as_ref().unwrap().len() <= MAX_INTERNED_STRINGS);
    }

    #[test]
    fn direct_decoder_rejects_invalid_utf8_in_string_key_label_and_zone() {
        // Invalid UTF-8 bytes 0xFF 0xFE
        let bad_str: &[u8] = &[0x82, 0xFF, 0xFE];
        assert!(matches!(
            decode_result_value_slice(bad_str),
            Err(NetError::ProtocolError(_))
        ));

        // TinyMap(1) with invalid UTF-8 key
        let bad_key: &[u8] = &[0xA1, 0x82, 0xFF, 0xFE, 0x01];
        assert!(matches!(
            decode_result_value_slice(bad_key),
            Err(NetError::ProtocolError(_))
        ));

        // Node(0x4E) with invalid UTF-8 label
        let bad_label: &[u8] = &[0xB3, 0x4E, 0x01, 0x91, 0x82, 0xFF, 0xFE, 0xA0];
        assert!(matches!(
            decode_result_value_slice(bad_label),
            Err(NetError::ProtocolError(_))
        ));

        // DateTimeZoneId(0x69) with invalid UTF-8 zone_id
        let bad_zone: &[u8] = &[0xB3, 0x69, 0x01, 0x02, 0x82, 0xFF, 0xFE];
        assert!(matches!(
            decode_result_value_slice(bad_zone),
            Err(NetError::ProtocolError(_))
        ));
    }

    #[cfg(feature = "bolt")]
    #[test]
    fn success_slice_decoders_match_bolt_response_decode() {
        let mut fields_meta = HashMap::new();
        fields_meta.insert(
            "fields".to_string(),
            BoltValue::List(vec![
                BoltValue::String("a".to_string()),
                BoltValue::Integer(99), // skipped just like filter_map(as_str)
                BoltValue::String("b".to_string()),
            ]),
        );
        fields_meta.insert("t_first".to_string(), BoltValue::Integer(4));
        fields_meta.insert(
            "extra_nested".to_string(),
            BoltValue::Map(HashMap::from([(
                "k".to_string(),
                BoltValue::List(vec![BoltValue::Float(1.5), BoltValue::Bytes(vec![1, 2])]),
            )])),
        );
        let run_success = BoltValue::Structure {
            tag: 0x70,
            fields: vec![BoltValue::Map(fields_meta)],
        };
        let mut run_buf = BytesMut::new();
        PackStream::encode(&run_success, &mut run_buf);
        let cols = decode_run_success_columns(&run_buf).unwrap().unwrap();
        assert_eq!(cols, vec!["a".to_string(), "b".to_string()]);

        let stats = HashMap::from([
            ("nodes-created".to_string(), BoltValue::Integer(3)),
            ("nodes-deleted".to_string(), BoltValue::Integer(1)),
            ("relationships-created".to_string(), BoltValue::Integer(2)),
            ("relationships-deleted".to_string(), BoltValue::Integer(4)),
            ("properties-set".to_string(), BoltValue::Integer(5)),
            ("labels-added".to_string(), BoltValue::Integer(6)),
            ("labels-removed".to_string(), BoltValue::Integer(7)),
            ("indexes-added".to_string(), BoltValue::Integer(8)),
            ("constraints-added".to_string(), BoltValue::Integer(9)),
            ("other-stat".to_string(), BoltValue::Integer(100)),
        ]);
        let pull_meta = HashMap::from([
            ("stats".to_string(), BoltValue::Map(stats)),
            ("t_first".to_string(), BoltValue::Integer(11)),
            ("t_last".to_string(), BoltValue::Integer(42)),
            (
                "bookmark".to_string(),
                BoltValue::String("bm:1".to_string()),
            ),
            ("has_more".to_string(), BoltValue::Boolean(false)),
        ]);
        let pull_success = BoltValue::Structure {
            tag: 0x70,
            fields: vec![BoltValue::Map(pull_meta)],
        };
        let mut pull_buf = BytesMut::new();
        PackStream::encode(&pull_success, &mut pull_buf);
        let summary = decode_pull_success_summary(&pull_buf).unwrap().unwrap();
        assert_eq!(summary.nodes_created, 3);
        assert_eq!(summary.nodes_deleted, 1);
        assert_eq!(summary.relationships_created, 2);
        assert_eq!(summary.relationships_deleted, 4);
        assert_eq!(summary.properties_set, 5);
        assert_eq!(summary.labels_added, 6);
        assert_eq!(summary.labels_removed, 7);
        assert_eq!(summary.indexes_added, 8);
        assert_eq!(summary.constraints_added, 9);
        assert_eq!(summary.execution_time_ms, 42);

        // Non-SUCCESS tag (0x7F FAILURE) returns Ok(None) for fallback handling
        let failure = BoltValue::Structure {
            tag: 0x7F,
            fields: vec![BoltValue::Map(HashMap::new())],
        };
        let mut fail_buf = BytesMut::new();
        PackStream::encode(&failure, &mut fail_buf);
        assert!(decode_run_success_columns(&fail_buf).unwrap().is_none());
        assert!(decode_pull_success_summary(&fail_buf).unwrap().is_none());

        // Malformed UTF-8 in skipped metadata string is rejected
        let bad_run: &[u8] = &[0xB1, 0x70, 0xA1, 0x81, b'x', 0x82, 0xFF, 0xFE];
        assert!(matches!(
            decode_run_success_columns(bad_run),
            Err(NetError::ProtocolError(_))
        ));
    }
}
