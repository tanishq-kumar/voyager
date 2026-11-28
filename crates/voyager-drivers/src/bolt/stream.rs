//! Bolt chunk framing encoder and decoder for TCP streams.

use std::ops::Deref;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::error::{NetError, Result};

/// Maximum size of a single Bolt chunk payload in bytes (64 KB - 1).
pub const MAX_CHUNK_SIZE: usize = 65535;
/// Default chunk size used when splitting outbound messages (64 KB - 1 for max TCP throughput).
pub const DEFAULT_CHUNK_SIZE: usize = MAX_CHUNK_SIZE;
/// Maximum allowed Bolt message frame size (128 MB) to prevent unbounded memory allocation.
pub const MAX_BOLT_MESSAGE_SIZE: usize = 128 * 1024 * 1024;

/// Formats a raw PackStream message payload into Bolt chunk frames with `0x00 0x00` terminator.
pub fn encode_chunks(payload: &[u8], buf: &mut BytesMut) {
    let mut offset = 0;
    while offset < payload.len() {
        let chunk_len = (payload.len() - offset).min(MAX_CHUNK_SIZE);
        buf.put_u16(chunk_len as u16);
        buf.put_slice(&payload[offset..offset + chunk_len]);
        offset += chunk_len;
    }
    // Terminating empty chunk
    buf.put_u16(0);
}

/// Reads and reassembles a chunked Bolt message from an asynchronous reader using a persistent read buffer.
pub async fn read_message_frame_buffered<R: AsyncRead + Unpin>(
    reader: &mut R,
    read_buf: &mut BytesMut,
) -> Result<Bytes> {
    let mut message_buf = BytesMut::new();

    loop {
        // 1. Ensure at least 2 bytes are available in read_buf for chunk header
        while read_buf.len() < 2 {
            let bytes_read = reader.read_buf(read_buf).await.map_err(|e| {
                NetError::ConnectionFailed(format!("Failed to read chunk header: {}", e))
            })?;
            if bytes_read == 0 {
                return Err(NetError::ConnectionFailed(
                    "Connection closed by peer while waiting for chunk header".to_string(),
                ));
            }
        }

        let chunk_size = u16::from_be_bytes([read_buf[0], read_buf[1]]) as usize;
        read_buf.advance(2);

        if chunk_size == 0 {
            if message_buf.is_empty() {
                // Bolt NOOP keepalive chunk (0x00 0x00) sent while awaiting results.
                continue;
            }
            // End of message frame reached
            break;
        }

        if message_buf.len() + chunk_size > MAX_BOLT_MESSAGE_SIZE {
            return Err(NetError::ProtocolError(format!(
                "Bolt message frame exceeded maximum allowed size of {} bytes",
                MAX_BOLT_MESSAGE_SIZE
            )));
        }

        // 2. Ensure all chunk_size bytes for payload are present in read_buf
        while read_buf.len() < chunk_size {
            let bytes_read = reader.read_buf(read_buf).await.map_err(|e| {
                NetError::ConnectionFailed(format!("Failed to read chunk payload: {}", e))
            })?;
            if bytes_read == 0 {
                return Err(NetError::ConnectionFailed(
                    "Connection closed by peer mid-chunk payload".to_string(),
                ));
            }
        }

        message_buf.extend_from_slice(&read_buf[..chunk_size]);
        read_buf.advance(chunk_size);
    }

    Ok(message_buf.freeze())
}

/// Zero-copy / zero-allocation view of a single Bolt message frame read from a buffered stream.
///
/// For single-chunk messages (the common case for `RECORD` rows), borrows directly from the
/// caller's persistent `read_buf` and advances `read_buf` on drop. For multi-chunk messages,
/// reassembles chunks into the caller's reusable `scratch` buffer.
pub enum BoltFrame<'a> {
    /// Single-chunk frame borrowed directly from `read_buf`.
    Direct {
        /// Persistent connection read buffer holding the frame bytes at offset `0..payload_len`.
        read_buf: &'a mut BytesMut,
        /// Byte length of the single chunk payload.
        payload_len: usize,
        /// Total bytes to advance on drop (`payload_len + 2` for the `0x00 0x00` terminator).
        consume_len: usize,
    },
    /// Multi-chunk frame reassembled in `scratch`.
    Buffered(&'a [u8]),
}

impl Deref for BoltFrame<'_> {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        match self {
            Self::Direct {
                read_buf,
                payload_len,
                ..
            } => read_buf.get(..*payload_len).unwrap_or_default(),
            Self::Buffered(slice) => slice,
        }
    }
}

impl Drop for BoltFrame<'_> {
    #[inline]
    fn drop(&mut self) {
        if let Self::Direct {
            read_buf,
            consume_len,
            ..
        } = self
        {
            let n = (*consume_len).min(read_buf.len());
            read_buf.advance(n);
        }
    }
}

/// Reads and reassembles a chunked Bolt message into a borrowed [`BoltFrame`] with zero
/// per-message heap allocations.
///
/// Enforces the same NOOP keepalive (`0x00 0x00`) skipping and [`MAX_BOLT_MESSAGE_SIZE`]
/// limit as [`read_message_frame_buffered`].
pub async fn read_message_frame_borrowed<'a, R: AsyncRead + Unpin>(
    reader: &mut R,
    read_buf: &'a mut BytesMut,
    scratch: &'a mut Vec<u8>,
) -> Result<BoltFrame<'a>> {
    // 1. Read the first non-empty chunk header (skipping NOOP 0x00 0x00 keepalive chunks).
    let first_chunk_size = loop {
        while read_buf.len() < 2 {
            let bytes_read = reader.read_buf(read_buf).await.map_err(|e| {
                NetError::ConnectionFailed(format!("Failed to read chunk header: {}", e))
            })?;
            if bytes_read == 0 {
                return Err(NetError::ConnectionFailed(
                    "Connection closed by peer while waiting for chunk header".to_string(),
                ));
            }
        }
        let chunk_size = u16::from_be_bytes([read_buf[0], read_buf[1]]) as usize;
        read_buf.advance(2);
        if chunk_size == 0 {
            // Bolt NOOP keepalive chunk (0x00 0x00) sent while awaiting results.
            continue;
        }
        if chunk_size > MAX_BOLT_MESSAGE_SIZE {
            return Err(NetError::ProtocolError(format!(
                "Bolt message frame exceeded maximum allowed size of {} bytes",
                MAX_BOLT_MESSAGE_SIZE
            )));
        }
        break chunk_size;
    };

    // 2. Ensure the first chunk payload plus the following 2-byte next-chunk header are buffered.
    let needed = first_chunk_size + 2;
    if read_buf.len() < needed {
        read_buf.reserve(needed - read_buf.len());
        while read_buf.len() < needed {
            let bytes_read = reader.read_buf(read_buf).await.map_err(|e| {
                NetError::ConnectionFailed(format!("Failed to read chunk payload: {}", e))
            })?;
            if bytes_read == 0 {
                return Err(NetError::ConnectionFailed(
                    "Connection closed by peer mid-chunk payload".to_string(),
                ));
            }
        }
    }

    // 3. Fast path: single-chunk message terminated immediately by `0x00 0x00`.
    if read_buf[first_chunk_size] == 0 && read_buf[first_chunk_size + 1] == 0 {
        return Ok(BoltFrame::Direct {
            read_buf,
            payload_len: first_chunk_size,
            consume_len: needed,
        });
    }

    // 4. Multi-chunk path: reassemble chunks into reusable `scratch` buffer.
    scratch.clear();
    scratch.extend_from_slice(&read_buf[..first_chunk_size]);
    read_buf.advance(first_chunk_size);

    loop {
        while read_buf.len() < 2 {
            let bytes_read = reader.read_buf(read_buf).await.map_err(|e| {
                NetError::ConnectionFailed(format!("Failed to read chunk header: {}", e))
            })?;
            if bytes_read == 0 {
                return Err(NetError::ConnectionFailed(
                    "Connection closed by peer while waiting for chunk header".to_string(),
                ));
            }
        }

        let chunk_size = u16::from_be_bytes([read_buf[0], read_buf[1]]) as usize;
        read_buf.advance(2);

        if chunk_size == 0 {
            break;
        }

        if scratch.len() + chunk_size > MAX_BOLT_MESSAGE_SIZE {
            return Err(NetError::ProtocolError(format!(
                "Bolt message frame exceeded maximum allowed size of {} bytes",
                MAX_BOLT_MESSAGE_SIZE
            )));
        }

        if read_buf.len() < chunk_size {
            read_buf.reserve(chunk_size - read_buf.len());
            while read_buf.len() < chunk_size {
                let bytes_read = reader.read_buf(read_buf).await.map_err(|e| {
                    NetError::ConnectionFailed(format!("Failed to read chunk payload: {}", e))
                })?;
                if bytes_read == 0 {
                    return Err(NetError::ConnectionFailed(
                        "Connection closed by peer mid-chunk payload".to_string(),
                    ));
                }
            }
        }

        scratch.extend_from_slice(&read_buf[..chunk_size]);
        read_buf.advance(chunk_size);
    }

    Ok(BoltFrame::Buffered(scratch.as_slice()))
}

/// Reads and reassembles a chunked Bolt message from an asynchronous reader until `0x00 0x00` is encountered.
pub async fn read_message_frame<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Bytes> {
    let mut message_buf = BytesMut::new();

    loop {
        let mut header = [0u8; 2];
        reader.read_exact(&mut header).await.map_err(|e| {
            NetError::ConnectionFailed(format!("Failed to read chunk header: {}", e))
        })?;

        let chunk_size = u16::from_be_bytes(header) as usize;
        if chunk_size == 0 {
            if message_buf.is_empty() {
                // Bolt NOOP keepalive chunk (0x00 0x00) sent while awaiting results.
                continue;
            }
            // End of message frame reached
            break;
        }

        if message_buf.len() + chunk_size > MAX_BOLT_MESSAGE_SIZE {
            return Err(NetError::ProtocolError(format!(
                "Bolt message frame exceeded maximum allowed size of {} bytes",
                MAX_BOLT_MESSAGE_SIZE
            )));
        }

        let start_len = message_buf.len();
        message_buf.resize(start_len + chunk_size, 0);
        reader
            .read_exact(&mut message_buf[start_len..start_len + chunk_size])
            .await
            .map_err(|e| {
                NetError::ConnectionFailed(format!("Failed to read chunk payload: {}", e))
            })?;
    }

    Ok(message_buf.freeze())
}

/// Writes an encoded chunk buffer to an asynchronous writer and flushes the socket.
pub async fn write_message_frame<W: AsyncWrite + Unpin>(writer: &mut W, buf: &[u8]) -> Result<()> {
    writer
        .write_all(buf)
        .await
        .map_err(|e| NetError::ConnectionFailed(format!("Failed to write chunk frame: {}", e)))?;
    writer
        .flush()
        .await
        .map_err(|e| NetError::ConnectionFailed(format!("Failed to flush socket: {}", e)))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[tokio::test]
    async fn borrowed_frame_handles_single_chunk_multi_chunk_noop_and_pipelined() {
        let mut wire = BytesMut::new();
        // Leading NOOP keepalives
        wire.put_u16(0);
        wire.put_u16(0);
        // Single-chunk frame 1
        encode_chunks(b"hello-single", &mut wire);
        // NOOP between frames
        wire.put_u16(0);
        // Multi-chunk frame 2 (explicitly split into 2 chunks)
        wire.put_u16(5);
        wire.put_slice(b"part1");
        wire.put_u16(6);
        wire.put_slice(b"-part2");
        wire.put_u16(0);
        // Single-chunk frame 3
        encode_chunks(b"tail", &mut wire);

        let mut cursor = Cursor::new(wire.as_ref());
        let mut read_buf = BytesMut::with_capacity(64);
        let mut scratch = Vec::new();

        {
            let f1 = read_message_frame_borrowed(&mut cursor, &mut read_buf, &mut scratch)
                .await
                .unwrap();
            assert!(matches!(f1, BoltFrame::Direct { .. }));
            assert_eq!(&*f1, b"hello-single");
        }
        {
            let f2 = read_message_frame_borrowed(&mut cursor, &mut read_buf, &mut scratch)
                .await
                .unwrap();
            assert!(matches!(f2, BoltFrame::Buffered(_)));
            assert_eq!(&*f2, b"part1-part2");
        }
        {
            let f3 = read_message_frame_borrowed(&mut cursor, &mut read_buf, &mut scratch)
                .await
                .unwrap();
            assert!(matches!(f3, BoltFrame::Direct { .. }));
            assert_eq!(&*f3, b"tail");
        }
    }

    #[tokio::test]
    async fn borrowed_frame_rejects_truncated_stream() {
        let wire: &[u8] = &[0x00, 0x05, b'a', b'b'];
        let mut cursor = Cursor::new(wire);
        let mut read_buf = BytesMut::with_capacity(64);
        let mut scratch = Vec::new();
        let err = read_message_frame_borrowed(&mut cursor, &mut read_buf, &mut scratch).await;
        assert!(matches!(err, Err(NetError::ConnectionFailed(_))));
    }
}
