//! PostgreSQL asynchronous message framing, encoder, and decoder.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::error::{NetError, Result};
use crate::postgres::message::{BackendMessage, FrontendMessage, StartupMessage};

/// Maximum allowable single message size in bytes (128 MB safety guard).
pub const MAX_PG_MESSAGE_SIZE: usize = 128 * 1024 * 1024;

/// Default internal buffer capacity for socket writes.
pub const DEFAULT_BUFFER_CAPACITY: usize = 8 * 1024;

/// Capacity reserved for buffered backend reads: one large read fills many small messages.
pub const DEFAULT_READ_BUFFER_CAPACITY: usize = 64 * 1024;

/// Asynchronously encodes and writes the initial PostgreSQL `StartupMessage`.
pub async fn write_startup_message<W>(writer: &mut W, msg: &StartupMessage) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut buf = BytesMut::with_capacity(DEFAULT_BUFFER_CAPACITY);
    msg.encode(&mut buf)?;
    writer.write_all(&buf).await.map_err(NetError::IoError)?;
    writer.flush().await.map_err(NetError::IoError)?;
    Ok(())
}

/// Asynchronously encodes and writes a single `FrontendMessage`.
pub async fn write_frontend_message<W>(writer: &mut W, msg: &FrontendMessage) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut buf = BytesMut::with_capacity(DEFAULT_BUFFER_CAPACITY);
    msg.encode(&mut buf)?;
    writer.write_all(&buf).await.map_err(NetError::IoError)?;
    writer.flush().await.map_err(NetError::IoError)?;
    Ok(())
}

/// Asynchronously encodes and writes multiple `FrontendMessage`s in a single network flush.
pub async fn write_frontend_messages<W>(writer: &mut W, msgs: &[FrontendMessage]) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut buf = BytesMut::with_capacity(DEFAULT_BUFFER_CAPACITY);
    for msg in msgs {
        msg.encode(&mut buf)?;
    }
    writer.write_all(&buf).await.map_err(NetError::IoError)?;
    writer.flush().await.map_err(NetError::IoError)?;
    Ok(())
}

/// Encodes and writes an `Execute(portal, max_rows)` + `Flush` pair into a reusable
/// buffer and sends both in a single `write_all` + `flush` syscall without allocating.
pub async fn write_execute_flush<W>(
    writer: &mut W,
    buf: &mut BytesMut,
    portal: &str,
    max_rows: i32,
) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    if portal.as_bytes().contains(&0) {
        return Err(NetError::ProtocolError(
            "PostgreSQL Execute portal contains interior NUL byte".to_string(),
        ));
    }
    let body_len = portal
        .len()
        .checked_add(5)
        .and_then(|n| n.checked_add(4))
        .and_then(|n| i32::try_from(n).ok())
        .ok_or_else(|| {
            NetError::ProtocolError("PostgreSQL Execute message length overflow".to_string())
        })?;
    buf.clear();
    if buf.capacity() > 64 * 1024 {
        *buf = BytesMut::with_capacity(256);
    }
    buf.reserve(5 + portal.len() + 5 + 5);
    // Execute ('E')
    buf.put_u8(b'E');
    buf.put_i32(body_len);
    buf.put_slice(portal.as_bytes());
    buf.put_u8(0);
    buf.put_i32(max_rows);
    // Flush ('H')
    buf.put_u8(b'H');
    buf.put_i32(4);

    writer.write_all(buf).await.map_err(NetError::IoError)?;
    writer.flush().await.map_err(NetError::IoError)?;
    Ok(())
}

/// Fills `buf` until it holds at least `needed` bytes, reading in large chunks.
async fn fill_buffered<R>(reader: &mut R, buf: &mut BytesMut, needed: usize) -> Result<()>
where
    R: AsyncRead + Unpin,
{
    if buf.len() >= needed {
        return Ok(());
    }
    if buf.is_empty() {
        buf.reserve(needed.max(DEFAULT_READ_BUFFER_CAPACITY));
    }
    while buf.len() < needed {
        let bytes_read = reader.read_buf(buf).await.map_err(NetError::IoError)?;
        if bytes_read == 0 {
            return Err(NetError::Closed(
                "PostgreSQL connection closed unexpectedly by remote host".to_string(),
            ));
        }
    }
    Ok(())
}

/// Asynchronously reads and decodes the next `BackendMessage` from the network stream.
///
/// Reads go through the reusable `read_buf` in large chunks instead of three
/// syscalls (`read_u8`, `read_i32`, `read_exact`) and a fresh `Vec` per backend
/// message. The decoded payload is handed out as a zero-copy [`Bytes`] slice of
/// the buffered read.
pub async fn read_backend_message<R>(
    reader: &mut R,
    read_buf: &mut BytesMut,
) -> Result<BackendMessage>
where
    R: AsyncRead + Unpin,
{
    // Message header: 1-byte type identifier + 4-byte big-endian length.
    fill_buffered(reader, read_buf, 5).await?;
    let msg_type = read_buf[0];
    let length = i32::from_be_bytes([read_buf[1], read_buf[2], read_buf[3], read_buf[4]]);
    if length < 4 {
        return Err(NetError::ProtocolError(format!(
            "Invalid PostgreSQL message length: {}",
            length
        )));
    }

    let payload_len = (length - 4) as usize;
    if payload_len > MAX_PG_MESSAGE_SIZE {
        return Err(NetError::ProtocolError(format!(
            "PostgreSQL message size {} exceeds maximum allowed limit of {} bytes",
            payload_len, MAX_PG_MESSAGE_SIZE
        )));
    }

    let total = 5 + payload_len;
    fill_buffered(reader, read_buf, total).await?;
    read_buf.advance(5);
    let payload: Bytes = read_buf.split_to(payload_len).freeze();
    BackendMessage::decode(msg_type, payload)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::message::TransactionStatus;
    use tokio::io::{AsyncWriteExt, duplex};

    fn frame(msg_type: u8, payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(5 + payload.len());
        out.push(msg_type);
        out.extend_from_slice(&(4 + payload.len() as i32).to_be_bytes());
        out.extend_from_slice(payload);
        out
    }

    #[tokio::test]
    async fn buffered_read_decodes_consecutive_messages_from_one_read() {
        let (mut client, mut server) = duplex(4096);
        let mut wire = Vec::new();
        wire.extend_from_slice(&frame(b'C', b"SELECT 1\0"));
        wire.extend_from_slice(&frame(b'Z', b"I"));
        server.write_all(&wire).await.unwrap();
        drop(server);

        let mut read_buf = BytesMut::new();
        let first = read_backend_message(&mut client, &mut read_buf)
            .await
            .unwrap();
        assert_eq!(
            first,
            BackendMessage::CommandComplete("SELECT 1".to_string())
        );
        let second = read_backend_message(&mut client, &mut read_buf)
            .await
            .unwrap();
        assert_eq!(
            second,
            BackendMessage::ReadyForQuery(TransactionStatus::Idle)
        );
    }

    #[tokio::test]
    async fn buffered_read_reports_closed_on_eof() {
        let (mut client, server) = duplex(16);
        drop(server);
        let mut read_buf = BytesMut::new();
        let error = read_backend_message(&mut client, &mut read_buf)
            .await
            .unwrap_err();
        assert!(matches!(error, NetError::Closed(_)));
    }

    #[tokio::test]
    async fn buffered_read_rejects_invalid_message_length() {
        let (mut client, mut server) = duplex(4096);
        let mut wire = Vec::new();
        wire.push(b'Z');
        wire.extend_from_slice(&3i32.to_be_bytes());
        server.write_all(&wire).await.unwrap();

        let mut read_buf = BytesMut::new();
        let error = read_backend_message(&mut client, &mut read_buf)
            .await
            .unwrap_err();
        assert!(matches!(error, NetError::ProtocolError(_)));
    }
}
