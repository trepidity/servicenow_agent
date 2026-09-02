//! Shared newline-framed JSON-RPC transport limits.
//!
//! Requests may carry a draft Knowledge body; responses must remain small
//! enough for MCP clients to consume without exhausting their context window.

use serde_json::{Value, json};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt};

/// Largest accepted newline-framed JSON-RPC request, including its delimiter.
pub const MAX_JSON_RPC_REQUEST_BYTES: usize = 256 * 1024;
/// Largest serialized JSON-RPC response, excluding its newline delimiter.
///
/// The complete MCP `tools/list` catalog is intentionally returned in one
/// response and currently needs roughly 76 KiB. Keep a finite transport
/// ceiling, but leave room for that required protocol response and modest
/// catalog growth.
pub const MAX_JSON_RPC_RESPONSE_BYTES: usize = 128 * 1024;
/// JSON-RPC server error used when a transport frame or result exceeds its budget.
pub const RESULT_TOO_LARGE_CODE: i64 = -32070;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameRead {
    Eof,
    Frame,
    TooLarge,
}

/// Read one newline-framed request without allocating beyond the request budget.
pub async fn read_frame<R>(reader: &mut R, buffer: &mut Vec<u8>) -> std::io::Result<FrameRead>
where
    R: AsyncBufRead + Unpin,
{
    buffer.clear();
    let mut limited = reader.take((MAX_JSON_RPC_REQUEST_BYTES + 1) as u64);
    let read = limited.read_until(b'\n', buffer).await?;
    drop(limited);

    if read == 0 {
        return Ok(FrameRead::Eof);
    }
    if buffer.len() > MAX_JSON_RPC_REQUEST_BYTES
        || (buffer.len() == MAX_JSON_RPC_REQUEST_BYTES && buffer.last() != Some(&b'\n'))
    {
        return Ok(FrameRead::TooLarge);
    }
    Ok(FrameRead::Frame)
}

pub fn result_too_large_data(actual_bytes: usize) -> Value {
    json!({
        "code": "RESULT_TOO_LARGE",
        "max_bytes": MAX_JSON_RPC_RESPONSE_BYTES,
        "result_bytes": actual_bytes,
    })
}

pub fn request_too_large_data() -> Value {
    json!({
        "code": "RESULT_TOO_LARGE",
        "max_bytes": MAX_JSON_RPC_REQUEST_BYTES,
    })
}
