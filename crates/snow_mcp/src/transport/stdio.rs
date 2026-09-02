use std::future::Future;

use tokio::io::{AsyncBufRead, AsyncWrite, AsyncWriteExt};

use crate::Result;
use crate::protocol::frame::{
    FrameRead, MAX_JSON_RPC_RESPONSE_BYTES, RESULT_TOO_LARGE_CODE, read_frame,
    request_too_large_data, result_too_large_data,
};
use crate::protocol::schema::{JsonRpcRequest, JsonRpcResponse};
use crate::server::McpServer;

pub struct StdioTransport;

impl StdioTransport {
    pub async fn serve_streams<R, W, F>(
        server: &McpServer,
        mut reader: R,
        mut writer: W,
        shutdown: F,
    ) -> Result<()>
    where
        R: AsyncBufRead + Unpin,
        W: AsyncWrite + Unpin,
        F: Future<Output = std::result::Result<(), std::io::Error>>,
    {
        tokio::pin!(shutdown);
        let mut frame = Vec::new();

        loop {
            tokio::select! {
                _ = &mut shutdown => break,
                read = read_frame(&mut reader, &mut frame) => {
                    let frame_status = read?;
                    let close_after_response = frame_status == FrameRead::TooLarge;
                    let response = match frame_status {
                        FrameRead::Eof => break,
                        FrameRead::TooLarge => JsonRpcResponse::error(
                            None,
                            RESULT_TOO_LARGE_CODE,
                            "request frame exceeds maximum size",
                            Some(request_too_large_data()),
                        ),
                        FrameRead::Frame => match serde_json::from_slice::<JsonRpcRequest>(&frame) {
                        Ok(request) => server.dispatch(request).await,
                        Err(err) => JsonRpcResponse::error(
                            None,
                            -32700,
                            "parse error",
                            Some(serde_json::json!({ "details": err.to_string() })),
                        ),
                        },
                    };
                    let payload = bounded_payload(response)?;
                    writer.write_all(&payload).await?;
                    writer.write_all(b"\n").await?;
                    writer.flush().await?;
                    if close_after_response {
                        break;
                    }
                }
            }
        }

        Ok(())
    }
}

pub(crate) fn bounded_payload(response: JsonRpcResponse) -> Result<Vec<u8>> {
    let payload = serde_json::to_vec(&response)?;
    if payload.len() <= MAX_JSON_RPC_RESPONSE_BYTES {
        return Ok(payload);
    }

    let fallback = JsonRpcResponse::error(
        response.id,
        RESULT_TOO_LARGE_CODE,
        "result exceeds maximum size",
        Some(result_too_large_data(payload.len())),
    );
    let fallback_payload = serde_json::to_vec(&fallback)?;
    if fallback_payload.len() <= MAX_JSON_RPC_RESPONSE_BYTES {
        return Ok(fallback_payload);
    }

    Ok(serde_json::to_vec(&JsonRpcResponse::error(
        None,
        RESULT_TOO_LARGE_CODE,
        "result exceeds maximum size",
        Some(result_too_large_data(payload.len())),
    ))?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn response_budget_returns_a_small_structured_error() {
        let payload = bounded_payload(JsonRpcResponse::ok(
            Some(json!(1)),
            json!({ "payload": "x".repeat(MAX_JSON_RPC_RESPONSE_BYTES) }),
        ))
        .expect("response encoding");

        assert!(payload.len() <= MAX_JSON_RPC_RESPONSE_BYTES);
        let response: serde_json::Value = serde_json::from_slice(&payload).expect("JSON-RPC");
        assert_eq!(response["error"]["code"], RESULT_TOO_LARGE_CODE);
        assert_eq!(response["error"]["data"]["code"], "RESULT_TOO_LARGE");
    }
}
