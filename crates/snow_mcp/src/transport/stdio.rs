use std::future::Future;

use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt};

use crate::Result;
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
        let mut line = String::new();

        loop {
            tokio::select! {
                _ = &mut shutdown => break,
                read = reader.read_line(&mut line) => {
                    let read = read?;
                    if read == 0 {
                        break;
                    }
                    let response = match serde_json::from_str::<JsonRpcRequest>(&line) {
                        Ok(request) => server.dispatch(request).await,
                        Err(err) => JsonRpcResponse::error(
                            None,
                            -32700,
                            "parse error",
                            Some(serde_json::json!({ "details": err.to_string() })),
                        ),
                    };
                    let payload = serde_json::to_vec(&response)?;
                    writer.write_all(&payload).await?;
                    writer.write_all(b"\n").await?;
                    writer.flush().await?;
                    line.clear();
                }
            }
        }

        Ok(())
    }
}
