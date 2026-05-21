use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use snow_core::ipc::IpcEndpoint;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use super::paths::DaemonPaths;

/// Probe whether the daemon endpoint accepts a connection.
pub(crate) fn endpoint_alive(paths: &DaemonPaths) -> bool {
    let endpoint = paths.endpoint.clone();
    block_on_endpoint(async move { endpoint.connect().await.is_ok() }).unwrap_or(false)
}

pub(crate) fn request(paths: &DaemonPaths, method: &str, params: Value) -> Result<Value> {
    let endpoint = paths.endpoint.clone();
    block_on_endpoint(async move { request_endpoint(endpoint, method, params).await })?
}

fn block_on_endpoint<F, T>(future: F) -> Result<T>
where
    F: std::future::Future<Output = T>,
{
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("building daemon endpoint probe runtime")?;
    Ok(runtime.block_on(future))
}

async fn request_endpoint(endpoint: IpcEndpoint, method: &str, params: Value) -> Result<Value> {
    let mut stream = endpoint
        .connect()
        .await
        .with_context(|| format!("connecting to daemon endpoint {endpoint}"))?;
    let payload = json!({
        "jsonrpc": "2.0",
        "method": method,
        "params": params,
        "id": 1,
    });
    stream
        .write_all(serde_json::to_string(&payload)?.as_bytes())
        .await?;
    stream.write_all(b"\n").await?;
    stream.flush().await?;

    let mut line = String::new();
    BufReader::new(stream).read_line(&mut line).await?;
    let response: Value = serde_json::from_str(&line)?;
    if let Some(error) = response.get("error") {
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("daemon request failed");
        bail!("{message}");
    }
    Ok(response.get("result").cloned().unwrap_or(Value::Null))
}
