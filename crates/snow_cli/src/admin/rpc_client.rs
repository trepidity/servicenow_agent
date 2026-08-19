//! Thin async wrapper over the daemon UDS client used by the admin TUI.
//!
//! This module deliberately avoids reimplementing JSON-RPC framing —
//! it delegates to [`crate::tui_client::DaemonRpcClient::call_raw`].

use anyhow::{Context, Result};
use serde_json::Value;

use crate::daemon_cmd::paths::DaemonPaths;
use crate::tui_client::DaemonRpcClient;

/// Wire-format mirror of `snow_daemon::jobs::JobKind`.
///
/// Kept independent of `snow_daemon` so the CLI does not pick up the daemon
/// crate as a type-level dependency for the admin TUI surface.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JobKindWire {
    KbSync,
    KbSyncFull,
    VerifyVault,
    PruneOrphans,
    RepairVault,
    SemanticIndexRebuild,
}

/// Admin TUI's view of the daemon RPC surface.
pub struct AdminRpc {
    paths: DaemonPaths,
    client: DaemonRpcClient,
}

impl AdminRpc {
    /// Resolve daemon paths and build the underlying RPC client.
    pub fn resolve() -> Result<Self> {
        let paths = DaemonPaths::resolve().context("failed to resolve daemon paths")?;
        let client = DaemonRpcClient::new(paths.socket.clone(), None);
        Ok(Self { paths, client })
    }

    /// Cheap heartbeat — uses the always-present `get_degraded_reads` method
    /// as a liveness probe.
    pub async fn ping(&self) -> Result<()> {
        self.call("get_degraded_reads", serde_json::json!({}))
            .await
            .map(|_| ())
    }

    /// Invoke a daemon RPC method by name and return the raw `result` value.
    pub async fn call(&self, method: &str, params: Value) -> Result<Value> {
        self.client
            .call_raw(method, params)
            .await
            .map_err(|err| anyhow::anyhow!(err.to_string()))
    }

    /// Access the resolved daemon paths (e.g. for log tailing).
    #[allow(dead_code)]
    pub fn paths(&self) -> &DaemonPaths {
        &self.paths
    }
}
