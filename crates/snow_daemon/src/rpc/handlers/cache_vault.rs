use super::*;

#[derive(Debug, Serialize)]
pub(in crate::rpc) struct CacheInfo {
    pub(in crate::rpc) vault_path: String,
    pub(in crate::rpc) sqlite_path: String,
    pub(in crate::rpc) schema_version: i64,
    pub(in crate::rpc) db_size_mb: u64,
    pub(in crate::rpc) total_rows: u64,
}

pub(in crate::rpc) fn cache_info(core: &SnowCore) -> Result<CacheInfo> {
    let status = core.cache_status()?;
    // Keep the established wire field stable while the cache itself is now
    // identified by an exact format marker rather than an upgrade sequence.
    let schema_version = 11;

    Ok(CacheInfo {
        vault_path: status.vault_path.display().to_string(),
        sqlite_path: status.sqlite_path.display().to_string(),
        schema_version,
        db_size_mb: status.db_size_bytes / (1024 * 1024),
        total_rows: status.total_rows,
    })
}

pub(in crate::rpc) async fn dispatch_cache_vault(
    method: RpcMethod,
    id: Option<Value>,
    request: &JsonRpcRequest,
    state: &Arc<DaemonState>,
    transport: &DaemonTransport<'_>,
) -> JsonRpcResponse {
    match method {
        RpcMethod::VaultPath => JsonRpcResponse::ok(id, json!({ "path": transport.vault_path() })),
        RpcMethod::GetDegradedReads => JsonRpcResponse::ok(
            id,
            json!({
                "records": state.core.degraded_reads(),
            }),
        ),
        RpcMethod::CacheInfo => match cache_info(state.core.as_ref()) {
            Ok(info) => JsonRpcResponse::ok(id, json!(info)),
            Err(err) => internal_error(id, err),
        },
        RpcMethod::RepairVault => match state.core.repair_vault().await {
            Ok(report) => JsonRpcResponse::ok(id, json!({ "report": report })),
            Err(err) => internal_error(id, err),
        },
        RpcMethod::RebuildCache => match state.core.rebuild_cache() {
            Ok(report) => JsonRpcResponse::ok(id, json!({ "report": report })),
            Err(err) => internal_error(id, err),
        },
        RpcMethod::VerifyVault => match state.core.verify_vault() {
            Ok(report) => JsonRpcResponse::ok(id, json!({ "report": report })),
            Err(err) => internal_error(id, err),
        },
        RpcMethod::PruneOrphans => {
            let dry_run = request
                .params
                .get("dry_run")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            match state.core.prune_orphans(dry_run).await {
                Ok(report) => JsonRpcResponse::ok(id, json!({ "report": report })),
                Err(err) => internal_error(id, err),
            }
        }
        _ => unreachable!("method routed to the wrong RPC feature handler"),
    }
}
