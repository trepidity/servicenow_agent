use anyhow::{Context, Result, anyhow};
use interprocess::local_socket::tokio::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use snow_core::ipc::IpcEndpoint;
use snow_core::{
    KnowledgeSemanticSearchFilters, RecordLookup, ResourceType, SearchScope, SnowCore, SnowRecord,
    TaskSlaParentRef, cache::store::Store, query::filter::ListQuery,
};
use std::future::Future;
use std::io::ErrorKind;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::fs;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::signal;
use tokio::sync::Notify;
use tokio_util::task::TaskTracker;

use crate::transport::{
    DaemonBusinessApplicationDiagnostic, DaemonKnowledgeSemanticStatus, DaemonKnowledgeStatus,
    DaemonKnowledgeSyncOutcome, DaemonKnowledgeTagSummary, DaemonSemanticIndexSummary,
};
use crate::{DaemonState, transport::DaemonTransport};
use snow_core::vault::markdown::{
    render_approval_record, render_knowledge_article, render_snow_record,
};

mod wire;

pub use wire::{JsonRpcError, JsonRpcRequest, JsonRpcResponse, RpcMethod};

#[derive(Clone)]
pub struct JsonRpcServer {
    state: Arc<DaemonState>,
    endpoint: IpcEndpoint,
    shutdown: Arc<Notify>,
    drain_timeout: Duration,
    /// When `Some`, the daemon shuts itself down after this much time with no
    /// connected clients. `None` disables idle shutdown (pinned daemon).
    idle_timeout: Option<Duration>,
}

const CONNECTION_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// Default idle window after which an otherwise-unused daemon self-terminates.
/// Lazily auto-spawned daemons would otherwise linger until reboot.
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(30 * 60);

impl JsonRpcServer {
    pub fn new(state: Arc<DaemonState>, endpoint: impl Into<IpcEndpoint>) -> Self {
        Self {
            state,
            endpoint: endpoint.into(),
            shutdown: Arc::new(Notify::new()),
            drain_timeout: CONNECTION_DRAIN_TIMEOUT,
            idle_timeout: Some(DEFAULT_IDLE_TIMEOUT),
        }
    }

    /// Configure idle self-shutdown. `Some(d)` shuts the daemon down after `d`
    /// of inactivity with no connected clients; `None` disables it.
    pub fn with_idle_timeout(mut self, idle_timeout: Option<Duration>) -> Self {
        self.idle_timeout = idle_timeout;
        self
    }

    #[cfg(test)]
    fn with_drain_timeout(mut self, drain_timeout: Duration) -> Self {
        self.drain_timeout = drain_timeout;
        self
    }

    pub async fn serve(self) -> Result<()> {
        self.serve_until(shutdown_signal()).await
    }

    pub async fn serve_until<F>(self, shutdown: F) -> Result<()>
    where
        F: Future<Output = Result<(), std::io::Error>>,
    {
        prepare_listener(&self.endpoint).await?;
        let listener = self
            .endpoint
            .listen()
            .with_context(|| format!("failed to bind {}", self.endpoint))?;
        eprintln!("snow_daemon: json-rpc listening on {}", self.endpoint);
        let tracker = TaskTracker::new();
        tokio::pin!(shutdown);

        // Activity tracking for idle self-shutdown: a count of in-flight
        // connections and the instant of the most recent connect/disconnect.
        let active = Arc::new(AtomicUsize::new(0));
        let last_activity = Arc::new(Mutex::new(Instant::now()));
        let idle_monitor = self.idle_timeout.map(|timeout| {
            let active = Arc::clone(&active);
            let last_activity = Arc::clone(&last_activity);
            let shutdown = Arc::clone(&self.shutdown);
            tokio::task::spawn_local(idle_monitor(timeout, active, last_activity, shutdown))
        });

        loop {
            tokio::select! {
                accept = listener.accept() => {
                    let stream = accept?;
                    let state = Arc::clone(&self.state);
                    let shutdown = Arc::clone(&self.shutdown);
                    let active = Arc::clone(&active);
                    let last_activity = Arc::clone(&last_activity);
                    active.fetch_add(1, Ordering::SeqCst);
                    touch(&last_activity);
                    tracker.spawn_local(async move {
                        if let Err(err) = handle_connection(stream, state, shutdown).await {
                            eprintln!("json-rpc connection error: {err:#}");
                        }
                        active.fetch_sub(1, Ordering::SeqCst);
                        touch(&last_activity);
                    });
                }
                _ = &mut shutdown => {
                    break;
                }
                _ = self.shutdown.notified() => {
                    break;
                }
            }
        }

        if let Some(handle) = idle_monitor {
            handle.abort();
        }
        tracker.close();
        let _ = cleanup_listener(&self.endpoint).await;
        let _ = tokio::time::timeout(self.drain_timeout, tracker.wait()).await;

        Ok(())
    }
}

/// Record activity (a connect or disconnect) by resetting the idle clock.
fn touch(last_activity: &Mutex<Instant>) {
    if let Ok(mut guard) = last_activity.lock() {
        *guard = Instant::now();
    }
}

/// Trigger `shutdown` once there have been zero connected clients for at least
/// `timeout`. Polls on a fraction of the timeout so a short configured window
/// (e.g. in tests) is honored promptly without busy-spinning a long one.
async fn idle_monitor(
    timeout: Duration,
    active: Arc<AtomicUsize>,
    last_activity: Arc<Mutex<Instant>>,
    shutdown: Arc<Notify>,
) {
    let tick = (timeout / 4).clamp(Duration::from_millis(50), Duration::from_secs(60));
    loop {
        tokio::time::sleep(tick).await;
        if active.load(Ordering::SeqCst) != 0 {
            continue;
        }
        let idle_for = last_activity
            .lock()
            .map(|guard| guard.elapsed())
            .unwrap_or_default();
        if idle_for >= timeout {
            shutdown.notify_one();
            break;
        }
    }
}

async fn prepare_listener(endpoint: &IpcEndpoint) -> std::io::Result<()> {
    if let Some(path) = endpoint.filesystem_path() {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent).await?;
        }
        let _ = fs::remove_file(path).await;
    }
    Ok(())
}

async fn cleanup_listener(endpoint: &IpcEndpoint) -> std::io::Result<()> {
    if let Some(path) = endpoint.filesystem_path() {
        let _ = fs::remove_file(path).await;
    }
    Ok(())
}

async fn shutdown_signal() -> Result<(), std::io::Error> {
    #[cfg(unix)]
    {
        let mut terminate = signal::unix::signal(signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = signal::ctrl_c() => result,
            _ = terminate.recv() => Ok(()),
        }
    }

    #[cfg(not(unix))]
    {
        signal::ctrl_c().await
    }
}

async fn handle_connection<S>(
    stream: S,
    state: Arc<DaemonState>,
    shutdown: Arc<Notify>,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (reader, mut writer) = tokio::io::split(stream);
    let mut reader = BufReader::new(reader);
    let mut line = String::new();

    loop {
        line.clear();
        let read = match reader.read_line(&mut line).await {
            Ok(read) => read,
            Err(err) if is_peer_disconnect(&err) => break,
            Err(err) => return Err(err.into()),
        };
        if read == 0 {
            break;
        }

        let mut should_shutdown = false;
        let response = match serde_json::from_str::<JsonRpcRequest>(&line) {
            Ok(request) => {
                should_shutdown = RpcMethod::from_method(&request.method) == RpcMethod::Shutdown;
                dispatch(request, &state).await
            }
            Err(err) => JsonRpcResponse::error(
                None,
                -32700,
                "parse error",
                Some(json!({ "details": err.to_string() })),
            ),
        };

        let payload = serde_json::to_vec(&response)?;
        if let Err(err) = writer.write_all(&payload).await {
            if is_peer_disconnect(&err) {
                break;
            }
            return Err(err.into());
        }
        if let Err(err) = writer.write_all(b"\n").await {
            if is_peer_disconnect(&err) {
                break;
            }
            return Err(err.into());
        }
        writer.flush().await?;

        if should_shutdown {
            shutdown.notify_one();
            break;
        }
    }

    Ok(())
}

fn is_peer_disconnect(err: &std::io::Error) -> bool {
    matches!(
        err.kind(),
        ErrorKind::BrokenPipe
            | ErrorKind::ConnectionReset
            | ErrorKind::ConnectionAborted
            | ErrorKind::UnexpectedEof
    )
}

async fn dispatch(request: JsonRpcRequest, state: &Arc<DaemonState>) -> JsonRpcResponse {
    let id = request.id.clone();
    let transport = DaemonTransport::new(state.core.as_ref());
    match RpcMethod::from_method(&request.method) {
        RpcMethod::ContractInfo => JsonRpcResponse::ok(id, contract_info(state.as_ref())),
        RpcMethod::Ping => JsonRpcResponse::ok(id, json!({ "ok": true })),
        RpcMethod::Shutdown => JsonRpcResponse::ok(id, json!({ "status": "shutting_down" })),
        RpcMethod::VaultPath => JsonRpcResponse::ok(id, json!({ "path": transport.vault_path() })),
        RpcMethod::GetRecord => match extract_record_lookup(&request.params) {
            Ok(RecordLookup::Number(number)) => {
                match get_record_cached_or_fresh(state.core.as_ref(), &number).await {
                    Ok(Some(record)) => {
                        daemon_record_response_with_private_task_context(id, &transport, &record)
                            .await
                    }
                    Ok(None) => JsonRpcResponse::error(id, -32004, "record not found", None),
                    Err(err) => map_record_lookup_error(id, err),
                }
            }
            Ok(RecordLookup::TableSysId { table, sys_id }) => match state
                .core
                .get_record_by_table_sys_id_fresh(&table, &sys_id)
                .await
            {
                Ok(Some(record)) => {
                    daemon_record_response_with_private_task_context(id, &transport, &record).await
                }
                Ok(None) => JsonRpcResponse::error(id, -32004, "record not found", None),
                Err(err) => map_record_lookup_error(id, err),
            },
            Err(err) => invalid_params(id, err),
        },
        RpcMethod::GetRecordFresh => match extract_record_lookup(&request.params) {
            Ok(RecordLookup::Number(number)) => match state.core.get_record_fresh(&number).await {
                Ok(Some(record)) => {
                    daemon_record_response_with_private_task_context(id, &transport, &record).await
                }
                Ok(None) => JsonRpcResponse::error(id, -32004, "record not found", None),
                Err(err) => map_record_lookup_error(id, err),
            },
            Ok(RecordLookup::TableSysId { table, sys_id }) => match state
                .core
                .get_record_by_table_sys_id_fresh(&table, &sys_id)
                .await
            {
                Ok(Some(record)) => {
                    daemon_record_response_with_private_task_context(id, &transport, &record).await
                }
                Ok(None) => JsonRpcResponse::error(id, -32004, "record not found", None),
                Err(err) => map_record_lookup_error(id, err),
            },
            Err(err) => invalid_params(id, err),
        },
        RpcMethod::TaskSlaStatus => match extract_number(&request.params) {
            Ok(number) => match state.core.task_sla_status_for_number(&number).await {
                Ok(status) => JsonRpcResponse::ok(id, json!({ "status": status })),
                Err(err) => internal_error(id, err),
            },
            Err(err) => invalid_params(id, err),
        },
        RpcMethod::TaskSlaStatusForTasks => match extract_task_sla_parent_refs(&request.params) {
            Ok(parents) => match state.core.task_sla_status_for_tasks(&parents).await {
                Ok(statuses) => JsonRpcResponse::ok(id, json!({ "statuses": statuses })),
                Err(err) => internal_error(id, err),
            },
            Err(err) => invalid_params(id, err),
        },
        RpcMethod::GetKnowledgeArticle | RpcMethod::GetArticle => {
            match extract_number(&request.params) {
                Ok(number) => match state
                    .core
                    .get_knowledge_article_cached_or_fresh(&number)
                    .await
                {
                    Ok(Some(article)) => match transport.knowledge_article(&article) {
                        Ok(article_dto) => JsonRpcResponse::ok(
                            id,
                            json!({
                                "article": article_dto,
                                "markdown": render_knowledge_article(&article),
                            }),
                        ),
                        Err(err) => internal_error(id, err),
                    },
                    Ok(None) => {
                        JsonRpcResponse::error(id, -32004, "knowledge article not found", None)
                    }
                    Err(err) => internal_error(id, err),
                },
                Err(err) => invalid_params(id, err),
            }
        }
        RpcMethod::GetKnowledgeArticleFresh | RpcMethod::GetArticleFresh => {
            match extract_number(&request.params) {
                Ok(number) => match state.core.get_knowledge_article_fresh(&number).await {
                    Ok(Some(article)) => match transport.knowledge_article(&article) {
                        Ok(article_dto) => JsonRpcResponse::ok(
                            id,
                            json!({
                                "article": article_dto,
                                "markdown": render_knowledge_article(&article),
                            }),
                        ),
                        Err(err) => internal_error(id, err),
                    },
                    Ok(None) => {
                        JsonRpcResponse::error(id, -32004, "knowledge article not found", None)
                    }
                    Err(err) => internal_error(id, err),
                },
                Err(err) => invalid_params(id, err),
            }
        }
        RpcMethod::SearchKnowledge => match extract_knowledge_search_filters(&request.params) {
            Ok((query, filters)) => match state.core.search_knowledge(&query, filters).await {
                Ok(articles) => {
                    let mut article_dtos = Vec::with_capacity(articles.len());
                    for article in articles {
                        match transport.knowledge_article(&article) {
                            Ok(article) => article_dtos.push(article),
                            Err(err) => return internal_error(id, err),
                        }
                    }
                    JsonRpcResponse::ok(id, json!({ "articles": article_dtos }))
                }
                Err(err) => internal_error(id, err),
            },
            Err(err) => invalid_params(id, err),
        },
        RpcMethod::KbSemanticSearch => match extract_kb_semantic_search_filters(&request.params) {
            Ok((query, filters)) => {
                match state.core.search_knowledge_semantic(&query, filters).await {
                    Ok(hits) => {
                        let mut hit_dtos = Vec::with_capacity(hits.len());
                        for hit in hits {
                            match transport.knowledge_search_hit(&hit) {
                                Ok(hit) => hit_dtos.push(hit),
                                Err(err) => return internal_error(id, err),
                            }
                        }
                        JsonRpcResponse::ok(id, json!({ "hits": hit_dtos }))
                    }
                    Err(err) => internal_error(id, err),
                }
            }
            Err(err) => invalid_params(id, err),
        },
        RpcMethod::SearchRecords => match extract_search_records_params(&request.params) {
            Ok(params) => match state
                .core
                .search_enriched(&params.query, parse_search_scope(params.scope.as_deref()))
                .await
            {
                Ok(results) => {
                    let mut search_results = Vec::new();
                    for result in results.into_iter().take(params.limit.unwrap_or(20)) {
                        match transport.search_result(&result).await {
                            Ok(result) => search_results.push(result),
                            Err(err) => return internal_error(id, err),
                        }
                    }
                    JsonRpcResponse::ok(id, json!({ "results": search_results }))
                }
                Err(err) => internal_error(id, err),
            },
            Err(err) => invalid_params(id, err),
        },
        RpcMethod::UserLookup => match extract_user_lookup_params(&request.params) {
            Ok(params) => match state.core.lookup_user(params).await {
                Ok(Some(result)) => JsonRpcResponse::ok(id, json!(result)),
                Ok(None) => JsonRpcResponse::error(id, -32004, "user not found", None),
                Err(err) => internal_error(id, err),
            },
            Err(err) => invalid_params(id, err),
        },
        RpcMethod::UserSearch => match extract_user_search_params(&request.params) {
            Ok(params) => match state.core.search_users(params).await {
                Ok(users) => JsonRpcResponse::ok(id, json!({ "users": users })),
                Err(err) => internal_error(id, err),
            },
            Err(err) => invalid_params(id, err),
        },
        RpcMethod::BusinessApplicationGet => {
            match extract_business_application_lookup_params(&request.params) {
                Ok(lookup) => match get_business_application_cached(state.core.as_ref(), &lookup)
                    .await
                {
                    Ok(Some(record)) => match transport.business_application(&record) {
                        Ok(business_application) => {
                            let record_dto = business_application.record.clone();
                            JsonRpcResponse::ok(
                                id,
                                json!({
                                    "business_application": business_application,
                                    "record": record_dto,
                                    "markdown": render_snow_record(&record),
                                }),
                            )
                        }
                        Err(err) => internal_error(id, err),
                    },
                    Ok(None) => {
                        JsonRpcResponse::error(id, -32004, "business application not found", None)
                    }
                    Err(err) => internal_error(id, err),
                },
                Err(err) => invalid_params(id, err),
            }
        }
        RpcMethod::BusinessApplicationGetFresh => {
            match extract_business_application_lookup_params(&request.params) {
                Ok(lookup) => match extract_business_application_hydration_options(&request.params)
                {
                    Ok(options) => {
                        let core_lookup = match lookup {
                            BusinessApplicationLookup::SysId(sys_id) => {
                                match snow_core::BusinessApplicationLookup::sys_id(sys_id) {
                                    Ok(lookup) => lookup,
                                    Err(err) => return invalid_params(id, err),
                                }
                            }
                            BusinessApplicationLookup::Name(name) => {
                                snow_core::BusinessApplicationLookup::exact_name(name)
                            }
                        };
                        match state
                            .core
                            .get_business_application_fresh(core_lookup, options.clone().into())
                            .await
                        {
                            Ok(Some(application)) => {
                                match transport.business_application(&application.record) {
                                    Ok(mut business_application) => {
                                        business_application.unresolved_references =
                                            business_application_diagnostics(
                                                &application.unresolved_references,
                                            );
                                        let record_dto = business_application.record.clone();
                                        JsonRpcResponse::ok(
                                            id,
                                            json!({
                                                "business_application": business_application,
                                                "record": record_dto,
                                                "markdown": render_snow_record(&application.record),
                                                "hydration": options,
                                            }),
                                        )
                                    }
                                    Err(err) => internal_error(id, err),
                                }
                            }
                            Ok(None) => JsonRpcResponse::error(
                                id,
                                -32004,
                                "business application not found",
                                None,
                            ),
                            Err(err) => internal_error(id, err),
                        }
                    }
                    Err(err) => invalid_params(id, err),
                },
                Err(err) => invalid_params(id, err),
            }
        }
        RpcMethod::BusinessApplicationSearch => {
            match extract_business_application_search_params(&request.params) {
                Ok((params, options)) => {
                    match state
                        .core
                        .search_business_applications_live(params, options.clone().into())
                        .await
                    {
                        Ok(business_applications) => {
                            let mut applications = Vec::with_capacity(business_applications.len());
                            let mut record_dtos = Vec::new();
                            for application in business_applications {
                                match transport.business_application(&application.record) {
                                    Ok(mut application_dto) => {
                                        application_dto.unresolved_references =
                                            business_application_diagnostics(
                                                &application.unresolved_references,
                                            );
                                        record_dtos.push(application_dto.record.clone());
                                        applications.push(application_dto);
                                    }
                                    Err(err) => return internal_error(id, err),
                                }
                            }
                            JsonRpcResponse::ok(
                                id,
                                json!({
                                    "business_applications": applications,
                                    "records": record_dtos,
                                    "hydration": options,
                                }),
                            )
                        }
                        Err(err) => internal_error(id, err),
                    }
                }
                Err(err) => invalid_params(id, err),
            }
        }
        RpcMethod::BusinessApplicationQuery => {
            match extract_business_application_query_params(&request.params) {
                Ok(params) => match query_business_applications_local(state.core.as_ref(), &params)
                    .await
                {
                    Ok(records) => {
                        let mut applications = Vec::with_capacity(records.len());
                        for record in records {
                            match transport.business_application(&record) {
                                Ok(application) => applications.push(application),
                                Err(err) => return internal_error(id, err),
                            }
                        }
                        JsonRpcResponse::ok(id, json!({ "business_applications": applications }))
                    }
                    Err(err) => internal_error(id, err),
                },
                Err(err) => invalid_params(id, err),
            }
        }
        RpcMethod::ResourcePlanList => match extract_resource_plan_list_params(&request.params) {
            Ok(params) => match state.core.resource_plan_list(params).await {
                Ok(mut response) => {
                    for record in &mut response.records {
                        if let Err(err) = transport.resource_plan_record(record) {
                            return internal_error(id, err);
                        }
                    }
                    JsonRpcResponse::ok(id, json!(response))
                }
                Err(err) => internal_error(id, err),
            },
            Err(err) => invalid_params(id, err),
        },
        // Group-scoped Incident page. Arguments and successful results map 1:1
        // onto the core contract so direct MCP and daemon-backed MCP consumers
        // receive the same record/page shape. The daemon adds only the
        // structured invalid-parameter mapping, never filtering or transport
        // enrichment.
        // Authority: docs/spec-incident-list-by-assignment-group.md#scope.
        RpcMethod::IncidentListByAssignmentGroup => {
            match extract_incident_list_by_assignment_group_params(&request.params) {
                Ok(params) => match state.core.incident_list_by_assignment_group(params).await {
                    Ok(page) => JsonRpcResponse::ok(
                        id,
                        json!({
                            "records": page.records,
                            "next_cursor": page.next_cursor,
                            "complete": page.complete,
                            "limit": page.limit,
                            "rows_inspected": page.rows_inspected,
                            "state": page.state,
                        }),
                    ),
                    Err(err) => incident_group_list_error_response(id, err),
                },
                Err(err) => invalid_params(id, err),
            }
        }
        RpcMethod::IncidentAssignmentGroups => {
            match state.core.incident_assignment_groups().await {
                Ok(groups) => JsonRpcResponse::ok(id, json!({"groups": groups})),
                Err(err) => internal_error(id, err),
            }
        }
        RpcMethod::IncidentAssignmentGroupQueue => {
            match serde_json::from_value::<snow_core::IncidentAssignmentGroupQueueInput>(
                request.params.clone(),
            ) {
                Ok(params) => match state.core.incident_assignment_group_queue(params).await {
                    Ok(page) => JsonRpcResponse::ok(id, json!(page)),
                    Err(err)
                        if err
                            .downcast_ref::<snow_core::IncidentAssignmentGroupOperationsError>()
                            .is_some() =>
                    {
                        invalid_params(id, err)
                    }
                    Err(err) => internal_error(id, err),
                },
                Err(err) => invalid_params(id, err),
            }
        }
        RpcMethod::BusinessApplicationServers => {
            // Deserialize directly into the canonical snow_core request contract
            // (which owns `deny_unknown_fields` and selector/bounds validation),
            // then validate up-front so bad params surface as `invalid_params`.
            // Validation is run here -- rather than relying on the inner
            // re-validation inside `SnowCore::business_application_servers` -- so a
            // validation failure maps to `-32602 invalid_params` instead of being
            // misreported as an internal/service error.
            match parse_business_application_servers_params(&request.params) {
                Ok(params) => {
                    let mut traversal_params = params.traversal;
                    traversal_params.persist = Some(params.persist);
                    traversal_params.prune_stale = params.prune_stale;
                    match business_application_servers(
                        state.core.as_ref(),
                        &transport,
                        traversal_params,
                    )
                    .await
                    {
                        Ok(Some(result)) => JsonRpcResponse::ok(id, result),
                        Ok(None) => JsonRpcResponse::error(
                            id,
                            -32004,
                            "business application not found",
                            Some(json!({
                                "endpoint_status": "live_confirmation_not_attempted",
                                "relationship_status": "unknown_not_synced"
                            })),
                        ),
                        Err(err) => internal_error(id, err),
                    }
                }
                Err(err) => invalid_params(id, err),
            }
        }
        RpcMethod::BusinessApplicationServersCached => {
            match parse_business_application_servers_cached_params(&request.params) {
                Ok(params) => {
                    match business_application_servers_cached(
                        state.core.as_ref(),
                        &transport,
                        params,
                    )
                    .await
                    {
                        Ok(Some(result)) => JsonRpcResponse::ok(id, result),
                        Ok(None) => JsonRpcResponse::error(
                            id,
                            -32004,
                            "business application not found",
                            None,
                        ),
                        Err(err) => internal_error(id, err),
                    }
                }
                Err(err) => invalid_params(id, err),
            }
        }
        RpcMethod::BusinessApplicationsForServer => {
            match parse_business_applications_for_server_params(&request.params) {
                Ok(params) => {
                    match business_applications_for_server(state.core.as_ref(), &transport, params)
                        .await
                    {
                        Ok(Some(result)) => JsonRpcResponse::ok(id, result),
                        Ok(None) => JsonRpcResponse::error(
                            id,
                            -32004,
                            "server not found",
                            Some(json!({
                                "endpoint_status": "live_confirmation_not_attempted",
                                "relationship_status": "unknown_not_synced"
                            })),
                        ),
                        Err(err) => internal_error(id, err),
                    }
                }
                Err(err) => invalid_params(id, err),
            }
        }
        RpcMethod::BusinessApplicationSync => {
            match extract_business_application_sync_params(&request.params) {
                Ok(params) => {
                    if params.all {
                        return match state
                            .core
                            .sync_all_business_applications(params.options.into())
                            .await
                        {
                            Ok(summary) => JsonRpcResponse::ok(id, json!({ "summary": summary })),
                            Err(err) => internal_error(id, err),
                        };
                    }
                    match state
                        .core
                        .sync_business_applications(params.search_params, params.options.into())
                        .await
                    {
                        Ok(summary) => JsonRpcResponse::ok(id, json!({ "summary": summary })),
                        Err(err) => internal_error(id, err),
                    }
                }
                Err(err) => invalid_params(id, err),
            }
        }
        RpcMethod::BusinessApplicationFields => {
            match extract_business_application_fields_params(&request.params) {
                Ok(params) => {
                    match business_application_fields(state.core.as_ref(), params).await {
                        Ok(fields) => JsonRpcResponse::ok(id, json!({ "fields": fields })),
                        Err(err) => internal_error(id, err),
                    }
                }
                Err(err) => invalid_params(id, err),
            }
        }
        RpcMethod::ServerGet => match extract_server_get_params(&request.params) {
            Ok(params) => match get_server_cached(state.core.as_ref(), &params.lookup).await {
                // Cache hit: return the cached record without a live query.
                Ok(Some(record)) => match transport.server(&record) {
                    Ok(server) => {
                        let record_dto = server.record.clone();
                        JsonRpcResponse::ok(
                            id,
                            json!({
                                "server": server,
                                "record": record_dto,
                                "markdown": render_snow_record(&record),
                            }),
                        )
                    }
                    Err(err) => internal_error(id, err),
                },
                // Cache miss: fall through to the live exact fetch. On the
                // CLI/daemon path we persist the hit (read primitive contract);
                // a confirmed 404 is the only -32004, transient/ACL failures map
                // to distinct codes.
                Ok(None) => match core_server_lookup(params.lookup) {
                    Ok(core_lookup) => {
                        match state
                            .core
                            .get_server_live(core_lookup, params.persist)
                            .await
                        {
                            Ok(Some(server)) => match transport.server(&server.record) {
                                Ok(server_dto) => {
                                    let record_dto = server_dto.record.clone();
                                    JsonRpcResponse::ok(
                                        id,
                                        json!({
                                            "server": server_dto,
                                            "record": record_dto,
                                            "markdown": render_snow_record(&server.record),
                                        }),
                                    )
                                }
                                Err(err) => internal_error(id, err),
                            },
                            // get_server_live never returns Ok(None); NotFound is
                            // an Err variant. Treat the impossible case as 404.
                            Ok(None) => {
                                JsonRpcResponse::error(id, -32004, "server not found", None)
                            }
                            Err(err) => server_get_error_response(id, err),
                        }
                    }
                    Err(err) => invalid_params(id, err),
                },
                Err(err) => internal_error(id, err),
            },
            Err(err) => invalid_params(id, err),
        },
        RpcMethod::ServerGetFresh => match extract_server_lookup_params(&request.params) {
            Ok(lookup) => match core_server_lookup(lookup) {
                Ok(lookup) => match state.core.get_server_fresh(lookup).await {
                    Ok(Some(server)) => match transport.server(&server.record) {
                        Ok(server_dto) => {
                            let record_dto = server_dto.record.clone();
                            JsonRpcResponse::ok(
                                id,
                                json!({
                                    "server": server_dto,
                                    "record": record_dto,
                                    "markdown": render_snow_record(&server.record),
                                }),
                            )
                        }
                        Err(err) => internal_error(id, err),
                    },
                    Ok(None) => JsonRpcResponse::error(id, -32004, "server not found", None),
                    Err(err) => internal_error(id, err),
                },
                Err(err) => invalid_params(id, err),
            },
            Err(err) => invalid_params(id, err),
        },
        RpcMethod::ServerSearch => match extract_server_search_params(&request.params) {
            Ok(params) => match state.core.search_servers_live(params).await {
                Ok(servers) => {
                    let mut server_dtos = Vec::with_capacity(servers.len());
                    let mut record_dtos = Vec::with_capacity(servers.len());
                    for server in servers {
                        match transport.server(&server.record) {
                            Ok(server_dto) => {
                                record_dtos.push(server_dto.record.clone());
                                server_dtos.push(server_dto);
                            }
                            Err(err) => return internal_error(id, err),
                        }
                    }
                    JsonRpcResponse::ok(
                        id,
                        json!({
                            "servers": server_dtos,
                            "records": record_dtos,
                        }),
                    )
                }
                Err(err) => internal_error(id, err),
            },
            Err(err) => invalid_params(id, err),
        },
        RpcMethod::ServerQuery => match extract_server_query_params(&request.params) {
            Ok(params) => match state.core.query_servers(params).await {
                Ok(records) => {
                    let mut servers = Vec::with_capacity(records.len());
                    for record in records {
                        match transport.server(&record) {
                            Ok(server) => servers.push(server),
                            Err(err) => return internal_error(id, err),
                        }
                    }
                    JsonRpcResponse::ok(id, json!({ "servers": servers }))
                }
                Err(err) => internal_error(id, err),
            },
            Err(err) => invalid_params(id, err),
        },
        RpcMethod::ServerFields => match extract_server_fields_params(&request.params) {
            Ok(params) => match server_fields(state.core.as_ref(), params).await {
                Ok(fields) => JsonRpcResponse::ok(id, json!({ "fields": fields })),
                Err(err) => internal_error(id, err),
            },
            Err(err) => invalid_params(id, err),
        },
        RpcMethod::ListKnowledgeBases => match state.core.list_knowledge_bases() {
            Ok(bases) => JsonRpcResponse::ok(
                id,
                json!({
                    "bases": bases
                        .into_iter()
                        .map(|base| transport.knowledge_base_summary(base))
                        .collect::<Vec<_>>()
                }),
            ),
            Err(err) => internal_error(id, err),
        },
        RpcMethod::ListCategories => match extract_string(&request.params, "knowledge_base_sys_id")
        {
            Ok(knowledge_base_sys_id) => match state.core.list_categories(&knowledge_base_sys_id) {
                Ok(categories) => JsonRpcResponse::ok(
                    id,
                    json!({
                        "categories": categories
                            .into_iter()
                            .map(|category| transport.knowledge_category_summary(category))
                            .collect::<Vec<_>>()
                    }),
                ),
                Err(err) => internal_error(id, err),
            },
            Err(err) => invalid_params(id, err),
        },
        RpcMethod::ListKnowledgeArticles => {
            match extract_list_knowledge_articles_params(&request.params) {
                Ok(params) => match state
                    .core
                    .list_knowledge_articles(
                        params.knowledge_base_sys_id.as_deref(),
                        params.category_sys_id.as_deref(),
                        params.limit,
                    )
                    .await
                {
                    Ok(articles) => {
                        let mut article_dtos = Vec::with_capacity(articles.len());
                        for article in articles {
                            match transport.knowledge_article(&article) {
                                Ok(article) => article_dtos.push(article),
                                Err(err) => return internal_error(id, err),
                            }
                        }
                        JsonRpcResponse::ok(id, json!({ "articles": article_dtos }))
                    }
                    Err(err) => internal_error(id, err),
                },
                Err(err) => invalid_params(id, err),
            }
        }
        RpcMethod::GetApproval => match extract_number(&request.params) {
            Ok(number) => match state.core.get_approval(&number).await {
                Ok(Some(approval)) => match transport.approval(&approval) {
                    Ok(approval_dto) => JsonRpcResponse::ok(
                        id,
                        json!({
                            "approval": approval_dto,
                            "markdown": render_approval_record(&approval),
                        }),
                    ),
                    Err(err) => internal_error(id, err),
                },
                Ok(None) => JsonRpcResponse::error(id, -32004, "approval not found", None),
                Err(err) => internal_error(id, err),
            },
            Err(err) => invalid_params(id, err),
        },
        RpcMethod::CatalogItemsSearch => {
            crate::catalog_write::handle_catalog_items_search(id, &request.params, state).await
        }
        RpcMethod::CatalogItemGet => {
            crate::catalog_write::handle_catalog_item_get(id, &request.params, state).await
        }
        RpcMethod::GetChildren => match extract_number(&request.params) {
            Ok(number) => match state.core.get_children(&number).await {
                Ok(records) => {
                    let mut record_dtos = Vec::with_capacity(records.len());
                    for record in records {
                        match transport.record(&record) {
                            Ok(record) => record_dtos.push(record),
                            Err(err) => return internal_error(id, err),
                        }
                    }
                    JsonRpcResponse::ok(id, json!({ "records": record_dtos, "parent": number }))
                }
                Err(err) => internal_error(id, err),
            },
            Err(err) => invalid_params(id, err),
        },
        RpcMethod::GetWorkNotes => match extract_record_lookup(&request.params) {
            Ok(lookup) => {
                match get_record_by_lookup_cached_or_fresh(state.core.as_ref(), lookup).await {
                    Ok(Some(record)) => match transport.record(&record) {
                        Ok(record) => {
                            JsonRpcResponse::ok(id, json!({ "work_notes": record.work_notes }))
                        }
                        Err(err) => internal_error(id, err),
                    },
                    Ok(None) => JsonRpcResponse::error(id, -32004, "record not found", None),
                    Err(err) => map_record_lookup_error(id, err),
                }
            }
            Err(err) => invalid_params(id, err),
        },
        RpcMethod::ListRecords => match extract_list_records_params(&request.params) {
            Ok(params) => {
                let mut query = ListQuery::new();
                if let Some(resource_type) = params.resource_type.as_deref() {
                    match parse_resource_type(resource_type) {
                        Ok(resource_type) => query = query.resource_type(resource_type),
                        Err(err) => return invalid_params(id, err),
                    }
                }
                if let Some(assigned_to) = params.assigned_to {
                    query = query.assigned_to(assigned_to);
                }
                if let Some(limit) = params.limit {
                    query = query.limit(limit);
                }
                if let Some(parent_number) = params.parent_number {
                    match state.core.get_record(&parent_number).await {
                        Ok(Some(parent)) => {
                            query = query.parent_sys_id(parent.sys_id);
                        }
                        Ok(None) => {
                            return JsonRpcResponse::error(
                                id,
                                -32004,
                                "parent record not found",
                                None,
                            );
                        }
                        Err(err) => return internal_error(id, err),
                    }
                }

                match state.core.list_records_query(query).await {
                    Ok(records) => {
                        let mut record_dtos = Vec::with_capacity(records.len());
                        for record in records {
                            match transport.record(&record) {
                                Ok(record) => record_dtos.push(record),
                                Err(err) => return internal_error(id, err),
                            }
                        }
                        JsonRpcResponse::ok(id, json!({ "records": record_dtos }))
                    }
                    Err(err) => internal_error(id, err),
                }
            }
            Err(err) => invalid_params(id, err),
        },
        RpcMethod::MyTasks => match state.core.my_tasks().await {
            Ok(records) => wrap_records_response(id, &transport, records),
            Err(err) => internal_error(id, err),
        },
        RpcMethod::MyTasksFresh => match state.core.my_tasks_fresh().await {
            Ok(records) => wrap_records_response(id, &transport, records),
            Err(err) => internal_error(id, err),
        },
        RpcMethod::ListMyTasks => match state.core.my_tasks().await {
            Ok(records) if !records.is_empty() => wrap_records_response(id, &transport, records),
            Ok(_) | Err(_) => match state.core.my_tasks_fresh().await {
                Ok(records) => wrap_records_response(id, &transport, records),
                Err(err) => internal_error(id, err),
            },
        },
        RpcMethod::MyApprovals => match state.core.my_approvals_with_routing_fresh().await {
            Ok(response) => wrap_list_my_approvals_response(id, &transport, response),
            Err(err) => internal_error(id, err),
        },
        RpcMethod::MyApprovalsFresh => match state.core.my_approvals_with_routing_fresh().await {
            Ok(response) => wrap_list_my_approvals_response(id, &transport, response),
            Err(err) => internal_error(id, err),
        },
        RpcMethod::ListMyApprovals => match state.core.my_approvals_with_routing_fresh().await {
            Ok(response) => wrap_list_my_approvals_response(id, &transport, response),
            Err(err) => internal_error(id, err),
        },
        RpcMethod::MyProjects => match state.core.my_projects().await {
            Ok(records) => wrap_records_response(id, &transport, records),
            Err(err) => internal_error(id, err),
        },
        RpcMethod::MyProjectsFresh => match state.core.my_projects_fresh().await {
            Ok(records) => wrap_records_response(id, &transport, records),
            Err(err) => internal_error(id, err),
        },
        RpcMethod::ListMyProjects => match state.core.my_projects().await {
            Ok(records) if !records.is_empty() => wrap_records_response(id, &transport, records),
            Ok(_) | Err(_) => match state.core.my_projects_fresh().await {
                Ok(records) => wrap_records_response(id, &transport, records),
                Err(err) => internal_error(id, err),
            },
        },
        RpcMethod::MyStoriesFresh | RpcMethod::ListMyStories => {
            match state.core.my_stories_fresh().await {
                Ok(records) => wrap_records_response(id, &transport, records),
                Err(err) => internal_error(id, err),
            }
        }
        RpcMethod::MyIncidentsFresh | RpcMethod::ListMyIncidents => {
            match state.core.my_incidents_fresh().await {
                Ok(records) => wrap_records_response(id, &transport, records),
                Err(err) => internal_error(id, err),
            }
        }
        RpcMethod::AddWorkNote => match (
            extract_number(&request.params),
            extract_string(&request.params, "text"),
        ) {
            (Ok(number), Ok(text)) => match state.core.add_work_note(&number, &text).await {
                Ok(Some(record)) => match transport.record(&record) {
                    Ok(record) => JsonRpcResponse::ok(id, json!({ "record": record })),
                    Err(err) => internal_error(id, err),
                },
                Ok(None) => JsonRpcResponse::error(id, -32004, "record not found", None),
                Err(err) => internal_error(id, err),
            },
            (Err(err), _) | (_, Err(err)) => invalid_params(id, err),
        },
        RpcMethod::AttachmentList => match extract_number(&request.params) {
            Ok(number) => match state.core.list_attachments(&number).await {
                Ok(Some(attachments)) => {
                    JsonRpcResponse::ok(id, json!({ "attachments": attachments }))
                }
                Ok(None) => JsonRpcResponse::error(id, -32004, "record not found", None),
                Err(err) => internal_error(id, err),
            },
            Err(err) => invalid_params(id, err),
        },
        RpcMethod::AttachmentUpload => {
            match serde_json::from_value::<AttachmentUploadParams>(request.params.clone()) {
                Ok(params) => match state
                    .core
                    .upload_attachment_file(
                        &params.number,
                        &params.path,
                        params.file_name.as_deref(),
                        params.content_type.as_deref(),
                    )
                    .await
                {
                    Ok(Some(attachment)) => {
                        JsonRpcResponse::ok(id, json!({ "attachment": attachment }))
                    }
                    Ok(None) => JsonRpcResponse::error(id, -32004, "record not found", None),
                    Err(err) => internal_error(id, err),
                },
                Err(err) => invalid_params(id, err),
            }
        }
        RpcMethod::SetState => {
            match serde_json::from_value::<SetStateParams>(request.params.clone()) {
                Ok(params) => match state.core.set_state(&params.number, &params.state).await {
                    Ok(Some(record)) => match transport.record(&record) {
                        Ok(record) => JsonRpcResponse::ok(id, json!({ "record": record })),
                        Err(err) => internal_error(id, err),
                    },
                    Ok(None) => JsonRpcResponse::error(id, -32004, "record not found", None),
                    Err(err) => internal_error(id, err),
                },
                Err(err) => invalid_params(id, err),
            }
        }
        RpcMethod::FieldChoices => {
            match serde_json::from_value::<FieldChoicesParams>(request.params.clone()) {
                Ok(params) => match state.core.field_choices(&params.table, &params.field).await {
                    Ok(choices) => JsonRpcResponse::ok(id, json!({ "choices": choices })),
                    Err(err) => internal_error(id, err),
                },
                Err(err) => invalid_params(id, err),
            }
        }
        RpcMethod::Approve => handle_approval_approve(id, &request.params, state, &transport).await,
        RpcMethod::ApprovalApprove => {
            handle_approval_approve(id, &request.params, state, &transport).await
        }
        RpcMethod::Reject => handle_approval_reject(id, &request.params, state, &transport).await,
        RpcMethod::ApprovalReject => {
            handle_approval_reject(id, &request.params, state, &transport).await
        }
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
        RpcMethod::KbSync => match extract_kb_sync_params(&request.params) {
            Ok(params) => match state
                .core
                .sync_knowledge(params.full, params.with_bodies)
                .await
            {
                Ok(sync) => JsonRpcResponse::ok(
                    id,
                    json!({ "sync": DaemonKnowledgeSyncOutcome::from(sync) }),
                ),
                Err(err) => internal_error(id, err),
            },
            Err(err) => invalid_params(id, err),
        },
        RpcMethod::KbListTags => match extract_kb_list_tags_params(&request.params) {
            Ok(params) => {
                let layer = match params.layer.as_deref() {
                    Some("all") | None => None,
                    Some(layer) => match core_kb_tag_layer(layer) {
                        Ok(layer) => Some(layer),
                        Err(err) => return invalid_params(id, err),
                    },
                };
                match state.core.list_knowledge_tags(layer, params.min_count) {
                    Ok(tags) => JsonRpcResponse::ok(
                        id,
                        json!({
                            "tags": tags
                                .into_iter()
                                .map(DaemonKnowledgeTagSummary::from)
                                .collect::<Vec<_>>()
                        }),
                    ),
                    Err(err) => internal_error(id, err),
                }
            }
            Err(err) => invalid_params(id, err),
        },
        RpcMethod::KbStatus => match state.core.knowledge_status() {
            Ok(status) => {
                JsonRpcResponse::ok(id, json!({ "status": DaemonKnowledgeStatus::from(status) }))
            }
            Err(err) => internal_error(id, err),
        },
        RpcMethod::KbSemanticStatus => match state.core.knowledge_semantic_status().await {
            Ok(status) => JsonRpcResponse::ok(
                id,
                json!({ "status": DaemonKnowledgeSemanticStatus::from(status) }),
            ),
            Err(err) => internal_error(id, err),
        },
        RpcMethod::KbSemanticRebuild => {
            let full = request
                .params
                .get("full")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            match state.core.rebuild_knowledge_semantic_index(full).await {
                Ok(summary) => JsonRpcResponse::ok(
                    id,
                    json!({ "summary": DaemonSemanticIndexSummary::from(summary) }),
                ),
                Err(err) => internal_error(id, err),
            }
        }
        RpcMethod::RefreshAll => JsonRpcResponse::ok(id, json!({ "status": "queued" })),
        RpcMethod::SchedulerStatus => JsonRpcResponse::ok(id, json!({ "status": "available" })),
        RpcMethod::SchedulerTriggerNow => JsonRpcResponse::ok(id, json!({ "status": "queued" })),
        RpcMethod::StartJob => {
            match serde_json::from_value::<StartJobParams>(request.params.clone()) {
                Ok(p) => {
                    let runner = crate::jobs::AppRunner {
                        state: state.clone(),
                    };
                    let job_id =
                        crate::jobs::spawn(state.jobs.clone(), p.kind, p.params, runner).await;
                    JsonRpcResponse::ok(id, json!({ "job_id": job_id }))
                }
                Err(err) => invalid_params(id, err),
            }
        }
        RpcMethod::GetJob => match serde_json::from_value::<JobIdParams>(request.params.clone()) {
            Ok(p) => match state.jobs.get(p.job_id).await {
                Some(job) => match serde_json::to_value(job) {
                    Ok(value) => JsonRpcResponse::ok(id, value),
                    Err(err) => internal_error(id, err),
                },
                None => JsonRpcResponse::ok(id, Value::Null),
            },
            Err(err) => invalid_params(id, err),
        },
        RpcMethod::ListJobs => {
            let p: ListJobsParams =
                serde_json::from_value(request.params.clone()).unwrap_or_default();
            let jobs = state
                .jobs
                .list(crate::jobs::ListJobsFilter {
                    include_finished: p.include_finished,
                    limit: p.limit,
                })
                .await;
            match serde_json::to_value(jobs) {
                Ok(value) => JsonRpcResponse::ok(id, value),
                Err(err) => internal_error(id, err),
            }
        }
        RpcMethod::CancelJob => match serde_json::from_value::<JobIdParams>(request.params.clone())
        {
            Ok(p) => {
                let cancelled = state.jobs.cancel(p.job_id).await;
                JsonRpcResponse::ok(id, json!({ "cancelled": cancelled }))
            }
            Err(err) => invalid_params(id, err),
        },
        RpcMethod::PlanGet => crate::story_write::handle_plan_get(id, &request.params, state).await,
        RpcMethod::CatalogPlanRequest => {
            crate::catalog_write::handle_catalog_plan_request(id, &request.params, state).await
        }
        RpcMethod::CatalogSubmitRequest => {
            crate::catalog_write::handle_catalog_submit_request(id, &request.params, state).await
        }
        RpcMethod::WorkNotePlanAdd => {
            crate::work_note_write::handle_work_note_plan_add(id, &request.params, state).await
        }
        RpcMethod::WorkNoteApplyAdd => {
            crate::work_note_write::handle_work_note_apply_add(id, &request.params, state).await
        }
        RpcMethod::ChangeRequestPlanCreate
        | RpcMethod::ChangeRequestPlanUpdate
        | RpcMethod::ChangeTaskPlanCreate
        | RpcMethod::ChangeTaskPlanUpdate
        | RpcMethod::IncidentPlanUpdate => {
            crate::change_write::handle_change_plan(id, &request.method, &request.params, state)
                .await
        }
        RpcMethod::ChangeRequestApplyCreate
        | RpcMethod::ChangeRequestApplyUpdate
        | RpcMethod::ChangeTaskApplyCreate
        | RpcMethod::ChangeTaskApplyUpdate
        | RpcMethod::IncidentApplyUpdate => {
            crate::change_write::handle_change_apply(id, &request.method, &request.params, state)
                .await
        }
        RpcMethod::ResourcePlanPlanCreate | RpcMethod::ResourcePlanPlanUpdate => {
            crate::resource_plan_write::handle_resource_plan_plan(
                id,
                &request.method,
                &request.params,
                state,
            )
            .await
        }
        RpcMethod::ResourcePlanApplyCreate | RpcMethod::ResourcePlanApplyUpdate => {
            crate::resource_plan_write::handle_resource_plan_apply(
                id,
                &request.method,
                &request.params,
                state,
            )
            .await
        }
        RpcMethod::StoryPlanCreate
        | RpcMethod::StoryPlanUpdate
        | RpcMethod::StoryTaskPlanCreate
        | RpcMethod::StoryTaskPlanUpdate => {
            crate::story_write::handle_story_plan(id, &request.method, &request.params, state).await
        }
        RpcMethod::StoryApplyCreate
        | RpcMethod::StoryApplyUpdate
        | RpcMethod::StoryTaskApplyCreate
        | RpcMethod::StoryTaskApplyUpdate => {
            crate::story_write::handle_story_apply(id, &request.method, &request.params, state)
                .await
        }
        RpcMethod::TimecardList => {
            crate::timecard_write::handle_timecard_list(id, &request.params, state).await
        }
        RpcMethod::TimecardSetHours => {
            crate::timecard_write::handle_timecard_set_hours(id, &request.params, state).await
        }
        RpcMethod::TimecardPlanSetHours => {
            crate::timecard_write::handle_timecard_plan_set_hours(id, &request.params, state).await
        }
        RpcMethod::TimecardApplySetHours => {
            crate::timecard_write::handle_timecard_apply_set_hours(id, &request.params, state).await
        }
        RpcMethod::Unknown => JsonRpcResponse::error(id, -32601, "method not found", None),
    }
}

/// Parameters for the `start_job` JSON-RPC method.
#[derive(Debug, Deserialize)]
struct StartJobParams {
    /// Discriminator selecting which worker kind to dispatch.
    kind: crate::jobs::JobKind,
    /// Free-form per-kind parameters forwarded to the worker.
    #[serde(default)]
    params: Value,
}

/// Parameters for `get_job` and `cancel_job` (a bare job id).
#[derive(Debug, Deserialize)]
struct JobIdParams {
    /// The id of the job to inspect or cancel.
    job_id: uuid::Uuid,
}

/// Parameters for the `list_jobs` JSON-RPC method.
#[derive(Debug, Deserialize, Default)]
struct ListJobsParams {
    /// If true, finished (terminal) jobs are also returned.
    #[serde(default)]
    include_finished: bool,
    /// Optional cap on the number of jobs returned.
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct SetStateParams {
    number: String,
    state: String,
}

#[derive(Debug, Deserialize)]
struct FieldChoicesParams {
    table: String,
    field: String,
}

#[derive(Debug, Deserialize)]
struct AttachmentUploadParams {
    number: String,
    path: PathBuf,
    #[serde(default)]
    file_name: Option<String>,
    #[serde(default)]
    content_type: Option<String>,
}

#[derive(Debug, Serialize)]
struct CacheInfo {
    vault_path: String,
    sqlite_path: String,
    schema_version: i64,
    db_size_mb: u64,
    total_rows: u64,
}

const CONTRACT_VERSION: &str = "daemon-json-rpc-v1";

const SUPPORTED_RPC_METHODS: &[&str] = &[
    "contract_info",
    "ping",
    "get_record",
    "get_record_fresh",
    "get_article",
    "get_article_fresh",
    "task_sla_status",
    "task_sla_status_for_tasks",
    "search_records",
    "user_lookup",
    "user_search",
    "business_application_get",
    "business_application_get_fresh",
    "business_application_search",
    "business_application_query",
    "business_application_servers",
    "business_application_servers_cached",
    "business_applications_for_server",
    "business_application_sync",
    "business_application_fields",
    "resource_plan_list",
    "incident_list_by_assignment_group",
    "incident_assignment_groups",
    "incident_assignment_group_queue",
    "server_get",
    "server_get_fresh",
    "server_search",
    "server_query",
    "server_fields",
    "search_knowledge",
    "kb_semantic_search",
    "list_knowledge_bases",
    "list_categories",
    "list_knowledge_articles",
    "get_approval",
    "catalog_items_search",
    "catalog_item_get",
    "catalog_plan_request",
    "catalog_submit_request",
    "get_children",
    "get_work_notes",
    "list_records",
    "list_my_tasks",
    "list_my_approvals",
    "list_my_projects",
    "list_my_stories",
    "list_my_incidents",
    "vault_path",
    "add_work_note",
    "attachment_list",
    "attachment_upload",
    "set_state",
    "field_choices",
    "approval_approve",
    "approval_reject",
    "get_degraded_reads",
    "cache_info",
    "repair_vault",
    "rebuild_cache",
    "verify_vault",
    "prune_orphans",
    "refresh_all",
    "kb_sync",
    "kb_list_tags",
    "kb_status",
    "kb_semantic_status",
    "kb_semantic_rebuild",
    "scheduler.status",
    "scheduler.trigger_now",
    "start_job",
    "get_job",
    "list_jobs",
    "cancel_job",
    "plan_get",
    "work_note_plan_add",
    "work_note_apply_add",
    "change_request_plan_create",
    "change_request_apply_create",
    "change_request_plan_update",
    "change_request_apply_update",
    "change_task_plan_create",
    "change_task_apply_create",
    "change_task_plan_update",
    "change_task_apply_update",
    "incident_plan_update",
    "incident_apply_update",
    "resource_plan_plan_create",
    "resource_plan_apply_create",
    "resource_plan_plan_update",
    "resource_plan_apply_update",
    "story_plan_create",
    "story_apply_create",
    "story_plan_update",
    "story_apply_update",
    "story_task_plan_create",
    "story_task_apply_create",
    "story_task_plan_update",
    "story_task_apply_update",
    "timecard_list",
    "timecard_set_hours",
    "timecard_plan_set_hours",
    "timecard_apply_set_hours",
    "shutdown",
];

const DEPRECATED_RPC_ALIASES: &[(&str, &str)] = &[
    ("get_knowledge_article", "get_article"),
    ("get_knowledge_article_fresh", "get_article_fresh"),
    ("my_tasks", "list_my_tasks"),
    ("my_tasks_fresh", "list_my_tasks"),
    ("my_approvals", "list_my_approvals"),
    ("my_approvals_fresh", "list_my_approvals"),
    ("my_projects", "list_my_projects"),
    ("my_projects_fresh", "list_my_projects"),
    ("my_stories_fresh", "list_my_stories"),
    ("my_incidents_fresh", "list_my_incidents"),
    ("approve", "approval_approve"),
    ("reject", "approval_reject"),
];

fn contract_info(state: &DaemonState) -> Value {
    let env_label =
        std::env::var("SNOW_ENV").unwrap_or_else(|_| crate::DEFAULT_DAEMON_ENV.to_string());
    let instance_host = normalize_instance_host(&state.core.config().instance.url);
    let (mcp_mode, mcp_transport) =
        normalize_mcp_availability(state.core.config().daemon.mcp_transport.as_str());
    let deprecated_aliases = DEPRECATED_RPC_ALIASES
        .iter()
        .map(|(method, replacement)| {
            json!({
                "method": method,
                "replacement": replacement,
            })
        })
        .collect::<Vec<_>>();

    json!({
        "contract_version": CONTRACT_VERSION,
        "daemon_version": env!("CARGO_PKG_VERSION"),
        "instance_host": instance_host,
        "supported_methods": SUPPORTED_RPC_METHODS,
        "deprecated_aliases": deprecated_aliases,
        "environment": {
            "label": env_label,
            "instance_host": instance_host,
            "username": state.core.config().instance.user,
        },
        "warming_model": "passive",
        "mcp_availability": {
            "mode": mcp_mode,
            "transport": mcp_transport,
        },
    })
}

fn normalize_instance_host(instance_url: &str) -> Option<String> {
    let trimmed = instance_url.trim();
    if trimmed.is_empty() {
        return None;
    }

    let without_scheme = trimmed
        .split_once("://")
        .map(|(_, rest)| rest)
        .unwrap_or(trimmed);
    let authority = without_scheme
        .split(['/', '?', '#'])
        .next()
        .unwrap_or("")
        .trim_end_matches('/');
    let host_port = authority
        .rsplit_once('@')
        .map(|(_, host)| host)
        .unwrap_or(authority);

    let host = if let Some(rest) = host_port.strip_prefix('[') {
        rest.split(']').next().unwrap_or("")
    } else {
        host_port.split(':').next().unwrap_or("")
    };

    if host.is_empty() {
        None
    } else {
        Some(host.to_string())
    }
}

fn normalize_mcp_availability(configured_transport: &str) -> (&'static str, &'static str) {
    match configured_transport {
        "stdio" => ("local_stdio", "stdio"),
        "disabled" | "" => ("disabled", "disabled"),
        "http" => ("future_remote_transport", "http"),
        "sse" => ("future_remote_transport", "sse"),
        _ => ("unknown", "unknown"),
    }
}

fn cache_info(core: &SnowCore) -> Result<CacheInfo> {
    let vault_path = core.vault_path().to_path_buf();
    let sqlite_path = vault_path
        .parent()
        .map(|parent| parent.join("snow.db"))
        .unwrap_or_else(|| PathBuf::from("snow.db"));
    let _store = Store::open(&sqlite_path)?;
    // Keep the established wire field stable while the cache itself is now
    // identified by an exact format marker rather than an upgrade sequence.
    let schema_version = 11;
    let total_rows = count_cache_records(&sqlite_path)?;
    let db_size_mb = std::fs::metadata(&sqlite_path)
        .map(|meta| meta.len() / (1024 * 1024))
        .unwrap_or(0);

    Ok(CacheInfo {
        vault_path: vault_path.display().to_string(),
        sqlite_path: sqlite_path.display().to_string(),
        schema_version,
        db_size_mb,
        total_rows,
    })
}

fn count_cache_records(sqlite_path: &std::path::Path) -> Result<u64> {
    let conn = rusqlite::Connection::open(sqlite_path)?;
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM records", [], |row| row.get(0))?;
    Ok(count.max(0) as u64)
}

async fn handle_approval_approve(
    id: Option<Value>,
    params: &Value,
    state: &DaemonState,
    transport: &DaemonTransport<'_>,
) -> JsonRpcResponse {
    if !approval_tool_enabled(state, "approval_approve") {
        return approval_policy_denied(id, "approval_approve");
    }

    let target = match extract_approval_action_target(params) {
        Ok(target) => target,
        Err(err) => return invalid_params(id, err),
    };
    let result = match target {
        ApprovalActionTarget::Number(number) => state.core.approve(&number, None).await,
        ApprovalActionTarget::ApprovalSysId(approval_sys_id) => {
            state.core.approve_approval(&approval_sys_id, None).await
        }
    };
    match result {
        Ok(Some(record)) => daemon_record_response(id, transport, &record),
        Ok(None) => JsonRpcResponse::error(id, -32004, "record not found", None),
        Err(err) => internal_error(id, err),
    }
}

async fn handle_approval_reject(
    id: Option<Value>,
    params: &Value,
    state: &DaemonState,
    transport: &DaemonTransport<'_>,
) -> JsonRpcResponse {
    if !approval_tool_enabled(state, "approval_reject") {
        return approval_policy_denied(id, "approval_reject");
    }

    let target = match extract_approval_action_target(params) {
        Ok(target) => target,
        Err(err) => return invalid_params(id, err),
    };
    let reason = match extract_string(params, "reason") {
        Ok(reason) => reason,
        Err(err) => return invalid_params(id, err),
    };
    let result = match target {
        ApprovalActionTarget::Number(number) => state.core.reject(&number, &reason).await,
        ApprovalActionTarget::ApprovalSysId(approval_sys_id) => {
            state.core.reject_approval(&approval_sys_id, &reason).await
        }
    };
    match result {
        Ok(Some(record)) => daemon_record_response(id, transport, &record),
        Ok(None) => JsonRpcResponse::error(id, -32004, "record not found", None),
        Err(err) => internal_error(id, err),
    }
}

fn approval_tool_enabled(state: &DaemonState, tool: &str) -> bool {
    state
        .mcp_config
        .policy
        .tool_enabled_in_environment(tool, &state.mcp_config.environment.label)
}

fn approval_policy_denied(id: Option<Value>, tool: &str) -> JsonRpcResponse {
    JsonRpcResponse::error(
        id,
        -32040,
        "policy denied",
        Some(json!({
            "details": "approval action tool is disabled by current MCP policy",
            "tool": tool,
        })),
    )
}

fn daemon_record_response(
    id: Option<Value>,
    transport: &DaemonTransport<'_>,
    record: &SnowRecord,
) -> JsonRpcResponse {
    match transport.record(record) {
        Ok(record) => JsonRpcResponse::ok(id, json!({ "record": record })),
        Err(err) => internal_error(id, err),
    }
}

async fn daemon_record_response_with_private_task_context(
    id: Option<Value>,
    transport: &DaemonTransport<'_>,
    record: &SnowRecord,
) -> JsonRpcResponse {
    match transport.record_with_private_task_context(record).await {
        Ok(record) => JsonRpcResponse::ok(id, json!({ "record": record })),
        Err(err) => internal_error(id, err),
    }
}

fn extract_number(params: &Value) -> Result<String> {
    match params {
        Value::Object(map) => map
            .get("number")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .ok_or_else(|| anyhow!("missing required field `number`")),
        _ => Err(anyhow!("expected object params")),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ApprovalActionTarget {
    Number(String),
    ApprovalSysId(String),
}

fn extract_approval_action_target(params: &Value) -> Result<ApprovalActionTarget> {
    let Value::Object(map) = params else {
        return Err(anyhow!("expected object params"));
    };
    let number = map.get("number").and_then(Value::as_str);
    let approval_sys_id = map.get("approval_sys_id").and_then(Value::as_str);

    match (number, approval_sys_id) {
        (Some(number), None) => Ok(ApprovalActionTarget::Number(number.to_owned())),
        (None, Some(approval_sys_id)) => Ok(ApprovalActionTarget::ApprovalSysId(
            snow_core::normalize_record_lookup_sys_id(approval_sys_id)?,
        )),
        (Some(_), Some(_)) => Err(anyhow!(
            "provide either `number` or `approval_sys_id`, not both"
        )),
        (None, None) => Err(anyhow!(
            "missing required lookup: provide either `number` or `approval_sys_id`"
        )),
    }
}

pub(crate) fn extract_record_lookup(params: &Value) -> Result<RecordLookup> {
    let Value::Object(map) = params else {
        return Err(anyhow!("expected object params"));
    };

    let number = map.get("number").and_then(Value::as_str);
    let table = map.get("table").and_then(Value::as_str);
    let sys_id = map.get("sys_id").and_then(Value::as_str);

    match (number, table, sys_id) {
        (Some(number), None, None) => Ok(RecordLookup::Number(number.to_owned())),
        (None, Some(table), Some(sys_id)) => Ok(RecordLookup::TableSysId {
            table: snow_core::normalize_record_lookup_table(table)?,
            sys_id: snow_core::normalize_record_lookup_sys_id(sys_id)?,
        }),
        (Some(_), Some(_), _) | (Some(_), _, Some(_)) => Err(anyhow!(
            "provide either `number` or `table` + `sys_id`, not both"
        )),
        (None, None, Some(_)) => Err(anyhow!(
            "missing required lookup: provide either `number` or `table` + `sys_id`"
        )),
        (None, Some(_), None) => Err(anyhow!("missing required field `sys_id`")),
        (None, None, None) => Err(anyhow!(
            "missing required lookup: provide either `number` or `table` + `sys_id`"
        )),
    }
}

fn extract_string(params: &Value, field: &str) -> Result<String> {
    match params {
        Value::Object(map) => map
            .get(field)
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .ok_or_else(|| anyhow!("missing required field `{field}`")),
        _ => Err(anyhow!("expected object params")),
    }
}

fn extract_task_sla_parent_refs(params: &Value) -> Result<Vec<TaskSlaParentRef>> {
    let params: TaskSlaStatusForTasksParams = serde_json::from_value(params.clone())?;
    Ok(params.parents)
}

fn extract_knowledge_search_filters(
    params: &Value,
) -> Result<(String, snow_core::KnowledgeSearchFilters)> {
    let Value::Object(map) = params else {
        return Err(anyhow!("expected object params"));
    };

    let query = map
        .get("query")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| anyhow!("missing required field `query`"))?;
    let limit = map
        .get("limit")
        .and_then(Value::as_u64)
        .map(|value| value as usize);

    Ok((
        query,
        snow_core::KnowledgeSearchFilters {
            knowledge_base: map
                .get("knowledge_base")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            category: map
                .get("category")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            limit,
        },
    ))
}

pub(crate) fn extract_kb_semantic_search_filters(
    params: &Value,
) -> Result<(String, KnowledgeSemanticSearchFilters)> {
    let filters: KbSemanticSearchParams = serde_json::from_value(params.clone())?;
    if filters.query.trim().is_empty() {
        return Err(anyhow!("missing required field `query`"));
    }

    Ok((
        filters.query,
        KnowledgeSemanticSearchFilters {
            knowledge_base: filters.knowledge_base,
            category: filters.category,
            limit: filters.limit,
            mode: filters.mode,
            min_score_millis: filters.min_score_millis,
        },
    ))
}

#[derive(Debug, Clone, Deserialize, Default)]
struct SearchRecordsParams {
    query: String,
    #[serde(default)]
    scope: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct KbSemanticSearchParams {
    query: String,
    #[serde(default)]
    knowledge_base: Option<String>,
    #[serde(default)]
    category: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    mode: snow_core::KnowledgeSearchMode,
    #[serde(default)]
    min_score_millis: Option<u32>,
}

#[derive(Debug, Clone, Deserialize)]
struct TaskSlaStatusForTasksParams {
    parents: Vec<TaskSlaParentRef>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct ListRecordsParams {
    #[serde(default)]
    resource_type: Option<String>,
    #[serde(default)]
    parent_number: Option<String>,
    #[serde(default)]
    assigned_to: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum BusinessApplicationLookup {
    SysId(String),
    Name(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ServerLookup {
    SysId(String),
    Name(String),
    IpAddress(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ServerGetRpcParams {
    lookup: ServerLookup,
    persist: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct BusinessApplicationHydrationOptions {
    #[serde(default = "default_true")]
    persist: bool,
    #[serde(default = "default_true")]
    resolve_references: bool,
    #[serde(default = "default_reference_depth")]
    reference_depth: usize,
    #[serde(default)]
    refresh_dictionary: bool,
}

impl Default for BusinessApplicationHydrationOptions {
    fn default() -> Self {
        Self {
            persist: true,
            resolve_references: true,
            reference_depth: 1,
            refresh_dictionary: false,
        }
    }
}

impl From<BusinessApplicationHydrationOptions> for snow_core::BusinessApplicationHydrationOptions {
    fn from(options: BusinessApplicationHydrationOptions) -> Self {
        Self {
            persist: options.persist,
            resolve_references: options.resolve_references,
            reference_depth: options.reference_depth,
            refresh_dictionary: options.refresh_dictionary,
        }
    }
}

#[derive(Debug, Clone)]
struct BusinessApplicationSyncRequest {
    all: bool,
    search_params: Option<snow_core::BusinessApplicationSearchParams>,
    options: BusinessApplicationHydrationOptions,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct BusinessApplicationQueryParams {
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    filters: Vec<BusinessApplicationFieldFilter>,
    #[serde(default)]
    include_tombstoned: bool,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    offset: Option<usize>,
    #[serde(default)]
    sort: Vec<BusinessApplicationSortField>,
}

#[derive(Debug, Clone, Deserialize)]
struct BusinessApplicationFieldFilter {
    field: String,
    op: String,
    #[serde(default)]
    value: Value,
}

#[derive(Debug, Clone, Deserialize)]
struct BusinessApplicationSortField {
    field: String,
    #[serde(default)]
    direction: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct BusinessApplicationFieldsParams {
    #[serde(default)]
    refresh_dictionary: bool,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct ServerFieldsParams {}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct ServerFieldSummary {
    field: String,
    observed_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    sample_value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sample_display_value: Option<String>,
}

/// One entry of the `business_application_fields` response.
///
/// `observed_count`/`sample_*` come from the locally projected Business
/// Application records. When dictionary metadata is present the `label`,
/// `field_type`, `reference_table`, `mandatory`, `read_only`, `choice`, and
/// `max_length` fields are merged in and `dictionary_verified` is `true`;
/// otherwise those remain `None`/`false` and the entry falls back to
/// observed-only behavior.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct BusinessApplicationFieldSummary {
    field: String,
    observed_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    sample_value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sample_display_value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    field_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reference_table: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    mandatory: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    read_only: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    choice: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_length: Option<i64>,
    dictionary_verified: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    diagnostic: Option<String>,
}

fn default_true() -> bool {
    true
}

fn default_reference_depth() -> usize {
    1
}

#[derive(Debug, Clone, Deserialize, Default)]
struct ListKnowledgeArticlesParams {
    #[serde(default)]
    knowledge_base_sys_id: Option<String>,
    #[serde(default)]
    category_sys_id: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub(crate) struct KbSyncParams {
    #[serde(default)]
    pub(crate) full: bool,
    #[serde(default)]
    pub(crate) with_bodies: bool,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub(crate) struct KbListTagsParams {
    #[serde(default)]
    pub(crate) layer: Option<String>,
    #[serde(default = "default_min_count")]
    pub(crate) min_count: usize,
}

fn extract_search_records_params(params: &Value) -> Result<SearchRecordsParams> {
    let params: SearchRecordsParams = serde_json::from_value(params.clone())?;
    if params.query.trim().is_empty() {
        return Err(anyhow!("missing required field `query`"));
    }
    Ok(params)
}

fn extract_user_lookup_params(params: &Value) -> Result<snow_core::UserLookup> {
    let params: snow_core::UserLookup = serde_json::from_value(params.clone())?;
    params.validate_selector()?;
    Ok(params)
}

fn extract_user_search_params(params: &Value) -> Result<snow_core::UserSearch> {
    let params: snow_core::UserSearch = serde_json::from_value(params.clone())?;
    params.validate()?;
    Ok(params)
}

fn extract_business_application_search_params(
    params: &Value,
) -> Result<(
    snow_core::BusinessApplicationSearchParams,
    BusinessApplicationHydrationOptions,
)> {
    let mut search_params = params.clone();
    let mut options = BusinessApplicationHydrationOptions::default();
    if let Value::Object(map) = &mut search_params {
        if let Some(value) = map.remove("persist").and_then(|value| value.as_bool()) {
            options.persist = value;
        }
        if let Some(value) = map
            .remove("resolve_references")
            .and_then(|value| value.as_bool())
        {
            options.resolve_references = value;
        }
        if let Some(value) = map
            .remove("reference_depth")
            .and_then(|value| value.as_u64())
        {
            if value > 2 {
                return Err(anyhow!("`reference_depth` must be 0, 1, or 2"));
            }
            options.reference_depth = value as usize;
        }
        if let Some(value) = map
            .remove("refresh_dictionary")
            .and_then(|value| value.as_bool())
        {
            options.refresh_dictionary = value;
        }
    }
    let params: snow_core::BusinessApplicationSearchParams = serde_json::from_value(search_params)?;
    params.validate()?;
    Ok((params, options))
}

/// Parse `business_application_sync` params: optional search params plus
/// hydration options. Unlike search, the search params are optional; when no
/// search field is supplied we pass `None` so core runs the default bounded
/// Business Application search.
fn extract_business_application_sync_params(
    params: &Value,
) -> Result<BusinessApplicationSyncRequest> {
    let mut sync_params = params.clone();
    let mut all = false;
    if let Value::Object(map) = &mut sync_params {
        match map.remove("all") {
            Some(Value::Bool(value)) => all = value,
            Some(_) => return Err(anyhow!("`all` must be a boolean")),
            None => {}
        }
    }
    let (search_params, options) = extract_business_application_search_params(&sync_params)?;
    // Treat an all-default params object (no filters set) as "no search params".
    let has_filter = search_params != snow_core::BusinessApplicationSearchParams::default();
    if all && has_filter {
        return Err(anyhow!(
            "`all` cannot be combined with Business Application search filters"
        ));
    }
    Ok(BusinessApplicationSyncRequest {
        all,
        search_params: (!all && has_filter).then_some(search_params),
        options,
    })
}

fn extract_business_application_hydration_options(
    params: &Value,
) -> Result<BusinessApplicationHydrationOptions> {
    let mut options = BusinessApplicationHydrationOptions::default();
    let Value::Object(map) = params else {
        return Ok(options);
    };
    if let Some(value) = map.get("persist").and_then(Value::as_bool) {
        options.persist = value;
    }
    if let Some(value) = map.get("resolve_references").and_then(Value::as_bool) {
        options.resolve_references = value;
    }
    if let Some(value) = map.get("reference_depth").and_then(Value::as_u64) {
        if value > 2 {
            return Err(anyhow!("`reference_depth` must be 0, 1, or 2"));
        }
        options.reference_depth = value as usize;
    }
    if let Some(value) = map.get("refresh_dictionary").and_then(Value::as_bool) {
        options.refresh_dictionary = value;
    }
    Ok(options)
}

fn extract_business_application_lookup_params(params: &Value) -> Result<BusinessApplicationLookup> {
    let Value::Object(map) = params else {
        return Err(anyhow!("expected object params"));
    };
    let sys_id = map.get("sys_id").and_then(Value::as_str).map(str::trim);
    let name = map.get("name").and_then(Value::as_str).map(str::trim);
    match (
        sys_id.filter(|value| !value.is_empty()),
        name.filter(|value| !value.is_empty()),
    ) {
        (Some(sys_id), None) => Ok(BusinessApplicationLookup::SysId(
            snow_core::normalize_record_lookup_sys_id(sys_id)?,
        )),
        (None, Some(name)) => Ok(BusinessApplicationLookup::Name(name.to_string())),
        (Some(_), Some(_)) => Err(anyhow!("provide exactly one of `sys_id` or `name`")),
        (None, None) => Err(anyhow!(
            "missing required lookup: provide `sys_id` or `name`"
        )),
    }
}

fn extract_business_application_query_params(
    params: &Value,
) -> Result<BusinessApplicationQueryParams> {
    let query: BusinessApplicationQueryParams = serde_json::from_value(params.clone())?;
    if query.limit == Some(0) {
        return Err(anyhow!("`limit` must be at least 1"));
    }
    if query.limit.unwrap_or(20) > 500 {
        return Err(anyhow!("`limit` must be at most 500"));
    }
    Ok(query)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BusinessApplicationServersRpcParams {
    traversal: snow_core::BusinessApplicationServersParams,
    persist: bool,
    prune_stale: bool,
}

/// Deserialize an incoming `business_application_servers` request into the
/// canonical [`snow_core::BusinessApplicationServersParams`] traversal contract
/// plus daemon-level persistence controls, then run validation up-front.
///
/// The core type owns `#[serde(deny_unknown_fields)]` and
/// [`snow_core::BusinessApplicationServersParams::validate`], so unknown fields,
/// the selector XOR (`number` vs `sys_id`), the `BA:<sys_id>` fallback guard,
/// the traversal bounds (`max_depth`/`max_cis`/`max_edges`) and selector
/// normalization are all enforced in one place. Validating here (instead of
/// leaning on the re-validation inside `SnowCore::business_application_servers`)
/// is what lets the dispatcher classify a validation failure as
/// `invalid_params` rather than a service/internal error.
fn parse_business_application_servers_params(
    params: &Value,
) -> Result<BusinessApplicationServersRpcParams> {
    let mut traversal_params = params.clone();
    let mut persist = true;
    let mut prune_stale = false;
    if let Value::Object(map) = &mut traversal_params {
        match map.remove("persist") {
            Some(Value::Bool(value)) => persist = value,
            Some(_) => return Err(anyhow!("`persist` must be a boolean")),
            None => {}
        }
        match map.remove("prune_stale") {
            Some(Value::Bool(value)) => prune_stale = value,
            Some(_) => return Err(anyhow!("`prune_stale` must be a boolean")),
            None => {}
        }
    }
    if prune_stale && !persist {
        return Err(anyhow!("`prune_stale` requires `persist=true`"));
    }
    let params: snow_core::BusinessApplicationServersParams =
        serde_json::from_value(traversal_params)?;
    // Surface validation errors here so the caller maps them to invalid_params.
    // The resulting options are discarded; core re-validates during traversal.
    params.validate()?;
    Ok(BusinessApplicationServersRpcParams {
        traversal: params,
        persist,
        prune_stale,
    })
}

fn parse_business_application_servers_cached_params(
    params: &Value,
) -> Result<snow_core::BusinessApplicationServersCachedParams> {
    let params: snow_core::BusinessApplicationServersCachedParams =
        serde_json::from_value(params.clone())?;
    params.validate()?;
    Ok(params)
}

fn parse_business_applications_for_server_params(
    params: &Value,
) -> Result<snow_core::BusinessApplicationsForServerParams> {
    let params: snow_core::BusinessApplicationsForServerParams =
        serde_json::from_value(params.clone())?;
    params.validate()?;
    Ok(params)
}

fn extract_business_application_fields_params(
    params: &Value,
) -> Result<BusinessApplicationFieldsParams> {
    Ok(serde_json::from_value(params.clone())?)
}

fn extract_resource_plan_list_params(params: &Value) -> Result<snow_core::ResourcePlanListInput> {
    let input: snow_core::ResourcePlanListInput = serde_json::from_value(params.clone())?;
    snow_core::validate_list_input(input.clone())?;
    Ok(input)
}

/// Deserializes and pre-validates group-scoped Incident page arguments.
///
/// Validation runs here as well as inside the core so a malformed group
/// `sys_id`, cursor, or page size surfaces as `-32602 invalid params` rather
/// than an internal error.
fn extract_incident_list_by_assignment_group_params(
    params: &Value,
) -> Result<snow_core::IncidentAssignmentGroupListInput> {
    let input: snow_core::IncidentAssignmentGroupListInput =
        serde_json::from_value(params.clone())?;
    snow_core::validate_incident_assignment_group_input(input.clone())?;
    Ok(input)
}

/// Maps a group-scoped Incident page failure onto the JSON-RPC error contract.
///
/// An unresolved state carries the live choice list through as structured
/// `data` so an agent can correct its selector without a second round trip;
/// anything that is not a caller-argument problem stays an internal error.
fn incident_group_list_error_response(id: Option<Value>, err: anyhow::Error) -> JsonRpcResponse {
    match err.downcast_ref::<snow_core::IncidentAssignmentGroupListError>() {
        Some(snow_core::IncidentAssignmentGroupListError::InvalidParams(_)) => {
            invalid_params(id, err)
        }
        Some(snow_core::IncidentAssignmentGroupListError::UnresolvedState {
            requested,
            ambiguous,
            choices,
        }) => JsonRpcResponse::error(
            id,
            -32602,
            "invalid params",
            Some(json!({
                "details": err.to_string(),
                "field": "state",
                "requested": requested,
                "ambiguous": ambiguous,
                "choices": choices
                    .iter()
                    .map(|choice| json!({ "value": choice.value, "label": choice.label }))
                    .collect::<Vec<_>>(),
            })),
        ),
        None => internal_error(id, err),
    }
}

fn extract_server_lookup_params(params: &Value) -> Result<ServerLookup> {
    let Value::Object(map) = params else {
        return Err(anyhow!("expected object params"));
    };
    let sys_id = map.get("sys_id").and_then(Value::as_str).map(str::trim);
    let name = map.get("name").and_then(Value::as_str).map(str::trim);
    let ip_address = map.get("ip_address").and_then(Value::as_str).map(str::trim);
    match (
        sys_id.filter(|value| !value.is_empty()),
        name.filter(|value| !value.is_empty()),
        ip_address.filter(|value| !value.is_empty()),
    ) {
        (Some(sys_id), None, None) => Ok(ServerLookup::SysId(
            snow_core::normalize_record_lookup_sys_id(sys_id)?,
        )),
        (None, Some(name), None) => Ok(ServerLookup::Name(name.to_string())),
        (None, None, Some(ip_address)) => Ok(ServerLookup::IpAddress(ip_address.to_string())),
        (None, None, None) => Err(anyhow!(
            "missing required lookup: provide `sys_id`, `name`, or `ip_address`"
        )),
        _ => Err(anyhow!(
            "provide exactly one of `sys_id`, `name`, or `ip_address`"
        )),
    }
}

fn extract_server_get_params(params: &Value) -> Result<ServerGetRpcParams> {
    let Value::Object(map) = params else {
        return Err(anyhow!("expected object params"));
    };
    let persist = match map.get("persist") {
        Some(Value::Bool(value)) => *value,
        Some(_) => return Err(anyhow!("`persist` must be a boolean")),
        None => true,
    };
    Ok(ServerGetRpcParams {
        lookup: extract_server_lookup_params(params)?,
        persist,
    })
}

fn core_server_lookup(lookup: ServerLookup) -> Result<snow_core::ServerLookup> {
    Ok(match lookup {
        ServerLookup::SysId(sys_id) => snow_core::ServerLookup::sys_id(sys_id)?,
        ServerLookup::Name(name) => snow_core::ServerLookup::exact_name(name),
        ServerLookup::IpAddress(ip_address) => snow_core::ServerLookup::ip_address(ip_address),
    })
}

fn extract_server_search_params(params: &Value) -> Result<snow_core::ServerSearchParams> {
    let params: snow_core::ServerSearchParams = serde_json::from_value(params.clone())?;
    params.validate()?;
    Ok(params)
}

fn extract_server_query_params(params: &Value) -> Result<snow_core::ServerQuery> {
    let params: snow_core::ServerQuery = serde_json::from_value(params.clone())?;
    params.validate()?;
    Ok(params)
}

fn extract_server_fields_params(params: &Value) -> Result<ServerFieldsParams> {
    Ok(serde_json::from_value(params.clone())?)
}

async fn get_business_application_cached(
    core: &SnowCore,
    lookup: &BusinessApplicationLookup,
) -> Result<Option<SnowRecord>> {
    let records = core
        .list_records_query(
            ListQuery::new()
                .resource_type(ResourceType::BusinessApplication)
                .include_tombstoned(false),
        )
        .await?;
    Ok(records.into_iter().find(|record| match lookup {
        BusinessApplicationLookup::SysId(sys_id) => record.sys_id.eq_ignore_ascii_case(sys_id),
        BusinessApplicationLookup::Name(name) => business_application_name(record) == name.trim(),
    }))
}

async fn get_server_cached(core: &SnowCore, lookup: &ServerLookup) -> Result<Option<SnowRecord>> {
    let records = core
        .list_records_query(
            ListQuery::new()
                .resource_type(ResourceType::Server)
                .include_tombstoned(false),
        )
        .await?;
    Ok(records.into_iter().find(|record| match lookup {
        ServerLookup::SysId(sys_id) => record.sys_id.eq_ignore_ascii_case(sys_id),
        ServerLookup::Name(name) => server_name(record) == name.trim(),
        ServerLookup::IpAddress(ip_address) => server_field(record, "ip_address")
            .is_some_and(|value| value.eq_ignore_ascii_case(ip_address.trim())),
    }))
}

async fn query_business_applications_local(
    core: &SnowCore,
    params: &BusinessApplicationQueryParams,
) -> Result<Vec<SnowRecord>> {
    core.query_business_applications(core_business_application_query(params)?)
        .await
}

fn core_business_application_query(
    params: &BusinessApplicationQueryParams,
) -> Result<snow_core::query::filter::BusinessApplicationQuery> {
    let filters = params
        .filters
        .iter()
        .map(|filter| {
            Ok(snow_core::query::filter::FieldFilter {
                field: filter.field.clone(),
                op: core_field_operator(filter.op.as_str())?,
                value: filter.value.clone(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let sort = params
        .sort
        .iter()
        .map(|field| snow_core::query::filter::SortField {
            field: field.field.clone(),
            direction: if field
                .direction
                .as_deref()
                .is_some_and(|direction| direction.eq_ignore_ascii_case("desc"))
            {
                snow_core::query::filter::SortDirection::Desc
            } else {
                snow_core::query::filter::SortDirection::Asc
            },
        })
        .collect();

    Ok(snow_core::query::filter::BusinessApplicationQuery {
        text: params.text.clone(),
        filters,
        include_tombstoned: params.include_tombstoned,
        limit: params.limit,
        offset: params.offset,
        sort,
        allow_unknown_fields: true,
    })
}

fn core_field_operator(op: &str) -> Result<snow_core::query::filter::FieldOperator> {
    Ok(match op.trim().to_ascii_lowercase().as_str() {
        "eq" => snow_core::query::filter::FieldOperator::Eq,
        "ne" => snow_core::query::filter::FieldOperator::Ne,
        "contains" => snow_core::query::filter::FieldOperator::Contains,
        "starts_with" | "startswith" => snow_core::query::filter::FieldOperator::StartsWith,
        "in" => snow_core::query::filter::FieldOperator::In,
        "is_empty" => snow_core::query::filter::FieldOperator::IsEmpty,
        "is_not_empty" => snow_core::query::filter::FieldOperator::IsNotEmpty,
        "gt" => snow_core::query::filter::FieldOperator::Gt,
        "gte" => snow_core::query::filter::FieldOperator::Gte,
        "lt" => snow_core::query::filter::FieldOperator::Lt,
        "lte" => snow_core::query::filter::FieldOperator::Lte,
        other => {
            return Err(anyhow!(
                "unsupported Business Application field operator `{other}`"
            ));
        }
    })
}

async fn business_application_servers(
    core: &SnowCore,
    transport: &DaemonTransport<'_>,
    params: snow_core::BusinessApplicationServersParams,
) -> Result<Option<Value>> {
    let Some(result) = core.business_application_servers(params).await? else {
        return Ok(None);
    };

    let result_value = serde_json::to_value(&result)?;
    let mut servers = Vec::with_capacity(result.servers.len());
    for server in result.servers {
        // Per-server `source` tag. Traversal servers are the default
        // (`cmdb_rel_ci`) and are omitted from `server_sources`, so only
        // `ci_owner_group` fallback servers carry an explicit source here.
        let source = result.server_sources.get(&server.record.sys_id).copied();
        let mut server_value = serde_json::to_value(transport.server(&server.record)?)?;
        if let (Some(source), Some(object)) = (source, server_value.as_object_mut()) {
            object.insert("source".to_string(), json!(source.as_str()));
        }
        servers.push(server_value);
    }

    let mut response = json!({
        "business_application": result.business_application,
        "servers": servers,
        "relationship_summary": result.relationship_summary,
        "diagnostics": result.diagnostics,
        "server_paths": result.server_paths,
    });
    if let (Some(response), Some(result_value)) =
        (response.as_object_mut(), result_value.as_object())
    {
        for (key, value) in result_value {
            // The per-server `source` tag is already attached to each server
            // above; the top-level `server_sources` map is an internal merge
            // helper, not part of the response contract, so it is not surfaced.
            if key == "server_sources" {
                continue;
            }
            response.entry(key.clone()).or_insert_with(|| value.clone());
        }
    }

    Ok(Some(response))
}

async fn business_application_servers_cached(
    core: &SnowCore,
    transport: &DaemonTransport<'_>,
    params: snow_core::BusinessApplicationServersCachedParams,
) -> Result<Option<Value>> {
    let Some(result) = core.business_application_servers_cached(params).await? else {
        return Ok(None);
    };

    let business_application = transport.business_application(&result.business_application)?;
    let mut servers = Vec::with_capacity(result.servers.len());
    for relationship in result.servers {
        servers.push(json!({
            "server": transport.server(&relationship.server)?,
            "server_table": relationship.server_table,
            "provenance": relationship.provenance,
            "min_depth": relationship.min_depth,
            "paths": relationship.paths,
            "tombstoned_at": relationship.tombstoned_at,
        }));
    }

    Ok(Some(json!({
        "business_application": business_application,
        "servers": servers,
        "endpoint_status": result.endpoint_status,
        "relationship_status": result.relationship_status,
        "inventory_health": result.inventory_health,
    })))
}

async fn business_applications_for_server(
    core: &SnowCore,
    transport: &DaemonTransport<'_>,
    params: snow_core::BusinessApplicationsForServerParams,
) -> Result<Option<Value>> {
    let Some(result) = core.business_applications_for_server(params).await? else {
        return Ok(None);
    };

    let mut servers = Vec::with_capacity(result.servers.len());
    for server_relationships in result.servers {
        let mut business_applications =
            Vec::with_capacity(server_relationships.business_applications.len());
        for relationship in server_relationships.business_applications {
            business_applications.push(json!({
                "business_application": transport.business_application(&relationship.business_application)?,
                "provenance": relationship.provenance,
                "min_depth": relationship.min_depth,
                "paths": relationship.paths,
                "inventory_health": relationship.inventory_health,
                "tombstoned_at": relationship.tombstoned_at,
            }));
        }
        servers.push(json!({
            "server": transport.server(&server_relationships.server)?,
            "business_applications": business_applications,
            "endpoint_status": server_relationships.endpoint_status,
            "relationship_status": server_relationships.relationship_status,
        }));
    }

    Ok(Some(json!({
        "servers": servers,
        "endpoint_status": result.endpoint_status,
        "relationship_status": result.relationship_status,
    })))
}

async fn business_application_fields(
    core: &SnowCore,
    params: BusinessApplicationFieldsParams,
) -> Result<Vec<BusinessApplicationFieldSummary>> {
    // When requested, refresh the live dictionary before reading. Best-effort:
    // a refresh failure leaves us with whatever cached/observed data exists.
    if params.refresh_dictionary {
        let _ = core.refresh_business_application_dictionary().await;
    }

    // Load cached dictionary metadata (empty map => degraded/observed-only mode).
    let dictionary = core
        .business_application_dictionary()
        .await
        .unwrap_or_default();

    let records = core
        .list_records_query(ListQuery::new().resource_type(ResourceType::BusinessApplication))
        .await?;
    let mut fields: std::collections::BTreeMap<String, BusinessApplicationFieldSummary> =
        std::collections::BTreeMap::new();

    // Seed entries from the dictionary so verified fields appear even when no
    // record has yet been observed locally.
    for (name, row) in &dictionary {
        fields
            .entry(name.clone())
            .or_insert_with(|| dictionary_field_summary(name, row));
    }

    for record in records {
        for (name, value) in record.fields {
            let entry = fields.entry(name.clone()).or_insert_with(|| {
                // No dictionary row for this observed field: fall back to the
                // observed-only summary, attaching a degraded diagnostic when a
                // dictionary was expected but is unavailable.
                BusinessApplicationFieldSummary {
                    field: name.clone(),
                    observed_count: 0,
                    sample_value: None,
                    sample_display_value: None,
                    label: None,
                    field_type: None,
                    reference_table: None,
                    mandatory: None,
                    read_only: None,
                    choice: None,
                    max_length: None,
                    dictionary_verified: false,
                    diagnostic: (params.refresh_dictionary && dictionary.is_empty()).then(|| {
                        "dictionary unavailable; field metadata is observed-only".to_string()
                    }),
                }
            });
            entry.observed_count += 1;
            if entry.sample_value.is_none() && !value.value.trim().is_empty() {
                entry.sample_value = Some(value.value);
            }
            if entry.sample_display_value.is_none() {
                entry.sample_display_value = value
                    .display_value
                    .filter(|display| !display.trim().is_empty());
            }
        }
    }
    Ok(fields.into_values().collect())
}

async fn server_fields(
    core: &SnowCore,
    _params: ServerFieldsParams,
) -> Result<Vec<ServerFieldSummary>> {
    let records = core
        .list_records_query(ListQuery::new().resource_type(ResourceType::Server))
        .await?;
    let mut fields = std::collections::BTreeMap::<String, ServerFieldSummary>::new();
    for record in records {
        for (name, value) in record.fields {
            let entry = fields.entry(name.clone()).or_insert(ServerFieldSummary {
                field: name,
                observed_count: 0,
                sample_value: None,
                sample_display_value: None,
            });
            entry.observed_count += 1;
            if entry.sample_value.is_none() && !value.value.trim().is_empty() {
                entry.sample_value = Some(value.value);
            }
            if entry.sample_display_value.is_none() {
                entry.sample_display_value = value
                    .display_value
                    .filter(|display| !display.trim().is_empty());
            }
        }
    }
    Ok(fields.into_values().collect())
}

/// Build an enriched field summary from a cached dictionary row.
fn dictionary_field_summary(
    name: &str,
    row: &snow_core::cache::store::BusinessApplicationFieldDictionaryRow,
) -> BusinessApplicationFieldSummary {
    BusinessApplicationFieldSummary {
        field: name.to_string(),
        observed_count: 0,
        sample_value: None,
        sample_display_value: None,
        label: row.field_label.clone(),
        field_type: row.field_type.clone(),
        reference_table: row.reference_table.clone(),
        mandatory: Some(row.mandatory),
        read_only: Some(row.read_only),
        choice: Some(row.choice),
        max_length: row.max_length,
        dictionary_verified: true,
        diagnostic: None,
    }
}

fn business_application_diagnostics(
    diagnostics: &[snow_core::ReferenceResolutionDiagnostic],
) -> Vec<DaemonBusinessApplicationDiagnostic> {
    diagnostics
        .iter()
        .map(|diagnostic| DaemonBusinessApplicationDiagnostic {
            field: diagnostic.field.clone(),
            sys_id: (!diagnostic.reference_sys_id.is_empty())
                .then(|| diagnostic.reference_sys_id.clone()),
            table: (!diagnostic.reference_table.is_empty())
                .then(|| diagnostic.reference_table.clone()),
            diagnostic: diagnostic.message.clone().unwrap_or_else(|| {
                format!("{:?}", diagnostic.reason)
                    .chars()
                    .enumerate()
                    .flat_map(|(idx, ch)| {
                        if idx > 0 && ch.is_ascii_uppercase() {
                            vec!['_', ch.to_ascii_lowercase()]
                        } else {
                            vec![ch.to_ascii_lowercase()]
                        }
                    })
                    .collect()
            }),
        })
        .collect()
}

fn business_application_name(record: &SnowRecord) -> String {
    record
        .fields
        .get("name")
        .and_then(|field| {
            field
                .display_value
                .as_ref()
                .or(Some(&field.value))
                .filter(|value| !value.trim().is_empty())
                .cloned()
        })
        .or_else(|| {
            (!record.short_description.trim().is_empty()).then(|| record.short_description.clone())
        })
        .unwrap_or_else(|| record.sys_id.clone())
}

fn server_name(record: &SnowRecord) -> String {
    server_field(record, "name")
        .or_else(|| {
            (!record.short_description.trim().is_empty()).then(|| record.short_description.clone())
        })
        .unwrap_or_else(|| record.sys_id.clone())
}

fn server_field(record: &SnowRecord, field: &str) -> Option<String> {
    record.fields.get(field).and_then(|value| {
        value
            .display_value
            .clone()
            .or_else(|| Some(value.value.clone()))
            .filter(|value| !value.trim().is_empty())
    })
}

fn extract_list_records_params(params: &Value) -> Result<ListRecordsParams> {
    Ok(serde_json::from_value(params.clone())?)
}

fn extract_list_knowledge_articles_params(params: &Value) -> Result<ListKnowledgeArticlesParams> {
    Ok(serde_json::from_value(params.clone())?)
}

pub(crate) fn extract_kb_sync_params(params: &Value) -> Result<KbSyncParams> {
    Ok(serde_json::from_value(params.clone())?)
}

pub(crate) fn extract_kb_list_tags_params(params: &Value) -> Result<KbListTagsParams> {
    let params: KbListTagsParams = serde_json::from_value(params.clone())?;
    if params.min_count == 0 {
        return Err(anyhow!("`min_count` must be at least 1"));
    }
    if let Some(layer) = params.layer.as_deref() {
        validate_kb_tag_filter(layer)?;
    }
    Ok(params)
}

fn parse_resource_type(resource_type: &str) -> Result<ResourceType> {
    let normalized = resource_type.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "task" => Ok(ResourceType::Task),
        "incident" => Ok(ResourceType::Incident),
        "change" | "change_request" => Ok(ResourceType::Change),
        "change_task" => Ok(ResourceType::ChangeTask),
        "request" | "sc_req_item" | "request_item" => Ok(ResourceType::Request),
        "request_task" | "sc_task" => Ok(ResourceType::RequestTask),
        "project" | "pm_project" => Ok(ResourceType::Project),
        "demand" | "dmn_demand" => Ok(ResourceType::Demand),
        "demand_task" | "dmn_demand_task" | "dmntsk" => Ok(ResourceType::DemandTask),
        "resource_plan" | "resourceplan" | "rpln" => Ok(ResourceType::ResourcePlan),
        "story" | "rm_story" => Ok(ResourceType::Story),
        "scrum_task" | "rm_scrum_task" => Ok(ResourceType::ScrumTask),
        "knowledge" | "kb_knowledge" => Ok(ResourceType::Knowledge),
        "approval" | "sysapproval_approver" => Ok(ResourceType::Approval),
        "business_application" | "business_app" | "cmdb_ci_business_app" => {
            Ok(ResourceType::BusinessApplication)
        }
        "server"
        | "servers"
        | "cmdb_ci_server"
        | "cmdb_ci_linux_server"
        | "cmdb_ci_win_server"
        | "linux_server"
        | "windows_server" => Ok(ResourceType::Server),
        "private_task" | "vtb_task" => Ok(ResourceType::PrivateTask),
        _ => Err(anyhow!("unsupported resource_type `{resource_type}`")),
    }
}

fn parse_search_scope(scope: Option<&str>) -> SearchScope {
    match scope.unwrap_or("all").trim().to_ascii_lowercase().as_str() {
        "knowledge" => SearchScope::Knowledge,
        "work_notes" => SearchScope::WorkNotes,
        _ => SearchScope::All,
    }
}

fn default_min_count() -> usize {
    1
}

fn validate_kb_tag_filter(layer: &str) -> Result<()> {
    match layer.trim().to_ascii_lowercase().as_str() {
        "all" | "sn" | "auto" | "user" => Ok(()),
        _ => Err(anyhow!("unsupported tag layer `{layer}`")),
    }
}

pub(crate) fn core_kb_tag_layer(layer: &str) -> Result<snow_core::KnowledgeTagLayer> {
    match layer.trim().to_ascii_lowercase().as_str() {
        "sn" => Ok(snow_core::KnowledgeTagLayer::Sn),
        "auto" => Ok(snow_core::KnowledgeTagLayer::Auto),
        "user" => Ok(snow_core::KnowledgeTagLayer::User),
        _ => Err(anyhow!("unsupported tag layer `{layer}`")),
    }
}

async fn get_record_cached_or_fresh(core: &SnowCore, number: &str) -> Result<Option<SnowRecord>> {
    match core.get_record(number).await? {
        Some(record) => Ok(Some(record)),
        None => core.get_record_fresh(number).await,
    }
}

pub(crate) async fn get_record_by_lookup_cached_or_fresh(
    core: &SnowCore,
    lookup: RecordLookup,
) -> Result<Option<SnowRecord>> {
    match lookup {
        RecordLookup::Number(number) => get_record_cached_or_fresh(core, &number).await,
        RecordLookup::TableSysId { table, sys_id } => {
            core.get_record_by_table_sys_id_fresh(&table, &sys_id).await
        }
    }
}

fn wrap_records_response(
    id: Option<Value>,
    transport: &DaemonTransport<'_>,
    records: Vec<snow_core::SnowRecord>,
) -> JsonRpcResponse {
    let mut record_dtos = Vec::with_capacity(records.len());
    for record in records {
        match transport.record(&record) {
            Ok(record) => record_dtos.push(record),
            Err(err) => return internal_error(id, err),
        }
    }
    JsonRpcResponse::ok(id, json!({ "records": record_dtos }))
}

fn wrap_list_my_approvals_response(
    id: Option<Value>,
    transport: &DaemonTransport<'_>,
    response: snow_core::ListMyApprovalsResponse,
) -> JsonRpcResponse {
    let mut approval_dtos = Vec::with_capacity(response.records.len());
    for approval in response.records {
        match transport.approval(&approval) {
            Ok(approval) => approval_dtos.push(approval),
            Err(err) => return internal_error(id, err),
        }
    }
    JsonRpcResponse::ok(
        id,
        json!({
            "records": approval_dtos,
            "query_summary": response.query_summary,
        }),
    )
}

fn internal_error(id: Option<Value>, err: impl ToString) -> JsonRpcResponse {
    JsonRpcResponse::error(
        id,
        -32000,
        "internal error",
        Some(json!({ "details": err.to_string() })),
    )
}

/// JSON-RPC code for an unresolvable record-number prefix (caller mistake).
const UNKNOWN_PREFIX_CODE: i64 = -32006;

/// Map core lookup failures that are caller mistakes (unknown prefix) to a
/// structured JSON-RPC error instead of `internal_error` (-32000).
fn map_record_lookup_error(id: Option<Value>, err: impl ToString) -> JsonRpcResponse {
    let details = err.to_string();
    if is_unknown_prefix_error_message(&details) {
        return JsonRpcResponse::error(
            id,
            UNKNOWN_PREFIX_CODE,
            "unknown record prefix",
            Some(json!({ "details": details })),
        );
    }
    internal_error(id, details)
}

fn is_unknown_prefix_error_message(message: &str) -> bool {
    // Prefer a typed snow_core error when one exists; until then match the
    // stable substring from CoreContext::get_record_fresh_with_source.
    message.contains("unknown ServiceNow prefix")
}

/// Map a structured [`snow_core::ServerGetError`] from the live `server_get`
/// fallback onto a distinct JSON-RPC error. A confirmed not-found is the only
/// `-32004`; ACL, network/timeout, and duplicate-CI disambiguation each get
/// their own code so callers never mistake a transient failure for a genuine
/// not-found.
fn server_get_error_response(id: Option<Value>, err: snow_core::ServerGetError) -> JsonRpcResponse {
    use snow_core::ServerGetError;
    match err {
        ServerGetError::NotFound => JsonRpcResponse::error(id, -32004, "server not found", None),
        ServerGetError::AclRestricted(detail) => JsonRpcResponse::error(
            id,
            -32003,
            "server is ACL-restricted",
            Some(json!({ "details": detail })),
        ),
        ServerGetError::Network(detail) => JsonRpcResponse::error(
            id,
            -32001,
            "network error reaching ServiceNow",
            Some(json!({ "details": detail })),
        ),
        ServerGetError::Disambiguation { selector, matched } => JsonRpcResponse::error(
            id,
            -32005,
            "multiple servers matched selector",
            Some(json!({ "selector": selector, "matched": matched })),
        ),
        ServerGetError::Hydration(detail) => internal_error(id, detail),
        ServerGetError::Other(detail) => internal_error(id, detail),
    }
}

fn invalid_params(id: Option<Value>, err: impl ToString) -> JsonRpcResponse {
    JsonRpcResponse::error(
        id,
        -32602,
        "invalid params",
        Some(json!({ "details": err.to_string() })),
    )
}

impl JsonRpcResponse {
    pub(crate) fn ok(id: Option<Value>, result: Value) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            result: Some(result),
            error: None,
            id,
        }
    }

    pub(crate) fn error(id: Option<Value>, code: i64, message: &str, data: Option<Value>) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            result: None,
            error: Some(JsonRpcError {
                code,
                message: message.to_string(),
                data,
            }),
            id,
        }
    }
}

#[cfg(test)]
mod tests;
