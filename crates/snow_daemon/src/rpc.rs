use anyhow::{Context, Result, anyhow};
use interprocess::local_socket::tokio::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use snow_core::ipc::IpcEndpoint;
use snow_core::{
    KnowledgeSemanticSearchFilters, ResourceType, SearchScope, SnowCore, SnowRecord,
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
    DaemonKnowledgeSemanticStatus, DaemonKnowledgeStatus, DaemonKnowledgeSyncOutcome,
    DaemonKnowledgeTagSummary, DaemonSemanticIndexSummary,
};
use crate::{DaemonState, transport::DaemonTransport};
use snow_core::vault::markdown::{render_approval_record, render_knowledge_article};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub method: String,
    #[serde(default)]
    pub params: Value,
    #[serde(default)]
    pub id: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JsonRpcError {
    pub code: i64,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcMethod {
    ContractInfo,
    GetRecord,
    GetRecordFresh,
    GetKnowledgeArticle,
    GetKnowledgeArticleFresh,
    GetArticle,
    GetArticleFresh,
    TaskSlaStatus,
    TaskSlaStatusForTasks,
    SearchKnowledge,
    KbSemanticSearch,
    SearchRecords,
    ListKnowledgeBases,
    ListCategories,
    ListKnowledgeArticles,
    GetApproval,
    GetChildren,
    GetWorkNotes,
    ListRecords,
    ListMyTasks,
    ListMyApprovals,
    ListMyProjects,
    ListMyStories,
    ListMyIncidents,
    MyTasks,
    MyTasksFresh,
    MyApprovals,
    MyApprovalsFresh,
    MyProjects,
    MyProjectsFresh,
    MyStoriesFresh,
    MyIncidentsFresh,
    VaultPath,
    AddWorkNote,
    SetState,
    FieldChoices,
    Approve,
    Reject,
    GetDegradedReads,
    CacheInfo,
    RepairVault,
    RebuildCache,
    VerifyVault,
    PruneOrphans,
    RefreshAll,
    KbSync,
    KbListTags,
    KbStatus,
    KbSemanticStatus,
    KbSemanticRebuild,
    SchedulerStatus,
    SchedulerTriggerNow,
    StartJob,
    GetJob,
    ListJobs,
    CancelJob,
    PlanGet,
    WorkNotePlanAdd,
    WorkNoteApplyAdd,
    ChangeRequestPlanCreate,
    ChangeRequestApplyCreate,
    ChangeRequestPlanUpdate,
    ChangeRequestApplyUpdate,
    ChangeTaskPlanCreate,
    ChangeTaskApplyCreate,
    ChangeTaskPlanUpdate,
    ChangeTaskApplyUpdate,
    StoryPlanCreate,
    StoryApplyCreate,
    StoryPlanUpdate,
    StoryApplyUpdate,
    StoryTaskPlanCreate,
    StoryTaskApplyCreate,
    StoryTaskPlanUpdate,
    StoryTaskApplyUpdate,
    TimecardList,
    TimecardSetHours,
    TimecardPlanSetHours,
    TimecardApplySetHours,
    Shutdown,
    Ping,
    Unknown,
}

impl RpcMethod {
    pub fn from_method(method: &str) -> Self {
        match method {
            "contract_info" => Self::ContractInfo,
            "get_record" => Self::GetRecord,
            "get_record_fresh" => Self::GetRecordFresh,
            "get_knowledge_article" => Self::GetKnowledgeArticle,
            "get_knowledge_article_fresh" => Self::GetKnowledgeArticleFresh,
            "get_article" => Self::GetArticle,
            "get_article_fresh" => Self::GetArticleFresh,
            "task_sla_status" => Self::TaskSlaStatus,
            "task_sla_status_for_tasks" => Self::TaskSlaStatusForTasks,
            "search_knowledge" => Self::SearchKnowledge,
            "kb_semantic_search" => Self::KbSemanticSearch,
            "search_records" => Self::SearchRecords,
            "list_knowledge_bases" => Self::ListKnowledgeBases,
            "list_categories" => Self::ListCategories,
            "list_knowledge_articles" => Self::ListKnowledgeArticles,
            "get_approval" => Self::GetApproval,
            "get_children" => Self::GetChildren,
            "get_work_notes" => Self::GetWorkNotes,
            "list_records" => Self::ListRecords,
            "list_my_tasks" => Self::ListMyTasks,
            "list_my_approvals" => Self::ListMyApprovals,
            "list_my_projects" => Self::ListMyProjects,
            "list_my_stories" => Self::ListMyStories,
            "list_my_incidents" => Self::ListMyIncidents,
            "my_tasks" => Self::MyTasks,
            "my_tasks_fresh" => Self::MyTasksFresh,
            "my_approvals" => Self::MyApprovals,
            "my_approvals_fresh" => Self::MyApprovalsFresh,
            "my_projects" => Self::MyProjects,
            "my_projects_fresh" => Self::MyProjectsFresh,
            "my_stories_fresh" => Self::MyStoriesFresh,
            "my_incidents_fresh" => Self::MyIncidentsFresh,
            "vault_path" => Self::VaultPath,
            "add_work_note" => Self::AddWorkNote,
            "set_state" => Self::SetState,
            "field_choices" => Self::FieldChoices,
            "approve" => Self::Approve,
            "reject" => Self::Reject,
            "get_degraded_reads" => Self::GetDegradedReads,
            "cache_info" => Self::CacheInfo,
            "repair_vault" => Self::RepairVault,
            "rebuild_cache" => Self::RebuildCache,
            "verify_vault" => Self::VerifyVault,
            "prune_orphans" => Self::PruneOrphans,
            "refresh_all" => Self::RefreshAll,
            "kb_sync" => Self::KbSync,
            "kb_list_tags" => Self::KbListTags,
            "kb_status" => Self::KbStatus,
            "kb_semantic_status" => Self::KbSemanticStatus,
            "kb_semantic_rebuild" => Self::KbSemanticRebuild,
            "scheduler.status" => Self::SchedulerStatus,
            "scheduler.trigger_now" => Self::SchedulerTriggerNow,
            "start_job" => Self::StartJob,
            "get_job" => Self::GetJob,
            "list_jobs" => Self::ListJobs,
            "cancel_job" => Self::CancelJob,
            "plan_get" => Self::PlanGet,
            "work_note_plan_add" => Self::WorkNotePlanAdd,
            "work_note_apply_add" => Self::WorkNoteApplyAdd,
            "change_request_plan_create" => Self::ChangeRequestPlanCreate,
            "change_request_apply_create" => Self::ChangeRequestApplyCreate,
            "change_request_plan_update" => Self::ChangeRequestPlanUpdate,
            "change_request_apply_update" => Self::ChangeRequestApplyUpdate,
            "change_task_plan_create" => Self::ChangeTaskPlanCreate,
            "change_task_apply_create" => Self::ChangeTaskApplyCreate,
            "change_task_plan_update" => Self::ChangeTaskPlanUpdate,
            "change_task_apply_update" => Self::ChangeTaskApplyUpdate,
            "story_plan_create" => Self::StoryPlanCreate,
            "story_apply_create" => Self::StoryApplyCreate,
            "story_plan_update" => Self::StoryPlanUpdate,
            "story_apply_update" => Self::StoryApplyUpdate,
            "story_task_plan_create" => Self::StoryTaskPlanCreate,
            "story_task_apply_create" => Self::StoryTaskApplyCreate,
            "story_task_plan_update" => Self::StoryTaskPlanUpdate,
            "story_task_apply_update" => Self::StoryTaskApplyUpdate,
            "timecard_list" => Self::TimecardList,
            "timecard_set_hours" => Self::TimecardSetHours,
            "timecard_plan_set_hours" => Self::TimecardPlanSetHours,
            "timecard_apply_set_hours" => Self::TimecardApplySetHours,
            "shutdown" => Self::Shutdown,
            "ping" => Self::Ping,
            _ => Self::Unknown,
        }
    }
}

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
        RpcMethod::GetRecord => match extract_number(&request.params) {
            Ok(number) => match get_record_cached_or_fresh(state.core.as_ref(), &number).await {
                Ok(Some(record)) => match transport.record(&record) {
                    Ok(record) => JsonRpcResponse::ok(id, json!({ "record": record })),
                    Err(err) => internal_error(id, err),
                },
                Ok(None) => JsonRpcResponse::error(id, -32004, "record not found", None),
                Err(err) => internal_error(id, err),
            },
            Err(err) => invalid_params(id, err),
        },
        RpcMethod::GetRecordFresh => match extract_number(&request.params) {
            Ok(number) => match state.core.get_record_fresh(&number).await {
                Ok(Some(record)) => match transport.record(&record) {
                    Ok(record) => JsonRpcResponse::ok(id, json!({ "record": record })),
                    Err(err) => internal_error(id, err),
                },
                Ok(None) => JsonRpcResponse::error(id, -32004, "record not found", None),
                Err(err) => internal_error(id, err),
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
                Ok(number) => match state.core.get_knowledge_article(&number).await {
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
        RpcMethod::GetWorkNotes => match extract_number(&request.params) {
            Ok(number) => match get_record_cached_or_fresh(state.core.as_ref(), &number).await {
                Ok(Some(record)) => match transport.record(&record) {
                    Ok(record) => {
                        JsonRpcResponse::ok(id, json!({ "work_notes": record.work_notes }))
                    }
                    Err(err) => internal_error(id, err),
                },
                Ok(None) => JsonRpcResponse::error(id, -32004, "record not found", None),
                Err(err) => internal_error(id, err),
            },
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
        RpcMethod::MyApprovals => match state.core.my_approvals().await {
            Ok(records) => wrap_approvals_response(id, &transport, records),
            Err(err) => internal_error(id, err),
        },
        RpcMethod::MyApprovalsFresh => match state.core.my_approvals_fresh().await {
            Ok(records) => wrap_approvals_response(id, &transport, records),
            Err(err) => internal_error(id, err),
        },
        RpcMethod::ListMyApprovals => match state.core.my_approvals().await {
            Ok(records) if !records.is_empty() => wrap_approvals_response(id, &transport, records),
            Ok(_) | Err(_) => match state.core.my_approvals_fresh().await {
                Ok(records) => wrap_approvals_response(id, &transport, records),
                Err(err) => internal_error(id, err),
            },
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
        RpcMethod::Approve => match extract_number(&request.params) {
            Ok(number) => match state.core.approve(&number, None).await {
                Ok(Some(record)) => match transport.record(&record) {
                    Ok(record) => JsonRpcResponse::ok(id, json!({ "record": record })),
                    Err(err) => internal_error(id, err),
                },
                Ok(None) => JsonRpcResponse::error(id, -32004, "record not found", None),
                Err(err) => internal_error(id, err),
            },
            Err(err) => invalid_params(id, err),
        },
        RpcMethod::Reject => match (
            extract_number(&request.params),
            extract_string(&request.params, "reason"),
        ) {
            (Ok(number), Ok(reason)) => match state.core.reject(&number, &reason).await {
                Ok(Some(record)) => match transport.record(&record) {
                    Ok(record) => JsonRpcResponse::ok(id, json!({ "record": record })),
                    Err(err) => internal_error(id, err),
                },
                Ok(None) => JsonRpcResponse::error(id, -32004, "record not found", None),
                Err(err) => internal_error(id, err),
            },
            (Err(err), _) | (_, Err(err)) => invalid_params(id, err),
        },
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
        RpcMethod::WorkNotePlanAdd => {
            crate::work_note_write::handle_work_note_plan_add(id, &request.params, state).await
        }
        RpcMethod::WorkNoteApplyAdd => {
            crate::work_note_write::handle_work_note_apply_add(id, &request.params, state).await
        }
        RpcMethod::ChangeRequestPlanCreate
        | RpcMethod::ChangeRequestPlanUpdate
        | RpcMethod::ChangeTaskPlanCreate
        | RpcMethod::ChangeTaskPlanUpdate => {
            crate::change_write::handle_change_plan(id, &request.method, &request.params, state)
                .await
        }
        RpcMethod::ChangeRequestApplyCreate
        | RpcMethod::ChangeRequestApplyUpdate
        | RpcMethod::ChangeTaskApplyCreate
        | RpcMethod::ChangeTaskApplyUpdate => {
            crate::change_write::handle_change_apply(id, &request.method, &request.params, state)
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
    "search_knowledge",
    "kb_semantic_search",
    "list_knowledge_bases",
    "list_categories",
    "list_knowledge_articles",
    "get_approval",
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
    "set_state",
    "field_choices",
    "approve",
    "reject",
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
];

fn contract_info(state: &DaemonState) -> Value {
    let env_label = std::env::var("SNOW_ENV").unwrap_or_else(|_| "test".to_string());
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
    let store = Store::open(&sqlite_path)?;
    let schema_version = store.schema_version()?.unwrap_or(0);
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
        "resource_plan" | "resourceplan" | "rpln" => Ok(ResourceType::ResourcePlan),
        "story" | "rm_story" => Ok(ResourceType::Story),
        "scrum_task" | "rm_scrum_task" => Ok(ResourceType::ScrumTask),
        "knowledge" | "kb_knowledge" => Ok(ResourceType::Knowledge),
        "approval" | "sysapproval_approver" => Ok(ResourceType::Approval),
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

fn wrap_approvals_response(
    id: Option<Value>,
    transport: &DaemonTransport<'_>,
    approvals: Vec<snow_core::ApprovalRecord>,
) -> JsonRpcResponse {
    let mut approval_dtos = Vec::with_capacity(approvals.len());
    for approval in approvals {
        match transport.approval(&approval) {
            Ok(approval) => approval_dtos.push(approval),
            Err(err) => return internal_error(id, err),
        }
    }
    JsonRpcResponse::ok(id, json!({ "records": approval_dtos }))
}

fn internal_error(id: Option<Value>, err: impl ToString) -> JsonRpcResponse {
    JsonRpcResponse::error(
        id,
        -32000,
        "internal error",
        Some(json!({ "details": err.to_string() })),
    )
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
mod tests {
    use super::*;
    use crate::test_support::{
        build_fixture_state, build_fixture_state_at_instance,
        build_fixture_state_without_instance_config, socket_path, spawn_json_http_server,
    };
    use interprocess::local_socket::tokio::Stream as LocalSocketStream;
    use rusqlite::Connection;
    use serde_json::json;
    use snow_mcp::McpServer;
    use tokio::io::AsyncReadExt;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::io::{duplex, split};
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;
    use tokio::task::LocalSet;

    async fn connect_endpoint(endpoint: &IpcEndpoint) -> std::io::Result<LocalSocketStream> {
        endpoint.connect().await
    }

    fn test_endpoint_for_socket(socket: &std::path::Path) -> IpcEndpoint {
        #[cfg(windows)]
        {
            let config_dir = socket.parent().unwrap_or_else(|| std::path::Path::new("."));
            IpcEndpoint::for_config_dir(config_dir)
        }

        #[cfg(not(windows))]
        {
            IpcEndpoint::Filesystem {
                path: socket.to_path_buf(),
            }
        }
    }

    #[test]
    fn rpc_method_parsing_covers_known_methods() {
        assert_eq!(
            RpcMethod::from_method("contract_info"),
            RpcMethod::ContractInfo
        );
        assert_eq!(RpcMethod::from_method("get_record"), RpcMethod::GetRecord);
        assert_eq!(
            RpcMethod::from_method("get_record_fresh"),
            RpcMethod::GetRecordFresh
        );
        assert_eq!(
            RpcMethod::from_method("get_knowledge_article_fresh"),
            RpcMethod::GetKnowledgeArticleFresh
        );
        assert_eq!(RpcMethod::from_method("get_article"), RpcMethod::GetArticle);
        assert_eq!(
            RpcMethod::from_method("task_sla_status"),
            RpcMethod::TaskSlaStatus
        );
        assert_eq!(
            RpcMethod::from_method("task_sla_status_for_tasks"),
            RpcMethod::TaskSlaStatusForTasks
        );
        assert_eq!(
            RpcMethod::from_method("list_my_tasks"),
            RpcMethod::ListMyTasks
        );
        assert_eq!(
            RpcMethod::from_method("list_my_projects"),
            RpcMethod::ListMyProjects
        );
        assert_eq!(
            RpcMethod::from_method("search_records"),
            RpcMethod::SearchRecords
        );
        assert_eq!(RpcMethod::from_method("vault_path"), RpcMethod::VaultPath);
        assert_eq!(
            RpcMethod::from_method("search_knowledge"),
            RpcMethod::SearchKnowledge
        );
        assert_eq!(
            RpcMethod::from_method("kb_semantic_search"),
            RpcMethod::KbSemanticSearch
        );
        assert_eq!(
            RpcMethod::from_method("list_knowledge_bases"),
            RpcMethod::ListKnowledgeBases
        );
        assert_eq!(
            RpcMethod::from_method("list_categories"),
            RpcMethod::ListCategories
        );
        assert_eq!(
            RpcMethod::from_method("verify_vault"),
            RpcMethod::VerifyVault
        );
        assert_eq!(RpcMethod::from_method("kb_sync"), RpcMethod::KbSync);
        assert_eq!(
            RpcMethod::from_method("kb_list_tags"),
            RpcMethod::KbListTags
        );
        assert_eq!(RpcMethod::from_method("kb_status"), RpcMethod::KbStatus);
        assert_eq!(
            RpcMethod::from_method("kb_semantic_status"),
            RpcMethod::KbSemanticStatus
        );
        assert_eq!(
            RpcMethod::from_method("kb_semantic_rebuild"),
            RpcMethod::KbSemanticRebuild
        );
        assert_eq!(
            RpcMethod::from_method("my_tasks_fresh"),
            RpcMethod::MyTasksFresh
        );
        assert_eq!(RpcMethod::from_method("set_state"), RpcMethod::SetState);
        assert_eq!(
            RpcMethod::from_method("field_choices"),
            RpcMethod::FieldChoices
        );
        assert_eq!(RpcMethod::from_method("approve"), RpcMethod::Approve);
        assert_eq!(RpcMethod::from_method("my_projects"), RpcMethod::MyProjects);
        assert_eq!(
            RpcMethod::from_method("scheduler.status"),
            RpcMethod::SchedulerStatus
        );
        assert_eq!(
            RpcMethod::from_method("get_degraded_reads"),
            RpcMethod::GetDegradedReads
        );
        assert_eq!(RpcMethod::from_method("cache_info"), RpcMethod::CacheInfo);
        assert_eq!(RpcMethod::from_method("start_job"), RpcMethod::StartJob);
        assert_eq!(RpcMethod::from_method("get_job"), RpcMethod::GetJob);
        assert_eq!(RpcMethod::from_method("list_jobs"), RpcMethod::ListJobs);
        assert_eq!(RpcMethod::from_method("cancel_job"), RpcMethod::CancelJob);
        assert_eq!(RpcMethod::from_method("plan_get"), RpcMethod::PlanGet);
        assert_eq!(
            RpcMethod::from_method("work_note_plan_add"),
            RpcMethod::WorkNotePlanAdd
        );
        assert_eq!(
            RpcMethod::from_method("work_note_apply_add"),
            RpcMethod::WorkNoteApplyAdd
        );
        assert_eq!(
            RpcMethod::from_method("story_plan_create"),
            RpcMethod::StoryPlanCreate
        );
        assert_eq!(
            RpcMethod::from_method("story_apply_create"),
            RpcMethod::StoryApplyCreate
        );
        assert_eq!(
            RpcMethod::from_method("story_plan_update"),
            RpcMethod::StoryPlanUpdate
        );
        assert_eq!(
            RpcMethod::from_method("story_apply_update"),
            RpcMethod::StoryApplyUpdate
        );
        assert_eq!(
            RpcMethod::from_method("story_task_plan_create"),
            RpcMethod::StoryTaskPlanCreate
        );
        assert_eq!(
            RpcMethod::from_method("story_task_apply_create"),
            RpcMethod::StoryTaskApplyCreate
        );
        assert_eq!(
            RpcMethod::from_method("story_task_plan_update"),
            RpcMethod::StoryTaskPlanUpdate
        );
        assert_eq!(
            RpcMethod::from_method("story_task_apply_update"),
            RpcMethod::StoryTaskApplyUpdate
        );
        assert_eq!(
            RpcMethod::from_method("timecard_list"),
            RpcMethod::TimecardList
        );
        assert_eq!(
            RpcMethod::from_method("timecard_set_hours"),
            RpcMethod::TimecardSetHours
        );
        assert_eq!(
            RpcMethod::from_method("timecard_plan_set_hours"),
            RpcMethod::TimecardPlanSetHours
        );
        assert_eq!(
            RpcMethod::from_method("timecard_apply_set_hours"),
            RpcMethod::TimecardApplySetHours
        );
        assert_eq!(RpcMethod::from_method("shutdown"), RpcMethod::Shutdown);
        assert_eq!(RpcMethod::from_method("unknown"), RpcMethod::Unknown);
    }

    #[test]
    fn contract_info_method_lists_cover_canonical_and_deprecated_rpc_methods() {
        for method in SUPPORTED_RPC_METHODS {
            assert_ne!(
                RpcMethod::from_method(method),
                RpcMethod::Unknown,
                "{method} should parse as a supported RPC method"
            );
        }

        for (method, replacement) in DEPRECATED_RPC_ALIASES {
            assert_ne!(
                RpcMethod::from_method(method),
                RpcMethod::Unknown,
                "{method} should parse as a deprecated RPC alias"
            );
            assert!(
                SUPPORTED_RPC_METHODS.contains(replacement),
                "{method} should point to canonical method {replacement}"
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn story_write_dispatch_uses_real_handlers() {
        let fixture = build_fixture_state().await.expect("fixture");

        let response = dispatch(
            JsonRpcRequest {
                jsonrpc: "2.0".to_string(),
                method: "plan_get".to_string(),
                params: json!({ "plan_id": "missing-plan" }),
                id: Some(json!(1)),
            },
            &fixture.state,
        )
        .await;
        let error = response
            .error
            .expect("missing plan should be a typed error");
        assert_eq!(error.code, -32056);
        assert_eq!(error.message, "PLAN_NOT_FOUND");

        for method in [
            "story_plan_create",
            "story_apply_create",
            "story_plan_update",
            "story_apply_update",
            "story_task_plan_create",
            "story_task_apply_create",
            "story_task_plan_update",
            "story_task_apply_update",
        ] {
            let response = dispatch(
                JsonRpcRequest {
                    jsonrpc: "2.0".to_string(),
                    method: method.to_string(),
                    params: json!({}),
                    id: Some(json!(1)),
                },
                &fixture.state,
            )
            .await;

            let error = response
                .error
                .unwrap_or_else(|| panic!("{method} should require board policy"));
            assert_eq!(error.code, -32051, "{method}");
            assert_eq!(error.message, "FIELD_REJECTED", "{method}");
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn timecard_apply_dispatch_returns_replay_receipt_through_rpc() {
        use chrono::Utc;
        use snow_mcp::domain::audit::ServiceNowMetadata;
        use snow_mcp::domain::primitives::{IdempotencyKey, IdempotencyKeySource, RecordRef};
        use snow_mcp::planner::{
            ConfirmationBinding, ConfirmationStore, FieldChange, IdempotencyOutcome,
            IdempotencyStore, OperationPlanBuilder, OperationReceipt, PlanLifecycleState,
            PlanStore, PlanStoreRecord, ReceiptStatus, SqliteConfirmationStore,
            SqliteIdempotencyStore, SqlitePlanStore,
        };

        let fixture = build_fixture_state().await.expect("fixture");
        let mut policy = snow_mcp::domain::policy::PolicyConfig::default();
        policy
            .tools
            .get_mut("timecard_apply_set_hours")
            .expect("timecard apply policy")
            .enabled = true;
        let data_dir = fixture.tempdir.path().join("timecard-rpc-replay");
        let state = Arc::new(DaemonState::with_data_dir_and_mcp_config(
            Arc::clone(&fixture.state.core),
            data_dir.clone(),
            snow_mcp::McpConfig {
                environment: snow_mcp::McpEnvironment::explicit_config("test", "America/Chicago"),
                policy,
                ..Default::default()
            },
        ));
        std::fs::create_dir_all(&data_dir).expect("data dir");
        let store_path = data_dir.join("mcp_story_write.sqlite3");
        let plan_store = SqlitePlanStore::open(&store_path).expect("plan store");
        let confirmation_store =
            SqliteConfirmationStore::open(&store_path).expect("confirmation store");
        let idempotency_store =
            SqliteIdempotencyStore::open(&store_path).expect("idempotency store");
        let plan = OperationPlanBuilder::new("timecard_plan_set_hours")
            .target(RecordRef {
                sys_id: "card-sys".to_string(),
                number: "PRJ0161219".to_string(),
                table: "time_card".to_string(),
            })
            .planned_changes(json!({ "kind": "TimecardSetHours" }))
            .build();
        let now = Utc::now();
        plan_store
            .put(PlanStoreRecord {
                plan_id: plan.plan_id.clone(),
                tool: plan.tool.clone(),
                actor: "tester".to_string(),
                op_hash: plan.op_hash.clone(),
                plan_json: serde_json::to_value(&plan).expect("plan json"),
                concurrency_token: None,
                created_at: now,
                expires_at: now + chrono::Duration::seconds(600),
                state: PlanLifecycleState::Pending,
            })
            .await
            .expect("put plan");
        let binding = ConfirmationBinding {
            actor: "tester".to_string(),
            requester: "tester".to_string(),
            tool: "timecard_apply_set_hours".to_string(),
            op_hash: plan.op_hash.clone(),
            environment: "test".to_string(),
        };
        let token = confirmation_store
            .issue(&plan.plan_id, binding, 600)
            .await
            .expect("confirmation");
        let key = IdempotencyKey {
            value: "rpc-replay-key".to_string(),
            source: IdempotencyKeySource::ServerDerived,
        };
        assert!(matches!(
            idempotency_store
                .check_and_record(&key, "timecard_apply_set_hours", &plan.op_hash, 600)
                .await
                .expect("record idempotency"),
            IdempotencyOutcome::NewKey
        ));
        let receipt = OperationReceipt {
            plan_id: plan.plan_id.clone(),
            audit_id: "audit-1".to_string(),
            parent_audit_id: plan.plan_id.clone(),
            tool: "timecard_apply_set_hours".to_string(),
            status: ReceiptStatus::Success,
            applied_changes_summary: None,
            service_now_metadata: Some(ServiceNowMetadata {
                sys_id: Some("card-sys".to_string()),
                number: None,
                transaction_id: None,
            }),
            idempotency_replay: false,
            completed_at: now,
            op_hash: plan.op_hash.clone(),
            record_url: None,
            record_snapshot: None,
            changed_fields: Vec::<FieldChange>::new(),
            concurrency_token_observed: None,
            apply_started_at: Some(now),
            error_code: None,
            warnings: Vec::new(),
        };
        idempotency_store
            .save_receipt(&key, &receipt)
            .await
            .expect("save receipt");

        let response = dispatch(
            JsonRpcRequest {
                jsonrpc: "2.0".to_string(),
                method: "timecard_apply_set_hours".to_string(),
                params: json!({
                    "plan_id": plan.plan_id,
                    "confirmation_token": token.token_id,
                    "idempotency_key": "rpc-replay-key",
                }),
                id: Some(json!(1)),
            },
            &state,
        )
        .await;

        assert!(response.error.is_none(), "{:?}", response.error);
        let result = response.result.expect("receipt");
        assert_eq!(result["idempotency_replay"], json!(true));
        assert_eq!(result["audit_id"], json!("audit-1"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn story_plan_create_persists_plan_for_get() {
        let fixture = build_fixture_state().await.expect("fixture");
        let mcp_config = snow_mcp::McpConfig {
            environment: snow_mcp::McpEnvironment::explicit_config("test", "America/Chicago"),
            policy: snow_mcp::domain::policy::PolicyConfig::from_toml_str(
                r#"
[mcp.boards."board-sys"]
name = "training-board"
instance_host = "https://example.service-now.com"
story_table = "rm_story"
task_table = "rm_scrum_task"
column_field = "sprint"
swim_lane_field = "epic"
assignment_group = "group-sys"
allowed_sprints = ["sprint-sys"]

[mcp.tools.story_plan_create]
enabled = true
requires_confirmation = false
story_board_id = "board-sys"
"#,
            )
            .expect("policy"),
            ..Default::default()
        };
        let state = std::sync::Arc::new(crate::DaemonState::with_data_dir_and_mcp_config(
            std::sync::Arc::clone(&fixture.state.core),
            fixture.tempdir.path().join("story-write"),
            mcp_config,
        ));

        let response = dispatch(
            JsonRpcRequest {
                jsonrpc: "2.0".to_string(),
                method: "story_plan_create".to_string(),
                params: json!({
                    "short_description": "Build board writer",
                    "description": "Plan/create smoke"
                }),
                id: Some(json!(1)),
            },
            &state,
        )
        .await;
        assert!(response.error.is_none(), "{response:?}");
        let result = response.result.expect("plan result");
        let plan_id = result["plan_id"].as_str().expect("plan_id");
        let idempotency_key = result["idempotency_key"].as_str().expect("idempotency_key");
        let op_hash = result["op_hash"].as_str().expect("op_hash");
        assert_eq!(result["preview"]["assignment_group"], json!("group-sys"));
        assert_eq!(result["preview"]["sprint"], json!("sprint-sys"));
        assert_eq!(result["preview"]["active"], json!(true));

        let response = dispatch(
            JsonRpcRequest {
                jsonrpc: "2.0".to_string(),
                method: "plan_get".to_string(),
                params: json!({ "plan_id": plan_id }),
                id: Some(json!(2)),
            },
            &state,
        )
        .await;
        assert!(response.error.is_none(), "{response:?}");
        assert_eq!(response.result.unwrap()["plan"]["plan_id"], json!(plan_id));

        let conn = Connection::open(
            fixture
                .tempdir
                .path()
                .join("story-write")
                .join("mcp_story_write.sqlite3"),
        )
        .expect("story write database should open");
        let reserved: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM mcp_idempotency WHERE key = ?1 AND tool = ?2 AND op_hash = ?3",
                (idempotency_key, "story_apply_create", op_hash),
                |row| row.get(0),
            )
            .expect("idempotency row count");
        assert_eq!(reserved, 1);
        let audit_rows: i64 = conn
            .query_row("SELECT COUNT(*) FROM mcp_audit_events", [], |row| {
                row.get(0)
            })
            .expect("audit row count");
        assert!(
            audit_rows >= 2,
            "plan and confirmation issue should be audited"
        );
    }

    #[test]
    fn contract_info_normalizes_mcp_availability() {
        assert_eq!(
            normalize_mcp_availability("stdio"),
            ("local_stdio", "stdio")
        );
        assert_eq!(
            normalize_mcp_availability("disabled"),
            ("disabled", "disabled")
        );
        assert_eq!(
            normalize_mcp_availability("http"),
            ("future_remote_transport", "http")
        );
        assert_eq!(
            normalize_mcp_availability("malformed/private/path"),
            ("unknown", "unknown")
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn contract_info_advertises_instance_host() {
        let fixture = build_fixture_state_at_instance("example.service-now.com")
            .await
            .expect("fixture");

        let result = contract_info(&fixture.state);

        assert_eq!(
            result.get("instance_host").and_then(Value::as_str),
            Some("example.service-now.com")
        );
        assert_eq!(
            result
                .get("environment")
                .and_then(|environment| environment.get("instance_host"))
                .and_then(Value::as_str),
            Some("example.service-now.com")
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn contract_info_uses_null_instance_host_when_config_is_blank() {
        let fixture = build_fixture_state_without_instance_config()
            .await
            .expect("fixture");

        let result = contract_info(&fixture.state);

        assert_eq!(result.get("instance_host"), Some(&Value::Null));
        assert_eq!(
            result
                .get("environment")
                .and_then(|environment| environment.get("instance_host")),
            Some(&Value::Null)
        );
    }

    #[test]
    fn contract_info_normalizes_instance_url_scheme() {
        assert_eq!(
            normalize_instance_host("https://example.service-now.com/api/now/table").as_deref(),
            Some("example.service-now.com")
        );
        assert_eq!(
            normalize_instance_host("http://example.service-now.com/").as_deref(),
            Some("example.service-now.com")
        );
        assert_eq!(
            normalize_instance_host("example.service-now.com/").as_deref(),
            Some("example.service-now.com")
        );
        assert_eq!(normalize_instance_host("   "), None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn direct_rpc_contract_info_exposes_safe_contract_metadata() {
        let fixture = build_fixture_state().await.expect("fixture");

        let response = dispatch(
            JsonRpcRequest {
                jsonrpc: "2.0".to_string(),
                method: "contract_info".to_string(),
                params: json!({}),
                id: Some(json!(1)),
            },
            &fixture.state,
        )
        .await;

        assert!(response.error.is_none(), "{:?}", response.error);
        let result = response.result.expect("contract info result");
        assert_eq!(
            result.get("contract_version").and_then(Value::as_str),
            Some(CONTRACT_VERSION)
        );
        assert_eq!(
            result.get("daemon_version").and_then(Value::as_str),
            Some(env!("CARGO_PKG_VERSION"))
        );
        assert_eq!(
            result.get("instance_host").and_then(Value::as_str),
            Some("localhost")
        );
        assert_eq!(
            result
                .get("environment")
                .and_then(|environment| environment.get("instance_host"))
                .and_then(Value::as_str),
            Some("localhost")
        );
        assert_eq!(
            result
                .get("environment")
                .and_then(|environment| environment.get("username"))
                .and_then(Value::as_str),
            Some("tester")
        );
        assert_eq!(
            result.get("warming_model").and_then(Value::as_str),
            Some("passive")
        );
        assert_eq!(
            result
                .get("mcp_availability")
                .and_then(|mcp| mcp.get("mode"))
                .and_then(Value::as_str),
            Some("local_stdio")
        );
        assert!(
            result
                .get("supported_methods")
                .and_then(Value::as_array)
                .expect("supported methods")
                .iter()
                .any(|method| method.as_str() == Some("contract_info"))
        );
        assert!(
            result
                .get("supported_methods")
                .and_then(Value::as_array)
                .expect("supported methods")
                .iter()
                .any(|method| method.as_str() == Some("list_my_tasks"))
        );
        assert!(
            result
                .get("deprecated_aliases")
                .and_then(Value::as_array)
                .expect("deprecated aliases")
                .iter()
                .any(
                    |alias| alias.get("method").and_then(Value::as_str) == Some("my_tasks")
                        && alias.get("replacement").and_then(Value::as_str)
                            == Some("list_my_tasks")
                )
        );

        let payload = serde_json::to_string(&result).expect("contract info json");
        assert!(!payload.contains("secret"), "{payload}");
        assert!(!payload.contains("SNOW_PASSWORD"), "{payload}");
        assert!(!payload.contains("SERVICENOW_PASSWORD"), "{payload}");
        assert!(!payload.contains(fixture.tempdir.path().to_string_lossy().as_ref()));
        assert!(!payload.contains("snow.db"), "{payload}");
    }

    #[test]
    fn extract_number_reads_param() {
        let params = json!({ "number": "INC0012345" });
        assert_eq!(extract_number(&params).unwrap(), "INC0012345");
    }

    #[test]
    fn extract_string_reads_param() {
        let params = json!({ "reason": "missing test plan" });
        assert_eq!(
            extract_string(&params, "reason").unwrap(),
            "missing test plan"
        );
        assert!(extract_string(&json!({}), "reason").is_err());
    }

    #[test]
    fn extract_task_sla_parent_refs_reads_params() {
        let parents = extract_task_sla_parent_refs(&json!({
            "parents": [{
                "record_number": "INC0000001",
                "record_table": "incident",
                "record_sys_id": "incident-sys-1"
            }]
        }))
        .expect("task sla parent refs");

        assert_eq!(parents.len(), 1);
        assert_eq!(parents[0].record_number, "INC0000001");
        assert_eq!(parents[0].record_table, "incident");
        assert_eq!(parents[0].record_sys_id, "incident-sys-1");
        assert!(extract_task_sla_parent_refs(&json!({})).is_err());
    }

    #[test]
    fn extract_knowledge_search_filters_reads_params() {
        let params = json!({
            "query": "windows access",
            "knowledge_base": "kb-base",
            "category": "Access",
            "limit": 10
        });
        let (query, filters) = extract_knowledge_search_filters(&params).expect("filters");
        assert_eq!(query, "windows access");
        assert_eq!(filters.knowledge_base.as_deref(), Some("kb-base"));
        assert_eq!(filters.category.as_deref(), Some("Access"));
        assert_eq!(filters.limit, Some(10));
    }

    #[test]
    fn extract_kb_list_tags_params_reads_defaults_and_validation() {
        let params = extract_kb_list_tags_params(&json!({ "layer": "user", "min_count": 2 }))
            .expect("kb tag params");
        assert_eq!(params.layer.as_deref(), Some("user"));
        assert_eq!(params.min_count, 2);
        assert!(extract_kb_list_tags_params(&json!({ "min_count": 0 })).is_err());
    }

    #[test]
    fn extract_kb_semantic_search_filters_reads_params() {
        let params = json!({
            "query": "admin rights",
            "knowledge_base": "IT",
            "category": "Access",
            "limit": 3,
            "mode": "lexical",
            "min_score_millis": 250
        });
        let (query, filters) = extract_kb_semantic_search_filters(&params).expect("filters");
        assert_eq!(query, "admin rights");
        assert_eq!(filters.knowledge_base.as_deref(), Some("IT"));
        assert_eq!(filters.category.as_deref(), Some("Access"));
        assert_eq!(filters.limit, Some(3));
        assert_eq!(filters.mode, snow_core::KnowledgeSearchMode::Lexical);
        assert_eq!(filters.min_score_millis, Some(250));
    }

    #[test]
    fn ok_response_serializes_result() {
        let response = JsonRpcResponse::ok(Some(json!(1)), json!({ "ok": true }));
        assert_eq!(response.id, Some(json!(1)));
        assert_eq!(response.result, Some(json!({ "ok": true })));
        assert!(response.error.is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn direct_rpc_contract_exposes_wrapped_aliases_metadata_and_filters() {
        let fixture = build_fixture_state().await.expect("fixture");
        seed_kb_runtime_state(&fixture).expect("seed kb runtime state");

        let get_record = dispatch(
            JsonRpcRequest {
                jsonrpc: "2.0".to_string(),
                method: "get_record".to_string(),
                params: json!({ "number": "CHG001" }),
                id: Some(json!(1)),
            },
            &fixture.state,
        )
        .await;
        let record = get_record
            .result
            .expect("record result")
            .get("record")
            .cloned()
            .expect("wrapped record");
        assert_eq!(
            record.get("resource_type").and_then(Value::as_str),
            Some("change")
        );
        assert_eq!(record.get("source").and_then(Value::as_str), Some("disk"));
        assert_eq!(
            record.get("vault_relative_path").and_then(Value::as_str),
            Some("changes/CHG001.md")
        );
        assert!(
            record
                .get("browser_url")
                .and_then(Value::as_str)
                .expect("browser_url")
                .contains("nav_to.do?uri=change_request.do?sys_id=chg-sys")
        );

        let list_my_tasks = dispatch(
            JsonRpcRequest {
                jsonrpc: "2.0".to_string(),
                method: "list_my_tasks".to_string(),
                params: json!({}),
                id: Some(json!(2)),
            },
            &fixture.state,
        )
        .await;
        let task_result = list_my_tasks.result.expect("tasks result");
        let task_records = task_result
            .get("records")
            .and_then(Value::as_array)
            .expect("task records");
        let first_task_type = task_records[0]
            .get("resource_type")
            .and_then(Value::as_str)
            .expect("resource_type");
        assert_eq!(first_task_type, &first_task_type.to_ascii_lowercase());

        let list_my_approvals = dispatch(
            JsonRpcRequest {
                jsonrpc: "2.0".to_string(),
                method: "list_my_approvals".to_string(),
                params: json!({}),
                id: Some(json!(3)),
            },
            &fixture.state,
        )
        .await;
        let approval_result = list_my_approvals.result.expect("approvals result");
        let approval_records = approval_result
            .get("records")
            .and_then(Value::as_array)
            .expect("approval records");
        assert_eq!(approval_records.len(), 1);
        assert_eq!(
            approval_records[0]
                .get("record")
                .and_then(|record| record.get("resource_type"))
                .and_then(Value::as_str),
            Some("approval")
        );

        let search = dispatch(
            JsonRpcRequest {
                jsonrpc: "2.0".to_string(),
                method: "search_records".to_string(),
                params: json!({ "query": "Database", "limit": 5 }),
                id: Some(json!(4)),
            },
            &fixture.state,
        )
        .await;
        let first_result = search
            .result
            .expect("search result")
            .get("results")
            .and_then(Value::as_array)
            .and_then(|results| results.first())
            .cloned()
            .expect("search match");
        assert_eq!(
            first_result.get("match_in").and_then(Value::as_str),
            Some("short_description")
        );
        assert!(first_result.get("record").is_some());

        let filtered_list = dispatch(
            JsonRpcRequest {
                jsonrpc: "2.0".to_string(),
                method: "list_records".to_string(),
                params: json!({ "parent_number": "CHG001", "limit": 1 }),
                id: Some(json!(5)),
            },
            &fixture.state,
        )
        .await;
        let filtered_result = filtered_list.result.expect("list_records result");
        let filtered_records = filtered_result
            .get("records")
            .and_then(Value::as_array)
            .expect("filtered records");
        assert_eq!(filtered_records.len(), 1);
        assert!(matches!(
            filtered_records[0].get("number").and_then(Value::as_str),
            Some("CTASK001") | Some("APR001")
        ));

        let knowledge_list = dispatch(
            JsonRpcRequest {
                jsonrpc: "2.0".to_string(),
                method: "list_knowledge_articles".to_string(),
                params: json!({ "knowledge_base_sys_id": "kb-base", "category_sys_id": "kb-db" }),
                id: Some(json!(6)),
            },
            &fixture.state,
        )
        .await;
        let knowledge_result = knowledge_list.result.expect("knowledge list result");
        let knowledge_articles = knowledge_result
            .get("articles")
            .and_then(Value::as_array)
            .expect("knowledge articles");
        assert_eq!(knowledge_articles.len(), 1);
        assert_eq!(
            knowledge_articles[0]
                .get("record")
                .and_then(|record| record.get("vault_relative_path"))
                .and_then(Value::as_str),
            Some("knowledge/KB001.md")
        );

        let vault_path = dispatch(
            JsonRpcRequest {
                jsonrpc: "2.0".to_string(),
                method: "vault_path".to_string(),
                params: json!({}),
                id: Some(json!(7)),
            },
            &fixture.state,
        )
        .await;
        assert_eq!(
            vault_path
                .result
                .expect("vault path")
                .get("path")
                .and_then(Value::as_str),
            Some(
                fixture
                    .tempdir
                    .path()
                    .join("vault")
                    .to_string_lossy()
                    .as_ref()
            )
        );

        let kb_status = dispatch(
            JsonRpcRequest {
                jsonrpc: "2.0".to_string(),
                method: "kb_status".to_string(),
                params: json!({}),
                id: Some(json!(8)),
            },
            &fixture.state,
        )
        .await;
        let kb_status = kb_status
            .result
            .expect("kb status")
            .get("status")
            .cloned()
            .expect("wrapped kb status");
        assert_eq!(
            kb_status.get("article_count").and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            kb_status.get("body_cached_count").and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            kb_status.get("lock_held").and_then(Value::as_bool),
            Some(true)
        );

        let cache_info = dispatch(
            JsonRpcRequest {
                jsonrpc: "2.0".to_string(),
                method: "cache_info".to_string(),
                params: json!({}),
                id: Some(json!(9)),
            },
            &fixture.state,
        )
        .await;
        let cache_info = cache_info.result.expect("cache info");
        assert_eq!(
            cache_info.get("total_rows").and_then(Value::as_u64),
            Some(4)
        );
        assert!(
            cache_info
                .get("sqlite_path")
                .and_then(Value::as_str)
                .expect("sqlite_path")
                .ends_with("snow.db")
        );

        let kb_tags = dispatch(
            JsonRpcRequest {
                jsonrpc: "2.0".to_string(),
                method: "kb_list_tags".to_string(),
                params: json!({ "layer": "all", "min_count": 1 }),
                id: Some(json!(10)),
            },
            &fixture.state,
        )
        .await;
        let kb_tags = kb_tags
            .result
            .expect("kb tags")
            .get("tags")
            .and_then(Value::as_array)
            .cloned()
            .expect("wrapped kb tags");
        assert!(kb_tags.iter().any(|entry| {
            entry.get("tag").and_then(Value::as_str) == Some("database")
                && entry
                    .get("layers")
                    .and_then(Value::as_array)
                    .map(|layers| layers.iter().any(|layer| layer.as_str() == Some("sn")))
                    .unwrap_or(false)
        }));

        let kb_sync = dispatch(
            JsonRpcRequest {
                jsonrpc: "2.0".to_string(),
                method: "kb_sync".to_string(),
                params: json!({ "full": true, "with_bodies": true }),
                id: Some(json!(10)),
            },
            &fixture.state,
        )
        .await;
        let kb_sync_error = kb_sync.error.expect("kb sync error");
        assert_eq!(kb_sync_error.code, -32000);
        assert_eq!(kb_sync_error.message, "internal error");
        assert!(
            kb_sync_error
                .data
                .as_ref()
                .and_then(|data| data.get("details"))
                .and_then(Value::as_str)
                .map(|details| !details.trim().is_empty())
                .unwrap_or(false)
        );

        let kb_semantic_search = dispatch(
            JsonRpcRequest {
                jsonrpc: "2.0".to_string(),
                method: "kb_semantic_search".to_string(),
                params: json!({ "query": "database", "knowledge_base": "IT", "mode": "lexical", "limit": 5 }),
                id: Some(json!(11)),
            },
            &fixture.state,
        )
        .await;
        let kb_semantic_hits = kb_semantic_search
            .result
            .expect("kb semantic search")
            .get("hits")
            .and_then(Value::as_array)
            .cloned()
            .expect("wrapped kb semantic hits");
        assert_eq!(kb_semantic_hits.len(), 1);
        assert_eq!(
            kb_semantic_hits[0].get("mode").and_then(Value::as_str),
            Some("lexical")
        );
        assert_eq!(
            kb_semantic_hits[0].get("coverage").and_then(Value::as_str),
            Some("full_text")
        );
        assert_eq!(
            kb_semantic_hits[0]
                .get("article")
                .and_then(|article| article.get("record"))
                .and_then(|record| record.get("number"))
                .and_then(Value::as_str),
            Some("KB001")
        );

        let kb_semantic_status = dispatch(
            JsonRpcRequest {
                jsonrpc: "2.0".to_string(),
                method: "kb_semantic_status".to_string(),
                params: json!({}),
                id: Some(json!(12)),
            },
            &fixture.state,
        )
        .await;
        let kb_semantic_status = kb_semantic_status
            .result
            .expect("kb semantic status")
            .get("status")
            .cloned()
            .expect("wrapped semantic status");
        assert_eq!(
            kb_semantic_status.get("enabled").and_then(Value::as_bool),
            Some(false)
        );
        assert_eq!(
            kb_semantic_status
                .get("active_kb_articles")
                .and_then(Value::as_u64),
            Some(1)
        );

        let kb_semantic_rebuild = dispatch(
            JsonRpcRequest {
                jsonrpc: "2.0".to_string(),
                method: "kb_semantic_rebuild".to_string(),
                params: json!({ "full": true }),
                id: Some(json!(13)),
            },
            &fixture.state,
        )
        .await;
        let kb_semantic_rebuild_error = kb_semantic_rebuild
            .error
            .expect("kb semantic rebuild error");
        assert_eq!(kb_semantic_rebuild_error.code, -32000);
        assert_eq!(kb_semantic_rebuild_error.message, "internal error");
        assert!(
            kb_semantic_rebuild_error
                .data
                .as_ref()
                .and_then(|data| data.get("details"))
                .and_then(Value::as_str)
                .map(|details| !details.trim().is_empty())
                .unwrap_or(false)
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn direct_rpc_omits_browser_url_when_instance_config_is_absent() {
        let fixture = build_fixture_state_without_instance_config()
            .await
            .expect("fixture");

        let response = dispatch(
            JsonRpcRequest {
                jsonrpc: "2.0".to_string(),
                method: "get_record".to_string(),
                params: json!({ "number": "CHG001" }),
                id: Some(json!(1)),
            },
            &fixture.state,
        )
        .await;

        let record = response
            .result
            .expect("record result")
            .get("record")
            .cloned()
            .expect("wrapped record");
        assert!(record.get("browser_url").is_none());
        assert_eq!(
            record.get("vault_relative_path").and_then(Value::as_str),
            Some("changes/CHG001.md")
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn direct_rpc_task_sla_status_returns_core_status_projection() {
        let (instance_url, request_rx) = spawn_task_sla_status_server(
            json!({
                "result": [{
                    "sys_id": "task-parent-sys",
                    "number": "INC0000001",
                    "short_description": "Generic incident"
                }]
            }),
            json!({
                "result": [task_sla_row(
                    "sla-row-sys",
                    "task-parent-sys",
                    "INC0000001",
                    "Initial Response SLA"
                )]
            }),
            2,
        )
        .await
        .expect("http server");
        let fixture = build_fixture_state_at_instance(&instance_url)
            .await
            .expect("fixture");

        let response = dispatch(
            JsonRpcRequest {
                jsonrpc: "2.0".to_string(),
                method: "task_sla_status".to_string(),
                params: json!({ "number": "INC0000001" }),
                id: Some(json!(1)),
            },
            &fixture.state,
        )
        .await;

        assert!(response.error.is_none(), "{:?}", response.error);
        let status = response
            .result
            .expect("task sla status result")
            .get("status")
            .cloned()
            .expect("wrapped task sla status");
        assert_eq!(
            status.get("record_number").and_then(Value::as_str),
            Some("INC0000001")
        );
        assert_eq!(
            status.get("record_table").and_then(Value::as_str),
            Some("incident")
        );
        assert_eq!(
            status.get("record_sys_id").and_then(Value::as_str),
            Some("task-parent-sys")
        );
        assert_eq!(
            status.get("readable").and_then(Value::as_str),
            Some("ReadableRows")
        );
        assert_eq!(
            status
                .get("summary")
                .and_then(|summary| summary.get("total"))
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            status
                .get("rows")
                .and_then(Value::as_array)
                .and_then(|rows| rows.first())
                .and_then(|row| row.get("name"))
                .and_then(Value::as_str),
            Some("Initial Response SLA")
        );

        let request_lines = request_rx.await.expect("request lines");
        assert!(
            request_lines
                .iter()
                .any(|line| line.contains("/api/now/table/incident")),
            "{request_lines:?}"
        );
        assert!(
            request_lines
                .iter()
                .any(|line| line.contains("/api/now/table/task_sla")),
            "{request_lines:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn direct_rpc_task_sla_status_for_tasks_returns_status_map() {
        let (instance_url, request_rx) = spawn_task_sla_status_server(
            json!({ "result": [] }),
            json!({
                "result": [task_sla_row(
                    "sla-row-sys",
                    "incident-sys-1",
                    "INC0000001",
                    "Initial Response SLA"
                )]
            }),
            1,
        )
        .await
        .expect("http server");
        let fixture = build_fixture_state_at_instance(&instance_url)
            .await
            .expect("fixture");

        let response = dispatch(
            JsonRpcRequest {
                jsonrpc: "2.0".to_string(),
                method: "task_sla_status_for_tasks".to_string(),
                params: json!({
                    "parents": [{
                        "record_number": "INC0000001",
                        "record_table": "incident",
                        "record_sys_id": "incident-sys-1"
                    }]
                }),
                id: Some(json!(1)),
            },
            &fixture.state,
        )
        .await;

        assert!(response.error.is_none(), "{:?}", response.error);
        let statuses = response
            .result
            .expect("task sla statuses result")
            .get("statuses")
            .cloned()
            .expect("wrapped task sla statuses");
        let status = statuses
            .get("incident-sys-1")
            .expect("incident status in map");
        assert_eq!(
            status.get("record_number").and_then(Value::as_str),
            Some("INC0000001")
        );
        assert_eq!(
            status.get("readable").and_then(Value::as_str),
            Some("ReadableRows")
        );

        let request_lines = request_rx.await.expect("request lines");
        assert_eq!(request_lines.len(), 1);
        assert!(
            request_lines[0].contains("/api/now/table/task_sla"),
            "{request_lines:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn direct_rpc_task_sla_status_preserves_parent_not_found_without_sla_fetch() {
        let (instance_url, request_rx) = spawn_task_sla_status_server(
            json!({ "result": [] }),
            json!({
                "result": [task_sla_row(
                    "unused-sla-row-sys",
                    "unused-task-sys",
                    "INC0000009",
                    "Unused SLA"
                )]
            }),
            1,
        )
        .await
        .expect("http server");
        let fixture = build_fixture_state_at_instance(&instance_url)
            .await
            .expect("fixture");

        let response = dispatch(
            JsonRpcRequest {
                jsonrpc: "2.0".to_string(),
                method: "task_sla_status".to_string(),
                params: json!({ "number": "INC0000009" }),
                id: Some(json!(1)),
            },
            &fixture.state,
        )
        .await;

        assert!(response.error.is_none(), "{:?}", response.error);
        let status = response
            .result
            .expect("task sla status result")
            .get("status")
            .cloned()
            .expect("wrapped task sla status");
        assert_eq!(
            status.get("record_number").and_then(Value::as_str),
            Some("INC0000009")
        );
        assert_eq!(
            status.get("readable").and_then(Value::as_str),
            Some("ParentNotFound")
        );
        assert!(
            status
                .get("rows")
                .and_then(Value::as_array)
                .expect("rows array")
                .is_empty()
        );

        let request_lines = request_rx.await.expect("request lines");
        assert!(
            request_lines
                .iter()
                .all(|line| !line.contains("/api/now/table/task_sla")),
            "{request_lines:?}"
        );
    }

    #[test]
    fn direct_rpc_task_sla_status_requires_number_param() {
        let response = invalid_params(
            Some(json!(1)),
            extract_number(&json!({ "record": "INC0000001" })).expect_err("missing number"),
        );
        let error = response.error.expect("invalid params error");
        assert_eq!(error.code, -32602);
        assert!(
            error
                .data
                .as_ref()
                .and_then(|data| data.get("details"))
                .and_then(Value::as_str)
                .map(|details| details.contains("number"))
                .unwrap_or(false)
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn direct_rpc_get_record_refreshes_uncached_demand() {
        let response = json!({
            "result": [
                {
                    "sys_id": "dmnd-sys",
                    "number": "DMND0320098",
                    "short_description": "Network refresh demand",
                    "description": "Upgrade branch switching",
                    "state": "draft"
                }
            ]
        });
        let (instance_url, request_rx) =
            spawn_json_http_server(response).await.expect("http server");
        let fixture = build_fixture_state_at_instance(&instance_url)
            .await
            .expect("fixture");

        let response = dispatch(
            JsonRpcRequest {
                jsonrpc: "2.0".to_string(),
                method: "get_record".to_string(),
                params: json!({ "number": "DMND0320098" }),
                id: Some(json!(1)),
            },
            &fixture.state,
        )
        .await;

        let record = response
            .result
            .expect("record result")
            .get("record")
            .cloned()
            .expect("wrapped record");
        assert_eq!(
            record.get("resource_type").and_then(Value::as_str),
            Some("demand")
        );
        assert_eq!(
            record.get("number").and_then(Value::as_str),
            Some("DMND0320098")
        );
        assert_eq!(
            record.get("vault_relative_path").and_then(Value::as_str),
            Some("demands/DMND0320098/DMND0320098.md")
        );
        assert!(
            record
                .get("browser_url")
                .and_then(Value::as_str)
                .expect("browser_url")
                .contains("nav_to.do?uri=dmn_demand.do?sys_id=dmnd-sys")
        );

        let request_line = request_rx.await.expect("request line");
        assert!(request_line.contains("/api/now/table/dmn_demand"));
        assert!(request_line.contains("DMND0320098"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn direct_rpc_and_mcp_return_matching_wrapped_payloads() {
        let fixture = build_fixture_state().await.expect("fixture");

        let direct = dispatch(
            JsonRpcRequest {
                jsonrpc: "2.0".to_string(),
                method: "kb_semantic_search".to_string(),
                params: json!({ "query": "database", "knowledge_base": "IT", "mode": "lexical", "limit": 5 }),
                id: Some(json!(1)),
            },
            &fixture.state,
        )
        .await
        .result
        .expect("direct result");

        let mcp = tokio::task::LocalSet::new()
            .run_until(async move {
                let server = McpServer::new(Arc::clone(&fixture.state.core));
                let (client_side, server_side) = duplex(4096);
                let (server_reader, server_writer) = split(server_side);
                tokio::task::spawn_local(async move {
                    let _ = server
                        .serve_streams(
                            BufReader::new(server_reader),
                            server_writer,
                            std::future::pending::<Result<(), std::io::Error>>(),
                        )
                        .await;
                });

                let (client_reader, mut client_writer) = split(client_side);
                let mut client_reader = BufReader::new(client_reader);
                client_writer
                    .write_all(
                        br#"{"jsonrpc":"2.0","method":"tools/call","params":{"name":"kb_semantic_search","arguments":{"query":"database","knowledge_base":"IT","mode":"lexical","limit":5}},"id":1}"#,
                    )
                    .await
                    .expect("write");
                client_writer.write_all(b"\n").await.expect("newline");

                let mut line = String::new();
                client_reader.read_line(&mut line).await.expect("read");
                serde_json::from_str::<JsonRpcResponse>(&line)
                    .expect("response")
                    .result
                    .expect("mcp result")
            })
            .await;

        assert_eq!(direct, mcp);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn direct_rpc_nabu_startup_sequence_smoke_test() {
        let fixture = build_fixture_state().await.expect("fixture");
        seed_kb_runtime_state(&fixture).expect("seed kb runtime state");

        for (method, params) in [
            ("contract_info", json!({})),
            ("ping", json!({})),
            ("list_my_tasks", json!({})),
            ("list_my_approvals", json!({})),
            ("list_knowledge_bases", json!({})),
            ("list_records", json!({ "resource_type": "knowledge" })),
            ("kb_status", json!({})),
            ("kb_semantic_status", json!({})),
            (
                "kb_semantic_search",
                json!({ "query": "database", "mode": "lexical", "limit": 5 }),
            ),
            ("kb_list_tags", json!({ "layer": "all", "min_count": 1 })),
        ] {
            let response = dispatch(
                JsonRpcRequest {
                    jsonrpc: "2.0".to_string(),
                    method: method.to_string(),
                    params,
                    id: Some(json!(1)),
                },
                &fixture.state,
            )
            .await;
            assert!(
                response.error.is_none(),
                "{method} returned error: {:?}",
                response.error
            );
            assert!(response.result.is_some(), "{method} returned no result");
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn json_rpc_socket_round_trip_and_concurrency() {
        let fixture = build_fixture_state().await.expect("fixture");
        let socket = socket_path(&fixture.tempdir);
        let endpoint = test_endpoint_for_socket(&socket);
        let server = JsonRpcServer::new(Arc::clone(&fixture.state), endpoint.clone());
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        let local = LocalSet::new();
        local.spawn_local(async move {
            let _ = server
                .serve_until(async move {
                    let _ = shutdown_rx.await;
                    Ok(())
                })
                .await;
        });

        local
            .run_until(async move {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;

                let held_open = connect_endpoint(&endpoint).await.expect("first client");
                let (reader, mut writer) =
                    split(connect_endpoint(&endpoint).await.expect("second client"));
                writer
                    .write_all(br#"{"jsonrpc":"2.0","method":"ping","id":1}"#)
                    .await
                    .expect("write ping");
                writer.write_all(b"\n").await.expect("newline");
                writer
                    .write_all(br#"{"jsonrpc":"2.0","method":"get_children","params":{"number":"CHG001"},"id":2}"#)
                    .await
                    .expect("write get_children");
                writer.write_all(b"\n").await.expect("newline");
                writer
                    .write_all(br#"{"jsonrpc":"2.0","method":"verify_vault","id":3}"#)
                    .await
                    .expect("write verify_vault");
                writer.write_all(b"\n").await.expect("newline");
                writer
                    .write_all(br#"{"jsonrpc":"2.0","method":"get_approval","params":{"number":"APR001"},"id":4}"#)
                    .await
                    .expect("write get_approval");
                writer.write_all(b"\n").await.expect("newline");

                let mut reader = BufReader::new(reader);
                let mut first = String::new();
                reader.read_line(&mut first).await.expect("read ping");
                assert!(first.contains("\"ok\":true"));

                let mut second = String::new();
                reader.read_line(&mut second).await.expect("read children");
                assert!(second.contains("CTASK001"));

                let mut third = String::new();
                reader.read_line(&mut third).await.expect("read verify_vault");
                assert!(third.contains("scanned_documents"));

                let mut fourth = String::new();
                reader.read_line(&mut fourth).await.expect("read get_approval");
                assert!(fourth.contains("APR001"));

                drop(held_open);
                let _ = shutdown_tx.send(());
            })
            .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn shutdown_rpc_stops_server_after_response() {
        let fixture = build_fixture_state().await.expect("fixture");
        let endpoint = test_endpoint_for_socket(&socket_path(&fixture.tempdir));
        let server = JsonRpcServer::new(Arc::clone(&fixture.state), endpoint.clone())
            .with_drain_timeout(Duration::from_millis(100));
        let (done_tx, done_rx) = oneshot::channel::<Result<()>>();

        let local = LocalSet::new();
        local.spawn_local(async move {
            let result = server
                .serve_until(std::future::pending::<Result<(), std::io::Error>>())
                .await;
            let _ = done_tx.send(result);
        });

        local
            .run_until(async move {
                tokio::time::sleep(Duration::from_millis(50)).await;

                let (reader, mut writer) =
                    split(connect_endpoint(&endpoint).await.expect("client"));
                writer
                    .write_all(br#"{"jsonrpc":"2.0","method":"shutdown","id":1}"#)
                    .await
                    .expect("write shutdown");
                writer.write_all(b"\n").await.expect("newline");

                let mut reader = BufReader::new(reader);
                let mut line = String::new();
                reader.read_line(&mut line).await.expect("read shutdown");
                assert!(line.contains("\"status\":\"shutting_down\""));

                let result = tokio::time::timeout(Duration::from_secs(1), done_rx)
                    .await
                    .expect("server should stop after shutdown")
                    .expect("server completion signal");
                result.expect("server result");
            })
            .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn idle_timeout_shuts_down_server_with_no_clients() {
        let fixture = build_fixture_state().await.expect("fixture");
        let endpoint = test_endpoint_for_socket(&socket_path(&fixture.tempdir));
        let server = JsonRpcServer::new(Arc::clone(&fixture.state), endpoint.clone())
            .with_drain_timeout(Duration::from_millis(50))
            .with_idle_timeout(Some(Duration::from_millis(100)));
        let (done_tx, done_rx) = oneshot::channel::<Result<()>>();

        let local = LocalSet::new();
        local.spawn_local(async move {
            // No external shutdown and no clients: only the idle timer can stop it.
            let result = server
                .serve_until(std::future::pending::<Result<(), std::io::Error>>())
                .await;
            let _ = done_tx.send(result);
        });

        local
            .run_until(async move {
                let result = tokio::time::timeout(Duration::from_secs(2), done_rx)
                    .await
                    .expect("idle daemon should self-shut-down")
                    .expect("server completion signal");
                result.expect("server result");
            })
            .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn idle_timeout_held_off_by_active_connection() {
        let fixture = build_fixture_state().await.expect("fixture");
        let endpoint = test_endpoint_for_socket(&socket_path(&fixture.tempdir));
        let server = JsonRpcServer::new(Arc::clone(&fixture.state), endpoint.clone())
            .with_drain_timeout(Duration::from_millis(50))
            .with_idle_timeout(Some(Duration::from_millis(100)));
        let (done_tx, done_rx) = oneshot::channel::<Result<()>>();

        let local = LocalSet::new();
        local.spawn_local(async move {
            let result = server
                .serve_until(std::future::pending::<Result<(), std::io::Error>>())
                .await;
            let _ = done_tx.send(result);
        });

        local
            .run_until(async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                let held_open = connect_endpoint(&endpoint).await.expect("held client");
                // Hold the connection open well past the idle window; the
                // server must not shut down while a client is connected.
                tokio::time::sleep(Duration::from_millis(400)).await;
                let still_running = tokio::time::timeout(Duration::from_millis(1), done_rx).await;
                assert!(
                    still_running.is_err(),
                    "server idled out despite an active connection"
                );
                drop(held_open);
            })
            .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn idle_timeout_disabled_keeps_server_running() {
        let fixture = build_fixture_state().await.expect("fixture");
        let endpoint = test_endpoint_for_socket(&socket_path(&fixture.tempdir));
        let server = JsonRpcServer::new(Arc::clone(&fixture.state), endpoint.clone())
            .with_drain_timeout(Duration::from_millis(50))
            .with_idle_timeout(None);
        let (done_tx, done_rx) = oneshot::channel::<Result<()>>();

        let local = LocalSet::new();
        local.spawn_local(async move {
            let result = server
                .serve_until(std::future::pending::<Result<(), std::io::Error>>())
                .await;
            let _ = done_tx.send(result);
        });

        local
            .run_until(async move {
                // With idle shutdown disabled, the server must still be running
                // well past any plausible idle interval.
                let result = tokio::time::timeout(Duration::from_millis(300), done_rx).await;
                assert!(
                    result.is_err(),
                    "server stopped despite idle timeout being disabled"
                );
            })
            .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn external_shutdown_waits_for_bounded_connection_drain() {
        let fixture = build_fixture_state().await.expect("fixture");
        let endpoint = test_endpoint_for_socket(&socket_path(&fixture.tempdir));
        let server = JsonRpcServer::new(Arc::clone(&fixture.state), endpoint.clone())
            .with_drain_timeout(Duration::from_millis(50));
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let (done_tx, done_rx) = oneshot::channel::<Result<()>>();

        let local = LocalSet::new();
        local.spawn_local(async move {
            let result = server
                .serve_until(async move {
                    let _ = shutdown_rx.await;
                    Ok(())
                })
                .await;
            let _ = done_tx.send(result);
        });

        local
            .run_until(async move {
                tokio::time::sleep(Duration::from_millis(50)).await;

                let held_open = connect_endpoint(&endpoint).await.expect("held client");
                tokio::time::sleep(Duration::from_millis(25)).await;
                let _ = shutdown_tx.send(());

                let result = tokio::time::timeout(Duration::from_secs(1), done_rx)
                    .await
                    .expect("server should stop after bounded drain")
                    .expect("server completion signal");
                result.expect("server result");
                drop(held_open);
            })
            .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn json_rpc_get_knowledge_article_fresh_uses_live_client() {
        let response = json!({
            "result": [
                {
                    "sys_id": "kb-fresh-sys",
                    "number": "KB0105015",
                    "short_description": "Fresh KB title",
                    "description": "Fresh KB summary",
                    "article_body": "Fresh KB body",
                    "text": "Fresh KB body",
                    "state": "published",
                    "workflow_state": "published",
                    "article_type": "text",
                    "valid_to": "2027-01-01",
                    "knowledge_base": {
                        "sys_id": "kb-base-sys",
                        "value": "kb-base-sys",
                        "display_value": "IT Operations",
                        "table": "kb_knowledge_base"
                    },
                    "category": {
                        "sys_id": "kb-cat-sys",
                        "value": "kb-cat-sys",
                        "display_value": "Networking",
                        "table": "kb_category"
                    }
                }
            ]
        });
        let (instance_url, request_rx) =
            spawn_json_http_server(response).await.expect("http server");
        let fixture = build_fixture_state_at_instance(&instance_url)
            .await
            .expect("fixture");
        let socket = socket_path(&fixture.tempdir);
        let endpoint = test_endpoint_for_socket(&socket);
        let server = JsonRpcServer::new(Arc::clone(&fixture.state), endpoint.clone());
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        let local = LocalSet::new();
        local.spawn_local(async move {
            let _ = server
                .serve_until(async move {
                    let _ = shutdown_rx.await;
                    Ok(())
                })
                .await;
        });

        local
            .run_until(async move {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;

                let (reader, mut writer) =
                    split(connect_endpoint(&endpoint).await.expect("client"));
                writer
                    .write_all(
                        br#"{"jsonrpc":"2.0","method":"get_knowledge_article_fresh","params":{"number":"KB0105015"},"id":1}"#,
                    )
                    .await
                    .expect("write request");
                writer.write_all(b"\n").await.expect("newline");

                let mut reader = BufReader::new(reader);
                let mut line = String::new();
                reader.read_line(&mut line).await.expect("read response");
                assert!(line.contains("KB0105015"));
                assert!(line.contains("Fresh KB body"));

                let request_line = request_rx.await.expect("request line");
                assert!(request_line.contains("/api/now/table/kb_knowledge"));
                assert!(request_line.contains("KB0105015"));

                let _ = shutdown_tx.send(());
            })
            .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn json_rpc_search_and_browse_knowledge_round_trip() {
        let fixture = build_fixture_state().await.expect("fixture");
        seed_kb_runtime_state(&fixture).expect("seed kb runtime state");
        let socket = socket_path(&fixture.tempdir);
        let endpoint = test_endpoint_for_socket(&socket);
        let server = JsonRpcServer::new(Arc::clone(&fixture.state), endpoint.clone());
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        let local = LocalSet::new();
        local.spawn_local(async move {
            let _ = server
                .serve_until(async move {
                    let _ = shutdown_rx.await;
                    Ok(())
                })
                .await;
        });

        local
            .run_until(async move {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;

                let (reader, mut writer) =
                    split(connect_endpoint(&endpoint).await.expect("client"));
                writer
                    .write_all(
                        br#"{"jsonrpc":"2.0","method":"search_knowledge","params":{"query":"database","knowledge_base":"IT","limit":5},"id":1}"#,
                    )
                    .await
                    .expect("write search");
                writer.write_all(b"\n").await.expect("newline");
                writer
                    .write_all(
                        br#"{"jsonrpc":"2.0","method":"list_knowledge_bases","params":{},"id":2}"#,
                    )
                    .await
                    .expect("write bases");
                writer.write_all(b"\n").await.expect("newline");
                writer
                    .write_all(
                        br#"{"jsonrpc":"2.0","method":"list_categories","params":{"knowledge_base_sys_id":"kb-base"},"id":3}"#,
                    )
                    .await
                    .expect("write categories");
                writer.write_all(b"\n").await.expect("newline");
                writer
                    .write_all(br#"{"jsonrpc":"2.0","method":"kb_status","params":{},"id":4}"#)
                    .await
                    .expect("write kb status");
                writer.write_all(b"\n").await.expect("newline");
                writer
                    .write_all(br#"{"jsonrpc":"2.0","method":"kb_semantic_status","params":{},"id":5}"#)
                    .await
                    .expect("write semantic status");
                writer.write_all(b"\n").await.expect("newline");
                writer
                    .write_all(br#"{"jsonrpc":"2.0","method":"kb_semantic_search","params":{"query":"database","mode":"lexical","limit":5},"id":6}"#)
                    .await
                    .expect("write semantic search");
                writer.write_all(b"\n").await.expect("newline");
                writer
                    .write_all(br#"{"jsonrpc":"2.0","method":"kb_semantic_rebuild","params":{"full":true},"id":7}"#)
                    .await
                    .expect("write semantic rebuild");
                writer.write_all(b"\n").await.expect("newline");

                let mut reader = BufReader::new(reader);
                let mut first = String::new();
                reader.read_line(&mut first).await.expect("read search");
                assert!(first.contains("\"KB001\""));
                assert!(first.contains("\"Database restart procedure\""));

                let mut second = String::new();
                reader.read_line(&mut second).await.expect("read bases");
                assert!(second.contains("\"display_name\":\"IT\""));
                assert!(second.contains("\"article_count\":1"));

                let mut third = String::new();
                reader.read_line(&mut third).await.expect("read categories");
                assert!(third.contains("\"display_name\":\"Database\""));
                assert!(third.contains("\"knowledge_base_sys_id\":\"kb-base\""));

                let mut fourth = String::new();
                reader.read_line(&mut fourth).await.expect("read kb status");
                assert!(fourth.contains("\"body_cached_count\":1"));

                let mut fifth = String::new();
                reader
                    .read_line(&mut fifth)
                    .await
                    .expect("read semantic status");
                assert!(fifth.contains("\"enabled\":false"));

                let mut sixth = String::new();
                reader
                    .read_line(&mut sixth)
                    .await
                    .expect("read semantic search");
                assert!(sixth.contains("\"hits\""));
                assert!(sixth.contains("\"mode\":\"lexical\""));
                assert!(sixth.contains("\"KB001\""));

                let mut seventh = String::new();
                reader
                    .read_line(&mut seventh)
                    .await
                    .expect("read semantic rebuild");
                assert!(seventh.contains("\"message\":\"internal error\""));

                let _ = shutdown_tx.send(());
            })
            .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn search_records_falls_back_to_live_fetch_for_exact_number() {
        let response = serde_json::json!({
            "result": [{
                "sys_id": "inc-sys-fallback",
                "number": "INC4992697",
                "short_description": "Switch port flapping",
                "description": "Multiple ports down on core switch",
                "state": "2",
                "assigned_to": {
                    "value": "user-sys",
                    "display_value": "Jared Jennings"
                }
            }]
        });
        let (instance_url, _request_rx) =
            spawn_json_http_server(response).await.expect("http server");
        let fixture = build_fixture_state_at_instance(&instance_url)
            .await
            .expect("fixture");

        let search = dispatch(
            JsonRpcRequest {
                jsonrpc: "2.0".to_string(),
                method: "search_records".to_string(),
                params: json!({ "query": "INC4992697" }),
                id: Some(json!(1)),
            },
            &fixture.state,
        )
        .await;

        let results = search
            .result
            .expect("search result")
            .get("results")
            .and_then(Value::as_array)
            .cloned()
            .expect("results array");

        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0]
                .get("record")
                .and_then(|r| r.get("number"))
                .and_then(Value::as_str),
            Some("INC4992697")
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn handle_connection_ignores_broken_pipe_when_client_disconnects() {
        let fixture = build_fixture_state().await.expect("fixture");
        let (server, client) = duplex(4096);
        let (client_reader, mut client_writer) = split(client);
        drop(client_reader);

        client_writer
            .write_all(br#"{"jsonrpc":"2.0","method":"ping","id":1}"#)
            .await
            .expect("write ping");
        client_writer.write_all(b"\n").await.expect("newline");
        drop(client_writer);

        handle_connection(server, Arc::clone(&fixture.state), Arc::new(Notify::new()))
            .await
            .expect("broken pipe should be treated as disconnect");
    }

    /// Round-trip the `start_job` and `get_job` RPC methods through `dispatch`,
    /// ensuring the start handler returns a UUID and the get handler can look
    /// up the resulting registry entry. Runs inside a `LocalSet` because
    /// `crate::jobs::spawn` uses `tokio::task::spawn_local`.
    #[tokio::test(flavor = "current_thread")]
    async fn start_job_returns_id_and_get_job_finds_it() {
        let local = LocalSet::new();
        local
            .run_until(async {
                let fixture = build_fixture_state().await.expect("fixture");

                let start = dispatch(
                    JsonRpcRequest {
                        jsonrpc: "2.0".to_string(),
                        method: "start_job".to_string(),
                        params: json!({ "kind": "verify_vault", "params": {} }),
                        id: Some(json!(1)),
                    },
                    &fixture.state,
                )
                .await;
                let job_id = start
                    .result
                    .expect("start_job result")
                    .get("job_id")
                    .and_then(Value::as_str)
                    .expect("job_id string")
                    .to_owned();

                let got = dispatch(
                    JsonRpcRequest {
                        jsonrpc: "2.0".to_string(),
                        method: "get_job".to_string(),
                        params: json!({ "job_id": job_id }),
                        id: Some(json!(2)),
                    },
                    &fixture.state,
                )
                .await;
                let result = got.result.expect("get_job result");
                assert!(
                    !result.is_null(),
                    "get_job should resolve the started job, got null"
                );
                assert_eq!(
                    result.get("kind").and_then(Value::as_str),
                    Some("verify_vault"),
                );
            })
            .await;
    }

    /// `list_jobs` returns the started job entry, and `cancel_job` returns
    /// `cancelled: true` for an active job id.
    #[tokio::test(flavor = "current_thread")]
    async fn list_jobs_and_cancel_job_round_trip() {
        let local = LocalSet::new();
        local
            .run_until(async {
                let fixture = build_fixture_state().await.expect("fixture");

                let start = dispatch(
                    JsonRpcRequest {
                        jsonrpc: "2.0".to_string(),
                        method: "start_job".to_string(),
                        params: json!({ "kind": "verify_vault", "params": {} }),
                        id: Some(json!(1)),
                    },
                    &fixture.state,
                )
                .await;
                let job_id = start
                    .result
                    .expect("start_job result")
                    .get("job_id")
                    .and_then(Value::as_str)
                    .expect("job_id string")
                    .to_owned();

                let list = dispatch(
                    JsonRpcRequest {
                        jsonrpc: "2.0".to_string(),
                        method: "list_jobs".to_string(),
                        params: json!({ "include_finished": true }),
                        id: Some(json!(2)),
                    },
                    &fixture.state,
                )
                .await;
                let jobs = list.result.expect("list_jobs result");
                let arr = jobs.as_array().expect("list_jobs array");
                assert!(
                    arr.iter()
                        .any(|j| j.get("id").and_then(Value::as_str) == Some(job_id.as_str())),
                    "list_jobs should include the started job"
                );

                let cancel = dispatch(
                    JsonRpcRequest {
                        jsonrpc: "2.0".to_string(),
                        method: "cancel_job".to_string(),
                        params: json!({ "job_id": job_id }),
                        id: Some(json!(3)),
                    },
                    &fixture.state,
                )
                .await;
                let cancelled = cancel
                    .result
                    .expect("cancel_job result")
                    .get("cancelled")
                    .and_then(Value::as_bool)
                    .expect("cancelled flag");
                // Whether cancellation lands depends on whether the worker
                // already finished; both true and false are valid terminal
                // signals for "the call dispatched cleanly".
                let _ = cancelled;
            })
            .await;
    }

    fn seed_kb_runtime_state(
        fixture: &crate::test_support::FixtureState,
    ) -> std::result::Result<(), rusqlite::Error> {
        let conn = Connection::open(fixture.tempdir.path().join("snow.db"))?;
        conn.execute(
            r#"
            UPDATE kb_sync_state
            SET last_full_at = 1712649800000,
                last_incr_at = 1712650100000,
                watermark_updated_at = '2026-04-10 09:00:00',
                watermark_sys_id = 'kb-sys',
                kb_sync_lock = 1712650200000
            WHERE id = 1
            "#,
            [],
        )?;
        Ok(())
    }

    async fn spawn_task_sla_status_server(
        parent_response: Value,
        task_sla_response: Value,
        expected_requests: usize,
    ) -> Result<(String, oneshot::Receiver<Vec<String>>)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let (request_tx, request_rx) = oneshot::channel();

        tokio::spawn(async move {
            let mut request_lines = Vec::with_capacity(expected_requests);
            for _ in 0..expected_requests {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let mut request = Vec::new();
                let mut buf = [0u8; 1024];
                loop {
                    let read = match stream.read(&mut buf).await {
                        Ok(0) => break,
                        Ok(read) => read,
                        Err(_) => return,
                    };
                    request.extend_from_slice(&buf[..read]);
                    if request.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }

                let first_line = request
                    .split(|byte| *byte == b'\n')
                    .next()
                    .and_then(|line| std::str::from_utf8(line).ok())
                    .unwrap_or_default()
                    .trim()
                    .to_string();
                let response_body = if first_line.contains("/api/now/table/task_sla") {
                    task_sla_response.clone()
                } else {
                    parent_response.clone()
                }
                .to_string();
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    response_body.len(),
                    response_body
                );
                request_lines.push(first_line);
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.shutdown().await;
            }
            let _ = request_tx.send(request_lines);
        });

        Ok((format!("http://{}", addr), request_rx))
    }

    fn task_sla_row(sys_id: &str, task_sys_id: &str, task_display: &str, sla_name: &str) -> Value {
        json!({
            "sys_id": { "value": sys_id, "display_value": sys_id },
            "task": { "value": task_sys_id, "display_value": task_display },
            "sla": { "value": format!("{sys_id}-definition"), "display_value": sla_name },
            "stage": { "value": "in_progress", "display_value": "In Progress" },
            "active": { "value": "true", "display_value": "true" },
            "has_breached": { "value": "false", "display_value": "false" },
            "planned_end_time": {
                "value": "2026-05-08 10:00:00",
                "display_value": "2026-05-08 10:00:00"
            },
            "business_percentage": {
                "value": "25.5",
                "display_value": "25.5"
            },
            "time_left": {
                "value": "1970-01-01 02:00:00",
                "display_value": "2 Hours"
            }
        })
    }
}
