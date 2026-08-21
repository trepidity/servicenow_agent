use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use snow_core::{
    ApprovalRecord, ApprovalRoutedVia, AttachmentMetadata, CacheSource, FieldChoice, FieldValue,
    JournalEntry, KnowledgeArticle, KnowledgeBaseSummary, KnowledgeCategorySummary,
    KnowledgeEmbeddingCoverage, KnowledgeSearchFilters, KnowledgeSearchHit, KnowledgeSearchMode,
    KnowledgeSemanticSearchFilters, KnowledgeSemanticStatus, MatchField, RecordRef, Reference,
    ResourceType, SearchMatchReason, SemanticIndexSummary, SnowCore, SnowRecord, TaskSlaParentRef,
    TaskSlaStatus,
    ipc::IpcEndpoint,
    resource::timecard::{SetMode, TimeCard, TimeValue, TimecardSheet, WeekSelector, Weekday},
};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};

use crate::error::SnowError;

#[derive(Clone)]
pub enum TuiClient {
    Local(Arc<SnowCore>),
    Remote(Arc<DaemonRpcClient>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TuiRuntimeIdentity {
    pub mode: String,
    pub username: String,
}

impl TuiClient {
    pub fn local(core: Arc<SnowCore>) -> Self {
        Self::Local(core)
    }

    #[allow(dead_code)]
    pub fn remote(socket_path: PathBuf, instance_url: Option<String>) -> Self {
        Self::Remote(Arc::new(DaemonRpcClient::new(socket_path, instance_url)))
    }

    pub fn remote_endpoint_with_auto_spawn(
        endpoint: IpcEndpoint,
        instance_url: Option<String>,
        env_name: String,
    ) -> Self {
        Self::Remote(Arc::new(DaemonRpcClient::with_endpoint_auto_spawn(
            endpoint,
            instance_url,
            env_name,
        )))
    }

    pub fn is_remote(&self) -> bool {
        matches!(self, Self::Remote(_))
    }

    pub async fn runtime_identity(
        &self,
        fallback_username: Option<&str>,
    ) -> Result<TuiRuntimeIdentity, SnowError> {
        match self {
            Self::Local(core) => Ok(TuiRuntimeIdentity {
                mode: "local".to_string(),
                username: core.config().instance.user.clone(),
            }),
            Self::Remote(client) => {
                let username = client
                    .daemon_username()
                    .await
                    .ok()
                    .flatten()
                    .or_else(|| fallback_username.map(ToOwned::to_owned))
                    .unwrap_or_else(|| "unknown".to_string());
                Ok(TuiRuntimeIdentity {
                    mode: "daemon".to_string(),
                    username,
                })
            }
        }
    }

    pub fn as_local_core(&self) -> Option<&SnowCore> {
        match self {
            Self::Local(core) => Some(core.as_ref()),
            Self::Remote(_) => None,
        }
    }

    pub fn browser_url_by_id(&self, table: &str, sys_id: &str) -> Result<String, SnowError> {
        match self {
            Self::Local(core) => core
                .client()
                .browser_url_by_id(table, sys_id)
                .map_err(SnowError::from),
            Self::Remote(client) => client.browser_url_by_id(table, sys_id),
        }
    }

    pub async fn get_record(&self, number: &str) -> Result<Option<SnowRecord>, SnowError> {
        match self {
            Self::Local(core) => core.get_record(number).await.map_err(SnowError::from),
            Self::Remote(client) => client.get_record(number).await,
        }
    }

    pub async fn get_record_fresh(&self, number: &str) -> Result<Option<SnowRecord>, SnowError> {
        match self {
            Self::Local(core) => core.get_record_fresh(number).await.map_err(SnowError::from),
            Self::Remote(client) => client.get_record_fresh(number).await,
        }
    }

    pub async fn get_record_by_table_sys_id_fresh(
        &self,
        table: &str,
        sys_id: &str,
    ) -> Result<Option<SnowRecord>, SnowError> {
        match self {
            Self::Local(core) => core
                .get_record_by_table_sys_id_fresh(table, sys_id)
                .await
                .map_err(SnowError::from),
            Self::Remote(client) => client.get_record_by_table_sys_id_fresh(table, sys_id).await,
        }
    }

    #[allow(dead_code)]
    pub async fn task_sla_status(&self, number: &str) -> Result<TaskSlaStatus, SnowError> {
        match self {
            Self::Local(core) => core
                .task_sla_status_for_number(number)
                .await
                .map_err(SnowError::from),
            Self::Remote(client) => client.task_sla_status(number).await,
        }
    }

    pub async fn task_sla_status_for_tasks(
        &self,
        parents: &[TaskSlaParentRef],
    ) -> Result<HashMap<String, TaskSlaStatus>, SnowError> {
        match self {
            Self::Local(core) => core
                .task_sla_status_for_tasks(parents)
                .await
                .map_err(SnowError::from),
            Self::Remote(client) => client.task_sla_status_for_tasks(parents).await,
        }
    }

    #[allow(dead_code)]
    pub async fn get_knowledge_article(
        &self,
        number: &str,
    ) -> Result<Option<KnowledgeArticle>, SnowError> {
        match self {
            Self::Local(core) => core
                .get_knowledge_article(number)
                .await
                .map_err(SnowError::from),
            Self::Remote(client) => client.get_knowledge_article(number).await,
        }
    }

    pub async fn get_knowledge_article_fresh(
        &self,
        number: &str,
    ) -> Result<Option<KnowledgeArticle>, SnowError> {
        match self {
            Self::Local(core) => core
                .get_knowledge_article_fresh(number)
                .await
                .map_err(SnowError::from),
            Self::Remote(client) => client.get_knowledge_article_fresh(number).await,
        }
    }

    pub async fn search_knowledge(
        &self,
        query: &str,
        filters: KnowledgeSearchFilters,
    ) -> Result<Vec<KnowledgeArticle>, SnowError> {
        match self {
            Self::Local(core) => core
                .search_knowledge(query, filters)
                .await
                .map_err(SnowError::from),
            Self::Remote(client) => client.search_knowledge(query, filters).await,
        }
    }

    #[allow(dead_code)]
    pub async fn search_knowledge_semantic(
        &self,
        query: &str,
        filters: KnowledgeSemanticSearchFilters,
    ) -> Result<Vec<KnowledgeSearchHit>, SnowError> {
        match self {
            Self::Local(core) => core
                .search_knowledge_semantic(query, filters)
                .await
                .map_err(SnowError::from),
            Self::Remote(client) => client.search_knowledge_semantic(query, filters).await,
        }
    }

    #[allow(dead_code)]
    pub async fn knowledge_semantic_status(&self) -> Result<KnowledgeSemanticStatus, SnowError> {
        match self {
            Self::Local(core) => core
                .knowledge_semantic_status()
                .await
                .map_err(SnowError::from),
            Self::Remote(client) => client.knowledge_semantic_status().await,
        }
    }

    #[allow(dead_code)]
    pub async fn rebuild_knowledge_semantic_index(
        &self,
        full: bool,
    ) -> Result<SemanticIndexSummary, SnowError> {
        match self {
            Self::Local(core) => core
                .rebuild_knowledge_semantic_index(full)
                .await
                .map_err(SnowError::from),
            Self::Remote(client) => client.rebuild_knowledge_semantic_index(full).await,
        }
    }

    pub async fn list_knowledge_bases(&self) -> Result<Vec<KnowledgeBaseSummary>, SnowError> {
        match self {
            Self::Local(core) => core.list_knowledge_bases().map_err(SnowError::from),
            Self::Remote(client) => client.list_knowledge_bases().await,
        }
    }

    pub async fn list_categories(
        &self,
        knowledge_base_sys_id: &str,
    ) -> Result<Vec<KnowledgeCategorySummary>, SnowError> {
        match self {
            Self::Local(core) => core
                .list_categories(knowledge_base_sys_id)
                .map_err(SnowError::from),
            Self::Remote(client) => client.list_categories(knowledge_base_sys_id).await,
        }
    }

    pub async fn get_children(&self, number: &str) -> Result<Vec<SnowRecord>, SnowError> {
        match self {
            Self::Local(core) => core.get_children(number).await.map_err(SnowError::from),
            Self::Remote(client) => client.get_children(number).await,
        }
    }

    pub async fn my_approvals_fresh(&self) -> Result<Vec<ApprovalRecord>, SnowError> {
        match self {
            Self::Local(core) => core.my_approvals_fresh().await.map_err(SnowError::from),
            Self::Remote(client) => client.my_approvals_fresh().await,
        }
    }

    pub async fn my_tasks_fresh(&self) -> Result<Vec<SnowRecord>, SnowError> {
        match self {
            Self::Local(core) => core.my_tasks_fresh().await.map_err(SnowError::from),
            Self::Remote(client) => client.my_tasks_fresh().await,
        }
    }

    pub async fn my_stories_fresh(&self) -> Result<Vec<SnowRecord>, SnowError> {
        match self {
            Self::Local(core) => core.my_stories_fresh().await.map_err(SnowError::from),
            Self::Remote(client) => client.my_stories_fresh().await,
        }
    }

    pub async fn my_incidents_fresh(&self) -> Result<Vec<SnowRecord>, SnowError> {
        match self {
            Self::Local(core) => core.my_incidents_fresh().await.map_err(SnowError::from),
            Self::Remote(client) => client.my_incidents_fresh().await,
        }
    }

    #[allow(dead_code)]
    pub async fn timecard_list(&self, week: WeekSelector) -> Result<TimecardSheet, SnowError> {
        match self {
            Self::Local(core) => core.list_my_timecards(week).await.map_err(SnowError::from),
            Self::Remote(client) => client.timecard_list(week).await,
        }
    }

    #[allow(dead_code)]
    pub async fn timecard_set_hours(
        &self,
        sys_id: &str,
        day: Weekday,
        hours: &str,
        mode: SetMode,
    ) -> Result<TimeCard, SnowError> {
        match self {
            Self::Local(core) => {
                let value = parse_time_value(hours)?;
                core.set_timecard_hours(sys_id, day, value, mode)
                    .await
                    .map_err(SnowError::from)
            }
            Self::Remote(client) => client.timecard_set_hours(sys_id, day, hours, mode).await,
        }
    }

    pub async fn add_work_note(
        &self,
        number: &str,
        message: &str,
    ) -> Result<Option<SnowRecord>, SnowError> {
        match self {
            Self::Local(core) => core
                .add_work_note(number, message)
                .await
                .map_err(SnowError::from),
            Self::Remote(client) => client.add_work_note(number, message).await,
        }
    }

    #[allow(dead_code)]
    pub async fn list_attachments(
        &self,
        number: &str,
    ) -> Result<Option<Vec<AttachmentMetadata>>, SnowError> {
        match self {
            Self::Local(core) => core.list_attachments(number).await.map_err(SnowError::from),
            Self::Remote(client) => client.list_attachments(number).await,
        }
    }

    #[allow(dead_code)]
    pub async fn upload_attachment_file(
        &self,
        number: &str,
        path: &Path,
        file_name: Option<&str>,
        content_type: Option<&str>,
    ) -> Result<Option<AttachmentMetadata>, SnowError> {
        match self {
            Self::Local(core) => core
                .upload_attachment_file(number, path, file_name, content_type)
                .await
                .map_err(SnowError::from),
            Self::Remote(client) => {
                client
                    .upload_attachment_file(number, path, file_name, content_type)
                    .await
            }
        }
    }

    pub async fn set_state(
        &self,
        number: &str,
        state: &str,
    ) -> Result<Option<SnowRecord>, SnowError> {
        match self {
            Self::Local(core) => core.set_state(number, state).await.map_err(SnowError::from),
            Self::Remote(client) => client.set_state(number, state).await,
        }
    }

    pub async fn field_choices(
        &self,
        table: &str,
        field: &str,
    ) -> Result<Vec<FieldChoice>, SnowError> {
        match self {
            Self::Local(core) => core
                .field_choices(table, field)
                .await
                .map_err(SnowError::from),
            Self::Remote(client) => client.field_choices(table, field).await,
        }
    }

    pub async fn approve(&self, number: &str) -> Result<Option<SnowRecord>, SnowError> {
        match self {
            Self::Local(core) => core.approve(number, None).await.map_err(SnowError::from),
            Self::Remote(client) => client.approve(number).await,
        }
    }

    pub async fn reject(
        &self,
        number: &str,
        reason: &str,
    ) -> Result<Option<SnowRecord>, SnowError> {
        match self {
            Self::Local(core) => core.reject(number, reason).await.map_err(SnowError::from),
            Self::Remote(client) => client.reject(number, reason).await,
        }
    }

    pub async fn pending_approval_for(
        &self,
        target_sys_id: &str,
    ) -> Result<Option<ApprovalRecord>, SnowError> {
        let approvals = self.my_approvals_fresh().await?;
        Ok(approvals
            .into_iter()
            .find(|approval| approval.target.sys_id == target_sys_id))
    }

    pub async fn batch_children(
        &self,
        parents: &[SnowRecord],
    ) -> Result<Vec<(String, Vec<SnowRecord>)>, SnowError> {
        let mut groups = Vec::with_capacity(parents.len());
        for parent in parents {
            groups.push((
                parent.sys_id.clone(),
                self.get_children(&parent.number).await?,
            ));
        }
        Ok(groups)
    }
}

#[derive(Clone)]
pub struct DaemonRpcClient {
    endpoint: IpcEndpoint,
    instance_url: Option<String>,
    auto_spawn_env: Option<String>,
    next_id: Arc<AtomicU64>,
}

impl DaemonRpcClient {
    pub fn new(socket_path: PathBuf, instance_url: Option<String>) -> Self {
        let endpoint = IpcEndpoint::Filesystem { path: socket_path };
        Self {
            endpoint,
            instance_url,
            auto_spawn_env: None,
            next_id: Arc::new(AtomicU64::new(1)),
        }
    }

    pub fn with_endpoint_auto_spawn(
        endpoint: IpcEndpoint,
        instance_url: Option<String>,
        env_name: String,
    ) -> Self {
        Self {
            endpoint,
            instance_url,
            auto_spawn_env: Some(env_name),
            next_id: Arc::new(AtomicU64::new(1)),
        }
    }

    pub async fn get_record(&self, number: &str) -> Result<Option<SnowRecord>, SnowError> {
        let record: Option<DaemonSnowRecord> = self
            .call_optional_wrapped("get_record", Some(json!({ "number": number })), "record")
            .await?;
        Ok(record.map(Into::into))
    }

    pub async fn get_record_fresh(&self, number: &str) -> Result<Option<SnowRecord>, SnowError> {
        let record: Option<DaemonSnowRecord> = self
            .call_optional_wrapped(
                "get_record_fresh",
                Some(json!({ "number": number })),
                "record",
            )
            .await?;
        Ok(record.map(Into::into))
    }

    pub async fn get_record_by_table_sys_id_fresh(
        &self,
        table: &str,
        sys_id: &str,
    ) -> Result<Option<SnowRecord>, SnowError> {
        let record: Option<DaemonSnowRecord> = self
            .call_optional_wrapped(
                "get_record",
                Some(json!({ "table": table, "sys_id": sys_id })),
                "record",
            )
            .await?;
        Ok(record.map(Into::into))
    }

    #[allow(dead_code)]
    pub async fn task_sla_status(&self, number: &str) -> Result<TaskSlaStatus, SnowError> {
        self.call_wrapped(
            "task_sla_status",
            Some(json!({ "number": number })),
            "status",
        )
        .await
    }

    pub async fn task_sla_status_for_tasks(
        &self,
        parents: &[TaskSlaParentRef],
    ) -> Result<HashMap<String, TaskSlaStatus>, SnowError> {
        self.call_wrapped(
            "task_sla_status_for_tasks",
            Some(json!({ "parents": parents })),
            "statuses",
        )
        .await
    }

    #[allow(dead_code)]
    pub async fn get_knowledge_article(
        &self,
        number: &str,
    ) -> Result<Option<KnowledgeArticle>, SnowError> {
        let article: Option<DaemonKnowledgeArticle> = self
            .call_optional_wrapped(
                "get_knowledge_article",
                Some(json!({ "number": number })),
                "article",
            )
            .await?;
        Ok(article.map(Into::into))
    }

    pub async fn get_knowledge_article_fresh(
        &self,
        number: &str,
    ) -> Result<Option<KnowledgeArticle>, SnowError> {
        let article: Option<DaemonKnowledgeArticle> = self
            .call_optional_wrapped(
                "get_knowledge_article_fresh",
                Some(json!({ "number": number })),
                "article",
            )
            .await?;
        Ok(article.map(Into::into))
    }

    pub async fn search_knowledge(
        &self,
        query: &str,
        filters: KnowledgeSearchFilters,
    ) -> Result<Vec<KnowledgeArticle>, SnowError> {
        let articles: Vec<DaemonKnowledgeArticle> = self
            .call_wrapped(
                "search_knowledge",
                Some(json!({
                    "query": query,
                    "knowledge_base": filters.knowledge_base,
                    "category": filters.category,
                    "limit": filters.limit
                })),
                "articles",
            )
            .await?;
        Ok(articles.into_iter().map(Into::into).collect())
    }

    pub async fn search_knowledge_semantic(
        &self,
        query: &str,
        filters: KnowledgeSemanticSearchFilters,
    ) -> Result<Vec<KnowledgeSearchHit>, SnowError> {
        let hits: Vec<DaemonKnowledgeSearchHit> = self
            .call_wrapped(
                "kb_semantic_search",
                Some(json!({
                    "query": query,
                    "knowledge_base": filters.knowledge_base,
                    "category": filters.category,
                    "limit": filters.limit,
                    "mode": filters.mode,
                    "min_score_millis": filters.min_score_millis,
                })),
                "hits",
            )
            .await?;
        Ok(hits.into_iter().map(Into::into).collect())
    }

    pub async fn knowledge_semantic_status(&self) -> Result<KnowledgeSemanticStatus, SnowError> {
        let status: DaemonKnowledgeSemanticStatus = self
            .call_wrapped("kb_semantic_status", None, "status")
            .await?;
        Ok(status.into())
    }

    pub async fn rebuild_knowledge_semantic_index(
        &self,
        full: bool,
    ) -> Result<SemanticIndexSummary, SnowError> {
        let summary: DaemonSemanticIndexSummary = self
            .call_wrapped(
                "kb_semantic_rebuild",
                Some(json!({ "full": full })),
                "summary",
            )
            .await?;
        Ok(summary.into())
    }

    /// Get one Business Application by sys_id or exact name.
    ///
    /// `fresh` selects the live `business_application_get_fresh` variant over the
    /// cache-first `business_application_get`. Returns `None` when the daemon
    /// reports the record is missing.
    pub async fn business_application_get(
        &self,
        sys_id: Option<&str>,
        name: Option<&str>,
        fresh: bool,
    ) -> Result<Option<BusinessApplicationView>, SnowError> {
        let method = if fresh {
            "business_application_get_fresh"
        } else {
            "business_application_get"
        };
        let mut params = serde_json::Map::new();
        if let Some(sys_id) = sys_id {
            params.insert("sys_id".to_string(), json!(sys_id));
        }
        if let Some(name) = name {
            params.insert("name".to_string(), json!(name));
        }
        let app: Option<DaemonBusinessApplication> = self
            .call_optional_wrapped(method, Some(Value::Object(params)), "business_application")
            .await?;
        Ok(app.map(Into::into))
    }

    /// Search Business Applications by name and operational-state exclusion.
    pub async fn business_application_search(
        &self,
        name: Option<&str>,
        operational_state_not: Option<&str>,
        limit: Option<usize>,
    ) -> Result<Vec<BusinessApplicationView>, SnowError> {
        let mut params = serde_json::Map::new();
        if let Some(name) = name {
            params.insert("name".to_string(), json!(name));
        }
        if let Some(state) = operational_state_not {
            params.insert("operational_state_not".to_string(), json!(state));
        }
        if let Some(limit) = limit {
            params.insert("limit".to_string(), json!(limit));
        }
        let apps: Vec<DaemonBusinessApplication> = self
            .call_wrapped(
                "business_application_search",
                Some(Value::Object(params)),
                "business_applications",
            )
            .await?;
        Ok(apps.into_iter().map(Into::into).collect())
    }

    /// Run a local all-field query. Returns the raw daemon JSON so the CLI stays
    /// decoupled from the backend's evolving record/projection shape.
    pub async fn business_application_query(
        &self,
        filters: &[BusinessApplicationQueryFilter],
        limit: Option<usize>,
    ) -> Result<Value, SnowError> {
        self.business_application_query_with_args(BusinessApplicationQueryArgs {
            text: None,
            filters,
            limit,
        })
        .await
    }

    /// Run a Business Application query with optional text, filters, and limit.
    pub async fn business_application_query_with_args(
        &self,
        args: BusinessApplicationQueryArgs<'_>,
    ) -> Result<Value, SnowError> {
        let params = business_application_query_params(args);
        self.business_application_query_with_params(params).await
    }

    /// Run a paged Business Application query against the local daemon cache.
    pub async fn business_application_query_page_with_args(
        &self,
        args: BusinessApplicationQueryPageArgs<'_>,
    ) -> Result<Value, SnowError> {
        let params = business_application_query_page_params(args);
        self.business_application_query_with_params(params).await
    }

    async fn business_application_query_with_params(
        &self,
        params: serde_json::Map<String, Value>,
    ) -> Result<Value, SnowError> {
        // The daemon may key results under "business_applications" or "records";
        // unwrap either, falling back to the whole payload.
        let raw: Value = self
            .call_wrapped(
                "business_application_query",
                Some(Value::Object(params)),
                "business_applications",
            )
            .await?;
        Ok(raw)
    }

    /// List the dictionary-backed Business Application fields. Returns the raw
    /// JSON array; the CLI renders it generically because the backend owns and
    /// may enrich the per-field shape.
    pub async fn business_application_fields(&self, refresh: bool) -> Result<Value, SnowError> {
        let params = if refresh {
            Some(json!({ "refresh_dictionary": true }))
        } else {
            None
        };
        self.call_wrapped("business_application_fields", params, "fields")
            .await
    }

    /// Return server CIs associated with a Business Application. The backend
    /// owns the evolving result shape, so callers receive the raw daemon result.
    pub async fn business_application_servers(
        &self,
        args: BusinessApplicationServersArgs<'_>,
    ) -> Result<Value, SnowError> {
        let params = business_application_servers_params(args);
        let response = self
            .send_request("business_application_servers", Some(Value::Object(params)))
            .await?;
        response
            .result
            .ok_or_else(|| SnowError::Api("daemon response missing result".to_string()))
    }

    /// Return cached server relationships for one Business Application. The
    /// backend owns the evolving result shape, so callers receive raw JSON.
    pub async fn business_application_servers_cached(
        &self,
        args: BusinessApplicationServersCachedArgs<'_>,
    ) -> Result<Value, SnowError> {
        let params = business_application_servers_cached_params(args);
        let response = self
            .send_request(
                "business_application_servers_cached",
                Some(Value::Object(params)),
            )
            .await?;
        response
            .result
            .ok_or_else(|| SnowError::Api("daemon response missing result".to_string()))
    }

    /// Return cached Business Application relationships for one Server. The
    /// backend owns the evolving result shape, so callers receive raw JSON.
    pub async fn business_applications_for_server(
        &self,
        args: BusinessApplicationsForServerArgs<'_>,
    ) -> Result<Value, SnowError> {
        let params = business_applications_for_server_params(args);
        let response = self
            .send_request(
                "business_applications_for_server",
                Some(Value::Object(params)),
            )
            .await?;
        response
            .result
            .ok_or_else(|| SnowError::Api("daemon response missing result".to_string()))
    }

    pub async fn server_get(
        &self,
        sys_id: Option<&str>,
        name: Option<&str>,
        ip_address: Option<&str>,
        fresh: bool,
    ) -> Result<Option<ServerView>, SnowError> {
        let method = if fresh {
            "server_get_fresh"
        } else {
            "server_get"
        };
        let mut params = serde_json::Map::new();
        if let Some(sys_id) = sys_id {
            params.insert("sys_id".to_string(), json!(sys_id));
        }
        if let Some(name) = name {
            params.insert("name".to_string(), json!(name));
        }
        if let Some(ip_address) = ip_address {
            params.insert("ip_address".to_string(), json!(ip_address));
        }
        let server: Option<DaemonServer> = self
            .call_optional_wrapped(method, Some(Value::Object(params)), "server")
            .await?;
        Ok(server.map(Into::into))
    }

    pub async fn server_search(
        &self,
        args: ServerQueryArgs<'_>,
    ) -> Result<Vec<ServerView>, SnowError> {
        let params = server_params(args);
        let servers: Vec<DaemonServer> = self
            .call_wrapped("server_search", Some(Value::Object(params)), "servers")
            .await?;
        Ok(servers.into_iter().map(Into::into).collect())
    }

    pub async fn server_query(
        &self,
        args: ServerQueryArgs<'_>,
    ) -> Result<Vec<ServerView>, SnowError> {
        let params = server_params(args);
        let servers: Vec<DaemonServer> = self
            .call_wrapped("server_query", Some(Value::Object(params)), "servers")
            .await?;
        Ok(servers.into_iter().map(Into::into).collect())
    }

    pub async fn server_fields(&self) -> Result<Value, SnowError> {
        self.call_wrapped("server_fields", None, "fields").await
    }

    /// Fetch the Incident typed-resource descriptor envelope.
    ///
    /// Returns the daemon result verbatim rather than unwrapping a field. The
    /// envelope's `operation`, `source`, and `completeness` are part of the
    /// contract every transport must report identically, so unwrapping here
    /// would make the CLI the one transport that reports something different.
    pub async fn incident_fields(&self) -> Result<Value, SnowError> {
        let response = self.send_request("incident_fields", None).await?;
        response
            .result
            .ok_or_else(|| SnowError::Api("daemon response missing result".to_string()))
    }

    pub async fn incident_get(&self, params: Value) -> Result<Value, SnowError> {
        let response = self.send_request("incident_get", Some(params)).await?;
        response
            .result
            .ok_or_else(|| SnowError::Api("daemon response missing result".to_string()))
    }

    pub async fn incident_query(&self, params: Value) -> Result<Value, SnowError> {
        let response = self.send_request("incident_query", Some(params)).await?;
        response
            .result
            .ok_or_else(|| SnowError::Api("daemon response missing result".to_string()))
    }

    /// Sync Business Applications into the local vault/cache with optional
    /// hydration. Returns the raw summary object; the CLI renders it generically.
    pub async fn business_application_sync(
        &self,
        name: Option<&str>,
        operational_state_not: Option<&str>,
        persist: bool,
        resolve_references: bool,
        reference_depth: Option<u32>,
        refresh_dictionary: bool,
    ) -> Result<Value, SnowError> {
        self.business_application_sync_with_args(BusinessApplicationSyncArgs {
            all: false,
            name,
            operational_state_not,
            persist,
            resolve_references,
            reference_depth,
            refresh_dictionary,
        })
        .await
    }

    /// Sync Business Applications with the full request shape understood by RPC.
    pub async fn business_application_sync_with_args(
        &self,
        args: BusinessApplicationSyncArgs<'_>,
    ) -> Result<Value, SnowError> {
        let params = business_application_sync_params(args);
        self.call_wrapped(
            "business_application_sync",
            Some(Value::Object(params)),
            "summary",
        )
        .await
    }

    pub async fn list_knowledge_bases(&self) -> Result<Vec<KnowledgeBaseSummary>, SnowError> {
        self.call_wrapped("list_knowledge_bases", None, "bases")
            .await
    }

    pub async fn list_categories(
        &self,
        knowledge_base_sys_id: &str,
    ) -> Result<Vec<KnowledgeCategorySummary>, SnowError> {
        self.call_wrapped(
            "list_categories",
            Some(json!({ "knowledge_base_sys_id": knowledge_base_sys_id })),
            "categories",
        )
        .await
    }

    pub async fn get_children(&self, number: &str) -> Result<Vec<SnowRecord>, SnowError> {
        let records: Vec<DaemonSnowRecord> = self
            .call_wrapped("get_children", Some(json!({ "number": number })), "records")
            .await?;
        Ok(records.into_iter().map(Into::into).collect())
    }

    pub async fn my_approvals_fresh(&self) -> Result<Vec<ApprovalRecord>, SnowError> {
        let approvals: Vec<DaemonApprovalRecord> = self
            .call_wrapped("my_approvals_fresh", None, "records")
            .await?;
        Ok(approvals.into_iter().map(Into::into).collect())
    }

    pub async fn my_tasks_fresh(&self) -> Result<Vec<SnowRecord>, SnowError> {
        let records: Vec<DaemonSnowRecord> =
            self.call_wrapped("my_tasks_fresh", None, "records").await?;
        Ok(records.into_iter().map(Into::into).collect())
    }

    pub async fn my_stories_fresh(&self) -> Result<Vec<SnowRecord>, SnowError> {
        let records: Vec<DaemonSnowRecord> = self
            .call_wrapped("my_stories_fresh", None, "records")
            .await?;
        Ok(records.into_iter().map(Into::into).collect())
    }

    pub async fn my_incidents_fresh(&self) -> Result<Vec<SnowRecord>, SnowError> {
        let records: Vec<DaemonSnowRecord> = self
            .call_wrapped("my_incidents_fresh", None, "records")
            .await?;
        Ok(records.into_iter().map(Into::into).collect())
    }

    #[allow(dead_code)]
    pub async fn timecard_list(&self, week: WeekSelector) -> Result<TimecardSheet, SnowError> {
        self.call_wrapped("timecard_list", Some(week_selector_params(week)), "sheet")
            .await
    }

    #[allow(dead_code)]
    pub async fn timecard_set_hours(
        &self,
        sys_id: &str,
        day: Weekday,
        hours: &str,
        mode: SetMode,
    ) -> Result<TimeCard, SnowError> {
        self.call_wrapped(
            "timecard_set_hours",
            Some(json!({
                "sys_id": sys_id,
                "day": weekday_token(day),
                "hours": hours,
                "mode": set_mode_token(mode),
            })),
            "card",
        )
        .await
    }

    pub async fn add_work_note(
        &self,
        number: &str,
        message: &str,
    ) -> Result<Option<SnowRecord>, SnowError> {
        let record: Option<DaemonSnowRecord> = self
            .call_optional_wrapped(
                "add_work_note",
                Some(json!({ "number": number, "text": message })),
                "record",
            )
            .await?;
        Ok(record.map(Into::into))
    }

    #[allow(dead_code)]
    pub async fn list_attachments(
        &self,
        number: &str,
    ) -> Result<Option<Vec<AttachmentMetadata>>, SnowError> {
        self.call_optional_wrapped(
            "attachment_list",
            Some(json!({ "number": number })),
            "attachments",
        )
        .await
    }

    #[allow(dead_code)]
    pub async fn upload_attachment_file(
        &self,
        number: &str,
        path: &Path,
        file_name: Option<&str>,
        content_type: Option<&str>,
    ) -> Result<Option<AttachmentMetadata>, SnowError> {
        self.call_optional_wrapped(
            "attachment_upload",
            Some(json!({
                "number": number,
                "path": path.display().to_string(),
                "file_name": file_name,
                "content_type": content_type,
            })),
            "attachment",
        )
        .await
    }

    pub async fn set_state(
        &self,
        number: &str,
        state: &str,
    ) -> Result<Option<SnowRecord>, SnowError> {
        let record: Option<DaemonSnowRecord> = self
            .call_optional_wrapped(
                "set_state",
                Some(json!({ "number": number, "state": state })),
                "record",
            )
            .await?;
        Ok(record.map(Into::into))
    }

    pub async fn field_choices(
        &self,
        table: &str,
        field: &str,
    ) -> Result<Vec<FieldChoice>, SnowError> {
        self.call_wrapped(
            "field_choices",
            Some(json!({ "table": table, "field": field })),
            "choices",
        )
        .await
    }

    pub async fn approve(&self, number: &str) -> Result<Option<SnowRecord>, SnowError> {
        let record: Option<DaemonSnowRecord> = self
            .call_optional_wrapped("approve", Some(json!({ "number": number })), "record")
            .await?;
        Ok(record.map(Into::into))
    }

    pub async fn reject(
        &self,
        number: &str,
        reason: &str,
    ) -> Result<Option<SnowRecord>, SnowError> {
        let record: Option<DaemonSnowRecord> = self
            .call_optional_wrapped(
                "reject",
                Some(json!({ "number": number, "reason": reason })),
                "record",
            )
            .await?;
        Ok(record.map(Into::into))
    }

    pub fn browser_url_by_id(&self, table: &str, sys_id: &str) -> Result<String, SnowError> {
        let Some(instance_url) = self.instance_url.as_deref() else {
            return Err(SnowError::Api(
                "Browser open is unavailable in daemon mode without SERVICENOW_INSTANCE."
                    .to_string(),
            ));
        };
        let base = normalize_instance_url(instance_url);
        Ok(format!("{base}/nav_to.do?uri={table}.do?sys_id={sys_id}"))
    }

    pub async fn daemon_username(&self) -> Result<Option<String>, SnowError> {
        let response = self.send_request("contract_info", None).await?;
        let result = response
            .result
            .ok_or_else(|| SnowError::Api("daemon response missing result".to_string()))?;
        let info: DaemonContractInfo =
            serde_json::from_value(result).map_err(|err| SnowError::Api(err.to_string()))?;
        Ok(info
            .environment
            .and_then(|environment| environment.username)
            .filter(|username| !username.trim().is_empty()))
    }

    /// Issue a raw JSON-RPC call and return the `result` field as a `Value`.
    ///
    /// Used by the admin TUI which needs to invoke arbitrary daemon methods
    /// (e.g. `start_job`, `get_job`) without committing to a typed wrapper.
    pub async fn call_raw(&self, method: &str, params: Value) -> Result<Value, SnowError> {
        let response = self.send_request(method, Some(params)).await?;
        response
            .result
            .ok_or_else(|| SnowError::Api("daemon response missing result".to_string()))
    }

    async fn call_wrapped<T: DeserializeOwned>(
        &self,
        method: &str,
        params: Option<Value>,
        field: &str,
    ) -> Result<T, SnowError> {
        let response = self.send_request(method, params).await?;
        let result = response
            .result
            .ok_or_else(|| SnowError::Api("daemon response missing result".to_string()))?;
        serde_json::from_value(unwrap_field_or_self(result, field))
            .map_err(|err| SnowError::Api(err.to_string()))
    }

    async fn call_optional_wrapped<T: DeserializeOwned>(
        &self,
        method: &str,
        params: Option<Value>,
        field: &str,
    ) -> Result<Option<T>, SnowError> {
        match self.send_request(method, params).await {
            Ok(response) => {
                let result = response
                    .result
                    .ok_or_else(|| SnowError::Api("daemon response missing result".to_string()))?;
                serde_json::from_value(unwrap_field_or_self(result, field))
                    .map(Some)
                    .map_err(|err| SnowError::Api(err.to_string()))
            }
            Err(SnowError::NotFound(_)) => Ok(None),
            Err(err) => Err(err),
        }
    }

    async fn send_request(
        &self,
        method: &str,
        params: Option<Value>,
    ) -> Result<RpcResponse, SnowError> {
        let stream = match self.endpoint.connect().await {
            Ok(stream) => stream,
            Err(err) => {
                let Some(env_name) = self.auto_spawn_env.clone() else {
                    return Err(SnowError::Api(format!(
                        "failed to connect to daemon endpoint {}: {err}",
                        self.endpoint
                    )));
                };
                self.auto_spawn_daemon(env_name).await?;
                let deadline = Instant::now() + Duration::from_secs(60);
                loop {
                    match self.endpoint.connect().await {
                        Ok(stream) => break stream,
                        Err(err) => {
                            if Instant::now() >= deadline {
                                return Err(SnowError::Api(format!(
                                    "failed to connect to daemon endpoint {} after auto-start: {err}",
                                    self.endpoint
                                )));
                            }
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                    }
                }
            }
        };

        self.send_request_on_stream(stream, method, params).await
    }

    async fn send_request_on_stream<S>(
        &self,
        mut stream: S,
        method: &str,
        params: Option<Value>,
    ) -> Result<RpcResponse, SnowError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let request = RpcRequest {
            jsonrpc: "2.0",
            method: method.to_string(),
            params: params.unwrap_or_else(|| json!({})),
            id: json!(id),
        };
        let payload =
            serde_json::to_vec(&request).map_err(|err| SnowError::Api(err.to_string()))?;
        stream.write_all(&payload).await?;
        stream.write_all(b"\n").await?;

        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        reader.read_line(&mut line).await?;
        let response: RpcResponse =
            serde_json::from_str(&line).map_err(|err| SnowError::Api(err.to_string()))?;
        if let Some(error) = response.error.as_ref() {
            if error.code == -32004 {
                return Err(SnowError::NotFound(error.message.clone()));
            }
            let details = error
                .data
                .as_ref()
                .map(Value::to_string)
                .unwrap_or_default();
            let message = if details.is_empty() {
                error.message.clone()
            } else {
                format!("{}: {details}", error.message)
            };
            return Err(SnowError::Api(message));
        }
        Ok(response)
    }

    async fn auto_spawn_daemon(&self, env_name: String) -> Result<(), SnowError> {
        tokio::task::spawn_blocking(move || crate::daemon_cmd::start::ensure_running(&env_name))
            .await
            .map_err(|err| SnowError::Api(format!("daemon auto-start task failed: {err}")))?
            .map(|_| ())
            .map_err(|err| SnowError::Api(format!("failed to auto-start daemon: {err:#}")))
    }
}

fn normalize_instance_url(instance: &str) -> String {
    if instance.starts_with("http://") || instance.starts_with("https://") {
        instance.trim_end_matches('/').to_string()
    } else {
        format!("https://{}", instance.trim_end_matches('/'))
    }
}

fn unwrap_field_or_self(result: Value, field: &str) -> Value {
    match result {
        Value::Object(mut map) => map.remove(field).unwrap_or(Value::Object(map)),
        other => other,
    }
}

#[allow(dead_code)]
fn parse_time_value(hours: &str) -> Result<TimeValue, SnowError> {
    hours
        .parse::<TimeValue>()
        .map_err(|_| SnowError::Api(format!("invalid hours value {hours:?}")))
}

#[allow(dead_code)]
fn week_selector_params(week: WeekSelector) -> Value {
    match week {
        WeekSelector::Current => json!({}),
        WeekSelector::Date(date) => json!({ "week": date.format("%Y-%m-%d").to_string() }),
    }
}

#[allow(dead_code)]
fn weekday_token(day: Weekday) -> &'static str {
    match day {
        Weekday::Sun => "sun",
        Weekday::Mon => "mon",
        Weekday::Tue => "tue",
        Weekday::Wed => "wed",
        Weekday::Thu => "thu",
        Weekday::Fri => "fri",
        Weekday::Sat => "sat",
    }
}

#[allow(dead_code)]
fn set_mode_token(mode: SetMode) -> &'static str {
    match mode {
        SetMode::Set => "set",
        SetMode::Add => "add",
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct DaemonSnowRecord {
    sys_id: String,
    number: String,
    table: String,
    resource_type: DaemonResourceType,
    state: String,
    short_description: String,
    description: String,
    fields: std::collections::HashMap<String, DaemonFieldValue>,
    work_notes: Vec<DaemonJournalEntry>,
    comments: Vec<DaemonJournalEntry>,
    parent: Option<DaemonRecordRef>,
    children: Vec<DaemonRecordRef>,
    references: std::collections::HashMap<String, DaemonReference>,
    synced_at: DateTime<Utc>,
    source: DaemonCacheSource,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct DaemonKnowledgeArticle {
    record: DaemonSnowRecord,
    knowledge_base: DaemonReference,
    category: DaemonReference,
    article_type: String,
    content: String,
    sn_tags: Vec<String>,
    auto_tags: Vec<String>,
    user_tags: Vec<String>,
    body_cached: bool,
    published_at: Option<DateTime<Utc>>,
    author: Option<DaemonReference>,
    valid_to: Option<NaiveDate>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct DaemonBusinessApplication {
    record: DaemonSnowRecord,
    name: String,
    business_owner: Option<DaemonReference>,
    is_owner: Option<DaemonReference>,
    ci_owner_group: Option<DaemonReference>,
    primary_support_group: Option<DaemonReference>,
    operational_state: Option<DaemonFieldValue>,
    primary_portfolio: Option<DaemonReference>,
    attested_date: Option<String>,
    fields: std::collections::HashMap<String, DaemonFieldValue>,
    unresolved_references: Vec<DaemonBusinessApplicationDiagnostic>,
    browser_url: Option<String>,
    vault_relative_path: Option<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct DaemonServer {
    record: DaemonSnowRecord,
    name: String,
    ip_address: Option<String>,
    class_name: Option<String>,
    ci_owner_group: Option<DaemonReference>,
    support_group: Option<DaemonReference>,
    operational_status: Option<DaemonFieldValue>,
    fields: HashMap<String, DaemonFieldValue>,
    browser_url: Option<String>,
    vault_relative_path: Option<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct DaemonBusinessApplicationDiagnostic {
    field: String,
    sys_id: Option<String>,
    table: Option<String>,
    diagnostic: String,
}

/// One field/operator/value filter passed to `business_application_query`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BusinessApplicationQueryFilter {
    pub field: String,
    /// Operator token understood by the daemon query layer (e.g. "contains", "eq").
    pub operator: String,
    pub value: String,
}

/// Request arguments passed to `business_application_query`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BusinessApplicationQueryArgs<'a> {
    pub text: Option<&'a str>,
    pub filters: &'a [BusinessApplicationQueryFilter],
    pub limit: Option<usize>,
}

/// Paged request arguments passed to `business_application_query`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BusinessApplicationQueryPageArgs<'a> {
    pub query: BusinessApplicationQueryArgs<'a>,
    pub offset: Option<usize>,
}

pub(crate) fn business_application_query_params(
    args: BusinessApplicationQueryArgs<'_>,
) -> serde_json::Map<String, Value> {
    let mut params = serde_json::Map::new();
    if let Some(text) = args.text {
        params.insert("text".to_string(), json!(text));
    }
    params.insert(
        "filters".to_string(),
        json!(
            args.filters
                .iter()
                .map(|f| json!({
                    "field": f.field,
                    "op": f.operator,
                    "value": f.value,
                }))
                .collect::<Vec<_>>()
        ),
    );
    if let Some(limit) = args.limit {
        params.insert("limit".to_string(), json!(limit));
    }
    params
}

pub(crate) fn business_application_query_page_params(
    args: BusinessApplicationQueryPageArgs<'_>,
) -> serde_json::Map<String, Value> {
    let mut params = business_application_query_params(args.query);
    if let Some(offset) = args.offset {
        params.insert("offset".to_string(), json!(offset));
    }
    params
}

/// Request arguments passed to `business_application_servers`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BusinessApplicationServersArgs<'a> {
    pub number: Option<&'a str>,
    pub sys_id: Option<&'a str>,
    pub max_depth: Option<usize>,
    pub max_cis: Option<usize>,
    pub max_edges: Option<usize>,
    pub max_service_membership_associations: Option<usize>,
    pub max_service_membership_pages: Option<usize>,
    pub relationship_type: &'a [String],
    pub include_paths: bool,
    /// Fallback strategy wire value (`none` or `ci_owner_group`). Sent to the
    /// daemon only when not `none`, so the default request shape is unchanged.
    pub fallback_strategy: &'a str,
    pub persist: bool,
    pub prune_stale: bool,
}

pub(crate) fn business_application_servers_params(
    args: BusinessApplicationServersArgs<'_>,
) -> serde_json::Map<String, Value> {
    let mut params = serde_json::Map::new();
    if let Some(number) = args.number {
        params.insert("number".to_string(), json!(number));
    }
    if let Some(sys_id) = args.sys_id {
        params.insert("sys_id".to_string(), json!(sys_id));
    }
    if let Some(max_depth) = args.max_depth {
        params.insert("max_depth".to_string(), json!(max_depth));
    }
    if let Some(max_cis) = args.max_cis {
        params.insert("max_cis".to_string(), json!(max_cis));
    }
    if let Some(max_edges) = args.max_edges {
        params.insert("max_edges".to_string(), json!(max_edges));
    }
    if let Some(max_service_membership_associations) = args.max_service_membership_associations {
        params.insert(
            "max_service_membership_associations".to_string(),
            json!(max_service_membership_associations),
        );
    }
    if let Some(max_service_membership_pages) = args.max_service_membership_pages {
        params.insert(
            "max_service_membership_pages".to_string(),
            json!(max_service_membership_pages),
        );
    }
    params.insert(
        "relationship_type".to_string(),
        json!(args.relationship_type),
    );
    params.insert("include_paths".to_string(), json!(args.include_paths));
    // Only send fallback_strategy when explicitly non-default, keeping the
    // default request shape (and core behavior) unchanged.
    if !args.fallback_strategy.is_empty() && args.fallback_strategy != "none" {
        params.insert(
            "fallback_strategy".to_string(),
            json!(args.fallback_strategy),
        );
    }
    if !args.persist {
        params.insert("persist".to_string(), json!(false));
    }
    if args.prune_stale {
        params.insert("prune_stale".to_string(), json!(true));
    }
    params
}

/// Request arguments passed to `business_application_servers_cached`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BusinessApplicationServersCachedArgs<'a> {
    pub number: Option<&'a str>,
    pub sys_id: Option<&'a str>,
    pub include_tombstoned: bool,
}

pub(crate) fn business_application_servers_cached_params(
    args: BusinessApplicationServersCachedArgs<'_>,
) -> serde_json::Map<String, Value> {
    let mut params = serde_json::Map::new();
    if let Some(number) = args.number {
        params.insert("number".to_string(), json!(number));
    }
    if let Some(sys_id) = args.sys_id {
        params.insert("sys_id".to_string(), json!(sys_id));
    }
    if args.include_tombstoned {
        params.insert("include_tombstoned".to_string(), json!(true));
    }
    params
}

/// Request arguments passed to `business_applications_for_server`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BusinessApplicationsForServerArgs<'a> {
    pub sys_id: Option<&'a str>,
    pub include_tombstoned: bool,
}

pub(crate) fn business_applications_for_server_params(
    args: BusinessApplicationsForServerArgs<'_>,
) -> serde_json::Map<String, Value> {
    let mut params = serde_json::Map::new();
    if let Some(sys_id) = args.sys_id {
        params.insert("sys_id".to_string(), json!(sys_id));
    }
    if args.include_tombstoned {
        params.insert("include_tombstoned".to_string(), json!(true));
    }
    params
}

/// Request arguments passed to `business_application_sync`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BusinessApplicationSyncArgs<'a> {
    pub all: bool,
    pub name: Option<&'a str>,
    pub operational_state_not: Option<&'a str>,
    pub persist: bool,
    pub resolve_references: bool,
    pub reference_depth: Option<u32>,
    pub refresh_dictionary: bool,
}

pub(crate) fn business_application_sync_params(
    args: BusinessApplicationSyncArgs<'_>,
) -> serde_json::Map<String, Value> {
    let mut params = serde_json::Map::new();
    if args.all {
        params.insert("all".to_string(), json!(true));
    }
    if let Some(name) = args.name {
        params.insert("name".to_string(), json!(name));
    }
    if let Some(state) = args.operational_state_not {
        params.insert("operational_state_not".to_string(), json!(state));
    }
    params.insert("persist".to_string(), json!(args.persist));
    params.insert(
        "resolve_references".to_string(),
        json!(args.resolve_references),
    );
    if let Some(depth) = args.reference_depth {
        params.insert("reference_depth".to_string(), json!(depth));
    }
    params.insert(
        "refresh_dictionary".to_string(),
        json!(args.refresh_dictionary),
    );
    params
}

/// Filter bag passed to `server_search` and `server_query`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ServerQueryArgs<'a> {
    pub text: Option<&'a str>,
    pub name: Option<&'a str>,
    pub ip_address: Option<&'a str>,
    pub ci_owner_group: Option<&'a str>,
    pub class: Option<&'a str>,
    pub limit: Option<usize>,
}

fn server_params(args: ServerQueryArgs<'_>) -> serde_json::Map<String, Value> {
    let mut params = serde_json::Map::new();
    if let Some(text) = args.text {
        params.insert("text".to_string(), json!(text));
    }
    if let Some(name) = args.name {
        params.insert("name".to_string(), json!(name));
    }
    if let Some(ip_address) = args.ip_address {
        params.insert("ip_address".to_string(), json!(ip_address));
    }
    if let Some(ci_owner_group) = args.ci_owner_group {
        params.insert("ci_owner_group".to_string(), json!(ci_owner_group));
    }
    if let Some(class) = args.class {
        params.insert("class".to_string(), json!(class));
    }
    if let Some(limit) = args.limit {
        params.insert("limit".to_string(), json!(limit));
    }
    params
}

/// Public, daemon-decoupled view of a Business Application reference target.
///
/// The CLI display layer renders these instead of the private daemon DTOs so
/// `display.rs` does not depend on the daemon transport types.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BusinessApplicationRef {
    pub sys_id: String,
    pub table: String,
    pub display_name: String,
}

impl From<DaemonReference> for BusinessApplicationRef {
    fn from(value: DaemonReference) -> Self {
        Self {
            sys_id: value.sys_id,
            table: value.table,
            display_name: value.display_name,
        }
    }
}

/// A single dictionary/runtime field value rendered in the all-fields table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BusinessApplicationFieldValue {
    pub value: String,
    pub display_value: Option<String>,
}

impl From<DaemonFieldValue> for BusinessApplicationFieldValue {
    fn from(value: DaemonFieldValue) -> Self {
        Self {
            value: value.value,
            display_value: value.display_value,
        }
    }
}

/// Diagnostic describing a reference that could not be resolved locally.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BusinessApplicationDiagnostic {
    pub field: String,
    pub sys_id: Option<String>,
    pub table: Option<String>,
    pub diagnostic: String,
}

impl From<DaemonBusinessApplicationDiagnostic> for BusinessApplicationDiagnostic {
    fn from(value: DaemonBusinessApplicationDiagnostic) -> Self {
        Self {
            field: value.field,
            sys_id: value.sys_id,
            table: value.table,
            diagnostic: value.diagnostic,
        }
    }
}

/// Public Business Application view consumed by the CLI display helpers.
///
/// This mirrors `DaemonBusinessApplication` but drops the daemon-internal
/// `record`/transport coupling, exposing only what the CLI needs to render.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BusinessApplicationView {
    pub sys_id: String,
    pub number: String,
    pub name: String,
    pub business_owner: Option<BusinessApplicationRef>,
    pub is_owner: Option<BusinessApplicationRef>,
    pub ci_owner_group: Option<BusinessApplicationRef>,
    pub primary_support_group: Option<BusinessApplicationRef>,
    pub operational_state: Option<BusinessApplicationFieldValue>,
    pub primary_portfolio: Option<BusinessApplicationRef>,
    pub attested_date: Option<String>,
    /// All projected fields, sorted by key for stable rendering.
    pub fields: Vec<(String, BusinessApplicationFieldValue)>,
    pub unresolved_references: Vec<BusinessApplicationDiagnostic>,
    pub browser_url: Option<String>,
    pub vault_relative_path: Option<String>,
}

impl From<DaemonBusinessApplication> for BusinessApplicationView {
    fn from(value: DaemonBusinessApplication) -> Self {
        // Sort fields by key so human output is deterministic.
        let mut fields: Vec<(String, BusinessApplicationFieldValue)> = value
            .fields
            .into_iter()
            .map(|(key, val)| (key, val.into()))
            .collect();
        fields.sort_by(|a, b| a.0.cmp(&b.0));
        Self {
            sys_id: value.record.sys_id,
            number: value.record.number,
            name: value.name,
            business_owner: value.business_owner.map(Into::into),
            is_owner: value.is_owner.map(Into::into),
            ci_owner_group: value.ci_owner_group.map(Into::into),
            primary_support_group: value.primary_support_group.map(Into::into),
            operational_state: value.operational_state.map(Into::into),
            primary_portfolio: value.primary_portfolio.map(Into::into),
            attested_date: value.attested_date,
            fields,
            unresolved_references: value
                .unresolved_references
                .into_iter()
                .map(Into::into)
                .collect(),
            browser_url: value.browser_url,
            vault_relative_path: value.vault_relative_path,
        }
    }
}

/// Public, daemon-decoupled server reference target.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ServerRef {
    pub sys_id: String,
    pub table: String,
    pub display_name: String,
}

impl From<DaemonReference> for ServerRef {
    fn from(value: DaemonReference) -> Self {
        Self {
            sys_id: value.sys_id,
            table: value.table,
            display_name: value.display_name,
        }
    }
}

/// A projected server field value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ServerFieldValue {
    pub value: String,
    pub display_value: Option<String>,
}

impl From<DaemonFieldValue> for ServerFieldValue {
    fn from(value: DaemonFieldValue) -> Self {
        Self {
            value: value.value,
            display_value: value.display_value,
        }
    }
}

/// Public server view consumed by CLI display helpers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ServerView {
    pub sys_id: String,
    pub number: String,
    pub name: String,
    pub ip_address: Option<String>,
    pub class_name: Option<String>,
    pub ci_owner_group: Option<ServerRef>,
    pub support_group: Option<ServerRef>,
    pub operational_status: Option<ServerFieldValue>,
    /// All projected fields, sorted by key for stable rendering.
    pub fields: Vec<(String, ServerFieldValue)>,
    pub browser_url: Option<String>,
    pub vault_relative_path: Option<String>,
}

impl From<DaemonServer> for ServerView {
    fn from(value: DaemonServer) -> Self {
        let mut fields: Vec<(String, ServerFieldValue)> = value
            .fields
            .into_iter()
            .map(|(key, val)| (key, val.into()))
            .collect();
        fields.sort_by(|a, b| a.0.cmp(&b.0));
        Self {
            sys_id: value.record.sys_id,
            number: value.record.number,
            name: value.name,
            ip_address: value.ip_address,
            class_name: value.class_name,
            ci_owner_group: value.ci_owner_group.map(Into::into),
            support_group: value.support_group.map(Into::into),
            operational_status: value.operational_status.map(Into::into),
            fields,
            browser_url: value.browser_url,
            vault_relative_path: value.vault_relative_path,
        }
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct DaemonContractInfo {
    environment: Option<DaemonContractEnvironment>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct DaemonContractEnvironment {
    username: Option<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
struct DaemonKnowledgeSearchHit {
    article: DaemonKnowledgeArticle,
    mode: DaemonKnowledgeSearchMode,
    score: f32,
    semantic_score: Option<f32>,
    lexical_score: Option<f32>,
    coverage: DaemonKnowledgeEmbeddingCoverage,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum DaemonKnowledgeSearchMode {
    Lexical,
    Semantic,
    Hybrid,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum DaemonKnowledgeEmbeddingCoverage {
    Metadata,
    FullText,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct DaemonKnowledgeSemanticStatus {
    enabled: bool,
    provider: String,
    model: String,
    dimensions: usize,
    active_kb_articles: usize,
    metadata_embeddings: usize,
    full_text_embeddings: usize,
    stale_rows: usize,
    orphan_rows: usize,
    last_rebuild_at: Option<DateTime<Utc>>,
    last_error: Option<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct DaemonSemanticIndexSummary {
    full: bool,
    indexed_rows: usize,
    metadata_embeddings: usize,
    full_text_embeddings: usize,
    stale_rows: usize,
    orphan_rows: usize,
    last_rebuild_at: Option<DateTime<Utc>>,
    last_error: Option<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct DaemonApprovalRecord {
    record: DaemonSnowRecord,
    approver: DaemonReference,
    target: DaemonRecordRef,
    requested_at: DateTime<Utc>,
    due_date: Option<DateTime<Utc>>,
    #[serde(default)]
    routed_via: ApprovalRoutedVia,
    #[serde(default)]
    approver_group: Option<DaemonReference>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct DaemonFieldValue {
    value: String,
    display_value: Option<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct DaemonJournalEntry {
    timestamp: DateTime<Utc>,
    author: String,
    body: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct DaemonReference {
    sys_id: String,
    table: String,
    display_name: String,
    extra: std::collections::HashMap<String, String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct DaemonRecordRef {
    sys_id: String,
    number: String,
    table: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct DaemonSearchMatchReason {
    field: DaemonMatchField,
    value: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum DaemonResourceType {
    Task,
    Incident,
    Change,
    ChangeTask,
    Request,
    RequestTask,
    Project,
    Demand,
    DemandTask,
    ResourcePlan,
    Story,
    ScrumTask,
    Timecard,
    Knowledge,
    Approval,
    BusinessApplication,
    Server,
    PrivateTask,
    Unknown,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum DaemonMatchField {
    Number,
    ShortDescription,
    Description,
    WorkNotes,
    Content,
    Tag,
    Keyword,
    Alias,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum DaemonCacheSource {
    Memory,
    Disk,
    Api,
}

impl From<DaemonSnowRecord> for SnowRecord {
    fn from(value: DaemonSnowRecord) -> Self {
        Self {
            sys_id: value.sys_id,
            number: value.number,
            table: value.table,
            resource_type: value.resource_type.into(),
            state: value.state,
            short_description: value.short_description,
            description: value.description,
            fields: value
                .fields
                .into_iter()
                .map(|(key, value)| (key, value.into()))
                .collect(),
            work_notes: value.work_notes.into_iter().map(Into::into).collect(),
            comments: value.comments.into_iter().map(Into::into).collect(),
            parent: value.parent.map(Into::into),
            children: value.children.into_iter().map(Into::into).collect(),
            references: value
                .references
                .into_iter()
                .map(|(key, value)| (key, value.into()))
                .collect(),
            synced_at: value.synced_at,
            source: value.source.into(),
        }
    }
}

impl From<DaemonKnowledgeArticle> for KnowledgeArticle {
    fn from(value: DaemonKnowledgeArticle) -> Self {
        Self {
            record: value.record.into(),
            knowledge_base: value.knowledge_base.into(),
            category: value.category.into(),
            article_type: value.article_type,
            content: value.content,
            sn_tags: value.sn_tags,
            auto_tags: value.auto_tags,
            user_tags: value.user_tags,
            body_cached: value.body_cached,
            published_at: value.published_at,
            author: value.author.map(Into::into),
            valid_to: value.valid_to,
        }
    }
}

impl From<DaemonKnowledgeSearchHit> for KnowledgeSearchHit {
    fn from(value: DaemonKnowledgeSearchHit) -> Self {
        Self {
            article: value.article.into(),
            mode: value.mode.into(),
            score: value.score,
            semantic_score: value.semantic_score,
            lexical_score: value.lexical_score,
            coverage: value.coverage.into(),
        }
    }
}

impl From<DaemonKnowledgeSearchMode> for KnowledgeSearchMode {
    fn from(value: DaemonKnowledgeSearchMode) -> Self {
        match value {
            DaemonKnowledgeSearchMode::Lexical => Self::Lexical,
            DaemonKnowledgeSearchMode::Semantic => Self::Semantic,
            DaemonKnowledgeSearchMode::Hybrid => Self::Hybrid,
        }
    }
}

impl From<DaemonKnowledgeEmbeddingCoverage> for KnowledgeEmbeddingCoverage {
    fn from(value: DaemonKnowledgeEmbeddingCoverage) -> Self {
        match value {
            DaemonKnowledgeEmbeddingCoverage::Metadata => Self::Metadata,
            DaemonKnowledgeEmbeddingCoverage::FullText => Self::FullText,
        }
    }
}

impl From<DaemonKnowledgeSemanticStatus> for KnowledgeSemanticStatus {
    fn from(value: DaemonKnowledgeSemanticStatus) -> Self {
        Self {
            enabled: value.enabled,
            provider: value.provider,
            model: value.model,
            dimensions: value.dimensions,
            active_kb_articles: value.active_kb_articles,
            metadata_embeddings: value.metadata_embeddings,
            full_text_embeddings: value.full_text_embeddings,
            stale_rows: value.stale_rows,
            orphan_rows: value.orphan_rows,
            last_rebuild_at: value.last_rebuild_at,
            last_error: value.last_error,
        }
    }
}

impl From<DaemonSemanticIndexSummary> for SemanticIndexSummary {
    fn from(value: DaemonSemanticIndexSummary) -> Self {
        Self {
            full: value.full,
            indexed_rows: value.indexed_rows,
            metadata_embeddings: value.metadata_embeddings,
            full_text_embeddings: value.full_text_embeddings,
            stale_rows: value.stale_rows,
            orphan_rows: value.orphan_rows,
            last_rebuild_at: value.last_rebuild_at,
            last_error: value.last_error,
        }
    }
}

impl From<DaemonApprovalRecord> for ApprovalRecord {
    fn from(value: DaemonApprovalRecord) -> Self {
        Self {
            record: value.record.into(),
            approver: value.approver.into(),
            target: value.target.into(),
            requested_at: value.requested_at,
            due_date: value.due_date,
            routed_via: value.routed_via,
            approver_group: value.approver_group.map(Into::into),
        }
    }
}

impl From<DaemonFieldValue> for FieldValue {
    fn from(value: DaemonFieldValue) -> Self {
        Self {
            value: value.value,
            display_value: value.display_value,
        }
    }
}

impl From<DaemonJournalEntry> for JournalEntry {
    fn from(value: DaemonJournalEntry) -> Self {
        Self {
            timestamp: value.timestamp,
            author: value.author,
            body: value.body,
        }
    }
}

impl From<DaemonReference> for Reference {
    fn from(value: DaemonReference) -> Self {
        Self {
            sys_id: value.sys_id,
            table: value.table,
            display_name: value.display_name,
            extra: value.extra,
        }
    }
}

impl From<DaemonRecordRef> for RecordRef {
    fn from(value: DaemonRecordRef) -> Self {
        Self {
            sys_id: value.sys_id,
            number: value.number,
            table: value.table,
        }
    }
}

impl From<DaemonSearchMatchReason> for SearchMatchReason {
    fn from(value: DaemonSearchMatchReason) -> Self {
        Self {
            field: value.field.into(),
            value: value.value,
        }
    }
}

impl From<DaemonResourceType> for ResourceType {
    fn from(value: DaemonResourceType) -> Self {
        match value {
            DaemonResourceType::Task => Self::Task,
            DaemonResourceType::Incident => Self::Incident,
            DaemonResourceType::Change => Self::Change,
            DaemonResourceType::ChangeTask => Self::ChangeTask,
            DaemonResourceType::Request => Self::Request,
            DaemonResourceType::RequestTask => Self::RequestTask,
            DaemonResourceType::Project => Self::Project,
            DaemonResourceType::Demand => Self::Demand,
            DaemonResourceType::DemandTask => Self::DemandTask,
            DaemonResourceType::ResourcePlan => Self::ResourcePlan,
            DaemonResourceType::Story => Self::Story,
            DaemonResourceType::ScrumTask => Self::ScrumTask,
            DaemonResourceType::Timecard => Self::Timecard,
            DaemonResourceType::Knowledge => Self::Knowledge,
            DaemonResourceType::Approval => Self::Approval,
            DaemonResourceType::BusinessApplication => Self::BusinessApplication,
            DaemonResourceType::Server => Self::Server,
            DaemonResourceType::PrivateTask => Self::PrivateTask,
            DaemonResourceType::Unknown => Self::Unknown,
        }
    }
}

impl From<DaemonMatchField> for MatchField {
    fn from(value: DaemonMatchField) -> Self {
        match value {
            DaemonMatchField::Number => Self::Number,
            DaemonMatchField::ShortDescription => Self::ShortDescription,
            DaemonMatchField::Description => Self::Description,
            DaemonMatchField::WorkNotes => Self::WorkNotes,
            DaemonMatchField::Content => Self::Content,
            DaemonMatchField::Tag => Self::Tag,
            DaemonMatchField::Keyword => Self::Keyword,
            DaemonMatchField::Alias => Self::Alias,
        }
    }
}

impl From<DaemonCacheSource> for CacheSource {
    fn from(value: DaemonCacheSource) -> Self {
        match value {
            DaemonCacheSource::Memory => Self::Memory,
            DaemonCacheSource::Disk => Self::Disk,
            DaemonCacheSource::Api => Self::Api,
        }
    }
}

#[derive(serde::Serialize)]
struct RpcRequest {
    jsonrpc: &'static str,
    method: String,
    params: Value,
    id: Value,
}

#[derive(serde::Deserialize)]
struct RpcResponse {
    result: Option<Value>,
    error: Option<RpcError>,
}

#[derive(serde::Deserialize)]
struct RpcError {
    code: i64,
    message: String,
    data: Option<Value>,
}

#[allow(dead_code)]
fn _assert_send_sync_path(_: &Path) {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unwrap_field_or_self_returns_wrapped_value_and_falls_back_to_raw_object() {
        assert_eq!(
            unwrap_field_or_self(json!({ "record": { "number": "CHG001" } }), "record"),
            json!({ "number": "CHG001" })
        );
        assert_eq!(
            unwrap_field_or_self(json!({ "number": "CHG001" }), "record"),
            json!({ "number": "CHG001" })
        );
    }

    #[test]
    fn business_application_query_params_use_daemon_filter_op_key() {
        let filters = vec![
            BusinessApplicationQueryFilter {
                field: "business_owner".to_string(),
                operator: "contains".to_string(),
                value: "User".to_string(),
            },
            BusinessApplicationQueryFilter {
                field: "operational_state".to_string(),
                operator: "eq".to_string(),
                value: "1".to_string(),
            },
        ];

        let params = business_application_query_params(BusinessApplicationQueryArgs {
            text: Some("portfolio"),
            filters: &filters,
            limit: Some(50),
        });

        assert_eq!(
            Value::Object(params),
            json!({
                "text": "portfolio",
                "filters": [
                    {
                        "field": "business_owner",
                        "op": "contains",
                        "value": "User"
                    },
                    {
                        "field": "operational_state",
                        "op": "eq",
                        "value": "1"
                    }
                ],
                "limit": 50
            })
        );
    }

    #[test]
    fn business_application_query_page_params_include_offset() {
        let filters = vec![BusinessApplicationQueryFilter {
            field: "name".to_string(),
            operator: "contains".to_string(),
            value: "Example".to_string(),
        }];

        let params = business_application_query_page_params(BusinessApplicationQueryPageArgs {
            query: BusinessApplicationQueryArgs {
                text: None,
                filters: &filters,
                limit: Some(500),
            },
            offset: Some(1_000),
        });

        assert_eq!(
            Value::Object(params),
            json!({
                "filters": [
                    {
                        "field": "name",
                        "op": "contains",
                        "value": "Example"
                    }
                ],
                "limit": 500,
                "offset": 1000
            })
        );
    }

    #[test]
    fn business_application_servers_params_use_daemon_contract_keys() {
        let relationship_type = vec![
            "<RELATIONSHIP_TYPE>".to_string(),
            "<SECOND_RELATIONSHIP_TYPE>".to_string(),
        ];

        let params = business_application_servers_params(BusinessApplicationServersArgs {
            number: Some("<APM_NUMBER>"),
            sys_id: None,
            max_depth: Some(2),
            max_cis: Some(500),
            max_edges: Some(2_000),
            max_service_membership_associations: Some(3_000),
            max_service_membership_pages: Some(30),
            relationship_type: &relationship_type,
            include_paths: true,
            fallback_strategy: "none",
            persist: true,
            prune_stale: false,
        });

        assert_eq!(
            Value::Object(params),
            json!({
                "number": "<APM_NUMBER>",
                "max_depth": 2,
                "max_cis": 500,
                "max_edges": 2000,
                "max_service_membership_associations": 3000,
                "max_service_membership_pages": 30,
                "relationship_type": [
                    "<RELATIONSHIP_TYPE>",
                    "<SECOND_RELATIONSHIP_TYPE>"
                ],
                "include_paths": true
            })
        );
    }

    #[test]
    fn business_application_servers_params_emit_fallback_strategy_only_when_set() {
        // ci_owner_group is forwarded to the daemon.
        let params = business_application_servers_params(BusinessApplicationServersArgs {
            number: Some("<APM_NUMBER>"),
            relationship_type: &[],
            fallback_strategy: "ci_owner_group",
            persist: true,
            ..Default::default()
        });
        assert_eq!(
            params.get("fallback_strategy"),
            Some(&json!("ci_owner_group"))
        );

        // none / empty are omitted so the default request shape is unchanged.
        for strategy in ["none", ""] {
            let params = business_application_servers_params(BusinessApplicationServersArgs {
                number: Some("<APM_NUMBER>"),
                relationship_type: &[],
                fallback_strategy: strategy,
                persist: true,
                ..Default::default()
            });
            assert!(
                !params.contains_key("fallback_strategy"),
                "fallback_strategy `{strategy}` must be omitted"
            );
        }
    }

    #[test]
    fn business_application_servers_params_support_sys_id_selector() {
        let params = business_application_servers_params(BusinessApplicationServersArgs {
            number: None,
            sys_id: Some("<BUSINESS_APP_SYS_ID>"),
            max_depth: None,
            max_cis: None,
            max_edges: None,
            max_service_membership_associations: None,
            max_service_membership_pages: None,
            relationship_type: &[],
            include_paths: false,
            fallback_strategy: "none",
            persist: true,
            prune_stale: false,
        });

        assert_eq!(
            Value::Object(params),
            json!({
                "sys_id": "<BUSINESS_APP_SYS_ID>",
                "relationship_type": [],
                "include_paths": false
            })
        );
    }

    #[test]
    fn business_application_servers_params_include_opt_in_persistence_flags() {
        let no_persist_params =
            business_application_servers_params(BusinessApplicationServersArgs {
                number: Some("<APM_NUMBER>"),
                sys_id: None,
                max_depth: None,
                max_cis: None,
                max_edges: None,
                max_service_membership_associations: None,
                max_service_membership_pages: None,
                relationship_type: &[],
                include_paths: false,
                fallback_strategy: "none",
                persist: false,
                prune_stale: false,
            });

        assert_eq!(
            Value::Object(no_persist_params),
            json!({
                "number": "<APM_NUMBER>",
                "relationship_type": [],
                "include_paths": false,
                "persist": false
            })
        );

        let prune_params = business_application_servers_params(BusinessApplicationServersArgs {
            number: Some("<APM_NUMBER>"),
            sys_id: None,
            max_depth: None,
            max_cis: None,
            max_edges: None,
            max_service_membership_associations: None,
            max_service_membership_pages: None,
            relationship_type: &[],
            include_paths: false,
            fallback_strategy: "none",
            persist: true,
            prune_stale: true,
        });

        assert_eq!(
            Value::Object(prune_params),
            json!({
                "number": "<APM_NUMBER>",
                "relationship_type": [],
                "include_paths": false,
                "prune_stale": true
            })
        );
    }

    #[test]
    fn business_application_servers_cached_params_use_daemon_contract_keys() {
        let params =
            business_application_servers_cached_params(BusinessApplicationServersCachedArgs {
                number: Some("<APM_NUMBER>"),
                sys_id: None,
                include_tombstoned: true,
            });

        assert_eq!(
            Value::Object(params),
            json!({
                "number": "<APM_NUMBER>",
                "include_tombstoned": true
            })
        );
    }

    #[test]
    fn business_applications_for_server_params_use_daemon_contract_keys() {
        let params = business_applications_for_server_params(BusinessApplicationsForServerArgs {
            sys_id: Some("<SERVER_SYS_ID>"),
            include_tombstoned: true,
        });

        assert_eq!(
            Value::Object(params),
            json!({
                "sys_id": "<SERVER_SYS_ID>",
                "include_tombstoned": true
            })
        );
    }

    #[test]
    fn business_application_sync_params_include_all_contract_flag() {
        let params = business_application_sync_params(BusinessApplicationSyncArgs {
            all: true,
            name: None,
            operational_state_not: None,
            persist: true,
            resolve_references: true,
            reference_depth: Some(1),
            refresh_dictionary: true,
        });

        assert_eq!(
            Value::Object(params),
            json!({
                "all": true,
                "persist": true,
                "resolve_references": true,
                "reference_depth": 1,
                "refresh_dictionary": true
            })
        );
    }

    #[test]
    fn daemon_contract_info_decodes_runtime_username() {
        let info: DaemonContractInfo = serde_json::from_value(json!({
            "environment": {
                "label": "prd",
                "username": "svc.agent"
            }
        }))
        .expect("contract info");

        assert_eq!(
            info.environment
                .and_then(|environment| environment.username),
            Some("svc.agent".to_string())
        );
    }

    #[test]
    fn daemon_task_sla_status_decodes_wrapped_product_type() {
        let status: TaskSlaStatus = serde_json::from_value(unwrap_field_or_self(
            json!({
                "status": {
                    "record_number": "INC0000001",
                    "record_table": "incident",
                    "record_sys_id": "incident-sys-1",
                    "rows": [],
                    "summary": {
                        "total": 0,
                        "active": 0,
                        "breached": 0,
                        "next_breach": null,
                        "highest_business_elapsed": null
                    },
                    "readable": "EmptyOrAclRestricted"
                }
            }),
            "status",
        ))
        .expect("task sla status");

        assert_eq!(status.record_number, "INC0000001");
        assert!(status.rows.is_empty());
    }

    #[test]
    fn daemon_task_sla_status_for_tasks_decodes_wrapped_status_map() {
        let statuses: HashMap<String, TaskSlaStatus> =
            serde_json::from_value(unwrap_field_or_self(
                json!({
                    "statuses": {
                        "incident-sys-1": {
                            "record_number": "INC0000001",
                            "record_table": "incident",
                            "record_sys_id": "incident-sys-1",
                            "rows": [],
                            "summary": {
                                "total": 0,
                                "active": 0,
                                "breached": 0,
                                "next_breach": null,
                                "highest_business_elapsed": null
                            },
                            "readable": "EmptyOrAclRestricted"
                        }
                    }
                }),
                "statuses",
            ))
            .expect("task sla status map");

        assert_eq!(
            statuses
                .get("incident-sys-1")
                .expect("incident status")
                .record_number,
            "INC0000001"
        );
    }

    #[test]
    fn daemon_record_converts_snake_case_transport_enums_into_core_types() {
        let record: DaemonSnowRecord = serde_json::from_value(json!({
            "sys_id": "ctask-sys",
            "number": "CTASK001",
            "table": "change_task",
            "resource_type": "change_task",
            "state": "Open",
            "short_description": "Validate replication",
            "description": "Check replica lag",
            "fields": {},
            "work_notes": [],
            "comments": [],
            "parent": null,
            "children": [],
            "references": {},
            "synced_at": "2026-04-10T00:00:00Z",
            "source": "disk",
            "browser_url": "http://localhost/nav_to.do?uri=change_task.do?sys_id=ctask-sys",
            "vault_relative_path": "changes/CHG001/CTASK001.md"
        }))
        .expect("daemon record");

        let record: SnowRecord = record.into();
        assert_eq!(record.resource_type, ResourceType::ChangeTask);
        assert_eq!(record.source, CacheSource::Disk);
    }

    #[test]
    fn daemon_resource_type_decodes_common_task_primitives() {
        for (transport, expected) in [
            ("task", ResourceType::Task),
            ("project", ResourceType::Project),
            ("demand", ResourceType::Demand),
        ] {
            let resource_type: DaemonResourceType =
                serde_json::from_value(json!(transport)).expect("daemon resource type");
            assert_eq!(ResourceType::from(resource_type), expected);
        }
    }

    #[test]
    fn daemon_approval_converts_nested_transport_record() {
        let approval: DaemonApprovalRecord = serde_json::from_value(json!({
            "record": {
                "sys_id": "apr-sys",
                "number": "APR001",
                "table": "sysapproval_approver",
                "resource_type": "approval",
                "state": "requested",
                "short_description": "Approval",
                "description": "Approval pending",
                "fields": {},
                "work_notes": [],
                "comments": [],
                "parent": null,
                "children": [],
                "references": {},
                "synced_at": "2026-04-10T00:00:00Z",
                "source": "disk"
            },
            "approver": {
                "sys_id": "user-1",
                "table": "sys_user",
                "display_name": "Example User",
                "extra": {}
            },
            "target": {
                "sys_id": "chg-sys",
                "number": "CHG001",
                "table": "change_request"
            },
            "requested_at": "2026-04-10T00:00:00Z",
            "due_date": null
        }))
        .expect("daemon approval");

        let approval: ApprovalRecord = approval.into();
        assert_eq!(approval.record.resource_type, ResourceType::Approval);
        assert_eq!(approval.target.number, "CHG001");
    }
}
