use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::SystemTime;

use chrono::{Local, NaiveDateTime};
use servicenow_rs::prelude::{DisplayValue, Record};
use snow_core::display as core_display;
use snow_core::{
    ApprovalRecord as CachedApprovalRecord, KnowledgeSearchFilters, SnowRecord, TaskSlaParentRef,
    TaskSlaReadability, TaskSlaStatus,
};

use super::{
    App, ApprovalRef, BackgroundJobKey, BackgroundJobResult, BrowserSelection, BrowserState,
    ChildRelation, ChoiceItem, DashboardRow, DashboardTab, DetailChildColumn,
    DetailChildSummaryModel, DetailChildValue, DetailColumnWidth, DetailFieldSpec, DetailPageModel,
    DetailSectionSpec, EntityKind, MAX_WORK_NOTES, ModalState, PrefetchTask, RecordDetail,
    RecordRef, TaskSlaDetail, UserChoice,
};
use crate::display;
use crate::error::SnowError;
use crate::tui_client::TuiClient;

pub(super) enum MutationRequest {
    Note {
        target: RecordRef,
        message: String,
    },
    Approve {
        approval: ApprovalRef,
    },
    Reject {
        approval: ApprovalRef,
        reason: String,
    },
    StateUpdate {
        target: RecordRef,
        field_name: String,
        raw_value: String,
    },
    Reassign {
        target: RecordRef,
        assignee_sys_id: String,
    },
}

impl MutationRequest {
    fn job_key(&self) -> Option<BackgroundJobKey> {
        let (kind, table, sys_id) = match self {
            Self::Note { target, .. } => ("note", target.table.as_str(), target.sys_id.as_str()),
            Self::Approve { approval } => (
                "approve",
                approval.source_table.as_str(),
                approval.source_sys_id.as_str(),
            ),
            Self::Reject { approval, .. } => (
                "reject",
                approval.source_table.as_str(),
                approval.source_sys_id.as_str(),
            ),
            Self::StateUpdate { target, .. } => {
                ("state", target.table.as_str(), target.sys_id.as_str())
            }
            Self::Reassign { target, .. } => {
                ("reassign", target.table.as_str(), target.sys_id.as_str())
            }
        };
        Some(BackgroundJobKey::Mutation(format!(
            "{kind}:{table}:{sys_id}"
        )))
    }

    async fn execute(self, core: &TuiClient) -> Result<&'static str, SnowError> {
        match self {
            Self::Note { target, message } => {
                core.add_work_note(&target.number, &message).await?;
                Ok("Work note added.")
            }
            Self::Approve { approval } => {
                core.approve(&approval.source_number).await?;
                Ok("Approved.")
            }
            Self::Reject { approval, reason } => {
                core.reject(&approval.source_number, &reason).await?;
                Ok("Rejected.")
            }
            Self::StateUpdate {
                target,
                field_name,
                raw_value,
            } => {
                if field_name == "state" {
                    core.set_state(&target.number, &raw_value).await?;
                    Ok("State updated.")
                } else if let Some(local) = core.as_local_core() {
                    local
                        .client()
                        .table(&target.table)
                        .fields(&["sys_id", "number", &field_name])
                        .display_value(DisplayValue::Both)
                        .update(&target.sys_id, serde_json::json!({ field_name: raw_value }))
                        .await?;
                    Ok("State updated.")
                } else {
                    Err(SnowError::Api(
                        "Field updates other than state are not supported in daemon-backed TUI mode yet."
                            .to_string(),
                    ))
                }
            }
            Self::Reassign {
                target,
                assignee_sys_id,
            } => {
                if let Some(local) = core.as_local_core() {
                    local
                        .client()
                        .table(&target.table)
                        .fields(&["sys_id", "number", "assigned_to"])
                        .display_value(DisplayValue::Both)
                        .update(
                            &target.sys_id,
                            serde_json::json!({ "assigned_to": assignee_sys_id }),
                        )
                        .await?;
                    Ok("Assignee updated.")
                } else {
                    Err(SnowError::Api(
                        "Reassignment is not supported in daemon-backed TUI mode yet.".to_string(),
                    ))
                }
            }
        }
    }
}

impl App {
    fn spawn_prefetch_task(&mut self, task: PrefetchTask) -> bool {
        let key = match &task {
            PrefetchTask::Detail(record) => {
                BackgroundJobKey::Detail(detail_cache_key(&record.table, &record.sys_id))
            }
            PrefetchTask::Children(record) => {
                BackgroundJobKey::Children(children_cache_key(&record.table, &record.sys_id))
            }
            PrefetchTask::BatchChildren { relation, parents } => {
                BackgroundJobKey::BatchChildren(batch_children_job_key(*relation, parents))
            }
            PrefetchTask::Approval(record) => {
                BackgroundJobKey::Approval(approval_cache_key(&record.table, &record.sys_id))
            }
        };

        if !self.inflight_jobs.insert(key) {
            return false;
        }

        let core = Arc::clone(&self.core);
        self.background_jobs.spawn_local(async move {
            match task {
                PrefetchTask::Detail(record) => {
                    let detail = if let Some(kind) = record_kind(&record) {
                        match fetch_detail(core.as_ref(), kind, &record).await {
                            Ok(detail) => detail,
                            Err(err) => detail_error_placeholder(&record, &err),
                        }
                    } else {
                        unsupported_detail_placeholder(&record)
                    };
                    Ok(BackgroundJobResult::Detail { record, detail })
                }
                PrefetchTask::Children(record) => {
                    let Some(kind) = record_kind(&record) else {
                        return Ok(BackgroundJobResult::Children {
                            record,
                            children: Vec::new(),
                        });
                    };
                    let children =
                        fetch_children(core.as_ref(), &record, child_relation(kind)).await?;
                    Ok(BackgroundJobResult::Children { record, children })
                }
                PrefetchTask::BatchChildren { relation, parents } => {
                    let Some(_) = relation.child_table else {
                        return Ok(BackgroundJobResult::BatchChildren {
                            relation,
                            groups: parents
                                .into_iter()
                                .map(|parent| (parent, Vec::new()))
                                .collect(),
                        });
                    };
                    let cached_parents: Vec<SnowRecord> = parents
                        .iter()
                        .map(|parent| SnowRecord {
                            sys_id: parent.sys_id.clone(),
                            number: parent.number.clone(),
                            table: parent.table.clone(),
                            resource_type: snow_core::ResourceType::from_table(&parent.table),
                            state: parent.state_value.clone().unwrap_or_default(),
                            short_description: parent.short_description.clone(),
                            description: String::new(),
                            fields: std::collections::HashMap::new(),
                            work_notes: Vec::new(),
                            comments: Vec::new(),
                            parent: None,
                            children: Vec::new(),
                            references: std::collections::HashMap::new(),
                            synced_at: chrono::Utc::now(),
                            source: snow_core::CacheSource::Disk,
                        })
                        .collect();
                    let raw_groups = core.batch_children(&cached_parents).await?;
                    let groups = parents
                        .into_iter()
                        .map(|parent| {
                            let records = raw_groups
                                .iter()
                                .find(|(source_id, _)| source_id == &parent.sys_id)
                                .map(|(_, records)| records.clone())
                                .unwrap_or_default();
                            let children = records
                                .into_iter()
                                .map(map_child_record)
                                .collect::<Vec<_>>();
                            (parent, children)
                        })
                        .collect();
                    Ok(BackgroundJobResult::BatchChildren { relation, groups })
                }
                PrefetchTask::Approval(record) => {
                    let approval = fetch_pending_approval(core.as_ref(), &record).await?;
                    Ok(BackgroundJobResult::Approval { record, approval })
                }
            }
        });

        true
    }

    pub(super) fn dispatch_background_work(&mut self) -> bool {
        let mut spawned = false;

        while self.inflight_jobs.len() < 6 {
            let Some(task) = self.prefetch_queue.pop() else {
                break;
            };
            if self.spawn_prefetch_task(task) {
                spawned = true;
            }
        }

        spawned
    }

    pub(super) fn apply_completed_jobs(&mut self) -> Result<bool, SnowError> {
        let mut changed = false;

        while let Some(result) = self.background_jobs.try_join_next() {
            let output = result.map_err(|err| SnowError::Api(err.to_string()))??;
            changed = true;

            match output {
                BackgroundJobResult::DashboardRefresh { tab, result } => {
                    self.inflight_jobs
                        .remove(&BackgroundJobKey::DashboardRefresh(tab));
                    apply_tab_result(&mut self.dashboard, tab, result);
                    self.normalize_dashboard_selection();
                    if let Some(progress) = self.dashboard_refresh_progress.as_mut() {
                        progress.completed += 1;
                        let completed = progress.completed;
                        let total = progress.total;
                        let user_requested = progress.user_requested;
                        if completed >= total {
                            self.last_refresh_at = Some(SystemTime::now());
                            self.last_refresh_tick = std::time::Instant::now();
                            self.dashboard_refresh_progress = None;
                            self.refill_prefetch_queue();
                            if user_requested {
                                self.set_status(
                                    format!("Dashboard refreshed. {completed}/{total} complete."),
                                    false,
                                );
                            }
                        } else {
                            self.set_status(
                                format!("Refreshing dashboard... {completed}/{total} complete",),
                                false,
                            );
                        }
                    }
                }
                BackgroundJobResult::Detail { record, detail } => {
                    let key = detail_cache_key(&record.table, &record.sys_id);
                    self.inflight_jobs
                        .remove(&BackgroundJobKey::Detail(key.clone()));
                    self.detail_cache.insert(key, detail.clone());
                    if let Some(browser) = self.browser.as_mut() {
                        if browser.parent.sys_id == record.sys_id {
                            browser.parent_detail = detail.clone();
                        } else if browser
                            .children
                            .iter()
                            .any(|child| child.sys_id == record.sys_id)
                        {
                            browser
                                .detail_cache
                                .insert(record.sys_id.clone(), detail.clone());
                        }
                    }
                }
                BackgroundJobResult::Children { record, children } => {
                    let key = children_cache_key(&record.table, &record.sys_id);
                    self.inflight_jobs.remove(&BackgroundJobKey::Children(key));
                    self.children_cache.insert(
                        children_cache_key(&record.table, &record.sys_id),
                        children.clone(),
                    );
                    if let Some(browser) = self.browser.as_mut()
                        && browser.parent.sys_id == record.sys_id
                    {
                        browser.children = children;
                    }
                    self.normalize_browser_selection();
                }
                BackgroundJobResult::BatchChildren { relation, groups } => {
                    let parents: Vec<RecordRef> =
                        groups.iter().map(|(record, _)| record.clone()).collect();
                    self.inflight_jobs.remove(&BackgroundJobKey::BatchChildren(
                        batch_children_job_key(relation, &parents),
                    ));
                    for (record, children) in groups {
                        self.children_cache.insert(
                            children_cache_key(&record.table, &record.sys_id),
                            children.clone(),
                        );
                        if let Some(browser) = self.browser.as_mut()
                            && browser.parent.sys_id == record.sys_id
                        {
                            browser.children = children;
                        }
                        self.normalize_browser_selection();
                    }
                }
                BackgroundJobResult::Approval { record, approval } => {
                    let key = approval_cache_key(&record.table, &record.sys_id);
                    self.inflight_jobs.remove(&BackgroundJobKey::Approval(key));
                    self.approval_cache.insert(
                        approval_cache_key(&record.table, &record.sys_id),
                        approval.clone(),
                    );
                    if let Some(browser) = self.browser.as_mut()
                        && browser.parent.sys_id == record.sys_id
                    {
                        browser.approval = approval;
                    }
                }
                BackgroundJobResult::KnowledgeBases { result } => {
                    self.inflight_jobs.remove(&BackgroundJobKey::KnowledgeBases);
                    match result {
                        Ok(bases) => {
                            self.knowledge.set_bases(bases);
                            if let Some(base_sys_id) = self
                                .knowledge
                                .selected_base_sys_id()
                                .map(ToString::to_string)
                            {
                                self.start_knowledge_categories_load(&base_sys_id);
                            }
                        }
                        Err(err) => {
                            self.knowledge.error = Some(err.to_string());
                            self.set_error(err.to_string());
                        }
                    }
                }
                BackgroundJobResult::KnowledgeCategories {
                    base_sys_id,
                    result,
                } => {
                    self.inflight_jobs
                        .remove(&BackgroundJobKey::KnowledgeCategories(base_sys_id.clone()));
                    match result {
                        Ok(categories) => {
                            self.knowledge.set_categories(base_sys_id, categories);
                        }
                        Err(err) => {
                            self.knowledge.error = Some(err.to_string());
                            self.set_error(err.to_string());
                        }
                    }
                }
                BackgroundJobResult::KnowledgeSearch { query, result } => {
                    self.inflight_jobs
                        .remove(&BackgroundJobKey::KnowledgeSearch(query.clone()));
                    let current_query = self.knowledge.query.value.trim().to_string();
                    if current_query != query.trim() {
                        continue;
                    }
                    match result {
                        Ok(results) => {
                            self.knowledge.set_results(results);
                            self.knowledge.loading = false;
                            self.knowledge.focus = if self.knowledge.results.is_empty() {
                                super::knowledge::KnowledgeFocus::Search
                            } else {
                                super::knowledge::KnowledgeFocus::Results
                            };
                            self.schedule_knowledge_category_load();
                        }
                        Err(err) => {
                            self.knowledge.loading = false;
                            self.knowledge.error = Some(err.to_string());
                            self.set_error(err.to_string());
                        }
                    }
                }
                BackgroundJobResult::KnowledgeArticle { number, result } => {
                    self.inflight_jobs
                        .remove(&BackgroundJobKey::KnowledgeArticle(number.clone()));
                    match result {
                        Ok(Some(article)) => {
                            self.knowledge.query.value = number.clone();
                            self.knowledge.set_results(vec![article]);
                            self.knowledge.loading = false;
                            self.knowledge.focus = super::knowledge::KnowledgeFocus::Results;
                            self.screen = super::Screen::Knowledge;
                            self.set_status(format!("Opened {}.", number), false);
                            self.schedule_knowledge_category_load();
                            self.start_knowledge_bases_load();
                        }
                        Ok(None) => {
                            self.knowledge.loading = false;
                            self.knowledge.error = Some(format!("{number} not found."));
                            self.set_error(format!("{number} not found."));
                        }
                        Err(err) => {
                            self.knowledge.loading = false;
                            self.knowledge.error = Some(err.to_string());
                            self.set_error(err.to_string());
                        }
                    }
                }
                BackgroundJobResult::StateChoices {
                    table,
                    field,
                    choices,
                } => {
                    self.inflight_jobs
                        .remove(&BackgroundJobKey::StateChoices(format!("{table}:{field}")));
                    match choices {
                        Ok(choices) => {
                            self.choice_cache
                                .insert((table, field.clone()), choices.clone());
                            let current_state = self
                                .selected_action_record()
                                .and_then(|record| record.state_value);
                            if let Some(ModalState::StatePicker(modal)) = self.modal.as_mut()
                                && modal.field_name == field
                            {
                                let selected = current_state
                                    .as_deref()
                                    .and_then(|value| {
                                        choices.iter().position(|choice| choice.value == value)
                                    })
                                    .unwrap_or(0);
                                modal.choices = choices;
                                modal.selected = selected;
                                modal.loading = false;
                            }
                        }
                        Err(err) => {
                            if matches!(self.modal, Some(ModalState::StatePicker(_))) {
                                self.modal = None;
                            }
                            self.set_error(err.to_string());
                        }
                    }
                }
                BackgroundJobResult::UserSearch { query, results } => {
                    self.inflight_jobs
                        .remove(&BackgroundJobKey::UserSearch(query.clone()));
                    if let Some(ModalState::Reassign(modal)) = self.modal.as_mut()
                        && modal.query.value.trim() == query
                    {
                        modal.results = results;
                        modal.selected = 0;
                        modal.last_searched = query;
                        modal.loading = false;
                    }
                }
                BackgroundJobResult::JumpLookup { number, result } => {
                    self.inflight_jobs
                        .remove(&BackgroundJobKey::JumpLookup(number.clone()));
                    match result {
                        Ok((record, detail, children, approval)) => {
                            self.open_browser_for_record(
                                record.clone(),
                                detail,
                                children,
                                approval,
                            );
                            self.modal = None;
                            self.set_status(format!("Opened {}.", record.number), false);
                        }
                        Err(err) => {
                            self.modal = None;
                            self.set_error(err.to_string());
                        }
                    }
                }
                BackgroundJobResult::Mutation { message } => {
                    let keys: Vec<_> = self
                        .inflight_jobs
                        .iter()
                        .filter(|key| matches!(key, BackgroundJobKey::Mutation(_)))
                        .cloned()
                        .collect();
                    for key in keys {
                        self.inflight_jobs.remove(&key);
                    }
                    self.after_mutation(message)?;
                }
            }
        }

        Ok(changed)
    }

    pub(super) fn start_dashboard_refresh(&mut self, user_requested: bool) {
        if DashboardTab::all().into_iter().any(|tab| {
            self.inflight_jobs
                .contains(&BackgroundJobKey::DashboardRefresh(tab))
        }) {
            return;
        }
        self.dashboard_refresh_progress =
            Some(super::DashboardRefreshProgress::new(user_requested));
        for tab in DashboardTab::all() {
            self.inflight_jobs
                .insert(BackgroundJobKey::DashboardRefresh(tab));
            let core = Arc::clone(&self.core);
            self.background_jobs.spawn_local(async move {
                let result = match tab {
                    DashboardTab::PendingApprovals => fetch_approvals(core.as_ref()).await,
                    DashboardTab::MyTasks => fetch_tasks(core.as_ref()).await,
                    DashboardTab::MyStories => fetch_stories(core.as_ref()).await,
                    DashboardTab::MyIncidents => fetch_incidents(core.as_ref()).await,
                };
                Ok(BackgroundJobResult::DashboardRefresh { tab, result })
            });
        }
        self.set_status("Refreshing dashboard... 0/4 complete", false);
    }

    pub(super) fn start_knowledge_bases_load(&mut self) {
        if self
            .inflight_jobs
            .contains(&BackgroundJobKey::KnowledgeBases)
        {
            return;
        }
        if !self.knowledge.bases.is_empty() {
            return;
        }
        self.inflight_jobs.insert(BackgroundJobKey::KnowledgeBases);
        let core = Arc::clone(&self.core);
        self.background_jobs.spawn_local(async move {
            let result = core.list_knowledge_bases().await;
            Ok(BackgroundJobResult::KnowledgeBases { result })
        });
    }

    pub(super) fn start_knowledge_categories_load(&mut self, base_sys_id: &str) {
        let base_sys_id = base_sys_id.trim().to_string();
        if base_sys_id.is_empty() {
            return;
        }
        let job_key = BackgroundJobKey::KnowledgeCategories(base_sys_id.clone());
        if self.inflight_jobs.contains(&job_key) {
            return;
        }
        if self
            .knowledge
            .categories_base_sys_id
            .as_deref()
            .is_some_and(|loaded| loaded == base_sys_id)
        {
            return;
        }

        self.inflight_jobs.insert(job_key);
        let core = Arc::clone(&self.core);
        self.background_jobs.spawn_local(async move {
            let result = core.list_categories(&base_sys_id).await;
            Ok(BackgroundJobResult::KnowledgeCategories {
                base_sys_id,
                result,
            })
        });
    }

    pub(super) fn start_knowledge_search(&mut self, query: &str) {
        let trimmed = query.trim().to_string();
        self.knowledge.query.value = query.to_string();
        self.knowledge.error = None;
        if trimmed.is_empty() {
            self.knowledge.set_results(Vec::new());
            self.knowledge.categories.clear();
            self.knowledge.categories_base_sys_id = None;
            self.knowledge.loading = false;
            return;
        }

        let job_key = BackgroundJobKey::KnowledgeSearch(trimmed.clone());
        if self.inflight_jobs.contains(&job_key) {
            self.knowledge.loading = true;
            return;
        }

        self.inflight_jobs.insert(job_key);
        self.knowledge.loading = true;
        let core = Arc::clone(&self.core);
        self.background_jobs.spawn_local(async move {
            let result = core
                .search_knowledge(&trimmed, KnowledgeSearchFilters::default())
                .await;
            Ok(BackgroundJobResult::KnowledgeSearch {
                query: trimmed,
                result,
            })
        });
    }

    pub(super) fn start_knowledge_lookup(&mut self, number: &str) {
        let number = number.trim().to_ascii_uppercase();
        if number.is_empty() {
            self.set_error("Knowledge article number cannot be empty.");
            return;
        }

        let job_key = BackgroundJobKey::KnowledgeArticle(number.clone());
        if self.inflight_jobs.contains(&job_key) {
            return;
        }

        self.inflight_jobs.insert(job_key);
        self.screen = super::Screen::Knowledge;
        self.knowledge.query.value = number.clone();
        self.knowledge.results.clear();
        self.knowledge.selected = 0;
        self.knowledge.focus = super::knowledge::KnowledgeFocus::Search;
        self.knowledge.loading = true;
        let core = Arc::clone(&self.core);
        self.background_jobs.spawn_local(async move {
            let result = core.get_knowledge_article_fresh(&number).await;
            Ok(BackgroundJobResult::KnowledgeArticle { number, result })
        });
    }

    pub(super) fn start_state_choices_load(&mut self, table: &str, field: &str) {
        let cache_key = (table.to_string(), field.to_string());
        if let Some(cached) = self.choice_cache.get(&cache_key).cloned() {
            if let Some(ModalState::StatePicker(modal)) = self.modal.as_mut() {
                modal.choices = cached;
                modal.selected = 0;
                modal.loading = false;
            }
            return;
        }

        let job_key = BackgroundJobKey::StateChoices(format!("{table}:{field}"));
        if self.inflight_jobs.contains(&job_key) {
            return;
        }
        self.inflight_jobs.insert(job_key);
        let core = Arc::clone(&self.core);
        let table = table.to_string();
        let field = field.to_string();
        self.background_jobs.spawn_local(async move {
            let choices = fetch_choices_for_state_modal(core.as_ref(), &table, &field).await;
            Ok(BackgroundJobResult::StateChoices {
                table,
                field,
                choices,
            })
        });
    }

    pub(super) fn start_user_search(&mut self, query: &str) {
        let trimmed = query.trim();
        if trimmed.len() < 2 {
            if let Some(ModalState::Reassign(modal)) = self.modal.as_mut() {
                modal.results.clear();
                modal.selected = 0;
                modal.last_searched.clear();
                modal.loading = false;
            }
            return;
        }

        let job_key = BackgroundJobKey::UserSearch(trimmed.to_string());
        if self.inflight_jobs.contains(&job_key) {
            return;
        }
        if let Some(ModalState::Reassign(modal)) = self.modal.as_mut() {
            modal.loading = true;
        }
        self.inflight_jobs.insert(job_key);
        let core = Arc::clone(&self.core);
        let query = trimmed.to_string();
        self.background_jobs.spawn_local(async move {
            let results = search_users_query(core.as_ref(), &query).await?;
            Ok(BackgroundJobResult::UserSearch { query, results })
        });
    }

    pub(super) fn start_jump_lookup(&mut self, input: &str) {
        let lookup_input = input.trim().to_string();
        if lookup_input.is_empty() {
            self.set_error("Record number or URL cannot be empty.");
            return;
        }

        let job_key = BackgroundJobKey::JumpLookup(lookup_input.clone());
        if self.inflight_jobs.contains(&job_key) {
            return;
        }

        self.inflight_jobs.insert(job_key);
        let core = Arc::clone(&self.core);
        let lookup_input_for_job = lookup_input.clone();
        self.background_jobs.spawn_local(async move {
            let result = async {
                let record = lookup_record_ref(core.as_ref(), &lookup_input_for_job).await?;
                let Some(kind) = record.kind else {
                    return Err(SnowError::Api(format!(
                        "Jump is not supported for {} yet.",
                        record.table
                    )));
                };
                let relation = child_relation(kind);
                let (detail, children, approval) = tokio::try_join!(
                    fetch_detail(core.as_ref(), kind, &record),
                    fetch_children(core.as_ref(), &record, relation),
                    fetch_pending_approval(core.as_ref(), &record),
                )?;
                Ok((record, detail, children, approval))
            }
            .await;
            Ok(BackgroundJobResult::JumpLookup {
                number: lookup_input_for_job,
                result,
            })
        });
        self.set_status(format!("Jumping to {lookup_input}..."), false);
    }

    pub(super) fn start_mutation(&mut self, request: MutationRequest) {
        let Some(job_key) = request.job_key() else {
            return;
        };
        if self.inflight_jobs.contains(&job_key) {
            return;
        }
        self.inflight_jobs.insert(job_key);
        let core = Arc::clone(&self.core);
        self.background_jobs.spawn_local(async move {
            let message = request.execute(core.as_ref()).await?;
            Ok(BackgroundJobResult::Mutation { message })
        });
    }

    pub(super) fn open_selected_record(&mut self) -> Result<(), SnowError> {
        let selected = match self.active_row() {
            Some(row) => row.record.clone(),
            None => {
                self.set_error("No record selected.");
                return Ok(());
            }
        };

        let kind = match selected.kind {
            Some(kind) => kind,
            None => {
                self.set_error("Drill-in not supported for this record type.");
                return Ok(());
            }
        };

        let relation = child_relation(kind);
        let detail_key = detail_cache_key(&selected.table, &selected.sys_id);
        let children_key = children_cache_key(&selected.table, &selected.sys_id);
        let approval_key = approval_cache_key(&selected.table, &selected.sys_id);
        let parent_detail = self
            .detail_cache
            .get(&detail_key)
            .cloned()
            .unwrap_or_else(|| placeholder_detail(&selected));
        let children = self
            .children_cache
            .get(&children_key)
            .cloned()
            .unwrap_or_default();
        let approval = selected
            .approval
            .clone()
            .or_else(|| self.approval_cache.get(&approval_key).cloned().flatten());
        let browser = BrowserState {
            parent: selected.clone(),
            parent_kind: kind,
            relation,
            parent_detail: parent_detail.clone(),
            children,
            selected: BrowserSelection::Parent,
            task_sla_expanded: false,
            detail_cache: HashMap::new(),
            approval,
            search: super::QuickSearch::new(),
            child_state_filter: None,
        };

        self.prefetch_queue = super::build_browser_prefetch_queue(&browser, self.show_closed);
        if !self.detail_cache.contains_key(&detail_key) {
            self.queue_prefetch_priority(super::PrefetchTask::Detail(selected.clone()));
        }
        if relation.child_table.is_some() && !self.children_cache.contains_key(&children_key) {
            self.queue_prefetch_priority(super::PrefetchTask::Children(selected.clone()));
        }
        if selected.approval.is_some() && !self.approval_cache.contains_key(&approval_key) {
            self.queue_prefetch(super::PrefetchTask::Approval(selected.clone()));
        }
        self.browser = Some(browser);
        self.screen = super::Screen::Browser;
        self.set_status("Opened record browser.", false);
        Ok(())
    }

    fn after_mutation(&mut self, message: &str) -> Result<(), SnowError> {
        let browser_parent = self.browser.as_ref().map(|browser| browser.parent.clone());
        let task_sla_expanded = self
            .browser
            .as_ref()
            .is_some_and(|browser| browser.task_sla_expanded);
        self.detail_cache.clear();
        self.children_cache.clear();
        self.approval_cache.clear();
        self.prefetch_queue.clear();
        if let Some(parent) = browser_parent {
            let relation = parent.kind.map(child_relation).unwrap_or(ChildRelation {
                parent_kind: EntityKind::Incident,
                child_kind: None,
                child_table: None,
                child_link_field: None,
            });
            self.browser = Some(BrowserState {
                parent: parent.clone(),
                parent_kind: parent.kind.unwrap_or(EntityKind::Incident),
                relation,
                parent_detail: placeholder_detail(&parent),
                children: Vec::new(),
                selected: BrowserSelection::Parent,
                task_sla_expanded,
                detail_cache: HashMap::new(),
                approval: parent.approval.clone(),
                search: super::QuickSearch::new(),
                child_state_filter: None,
            });
            self.queue_prefetch_priority(super::PrefetchTask::Detail(parent.clone()));
            if relation.child_table.is_some() {
                self.queue_prefetch_priority(super::PrefetchTask::Children(parent.clone()));
            }
            if parent.approval.is_some() {
                self.queue_prefetch(super::PrefetchTask::Approval(parent));
            }
        }
        self.start_dashboard_refresh(false);
        self.set_status(message, false);
        Ok(())
    }

    pub(super) fn active_approval(&self) -> Option<ApprovalRef> {
        match self.screen {
            super::Screen::Dashboard => self
                .active_row()
                .and_then(|row| row.record.approval.clone()),
            super::Screen::Browser => self
                .browser
                .as_ref()
                .and_then(|browser| browser.approval.clone()),
            super::Screen::Knowledge => None,
        }
    }
}

fn apply_tab_result(
    dashboard: &mut super::DashboardState,
    tab: DashboardTab,
    result: Result<Vec<DashboardRow>, SnowError>,
) {
    match result {
        Ok(rows) => dashboard.set_rows(tab, rows),
        Err(err) => dashboard.set_error(tab, err.to_string()),
    }
}

async fn fetch_approvals(core: &TuiClient) -> Result<Vec<DashboardRow>, SnowError> {
    let approvals = core.my_approvals_fresh().await?;
    Ok(approvals.into_iter().map(map_approval_row).collect())
}

async fn fetch_tasks(core: &TuiClient) -> Result<Vec<DashboardRow>, SnowError> {
    let mut rows: Vec<_> = core
        .my_tasks_fresh()
        .await?
        .into_iter()
        .map(map_task_row)
        .collect();
    rows.sort_by(|a, b| a.record.number.cmp(&b.record.number));
    Ok(rows)
}

async fn fetch_stories(core: &TuiClient) -> Result<Vec<DashboardRow>, SnowError> {
    Ok(core
        .my_stories_fresh()
        .await?
        .into_iter()
        .map(map_story_row)
        .collect())
}

async fn fetch_incidents(core: &TuiClient) -> Result<Vec<DashboardRow>, SnowError> {
    let records = core.my_incidents_fresh().await?;
    let task_sla_statuses = fetch_incident_task_sla_statuses(core, &records).await;
    Ok(records
        .into_iter()
        .map(|record| {
            let task_sla = match &task_sla_statuses {
                Ok(statuses) => statuses
                    .get(&record.sys_id)
                    .map(incident_task_sla_dashboard_label)
                    .unwrap_or_else(|| "-".to_string()),
                Err(_) => "SLA error".to_string(),
            };
            map_incident_row(record, task_sla)
        })
        .collect())
}

async fn fetch_incident_task_sla_statuses(
    core: &TuiClient,
    records: &[SnowRecord],
) -> Result<HashMap<String, TaskSlaStatus>, SnowError> {
    if records.is_empty() {
        return Ok(HashMap::new());
    }

    let parents: Vec<_> = records
        .iter()
        .map(|record| TaskSlaParentRef {
            record_number: record.number.clone(),
            record_table: record.table.clone(),
            record_sys_id: record.sys_id.clone(),
        })
        .collect();
    core.task_sla_status_for_tasks(&parents).await
}

pub(super) async fn fetch_children(
    core: &TuiClient,
    parent: &RecordRef,
    _relation: ChildRelation,
) -> Result<Vec<RecordRef>, SnowError> {
    Ok(core
        .get_children(&parent.number)
        .await?
        .into_iter()
        .map(map_child_record)
        .collect())
}

pub(super) async fn fetch_detail(
    core: &TuiClient,
    kind: EntityKind,
    target: &RecordRef,
) -> Result<RecordDetail, SnowError> {
    join_detail_and_task_sla(
        fetch_detail_without_task_sla(core, kind, target),
        core.task_sla_status(&target.number),
    )
    .await
}

async fn join_detail_and_task_sla<D, S>(
    detail_fetch: D,
    task_sla_fetch: S,
) -> Result<RecordDetail, SnowError>
where
    D: Future<Output = Result<RecordDetail, SnowError>>,
    S: Future<Output = Result<TaskSlaStatus, SnowError>>,
{
    let (detail_result, task_sla_result) = tokio::join!(detail_fetch, task_sla_fetch);
    let mut detail = detail_result?;
    detail.task_sla = match task_sla_result {
        Ok(status) => TaskSlaDetail::Status(Box::new(status)),
        Err(err) => TaskSlaDetail::Error(err.to_string()),
    };
    Ok(detail)
}

async fn fetch_detail_without_task_sla(
    core: &TuiClient,
    kind: EntityKind,
    target: &RecordRef,
) -> Result<RecordDetail, SnowError> {
    let record = core
        .get_record_fresh(&target.number)
        .await?
        .ok_or_else(|| SnowError::NotFound(format!("{} not found.", target.number)))?;
    let raw_record = cached_to_record(&record);
    let page = detail_page_model(kind);
    let mut extra_sections = detail_extra_sections(&page, &raw_record);
    if kind == EntityKind::ChangeRequest
        && let Some(approvers) = fetch_pending_approvers(core, &target.sys_id).await?
    {
        extra_sections.push(("Pending Approvers".to_string(), approvers));
    }

    Ok(RecordDetail {
        field_lines: detail_field_lines(&page, &raw_record),
        page,
        description: (!record.description.trim().is_empty())
            .then(|| display::strip_html(&record.description)),
        extra_sections,
        work_notes: raw_record
            .parse_journal("work_notes")
            .into_iter()
            .filter(|entry| !entry.is_email())
            .take(MAX_WORK_NOTES)
            .collect(),
        notes_loaded: true,
        record: raw_record,
        task_sla: TaskSlaDetail::Loading,
    })
}

async fn fetch_choices(
    core: &TuiClient,
    table: &str,
    field: &str,
) -> Result<Vec<ChoiceItem>, SnowError> {
    Ok(core
        .field_choices(table, field)
        .await?
        .into_iter()
        .map(|choice| ChoiceItem {
            label: choice.label,
            value: choice.value,
        })
        .collect())
}

async fn fetch_choices_for_state_modal(
    core: &TuiClient,
    table: &str,
    field: &str,
) -> Result<Vec<ChoiceItem>, SnowError> {
    let curated = curated_choices(table, field);
    if !curated.is_empty() {
        return Ok(curated);
    }

    let choices = fetch_choices(core, table, field).await?;
    if choices.is_empty() {
        return Err(SnowError::Api(format!(
            "No choices returned for {table}.{field}."
        )));
    }
    Ok(choices)
}

async fn search_users_query(core: &TuiClient, query: &str) -> Result<Vec<UserChoice>, SnowError> {
    let Some(local) = core.as_local_core() else {
        return Err(SnowError::Api(
            "User search is not supported in daemon-backed TUI mode yet.".to_string(),
        ));
    };
    let results = local
        .client()
        .table("sys_user")
        .contains("name", query)
        .fields(&["sys_id", "name", "user_name"])
        .display_value(DisplayValue::Display)
        .limit(20)
        .execute()
        .await?;

    Ok(results
        .records
        .into_iter()
        .map(|record| {
            let sys_id = record.sys_id.clone();
            let label = format!(
                "{} ({})",
                record.get_str("name").unwrap_or("-"),
                record.get_str("user_name").unwrap_or("-")
            );
            UserChoice { sys_id, label }
        })
        .collect())
}

fn curated_choices(table: &str, field: &str) -> Vec<ChoiceItem> {
    if field != "state" {
        return Vec::new();
    }

    let pairs: &[(&str, &str)] = match table {
        "task" | "change_task" | "rm_scrum_task" | "sc_task" => &[
            ("-5", "Pending"),
            ("1", "Open"),
            ("2", "Work in Progress"),
            ("3", "Closed Complete"),
            ("4", "Closed Incomplete"),
            ("7", "Closed Skipped"),
        ],
        "incident" => &[
            ("1", "New"),
            ("2", "In Progress"),
            ("3", "On Hold"),
            ("6", "Resolved"),
            ("7", "Closed"),
        ],
        _ => &[],
    };

    pairs
        .iter()
        .map(|(value, label)| ChoiceItem {
            value: (*value).to_string(),
            label: (*label).to_string(),
        })
        .collect()
}

pub(super) fn detail_cache_key(table: &str, sys_id: &str) -> String {
    format!("{table}:{sys_id}")
}

pub(super) fn children_cache_key(table: &str, sys_id: &str) -> String {
    format!("{table}:{sys_id}:children")
}

pub(super) fn approval_cache_key(table: &str, sys_id: &str) -> String {
    format!("{table}:{sys_id}:approval")
}

fn batch_children_job_key(relation: ChildRelation, parents: &[RecordRef]) -> String {
    let child_table = relation.child_table.unwrap_or("-");
    let parent_ids = parents
        .iter()
        .map(|record| record.sys_id.as_str())
        .collect::<Vec<_>>()
        .join(",");
    format!("{child_table}:{parent_ids}")
}

fn map_child_record(record: SnowRecord) -> RecordRef {
    let table = canonical_record_table_for_number(&record.table, &record.number);
    let is_resource_plan = table == "resource_plan";
    let short_description = if table == "resource_plan" && record.short_description.is_empty() {
        cached_display(&record, "task").unwrap_or_else(|| "Resource plan".to_string())
    } else {
        record.short_description.clone()
    };
    RecordRef {
        kind: entity_kind_for_table(&table),
        table,
        sys_id: record.sys_id.clone(),
        number: record.number.clone(),
        short_description,
        assigned_to: if is_resource_plan {
            resource_plan_resource_label(&record)
        } else {
            cached_display(&record, "assigned_to")
        },
        planned_start: cached_planned_start(&record),
        planned_end: cached_planned_end(&record),
        planned_hours: cached_display(&record, "planned_hours"),
        allocated_hours: cached_display(&record, "allocated_hours"),
        confirmed_hours: cached_display(&record, "confirmed_hours"),
        state_label: cached_display(&record, "state")
            .or_else(|| (!record.state.is_empty()).then(|| record.state.clone())),
        state_value: cached_value(&record, "state"),
        record_class: cached_value(&record, "sys_class_name"),
        parent_table: record
            .parent
            .as_ref()
            .map(|parent| canonical_record_table(&parent.table)),
        parent_sys_id: record.parent.as_ref().map(|parent| parent.sys_id.clone()),
        parent_number: record.parent.as_ref().map(|parent| parent.number.clone()),
        approval: None,
    }
}

fn resource_plan_resource_label(record: &SnowRecord) -> Option<String> {
    cached_display(record, "user_resource")
        .or_else(|| cached_display(record, "group_resource"))
        .or_else(|| cached_display(record, "resource_type"))
}

fn map_task_row(record: SnowRecord) -> DashboardRow {
    let row_record = map_child_record(record.clone());
    let parent_number = row_record
        .parent_number
        .clone()
        .unwrap_or_else(|| "-".to_string());
    DashboardRow {
        cells: vec![
            row_record.number.clone(),
            row_record.short_description.clone(),
            row_record
                .state_label
                .clone()
                .unwrap_or_else(|| "-".to_string()),
            row_record
                .assigned_to
                .clone()
                .unwrap_or_else(|| "-".to_string()),
            parent_number,
        ],
        record: row_record,
    }
}

fn map_story_row(record: SnowRecord) -> DashboardRow {
    let row_record = RecordRef {
        kind: Some(EntityKind::Story),
        table: record.table.clone(),
        sys_id: record.sys_id.clone(),
        number: record.number.clone(),
        short_description: record.short_description.clone(),
        assigned_to: cached_display(&record, "assigned_to"),
        planned_start: cached_value(&record, "start_date"),
        planned_end: None,
        planned_hours: None,
        allocated_hours: None,
        confirmed_hours: None,
        state_label: cached_display(&record, "state")
            .or_else(|| (!record.state.is_empty()).then(|| record.state.clone())),
        state_value: cached_value(&record, "state"),
        record_class: cached_value(&record, "sys_class_name"),
        parent_table: None,
        parent_sys_id: None,
        parent_number: None,
        approval: None,
    };
    DashboardRow {
        cells: vec![
            row_record.number.clone(),
            row_record.short_description.clone(),
            row_record
                .state_label
                .clone()
                .unwrap_or_else(|| "-".to_string()),
            cached_value(&record, "story_points").unwrap_or_else(|| "-".to_string()),
            cached_display(&record, "sprint").unwrap_or_else(|| "-".to_string()),
        ],
        record: row_record,
    }
}

fn map_incident_row(record: SnowRecord, task_sla: String) -> DashboardRow {
    let row_record = RecordRef {
        kind: Some(EntityKind::Incident),
        table: record.table.clone(),
        sys_id: record.sys_id.clone(),
        number: record.number.clone(),
        short_description: record.short_description.clone(),
        assigned_to: cached_display(&record, "assigned_to"),
        planned_start: None,
        planned_end: None,
        planned_hours: None,
        allocated_hours: None,
        confirmed_hours: None,
        state_label: cached_display(&record, "state")
            .or_else(|| (!record.state.is_empty()).then(|| record.state.clone())),
        state_value: cached_value(&record, "state"),
        record_class: cached_value(&record, "sys_class_name"),
        parent_table: None,
        parent_sys_id: None,
        parent_number: None,
        approval: None,
    };
    DashboardRow {
        cells: vec![
            row_record.number.clone(),
            row_record.short_description.clone(),
            row_record
                .state_label
                .clone()
                .unwrap_or_else(|| "-".to_string()),
            cached_display(&record, "priority").unwrap_or_else(|| "-".to_string()),
            task_sla,
            cached_display(&record, "opened_at").unwrap_or_else(|| "-".to_string()),
        ],
        record: row_record,
    }
}

fn incident_task_sla_dashboard_label(status: &TaskSlaStatus) -> String {
    match status.readable {
        TaskSlaReadability::ReadableRows => {
            let mut parts = vec![format!(
                "A:{} B:{}",
                status.summary.active, status.summary.breached
            )];
            if let Some(next) = status.summary.next_breach.as_ref() {
                let time_left = core_display::format_time_left(next.time_left.as_deref());
                if time_left != "-" {
                    parts.push(format!("next {time_left}"));
                } else if let Some(planned_end) = next
                    .planned_end_time
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                {
                    parts.push(format!("next {planned_end}"));
                }
            }
            let highest =
                core_display::format_business_elapsed(status.summary.highest_business_elapsed);
            if highest != "-" {
                parts.push(highest);
            }
            parts.join(" | ")
        }
        TaskSlaReadability::EmptyOrAclRestricted => "none/ACL".to_string(),
        TaskSlaReadability::ParentNotFound => "parent missing".to_string(),
        TaskSlaReadability::NotApplicable => "n/a".to_string(),
    }
}

fn map_approval_row(approval: CachedApprovalRecord) -> DashboardRow {
    let record = &approval.record;
    let target_table =
        canonical_record_table_for_number(&approval.target.table, &approval.target.number);
    let target = RecordRef {
        kind: entity_kind_for_table(&target_table),
        table: target_table.clone(),
        sys_id: approval.target.sys_id.clone(),
        number: approval.target.number.clone(),
        short_description: record
            .fields
            .get("sysapproval.short_description")
            .map(|field| {
                field
                    .display_value
                    .clone()
                    .unwrap_or_else(|| field.value.clone())
            })
            .unwrap_or_else(|| record.short_description.clone()),
        assigned_to: None,
        planned_start: None,
        planned_end: None,
        planned_hours: None,
        allocated_hours: None,
        confirmed_hours: None,
        state_label: record.fields.get("sysapproval.state").and_then(|field| {
            field
                .display_value
                .clone()
                .or_else(|| Some(field.value.clone()))
        }),
        state_value: record
            .fields
            .get("sysapproval.state")
            .map(|field| field.value.clone()),
        record_class: record
            .fields
            .get("sysapproval.sys_class_name")
            .map(|field| field.value.clone()),
        parent_table: None,
        parent_sys_id: None,
        parent_number: None,
        approval: Some(ApprovalRef {
            approval_sys_id: record.sys_id.clone(),
            document_id: cached_value(record, "document_id").unwrap_or_default(),
            source_table: target_table,
            source_sys_id: approval.target.sys_id.clone(),
            source_number: approval.target.number.clone(),
        }),
    };

    DashboardRow {
        cells: vec![
            target.number.clone(),
            target.short_description.clone(),
            target
                .state_label
                .clone()
                .unwrap_or_else(|| "-".to_string()),
            relative_time(cached_display(record, "sys_created_on").as_deref()),
        ],
        record: target,
    }
}

fn cached_to_record(record: &SnowRecord) -> Record {
    let mut raw = Record::new(&record.table, &record.sys_id);
    raw.set(
        "sys_id",
        servicenow_rs::prelude::FieldValue {
            value: Some(serde_json::Value::String(record.sys_id.clone())),
            display_value: Some(record.sys_id.clone()),
            link: None,
        },
    );
    for (field_name, field_value) in &record.fields {
        raw.set(
            field_name.clone(),
            servicenow_rs::prelude::FieldValue {
                value: Some(serde_json::Value::String(field_value.value.clone())),
                display_value: field_value.display_value.clone(),
                link: None,
            },
        );
    }
    raw
}

fn cached_display(record: &SnowRecord, field_name: &str) -> Option<String> {
    record.fields.get(field_name).and_then(|field| {
        field
            .display_value
            .clone()
            .or_else(|| (!field.value.is_empty()).then(|| field.value.clone()))
    })
}

fn cached_value(record: &SnowRecord, field_name: &str) -> Option<String> {
    record
        .fields
        .get(field_name)
        .and_then(|field| (!field.value.is_empty()).then(|| field.value.clone()))
}

fn cached_planned_start(record: &SnowRecord) -> Option<String> {
    [
        "planned_start_date",
        "planned_start",
        "work_start",
        "expected_start",
        "start_date",
    ]
    .into_iter()
    .find_map(|field| cached_display(record, field))
}

fn cached_planned_end(record: &SnowRecord) -> Option<String> {
    [
        "planned_end_date",
        "planned_end",
        "work_end",
        "expected_end",
        "end_date",
    ]
    .into_iter()
    .find_map(|field| cached_display(record, field))
}

pub(super) fn placeholder_detail(record: &RecordRef) -> RecordDetail {
    let mut raw = Record::new(&record.table, &record.sys_id);
    raw.set(
        "number",
        servicenow_rs::prelude::FieldValue::from_display(record.number.clone()),
    );
    raw.set(
        "short_description",
        servicenow_rs::prelude::FieldValue::from_display(record.short_description.clone()),
    );
    RecordDetail {
        page: record
            .kind
            .map(detail_page_model)
            .unwrap_or_else(generic_detail_page_model),
        record: raw,
        description: Some("Loading...".to_string()),
        extra_sections: Vec::new(),
        field_lines: vec![("Status".to_string(), "Loading...".to_string())],
        work_notes: Vec::new(),
        notes_loaded: false,
        task_sla: TaskSlaDetail::Loading,
    }
}

fn detail_error_placeholder(record: &RecordRef, err: &SnowError) -> RecordDetail {
    let mut detail = placeholder_detail(record);
    detail.field_lines = vec![
        ("Status".to_string(), "Cached row".to_string()),
        ("Live Detail".to_string(), format!("Unavailable: {err}")),
    ];
    detail.description = Some(
        "Live detail refresh failed. This cached record may be stale or inaccessible.".to_string(),
    );
    detail.work_notes = Vec::new();
    detail.notes_loaded = true;
    detail.task_sla =
        TaskSlaDetail::Error(format!("Skipped because live detail refresh failed: {err}"));
    detail
}

fn unsupported_detail_placeholder(record: &RecordRef) -> RecordDetail {
    detail_error_placeholder(
        record,
        &SnowError::Api(format!(
            "Detail prefetch is not supported for {} records.",
            record.table
        )),
    )
}

pub(super) async fn fetch_pending_approval(
    core: &TuiClient,
    record: &RecordRef,
) -> Result<Option<ApprovalRef>, SnowError> {
    Ok(core
        .pending_approval_for(&record.sys_id)
        .await?
        .map(|approval| ApprovalRef {
            approval_sys_id: approval.record.sys_id.clone(),
            document_id: cached_value(&approval.record, "document_id").unwrap_or_default(),
            source_table: approval.target.table.clone(),
            source_sys_id: record.sys_id.clone(),
            source_number: record.number.clone(),
        }))
}

async fn fetch_pending_approvers(
    _core: &TuiClient,
    _change_sys_id: &str,
) -> Result<Option<String>, SnowError> {
    Ok(None)
}

async fn lookup_record_ref(core: &TuiClient, input: &str) -> Result<RecordRef, SnowError> {
    if let Some((table, sys_id)) = parse_record_url(input) {
        return lookup_record_ref_by_sys_id(core, &table, &sys_id).await;
    }

    lookup_record_ref_by_number(core, input).await
}

async fn lookup_record_ref_by_number(
    core: &TuiClient,
    number: &str,
) -> Result<RecordRef, SnowError> {
    let number = number.trim().to_ascii_uppercase();
    let record = match core.get_record(&number).await? {
        Some(record) => record,
        None => core
            .get_record_fresh(&number)
            .await?
            .ok_or_else(|| SnowError::NotFound(format!("{number} not found.")))?,
    };
    Ok(map_child_record(record))
}

async fn lookup_record_ref_by_sys_id(
    core: &TuiClient,
    table: &str,
    sys_id: &str,
) -> Result<RecordRef, SnowError> {
    let record = core
        .get_record_by_table_sys_id_fresh(table, sys_id)
        .await?
        .ok_or_else(|| SnowError::NotFound(format!("{table}:{sys_id} not found.")))?;
    Ok(map_child_record(record))
}

fn parse_record_url(input: &str) -> Option<(String, String)> {
    let input = input.trim();
    if !input.starts_with("http://") && !input.starts_with("https://") {
        return None;
    }

    let (_, rest) = input.split_once("://")?;
    let path_and_query = rest.split_once('/').map(|(_, value)| value).unwrap_or("");
    let direct = path_and_query
        .split_once('?')
        .and_then(|(path, query)| parse_table_and_sys_id(path, query));
    if direct.is_some() {
        return direct;
    }

    let nav_to_target = path_and_query
        .split_once('?')
        .and_then(|(path, query)| {
            (path == "nav_to.do")
                .then_some(query)
                .and_then(extract_query_param)
        })
        .and_then(|uri| percent_decode(&uri))
        .and_then(|target| {
            target
                .split_once('?')
                .and_then(|(path, query)| parse_table_and_sys_id(path, query))
        });
    if nav_to_target.is_some() {
        return nav_to_target;
    }

    let encoded_target = path_and_query
        .strip_prefix("now/nav/ui/classic/params/target/")
        .and_then(percent_decode)
        .and_then(|target| {
            target
                .split_once('?')
                .and_then(|(path, query)| parse_table_and_sys_id(path, query))
        });
    if encoded_target.is_some() {
        return encoded_target;
    }

    None
}

fn extract_query_param(query: &str) -> Option<String> {
    query
        .split('&')
        .filter_map(|part| part.split_once('='))
        .find_map(|(key, value)| (key == "uri").then_some(value.to_string()))
}

fn parse_table_and_sys_id(path: &str, query: &str) -> Option<(String, String)> {
    let table = path.strip_suffix(".do")?;
    let sys_id = query
        .split('&')
        .filter_map(|part| part.split_once('='))
        .find_map(|(key, value)| (key == "sys_id").then_some(value))?;
    if table.is_empty() || sys_id.is_empty() {
        return None;
    }

    Some((table.to_string(), sys_id.to_string()))
}

fn percent_decode(input: &str) -> Option<String> {
    let bytes = input.as_bytes();
    let mut decoded = String::with_capacity(input.len());
    let mut index = 0;

    while index < bytes.len() {
        match bytes[index] {
            b'%' if index + 2 < bytes.len() => {
                let hi = decode_hex(bytes[index + 1])?;
                let lo = decode_hex(bytes[index + 2])?;
                decoded.push((hi * 16 + lo) as char);
                index += 3;
            }
            b'+' => {
                decoded.push(' ');
                index += 1;
            }
            byte => {
                decoded.push(byte as char);
                index += 1;
            }
        }
    }

    Some(decoded)
}

fn decode_hex(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

pub(super) fn entity_kind_for_table(table: &str) -> Option<EntityKind> {
    let normalized = normalize_table_name(table);
    if is_change_request_table(&normalized) {
        return Some(EntityKind::ChangeRequest);
    }

    match normalized.as_str() {
        "task" => Some(EntityKind::Task),
        "change_task" => Some(EntityKind::ChangeTask),
        "dmn_demand" => Some(EntityKind::Demand),
        "pm_project" => Some(EntityKind::Project),
        "resource_plan" => Some(EntityKind::ResourcePlan),
        "rm_story" => Some(EntityKind::Story),
        "rm_scrum_task" => Some(EntityKind::ScrumTask),
        "sc_req_item" => Some(EntityKind::RequestItem),
        "sc_task" => Some(EntityKind::CatalogTask),
        "incident" => Some(EntityKind::Incident),
        _ => None,
    }
}

fn record_kind(record: &RecordRef) -> Option<EntityKind> {
    record.kind.or_else(|| entity_kind_for_table(&record.table))
}

fn canonical_record_table(table: &str) -> String {
    let normalized = normalize_table_name(table);
    if is_change_request_table(&normalized) {
        "change_request".to_string()
    } else {
        normalized
    }
}

fn canonical_record_table_for_number(table: &str, number: &str) -> String {
    let normalized = normalize_table_name(table);
    if is_change_request_table(&normalized) || is_change_request_number(number) {
        "change_request".to_string()
    } else {
        normalized
    }
}

fn normalize_table_name(table: &str) -> String {
    table.trim().to_ascii_lowercase().replace([' ', '-'], "_")
}

fn is_change_request_table(normalized: &str) -> bool {
    matches!(
        normalized,
        "change" | "change_request" | "normal_change" | "standard_change" | "emergency_change"
    ) || normalized.starts_with("change_request_")
}

fn is_change_request_number(number: &str) -> bool {
    number.trim().to_ascii_uppercase().starts_with("CHG")
}

pub(super) fn child_relation(kind: EntityKind) -> ChildRelation {
    match kind {
        EntityKind::ChangeRequest => ChildRelation {
            parent_kind: kind,
            child_kind: Some(EntityKind::ChangeTask),
            child_table: Some("change_task"),
            child_link_field: Some("change_request"),
        },
        EntityKind::Story => ChildRelation {
            parent_kind: kind,
            child_kind: Some(EntityKind::ScrumTask),
            child_table: Some("rm_scrum_task"),
            child_link_field: Some("story"),
        },
        EntityKind::RequestItem => ChildRelation {
            parent_kind: kind,
            child_kind: Some(EntityKind::CatalogTask),
            child_table: Some("sc_task"),
            child_link_field: Some("request_item"),
        },
        EntityKind::Demand | EntityKind::Project => ChildRelation {
            parent_kind: kind,
            child_kind: Some(EntityKind::ResourcePlan),
            child_table: Some("resource_plan"),
            child_link_field: Some("task"),
        },
        EntityKind::ResourcePlan => ChildRelation {
            parent_kind: kind,
            child_kind: None,
            child_table: None,
            child_link_field: None,
        },
        EntityKind::Task
        | EntityKind::ChangeTask
        | EntityKind::ScrumTask
        | EntityKind::CatalogTask
        | EntityKind::Incident => ChildRelation {
            parent_kind: kind,
            child_kind: None,
            child_table: None,
            child_link_field: None,
        },
    }
}

pub(super) fn lifecycle_field(kind: EntityKind) -> &'static str {
    match kind {
        EntityKind::Task
        | EntityKind::ChangeRequest
        | EntityKind::ChangeTask
        | EntityKind::Demand
        | EntityKind::Project
        | EntityKind::ResourcePlan
        | EntityKind::Story
        | EntityKind::ScrumTask
        | EntityKind::RequestItem
        | EntityKind::CatalogTask
        | EntityKind::Incident => "state",
    }
}

const CHANGE_TASK_SUMMARY_COLUMNS: &[DetailChildColumn] = &[
    DetailChildColumn {
        header: "CTASK",
        width: DetailColumnWidth::Length(14),
        value: DetailChildValue::Number,
    },
    DetailChildColumn {
        header: "Assigned To",
        width: DetailColumnWidth::Percentage(32),
        value: DetailChildValue::AssignedTo,
    },
    DetailChildColumn {
        header: "Planned Start",
        width: DetailColumnWidth::Length(16),
        value: DetailChildValue::PlannedStart,
    },
    DetailChildColumn {
        header: "State",
        width: DetailColumnWidth::Length(14),
        value: DetailChildValue::State,
    },
];

const STORY_TASK_SUMMARY_COLUMNS: &[DetailChildColumn] = &[
    DetailChildColumn {
        header: "Task",
        width: DetailColumnWidth::Length(14),
        value: DetailChildValue::Number,
    },
    DetailChildColumn {
        header: "Assigned To",
        width: DetailColumnWidth::Percentage(26),
        value: DetailChildValue::AssignedTo,
    },
    DetailChildColumn {
        header: "State",
        width: DetailColumnWidth::Length(14),
        value: DetailChildValue::State,
    },
    DetailChildColumn {
        header: "Short Description",
        width: DetailColumnWidth::Percentage(40),
        value: DetailChildValue::ShortDescription,
    },
];

const RESOURCE_PLAN_SUMMARY_COLUMNS: &[DetailChildColumn] = &[
    DetailChildColumn {
        header: "RPLN",
        width: DetailColumnWidth::Length(14),
        value: DetailChildValue::Number,
    },
    DetailChildColumn {
        header: "Resource",
        width: DetailColumnWidth::Percentage(28),
        value: DetailChildValue::Resource,
    },
    DetailChildColumn {
        header: "Start",
        width: DetailColumnWidth::Length(12),
        value: DetailChildValue::PlannedStart,
    },
    DetailChildColumn {
        header: "End",
        width: DetailColumnWidth::Length(12),
        value: DetailChildValue::PlannedEnd,
    },
    DetailChildColumn {
        header: "Hours",
        width: DetailColumnWidth::Length(16),
        value: DetailChildValue::Hours,
    },
    DetailChildColumn {
        header: "State",
        width: DetailColumnWidth::Length(14),
        value: DetailChildValue::State,
    },
];

fn detail_page_model(kind: EntityKind) -> DetailPageModel {
    match kind {
        EntityKind::Task => DetailPageModel {
            title: "Task Details",
            field_specs: vec![
                field("State", &["state"]),
                field("Assigned To", &["assigned_to"]),
                field("Group", &["assignment_group"]),
                field("Opened", &["opened_at", "opened"]),
                field(
                    "Planned Start",
                    &[
                        "planned_start_date",
                        "planned_start",
                        "work_start",
                        "expected_start",
                        "start_date",
                    ],
                ),
                field("Due", &["due_date"]),
            ],
            section_specs: Vec::new(),
            sections_title: "Details",
            child_summary: None,
        },
        EntityKind::ChangeRequest => DetailPageModel {
            title: "Change Details",
            field_specs: vec![
                field("State", &["state"]),
                field("Category", &["category"]),
                field("Type", &["type"]),
                field("Assigned To", &["assigned_to"]),
                field("Group", &["assignment_group"]),
                field("Approval", &["approval"]),
                field("Risk", &["risk"]),
                field("Impact", &["impact"]),
                field("Planned Start", &["start_date"]),
                field("Planned End", &["end_date"]),
            ],
            section_specs: vec![
                section("Change Justification", "justification"),
                section("Change Plan", "change_plan"),
                section("Implementation Steps", "implementation_plan"),
                section("Backout Plan", "backout_plan"),
                section("Test Plan", "test_plan"),
            ],
            sections_title: "Change Detail",
            child_summary: Some(child_summary(
                "Change Tasks",
                "Loading change tasks...",
                "No change tasks found.",
                "No change tasks match the current filters.",
                CHANGE_TASK_SUMMARY_COLUMNS,
            )),
        },
        EntityKind::ChangeTask | EntityKind::ScrumTask | EntityKind::CatalogTask => {
            DetailPageModel {
                title: match kind {
                    EntityKind::ChangeTask => "Change Task Details",
                    EntityKind::ScrumTask => "Story Task Details",
                    EntityKind::CatalogTask => "Catalog Task Details",
                    _ => "Task Details",
                },
                field_specs: vec![
                    field("State", &["state"]),
                    field("Assigned To", &["assigned_to"]),
                    field("Group", &["assignment_group"]),
                    field("Parent", &["change_request", "story", "request_item"]),
                    field(
                        "Planned Start",
                        &[
                            "planned_start_date",
                            "planned_start",
                            "work_start",
                            "expected_start",
                            "start_date",
                        ],
                    ),
                    field("Due", &["due_date"]),
                ],
                section_specs: Vec::new(),
                sections_title: "Details",
                child_summary: None,
            }
        }
        EntityKind::Demand => DetailPageModel {
            title: "Demand Details",
            field_specs: vec![
                field("State", &["state"]),
                field("Priority", &["priority"]),
                field("Requested By", &["requested_by"]),
                field("Assigned To", &["assigned_to"]),
                field("Manager", &["demand_manager"]),
                field("Start", &["start_date"]),
                field("End", &["end_date"]),
            ],
            section_specs: vec![
                section("Business Case", "business_case"),
                section("Goals", "goals"),
            ],
            sections_title: "Demand Detail",
            child_summary: Some(child_summary(
                "Resource Plans",
                "Loading resource plans...",
                "No resource plans found.",
                "No resource plans match the current filters.",
                RESOURCE_PLAN_SUMMARY_COLUMNS,
            )),
        },
        EntityKind::Project => DetailPageModel {
            title: "Project Details",
            field_specs: vec![
                field("State", &["state"]),
                field("Demand", &["demand"]),
                field("Manager", &["project_manager"]),
                field("Start", &["start_date"]),
                field("End", &["end_date"]),
                field("% Complete", &["percent_complete"]),
            ],
            section_specs: vec![
                section("Goals", "goals"),
                section("Business Case", "business_case"),
            ],
            sections_title: "Project Detail",
            child_summary: Some(child_summary(
                "Resource Plans",
                "Loading resource plans...",
                "No resource plans found.",
                "No resource plans match the current filters.",
                RESOURCE_PLAN_SUMMARY_COLUMNS,
            )),
        },
        EntityKind::ResourcePlan => DetailPageModel {
            title: "Resource Plan Details",
            field_specs: vec![
                field("State", &["state"]),
                field("Task", &["task"]),
                field("Resource Type", &["resource_type"]),
                field("User Resource", &["user_resource"]),
                field("Group Resource", &["group_resource"]),
                field("Start", &["start_date"]),
                field("End", &["end_date"]),
                field("Planned Hours", &["planned_hours"]),
                field("Allocated Hours", &["allocated_hours"]),
                field("Confirmed Hours", &["confirmed_hours"]),
            ],
            section_specs: Vec::new(),
            sections_title: "Details",
            child_summary: None,
        },
        EntityKind::Story => DetailPageModel {
            title: "Story Details",
            field_specs: vec![
                field("State", &["state"]),
                field("Priority", &["priority"]),
                field("Assigned To", &["assigned_to"]),
                field("Story Points", &["story_points"]),
                field("Blocked", &["blocked"]),
                field("Sprint", &["sprint"]),
                field("Product", &["product"]),
                field("Epic", &["epic"]),
            ],
            section_specs: vec![section("Acceptance Criteria", "acceptance_criteria")],
            sections_title: "Story Detail",
            child_summary: Some(child_summary(
                "Story Tasks",
                "Loading story tasks...",
                "No story tasks found.",
                "No story tasks match the current filters.",
                STORY_TASK_SUMMARY_COLUMNS,
            )),
        },
        EntityKind::RequestItem => DetailPageModel {
            title: "Request Item Details",
            field_specs: vec![
                field("State", &["state"]),
                field("Assigned To", &["assigned_to"]),
                field("Group", &["assignment_group"]),
                field("Opened", &["opened_at"]),
                field("Due", &["due_date"]),
            ],
            section_specs: Vec::new(),
            sections_title: "Details",
            child_summary: None,
        },
        EntityKind::Incident => DetailPageModel {
            title: "Incident Details",
            field_specs: vec![
                field("State", &["state"]),
                field("Priority", &["priority"]),
                field("Assigned To", &["assigned_to"]),
                field("Group", &["assignment_group"]),
                field("Opened", &["opened_at"]),
                field("Resolved", &["resolved_at"]),
            ],
            section_specs: Vec::new(),
            sections_title: "Details",
            child_summary: None,
        },
    }
}

fn generic_detail_page_model() -> DetailPageModel {
    DetailPageModel {
        title: "Record Details",
        field_specs: Vec::new(),
        section_specs: Vec::new(),
        sections_title: "Details",
        child_summary: None,
    }
}

fn field(label: &'static str, fields: &'static [&'static str]) -> DetailFieldSpec {
    DetailFieldSpec { label, fields }
}

fn section(label: &'static str, field: &'static str) -> DetailSectionSpec {
    DetailSectionSpec { label, field }
}

fn child_summary(
    title: &'static str,
    loading_text: &'static str,
    empty_text: &'static str,
    filtered_empty_text: &'static str,
    columns: &'static [DetailChildColumn],
) -> DetailChildSummaryModel {
    DetailChildSummaryModel {
        title,
        loading_text,
        empty_text,
        filtered_empty_text,
        row_limit: 5,
        columns,
    }
}

fn detail_field_lines(page: &DetailPageModel, record: &Record) -> Vec<(String, String)> {
    page.field_specs
        .iter()
        .filter_map(|spec| optional_detail_line(record, spec.label, spec.fields))
        .collect()
}

fn detail_extra_sections(page: &DetailPageModel, record: &Record) -> Vec<(String, String)> {
    page.section_specs
        .iter()
        .filter_map(|spec| {
            record
                .get_str(spec.field)
                .map(display::strip_html)
                .filter(|value| !value.trim().is_empty())
                .map(|value| (spec.label.to_string(), value))
        })
        .collect()
}

fn optional_detail_line(record: &Record, label: &str, fields: &[&str]) -> Option<(String, String)> {
    fields
        .iter()
        .find_map(|field| record.get_display(field).or(record.get_str(field)))
        .map(|value| (label.to_string(), value.to_string()))
}

fn relative_time(input: Option<&str>) -> String {
    let Some(input) = input else {
        return "-".to_string();
    };
    let parsed = NaiveDateTime::parse_from_str(input, "%Y-%m-%d %H:%M:%S");
    let Ok(parsed) = parsed else {
        return input.to_string();
    };
    let now = Local::now().naive_local();
    let delta = now - parsed;
    if delta.num_minutes() < 1 {
        "just now".to_string()
    } else if delta.num_hours() < 1 {
        format!("{}m ago", delta.num_minutes())
    } else if delta.num_days() < 1 {
        format!("{}h ago", delta.num_hours())
    } else {
        format!("{}d ago", delta.num_days())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::Arc;

    use serde_json::json;
    use servicenow_rs::prelude::{BasicAuth, ServiceNowClient};
    use snow_core::SnowCore;
    use snow_core::{
        CacheSource, FieldValue, ResourceType, TaskSlaReadability, TaskSlaSummaryView, TaskSlaView,
    };
    use tempfile::TempDir;
    use tokio::sync::oneshot;
    use tokio::time::{Duration, timeout};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test(flavor = "current_thread")]
    async fn jump_lookup_hydrates_uncached_project_number() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/pm_project"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "result": [{
                    "sys_id": "project-sys",
                    "number": "PRJ0161206",
                    "short_description": "Wireless refresh project",
                    "description": "Refresh wireless infrastructure",
                    "state": "2"
                }]
            })))
            .expect(2)
            .mount(&server)
            .await;

        let client = ServiceNowClient::builder()
            .instance(server.uri())
            .auth(BasicAuth::new("test_user", "test_pass"))
            .allow_http()
            .build()
            .await
            .expect("client");
        let tempdir = TempDir::new().expect("tempdir");
        let core = SnowCore::builder()
            .client(client)
            .vault_path(tempdir.path().join("vault"))
            .build()
            .await
            .expect("core");
        let tui = TuiClient::local(Arc::new(core));

        let record = lookup_record_ref_by_number(&tui, "prj0161206")
            .await
            .expect("lookup should hydrate uncached project");

        assert_eq!(record.kind, Some(EntityKind::Project));
        assert_eq!(record.table, "pm_project");
        assert_eq!(record.sys_id, "project-sys");
        assert_eq!(record.number, "PRJ0161206");
        assert_eq!(record.short_description, "Wireless refresh project");
        assert!(
            tui.get_record("PRJ0161206")
                .await
                .expect("cached record")
                .is_some()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn task_sla_error_does_not_fail_detail_fetch_result() {
        let detail = join_detail_and_task_sla(async { Ok(sample_detail()) }, async {
            Err(SnowError::Api("SLA fixture failure".to_string()))
        })
        .await
        .expect("detail should still render");

        assert!(
            matches!(detail.task_sla, TaskSlaDetail::Error(ref err) if err == "SLA fixture failure")
        );
        assert_eq!(detail.record.get_str("number"), Some("sample-record"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn detail_and_task_sla_futures_start_concurrently() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (detail_started_tx, detail_started_rx) = oneshot::channel();
                let (task_sla_started_tx, task_sla_started_rx) = oneshot::channel();
                let (release_detail_tx, release_detail_rx) = oneshot::channel();
                let (release_task_sla_tx, release_task_sla_rx) = oneshot::channel();

                let handle = tokio::task::spawn_local(join_detail_and_task_sla(
                    async move {
                        let _ = detail_started_tx.send(());
                        release_detail_rx.await.expect("release detail");
                        Ok(sample_detail())
                    },
                    async move {
                        let _ = task_sla_started_tx.send(());
                        release_task_sla_rx.await.expect("release task sla");
                        Ok(sample_task_sla_status())
                    },
                ));

                timeout(Duration::from_millis(100), async {
                    let _ = detail_started_rx.await;
                    let _ = task_sla_started_rx.await;
                })
                .await
                .expect("both fetch futures should be polled before either completes");

                let _ = release_detail_tx.send(());
                let _ = release_task_sla_tx.send(());
                let detail = handle.await.expect("join task").expect("detail result");
                assert!(matches!(detail.task_sla, TaskSlaDetail::Status(_)));
            })
            .await;
    }

    #[test]
    fn detail_error_placeholder_keeps_cached_record_renderable() {
        let detail = detail_error_placeholder(
            &sample_record_ref(),
            &SnowError::NotFound("sample-record not found.".to_string()),
        );

        assert_eq!(detail.record.get_str("number"), Some("sample-record"));
        assert_eq!(
            detail.description.as_deref(),
            Some("Live detail refresh failed. This cached record may be stale or inaccessible.")
        );
        assert!(detail.notes_loaded);
        assert!(detail.field_lines.iter().any(|(label, value)| {
            label == "Live Detail" && value.contains("sample-record not found")
        }));
        assert!(matches!(detail.task_sla, TaskSlaDetail::Error(_)));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn unsupported_detail_prefetch_returns_placeholder_instead_of_error() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let core = Arc::new(TuiClient::remote(
                    PathBuf::from("/tmp/missing-snow-daemon.sock"),
                    None,
                ));
                let mut app = App::new(core, "test", "daemon", "test_user", None, false);
                let mut record = sample_record_ref();
                record.kind = None;
                record.table = "u_custom_table".to_string();
                record.sys_id = "custom-sys".to_string();
                record.number = "CUSTOM001".to_string();

                assert!(app.spawn_prefetch_task(PrefetchTask::Detail(record.clone())));
                super::super::yield_to_local_scheduler().await;

                assert!(
                    app.apply_completed_jobs()
                        .expect("prefetch should complete")
                );
                let detail = app
                    .detail_cache
                    .get(&detail_cache_key(&record.table, &record.sys_id))
                    .expect("placeholder detail should be cached");
                assert!(detail.field_lines.iter().any(|(label, value)| {
                    label == "Live Detail"
                        && value
                            .contains("Detail prefetch is not supported for u_custom_table records")
                }));
            })
            .await;
    }

    #[test]
    fn base_task_rows_are_drill_in_capable() {
        let kind = entity_kind_for_table("task").expect("task kind");
        let relation = child_relation(kind);

        assert_eq!(kind, EntityKind::Task);
        assert_eq!(lifecycle_field(kind), "state");
        assert!(relation.child_table.is_none());
    }

    #[test]
    fn detail_page_models_cover_business_parent_records() {
        let change = detail_page_model(EntityKind::ChangeRequest);
        assert_eq!(change.title, "Change Details");
        assert_eq!(
            change.child_summary.expect("change tasks").title,
            "Change Tasks"
        );
        assert!(
            change
                .section_specs
                .iter()
                .any(|section| section.field == "implementation_plan")
        );

        let demand = detail_page_model(EntityKind::Demand);
        assert_eq!(demand.title, "Demand Details");
        assert_eq!(
            demand.child_summary.expect("demand resource plans").title,
            "Resource Plans"
        );

        let story = detail_page_model(EntityKind::Story);
        assert_eq!(story.title, "Story Details");
        assert_eq!(
            story.child_summary.expect("story tasks").title,
            "Story Tasks"
        );
        assert!(
            story
                .section_specs
                .iter()
                .any(|section| section.field == "acceptance_criteria")
        );

        let project = detail_page_model(EntityKind::Project);
        assert_eq!(project.title, "Project Details");
        assert_eq!(
            project.child_summary.expect("project resource plans").title,
            "Resource Plans"
        );
    }

    #[test]
    fn detail_page_model_extracts_fields_and_rich_sections() {
        let record = Record::from_json(
            "change_request",
            &json!({
                "sys_id": "chg-sys",
                "number": "CHG0001234",
                "short_description": "Upgrade network",
                "state": "Assess",
                "assigned_to": "Change Owner",
                "implementation_plan": "<p>Implement during window</p>",
                "backout_plan": "<p>Restore previous config</p>"
            }),
            DisplayValue::Display,
        )
        .expect("record fixture");
        let model = detail_page_model(EntityKind::ChangeRequest);

        let fields = detail_field_lines(&model, &record);
        assert!(
            fields
                .iter()
                .any(|(label, value)| { label == "State" && value == "Assess" })
        );
        assert!(
            fields
                .iter()
                .any(|(label, value)| { label == "Assigned To" && value == "Change Owner" })
        );

        let sections = detail_extra_sections(&model, &record);
        assert!(sections.iter().any(|(label, value)| {
            label == "Implementation Steps" && value.contains("Implement during window")
        }));
        assert!(sections.iter().any(|(label, value)| {
            label == "Backout Plan" && value.contains("Restore previous config")
        }));
    }

    #[test]
    fn incident_task_sla_dashboard_label_summarizes_readable_status() {
        let status = TaskSlaStatus {
            record_number: "INC0000001".to_string(),
            record_table: "incident".to_string(),
            record_sys_id: "incident-sys-1".to_string(),
            rows: Vec::new(),
            summary: TaskSlaSummaryView {
                total: 2,
                active: 1,
                breached: 0,
                next_breach: Some(TaskSlaView {
                    name: Some("Response SLA".to_string()),
                    stage: Some("in_progress".to_string()),
                    active: Some(true),
                    breached: Some(false),
                    planned_end_time: Some("2026-05-11 14:00:00".to_string()),
                    business_elapsed_percentage: Some(80.0),
                    time_left: Some("2 Hours".to_string()),
                }),
                highest_business_elapsed: Some(80.0),
            },
            readable: TaskSlaReadability::ReadableRows,
        };

        assert_eq!(
            incident_task_sla_dashboard_label(&status),
            "A:1 B:0 | next 2 Hours | 80%"
        );
    }

    #[test]
    fn incident_task_sla_dashboard_label_preserves_acl_ambiguity() {
        let status = TaskSlaStatus {
            record_number: "INC0000001".to_string(),
            record_table: "incident".to_string(),
            record_sys_id: "incident-sys-1".to_string(),
            rows: Vec::new(),
            summary: TaskSlaSummaryView {
                total: 0,
                active: 0,
                breached: 0,
                next_breach: None,
                highest_business_elapsed: None,
            },
            readable: TaskSlaReadability::EmptyOrAclRestricted,
        };

        assert_eq!(incident_task_sla_dashboard_label(&status), "none/ACL");
    }

    #[test]
    fn map_incident_row_includes_task_sla_cell() {
        let row = map_incident_row(sample_incident_record(), "A:1 B:0".to_string());

        assert_eq!(row.cells.len(), 6);
        assert_eq!(row.cells[0], "INC0000001");
        assert_eq!(row.cells[3], "2 - High");
        assert_eq!(row.cells[4], "A:1 B:0");
        assert_eq!(row.cells[5], "2026-05-11 10:00:00");
    }

    #[test]
    fn change_request_display_table_maps_to_supported_detail_kind() {
        let mut record = sample_incident_record();
        record.sys_id = "chg-sys".to_string();
        record.number = "CHG0332518".to_string();
        record.table = "Change Request".to_string();
        record.resource_type = ResourceType::Change;

        let mapped = map_child_record(record);

        assert_eq!(mapped.kind, Some(EntityKind::ChangeRequest));
        assert_eq!(mapped.table, "change_request");
    }

    #[test]
    fn change_request_subclass_maps_to_supported_detail_kind() {
        assert_eq!(
            entity_kind_for_table("change_request_normal"),
            Some(EntityKind::ChangeRequest)
        );
    }

    #[test]
    fn change_request_number_canonicalizes_ambiguous_cached_table() {
        let mut record = sample_incident_record();
        record.sys_id = "chg-sys".to_string();
        record.number = "CHG0332518".to_string();
        record.table = "Normal".to_string();
        record.resource_type = ResourceType::Change;

        let mapped = map_child_record(record);

        assert_eq!(mapped.kind, Some(EntityKind::ChangeRequest));
        assert_eq!(mapped.table, "change_request");
    }

    #[test]
    fn approval_row_canonicalizes_change_target_table() {
        let approval = CachedApprovalRecord {
            record: sample_incident_record(),
            approver: snow_core::Reference {
                sys_id: "approver-sys".to_string(),
                table: "sys_user".to_string(),
                display_name: "Approver".to_string(),
                extra: HashMap::new(),
            },
            target: snow_core::RecordRef {
                sys_id: "chg-sys".to_string(),
                number: "CHG0332518".to_string(),
                table: "Normal Change".to_string(),
            },
            requested_at: chrono::Utc::now(),
            due_date: None,
        };

        let row = map_approval_row(approval);

        assert_eq!(row.record.kind, Some(EntityKind::ChangeRequest));
        assert_eq!(row.record.table, "change_request");
        assert_eq!(
            row.record
                .approval
                .as_ref()
                .map(|approval| approval.source_table.as_str()),
            Some("change_request")
        );
    }

    fn sample_detail() -> RecordDetail {
        placeholder_detail(&sample_record_ref())
    }

    fn sample_incident_record() -> SnowRecord {
        let mut fields = HashMap::new();
        fields.insert(
            "state".to_string(),
            FieldValue {
                value: "2".to_string(),
                display_value: Some("Assigned".to_string()),
            },
        );
        fields.insert(
            "priority".to_string(),
            FieldValue {
                value: "2".to_string(),
                display_value: Some("2 - High".to_string()),
            },
        );
        fields.insert(
            "opened_at".to_string(),
            FieldValue {
                value: "2026-05-11 10:00:00".to_string(),
                display_value: Some("2026-05-11 10:00:00".to_string()),
            },
        );

        SnowRecord {
            sys_id: "incident-sys-1".to_string(),
            number: "INC0000001".to_string(),
            table: "incident".to_string(),
            resource_type: ResourceType::Incident,
            state: "2".to_string(),
            short_description: "Sample incident".to_string(),
            description: String::new(),
            fields,
            work_notes: Vec::new(),
            comments: Vec::new(),
            parent: None,
            children: Vec::new(),
            references: HashMap::new(),
            synced_at: chrono::Utc::now(),
            source: CacheSource::Memory,
        }
    }

    fn sample_record_ref() -> RecordRef {
        RecordRef {
            kind: Some(EntityKind::Incident),
            table: "incident".to_string(),
            sys_id: "sample-parent".to_string(),
            number: "sample-record".to_string(),
            short_description: "Sample record".to_string(),
            assigned_to: None,
            planned_start: None,
            planned_end: None,
            planned_hours: None,
            allocated_hours: None,
            confirmed_hours: None,
            state_label: Some("Open".to_string()),
            state_value: Some("1".to_string()),
            record_class: None,
            parent_table: None,
            parent_sys_id: None,
            parent_number: None,
            approval: None,
        }
    }

    fn sample_task_sla_status() -> TaskSlaStatus {
        TaskSlaStatus {
            record_number: "sample-record".to_string(),
            record_table: "incident".to_string(),
            record_sys_id: "sample-parent".to_string(),
            rows: Vec::new(),
            summary: TaskSlaSummaryView {
                total: 0,
                active: 0,
                breached: 0,
                next_breach: None,
                highest_business_elapsed: None,
            },
            readable: TaskSlaReadability::EmptyOrAclRestricted,
        }
    }
}
