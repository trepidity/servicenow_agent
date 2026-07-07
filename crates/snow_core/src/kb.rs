use crate::context::CoreContext;
use crate::convert::{record_row_from_runtime_record, serialize_vault_document};
use crate::helpers::{document_content, document_tag_tokens, document_work_notes};
use crate::resource::knowledge::KnowledgeResource;
use crate::vault::VaultDocument;
use crate::vault::layout::slugify;
use crate::{
    KnowledgeArticle, ResourceType, SnowCore, empty_reference, normalize_knowledge_article,
};
use anyhow::{Context, Result, anyhow, ensure};
use chrono::{DateTime, Utc};
use reqwest::Client as HttpClient;
use serde_json::Value;
use servicenow_rs::prelude::{DisplayValue, Order, Record};
use servicenow_rs::query::builder::TableApi;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

const KB_PAGE_SIZE: u32 = 200;
const KB_LOCK_STALE_MS: i64 = 30 * 60 * 1000;
const KB_METADATA_FIELDS: &[&str] = &[
    "sys_id",
    "number",
    "short_description",
    "description",
    "state",
    "workflow_state",
    "article_type",
    "valid_to",
    "published",
    "knowledge_base",
    "category",
    "kb_knowledge_base",
    "kb_category",
    "author",
    "sys_updated_on",
    "u_keywords",
    "keywords",
    "meta",
    "sys_tags",
];
const KB_FULL_FIELDS: &[&str] = &[
    "sys_id",
    "number",
    "short_description",
    "description",
    "state",
    "workflow_state",
    "article_type",
    "valid_to",
    "published",
    "knowledge_base",
    "category",
    "kb_knowledge_base",
    "kb_category",
    "author",
    "sys_updated_on",
    "u_keywords",
    "keywords",
    "meta",
    "sys_tags",
    "article_body",
    "text",
];
const KB_SN_TAG_FIELDS: &[&str] = &["u_keywords", "keywords", "meta", "sys_tags"];
const KB_STOPWORDS: &[&str] = &[
    "a",
    "an",
    "and",
    "are",
    "article",
    "articles",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "how",
    "in",
    "into",
    "is",
    "it",
    "its",
    "kb",
    "knowledge",
    "of",
    "on",
    "or",
    "servicenow",
    "that",
    "the",
    "this",
    "to",
    "using",
    "with",
    "workflow",
];

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum KnowledgeSyncMode {
    Full,
    Incremental,
}

#[derive(
    Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq, PartialOrd, Ord,
)]
#[serde(rename_all = "snake_case")]
pub enum KnowledgeTagLayer {
    Sn,
    Auto,
    User,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct KnowledgeTagSummary {
    pub tag: String,
    pub count: usize,
    pub layers: Vec<KnowledgeTagLayer>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct KnowledgeStatus {
    pub article_count: usize,
    pub body_cached_count: usize,
    pub knowledge_base_count: usize,
    pub category_count: usize,
    pub last_full_at: Option<DateTime<Utc>>,
    pub last_incremental_at: Option<DateTime<Utc>>,
    pub watermark_updated_at: Option<String>,
    pub watermark_sys_id: Option<String>,
    pub lock_held: bool,
    pub lock_timestamp_ms: Option<i64>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct KnowledgeSyncOutcome {
    pub accepted: bool,
    pub mode: KnowledgeSyncMode,
    pub with_bodies: bool,
    pub status: String,
    pub details: Option<String>,
}

#[derive(Debug, Default)]
struct WatermarkProgress {
    processed: usize,
    tombstoned: usize,
    pruned: usize,
    watermark_updated_at: Option<String>,
    watermark_sys_id: Option<String>,
    seen_sys_ids: HashSet<String>,
    changed_record_sys_ids: HashSet<String>,
    removed_record_sys_ids: HashSet<String>,
    changed_base_sys_ids: HashSet<String>,
}

impl WatermarkProgress {
    fn observe(&mut self, record: &Record) {
        self.processed += 1;
        self.seen_sys_ids.insert(record.sys_id.clone());
        self.changed_record_sys_ids.insert(record.sys_id.clone());
        if let Some(knowledge_base) = KnowledgeResource::knowledge_base_reference(record)
            && !knowledge_base.sys_id.is_empty()
        {
            self.changed_base_sys_ids.insert(knowledge_base.sys_id);
        }
        let updated_at = record
            .get_raw("sys_updated_on")
            .or_else(|| record.get_str("sys_updated_on"))
            .unwrap_or_default()
            .trim()
            .to_string();
        let sys_id = record.sys_id.clone();
        if updated_at.is_empty() || sys_id.is_empty() {
            return;
        }

        let should_replace = match (
            self.watermark_updated_at.as_deref(),
            self.watermark_sys_id.as_deref(),
        ) {
            (None, _) => true,
            (Some(existing_updated_at), Some(existing_sys_id)) => {
                updated_at.as_str() > existing_updated_at
                    || (updated_at.as_str() == existing_updated_at
                        && sys_id.as_str() > existing_sys_id)
            }
            (Some(existing_updated_at), None) => updated_at.as_str() >= existing_updated_at,
        };

        if should_replace {
            self.watermark_updated_at = Some(updated_at);
            self.watermark_sys_id = Some(sys_id);
        }
    }
}

#[cfg(test)]
#[derive(Debug, Clone)]
struct CorpusTagDoc {
    article: KnowledgeArticle,
    tokens: Vec<String>,
}

#[derive(Debug, Clone)]
struct LlmTagResponse {
    tags: Vec<String>,
}

#[derive(Debug, Default)]
struct LocalKnowledgeIngest {
    changed_record_sys_ids: HashSet<String>,
    changed_base_sys_ids: HashSet<String>,
}

#[derive(Debug, Clone)]
struct KnowledgeRuntimeProjection {
    article: KnowledgeArticle,
    relative_path: PathBuf,
    modified_at_ms: i64,
}

impl SnowCore {
    pub async fn get_knowledge_article_fresh(
        &self,
        number: &str,
    ) -> Result<Option<KnowledgeArticle>> {
        self.get_knowledge_article_fresh_inner(number, true).await
    }

    pub(crate) async fn get_knowledge_article_fresh_inner(
        &self,
        number: &str,
        rebuild_semantic_index: bool,
    ) -> Result<Option<KnowledgeArticle>> {
        let Some(record) = self
            .ctx
            .client
            .table("kb_knowledge")
            .fields(KB_FULL_FIELDS)
            .equals("number", number)
            .display_value(DisplayValue::Both)
            .first()
            .await?
        else {
            return Ok(None);
        };

        self.persist_record(&record)?;
        let article = self.ctx.query.get_knowledge_article(number).await?;
        if rebuild_semantic_index
            && article
                .as_ref()
                .is_some_and(|article| article.body_cached && !article.content.trim().is_empty())
        {
            self.maybe_run_inline_semantic_rebuild("fresh knowledge article")
                .await;
        }
        Ok(article)
    }

    pub async fn sync_knowledge(
        &self,
        full: bool,
        with_bodies: bool,
    ) -> Result<KnowledgeSyncOutcome> {
        let local_ingest = self.ingest_local_knowledge_state()?;

        let lock_acquired = self
            .ctx
            .query
            .store()
            .acquire_kb_sync_lock(Utc::now().timestamp_millis(), KB_LOCK_STALE_MS)?;
        if !lock_acquired {
            return Ok(KnowledgeSyncOutcome {
                accepted: false,
                mode: if full {
                    KnowledgeSyncMode::Full
                } else {
                    KnowledgeSyncMode::Incremental
                },
                with_bodies,
                status: "locked".to_string(),
                details: Some("another KB sync is already in progress".to_string()),
            });
        }

        let result = self
            .sync_knowledge_locked(full, with_bodies, local_ingest)
            .await;
        let release_result = self.ctx.query.store().release_kb_sync_lock();
        match (result, release_result) {
            (Ok(outcome), Ok(())) => Ok(outcome),
            (Err(err), Ok(())) => Err(err),
            (Ok(_), Err(err)) => Err(err.into()),
            (Err(primary), Err(release_err)) => Err(anyhow!(
                "{primary}; failed to release KB sync lock: {release_err}"
            )),
        }
    }

    pub fn knowledge_status(&self) -> Result<KnowledgeStatus> {
        let store = self.ctx.query.store();
        let bases = store.list_knowledge_bases()?;
        let mut category_count = 0usize;
        for base in &bases {
            category_count += store
                .list_knowledge_categories(&base.knowledge_base_sys_id)?
                .len();
        }
        let state = store.get_kb_sync_state()?;

        Ok(KnowledgeStatus {
            article_count: store.count_knowledge_articles()?,
            body_cached_count: store.count_knowledge_articles_with_cached_body()?,
            knowledge_base_count: bases.len(),
            category_count,
            last_full_at: state.last_full_at,
            last_incremental_at: state.last_incr_at,
            watermark_updated_at: state.watermark_updated_at,
            watermark_sys_id: state.watermark_sys_id,
            lock_held: state.kb_sync_lock.is_some(),
            lock_timestamp_ms: state.kb_sync_lock,
        })
    }

    pub fn list_knowledge_tags(
        &self,
        layer: Option<KnowledgeTagLayer>,
        min_count: usize,
    ) -> Result<Vec<KnowledgeTagSummary>> {
        ensure!(min_count > 0, "min_count must be at least 1");

        let store = self.ctx.query.store();
        match layer {
            Some(layer) => Ok(store
                .list_knowledge_tags(layer.as_store_str(), min_count)?
                .into_iter()
                .map(|row| KnowledgeTagSummary {
                    tag: row.tag,
                    count: row.article_count,
                    layers: vec![layer],
                })
                .collect()),
            None => {
                let counts = store.list_knowledge_tags("all", min_count)?;
                let mut layers_by_tag = BTreeMap::<String, BTreeSet<KnowledgeTagLayer>>::new();
                for requested_layer in [
                    KnowledgeTagLayer::Sn,
                    KnowledgeTagLayer::Auto,
                    KnowledgeTagLayer::User,
                ] {
                    for row in store.list_knowledge_tags(requested_layer.as_store_str(), 1)? {
                        layers_by_tag
                            .entry(row.tag)
                            .or_default()
                            .insert(requested_layer);
                    }
                }

                Ok(counts
                    .into_iter()
                    .map(|row| KnowledgeTagSummary {
                        layers: layers_by_tag
                            .remove(&row.tag)
                            .unwrap_or_default()
                            .into_iter()
                            .collect(),
                        tag: row.tag,
                        count: row.article_count,
                    })
                    .collect())
            }
        }
    }

    async fn sync_knowledge_locked(
        &self,
        requested_full: bool,
        requested_with_bodies: bool,
        local_ingest: LocalKnowledgeIngest,
    ) -> Result<KnowledgeSyncOutcome> {
        let now = Utc::now();
        let previous = self.ctx.query.store().get_kb_sync_state()?;
        let mode = determine_sync_mode(
            requested_full,
            self.ctx
                .config
                .refresh
                .resources
                .get("knowledge")
                .and_then(|config| config.full_sync_interval.as_deref()),
            previous.last_full_at,
            previous.watermark_updated_at.as_deref(),
            previous.watermark_sys_id.as_deref(),
            now,
        );

        let actual_with_bodies =
            requested_with_bodies || matches!(mode, KnowledgeSyncMode::Incremental);
        let progress = match mode {
            KnowledgeSyncMode::Full => self.run_full_knowledge_sync(actual_with_bodies).await?,
            KnowledgeSyncMode::Incremental => {
                self.run_incremental_knowledge_sync(&previous).await?
            }
        };
        let auto_tag_drift = self
            .refresh_knowledge_auto_tags(mode, &local_ingest, &progress)
            .await?;
        let mut changed_base_sys_ids = progress.changed_base_sys_ids.clone();
        changed_base_sys_ids.extend(local_ingest.changed_base_sys_ids);
        self.rebuild_knowledge_indexes(mode, &changed_base_sys_ids)?;

        let watermark_updated_at = progress
            .watermark_updated_at
            .or(previous.watermark_updated_at.clone());
        let watermark_sys_id = progress
            .watermark_sys_id
            .or(previous.watermark_sys_id.clone());
        self.ctx
            .query
            .store()
            .set_kb_sync_state(&crate::cache::store::KbSyncStateRow {
                last_full_at: if matches!(mode, KnowledgeSyncMode::Full) {
                    Some(now)
                } else {
                    previous.last_full_at
                },
                last_incr_at: if matches!(mode, KnowledgeSyncMode::Incremental) {
                    Some(now)
                } else {
                    previous.last_incr_at
                },
                watermark_updated_at,
                watermark_sys_id,
                kb_sync_lock: None,
            })?;

        self.maybe_run_inline_semantic_rebuild("knowledge sync")
            .await;

        Ok(KnowledgeSyncOutcome {
            accepted: true,
            mode,
            with_bodies: actual_with_bodies,
            status: "completed".to_string(),
            details: Some(format!(
                "processed {} articles, auto-tag drift {}, tombstoned {}, pruned {}",
                progress.processed, auto_tag_drift, progress.tombstoned, progress.pruned
            )),
        })
    }

    async fn run_full_knowledge_sync(&self, with_bodies: bool) -> Result<WatermarkProgress> {
        let mut paginator = self.base_knowledge_query(with_bodies)?.paginate()?;
        let mut progress = WatermarkProgress::default();

        while let Some(page) = paginator.next_page().await? {
            for record in page.records {
                self.observe_existing_base_for_record(&record, &mut progress)?;
                self.persist_record(&record)?;
                progress.observe(&record);
            }
        }

        self.reconcile_full_knowledge_sync(Utc::now(), &mut progress)
            .await?;
        Ok(progress)
    }

    async fn run_incremental_knowledge_sync(
        &self,
        previous: &crate::cache::store::KbSyncStateRow,
    ) -> Result<WatermarkProgress> {
        let Some(updated_at) = previous.watermark_updated_at.as_deref() else {
            return self.run_full_knowledge_sync(true).await;
        };
        let Some(sys_id) = previous.watermark_sys_id.as_deref() else {
            return self.run_full_knowledge_sync(true).await;
        };

        let mut progress = WatermarkProgress::default();
        for query in [
            self.incremental_same_timestamp_query(updated_at, sys_id)?,
            self.incremental_newer_timestamp_query(updated_at)?,
        ] {
            let mut paginator = query.paginate()?;
            while let Some(page) = paginator.next_page().await? {
                for record in page.records {
                    self.observe_existing_base_for_record(&record, &mut progress)?;
                    self.persist_record(&record)?;
                    progress.observe(&record);
                }
            }
        }

        Ok(progress)
    }

    async fn reconcile_full_knowledge_sync(
        &self,
        now: DateTime<Utc>,
        progress: &mut WatermarkProgress,
    ) -> Result<()> {
        for row in self
            .ctx
            .query
            .store()
            .list_records_by_resource_type(ResourceType::Knowledge)?
        {
            if progress.seen_sys_ids.contains(&row.sys_id) {
                continue;
            }
            if let Some(existing_article) =
                self.ctx.query.store().get_knowledge_article(&row.sys_id)?
                && !existing_article.knowledge_base_sys_id.is_empty()
            {
                progress
                    .changed_base_sys_ids
                    .insert(existing_article.knowledge_base_sys_id);
            }
            match row.lifecycle() {
                crate::cache::store::RecordLifecycle::Active => {
                    self.tombstone_record(&row.sys_id, now)?;
                    progress.tombstoned += 1;
                    progress.removed_record_sys_ids.insert(row.sys_id);
                }
                crate::cache::store::RecordLifecycle::Tombstoned => {
                    self.prune_record(&row.sys_id, now).await?;
                    progress.pruned += 1;
                    progress.removed_record_sys_ids.insert(row.sys_id);
                }
                crate::cache::store::RecordLifecycle::Pruned => {}
            }
        }
        Ok(())
    }

    fn ingest_local_knowledge_state(&self) -> Result<LocalKnowledgeIngest> {
        let mut ingest = LocalKnowledgeIngest::default();
        let mut projections = Vec::new();

        for row in self
            .ctx
            .query
            .store()
            .list_active_knowledge_local_scan_rows()?
        {
            let absolute_path = self.ctx.vault_path.join(&row.file_path);
            let modified_at_ms = match file_modified_at_ms(&absolute_path) {
                Ok(modified_at_ms) => modified_at_ms,
                Err(_) => continue,
            };
            if row.modified_at_ms == Some(modified_at_ms) {
                continue;
            }

            let article = self
                .ctx
                .vault
                .read_knowledge_article(&absolute_path)
                .with_context(|| {
                    format!(
                        "failed to ingest local KB markdown {}",
                        absolute_path.display()
                    )
                })?;
            ingest
                .changed_record_sys_ids
                .insert(article.record.sys_id.clone());
            if !article.knowledge_base.sys_id.is_empty() {
                ingest
                    .changed_base_sys_ids
                    .insert(article.knowledge_base.sys_id.clone());
            }
            projections.push(KnowledgeRuntimeProjection {
                article,
                relative_path: PathBuf::from(&row.file_path),
                modified_at_ms,
            });
        }

        self.project_knowledge_runtime_updates(&projections)?;
        Ok(ingest)
    }

    fn load_existing_knowledge_article(&self, record_sys_id: &str) -> Option<KnowledgeArticle> {
        self.ctx.load_existing_knowledge_article(record_sys_id)
    }

    fn base_knowledge_query(&self, with_bodies: bool) -> Result<TableApi> {
        let mut query = self
            .ctx
            .client
            .table("kb_knowledge")
            .fields(if with_bodies {
                KB_FULL_FIELDS
            } else {
                KB_METADATA_FIELDS
            })
            .display_value(DisplayValue::Both)
            .order_by("sys_updated_on", Order::Asc)
            .order_by("sys_id", Order::Asc)
            .limit(KB_PAGE_SIZE);

        let filter = self
            .ctx
            .config
            .refresh
            .resources
            .get("knowledge")
            .map(|resource| resource.filter.as_str())
            .filter(|filter| !filter.trim().is_empty())
            .unwrap_or("workflow_state=published");
        query = apply_simple_encoded_filter(query, filter)?;
        Ok(query)
    }

    fn incremental_same_timestamp_query(&self, updated_at: &str, sys_id: &str) -> Result<TableApi> {
        Ok(self
            .base_knowledge_query(true)?
            .equals("sys_updated_on", updated_at)
            .greater_than("sys_id", sys_id))
    }

    fn incremental_newer_timestamp_query(&self, updated_at: &str) -> Result<TableApi> {
        Ok(self
            .base_knowledge_query(true)?
            .greater_than("sys_updated_on", updated_at))
    }

    async fn refresh_knowledge_auto_tags(
        &self,
        mode: KnowledgeSyncMode,
        local_ingest: &LocalKnowledgeIngest,
        progress: &WatermarkProgress,
    ) -> Result<usize> {
        match mode {
            KnowledgeSyncMode::Full => self.refresh_knowledge_auto_tags_full().await,
            KnowledgeSyncMode::Incremental => {
                self.refresh_knowledge_auto_tags_incremental(local_ingest, progress)
                    .await
            }
        }
    }

    async fn refresh_knowledge_auto_tags_full(&self) -> Result<usize> {
        let articles = self.load_active_knowledge_articles().await?;
        let term_entries = articles
            .iter()
            .map(|article| {
                (
                    article.record.sys_id.clone(),
                    unique_terms_for_article(article),
                )
            })
            .collect::<Vec<_>>();
        let term_stats = term_stats_from_entries(&term_entries);
        self.ctx.query.store().replace_kb_term_stats(&term_stats)?;
        self.ctx
            .query
            .store()
            .replace_all_kb_article_terms(&term_entries)?;

        let mut changed_articles = Vec::new();
        let mut drift = 0usize;
        for mut article in articles {
            let next_tags = self
                .derive_auto_tags_for_article(&article, &term_stats, term_entries.len())
                .await?;
            if next_tags == article.auto_tags {
                continue;
            }
            article.auto_tags = next_tags;
            changed_articles.push(article);
            drift += 1;
        }

        self.persist_knowledge_runtime_articles(&changed_articles)?;
        Ok(drift)
    }

    async fn refresh_knowledge_auto_tags_incremental(
        &self,
        local_ingest: &LocalKnowledgeIngest,
        progress: &WatermarkProgress,
    ) -> Result<usize> {
        let active_count = self.ctx.query.store().count_knowledge_articles()?;
        if active_count == 0 {
            self.ctx
                .query
                .store()
                .replace_kb_term_stats(&HashMap::new())?;
            self.ctx.query.store().replace_all_kb_article_terms(&[])?;
            return Ok(0);
        }

        let mut term_stats = self.ctx.query.store().load_kb_term_stats()?;
        if term_stats.is_empty() {
            return self.refresh_knowledge_auto_tags_full().await;
        }

        let removed_ids = progress
            .removed_record_sys_ids
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        for record_sys_id in &removed_ids {
            let old_terms = self.ctx.query.store().get_kb_article_terms(record_sys_id)?;
            decrement_term_stats(&mut term_stats, &old_terms);
        }
        self.ctx
            .query
            .store()
            .delete_kb_article_terms(&removed_ids)?;

        let mut changed_ids = progress.changed_record_sys_ids.clone();
        changed_ids.extend(local_ingest.changed_record_sys_ids.iter().cloned());
        if changed_ids.is_empty() {
            self.ctx.query.store().replace_kb_term_stats(&term_stats)?;
            return Ok(0);
        }

        let articles = self
            .load_knowledge_articles_by_sys_ids(&changed_ids)
            .await?;
        let mut term_entries = Vec::new();
        for article in &articles {
            let old_terms = self
                .ctx
                .query
                .store()
                .get_kb_article_terms(&article.record.sys_id)?;
            decrement_term_stats(&mut term_stats, &old_terms);
            let new_terms = unique_terms_for_article(article);
            increment_term_stats(&mut term_stats, &new_terms);
            term_entries.push((article.record.sys_id.clone(), new_terms));
        }
        self.ctx.query.store().replace_kb_term_stats(&term_stats)?;
        self.ctx
            .query
            .store()
            .replace_kb_article_terms_entries(&term_entries)?;

        let mut changed_articles = Vec::new();
        let mut drift = 0usize;
        for mut article in articles {
            let next_tags = self
                .derive_auto_tags_for_article(&article, &term_stats, active_count)
                .await?;
            if next_tags == article.auto_tags {
                continue;
            }
            article.auto_tags = next_tags;
            changed_articles.push(article);
            drift += 1;
        }

        self.persist_knowledge_runtime_articles(&changed_articles)?;
        Ok(drift)
    }

    async fn derive_auto_tags_for_article(
        &self,
        article: &KnowledgeArticle,
        term_stats: &HashMap<String, usize>,
        corpus_size: usize,
    ) -> Result<Vec<String>> {
        let mut next_tags = derive_auto_tags_with_stats(
            article,
            term_stats,
            corpus_size,
            self.ctx.config.kb.max_auto_tags.max(1),
        );
        match self.maybe_derive_llm_tags(article).await {
            Ok(Some(llm_tags)) => {
                next_tags = llm_tags;
            }
            Ok(None) => {}
            Err(err) => {
                eprintln!(
                    "snow_core: KB LLM tag derivation failed for {}: {err}",
                    article.record.number
                );
            }
        }
        Ok(next_tags)
    }

    fn persist_knowledge_runtime_articles(&self, articles: &[KnowledgeArticle]) -> Result<()> {
        if articles.is_empty() {
            return Ok(());
        }

        let mut projections = Vec::with_capacity(articles.len());
        for article in articles {
            let article = normalize_knowledge_article(article.clone());
            let persisted = self.ctx.vault.persist_knowledge_article(&article)?;
            let modified_at_ms = file_modified_at_ms(&persisted.path)?;
            projections.push(KnowledgeRuntimeProjection {
                article,
                relative_path: persisted.relative_path,
                modified_at_ms,
            });
        }
        self.project_knowledge_runtime_updates(&projections)
    }

    fn project_knowledge_runtime_updates(
        &self,
        projections: &[KnowledgeRuntimeProjection],
    ) -> Result<()> {
        if projections.is_empty() {
            return Ok(());
        }

        let mut record_rows = Vec::with_capacity(projections.len());
        let mut documents = Vec::with_capacity(projections.len());
        let mut file_states = Vec::with_capacity(projections.len());

        for projection in projections {
            let document =
                VaultDocument::Knowledge(normalize_knowledge_article(projection.article.clone()));
            let row = record_row_from_runtime_record(
                document.record(),
                Some(projection.relative_path.clone()),
                serialize_vault_document(&document).to_string(),
            );
            let work_notes = document_work_notes(document.record());
            let content = document_content(&document);
            let tag_tokens = document_tag_tokens(&document);
            record_rows.push((row, work_notes, content, tag_tokens));
            documents.push(document);
            file_states.push((
                projection.article.record.sys_id.clone(),
                projection.relative_path.to_string_lossy().into_owned(),
                projection.modified_at_ms,
            ));
        }

        let batch = record_rows
            .iter()
            .map(|(row, work_notes, content, tag_tokens)| {
                (
                    row,
                    work_notes.as_str(),
                    content.as_str(),
                    tag_tokens.as_str(),
                )
            })
            .collect::<Vec<_>>();
        self.ctx.query.store().upsert_records(&batch)?;
        self.ctx
            .query
            .store()
            .upsert_kb_local_file_states(&file_states)?;

        for document in &documents {
            self.project_runtime_document(document)?;
            self.persist_enrichment(document.record())?;
            self.ctx.cache.invalidate(&document.record().number);
            self.ctx.cache.put(document.record().clone());
        }
        Ok(())
    }

    async fn maybe_derive_llm_tags(
        &self,
        article: &KnowledgeArticle,
    ) -> Result<Option<Vec<String>>> {
        let config = &self.ctx.config.kb.llm_tags;
        if !config.enabled
            || config.endpoint.trim().is_empty()
            || config.model.trim().is_empty()
            || article.content.trim().is_empty()
        {
            return Ok(None);
        }

        let prompt = render_llm_tag_prompt(article);
        let endpoint = format!("{}/api/generate", config.endpoint.trim_end_matches('/'));
        let client = HttpClient::builder()
            .timeout(Duration::from_secs(config.timeout_seconds.max(1)))
            .build()
            .context("failed to build KB LLM tag client")?;
        let response = client
            .post(endpoint)
            .json(&serde_json::json!({
                "model": config.model,
                "stream": false,
                "format": "json",
                "prompt": prompt,
            }))
            .send()
            .await
            .context("failed to request KB LLM tags")?;
        let response = response
            .error_for_status()
            .context("KB LLM tag endpoint returned an error")?;
        let body = response
            .json::<Value>()
            .await
            .context("failed to decode KB LLM tag response")?;
        Ok(Some(parse_llm_tag_response(&body)?.tags))
    }

    fn rebuild_knowledge_indexes(
        &self,
        mode: KnowledgeSyncMode,
        changed_base_sys_ids: &HashSet<String>,
    ) -> Result<()> {
        let layout = self.ctx.vault.layout();
        let knowledge_root = layout.root().join("knowledge");
        let global_index_path = knowledge_root.join("INDEX.md");
        let bases = self.ctx.query.store().list_knowledge_bases()?;
        let mut grouped =
            BTreeMap::<(String, String, String), BTreeMap<(String, String), usize>>::new();
        let mut active_base_names = HashMap::new();
        let mut active_base_dirs = BTreeSet::new();

        for base in &bases {
            let base_dir =
                layout.knowledge_base_dir(&base.knowledge_base_sys_id, &base.knowledge_base_name);
            let base_slug = base_dir
                .file_name()
                .and_then(|value| value.to_str())
                .unwrap_or_default()
                .to_string();
            active_base_dirs.insert(base_slug.clone());
            active_base_names.insert(
                base.knowledge_base_sys_id.clone(),
                base.knowledge_base_name.clone(),
            );
            let categories = self
                .ctx
                .query
                .store()
                .list_knowledge_categories(&base.knowledge_base_sys_id)?;
            let grouped_categories = grouped
                .entry((
                    base.knowledge_base_sys_id.clone(),
                    base.knowledge_base_name.clone(),
                    base_slug,
                ))
                .or_default();
            for category in categories {
                grouped_categories.insert(
                    (category.category_sys_id, category.category_name),
                    category.article_count,
                );
            }
        }

        if matches!(mode, KnowledgeSyncMode::Full) && knowledge_root.join("bases").exists() {
            for base_dir in fs::read_dir(knowledge_root.join("bases"))? {
                let base_dir = base_dir?;
                let file_name = base_dir.file_name().to_string_lossy().into_owned();
                if !active_base_dirs.contains(&file_name) {
                    self.ctx.vault.delete(base_dir.path().join("INDEX.md"))?;
                }
            }
        }

        let tag_summaries = self.list_knowledge_tags(None, 1)?;
        let sync_state = self.ctx.query.store().get_kb_sync_state()?;
        let last_synced = sync_state
            .last_full_at
            .or(sync_state.last_incr_at)
            .unwrap_or_else(Utc::now);
        self.ctx.vault.write_markdown_file(
            &global_index_path,
            &render_global_index(&grouped, &tag_summaries, last_synced),
        )?;

        let base_ids_to_rebuild = if matches!(mode, KnowledgeSyncMode::Full) {
            bases
                .into_iter()
                .map(|base| base.knowledge_base_sys_id)
                .collect::<Vec<_>>()
        } else {
            let mut base_ids = changed_base_sys_ids.iter().cloned().collect::<Vec<_>>();
            base_ids.sort();
            base_ids
        };

        for base_sys_id in base_ids_to_rebuild {
            let Some(base_name) = active_base_names.get(&base_sys_id) else {
                if let Some(base_dir) =
                    find_base_directory_by_sys_id(&knowledge_root.join("bases"), &base_sys_id)?
                {
                    self.ctx.vault.delete(base_dir.join("INDEX.md"))?;
                }
                continue;
            };
            let base_dir = layout.knowledge_base_dir(&base_sys_id, base_name);
            let rows = self
                .ctx
                .query
                .store()
                .list_active_knowledge_index_rows_for_base(&base_sys_id)?;
            if rows.is_empty() {
                self.ctx.vault.delete(base_dir.join("INDEX.md"))?;
                continue;
            }
            self.ctx.vault.write_markdown_file(
                base_dir.join("INDEX.md"),
                &render_base_index_from_rows(self, &base_sys_id, base_name, &rows)?,
            )?;
        }

        Ok(())
    }

    fn observe_existing_base_for_record(
        &self,
        record: &Record,
        progress: &mut WatermarkProgress,
    ) -> Result<()> {
        let new_base = KnowledgeResource::knowledge_base_reference(record)
            .map(|reference| reference.sys_id)
            .unwrap_or_default();
        if let Some(existing) = self
            .ctx
            .query
            .store()
            .get_knowledge_article(&record.sys_id)?
            && !existing.knowledge_base_sys_id.is_empty()
            && existing.knowledge_base_sys_id != new_base
        {
            progress
                .changed_base_sys_ids
                .insert(existing.knowledge_base_sys_id);
        }
        Ok(())
    }

    async fn load_active_knowledge_articles(&self) -> Result<Vec<KnowledgeArticle>> {
        let rows = self
            .ctx
            .query
            .store()
            .list_active_records(Some(ResourceType::Knowledge))?;
        let mut articles = Vec::with_capacity(rows.len());
        let mut seen = HashSet::new();
        for row in rows {
            if !seen.insert(row.sys_id.clone()) {
                continue;
            }
            if let Some(article) = self.load_existing_knowledge_article(&row.sys_id) {
                articles.push(article);
                continue;
            }
            if let Some(article) = self.ctx.query.get_knowledge_article(&row.number).await?
                && article.record.sys_id == row.sys_id
            {
                articles.push(article);
            }
        }
        Ok(articles)
    }

    async fn load_knowledge_articles_by_sys_ids(
        &self,
        record_sys_ids: &HashSet<String>,
    ) -> Result<Vec<KnowledgeArticle>> {
        let mut record_sys_ids = record_sys_ids.iter().cloned().collect::<Vec<_>>();
        record_sys_ids.sort();
        let mut articles = Vec::new();
        for record_sys_id in record_sys_ids {
            let Some(row) = self
                .ctx
                .query
                .store()
                .get_record_by_sys_id(&record_sys_id)?
            else {
                continue;
            };
            if !row.in_scope {
                continue;
            }
            if let Some(article) = self.load_existing_knowledge_article(&record_sys_id) {
                articles.push(article);
                continue;
            }
            if let Some(article) = self.ctx.query.get_knowledge_article(&row.number).await?
                && article.record.sys_id == record_sys_id
            {
                articles.push(article);
            }
        }
        Ok(articles)
    }
}

impl CoreContext {
    pub(crate) fn build_knowledge_article(&self, record: &Record) -> Result<KnowledgeArticle> {
        let knowledge_base = KnowledgeResource::knowledge_base_reference(record)
            .unwrap_or_else(|| empty_reference("kb_knowledge_base"));
        let category = KnowledgeResource::category_reference(record)
            .unwrap_or_else(|| empty_reference("kb_category"));
        let mut article = normalize_knowledge_article(KnowledgeResource::from_servicenow(
            record,
            knowledge_base,
            category,
        ));

        if let Some(existing_article) = self.load_existing_knowledge_article(&record.sys_id) {
            if article.content.trim().is_empty()
                && existing_article.body_cached
                && !existing_article.content.trim().is_empty()
            {
                article.content = existing_article.content;
            }
            article.user_tags = existing_article.user_tags;
            article.auto_tags = existing_article.auto_tags;
            article.body_cached = article.body_cached || existing_article.body_cached;
        } else if let Some(existing_row) =
            self.query.store().get_knowledge_article(&record.sys_id)?
        {
            article.user_tags = existing_row.user_tags;
            article.auto_tags = existing_row.auto_tags;
            article.body_cached = article.body_cached || existing_row.body_cached;
        }

        article.sn_tags = derive_servicenow_tags(record);
        Ok(normalize_knowledge_article(article))
    }

    pub(crate) fn load_existing_knowledge_article(
        &self,
        record_sys_id: &str,
    ) -> Option<KnowledgeArticle> {
        let row = self
            .query
            .store()
            .get_record_by_sys_id(record_sys_id)
            .ok()
            .flatten()?;
        let relative_path = row.file_path?;
        self.vault
            .read_knowledge_article(self.vault_path.join(relative_path))
            .ok()
    }
}

#[derive(Debug, Clone)]
struct IndexEntry {
    number: String,
    title: String,
    article_relative_path: PathBuf,
    tags: Vec<String>,
}

impl KnowledgeTagLayer {
    fn as_store_str(self) -> &'static str {
        match self {
            Self::Sn => "sn",
            Self::Auto => "auto",
            Self::User => "user",
        }
    }
}

fn determine_sync_mode(
    requested_full: bool,
    full_sync_interval: Option<&str>,
    last_full_at: Option<DateTime<Utc>>,
    watermark_updated_at: Option<&str>,
    watermark_sys_id: Option<&str>,
    now: DateTime<Utc>,
) -> KnowledgeSyncMode {
    if requested_full || watermark_updated_at.is_none() || watermark_sys_id.is_none() {
        return KnowledgeSyncMode::Full;
    }

    if let Some(interval) = full_sync_interval.and_then(parse_duration) {
        let stale = last_full_at
            .and_then(|last_full_at| now.signed_duration_since(last_full_at).to_std().ok())
            .map(|elapsed| elapsed >= interval)
            .unwrap_or(true);
        if stale {
            return KnowledgeSyncMode::Full;
        }
    }

    KnowledgeSyncMode::Incremental
}

fn parse_duration(input: &str) -> Option<Duration> {
    if input.len() < 2 {
        return None;
    }
    let (value, unit) = input.split_at(input.len() - 1);
    let value = value.trim().parse::<u64>().ok()?;
    match unit {
        "s" => Some(Duration::from_secs(value)),
        "m" => Some(Duration::from_secs(value * 60)),
        "h" => Some(Duration::from_secs(value * 60 * 60)),
        "d" => Some(Duration::from_secs(value * 60 * 60 * 24)),
        _ => None,
    }
}

fn apply_simple_encoded_filter(mut query: TableApi, filter: &str) -> Result<TableApi> {
    for term in filter
        .split('^')
        .map(str::trim)
        .filter(|term| !term.is_empty())
    {
        if let Some((field, value)) = term.split_once("!=") {
            query = query.not_equals(field.trim(), value.trim());
            continue;
        }
        if let Some((field, value)) = term.split_once('=') {
            query = query.equals(field.trim(), value.trim());
            continue;
        }
        return Err(anyhow!("unsupported knowledge filter `{term}`"));
    }
    Ok(query)
}

fn derive_servicenow_tags(record: &Record) -> Vec<String> {
    let mut tags = Vec::new();
    let mut seen = HashSet::new();
    for field in KB_SN_TAG_FIELDS {
        let Some(raw) = record.get_raw(field).or_else(|| record.get_str(field)) else {
            continue;
        };

        if let Ok(values) = serde_json::from_str::<Vec<String>>(raw) {
            for value in values {
                push_tag(&mut tags, &mut seen, &value);
            }
            continue;
        }

        for token in raw.split([',', '\n', ';']) {
            push_tag(&mut tags, &mut seen, token);
        }
    }
    tags
}

fn derive_auto_tags_with_stats(
    article: &KnowledgeArticle,
    term_stats: &HashMap<String, usize>,
    corpus_size: usize,
    max_auto_tags: usize,
) -> Vec<String> {
    if corpus_size == 0 {
        return Vec::new();
    }

    let blocked = article
        .sn_tags
        .iter()
        .map(|tag| tag.trim().to_ascii_lowercase())
        .collect::<HashSet<_>>();
    let mut term_frequency = HashMap::<String, usize>::new();
    for token in tokenize_article(article) {
        *term_frequency.entry(token).or_default() += 1;
    }

    let mut scored = term_frequency
        .into_iter()
        .filter_map(|(token, count)| {
            if blocked.contains(&token) {
                return None;
            }
            let df = *term_stats.get(&token).unwrap_or(&1) as f64;
            let idf = ((corpus_size as f64 + 1.0) / (df + 1.0)).ln() + 1.0;
            Some((token, (count as f64) * idf, count))
        })
        .collect::<Vec<_>>();
    scored.sort_by(|left, right| {
        right
            .1
            .partial_cmp(&left.1)
            .unwrap_or(Ordering::Equal)
            .then_with(|| right.2.cmp(&left.2))
            .then_with(|| left.0.cmp(&right.0))
    });

    let mut tags = Vec::new();
    let mut seen = HashSet::new();
    for (token, _, _) in scored {
        if !seen.insert(token.clone()) {
            continue;
        }
        tags.push(token);
        if tags.len() >= max_auto_tags {
            break;
        }
    }
    tags
}

#[cfg(test)]
fn derive_corpus_auto_tags(
    articles: &[KnowledgeArticle],
    max_auto_tags: usize,
) -> HashMap<String, Vec<String>> {
    let docs = articles
        .iter()
        .cloned()
        .map(|article| CorpusTagDoc {
            tokens: unique_terms_for_article(&article),
            article,
        })
        .collect::<Vec<_>>();
    let term_entries = docs
        .iter()
        .map(|doc| (doc.article.record.sys_id.clone(), doc.tokens.clone()))
        .collect::<Vec<_>>();
    let doc_frequency = term_stats_from_entries(&term_entries);

    let mut derived = HashMap::new();
    for doc in docs {
        derived.insert(
            doc.article.record.sys_id.clone(),
            derive_auto_tags_with_stats(
                &doc.article,
                &doc_frequency,
                articles.len(),
                max_auto_tags,
            ),
        );
    }

    derived
}

fn unique_terms_for_article(article: &KnowledgeArticle) -> Vec<String> {
    let mut unique = tokenize_article(article);
    unique.sort();
    unique.dedup();
    unique
}

fn term_stats_from_entries(entries: &[(String, Vec<String>)]) -> HashMap<String, usize> {
    let mut stats = HashMap::<String, usize>::new();
    for (_, terms) in entries {
        for term in terms {
            *stats.entry(term.clone()).or_default() += 1;
        }
    }
    stats
}

fn increment_term_stats(stats: &mut HashMap<String, usize>, terms: &[String]) {
    for term in terms {
        *stats.entry(term.clone()).or_default() += 1;
    }
}

fn decrement_term_stats(stats: &mut HashMap<String, usize>, terms: &[String]) {
    for term in terms {
        let remove = match stats.get_mut(term) {
            Some(count) if *count > 1 => {
                *count -= 1;
                false
            }
            Some(_) => true,
            None => false,
        };
        if remove {
            stats.remove(term);
        }
    }
}

fn tokenize_article(article: &KnowledgeArticle) -> Vec<String> {
    let mut text = String::new();
    for _ in 0..3 {
        text.push_str(&article.record.short_description);
        text.push(' ');
    }
    for _ in 0..2 {
        text.push_str(&article.record.description);
        text.push(' ');
    }
    text.push_str(&strip_html(&article.content));

    text.split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter_map(|token| {
            let normalized = token.trim().to_ascii_lowercase();
            if normalized.len() < 3 || KB_STOPWORDS.contains(&normalized.as_str()) {
                return None;
            }
            Some(normalized)
        })
        .collect()
}

fn strip_html(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut in_tag = false;
    for ch in input.chars() {
        match ch {
            '<' => in_tag = true,
            '>' => {
                in_tag = false;
                out.push(' ');
            }
            _ if !in_tag => out.push(ch),
            _ => {}
        }
    }
    out
}

fn merged_top_tags(
    sn_tags: &[String],
    auto_tags: &[String],
    user_tags: &[String],
    limit: usize,
) -> Vec<String> {
    let mut tags = Vec::new();
    let mut seen = HashSet::new();
    for tag in sn_tags.iter().chain(auto_tags).chain(user_tags) {
        let normalized = tag.trim().to_ascii_lowercase();
        if normalized.is_empty() || !seen.insert(normalized.clone()) {
            continue;
        }
        tags.push(normalized);
        if tags.len() >= limit {
            break;
        }
    }
    tags
}

fn file_modified_at_ms(path: &Path) -> Result<i64> {
    let metadata = fs::metadata(path)?;
    let modified = metadata.modified()?;
    let duration = modified
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|err| anyhow!("invalid file mtime for {}: {err}", path.display()))?;
    Ok(duration.as_millis() as i64)
}

fn relative_from_base_index(file_path: &Path, base_relative_root: &Path) -> Option<PathBuf> {
    file_path
        .strip_prefix(base_relative_root)
        .ok()
        .map(Path::to_path_buf)
}

fn find_base_directory_by_sys_id(bases_root: &Path, base_sys_id: &str) -> Result<Option<PathBuf>> {
    if !bases_root.exists() {
        return Ok(None);
    }
    let prefix = format!("kb_{base_sys_id}_");
    for entry in fs::read_dir(bases_root)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Some(file_name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        if file_name == format!("kb_{base_sys_id}") || file_name.starts_with(&prefix) {
            return Ok(Some(path));
        }
    }
    Ok(None)
}

type GlobalIndexGroups = BTreeMap<(String, String, String), BTreeMap<(String, String), usize>>;

fn render_global_index(
    grouped: &GlobalIndexGroups,
    tag_summaries: &[KnowledgeTagSummary],
    last_synced: DateTime<Utc>,
) -> String {
    let article_count = grouped
        .values()
        .flat_map(|categories| categories.values())
        .copied()
        .sum::<usize>();
    let mut out = String::new();
    out.push_str("# ServiceNow Knowledge Catalog\n\n");
    out.push_str(&format!(
        "_Last synced: {} · {} articles across {} knowledge bases_\n\n",
        last_synced.format("%Y-%m-%d %H:%M UTC"),
        article_count,
        grouped.len()
    ));

    for ((_, base_name, base_slug), categories) in grouped {
        let base_count = categories.values().copied().sum::<usize>();
        out.push_str(&format!("## {} ({})\n", base_name, base_count));
        for ((_, category_name), article_count) in categories {
            let category_anchor = slugify(category_name);
            out.push_str(&format!(
                "- **[{}](bases/{}/INDEX.md#{})** ({})\n",
                category_name, base_slug, category_anchor, article_count
            ));
        }
        out.push('\n');
    }

    let top_tags = tag_summaries
        .iter()
        .take(50)
        .map(|tag| format!("`{}` ({})", tag.tag, tag.count))
        .collect::<Vec<_>>();
    if !top_tags.is_empty() {
        out.push_str("## All tags (top 50)\n");
        out.push_str(&top_tags.join(" · "));
        out.push('\n');
    }
    out
}

fn render_base_index(
    base_name: &str,
    categories: &BTreeMap<(String, String), Vec<IndexEntry>>,
) -> String {
    let mut out = String::new();
    out.push_str(&format!("# {} - Knowledge Base\n\n", base_name));
    let summary = categories
        .iter()
        .map(|((_, category_name), entries)| format!("{} in {}", entries.len(), category_name))
        .collect::<Vec<_>>()
        .join(" · ");
    if !summary.is_empty() {
        out.push_str(&format!("_{}_\n\n", summary));
    }

    for ((_, category_name), entries) in categories {
        out.push_str(&format!("## {}\n", category_name));
        out.push_str(&format!("_{} articles_\n", entries.len()));
        let mut entries = entries.clone();
        entries.sort_by(|left, right| {
            left.title
                .cmp(&right.title)
                .then_with(|| left.number.cmp(&right.number))
        });
        for entry in entries {
            let tags = entry
                .tags
                .iter()
                .map(|tag| format!("`{tag}`"))
                .collect::<Vec<_>>()
                .join(", ");
            if tags.is_empty() {
                out.push_str(&format!(
                    "- [`{}`]({}) **{}**\n",
                    entry.number,
                    entry.article_relative_path.display(),
                    entry.title
                ));
            } else {
                out.push_str(&format!(
                    "- [`{}`]({}) **{}** - {}\n",
                    entry.number,
                    entry.article_relative_path.display(),
                    entry.title,
                    tags
                ));
            }
        }
        out.push('\n');
    }
    out
}

fn render_base_index_from_rows(
    core: &SnowCore,
    base_sys_id: &str,
    base_name: &str,
    rows: &[crate::cache::store::KnowledgeIndexRow],
) -> Result<String> {
    let base_dir = core
        .ctx
        .vault
        .layout()
        .knowledge_base_dir(base_sys_id, base_name);
    let base_relative_root = core.ctx.vault.relative_path(&base_dir)?;
    let mut categories = BTreeMap::<(String, String), Vec<IndexEntry>>::new();
    for row in rows {
        let article_relative =
            relative_from_base_index(&PathBuf::from(&row.file_path), &base_relative_root)
                .unwrap_or_else(|| PathBuf::from(&row.file_path));
        let tags = merged_top_tags(&row.sn_tags, &row.auto_tags, &row.user_tags, 3);
        categories
            .entry((row.category_sys_id.clone(), row.category_name.clone()))
            .or_default()
            .push(IndexEntry {
                number: row.number.clone(),
                title: row.title.clone(),
                article_relative_path: article_relative,
                tags,
            });
    }
    Ok(render_base_index(base_name, &categories))
}

fn render_llm_tag_prompt(article: &KnowledgeArticle) -> String {
    format!(
        "Return 3 to 7 topical tags for this article as a JSON array of lowercase strings. Tags must be at most two words each.\n\nTitle: {}\nSummary: {}\nBody:\n{}",
        article.record.short_description,
        article.record.description,
        strip_html(&article.content)
    )
}

fn parse_llm_tag_response(value: &Value) -> Result<LlmTagResponse> {
    let tag_source = value
        .get("response")
        .and_then(Value::as_str)
        .unwrap_or_else(|| value.as_str().unwrap_or_default());
    let parsed = serde_json::from_str::<Value>(tag_source)
        .or_else(|_| Ok::<Value, serde_json::Error>(value.clone()))
        .context("failed to parse KB LLM tag payload")?;
    let array = if let Some(array) = parsed.as_array() {
        array.clone()
    } else if let Some(array) = parsed.get("tags").and_then(Value::as_array) {
        array.clone()
    } else {
        return Err(anyhow!("KB LLM tag payload did not contain a tag array"));
    };

    let mut tags = Vec::new();
    let mut seen = HashSet::new();
    for value in array {
        let Some(tag) = value.as_str() else {
            continue;
        };
        push_tag(&mut tags, &mut seen, tag);
    }
    Ok(LlmTagResponse { tags })
}

fn push_tag(tags: &mut Vec<String>, seen: &mut HashSet<String>, value: &str) {
    let normalized = value.trim().to_ascii_lowercase();
    if normalized.is_empty() || !seen.insert(normalized.clone()) {
        return;
    }
    tags.push(normalized);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SnowConfig;
    use crate::{CacheSource, KnowledgeSearchFilters, Reference, SnowRecord};
    use serde_json::json;
    use servicenow_rs::prelude::{BasicAuth, ServiceNowClient};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use tempfile::TempDir;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[test]
    fn sync_mode_promotes_to_full_without_watermark_or_when_stale() {
        assert_eq!(
            determine_sync_mode(false, Some("7d"), None, None, None, Utc::now()),
            KnowledgeSyncMode::Full
        );
        assert_eq!(
            determine_sync_mode(
                false,
                Some("7d"),
                Some(Utc::now() - chrono::Duration::days(10)),
                Some("2026-04-10 10:00:00"),
                Some("kb-1"),
                Utc::now(),
            ),
            KnowledgeSyncMode::Full
        );
        assert_eq!(
            determine_sync_mode(
                false,
                Some("7d"),
                Some(Utc::now()),
                Some("2026-04-10 10:00:00"),
                Some("kb-1"),
                Utc::now(),
            ),
            KnowledgeSyncMode::Incremental
        );
    }

    #[test]
    fn derives_servicenow_tags_from_multiple_fields() {
        let record = Record::from_json(
            "kb_knowledge",
            &serde_json::json!({
                "sys_id": "kb-1",
                "number": "KB001",
                "keywords": "Password Reset, SSO",
                "meta": "[\"runbook\", \"tier-1\"]",
                "sys_tags": "reset\naccount"
            }),
            DisplayValue::Both,
        )
        .expect("record");

        assert_eq!(
            derive_servicenow_tags(&record),
            vec![
                "password reset".to_string(),
                "sso".to_string(),
                "runbook".to_string(),
                "tier-1".to_string(),
                "reset".to_string(),
                "account".to_string(),
            ]
        );
    }

    #[test]
    fn parse_duration_understands_day_hour_minute_and_second_suffixes() {
        assert_eq!(parse_duration("15s"), Some(Duration::from_secs(15)));
        assert_eq!(parse_duration("2m"), Some(Duration::from_secs(120)));
        assert_eq!(parse_duration("3h"), Some(Duration::from_secs(10_800)));
        assert_eq!(parse_duration("7d"), Some(Duration::from_secs(604_800)));
        assert_eq!(parse_duration("bad"), None);
    }

    #[test]
    fn default_config_keeps_knowledge_full_sync_interval_and_tag_defaults() {
        let mut config = SnowConfig::default();
        config.apply_defaults();
        assert_eq!(
            config
                .refresh
                .resources
                .get("knowledge")
                .and_then(|resource| resource.full_sync_interval.as_deref()),
            Some("7d")
        );
        assert_eq!(config.kb.max_auto_tags, 5);
        assert_eq!(config.kb.llm_tags.timeout_seconds, 10);
    }

    #[test]
    fn corpus_tags_prefer_distinctive_terms() {
        let article = |sys_id: &str, number: &str, title: &str, summary: &str, body: &str| {
            normalize_knowledge_article(KnowledgeArticle {
                record: SnowRecord {
                    sys_id: sys_id.to_string(),
                    number: number.to_string(),
                    table: "kb_knowledge".to_string(),
                    resource_type: ResourceType::Knowledge,
                    state: "published".to_string(),
                    short_description: title.to_string(),
                    description: summary.to_string(),
                    fields: HashMap::new(),
                    work_notes: Vec::new(),
                    comments: Vec::new(),
                    parent: None,
                    children: Vec::new(),
                    references: HashMap::new(),
                    synced_at: Utc::now(),
                    source: CacheSource::Api,
                },
                knowledge_base: Reference {
                    sys_id: "base-1".to_string(),
                    table: "kb_knowledge_base".to_string(),
                    display_name: "IT".to_string(),
                    extra: HashMap::new(),
                },
                category: Reference {
                    sys_id: "cat-1".to_string(),
                    table: "kb_category".to_string(),
                    display_name: "Accounts".to_string(),
                    extra: HashMap::new(),
                },
                article_type: "text".to_string(),
                content: body.to_string(),
                sn_tags: vec!["runbook".to_string()],
                auto_tags: Vec::new(),
                user_tags: Vec::new(),
                body_cached: true,
                published_at: None,
                author: None,
                valid_to: None,
            })
        };

        let derived = derive_corpus_auto_tags(
            &[
                article(
                    "a1",
                    "KB001",
                    "Reset VPN credentials",
                    "VPN password reset",
                    "Use the vpn portal to rotate the vpn token and reconnect.",
                ),
                article(
                    "a2",
                    "KB002",
                    "Onboard a laptop",
                    "Laptop setup guide",
                    "Provision the laptop, install mdm, and complete workstation onboarding.",
                ),
            ],
            5,
        );

        assert!(derived["a1"].contains(&"vpn".to_string()));
        assert!(
            derived["a2"]
                .iter()
                .any(|tag| tag == "laptop" || tag == "workstation")
        );
    }

    #[test]
    fn parses_llm_tag_payloads() {
        let parsed = parse_llm_tag_response(&json!({
            "response": "[\"vpn\", \"runbook\", \"macos\"]"
        }))
        .expect("parsed");
        assert_eq!(parsed.tags, vec!["vpn", "runbook", "macos"]);

        let parsed = parse_llm_tag_response(&json!({
            "tags": ["password", "reset"]
        }))
        .expect("parsed");
        assert_eq!(parsed.tags, vec!["password", "reset"]);
    }

    fn mock_kb_record(idx: usize, base_idx: usize, category_idx: usize, updated_at: &str) -> Value {
        let number = format!("KB{:07}", idx + 1);
        let sys_id = format!("kb-sys-{idx:05}");
        let base_sys_id = format!("kb-base-{base_idx:02}");
        let category_sys_id = format!("kb-cat-{base_idx:02}-{category_idx:02}");
        let base_name = match base_idx % 4 {
            0 => "IT Operations",
            1 => "HR Services",
            2 => "Security",
            _ => "Employee Enablement",
        };
        let category_name = match category_idx % 4 {
            0 => "Accounts",
            1 => "Networking",
            2 => "Devices",
            _ => "Benefits",
        };
        let topic = if idx.is_multiple_of(2) {
            "vpn"
        } else {
            "laptop"
        };
        let platform = if idx.is_multiple_of(3) {
            "macos"
        } else {
            "windows"
        };
        json!({
            "sys_id": sys_id,
            "number": number,
            "short_description": format!("{topic} article {idx}"),
            "description": format!("How to use {topic} on {platform}"),
            "state": "published",
            "workflow_state": "published",
            "article_type": "text",
            "valid_to": "2030-12-31",
            "published": "2026-04-01 12:00:00",
            "knowledge_base": {
                "value": base_sys_id,
                "display_value": base_name
            },
            "category": {
                "value": category_sys_id,
                "display_value": category_name
            },
            "author": {
                "value": "user-sys",
                "display_value": "Casey User"
            },
            "sys_updated_on": updated_at,
            "u_keywords": format!("{topic}, {platform}"),
            "keywords": format!("{topic}, runbook"),
            "meta": format!("[\"{}\", \"tier-1\"]", platform),
            "sys_tags": "published\nmock",
            "article_body": format!(
                "<p>{topic} {platform} troubleshooting guide {idx}. Use {topic} reconnect steps and {platform} remediation.</p>"
            ),
            "text": format!("{topic} {platform} troubleshooting guide {idx}")
        })
    }

    async fn build_mock_core(
        server: &MockServer,
        vault_path: PathBuf,
        config_mutator: impl FnOnce(&mut SnowConfig),
    ) -> SnowCore {
        let client = ServiceNowClient::builder()
            .instance(server.uri())
            .auth(BasicAuth::new("test_user", "test_pass"))
            .allow_http()
            .build()
            .await
            .expect("client");
        let mut config = SnowConfig::default();
        config.apply_defaults();
        config_mutator(&mut config);

        SnowCore::builder()
            .client(client)
            .config(config)
            .vault_path(vault_path)
            .build()
            .await
            .expect("core")
    }

    async fn mount_kb_dataset(server: &MockServer, dataset: Arc<Mutex<Vec<Value>>>) {
        Mock::given(method("GET"))
            .and(path("/api/now/table/kb_knowledge"))
            .respond_with(move |request: &wiremock::Request| {
                let query = request
                    .url
                    .query_pairs()
                    .map(|(key, value)| (key.to_string(), value.to_string()))
                    .collect::<HashMap<_, _>>();
                let offset = query
                    .get("sysparm_offset")
                    .and_then(|value| value.parse::<usize>().ok())
                    .unwrap_or(0);
                let limit = query
                    .get("sysparm_limit")
                    .and_then(|value| value.parse::<usize>().ok())
                    .unwrap_or(KB_PAGE_SIZE as usize);
                let data = dataset.lock().expect("dataset");
                let filtered = data
                    .iter()
                    .filter(|record| {
                        query
                            .get("sysparm_query")
                            .map(|encoded| mock_record_matches_query(record, encoded))
                            .unwrap_or(true)
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                let result = filtered
                    .iter()
                    .skip(offset)
                    .take(limit)
                    .cloned()
                    .collect::<Vec<_>>();
                ResponseTemplate::new(200).set_body_json(json!({ "result": result }))
            })
            .mount(server)
            .await;
    }

    fn mock_record_matches_query(record: &Value, encoded: &str) -> bool {
        for term in encoded
            .split('^')
            .map(str::trim)
            .filter(|term| !term.is_empty())
        {
            if let Some((field, value)) = term.split_once("!=") {
                if mock_field_string(record, field) == Some(value.to_string()) {
                    return false;
                }
                continue;
            }
            if let Some((field, value)) = term.split_once('>') {
                let Some(current) = mock_field_string(record, field) else {
                    return false;
                };
                if current.as_str() <= value {
                    return false;
                }
                continue;
            }
            if let Some((field, value)) = term.split_once('=') {
                if mock_field_string(record, field) != Some(value.to_string()) {
                    return false;
                }
                continue;
            }
        }
        true
    }

    fn mock_field_string(record: &Value, field: &str) -> Option<String> {
        let value = record.get(field)?;
        if let Some(value) = value.as_str() {
            return Some(value.to_string());
        }
        value
            .get("value")
            .and_then(Value::as_str)
            .map(|value| value.to_string())
    }

    #[tokio::test]
    #[ignore = "KB integration"]
    async fn full_sync_reconciles_missing_rows_and_rebuilds_indexes() {
        let server = MockServer::start().await;
        let dataset = Arc::new(Mutex::new(vec![
            mock_kb_record(0, 0, 0, "2026-04-16 10:00:00"),
            mock_kb_record(1, 0, 1, "2026-04-16 10:00:01"),
            mock_kb_record(2, 1, 0, "2026-04-16 10:00:02"),
        ]));
        mount_kb_dataset(&server, dataset.clone()).await;

        let tempdir = TempDir::new().expect("tempdir");
        let core = build_mock_core(&server, tempdir.path().join("vault"), |_| {}).await;

        core.sync_knowledge(true, true).await.expect("first sync");
        let global_index = core.vault_path().join("knowledge/INDEX.md");
        assert!(global_index.exists());
        let global_body = std::fs::read_to_string(&global_index).expect("global index");
        assert!(global_body.contains("ServiceNow Knowledge Catalog"));

        dataset.lock().expect("dataset").remove(1);
        core.sync_knowledge(true, true).await.expect("second sync");

        let store = core.ctx.query.store();
        let missing = store
            .get_record_by_sys_id("kb-sys-00001")
            .expect("row present")
            .expect("row present");
        assert_eq!(
            missing.lifecycle(),
            crate::cache::store::RecordLifecycle::Tombstoned
        );
        assert_eq!(store.count_knowledge_articles().expect("count"), 2);

        core.sync_knowledge(true, true).await.expect("third sync");
        assert!(
            store
                .get_record_by_sys_id(&missing.sys_id)
                .expect("lookup")
                .is_none()
        );
    }

    #[tokio::test]
    #[ignore = "KB integration"]
    async fn sync_updates_auto_tags_and_generates_searchable_indexes() {
        let server = MockServer::start().await;
        let dataset = Arc::new(Mutex::new(vec![
            mock_kb_record(0, 0, 0, "2026-04-16 10:00:00"),
            mock_kb_record(1, 0, 0, "2026-04-16 10:00:01"),
            mock_kb_record(2, 0, 1, "2026-04-16 10:00:02"),
            mock_kb_record(3, 1, 0, "2026-04-16 10:00:03"),
        ]));
        mount_kb_dataset(&server, dataset).await;

        let tempdir = TempDir::new().expect("tempdir");
        let core = build_mock_core(&server, tempdir.path().join("vault"), |_| {}).await;
        core.sync_knowledge(true, true).await.expect("sync");

        let auto_tags = core
            .list_knowledge_tags(Some(KnowledgeTagLayer::Auto), 1)
            .expect("auto tags");
        assert!(!auto_tags.is_empty());
        assert!(auto_tags.iter().any(|row| row.count >= 1));

        let results = core
            .search_knowledge(
                "vpn",
                KnowledgeSearchFilters {
                    limit: Some(10),
                    ..KnowledgeSearchFilters::default()
                },
            )
            .await
            .expect("search");
        assert!(!results.is_empty());

        let first_row = core
            .ctx
            .query
            .store()
            .get_record_by_sys_id("kb-sys-00000")
            .expect("record lookup")
            .expect("record row");
        let first_path = core
            .vault_path()
            .join(first_row.file_path.expect("file path"));
        let parsed = core
            .ctx
            .vault
            .read_knowledge_article(&first_path)
            .expect("parsed article");
        assert!(!parsed.auto_tags.is_empty());

        assert!(core.vault_path().join("knowledge/INDEX.md").exists());
    }

    #[tokio::test]
    #[ignore = "KB integration"]
    async fn incremental_sync_rebuilds_only_changed_base_index() {
        let server = MockServer::start().await;
        let dataset = Arc::new(Mutex::new(vec![
            mock_kb_record(0, 0, 0, "2026-04-16 10:00:00"),
            mock_kb_record(1, 0, 1, "2026-04-16 10:00:01"),
            mock_kb_record(2, 1, 0, "2026-04-16 10:00:02"),
            mock_kb_record(3, 1, 1, "2026-04-16 10:00:03"),
        ]));
        mount_kb_dataset(&server, dataset.clone()).await;

        let tempdir = TempDir::new().expect("tempdir");
        let core = build_mock_core(&server, tempdir.path().join("vault"), |_| {}).await;
        core.sync_knowledge(true, true).await.expect("full sync");

        let base_zero = core
            .ctx
            .vault
            .layout()
            .knowledge_base_dir("kb-base-00", "IT Operations")
            .join("INDEX.md");
        let base_one = core
            .ctx
            .vault
            .layout()
            .knowledge_base_dir("kb-base-01", "HR Services")
            .join("INDEX.md");
        let before_zero = file_modified_at_ms(&base_zero).expect("base zero mtime");
        let before_one = file_modified_at_ms(&base_one).expect("base one mtime");

        std::thread::sleep(Duration::from_millis(20));
        dataset.lock().expect("dataset")[0]["sys_updated_on"] = json!("2026-04-16 11:00:00");
        let outcome = core
            .sync_knowledge(false, false)
            .await
            .expect("incremental sync");
        assert_eq!(outcome.mode, KnowledgeSyncMode::Incremental);

        let after_zero = file_modified_at_ms(&base_zero).expect("base zero mtime after");
        let after_one = file_modified_at_ms(&base_one).expect("base one mtime after");
        assert!(after_zero > before_zero);
        assert_eq!(after_one, before_one);
    }

    #[tokio::test]
    #[ignore = "performance baseline"]
    async fn knowledge_sync_baseline_large_mock_corpus() {
        let server = MockServer::start().await;
        let dataset = Arc::new(Mutex::new(
            (0..5_000)
                .map(|idx| mock_kb_record(idx, idx % 10, idx % 5, "2026-04-16 10:00:00"))
                .collect::<Vec<_>>(),
        ));
        mount_kb_dataset(&server, dataset).await;

        let tempdir = TempDir::new().expect("tempdir");
        let core = build_mock_core(&server, tempdir.path().join("vault"), |_| {}).await;

        let start = std::time::Instant::now();
        let outcome = core.sync_knowledge(true, true).await.expect("sync");
        let elapsed = start.elapsed();
        let status = core.knowledge_status().expect("status");
        eprintln!(
            "kb baseline: accepted={} mode={:?} articles={} cached_bodies={} elapsed_ms={}",
            outcome.accepted,
            outcome.mode,
            status.article_count,
            status.body_cached_count,
            elapsed.as_millis()
        );

        assert!(outcome.accepted);
        assert_eq!(status.article_count, 5_000);
        assert_eq!(status.body_cached_count, 5_000);
        assert!(core.vault_path().join("knowledge/INDEX.md").exists());
    }

    #[tokio::test]
    #[ignore = "performance baseline"]
    async fn knowledge_incremental_baseline_large_mock_corpus() {
        let server = MockServer::start().await;
        let dataset = Arc::new(Mutex::new(
            (0..5_000)
                .map(|idx| mock_kb_record(idx, idx % 10, idx % 5, "2026-04-16 10:00:00"))
                .collect::<Vec<_>>(),
        ));
        mount_kb_dataset(&server, dataset.clone()).await;

        let tempdir = TempDir::new().expect("tempdir");
        let core = build_mock_core(&server, tempdir.path().join("vault"), |_| {}).await;
        core.sync_knowledge(true, true)
            .await
            .expect("seed full sync");

        dataset.lock().expect("dataset")[0]["sys_updated_on"] = json!("2026-04-16 11:00:00");
        let start = std::time::Instant::now();
        let outcome = core
            .sync_knowledge(false, false)
            .await
            .expect("incremental sync");
        let elapsed = start.elapsed();
        eprintln!(
            "kb incremental baseline: accepted={} mode={:?} elapsed_ms={}",
            outcome.accepted,
            outcome.mode,
            elapsed.as_millis()
        );

        assert!(outcome.accepted);
        assert_eq!(outcome.mode, KnowledgeSyncMode::Incremental);
    }
}
