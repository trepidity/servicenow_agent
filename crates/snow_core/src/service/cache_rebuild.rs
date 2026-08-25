use anyhow::{Context, Result};
use chrono::Utc;
use servicenow_rs::prelude::{DisplayValue, Order};
use servicenow_rs::query::builder::TableApi;

use crate::cache::policy::CacheMode;
use crate::context::CoreContext;
use crate::{
    CacheRebuildKnowledgeScope, CacheRebuildProgressEvent, CacheRebuildProgressSink,
    ServiceNowCacheRebuildReport, ServiceNowCacheRebuildTableReport,
};

const REBUILD_SCOPE: &[(&str, &str, &str)] = &[
    ("incident", "incident", "incident"),
    ("change", "change_request", "change_request"),
    ("knowledge", "knowledge", "kb_knowledge"),
    (
        "business_application",
        "business_application",
        "cmdb_ci_business_app",
    ),
    (
        "service_catalog_product",
        "service_catalog_product",
        "sc_cat_item",
    ),
    ("server", "server", "cmdb_ci_server"),
];

#[derive(Clone)]
pub(crate) struct CacheRebuildService {
    ctx: CoreContext,
}

struct TableDrainProgress<'a> {
    index: usize,
    tables: usize,
    total_records: &'a mut usize,
    sink: &'a CacheRebuildProgressSink,
}

impl CacheRebuildService {
    pub(crate) fn new(ctx: CoreContext) -> Self {
        Self { ctx }
    }

    pub async fn rebuild_from_servicenow(
        &self,
        progress: &CacheRebuildProgressSink,
        page_limit: u32,
        knowledge_scope: &CacheRebuildKnowledgeScope,
    ) -> Result<ServiceNowCacheRebuildReport> {
        let policy = self.ctx.named_cache_policy.active();
        if knowledge_scope.knowledge_category_sys_id.is_some()
            && knowledge_scope.knowledge_base_sys_id.is_none()
        {
            anyhow::bail!("knowledge category requires a knowledge base scope");
        }
        let knowledge_base_sys_id = knowledge_scope
            .knowledge_base_sys_id
            .as_deref()
            .or_else(|| policy.knowledge_rebuild_base_sys_id())
            .map(str::to_owned);
        let enabled_tables = REBUILD_SCOPE
            .iter()
            .filter(|(_, object, _)| {
                policy.rule_for(rebuild_operation(object), object).mode != CacheMode::Live
                    && (*object != "knowledge" || knowledge_base_sys_id.is_some())
            })
            .copied()
            .collect::<Vec<_>>();
        progress(CacheRebuildProgressEvent::Tables {
            tables: enabled_tables.len(),
            page_size: page_limit,
        })?;
        let mut tables = Vec::new();
        let mut total_records = 0usize;
        for (position, (resource, _object, table)) in enabled_tables.iter().enumerate() {
            let index = position + 1;
            let query = match *resource {
                "business_application" => self
                    .base_query(table, page_limit)
                    .equals("sys_class_name", "cmdb_ci_business_app"),
                "knowledge" => {
                    let knowledge_base_sys_id =
                        knowledge_base_sys_id.as_deref().ok_or_else(|| {
                            anyhow::anyhow!("knowledge rebuild scope was not captured")
                        })?;
                    let query = self
                        .base_query(table, page_limit)
                        .equals("kb_knowledge_base", knowledge_base_sys_id);
                    if let Some(knowledge_category_sys_id) =
                        knowledge_scope.knowledge_category_sys_id.as_deref()
                    {
                        query.equals("kb_category", knowledge_category_sys_id)
                    } else {
                        query
                    }
                }
                _ => self.base_query(table, page_limit),
            };
            tables.push(
                self.drain_table(
                    resource,
                    table,
                    query,
                    TableDrainProgress {
                        index,
                        tables: enabled_tables.len(),
                        total_records: &mut total_records,
                        sink: progress,
                    },
                )
                .await?,
            );
        }

        Ok(ServiceNowCacheRebuildReport {
            source: "ServiceNow".to_string(),
            scope: if knowledge_scope.knowledge_base_sys_id.is_some() {
                "cache-policy-selected tables with command-scoped Knowledge projection".to_string()
            } else {
                "cache-policy-scoped ACL-readable projection".to_string()
            },
            pages: tables.iter().map(|table| table.pages).sum(),
            records: tables.iter().map(|table| table.records).sum(),
            tables,
            complete: true,
        })
    }

    fn base_query(&self, table: &str, page_limit: u32) -> TableApi {
        self.ctx
            .client
            .table(table)
            .display_value(DisplayValue::Both)
            .exclude_reference_link(true)
            .order_by("sys_id", Order::Asc)
            .limit(page_limit)
    }

    async fn drain_table(
        &self,
        resource: &str,
        table: &str,
        query: TableApi,
        progress: TableDrainProgress<'_>,
    ) -> Result<ServiceNowCacheRebuildTableReport> {
        (progress.sink)(CacheRebuildProgressEvent::TableStarted {
            index: progress.index,
            tables: progress.tables,
            resource: resource.to_string(),
            table: table.to_string(),
        })?;
        let mut paginator = query.paginate()?;
        let mut pages = 0usize;
        let mut records = 0usize;
        loop {
            if paginator.is_done() {
                break;
            }
            let page_number = pages + 1;
            (progress.sink)(CacheRebuildProgressEvent::RequestingPage {
                index: progress.index,
                tables: progress.tables,
                resource: resource.to_string(),
                table: table.to_string(),
                page: page_number,
            })?;
            let Some(page) = paginator
                .next_page()
                .await
                .with_context(|| format!("reading ServiceNow rebuild page for {table}"))?
            else {
                break;
            };
            if !page.errors.is_empty() {
                anyhow::bail!(
                    "ServiceNow rebuild page for {table} contained related-record errors: {:?}",
                    page.errors
                );
            }
            self.ctx
                .project_live_records_without_vault(&page.records)
                .with_context(|| format!("projecting ServiceNow rebuild page for {table}"))?;
            if table == "sc_cat_item" {
                let refreshed_at = Utc::now();
                for record in &page.records {
                    let item =
                        crate::resource::catalog::catalog_item_from_record(record, Vec::new());
                    self.ctx
                        .query
                        .store()
                        .upsert_narrowed_catalog_product(&item, refreshed_at)
                        .with_context(|| {
                            format!("projecting narrowed catalog product {}", record.sys_id)
                        })?;
                }
            }
            pages += 1;
            let page_records = page.records.len();
            records += page_records;
            *progress.total_records += page_records;
            (progress.sink)(CacheRebuildProgressEvent::PageProjected {
                index: progress.index,
                tables: progress.tables,
                resource: resource.to_string(),
                table: table.to_string(),
                page: page_number,
                page_records,
                table_records: records,
                total_records: *progress.total_records,
            })?;
        }
        (progress.sink)(CacheRebuildProgressEvent::TableCompleted {
            index: progress.index,
            tables: progress.tables,
            resource: resource.to_string(),
            table: table.to_string(),
            pages,
            records,
        })?;
        Ok(ServiceNowCacheRebuildTableReport {
            resource: resource.to_string(),
            table: table.to_string(),
            pages,
            records,
        })
    }
}

fn rebuild_operation(object: &str) -> &str {
    match object {
        "knowledge" => "get_article",
        "business_application" => "business_application_get",
        "service_catalog_product" => "catalog_item_get",
        "server" => "server_get",
        "incident" => "incident_get",
        "change_request" => "change_request_get",
        _ => "",
    }
}
