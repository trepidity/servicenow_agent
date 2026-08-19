use anyhow::{Context, Result};
use servicenow_rs::prelude::{DisplayValue, Order};
use servicenow_rs::query::builder::TableApi;

use crate::context::CoreContext;
use crate::{
    CacheRebuildProgressEvent, CacheRebuildProgressSink, ServiceNowCacheRebuildReport,
    ServiceNowCacheRebuildTableReport,
};

const PAGE_SIZE: u32 = 100;
const REBUILD_SCOPE: &[(&str, &str)] = &[
    ("incident", "incident"),
    ("change", "change_request"),
    ("change_task", "change_task"),
    ("request", "sc_req_item"),
    ("request_task", "sc_task"),
    ("story", "rm_story"),
    ("scrum_task", "rm_scrum_task"),
    ("knowledge", "kb_knowledge"),
    ("approval", "sysapproval_approver"),
    ("project", "pm_project"),
    ("demand", "dmn_demand"),
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
    ) -> Result<ServiceNowCacheRebuildReport> {
        self.reject_unknown_enabled_resources()?;
        let enabled_tables = REBUILD_SCOPE
            .iter()
            .filter_map(|(resource, table)| {
                self.ctx
                    .config
                    .refresh
                    .resources
                    .get(*resource)
                    .filter(|config| config.enabled)
                    .map(|_| (*resource, *table))
            })
            .chain(std::iter::once((
                "business_application",
                "cmdb_ci_business_app",
            )))
            .collect::<Vec<_>>();
        progress(CacheRebuildProgressEvent::Tables {
            tables: enabled_tables.len(),
            page_size: PAGE_SIZE,
        })?;
        let needs_user = enabled_tables.iter().any(|(resource, _)| {
            self.ctx
                .config
                .refresh
                .resources
                .get(*resource)
                .is_some_and(|config| config.filter.contains("{{user}}"))
        });
        let user_sys_id = if needs_user {
            progress(CacheRebuildProgressEvent::ResolvingUserScope)?;
            let user_sys_id = self.ctx.current_user_sys_id().await?;
            progress(CacheRebuildProgressEvent::UserScopeResolved)?;
            Some(user_sys_id)
        } else {
            None
        };

        let mut tables = Vec::new();
        let mut total_records = 0usize;
        for (position, (resource, table)) in enabled_tables.iter().enumerate() {
            let index = position + 1;
            let query = if *resource == "business_application" {
                self.base_query(table)
                    .equals("sys_class_name", "cmdb_ci_business_app")
            } else {
                let Some(config) = self.ctx.config.refresh.resources.get(*resource) else {
                    anyhow::bail!("enabled rebuild table `{resource}` lost its configuration");
                };
                apply_configured_filter(
                    self.base_query(table),
                    &config.filter,
                    user_sys_id.as_deref(),
                )
                .with_context(|| format!("validating rebuild filter for {resource}"))?
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
            scope: "configured ACL-readable projection".to_string(),
            pages: tables.iter().map(|table| table.pages).sum(),
            records: tables.iter().map(|table| table.records).sum(),
            tables,
            complete: true,
        })
    }

    fn base_query(&self, table: &str) -> TableApi {
        self.ctx
            .client
            .table(table)
            .display_value(DisplayValue::Both)
            .exclude_reference_link(true)
            .order_by("sys_id", Order::Asc)
            .limit(PAGE_SIZE)
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

    fn reject_unknown_enabled_resources(&self) -> Result<()> {
        for (resource, config) in &self.ctx.config.refresh.resources {
            if config.enabled
                && !REBUILD_SCOPE
                    .iter()
                    .any(|(known_resource, _)| resource == known_resource)
            {
                anyhow::bail!(
                    "enabled refresh resource `{resource}` has no ServiceNow cache rebuild mapping"
                );
            }
        }
        Ok(())
    }
}

fn apply_configured_filter(
    mut query: TableApi,
    filter: &str,
    user_sys_id: Option<&str>,
) -> Result<TableApi> {
    let filter = filter.trim();
    if filter.is_empty() {
        return Ok(query);
    }
    for condition in filter.split('^') {
        if condition.is_empty() || condition.starts_with("OR") || condition.starts_with("NQ") {
            anyhow::bail!("only AND-joined equality conditions are supported");
        }
        let (field, value) = condition
            .split_once('=')
            .ok_or_else(|| anyhow::anyhow!("unsupported condition `{condition}`"))?;
        if field.trim().is_empty() || value.trim().is_empty() {
            anyhow::bail!("filter condition `{condition}` has an empty field or value");
        }
        let value = if value.trim() == "{{user}}" {
            user_sys_id.ok_or_else(|| {
                anyhow::anyhow!("filter condition `{condition}` requires a resolved current user")
            })?
        } else {
            value.trim()
        };
        query = query.equals(field.trim(), value);
    }
    Ok(query)
}
