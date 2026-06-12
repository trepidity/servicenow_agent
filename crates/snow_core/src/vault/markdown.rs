use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;

use anyhow::{Context, Result, anyhow, bail};
use chrono::SecondsFormat;
use serde::{Deserialize, Serialize};

use crate::{
    ApprovalRecord, ApprovalRoutedVia, CacheSource, FieldValue, JournalEntry, KnowledgeArticle,
    RecordRef, Reference, ResourceType, SnowRecord, choose_reference_display_name,
    normalize_knowledge_article,
};

pub trait MarkdownRenderable {
    fn to_markdown(&self) -> String;
}

impl MarkdownRenderable for SnowRecord {
    fn to_markdown(&self) -> String {
        render_snow_record(self)
    }
}

impl MarkdownRenderable for KnowledgeArticle {
    fn to_markdown(&self) -> String {
        render_knowledge_article(self)
    }
}

impl MarkdownRenderable for ApprovalRecord {
    fn to_markdown(&self) -> String {
        render_approval_record(self)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct SnowRecordFrontmatter {
    sys_id: String,
    number: String,
    table: String,
    resource_type: String,
    state: String,
    short_description: String,
    description: String,
    synced_at: chrono::DateTime<chrono::Utc>,
    source: String,
    #[serde(default)]
    parent: Option<RecordRef>,
    #[serde(default)]
    children: Vec<RecordRef>,
    #[serde(default)]
    fields: HashMap<String, FieldValue>,
    #[serde(default)]
    references: HashMap<String, Reference>,
    #[serde(default)]
    work_notes: Vec<JournalEntry>,
    #[serde(default)]
    comments: Vec<JournalEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct KnowledgeArticleFrontmatter {
    sys_id: String,
    number: String,
    table: String,
    resource_type: String,
    workflow_state: String,
    #[serde(default)]
    short_description: String,
    knowledge_base: Reference,
    category: Reference,
    article_type: String,
    #[serde(default)]
    fields: HashMap<String, FieldValue>,
    #[serde(default)]
    references: HashMap<String, Reference>,
    #[serde(default)]
    published_at: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(default)]
    author: Option<Reference>,
    #[serde(default)]
    valid_to: Option<chrono::NaiveDate>,
    #[serde(default)]
    sn_tags: Vec<String>,
    #[serde(default)]
    auto_tags: Vec<String>,
    #[serde(default)]
    user_tags: Vec<String>,
    #[serde(default)]
    body_cached: bool,
    synced_at: chrono::DateTime<chrono::Utc>,
    source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ApprovalRecordFrontmatter {
    sys_id: String,
    number: String,
    table: String,
    resource_type: String,
    #[serde(default)]
    state: String,
    #[serde(default)]
    short_description: String,
    approver: Reference,
    target: RecordRef,
    requested_at: chrono::DateTime<chrono::Utc>,
    #[serde(default)]
    due_date: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(default)]
    routed_via: ApprovalRoutedVia,
    #[serde(default)]
    approver_group: Option<Reference>,
    synced_at: chrono::DateTime<chrono::Utc>,
    source: String,
}

pub fn render_snow_record(record: &SnowRecord) -> String {
    let mut out = String::new();
    out.push_str("---\n");
    push_scalar(&mut out, "sys_id", &record.sys_id);
    push_scalar(&mut out, "number", &record.number);
    push_scalar(&mut out, "table", &record.table);
    push_scalar(
        &mut out,
        "resource_type",
        resource_type_slug(&record.resource_type),
    );
    push_scalar(&mut out, "state", &record.state);
    push_scalar(&mut out, "short_description", &record.short_description);
    push_multiline(&mut out, "description", &record.description);
    push_scalar(
        &mut out,
        "synced_at",
        &record.synced_at.to_rfc3339_opts(SecondsFormat::Secs, true),
    );
    push_scalar(&mut out, "source", cache_source_slug(&record.source));

    if let Some(parent) = &record.parent {
        push_record_ref(&mut out, "parent", parent);
    }
    if !record.children.is_empty() {
        push_record_refs(&mut out, "children", &record.children);
    }
    if !record.fields.is_empty() {
        push_field_values(&mut out, "fields", &record.fields);
    }
    if !record.references.is_empty() {
        push_references(&mut out, "references", &record.references);
    }
    if !record.work_notes.is_empty() {
        push_journal_entries(&mut out, "work_notes", &record.work_notes);
    }
    if !record.comments.is_empty() {
        push_journal_entries(&mut out, "comments", &record.comments);
    }

    out.push_str("---\n\n");
    if record.resource_type == ResourceType::BusinessApplication {
        push_business_application_body(&mut out, record);
        return out;
    }
    writeln!(
        &mut out,
        "# {}: {}",
        record.number, record.short_description
    )
    .unwrap();
    out.push('\n');
    push_section(&mut out, "Description", &record.description);
    push_journal_section(&mut out, "Work Notes", &record.work_notes);
    push_journal_section(&mut out, "Comments", &record.comments);
    out
}

fn push_business_application_body(out: &mut String, record: &SnowRecord) {
    let name = record
        .fields
        .get("name")
        .map(|value| {
            value
                .display_value
                .as_deref()
                .unwrap_or(value.value.as_str())
        })
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(record.short_description.as_str());
    writeln!(out, "# {}", name).unwrap();
    out.push('\n');

    out.push_str("## Summary\n\n");
    writeln!(out, "- Sys ID: {}", record.sys_id).unwrap();
    writeln!(out, "- Table: {}", record.table).unwrap();
    writeln!(out, "- State: {}", record.state).unwrap();
    if !record.description.trim().is_empty() {
        writeln!(out, "- Description: {}", record.description.trim()).unwrap();
    }
    out.push('\n');

    out.push_str("## Ownership\n\n");
    for field in [
        "business_owner",
        "it_application_owner",
        "managed_by_group",
        "support_group",
        "portfolio",
    ] {
        if let Some(reference) = record.references.get(field) {
            writeln!(
                out,
                "- {}: {} ({})",
                field, reference.display_name, reference.sys_id
            )
            .unwrap();
        }
    }
    out.push('\n');

    out.push_str("## Operational\n\n");
    for field in ["operational_status", "attested_date", "sys_updated_on"] {
        if let Some(value) = record.fields.get(field) {
            writeln!(
                out,
                "- {}: {}",
                field,
                value
                    .display_value
                    .as_deref()
                    .unwrap_or(value.value.as_str())
            )
            .unwrap();
        }
    }
    out.push('\n');

    out.push_str("## Fields\n\n");
    let fields: BTreeMap<_, _> = record.fields.iter().collect();
    for (field, value) in fields {
        let rendered = value
            .display_value
            .as_deref()
            .unwrap_or(value.value.as_str());
        writeln!(out, "- {}: {}", field, rendered).unwrap();
    }
    out.push('\n');
}

pub fn render_knowledge_article(article: &KnowledgeArticle) -> String {
    let mut out = String::new();
    let record = &article.record;
    out.push_str("---\n");
    push_scalar(&mut out, "sys_id", &record.sys_id);
    push_scalar(&mut out, "number", &record.number);
    push_scalar(&mut out, "table", &record.table);
    push_scalar(
        &mut out,
        "resource_type",
        resource_type_slug(&record.resource_type),
    );
    let workflow_state = record
        .fields
        .get("workflow_state")
        .map(|value| {
            value
                .display_value
                .as_deref()
                .unwrap_or(value.value.as_str())
        })
        .unwrap_or_else(|| record.state.as_str());
    push_scalar(&mut out, "workflow_state", workflow_state);
    push_scalar(&mut out, "short_description", &record.short_description);
    push_reference(&mut out, "knowledge_base", &article.knowledge_base);
    push_reference(&mut out, "category", &article.category);
    push_scalar(&mut out, "article_type", &article.article_type);
    if !record.fields.is_empty() {
        push_field_values(&mut out, "fields", &record.fields);
    }
    if !record.references.is_empty() {
        push_references(&mut out, "references", &record.references);
    }
    if let Some(published_at) = article.published_at {
        push_scalar(
            &mut out,
            "published_at",
            &published_at.to_rfc3339_opts(SecondsFormat::Secs, true),
        );
    }
    if let Some(author) = &article.author {
        push_reference(&mut out, "author", author);
    }
    if let Some(valid_to) = article.valid_to {
        push_scalar(&mut out, "valid_to", &valid_to.to_string());
    }
    push_string_list(&mut out, "sn_tags", &article.sn_tags);
    push_string_list(&mut out, "auto_tags", &article.auto_tags);
    push_string_list(&mut out, "user_tags", &article.user_tags);
    push_boolean(&mut out, "body_cached", article.body_cached);
    push_scalar(
        &mut out,
        "synced_at",
        &record.synced_at.to_rfc3339_opts(SecondsFormat::Secs, true),
    );
    push_scalar(&mut out, "source", cache_source_slug(&record.source));
    out.push_str("---\n\n");

    writeln!(
        &mut out,
        "# {}: {}",
        record.number, record.short_description
    )
    .unwrap();
    out.push('\n');
    push_section(&mut out, "Summary", &record.description);
    push_section(&mut out, "Content", &article.content);
    out
}

pub fn render_approval_record(approval: &ApprovalRecord) -> String {
    let mut out = String::new();
    let record = &approval.record;
    out.push_str("---\n");
    push_scalar(&mut out, "sys_id", &record.sys_id);
    push_scalar(&mut out, "number", &record.number);
    push_scalar(&mut out, "table", &record.table);
    push_scalar(
        &mut out,
        "resource_type",
        resource_type_slug(&record.resource_type),
    );
    push_scalar(&mut out, "state", &record.state);
    push_scalar(&mut out, "short_description", &record.short_description);
    push_reference(&mut out, "approver", &approval.approver);
    push_record_ref(&mut out, "target", &approval.target);
    push_scalar(
        &mut out,
        "routed_via",
        match approval.routed_via {
            ApprovalRoutedVia::Direct => "direct",
            ApprovalRoutedVia::Group => "group",
        },
    );
    if let Some(approver_group) = &approval.approver_group {
        push_reference(&mut out, "approver_group", approver_group);
    }
    push_scalar(
        &mut out,
        "requested_at",
        &approval
            .requested_at
            .to_rfc3339_opts(SecondsFormat::Secs, true),
    );
    if let Some(due_date) = approval.due_date {
        push_scalar(
            &mut out,
            "due_date",
            &due_date.to_rfc3339_opts(SecondsFormat::Secs, true),
        );
    }
    push_scalar(
        &mut out,
        "synced_at",
        &record.synced_at.to_rfc3339_opts(SecondsFormat::Secs, true),
    );
    push_scalar(&mut out, "source", cache_source_slug(&record.source));
    out.push_str("---\n\n");

    let title = if approval.target.number.is_empty() {
        record.number.as_str()
    } else {
        approval.target.number.as_str()
    };
    writeln!(&mut out, "# Approval: {}", title).unwrap();
    out.push('\n');
    out.push_str("## Target\n\n");
    writeln!(&mut out, "- Number: {}", approval.target.number).unwrap();
    writeln!(&mut out, "- Table: {}", approval.target.table).unwrap();
    writeln!(&mut out, "- Sys ID: {}", approval.target.sys_id).unwrap();
    out.push('\n');
    out.push_str("## Approver\n\n");
    writeln!(&mut out, "- Name: {}", approval.approver.display_name).unwrap();
    writeln!(&mut out, "- Table: {}", approval.approver.table).unwrap();
    writeln!(&mut out, "- Sys ID: {}", approval.approver.sys_id).unwrap();
    out.push('\n');
    out
}

pub fn parse_snow_record(markdown: &str) -> Result<SnowRecord> {
    let normalized = normalize_markdown(markdown);
    let (frontmatter, body) = split_frontmatter(&normalized)?;
    let frontmatter: SnowRecordFrontmatter =
        serde_yaml::from_str(frontmatter).context("failed to parse SnowRecord frontmatter")?;
    let body = parse_body(body);

    Ok(SnowRecord {
        sys_id: frontmatter.sys_id,
        number: frontmatter.number,
        table: frontmatter.table,
        resource_type: resource_type_from_slug(&frontmatter.resource_type)?,
        state: frontmatter.state,
        short_description: title_short_description(&body).unwrap_or(frontmatter.short_description),
        description: section_or_default(&body, "Description", frontmatter.description),
        fields: frontmatter.fields,
        work_notes: frontmatter.work_notes,
        comments: frontmatter.comments,
        parent: frontmatter.parent,
        children: frontmatter.children,
        references: frontmatter.references,
        synced_at: frontmatter.synced_at,
        source: cache_source_from_slug(&frontmatter.source)?,
    })
}

pub fn parse_knowledge_article(markdown: &str) -> Result<KnowledgeArticle> {
    let normalized = normalize_markdown(markdown);
    let (frontmatter, body) = split_frontmatter(&normalized)?;
    let frontmatter = parse_knowledge_frontmatter(frontmatter)?;
    let body = parse_body(body);

    let short_description = title_short_description(&body).unwrap_or(frontmatter.short_description);
    let summary = section_or_default(&body, "Summary", String::new());
    let content = section_or_default(&body, "Content", String::new());
    let workflow_state = frontmatter.workflow_state.clone();
    let mut fields = frontmatter.fields;
    fields
        .entry("workflow_state".to_string())
        .or_insert_with(|| FieldValue {
            value: workflow_state.clone(),
            display_value: Some(workflow_state.clone()),
        });
    let mut references = frontmatter.references;
    references.insert(
        "knowledge_base".to_string(),
        frontmatter.knowledge_base.clone(),
    );
    references.insert("category".to_string(), frontmatter.category.clone());
    let author = frontmatter
        .author
        .clone()
        .or_else(|| references.get("author").cloned())
        .or_else(|| author_reference_from_fields(&fields));
    let published_at = frontmatter.published_at.or_else(|| {
        fields
            .get("published")
            .and_then(|value| parse_datetime(Some(value.value.as_str())))
    });
    if let Some(author) = &author {
        references.insert("author".to_string(), author.clone());
    }

    Ok(normalize_knowledge_article(KnowledgeArticle {
        record: SnowRecord {
            sys_id: frontmatter.sys_id,
            number: frontmatter.number,
            table: frontmatter.table,
            resource_type: resource_type_from_slug(&frontmatter.resource_type)?,
            state: workflow_state.clone(),
            short_description,
            description: summary,
            fields,
            work_notes: Vec::new(),
            comments: Vec::new(),
            parent: None,
            children: Vec::new(),
            references,
            synced_at: frontmatter.synced_at,
            source: cache_source_from_slug(&frontmatter.source)?,
        },
        knowledge_base: frontmatter.knowledge_base,
        category: frontmatter.category,
        article_type: frontmatter.article_type,
        content,
        sn_tags: frontmatter.sn_tags,
        auto_tags: frontmatter.auto_tags,
        user_tags: frontmatter.user_tags,
        body_cached: frontmatter.body_cached,
        published_at,
        author,
        valid_to: frontmatter.valid_to,
    }))
}

fn parse_knowledge_frontmatter(frontmatter: &str) -> Result<KnowledgeArticleFrontmatter> {
    match serde_yaml::from_str(frontmatter) {
        Ok(frontmatter) => Ok(frontmatter),
        Err(primary_err) => {
            let repaired = repair_legacy_yaml_block_scalars(frontmatter);
            serde_yaml::from_str(&repaired).map_err(|secondary_err| {
                anyhow!(
                    "failed to parse KnowledgeArticle frontmatter: {primary_err}; repair retry failed: {secondary_err}"
                )
            })
        }
    }
}

pub fn parse_approval_record(markdown: &str) -> Result<ApprovalRecord> {
    let normalized = normalize_markdown(markdown);
    let (frontmatter, _body) = split_frontmatter(&normalized)?;
    let frontmatter: ApprovalRecordFrontmatter =
        serde_yaml::from_str(frontmatter).context("failed to parse ApprovalRecord frontmatter")?;

    Ok(ApprovalRecord {
        record: SnowRecord {
            sys_id: frontmatter.sys_id,
            number: frontmatter.number,
            table: frontmatter.table,
            resource_type: resource_type_from_slug(&frontmatter.resource_type)?,
            state: frontmatter.state,
            short_description: frontmatter.short_description,
            description: String::new(),
            fields: HashMap::new(),
            work_notes: Vec::new(),
            comments: Vec::new(),
            parent: None,
            children: Vec::new(),
            references: HashMap::from([("approver".to_string(), frontmatter.approver.clone())]),
            synced_at: frontmatter.synced_at,
            source: cache_source_from_slug(&frontmatter.source)?,
        },
        approver: frontmatter.approver,
        target: frontmatter.target,
        requested_at: frontmatter.requested_at,
        due_date: frontmatter.due_date,
        routed_via: frontmatter.routed_via,
        approver_group: frontmatter.approver_group,
    })
}

fn push_section(out: &mut String, title: &str, body: &str) {
    writeln!(out, "## {}", title).unwrap();
    out.push('\n');
    if body.trim().is_empty() {
        out.push_str("_(empty)_\n\n");
    } else {
        out.push_str(body.trim_end());
        out.push_str("\n\n");
    }
}

fn push_journal_section(out: &mut String, title: &str, entries: &[JournalEntry]) {
    writeln!(out, "## {}", title).unwrap();
    out.push('\n');
    if entries.is_empty() {
        out.push_str("_(none)_\n\n");
        return;
    }
    for entry in entries {
        writeln!(
            out,
            "### {} — {}",
            entry.timestamp.to_rfc3339_opts(SecondsFormat::Secs, true),
            entry.author
        )
        .unwrap();
        if entry.body.trim().is_empty() {
            out.push_str("_(empty)_\n\n");
        } else {
            out.push_str(entry.body.trim_end());
            out.push_str("\n\n");
        }
    }
}

fn push_scalar(out: &mut String, key: &str, value: &str) {
    let _ = writeln!(out, "{key}: {}", yaml_scalar(value));
}

fn push_multiline(out: &mut String, key: &str, value: &str) {
    let _ = writeln!(out, "{key}: |");
    if value.trim().is_empty() {
        let _ = writeln!(out, "  ");
        return;
    }
    for line in value.lines() {
        let _ = writeln!(out, "  {line}");
    }
}

fn push_reference(out: &mut String, key: &str, reference: &Reference) {
    let _ = writeln!(out, "{key}:");
    let _ = writeln!(out, "  sys_id: {}", yaml_scalar(&reference.sys_id));
    let _ = writeln!(out, "  table: {}", yaml_scalar(&reference.table));
    let _ = writeln!(
        out,
        "  display_name: {}",
        yaml_scalar(&reference.display_name)
    );
    if !reference.extra.is_empty() {
        let _ = writeln!(out, "  extra:");
        let extra: BTreeMap<_, _> = reference.extra.iter().collect();
        for (extra_key, extra_value) in extra {
            let _ = writeln!(out, "    {extra_key}: {}", yaml_scalar(extra_value));
        }
    }
}

fn push_record_ref(out: &mut String, key: &str, reference: &RecordRef) {
    let _ = writeln!(out, "{key}:");
    let _ = writeln!(out, "  sys_id: {}", yaml_scalar(&reference.sys_id));
    let _ = writeln!(out, "  number: {}", yaml_scalar(&reference.number));
    let _ = writeln!(out, "  table: {}", yaml_scalar(&reference.table));
}

fn push_record_refs(out: &mut String, key: &str, refs: &[RecordRef]) {
    let _ = writeln!(out, "{key}:");
    for reference in refs {
        let _ = writeln!(out, "  - sys_id: {}", yaml_scalar(&reference.sys_id));
        let _ = writeln!(out, "    number: {}", yaml_scalar(&reference.number));
        let _ = writeln!(out, "    table: {}", yaml_scalar(&reference.table));
    }
}

fn push_references(out: &mut String, key: &str, references: &HashMap<String, Reference>) {
    let _ = writeln!(out, "{key}:");
    let sorted: BTreeMap<_, _> = references.iter().collect();
    for (field_name, reference) in sorted {
        let _ = writeln!(out, "  {field_name}:");
        let _ = writeln!(out, "    sys_id: {}", yaml_scalar(&reference.sys_id));
        let _ = writeln!(out, "    table: {}", yaml_scalar(&reference.table));
        let _ = writeln!(
            out,
            "    display_name: {}",
            yaml_scalar(&reference.display_name)
        );
        if !reference.extra.is_empty() {
            let _ = writeln!(out, "    extra:");
            let extra: BTreeMap<_, _> = reference.extra.iter().collect();
            for (extra_key, extra_value) in extra {
                let _ = writeln!(out, "      {extra_key}: {}", yaml_scalar(extra_value));
            }
        }
    }
}

fn push_field_values(out: &mut String, key: &str, fields: &HashMap<String, FieldValue>) {
    let _ = writeln!(out, "{key}:");
    let sorted: BTreeMap<_, _> = fields.iter().collect();
    for (field_name, value) in sorted {
        let _ = writeln!(out, "  {field_name}:");
        let _ = writeln!(out, "    value: {}", yaml_scalar_indented(&value.value, 6));
        if let Some(display_value) = &value.display_value {
            // display_value is at 4-space indent; block literal content must use 6-space indent.
            let _ = writeln!(
                out,
                "    display_value: {}",
                yaml_scalar_indented(display_value, 6)
            );
        }
    }
}

fn push_journal_entries(out: &mut String, key: &str, entries: &[JournalEntry]) {
    let _ = writeln!(out, "{key}:");
    for entry in entries {
        let _ = writeln!(
            out,
            "  - timestamp: {}",
            yaml_scalar(&entry.timestamp.to_rfc3339_opts(SecondsFormat::Secs, true))
        );
        let _ = writeln!(out, "    author: {}", yaml_scalar(&entry.author));
        let _ = writeln!(out, "    body: {}", yaml_scalar(&entry.body));
    }
}

fn push_string_list(out: &mut String, key: &str, values: &[String]) {
    let _ = writeln!(out, "{key}:");
    for value in values {
        let _ = writeln!(out, "  - {}", yaml_scalar(value));
    }
}

fn push_boolean(out: &mut String, key: &str, value: bool) {
    let _ = writeln!(out, "{key}: {}", if value { "true" } else { "false" });
}

fn yaml_scalar(value: &str) -> String {
    yaml_scalar_indented(value, 2)
}

fn yaml_scalar_indented(value: &str, indent: usize) -> String {
    if value.contains('\n') {
        let prefix = " ".repeat(indent);
        let mut rendered = String::from("|\n");
        for line in value.lines() {
            rendered.push_str(&prefix);
            rendered.push_str(line);
            rendered.push('\n');
        }
        if value.ends_with('\n') {
            rendered
        } else {
            rendered.trim_end_matches('\n').to_string()
        }
    } else {
        serde_json::to_string(value).unwrap_or_else(|_| format!("{value:?}"))
    }
}

fn resource_type_slug(resource_type: &ResourceType) -> &'static str {
    match resource_type {
        ResourceType::Task => "task",
        ResourceType::Incident => "incident",
        ResourceType::Change => "change",
        ResourceType::ChangeTask => "change_task",
        ResourceType::Request => "request",
        ResourceType::RequestTask => "request_task",
        ResourceType::Project => "project",
        ResourceType::Demand => "demand",
        ResourceType::DemandTask => "demand_task",
        ResourceType::ResourcePlan => "resource_plan",
        ResourceType::Story => "story",
        ResourceType::ScrumTask => "scrum_task",
        ResourceType::Timecard => "timecard",
        ResourceType::Knowledge => "knowledge",
        ResourceType::Approval => "approval",
        ResourceType::BusinessApplication => "business_application",
        ResourceType::Server => "server",
    }
}

fn resource_type_from_slug(slug: &str) -> Result<ResourceType> {
    Ok(match slug {
        "task" => ResourceType::Task,
        "incident" => ResourceType::Incident,
        "change" => ResourceType::Change,
        "change_task" => ResourceType::ChangeTask,
        "request" => ResourceType::Request,
        "request_task" => ResourceType::RequestTask,
        "project" => ResourceType::Project,
        "demand" => ResourceType::Demand,
        "demand_task" => ResourceType::DemandTask,
        "resource_plan" => ResourceType::ResourcePlan,
        "story" => ResourceType::Story,
        "scrum_task" => ResourceType::ScrumTask,
        "timecard" => ResourceType::Timecard,
        "knowledge" => ResourceType::Knowledge,
        "approval" => ResourceType::Approval,
        "business_application" | "business_app" | "cmdb_ci_business_app" => {
            ResourceType::BusinessApplication
        }
        "server" | "cmdb_ci_server" | "cmdb_ci_linux_server" | "cmdb_ci_win_server" => {
            ResourceType::Server
        }
        other => bail!("unknown resource_type slug: {other}"),
    })
}

fn cache_source_slug(source: &CacheSource) -> &'static str {
    match source {
        CacheSource::Memory => "memory",
        CacheSource::Disk => "disk",
        CacheSource::Api => "api",
    }
}

fn cache_source_from_slug(source: &str) -> Result<CacheSource> {
    Ok(match source {
        "memory" => CacheSource::Memory,
        "disk" => CacheSource::Disk,
        "api" => CacheSource::Api,
        other => bail!("unknown cache source slug: {other}"),
    })
}

fn normalize_markdown(markdown: &str) -> String {
    markdown.replace("\r\n", "\n")
}

fn repair_legacy_yaml_block_scalars(frontmatter: &str) -> String {
    let mut repaired = String::new();
    let mut block_indent: Option<usize> = None;
    let mut block_content_indent = 0usize;

    for line in frontmatter.lines() {
        let trimmed = line.trim_start();
        let leading_spaces = line.len().saturating_sub(trimmed.len());

        if let Some(indent) = block_indent
            && !trimmed.is_empty()
            && leading_spaces <= indent
            && looks_like_yaml_key(trimmed)
        {
            block_indent = None;
        }

        if block_indent.is_some() {
            if trimmed.is_empty() {
                repaired.push_str(&" ".repeat(block_content_indent));
                repaired.push('\n');
                continue;
            }

            if leading_spaces < block_content_indent {
                repaired.push_str(&" ".repeat(block_content_indent));
                repaired.push_str(trimmed);
                repaired.push('\n');
                continue;
            }
        }

        repaired.push_str(line);
        repaired.push('\n');

        if trimmed.ends_with(": |") || trimmed.ends_with(": |-") || trimmed.ends_with(": |+") {
            block_indent = Some(leading_spaces);
            block_content_indent = leading_spaces + 2;
        }
    }

    repaired
}

fn looks_like_yaml_key(line: &str) -> bool {
    if line == "---" || line == "..." {
        return true;
    }

    let candidate = line.strip_prefix("- ").unwrap_or(line);
    let Some((key, rest)) = candidate.split_once(':') else {
        return false;
    };

    if key.is_empty() || key.contains("://") {
        return false;
    }

    if !(rest.is_empty()
        || rest.starts_with(' ')
        || rest == "|"
        || rest == "|-"
        || rest == "|+"
        || rest == ">"
        || rest == ">-"
        || rest == ">+")
    {
        return false;
    }

    key.chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '"' | '\''))
}

fn split_frontmatter(markdown: &str) -> Result<(&str, &str)> {
    let rest = markdown
        .strip_prefix("---\n")
        .ok_or_else(|| anyhow!("markdown missing opening frontmatter delimiter"))?;
    let (frontmatter, body) = rest
        .split_once("\n---\n")
        .ok_or_else(|| anyhow!("markdown missing closing frontmatter delimiter"))?;
    Ok((frontmatter, body))
}

#[derive(Debug, Default)]
struct ParsedBody {
    title: Option<String>,
    sections: HashMap<String, String>,
}

fn parse_body(body: &str) -> ParsedBody {
    let mut parsed = ParsedBody::default();
    let mut current_section: Option<String> = None;
    let mut current_body = String::new();

    for line in body.lines() {
        if parsed.title.is_none() {
            if line.trim().is_empty() {
                continue;
            }
            if let Some(title) = line.strip_prefix("# ") {
                parsed.title = Some(title.trim().to_string());
                continue;
            }
        }

        if let Some(section) = line.strip_prefix("## ") {
            if let Some(previous) = current_section.replace(section.trim().to_string()) {
                parsed.sections.insert(previous, trim_block(&current_body));
                current_body.clear();
            }
            continue;
        }

        if current_section.is_some() {
            current_body.push_str(line);
            current_body.push('\n');
        }
    }

    if let Some(previous) = current_section {
        parsed.sections.insert(previous, trim_block(&current_body));
    }

    parsed
}

fn trim_block(input: &str) -> String {
    input.trim().to_string()
}

fn title_short_description(body: &ParsedBody) -> Option<String> {
    let title = body.title.as_ref()?;
    let (_, short_description) = title.split_once(": ")?;
    Some(short_description.trim().to_string())
}

fn section_or_default(body: &ParsedBody, section: &str, default: String) -> String {
    body.sections
        .get(section)
        .cloned()
        .filter(|value| !value.is_empty() && value != "_(empty)_" && value != "_(none)_")
        .unwrap_or(default)
}

fn author_reference_from_fields(fields: &HashMap<String, FieldValue>) -> Option<Reference> {
    let author = fields.get("author")?;
    let sys_id = author.value.trim();
    if sys_id.is_empty() {
        return None;
    }

    Some(Reference {
        sys_id: sys_id.to_string(),
        table: "sys_user".to_string(),
        display_name: choose_reference_display_name([author.display_value.as_deref()]),
        extra: HashMap::new(),
    })
}

fn parse_datetime(value: Option<&str>) -> Option<chrono::DateTime<chrono::Utc>> {
    let value = value?.trim();
    if value.is_empty() {
        return None;
    }

    chrono::DateTime::parse_from_rfc3339(value)
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .ok()
        .or_else(|| {
            chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S")
                .ok()
                .map(|dt| {
                    chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(dt, chrono::Utc)
                })
        })
}

#[cfg(test)]
fn knowledge_workflow_field(workflow_state: &str) -> HashMap<String, FieldValue> {
    HashMap::from([(
        "workflow_state".to_string(),
        FieldValue {
            value: workflow_state.to_string(),
            display_value: Some(workflow_state.to_string()),
        },
    )])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CacheSource, FieldValue, RecordRef, ResourceType, SnowRecord};
    use chrono::{TimeZone, Utc};

    fn sample_record() -> SnowRecord {
        let mut fields = HashMap::new();
        fields.insert(
            "assigned_to".to_string(),
            FieldValue {
                value: "sys-1".to_string(),
                display_value: Some("Jared".to_string()),
            },
        );
        let mut references = HashMap::new();
        references.insert(
            "assignment_group".to_string(),
            Reference {
                sys_id: "group-1".to_string(),
                table: "sys_user_group".to_string(),
                display_name: "Network Operations".to_string(),
                extra: HashMap::from([("email".to_string(), "netops@example.com".to_string())]),
            },
        );
        SnowRecord {
            sys_id: "sys-123".to_string(),
            number: "INC0012345".to_string(),
            table: "incident".to_string(),
            resource_type: ResourceType::Incident,
            state: "In Progress".to_string(),
            short_description: "VPN connectivity drops".to_string(),
            description: "Users are seeing disconnects.".to_string(),
            fields,
            work_notes: vec![JournalEntry {
                timestamp: Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
                author: "Jared".to_string(),
                body: "Investigating.".to_string(),
            }],
            comments: vec![JournalEntry {
                timestamp: Utc.timestamp_opt(1_712_649_900, 0).unwrap(),
                author: "Jane".to_string(),
                body: "Please update impacted users.".to_string(),
            }],
            parent: Some(RecordRef {
                sys_id: "parent-sys".to_string(),
                number: "CHG0001234".to_string(),
                table: "change_request".to_string(),
            }),
            children: vec![RecordRef {
                sys_id: "child-sys".to_string(),
                number: "TASK0001".to_string(),
                table: "task".to_string(),
            }],
            references,
            synced_at: Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
            source: CacheSource::Api,
        }
    }

    fn sample_knowledge_article() -> KnowledgeArticle {
        let mut fields = knowledge_workflow_field("published");
        fields.insert(
            "u_supported_product".to_string(),
            FieldValue {
                value: "vpn".to_string(),
                display_value: Some("VPN".to_string()),
            },
        );
        fields.insert(
            "published".to_string(),
            FieldValue {
                value: "2026-04-10 09:00:00".to_string(),
                display_value: Some("2026-04-10 09:00:00".to_string()),
            },
        );
        fields.insert(
            "author".to_string(),
            FieldValue {
                value: "user-1".to_string(),
                display_value: Some("Jared Jennings".to_string()),
            },
        );
        let mut references = HashMap::from([
            (
                "knowledge_base".to_string(),
                Reference {
                    sys_id: "kb-base".to_string(),
                    table: "kb_knowledge_base".to_string(),
                    display_name: "IT Operations".to_string(),
                    extra: HashMap::new(),
                },
            ),
            (
                "category".to_string(),
                Reference {
                    sys_id: "kb-cat".to_string(),
                    table: "kb_category".to_string(),
                    display_name: "Networking".to_string(),
                    extra: HashMap::new(),
                },
            ),
        ]);
        references.insert(
            "author".to_string(),
            Reference {
                sys_id: "user-1".to_string(),
                table: "sys_user".to_string(),
                display_name: "Jared Jennings".to_string(),
                extra: HashMap::new(),
            },
        );
        KnowledgeArticle {
            record: SnowRecord {
                sys_id: "kb-sys".to_string(),
                number: "KB0012345".to_string(),
                table: "kb_knowledge".to_string(),
                resource_type: ResourceType::Knowledge,
                state: "published".to_string(),
                short_description: "VPN Troubleshooting Runbook".to_string(),
                description: "Standard operating procedure for VPN issues.".to_string(),
                fields,
                work_notes: Vec::new(),
                comments: Vec::new(),
                parent: None,
                children: Vec::new(),
                references,
                synced_at: Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
                source: CacheSource::Disk,
            },
            knowledge_base: Reference {
                sys_id: "kb-base".to_string(),
                table: "kb_knowledge_base".to_string(),
                display_name: "IT Operations".to_string(),
                extra: HashMap::new(),
            },
            category: Reference {
                sys_id: "kb-cat".to_string(),
                table: "kb_category".to_string(),
                display_name: "Networking".to_string(),
                extra: HashMap::new(),
            },
            article_type: "text".to_string(),
            content: "1. Verify gateway.\n2. Check client version.".to_string(),
            sn_tags: vec!["vpn".to_string()],
            auto_tags: vec!["gateway".to_string()],
            user_tags: vec!["runbook".to_string()],
            body_cached: true,
            published_at: Some(Utc.timestamp_opt(1_712_649_600, 0).unwrap()),
            author: Some(Reference {
                sys_id: "user-1".to_string(),
                table: "sys_user".to_string(),
                display_name: "Jared Jennings".to_string(),
                extra: HashMap::new(),
            }),
            valid_to: Some(chrono::NaiveDate::from_ymd_opt(2027, 1, 1).unwrap()),
        }
    }

    fn sample_approval_record() -> ApprovalRecord {
        ApprovalRecord {
            record: SnowRecord {
                sys_id: "apr-sys".to_string(),
                number: "APR0001234".to_string(),
                table: "sysapproval_approver".to_string(),
                resource_type: ResourceType::Approval,
                state: "requested".to_string(),
                short_description: "CAB approval for CHG0001234".to_string(),
                description: String::new(),
                fields: HashMap::new(),
                work_notes: Vec::new(),
                comments: Vec::new(),
                parent: None,
                children: Vec::new(),
                references: HashMap::from([(
                    "approver".to_string(),
                    Reference {
                        sys_id: "user-1".to_string(),
                        table: "sys_user".to_string(),
                        display_name: "Jared Jennings".to_string(),
                        extra: HashMap::new(),
                    },
                )]),
                synced_at: Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
                source: CacheSource::Api,
            },
            approver: Reference {
                sys_id: "user-1".to_string(),
                table: "sys_user".to_string(),
                display_name: "Jared Jennings".to_string(),
                extra: HashMap::new(),
            },
            target: RecordRef {
                sys_id: "chg-sys".to_string(),
                number: "CHG0001234".to_string(),
                table: "change_request".to_string(),
            },
            requested_at: Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
            due_date: Some(Utc.timestamp_opt(1_712_736_000, 0).unwrap()),
            routed_via: ApprovalRoutedVia::Direct,
            approver_group: None,
        }
    }

    #[test]
    fn renders_record_markdown() {
        let markdown = render_snow_record(&sample_record());
        assert!(markdown.contains("sys_id:"));
        assert!(markdown.contains("## Description"));
        assert!(markdown.contains("## Work Notes"));
        assert!(markdown.contains("VPN connectivity drops"));
    }

    #[test]
    fn round_trips_snow_record_markdown() {
        let record = sample_record();
        let markdown = render_snow_record(&record);
        let parsed = parse_snow_record(&markdown).expect("parse snow record");
        assert_eq!(parsed, record);
    }

    #[test]
    fn round_trips_knowledge_article_markdown() {
        let article = sample_knowledge_article();
        let markdown = render_knowledge_article(&article);
        let parsed = parse_knowledge_article(&markdown).expect("parse knowledge article");
        assert_eq!(parsed, article);
    }

    #[test]
    fn parses_legacy_knowledge_markdown_without_tag_frontmatter() {
        let markdown = r#"---
sys_id: "kb-sys"
number: "KB0012345"
table: "kb_knowledge"
resource_type: "knowledge"
workflow_state: "published"
short_description: "VPN Troubleshooting Runbook"
knowledge_base:
  sys_id: "kb-base"
  table: "kb_knowledge_base"
  display_name: "IT Operations"
category:
  sys_id: "kb-cat"
  table: "kb_category"
  display_name: "Networking"
article_type: "text"
published_at: "2024-04-10T12:00:00Z"
valid_to: 2027-01-01
synced_at: "2024-04-10T12:00:00Z"
source: "disk"
---

# KB0012345: VPN Troubleshooting Runbook

## Summary

Standard operating procedure for VPN issues.

## Content

1. Verify gateway.
2. Check client version.
"#;

        let parsed = parse_knowledge_article(markdown).expect("parse legacy knowledge article");
        assert!(parsed.sn_tags.is_empty());
        assert!(parsed.auto_tags.is_empty());
        assert!(parsed.user_tags.is_empty());
        assert!(parsed.body_cached);
        assert_eq!(
            parsed.content,
            "1. Verify gateway.\n2. Check client version."
        );
    }

    #[test]
    fn parses_legacy_knowledge_markdown_with_malformed_block_scalar_indentation() {
        let markdown = r#"---
sys_id: "kb-sys"
number: "KB0012345"
table: "kb_knowledge"
resource_type: "knowledge"
workflow_state: "published"
short_description: "Submitting Access Requests"
knowledge_base:
  sys_id: ""
  table: "kb_knowledge_base"
  display_name: ""
category:
  sys_id: ""
  table: "kb_category"
  display_name: ""
article_type: "text"
fields:
  meta:
    value: |
  access_request,active_directory
  text:
    value: |
  <p>Body line 1</p>
  <p>Body line 2</p>
synced_at: "2026-04-10T23:47:36Z"
source: "api"
---

# KB0012345: Submitting Access Requests

## Summary

Access requests require specific workflows.

## Content

<p>Body line 1</p>
<p>Body line 2</p>
"#;

        let parsed =
            parse_knowledge_article(markdown).expect("parse malformed legacy knowledge article");
        assert_eq!(
            parsed
                .record
                .fields
                .get("meta")
                .expect("meta field")
                .value
                .trim_end(),
            "access_request,active_directory"
        );
        assert_eq!(
            parsed
                .record
                .fields
                .get("text")
                .expect("text field")
                .value
                .trim_end(),
            "<p>Body line 1</p>\n<p>Body line 2</p>"
        );
        assert_eq!(parsed.content, "<p>Body line 1</p>\n<p>Body line 2</p>");
    }

    #[test]
    fn renders_approval_markdown() {
        let approval = sample_approval_record();
        let markdown = render_approval_record(&approval);
        assert!(markdown.contains("# Approval: CHG0001234"));
        assert!(markdown.contains("## Target"));
        assert!(markdown.contains("## Approver"));
    }

    #[test]
    fn round_trips_approval_markdown() {
        let approval = sample_approval_record();
        let markdown = render_approval_record(&approval);
        let parsed = parse_approval_record(&markdown).expect("parse approval");
        assert_eq!(parsed, approval);
    }
}
