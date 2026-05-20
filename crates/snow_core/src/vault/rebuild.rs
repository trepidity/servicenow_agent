use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::vault::markdown::{parse_approval_record, parse_knowledge_article, parse_snow_record};
use crate::{ApprovalRecord, KnowledgeArticle, SnowRecord};

#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum VaultDocument {
    Record(SnowRecord),
    Knowledge(KnowledgeArticle),
    Approval(ApprovalRecord),
}

impl VaultDocument {
    pub fn record(&self) -> &SnowRecord {
        match self {
            Self::Record(record) => record,
            Self::Knowledge(article) => &article.record,
            Self::Approval(approval) => &approval.record,
        }
    }
}

#[derive(Debug, Clone)]
pub struct VaultDocumentEntry {
    pub absolute_path: PathBuf,
    pub relative_path: PathBuf,
    pub document: VaultDocument,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VaultScanFailure {
    pub absolute_path: PathBuf,
    pub relative_path: PathBuf,
    pub error: String,
}

#[derive(Debug, Clone)]
pub struct VaultScanReport {
    pub entries: Vec<VaultDocumentEntry>,
    pub failures: Vec<VaultScanFailure>,
}

impl VaultDocumentEntry {
    pub fn record(&self) -> &SnowRecord {
        self.document.record()
    }
}

pub fn scan_documents(root: impl AsRef<Path>) -> Result<Vec<VaultDocumentEntry>> {
    let report = scan_documents_detailed(root)?;
    if let Some(failure) = report.failures.first() {
        bail!(
            "failed to parse vault markdown {}: {}",
            failure.absolute_path.display(),
            failure.error
        );
    }
    Ok(report.entries)
}

pub fn scan_documents_detailed(root: impl AsRef<Path>) -> Result<VaultScanReport> {
    let root = root.as_ref();
    let mut entries = Vec::new();
    let mut failures = Vec::new();
    collect_markdown_files(root, root, &mut entries, &mut failures)?;
    entries.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    failures.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    Ok(VaultScanReport { entries, failures })
}

fn collect_markdown_files(
    root: &Path,
    current: &Path,
    documents: &mut Vec<VaultDocumentEntry>,
    failures: &mut Vec<VaultScanFailure>,
) -> Result<()> {
    if !current.exists() {
        return Ok(());
    }

    for entry in fs::read_dir(current)
        .with_context(|| format!("failed to read vault directory {}", current.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_markdown_files(root, &path, documents, failures)?;
            continue;
        }
        if path.extension().and_then(|ext| ext.to_str()) != Some("md") {
            continue;
        }
        if path.file_name().and_then(|name| name.to_str()) == Some("INDEX.md") {
            continue;
        }

        let relative_path = path
            .strip_prefix(root)
            .map(Path::to_path_buf)
            .with_context(|| {
                format!(
                    "vault markdown path {} is not inside root {}",
                    path.display(),
                    root.display()
                )
            })?;
        match fs::read_to_string(&path)
            .with_context(|| format!("failed to read vault markdown {}", path.display()))
            .and_then(|markdown| parse_document(&markdown))
        {
            Ok(document) => documents.push(VaultDocumentEntry {
                absolute_path: path,
                relative_path,
                document,
            }),
            Err(err) => failures.push(VaultScanFailure {
                absolute_path: path,
                relative_path,
                error: err.to_string(),
            }),
        }
    }

    Ok(())
}

fn parse_document(markdown: &str) -> Result<VaultDocument> {
    if let Ok(article) = parse_knowledge_article(markdown) {
        return Ok(VaultDocument::Knowledge(article));
    }
    if let Ok(approval) = parse_approval_record(markdown) {
        return Ok(VaultDocument::Approval(approval));
    }
    if let Ok(record) = parse_snow_record(markdown) {
        return Ok(VaultDocument::Record(record));
    }
    bail!("unsupported markdown document")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vault::VaultManager;
    use crate::{CacheSource, FieldValue, Reference, ResourceType};
    use chrono::{TimeZone, Utc};
    use std::collections::HashMap;
    use tempfile::tempdir;

    fn sample_record() -> SnowRecord {
        SnowRecord {
            sys_id: "sys-1".to_string(),
            number: "INC001".to_string(),
            table: "incident".to_string(),
            resource_type: ResourceType::Incident,
            state: "Open".to_string(),
            short_description: "Vault doc".to_string(),
            description: "Body".to_string(),
            fields: HashMap::from([(
                "assigned_to".to_string(),
                FieldValue {
                    value: "user-1".to_string(),
                    display_value: Some("User 1".to_string()),
                },
            )]),
            work_notes: Vec::new(),
            comments: Vec::new(),
            parent: None,
            children: Vec::new(),
            references: HashMap::new(),
            synced_at: Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
            source: CacheSource::Disk,
        }
    }

    fn sample_article() -> KnowledgeArticle {
        KnowledgeArticle {
            record: SnowRecord {
                sys_id: "kb-sys".to_string(),
                number: "KB001".to_string(),
                table: "kb_knowledge".to_string(),
                resource_type: ResourceType::Knowledge,
                state: "published".to_string(),
                short_description: "KB".to_string(),
                description: "Summary".to_string(),
                fields: HashMap::new(),
                work_notes: Vec::new(),
                comments: Vec::new(),
                parent: None,
                children: Vec::new(),
                references: HashMap::new(),
                synced_at: Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
                source: CacheSource::Disk,
            },
            knowledge_base: Reference {
                sys_id: "kb-base".to_string(),
                table: "kb_knowledge_base".to_string(),
                display_name: "IT".to_string(),
                extra: HashMap::new(),
            },
            category: Reference {
                sys_id: "kb-cat".to_string(),
                table: "kb_category".to_string(),
                display_name: "Net".to_string(),
                extra: HashMap::new(),
            },
            article_type: "text".to_string(),
            content: "KB content".to_string(),
            sn_tags: vec!["vpn".to_string()],
            auto_tags: vec!["gateway".to_string()],
            user_tags: vec!["runbook".to_string()],
            body_cached: true,
            published_at: Some(Utc.timestamp_opt(1_712_649_600, 0).unwrap()),
            author: None,
            valid_to: None,
        }
    }

    #[test]
    fn scans_typed_vault_documents() {
        let tempdir = tempdir().expect("tempdir");
        let vault = VaultManager::new(tempdir.path());
        vault.write_record(&sample_record()).expect("record");
        vault
            .write_knowledge_article(&sample_article())
            .expect("article");

        let entries = scan_documents(tempdir.path()).expect("scan");
        assert_eq!(entries.len(), 2);
        assert!(
            entries
                .iter()
                .any(|entry| matches!(entry.document, VaultDocument::Record(_)))
        );
        assert!(
            entries
                .iter()
                .any(|entry| matches!(entry.document, VaultDocument::Knowledge(_)))
        );
    }
}
