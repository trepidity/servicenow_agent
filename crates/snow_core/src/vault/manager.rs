use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use uuid::Uuid;

use crate::{ApprovalRecord, KnowledgeArticle, SnowRecord};

use super::layout::VaultLayout;
use super::markdown::{
    parse_approval_record, parse_knowledge_article, parse_snow_record, render_approval_record,
    render_knowledge_article, render_snow_record,
};

#[derive(Debug, Clone)]
pub struct VaultManager {
    layout: VaultLayout,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedDocument {
    pub path: PathBuf,
    pub relative_path: PathBuf,
}

impl VaultManager {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            layout: VaultLayout::new(root),
        }
    }

    pub fn layout(&self) -> &VaultLayout {
        &self.layout
    }

    pub fn persist_record(&self, record: &SnowRecord) -> Result<PersistedDocument> {
        self.persist_rendered(self.layout.record_path(record), &render_snow_record(record))
    }

    pub fn write_record(&self, record: &SnowRecord) -> Result<PathBuf> {
        self.persist_record(record).map(|document| document.path)
    }

    pub fn read_record(&self, path: impl AsRef<Path>) -> Result<SnowRecord> {
        let markdown = fs::read_to_string(path.as_ref())
            .with_context(|| format!("failed to read markdown file {}", path.as_ref().display()))?;
        parse_snow_record(&markdown)
    }

    pub fn write_knowledge_article(&self, article: &KnowledgeArticle) -> Result<PathBuf> {
        self.persist_knowledge_article(article)
            .map(|document| document.path)
    }

    pub fn persist_knowledge_article(
        &self,
        article: &KnowledgeArticle,
    ) -> Result<PersistedDocument> {
        self.persist_rendered(
            self.layout.record_path(&article.record),
            &render_knowledge_article(article),
        )
    }

    pub fn read_knowledge_article(&self, path: impl AsRef<Path>) -> Result<KnowledgeArticle> {
        let markdown = fs::read_to_string(path.as_ref())
            .with_context(|| format!("failed to read markdown file {}", path.as_ref().display()))?;
        parse_knowledge_article(&markdown)
    }

    pub fn write_approval(&self, approval: &ApprovalRecord) -> Result<PathBuf> {
        self.persist_approval(approval)
            .map(|document| document.path)
    }

    pub fn persist_approval(&self, approval: &ApprovalRecord) -> Result<PersistedDocument> {
        self.persist_rendered(
            self.layout.approval_path(approval),
            &render_approval_record(approval),
        )
    }

    pub fn read_approval(&self, path: impl AsRef<Path>) -> Result<ApprovalRecord> {
        let markdown = fs::read_to_string(path.as_ref())
            .with_context(|| format!("failed to read markdown file {}", path.as_ref().display()))?;
        parse_approval_record(&markdown)
    }

    pub fn delete(&self, path: impl AsRef<Path>) -> Result<()> {
        match fs::remove_file(path.as_ref()) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err).with_context(|| {
                format!("failed to delete markdown file {}", path.as_ref().display())
            }),
        }
    }

    pub fn relative_path(&self, path: impl AsRef<Path>) -> Result<PathBuf> {
        path.as_ref()
            .strip_prefix(self.layout.root())
            .map(Path::to_path_buf)
            .with_context(|| {
                format!(
                    "path {} is not inside vault root {}",
                    path.as_ref().display(),
                    self.layout.root().display()
                )
            })
    }

    pub fn write_markdown_file(
        &self,
        path: impl AsRef<Path>,
        contents: &str,
    ) -> Result<PersistedDocument> {
        self.persist_rendered(path.as_ref().to_path_buf(), contents)
    }

    fn persist_rendered(&self, path: PathBuf, contents: &str) -> Result<PersistedDocument> {
        self.write_markdown(&path, contents)?;
        let relative_path = self.relative_path(&path)?;
        Ok(PersistedDocument {
            path,
            relative_path,
        })
    }

    fn write_markdown(&self, path: &Path, contents: &str) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!(
                    "failed to create markdown parent directory {}",
                    parent.display()
                )
            })?;
        }

        let tmp_name = format!(
            ".{}.{}.tmp",
            path.file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("vault"),
            Uuid::new_v4()
        );
        let tmp_path = path.with_file_name(tmp_name);
        fs::write(&tmp_path, contents).with_context(|| {
            format!("failed to write temp markdown file {}", tmp_path.display())
        })?;
        fs::rename(&tmp_path, path).with_context(|| {
            format!(
                "failed to atomically rename markdown file {} -> {}",
                tmp_path.display(),
                path.display()
            )
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        CacheSource, FieldValue, JournalEntry, RecordRef, Reference, ResourceType, SnowRecord,
    };
    use chrono::{TimeZone, Utc};
    use std::collections::HashMap;
    use tempfile::tempdir;

    fn sample_record() -> SnowRecord {
        SnowRecord {
            sys_id: "sys-123".to_string(),
            number: "INC0012345".to_string(),
            table: "incident".to_string(),
            resource_type: ResourceType::Incident,
            state: "In Progress".to_string(),
            short_description: "VPN connectivity drops".to_string(),
            description: "Users are seeing disconnects.".to_string(),
            fields: HashMap::from([(
                "assigned_to".to_string(),
                FieldValue {
                    value: "sys-1".to_string(),
                    display_value: Some("Jared".to_string()),
                },
            )]),
            work_notes: vec![JournalEntry {
                timestamp: Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
                author: "Jared".to_string(),
                body: "Investigating.".to_string(),
            }],
            comments: Vec::new(),
            parent: None,
            children: Vec::new(),
            references: HashMap::new(),
            synced_at: Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
            source: CacheSource::Api,
        }
    }

    fn sample_knowledge_article() -> KnowledgeArticle {
        KnowledgeArticle {
            record: SnowRecord {
                sys_id: "kb-sys".to_string(),
                number: "KB0012345".to_string(),
                table: "kb_knowledge".to_string(),
                resource_type: ResourceType::Knowledge,
                state: "published".to_string(),
                short_description: "VPN Troubleshooting Runbook".to_string(),
                description: "Standard operating procedure for VPN issues.".to_string(),
                fields: HashMap::from([(
                    "workflow_state".to_string(),
                    FieldValue {
                        value: "published".to_string(),
                        display_value: Some("published".to_string()),
                    },
                )]),
                work_notes: Vec::new(),
                comments: Vec::new(),
                parent: None,
                children: Vec::new(),
                references: HashMap::from([
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
                ]),
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
            author: None,
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
        }
    }

    #[test]
    fn writes_and_reads_record_markdown() {
        let tempdir = tempdir().expect("tempdir");
        let vault = VaultManager::new(tempdir.path());
        let record = sample_record();

        let path = vault.write_record(&record).expect("write record");
        assert!(path.exists());

        let loaded = vault.read_record(&path).expect("read record");
        assert_eq!(loaded, record);
        assert_eq!(
            vault.relative_path(&path).expect("relative path"),
            PathBuf::from("incidents/INC0012345.md")
        );
    }

    #[test]
    fn typed_record_persistence_returns_relative_path() {
        let tempdir = tempdir().expect("tempdir");
        let vault = VaultManager::new(tempdir.path());
        let record = sample_record();

        let persisted = vault.persist_record(&record).expect("persist record");
        assert_eq!(
            persisted.relative_path,
            PathBuf::from("incidents/INC0012345.md")
        );
        assert_eq!(
            persisted.path,
            tempdir.path().join("incidents/INC0012345.md")
        );
    }

    #[test]
    fn writes_and_reads_knowledge_markdown() {
        let tempdir = tempdir().expect("tempdir");
        let vault = VaultManager::new(tempdir.path());
        let article = sample_knowledge_article();

        let path = vault
            .write_knowledge_article(&article)
            .expect("write knowledge article");
        assert!(path.exists());

        let loaded = vault
            .read_knowledge_article(&path)
            .expect("read knowledge article");
        assert_eq!(loaded, article);
    }

    #[test]
    fn writes_and_reads_approval_markdown() {
        let tempdir = tempdir().expect("tempdir");
        let vault = VaultManager::new(tempdir.path());
        let approval = sample_approval_record();

        let path = vault.write_approval(&approval).expect("write approval");
        assert!(path.exists());

        let loaded = vault.read_approval(&path).expect("read approval");
        assert_eq!(loaded, approval);

        vault.delete(&path).expect("delete markdown");
        assert!(!path.exists());
    }
}
