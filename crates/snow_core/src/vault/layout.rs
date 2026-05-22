use std::path::{Path, PathBuf};

use crate::{ApprovalRecord, RecordRef, ResourceType, SnowRecord};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VaultLayout {
    root: PathBuf,
}

impl VaultLayout {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn incidents_dir(&self) -> PathBuf {
        self.root.join("incidents")
    }

    pub fn changes_dir(&self) -> PathBuf {
        self.root.join("changes")
    }

    pub fn requests_dir(&self) -> PathBuf {
        self.root.join("requests")
    }

    pub fn projects_dir(&self) -> PathBuf {
        self.root.join("projects")
    }

    pub fn demands_dir(&self) -> PathBuf {
        self.root.join("demands")
    }

    pub fn stories_dir(&self) -> PathBuf {
        self.root.join("stories")
    }

    pub fn knowledge_dir(&self) -> PathBuf {
        self.root.join("knowledge").join("bases")
    }

    pub fn approvals_dir(&self) -> PathBuf {
        self.root.join("approvals")
    }

    pub fn users_index_path(&self) -> PathBuf {
        self.root.join("users").join("_index.json")
    }

    pub fn record_path(&self, record: &SnowRecord) -> PathBuf {
        match record.resource_type {
            ResourceType::Task => self
                .root
                .join("tasks")
                .join(file_name(&record.number, &record.sys_id)),
            ResourceType::Incident => self
                .incidents_dir()
                .join(file_name(&record.number, &record.sys_id)),
            ResourceType::Change => {
                hierarchical_parent_record_path(self.changes_dir(), &record.number, &record.sys_id)
            }
            ResourceType::ChangeTask => hierarchical_child_record_path(
                self.changes_dir(),
                &record.number,
                record.parent.as_ref(),
                &record.sys_id,
            ),
            ResourceType::Request => {
                hierarchical_parent_record_path(self.requests_dir(), &record.number, &record.sys_id)
            }
            ResourceType::RequestTask => hierarchical_child_record_path(
                self.requests_dir(),
                &record.number,
                record.parent.as_ref(),
                &record.sys_id,
            ),
            ResourceType::Project => {
                hierarchical_parent_record_path(self.projects_dir(), &record.number, &record.sys_id)
            }
            ResourceType::Demand => {
                hierarchical_parent_record_path(self.demands_dir(), &record.number, &record.sys_id)
            }
            ResourceType::ResourcePlan => self
                .root
                .join("resource_plans")
                .join(file_name(&record.number, &record.sys_id)),
            ResourceType::Story => {
                hierarchical_parent_record_path(self.stories_dir(), &record.number, &record.sys_id)
            }
            ResourceType::ScrumTask => hierarchical_child_record_path(
                self.stories_dir(),
                &record.number,
                record.parent.as_ref(),
                &record.sys_id,
            ),
            ResourceType::Timecard => self
                .root
                .join("timecards")
                .join(file_name(&record.number, &record.sys_id)),
            ResourceType::Knowledge => self
                .knowledge_article_path_from_record(record)
                .unwrap_or_else(|| {
                    self.root
                        .join("knowledge")
                        .join(file_name(&record.number, &record.sys_id))
                }),
            ResourceType::Approval => self
                .approvals_dir()
                .join(file_name(&record.number, &record.sys_id)),
        }
    }

    pub fn knowledge_base_dir(&self, sys_id: &str, display_name: &str) -> PathBuf {
        self.knowledge_dir()
            .join(stable_slug("kb", sys_id, display_name))
    }

    pub fn knowledge_category_dir(&self, sys_id: &str, display_name: &str) -> PathBuf {
        PathBuf::from(stable_slug("cat", sys_id, display_name))
    }

    pub fn knowledge_article_path(
        &self,
        knowledge_base: &crate::Reference,
        category: &crate::Reference,
        article_number: &str,
        article_sys_id: &str,
    ) -> PathBuf {
        self.knowledge_base_dir(&knowledge_base.sys_id, &knowledge_base.display_name)
            .join(self.knowledge_category_dir(&category.sys_id, &category.display_name))
            .join(file_name(article_number, article_sys_id))
    }

    pub fn knowledge_article_path_from_record(&self, record: &SnowRecord) -> Option<PathBuf> {
        let knowledge_base = record.references.get("knowledge_base")?;
        let category = record.references.get("category")?;
        Some(self.knowledge_article_path(knowledge_base, category, &record.number, &record.sys_id))
    }

    pub fn approval_path(&self, approval: &ApprovalRecord) -> PathBuf {
        let file_name = if !approval.record.number.is_empty() {
            file_name(&approval.record.number, &approval.record.sys_id)
        } else {
            file_name(&approval.record.sys_id, &approval.record.sys_id)
        };
        self.approvals_dir().join(file_name)
    }
}

fn hierarchical_parent_record_path(root: PathBuf, number: &str, sys_id: &str) -> PathBuf {
    root.join(number).join(file_name(number, sys_id))
}

fn hierarchical_child_record_path(
    root: PathBuf,
    number: &str,
    parent: Option<&RecordRef>,
    sys_id: &str,
) -> PathBuf {
    match parent {
        Some(parent) if !parent.number.is_empty() => {
            root.join(&parent.number).join(file_name(number, sys_id))
        }
        _ => root.join(file_name(number, sys_id)),
    }
}

fn file_name(primary: &str, fallback: &str) -> String {
    let base = if primary.is_empty() {
        fallback
    } else {
        primary
    };
    format!("{base}.md")
}

fn stable_slug(prefix: &str, sys_id: &str, display_name: &str) -> String {
    let slug = slugify(display_name);
    if slug.is_empty() {
        format!("{prefix}_{sys_id}")
    } else {
        format!("{prefix}_{sys_id}_{slug}")
    }
}

pub fn slugify(value: &str) -> String {
    let mut slug = String::with_capacity(value.len());
    let mut prev_dash = false;

    for ch in value.chars() {
        let lower = ch.to_ascii_lowercase();
        if lower.is_ascii_alphanumeric() {
            slug.push(lower);
            prev_dash = false;
        } else if !prev_dash {
            slug.push('-');
            prev_dash = true;
        }
    }

    slug.trim_matches('-').to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CacheSource, Reference};
    use chrono::{TimeZone, Utc};
    use std::collections::HashMap;

    fn sample_record(resource_type: ResourceType, number: &str) -> SnowRecord {
        SnowRecord {
            sys_id: format!("sys-{number}"),
            number: number.to_string(),
            table: "test".to_string(),
            resource_type,
            state: "Open".to_string(),
            short_description: "Test".to_string(),
            description: String::new(),
            fields: HashMap::new(),
            work_notes: Vec::new(),
            comments: Vec::new(),
            parent: None,
            children: Vec::new(),
            references: HashMap::new(),
            synced_at: Utc.timestamp_opt(0, 0).unwrap(),
            source: CacheSource::Api,
        }
    }

    #[test]
    fn slugifies_display_names() {
        assert_eq!(slugify("IT Operations"), "it-operations");
        assert_eq!(slugify("Networking / Core"), "networking-core");
        assert_eq!(slugify("  "), "");
    }

    #[test]
    fn builds_hierarchical_paths() {
        let layout = VaultLayout::new("/tmp/vault");
        let parent = sample_record(ResourceType::Change, "CHG0001234");
        assert_eq!(
            layout.record_path(&parent),
            PathBuf::from("/tmp/vault/changes/CHG0001234/CHG0001234.md")
        );

        let mut record = sample_record(ResourceType::ChangeTask, "CTASK0005678");
        record.parent = Some(RecordRef {
            sys_id: "parent-sys".to_string(),
            number: "CHG0001234".to_string(),
            table: "change_request".to_string(),
        });

        assert_eq!(
            layout.record_path(&record),
            PathBuf::from("/tmp/vault/changes/CHG0001234/CTASK0005678.md")
        );
    }

    #[test]
    fn builds_knowledge_paths_with_stable_slugs() {
        let layout = VaultLayout::new("/tmp/vault");
        let kb = Reference {
            sys_id: "aabb1122ccdd".to_string(),
            table: "kb_knowledge_base".to_string(),
            display_name: "IT Operations".to_string(),
            extra: HashMap::new(),
        };
        let cat = Reference {
            sys_id: "eeff3344aabb".to_string(),
            table: "kb_category".to_string(),
            display_name: "Networking".to_string(),
            extra: HashMap::new(),
        };

        assert_eq!(
            layout.knowledge_article_path(&kb, &cat, "KB0012345", "article-sys"),
            PathBuf::from(
                "/tmp/vault/knowledge/bases/kb_aabb1122ccdd_it-operations/cat_eeff3344aabb_networking/KB0012345.md"
            )
        );

        let mut record = sample_record(ResourceType::Knowledge, "KB0012345");
        record.references.insert("knowledge_base".to_string(), kb);
        record.references.insert("category".to_string(), cat);
        assert_eq!(
            layout.record_path(&record),
            PathBuf::from(
                "/tmp/vault/knowledge/bases/kb_aabb1122ccdd_it-operations/cat_eeff3344aabb_networking/KB0012345.md"
            )
        );
    }
}
