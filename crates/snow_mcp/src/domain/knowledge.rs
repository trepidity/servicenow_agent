use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::primitives::{Citation, RecordRef};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct KnowledgeArticleRef {
    pub number: String,
    pub sys_id: String,
    pub knowledge_base: String,
    pub title: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProcedureDraft {
    pub vault_relative_path: String,
    pub source_article: KnowledgeArticleRef,
    pub steps: Vec<ProcedureStep>,
    pub raw_evidence: String,
    pub citations: Vec<Citation>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProcedureStep {
    pub ordinal: u32,
    pub text: String,
    pub references: Vec<RecordRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum EvidenceVerdict {
    Sufficient {
        citations: Vec<Citation>,
    },
    Stale {
        article_numbers: Vec<String>,
        age_days_max: u32,
    },
    Insufficient {
        reason: String,
    },
    Conflicting {
        reason: String,
        citations: Vec<Citation>,
    },
    InjectionDetected {
        article_numbers: Vec<String>,
        pattern: String,
    },
}
