mod rules;
pub mod vtb_context;

use crate::SnowRecord;

pub use rules::{
    derive_aliases, derive_bundle, derive_keywords, derive_tags, normalize_alias,
    normalize_keyword, tokenize_text,
};
pub use vtb_context::{
    VtbCardRow, VtbChecklistItem, VtbContext, VtbSchema, enrich_vtb_context, fetch_card_for_task,
    fetch_checklist_items,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnrichmentOrigin {
    ResourceType,
    State,
    Title,
    Description,
    WorkNotes,
    Comments,
    Reference,
    Number,
    Token,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TagCandidate {
    pub value: String,
    pub origin: EnrichmentOrigin,
    pub weight: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KeywordCandidate {
    pub value: String,
    pub origin: EnrichmentOrigin,
    pub weight: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AliasCandidate {
    pub value: String,
    pub origin: EnrichmentOrigin,
    pub weight: f64,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct EnrichmentBundle {
    pub tags: Vec<TagCandidate>,
    pub keywords: Vec<KeywordCandidate>,
    pub aliases: Vec<AliasCandidate>,
}

impl EnrichmentBundle {
    pub fn is_empty(&self) -> bool {
        self.tags.is_empty() && self.keywords.is_empty() && self.aliases.is_empty()
    }
}

pub fn derive_for_record(record: &SnowRecord) -> EnrichmentBundle {
    derive_bundle(record)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        CacheSource, FieldValue, JournalEntry, RecordRef, Reference, ResourceType, SnowRecord,
    };
    use chrono::{TimeZone, Utc};
    use std::collections::HashMap;

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
                    value: "user-1".to_string(),
                    display_value: Some("Casey User".to_string()),
                },
            )]),
            work_notes: vec![JournalEntry {
                timestamp: Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
                author: "Jared".to_string(),
                body: "Network team is investigating the VPN gateway.".to_string(),
            }],
            comments: vec![JournalEntry {
                timestamp: Utc.timestamp_opt(1_712_649_900, 0).unwrap(),
                author: "Jane".to_string(),
                body: "Please update impacted users.".to_string(),
            }],
            parent: Some(RecordRef {
                sys_id: "chg-sys".to_string(),
                number: "CHG0001234".to_string(),
                table: "change_request".to_string(),
            }),
            children: Vec::new(),
            references: HashMap::from([(
                "assignment_group".to_string(),
                Reference {
                    sys_id: "grp-1".to_string(),
                    table: "sys_user_group".to_string(),
                    display_name: "Network Operations".to_string(),
                    extra: HashMap::new(),
                },
            )]),
            synced_at: Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
            source: CacheSource::Api,
        }
    }

    #[test]
    fn derives_stable_bundle() {
        let first = derive_for_record(&sample_record());
        let second = derive_for_record(&sample_record());
        assert_eq!(first, second);
    }

    #[test]
    fn derives_tags_keywords_and_aliases() {
        let bundle = derive_for_record(&sample_record());

        let tags: Vec<_> = bundle.tags.iter().map(|item| item.value.as_str()).collect();
        assert!(tags.contains(&"incident"));
        assert!(tags.contains(&"in-progress"));
        assert!(tags.contains(&"vpn"));
        assert!(tags.contains(&"network"));

        let keywords: Vec<_> = bundle
            .keywords
            .iter()
            .map(|item| item.value.as_str())
            .collect();
        assert!(keywords.contains(&"vpn"));
        assert!(keywords.contains(&"connectivity"));
        assert!(keywords.contains(&"network"));
        assert!(keywords.contains(&"investigating"));

        let aliases: Vec<_> = bundle
            .aliases
            .iter()
            .map(|item| item.value.as_str())
            .collect();
        assert!(aliases.contains(&"inc0012345"));
        assert!(aliases.contains(&"vpn connectivity drops"));
        assert!(aliases.contains(&"vcd"));
    }
}
