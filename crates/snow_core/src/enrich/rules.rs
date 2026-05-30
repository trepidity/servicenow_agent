use std::collections::{BTreeMap, BTreeSet};

use crate::{JournalEntry, ResourceType, SnowRecord};

use super::{AliasCandidate, EnrichmentBundle, EnrichmentOrigin, KeywordCandidate, TagCandidate};

const TAG_TERMS: &[(&str, &str)] = &[
    ("vpn", "vpn"),
    ("network", "network"),
    ("dns", "dns"),
    ("server", "server"),
    ("database", "database"),
    ("access", "access"),
    ("login", "login"),
    ("authentication", "authentication"),
    ("change", "change"),
    ("incident", "incident"),
    ("approval", "approval"),
    ("runbook", "runbook"),
    ("outage", "outage"),
    ("deploy", "deploy"),
    ("patch", "patch"),
];

const STOPWORDS: &[&str] = &[
    "the", "and", "for", "with", "from", "that", "this", "your", "you", "are", "was", "were",
    "has", "have", "had", "not", "but", "can", "all", "any", "can", "into", "out", "our", "over",
    "under", "when", "where", "what", "why", "how", "please", "update", "updated", "users", "user",
    "team", "issue", "issues", "work", "note", "notes",
];

pub fn derive_bundle(record: &SnowRecord) -> EnrichmentBundle {
    EnrichmentBundle {
        tags: derive_tags(record),
        keywords: derive_keywords(record),
        aliases: derive_aliases(record),
    }
}

pub fn derive_tags(record: &SnowRecord) -> Vec<TagCandidate> {
    let mut seen = BTreeMap::<String, TagCandidate>::new();

    insert_tag(
        &mut seen,
        resource_type_tag(record.resource_type.clone()),
        EnrichmentOrigin::ResourceType,
        1.0,
    );
    insert_tag(
        &mut seen,
        normalize_tag(&record.state),
        EnrichmentOrigin::State,
        0.8,
    );

    for term in extract_domain_terms(record) {
        insert_tag(&mut seen, term, EnrichmentOrigin::Token, 0.9);
    }

    for reference in record.references.values() {
        for term in extract_domain_terms_from_text(&reference.display_name) {
            insert_tag(&mut seen, term, EnrichmentOrigin::Reference, 0.75);
        }
    }

    seen.into_values().collect()
}

pub fn derive_keywords(record: &SnowRecord) -> Vec<KeywordCandidate> {
    let mut weights = BTreeMap::<String, (EnrichmentOrigin, f64)>::new();

    accumulate_text(
        &mut weights,
        &record.short_description,
        EnrichmentOrigin::Title,
        2.0,
    );
    accumulate_text(
        &mut weights,
        &record.description,
        EnrichmentOrigin::Description,
        1.5,
    );
    for entry in &record.work_notes {
        accumulate_journal_entry(&mut weights, entry, EnrichmentOrigin::WorkNotes, 1.0);
    }
    for entry in &record.comments {
        accumulate_journal_entry(&mut weights, entry, EnrichmentOrigin::Comments, 1.0);
    }
    for reference in record.references.values() {
        accumulate_text(
            &mut weights,
            &reference.display_name,
            EnrichmentOrigin::Reference,
            0.75,
        );
    }

    weights
        .into_iter()
        .map(|(value, (origin, weight))| KeywordCandidate {
            value,
            origin,
            weight,
        })
        .collect()
}

pub fn derive_aliases(record: &SnowRecord) -> Vec<AliasCandidate> {
    let mut seen = BTreeMap::<String, AliasCandidate>::new();

    insert_alias(
        &mut seen,
        normalize_alias(&record.number),
        EnrichmentOrigin::Number,
        2.0,
    );
    insert_alias(
        &mut seen,
        normalize_alias(&record.short_description),
        EnrichmentOrigin::Title,
        1.0,
    );

    let title_acronym = acronym(&record.short_description);
    if !title_acronym.is_empty() {
        insert_alias(&mut seen, title_acronym, EnrichmentOrigin::Title, 0.85);
    }

    for reference in record.references.values() {
        insert_alias(
            &mut seen,
            normalize_alias(&reference.display_name),
            EnrichmentOrigin::Reference,
            0.75,
        );
    }

    seen.into_values().collect()
}

pub fn tokenize_text(value: &str) -> Vec<String> {
    let mut tokens = BTreeSet::new();
    for token in value.split(|ch: char| !ch.is_ascii_alphanumeric()) {
        let token = token.trim().to_ascii_lowercase();
        if token.len() < 3 {
            continue;
        }
        if STOPWORDS.contains(&token.as_str()) {
            continue;
        }
        tokens.insert(token);
    }
    tokens.into_iter().collect()
}

pub fn normalize_keyword(value: &str) -> String {
    normalize_phrase(value)
}

pub fn normalize_alias(value: &str) -> String {
    normalize_phrase(value)
}

fn normalize_tag(value: &str) -> String {
    normalize_phrase(value).replace(' ', "-")
}

fn insert_tag(
    seen: &mut BTreeMap<String, TagCandidate>,
    value: String,
    origin: EnrichmentOrigin,
    weight: f64,
) {
    if value.is_empty() {
        return;
    }
    seen.entry(value.clone()).or_insert(TagCandidate {
        value,
        origin,
        weight,
    });
}

fn insert_alias(
    seen: &mut BTreeMap<String, AliasCandidate>,
    value: String,
    origin: EnrichmentOrigin,
    weight: f64,
) {
    if value.is_empty() {
        return;
    }
    seen.entry(value.clone()).or_insert(AliasCandidate {
        value,
        origin,
        weight,
    });
}

fn accumulate_text(
    weights: &mut BTreeMap<String, (EnrichmentOrigin, f64)>,
    text: &str,
    origin: EnrichmentOrigin,
    weight: f64,
) {
    for token in tokenize_text(text) {
        let entry = weights.entry(token).or_insert((origin, 0.0));
        entry.1 += weight;
    }
}

fn accumulate_journal_entry(
    weights: &mut BTreeMap<String, (EnrichmentOrigin, f64)>,
    entry: &JournalEntry,
    origin: EnrichmentOrigin,
    weight: f64,
) {
    accumulate_text(weights, &entry.body, origin, weight);
}

fn extract_domain_terms(record: &SnowRecord) -> Vec<String> {
    let mut terms = BTreeSet::new();
    let mut text = String::new();
    text.push_str(&record.short_description);
    text.push(' ');
    text.push_str(&record.description);
    for entry in &record.work_notes {
        text.push(' ');
        text.push_str(&entry.body);
    }
    for entry in &record.comments {
        text.push(' ');
        text.push_str(&entry.body);
    }
    for term in extract_domain_terms_from_text(&text) {
        terms.insert(term);
    }
    terms.into_iter().collect()
}

fn extract_domain_terms_from_text(text: &str) -> Vec<String> {
    let tokens = tokenize_text(text);
    let mut terms = BTreeSet::new();
    for token in tokens {
        if let Some((_, canonical)) = TAG_TERMS.iter().find(|(needle, _)| token == *needle) {
            terms.insert((*canonical).to_string());
        }
    }
    terms.into_iter().collect()
}

fn normalize_phrase(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut prev_space = false;

    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
            prev_space = false;
        } else if !prev_space {
            out.push(' ');
            prev_space = true;
        }
    }

    out.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn resource_type_tag(resource_type: ResourceType) -> String {
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
    }
    .to_string()
}

fn acronym(value: &str) -> String {
    let mut chars = String::new();
    for token in normalize_phrase(value).split_whitespace() {
        if token.len() < 3 {
            continue;
        }
        if let Some(first) = token.chars().next() {
            chars.push(first);
        }
    }
    if (3..=12).contains(&chars.len()) {
        chars
    } else {
        String::new()
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
    fn tokenize_text_filters_noise() {
        let tokens = tokenize_text("VPN, network! and the server.");
        assert_eq!(tokens, vec!["network", "server", "vpn"]);
    }

    #[test]
    fn alias_normalization_is_stable() {
        assert_eq!(
            normalize_alias("VPN Connectivity Drops"),
            "vpn connectivity drops"
        );
        assert_eq!(
            normalize_keyword("VPN Connectivity Drops"),
            "vpn connectivity drops"
        );
    }

    #[test]
    fn derive_bundle_is_deterministic() {
        let first = derive_bundle(&sample_record());
        let second = derive_bundle(&sample_record());
        assert_eq!(first, second);
    }

    #[test]
    fn derive_bundle_contains_expected_values() {
        let bundle = derive_bundle(&sample_record());

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
