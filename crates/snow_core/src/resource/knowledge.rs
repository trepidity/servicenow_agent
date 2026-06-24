use chrono::NaiveDate;
use servicenow_rs::model::value::parse_servicenow_timestamp;
use servicenow_rs::prelude::Record;

use crate::{
    CacheSource, FieldValue, KnowledgeArticle, Reference, ResourceType, SnowRecord,
    choose_reference_display_name, normalize_knowledge_article,
};

#[derive(Debug, Clone, Default)]
pub struct KnowledgeResource;

impl KnowledgeResource {
    pub fn from_servicenow(
        record: &Record,
        knowledge_base: Reference,
        category: Reference,
    ) -> KnowledgeArticle {
        let content = canonical_body(record);
        let body_cached = body_fields_present(record);
        normalize_knowledge_article(KnowledgeArticle {
            record: Self::record_model(record, knowledge_base.clone(), category.clone()),
            knowledge_base,
            category,
            article_type: record.get_str("article_type").unwrap_or("text").to_string(),
            content: content.clone(),
            sn_tags: Vec::new(),
            auto_tags: Vec::new(),
            user_tags: Vec::new(),
            body_cached,
            published_at: parse_servicenow_timestamp(record.get_str("published")),
            author: Self::author_reference(record),
            valid_to: parse_naive_date(record.get_str("valid_to")),
        })
    }

    pub fn record_model(
        record: &Record,
        knowledge_base: Reference,
        category: Reference,
    ) -> SnowRecord {
        let mut model = SnowRecord::from_servicenow(record);
        model.resource_type = ResourceType::Knowledge;
        model.table = "kb_knowledge".to_string();
        model.synced_at = chrono::Utc::now();
        model.source = CacheSource::Api;
        model
            .references
            .insert("knowledge_base".to_string(), knowledge_base);
        model.references.insert("category".to_string(), category);
        if let Some(author) = Self::author_reference(record) {
            model.references.insert("author".to_string(), author);
        }
        model
    }

    pub fn knowledge_base_reference(record: &Record) -> Option<Reference> {
        reference_from_candidates(
            record,
            &["knowledge_base", "kb_knowledge_base"],
            "kb_knowledge_base",
        )
    }

    pub fn category_reference(record: &Record) -> Option<Reference> {
        reference_from_candidates(record, &["category", "kb_category"], "kb_category")
    }

    pub fn author_reference(record: &Record) -> Option<Reference> {
        reference_from_candidates(record, &["author"], "sys_user")
    }

    pub fn field_values(record: &Record) -> std::collections::HashMap<String, FieldValue> {
        record
            .fields()
            .iter()
            .map(|(key, value)| {
                (
                    key.clone(),
                    FieldValue {
                        value: value
                            .value
                            .as_ref()
                            .map(|v| match v {
                                serde_json::Value::String(s) => s.clone(),
                                other => other.to_string(),
                            })
                            .unwrap_or_default(),
                        display_value: value.display_value.clone(),
                    },
                )
            })
            .collect()
    }
}

fn reference_from_candidates(record: &Record, fields: &[&str], table: &str) -> Option<Reference> {
    for field in fields {
        let Some(sys_id) = record.get_raw(field).or_else(|| record.get_str(field)) else {
            continue;
        };
        let display_name = choose_reference_display_name([
            record.get_display(field),
            record.get_str(&format!("{field}.name")),
            record.get_str(&format!("{field}.label")),
            record.get_str(&format!("{field}.title")),
            record.get_str(&format!("{field}.user_name")),
            record.get_str(&format!("{field}.email")),
            record.get_str(&format!("{field}.number")),
        ]);

        return Some(Reference {
            sys_id: sys_id.to_string(),
            table: table.to_string(),
            display_name,
            extra: std::collections::HashMap::new(),
        });
    }

    None
}

fn canonical_body(record: &Record) -> String {
    [record.get_str("article_body"), record.get_str("text")]
        .into_iter()
        .flatten()
        .map(str::trim)
        .find(|value| !value.is_empty())
        .unwrap_or_default()
        .to_string()
}

fn body_fields_present(record: &Record) -> bool {
    record.fields().contains_key("article_body") || record.fields().contains_key("text")
}

fn parse_naive_date(value: Option<&str>) -> Option<NaiveDate> {
    let value = value?.trim();
    if value.is_empty() {
        return None;
    }

    NaiveDate::parse_from_str(value, "%Y-%m-%d").ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use servicenow_rs::model::value::FieldValue as SnFieldValue;

    fn empty_reference(table: &str) -> Reference {
        Reference {
            sys_id: String::new(),
            table: table.to_string(),
            display_name: String::new(),
            extra: std::collections::HashMap::new(),
        }
    }

    #[test]
    fn knowledge_references_fall_back_to_kb_prefixed_fields() {
        let mut record = Record::new("kb_knowledge", "kb-sys");
        record.set(
            "kb_knowledge_base",
            SnFieldValue {
                value: Some(serde_json::json!("kb-base")),
                display_value: Some("IT Knowledge".to_string()),
                link: None,
            },
        );
        record.set(
            "kb_category",
            SnFieldValue {
                value: Some(serde_json::json!("kb-cat")),
                display_value: Some("Access/Security".to_string()),
                link: None,
            },
        );

        let knowledge_base =
            KnowledgeResource::knowledge_base_reference(&record).expect("knowledge base");
        let category = KnowledgeResource::category_reference(&record).expect("category");

        assert_eq!(knowledge_base.sys_id, "kb-base");
        assert_eq!(knowledge_base.display_name, "IT Knowledge");
        assert_eq!(category.sys_id, "kb-cat");
        assert_eq!(category.display_name, "Access/Security");
    }

    #[test]
    fn body_cached_marks_full_body_projection_even_when_empty() {
        let record = Record::from_json(
            "kb_knowledge",
            &serde_json::json!({
                "sys_id": "kb-empty-body-sys",
                "number": "KB0001",
                "short_description": "Empty body",
                "article_body": "",
                "text": ""
            }),
            servicenow_rs::prelude::DisplayValue::Both,
        )
        .expect("knowledge record");

        let article = KnowledgeResource::from_servicenow(
            &record,
            empty_reference("kb_knowledge_base"),
            empty_reference("kb_category"),
        );

        assert!(article.body_cached);
        assert!(article.content.is_empty());
    }

    #[test]
    fn body_cached_stays_false_for_metadata_only_projection() {
        let record = Record::from_json(
            "kb_knowledge",
            &serde_json::json!({
                "sys_id": "kb-metadata-only-sys",
                "number": "KB0001",
                "short_description": "Metadata only"
            }),
            servicenow_rs::prelude::DisplayValue::Both,
        )
        .expect("knowledge record");

        let article = KnowledgeResource::from_servicenow(
            &record,
            empty_reference("kb_knowledge_base"),
            empty_reference("kb_category"),
        );

        assert!(!article.body_cached);
        assert!(article.content.is_empty());
    }
}
