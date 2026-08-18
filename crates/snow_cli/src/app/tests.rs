use super::{
    BusinessAppFilter, KnowledgeStatusSnapshot, ShowTarget, TimecardSelectorShape,
    business_app_export, classify_show_target, classify_timecard_selector,
    collect_timecard_updates, format_business_application_servers_cached_result,
    format_business_application_servers_result, format_business_applications_for_server_result,
    format_task_sla_status, is_show_sla_alias, load_knowledge_status, load_knowledge_tags,
    normalize_hours, weekday_index,
};
use crate::cli::KnowledgeTagLayer;
use business_app_export::BusinessAppExportFormat;
use chrono::TimeZone;
use rusqlite::Connection;
use snow_core::cache::store::{KnowledgeArticleRow, RecordRow, Store};
use snow_core::{
    CacheSource, FieldValue, KnowledgeArticle, Reference, ResourceType, SnowRecord,
    TaskSlaReadability, TaskSlaStatus, TaskSlaSummaryView, TaskSlaView,
    resource::timecard::Weekday,
};
use std::collections::HashMap;
use tempfile::tempdir;

fn sample_business_app_query_result() -> serde_json::Value {
    serde_json::json!({
        "business_applications": [
            {
                "browser_url": "https://example.service-now.com/nav_to.do?uri=cmdb_ci_business_app.do?sys_id=example-sys-id",
                "vault_relative_path": "business_applications/example-application.md",
                "record": {
                    "sys_id": "example-sys-id",
                    "number": "EXAMPLE-APP-001",
                    "short_description": "Description \"quoted\"\nsecond line"
                },
                "name": "Example Application, Core",
                "business_owner": {
                    "sys_id": "example-owner-sys-id",
                    "table": "sys_user",
                    "display_name": "Example Owner",
                    "extra": {}
                },
                "operational_state": {
                    "value": "1",
                    "display_value": "In Use"
                },
                "attested_date": "2026-01-31",
                "fields": {
                    "custom_beta": {
                        "value": "raw \"quote\"",
                        "display_value": null
                    },
                    "custom_alpha": {
                        "value": "raw",
                        "display_value": "Display, Value"
                    },
                    "managed_by_group": {
                        "value": "covered-by-base-source",
                        "display_value": "Covered By Base Source"
                    }
                }
            }
        ]
    })
}

#[test]
fn business_app_servers_human_output_summarizes_complete_result() {
    let result = serde_json::json!({
        "business_application": {
            "sys_id": "<BUSINESS_APP_SYS_ID>",
            "number": "<APM_NUMBER>",
            "name": "<BUSINESS_APP_NAME>",
            "table": "cmdb_ci_business_app"
        },
        "servers": [
            {
                "record": {
                    "sys_id": "<SERVER_SYS_ID>",
                    "number": "<SERVER_NUMBER>",
                    "table": "cmdb_ci_linux_server"
                },
                "name": "<SERVER_NAME>",
                "ip_address": "<SERVER_IP>",
                "class_name": "cmdb_ci_linux_server",
                "operational_status": {
                    "value": "<STATUS_VALUE>",
                    "display_value": "<STATUS_DISPLAY>"
                }
            }
        ],
        "relationship_summary": {
            "max_depth": 2,
            "servers_found": 1,
            "depth_limit_reached": false,
            "truncated": false,
            "truncated_count": 0,
            "acl_restricted_count": 0,
            "degraded_reasons": {}
        }
    });

    let out = format_business_application_servers_result(&result);

    assert!(out.contains("Business Application: <APM_NUMBER> <BUSINESS_APP_NAME>"));
    assert!(out.contains("Servers found: 1"));
    assert!(out.contains("Max depth: 2"));
    assert!(out.contains("Completeness: complete"));
    assert!(out.contains("Degraded: none"));
    assert!(out.contains("<SERVER_NAME>"));
    assert!(out.contains("cmdb_ci_linux_server"));
    assert!(out.contains("<SERVER_IP>"));
    assert!(out.contains("<STATUS_DISPLAY>"));
}

#[test]
fn business_app_servers_human_output_surfaces_partial_results() {
    let result = serde_json::json!({
        "business_application": {
            "sys_id": "<BUSINESS_APP_SYS_ID>",
            "number": "<APM_NUMBER>",
            "name": "<BUSINESS_APP_NAME>"
        },
        "servers": [],
        "relationship_summary": {
            "max_depth": 2,
            "servers_found": 0,
            "depth_limit_reached": true,
            "truncated": true,
            "truncated_count": 1,
            "acl_restricted_count": 1,
            "degraded_reasons": {
                "reference_acl_restricted": 1
            }
        }
    });

    let out = format_business_application_servers_result(&result);

    assert!(out.contains("Completeness: partial"));
    assert!(out.contains("depth_limit_reached"));
    assert!(out.contains("truncated"));
    assert!(out.contains("acl_restricted"));
    assert!(out.contains("reference_acl_restricted"));
    assert!(out.contains("No associated server CIs found within max depth 2."));
}

#[test]
fn business_app_servers_cached_human_output_summarizes_relationships() {
    let result = serde_json::json!({
        "business_application": {
            "sys_id": "<BUSINESS_APP_SYS_ID>",
            "number": "<APM_NUMBER>",
            "name": "<BUSINESS_APP_NAME>"
        },
        "servers": [
            {
                "server": {
                    "sys_id": "<SERVER_SYS_ID>",
                    "name": "<SERVER_NAME>",
                    "class_name": "cmdb_ci_linux_server",
                    "ip_address": "<SERVER_IP>",
                    "operational_status": {
                        "value": "<STATUS_VALUE>",
                        "display_value": "<STATUS_DISPLAY>"
                    }
                },
                "provenance": "live_traversal",
                "min_depth": 2,
                "tombstoned_at": null
            }
        ]
    });

    let out = format_business_application_servers_cached_result(&result);

    assert!(out.contains("Business Application: <APM_NUMBER> <BUSINESS_APP_NAME>"));
    assert!(out.contains("Cached servers found: 1"));
    assert!(out.contains("<SERVER_NAME>"));
    assert!(out.contains("cmdb_ci_linux_server"));
    assert!(out.contains("<SERVER_IP>"));
    assert!(out.contains("<STATUS_DISPLAY>"));
    assert!(out.contains("[depth 2, live_traversal]"));
}

#[test]
fn business_applications_for_server_human_output_summarizes_reverse_relationships() {
    let result = serde_json::json!({
        "servers": [
            {
                "server": {
                    "sys_id": "<SERVER_SYS_ID>",
                    "name": "<SERVER_NAME>",
                    "class_name": "cmdb_ci_linux_server",
                    "ip_address": "<SERVER_IP>"
                },
                "business_applications": [
                    {
                        "business_application": {
                            "sys_id": "<BUSINESS_APP_SYS_ID>",
                            "number": "<APM_NUMBER>",
                            "name": "<BUSINESS_APP_NAME>"
                        },
                        "provenance": "live_traversal",
                        "min_depth": 1,
                        "tombstoned_at": "<TOMBSTONED_AT>"
                    }
                ]
            }
        ]
    });

    let out = format_business_applications_for_server_result(&result);

    assert!(out.contains("Matched servers: 1"));
    assert!(out.contains("Cached Business Applications found: 1"));
    assert!(out.contains("Server: <SERVER_NAME>"));
    assert!(out.contains("<APM_NUMBER> <BUSINESS_APP_NAME>"));
    assert!(out.contains("[depth 1, live_traversal, tombstoned]"));
}

#[test]
fn business_app_export_validates_limit_before_query() {
    assert!(business_app_export::validate_limit(None).is_ok());
    assert!(business_app_export::validate_limit(Some(1)).is_ok());
    assert!(business_app_export::validate_limit(Some(500)).is_ok());

    let zero = business_app_export::validate_limit(Some(0)).expect_err("zero limit");
    assert!(zero.to_string().contains("at least 1"));

    let too_high = business_app_export::validate_limit(Some(501)).expect_err("high limit");
    assert!(too_high.to_string().contains("at most 500"));
}

#[test]
fn business_app_export_validates_text_before_query() {
    assert!(business_app_export::validate_text(None).is_ok());
    assert!(business_app_export::validate_text(Some("Example Application")).is_ok());

    let err = business_app_export::validate_text(Some("   ")).expect_err("blank text");
    assert!(err.to_string().contains("--text must not be empty"));
}

#[test]
fn business_app_export_all_validation_rejects_bounded_options() {
    assert!(super::validate_business_app_export_all_options(None, &[], None).is_ok());

    let filter = vec![BusinessAppFilter {
        field: "name".to_string(),
        operator: "contains".to_string(),
        value: "Example".to_string(),
    }];
    let err = super::validate_business_app_export_all_options(None, &filter, None)
        .expect_err("filter should conflict with --all");
    assert!(err.to_string().contains("accepts only --all"));

    let err = super::validate_business_app_export_all_options(Some("Example"), &[], Some(50))
        .expect_err("text/limit should conflict with --all");
    assert!(err.to_string().contains("accepts only --all"));
}

#[test]
fn business_app_sync_all_validation_rejects_bounded_filters() {
    assert!(super::validate_business_app_sync_all_options(None, None).is_ok());

    let err = super::validate_business_app_sync_all_options(Some("Example"), None)
        .expect_err("name should conflict with --all");
    assert!(
        err.to_string()
            .contains("does not accept bounded sync filters")
    );

    let err = super::validate_business_app_sync_all_options(None, Some("2"))
        .expect_err("operational-state-not should conflict with --all");
    assert!(
        err.to_string()
            .contains("does not accept bounded sync filters")
    );
}

#[test]
fn business_app_export_appends_query_pages_in_order() {
    let mut records = Vec::new();
    let first = serde_json::json!({
        "business_applications": [
            { "name": "First", "record": { "sys_id": "1" } },
            { "name": "Second", "record": { "sys_id": "2" } }
        ]
    });
    let second = serde_json::json!([{ "name": "Third", "record": { "sys_id": "3" } }]);

    let first_count = business_app_export::append_records_from_query_result(&mut records, &first)
        .expect("first page");
    let second_count = business_app_export::append_records_from_query_result(&mut records, &second)
        .expect("second page");

    assert_eq!(first_count, 2);
    assert_eq!(second_count, 1);
    assert_eq!(records.len(), 3);
    assert_eq!(records[0]["name"], "First");
    assert_eq!(records[2]["record"]["sys_id"], "3");
}

#[test]
fn business_app_export_appends_beyond_single_query_cap() {
    let mut records = Vec::new();
    let first = serde_json::json!({
        "business_applications": (0..business_app_export::EXPORT_ALL_PAGE_SIZE)
            .map(|index| serde_json::json!({
                "name": format!("Application {index:03}"),
                "record": { "sys_id": format!("first-{index:03}") }
            }))
            .collect::<Vec<_>>()
    });
    let second = serde_json::json!({
        "business_applications": [
            { "name": "Application 500", "record": { "sys_id": "second-000" } }
        ]
    });

    let first_count = business_app_export::append_records_from_query_result(&mut records, &first)
        .expect("first page");
    let second_count = business_app_export::append_records_from_query_result(&mut records, &second)
        .expect("second page");
    let result = serde_json::Value::Array(records);

    assert_eq!(first_count, business_app_export::EXPORT_ALL_PAGE_SIZE);
    assert_eq!(second_count, 1);
    assert_eq!(
        business_app_export::record_count(&result).expect("record count"),
        business_app_export::EXPORT_ALL_PAGE_SIZE + 1
    );
    assert_eq!(
        result[business_app_export::EXPORT_ALL_PAGE_SIZE]["record"]["sys_id"],
        "second-000"
    );
}

#[test]
fn business_app_export_page_append_rejects_non_object_records() {
    let mut records = Vec::new();
    let err = business_app_export::append_records_from_query_result(
        &mut records,
        &serde_json::json!({ "business_applications": ["not an object"] }),
    )
    .expect_err("non-object record should fail");

    assert!(
        err.to_string()
            .contains("record at index 0 was not an object")
    );
    assert!(records.is_empty());
}

#[test]
fn business_app_filters_to_query_maps_empty_to_empty() {
    let filters = super::business_app_filters_to_query(Vec::new());
    assert!(filters.is_empty());
}

#[test]
fn business_app_filters_to_query_preserves_mixed_operator_order() {
    // Mixed operators in eq-then-contains order. The conversion is a direct,
    // order-preserving mapping: clap already fixed the order, so there is no
    // argv re-read or homogeneous fallback that could mis-pair these.
    let filters = super::business_app_filters_to_query(vec![
        BusinessAppFilter {
            field: "number".to_string(),
            operator: "eq".to_string(),
            value: "EXAMPLE-APP-001".to_string(),
        },
        BusinessAppFilter {
            field: "name".to_string(),
            operator: "contains".to_string(),
            value: "Example".to_string(),
        },
    ]);

    assert_eq!(filters[0].field, "number");
    assert_eq!(filters[0].operator, "eq");
    assert_eq!(filters[0].value, "EXAMPLE-APP-001");
    assert_eq!(filters[1].field, "name");
    assert_eq!(filters[1].operator, "contains");
    assert_eq!(filters[1].value, "Example");
}

#[test]
fn business_app_export_validates_output_parent_before_query() {
    let tempdir = tempdir().expect("tempdir");
    let output = tempdir.path().join("business-apps.csv");
    business_app_export::validate_output_parent(&output).expect("existing parent");

    let err = business_app_export::validate_output_parent(tempdir.path()).expect_err("directory");
    assert!(err.to_string().contains("must name a file"));

    let missing_parent = tempdir.path().join("missing").join("business-apps.csv");
    let err =
        business_app_export::validate_output_parent(&missing_parent).expect_err("missing parent");
    assert!(err.to_string().contains("parent does not exist"));

    let parent_file = tempdir.path().join("parent-file");
    std::fs::write(&parent_file, "not a directory").expect("parent file");
    let child_output = parent_file.join("business-apps.csv");
    let err = business_app_export::validate_output_parent(&child_output).expect_err("parent file");
    assert!(err.to_string().contains("not a directory"));
}

#[test]
fn business_app_export_serializes_json_array() {
    let bytes = business_app_export::serialize(
        &sample_business_app_query_result(),
        BusinessAppExportFormat::Json,
    )
    .expect("json export");
    let parsed: serde_json::Value = serde_json::from_slice(&bytes).expect("valid json");

    let records = parsed.as_array().expect("json array");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["name"], "Example Application, Core");
}

#[test]
fn business_app_export_serializes_jsonl_compact_objects() {
    let bytes = business_app_export::serialize(
        &sample_business_app_query_result(),
        BusinessAppExportFormat::Jsonl,
    )
    .expect("jsonl export");
    let text = String::from_utf8(bytes).expect("utf8 jsonl");

    assert!(text.ends_with('\n'));
    let lines = text.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), 1);
    assert!(!lines[0].contains('\n'));
    let parsed: serde_json::Value = serde_json::from_str(lines[0]).expect("jsonl row");
    assert_eq!(parsed["record"]["number"], "EXAMPLE-APP-001");
}

#[test]
fn business_app_export_serializes_csv_with_deterministic_headers_and_escaping() {
    let bytes = business_app_export::serialize(
        &sample_business_app_query_result(),
        BusinessAppExportFormat::Csv,
    )
    .expect("csv export");
    let csv = String::from_utf8(bytes).expect("utf8 csv");
    let header = csv.lines().next().expect("header");

    assert_eq!(
        header,
        "record.sys_id,record.number,name,record.short_description,operational_state,business_owner,is_owner,ci_owner_group,primary_support_group,primary_portfolio,attested_date,vault_relative_path,browser_url,custom_alpha,custom_beta"
    );
    assert!(!header.contains("managed_by_group"));
    assert!(csv.contains("\"Example Application, Core\""));
    assert!(csv.contains("\"Description \"\"quoted\"\"\nsecond line\""));
    assert!(csv.contains("In Use"));
    assert!(csv.contains("Example Owner"));
    assert!(csv.contains("\"Display, Value\""));
    assert!(csv.contains("\"raw \"\"quote\"\"\""));
}

#[test]
fn business_app_export_serializes_empty_formats() {
    let empty = serde_json::json!({ "business_applications": [] });

    let json =
        business_app_export::serialize(&empty, BusinessAppExportFormat::Json).expect("empty json");
    assert_eq!(String::from_utf8(json).expect("utf8"), "[]");

    let jsonl = business_app_export::serialize(&empty, BusinessAppExportFormat::Jsonl)
        .expect("empty jsonl");
    assert!(jsonl.is_empty());

    let csv =
        business_app_export::serialize(&empty, BusinessAppExportFormat::Csv).expect("empty csv");
    assert_eq!(
        String::from_utf8(csv).expect("utf8"),
        "record.sys_id,record.number,name,record.short_description,operational_state,business_owner,is_owner,ci_owner_group,primary_support_group,primary_portfolio,attested_date,vault_relative_path,browser_url\n"
    );
}

#[test]
fn business_app_export_write_file_replaces_target_after_temp_write() {
    let tempdir = tempdir().expect("tempdir");
    let output = tempdir.path().join("business-apps.json");
    std::fs::write(&output, "old content").expect("old file");

    business_app_export::write_file(&output, b"new content").expect("write export");

    assert_eq!(
        std::fs::read_to_string(&output).expect("read output"),
        "new content"
    );
}

#[test]
fn classify_show_target_routes_inc_and_chg_correctly() {
    assert_eq!(classify_show_target("INC4924830"), ShowTarget::Incident);
    assert_eq!(classify_show_target("CHG0329219"), ShowTarget::Change);
}

#[test]
fn classify_show_target_routes_known_special_cases() {
    assert_eq!(classify_show_target("REQ2684923"), ShowTarget::Request);
    assert_eq!(classify_show_target("KB0101565"), ShowTarget::Knowledge);
    assert_eq!(classify_show_target("STSK0049275"), ShowTarget::StoryTask);
    assert_eq!(
        classify_show_target("RPLN0091599"),
        ShowTarget::ResourcePlan
    );
    assert_eq!(classify_show_target("PTSK0000001"), ShowTarget::PrivateTask);
}

#[test]
fn task_sla_show_alias_requires_one_sla_extra() {
    assert!(is_show_sla_alias(&["sla".to_string()]));
    assert!(is_show_sla_alias(&["SLA".to_string()]));
    assert!(!is_show_sla_alias(&[]));
    assert!(!is_show_sla_alias(&[
        "sla".to_string(),
        "activity".to_string()
    ]));
}

#[test]
fn timecard_selector_shape_is_hardened() {
    assert_eq!(
        classify_timecard_selector("0123456789abcdef0123456789abcdef"),
        TimecardSelectorShape::SysId
    );
    assert_eq!(
        classify_timecard_selector("3"),
        TimecardSelectorShape::Index(3)
    );
    assert_eq!(
        classify_timecard_selector("PRJ0161219"),
        TimecardSelectorShape::Task
    );
    assert_eq!(
        classify_timecard_selector("1234abcd"),
        TimecardSelectorShape::Task
    );
}

#[test]
fn timecard_update_parser_accepts_single_or_multi_day_forms() {
    let updates = collect_timecard_updates(
        Some("mon"),
        Some("8.00"),
        [
            (Weekday::Sun, None),
            (Weekday::Mon, None),
            (Weekday::Tue, None),
            (Weekday::Wed, None),
            (Weekday::Thu, None),
            (Weekday::Fri, None),
            (Weekday::Sat, None),
        ],
    )
    .unwrap();
    assert_eq!(updates.len(), 1);
    assert_eq!(weekday_index(updates[0].day), 1);
    assert_eq!(updates[0].hours, "8");

    let updates = collect_timecard_updates(
        None,
        None,
        [
            (Weekday::Sun, None),
            (Weekday::Mon, Some("8")),
            (Weekday::Tue, Some("4.50")),
            (Weekday::Wed, None),
            (Weekday::Thu, None),
            (Weekday::Fri, None),
            (Weekday::Sat, None),
        ],
    )
    .unwrap();
    assert_eq!(updates.len(), 2);
    assert_eq!(weekday_index(updates[0].day), 1);
    assert_eq!(updates[1].hours, "4.5");
}

#[test]
fn timecard_hours_normalize_and_validate() {
    assert_eq!(normalize_hours("8.00").unwrap(), "8");
    assert_eq!(normalize_hours("7.25").unwrap(), "7.25");
    assert!(normalize_hours("-1").is_err());
    assert!(normalize_hours("24.01").is_err());
    assert!(normalize_hours("abc").is_err());
}

#[test]
fn task_sla_output_formats_readable_rows_in_product_order() {
    let first = task_sla_view(
        "First response",
        Some("in_progress"),
        Some(true),
        Some(false),
        "2026-05-08 12:00:00",
        Some(65.25),
        Some(" 1970-01-01 04:00:00 "),
    );
    let second = task_sla_view(
        "Second response",
        Some("completed"),
        Some(false),
        Some(true),
        "2026-05-09 12:00:00",
        Some(90.0),
        None,
    );
    let status = task_sla_status(
        TaskSlaReadability::ReadableRows,
        vec![first.clone(), second],
        TaskSlaSummaryView {
            total: 2,
            active: 1,
            breached: 1,
            next_breach: Some(first),
            highest_business_elapsed: Some(90.0),
        },
    );

    let rendered = format_task_sla_status(&status);

    assert!(rendered.contains("Task SLA: TASK000001 (task)"));
    assert!(rendered.contains("summary:\n  total: 2\n  active: 1\n  breached: 1"));
    assert!(rendered.contains(
        "next breach: 2026-05-08 12:00:00 (First response, time left 1970-01-01 04:00:00)"
    ));
    assert!(rendered.contains("highest business elapsed: 90%"));
    assert!(rendered.contains("business elapsed: 65.2%"));
    let first_pos = rendered.find("1. First response").unwrap();
    let second_pos = rendered.find("2. Second response").unwrap();
    assert!(first_pos < second_pos, "{rendered}");
}

#[test]
fn task_sla_output_preserves_empty_or_acl_ambiguity() {
    let status = task_sla_status(
        TaskSlaReadability::EmptyOrAclRestricted,
        Vec::new(),
        zero_task_sla_summary(),
    );

    let rendered = format_task_sla_status(&status);

    assert!(rendered.contains("total: 0"));
    assert!(rendered.contains("No readable Task SLA rows or none attached"));
    assert!(rendered.contains("ACL-restricted"));
}

#[test]
fn task_sla_output_reports_parent_not_found() {
    let status = TaskSlaStatus {
        record_number: "TASK000404".to_string(),
        record_table: String::new(),
        record_sys_id: String::new(),
        rows: Vec::new(),
        summary: zero_task_sla_summary(),
        readable: TaskSlaReadability::ParentNotFound,
    };

    let rendered = format_task_sla_status(&status);

    assert!(rendered.contains("Record not found: TASK000404"));
    assert!(!rendered.contains("summary:"));
}

#[test]
fn task_sla_output_reports_not_applicable_record_type() {
    let status = TaskSlaStatus {
        record_number: "KB000001".to_string(),
        record_table: "kb_knowledge".to_string(),
        record_sys_id: "kb-sys-1".to_string(),
        rows: Vec::new(),
        summary: zero_task_sla_summary(),
        readable: TaskSlaReadability::NotApplicable,
    };

    let rendered = format_task_sla_status(&status);

    assert!(rendered.contains("Task SLAs do not apply to this record type: kb_knowledge"));
}

#[test]
fn formats_knowledge_article_details_with_metadata() {
    let article = KnowledgeArticle {
        record: SnowRecord {
            sys_id: "kb-sys".to_string(),
            number: "KB0105015".to_string(),
            table: "kb_knowledge".to_string(),
            resource_type: ResourceType::Knowledge,
            state: "published".to_string(),
            short_description: "Windows server admin access".to_string(),
            description: "Summary body".to_string(),
            fields: HashMap::from([(
                "workflow_state".to_string(),
                FieldValue {
                    value: "published".to_string(),
                    display_value: Some("Published".to_string()),
                },
            )]),
            work_notes: Vec::new(),
            comments: Vec::new(),
            parent: None,
            children: Vec::new(),
            references: HashMap::new(),
            synced_at: chrono::Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
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
            display_name: "Security".to_string(),
            extra: HashMap::new(),
        },
        article_type: "text".to_string(),
        content: "Step 1: Request access.".to_string(),
        sn_tags: vec!["password".to_string()],
        auto_tags: vec!["authentication".to_string()],
        user_tags: vec!["runbook".to_string()],
        body_cached: true,
        published_at: Some(chrono::Utc.timestamp_opt(1_712_649_800, 0).unwrap()),
        author: Some(Reference {
            sys_id: "user-1".to_string(),
            table: "sys_user".to_string(),
            display_name: "Casey User".to_string(),
            extra: HashMap::new(),
        }),
        valid_to: Some(chrono::NaiveDate::from_ymd_opt(2027, 1, 1).unwrap()),
    };

    let rendered = super::format_knowledge_article(&article, true);
    assert!(rendered.contains("KB0105015 [published] Windows server admin access"));
    assert!(rendered.contains("knowledge base: IT Operations"));
    assert!(rendered.contains("author: Casey User (user-1)"));
    assert!(rendered.contains("published: "));
    assert!(rendered.contains("valid to: 2027-01-01"));
    assert!(rendered.contains("Summary:"));
    assert!(rendered.contains("Content:"));

    let summary_only = super::format_knowledge_article(&article, false);
    assert!(!summary_only.contains("Summary:"));
    assert!(!summary_only.contains("Content:"));
}

#[test]
fn load_knowledge_status_and_tags_read_runtime_catalog() {
    let tempdir = tempdir().expect("tempdir");
    let db_path = tempdir.path().join("snow.db");
    let store = Store::open(&db_path).expect("store");
    let now = chrono::Utc.timestamp_opt(1_712_649_600, 0).unwrap();

    store
        .upsert_record(
            &RecordRow::active(
                "kb-sys",
                "KB001",
                "kb_knowledge",
                ResourceType::Knowledge,
                now,
            ),
            "",
            "Sample body",
        )
        .expect("record");
    store
        .upsert_knowledge_article(&KnowledgeArticleRow {
            record_sys_id: "kb-sys".to_string(),
            number: "KB001".to_string(),
            title: "Sample KB".to_string(),
            workflow_state: "published".to_string(),
            knowledge_base_sys_id: "kb-base".to_string(),
            knowledge_base_name: "IT".to_string(),
            category_sys_id: "kb-cat".to_string(),
            category_name: "Accounts".to_string(),
            author_sys_id: None,
            author_name: None,
            published_at: Some("2026-04-10 09:00:00".to_string()),
            valid_to: None,
            article_type: "text".to_string(),
            sys_updated_on: Some("2026-04-10 09:00:00".to_string()),
            sn_tags: vec!["password".to_string()],
            auto_tags: vec!["authentication".to_string()],
            user_tags: vec!["runbook".to_string()],
            body_cached: true,
        })
        .expect("knowledge article");
    store
        .upsert_record(
            &RecordRow::active(
                "kb-old",
                "KB000",
                "kb_knowledge",
                ResourceType::Knowledge,
                now,
            ),
            "",
            "Old body",
        )
        .expect("old record");
    store
        .upsert_knowledge_article(&KnowledgeArticleRow {
            record_sys_id: "kb-old".to_string(),
            number: "KB000".to_string(),
            title: "Old KB".to_string(),
            workflow_state: "published".to_string(),
            knowledge_base_sys_id: "kb-base".to_string(),
            knowledge_base_name: "IT".to_string(),
            category_sys_id: "kb-old-cat".to_string(),
            category_name: "Legacy".to_string(),
            author_sys_id: None,
            author_name: None,
            published_at: Some("2026-04-09 09:00:00".to_string()),
            valid_to: None,
            article_type: "text".to_string(),
            sys_updated_on: Some("2026-04-09 09:00:00".to_string()),
            sn_tags: vec!["legacy".to_string()],
            auto_tags: Vec::new(),
            user_tags: Vec::new(),
            body_cached: true,
        })
        .expect("old knowledge article");
    store
        .tombstone_record("kb-old", now)
        .expect("tombstone old record");

    let conn = Connection::open(&db_path).expect("connection");
    conn.execute(
        r#"
            UPDATE kb_sync_state
            SET last_full_at = 1712649800000,
                last_incr_at = 1712650100000,
                watermark_updated_at = '2026-04-10 09:00:00',
                watermark_sys_id = 'kb-sys',
                kb_sync_lock = 1712650200000
            WHERE id = 1
            "#,
        [],
    )
    .expect("seed kb state");

    let status: KnowledgeStatusSnapshot = load_knowledge_status(&db_path).expect("status");
    assert_eq!(status.article_count, 1);
    assert_eq!(status.body_cached_count, 1);
    assert_eq!(status.knowledge_base_count, 1);
    assert_eq!(status.category_count, 1);
    assert!(status.lock_held);

    let tags = load_knowledge_tags(&db_path, KnowledgeTagLayer::All, 1).expect("tags");
    assert!(tags.iter().any(|tag| tag.tag == "password"));
    assert!(tags.iter().any(|tag| tag.tag == "authentication"));
    assert!(tags.iter().any(|tag| tag.tag == "runbook"));
}

fn task_sla_status(
    readable: TaskSlaReadability,
    rows: Vec<TaskSlaView>,
    summary: TaskSlaSummaryView,
) -> TaskSlaStatus {
    TaskSlaStatus {
        record_number: "TASK000001".to_string(),
        record_table: "task".to_string(),
        record_sys_id: "task-sys-1".to_string(),
        rows,
        summary,
        readable,
    }
}

fn task_sla_view(
    name: &str,
    stage: Option<&str>,
    active: Option<bool>,
    breached: Option<bool>,
    planned_end_time: &str,
    business_elapsed_percentage: Option<f64>,
    time_left: Option<&str>,
) -> TaskSlaView {
    TaskSlaView {
        name: Some(name.to_string()),
        stage: stage.map(str::to_string),
        active,
        breached,
        planned_end_time: Some(planned_end_time.to_string()),
        business_elapsed_percentage,
        time_left: time_left.map(str::to_string),
    }
}

fn zero_task_sla_summary() -> TaskSlaSummaryView {
    TaskSlaSummaryView {
        total: 0,
        active: 0,
        breached: 0,
        next_breach: None,
        highest_business_elapsed: None,
    }
}
