use super::*;
use chrono::TimeZone;
use serde_json::Value;
use std::collections::HashMap;

#[test]
fn initializes_schema_and_persists_records() {
    let store = Store::open_in_memory().expect("store");
    let record = RecordRow::active(
        "8a4d2e0ec3577e5433b2b643e4013100",
        "INC0012345",
        "incident",
        ResourceType::Incident,
        Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
    );
    store
        .upsert_record(&record, "work note", "content")
        .expect("insert");

    let loaded = store
        .get_record_by_number("INC0012345")
        .expect("query")
        .expect("row");

    assert_eq!(loaded.number, "INC0012345");
    assert!(loaded.in_scope);
    assert_eq!(store.count_active_records().unwrap(), 1);
}

// L1 on-disk contract: a record first persisted with a vault path must remain
// recoverable if a later cache projection no longer has that path.
#[test]
fn vault_backed_provenance_survives_path_loss_and_cache_only_adoption() {
    let store = Store::open_in_memory().expect("store");
    let mut vault_backed = RecordRow::active(
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "INC0019999",
        "incident",
        ResourceType::Incident,
        Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
    );
    vault_backed.file_path = Some("incidents/INC0019999.md".to_string());
    store
        .upsert_record(&vault_backed, "", "")
        .expect("insert vault-backed row");

    let mut path_lost = vault_backed.clone();
    path_lost.file_path = None;
    store
        .upsert_record(&path_lost, "", "")
        .expect("update after markdown path loss");

    assert_eq!(
        store
            .active_record_vault_provenance()
            .expect("provenance before adoption")
            .get(&vault_backed.sys_id),
        Some(&VaultProjectionProvenance::VaultBacked)
    );
    assert_eq!(
        store
            .adopt_legacy_cache_only_records()
            .expect("adopt cache-only projection"),
        0,
        "a formerly vault-backed record must not be adopted as cache-only"
    );
    assert_eq!(
        store
            .active_record_vault_provenance()
            .expect("provenance after adoption")
            .get(&vault_backed.sys_id),
        Some(&VaultProjectionProvenance::VaultBacked)
    );
}

#[test]
fn persists_base_task_resource_type() {
    let store = Store::open_in_memory().expect("store");
    let record = RecordRow::active(
        "task-sys",
        "TASK0012345",
        "task",
        ResourceType::Task,
        Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
    );

    store.upsert_record(&record, "", "").expect("insert task");

    let loaded = store
        .get_record_by_number_and_type("TASK0012345", ResourceType::Task)
        .expect("query")
        .expect("row");
    let listed = store
        .list_active_records(Some(ResourceType::Task))
        .expect("list tasks");

    assert_eq!(loaded.resource_type, ResourceType::Task);
    assert_eq!(loaded.table_name, "task");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].number, "TASK0012345");
}

fn business_application_record(sys_id: &str, name: &str, cost: &str) -> SnowRecord {
    let owner_sys_id = "6816f79cc0a8016401c5a33be04be441";
    SnowRecord {
        sys_id: sys_id.to_string(),
        number: format!("BA:{sys_id}"),
        table: "cmdb_ci_business_app".to_string(),
        resource_type: ResourceType::BusinessApplication,
        state: "1".to_string(),
        short_description: name.to_string(),
        description: "Business Application".to_string(),
        fields: HashMap::from([
            (
                "sys_id".to_string(),
                FieldValue {
                    value: sys_id.to_string(),
                    display_value: None,
                },
            ),
            (
                "name".to_string(),
                FieldValue {
                    value: name.to_string(),
                    display_value: None,
                },
            ),
            (
                "business_owner".to_string(),
                FieldValue {
                    value: owner_sys_id.to_string(),
                    display_value: Some("Jane Owner".to_string()),
                },
            ),
            (
                "operational_status".to_string(),
                FieldValue {
                    value: "1".to_string(),
                    display_value: Some("Operational".to_string()),
                },
            ),
            (
                "attested_date".to_string(),
                FieldValue {
                    value: "2026-05-01".to_string(),
                    display_value: None,
                },
            ),
            (
                "u_cost".to_string(),
                FieldValue {
                    value: cost.to_string(),
                    display_value: None,
                },
            ),
            (
                "u_code".to_string(),
                FieldValue {
                    value: "ABC-123".to_string(),
                    display_value: None,
                },
            ),
            (
                "u_region".to_string(),
                FieldValue {
                    value: "north".to_string(),
                    display_value: Some("North".to_string()),
                },
            ),
            (
                "u_empty".to_string(),
                FieldValue {
                    value: String::new(),
                    display_value: None,
                },
            ),
        ]),
        work_notes: Vec::new(),
        comments: Vec::new(),
        parent: None,
        children: Vec::new(),
        references: HashMap::new(),
        synced_at: Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
        source: crate::CacheSource::Disk,
    }
}

fn insert_business_application(store: &Store, record: &SnowRecord) {
    let mut row = RecordRow::active(
        record.sys_id.clone(),
        record.number.clone(),
        "cmdb_ci_business_app",
        ResourceType::BusinessApplication,
        Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
    );
    row.short_desc = Some(record.short_description.clone());
    row.description = Some(record.description.clone());
    row.raw_json = serde_json::to_string(&field_values_to_json_map(&record.fields))
        .expect("raw business application json");
    store.upsert_record(&row, "", "").expect("insert BA row");
    let raw = serde_json::from_str::<Value>(&row.raw_json).expect("raw json");
    store
        .upsert_business_application_projection(record, Some(&raw))
        .expect("project BA row");
}

#[test]
fn initializes_current_cache_projection_user_cache_ba_server_and_health_tables() {
    let store = Store::open_in_memory().expect("store");

    assert_eq!(
        store.get_meta_value("cache_format").expect("cache format"),
        Some(CACHE_FORMAT_ID.to_string())
    );
    for table in [
        "business_applications",
        "business_application_fields",
        "business_application_field_dictionary",
        "business_application_servers",
        "business_application_server_inventory_health",
        "primitive_objects",
        "primitive_object_fields",
        "cached_users",
        "cached_user_queries",
    ] {
        assert!(store.table_exists(table).expect("table exists"), "{table}");
    }
    for index in [
        "idx_ba_name",
        "idx_ba_fields_ref",
        "idx_ba_servers_ba",
        "idx_ba_servers_server",
        "idx_ba_servers_one_live_pair",
        "idx_primitive_fields_ref",
        "idx_cached_users_user_name",
        "idx_cached_users_email",
        "idx_cached_users_employee_number",
        "idx_cached_users_name",
        "idx_cached_users_first_last",
        "idx_cached_user_queries_expires",
    ] {
        assert!(store.index_exists(index).expect("index exists"), "{index}");
    }
}

#[test]
fn business_application_server_memberships_round_trip_by_ba_and_server() {
    let store = Store::open_in_memory().expect("store");
    let now = Utc.timestamp_opt(1_779_840_000, 0).unwrap();
    let ba = RecordRow::active(
        "54a4b61b6fe845000ed852a03f3ee4d0",
        "APM0000001",
        "cmdb_ci_business_app",
        ResourceType::BusinessApplication,
        now,
    );
    let server = RecordRow::active(
        "7f4a6e2f1c23456789abcdef01234567",
        "SRV0000001",
        "cmdb_ci_linux_server",
        ResourceType::Server,
        now,
    );
    store.upsert_record(&ba, "", "").expect("insert BA");
    store.upsert_record(&server, "", "").expect("insert server");
    let membership = BusinessApplicationServerMembershipRow {
        ba_sys_id: "54a4b61b6fe845000ed852a03f3ee4d0".to_string(),
        server_sys_id: "7f4a6e2f1c23456789abcdef01234567".to_string(),
        server_table: "cmdb_ci_linux_server".to_string(),
        provenance: "cmdb_rel_ci".to_string(),
        min_depth: 2,
        paths_json: serde_json::json!([{
            "edges": [{
                "depth": 1,
                "parent_sys_id": "54a4b61b6fe845000ed852a03f3ee4d0",
                "child_sys_id": "7f4a6e2f1c23456789abcdef01234567",
                "direction": "parent_to_child",
                "relationship_type": {
                    "value": "Depends on::Used by"
                }
            }]
        }])
        .to_string(),
        discovered_at: now,
        last_seen_at: now,
        tombstoned_at: None,
    };

    store
        .upsert_business_application_server_membership(&membership)
        .expect("upsert membership");

    let by_ba = store
        .list_business_application_server_memberships_for_ba(&membership.ba_sys_id, false)
        .expect("list by BA");
    assert_eq!(by_ba, vec![membership.clone()]);

    let by_server = store
        .list_business_application_server_memberships_for_server(&membership.server_sys_id, false)
        .expect("list by server");
    assert_eq!(by_server, vec![membership]);
}

#[test]
fn tombstones_stale_business_application_server_memberships_and_reactivates_on_upsert() {
    let store = Store::open_in_memory().expect("store");
    let old = Utc.timestamp_opt(1_779_840_000, 0).unwrap();
    let cutoff = Utc.timestamp_opt(1_779_843_600, 0).unwrap();
    let tombstoned_at = Utc.timestamp_opt(1_779_847_200, 0).unwrap();
    let ba = RecordRow::active(
        "54a4b61b6fe845000ed852a03f3ee4d0",
        "APM0000001",
        "cmdb_ci_business_app",
        ResourceType::BusinessApplication,
        old,
    );
    let server = RecordRow::active(
        "7f4a6e2f1c23456789abcdef01234567",
        "SRV0000001",
        "cmdb_ci_linux_server",
        ResourceType::Server,
        old,
    );
    store.upsert_record(&ba, "", "").expect("insert BA");
    store.upsert_record(&server, "", "").expect("insert server");
    let mut membership = BusinessApplicationServerMembershipRow {
        ba_sys_id: ba.sys_id.clone(),
        server_sys_id: server.sys_id.clone(),
        server_table: server.table_name.clone(),
        provenance: "relationship".to_string(),
        min_depth: 1,
        paths_json: "[]".to_string(),
        discovered_at: old,
        last_seen_at: old,
        tombstoned_at: None,
    };
    store
        .upsert_business_application_server_membership(&membership)
        .expect("upsert membership");

    let pruned = store
        .tombstone_stale_business_application_server_memberships(&ba.sys_id, cutoff, tombstoned_at)
        .expect("tombstone stale");
    assert_eq!(pruned, 1);
    assert!(
        store
            .list_business_application_server_memberships_for_ba(&ba.sys_id, false)
            .expect("active memberships")
            .is_empty()
    );
    let tombstoned = store
        .list_business_application_server_memberships_for_ba(&ba.sys_id, true)
        .expect("tombstoned memberships");
    assert_eq!(tombstoned.len(), 1);
    assert_eq!(tombstoned[0].tombstoned_at, Some(tombstoned_at));

    membership.last_seen_at = cutoff;
    store
        .upsert_business_application_server_membership(&membership)
        .expect("reactivate membership");
    let active = store
        .list_business_application_server_memberships_for_ba(&ba.sys_id, false)
        .expect("active memberships after re-seen");
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].tombstoned_at, None);
}

#[test]
fn business_application_server_membership_transition_keeps_one_live_pair_and_first_seen() {
    let store = Store::open_in_memory().expect("store");
    let first_seen = Utc.timestamp_opt(1_779_840_000, 0).unwrap();
    let later_seen = Utc.timestamp_opt(1_779_843_600, 0).unwrap();
    let ba = RecordRow::active(
        "54a4b61b6fe845000ed852a03f3ee4d0",
        "APM0000001",
        "cmdb_ci_business_app",
        ResourceType::BusinessApplication,
        first_seen,
    );
    let server = RecordRow::active(
        "7f4a6e2f1c23456789abcdef01234567",
        "SRV0000001",
        "cmdb_ci_linux_server",
        ResourceType::Server,
        first_seen,
    );
    store.upsert_record(&ba, "", "").expect("insert BA");
    store.upsert_record(&server, "", "").expect("insert server");

    let relationship = BusinessApplicationServerMembershipRow {
        ba_sys_id: ba.sys_id.clone(),
        server_sys_id: server.sys_id.clone(),
        server_table: server.table_name.clone(),
        provenance: "relationship".to_string(),
        min_depth: 2,
        paths_json: "[]".to_string(),
        discovered_at: first_seen,
        last_seen_at: first_seen,
        tombstoned_at: None,
    };
    store
        .upsert_business_application_server_membership(&relationship)
        .expect("insert relationship membership");

    let mut both = relationship.clone();
    both.provenance = "both".to_string();
    both.min_depth = 1;
    both.discovered_at = later_seen;
    both.last_seen_at = later_seen;
    store
        .upsert_business_application_server_membership(&both)
        .expect("transition to both");

    let active = store
        .list_business_application_server_memberships_for_ba(&ba.sys_id, false)
        .expect("active memberships");
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].provenance, "both");
    assert_eq!(active[0].discovered_at, first_seen);
    assert_eq!(active[0].last_seen_at, later_seen);

    let all = store
        .list_business_application_server_memberships_for_ba(&ba.sys_id, true)
        .expect("all memberships");
    assert_eq!(all.len(), 2);
    let tombstoned = all
        .iter()
        .find(|row| row.provenance == "relationship")
        .expect("relationship history row");
    assert_eq!(tombstoned.tombstoned_at, Some(later_seen));
}

#[test]
fn business_application_server_membership_unique_index_rejects_duplicate_live_pair() {
    let store = Store::open_in_memory().expect("store");
    let now = Utc.timestamp_opt(1_779_840_000, 0).unwrap();
    let ba = RecordRow::active(
        "54a4b61b6fe845000ed852a03f3ee4d0",
        "APM0000001",
        "cmdb_ci_business_app",
        ResourceType::BusinessApplication,
        now,
    );
    let server = RecordRow::active(
        "7f4a6e2f1c23456789abcdef01234567",
        "SRV0000001",
        "cmdb_ci_linux_server",
        ResourceType::Server,
        now,
    );
    store.upsert_record(&ba, "", "").expect("insert BA");
    store.upsert_record(&server, "", "").expect("insert server");
    store
        .conn
        .execute(
            r#"
                INSERT INTO business_application_servers (
                    ba_sys_id, server_sys_id, server_table, provenance, min_depth,
                    paths_json, discovered_at, last_seen_at, tombstoned_at
                ) VALUES (?1, ?2, ?3, 'relationship', 1, '[]', ?4, ?4, NULL)
                "#,
            params![&ba.sys_id, &server.sys_id, &server.table_name, to_ts(now)],
        )
        .expect("insert relationship");
    let err = store
        .conn
        .execute(
            r#"
                INSERT INTO business_application_servers (
                    ba_sys_id, server_sys_id, server_table, provenance, min_depth,
                    paths_json, discovered_at, last_seen_at, tombstoned_at
                ) VALUES (?1, ?2, ?3, 'service_membership', 1, '[]', ?4, ?4, NULL)
                "#,
            params![&ba.sys_id, &server.sys_id, &server.table_name, to_ts(now)],
        )
        .expect_err("duplicate live pair rejected");
    assert!(err.to_string().contains("UNIQUE constraint failed"));
}

#[test]
fn business_application_server_inventory_health_round_trips() {
    let store = Store::open_in_memory().expect("store");
    let started = Utc.timestamp_opt(1_779_840_000, 0).unwrap();
    let completed = Utc.timestamp_opt(1_779_840_030, 0).unwrap();
    let ba = RecordRow::active(
        "54a4b61b6fe845000ed852a03f3ee4d0",
        "APM0000001",
        "cmdb_ci_business_app",
        ResourceType::BusinessApplication,
        started,
    );
    store.upsert_record(&ba, "", "").expect("insert BA");

    let health = BusinessApplicationServerInventoryHealthRow {
        ba_sys_id: ba.sys_id.clone(),
        run_started_at: started,
        run_completed_at: completed,
        service_membership_status: "acl_restricted".to_string(),
        relationship_status: "ok".to_string(),
        inventory_status: "service_membership_degraded".to_string(),
        summary_json: serde_json::json!({
            "service_membership_status": "acl_restricted",
            "acl_restricted_count": 1
        })
        .to_string(),
    };
    store
        .upsert_business_application_server_inventory_health(&health)
        .expect("upsert health");

    assert_eq!(
        store
            .get_business_application_server_inventory_health(&ba.sys_id)
            .expect("get health"),
        Some(health)
    );
}

#[test]
fn cached_users_round_trip_and_list_by_ids() {
    let store = Store::open_in_memory().expect("store");
    let synced_at = Utc.timestamp_opt(1_779_840_000, 0).unwrap();
    let user = CachedUserRow {
        sys_id: "6816f79cc0a8016401c5a33be04be441".to_string(),
        user_name: Some("jowner".to_string()),
        name: Some("Jane Owner".to_string()),
        first_name: Some("Jane".to_string()),
        last_name: Some("Owner".to_string()),
        email: Some("jane.owner@example.test".to_string()),
        employee_number: Some("12345".to_string()),
        active: Some(true),
        department: Some("IT".to_string()),
        location: Some("Dallas".to_string()),
        title: Some("Director".to_string()),
        raw_json: serde_json::json!({
            "sys_id": "6816f79cc0a8016401c5a33be04be441",
            "user_name": "jowner"
        })
        .to_string(),
        synced_at,
        sys_updated_on: Some("2026-05-31 12:34:56".to_string()),
    };

    store.upsert_cached_user(&user).expect("upsert user");

    let loaded = store
        .get_cached_user(&user.sys_id)
        .expect("get user")
        .expect("cached user");
    assert_eq!(loaded, user);

    let listed = store
        .list_cached_users_by_sys_ids(&[
            "missing".to_string(),
            "6816f79cc0a8016401c5a33be04be441".to_string(),
        ])
        .expect("list users");
    assert_eq!(listed, vec![user]);
}

#[test]
fn cached_user_query_results_use_seven_day_ttl() {
    let store = Store::open_in_memory().expect("store");
    let synced_at = Utc.timestamp_opt(1_779_840_000, 0).unwrap();
    let ids = vec![
        "6816f79cc0a8016401c5a33be04be441".to_string(),
        "8a4d2e0ec3577e5433b2b643e4013100".to_string(),
    ];

    let stored = store
        .put_cached_user_query_result("active:name=Jane", &ids, synced_at)
        .expect("put query");
    assert_eq!(stored.expires_at, synced_at + cached_user_query_ttl());

    let fresh = store
        .get_cached_user_query_result("active:name=Jane", synced_at + Duration::days(6))
        .expect("get fresh query")
        .expect("fresh query");
    assert_eq!(fresh.result_sys_ids, ids);

    let expired = store
        .get_cached_user_query_result("active:name=Jane", synced_at + Duration::days(7))
        .expect("get expired query");
    assert!(expired.is_none());
}

#[test]
fn projects_business_application_unknown_fields_and_references() {
    let store = Store::open_in_memory().expect("store");
    let record = business_application_record("54a4b61b6fe845000ed852a03f3ee4d0", "Epic", "150");
    insert_business_application(&store, &record);

    let projection = store
        .get_business_application_projection(&record.sys_id)
        .expect("projection")
        .expect("projection row");
    let fields = store
        .list_business_application_fields(&record.sys_id)
        .expect("fields");

    assert_eq!(projection.name, "Epic");
    assert_eq!(
        projection.business_owner_sys_id.as_deref(),
        Some("6816f79cc0a8016401c5a33be04be441")
    );
    assert_eq!(
        projection.business_owner_name.as_deref(),
        Some("Jane Owner")
    );
    assert!(fields.iter().any(|field| field.field_name == "u_cost"));
    let custom = fields
        .iter()
        .find(|field| field.field_name == "u_region")
        .expect("custom field");
    assert_eq!(custom.value_text.as_deref(), Some("north"));
    assert_eq!(custom.display_value.as_deref(), Some("North"));
}

#[test]
fn local_business_application_query_filters_all_projected_field_shapes() {
    use crate::query::filter::{BusinessApplicationQuery, FieldOperator, SortDirection};

    let store = Store::open_in_memory().expect("store");
    let epic = business_application_record("54a4b61b6fe845000ed852a03f3ee4d0", "Epic", "150");
    let other = business_application_record("74a4b61b6fe845000ed852a03f3ee4d1", "Other", "50");
    insert_business_application(&store, &epic);
    insert_business_application(&store, &other);

    let rows = store
        .query_business_application_records(
            &BusinessApplicationQuery::new()
                .filter("name", FieldOperator::Contains, serde_json::json!("pic"))
                .filter(
                    "business_owner",
                    FieldOperator::Eq,
                    serde_json::json!("6816f79cc0a8016401c5a33be04be441"),
                )
                .filter(
                    "business_owner",
                    FieldOperator::Contains,
                    serde_json::json!("Jane"),
                )
                .filter(
                    "operational_status",
                    FieldOperator::Eq,
                    serde_json::json!("Operational"),
                )
                .filter(
                    "attested_date",
                    FieldOperator::Gte,
                    serde_json::json!("2026-01-01"),
                )
                .filter("u_cost", FieldOperator::Gt, serde_json::json!(100))
                .filter("u_cost", FieldOperator::Lte, serde_json::json!(200))
                .filter("u_cost", FieldOperator::Ne, serde_json::json!(0))
                .filter(
                    "u_code",
                    FieldOperator::StartsWith,
                    serde_json::json!("ABC"),
                )
                .filter(
                    "u_region",
                    FieldOperator::In,
                    serde_json::json!(["South", "North"]),
                )
                .filter("u_empty", FieldOperator::IsEmpty, Value::Null)
                .filter("u_code", FieldOperator::IsNotEmpty, Value::Null)
                .text("owner")
                .sort_by("name", SortDirection::Desc)
                .limit(10),
        )
        .expect("query BA records");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].sys_id, epic.sys_id);
}

#[test]
fn stores_unresolved_primitive_stub() {
    let store = Store::open_in_memory().expect("store");

    store
        .upsert_unresolved_primitive_stub(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "u_unknown_reference",
            "Unknown Owner",
            PrimitiveResolutionStatus::UnknownTable,
            Some("table not in primitive allowlist".to_string()),
        )
        .expect("stub");

    let primitive = store
        .get_primitive_object("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        .expect("primitive")
        .expect("primitive row");
    assert_eq!(primitive.table_name, "u_unknown_reference");
    assert_eq!(
        primitive.resolution_status,
        PrimitiveResolutionStatus::UnknownTable
    );
    assert_eq!(primitive.display_name, "Unknown Owner");
}

#[test]
fn business_application_resource_type_is_canonical_string_with_table_alias() {
    let store = Store::open_in_memory().expect("store");
    let record = RecordRow::active(
        "54a4b61b6fe845000ed852a03f3ee4d0",
        "BA:54a4b61b6fe845000ed852a03f3ee4d0",
        "cmdb_ci_business_app",
        ResourceType::BusinessApplication,
        Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
    );

    store
        .upsert_record(&record, "", "")
        .expect("insert business app");

    let stored_resource_type: String = store
        .conn
        .query_row(
            "SELECT resource_type FROM records WHERE sys_id = ?1",
            ["54a4b61b6fe845000ed852a03f3ee4d0"],
            |row| row.get(0),
        )
        .expect("stored resource type");
    assert_eq!(stored_resource_type, "business_application");

    let loaded = store
        .get_record_by_number_and_type(
            "BA:54a4b61b6fe845000ed852a03f3ee4d0",
            ResourceType::BusinessApplication,
        )
        .expect("query")
        .expect("row");
    assert_eq!(loaded.resource_type, ResourceType::BusinessApplication);
    assert_eq!(
        str_to_resource_type("cmdb_ci_business_app").unwrap(),
        ResourceType::BusinessApplication
    );
}

#[test]
fn allows_duplicate_numbers_across_resource_types() {
    let store = Store::open_in_memory().expect("store");
    let change = RecordRow::active(
        "chg-sys",
        "CHG0012345",
        "change_request",
        ResourceType::Change,
        Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
    );
    let approval = RecordRow::active(
        "apr-sys",
        "CHG0012345",
        "sysapproval_approver",
        ResourceType::Approval,
        Utc.timestamp_opt(1_712_649_601, 0).unwrap(),
    );

    store.upsert_record(&change, "", "").expect("insert change");
    store
        .upsert_record(&approval, "", "")
        .expect("insert approval");

    let primary = store
        .get_primary_record_by_number("CHG0012345")
        .expect("query")
        .expect("row");
    let approval_row = store
        .get_record_by_number_and_type("CHG0012345", ResourceType::Approval)
        .expect("approval query")
        .expect("approval row");

    assert_eq!(primary.sys_id, "chg-sys");
    assert_eq!(approval_row.sys_id, "apr-sys");
    assert_eq!(store.count_active_records().expect("count"), 2);
}

#[test]
fn sync_state_round_trips() {
    let store = Store::open_in_memory().expect("store");
    let row = SyncStateRow {
        resource_type: "incident".to_string(),
        last_full: Some(Utc.timestamp_opt(1_712_649_600, 0).unwrap()),
        last_incr: Some(Utc.timestamp_opt(1_712_649_800, 0).unwrap()),
        high_watermark: Some(Utc.timestamp_opt(1_712_650_000, 0).unwrap()),
        cursor: Some("page-2".to_string()),
        filter_hash: Some("abc123".to_string()),
    };
    store.set_sync_state(&row).expect("sync state");
    let loaded = store
        .get_sync_state("incident")
        .expect("query")
        .expect("row");
    assert_eq!(loaded.cursor.as_deref(), Some("page-2"));
    assert_eq!(loaded.high_watermark.unwrap().timestamp(), 1_712_650_000);
}

#[test]
fn enrichment_rows_round_trip_and_replace() {
    let store = Store::open_in_memory().expect("store");
    let record = RecordRow::active(
        "sys-1",
        "INC0000001",
        "incident",
        ResourceType::Incident,
        Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
    );
    store.upsert_record(&record, "", "").expect("insert");

    store
        .replace_tags(
            "sys-1",
            &[
                TagRow {
                    record_sys_id: "sys-1".to_string(),
                    tag: "network".to_string(),
                    source: "derived".to_string(),
                    weight: 1.0,
                },
                TagRow {
                    record_sys_id: "sys-1".to_string(),
                    tag: "vpn".to_string(),
                    source: "manual".to_string(),
                    weight: 2.0,
                },
            ],
        )
        .expect("replace tags");
    store
        .replace_keywords(
            "sys-1",
            &[KeywordRow {
                record_sys_id: "sys-1".to_string(),
                keyword: "connectivity".to_string(),
                source: "derived".to_string(),
                weight: 1.5,
            }],
        )
        .expect("replace keywords");
    store
        .replace_aliases(
            "sys-1",
            &[AliasRow {
                record_sys_id: "sys-1".to_string(),
                alias: "vpn issue".to_string(),
                kind: "short_title".to_string(),
                source: "derived".to_string(),
            }],
        )
        .expect("replace aliases");

    assert_eq!(store.list_tags("sys-1").expect("list tags").len(), 2);
    assert_eq!(
        store.list_keywords("sys-1").expect("list keywords")[0].keyword,
        "connectivity"
    );
    assert_eq!(
        store.list_aliases("sys-1").expect("list aliases")[0].alias,
        "vpn issue"
    );

    assert_eq!(
        store
            .find_record_sys_ids_by_tag("network", 10)
            .expect("find tag"),
        vec!["sys-1".to_string()]
    );
    assert_eq!(
        store
            .find_record_sys_ids_by_keyword("connectivity", 10)
            .expect("find keyword"),
        vec!["sys-1".to_string()]
    );
    assert_eq!(
        store
            .find_record_sys_ids_by_alias("vpn issue", 10)
            .expect("find alias"),
        vec!["sys-1".to_string()]
    );

    store
        .replace_tags(
            "sys-1",
            &[TagRow {
                record_sys_id: "sys-1".to_string(),
                tag: "incident".to_string(),
                source: "derived".to_string(),
                weight: 1.0,
            }],
        )
        .expect("replace tags again");
    assert_eq!(
        store
            .list_tags("sys-1")
            .expect("list replaced tags")
            .into_iter()
            .map(|row| row.tag)
            .collect::<Vec<_>>(),
        vec!["incident".to_string()]
    );
}

#[test]
fn enrichment_lookups_are_exact_ordered_and_limited() {
    let store = Store::open_in_memory().expect("store");
    let first = RecordRow::active(
        "sys-1",
        "INC0000001",
        "incident",
        ResourceType::Incident,
        Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
    );
    let second = RecordRow::active(
        "sys-2",
        "INC0000002",
        "incident",
        ResourceType::Incident,
        Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
    );
    store.upsert_record(&first, "", "").expect("insert first");
    store.upsert_record(&second, "", "").expect("insert second");

    store
        .replace_tags(
            "sys-2",
            &[TagRow {
                record_sys_id: "sys-2".to_string(),
                tag: "shared".to_string(),
                source: "derived".to_string(),
                weight: 1.0,
            }],
        )
        .expect("insert second tag");
    store
        .replace_tags(
            "sys-1",
            &[TagRow {
                record_sys_id: "sys-1".to_string(),
                tag: "shared".to_string(),
                source: "derived".to_string(),
                weight: 1.0,
            }],
        )
        .expect("insert first tag");

    assert_eq!(
        store
            .find_record_sys_ids_by_tag("shared", 1)
            .expect("limited lookup"),
        vec!["sys-1".to_string()]
    );
    assert_eq!(
        store
            .find_record_sys_ids_by_tag("shared", 10)
            .expect("full lookup"),
        vec!["sys-1".to_string(), "sys-2".to_string()]
    );
    assert_eq!(
        store
            .find_record_sys_ids_by_tag("missing", 10)
            .expect("missing lookup"),
        Vec::<String>::new()
    );

    store
        .replace_keywords(
            "sys-2",
            &[KeywordRow {
                record_sys_id: "sys-2".to_string(),
                keyword: "network".to_string(),
                source: "derived".to_string(),
                weight: 1.0,
            }],
        )
        .expect("insert keyword");
    assert_eq!(
        store
            .find_record_sys_ids_by_keyword("network", 10)
            .expect("keyword lookup"),
        vec!["sys-2".to_string()]
    );

    store
        .replace_aliases(
            "sys-1",
            &[AliasRow {
                record_sys_id: "sys-1".to_string(),
                alias: "net issue".to_string(),
                kind: "short_title".to_string(),
                source: "derived".to_string(),
            }],
        )
        .expect("insert alias");
    assert_eq!(
        store
            .find_record_sys_ids_by_alias("net issue", 10)
            .expect("alias lookup"),
        vec!["sys-1".to_string()]
    );
}

#[test]
fn knowledge_article_projection_round_trips_and_groups() {
    let store = Store::open_in_memory().expect("store");
    let first = RecordRow::active(
        "kb-1",
        "KB001",
        "kb_knowledge",
        ResourceType::Knowledge,
        Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
    );
    let second = RecordRow::active(
        "kb-2",
        "KB002",
        "kb_knowledge",
        ResourceType::Knowledge,
        Utc.timestamp_opt(1_712_649_700, 0).unwrap(),
    );
    store
        .upsert_record(&first, "", "gateway")
        .expect("insert first");
    store
        .upsert_record(&second, "", "access")
        .expect("insert second");

    store
        .upsert_knowledge_article(&KnowledgeArticleRow {
            record_sys_id: "kb-1".to_string(),
            number: "KB001".to_string(),
            title: "VPN Runbook".to_string(),
            workflow_state: "published".to_string(),
            knowledge_base_sys_id: "kb-base".to_string(),
            knowledge_base_name: "IT".to_string(),
            category_sys_id: "kb-cat-1".to_string(),
            category_name: "Network".to_string(),
            author_sys_id: Some("user-1".to_string()),
            author_name: Some("Casey User".to_string()),
            published_at: Some("2026-04-10 09:00:00".to_string()),
            valid_to: Some("2027-01-01".to_string()),
            article_type: "text".to_string(),
            sys_updated_on: Some("2026-04-10 09:00:00".to_string()),
            sn_tags: vec!["vpn".to_string()],
            auto_tags: vec!["gateway".to_string()],
            user_tags: vec!["runbook".to_string()],
            body_cached: true,
        })
        .expect("upsert first article");
    store
        .upsert_knowledge_article(&KnowledgeArticleRow {
            record_sys_id: "kb-2".to_string(),
            number: "KB002".to_string(),
            title: "Windows Access".to_string(),
            workflow_state: "published".to_string(),
            knowledge_base_sys_id: "kb-base".to_string(),
            knowledge_base_name: "IT".to_string(),
            category_sys_id: "kb-cat-2".to_string(),
            category_name: "Access".to_string(),
            author_sys_id: None,
            author_name: None,
            published_at: None,
            valid_to: None,
            article_type: "text".to_string(),
            sys_updated_on: None,
            sn_tags: Vec::new(),
            auto_tags: Vec::new(),
            user_tags: Vec::new(),
            body_cached: false,
        })
        .expect("upsert second article");

    let loaded = store
        .get_knowledge_article("kb-1")
        .expect("query")
        .expect("row");
    assert_eq!(loaded.title, "VPN Runbook");
    assert_eq!(loaded.author_name.as_deref(), Some("Casey User"));

    let bases = store.list_knowledge_bases().expect("bases");
    assert_eq!(
        bases,
        vec![KnowledgeBaseSummaryRow {
            knowledge_base_sys_id: "kb-base".to_string(),
            knowledge_base_name: "IT".to_string(),
            article_count: 2,
        }]
    );

    let categories = store
        .list_knowledge_categories("kb-base")
        .expect("categories");
    assert_eq!(categories.len(), 2);
    assert_eq!(categories[0].category_name, "Access");
    assert_eq!(categories[0].article_count, 1);
    assert_eq!(categories[1].category_name, "Network");
    assert_eq!(categories[1].article_count, 1);
}

#[test]
fn fts_search_matches_tag_tokens() {
    let store = Store::open_in_memory().expect("store");
    let row = RecordRow::active(
        "kb-1",
        "KB001",
        "kb_knowledge",
        ResourceType::Knowledge,
        Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
    );
    store
        .upsert_record_with_tags(&row, "", "Gateway verification", "vpn runbook")
        .expect("insert tagged kb row");

    assert_eq!(
        store.search_fts("vpn", 10).expect("search vpn"),
        vec!["KB001".to_string()]
    );
    assert_eq!(
        store.search_fts("runbook", 10).expect("search runbook"),
        vec!["KB001".to_string()]
    );
}

#[test]
fn knowledge_embedding_round_trips_and_counts_by_coverage() {
    let store = Store::open_in_memory().expect("store");
    let record = RecordRow::active(
        "kb-1",
        "KB001",
        "kb_knowledge",
        ResourceType::Knowledge,
        Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
    );
    store.upsert_record(&record, "", "").expect("insert record");
    store
        .upsert_knowledge_article(&KnowledgeArticleRow {
            record_sys_id: "kb-1".to_string(),
            number: "KB001".to_string(),
            title: "VPN Runbook".to_string(),
            workflow_state: "published".to_string(),
            knowledge_base_sys_id: "kb-base".to_string(),
            knowledge_base_name: "IT".to_string(),
            category_sys_id: "kb-cat".to_string(),
            category_name: "Network".to_string(),
            author_sys_id: None,
            author_name: None,
            published_at: None,
            valid_to: None,
            article_type: "text".to_string(),
            sys_updated_on: None,
            sn_tags: Vec::new(),
            auto_tags: Vec::new(),
            user_tags: Vec::new(),
            body_cached: true,
        })
        .expect("insert knowledge row");

    let vector = crate::semantic::normalize_unit_vector(&[3.0, 4.0]).expect("vector");
    store
        .upsert_knowledge_embedding(&KnowledgeEmbeddingRow {
            record_sys_id: "kb-1".to_string(),
            model: "stub".to_string(),
            provider: "stub".to_string(),
            dimensions: vector.len(),
            coverage: KnowledgeEmbeddingCoverage::FullText,
            content_hash: "abc".to_string(),
            vector: vector.clone(),
            updated_at: Utc.timestamp_opt(1_712_649_700, 0).unwrap(),
        })
        .expect("insert embedding");

    let loaded = store
        .get_knowledge_embedding("kb-1")
        .expect("get embedding")
        .expect("embedding row");
    assert_eq!(loaded.dimensions, 2);
    assert_eq!(loaded.coverage, KnowledgeEmbeddingCoverage::FullText);
    assert_eq!(loaded.vector, vector);
    assert_eq!(
        store
            .count_knowledge_embeddings_by_coverage("stub", KnowledgeEmbeddingCoverage::FullText,)
            .expect("coverage count"),
        1
    );
}

#[test]
fn knowledge_embedding_decode_rejects_dimension_mismatch() {
    let err = decode_embedding_vector(&[0_u8; 7], 2).expect_err("dimension mismatch");
    assert!(matches!(
        err,
        StoreError::InvalidEmbeddingVectorLength {
            expected: 8,
            actual: 7
        }
    ));
}

#[test]
fn kb_article_term_writes_dedupe_duplicate_record_ids() {
    let store = Store::open_in_memory().expect("store");

    store
        .replace_all_kb_article_terms(&[
            ("kb-1".to_string(), vec!["vpn".to_string()]),
            ("kb-1".to_string(), vec!["runbook".to_string()]),
        ])
        .expect("replace all with duplicates");
    assert_eq!(
        store
            .get_kb_article_terms("kb-1")
            .expect("terms after replace all"),
        vec!["runbook".to_string()]
    );

    store
        .replace_kb_article_terms_entries(&[
            ("kb-1".to_string(), vec!["network".to_string()]),
            ("kb-1".to_string(), vec!["gateway".to_string()]),
        ])
        .expect("replace entries with duplicates");
    assert_eq!(
        store
            .get_kb_article_terms("kb-1")
            .expect("terms after replace entries"),
        vec!["gateway".to_string()]
    );
}

#[test]
fn tombstone_and_prune_flow_updates_state() {
    let store = Store::open_in_memory().expect("store");
    let record = RecordRow::active(
        "sys-1",
        "INC0000001",
        "incident",
        ResourceType::Incident,
        Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
    );
    store.upsert_record(&record, "", "").expect("insert");
    store
        .replace_tags(
            "sys-1",
            &[TagRow {
                record_sys_id: "sys-1".to_string(),
                tag: "vpn".to_string(),
                source: "derived".to_string(),
                weight: 1.0,
            }],
        )
        .expect("insert tag");
    store
        .replace_keywords(
            "sys-1",
            &[KeywordRow {
                record_sys_id: "sys-1".to_string(),
                keyword: "network".to_string(),
                source: "derived".to_string(),
                weight: 1.0,
            }],
        )
        .expect("insert keyword");
    store
        .replace_aliases(
            "sys-1",
            &[AliasRow {
                record_sys_id: "sys-1".to_string(),
                alias: "vpn ticket".to_string(),
                kind: "short_title".to_string(),
                source: "derived".to_string(),
            }],
        )
        .expect("insert alias");
    store
        .upsert_knowledge_article(&KnowledgeArticleRow {
            record_sys_id: "sys-1".to_string(),
            number: "KB001".to_string(),
            title: "VPN Runbook".to_string(),
            workflow_state: "published".to_string(),
            knowledge_base_sys_id: "kb-base".to_string(),
            knowledge_base_name: "IT".to_string(),
            category_sys_id: "kb-cat".to_string(),
            category_name: "Network".to_string(),
            author_sys_id: None,
            author_name: None,
            published_at: None,
            valid_to: None,
            article_type: "text".to_string(),
            sys_updated_on: None,
            sn_tags: Vec::new(),
            auto_tags: Vec::new(),
            user_tags: Vec::new(),
            body_cached: false,
        })
        .expect("insert knowledge row");
    let tombstoned = record
        .clone()
        .tombstone(Utc.timestamp_opt(1_712_650_100, 0).unwrap());
    store
        .tombstone_record(&tombstoned.sys_id, tombstoned.tombstoned_at.unwrap())
        .expect("tombstone");
    assert_eq!(
        store
            .get_record_by_sys_id("sys-1")
            .unwrap()
            .unwrap()
            .lifecycle(),
        RecordLifecycle::Tombstoned
    );
    store
        .prune_record("sys-1", Utc.timestamp_opt(1_712_650_200, 0).unwrap())
        .expect("prune");
    assert!(store.get_record_by_sys_id("sys-1").unwrap().is_none());
    assert!(store.list_tags("sys-1").unwrap().is_empty());
    assert!(store.list_keywords("sys-1").unwrap().is_empty());
    assert!(store.list_aliases("sys-1").unwrap().is_empty());
    assert!(store.get_knowledge_article("sys-1").unwrap().is_none());
    assert!(
        store
            .find_record_sys_ids_by_tag("vpn", 10)
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .find_record_sys_ids_by_keyword("network", 10)
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .find_record_sys_ids_by_alias("vpn ticket", 10)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn test_upsert_records_batch() {
    let store = Store::open(":memory:").unwrap();
    let now = Utc::now();
    let rows: Vec<_> = (0..5)
        .map(|i| {
            RecordRow::active(
                format!("sys-{i}"),
                format!("INC{i:04}"),
                "incident",
                ResourceType::Incident,
                now,
            )
        })
        .collect();

    let entries: Vec<(&RecordRow, &str, &str, &str)> =
        rows.iter().map(|row| (row, "", "", "")).collect();

    store.upsert_records(&entries).unwrap();

    let active = store
        .list_active_records(Some(ResourceType::Incident))
        .unwrap();
    assert_eq!(active.len(), 5);
}

#[test]
fn test_list_active_records_paginated() {
    let store = Store::open_in_memory().expect("store");
    let now = Utc::now();
    for i in 0..10 {
        let row = RecordRow::active(
            format!("sys-{i}"),
            format!("INC{i:04}"),
            "incident",
            ResourceType::Incident,
            now,
        );
        store.upsert_record(&row, "", "").unwrap();
    }

    let page1 = store
        .list_active_records_paginated(Some(ResourceType::Incident), 5, 0)
        .unwrap();
    assert_eq!(page1.len(), 5);

    let page2 = store
        .list_active_records_paginated(Some(ResourceType::Incident), 5, 5)
        .unwrap();
    assert_eq!(page2.len(), 5);

    assert_ne!(page1[0].sys_id, page2[0].sys_id);
}

#[test]
fn test_cleanup_orphaned_enrichments() {
    let store = Store::open_in_memory().unwrap();
    let now = Utc::now();

    let row = RecordRow::active("sys-1", "INC0001", "incident", ResourceType::Incident, now);
    store.upsert_record(&row, "", "").unwrap();

    store
        .replace_tags(
            "sys-1",
            &[TagRow {
                record_sys_id: "sys-1".to_string(),
                tag: "vpn".to_string(),
                source: "derived".to_string(),
                weight: 1.0,
            }],
        )
        .unwrap();
    store
        .replace_keywords(
            "sys-1",
            &[KeywordRow {
                record_sys_id: "sys-1".to_string(),
                keyword: "network".to_string(),
                source: "derived".to_string(),
                weight: 1.0,
            }],
        )
        .unwrap();

    // Tombstone the record (sets in_scope = 0)
    store.tombstone_record("sys-1", now).unwrap();

    // Cleanup should remove orphaned enrichments for tombstoned records
    let removed = store.cleanup_orphaned_enrichments().unwrap();
    assert!(removed > 0, "expected orphaned enrichments to be removed");
    assert!(store.list_tags("sys-1").unwrap().is_empty());
    assert!(store.list_keywords("sys-1").unwrap().is_empty());
}

#[test]
fn test_cleanup_orphaned_relationships() {
    let store = Store::open_in_memory().unwrap();
    let now = Utc::now();

    let row = RecordRow::active("sys-1", "INC0001", "incident", ResourceType::Incident, now);
    store.upsert_record(&row, "", "").unwrap();

    store
        .upsert_relationship(&RelationshipRow {
            source_id: "sys-1".to_string(),
            target_id: "ref-999".to_string(),
            rel_type: "reference".to_string(),
            field_name: "assigned_to".to_string(),
        })
        .unwrap();

    // Tombstone the record so in_scope = 0
    store.tombstone_record("sys-1", now).unwrap();

    let removed = store.cleanup_orphaned_relationships().unwrap();
    assert_eq!(removed, 1);
}

#[test]
fn test_cleanup_orphaned_references() {
    let store = Store::open_in_memory().unwrap();
    let now = Utc::now();

    // Insert a reference that has no corresponding relationship entry
    store
        .upsert_reference(&ReferenceRow {
            sys_id: "ref-orphan".to_string(),
            table_name: "sys_user".to_string(),
            display_name: "Orphan User".to_string(),
            extra_json: "{}".to_string(),
            synced_at: now,
            expires_at: None,
        })
        .unwrap();

    // cleanup_orphaned_references removes references not in any relationship's target_id
    let removed = store.cleanup_orphaned_references().unwrap();
    assert_eq!(removed, 1);
}
