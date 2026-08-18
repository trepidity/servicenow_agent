use super::*;
use crate::ResourceType;
use crate::resource::business_application::{
    BUSINESS_APPLICATION_DEGRADED_REASON_CMDB_RELATIONSHIPS_UNMAPPED,
    BUSINESS_APPLICATION_SERVERS_DEFAULT_MAX_SERVICE_MEMBERSHIP_ASSOCIATIONS,
    BUSINESS_APPLICATION_SERVERS_DEFAULT_MAX_SERVICE_MEMBERSHIP_PAGES,
    BusinessApplicationServersCachedParams, BusinessApplicationsForServerParams,
    RelationshipKnowledgeStatus,
};
use crate::tests::{core_for_mock_server, mock_server_test_lock};
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn business_application_search_builds_supported_filters() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_ci_business_app"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [{
                "sys_id": "54a4b61b6fe845000ed852a03f3ee4d0",
                "name": "Epic",
                "short_description": "Epic",
                "sys_class_name": "cmdb_ci_business_app",
                "business_owner": { "value": "owner-sys", "display_value": "Jane Owner" },
                "it_application_owner": { "value": "is-owner-sys", "display_value": "Alex IS" },
                "managed_by_group": { "value": "ci-group-sys", "display_value": "CI Owners" },
                "support_group": { "value": "support-group-sys", "display_value": "App Support" },
                "operational_status": { "value": "1", "display_value": "Operational" },
                "portfolio": { "value": "portfolio-sys", "display_value": "Clinical" },
                "attested_date": "2026-05-01",
                "u_custom_field": { "value": "raw-custom", "display_value": "Custom Display" },
                "u_json_blob": { "value": { "kept": true }, "display_value": "Kept" }
            }]
        })))
        .mount(&server)
        .await;

    let (core, tempdir) = core_for_mock_server(&server).await;
    let records = core
        .search_business_applications(BusinessApplicationSearchParams {
            name: Some("Epic".to_string()),
            business_owner: Some("Jane Owner".to_string()),
            is_owner: Some("Alex IS".to_string()),
            ci_owner_group: Some("CI Owners".to_string()),
            primary_support_group: Some("App Support".to_string()),
            operational_state_not: Some("non-operational".to_string()),
            primary_portfolio: Some("Clinical".to_string()),
            attested_date: Some("2026-05-01".to_string()),
            limit: Some(2),
            ..Default::default()
        })
        .await
        .expect("business applications");

    assert_eq!(records.len(), 1);
    assert_eq!(records[0].number, "BA:54a4b61b6fe845000ed852a03f3ee4d0");
    assert_eq!(records[0].table, "cmdb_ci_business_app");
    assert_eq!(records[0].resource_type, ResourceType::BusinessApplication);
    assert_eq!(records[0].fields["name"].value, "Epic");
    assert_eq!(records[0].fields["u_custom_field"].value, "raw-custom");
    assert_eq!(records[0].fields["u_json_blob"].value, "{\"kept\":true}");
    assert!(
            tempdir
                .path()
                .join("vault/business_applications/business_application_54a4b61b6fe845000ed852a03f3ee4d0_epic.md")
                .exists()
        );

    let requests = server.received_requests().await.expect("requests");
    let request = requests
        .iter()
        .find(|request| request.url.path() == "/api/now/table/cmdb_ci_business_app")
        .expect("business app request");
    let query = request
        .url
        .query_pairs()
        .collect::<std::collections::HashMap<_, _>>();
    assert_eq!(
        query.get("sysparm_query").map(|value| value.as_ref()),
        Some(
            "sys_class_name=cmdb_ci_business_app^nameLIKEEpic^business_owner.nameLIKEJane Owner^it_application_owner.nameLIKEAlex IS^managed_by_group.nameLIKECI Owners^support_group.nameLIKEApp Support^portfolio.nameLIKEClinical^operational_status!=2^attested_date=2026-05-01^ORDERBYname"
        )
    );
    assert_eq!(
        query.get("sysparm_fields").map(|value| value.as_ref()),
        None
    );
    assert_eq!(
        query
            .get("sysparm_display_value")
            .map(|value| value.as_ref()),
        Some("all")
    );
    assert_eq!(
        query
            .get("sysparm_exclude_reference_link")
            .map(|value| value.as_ref()),
        Some("true")
    );
    assert_eq!(
        query.get("sysparm_limit").map(|value| value.as_ref()),
        Some("2")
    );
}

#[tokio::test]
async fn business_application_search_persists_resolved_reference_primitives() {
    let server = MockServer::start().await;
    let owner_sys_id = "6816f79cc0a8016401c5a33be04be441";
    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_ci_business_app"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [{
                "sys_id": "54a4b61b6fe845000ed852a03f3ee4d0",
                "name": "Epic",
                "sys_class_name": "cmdb_ci_business_app",
                "business_owner": {
                    "value": owner_sys_id,
                    "display_value": "Jane Owner"
                },
                "operational_status": { "value": "1", "display_value": "Operational" }
            }]
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/api/now/table/sys_user/{owner_sys_id}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": {
                "sys_id": owner_sys_id,
                "name": "Jane Owner",
                "user_name": "jowner",
                "email": "jane.owner@example.invalid",
                "active": { "value": "true", "display_value": "true" },
                "sys_updated_on": "2026-05-30 12:00:00"
            }
        })))
        .mount(&server)
        .await;

    let (core, tempdir) = core_for_mock_server(&server).await;
    let applications = core
        .search_business_applications_live(
            BusinessApplicationSearchParams {
                name: Some("Epic".to_string()),
                limit: Some(1),
                ..Default::default()
            },
            BusinessApplicationHydrationOptions::default(),
        )
        .await
        .expect("business applications");

    assert_eq!(applications.len(), 1);
    assert!(
        applications[0]
            .unresolved_references
            .iter()
            .all(|diagnostic| { diagnostic.reference_sys_id != owner_sys_id })
    );
    let primitive = core
        .ctx
        .query
        .store()
        .get_primitive_object(owner_sys_id)
        .expect("primitive lookup")
        .expect("primitive object");
    assert_eq!(primitive.table_name, "sys_user");
    assert_eq!(primitive.resource_type, "user_primitive");
    assert_eq!(primitive.display_name, "Jane Owner");
    assert_eq!(
        primitive.resolution_status,
        PrimitiveResolutionStatus::Resolved
    );
    let file_path = primitive.file_path.expect("primitive vault path");
    assert!(file_path.starts_with("users/user_6816f79cc0a8016401c5a33be04be441_jane-owner.md"));
    assert!(tempdir.path().join("vault").join(file_path).exists());
}

#[tokio::test]
async fn business_application_fresh_get_omits_sysparm_fields() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(
            "/api/now/table/cmdb_ci_business_app/54a4b61b6fe845000ed852a03f3ee4d0",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": {
                "sys_id": "54a4b61b6fe845000ed852a03f3ee4d0",
                "name": "Epic",
                "sys_class_name": "cmdb_ci_business_app",
                "operational_status": { "value": "1", "display_value": "Operational" },
                "u_observed": "yes"
            }
        })))
        .mount(&server)
        .await;

    let (core, _tempdir) = core_for_mock_server(&server).await;
    let business_application = core
        .get_business_application_fresh(
            BusinessApplicationLookup::sys_id("54A4B61B6FE845000ED852A03F3EE4D0").unwrap(),
            BusinessApplicationHydrationOptions::default(),
        )
        .await
        .expect("fresh get")
        .expect("business application");

    assert_eq!(business_application.name, "Epic");
    assert_eq!(
        business_application.record.number,
        "BA:54a4b61b6fe845000ed852a03f3ee4d0"
    );
    assert_eq!(
        business_application.fields["u_observed"].value,
        serde_json::json!("yes")
    );

    let requests = server.received_requests().await.expect("requests");
    let request = requests
        .iter()
        .find(|request| {
            request.url.path()
                == "/api/now/table/cmdb_ci_business_app/54a4b61b6fe845000ed852a03f3ee4d0"
        })
        .expect("business app get request");
    let query = request
        .url
        .query_pairs()
        .collect::<std::collections::HashMap<_, _>>();
    assert_eq!(query.get("sysparm_fields"), None);
    assert_eq!(
        query
            .get("sysparm_display_value")
            .map(|value| value.as_ref()),
        Some("all")
    );
    assert_eq!(
        query
            .get("sysparm_exclude_reference_link")
            .map(|value| value.as_ref()),
        Some("true")
    );
}

#[test]
fn business_application_servers_params_validate_selector_and_bounds() {
    let options = BusinessApplicationServersParams {
        number: Some("apm0000001".to_string()),
        ..Default::default()
    }
    .validate()
    .expect("valid number selector");
    assert_eq!(
        options.selector,
        BusinessApplicationServersSelector::Number("APM0000001".to_string())
    );
    assert_eq!(options.max_depth, 2);
    assert_eq!(options.max_cis, 500);
    assert_eq!(options.max_edges, 2000);

    let err = BusinessApplicationServersParams::default()
        .validate()
        .expect_err("missing selector should fail")
        .to_string();
    assert!(err.contains("exactly one"));

    let err = BusinessApplicationServersParams {
        number: Some("APM0000001".to_string()),
        sys_id: Some("11111111111111111111111111111111".to_string()),
        ..Default::default()
    }
    .validate()
    .expect_err("dual selector should fail")
    .to_string();
    assert!(err.contains("exactly one"));

    let err = BusinessApplicationServersParams {
        number: Some("BA:11111111111111111111111111111111".to_string()),
        ..Default::default()
    }
    .validate()
    .expect_err("synthetic BA number should fail")
    .to_string();
    assert!(err.contains("BA:<sys_id>"));

    let err = BusinessApplicationServersParams {
        number: Some("APP0000001".to_string()),
        ..Default::default()
    }
    .validate()
    .expect_err("non-APM number should fail")
    .to_string();
    assert!(err.contains("<APM_NUMBER>"));

    let err = BusinessApplicationServersParams {
        sys_id: Some("11111111111111111111111111111111".to_string()),
        max_depth: Some(5),
        ..Default::default()
    }
    .validate()
    .expect_err("max depth should be bounded")
    .to_string();
    assert!(err.contains("at most 4"));
}

#[test]
fn business_application_server_path_chains_reject_mixed_directions() {
    let root = "11111111111111111111111111111111";
    let middle = "22222222222222222222222222222222";
    let leaf = "33333333333333333333333333333333";
    let rel_type = BusinessApplicationRelationshipType {
        value: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
        display_value: Some("Depends on::Used by".to_string()),
    };
    let mut paths_by_ci: HashMap<String, Vec<Vec<BusinessApplicationServerPathEdge>>> =
        HashMap::from([(root.to_string(), vec![Vec::new()])]);

    assert!(extend_path_chains(
        &mut paths_by_ci,
        root,
        middle,
        BusinessApplicationServerPathEdge {
            depth: 1,
            parent_sys_id: root.to_string(),
            child_sys_id: middle.to_string(),
            direction: BusinessApplicationRelationshipDirection::ParentToChild,
            relationship_type: rel_type.clone(),
            edge_source: BusinessApplicationServerPathEdgeSource::Relationship,
        },
    ));

    assert!(!extend_path_chains(
        &mut paths_by_ci,
        middle,
        leaf,
        BusinessApplicationServerPathEdge {
            depth: 2,
            parent_sys_id: leaf.to_string(),
            child_sys_id: middle.to_string(),
            direction: BusinessApplicationRelationshipDirection::ChildToParent,
            relationship_type: rel_type,
            edge_source: BusinessApplicationServerPathEdgeSource::Relationship,
        },
    ));
    assert!(!paths_by_ci.contains_key(leaf));
}

#[tokio::test]
async fn business_application_servers_batches_bfs_levels_and_hydrates_servers() {
    let _guard = mock_server_test_lock().await;
    let server = MockServer::start().await;
    let app = "11111111111111111111111111111111";
    let service = "22222222222222222222222222222222";
    let linux = "33333333333333333333333333333333";
    let windows = "44444444444444444444444444444444";
    let component = "55555555555555555555555555555555";
    let rel_type = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let rel_row_1 = "99999999999999999999999999999991";
    let rel_row_2 = "99999999999999999999999999999992";
    let rel_row_3 = "99999999999999999999999999999993";
    let rel_row_4 = "99999999999999999999999999999994";

    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_ci_business_app"))
        .and(query_param(
            "sysparm_query",
            "sys_class_name=cmdb_ci_business_app^number=APM0000001",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [{
                "sys_id": app,
                "number": "APM0000001",
                "name": "Application Alpha",
                "sys_class_name": "cmdb_ci_business_app",
                "operational_status": { "value": "1", "display_value": "Operational" }
            }]
        })))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("parentIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    relationship_row(rel_row_1, app, service, rel_type, "cmdb_ci_business_app", "cmdb_ci_service"),
                    relationship_row(rel_row_2, app, component, rel_type, "cmdb_ci_business_app", "cmdb_ci_appl"),
                    relationship_row(rel_row_3, app, linux, rel_type, "cmdb_ci_business_app", "cmdb_ci_linux_server")
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_rel_ci"))
        .and(query_param("sysparm_query", format!("childIN{app}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": []
        })))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_ci_server"))
        .and(query_param("sysparm_query", format!("sys_idIN{linux}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [server_row(linux, "linux-alpha.example.com", "cmdb_ci_linux_server")]
        })))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param(
                "sysparm_query",
                format!("parentIN{service},{component}"),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    relationship_row(rel_row_4, component, windows, rel_type, "cmdb_ci_appl", "cmdb_ci_win_server")
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_rel_ci"))
        .and(query_param(
            "sysparm_query",
            format!("childIN{service},{component}"),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": []
        })))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_ci_server"))
        .and(query_param("sysparm_query", format!("sys_idIN{windows}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [server_row(windows, "windows-alpha.example.com", "cmdb_ci_win_server")]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let (core, _tempdir) = core_for_mock_server(&server).await;
    let result = core
        .business_application_servers(BusinessApplicationServersParams {
            number: Some("APM0000001".to_string()),
            include_paths: true,
            persist: Some(true),
            ..Default::default()
        })
        .await
        .expect("business application servers")
        .expect("business application present");

    assert_eq!(result.business_application.sys_id, app);
    assert_eq!(result.servers.len(), 2);
    assert_eq!(result.relationship_summary.relationships_examined, 4);
    assert_eq!(result.relationship_summary.cis_examined, 5);
    assert_eq!(result.relationship_summary.servers_found, 2);
    assert_eq!(result.relationship_summary.persisted_servers, 2);
    assert_eq!(result.relationship_summary.membership_upserts, 2);
    assert_eq!(result.relationship_summary.membership_pruned, 0);
    assert!(!result.relationship_summary.truncated);
    assert_eq!(result.server_paths[linux][0].edges.len(), 1);
    assert_eq!(result.server_paths[windows][0].edges.len(), 2);
    assert_eq!(
        result.server_paths[linux][0].edges[0].direction,
        BusinessApplicationRelationshipDirection::ParentToChild
    );

    let serialized = serde_json::to_string(&result).expect("serialize result");
    assert!(!serialized.contains(rel_row_1));
    assert!(!serialized.contains(rel_row_2));
    assert!(!serialized.contains(rel_row_3));
    assert!(!serialized.contains(rel_row_4));

    let cached_forward = core
        .business_application_servers_cached(BusinessApplicationServersCachedParams {
            sys_id: Some(app.to_string()),
            ..Default::default()
        })
        .await
        .expect("cached forward lookup")
        .expect("business application cached");
    assert_eq!(cached_forward.business_application.sys_id, app);
    assert_eq!(cached_forward.servers.len(), 2);
    assert_eq!(cached_forward.servers[0].server.sys_id, linux);
    assert_eq!(cached_forward.servers[0].provenance, "relationship");
    assert_eq!(cached_forward.servers[0].min_depth, 1);
    assert_eq!(cached_forward.servers[0].paths[0].depth(), 1);
    assert_eq!(cached_forward.servers[1].server.sys_id, windows);
    assert_eq!(cached_forward.servers[1].min_depth, 2);
    assert_eq!(cached_forward.servers[1].paths[0].depth(), 2);
    assert_eq!(
        cached_forward.relationship_status,
        RelationshipKnowledgeStatus::KnownRelationships
    );
    let forward_health = cached_forward
        .inventory_health
        .as_ref()
        .expect("forward inventory health");
    assert_eq!(forward_health.ba_sys_id, app);
    assert_eq!(forward_health.service_membership_status, "not_attempted");
    assert_eq!(forward_health.relationship_status, "ok");
    assert_eq!(forward_health.inventory_status, "complete");

    let cached_reverse = core
        .business_applications_for_server(BusinessApplicationsForServerParams {
            name: Some("linux-alpha.example.com".to_string()),
            ..Default::default()
        })
        .await
        .expect("cached reverse lookup")
        .expect("server cached");
    assert_eq!(cached_reverse.servers.len(), 1);
    assert_eq!(cached_reverse.servers[0].server.sys_id, linux);
    assert_eq!(cached_reverse.servers[0].business_applications.len(), 1);
    assert_eq!(
        cached_reverse.servers[0].business_applications[0]
            .business_application
            .sys_id,
        app
    );
    assert_eq!(
        cached_reverse.servers[0].relationship_status,
        RelationshipKnowledgeStatus::KnownRelationships
    );
    assert_eq!(
        cached_reverse.servers[0].business_applications[0]
            .inventory_health
            .as_ref()
            .expect("reverse inventory health")
            .inventory_status,
        "complete"
    );

    let requests = server.received_requests().await.expect("requests");
    let relationship_queries = requests
        .iter()
        .filter(|request| request.url.path() == "/api/now/table/cmdb_rel_ci")
        .filter_map(|request| {
            request
                .url
                .query_pairs()
                .find(|(key, _)| key == "sysparm_query")
                .map(|(_, value)| value.to_string())
        })
        .collect::<Vec<_>>();
    assert_eq!(
        relationship_queries,
        vec![
            format!("parentIN{app}"),
            format!("childIN{app}"),
            format!("parentIN{service},{component}"),
            format!("childIN{service},{component}"),
        ]
    );
}

/// Fix #3 regression guard: in a diamond topology (`app -> A -> S` and
/// `app -> B -> S`) the server `S` is reachable via two distinct parents.
/// With `include_paths`, BOTH routes must be recorded (the plural
/// `server_paths` Vec holds two entries), while `S` remains a single server
/// result. The second discovery is an alternate forward path, not a cycle.
#[tokio::test]
async fn business_application_servers_records_diamond_alternate_paths() {
    let _guard = mock_server_test_lock().await;
    let server = MockServer::start().await;
    let app = "11111111111111111111111111111111";
    let branch_a = "22222222222222222222222222222222";
    let branch_b = "33333333333333333333333333333333";
    let leaf = "44444444444444444444444444444444";
    let rel_type = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_ci_business_app"))
        .and(query_param(
            "sysparm_query",
            "sys_class_name=cmdb_ci_business_app^number=APM0000001",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [{
                "sys_id": app,
                "number": "APM0000001",
                "name": "Application Alpha",
                "sys_class_name": "cmdb_ci_business_app"
            }]
        })))
        .expect(1)
        .mount(&server)
        .await;

    // Depth 1: app -> A and app -> B (two intermediate application CIs).
    Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("parentIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    relationship_row("99999999999999999999999999999991", app, branch_a, rel_type, "cmdb_ci_business_app", "cmdb_ci_appl"),
                    relationship_row("99999999999999999999999999999992", app, branch_b, rel_type, "cmdb_ci_business_app", "cmdb_ci_appl")
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_rel_ci"))
        .and(query_param("sysparm_query", format!("childIN{app}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": []
        })))
        .expect(1)
        .mount(&server)
        .await;

    // Depth 2: both A and B point at the SAME leaf server S.
    Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param(
                "sysparm_query",
                format!("parentIN{branch_a},{branch_b}"),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    relationship_row("99999999999999999999999999999993", branch_a, leaf, rel_type, "cmdb_ci_appl", "cmdb_ci_linux_server"),
                    relationship_row("99999999999999999999999999999994", branch_b, leaf, rel_type, "cmdb_ci_appl", "cmdb_ci_linux_server")
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_rel_ci"))
        .and(query_param(
            "sysparm_query",
            format!("childIN{branch_a},{branch_b}"),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": []
        })))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_ci_server"))
        .and(query_param("sysparm_query", format!("sys_idIN{leaf}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [server_row(leaf, "linux-leaf.example.com", "cmdb_ci_linux_server")]
        })))
        .mount(&server)
        .await;

    let (core, _tempdir) = core_for_mock_server(&server).await;
    let result = core
        .business_application_servers(BusinessApplicationServersParams {
            number: Some("APM0000001".to_string()),
            include_paths: true,
            ..Default::default()
        })
        .await
        .expect("business application servers")
        .expect("business application present");

    // One server, two distinct routes to it.
    assert_eq!(result.servers.len(), 1);
    assert_eq!(result.servers[0].record.sys_id, leaf);
    let paths = result
        .server_paths
        .get(leaf)
        .expect("leaf should have recorded paths");
    assert_eq!(paths.len(), 2, "both diamond routes must be recorded");
    // Each route is two edges long: app->branch then branch->leaf.
    assert!(paths.iter().all(|route| route.edges.len() == 2));
    // The two routes go through different branch CIs.
    let mut branches = paths
        .iter()
        .map(|route| route.edges[0].to_sys_id().to_string())
        .collect::<Vec<_>>();
    branches.sort();
    assert_eq!(branches, vec![branch_a.to_string(), branch_b.to_string()]);
}

/// Task #10: the path-edge traversal endpoints are no longer stored — they are
/// derived from `parent_sys_id`/`child_sys_id`/`direction`. This pins that the
/// derivation matches the old stored semantics for BOTH crossing directions,
/// and that a path's depth equals its edge count.
#[test]
fn business_application_path_edge_derives_endpoints_from_direction() {
    let parent = "pppppppppppppppppppppppppppppppp";
    let child = "cccccccccccccccccccccccccccccccc";
    let rel = BusinessApplicationRelationshipEdge {
        parent_sys_id: parent.to_string(),
        child_sys_id: child.to_string(),
        parent_class: None,
        child_class: None,
        relationship_type: BusinessApplicationRelationshipType {
            value: "dep".to_string(),
            display_value: None,
        },
    };

    // Parent→child crossing: traversal entered at the parent, exited at child.
    let down = rel.path_edge(1, BusinessApplicationRelationshipDirection::ParentToChild);
    assert_eq!(down.from_sys_id(), parent);
    assert_eq!(down.to_sys_id(), child);

    // Child→parent crossing: the endpoints flip.
    let up = rel.path_edge(2, BusinessApplicationRelationshipDirection::ChildToParent);
    assert_eq!(up.from_sys_id(), child);
    assert_eq!(up.to_sys_id(), parent);

    // A path's depth/len is exactly its edge count.
    let path = BusinessApplicationServerPath {
        edges: vec![down, up],
    };
    assert_eq!(path.depth(), 2);
    assert_eq!(path.len(), 2);
    assert!(!path.is_empty());
    assert!(BusinessApplicationServerPath { edges: vec![] }.is_empty());
}

/// Fix #5: the paginated edge read must surface truncation when the
/// `max_edges` budget is consumed while more edge pages remain. Here
/// `max_edges = 2` and the first (and only fetched) page returns exactly two
/// edges, filling the page; with `no_count()` the paginator cannot prove it
/// is done, so reaching the budget with the paginator not exhausted sets
/// `edge_limit_reached`. This is the boundary the old single-request guard
/// (`rows.len() > remaining`) could never detect when a server-side cap
/// returned fewer rows than requested.
#[tokio::test]
async fn business_application_servers_edge_budget_paginates_and_truncates() {
    let _guard = mock_server_test_lock().await;
    let server = MockServer::start().await;
    let app = "11111111111111111111111111111111";
    let ci_one = "22222222222222222222222222222222";
    let ci_two = "33333333333333333333333333333333";
    let rel_type = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_ci_business_app"))
        .and(query_param(
            "sysparm_query",
            "sys_class_name=cmdb_ci_business_app^number=APM0000001",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [{
                "sys_id": app,
                "number": "APM0000001",
                "name": "Application Alpha",
                "sys_class_name": "cmdb_ci_business_app"
            }]
        })))
        .expect(1)
        .mount(&server)
        .await;

    // First parent page is full (2 == page size derived from max_edges=2),
    // so the paginator believes more pages may exist. The budget caps here.
    Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("parentIN{app}")))
            .and(query_param("sysparm_offset", "0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    relationship_row("99999999999999999999999999999991", app, ci_one, rel_type, "cmdb_ci_business_app", "cmdb_ci_appl"),
                    relationship_row("99999999999999999999999999999992", app, ci_two, rel_type, "cmdb_ci_business_app", "cmdb_ci_appl")
                ]
            })))
            .mount(&server)
            .await;

    let (core, _tempdir) = core_for_mock_server(&server).await;
    let result = core
        .business_application_servers(BusinessApplicationServersParams {
            number: Some("APM0000001".to_string()),
            max_edges: Some(2),
            ..Default::default()
        })
        .await
        .expect("business application servers")
        .expect("business application present");

    assert!(
        result.relationship_summary.edge_limit_reached,
        "consuming max_edges with pages remaining must set edge_limit_reached"
    );
    assert!(result.relationship_summary.truncated);
    assert_eq!(result.relationship_summary.relationships_examined, 2);
}

/// Task #8 regression: the parent and child directions are read CONCURRENTLY
/// but share a single `max_edges` budget that must be consumed in a stable
/// parent-then-child order. Here `max_edges = 2`, the parent direction returns
/// one edge and the child direction returns two. The merge must credit the
/// parent's single edge first, then cap the child's contribution to the one
/// remaining unit of budget — yielding exactly two examined edges (never three)
/// and flagging truncation. This proves concurrency does not double-count or
/// exceed the shared budget, and that the merge order is deterministic.
#[tokio::test]
async fn business_application_servers_shared_edge_budget_splits_across_directions() {
    let _guard = mock_server_test_lock().await;
    let server = MockServer::start().await;
    let app = "11111111111111111111111111111111";
    let ci_one = "22222222222222222222222222222222";
    let ci_two = "33333333333333333333333333333333";
    let ci_three = "44444444444444444444444444444444";
    let rel_type = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_ci_business_app"))
        .and(query_param(
            "sysparm_query",
            "sys_class_name=cmdb_ci_business_app^number=APM0000001",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [{
                "sys_id": app,
                "number": "APM0000001",
                "name": "Application Alpha",
                "sys_class_name": "cmdb_ci_business_app"
            }]
        })))
        .expect(1)
        .mount(&server)
        .await;

    // Parent direction: a single edge. Merged first, it consumes one unit of
    // the shared budget (max_edges = 2).
    Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("parentIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    relationship_row("99999999999999999999999999999991", app, ci_one, rel_type, "cmdb_ci_business_app", "cmdb_ci_appl")
                ]
            })))
            .mount(&server)
            .await;

    // Child direction: two edges, but only one unit of budget is left after the
    // parent merge, so exactly one of these survives the merge truncation.
    Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("childIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    relationship_row("99999999999999999999999999999992", ci_two, app, rel_type, "cmdb_ci_appl", "cmdb_ci_business_app"),
                    relationship_row("99999999999999999999999999999993", ci_three, app, rel_type, "cmdb_ci_appl", "cmdb_ci_business_app")
                ]
            })))
            .mount(&server)
            .await;

    let (core, _tempdir) = core_for_mock_server(&server).await;
    let result = core
        .business_application_servers(BusinessApplicationServersParams {
            number: Some("APM0000001".to_string()),
            max_edges: Some(2),
            ..Default::default()
        })
        .await
        .expect("business application servers")
        .expect("business application present");

    assert_eq!(
        result.relationship_summary.relationships_examined, 2,
        "shared budget must cap combined parent+child edges at max_edges"
    );
    assert!(
        result.relationship_summary.edge_limit_reached,
        "truncating the child direction at the shared budget sets edge_limit_reached"
    );
    assert!(result.relationship_summary.truncated);
}

/// Fix #4: `max_cis` bounds the CIs examined BEYOND the root BA. With
/// `max_cis = 1` and two adjacent CIs, exactly one non-root CI is examined
/// and the second is truncated. The root BA does not consume the budget, so a
/// caller asking for one CI is not silently short-changed to zero.
#[tokio::test]
async fn business_application_servers_reports_ci_limit_truncation() {
    let _guard = mock_server_test_lock().await;
    let server = MockServer::start().await;
    let app = "11111111111111111111111111111111";
    let service = "22222222222222222222222222222222";
    let service_two = "55555555555555555555555555555555";
    let rel_type = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_ci_business_app"))
        .and(query_param(
            "sysparm_query",
            "sys_class_name=cmdb_ci_business_app^number=APM0000001",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [{
                "sys_id": app,
                "number": "APM0000001",
                "name": "Application Alpha",
                "sys_class_name": "cmdb_ci_business_app"
            }]
        })))
        .expect(1)
        .mount(&server)
        .await;
    // Two adjacent non-root CIs; with max_cis=1 only the first is examined.
    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_rel_ci"))
        .and(query_param("sysparm_query", format!("parentIN{app}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [
                relationship_row(
                    "99999999999999999999999999999991",
                    app,
                    service,
                    rel_type,
                    "cmdb_ci_business_app",
                    "cmdb_ci_service"
                ),
                relationship_row(
                    "99999999999999999999999999999992",
                    app,
                    service_two,
                    rel_type,
                    "cmdb_ci_business_app",
                    "cmdb_ci_service"
                )
            ]
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_rel_ci"))
        .and(query_param("sysparm_query", format!("childIN{app}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": []
        })))
        .expect(1)
        .mount(&server)
        .await;
    // The first examined CI (service) is hydrated as a non-server class and
    // expanded into the next depth; it has no further relationships.
    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_rel_ci"))
        .and(query_param("sysparm_query", format!("parentIN{service}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": []
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_rel_ci"))
        .and(query_param("sysparm_query", format!("childIN{service}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": []
        })))
        .mount(&server)
        .await;

    let (core, _tempdir) = core_for_mock_server(&server).await;
    let result = core
        .business_application_servers(BusinessApplicationServersParams {
            number: Some("APM0000001".to_string()),
            max_cis: Some(1),
            ..Default::default()
        })
        .await
        .expect("business application servers")
        .expect("business application present");

    assert!(result.servers.is_empty());
    assert!(result.relationship_summary.ci_limit_reached);
    assert!(result.relationship_summary.truncated);
    assert_eq!(result.relationship_summary.truncated_count, 1);
    // One non-root CI examined plus the root => 2 in cis_examined.
    assert_eq!(result.relationship_summary.cis_examined, 2);
    assert_eq!(
        result
            .relationship_summary
            .degraded_reasons
            .get("fanout_limit_exceeded")
            .copied(),
        Some(1)
    );
    // The SECOND CI (service_two) is the one truncated by the budget.
    assert!(result.diagnostics.iter().any(|diagnostic| {
        diagnostic.reason == ReferenceResolutionReason::FanoutLimitExceeded
            && diagnostic.reference_sys_id == service_two
    }));
}

/// Fix #1 regression guard: a `cmdb_ci_server` subclass that is NOT in the
/// legacy `SERVER_TABLES` allowlist (here `cmdb_ci_esx_server`) must still be
/// classified as a server and hydrated, not traversed through as an
/// intermediate CI. The hydration query targets the base `cmdb_ci_server`
/// table and must NOT pin `sys_class_name` to the allowlist, otherwise the
/// subclass record would be filtered out server-side.
#[tokio::test]
async fn business_application_servers_collects_server_subclasses() {
    let _guard = mock_server_test_lock().await;
    let server = MockServer::start().await;
    let app = "11111111111111111111111111111111";
    let esx = "66666666666666666666666666666666";
    let rel_type = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let rel_row = "99999999999999999999999999999991";

    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_ci_business_app"))
        .and(query_param(
            "sysparm_query",
            "sys_class_name=cmdb_ci_business_app^number=APM0000001",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [{
                "sys_id": app,
                "number": "APM0000001",
                "name": "Application Alpha",
                "sys_class_name": "cmdb_ci_business_app"
            }]
        })))
        .expect(1)
        .mount(&server)
        .await;

    // The BA depends on an ESX server subclass directly.
    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_rel_ci"))
        .and(query_param("sysparm_query", format!("parentIN{app}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [
                relationship_row(
                    rel_row,
                    app,
                    esx,
                    rel_type,
                    "cmdb_ci_business_app",
                    "cmdb_ci_esx_server"
                )
            ]
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_rel_ci"))
        .and(query_param("sysparm_query", format!("childIN{app}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": []
        })))
        .expect(1)
        .mount(&server)
        .await;

    // Hydration must query the base server table by sys_id only (no
    // sys_class_name allowlist filter) so the ESX subclass row is returned.
    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_ci_server"))
        .and(query_param("sysparm_query", format!("sys_idIN{esx}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [server_row(esx, "esx-alpha.example.com", "cmdb_ci_esx_server")]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let (core, _tempdir) = core_for_mock_server(&server).await;
    let result = core
        .business_application_servers(BusinessApplicationServersParams {
            number: Some("APM0000001".to_string()),
            ..Default::default()
        })
        .await
        .expect("business application servers")
        .expect("business application present");

    assert_eq!(result.servers.len(), 1, "ESX subclass should be a server");
    assert_eq!(result.servers[0].record.sys_id, esx);
    assert_eq!(
        result.servers[0].class_name.as_deref(),
        Some("cmdb_ci_esx_server")
    );
    assert_eq!(result.relationship_summary.servers_found, 1);
}

/// Task #1-deeper: a server subclass whose table name does NOT end in
/// `_server` and is in no allowlist (here `cmdb_ci_acme_compute`) is invisible
/// to every cheap heuristic. It must still be recognized as a server via the
/// metadata-backed `sys_db_object` super_class descent — the class extends
/// `cmdb_ci_server`, so walking its ancestry reveals the server base table and
/// the CI is collected/hydrated rather than traversed through.
#[tokio::test]
async fn business_application_servers_detects_custom_subclass_via_hierarchy() {
    let _guard = mock_server_test_lock().await;
    let server = MockServer::start().await;
    let app = "11111111111111111111111111111111";
    let compute = "77777777777777777777777777777777";
    let rel_type = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let rel_row = "99999999999999999999999999999991";
    // sys_id of the cmdb_ci_server class row in sys_db_object.
    let server_class = "5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e";

    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_ci_business_app"))
        .and(query_param(
            "sysparm_query",
            "sys_class_name=cmdb_ci_business_app^number=APM0000001",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [{
                "sys_id": app,
                "number": "APM0000001",
                "name": "Application Alpha",
                "sys_class_name": "cmdb_ci_business_app"
            }]
        })))
        .expect(1)
        .mount(&server)
        .await;

    // The BA depends on a custom server subclass whose name does not end in
    // `_server`, so no cheap heuristic classifies it.
    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_rel_ci"))
        .and(query_param("sysparm_query", format!("parentIN{app}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [
                relationship_row(
                    rel_row,
                    app,
                    compute,
                    rel_type,
                    "cmdb_ci_business_app",
                    "cmdb_ci_acme_compute"
                )
            ]
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_rel_ci"))
        .and(query_param("sysparm_query", format!("childIN{app}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": []
        })))
        .mount(&server)
        .await;

    // super_class descent: cmdb_ci_acme_compute -> (super_class sys_id) ->
    // cmdb_ci_server, which terminates the walk.
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_db_object"))
        .and(query_param("sysparm_query", "name=cmdb_ci_acme_compute"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [{ "name": "cmdb_ci_acme_compute", "super_class": server_class }]
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_db_object"))
        .and(query_param(
            "sysparm_query",
            format!("sys_id={server_class}"),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [{ "name": "cmdb_ci_server" }]
        })))
        .mount(&server)
        .await;
    // Once cmdb_ci_server becomes the cursor, terminate the walk.
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_db_object"))
        .and(query_param("sysparm_query", "name=cmdb_ci_server"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [{ "name": "cmdb_ci_server", "super_class": "" }]
        })))
        .mount(&server)
        .await;

    // Hydration of the recognized server by sys_id against the base table.
    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_ci_server"))
        .and(query_param("sysparm_query", format!("sys_idIN{compute}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [server_row(compute, "acme-compute-01.example.com", "cmdb_ci_acme_compute")]
        })))
        .mount(&server)
        .await;

    let (core, _tempdir) = core_for_mock_server(&server).await;
    let result = core
        .business_application_servers(BusinessApplicationServersParams {
            number: Some("APM0000001".to_string()),
            ..Default::default()
        })
        .await
        .expect("business application servers")
        .expect("business application present");

    assert_eq!(
        result.servers.len(),
        1,
        "custom subclass extending cmdb_ci_server must be detected via hierarchy"
    );
    assert_eq!(result.servers[0].record.sys_id, compute);
    assert_eq!(result.relationship_summary.servers_found, 1);
}

/// Fix #2 regression guard: with the default (unspecified) relationship-type
/// filter, an edge must match by the stable `cmdb_rel_type` identity (sys_id)
/// even when the instance's display label has been renamed/localized so it no
/// longer equals any default label string. The traversal resolves the default
/// label set to sys_ids once via a `cmdb_rel_type` lookup and matches on those.
#[tokio::test]
async fn business_application_servers_default_types_match_by_resolved_identity() {
    let _guard = mock_server_test_lock().await;
    let server = MockServer::start().await;
    let app = "11111111111111111111111111111111";
    let linux = "33333333333333333333333333333333";
    // sys_id of the "Depends on::Used by" cmdb_rel_type on this instance.
    let depends_on = "dededededededededededededededede";
    let rel_row = "99999999999999999999999999999991";

    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_ci_business_app"))
        .and(query_param(
            "sysparm_query",
            "sys_class_name=cmdb_ci_business_app^number=APM0000001",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [{
                "sys_id": app,
                "number": "APM0000001",
                "name": "Application Alpha",
                "sys_class_name": "cmdb_ci_business_app"
            }]
        })))
        .expect(1)
        .mount(&server)
        .await;

    // The default-label resolution query against cmdb_rel_type returns the
    // sys_id of the "Depends on::Used by" type (by its stored name).
    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_rel_type"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [
                { "sys_id": depends_on, "name": "Depends on::Used by" }
            ]
        })))
        .mount(&server)
        .await;

    // The edge carries the resolved sys_id but a RENAMED display label that
    // does not equal any default label string.
    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_rel_ci"))
        .and(query_param("sysparm_query", format!("parentIN{app}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [
                relationship_row_typed(
                    rel_row,
                    app,
                    linux,
                    depends_on,
                    "Depende de::Usado por",
                    "cmdb_ci_business_app",
                    "cmdb_ci_linux_server"
                )
            ]
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_rel_ci"))
        .and(query_param("sysparm_query", format!("childIN{app}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": []
        })))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_ci_server"))
        .and(query_param("sysparm_query", format!("sys_idIN{linux}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [server_row(linux, "linux-alpha.example.com", "cmdb_ci_linux_server")]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let (core, _tempdir) = core_for_mock_server(&server).await;
    let result = core
        .business_application_servers(BusinessApplicationServersParams {
            number: Some("APM0000001".to_string()),
            ..Default::default()
        })
        .await
        .expect("business application servers")
        .expect("business application present");

    assert_eq!(
        result.servers.len(),
        1,
        "default filter must match renamed-label edge by resolved sys_id"
    );
    assert_eq!(result.servers[0].record.sys_id, linux);
}

/// Fix #2: an explicitly-supplied EMPTY relationship-type allowlist means
/// "match all", so an edge with an arbitrary type is still traversed and its
/// server collected, and no cmdb_rel_type resolution query is required.
#[tokio::test]
async fn business_application_servers_explicit_empty_types_match_all() {
    let _guard = mock_server_test_lock().await;
    let server = MockServer::start().await;
    let app = "11111111111111111111111111111111";
    let linux = "33333333333333333333333333333333";
    let weird_type = "cccccccccccccccccccccccccccccccc";
    let rel_row = "99999999999999999999999999999991";

    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_ci_business_app"))
        .and(query_param(
            "sysparm_query",
            "sys_class_name=cmdb_ci_business_app^number=APM0000001",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [{
                "sys_id": app,
                "number": "APM0000001",
                "name": "Application Alpha",
                "sys_class_name": "cmdb_ci_business_app"
            }]
        })))
        .expect(1)
        .mount(&server)
        .await;

    // An arbitrary, non-default relationship type with an unfamiliar label.
    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_rel_ci"))
        .and(query_param("sysparm_query", format!("parentIN{app}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [
                relationship_row_typed(
                    rel_row,
                    app,
                    linux,
                    weird_type,
                    "Some Custom Relationship",
                    "cmdb_ci_business_app",
                    "cmdb_ci_linux_server"
                )
            ]
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_rel_ci"))
        .and(query_param("sysparm_query", format!("childIN{app}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": []
        })))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_ci_server"))
        .and(query_param("sysparm_query", format!("sys_idIN{linux}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [server_row(linux, "linux-alpha.example.com", "cmdb_ci_linux_server")]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let (core, _tempdir) = core_for_mock_server(&server).await;
    // An explicit empty allowlist (caller passed `relationship_type: []`
    // explicitly via the options) means "match all".
    let options = BusinessApplicationServersOptions {
        selector: BusinessApplicationServersSelector::Number("APM0000001".to_string()),
        max_depth: 2,
        max_cis: 500,
        max_edges: 2000,
        max_service_membership_associations:
            BUSINESS_APPLICATION_SERVERS_DEFAULT_MAX_SERVICE_MEMBERSHIP_ASSOCIATIONS,
        max_service_membership_pages:
            BUSINESS_APPLICATION_SERVERS_DEFAULT_MAX_SERVICE_MEMBERSHIP_PAGES,
        relationship_type: Vec::new(),
        include_paths: false,
        fallback_strategy: FallbackStrategy::None,
        persist: false,
        prune_stale: false,
    };
    // defaults_when_empty = false => empty allowlist means "match all".
    let result = core
        .business_applications
        .business_application_servers_with_options(options, false)
        .await
        .expect("business application servers")
        .expect("business application present");

    assert_eq!(
        result.servers.len(),
        1,
        "explicit empty allowlist must match all relationship types"
    );
}

fn service_membership_row(
    sys_id: &str,
    service_id: &str,
    ci_id: &str,
    ci_class_name: &str,
) -> serde_json::Value {
    serde_json::json!({
        "sys_id": sys_id,
        "service_id": {
            "value": service_id,
            "display_value": "Application Service"
        },
        "service_id.sys_class_name": "cmdb_ci_service",
        "ci_id": {
            "value": ci_id,
            "display_value": "Server CI"
        },
        "ci_id.sys_class_name": ci_class_name
    })
}

#[tokio::test]
async fn business_application_servers_returns_service_membership_servers() {
    let _guard = mock_server_test_lock().await;
    let server = MockServer::start().await;
    let app = "11111111111111111111111111111111";
    let service = "22222222222222222222222222222222";
    let linux = "33333333333333333333333333333333";
    let consumes = "cccccccccccccccccccccccccccccccc";

    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_ci_business_app"))
        .and(query_param(
            "sysparm_query",
            "sys_class_name=cmdb_ci_business_app^number=APM0000001",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [{
                "sys_id": app,
                "number": "APM0000001",
                "name": "Application Alpha",
                "sys_class_name": "cmdb_ci_business_app"
            }]
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_rel_ci"))
        .and(query_param("sysparm_query", format!("parentIN{app}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [
                relationship_row_typed(
                    "99999999999999999999999999999991",
                    app,
                    service,
                    consumes,
                    "Consumes::Consumed by",
                    "cmdb_ci_business_app",
                    "cmdb_ci_service"
                )
            ]
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_rel_ci"))
        .and(query_param("sysparm_query", format!("childIN{app}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": []
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/svc_ci_assoc"))
        .and(query_param(
            "sysparm_query",
            format!("service_idIN{service}"),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [service_membership_row(
                "88888888888888888888888888888881",
                service,
                linux,
                "cmdb_ci_linux_server"
            )]
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_ci_server"))
        .and(query_param("sysparm_query", format!("sys_idIN{linux}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [server_row(linux, "linux-service.example.com", "cmdb_ci_linux_server")]
        })))
        .mount(&server)
        .await;

    let (core, _tempdir) = core_for_mock_server(&server).await;
    let result = core
        .business_application_servers(BusinessApplicationServersParams {
            number: Some("APM0000001".to_string()),
            include_paths: true,
            persist: Some(true),
            ..Default::default()
        })
        .await
        .expect("business application servers")
        .expect("business application present");

    assert_eq!(result.servers.len(), 1);
    assert_eq!(
        result.server_provenance.get(linux),
        Some(&BusinessApplicationServerProvenance::ServiceMembership)
    );
    let path = &result.server_paths[linux][0];
    assert_eq!(path.depth(), 2);
    assert_eq!(
        path.edges.last().map(|edge| &edge.edge_source),
        Some(&BusinessApplicationServerPathEdgeSource::ServiceMembership)
    );
    assert_eq!(
        path.edges
            .last()
            .map(|edge| edge.relationship_type.value.as_str()),
        Some("service_member_of")
    );
    assert_eq!(
        result
            .inventory_health
            .as_ref()
            .map(|health| health.service_membership_status.as_str()),
        Some("ok")
    );

    let cached = core
        .business_application_servers_cached(BusinessApplicationServersCachedParams {
            sys_id: Some(app.to_string()),
            ..Default::default()
        })
        .await
        .expect("cached forward lookup")
        .expect("business application cached");
    assert_eq!(cached.servers.len(), 1);
    assert_eq!(cached.servers[0].provenance, "service_membership");
    assert_eq!(cached.servers[0].min_depth, 2);
}

#[tokio::test]
async fn business_application_servers_merges_relationship_and_service_membership_provenance() {
    let _guard = mock_server_test_lock().await;
    let server = MockServer::start().await;
    let app = "11111111111111111111111111111111";
    let service = "22222222222222222222222222222222";
    let linux = "33333333333333333333333333333333";
    let runs = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let consumes = "cccccccccccccccccccccccccccccccc";

    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_ci_business_app"))
        .and(query_param(
            "sysparm_query",
            "sys_class_name=cmdb_ci_business_app^number=APM0000001",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [{
                "sys_id": app,
                "number": "APM0000001",
                "name": "Application Alpha",
                "sys_class_name": "cmdb_ci_business_app"
            }]
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_rel_ci"))
        .and(query_param("sysparm_query", format!("parentIN{app}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [
                relationship_row_typed(
                    "99999999999999999999999999999991",
                    app,
                    linux,
                    runs,
                    "Runs on::Runs",
                    "cmdb_ci_business_app",
                    "cmdb_ci_linux_server"
                ),
                relationship_row_typed(
                    "99999999999999999999999999999992",
                    app,
                    service,
                    consumes,
                    "Consumes::Consumed by",
                    "cmdb_ci_business_app",
                    "cmdb_ci_service"
                )
            ]
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_rel_ci"))
        .and(query_param("sysparm_query", format!("childIN{app}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": []
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/svc_ci_assoc"))
        .and(query_param(
            "sysparm_query",
            format!("service_idIN{service}"),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [service_membership_row(
                "88888888888888888888888888888881",
                service,
                linux,
                "cmdb_ci_linux_server"
            )]
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_ci_server"))
        .and(query_param("sysparm_query", format!("sys_idIN{linux}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [server_row(linux, "linux-both.example.com", "cmdb_ci_linux_server")]
        })))
        .expect(2)
        .mount(&server)
        .await;

    let (core, _tempdir) = core_for_mock_server(&server).await;
    let result = core
        .business_application_servers(BusinessApplicationServersParams {
            number: Some("APM0000001".to_string()),
            include_paths: true,
            persist: Some(true),
            ..Default::default()
        })
        .await
        .expect("business application servers")
        .expect("business application present");

    assert_eq!(result.servers.len(), 1);
    assert_eq!(
        result.server_provenance.get(linux),
        Some(&BusinessApplicationServerProvenance::Both)
    );
    assert_eq!(result.server_paths[linux].len(), 2);

    let cached = core
        .business_application_servers_cached(BusinessApplicationServersCachedParams {
            sys_id: Some(app.to_string()),
            ..Default::default()
        })
        .await
        .expect("cached forward lookup")
        .expect("business application cached");
    assert_eq!(cached.servers.len(), 1);
    assert_eq!(cached.servers[0].provenance, "both");
}

#[tokio::test]
async fn business_application_servers_service_membership_acl_degrades_relationship_results() {
    let _guard = mock_server_test_lock().await;
    let server = MockServer::start().await;
    let app = "11111111111111111111111111111111";
    let service = "22222222222222222222222222222222";
    let linux = "33333333333333333333333333333333";
    let runs = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let consumes = "cccccccccccccccccccccccccccccccc";

    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_ci_business_app"))
        .and(query_param(
            "sysparm_query",
            "sys_class_name=cmdb_ci_business_app^number=APM0000001",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [{
                "sys_id": app,
                "number": "APM0000001",
                "name": "Application Alpha",
                "sys_class_name": "cmdb_ci_business_app"
            }]
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_rel_ci"))
        .and(query_param("sysparm_query", format!("parentIN{app}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [
                relationship_row_typed(
                    "99999999999999999999999999999991",
                    app,
                    linux,
                    runs,
                    "Runs on::Runs",
                    "cmdb_ci_business_app",
                    "cmdb_ci_linux_server"
                ),
                relationship_row_typed(
                    "99999999999999999999999999999992",
                    app,
                    service,
                    consumes,
                    "Consumes::Consumed by",
                    "cmdb_ci_business_app",
                    "cmdb_ci_service"
                )
            ]
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_rel_ci"))
        .and(query_param("sysparm_query", format!("childIN{app}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": []
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/svc_ci_assoc"))
        .and(query_param(
            "sysparm_query",
            format!("service_idIN{service}"),
        ))
        .respond_with(ResponseTemplate::new(403).set_body_json(serde_json::json!({
            "error": { "message": "Forbidden" },
            "status": "failure"
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_ci_server"))
        .and(query_param("sysparm_query", format!("sys_idIN{linux}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [server_row(linux, "linux-acl.example.com", "cmdb_ci_linux_server")]
        })))
        .mount(&server)
        .await;

    let (core, _tempdir) = core_for_mock_server(&server).await;
    let result = core
        .business_application_servers(BusinessApplicationServersParams {
            number: Some("APM0000001".to_string()),
            persist: Some(true),
            ..Default::default()
        })
        .await
        .expect("business application servers")
        .expect("business application present");

    assert_eq!(result.servers.len(), 1);
    let health = result.inventory_health.expect("inventory health");
    assert_eq!(health.service_membership_status, "acl_restricted");
    assert_eq!(health.relationship_status, "ok");
    assert_eq!(health.inventory_status, "service_membership_degraded");
}

#[tokio::test]
async fn business_application_servers_service_membership_accepts_server_subclasses() {
    let _guard = mock_server_test_lock().await;
    let server = MockServer::start().await;
    let app = "11111111111111111111111111111111";
    let service = "22222222222222222222222222222222";
    let esx = "66666666666666666666666666666666";
    let consumes = "cccccccccccccccccccccccccccccccc";

    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_ci_business_app"))
        .and(query_param(
            "sysparm_query",
            "sys_class_name=cmdb_ci_business_app^number=APM0000001",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [{
                "sys_id": app,
                "number": "APM0000001",
                "name": "Application Alpha",
                "sys_class_name": "cmdb_ci_business_app"
            }]
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_rel_ci"))
        .and(query_param("sysparm_query", format!("parentIN{app}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [
                relationship_row_typed(
                    "99999999999999999999999999999991",
                    app,
                    service,
                    consumes,
                    "Consumes::Consumed by",
                    "cmdb_ci_business_app",
                    "cmdb_ci_service"
                )
            ]
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_rel_ci"))
        .and(query_param("sysparm_query", format!("childIN{app}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": []
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/svc_ci_assoc"))
        .and(query_param(
            "sysparm_query",
            format!("service_idIN{service}"),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [service_membership_row(
                "88888888888888888888888888888881",
                service,
                esx,
                "cmdb_ci_esx_server"
            )]
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_ci_server"))
        .and(query_param("sysparm_query", format!("sys_idIN{esx}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [server_row(esx, "esx-service.example.com", "cmdb_ci_esx_server")]
        })))
        .mount(&server)
        .await;

    let (core, _tempdir) = core_for_mock_server(&server).await;
    let result = core
        .business_application_servers(BusinessApplicationServersParams {
            number: Some("APM0000001".to_string()),
            ..Default::default()
        })
        .await
        .expect("business application servers")
        .expect("business application present");

    assert_eq!(result.servers.len(), 1);
    assert_eq!(result.servers[0].record.sys_id, esx);
    assert_eq!(
        result.server_provenance.get(esx),
        Some(&BusinessApplicationServerProvenance::ServiceMembership)
    );
}

// ---- ci_owner_group CMDB-gap fallback fixtures (Part 2) ----------------

const FALLBACK_APP: &str = "11111111111111111111111111111111";
const FALLBACK_GROUP: &str = "9999999999999999999999999999aaaa";
const FALLBACK_LINUX: &str = "33333333333333333333333333333333";
const FALLBACK_WINDOWS: &str = "44444444444444444444444444444444";

/// Mount a BA record with the RAW `u_ci_owner_group` field populated and a
/// present-but-empty `managed_by_group` alias — the live-data shape the
/// field-mapping requirement guards against.
async fn mount_fallback_ba(server: &MockServer, group_sys_id: Option<&str>) {
    let mut record = serde_json::json!({
        "sys_id": FALLBACK_APP,
        "number": "APM0000001",
        "name": "Application Alpha",
        "sys_class_name": "cmdb_ci_business_app",
        // The typed `ci_owner_group` alias maps here and is intentionally
        // empty on live data; the fallback must NOT read it.
        "managed_by_group": { "value": "", "display_value": "" }
    });
    if let Some(group_sys_id) = group_sys_id {
        record.as_object_mut().unwrap().insert(
            BUSINESS_APPLICATION_CI_OWNER_GROUP_RAW_FIELD.to_string(),
            serde_json::json!({ "value": group_sys_id, "display_value": "Owner Group" }),
        );
    }
    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_ci_business_app"))
        .and(query_param(
            "sysparm_query",
            "sys_class_name=cmdb_ci_business_app^number=APM0000001",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [record]
        })))
        .mount(server)
        .await;
}

/// Mount the two empty `cmdb_rel_ci` direction reads so the traversal finds 0
/// servers (default depth 2 only issues the depth-1 frontier reads).
async fn mount_empty_traversal(server: &MockServer) {
    for direction in ["parent", "child"] {
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param(
                "sysparm_query",
                format!("{direction}IN{FALLBACK_APP}"),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": []
            })))
            .mount(server)
            .await;
    }
}

#[tokio::test]
async fn ci_owner_group_fallback_returns_tagged_servers_when_traversal_empty() {
    let _guard = mock_server_test_lock().await;
    let server = MockServer::start().await;
    mount_fallback_ba(&server, Some(FALLBACK_GROUP)).await;
    mount_empty_traversal(&server).await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_ci_server"))
        .and(query_param(
            "sysparm_query",
            format!("{BUSINESS_APPLICATION_CI_OWNER_GROUP_RAW_FIELD}={FALLBACK_GROUP}^ORDERBYname"),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [
                server_row(FALLBACK_LINUX, "linux-fallback.example.com", "cmdb_ci_linux_server"),
                server_row(FALLBACK_WINDOWS, "windows-fallback.example.com", "cmdb_ci_win_server")
            ]
        })))
        .mount(&server)
        .await;

    let (core, _tempdir) = core_for_mock_server(&server).await;
    let result = core
        .business_application_servers(BusinessApplicationServersParams {
            number: Some("APM0000001".to_string()),
            fallback_strategy: FallbackStrategy::CiOwnerGroup,
            ..Default::default()
        })
        .await
        .expect("ba servers")
        .expect("ba present");

    assert_eq!(result.servers.len(), 2);
    for server in &result.servers {
        assert_eq!(
            result.server_sources.get(&server.record.sys_id),
            Some(&ServerResultSource::CiOwnerGroupFallback)
        );
    }
    let summary = &result.relationship_summary;
    assert!(summary.fallback_used);
    assert_eq!(summary.cmdb_servers_found, Some(0));
    assert_eq!(summary.servers_found, 2);
    assert_eq!(summary.fallback_strategy.as_deref(), Some("ci_owner_group"));
    assert_eq!(
        summary.fallback_group_sys_id.as_deref(),
        Some(FALLBACK_GROUP)
    );
    assert_eq!(
        summary.fallback_group_display_name.as_deref(),
        Some("Owner Group")
    );
    assert_eq!(
        summary
            .degraded_reasons
            .get(BUSINESS_APPLICATION_DEGRADED_REASON_CMDB_RELATIONSHIPS_UNMAPPED),
        Some(&1)
    );
    // Fallback never persists: no traversal servers, no membership upserts.
    assert_eq!(summary.persisted_servers, 0);
    assert_eq!(summary.membership_upserts, 0);
    assert!(result.server_provenance.is_empty());
}

#[tokio::test]
async fn ci_owner_group_fallback_fires_even_though_managed_by_group_empty() {
    let _guard = mock_server_test_lock().await;
    // Field-mapping proof: the BA's managed_by_group alias is empty; the
    // fallback fires because it sources/filters on the RAW u_ci_owner_group.
    let server = MockServer::start().await;
    mount_fallback_ba(&server, Some(FALLBACK_GROUP)).await;
    mount_empty_traversal(&server).await;
    // The ONLY server query the fallback may issue is the exact raw-field
    // filter. A managed_by_group-based query would not match this mock and
    // the call would 404/timeout, failing the test.
    Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .and(query_param(
                "sysparm_query",
                format!("{BUSINESS_APPLICATION_CI_OWNER_GROUP_RAW_FIELD}={FALLBACK_GROUP}^ORDERBYname"),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [server_row(FALLBACK_LINUX, "linux-fallback.example.com", "cmdb_ci_linux_server")]
            })))
            .expect(1)
            .mount(&server)
            .await;

    let (core, _tempdir) = core_for_mock_server(&server).await;
    let result = core
        .business_application_servers(BusinessApplicationServersParams {
            number: Some("APM0000001".to_string()),
            fallback_strategy: FallbackStrategy::CiOwnerGroup,
            ..Default::default()
        })
        .await
        .expect("ba servers")
        .expect("ba present");

    assert_eq!(result.servers.len(), 1);
    assert!(result.relationship_summary.fallback_used);
}

#[tokio::test]
async fn ci_owner_group_fallback_does_not_fire_when_traversal_finds_servers() {
    let _guard = mock_server_test_lock().await;
    let server = MockServer::start().await;
    let rel_type = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    mount_fallback_ba(&server, Some(FALLBACK_GROUP)).await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_rel_ci"))
        .and(query_param(
            "sysparm_query",
            format!("parentIN{FALLBACK_APP}"),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [relationship_row(
                "99999999999999999999999999999991",
                FALLBACK_APP,
                FALLBACK_LINUX,
                rel_type,
                "cmdb_ci_business_app",
                "cmdb_ci_linux_server"
            )]
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_rel_ci"))
        .and(query_param(
            "sysparm_query",
            format!("childIN{FALLBACK_APP}"),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": []
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_ci_server"))
        .and(query_param(
            "sysparm_query",
            format!("sys_idIN{FALLBACK_LINUX}"),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [server_row(FALLBACK_LINUX, "linux-real.example.com", "cmdb_ci_linux_server")]
        })))
        .mount(&server)
        .await;

    let (core, _tempdir) = core_for_mock_server(&server).await;
    let result = core
        .business_application_servers(BusinessApplicationServersParams {
            number: Some("APM0000001".to_string()),
            fallback_strategy: FallbackStrategy::CiOwnerGroup,
            ..Default::default()
        })
        .await
        .expect("ba servers")
        .expect("ba present");

    assert_eq!(result.servers.len(), 1);
    // Fallback did NOT fire: no fallback servers, no degraded gap reason.
    assert!(!result.relationship_summary.fallback_used);
    assert!(result.server_sources.is_empty());
    // cmdb_servers_found still reported (strategy requested) and equals the
    // traversal count.
    assert_eq!(result.relationship_summary.cmdb_servers_found, Some(1));
    assert_eq!(
        result.server_sources.get(FALLBACK_LINUX),
        None,
        "traversal servers are not tagged as fallback"
    );
}

#[tokio::test]
async fn ci_owner_group_fallback_no_group_emits_clean_diagnostic() {
    let _guard = mock_server_test_lock().await;
    let server = MockServer::start().await;
    mount_fallback_ba(&server, None).await;
    mount_empty_traversal(&server).await;

    let (core, _tempdir) = core_for_mock_server(&server).await;
    let result = core
        .business_application_servers(BusinessApplicationServersParams {
            number: Some("APM0000001".to_string()),
            fallback_strategy: FallbackStrategy::CiOwnerGroup,
            ..Default::default()
        })
        .await
        .expect("ba servers")
        .expect("ba present");

    assert!(result.servers.is_empty());
    assert!(!result.relationship_summary.fallback_used);
    assert_eq!(result.relationship_summary.cmdb_servers_found, Some(0));
    // Clean structured diagnostic, not an error.
    assert!(
        result.diagnostics.iter().any(|diagnostic| {
            diagnostic.field == BUSINESS_APPLICATION_CI_OWNER_GROUP_RAW_FIELD
        })
    );
}

#[tokio::test]
async fn ci_owner_group_fallback_strategy_none_adds_no_fields() {
    let _guard = mock_server_test_lock().await;
    let server = MockServer::start().await;
    mount_fallback_ba(&server, Some(FALLBACK_GROUP)).await;
    mount_empty_traversal(&server).await;

    let (core, _tempdir) = core_for_mock_server(&server).await;
    let result = core
        .business_application_servers(BusinessApplicationServersParams {
            number: Some("APM0000001".to_string()),
            // default: FallbackStrategy::None
            ..Default::default()
        })
        .await
        .expect("ba servers")
        .expect("ba present");

    assert!(result.servers.is_empty());
    assert!(result.server_sources.is_empty());
    let summary = &result.relationship_summary;
    assert!(!summary.fallback_used);
    assert_eq!(summary.cmdb_servers_found, None);
    assert_eq!(summary.fallback_strategy, None);
    assert_eq!(summary.fallback_group_sys_id, None);
    // No new fields appear in the default-path serialization.
    let serialized = serde_json::to_value(summary).expect("serialize summary");
    let object = serialized.as_object().expect("summary object");
    assert!(!object.contains_key("cmdb_servers_found"));
    assert!(!object.contains_key("fallback_used"));
    assert!(!object.contains_key("fallback_strategy"));
}

#[tokio::test]
async fn ci_owner_group_fallback_group_with_zero_servers() {
    let _guard = mock_server_test_lock().await;
    let server = MockServer::start().await;
    mount_fallback_ba(&server, Some(FALLBACK_GROUP)).await;
    mount_empty_traversal(&server).await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_ci_server"))
        .and(query_param(
            "sysparm_query",
            format!("{BUSINESS_APPLICATION_CI_OWNER_GROUP_RAW_FIELD}={FALLBACK_GROUP}^ORDERBYname"),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": []
        })))
        .mount(&server)
        .await;

    let (core, _tempdir) = core_for_mock_server(&server).await;
    let result = core
        .business_application_servers(BusinessApplicationServersParams {
            number: Some("APM0000001".to_string()),
            fallback_strategy: FallbackStrategy::CiOwnerGroup,
            ..Default::default()
        })
        .await
        .expect("ba servers")
        .expect("ba present");

    assert!(result.servers.is_empty());
    // The owner group exists and was queried, so the data-quality gap is real:
    // fallback_used is true even though it returned no servers.
    assert!(result.relationship_summary.fallback_used);
    assert_eq!(result.relationship_summary.cmdb_servers_found, Some(0));
}

#[tokio::test]
async fn ci_owner_group_fallback_acl_restricted_query() {
    let _guard = mock_server_test_lock().await;
    let server = MockServer::start().await;
    mount_fallback_ba(&server, Some(FALLBACK_GROUP)).await;
    mount_empty_traversal(&server).await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_ci_server"))
        .and(query_param(
            "sysparm_query",
            format!("{BUSINESS_APPLICATION_CI_OWNER_GROUP_RAW_FIELD}={FALLBACK_GROUP}^ORDERBYname"),
        ))
        .respond_with(ResponseTemplate::new(403).set_body_json(serde_json::json!({
            "error": { "message": "ACL restricted" }
        })))
        .mount(&server)
        .await;

    let (core, _tempdir) = core_for_mock_server(&server).await;
    let result = core
        .business_application_servers(BusinessApplicationServersParams {
            number: Some("APM0000001".to_string()),
            fallback_strategy: FallbackStrategy::CiOwnerGroup,
            ..Default::default()
        })
        .await
        .expect("ba servers")
        .expect("ba present");

    assert!(result.servers.is_empty());
    assert!(!result.relationship_summary.fallback_used);
    assert!(result.relationship_summary.acl_restricted_count >= 1);
    assert!(result.diagnostics.iter().any(|diagnostic| {
        diagnostic.reason == ReferenceResolutionReason::ReferenceAclRestricted
    }));
}

#[tokio::test]
async fn ci_owner_group_fallback_tombstoned_group_no_panic() {
    let _guard = mock_server_test_lock().await;
    let server = MockServer::start().await;
    mount_fallback_ba(&server, Some(FALLBACK_GROUP)).await;
    mount_empty_traversal(&server).await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_ci_server"))
        .and(query_param(
            "sysparm_query",
            format!("{BUSINESS_APPLICATION_CI_OWNER_GROUP_RAW_FIELD}={FALLBACK_GROUP}^ORDERBYname"),
        ))
        .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
            "error": { "message": "No record found" }
        })))
        .mount(&server)
        .await;

    let (core, _tempdir) = core_for_mock_server(&server).await;
    let result = core
        .business_application_servers(BusinessApplicationServersParams {
            number: Some("APM0000001".to_string()),
            fallback_strategy: FallbackStrategy::CiOwnerGroup,
            ..Default::default()
        })
        .await
        .expect("ba servers")
        .expect("ba present");

    assert!(result.servers.is_empty());
    assert!(!result.relationship_summary.fallback_used);
    assert!(result.diagnostics.iter().any(|diagnostic| {
        diagnostic.field == BUSINESS_APPLICATION_CI_OWNER_GROUP_RAW_FIELD
            && diagnostic.reason == ReferenceResolutionReason::ReferenceNotFound
    }));
}

#[tokio::test]
async fn ci_owner_group_fallback_writes_no_durable_membership_rows() {
    let _guard = mock_server_test_lock().await;
    // Load-bearing live-only assertion: a fallback-triggering run leaves the
    // durable BA↔server inventory tables unchanged. We run with persist=true
    // (the CLI/daemon default) so the only writes would come from the
    // traversal path; the fallback servers must not appear in the cached
    // forward/reverse projections or the inventory-health row.
    let server = MockServer::start().await;
    mount_fallback_ba(&server, Some(FALLBACK_GROUP)).await;
    mount_empty_traversal(&server).await;
    Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .and(query_param(
                "sysparm_query",
                format!("{BUSINESS_APPLICATION_CI_OWNER_GROUP_RAW_FIELD}={FALLBACK_GROUP}^ORDERBYname"),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [server_row(FALLBACK_LINUX, "linux-fallback.example.com", "cmdb_ci_linux_server")]
            })))
            .mount(&server)
            .await;

    let (core, _tempdir) = core_for_mock_server(&server).await;
    let result = core
        .business_application_servers(BusinessApplicationServersParams {
            number: Some("APM0000001".to_string()),
            fallback_strategy: FallbackStrategy::CiOwnerGroup,
            persist: Some(true),
            ..Default::default()
        })
        .await
        .expect("ba servers")
        .expect("ba present");

    // The live response carries the fallback server...
    assert_eq!(result.servers.len(), 1);
    assert!(result.relationship_summary.fallback_used);
    // ...but nothing was persisted: 0 membership upserts, 0 persisted servers.
    assert_eq!(result.relationship_summary.persisted_servers, 0);
    assert_eq!(result.relationship_summary.membership_upserts, 0);

    // The durable forward projection has NO servers for this BA.
    let cached_forward = core
        .business_application_servers_cached(BusinessApplicationServersCachedParams {
            sys_id: Some(FALLBACK_APP.to_string()),
            ..Default::default()
        })
        .await
        .expect("cached forward");
    if let Some(cached) = cached_forward {
        assert!(
            cached.servers.is_empty(),
            "fallback server must not be persisted to the forward projection"
        );
    }

    // The durable reverse projection has NO BA for the fallback server.
    let cached_reverse = core
        .business_applications_for_server(BusinessApplicationsForServerParams {
            sys_id: Some(FALLBACK_LINUX.to_string()),
            ..Default::default()
        })
        .await
        .expect("cached reverse");
    if let Some(cached) = cached_reverse {
        for entry in &cached.servers {
            assert!(
                entry.business_applications.is_empty(),
                "fallback server must not gain a durable BA association"
            );
        }
    }
}

fn relationship_row(
    sys_id: &str,
    parent: &str,
    child: &str,
    relationship_type: &str,
    parent_class: &str,
    child_class: &str,
) -> serde_json::Value {
    relationship_row_typed(
        sys_id,
        parent,
        child,
        relationship_type,
        "Depends on::Used by",
        parent_class,
        child_class,
    )
}

/// Like [`relationship_row`] but lets a test set the relationship type's
/// display label independently of its sys_id, so Fix #2 can simulate a
/// renamed/localized `cmdb_rel_type` label.
fn relationship_row_typed(
    sys_id: &str,
    parent: &str,
    child: &str,
    relationship_type: &str,
    relationship_type_label: &str,
    parent_class: &str,
    child_class: &str,
) -> serde_json::Value {
    serde_json::json!({
        "sys_id": sys_id,
        "parent": { "value": parent, "display_value": "Parent CI" },
        "child": { "value": child, "display_value": "Child CI" },
        "type": {
            "value": relationship_type,
            "display_value": relationship_type_label
        },
        "parent.sys_class_name": parent_class,
        "child.sys_class_name": child_class
    })
}

fn server_row(sys_id: &str, name: &str, class_name: &str) -> serde_json::Value {
    serde_json::json!({
        "sys_id": sys_id,
        "name": name,
        "sys_class_name": class_name,
        "ip_address": "192.0.2.10",
        "operational_status": { "value": "1", "display_value": "Operational" }
    })
}

/// Mount a `sys_db_object` response so `table_ancestors` terminates with no
/// parent (the Business Application table is treated as its own root).
async fn mount_no_table_ancestors(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_db_object"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [{ "name": BUSINESS_APPLICATION_TABLE, "super_class": "" }]
        })))
        .mount(server)
        .await;
}

#[tokio::test]
async fn business_application_dictionary_refresh_caches_metadata_and_promotes_aliases() {
    let server = MockServer::start().await;
    mount_no_table_ancestors(&server).await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_dictionary"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [
                {
                    "name": BUSINESS_APPLICATION_TABLE,
                    "element": "portfolio",
                    "column_label": "Primary Portfolio",
                    "internal_type": { "value": "reference", "display_value": "Reference" },
                    "reference": { "value": "pm_portfolio", "display_value": "Portfolio" },
                    "choice": "0",
                    "mandatory": "false",
                    "read_only": "false",
                    "max_length": "32",
                    "active": "true"
                },
                {
                    "name": BUSINESS_APPLICATION_TABLE,
                    "element": "operational_status",
                    "column_label": "Operational State",
                    "internal_type": { "value": "choice", "display_value": "Choice" },
                    "reference": "",
                    "choice": "1",
                    "mandatory": "false",
                    "read_only": "false",
                    "max_length": "40",
                    "active": "true"
                }
            ]
        })))
        .mount(&server)
        .await;

    let (core, _tempdir) = core_for_mock_server(&server).await;
    let persisted = core
        .refresh_business_application_dictionary()
        .await
        .expect("dictionary refresh");
    assert_eq!(persisted, 2);

    let dictionary = core
        .business_application_dictionary()
        .await
        .expect("dictionary read");
    assert_eq!(
        dictionary
            .get("portfolio")
            .and_then(|row| row.reference_table.clone()),
        Some("pm_portfolio".to_string())
    );
    assert_eq!(
        dictionary
            .get("operational_status")
            .map(|row| row.field_type.clone()),
        Some(Some("choice".to_string()))
    );
    assert!(dictionary["operational_status"].choice);

    let aliases = core.business_application_aliases().await.expect("aliases");
    // Dictionary-verified: portfolio target table discovered, version set,
    // and no DictionaryUnavailable diagnostic.
    assert_eq!(aliases.primary_portfolio, "portfolio");
    assert_eq!(
        aliases.primary_portfolio_table,
        Some("pm_portfolio".to_string())
    );
    assert!(aliases.dictionary_version.is_some());
    assert!(aliases.diagnostics.is_empty());
}

#[tokio::test]
async fn business_application_aliases_degrade_without_dictionary() {
    let server = MockServer::start().await;
    mount_no_table_ancestors(&server).await;
    let (core, _tempdir) = core_for_mock_server(&server).await;
    let aliases = core.business_application_aliases().await.expect("aliases");
    // Cache miss => baseline degraded with a DictionaryUnavailable diagnostic.
    assert_eq!(aliases.primary_portfolio, "portfolio");
    assert!(aliases.dictionary_version.is_none());
    assert!(aliases.diagnostics.iter().any(|diagnostic| {
        diagnostic.reason == ReferenceResolutionReason::DictionaryUnavailable
    }));
}

#[tokio::test]
async fn business_application_sync_summarizes_run() {
    let server = MockServer::start().await;
    mount_no_table_ancestors(&server).await;
    let owner_sys_id = "6816f79cc0a8016401c5a33be04be441";
    Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_business_app"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": "54a4b61b6fe845000ed852a03f3ee4d0",
                    "name": "Epic",
                    "sys_class_name": "cmdb_ci_business_app",
                    "business_owner": { "value": owner_sys_id, "display_value": "Jane Owner" },
                    "portfolio": { "value": "portfolio-sys-id-000000000000000", "display_value": "Clinical" },
                    "operational_status": { "value": "1", "display_value": "Operational" }
                }]
            })))
            .mount(&server)
            .await;
    Mock::given(method("GET"))
        .and(path(format!("/api/now/table/sys_user/{owner_sys_id}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": {
                "sys_id": owner_sys_id,
                "name": "Jane Owner",
                "user_name": "jowner",
                "email": "jane.owner@example.invalid",
                "active": { "value": "true", "display_value": "true" },
                "sys_updated_on": "2026-05-30 12:00:00"
            }
        })))
        .mount(&server)
        .await;

    let (core, _tempdir) = core_for_mock_server(&server).await;
    let summary = core
        .sync_business_applications(
            Some(BusinessApplicationSearchParams {
                name: Some("Epic".to_string()),
                limit: Some(5),
                ..Default::default()
            }),
            BusinessApplicationHydrationOptions::default(),
        )
        .await
        .expect("sync summary");

    assert!(!summary.all);
    assert_eq!(summary.table, BUSINESS_APPLICATION_TABLE);
    assert_eq!(summary.page_size, 5);
    assert_eq!(summary.pages, 1);
    assert_eq!(summary.total_returned, 1);
    assert_eq!(summary.total_applications, 1);
    assert_eq!(summary.persisted, 1);
    // Owner reference resolves; the portfolio reference is degraded because
    // no dictionary was loaded for this run.
    assert!(summary.references_resolved >= 1);
    assert!(summary.dictionary_degraded);
    assert!(!summary.dictionary_refreshed);
    assert!(
        summary
            .degraded_reasons
            .contains_key("dictionary_unavailable")
    );
}

#[tokio::test]
async fn business_application_sync_non_persistent_reports_zero_persisted() {
    let server = MockServer::start().await;
    mount_no_table_ancestors(&server).await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_ci_business_app"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [{
                "sys_id": "54a4b61b6fe845000ed852a03f3ee4d0",
                "name": "Epic",
                "sys_class_name": "cmdb_ci_business_app",
                "operational_status": { "value": "1", "display_value": "Operational" }
            }]
        })))
        .mount(&server)
        .await;

    let (core, _tempdir) = core_for_mock_server(&server).await;
    let summary = core
        .sync_business_applications(
            None,
            BusinessApplicationHydrationOptions {
                persist: false,
                ..Default::default()
            },
        )
        .await
        .expect("sync summary");

    assert_eq!(summary.total_applications, 1);
    assert_eq!(summary.total_returned, 1);
    assert_eq!(summary.persisted, 0);
}

fn business_application_page_record(index: usize) -> serde_json::Value {
    serde_json::json!({
        "sys_id": format!("{:032x}", index + 1),
        "name": format!("Application {:03}", index + 1),
        "number": format!("APP{:07}", index + 1),
        "sys_class_name": BUSINESS_APPLICATION_TABLE,
        "operational_status": { "value": "1", "display_value": "Operational" }
    })
}

#[tokio::test]
async fn business_application_sync_all_drains_live_pages_and_persists_each_page() {
    let server = MockServer::start().await;
    mount_no_table_ancestors(&server).await;
    let first_page = (0..100)
        .map(business_application_page_record)
        .collect::<Vec<_>>();
    let second_page = vec![business_application_page_record(100)];

    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_ci_business_app"))
        .and(query_param("sysparm_limit", "100"))
        .and(query_param("sysparm_offset", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": first_page
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/cmdb_ci_business_app"))
        .and(query_param("sysparm_limit", "100"))
        .and(query_param("sysparm_offset", "100"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": second_page
        })))
        .expect(1)
        .mount(&server)
        .await;

    let (core, _tempdir) = core_for_mock_server(&server).await;
    let summary = core
        .sync_all_business_applications(BusinessApplicationHydrationOptions::default())
        .await
        .expect("sync all summary");

    assert!(summary.all);
    assert_eq!(summary.table, BUSINESS_APPLICATION_TABLE);
    assert_eq!(summary.page_size, 100);
    assert_eq!(summary.pages, 2);
    assert_eq!(summary.total_returned, 101);
    assert_eq!(summary.total_applications, 101);
    assert_eq!(summary.persisted, 101);
    assert!(summary.dictionary_degraded);
    assert_eq!(
        summary
            .degraded_reasons
            .get("dictionary_unavailable")
            .copied(),
        Some(101)
    );

    let cached = core
        .query_business_applications(BusinessApplicationQuery {
            limit: Some(200),
            ..Default::default()
        })
        .await
        .expect("cached business applications");
    assert_eq!(cached.len(), 101);

    let requests = server.received_requests().await.expect("requests");
    let business_app_requests = requests
        .iter()
        .filter(|request| request.url.path() == "/api/now/table/cmdb_ci_business_app")
        .collect::<Vec<_>>();
    assert_eq!(business_app_requests.len(), 2);
    for request in business_app_requests {
        let query = request
            .url
            .query_pairs()
            .collect::<std::collections::HashMap<_, _>>();
        assert_eq!(
            query.get("sysparm_query").map(|value| value.as_ref()),
            Some("sys_class_name=cmdb_ci_business_app^ORDERBYname^ORDERBYsys_id")
        );
        assert_eq!(
            query
                .get("sysparm_display_value")
                .map(|value| value.as_ref()),
            Some("all")
        );
        assert_eq!(
            query
                .get("sysparm_exclude_reference_link")
                .map(|value| value.as_ref()),
            Some("true")
        );
    }
}

#[test]
fn business_application_reference_discovery_uses_known_map() {
    let record = Record::from_json(
        BUSINESS_APPLICATION_TABLE,
        &serde_json::json!({
            "sys_id": "54a4b61b6fe845000ed852a03f3ee4d0",
            "name": "Epic",
            "business_owner": {
                "value": "6816f79cc0a8016401c5a33be04be441",
                "display_value": "Jane Owner"
            },
            "support_group": {
                "value": "287ebd7da9fe198100f92cc8d1d2154e",
                "display_value": "App Support"
            },
            "portfolio": {
                "value": "46d44a23a9fe19810012d100cca80666",
                "display_value": "Clinical"
            }
        }),
        DisplayValue::Both,
    )
    .expect("record");

    let business_application = BusinessApplication::from_servicenow(
        &record,
        &BusinessApplicationFieldAliases::baseline_degraded(),
    )
    .expect("business application");

    assert_eq!(
        business_application
            .business_owner
            .as_ref()
            .map(|reference| reference.table.as_str()),
        Some("sys_user")
    );
    assert_eq!(
        business_application
            .primary_support_group
            .as_ref()
            .map(|reference| reference.table.as_str()),
        Some("sys_user_group")
    );
    assert!(
        business_application
            .references
            .iter()
            .any(|reference| reference.field == "portfolio"
                && reference.reference_table == "pm_portfolio"
                && reference.resolution_status == ReferenceResolutionStatus::Resolved)
    );
}
