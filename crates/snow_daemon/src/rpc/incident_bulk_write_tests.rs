//! T-OPS-04 L0 daemon JSON-RPC governed-write seam with local ServiceNow/state-store fakes.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use chrono::Utc;
use rusqlite::{Connection, OptionalExtension};
use serde_json::{Value, json};
use snow_core::cache::store::{RecordRow, TagRow};
use snow_core::query::QueryEngine;
use snow_core::vault::manager::VaultManager;
use snow_core::{CacheSource, ResourceType, SnowRecord};
use snow_mcp::audit::{AuditChainRange, AuditSink, SqliteAuditSink};
use snow_mcp::{DaemonBackedMcpBridge, JsonRpcRequest as McpRequest, McpServer};
use tokio::sync::oneshot;
use tokio::task::LocalSet;
use wiremock::matchers::{method, path, query_param_contains};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::{JsonRpcRequest, dispatch};
use crate::DaemonState;
use crate::test_support::socket_path;

const TARGETS: [(&str, &str, &str); 3] = [
    (
        "INC0012345",
        "00000000000000000000000000000001",
        "2026-08-20 12:00:00",
    ),
    (
        "INC0012346",
        "00000000000000000000000000000002",
        "2026-08-20 12:01:00",
    ),
    (
        "INC0012347",
        "00000000000000000000000000000003",
        "2026-08-20 12:02:00",
    ),
];

#[tokio::test(flavor = "current_thread")]
async fn daemon_bulk_apply_stops_on_first_failure_and_durably_replays_exact_partial_state() {
    let instance = MockServer::start().await;
    for (index, (number, sys_id, updated)) in TARGETS.iter().enumerate() {
        Mock::given(method("GET"))
            .and(path("/api/now/table/incident"))
            .and(query_param_contains(
                "sysparm_query",
                format!("number={number}"),
            ))
            .respond_with(incident_list(number, sys_id, updated))
            .expect(1)
            .mount(&instance)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/api/now/table/incident/{sys_id}")))
            .respond_with(incident_get(number, sys_id, updated))
            .expect(match index {
                0 => 2,
                1 => 2,
                _ => 1,
            })
            .mount(&instance)
            .await;
    }
    Mock::given(method("PATCH"))
        .and(path(format!("/api/now/table/incident/{}", TARGETS[0].1)))
        .respond_with(incident_get(
            TARGETS[0].0,
            TARGETS[0].1,
            "2026-08-20 13:00:00",
        ))
        .expect(1)
        .mount(&instance)
        .await;
    Mock::given(method("PATCH"))
        .and(path(format!("/api/now/table/incident/{}", TARGETS[1].1)))
        .respond_with(ResponseTemplate::new(500).set_body_json(json!({
            "error": {
                "message": "Safe upstream diagnostic echoed Public-safe journal body that must never enter a receipt or audit",
                "detail": "https://example.service-now.com authorization bearer token"
            }
        })))
        .expect(1)
        .mount(&instance)
        .await;
    Mock::given(method("PATCH"))
        .and(path(format!("/api/now/table/incident/{}", TARGETS[2].1)))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&instance)
        .await;

    let fixture = crate::test_support::build_fixture_state_at_instance(&instance.uri())
        .await
        .expect("fixture state");
    let state = Arc::new(DaemonState::with_data_dir_and_mcp_config(
        Arc::clone(&fixture.state.core),
        fixture.tempdir.path().join("incident-bulk-partial"),
        enabled_bulk_config(25),
    ));
    let pruned_path = seed_legacy_projection(fixture.tempdir.path(), TARGETS[0].0, TARGETS[0].1);
    let unrelated_path = seed_legacy_projection(
        fixture.tempdir.path(),
        "INC0099999",
        "ffffffffffffffffffffffffffffffff",
    );
    assert!(
        state
            .core
            .get_record(TARGETS[0].0)
            .await
            .expect("seeded target lookup")
            .is_some()
    );
    let plan = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "incident_bulk_plan_update".to_string(),
            params: json!({
                "shared_patch": {
                    "assignment_group": "0123456789abcdef0123456789abcdef",
                    "comments": "Public-safe journal body that must never enter a receipt or audit"
                },
                "targets": TARGETS.iter().rev().map(|(number, _, _)| json!({"number": number})).collect::<Vec<_>>()
            }),
            id: Some(json!(1)),
        },
        &state,
    )
    .await;
    assert!(plan.error.is_none(), "{plan:?}");
    let bundle = plan.result.expect("plan result");
    let audit_db = fixture
        .tempdir
        .path()
        .join("incident-bulk-partial/mcp_story_write.sqlite3");
    Connection::open(&audit_db)
        .expect("open durable state for retention fixture")
        .execute(
            "UPDATE mcp_audit_events SET timestamp = '2000-01-01T00:00:00Z' WHERE audit_id = ?1",
            [bundle["plan_id"].as_str().expect("plan id")],
        )
        .expect("age the plan audit beyond retention");
    let apply_params = apply_params(&bundle);

    let apply = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "incident_bulk_apply_update".to_string(),
            params: apply_params.clone(),
            id: Some(json!(2)),
        },
        &state,
    )
    .await;
    let error = apply.error.expect("partial failure");
    assert_eq!(error.code, -32046, "{error:?}");
    assert_eq!(error.message, "PARTIAL_FAILURE");
    let data = error.data.expect("partial data");
    assert_eq!(data["code"], "PARTIAL_FAILURE");
    assert_eq!(data["failure_code"], "UPSTREAM_ERROR");
    assert_eq!(data["upstream_applied"], false);
    assert_eq!(
        data["upstream_diagnostic"],
        "ServiceNow request failed with a redacted diagnostic"
    );
    let error_text = serde_json::to_string(&data).expect("partial error text");
    assert!(!error_text.contains("example.service-now.com"));
    assert!(!error_text.contains("secret journal body"));
    let receipt = &data["receipt"];
    assert_eq!(receipt["status"], "partial");
    assert_eq!(receipt["applied_count"], 1);
    assert_eq!(receipt["failed_count"], 1);
    assert_eq!(receipt["not_attempted_count"], 1);
    assert_eq!(
        receipt["target_results"]
            .as_array()
            .expect("target results")
            .iter()
            .map(|target| target["status"].as_str().unwrap_or_default())
            .collect::<Vec<_>>(),
        vec!["applied", "failed", "not_attempted"]
    );
    let serialized = serde_json::to_string(receipt).expect("receipt JSON");
    assert!(!serialized.contains("record_snapshot"));
    assert!(!serialized.contains("record_url"));
    assert!(!serialized.contains("work_notes"));
    assert!(!serialized.contains("Public-safe journal body"));

    let connection = Connection::open(audit_db).expect("open durable state");
    let retained_plan_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM mcp_audit_events WHERE audit_id = ?1",
            [bundle["plan_id"].as_str().expect("plan id")],
            |row| row.get(0),
        )
        .expect("query retained plan audit");
    assert_eq!(retained_plan_count, 0);
    let retained_count: i64 = connection
        .query_row("SELECT COUNT(*) FROM mcp_audit_events", [], |row| {
            row.get(0)
        })
        .expect("query retained audit count");
    assert!(
        retained_count >= 2,
        "attempt and outcome audits must survive"
    );
    let chain_breaks = SqliteAuditSink::open(connection.path().expect("audit database path"))
        .expect("open retained audit chain")
        .verify_chain(AuditChainRange::default())
        .await
        .expect("verify retained audit chain");
    assert!(
        chain_breaks.is_empty(),
        "retained chain must restart at GENESIS"
    );
    let event_json: String = connection
        .query_row(
            "SELECT event_json FROM mcp_audit_events WHERE audit_id = ?1",
            [receipt["audit_id"].as_str().expect("receipt audit id")],
            |row| row.get(0),
        )
        .optional()
        .expect("query outcome audit")
        .expect("durable outcome audit");
    let event: Value = serde_json::from_str(&event_json).expect("audit JSON");
    assert_eq!(event["parent_audit_id"], bundle["plan_id"]);
    assert_eq!(event["result_status"], "applied_partial");
    assert_eq!(
        event["error"]["reason"],
        "ServiceNow request failed with a redacted diagnostic"
    );
    assert!(!event_json.contains("example.service-now.com"));
    assert!(!event_json.contains("secret journal body"));
    assert!(!event_json.contains("Public-safe journal body"));
    assert!(
        state
            .core
            .get_record(TARGETS[0].0)
            .await
            .expect("pruned target lookup")
            .is_none()
    );
    assert!(!pruned_path.exists());
    assert!(
        state
            .core
            .get_record("INC0099999")
            .await
            .expect("unrelated lookup")
            .is_some()
    );
    assert!(unrelated_path.exists());
    let projection_db =
        Connection::open(fixture.tempdir.path().join("snow.db")).expect("open projection database");
    for (table, selector, value, expected) in [
        ("fts_records", "number", TARGETS[0].0, 0_i64),
        ("fts_records", "number", "INC0099999", 1_i64),
        ("record_tags", "record_sys_id", TARGETS[0].1, 0_i64),
        (
            "record_tags",
            "record_sys_id",
            "ffffffffffffffffffffffffffffffff",
            1_i64,
        ),
    ] {
        let count: i64 = projection_db
            .query_row(
                &format!("SELECT COUNT(*) FROM {table} WHERE {selector} = ?1"),
                [value],
                |row| row.get(0),
            )
            .expect("query projection index");
        assert_eq!(count, expected);
    }

    let replay = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "incident_bulk_apply_update".to_string(),
            params: apply_params.clone(),
            id: Some(json!(3)),
        },
        &state,
    )
    .await;
    assert!(replay.error.is_none(), "{replay:?}");
    let replay = replay.result.expect("replay receipt");
    assert_eq!(replay["idempotency_replay"], true);
    assert_eq!(replay["audit_id"], receipt["audit_id"]);

    let socket = socket_path(&fixture.tempdir);
    let server = crate::rpc::JsonRpcServer::new(
        Arc::clone(&state),
        snow_core::ipc::IpcEndpoint::from_socket_path(&socket),
    );
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let local = LocalSet::new();
    local.spawn_local(async move {
        let _ = server
            .serve_until(async move {
                let _ = shutdown_rx.await;
                Ok(())
            })
            .await;
    });
    local
        .run_until(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let call = McpRequest {
                jsonrpc: "2.0".to_string(),
                method: "tools/call".to_string(),
                params: json!({
                    "name": "incident_bulk_apply_update",
                    "arguments": apply_params,
                }),
                id: Some(json!(77)),
            };
            let direct = McpServer::new(Arc::clone(&state.core))
                .dispatch(call.clone())
                .await
                .result
                .expect("direct replay result");
            let bridge = DaemonBackedMcpBridge::from_socket(socket.clone())
                .dispatch(call)
                .await
                .result
                .expect("bridge replay result");
            assert_eq!(
                serde_json::to_vec(&direct).expect("direct replay bytes"),
                serde_json::to_vec(&bridge["structuredContent"]).expect("bridge replay bytes")
            );
            assert_eq!(direct["idempotency_replay"], true);
            let _ = shutdown_tx.send(());
        })
        .await;
}

#[tokio::test]
async fn daemon_bulk_apply_never_substitutes_the_prewrite_token_when_patch_omits_it() {
    let instance = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_choice"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": [{"value": "2", "label": "In Progress", "inactive": "false"}]
        })))
        .mount(&instance)
        .await;
    for (index, (number, sys_id, updated)) in TARGETS.iter().enumerate() {
        Mock::given(method("GET"))
            .and(path("/api/now/table/incident"))
            .and(query_param_contains(
                "sysparm_query",
                format!("number={number}"),
            ))
            .respond_with(incident_list(number, sys_id, updated))
            .expect(1)
            .mount(&instance)
            .await;
        if index == 0 {
            let reads = Arc::new(AtomicUsize::new(0));
            Mock::given(method("GET"))
                .and(path(format!("/api/now/table/incident/{sys_id}")))
                .respond_with({
                    let reads = Arc::clone(&reads);
                    move |_request: &wiremock::Request| {
                        if reads.fetch_add(1, Ordering::SeqCst) < 2 {
                            incident_get(number, sys_id, updated)
                        } else {
                            ResponseTemplate::new(500).set_body_json(json!({
                                "error": {"message": "post-write token unavailable"}
                            }))
                        }
                    }
                })
                .expect(6)
                .mount(&instance)
                .await;
        } else {
            Mock::given(method("GET"))
                .and(path(format!("/api/now/table/incident/{sys_id}")))
                .respond_with(incident_get(number, sys_id, updated))
                .expect(1)
                .mount(&instance)
                .await;
        }
        Mock::given(method("PATCH"))
            .and(path(format!("/api/now/table/incident/{sys_id}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "result": {"sys_id": sys_id, "number": number}
            })))
            .expect(if index == 0 { 1 } else { 0 })
            .mount(&instance)
            .await;
    }
    let fixture = crate::test_support::build_fixture_state_at_instance(&instance.uri())
        .await
        .expect("fixture state");
    let state = Arc::new(DaemonState::with_data_dir_and_mcp_config(
        Arc::clone(&fixture.state.core),
        fixture.tempdir.path().join("incident-bulk-token-omission"),
        enabled_bulk_config(25),
    ));
    let plan = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "incident_bulk_plan_update".to_string(),
            params: json!({
                "shared_patch": {"state": "2"},
                "targets": TARGETS.iter().map(|(number, _, _)| json!({"number": number})).collect::<Vec<_>>()
            }),
            id: Some(json!(10)),
        },
        &state,
    )
    .await
    .result
    .expect("plan result");
    let response = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "incident_bulk_apply_update".to_string(),
            params: apply_params(&plan),
            id: Some(json!(11)),
        },
        &state,
    )
    .await;
    let data = response
        .error
        .expect("coherence failure")
        .data
        .expect("error data");
    assert_eq!(data["failure_code"], "LOCAL_COHERENCE_FAILED");
    assert_eq!(data["upstream_applied"], true);
    assert_eq!(data["receipt"]["applied_count"], 1);
    assert_eq!(
        data["receipt"]["target_results"][0]["observed_sys_updated_on"],
        TARGETS[0].2
    );
    let audit_path = fixture
        .tempdir
        .path()
        .join("incident-bulk-token-omission/mcp_story_write.sqlite3");
    let connection = Connection::open(audit_path).expect("open audit database");
    let event_json: String = connection
        .query_row(
            "SELECT event_json FROM mcp_audit_events WHERE audit_id = ?1",
            [data["receipt"]["audit_id"]
                .as_str()
                .expect("outcome audit id")],
            |row| row.get(0),
        )
        .expect("partial outcome audit");
    let event: Value = serde_json::from_str(&event_json).expect("audit JSON");
    assert_eq!(event["error"]["code"], "LOCAL_COHERENCE_FAILED");
    assert_eq!(event["error"]["reason"], "LOCAL_COHERENCE_FAILED");
}

#[tokio::test]
async fn daemon_bulk_apply_detects_a_later_stale_target_in_all_target_preflight_before_any_patch() {
    let instance = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_choice"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": [{"value": "2", "label": "In Progress", "inactive": "false"}]
        })))
        .mount(&instance)
        .await;
    for (index, (number, sys_id, updated)) in TARGETS.iter().enumerate() {
        Mock::given(method("GET"))
            .and(path("/api/now/table/incident"))
            .and(query_param_contains(
                "sysparm_query",
                format!("number={number}"),
            ))
            .respond_with(incident_list(number, sys_id, updated))
            .expect(1)
            .mount(&instance)
            .await;
        let observed = if index == 2 {
            "2026-08-20 15:59:59"
        } else {
            updated
        };
        Mock::given(method("GET"))
            .and(path(format!("/api/now/table/incident/{sys_id}")))
            .respond_with(incident_get(number, sys_id, observed))
            .expect(1)
            .mount(&instance)
            .await;
        Mock::given(method("PATCH"))
            .and(path(format!("/api/now/table/incident/{sys_id}")))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&instance)
            .await;
    }
    let fixture = crate::test_support::build_fixture_state_at_instance(&instance.uri())
        .await
        .expect("fixture state");
    let state = Arc::new(DaemonState::with_data_dir_and_mcp_config(
        Arc::clone(&fixture.state.core),
        fixture.tempdir.path().join("incident-bulk-stale-preflight"),
        enabled_bulk_config(25),
    ));
    let plan = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "incident_bulk_plan_update".to_string(),
            params: json!({
                "shared_patch": {"state": "2"},
                "targets": TARGETS.iter().map(|(number, _, _)| json!({"number": number})).collect::<Vec<_>>()
            }),
            id: Some(json!(20)),
        },
        &state,
    )
    .await
    .result
    .expect("plan result");
    let response = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "incident_bulk_apply_update".to_string(),
            params: apply_params(&plan),
            id: Some(json!(21)),
        },
        &state,
    )
    .await;
    let error = response.error.expect("stale preflight denial");
    assert_eq!(error.code, -32053);
    assert_eq!(
        error.data.expect("error data")["code"],
        "CONCURRENCY_CONFLICT"
    );
}

#[tokio::test]
async fn daemon_bulk_first_target_upstream_failure_replays_the_durable_typed_zero_write_error() {
    let instance = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_choice"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": [{"value": "2", "label": "In Progress", "inactive": "false"}]
        })))
        .mount(&instance)
        .await;
    for (index, (number, sys_id, updated)) in TARGETS.iter().enumerate() {
        Mock::given(method("GET"))
            .and(path("/api/now/table/incident"))
            .and(query_param_contains(
                "sysparm_query",
                format!("number={number}"),
            ))
            .respond_with(incident_list(number, sys_id, updated))
            .expect(2)
            .mount(&instance)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/api/now/table/incident/{sys_id}")))
            .respond_with(incident_get(number, sys_id, updated))
            .expect(if index == 0 { 2 } else { 1 })
            .mount(&instance)
            .await;
    }
    Mock::given(method("PATCH"))
        .and(path(format!("/api/now/table/incident/{}", TARGETS[0].1)))
        .respond_with(ResponseTemplate::new(500).set_body_json(json!({
            "error": {"message": "public-safe rejection"}
        })))
        .expect(1)
        .mount(&instance)
        .await;
    let fixture = crate::test_support::build_fixture_state_at_instance(&instance.uri())
        .await
        .expect("fixture state");
    let state = Arc::new(DaemonState::with_data_dir_and_mcp_config(
        Arc::clone(&fixture.state.core),
        fixture.tempdir.path().join("incident-bulk-first-failure"),
        enabled_bulk_config(25),
    ));
    let plan = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "incident_bulk_plan_update".to_string(),
            params: json!({
                "shared_patch": {"state": "2"},
                "targets": TARGETS.iter().map(|(number, _, _)| json!({"number": number})).collect::<Vec<_>>()
            }),
            id: Some(json!(30)),
        },
        &state,
    )
    .await
    .result
    .expect("plan result");
    let params = apply_params(&plan);
    let first = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "incident_bulk_apply_update".to_string(),
            params: params.clone(),
            id: Some(json!(31)),
        },
        &state,
    )
    .await
    .error
    .expect("typed first-target failure");
    assert_eq!(first.code, -32059);
    assert_eq!(first.message, "UPSTREAM_ERROR");
    let replay = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "incident_bulk_apply_update".to_string(),
            params,
            id: Some(json!(32)),
        },
        &state,
    )
    .await
    .error
    .expect("durable typed replay");
    assert_eq!(replay.code, first.code);
    assert_eq!(replay.message, first.message);
    assert_eq!(replay.data, first.data);

    let second_plan = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "incident_bulk_plan_update".to_string(),
            params: json!({
                "shared_patch": {"comments": "A different public-safe operation"},
                "targets": TARGETS.iter().map(|(number, _, _)| json!({"number": number})).collect::<Vec<_>>()
            }),
            id: Some(json!(33)),
        },
        &state,
    )
    .await
    .result
    .expect("second plan result");
    assert_ne!(second_plan["op_hash"], plan["op_hash"]);
    let mut conflicting = apply_params(&second_plan);
    conflicting["idempotency_key"] = plan["idempotency_key"].clone();
    let conflict = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "incident_bulk_apply_update".to_string(),
            params: conflicting,
            id: Some(json!(34)),
        },
        &state,
    )
    .await
    .error
    .expect("different operation hash conflict");
    assert_eq!(
        conflict.data.expect("conflict data")["code"],
        "IDEMPOTENCY_CONFLICT"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn daemon_bulk_policy_and_target_bound_denials_issue_zero_servicenow_writes() {
    let instance = MockServer::start().await;
    Mock::given(method("PATCH"))
        .and(path(
            "/api/now/table/incident/00000000000000000000000000000001",
        ))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&instance)
        .await;
    let fixture = crate::test_support::build_fixture_state_at_instance(&instance.uri())
        .await
        .expect("fixture state");
    let denied_state = Arc::new(DaemonState::with_data_dir_and_mcp_config(
        Arc::clone(&fixture.state.core),
        fixture.tempdir.path().join("incident-bulk-denied"),
        snow_mcp::McpConfig::default(),
    ));
    let denied_response = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "incident_bulk_plan_update".to_string(),
            params: json!({"targets": [
                {"sys_id": TARGETS[0].1, "patch": {"state": "2"}},
                {"sys_id": TARGETS[1].1, "patch": {"state": "2"}},
                {"sys_id": TARGETS[2].1, "patch": {"state": "2"}}
            ]}),
            id: Some(json!(1)),
        },
        &denied_state,
    )
    .await;
    let default_denial = denied_response.error.expect("default denial");
    assert_eq!(default_denial.code, -32051);
    assert_eq!(
        default_denial.data.expect("default denial data")["code"],
        "POLICY_DENIED"
    );

    let bounded_state = Arc::new(DaemonState::with_data_dir_and_mcp_config(
        Arc::clone(&fixture.state.core),
        fixture.tempdir.path().join("incident-bulk-bounded"),
        enabled_bulk_config(25),
    ));
    let targets = (0..26)
        .map(|index| {
            json!({
                "sys_id": format!("{index:032x}"),
                "patch": {"assignment_group": "0123456789abcdef0123456789abcdef"}
            })
        })
        .collect::<Vec<_>>();
    let response = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "incident_bulk_plan_update".to_string(),
            params: json!({"targets": targets}),
            id: Some(json!(2)),
        },
        &bounded_state,
    )
    .await;
    let error = response.error.expect("26-target denial");
    assert_eq!(
        error.data.expect("error data")["code"],
        "TARGET_COUNT_INVALID"
    );
    let requests = instance
        .received_requests()
        .await
        .expect("received requests");
    assert!(
        requests
            .iter()
            .all(|request| request.method.as_str() != "PATCH")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn daemon_bulk_policy_intersection_and_strict_shape_denials_precede_servicenow_io() {
    let instance = MockServer::start().await;
    let fixture = crate::test_support::build_fixture_state_at_instance(&instance.uri())
        .await
        .expect("fixture state");
    let base_targets = (1..=4)
        .map(|index| {
            json!({
                "sys_id": format!("{index:032x}"),
                "patch": {"comments": "Public-safe note"}
            })
        })
        .collect::<Vec<_>>();

    let mut apply_disabled = enabled_bulk_config(25);
    apply_disabled
        .policy
        .tools
        .get_mut("incident_bulk_apply_update")
        .unwrap()
        .enabled = false;
    let response = dispatch_bulk_plan(
        &fixture,
        "apply-disabled",
        apply_disabled,
        json!({"targets": base_targets.clone()}),
    )
    .await;
    assert_eq!(
        response.error.unwrap().data.unwrap()["code"],
        "POLICY_DENIED"
    );

    let mut wrong_environment = enabled_bulk_config(25);
    for tool in ["incident_bulk_plan_update", "incident_bulk_apply_update"] {
        wrong_environment
            .policy
            .tools
            .get_mut(tool)
            .unwrap()
            .environments = vec!["production".to_string()];
    }
    let response = dispatch_bulk_plan(
        &fixture,
        "wrong-environment",
        wrong_environment,
        json!({"targets": base_targets[..3].to_vec()}),
    )
    .await;
    assert_eq!(
        response.error.unwrap().data.unwrap()["code"],
        "POLICY_DENIED"
    );

    let response = dispatch_bulk_plan(
        &fixture,
        "unknown-field",
        enabled_bulk_config(25),
        json!({"targets": base_targets[..3].to_vec(), "debug": true}),
    )
    .await;
    assert_eq!(response.error.unwrap().code, -32602);

    let ordinary_state = Arc::new(DaemonState::with_data_dir_and_mcp_config(
        Arc::clone(&fixture.state.core),
        fixture.tempdir.path().join("ordinary-rejects-bulk-shape"),
        enabled_bulk_config(25),
    ));
    let response = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "incident_plan_update".to_string(),
            params: json!({"targets": base_targets[..3].to_vec()}),
            id: Some(json!(99)),
        },
        &ordinary_state,
    )
    .await;
    assert_eq!(response.error.expect("ordinary strict shape").code, -32040);

    let mut missing_max = enabled_bulk_config(25);
    missing_max
        .policy
        .tools
        .get_mut("incident_bulk_plan_update")
        .unwrap()
        .max_targets = None;
    let response = dispatch_bulk_plan(
        &fixture,
        "missing-max",
        missing_max,
        json!({"targets": base_targets.clone()}),
    )
    .await;
    assert_eq!(
        response.error.unwrap().data.unwrap()["code"],
        "MAX_TARGETS_INVALID"
    );

    let mut lowered = enabled_bulk_config(25);
    lowered
        .policy
        .tools
        .get_mut("incident_bulk_plan_update")
        .unwrap()
        .max_targets = Some(3);
    let response = dispatch_bulk_plan(
        &fixture,
        "lowered-max",
        lowered,
        json!({"targets": base_targets.clone()}),
    )
    .await;
    assert_eq!(
        response.error.unwrap().data.unwrap()["code"],
        "TARGET_COUNT_INVALID"
    );

    let mut narrowed = enabled_bulk_config(25);
    narrowed
        .policy
        .tools
        .get_mut("incident_bulk_plan_update")
        .unwrap()
        .field_allowlist
        .remove("comments");
    let response = dispatch_bulk_plan(
        &fixture,
        "field-narrowed",
        narrowed,
        json!({"targets": base_targets[..3].to_vec()}),
    )
    .await;
    assert_eq!(
        response.error.unwrap().data.unwrap()["code"],
        "FIELD_REJECTED"
    );

    let response = dispatch_bulk_plan(
        &fixture,
        "overlap",
        enabled_bulk_config(25),
        json!({
            "shared_patch": {"comments": "Shared"},
            "targets": base_targets[..3].to_vec()
        }),
    )
    .await;
    assert_eq!(
        response.error.unwrap().data.unwrap()["code"],
        "PATCH_OVERLAP"
    );

    assert!(
        snow_mcp::domain::policy::PolicyConfig::from_toml_str(
            r#"
[mcp]
[mcp.tools.incident_bulk_plan_update]
max_targets = 26
"#
        )
        .is_err()
    );
    assert!(
        instance
            .received_requests()
            .await
            .expect("requests")
            .is_empty()
    );
}

async fn dispatch_bulk_plan(
    fixture: &crate::test_support::FixtureState,
    data_dir: &str,
    config: snow_mcp::McpConfig,
    params: Value,
) -> crate::rpc::JsonRpcResponse {
    let state = Arc::new(DaemonState::with_data_dir_and_mcp_config(
        Arc::clone(&fixture.state.core),
        fixture.tempdir.path().join(data_dir),
        config,
    ));
    dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "incident_bulk_plan_update".to_string(),
            params,
            id: Some(json!(1)),
        },
        &state,
    )
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn direct_and_bridge_mcp_advertise_and_preserve_bulk_governance_errors_over_real_socket() {
    let fixture = crate::test_support::build_fixture_state()
        .await
        .expect("fixture state");
    let state = Arc::new(DaemonState::with_data_dir_and_mcp_config(
        Arc::clone(&fixture.state.core),
        fixture.tempdir.path().join("incident-bulk-mcp"),
        enabled_bulk_config(25),
    ));
    let socket = socket_path(&fixture.tempdir);
    let server = crate::rpc::JsonRpcServer::new(
        Arc::clone(&state),
        snow_core::ipc::IpcEndpoint::from_socket_path(&socket),
    );
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let local = LocalSet::new();
    local.spawn_local(async move {
        let _ = server
            .serve_until(async move {
                let _ = shutdown_rx.await;
                Ok(())
            })
            .await;
    });
    local
        .run_until(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let request = McpRequest {
                jsonrpc: "2.0".to_string(),
                method: "tools/call".to_string(),
                params: json!({
                    "name": "incident_bulk_plan_update",
                    "arguments": {"targets": (0..26).map(|index| json!({
                        "sys_id": format!("{index:032x}"),
                        "patch": {"state": "2"}
                    })).collect::<Vec<_>>()}
                }),
                id: Some(json!(41)),
            };
            let direct = McpServer::new(Arc::clone(&state.core))
                .dispatch(request.clone())
                .await;
            let bridge = DaemonBackedMcpBridge::from_socket(socket.clone())
                .dispatch(request)
                .await;
            assert_eq!(
                serde_json::to_vec(direct.error.as_ref().expect("direct error"))
                    .expect("direct error bytes"),
                serde_json::to_vec(bridge.error.as_ref().expect("bridge error"))
                    .expect("bridge error bytes")
            );
            assert_eq!(
                direct
                    .error
                    .as_ref()
                    .and_then(|error| error.data.as_ref())
                    .map(|data| &data["code"]),
                Some(&json!("TARGET_COUNT_INVALID"))
            );
            let direct_tools = McpServer::new(Arc::clone(&state.core))
                .dispatch(McpRequest {
                    jsonrpc: "2.0".to_string(),
                    method: "tools/list".to_string(),
                    params: json!({}),
                    id: Some(json!(42)),
                })
                .await
                .result
                .expect("direct tools");
            let bridge_tools = DaemonBackedMcpBridge::from_socket(socket.clone())
                .dispatch(McpRequest {
                    jsonrpc: "2.0".to_string(),
                    method: "tools/list".to_string(),
                    params: json!({}),
                    id: Some(json!(43)),
                })
                .await
                .result
                .expect("bridge tools");
            for name in ["incident_bulk_plan_update", "incident_bulk_apply_update"] {
                assert!(tool_list_has(&direct_tools, name), "direct missing {name}");
                assert!(tool_list_has(&bridge_tools, name), "bridge missing {name}");
            }
            let _ = shutdown_tx.send(());
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn ordinary_incident_apply_reports_post_patch_coherence_failure_and_never_retries_patch() {
    let instance = MockServer::start().await;
    let sys_id = "dddddddddddddddddddddddddddddddd";
    Mock::given(method("GET"))
        .and(path("/api/now/table/incident"))
        .and(query_param_contains("sysparm_query", "number=INC0018888"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": [{
                "sys_id": {"value": sys_id, "display_value": sys_id},
                "number": {"value": "INC0018888", "display_value": "INC0018888"},
                "sys_updated_on": {"value": "2026-08-20 14:00:00", "display_value": "2026-08-20 14:00:00"},
                "sys_mod_count": {"value": "3", "display_value": "3"}
            }]
        })))
        .expect(2)
        .mount(&instance)
        .await;
    Mock::given(method("PATCH"))
        .and(path(format!("/api/now/table/incident/{sys_id}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": {"sys_id": sys_id, "number": "INC0018888"}
        })))
        .expect(1)
        .mount(&instance)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/api/now/table/incident/{sys_id}")))
        .respond_with(ResponseTemplate::new(500).set_body_json(json!({
            "error": {"message": "post-write read unavailable"}
        })))
        .mount(&instance)
        .await;
    let fixture = crate::test_support::build_fixture_state_at_instance(&instance.uri())
        .await
        .expect("fixture");
    let state = Arc::new(DaemonState::with_data_dir_and_mcp_config(
        Arc::clone(&fixture.state.core),
        fixture.tempdir.path().join("incident-single-coherence"),
        enabled_single_config(),
    ));
    let plan = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "incident_plan_update".to_string(),
            params: json!({
                "number": "INC0018888",
                "comments": "Public-safe journal body"
            }),
            id: Some(json!(1)),
        },
        &state,
    )
    .await
    .result
    .expect("single plan");
    let apply = json!({
        "plan_id": plan["plan_id"],
        "confirmation_token": plan["confirmation_token"],
        "idempotency_key": plan["idempotency_key"],
        "concurrency_token": plan["concurrency_token"],
    });
    let response = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "incident_apply_update".to_string(),
            params: apply.clone(),
            id: Some(json!(2)),
        },
        &state,
    )
    .await;
    let error = response.error.expect("coherence failure");
    assert_eq!(error.code, -32046);
    let data = error.data.expect("coherence data");
    assert_eq!(data["code"], "LOCAL_COHERENCE_FAILED");
    assert_eq!(data["upstream_applied"], true);
    assert!(
        !serde_json::to_string(&data)
            .unwrap()
            .contains("Public-safe journal body")
    );
    let replay = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "incident_apply_update".to_string(),
            params: apply,
            id: Some(json!(3)),
        },
        &state,
    )
    .await;
    assert_eq!(replay.error.expect("pending replay").code, -32060);
}

fn tool_list_has(result: &Value, name: &str) -> bool {
    result["tools"]
        .as_array()
        .is_some_and(|tools| tools.iter().any(|tool| tool["name"] == name))
}

fn enabled_bulk_config(max_targets: u64) -> snow_mcp::McpConfig {
    let mut policy = snow_mcp::domain::policy::PolicyConfig::default();
    for tool in ["incident_bulk_plan_update", "incident_bulk_apply_update"] {
        let tool_policy = policy.tools.get_mut(tool).expect("bulk policy");
        tool_policy.enabled = true;
        tool_policy.environments = vec!["test".to_string()];
        tool_policy.max_targets = Some(max_targets);
    }
    snow_mcp::McpConfig {
        environment: snow_mcp::McpEnvironment::explicit_config("test", "America/Chicago"),
        policy,
        ..Default::default()
    }
}

fn enabled_single_config() -> snow_mcp::McpConfig {
    let mut config = snow_mcp::McpConfig {
        environment: snow_mcp::McpEnvironment::explicit_config("test", "America/Chicago"),
        ..Default::default()
    };
    for tool in ["incident_plan_update", "incident_apply_update"] {
        let policy = config.policy.tools.get_mut(tool).expect("Incident policy");
        policy.enabled = true;
        policy.environments = vec!["test".to_string()];
    }
    config
}

fn incident_list(number: &str, sys_id: &str, updated: &str) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(json!({
        "result": [{
            "number": {"value": number, "display_value": number},
            "sys_id": {"value": sys_id, "display_value": sys_id},
            "sys_updated_on": {"value": updated, "display_value": updated},
            "assignment_group": {"value": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "display_value": "Example Operations"}
        }]
    }))
}

fn incident_get(number: &str, sys_id: &str, updated: &str) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(json!({
        "result": {
            "number": {"value": number, "display_value": number},
            "sys_id": {"value": sys_id, "display_value": sys_id},
            "sys_updated_on": {"value": updated, "display_value": updated},
            "assignment_group": {"value": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "display_value": "Example Operations"}
        }
    }))
}

fn apply_params(bundle: &Value) -> Value {
    json!({
        "plan_id": bundle["plan_id"],
        "confirmation_token": bundle["confirmation_token"],
        "idempotency_key": bundle["idempotency_key"],
        "concurrency_tokens": bundle["preview"]["targets"]
            .as_array()
            .expect("planned targets")
            .iter()
            .map(|target| json!({
                "sys_id": target["target"]["sys_id"],
                "sys_updated_on": target["concurrency_token"]["sys_updated_on"],
            }))
            .collect::<Vec<_>>()
    })
}

fn seed_legacy_projection(
    root: &std::path::Path,
    number: &str,
    sys_id: &str,
) -> std::path::PathBuf {
    let now = Utc::now();
    let record = SnowRecord {
        sys_id: sys_id.to_string(),
        number: number.to_string(),
        table: "incident".to_string(),
        resource_type: ResourceType::Incident,
        state: "In Progress".to_string(),
        short_description: "Legacy local projection".to_string(),
        description: "Public-safe fixture".to_string(),
        fields: HashMap::new(),
        work_notes: Vec::new(),
        comments: Vec::new(),
        parent: None,
        children: Vec::new(),
        references: HashMap::new(),
        synced_at: now,
        source: CacheSource::Disk,
    };
    let persisted = VaultManager::new(root.join("vault"))
        .persist_record(&record)
        .expect("seed legacy vault document");
    let engine = QueryEngine::open(root.join("snow.db")).expect("open projection store");
    engine
        .store()
        .upsert_record(
            &RecordRow {
                sys_id: sys_id.to_string(),
                number: number.to_string(),
                table_name: "incident".to_string(),
                resource_type: ResourceType::Incident,
                state: Some("In Progress".to_string()),
                short_desc: Some("Legacy local projection".to_string()),
                description: Some("Public-safe fixture".to_string()),
                assigned_to: None,
                parent_id: None,
                file_path: Some(persisted.relative_path.to_string_lossy().into_owned()),
                synced_at: now,
                sys_updated_on: now,
                etag: None,
                in_scope: true,
                last_seen_at: now,
                tombstoned_at: None,
                pruned_at: None,
                raw_json: "{}".to_string(),
            },
            "",
            "Public-safe fixture",
        )
        .expect("seed projection row");
    engine
        .store()
        .replace_tags(
            sys_id,
            &[TagRow {
                record_sys_id: sys_id.to_string(),
                tag: "legacy-projection".to_string(),
                source: "fixture".to_string(),
                weight: 1.0,
            }],
        )
        .expect("seed derived tag index");
    persisted.path
}
