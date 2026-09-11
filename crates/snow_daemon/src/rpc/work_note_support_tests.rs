//! L0 JSON-RPC regression tests for governed work-note field support.
//!
//! These drive `work_note_plan_add` through the daemon dispatcher against a
//! local ServiceNow fake. The mutation they catch is skipping or weakening the
//! live `sys_dictionary` proof that the resolved record table supports
//! `work_notes`.

use std::sync::Arc;

use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::DaemonState;
use crate::rpc::{JsonRpcRequest, dispatch};
use crate::test_support::build_fixture_state_at_instance;
use crate::test_support::build_fixture_state_with_ui_metadata;

const STORY: &str = "STRY0010001";
const NOTE: &str = "Validated against the resolved record table.";

#[tokio::test(flavor = "current_thread")]
#[ignore = "cross-repository release gate; run Mullet npm run test:servicenow-e2e -- <snow checkout>"]
async fn daemon_story_note_end_to_end_via_mullet_stdio() {
    let mullet_root =
        std::env::var("MULLET_STORY_TEST_ROOT").expect("explicit Mullet checkout required");
    let node = std::env::var("MULLET_STORY_TEST_NODE").expect("explicit Node executable required");
    let (instance, journal) = story_instance().await;
    mount_story_metadata(&instance, story_metadata()).await;
    let fixture = build_fixture_state_with_ui_metadata(&instance.uri())
        .await
        .expect("fixture");
    let state = work_note_enabled_state(&fixture);
    let socket = fixture.tempdir.path().join("mcp-story.sock");
    tokio::task::LocalSet::new()
        .run_until(async {
            let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
            let rpc = crate::rpc::JsonRpcServer::new(
                state,
                snow_core::ipc::IpcEndpoint::filesystem(&socket),
            );
            let server = tokio::task::spawn_local(rpc.serve_until(async {
                let _ = stopped.await;
                Ok(())
            }));
            let outcome = tokio::time::timeout(std::time::Duration::from_secs(30), async {
                while !socket.exists() {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                tokio::process::Command::new(node)
                    .args(["--import", "tsx", "tests/helpers/story-mcp-driver.ts"])
                    .arg(&socket)
                    .current_dir(&mullet_root)
                    .kill_on_drop(true)
                    .output()
                    .await
            })
            .await;
            let _ = stop.send(());
            server
                .await
                .expect("server task")
                .expect("daemon transport");
            let output = outcome.expect("E2E deadline").expect("MCP driver process");
            assert!(
                output.status.success(),
                "MCP driver failed: {}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert_eq!(
                *journal.lock().expect("journal"),
                vec![NOTE],
                "the full path must append exactly once"
            );
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn daemon_story_note_never_retries_an_ambiguous_provider_append() {
    let (instance, journal) = story_instance().await;
    mount_story_metadata(&instance, story_metadata()).await;
    let writes = Arc::clone(&journal);
    Mock::given(method("PATCH"))
        .and(path("/api/now/table/rm_story/story-sys"))
        .respond_with(move |request: &wiremock::Request| {
            let body: serde_json::Value = request.body_json().expect("PATCH JSON");
            assert_eq!(body, json!({ "work_notes": NOTE }));
            writes.lock().expect("journal").push(NOTE.to_string());
            // The append committed, but the response failed. Retrying duplicates it.
            ResponseTemplate::new(503)
                .set_body_json(json!({ "error": { "message": "upstream response unavailable" } }))
        })
        .with_priority(1)
        .mount(&instance)
        .await;
    let fixture = build_fixture_state_with_ui_metadata(&instance.uri())
        .await
        .expect("fixture");
    let state = work_note_enabled_state(&fixture);
    let planned = plan_work_note(&state, STORY).await;
    let plan = planned.result.expect("valid plan");
    let result = apply_work_note(&state, &plan).await;
    assert_eq!(
        result.error.expect("uncertain outcome").message,
        "PENDING_RESOLUTION_REQUIRED"
    );
    assert_eq!(
        *journal.lock().expect("journal"),
        vec![NOTE],
        "an append must never be retried by HTTP transport"
    );
    let replay = apply_work_note(&state, &plan).await;
    assert_eq!(
        replay
            .error
            .expect("replay must not retry an uncertain write")
            .message,
        "PENDING_RESOLUTION_REQUIRED"
    );
    assert_eq!(*journal.lock().expect("journal"), vec![NOTE]);
}

// L0 governed-write seam: independent HTTP state models an append-only journal.
// A successful plan alone is not evidence of an update or of safe replay.
async fn story_instance() -> (MockServer, Arc<std::sync::Mutex<Vec<String>>>) {
    let server = MockServer::start().await;
    let journal = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let record = json!({ "sys_id": "story-sys", "number": STORY,
        "sys_class_name": "rm_story", "short_description": "Example project",
        "state": "1", "sys_updated_on": "2026-09-01 12:00:00" });
    Mock::given(method("GET"))
        .and(path("/api/now/table/rm_story"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({ "result": [record.clone()] })),
        )
        .mount(&server)
        .await;
    for table in ["sys_dictionary", "sys_db_object"] {
        Mock::given(method("GET"))
            .and(path(format!("/api/now/table/{table}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "result": [] })))
            .mount(&server)
            .await;
    }
    let read_journal = Arc::clone(&journal);
    Mock::given(method("GET"))
        .and(path("/api/now/table/rm_story"))
        .and(wiremock::matchers::query_param(
            "sysparm_query",
            "sys_id=story-sys",
        ))
        .respond_with(move |_: &wiremock::Request| {
            let blob = read_journal
                .lock()
                .expect("journal")
                .iter()
                .map(|text| format!("2026-09-01 12:00:00 - Example User (Work notes)\n{text}\n\n"))
                .collect::<String>();
            ResponseTemplate::new(200)
                .set_body_json(json!({ "result": [{ "sys_id": "story-sys", "work_notes": blob }] }))
        })
        .with_priority(1)
        .mount(&server)
        .await;
    let write_journal = Arc::clone(&journal);
    Mock::given(method("PATCH"))
        .and(path("/api/now/table/rm_story/story-sys"))
        .respond_with(move |request: &wiremock::Request| {
            let body: serde_json::Value = request.body_json().expect("PATCH JSON");
            assert_eq!(
                body,
                json!({ "work_notes": NOTE }),
                "no unrelated field may be written"
            );
            write_journal
                .lock()
                .expect("journal")
                .push(body["work_notes"].as_str().expect("note").to_string());
            ResponseTemplate::new(200).set_body_json(json!({ "result": record }))
        })
        .mount(&server)
        .await;
    (server, journal)
}

async fn mount_story_metadata(server: &MockServer, body: serde_json::Value) {
    Mock::given(method("GET"))
        .and(path("/api/now/ui/meta/rm_story"))
        .and(wiremock::matchers::basic_auth("tester", "secret"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(server)
        .await;
}

fn story_metadata() -> serde_json::Value {
    json!({ "result": { "columns": { "work_notes": {
        "name": "work_notes", "type": "journal_input", "internal_type": "journal_input", "read_only": false
    } } } })
}

async fn apply_work_note(
    state: &Arc<DaemonState>,
    plan: &serde_json::Value,
) -> crate::rpc::JsonRpcResponse {
    dispatch(JsonRpcRequest {
        jsonrpc: "2.0".to_string(), method: "work_note_apply_add".to_string(),
        params: json!({ "plan_id": plan["plan_id"], "confirmation_token": plan["confirmation_token"], "idempotency_key": plan["idempotency_key"] }),
        id: Some(json!(2)),
    }, state).await
}

#[tokio::test(flavor = "current_thread")]
async fn daemon_story_note_appends_once_and_replays_when_ui_metadata_proves_hidden_dictionary_field()
 {
    let (instance, journal) = story_instance().await;
    mount_story_metadata(&instance, story_metadata()).await;
    let fixture = build_fixture_state_with_ui_metadata(&instance.uri())
        .await
        .expect("fixture");
    let state = work_note_enabled_state(&fixture);
    let planned = plan_work_note(&state, STORY).await;
    assert!(
        planned.error.is_none(),
        "Story preflight must use live available metadata: {planned:?}"
    );
    let plan = planned.result.expect("plan");
    assert_eq!(plan["target"]["table"], json!("rm_story"));
    assert!(
        journal.lock().expect("journal").is_empty(),
        "planning must not append"
    );

    let applied = apply_work_note(&state, &plan).await;
    assert!(applied.error.is_none(), "{applied:?}");
    assert_eq!(
        applied.result.as_ref().expect("receipt")["status"],
        json!("success")
    );
    assert_eq!(*journal.lock().expect("journal"), vec![NOTE]);
    let replay = apply_work_note(&state, &plan).await;
    assert!(replay.error.is_none(), "{replay:?}");
    assert_eq!(
        replay.result.expect("replay receipt")["idempotency_replay"],
        json!(true)
    );
    assert_eq!(
        *journal.lock().expect("journal"),
        vec![NOTE],
        "replay must not append twice"
    );
    let readback = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "get_record".to_string(),
            params: json!({ "number": STORY }),
            id: Some(json!(3)),
        },
        &state,
    )
    .await;
    assert!(readback.error.is_none(), "{readback:?}");
    assert!(
        readback
            .result
            .expect("fresh Story")
            .to_string()
            .contains(NOTE),
        "fresh journal read must include the append"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn daemon_story_note_refuses_apply_if_live_ui_metadata_disappears_after_plan() {
    let (instance, journal) = story_instance().await;
    Mock::given(method("GET"))
        .and(path("/api/now/ui/meta/rm_story"))
        .and(wiremock::matchers::basic_auth("tester", "secret"))
        .respond_with(ResponseTemplate::new(200).set_body_json(story_metadata()))
        .up_to_n_times(1)
        .mount(&instance)
        .await;
    let fixture = build_fixture_state_with_ui_metadata(&instance.uri())
        .await
        .expect("fixture");
    let state = work_note_enabled_state(&fixture);
    let planned = plan_work_note(&state, STORY).await;
    assert!(planned.error.is_none(), "{planned:?}");
    let plan = planned.result.expect("plan");
    mount_story_metadata(&instance, json!({ "result": { "columns": {} } })).await;
    let applied = apply_work_note(&state, &plan).await;
    assert_eq!(
        applied
            .error
            .expect("live support must be rechecked")
            .message,
        "WORK_NOTES_DISCOVERY_UNAVAILABLE"
    );
    assert!(
        journal.lock().expect("journal").is_empty(),
        "refusal must precede PATCH"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn daemon_story_note_does_not_infer_support_from_missing_or_malformed_ui_columns() {
    for metadata in [
        json!({ "result": { "columns": {} } }),
        json!({ "result": { "columns": { "work_notes": null } } }),
        json!({ "result": { "columns": { "work_notes": {} } } }),
        json!({ "result": { "columns": { "work_notes": { "name": "description", "type": "string" } } } }),
        json!({ "result": [] }),
    ] {
        let (instance, journal) = story_instance().await;
        mount_story_metadata(&instance, metadata).await;
        let fixture = build_fixture_state_with_ui_metadata(&instance.uri())
            .await
            .expect("fixture");
        let state = work_note_enabled_state(&fixture);
        let planned = plan_work_note(&state, STORY).await;
        let error = planned
            .error
            .expect("malformed metadata must not issue write authority");
        assert_eq!(error.message, "WORK_NOTES_DISCOVERY_UNAVAILABLE");
        assert_eq!(
            error.data.expect("reason")["reason"],
            json!("not_returned_by_instance")
        );
        assert!(planned.result.is_none());
        assert!(journal.lock().expect("journal").is_empty());
    }
}

fn work_note_enabled_state(fixture: &crate::test_support::FixtureState) -> Arc<DaemonState> {
    let mut policy = snow_mcp::domain::policy::PolicyConfig::default();
    policy
        .tools
        .get_mut("work_note_apply_add")
        .expect("work-note policy")
        .enabled = true;

    Arc::new(DaemonState::with_data_dir_and_mcp_config(
        Arc::clone(&fixture.state.core),
        fixture.tempdir.path().join("work-note-field-support"),
        snow_mcp::McpConfig {
            environment: snow_mcp::McpEnvironment::explicit_config("test", "America/Chicago"),
            policy,
            ..Default::default()
        },
    ))
}

async fn plan_work_note(state: &Arc<DaemonState>, number: &str) -> crate::rpc::JsonRpcResponse {
    dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "work_note_plan_add".to_string(),
            params: json!({
                "number": number,
                "work_notes": "Validated against the resolved record table."
            }),
            id: Some(json!(1)),
        },
        state,
    )
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn daemon_rpc_work_note_plan_add_succeeds_when_resolved_table_supports_work_notes() {
    let instance = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_dictionary"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": [{
                "name": "change_request",
                "element": "work_notes",
                "internal_type": "journal_input",
                "read_only": "false",
                "choice": "0",
                "active": "true"
            }]
        })))
        .expect(1)
        .mount(&instance)
        .await;
    mount_no_ancestors(&instance, "change_request").await;
    let fixture = build_fixture_state_at_instance(&instance.uri())
        .await
        .expect("fixture");
    let state = work_note_enabled_state(&fixture);

    let response = plan_work_note(&state, "CHG001").await;

    assert!(response.error.is_none(), "{response:?}");
    let result = response.result.expect("plan");
    assert_eq!(result["target"]["table"], json!("change_request"));
    assert_eq!(
        result["preview"]["work_notes"],
        json!("Validated against the resolved record table.")
    );
    assert!(result["confirmation_token"].is_string());
    assert!(result["idempotency_key"].is_string());
}

#[tokio::test(flavor = "current_thread")]
async fn daemon_rpc_work_note_plan_add_does_not_require_ancestry_when_target_defines_work_notes() {
    let instance = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_dictionary"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": [{
                "name": "change_request",
                "element": "work_notes",
                "internal_type": "journal_input",
                "active": "true"
            }]
        })))
        .expect(1)
        .mount(&instance)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_db_object"))
        .respond_with(ResponseTemplate::new(403).set_body_json(json!({
            "error": { "message": "Access denied" }
        })))
        .expect(0)
        .mount(&instance)
        .await;
    let fixture = build_fixture_state_at_instance(&instance.uri())
        .await
        .expect("fixture");
    let state = work_note_enabled_state(&fixture);

    let response = plan_work_note(&state, "CHG001").await;

    assert!(response.error.is_none(), "{response:?}");
    assert!(response.result.expect("plan")["confirmation_token"].is_string());
}

#[tokio::test(flavor = "current_thread")]
async fn daemon_rpc_work_note_plan_add_accepts_visible_work_notes_when_descriptor_type_is_hidden() {
    let instance = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_dictionary"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": [{
                "name": "change_request",
                "element": "work_notes",
                "active": "true"
            }]
        })))
        .expect(1)
        .mount(&instance)
        .await;
    mount_no_ancestors(&instance, "change_request").await;
    let fixture = build_fixture_state_at_instance(&instance.uri())
        .await
        .expect("fixture");
    let state = work_note_enabled_state(&fixture);

    let response = plan_work_note(&state, "CHG001").await;

    assert!(response.error.is_none(), "{response:?}");
    assert!(response.result.expect("plan")["confirmation_token"].is_string());
}

#[tokio::test(flavor = "current_thread")]
async fn daemon_rpc_work_note_plan_add_refuses_when_resolved_table_lacks_work_notes() {
    let instance = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_dictionary"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": [{
                "name": "resource_plan",
                "element": "short_description",
                "internal_type": "string",
                "read_only": "false",
                "choice": "0",
                "active": "true"
            }]
        })))
        .mount(&instance)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_db_object"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": [{
                "name": "resource_plan",
                "super_class": ""
            }]
        })))
        .mount(&instance)
        .await;
    let fixture = build_fixture_state_at_instance(&instance.uri())
        .await
        .expect("fixture");
    let state = work_note_enabled_state(&fixture);

    let response = plan_work_note(&state, "RPLN001").await;

    let error = response.error.expect("unsupported table must be refused");
    assert_eq!(error.code, -32053);
    assert_eq!(error.message, "WORK_NOTES_UNSUPPORTED");
    assert_eq!(
        error.data.expect("typed error data"),
        json!({
            "code": "WORK_NOTES_UNSUPPORTED",
            "field": "work_notes",
            "table": "resource_plan"
        })
    );
}

#[tokio::test(flavor = "current_thread")]
async fn daemon_rpc_work_note_plan_add_refuses_when_field_discovery_is_unavailable() {
    let instance = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_dictionary"))
        .respond_with(ResponseTemplate::new(403).set_body_json(json!({
            "error": { "message": "Access denied" }
        })))
        .mount(&instance)
        .await;
    let fixture = build_fixture_state_at_instance(&instance.uri())
        .await
        .expect("fixture");
    let state = work_note_enabled_state(&fixture);

    let response = plan_work_note(&state, "CHG001").await;

    let error = response
        .error
        .expect("unavailable discovery must be refused");
    assert_eq!(error.code, -32054);
    assert_eq!(error.message, "WORK_NOTES_DISCOVERY_UNAVAILABLE");
    assert_eq!(
        error.data.expect("typed error data"),
        json!({
            "code": "WORK_NOTES_DISCOVERY_UNAVAILABLE",
            "field": "work_notes",
            "reason": "acl_denied",
            "table": "change_request"
        })
    );
}

#[tokio::test(flavor = "current_thread")]
async fn daemon_rpc_work_note_apply_add_rechecks_field_discovery_before_writing() {
    let instance = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_dictionary"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": [{
                "name": "change_request",
                "element": "work_notes",
                "internal_type": "journal_input",
                "read_only": "false",
                "choice": "0",
                "active": "true"
            }]
        })))
        .up_to_n_times(1)
        .expect(1)
        .mount(&instance)
        .await;
    mount_no_ancestors(&instance, "change_request").await;
    let fixture = build_fixture_state_at_instance(&instance.uri())
        .await
        .expect("fixture");
    let state = work_note_enabled_state(&fixture);

    let plan = plan_work_note(&state, "CHG001").await;
    let result = plan.result.expect("work-note plan");

    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_dictionary"))
        .respond_with(ResponseTemplate::new(403).set_body_json(json!({
            "error": { "message": "Access denied" }
        })))
        .expect(1)
        .mount(&instance)
        .await;

    let response = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "work_note_apply_add".to_string(),
            params: json!({
                "plan_id": result["plan_id"],
                "confirmation_token": result["confirmation_token"],
                "idempotency_key": result["idempotency_key"]
            }),
            id: Some(json!(2)),
        },
        &state,
    )
    .await;

    let error = response
        .error
        .expect("apply must stop before the ServiceNow write");
    assert_eq!(error.code, -32054);
    assert_eq!(error.message, "WORK_NOTES_DISCOVERY_UNAVAILABLE");
    assert_eq!(
        error.data.expect("typed error data")["code"],
        json!("WORK_NOTES_DISCOVERY_UNAVAILABLE")
    );
}

async fn mount_no_ancestors(server: &MockServer, table: &str) {
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_db_object"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": [{ "name": table, "super_class": "" }]
        })))
        .mount(server)
        .await;
}
