//! T-OPS-04 L0 governed-write seam: compiled CLI -> real daemon socket -> local ServiceNow/state-store fakes.
#![cfg(unix)]

use std::collections::BTreeSet;
use std::fs;
use std::io::Write;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use servicenow_rs::prelude::{BasicAuth, ServiceNowClient};
use snow_core::SnowCore;
use snow_core::config::{
    DaemonConfig as CoreDaemonConfig, InstanceConfig, SnowConfig, VaultConfig,
};
use snow_core::credential::CredentialProvider;
use snow_core::ipc::IpcEndpoint;
use snow_daemon::{DaemonState, rpc::JsonRpcServer};
use tokio::sync::oneshot;
use tokio::task::LocalSet;
use wiremock::matchers::{method, path, query_param_contains};
use wiremock::{Mock, MockServer, ResponseTemplate};

const SYS_IDS: [&str; 3] = [
    "00000000000000000000000000000001",
    "00000000000000000000000000000002",
    "00000000000000000000000000000003",
];

#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::arc_with_non_send_sync)] // Production daemon API uses Arc on a LocalSet.
async fn compiled_cli_plans_a_canonical_governed_incident_bulk_update_over_a_real_socket() {
    let instance = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_choice"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": [{"value": "2", "label": "In Progress", "inactive": "false"}]
        })))
        .mount(&instance)
        .await;
    for (index, sys_id) in SYS_IDS.iter().enumerate() {
        let number = format!("INC001234{}", index + 5);
        Mock::given(method("GET"))
            .and(path("/api/now/table/incident"))
            .and(query_param_contains("sysparm_query", format!("number={number}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "result": [{
                    "sys_id": {"value": sys_id, "display_value": sys_id},
                    "number": {"value": number, "display_value": number},
                    "sys_updated_on": {"value": format!("2026-08-20 12:0{index}:00"), "display_value": format!("2026-08-20 12:0{index}:00")}
                }]
            })))
            .expect(1)
            .mount(&instance)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/api/now/table/incident/{sys_id}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "result": {
                    "sys_id": {"value": sys_id, "display_value": sys_id},
                    "number": {"value": number, "display_value": number},
                    "sys_updated_on": {"value": format!("2026-08-20 12:0{index}:00"), "display_value": format!("2026-08-20 12:0{index}:00")},
                    "assignment_group": {"value": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "display_value": "Example Operations"}
                }
            })))
            .expect(2)
            .mount(&instance)
            .await;
        Mock::given(method("PATCH"))
            .and(path(format!("/api/now/table/incident/{sys_id}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "result": {
                    "sys_id": sys_id,
                    "number": number,
                    "sys_updated_on": format!("2026-08-20 12:1{index}:00")
                }
            })))
            .expect(1)
            .mount(&instance)
            .await;
    }
    let single_sys_id = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
    Mock::given(method("GET"))
        .and(path("/api/now/table/incident"))
        .and(query_param_contains("sysparm_query", "number=INC0019999"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": [{
                "sys_id": {"value": single_sys_id, "display_value": single_sys_id},
                "number": {"value": "INC0019999", "display_value": "INC0019999"},
                "sys_updated_on": {"value": "2026-08-20 13:00:00", "display_value": "2026-08-20 13:00:00"},
                "sys_mod_count": {"value": "7", "display_value": "7"}
            }]
        })))
        .expect(2..=3)
        .mount(&instance)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/api/now/table/incident/{single_sys_id}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": {
                "sys_id": {"value": single_sys_id, "display_value": single_sys_id},
                "number": {"value": "INC0019999", "display_value": "INC0019999"},
                "sys_updated_on": {"value": "2026-08-20 13:01:00", "display_value": "2026-08-20 13:01:00"},
                "sys_mod_count": {"value": "8", "display_value": "8"}
            }
        })))
        .expect(1)
        .mount(&instance)
        .await;
    Mock::given(method("PATCH"))
        .and(path(format!("/api/now/table/incident/{single_sys_id}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": {"sys_id": single_sys_id, "number": "INC0019999"}
        })))
        .expect(1)
        .mount(&instance)
        .await;

    let home = tempfile::tempdir().expect("temporary home");
    let config_dir = home.path().join(".config/snow");
    fs::create_dir_all(&config_dir).expect("config directory");
    let socket = config_dir.join("daemon.sock");
    let vault = config_dir.join("vault");
    fs::create_dir_all(&vault).expect("vault directory");
    let request_path = home.path().join("bulk-request.json");
    fs::write(
        &request_path,
        serde_json::to_vec(&json!({
            "shared_patch": {"assignment_group": "0123456789abcdef0123456789abcdef"},
            "targets": [
                {"number": "INC0012347"},
                {"number": "INC0012345", "patch": {"state": "2"}},
                {"number": "INC0012346", "patch": {"work_notes": "Generic operator note"}}
            ]
        }))
        .expect("request JSON"),
    )
    .expect("write request fixture");
    let single_request_path = home.path().join("incident-request.json");
    fs::write(
        &single_request_path,
        serde_json::to_vec(&json!({
            "number": "INC0019999",
            "comments": "Public-safe single Incident journal body"
        }))
        .expect("single request JSON"),
    )
    .expect("write single request fixture");

    let client = ServiceNowClient::builder()
        .instance(instance.uri())
        .allow_http()
        .auth(BasicAuth::new("user@example.com", "test-password"))
        .build()
        .await
        .expect("local ServiceNow client");
    let write_client = ServiceNowClient::builder()
        .instance(instance.uri())
        .allow_http()
        .auth(BasicAuth::new("user@example.com", "test-password"))
        .max_retries(0)
        .build()
        .await
        .expect("local no-retry ServiceNow write client");
    let mut config = SnowConfig {
        instance: InstanceConfig {
            url: instance.uri(),
            user: "user@example.com".to_string(),
            credential: CredentialProvider::Env,
            portal: String::new(),
        },
        vault: VaultConfig {
            path: vault.clone(),
        },
        daemon: CoreDaemonConfig {
            socket_path: socket.clone(),
            mcp_transport: "disabled".to_string(),
        },
        ..Default::default()
    };
    config.apply_defaults();
    let core = SnowCore::builder()
        .config(config)
        .client(client)
        .write_client(write_client)
        .vault_path(vault)
        .build()
        .await
        .expect("Snow core");
    let policy_path = config_dir.join("mcp-policy.toml");
    fs::write(
        &policy_path,
        r#"
[mcp]

[mcp.tools.incident_bulk_plan_update]
enabled = true
requires_confirmation = true
requires_kb_evidence = false
field_allowlist = ["assigned_to", "assignment_group", "state", "work_notes", "comments"]
environments = ["test"]
confirmation_ttl_seconds = 600
max_targets = 25

[mcp.tools.incident_bulk_apply_update]
enabled = true
requires_confirmation = true
requires_kb_evidence = false
field_allowlist = ["assigned_to", "assignment_group", "state", "work_notes", "comments"]
environments = ["test"]
confirmation_ttl_seconds = 600
max_targets = 25

[mcp.tools.incident_plan_update]
enabled = true
requires_confirmation = true
requires_kb_evidence = false
field_allowlist = ["assigned_to", "assignment_group", "state", "work_notes", "comments"]
environments = ["test"]
confirmation_ttl_seconds = 600

[mcp.tools.incident_apply_update]
enabled = true
requires_confirmation = true
requires_kb_evidence = false
field_allowlist = ["assigned_to", "assignment_group", "state", "work_notes", "comments"]
environments = ["test"]
confirmation_ttl_seconds = 600
"#,
    )
    .expect("write MCP policy");
    // SAFETY: this integration test owns daemon construction and removes both
    // variables immediately after the synchronous constructor reads them.
    unsafe {
        std::env::set_var("SNOW_ENV", "test");
        std::env::set_var("SNOW_MCP_POLICY_PATH", &policy_path);
    }
    let core = Arc::new(core);
    let inspection_core = Arc::clone(&core);
    let daemon_state = DaemonState::new(core);
    // SAFETY: see the scoped setup above.
    unsafe {
        std::env::remove_var("SNOW_MCP_POLICY_PATH");
        std::env::remove_var("SNOW_ENV");
    }
    let server = JsonRpcServer::new(
        Arc::new(daemon_state),
        IpcEndpoint::from_socket_path(&socket),
    )
    .with_idle_timeout(None);
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let local = LocalSet::new();
    local.spawn_local(async move {
        server
            .serve_until(async move {
                let _ = shutdown_rx.await;
                Ok(())
            })
            .await
            .expect("daemon server");
    });

    let home_path = home.path().to_path_buf();
    let request = request_path.to_string_lossy().into_owned();
    let single_request = single_request_path.to_string_lossy().into_owned();
    let plan_inspection_core = Arc::clone(&inspection_core);
    let snow = env!("CARGO_BIN_EXE_snow");
    let (output, stale_output, apply_output, replay_output, single_plan, single_apply, single_race) =
        local
            .run_until(async move {
                wait_for_daemon(&socket).await;
                let plan_home = home_path.clone();
                let output = tokio::task::spawn_blocking(move || {
                    Command::new(snow)
                        .args([
                            "--env",
                            "test",
                            "incident",
                            "bulk-update",
                            "--request",
                            &request,
                            "--json",
                        ])
                        .env("HOME", plan_home)
                        .env("SNOW_ENV", "test")
                        .output()
                        .expect("run compiled snow CLI")
                })
                .await
                .expect("CLI task");
                let bundle: Value = serde_json::from_slice(&output.stdout).expect("plan bundle");
                let interactive_home = home_path.clone();
                let interactive_plan =
                    serde_json::to_vec(&bundle).expect("interactive stdin plan JSON");
                let interactive = tokio::task::spawn_blocking(move || {
                    let mut child = Command::new(snow)
                        .args([
                            "--env",
                            "test",
                            "incident",
                            "bulk-update",
                            "--plan",
                            "-",
                            "--apply",
                            "--json",
                        ])
                        .env("HOME", interactive_home)
                        .env("SNOW_ENV", "test")
                        .stdin(Stdio::piped())
                        .stdout(Stdio::piped())
                        .stderr(Stdio::piped())
                        .spawn()
                        .expect("spawn compiled interactive stdin CLI");
                    child
                        .stdin
                        .take()
                        .expect("interactive stdin pipe")
                        .write_all(&interactive_plan)
                        .expect("write interactive stdin plan");
                    child.wait_with_output().expect("interactive CLI output")
                })
                .await
                .expect("interactive CLI task");
                assert!(!interactive.status.success());
                let interactive_stderr = String::from_utf8_lossy(&interactive.stderr);
                assert!(interactive_stderr.contains("INC0012345"));
                assert!(interactive_stderr.contains(
                    "interactive confirmation requires a controlling terminal; use --yes"
                ));
                let plan_path = home_path.join("bulk-plan.json");
                fs::write(
                    &plan_path,
                    serde_json::to_vec(&bundle).expect("plan bundle JSON"),
                )
                .expect("save plan bundle");
                let mut stale_bundle = bundle.clone();
                stale_bundle["preview"]["targets"][2]["concurrency_token"]["sys_updated_on"] =
                    json!("2026-08-20 12:59:59");
                let stale_path = home_path.join("stale-plan.json");
                fs::write(
                    &stale_path,
                    serde_json::to_vec(&stale_bundle).expect("stale bundle JSON"),
                )
                .expect("save stale bundle");
                let stale_home = home_path.clone();
                let stale_plan = stale_path.to_string_lossy().into_owned();
                let stale_output = tokio::task::spawn_blocking(move || {
                    Command::new(snow)
                        .args([
                            "--env",
                            "test",
                            "incident",
                            "bulk-update",
                            "--plan",
                            &stale_plan,
                            "--apply",
                            "--yes",
                            "--json",
                        ])
                        .env("HOME", stale_home)
                        .env("SNOW_ENV", "test")
                        .output()
                        .expect("run stale compiled apply CLI")
                })
                .await
                .expect("stale CLI task");
                let apply_home = home_path.clone();
                let plan_input = serde_json::to_vec(&bundle).expect("stdin plan bundle JSON");
                let apply_output = tokio::task::spawn_blocking(move || {
                    let mut child = Command::new(snow)
                        .args([
                            "--env",
                            "test",
                            "incident",
                            "bulk-update",
                            "--plan",
                            "-",
                            "--apply",
                            "--yes",
                            "--json",
                        ])
                        .env("HOME", apply_home)
                        .env("SNOW_ENV", "test")
                        .stdin(Stdio::piped())
                        .stdout(Stdio::piped())
                        .stderr(Stdio::piped())
                        .spawn()
                        .expect("spawn compiled stdin apply CLI");
                    child
                        .stdin
                        .take()
                        .expect("stdin pipe")
                        .write_all(&plan_input)
                        .expect("write stdin plan bundle");
                    child.wait_with_output().expect("run compiled apply CLI")
                })
                .await
                .expect("apply CLI task");
                let replay_home = home_path.clone();
                let replay_plan = plan_path.to_string_lossy().into_owned();
                let replay_output = tokio::task::spawn_blocking(move || {
                    Command::new(snow)
                        .args([
                            "--env",
                            "test",
                            "incident",
                            "bulk-update",
                            "--plan",
                            &replay_plan,
                            "--apply",
                            "--yes",
                            "--json",
                        ])
                        .env("HOME", replay_home)
                        .env("SNOW_ENV", "test")
                        .output()
                        .expect("run compiled replay CLI")
                })
                .await
                .expect("replay CLI task");
                let single_home = home_path.clone();
                let single_plan = tokio::task::spawn_blocking(move || {
                    Command::new(snow)
                        .args([
                            "--env",
                            "test",
                            "incident",
                            "update",
                            "--request",
                            &single_request,
                            "--json",
                        ])
                        .env("HOME", single_home)
                        .env("SNOW_ENV", "test")
                        .output()
                        .expect("run compiled single plan CLI")
                })
                .await
                .expect("single plan CLI task");
                let single_bundle: Value =
                    serde_json::from_slice(&single_plan.stdout).expect("single plan bundle");
                assert!(
                    plan_inspection_core
                        .get_record("INC0019999")
                        .await
                        .expect("single plan live-only lookup")
                        .is_none(),
                    "single Incident planning must not create a local projection"
                );
                let single_plan_path = home_path.join("incident-plan.json");
                fs::write(
                    &single_plan_path,
                    serde_json::to_vec(&single_bundle).expect("single plan bundle JSON"),
                )
                .expect("save single plan bundle");
                let single_apply_home = home_path.clone();
                let single_plan_arg = single_plan_path.to_string_lossy().into_owned();
                let single_race_home = home_path.clone();
                let single_race_plan = single_plan_path.to_string_lossy().into_owned();
                let single_apply_task = tokio::task::spawn_blocking(move || {
                    Command::new(snow)
                        .args([
                            "--env",
                            "test",
                            "incident",
                            "update",
                            "--plan",
                            &single_plan_arg,
                            "--apply",
                            "--yes",
                            "--json",
                        ])
                        .env("HOME", single_apply_home)
                        .env("SNOW_ENV", "test")
                        .output()
                        .expect("run compiled single apply CLI")
                });
                let single_race_task = tokio::task::spawn_blocking(move || {
                    Command::new(snow)
                        .args([
                            "--env",
                            "test",
                            "incident",
                            "update",
                            "--plan",
                            &single_race_plan,
                            "--apply",
                            "--yes",
                            "--json",
                        ])
                        .env("HOME", single_race_home)
                        .env("SNOW_ENV", "test")
                        .output()
                        .expect("run concurrent compiled single apply CLI")
                });
                let (single_apply, single_race) = tokio::join!(single_apply_task, single_race_task);
                let single_apply = single_apply.expect("single apply CLI task");
                let single_race = single_race.expect("single race CLI task");
                let _ = shutdown_tx.send(());
                (
                    output,
                    stale_output,
                    apply_output,
                    replay_output,
                    single_plan,
                    single_apply,
                    single_race,
                )
            })
            .await;

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let actual: Value = serde_json::from_slice(&output.stdout).expect("CLI plan JSON");
    assert_eq!(actual["apply_tool"], json!("incident_bulk_apply_update"));
    assert_eq!(actual["op_hash"].as_str().map(str::len), Some(64));
    assert_eq!(
        actual["preview"],
        json!({
            "targets": [
                {
                    "target": {"number": "INC0012345", "sys_id": SYS_IDS[0]},
                    "patch": {"assignment_group": "0123456789abcdef0123456789abcdef", "state": "2"},
                    "concurrency_token": {"sys_updated_on": "2026-08-20 12:00:00"}
                },
                {
                    "target": {"number": "INC0012346", "sys_id": SYS_IDS[1]},
                    "patch": {"assignment_group": "0123456789abcdef0123456789abcdef", "work_notes": "Generic operator note"},
                    "concurrency_token": {"sys_updated_on": "2026-08-20 12:01:00"}
                },
                {
                    "target": {"number": "INC0012347", "sys_id": SYS_IDS[2]},
                    "patch": {"assignment_group": "0123456789abcdef0123456789abcdef"},
                    "concurrency_token": {"sys_updated_on": "2026-08-20 12:02:00"}
                }
            ]
        })
    );
    assert!(actual["plan_id"].as_str().is_some());
    assert!(actual["confirmation_token"].as_str().is_some());
    assert!(actual["idempotency_key"].as_str().is_some());
    assert!(actual["expires_at"].as_str().is_some());
    assert_eq!(
        BTreeSet::from_iter(actual.as_object().expect("plan object").keys().cloned()),
        BTreeSet::from([
            "apply_tool".to_string(),
            "confirmation_token".to_string(),
            "expires_at".to_string(),
            "idempotency_key".to_string(),
            "op_hash".to_string(),
            "plan_id".to_string(),
            "preview".to_string(),
        ])
    );
    assert!(!stale_output.status.success());
    assert!(
        String::from_utf8_lossy(&stale_output.stderr).contains("CONCURRENCY_TOKEN_INVALID"),
        "stale stderr: {}",
        String::from_utf8_lossy(&stale_output.stderr)
    );
    assert!(
        apply_output.status.success(),
        "apply stderr: {}",
        String::from_utf8_lossy(&apply_output.stderr)
    );
    let receipt: Value = serde_json::from_slice(&apply_output.stdout).expect("CLI receipt JSON");
    assert_eq!(receipt["status"], json!("success"));
    assert_eq!(receipt["applied_count"], json!(3));
    assert_eq!(receipt["failed_count"], json!(0));
    assert_eq!(receipt["not_attempted_count"], json!(0));
    assert_eq!(receipt["cache_coherent"], json!(true));
    assert_eq!(
        BTreeSet::from_iter(receipt.as_object().expect("receipt object").keys().cloned()),
        BTreeSet::from([
            "applied_count".to_string(),
            "apply_started_at".to_string(),
            "audit_id".to_string(),
            "cache_coherent".to_string(),
            "completed_at".to_string(),
            "failed_count".to_string(),
            "idempotency_replay".to_string(),
            "not_attempted_count".to_string(),
            "op_hash".to_string(),
            "parent_audit_id".to_string(),
            "plan_id".to_string(),
            "status".to_string(),
            "target_results".to_string(),
            "tool".to_string(),
        ])
    );
    assert_eq!(
        receipt["target_results"]
            .as_array()
            .expect("target results")
            .iter()
            .map(|result| result["status"].as_str().unwrap_or_default())
            .collect::<Vec<_>>(),
        vec!["applied", "applied", "applied"]
    );
    for target in receipt["target_results"]
        .as_array()
        .expect("target results")
    {
        assert_eq!(
            BTreeSet::from_iter(target.as_object().expect("target object").keys().cloned()),
            BTreeSet::from([
                "changed_fields".to_string(),
                "number".to_string(),
                "observed_sys_updated_on".to_string(),
                "status".to_string(),
                "sys_id".to_string(),
            ])
        );
        assert!(
            !target["observed_sys_updated_on"]
                .as_str()
                .unwrap_or_default()
                .is_empty()
        );
        let changes = target["changed_fields"].as_array().expect("changed fields");
        assert!(!changes.is_empty());
        for change in changes {
            assert_eq!(
                BTreeSet::from_iter(change.as_object().expect("change object").keys().cloned()),
                BTreeSet::from([
                    "after_hash".to_string(),
                    "before_hash".to_string(),
                    "field".to_string(),
                ])
            );
            assert_eq!(change["before_hash"].as_str().map(str::len), Some(64));
            assert_eq!(change["after_hash"].as_str().map(str::len), Some(64));
        }
    }
    let receipt_text = serde_json::to_string(&receipt).expect("receipt text");
    assert!(!receipt_text.contains("Generic operator note"));
    assert!(!receipt_text.contains("record_snapshot"));
    assert!(!receipt_text.contains("record_url"));
    assert!(
        replay_output.status.success(),
        "replay stderr: {}",
        String::from_utf8_lossy(&replay_output.stderr)
    );
    let replay: Value =
        serde_json::from_slice(&replay_output.stdout).expect("CLI replay receipt JSON");
    assert_eq!(replay["idempotency_replay"], json!(true));
    assert_eq!(replay["audit_id"], receipt["audit_id"]);
    for number in ["INC0012345", "INC0012346", "INC0012347"] {
        assert!(
            inspection_core
                .get_record(number)
                .await
                .expect("live-only post-write lookup")
                .is_none(),
            "successful apply must not create a local projection for {number}"
        );
    }
    assert!(
        single_plan.status.success(),
        "single plan stderr: {}",
        String::from_utf8_lossy(&single_plan.stderr)
    );
    let successful_single = [&single_apply, &single_race]
        .into_iter()
        .find(|output| output.status.success())
        .expect("one concurrent single apply succeeds");
    let loser = if std::ptr::eq(successful_single, &single_apply) {
        &single_race
    } else {
        &single_apply
    };
    assert!(
        loser.status.success()
            || String::from_utf8_lossy(&loser.stderr).contains("PENDING_RESOLUTION_REQUIRED")
            || String::from_utf8_lossy(&loser.stderr)
                .contains("CONFIRMATION_INVALID: {\"code\":\"CONFIRMATION_INVALID\",\"reason\":\"token already consumed\"}"),
        "safe concurrent loser stderr: {}",
        String::from_utf8_lossy(&loser.stderr)
    );
    let single_receipt: Value =
        serde_json::from_slice(&successful_single.stdout).expect("single receipt JSON");
    let single_receipt_text = serde_json::to_string(&single_receipt).expect("single receipt text");
    assert!(!single_receipt_text.contains("Public-safe single Incident journal body"));
    assert_eq!(single_receipt["record_url"], Value::Null);
    assert_eq!(single_receipt["record_snapshot"], Value::Null);
    assert!(
        inspection_core
            .get_record("INC0019999")
            .await
            .expect("single live-only post-write lookup")
            .is_none()
    );
    let audit_db = config_dir.join("mcp_story_write.sqlite3");
    let event_json: String = rusqlite::Connection::open(audit_db)
        .expect("open single audit store")
        .query_row(
            "SELECT event_json FROM mcp_audit_events WHERE audit_id = ?1",
            [single_receipt["audit_id"]
                .as_str()
                .expect("single audit id")],
            |row| row.get(0),
        )
        .expect("durable single outcome audit");
    assert!(!event_json.contains("Public-safe single Incident journal body"));
}

async fn wait_for_daemon(socket: &std::path::Path) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if socket.exists() {
            return;
        }
        assert!(Instant::now() < deadline, "daemon socket was not created");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
