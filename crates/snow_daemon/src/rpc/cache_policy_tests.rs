//! L0 daemon JSON-RPC seam for B-OPS-06 policy lifecycle behavior.

use serde_json::{Value, json};

use super::{JsonRpcRequest, dispatch};
use crate::test_support::build_fixture_state;

async fn call(
    state: &std::sync::Arc<crate::DaemonState>,
    method: &str,
    params: Value,
) -> super::JsonRpcResponse {
    dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: method.to_string(),
            params,
            id: Some(json!(7)),
        },
        state,
    )
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn daemon_rpc_validates_independently_authored_builtin_policy() {
    let fixture = build_fixture_state().await.expect("fixture");
    let response = call(&fixture.state, "cache_policy_validate", json!({})).await;
    assert!(response.error.is_none(), "unexpected RPC error");
    assert_eq!(
        response.result,
        Some(json!({
            "version": 1,
            "source": "built_in_defaults",
            "rule_count": 3,
            "fingerprint": "16701a2ecc0bc31d0a7c95d56107ab23ee5328e2606bafcc8ef86a0e06d29b28"
        }))
    );
}

#[tokio::test(flavor = "current_thread")]
async fn daemon_rpc_rejects_unknown_params_and_invalid_policy_without_replacing_snapshot() {
    let fixture = build_fixture_state().await.expect("fixture");
    let params_error = call(
        &fixture.state,
        "cache_policy_validate",
        json!({"path":"elsewhere.toml"}),
    )
    .await;
    assert_eq!(params_error.error.expect("error").code, -32602);

    let path = fixture.tempdir.path().join("cache-policy.toml");
    std::fs::write(&path, "version = 1\n[objects.server]\nmode = \"live\"\n")
        .expect("valid policy");
    let first = call(&fixture.state, "cache_policy_reload", json!({}))
        .await
        .result
        .expect("reload result");
    let accepted = first["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    assert_eq!(first["changed"], true);

    std::fs::write(
        &path,
        "version = 1\n[objects.server]\nmode = \"cache_only\"\nttl = \"24h\"\n",
    )
    .expect("different valid candidate");
    let validated = call(&fixture.state, "cache_policy_validate", json!({}))
        .await
        .result
        .expect("validate result");
    assert_ne!(validated["fingerprint"], accepted);

    std::fs::write(&path, "version = 1\n[objects.*]\nmode = \"live\"\n").expect("invalid policy");
    let rejected = call(&fixture.state, "cache_policy_reload", json!({})).await;
    assert_eq!(rejected.error.expect("invalid error").code, -32070);

    std::fs::remove_file(path).expect("remove invalid policy");
    let restored = call(&fixture.state, "cache_policy_reload", json!({}))
        .await
        .result
        .expect("restore result");
    assert_eq!(restored["previous_fingerprint"], accepted);
    assert_eq!(
        restored["fingerprint"],
        "16701a2ecc0bc31d0a7c95d56107ab23ee5328e2606bafcc8ef86a0e06d29b28"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn daemon_rpc_ttl_parsing_is_unicode_safe_and_legacy_compatible() {
    let fixture = build_fixture_state().await.expect("fixture");
    let path = fixture.tempdir.path().join("cache-policy.toml");
    std::fs::write(
        &path,
        "version = 1\n[objects.server]\nmode = \"cache_only\"\nttl = \"24h\"\n",
    )
    .expect("initial policy");
    let first = call(&fixture.state, "cache_policy_reload", json!({}))
        .await
        .result
        .expect("initial reload");
    let accepted = first["fingerprint"]
        .as_str()
        .expect("accepted fingerprint")
        .to_string();

    std::fs::write(
        &path,
        "version = 1\n[objects.server]\nmode = \"cache_only\"\nttl = \"60日\"\n",
    )
    .expect("non-ascii policy");
    let validation_error = call(&fixture.state, "cache_policy_validate", json!({}))
        .await
        .error
        .expect("non-ascii validation rejection");
    assert_eq!(validation_error.code, -32070);
    let reload_error = call(&fixture.state, "cache_policy_reload", json!({}))
        .await
        .error
        .expect("non-ascii reload rejection");
    assert_eq!(reload_error.code, -32070);

    std::fs::write(
        &path,
        "version = 1\n[objects.server]\nmode = \"cache_only\"\nttl = \" 2 days \"\n",
    )
    .expect("second legacy-spelling policy");
    let second = call(&fixture.state, "cache_policy_reload", json!({}))
        .await
        .result
        .expect("second legacy-spelling reload");
    assert_eq!(second["previous_fingerprint"], accepted);
}

#[tokio::test(flavor = "current_thread")]
async fn daemon_rpc_rejects_strict_schema_mode_ttl_and_operation_object_violations() {
    let fixture = build_fixture_state().await.expect("fixture");
    let path = fixture.tempdir.path().join("cache-policy.toml");
    for invalid in [
        "version = 2\n",
        "version = 1\nunknown = true\n",
        "version = 1\n[objects.server]\nmode = \"live\"\nttl = \"24h\"\n",
        "version = 1\n[objects.server]\nmode = \"cache_only\"\nttl = \"59s\"\n",
        "version = 1\n[operations.server_get]\nobject = \"knowledge\"\nmode = \"live\"\n",
        "version = 1\n[rebuild.knowledge]\nknowledge_base_sys_id = \"IT Knowledge\"\n",
        "version = 1\n[rebuild.knowledge]\nknowledge_base_sys_id = \"11111111111111111111111111111111\"\nunknown = true\n",
    ] {
        std::fs::write(&path, invalid).expect("invalid fixture");
        let response = call(&fixture.state, "cache_policy_validate", json!({})).await;
        let error = response.error.expect("strict rejection");
        assert_eq!(error.code, -32070, "fixture: {invalid}");
        assert_eq!(
            error.data.expect("error data")["code"],
            "CACHE_POLICY_INVALID"
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn daemon_rpc_fingerprints_knowledge_rebuild_scope_without_disclosing_it() {
    const FIRST_BASE: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    const SECOND_BASE: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    let fixture = build_fixture_state().await.expect("fixture");
    let path = fixture.tempdir.path().join("cache-policy.toml");
    std::fs::write(
        &path,
        format!("version = 1\n[rebuild.knowledge]\nknowledge_base_sys_id = \"{FIRST_BASE}\"\n"),
    )
    .expect("first policy");
    let first = call(&fixture.state, "cache_policy_validate", json!({}))
        .await
        .result
        .expect("first validation");
    assert_eq!(first["rule_count"], 4);
    assert_eq!(first["source"], "built_in_plus_file");
    assert!(first["fingerprint"].as_str().is_some());
    assert!(!first.to_string().contains(FIRST_BASE));

    std::fs::write(
        &path,
        format!("version = 1\n[rebuild.knowledge]\nknowledge_base_sys_id = \"{SECOND_BASE}\"\n"),
    )
    .expect("second policy");
    let second = call(&fixture.state, "cache_policy_validate", json!({}))
        .await
        .result
        .expect("second validation");
    assert_ne!(first["fingerprint"], second["fingerprint"]);
    assert!(!second.to_string().contains(SECOND_BASE));
}

#[tokio::test(flavor = "current_thread")]
async fn daemon_rpc_reports_existing_unreadable_policy_as_io_without_disclosing_path() {
    let fixture = build_fixture_state().await.expect("fixture");
    std::fs::create_dir(fixture.tempdir.path().join("cache-policy.toml"))
        .expect("directory at fixed policy path");

    let response = call(&fixture.state, "cache_policy_validate", json!({})).await;
    let error = response.error.expect("I/O error");
    assert_eq!(error.code, -32071);
    let data = error.data.expect("I/O data");
    assert_eq!(data["code"], "CACHE_POLICY_IO");
    assert!(data["kind"].as_str().is_some_and(|kind| !kind.is_empty()));
    assert!(
        !data
            .to_string()
            .contains(fixture.tempdir.path().to_string_lossy().as_ref())
    );
}

#[tokio::test(flavor = "current_thread")]
async fn cache_only_server_miss_never_falls_through_to_servicenow() {
    let fixture = build_fixture_state().await.expect("fixture");
    std::fs::write(
        fixture.tempdir.path().join("cache-policy.toml"),
        "version = 1\n[operations.server_get]\nobject = \"server\"\nmode = \"cache_only\"\nttl = \"24h\"\n",
    ).expect("policy");
    assert!(
        call(&fixture.state, "cache_policy_reload", json!({}))
            .await
            .error
            .is_none()
    );

    let response = call(
        &fixture.state,
        "server_get",
        json!({
            "sys_id": "00000000000000000000000000000001"
        }),
    )
    .await;
    let error = response.error.expect("cache-only miss");
    assert_eq!(error.code, -32072);
    assert_eq!(
        error.data,
        Some(json!({
            "code": "CACHE_MISS", "operation": "server_get", "object": "server"
        }))
    );
}

#[tokio::test(flavor = "current_thread")]
async fn daemon_rpc_rejects_cached_modes_for_live_only_incident_reads() {
    let fixture = build_fixture_state().await.expect("fixture");
    let path = fixture.tempdir.path().join("cache-policy.toml");
    for invalid in [
        "version = 1\n[objects.incident]\nmode = \"read_through\"\nttl = \"24h\"\n",
        "version = 1\n[operations.incident_get]\nobject = \"incident\"\nmode = \"cache_only\"\nttl = \"24h\"\n",
        "version = 1\n[operations.incident_query]\nobject = \"incident\"\nmode = \"read_through\"\nttl = \"24h\"\n",
    ] {
        std::fs::write(&path, invalid).expect("policy fixture");
        let response = call(&fixture.state, "cache_policy_validate", json!({})).await;
        let error = response.error.expect("live-only Incident policy rejection");
        assert_eq!(error.code, -32070, "fixture: {invalid}");
        assert_eq!(
            error.data.expect("error data")["code"],
            "CACHE_POLICY_INVALID"
        );
    }
}
