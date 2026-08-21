//! L0 daemon JSON-RPC seam for the B-OPS-06 catalog-product projection.

use std::sync::Arc;

use serde_json::{Value, json};
use snow_core::cache::store::{RecordRow, Store};
use snow_core::{CatalogItem, ResourceType};
use snow_mcp::{DaemonBackedMcpBridge, McpServer};
use tokio::io::BufReader;
use tokio::sync::oneshot;
use tokio::task::LocalSet;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::{JsonRpcRequest, dispatch};
use crate::test_support::build_fixture_state_at_instance;

const ITEM_SYS_ID: &str = "300d473b13f00c10906630128144b0d1";
const VARIABLE_SYS_ID: &str = "11111111111111111111111111111111";

async fn call(
    state: &std::sync::Arc<crate::DaemonState>,
    method_name: &str,
    params: Value,
) -> super::JsonRpcResponse {
    dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: method_name.to_string(),
            params,
            id: Some(json!(31)),
        },
        state,
    )
    .await
}

async fn mount_complete_catalog_item(instance: &MockServer) {
    mount_complete_catalog_item_with_count(instance, 1).await;
}

async fn mount_complete_catalog_item_with_count(instance: &MockServer, expected: u64) {
    Mock::given(method("GET"))
        .and(path(format!("/api/now/table/sc_cat_item/{ITEM_SYS_ID}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": {
                "sys_id": ITEM_SYS_ID,
                "name": "Example Access Request",
                "short_description": "Request example access",
                "sys_class_name": "sc_cat_item",
                "active": "true"
            }
        })))
        .expect(expected)
        .mount(instance)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/item_option_new"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": [{
                "sys_id": VARIABLE_SYS_ID,
                "name": "access_level",
                "question_text": "Access level",
                "type": { "value": "5", "display_value": "Select Box" },
                "mandatory": "true",
                "default_value": "read",
                "order": "100"
            }]
        })))
        .expect(expected)
        .mount(instance)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/io_set_item"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"result": []})))
        .expect(expected)
        .mount(instance)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/question_choice"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": [{
                "sys_id": "22222222222222222222222222222222",
                "question": VARIABLE_SYS_ID,
                "value": "read",
                "text": "Read only",
                "order": "100"
            }]
        })))
        .expect(expected)
        .mount(instance)
        .await;
}

async fn direct_mcp_catalog_response(
    core: Arc<snow_core::SnowCore>,
    tool: &str,
    arguments: Value,
) -> Value {
    let request = json!({
        "jsonrpc": "2.0",
        "method": "tools/call",
        "params": {"name": tool, "arguments": arguments},
        "id": 41
    });
    let input = format!("{request}\n");
    let mut output = Vec::new();
    McpServer::new(core)
        .serve_streams(
            BufReader::new(input.as_bytes()),
            &mut output,
            std::future::pending::<std::io::Result<()>>(),
        )
        .await
        .expect("direct MCP stream");
    serde_json::from_slice(&output).expect("direct MCP response JSON")
}

async fn direct_mcp_catalog_payload(
    core: Arc<snow_core::SnowCore>,
    tool: &str,
    arguments: Value,
) -> Value {
    let response = direct_mcp_catalog_response(core, tool, arguments).await;
    response
        .get("result")
        .cloned()
        .unwrap_or_else(|| panic!("direct MCP result missing: {response}"))
}

async fn daemon_backed_mcp_catalog_response(
    fixture: &crate::test_support::FixtureState,
    tool: &str,
    arguments: Value,
) -> Value {
    let socket = fixture.tempdir.path().join("catalog-mcp-parity.sock");
    let endpoint = snow_core::ipc::IpcEndpoint::from_socket_path(&socket);
    let server = crate::rpc::JsonRpcServer::new(Arc::clone(&fixture.state), endpoint);
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
            let request = json!({
                "jsonrpc": "2.0",
                "method": "tools/call",
                "params": {"name": tool, "arguments": arguments},
                "id": 41
            });
            let input = format!("{request}\n");
            let mut output = Vec::new();
            DaemonBackedMcpBridge::from_socket(socket)
                .serve_streams(
                    BufReader::new(input.as_bytes()),
                    &mut output,
                    std::future::pending::<std::io::Result<()>>(),
                )
                .await
                .expect("daemon-backed MCP stream");
            let _ = shutdown_tx.send(());
            serde_json::from_slice(&output).expect("daemon-backed MCP response JSON")
        })
        .await
}

async fn daemon_backed_mcp_catalog_payload(
    fixture: &crate::test_support::FixtureState,
    tool: &str,
    arguments: Value,
) -> Value {
    let response = daemon_backed_mcp_catalog_response(fixture, tool, arguments).await;
    response
        .pointer("/result/structuredContent")
        .cloned()
        .unwrap_or_else(|| panic!("daemon-backed MCP structured result missing: {response}"))
}

async fn mount_narrowed_catalog_search(instance: &MockServer, name: &str) {
    mount_narrowed_catalog_search_with_count(instance, name, 1).await;
}

async fn mount_narrowed_catalog_search_with_count(
    instance: &MockServer,
    name: &str,
    expected: u64,
) {
    Mock::given(method("GET"))
        .and(path("/api/now/table/sc_cat_item"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": [{
                "sys_id": ITEM_SYS_ID,
                "name": name,
                "short_description": "Request example access",
                "sys_class_name": "sc_cat_item",
                "active": "true"
            }]
        })))
        .expect(expected)
        .mount(instance)
        .await;
}

fn complete_catalog_item() -> CatalogItem {
    CatalogItem {
        sys_id: ITEM_SYS_ID.to_string(),
        name: "Example Access Request".to_string(),
        short_description: "Request example access".to_string(),
        table: "sc_cat_item".to_string(),
        variables: vec![snow_core::CatalogVariable {
            sys_id: VARIABLE_SYS_ID.to_string(),
            name: "access_level".to_string(),
            label: "Access level".to_string(),
            variable_type: "Select Box".to_string(),
            mandatory: true,
            default_value: Some("read".to_string()),
            reference_table: None,
            lookup_table: None,
            max_length: None,
            choices: vec![snow_core::CatalogChoice {
                value: "read".to_string(),
                label: "Read only".to_string(),
            }],
        }],
    }
}

fn assert_byte_identical(left: &Value, right: &Value, context: &str) {
    assert_eq!(
        serde_json::to_vec(left).expect("left JSON bytes"),
        serde_json::to_vec(right).expect("right JSON bytes"),
        "{context}"
    );
}

async fn reload_policy(fixture: &crate::test_support::FixtureState) {
    let response = call(&fixture.state, "cache_policy_reload", json!({})).await;
    assert!(
        response.error.is_none(),
        "policy reload failed: {:?}",
        response.error
    );
}

#[tokio::test(flavor = "current_thread")]
async fn direct_and_daemon_backed_mcp_live_catalog_get_match_literal_complete_envelope() {
    let instance = MockServer::start().await;
    mount_complete_catalog_item_with_count(&instance, 2).await;
    let fixture = build_fixture_state_at_instance(&instance.uri())
        .await
        .expect("fixture");
    std::fs::write(
        fixture.tempdir.path().join("cache-policy.toml"),
        concat!(
            "version = 1\n",
            "[operations.catalog_item_get]\n",
            "object = \"service_catalog_product\"\n",
            "mode = \"live\"\n",
        ),
    )
    .expect("live policy");
    reload_policy(&fixture).await;

    let arguments = json!({"sys_id": ITEM_SYS_ID});
    let direct = direct_mcp_catalog_payload(
        Arc::clone(&fixture.state.core),
        "catalog_item_get",
        arguments.clone(),
    )
    .await;
    let daemon_backed =
        daemon_backed_mcp_catalog_payload(&fixture, "catalog_item_get", arguments).await;
    let expected = json!({
        "operation": "catalog_item_get",
        "source": {"kind": "live"},
        "completeness": {"kind": "complete"},
        "data": {
            "item": {
                "sys_id": ITEM_SYS_ID,
                "name": "Example Access Request",
                "short_description": "Request example access",
                "table": "sc_cat_item",
                "variables": [{
                    "sys_id": VARIABLE_SYS_ID,
                    "name": "access_level",
                    "label": "Access level",
                    "variable_type": "Select Box",
                    "mandatory": true,
                    "default_value": "read",
                    "choices": [{"value": "read", "label": "Read only"}]
                }]
            }
        }
    });
    let expected_bytes = serde_json::to_vec(&expected).expect("literal envelope bytes");
    assert_eq!(
        serde_json::to_vec(&daemon_backed).expect("daemon-backed bytes"),
        expected_bytes,
        "daemon-backed MCP must match the literal catalog envelope"
    );
    assert_eq!(
        serde_json::to_vec(&direct).expect("direct bytes"),
        expected_bytes,
        "direct MCP must not return the legacy unwrapped item payload"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn direct_and_daemon_backed_mcp_live_catalog_search_match_literal_partial_envelope() {
    let instance = MockServer::start().await;
    mount_narrowed_catalog_search_with_count(&instance, "Example Access Request", 2).await;
    let fixture = build_fixture_state_at_instance(&instance.uri())
        .await
        .expect("fixture");
    std::fs::write(
        fixture.tempdir.path().join("cache-policy.toml"),
        concat!(
            "version = 1\n",
            "[operations.catalog_items_search]\n",
            "object = \"service_catalog_product\"\n",
            "mode = \"live\"\n",
        ),
    )
    .expect("live policy");
    reload_policy(&fixture).await;

    let arguments = json!({"query": "Access", "limit": 10});
    let direct = direct_mcp_catalog_payload(
        Arc::clone(&fixture.state.core),
        "catalog_items_search",
        arguments.clone(),
    )
    .await;
    let daemon_backed =
        daemon_backed_mcp_catalog_payload(&fixture, "catalog_items_search", arguments).await;
    let expected = json!({
        "operation": "catalog_items_search",
        "source": {"kind": "live"},
        "completeness": {"kind": "partial", "reason": "narrowed_projection"},
        "data": {
            "items": [{
                "sys_id": ITEM_SYS_ID,
                "name": "Example Access Request",
                "short_description": "Request example access",
                "table": "sc_cat_item"
            }]
        }
    });
    assert_byte_identical(&daemon_backed, &expected, "daemon-backed live search");
    assert_byte_identical(&direct, &expected, "direct live search");
}

#[tokio::test(flavor = "current_thread")]
async fn direct_and_daemon_backed_mcp_cached_catalog_reads_match_in_both_cached_modes() {
    let instance = MockServer::start().await;
    let fixture = build_fixture_state_at_instance(&instance.uri())
        .await
        .expect("fixture");
    let store = Store::open(fixture.tempdir.path().join("snow.db")).expect("cache store");
    let refreshed_at = chrono::DateTime::from_timestamp(chrono::Utc::now().timestamp(), 0)
        .expect("current timestamp");
    let refreshed_wire = refreshed_at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    store
        .upsert_complete_catalog_product(&complete_catalog_item(), refreshed_at)
        .expect("complete projection");
    let mut narrowed_item = complete_catalog_item();
    narrowed_item.variables.clear();
    store
        .upsert_narrowed_catalog_product(&narrowed_item, refreshed_at)
        .expect("narrowed projection");

    let expected_get = json!({
        "operation": "catalog_item_get",
        "source": {"kind": "cache", "last_refreshed_at": refreshed_wire},
        "completeness": {"kind": "complete"},
        "data": {"item": complete_catalog_item()}
    });
    let expected_search = json!({
        "operation": "catalog_items_search",
        "source": {"kind": "cache", "last_refreshed_at": refreshed_wire},
        "completeness": {"kind": "partial", "reason": "narrowed_projection"},
        "data": {
            "items": [{
                "sys_id": ITEM_SYS_ID,
                "name": "Example Access Request",
                "short_description": "Request example access",
                "table": "sc_cat_item"
            }]
        }
    });

    for mode in ["read_through", "cache_only"] {
        std::fs::write(
            fixture.tempdir.path().join("cache-policy.toml"),
            format!(
                concat!(
                    "version = 1\n",
                    "[operations.catalog_item_get]\n",
                    "object = \"service_catalog_product\"\n",
                    "mode = \"{}\"\n",
                    "ttl = \"30d\"\n",
                    "[operations.catalog_items_search]\n",
                    "object = \"service_catalog_product\"\n",
                    "mode = \"{}\"\n",
                    "ttl = \"30d\"\n",
                ),
                mode, mode
            ),
        )
        .expect("cached policy");
        reload_policy(&fixture).await;

        let get_args = json!({"sys_id": ITEM_SYS_ID});
        let direct_get = direct_mcp_catalog_payload(
            Arc::clone(&fixture.state.core),
            "catalog_item_get",
            get_args.clone(),
        )
        .await;
        let bridge_get =
            daemon_backed_mcp_catalog_payload(&fixture, "catalog_item_get", get_args).await;
        assert_byte_identical(&direct_get, &expected_get, &format!("direct {mode} get"));
        assert_byte_identical(
            &bridge_get,
            &expected_get,
            &format!("daemon-backed {mode} get"),
        );

        let search_args = json!({"query": "Access", "limit": 10});
        let direct_search = direct_mcp_catalog_payload(
            Arc::clone(&fixture.state.core),
            "catalog_items_search",
            search_args.clone(),
        )
        .await;
        let bridge_search =
            daemon_backed_mcp_catalog_payload(&fixture, "catalog_items_search", search_args).await;
        assert_byte_identical(
            &direct_search,
            &expected_search,
            &format!("direct {mode} search"),
        );
        assert_byte_identical(
            &bridge_search,
            &expected_search,
            &format!("daemon-backed {mode} search"),
        );
    }

    assert!(
        instance
            .received_requests()
            .await
            .expect("ServiceNow requests")
            .is_empty(),
        "fresh read-through and cache-only MCP calls must make zero ServiceNow requests"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn direct_and_daemon_backed_mcp_reject_generic_catalog_row_with_same_cache_miss() {
    let instance = MockServer::start().await;
    let fixture = build_fixture_state_at_instance(&instance.uri())
        .await
        .expect("fixture");
    let store = Store::open(fixture.tempdir.path().join("snow.db")).expect("cache store");
    let refreshed_at = chrono::DateTime::parse_from_rfc3339("2026-08-20T12:00:00Z")
        .expect("timestamp")
        .with_timezone(&chrono::Utc);
    let mut generic = RecordRow::active(
        ITEM_SYS_ID,
        "Example Access Request",
        "sc_cat_item",
        ResourceType::Unknown,
        refreshed_at,
    );
    generic.short_desc = Some("Request example access".to_string());
    generic.raw_json = json!({
        "sys_id": ITEM_SYS_ID,
        "name": "Example Access Request",
        "short_description": "Request example access",
        "sys_class_name": "sc_cat_item"
    })
    .to_string();
    store
        .upsert_record(&generic, "", "Request example access")
        .expect("generic projection");
    std::fs::write(
        fixture.tempdir.path().join("cache-policy.toml"),
        concat!(
            "version = 1\n",
            "[operations.catalog_item_get]\n",
            "object = \"service_catalog_product\"\n",
            "mode = \"cache_only\"\n",
            "ttl = \"30d\"\n",
        ),
    )
    .expect("cache-only policy");
    reload_policy(&fixture).await;

    let arguments = json!({"sys_id": ITEM_SYS_ID});
    let direct = direct_mcp_catalog_response(
        Arc::clone(&fixture.state.core),
        "catalog_item_get",
        arguments.clone(),
    )
    .await;
    let daemon_backed =
        daemon_backed_mcp_catalog_response(&fixture, "catalog_item_get", arguments).await;
    let expected = json!({
        "jsonrpc": "2.0",
        "error": {
            "code": -32072,
            "message": "cache miss",
            "data": {
                "code": "CACHE_MISS",
                "operation": "catalog_item_get",
                "object": "service_catalog_product"
            }
        },
        "id": 41
    });
    assert_byte_identical(&direct, &expected, "direct generic-row rejection");
    assert_byte_identical(
        &daemon_backed,
        &expected,
        "daemon-backed generic-row rejection",
    );
    assert!(
        instance
            .received_requests()
            .await
            .expect("ServiceNow requests")
            .is_empty(),
        "cache-only miss must make zero ServiceNow requests"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn direct_and_daemon_backed_mcp_stale_refresh_failure_never_returns_stale_catalog_data() {
    let fixture = build_fixture_state_at_instance("http://127.0.0.1:9")
        .await
        .expect("fixture");
    let store = Store::open(fixture.tempdir.path().join("snow.db")).expect("cache store");
    let stale_at = chrono::DateTime::parse_from_rfc3339("2020-01-01T00:00:00Z")
        .expect("timestamp")
        .with_timezone(&chrono::Utc);
    let mut stale = complete_catalog_item();
    stale.name = "Stale Item Must Not Escape".to_string();
    store
        .upsert_complete_catalog_product(&stale, stale_at)
        .expect("stale complete projection");
    store
        .upsert_narrowed_catalog_product(&stale, stale_at)
        .expect("stale narrowed projection");

    for (tool, arguments) in [
        ("catalog_item_get", json!({"sys_id": ITEM_SYS_ID})),
        (
            "catalog_items_search",
            json!({"query": "Stale", "limit": 10}),
        ),
    ] {
        let direct =
            direct_mcp_catalog_response(Arc::clone(&fixture.state.core), tool, arguments.clone())
                .await;
        let daemon_backed = daemon_backed_mcp_catalog_response(&fixture, tool, arguments).await;
        assert_byte_identical(
            &direct,
            &daemon_backed,
            &format!("{tool} stale refresh error parity"),
        );
        assert_eq!(direct["error"]["code"], -32000, "{tool}");
        assert_eq!(direct["error"]["message"], "internal error", "{tool}");
        assert!(direct.get("result").is_none(), "{tool} returned stale data");
        assert!(
            !direct.to_string().contains("Stale Item Must Not Escape"),
            "{tool} leaked the stale cached projection"
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn read_through_catalog_get_returns_literal_complete_live_envelope() {
    let instance = MockServer::start().await;
    mount_complete_catalog_item(&instance).await;
    let fixture = build_fixture_state_at_instance(&instance.uri())
        .await
        .expect("fixture");

    let response = call(
        &fixture.state,
        "catalog_item_get",
        json!({"sys_id": ITEM_SYS_ID}),
    )
    .await;

    assert!(
        response.error.is_none(),
        "catalog get failed: {:?}",
        response.error
    );
    assert_eq!(
        response.result,
        Some(json!({
            "operation": "catalog_item_get",
            "source": {"kind": "live"},
            "completeness": {"kind": "complete"},
            "data": {
                "item": {
                    "sys_id": ITEM_SYS_ID,
                    "name": "Example Access Request",
                    "short_description": "Request example access",
                    "table": "sc_cat_item",
                    "variables": [{
                        "sys_id": VARIABLE_SYS_ID,
                        "name": "access_level",
                        "label": "Access level",
                        "variable_type": "Select Box",
                        "mandatory": true,
                        "default_value": "read",
                        "choices": [{"value": "read", "label": "Read only"}]
                    }]
                }
            }
        }))
    );
}

#[tokio::test(flavor = "current_thread")]
async fn read_through_catalog_get_persists_complete_variables_and_choices_for_cache_only() {
    let instance = MockServer::start().await;
    mount_complete_catalog_item(&instance).await;
    let fixture = build_fixture_state_at_instance(&instance.uri())
        .await
        .expect("fixture");
    let params = json!({"sys_id": ITEM_SYS_ID});

    let live = call(&fixture.state, "catalog_item_get", params.clone()).await;
    assert!(
        live.error.is_none(),
        "live refresh failed: {:?}",
        live.error
    );
    instance.verify().await;
    instance.reset().await;

    std::fs::write(
        fixture.tempdir.path().join("cache-policy.toml"),
        concat!(
            "version = 1\n",
            "[operations.catalog_item_get]\n",
            "object = \"service_catalog_product\"\n",
            "mode = \"cache_only\"\n",
            "ttl = \"30d\"\n",
        ),
    )
    .expect("cache-only policy");
    reload_policy(&fixture).await;

    let cached = call(&fixture.state, "catalog_item_get", params).await;
    assert!(
        cached.error.is_none(),
        "complete cache read failed: {:?}",
        cached.error
    );
    let result = cached.result.expect("cached result");
    assert_eq!(result["operation"], "catalog_item_get");
    assert_eq!(result["source"]["kind"], "cache");
    assert!(result["source"]["last_refreshed_at"].is_string());
    assert_eq!(result["completeness"], json!({"kind": "complete"}));
    assert_eq!(
        result["data"]["item"]["variables"][0]["choices"],
        json!([{"value": "read", "label": "Read only"}])
    );
    assert!(
        instance
            .received_requests()
            .await
            .expect("requests")
            .is_empty()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn generic_catalog_row_cannot_satisfy_cache_only_complete_get() {
    let fixture = build_fixture_state_at_instance("http://127.0.0.1:9")
        .await
        .expect("fixture");
    let store = Store::open(fixture.tempdir.path().join("snow.db")).expect("cache store");
    let refreshed_at = chrono::DateTime::parse_from_rfc3339("2026-08-20T12:00:00Z")
        .expect("timestamp")
        .with_timezone(&chrono::Utc);
    let mut generic = RecordRow::active(
        ITEM_SYS_ID,
        "Example Access Request",
        "sc_cat_item",
        ResourceType::Unknown,
        refreshed_at,
    );
    generic.short_desc = Some("Request example access".to_string());
    generic.raw_json = json!({
        "sys_id": ITEM_SYS_ID,
        "name": "Example Access Request",
        "short_description": "Request example access",
        "sys_class_name": "sc_cat_item"
    })
    .to_string();
    store
        .upsert_record(&generic, "", "Request example access")
        .expect("generic projection");

    std::fs::write(
        fixture.tempdir.path().join("cache-policy.toml"),
        concat!(
            "version = 1\n",
            "[operations.catalog_item_get]\n",
            "object = \"service_catalog_product\"\n",
            "mode = \"cache_only\"\n",
            "ttl = \"30d\"\n",
        ),
    )
    .expect("cache-only policy");
    reload_policy(&fixture).await;

    let response = call(
        &fixture.state,
        "catalog_item_get",
        json!({"sys_id": ITEM_SYS_ID}),
    )
    .await;
    let error = response.error.expect("generic row must be a cache miss");
    assert_eq!(error.code, -32072);
    assert_eq!(
        error.data,
        Some(json!({
            "code": "CACHE_MISS",
            "operation": "catalog_item_get",
            "object": "service_catalog_product"
        }))
    );
}

#[tokio::test(flavor = "current_thread")]
async fn narrowed_catalog_search_is_partial_cache_with_literal_refresh_timestamp() {
    let fixture = build_fixture_state_at_instance("http://127.0.0.1:9")
        .await
        .expect("fixture");
    let store = Store::open(fixture.tempdir.path().join("snow.db")).expect("cache store");
    let refreshed_at = chrono::DateTime::parse_from_rfc3339("2026-08-20T12:00:00Z")
        .expect("timestamp")
        .with_timezone(&chrono::Utc);
    store
        .upsert_narrowed_catalog_product(
            &CatalogItem {
                sys_id: ITEM_SYS_ID.to_string(),
                name: "Example Access Request".to_string(),
                short_description: "Request example access".to_string(),
                table: "sc_cat_item".to_string(),
                variables: Vec::new(),
            },
            refreshed_at,
        )
        .expect("narrowed projection");

    std::fs::write(
        fixture.tempdir.path().join("cache-policy.toml"),
        concat!(
            "version = 1\n",
            "[operations.catalog_items_search]\n",
            "object = \"service_catalog_product\"\n",
            "mode = \"cache_only\"\n",
            "ttl = \"30d\"\n",
        ),
    )
    .expect("cache-only policy");
    reload_policy(&fixture).await;

    let response = call(
        &fixture.state,
        "catalog_items_search",
        json!({"query": "Access", "limit": 10}),
    )
    .await;
    assert_eq!(
        response.result,
        Some(json!({
            "operation": "catalog_items_search",
            "source": {
                "kind": "cache",
                "last_refreshed_at": "2026-08-20T12:00:00Z"
            },
            "completeness": {
                "kind": "partial",
                "reason": "narrowed_projection"
            },
            "data": {
                "items": [{
                    "sys_id": ITEM_SYS_ID,
                    "name": "Example Access Request",
                    "short_description": "Request example access",
                    "table": "sc_cat_item"
                }]
            }
        }))
    );
}

#[tokio::test(flavor = "current_thread")]
async fn live_catalog_get_ignores_complete_cache_and_does_not_replace_it() {
    let instance = MockServer::start().await;
    mount_complete_catalog_item(&instance).await;
    let fixture = build_fixture_state_at_instance(&instance.uri())
        .await
        .expect("fixture");
    let store = Store::open(fixture.tempdir.path().join("snow.db")).expect("cache store");
    let cached_at = chrono::DateTime::parse_from_rfc3339("2026-08-19T12:00:00Z")
        .expect("timestamp")
        .with_timezone(&chrono::Utc);
    store
        .upsert_complete_catalog_product(
            &CatalogItem {
                sys_id: ITEM_SYS_ID.to_string(),
                name: "Cached Item Must Be Ignored".to_string(),
                short_description: "Cached projection".to_string(),
                table: "sc_cat_item".to_string(),
                variables: Vec::new(),
            },
            cached_at,
        )
        .expect("complete projection");
    std::fs::write(
        fixture.tempdir.path().join("cache-policy.toml"),
        concat!(
            "version = 1\n",
            "[operations.catalog_item_get]\n",
            "object = \"service_catalog_product\"\n",
            "mode = \"live\"\n",
        ),
    )
    .expect("live policy");
    reload_policy(&fixture).await;

    let live = call(
        &fixture.state,
        "catalog_item_get",
        json!({"sys_id": ITEM_SYS_ID}),
    )
    .await;
    let live_result = live.result.expect("live result");
    assert_eq!(live_result["source"], json!({"kind": "live"}));
    assert_eq!(
        live_result["data"]["item"]["name"],
        "Example Access Request"
    );
    instance.verify().await;
    instance.reset().await;

    std::fs::write(
        fixture.tempdir.path().join("cache-policy.toml"),
        concat!(
            "version = 1\n",
            "[operations.catalog_item_get]\n",
            "object = \"service_catalog_product\"\n",
            "mode = \"cache_only\"\n",
            "ttl = \"30d\"\n",
        ),
    )
    .expect("cache-only policy");
    reload_policy(&fixture).await;
    let cached = call(
        &fixture.state,
        "catalog_item_get",
        json!({"sys_id": ITEM_SYS_ID}),
    )
    .await
    .result
    .expect("cached result");
    assert_eq!(
        cached["data"]["item"]["name"],
        "Cached Item Must Be Ignored"
    );
    assert_eq!(
        cached["source"],
        json!({
            "kind": "cache",
            "last_refreshed_at": "2026-08-19T12:00:00Z"
        })
    );
    assert!(
        instance
            .received_requests()
            .await
            .expect("requests")
            .is_empty()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn stale_read_through_catalog_get_never_falls_back_after_live_failure() {
    let fixture = build_fixture_state_at_instance("http://127.0.0.1:9")
        .await
        .expect("fixture");
    let store = Store::open(fixture.tempdir.path().join("snow.db")).expect("cache store");
    let stale_at = chrono::DateTime::parse_from_rfc3339("2020-01-01T00:00:00Z")
        .expect("timestamp")
        .with_timezone(&chrono::Utc);
    store
        .upsert_complete_catalog_product(
            &CatalogItem {
                sys_id: ITEM_SYS_ID.to_string(),
                name: "Stale Item".to_string(),
                short_description: "Must not hide an upstream failure".to_string(),
                table: "sc_cat_item".to_string(),
                variables: Vec::new(),
            },
            stale_at,
        )
        .expect("stale complete projection");

    let response = call(
        &fixture.state,
        "catalog_item_get",
        json!({"sys_id": ITEM_SYS_ID}),
    )
    .await;
    assert_eq!(response.error.expect("live failure").code, -32000);
}

#[tokio::test(flavor = "current_thread")]
async fn read_through_catalog_search_persists_only_a_partial_narrowed_projection() {
    let instance = MockServer::start().await;
    mount_narrowed_catalog_search(&instance, "Example Access Request").await;
    let fixture = build_fixture_state_at_instance(&instance.uri())
        .await
        .expect("fixture");
    let params = json!({"query": "Access", "limit": 10});

    let live = call(&fixture.state, "catalog_items_search", params.clone())
        .await
        .result
        .expect("live result");
    assert_eq!(live["source"], json!({"kind": "live"}));
    assert_eq!(
        live["completeness"],
        json!({"kind": "partial", "reason": "narrowed_projection"})
    );
    instance.verify().await;
    instance.reset().await;

    std::fs::write(
        fixture.tempdir.path().join("cache-policy.toml"),
        concat!(
            "version = 1\n",
            "[operations.catalog_items_search]\n",
            "object = \"service_catalog_product\"\n",
            "mode = \"cache_only\"\n",
            "ttl = \"30d\"\n",
        ),
    )
    .expect("cache-only policy");
    reload_policy(&fixture).await;
    let cached = call(&fixture.state, "catalog_items_search", params)
        .await
        .result
        .expect("cached result");
    assert_eq!(cached["source"]["kind"], "cache");
    assert!(cached["source"]["last_refreshed_at"].is_string());
    assert_eq!(
        cached["completeness"],
        json!({"kind": "partial", "reason": "narrowed_projection"})
    );
    assert_eq!(
        cached["data"]["items"][0]["variables"],
        Value::Null,
        "narrowed search must not synthesize complete variables"
    );
    assert!(
        instance
            .received_requests()
            .await
            .expect("requests")
            .is_empty()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn live_catalog_search_ignores_narrowed_cache_and_does_not_replace_it() {
    let instance = MockServer::start().await;
    mount_narrowed_catalog_search(&instance, "Live Access Request").await;
    let fixture = build_fixture_state_at_instance(&instance.uri())
        .await
        .expect("fixture");
    let store = Store::open(fixture.tempdir.path().join("snow.db")).expect("cache store");
    let cached_at = chrono::DateTime::parse_from_rfc3339("2026-08-19T12:00:00Z")
        .expect("timestamp")
        .with_timezone(&chrono::Utc);
    store
        .upsert_narrowed_catalog_product(
            &CatalogItem {
                sys_id: ITEM_SYS_ID.to_string(),
                name: "Cached Access Request".to_string(),
                short_description: "Cached projection".to_string(),
                table: "sc_cat_item".to_string(),
                variables: Vec::new(),
            },
            cached_at,
        )
        .expect("narrowed projection");
    std::fs::write(
        fixture.tempdir.path().join("cache-policy.toml"),
        concat!(
            "version = 1\n",
            "[operations.catalog_items_search]\n",
            "object = \"service_catalog_product\"\n",
            "mode = \"live\"\n",
        ),
    )
    .expect("live policy");
    reload_policy(&fixture).await;

    let live = call(
        &fixture.state,
        "catalog_items_search",
        json!({"query": "Access", "limit": 10}),
    )
    .await
    .result
    .expect("live result");
    assert_eq!(live["source"], json!({"kind": "live"}));
    assert_eq!(live["data"]["items"][0]["name"], "Live Access Request");
    instance.verify().await;
    instance.reset().await;

    std::fs::write(
        fixture.tempdir.path().join("cache-policy.toml"),
        concat!(
            "version = 1\n",
            "[operations.catalog_items_search]\n",
            "object = \"service_catalog_product\"\n",
            "mode = \"cache_only\"\n",
            "ttl = \"30d\"\n",
        ),
    )
    .expect("cache-only policy");
    reload_policy(&fixture).await;
    let cached = call(
        &fixture.state,
        "catalog_items_search",
        json!({"query": "Access", "limit": 10}),
    )
    .await
    .result
    .expect("cached result");
    assert_eq!(cached["data"]["items"][0]["name"], "Cached Access Request");
    assert_eq!(
        cached["source"]["last_refreshed_at"],
        "2026-08-19T12:00:00Z"
    );
    assert!(
        instance
            .received_requests()
            .await
            .expect("requests")
            .is_empty()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn exact_catalog_invalidation_removes_complete_typed_projection() {
    let instance = MockServer::start().await;
    mount_complete_catalog_item(&instance).await;
    let fixture = build_fixture_state_at_instance(&instance.uri())
        .await
        .expect("fixture");
    let params = json!({"sys_id": ITEM_SYS_ID});
    let refreshed = call(&fixture.state, "catalog_item_get", params.clone()).await;
    assert!(
        refreshed.error.is_none(),
        "catalog refresh failed: {:?}",
        refreshed.error
    );
    fixture
        .state
        .core
        .invalidate_cache_target("service_catalog_product", ITEM_SYS_ID)
        .await
        .expect("exact invalidation");

    std::fs::write(
        fixture.tempdir.path().join("cache-policy.toml"),
        concat!(
            "version = 1\n",
            "[operations.catalog_item_get]\n",
            "object = \"service_catalog_product\"\n",
            "mode = \"cache_only\"\n",
            "ttl = \"30d\"\n",
        ),
    )
    .expect("cache-only policy");
    reload_policy(&fixture).await;
    instance.reset().await;

    let response = call(&fixture.state, "catalog_item_get", params).await;
    assert_eq!(response.error.expect("cache miss").code, -32072);
    assert!(
        instance
            .received_requests()
            .await
            .expect("requests")
            .is_empty()
    );
}
