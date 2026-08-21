//! L0 daemon JSON-RPC seam for Server and Knowledge cache-policy behavior.

use serde_json::{Value, json};

use super::{JsonRpcRequest, dispatch};
use crate::test_support::{build_fixture_state_at_instance, spawn_json_http_server};

fn age_cached_segment(path: &std::path::Path, resource_type: snow_core::ResourceType) {
    let engine = snow_core::query::QueryEngine::open(path.join("snow.db")).expect("open cache");
    let mut rows = engine
        .store()
        .list_active_records(Some(resource_type.clone()))
        .expect("cached rows");
    assert!(!rows.is_empty(), "expected cached rows to age");
    for row in &mut rows {
        row.synced_at = chrono::Utc::now() - chrono::Duration::days(366);
        engine
            .store()
            .upsert_record(row, "", row.description.as_deref().unwrap_or_default())
            .expect("age cached row");
    }
    let vault_segment = match resource_type {
        snow_core::ResourceType::Server => Some("servers"),
        snow_core::ResourceType::Knowledge => Some("knowledge"),
        snow_core::ResourceType::BusinessApplication => Some("business_applications"),
        _ => None,
    };
    if let Some(segment) = vault_segment {
        let segment = path.join("vault").join(segment);
        if segment.exists() {
            std::fs::remove_dir_all(segment).expect("remove fresh vault projection");
        }
    }
}

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
            id: Some(json!(41)),
        },
        state,
    )
    .await
}

async fn reload_policy(state: &std::sync::Arc<crate::DaemonState>) {
    let response = call(state, "cache_policy_reload", json!({})).await;
    assert!(
        response.error.is_none(),
        "reload failed: {:?}",
        response.error
    );
}

#[tokio::test(flavor = "current_thread")]
async fn cache_only_server_search_and_query_miss_without_servicenow() {
    let fixture = build_fixture_state_at_instance("http://127.0.0.1:9")
        .await
        .expect("fixture");
    std::fs::write(
        fixture.tempdir.path().join("cache-policy.toml"),
        concat!(
            "version = 1\n",
            "[operations.server_search]\n",
            "object = \"server\"\n",
            "mode = \"cache_only\"\n",
            "ttl = \"24h\"\n",
            "[operations.server_query]\n",
            "object = \"server\"\n",
            "mode = \"cache_only\"\n",
            "ttl = \"24h\"\n",
        ),
    )
    .expect("cache-only policy");
    reload_policy(&fixture.state).await;

    for method in ["server_search", "server_query"] {
        let response = call(&fixture.state, method, json!({})).await;
        let error = response
            .error
            .unwrap_or_else(|| panic!("{method} must report a cache miss"));
        assert_eq!(error.code, -32072, "{method}");
        assert_eq!(
            error.data,
            Some(json!({
                "code":"CACHE_MISS",
                "operation":method,
                "object":"server"
            })),
            "{method}"
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn live_knowledge_search_uses_servicenow_and_returns_literal_envelope() {
    let (instance, request) = spawn_json_http_server(json!({
        "result": [{
            "sys_id": "11111111111111111111111111111111",
            "number": "KB0099999",
            "short_description": "Example live article",
            "text": "Example live body",
            "workflow_state": "published",
            "kb_knowledge_base": {
                "value": "22222222222222222222222222222222",
                "display_value": "Example Knowledge"
            },
            "kb_category": {
                "value": "33333333333333333333333333333333",
                "display_value": "Example Category"
            },
            "sys_updated_on": "2026-08-20 12:00:00"
        }]
    }))
    .await
    .expect("local ServiceNow fake");
    let fixture = build_fixture_state_at_instance(&instance)
        .await
        .expect("fixture");
    std::fs::write(
        fixture.tempdir.path().join("cache-policy.toml"),
        concat!(
            "version = 1\n",
            "[operations.search_knowledge]\n",
            "object = \"knowledge\"\n",
            "mode = \"live\"\n",
        ),
    )
    .expect("live policy");
    reload_policy(&fixture.state).await;

    let response = call(
        &fixture.state,
        "search_knowledge",
        json!({"query":"Example live","limit":10}),
    )
    .await;
    assert!(
        response.error.is_none(),
        "live search failed: {:?}",
        response.error
    );
    let result = response.result.expect("result");
    assert_eq!(result["operation"], "search_knowledge");
    assert_eq!(result["source"], json!({"kind":"live"}));
    assert_eq!(result["completeness"], json!({"kind":"complete"}));
    assert_eq!(
        result["data"]["articles"][0]["record"]["number"],
        "KB0099999"
    );
    assert!(
        request
            .await
            .expect("live request")
            .starts_with("GET /api/now/table/kb_knowledge?")
    );
    assert!(
        !fixture.tempdir.path().join("vault/knowledge").exists(),
        "live mode must not persist the Knowledge projection"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn cache_only_knowledge_get_search_and_list_return_timestamped_narrowed_truth() {
    let fixture = build_fixture_state_at_instance("http://127.0.0.1:9")
        .await
        .expect("fixture");
    std::fs::write(
        fixture.tempdir.path().join("cache-policy.toml"),
        concat!(
            "version = 1\n",
            "[objects.knowledge]\n",
            "mode = \"cache_only\"\n",
            "ttl = \"7d\"\n",
        ),
    )
    .expect("cache-only policy");
    reload_policy(&fixture.state).await;

    let get = call(&fixture.state, "get_article", json!({"number":"KB001"}))
        .await
        .result
        .expect("cached get");
    assert_eq!(get["operation"], "get_article");
    assert_eq!(get["source"]["kind"], "cache");
    assert!(get["source"]["last_refreshed_at"].is_string());
    assert_eq!(get["completeness"], json!({"kind":"complete"}));

    for (method, params) in [
        ("search_knowledge", json!({"query":"database"})),
        ("list_knowledge_articles", json!({})),
    ] {
        let result = call(&fixture.state, method, params)
            .await
            .result
            .unwrap_or_else(|| panic!("cached {method}"));
        assert_eq!(result["operation"], method);
        assert_eq!(result["source"]["kind"], "cache");
        assert!(result["source"]["last_refreshed_at"].is_string());
        assert_eq!(
            result["completeness"],
            json!({"kind":"partial","reason":"narrowed_projection"})
        );
    }
}

fn live_server_response() -> Value {
    json!({
        "result": [{
            "sys_id": "44444444444444444444444444444444",
            "name": "example-server-01",
            "short_description": "Example Linux server",
            "ip_address": "192.0.2.44",
            "sys_class_name": "cmdb_ci_linux_server",
            "operational_status": {"value":"1","display_value":"Operational"},
            "sys_updated_on": "2026-08-20 12:00:00"
        }]
    })
}

#[tokio::test(flavor = "current_thread")]
async fn live_server_search_has_zero_persistence_then_cache_only_misses() {
    let (instance, request) = spawn_json_http_server(live_server_response())
        .await
        .expect("local ServiceNow fake");
    let fixture = build_fixture_state_at_instance(&instance)
        .await
        .expect("fixture");
    let policy = fixture.tempdir.path().join("cache-policy.toml");
    std::fs::write(
        &policy,
        concat!(
            "version = 1\n",
            "[operations.server_search]\n",
            "object = \"server\"\n",
            "mode = \"live\"\n",
        ),
    )
    .expect("live policy");
    reload_policy(&fixture.state).await;

    let live = call(
        &fixture.state,
        "server_search",
        json!({"name":"example-server","limit":10}),
    )
    .await
    .result
    .expect("live server result");
    assert_eq!(live["source"], json!({"kind":"live"}));
    assert_eq!(live["completeness"], json!({"kind":"complete"}));
    assert_eq!(live["data"]["servers"][0]["name"], "example-server-01");
    request.await.expect("one live request");

    std::fs::write(
        &policy,
        concat!(
            "version = 1\n",
            "[operations.server_search]\n",
            "object = \"server\"\n",
            "mode = \"cache_only\"\n",
            "ttl = \"24h\"\n",
        ),
    )
    .expect("cache-only policy");
    reload_policy(&fixture.state).await;
    let miss = call(&fixture.state, "server_search", json!({}))
        .await
        .error
        .expect("live result must not have persisted");
    assert_eq!(miss.code, -32072);
}

#[tokio::test(flavor = "current_thread")]
async fn read_through_server_search_refreshes_then_server_query_uses_fresh_cache() {
    let (instance, request) = spawn_json_http_server(live_server_response())
        .await
        .expect("local ServiceNow fake");
    let fixture = build_fixture_state_at_instance(&instance)
        .await
        .expect("fixture");

    let refreshed = call(
        &fixture.state,
        "server_search",
        json!({"name":"example-server","limit":10}),
    )
    .await
    .result
    .expect("read-through refresh");
    assert_eq!(refreshed["source"], json!({"kind":"live"}));
    request.await.expect("one live refresh");

    let cached = call(
        &fixture.state,
        "server_query",
        json!({"name":"example-server","limit":10}),
    )
    .await
    .result
    .expect("fresh cache query");
    assert_eq!(cached["source"]["kind"], "cache");
    assert!(cached["source"]["last_refreshed_at"].is_string());
    assert_eq!(
        cached["completeness"],
        json!({"kind":"partial","reason":"narrowed_projection"})
    );
}

#[tokio::test(flavor = "current_thread")]
async fn stale_read_through_server_query_refresh_failure_never_falls_back() {
    let (instance, request) = spawn_json_http_server(live_server_response())
        .await
        .expect("local ServiceNow fake");
    let fixture = build_fixture_state_at_instance(&instance)
        .await
        .expect("fixture");
    let params = json!({"name":"example-server","limit":10});
    let first = call(&fixture.state, "server_query", params.clone()).await;
    assert!(
        first.error.is_none(),
        "initial refresh failed: {:?}",
        first.error
    );
    request.await.expect("initial live refresh");
    age_cached_segment(fixture.tempdir.path(), snow_core::ResourceType::Server);

    let stale = call(&fixture.state, "server_query", params).await;
    assert!(
        stale.result.is_none(),
        "stale data must not be returned after the live refresh fails: {stale:?}"
    );
    assert_eq!(stale.error.expect("upstream error").code, -32000);
}

#[tokio::test(flavor = "current_thread")]
async fn read_through_knowledge_search_refreshes_then_list_uses_fresh_cache() {
    let (instance, request) = spawn_json_http_server(json!({
        "result": [{
            "sys_id": "11111111111111111111111111111111",
            "number": "KB0099999",
            "short_description": "Example live article",
            "text": "Example live body",
            "workflow_state": "published",
            "kb_knowledge_base": {"value":"22222222222222222222222222222222","display_value":"Example Knowledge"},
            "kb_category": {"value":"33333333333333333333333333333333","display_value":"Example Category"},
            "sys_updated_on": "2026-08-20 12:00:00"
        }]
    }))
    .await
    .expect("local ServiceNow fake");
    let fixture = build_fixture_state_at_instance(&instance)
        .await
        .expect("fixture");
    fixture
        .state
        .core
        .invalidate_cache_segment("knowledge")
        .await
        .expect("empty Knowledge segment");

    let refreshed = call(
        &fixture.state,
        "search_knowledge",
        json!({"query":"Example live","limit":10}),
    )
    .await
    .result
    .expect("read-through refresh");
    assert_eq!(refreshed["source"], json!({"kind":"live"}));
    request.await.expect("one live refresh");

    let cached = call(
        &fixture.state,
        "list_knowledge_articles",
        json!({"knowledge_base_sys_id":"22222222222222222222222222222222"}),
    )
    .await
    .result
    .expect("fresh cached list");
    assert_eq!(cached["source"]["kind"], "cache");
    assert!(cached["source"]["last_refreshed_at"].is_string());
    assert_eq!(
        cached["completeness"],
        json!({"kind":"partial","reason":"narrowed_projection"})
    );
}
