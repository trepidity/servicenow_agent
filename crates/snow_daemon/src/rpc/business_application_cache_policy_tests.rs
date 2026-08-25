//! L0 daemon JSON-RPC seam for Business Application cache-policy behavior.

use serde_json::{Value, json};

use super::{JsonRpcRequest, dispatch};
use crate::test_support::{
    build_fixture_state_at_instance, spawn_json_http_sequence_server, spawn_json_http_server,
};

fn live_business_application_list() -> Value {
    business_application_list(
        "54a4b61b6fe845000ed852a03f3ee4d0",
        "Example Business Application",
    )
}

fn business_application_list(sys_id: &str, name: &str) -> Value {
    json!({
        "result": [{
            "sys_id": sys_id,
            "name": name,
            "short_description": name,
            "sys_class_name": "cmdb_ci_business_app",
            "operational_status": { "value": "1", "display_value": "Operational" },
            "sys_updated_on": "2026-08-20 12:00:00"
        }]
    })
}

fn age_business_application_cache(path: &std::path::Path) {
    let engine = snow_core::query::QueryEngine::open(path.join("snow.db")).expect("open cache");
    let mut rows = engine
        .store()
        .list_active_records(Some(snow_core::ResourceType::BusinessApplication))
        .expect("cached applications");
    assert!(!rows.is_empty(), "expected cached applications to age");
    for row in &mut rows {
        row.synced_at = chrono::Utc::now() - chrono::Duration::days(366);
        engine
            .store()
            .upsert_record(row, "", row.description.as_deref().unwrap_or_default())
            .expect("age cached application");
    }
    let vault = path.join("vault/business_applications");
    if vault.exists() {
        std::fs::remove_dir_all(vault).expect("remove fresh vault projection");
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
            id: Some(json!(23)),
        },
        state,
    )
    .await
}

fn live_business_application() -> Value {
    json!({
        "result": {
            "sys_id": "54a4b61b6fe845000ed852a03f3ee4d0",
            "name": "Example Business Application",
            "short_description": "Example Business Application",
            "sys_class_name": "cmdb_ci_business_app",
            "operational_status": { "value": "1", "display_value": "Operational" },
            "sys_updated_on": "2026-08-20 12:00:00"
        }
    })
}

#[tokio::test(flavor = "current_thread")]
async fn live_get_does_not_persist_and_cache_only_miss_never_contacts_servicenow() {
    let (instance, request) = spawn_json_http_server(live_business_application())
        .await
        .expect("local ServiceNow fake");
    let fixture = build_fixture_state_at_instance(&instance)
        .await
        .expect("fixture");
    let policy_path = fixture.tempdir.path().join("cache-policy.toml");
    std::fs::write(
        &policy_path,
        "version = 1\n[operations.business_application_get]\nobject = \"business_application\"\nmode = \"live\"\n",
    )
    .expect("live policy");
    assert!(
        call(&fixture.state, "cache_policy_reload", json!({}))
            .await
            .error
            .is_none()
    );

    let live = call(
        &fixture.state,
        "business_application_get",
        json!({"sys_id":"54a4b61b6fe845000ed852a03f3ee4d0"}),
    )
    .await;
    assert!(live.error.is_none(), "live read failed: {:?}", live.error);
    let live_result = live.result.expect("live result");
    assert_eq!(live_result["source"], json!({"kind":"live"}));
    assert_eq!(live_result["completeness"], json!({"kind":"complete"}));
    assert_eq!(
        live_result["data"]["business_application"]["name"],
        "Example Business Application"
    );
    assert!(
        !fixture
            .tempdir
            .path()
            .join("vault/business_applications")
            .exists(),
        "live mode must not create a Business Application vault projection"
    );
    assert!(
        request.await.expect("live request").starts_with(
            "GET /api/now/table/cmdb_ci_business_app/54a4b61b6fe845000ed852a03f3ee4d0"
        )
    );

    std::fs::write(
        &policy_path,
        "version = 1\n[operations.business_application_get]\nobject = \"business_application\"\nmode = \"cache_only\"\nttl = \"30d\"\n",
    )
    .expect("cache-only policy");
    assert!(
        call(&fixture.state, "cache_policy_reload", json!({}))
            .await
            .error
            .is_none()
    );
    let miss = call(
        &fixture.state,
        "business_application_get",
        json!({"sys_id":"54a4b61b6fe845000ed852a03f3ee4d0"}),
    )
    .await;
    let error = miss.error.expect("cache-only miss");
    assert_eq!(error.code, -32072);
    assert_eq!(
        error.data,
        Some(json!({
            "code":"CACHE_MISS",
            "operation":"business_application_get",
            "object":"business_application"
        }))
    );
}

#[tokio::test(flavor = "current_thread")]
async fn read_through_get_persists_complete_live_result_then_serves_fresh_cache() {
    let (instance, request) = spawn_json_http_server(live_business_application())
        .await
        .expect("local ServiceNow fake");
    let fixture = build_fixture_state_at_instance(&instance)
        .await
        .expect("fixture");
    let params = json!({"sys_id":"54a4b61b6fe845000ed852a03f3ee4d0"});

    let refreshed = call(&fixture.state, "business_application_get", params.clone()).await;
    assert!(
        refreshed.error.is_none(),
        "read-through refresh failed: {:?}",
        refreshed.error
    );
    assert_eq!(
        refreshed.result.as_ref().expect("refreshed result")["source"],
        json!({"kind":"live"})
    );
    request.await.expect("one live refresh");
    assert!(
        fixture
            .tempdir
            .path()
            .join("vault/business_applications/business_application_54a4b61b6fe845000ed852a03f3ee4d0_example-business-application.md")
            .exists(),
        "read-through refresh must persist the complete projection"
    );

    let cached = call(&fixture.state, "business_application_get", params).await;
    assert!(
        cached.error.is_none(),
        "fresh cache hit failed: {:?}",
        cached.error
    );
    let cached_result = cached.result.expect("cached result");
    assert_eq!(cached_result["source"]["kind"], "cache");
    assert!(cached_result["source"]["last_refreshed_at"].is_string());
    assert_eq!(
        cached_result["completeness"],
        json!({"kind":"partial","reason":"narrowed_projection"})
    );
    assert_eq!(cached_result["operation"], "business_application_get");
}

#[tokio::test(flavor = "current_thread")]
async fn cache_only_search_and_query_miss_without_contacting_servicenow() {
    let fixture = build_fixture_state_at_instance("http://127.0.0.1:9")
        .await
        .expect("fixture");
    std::fs::write(
        fixture.tempdir.path().join("cache-policy.toml"),
        concat!(
            "version = 1\n",
            "[operations.business_application_search]\n",
            "object = \"business_application\"\n",
            "mode = \"cache_only\"\n",
            "ttl = \"30d\"\n",
            "[operations.business_application_query]\n",
            "object = \"business_application\"\n",
            "mode = \"cache_only\"\n",
            "ttl = \"30d\"\n",
        ),
    )
    .expect("cache-only policy");
    assert!(
        call(&fixture.state, "cache_policy_reload", json!({}))
            .await
            .error
            .is_none()
    );

    for method in ["business_application_search", "business_application_query"] {
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
                "object":"business_application"
            })),
            "{method}"
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn live_query_has_zero_persistence_then_cache_only_query_misses() {
    let (instance, request) = spawn_json_http_server(live_business_application_list())
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
            "[operations.business_application_query]\n",
            "object = \"business_application\"\n",
            "mode = \"live\"\n",
        ),
    )
    .expect("live policy");
    assert!(
        call(&fixture.state, "cache_policy_reload", json!({}))
            .await
            .error
            .is_none()
    );

    let live = call(
        &fixture.state,
        "business_application_query",
        json!({"limit":10}),
    )
    .await
    .result
    .expect("live query");
    assert_eq!(live["source"], json!({"kind":"live"}));
    assert_eq!(live["completeness"], json!({"kind":"complete"}));
    request.await.expect("one live query");

    std::fs::write(
        &policy,
        concat!(
            "version = 1\n",
            "[operations.business_application_query]\n",
            "object = \"business_application\"\n",
            "mode = \"cache_only\"\n",
            "ttl = \"30d\"\n",
        ),
    )
    .expect("cache-only policy");
    assert!(
        call(&fixture.state, "cache_policy_reload", json!({}))
            .await
            .error
            .is_none()
    );
    assert_eq!(
        call(&fixture.state, "business_application_query", json!({}))
            .await
            .error
            .expect("cache-only miss")
            .code,
        -32072
    );
}

#[tokio::test(flavor = "current_thread")]
async fn read_through_search_refreshes_then_query_uses_fresh_cache() {
    let (instance, request) = spawn_json_http_server(live_business_application_list())
        .await
        .expect("local ServiceNow fake");
    let fixture = build_fixture_state_at_instance(&instance)
        .await
        .expect("fixture");

    let refreshed = call(
        &fixture.state,
        "business_application_search",
        json!({"limit":10}),
    )
    .await
    .result
    .expect("read-through search");
    assert_eq!(refreshed["source"], json!({"kind":"live"}));
    request.await.expect("one live refresh");

    let cached = call(
        &fixture.state,
        "business_application_query",
        json!({"limit":10}),
    )
    .await
    .result
    .expect("fresh cached query");
    assert_eq!(cached["source"]["kind"], "cache");
    assert!(cached["source"]["last_refreshed_at"].is_string());
    assert_eq!(
        cached["completeness"],
        json!({"kind":"partial","reason":"narrowed_projection"})
    );
}

#[tokio::test(flavor = "current_thread")]
async fn read_through_search_with_fresh_nonmatching_cache_falls_back_to_live() {
    let live_name = "Uncached Business Application";
    let (instance, requests) = spawn_json_http_sequence_server(vec![
        live_business_application_list(),
        business_application_list("54a4b61b6fe845000ed852a03f3ee4d1", live_name),
    ])
    .await
    .expect("local ServiceNow fake");
    let fixture = build_fixture_state_at_instance(&instance)
        .await
        .expect("fixture");

    let warmed = call(
        &fixture.state,
        "business_application_search",
        json!({"limit":10}),
    )
    .await
    .result
    .expect("initial live cache warm");
    assert_eq!(warmed["source"], json!({"kind":"live"}));

    let fallback = call(
        &fixture.state,
        "business_application_search",
        json!({"name":live_name, "limit":10}),
    )
    .await
    .result
    .expect("live fallback for nonmatching fresh cache");
    assert_eq!(fallback["source"], json!({"kind":"live"}));
    assert_eq!(
        fallback["data"]["business_applications"][0]["name"],
        live_name
    );
    assert_eq!(requests.await.expect("two ServiceNow reads").len(), 2);
}

#[tokio::test(flavor = "current_thread")]
async fn cache_only_search_with_fresh_nonmatching_cache_returns_empty_without_live_fallback() {
    let (instance, request) = spawn_json_http_server(live_business_application_list())
        .await
        .expect("local ServiceNow fake");
    let fixture = build_fixture_state_at_instance(&instance)
        .await
        .expect("fixture");

    call(
        &fixture.state,
        "business_application_search",
        json!({"limit":10}),
    )
    .await
    .result
    .expect("initial live cache warm");
    request.await.expect("initial ServiceNow read");

    std::fs::write(
        fixture.tempdir.path().join("cache-policy.toml"),
        concat!(
            "version = 1\n",
            "[operations.business_application_search]\n",
            "object = \"business_application\"\n",
            "mode = \"cache_only\"\n",
            "ttl = \"30d\"\n",
        ),
    )
    .expect("cache-only policy");
    assert!(
        call(&fixture.state, "cache_policy_reload", json!({}))
            .await
            .error
            .is_none()
    );

    let cached = call(
        &fixture.state,
        "business_application_search",
        json!({"name":"Uncached Business Application", "limit":10}),
    )
    .await;
    assert!(
        cached.error.is_none(),
        "cache-only search failed: {cached:?}"
    );
    let cached = cached.result.expect("cache-only result");
    assert_eq!(cached["source"]["kind"], "cache");
    assert_eq!(cached["data"]["business_applications"], json!([]));
}

#[tokio::test(flavor = "current_thread")]
async fn stale_read_through_query_refresh_failure_never_falls_back() {
    let (instance, request) = spawn_json_http_server(live_business_application_list())
        .await
        .expect("local ServiceNow fake");
    let fixture = build_fixture_state_at_instance(&instance)
        .await
        .expect("fixture");
    let params = json!({"limit":10});
    let first = call(&fixture.state, "business_application_query", params.clone()).await;
    assert!(
        first.error.is_none(),
        "initial refresh failed: {:?}",
        first.error
    );
    request.await.expect("initial live refresh");
    age_business_application_cache(fixture.tempdir.path());

    let stale = call(&fixture.state, "business_application_query", params).await;
    assert!(
        stale.result.is_none(),
        "stale Business Application data must not be returned: {stale:?}"
    );
    assert_eq!(stale.error.expect("upstream error").code, -32000);
}
