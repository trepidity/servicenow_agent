//! L0: daemon/MCP consumer calls with a provider-only HTTP fake.
use super::{JsonRpcRequest, dispatch};
use crate::test_support::build_fixture_state_with_ui_metadata;
use serde_json::{Value, json};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path, query_param},
};

const ID: &str = "11111111111111111111111111111111";
const NAME: &str = "FY27.Q1.03  Example Team";

fn sprint() -> Value {
    json!({"sys_id":ID,"number":"SPNT0000001","short_description":NAME,"sys_class_name":"rm_sprint"})
}

async fn call(
    state: &std::sync::Arc<crate::DaemonState>,
    method: &str,
    params: Value,
) -> super::JsonRpcResponse {
    dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".into(),
            method: method.into(),
            params,
            id: Some(json!(1)),
        },
        state,
    )
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn sprint_name_lookup_preserves_duplicate_candidates() {
    let instance = MockServer::start().await;
    Mock::given(method("GET")).and(path("/api/now/table/rm_sprint"))
        .and(query_param("sysparm_query",format!("short_description={NAME}^ORDERBYsys_id")))

        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"result":[sprint(),{"sys_id":"22222222222222222222222222222222","number":"SPNT0000002","short_description":NAME}]})))
        .expect(1).mount(&instance).await;
    Mock::given(method("GET")).and(path("/api/now/ui/meta/rm_sprint"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"result":{"columns":{"short_description":{"type":"string"},"sys_id":{"type":"GUID"}}}}))).mount(&instance).await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_dictionary"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"result":[{"element":"short_description"}]})),
        )
        .mount(&instance)
        .await;
    let fixture = build_fixture_state_with_ui_metadata(&instance.uri())
        .await
        .unwrap();
    let first = call(
        &fixture.state,
        "record_resolve",
        json!({"name":NAME,"table":"rm_sprint"}),
    )
    .await;
    assert!(first.error.is_none(), "{first:?}");
    let first = first.result.unwrap();
    assert_eq!(first["records"][0]["sys_id"], ID);
    assert_eq!(first["records"][1]["number"], "SPNT0000002");
    assert_eq!(first["complete"], true);
    assert_eq!(first["match_count"], 2);
    assert_eq!(first["cursor"], Value::Null);
}

#[tokio::test(flavor = "current_thread")]
async fn sprint_get_record_accepts_number_and_allowlisted_table_sys_id() {
    let instance = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/rm_sprint"))
        .and(query_param("sysparm_query", "number=SPNT0000001"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"result":[sprint()]})))
        .mount(&instance)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/api/now/table/rm_sprint/{ID}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"result":sprint()})))
        .mount(&instance)
        .await;
    let fixture = build_fixture_state_with_ui_metadata(&instance.uri())
        .await
        .unwrap();
    for selector in [
        json!({"number":"SPNT0000001"}),
        json!({"table":"rm_sprint","sys_id":ID}),
    ] {
        let response = call(&fixture.state, "get_record", selector).await;
        assert!(response.error.is_none(), "{response:?}");
        let result = response.result.unwrap();
        assert_eq!(result["record"]["sys_id"], ID);
        assert_eq!(result["record"]["number"], "SPNT0000001");
        assert_eq!(result["record"]["table"], "rm_sprint");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn sprint_query_rejects_invalid_selectors_before_provider_access() {
    let instance = MockServer::start().await;
    let fixture = build_fixture_state_with_ui_metadata(&instance.uri())
        .await
        .unwrap();
    for params in [
        json!({"name":" "}),
        json!({"name":"Example^ORactive=true"}),
        json!({"name":NAME,"table":"task^ORactive=true"}),
        json!({"name":NAME,"query":"active=true"}),
        json!({"name":NAME,"cursor":"bad"}),
        json!({"name":NAME,"sys_id":ID}),
    ] {
        assert_eq!(
            call(&fixture.state, "record_resolve", params)
                .await
                .error
                .unwrap()
                .code,
            -32602
        );
    }
    assert!(instance.received_requests().await.unwrap().is_empty());
}
