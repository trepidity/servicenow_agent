mod support;

use serde_json::json;
use snow_core::vault::markdown::{render_knowledge_article, render_snow_record};
use snow_mcp::{JsonRpcRequest, McpServer};

#[tokio::test]
async fn resources_list_and_read_match_core_markdown_rendering() {
    let fixture = support::build_fixture_state().await.expect("fixture");
    let server = McpServer::new(fixture.core.clone());

    let list = server
        .dispatch(JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "resources/list".to_string(),
            params: json!({}),
            id: Some(json!(1)),
        })
        .await
        .result
        .expect("list result");
    let resources = list["resources"].as_array().expect("resources");
    assert!(
        resources
            .iter()
            .any(|r| r["name"] == "ServiceNow record" && r["uri"] == "snow://records/{number}")
    );
    assert!(
        resources
            .iter()
            .any(|r| r["name"] == "ServiceNow knowledge article"
                && r["uri"] == "snow://knowledge/{number}")
    );
    assert!(
        resources
            .iter()
            .any(|r| r["name"] == "ServiceNow dashboard" && r["uri"] == "snow://dashboard")
    );

    let record = fixture
        .core
        .get_record("CHG001")
        .await
        .expect("record query")
        .expect("record");
    let read_record = server
        .dispatch(JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "resources/read".to_string(),
            params: json!({ "uri": "snow://records/CHG001" }),
            id: Some(json!(2)),
        })
        .await
        .result
        .expect("record resource result");
    assert_eq!(read_record["mimeType"], "text/markdown");
    assert_eq!(read_record["text"], render_snow_record(&record));

    let article = fixture
        .core
        .get_knowledge_article("KB001")
        .await
        .expect("article query")
        .expect("article");
    let read_article = server
        .dispatch(JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "resources/read".to_string(),
            params: json!({ "uri": "snow://knowledge/KB001" }),
            id: Some(json!(3)),
        })
        .await
        .result
        .expect("article resource result");
    assert_eq!(read_article["mimeType"], "text/markdown");
    assert_eq!(read_article["text"], render_knowledge_article(&article));
}
