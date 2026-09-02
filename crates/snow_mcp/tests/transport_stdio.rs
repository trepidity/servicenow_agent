mod support;

use anyhow::Result;
use snow_mcp::{McpServer, RESULT_TOO_LARGE_CODE};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, duplex, split};

#[tokio::test(flavor = "current_thread")]
async fn stdio_initialize_tools_call_and_resource_round_trip() {
    let fixture = support::build_fixture_state().await.expect("fixture");
    let server = McpServer::new(fixture.core);

    tokio::task::LocalSet::new()
        .run_until(async move {
            let (client_side, server_side) = duplex(4096);
            let (server_reader, server_writer) = split(server_side);

            tokio::task::spawn_local(async move {
                let _ = server
                    .serve_streams(
                        BufReader::new(server_reader),
                        server_writer,
                        std::future::pending::<Result<(), std::io::Error>>(),
                    )
                    .await;
            });

            let (client_reader, mut client_writer) = split(client_side);
            let mut client_reader = BufReader::new(client_reader);

            client_writer
                .write_all(br#"{"jsonrpc":"2.0","method":"initialize","id":1}"#)
                .await
                .expect("initialize write");
            client_writer.write_all(b"\n").await.expect("newline");

            client_writer
                .write_all(br#"{"jsonrpc":"2.0","method":"tools/call","params":{"name":"get_record","arguments":{"number":"CHG001"}},"id":2}"#)
                .await
                .expect("get_record write");
            client_writer.write_all(b"\n").await.expect("newline");

            client_writer
                .write_all(br#"{"jsonrpc":"2.0","method":"resources/read","params":{"uri":"snow://records/CHG001"},"id":3}"#)
                .await
                .expect("resource write");
            client_writer.write_all(b"\n").await.expect("newline");

            let mut line = String::new();
            client_reader.read_line(&mut line).await.expect("initialize read");
            assert!(line.contains("protocolVersion"));
            line.clear();

            client_reader.read_line(&mut line).await.expect("tool read");
            assert!(line.contains("CHG001"));
            assert!(line.contains("Database patch"));
            line.clear();

            client_reader.read_line(&mut line).await.expect("resource read");
            assert!(line.contains("Database patch"));
            assert!(line.ends_with('\n'));
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn stdio_rejects_an_oversized_request_frame() {
    let fixture = support::build_fixture_state().await.expect("fixture");
    let server = McpServer::new(fixture.core);
    let request = format!(
        r#"{{"jsonrpc":"2.0","method":"initialize","params":{{"padding":"{}"}},"id":1}}\n"#,
        "x".repeat(300 * 1024),
    );
    let mut output = Vec::new();

    server
        .serve_streams(
            BufReader::new(request.as_bytes()),
            &mut output,
            std::future::pending::<Result<(), std::io::Error>>(),
        )
        .await
        .expect("oversized input should receive a structured rejection");

    let response: serde_json::Value = serde_json::from_slice(&output).expect("JSON-RPC error");
    assert_eq!(response["error"]["code"], RESULT_TOO_LARGE_CODE);
    assert_eq!(response["error"]["data"]["code"], "RESULT_TOO_LARGE");
}
