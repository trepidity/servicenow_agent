//! L0 consumer seam: compiled CLI -> ServiceNow Table API -> durable export artifact.

use std::fs;
use std::io::{Cursor, Read};
use std::process::Command;

use serde_json::{Value, json};
use wiremock::matchers::{method, path, query_param_contains};
use wiremock::{Mock, MockServer, ResponseTemplate};

const DEMAND_SYS_ID: &str = "0123456789abcdef0123456789abcdef";
const PROJECT_SYS_ID: &str = "fedcba9876543210fedcba9876543210";

#[tokio::test(flavor = "multi_thread")]
async fn compiled_cli_exports_allowlisted_tables_to_declared_formats() {
    let server = MockServer::start().await;
    mount_record(
        &server,
        "dmn_demand",
        "DMND0001234",
        DEMAND_SYS_ID,
        "Example demand",
    )
    .await;
    mount_record(
        &server,
        "pm_project",
        "PRJ0001234",
        PROJECT_SYS_ID,
        "Example project",
    )
    .await;

    let home = tempfile::tempdir().expect("temporary home");
    let config_dir = home.path().join(".config/snow");
    let exports = home.path().join("exports");
    fs::create_dir_all(&config_dir).expect("config directory");
    fs::create_dir_all(&exports).expect("export directory");
    fs::write(
        config_dir.join(".env.test"),
        format!(
            "SERVICENOW_INSTANCE={}\nSERVICENOW_USERNAME=user@example.com\nSERVICENOW_PASSWORD=test-password\nSNOW_ALLOW_LOOPBACK_HTTP=true\n",
            server.uri()
        ),
    )
    .expect("test environment");

    for (format, filename) in [
        ("json", "demand.json"),
        ("jsonl", "demand.jsonl"),
        ("csv", "demand.csv"),
        ("markdown", "demand.md"),
        ("xlsx", "demand.xlsx"),
    ] {
        let output = exports.join(filename);
        let result = run_export(
            home.path(),
            &[
                "--table",
                "demand",
                "--number",
                "DMND0001234",
                "--format",
                format,
            ],
            &output,
        );
        assert!(
            result.status.success(),
            "{format} export failed: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert!(output.is_file(), "{format} export was not created");
    }

    let json: Value = serde_json::from_slice(&fs::read(exports.join("demand.json")).expect("JSON"))
        .expect("valid JSON export");
    assert_eq!(json["sys_id"], DEMAND_SYS_ID);
    assert_eq!(json["number"], "DMND0001234");

    let jsonl = fs::read_to_string(exports.join("demand.jsonl")).expect("JSONL");
    assert_eq!(
        serde_json::from_str::<Value>(jsonl.trim()).expect("valid JSONL record")["number"],
        "DMND0001234"
    );

    let csv = fs::read_to_string(exports.join("demand.csv")).expect("CSV");
    assert!(csv.starts_with("field,value\n"), "CSV: {csv}");
    assert!(csv.contains("number,DMND0001234"), "CSV: {csv}");

    let markdown = fs::read_to_string(exports.join("demand.md")).expect("Markdown");
    assert!(
        markdown.contains("# Demand DMND0001234"),
        "Markdown: {markdown}"
    );
    assert!(markdown.contains("Example demand"), "Markdown: {markdown}");

    let workbook = fs::read(exports.join("demand.xlsx")).expect("XLSX");
    assert!(
        workbook.starts_with(b"PK\x03\x04"),
        "XLSX is not a ZIP workbook"
    );
    let mut archive = zip::ZipArchive::new(Cursor::new(workbook)).expect("open XLSX workbook");
    let mut shared_strings = String::new();
    archive
        .by_name("xl/sharedStrings.xml")
        .expect("XLSX shared strings")
        .read_to_string(&mut shared_strings)
        .expect("read XLSX shared strings");
    assert!(
        shared_strings.contains("DMND0001234") && shared_strings.contains("Example demand"),
        "XLSX did not preserve exported values: {shared_strings}"
    );

    let project_output = exports.join("project.json");
    let project = run_export(
        home.path(),
        &[
            "--table",
            "project",
            "--sys-id",
            PROJECT_SYS_ID,
            "--format",
            "json",
        ],
        &project_output,
    );
    assert!(
        project.status.success(),
        "project export failed: {}",
        String::from_utf8_lossy(&project.stderr)
    );
    let project: Value = serde_json::from_slice(&fs::read(project_output).expect("project JSON"))
        .expect("valid JSON");
    assert_eq!(project["sys_id"], PROJECT_SYS_ID);
    assert_eq!(project["number"], "PRJ0001234");

    let disallowed_output = exports.join("disallowed.json");
    let disallowed = run_export(
        home.path(),
        &[
            "--table",
            "kb-knowledge",
            "--number",
            "KB0001234",
            "--format",
            "json",
        ],
        &disallowed_output,
    );
    assert!(
        !disallowed.status.success(),
        "disallowed table unexpectedly exported"
    );
    assert!(
        String::from_utf8_lossy(&disallowed.stderr).contains("invalid value"),
        "unexpected disallowed-table error: {}",
        String::from_utf8_lossy(&disallowed.stderr)
    );
    assert!(
        !disallowed_output.exists(),
        "disallowed table created an artifact"
    );
}

async fn mount_record(
    server: &MockServer,
    table: &str,
    number: &str,
    sys_id: &str,
    description: &str,
) {
    let record = json!({
        "sys_id": { "value": sys_id, "display_value": sys_id },
        "number": { "value": number, "display_value": number },
        "short_description": { "value": description, "display_value": description },
        "state": { "value": "1", "display_value": "New" },
        "description": { "value": "Example export detail", "display_value": "Example export detail" }
    });
    Mock::given(method("GET"))
        .and(path(format!("/api/now/table/{table}")))
        .and(query_param_contains(
            "sysparm_query",
            format!("number={number}"),
        ))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({ "result": [record.clone()] })),
        )
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/api/now/table/{table}/{sys_id}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "result": record })))
        .mount(server)
        .await;
}

fn run_export(
    home: &std::path::Path,
    args: &[&str],
    output: &std::path::Path,
) -> std::process::Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_snow"));
    command.args(["--env", "test", "export"]);
    command.args(args);
    command.arg("--output").arg(output);
    command.env("HOME", home).env("SNOW_ENV", "test");
    command.output().expect("run compiled snow export CLI")
}
