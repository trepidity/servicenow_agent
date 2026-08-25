//! L0 behavior tests for fail-closed typed-resource metadata discovery.
//!
//! Every test drives the real `SnowCore` against a local ServiceNow fake. The
//! subject is the consumer-visible descriptor, not the internal shape of the
//! discovery code, so these survive a refactor of `DescriptorService`.

use serde_json::json;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::resource::descriptor::{
    Completeness, FieldSupport, PagingSupport, Source, UnavailableReason,
};
use crate::tests::core_for_mock_server;

/// Answer the inheritance probe with a table that has no parent, so dictionary
/// discovery reads exactly one level.
async fn mount_no_ancestors(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_db_object"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": [{ "name": "incident", "super_class": "" }]
        })))
        .mount(server)
        .await;
}

/// A public-safe `sys_dictionary` page for the Incident table.
fn incident_dictionary() -> serde_json::Value {
    json!({
        "result": [
            {
                "name": "incident",
                "element": "short_description",
                "column_label": "Short description",
                "internal_type": { "value": "string", "display_value": "String" },
                "reference": "",
                "choice": "0",
                "read_only": "false",
                "active": "true"
            },
            {
                "name": "incident",
                "element": "assignment_group",
                "column_label": "Assignment group",
                "internal_type": { "value": "reference", "display_value": "Reference" },
                "reference": { "value": "sys_user_group", "display_value": "Group" },
                "choice": "0",
                "read_only": "false",
                "active": "true"
            },
            {
                "name": "incident",
                "element": "state",
                "column_label": "State",
                "internal_type": { "value": "integer", "display_value": "Integer" },
                "reference": "",
                "choice": "1",
                "read_only": "false",
                "active": "true"
            },
            {
                "name": "incident",
                "element": "sys_created_on",
                "column_label": "Created",
                "internal_type": { "value": "glide_date_time", "display_value": "Date/Time" },
                "reference": "",
                "choice": "0",
                "read_only": "true",
                "active": "true"
            }
        ]
    })
}

async fn mount_incident_dictionary(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_dictionary"))
        .respond_with(ResponseTemplate::new(200).set_body_json(incident_dictionary()))
        .mount(server)
        .await;
}

async fn mount_state_choices(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_choice"))
        .and(query_param("sysparm_query", "name=incident^element=state^ORDERBYsequence"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": [
                { "value": "1", "label": "New", "sequence": "1", "inactive": "false", "terminal": "false" },
                { "value": "7", "label": "Closed", "sequence": "2", "inactive": "false", "terminal": "true" }
            ]
        })))
        .mount(server)
        .await;
}

async fn mount_incident_task_inheritance(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_db_object"))
        .and(query_param("sysparm_query", "name=incident"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": [{
                "name": "incident",
                "super_class": { "value": "task-table-sys-id", "display_value": "Task" }
            }]
        })))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_db_object"))
        .and(query_param("sysparm_query", "sys_id=task-table-sys-id"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": [{ "name": "task" }]
        })))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_db_object"))
        .and(query_param("sysparm_query", "name=task"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": [{ "name": "task", "super_class": "" }]
        })))
        .mount(server)
        .await;
}

#[tokio::test]
async fn incident_fields_reports_only_dictionary_discovered_fields() {
    let server = MockServer::start().await;
    mount_no_ancestors(&server).await;
    mount_incident_dictionary(&server).await;
    mount_state_choices(&server).await;
    let (core, _tempdir) = core_for_mock_server(&server).await;

    let envelope = core.incident_fields().await.expect("descriptor");

    assert_eq!(envelope.operation, "incident_fields");
    assert_eq!(envelope.source, Source::Live);
    assert_eq!(envelope.completeness, Completeness::Complete);
    assert_eq!(envelope.data.table, "incident");

    let readable = envelope
        .data
        .readable_fields
        .available()
        .expect("readable fields discovered");
    let names: Vec<&str> = readable.iter().map(|field| field.name.as_str()).collect();
    assert_eq!(
        names,
        vec![
            "assignment_group",
            "short_description",
            "state",
            "sys_created_on"
        ],
        "only dictionary-discovered fields, sorted by native name"
    );
}

#[tokio::test]
async fn incident_fields_preserves_native_types_and_reference_targets() {
    let server = MockServer::start().await;
    mount_no_ancestors(&server).await;
    mount_incident_dictionary(&server).await;
    mount_state_choices(&server).await;
    let (core, _tempdir) = core_for_mock_server(&server).await;

    let envelope = core.incident_fields().await.expect("descriptor");
    let readable = envelope.data.readable_fields.available().expect("readable");

    let group = readable
        .iter()
        .find(|field| field.name == "assignment_group")
        .expect("assignment_group discovered");
    assert_eq!(group.kind, "reference", "native internal_type is preserved");
    assert_eq!(
        group.reference_table.as_deref(),
        Some("sys_user_group"),
        "reference target comes from the dictionary, not a guess"
    );

    let short_description = readable
        .iter()
        .find(|field| field.name == "short_description")
        .expect("short_description discovered");
    assert_eq!(
        short_description.reference_table, None,
        "a non-reference field carries no reference target"
    );
}

#[tokio::test]
async fn incident_fields_omits_read_only_fields_from_writable() {
    let server = MockServer::start().await;
    mount_no_ancestors(&server).await;
    mount_incident_dictionary(&server).await;
    mount_state_choices(&server).await;
    let (core, _tempdir) = core_for_mock_server(&server).await;

    let envelope = core.incident_fields().await.expect("descriptor");
    let writable = envelope.data.writable_fields.available().expect("writable");
    let names: Vec<&str> = writable.iter().map(|field| field.name.as_str()).collect();

    assert!(
        !names.contains(&"sys_created_on"),
        "ServiceNow marks sys_created_on read_only, so it is not writable: {names:?}"
    );
    assert!(names.contains(&"short_description"));
}

#[tokio::test]
async fn incident_fields_returns_choices_only_for_choice_fields() {
    let server = MockServer::start().await;
    mount_no_ancestors(&server).await;
    mount_incident_dictionary(&server).await;
    mount_state_choices(&server).await;
    let (core, _tempdir) = core_for_mock_server(&server).await;

    let envelope = core.incident_fields().await.expect("descriptor");
    let readable = envelope.data.readable_fields.available().expect("readable");

    let state = readable
        .iter()
        .find(|field| field.name == "state")
        .expect("state discovered");
    let choices = state.choices.available().expect("state choices discovered");
    assert_eq!(
        choices.iter().map(|c| c.value.as_str()).collect::<Vec<_>>(),
        vec!["1", "7"]
    );
    assert!(
        choices.iter().any(|c| c.value == "7" && c.terminal),
        "terminal flag survives discovery"
    );

    let short_description = readable
        .iter()
        .find(|field| field.name == "short_description")
        .expect("short_description discovered");
    assert_eq!(
        short_description.choices,
        FieldSupport::Unavailable {
            reason: UnavailableReason::NotSupportedByOperation
        },
        "a non-choice field reports unsupported, never an empty choice list"
    );
}

#[tokio::test]
async fn incident_fields_discovers_choices_inherited_from_a_parent_table() {
    let server = MockServer::start().await;
    mount_incident_task_inheritance(&server).await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_dictionary"))
        .and(query_param("sysparm_query", "name=incident^active=true"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "result": [] })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_dictionary"))
        .and(query_param("sysparm_query", "name=task^active=true"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": [{
                "name": "task",
                "element": "state",
                "column_label": "State",
                "internal_type": { "value": "integer", "display_value": "Integer" },
                "reference": "",
                "choice": "1",
                "read_only": "false",
                "active": "true"
            }]
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_choice"))
        .and(query_param(
            "sysparm_query",
            "name=incident^element=state^ORDERBYsequence",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "result": [] })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_choice"))
        .and(query_param("sysparm_query", "name=task^element=state^ORDERBYsequence"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": [
                { "value": "1", "label": "Open", "sequence": "1", "inactive": "false", "terminal": "false" },
                { "value": "3", "label": "Closed", "sequence": "2", "inactive": "false", "terminal": "true" }
            ]
        })))
        .mount(&server)
        .await;
    let (core, _tempdir) = core_for_mock_server(&server).await;

    let envelope = core.incident_fields().await.expect("descriptor");
    let state = envelope
        .data
        .readable_fields
        .available()
        .expect("readable")
        .iter()
        .find(|field| field.name == "state")
        .expect("inherited state field");
    let values = state
        .choices
        .available()
        .expect("inherited choices")
        .iter()
        .map(|choice| choice.value.as_str())
        .collect::<Vec<_>>();

    assert_eq!(values, vec!["1", "3"]);
}

#[tokio::test]
async fn incident_fields_reports_unavailable_rather_than_empty_when_dictionary_is_silent() {
    let server = MockServer::start().await;
    mount_no_ancestors(&server).await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_dictionary"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "result": [] })))
        .mount(&server)
        .await;
    let (core, _tempdir) = core_for_mock_server(&server).await;

    let envelope = core.incident_fields().await.expect("descriptor");

    // This is the whole point of FieldSupport: "the instance told us nothing"
    // must not be reportable as "this table has no fields".
    assert_eq!(
        envelope.data.readable_fields,
        FieldSupport::Unavailable {
            reason: UnavailableReason::NotReturnedByInstance
        }
    );
    assert_eq!(
        envelope.data.writable_fields,
        FieldSupport::Unavailable {
            reason: UnavailableReason::NotReturnedByInstance
        }
    );
    assert!(!envelope.data.readable_fields.is_available());
}

#[tokio::test]
async fn incident_fields_reports_acl_denial_as_unavailable_not_as_an_error() {
    let server = MockServer::start().await;
    mount_no_ancestors(&server).await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_dictionary"))
        .respond_with(ResponseTemplate::new(403).set_body_json(json!({
            "error": { "message": "Forbidden", "detail": "" }
        })))
        .mount(&server)
        .await;
    let (core, _tempdir) = core_for_mock_server(&server).await;

    let envelope = core
        .incident_fields()
        .await
        .expect("ACL denial is a descriptor state, not a transport failure");

    assert_eq!(
        envelope.data.readable_fields,
        FieldSupport::Unavailable {
            reason: UnavailableReason::AclDenied
        },
        "a permissions problem must be distinguishable from an empty table"
    );
}

#[tokio::test]
async fn incident_fields_advertises_native_cursor_paging_bounds() {
    let server = MockServer::start().await;
    mount_no_ancestors(&server).await;
    mount_incident_dictionary(&server).await;
    mount_state_choices(&server).await;
    let (core, _tempdir) = core_for_mock_server(&server).await;

    let envelope = core.incident_fields().await.expect("descriptor");

    assert_eq!(
        envelope.data.paging,
        PagingSupport::Cursor {
            default_limit: 50,
            max_limit: 200
        },
        "paging bounds match the approved B-OPS-01 Incidents row"
    );
}
