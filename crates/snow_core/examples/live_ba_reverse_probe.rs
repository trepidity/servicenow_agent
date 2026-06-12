use std::collections::{BTreeMap, BTreeSet, VecDeque};

use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::Value;
use servicenow_rs::prelude::{BasicAuth, DisplayValue, Record, ServiceNowClient};
use snow_core::{CredentialProvider, normalize_record_lookup_sys_id};

const BUSINESS_APPLICATION_TABLE: &str = "cmdb_ci_business_app";
const DEFAULT_RELATIONSHIP_TYPES: &[&str] = &[
    "Depends on::Used by",
    "Runs on::Runs",
    "Contains::Contained by",
    "Hosted on::Hosts",
    "Instantiates::Instantiated by",
    "Members::Member of",
];
const SERVICE_DISCOVERY_RELATIONSHIP_TYPES: &[&str] = &["Consumes::Consumed by", "Uses::Used by"];

#[derive(Debug, Serialize)]
struct ServerSummary {
    sys_id: String,
    name: Option<String>,
    class_name: Option<String>,
    operational_status: Option<String>,
}

#[derive(Debug, Serialize)]
struct BusinessApplicationHit {
    sys_id: String,
    number: Option<String>,
    name: Option<String>,
    operational_state: Option<String>,
    provenance: String,
    path: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ProbeOutput {
    server: ServerSummary,
    relationship_rows_examined: usize,
    service_membership_rows_examined: usize,
    business_applications: Vec<BusinessApplicationHit>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let instance = std::env::var("SERVICENOW_INSTANCE")
        .or_else(|_| std::env::var("SNOW_INSTANCE"))
        .context("SERVICENOW_INSTANCE/SNOW_INSTANCE is not set")?;
    let username = snow_core::credential::resolve_username_from_runtime_env()?;
    let password = CredentialProvider::from_runtime_env().resolve()?;
    let server_sys_id = std::env::var("SERVICENOW_SERVER_SYS_ID")
        .or_else(|_| std::env::var("SNOW_SERVER_SYS_ID"))
        .context("SERVICENOW_SERVER_SYS_ID/SNOW_SERVER_SYS_ID is not set")?;
    let server_sys_id = normalize_record_lookup_sys_id(&server_sys_id)?;
    let auth = BasicAuth::new(&username, password.as_str()).without_session();
    let client = ServiceNowClient::builder()
        .instance(&instance)
        .auth(auth)
        .build()
        .await?;
    drop(password);

    let server = client
        .table("cmdb_ci_server")
        .display_value(DisplayValue::Both)
        .exclude_reference_link(true)
        .get(&server_sys_id)
        .await?;

    let mut hits = BTreeMap::<String, BusinessApplicationHit>::new();
    let (relationship_rows_examined, direct_hits) =
        reverse_relationship_hits(&client, &server_sys_id).await?;
    for hit in direct_hits {
        hits.entry(hit.sys_id.clone()).or_insert(hit);
    }

    let (service_membership_rows_examined, service_hits) =
        reverse_service_membership_hits(&client, &server_sys_id).await?;
    for hit in service_hits {
        hits.entry(hit.sys_id.clone())
            .and_modify(|existing| {
                if !existing.provenance.contains("service_membership") {
                    existing.provenance.push_str("+service_membership");
                }
            })
            .or_insert(hit);
    }

    hydrate_hits(&client, &mut hits).await?;

    let output = ProbeOutput {
        server: ServerSummary {
            sys_id: server_sys_id,
            name: text(&server, "name"),
            class_name: text(&server, "sys_class_name"),
            operational_status: text(&server, "operational_status"),
        },
        relationship_rows_examined,
        service_membership_rows_examined,
        business_applications: hits.into_values().collect(),
    };

    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

async fn reverse_relationship_hits(
    client: &ServiceNowClient,
    server_sys_id: &str,
) -> Result<(usize, Vec<BusinessApplicationHit>)> {
    let allowed = resolve_type_allowlist(client, DEFAULT_RELATIONSHIP_TYPES).await?;
    let mut queue = VecDeque::from([(server_sys_id.to_string(), 0usize, Vec::<String>::new())]);
    let mut visited = BTreeSet::from([server_sys_id.to_string()]);
    let mut seen_edges = BTreeSet::new();
    let mut rows_examined = 0usize;
    let mut hits = Vec::new();

    while let Some((current, depth, path)) = queue.pop_front() {
        if depth >= 2 {
            continue;
        }
        for direction in ["parent", "child"] {
            let records = client
                .table("cmdb_rel_ci")
                .equals(direction, &current)
                .fields(&[
                    "sys_id",
                    "parent",
                    "parent.name",
                    "parent.sys_class_name",
                    "child",
                    "child.name",
                    "child.sys_class_name",
                    "type",
                ])
                .display_value(DisplayValue::Both)
                .exclude_reference_link(true)
                .no_count()
                .limit(1000)
                .execute()
                .await?
                .records;

            for record in records {
                if !seen_edges.insert(record.sys_id.clone()) {
                    continue;
                }
                rows_examined += 1;
                if !type_matches(&record, &allowed) {
                    continue;
                }

                let (other_field, other_sys_id) = if direction == "parent" {
                    ("child", ref_sys_id(&record, "child"))
                } else {
                    ("parent", ref_sys_id(&record, "parent"))
                };
                let Some(other_sys_id) = other_sys_id else {
                    continue;
                };
                let class = text(&record, &format!("{other_field}.sys_class_name"));
                let label = format!(
                    "cmdb_rel_ci:{}:{} -> {}",
                    text(&record, "type").unwrap_or_else(|| "unknown".to_string()),
                    text(
                        &record,
                        if direction == "parent" {
                            "parent.name"
                        } else {
                            "child.name"
                        }
                    )
                    .unwrap_or_else(|| current.clone()),
                    text(&record, &format!("{other_field}.name"))
                        .unwrap_or_else(|| other_sys_id.clone())
                );
                let mut next_path = path.clone();
                next_path.push(label);

                if class.as_deref() == Some(BUSINESS_APPLICATION_TABLE) {
                    hits.push(BusinessApplicationHit {
                        sys_id: other_sys_id,
                        number: None,
                        name: text(&record, &format!("{other_field}.name")),
                        operational_state: None,
                        provenance: "cmdb_rel_ci".to_string(),
                        path: next_path,
                    });
                } else if visited.insert(other_sys_id.clone()) {
                    queue.push_back((other_sys_id, depth + 1, next_path));
                }
            }
        }
    }

    Ok((rows_examined, hits))
}

async fn reverse_service_membership_hits(
    client: &ServiceNowClient,
    server_sys_id: &str,
) -> Result<(usize, Vec<BusinessApplicationHit>)> {
    let service_types =
        resolve_type_allowlist(client, SERVICE_DISCOVERY_RELATIONSHIP_TYPES).await?;
    let memberships = client
        .table("svc_ci_assoc")
        .equals("ci_id", server_sys_id)
        .fields(&[
            "sys_id",
            "service_id",
            "service_id.name",
            "service_id.sys_class_name",
            "ci_id",
            "ci_id.name",
            "ci_id.sys_class_name",
        ])
        .display_value(DisplayValue::Both)
        .exclude_reference_link(true)
        .no_count()
        .limit(1000)
        .execute()
        .await?
        .records;

    let rows_examined = memberships.len();
    let mut hits = Vec::new();
    let mut seen_edges = BTreeSet::new();

    for membership in memberships {
        let Some(service_sys_id) = ref_sys_id(&membership, "service_id") else {
            continue;
        };
        for direction in ["parent", "child"] {
            let records = client
                .table("cmdb_rel_ci")
                .equals(direction, &service_sys_id)
                .fields(&[
                    "sys_id",
                    "parent",
                    "parent.name",
                    "parent.sys_class_name",
                    "child",
                    "child.name",
                    "child.sys_class_name",
                    "type",
                ])
                .display_value(DisplayValue::Both)
                .exclude_reference_link(true)
                .no_count()
                .limit(1000)
                .execute()
                .await?
                .records;

            for record in records {
                if !seen_edges.insert(record.sys_id.clone())
                    || !type_matches(&record, &service_types)
                {
                    continue;
                }
                let (other_field, other_sys_id) = if direction == "parent" {
                    ("child", ref_sys_id(&record, "child"))
                } else {
                    ("parent", ref_sys_id(&record, "parent"))
                };
                let Some(other_sys_id) = other_sys_id else {
                    continue;
                };
                if text(&record, &format!("{other_field}.sys_class_name")).as_deref()
                    != Some(BUSINESS_APPLICATION_TABLE)
                {
                    continue;
                }
                hits.push(BusinessApplicationHit {
                    sys_id: other_sys_id,
                    number: None,
                    name: text(&record, &format!("{other_field}.name")),
                    operational_state: None,
                    provenance: "service_membership".to_string(),
                    path: vec![
                        format!(
                            "svc_ci_assoc:{} -> {}",
                            text(&membership, "ci_id.name")
                                .unwrap_or_else(|| server_sys_id.to_string()),
                            text(&membership, "service_id.name")
                                .unwrap_or_else(|| service_sys_id.clone())
                        ),
                        format!(
                            "cmdb_rel_ci:{}:{} -> {}",
                            text(&record, "type").unwrap_or_else(|| "unknown".to_string()),
                            text(
                                &record,
                                if direction == "parent" {
                                    "parent.name"
                                } else {
                                    "child.name"
                                }
                            )
                            .unwrap_or_else(|| service_sys_id.clone()),
                            text(&record, &format!("{other_field}.name"))
                                .unwrap_or_else(|| "business application".to_string())
                        ),
                    ],
                });
            }
        }
    }

    Ok((rows_examined, hits))
}

async fn hydrate_hits(
    client: &ServiceNowClient,
    hits: &mut BTreeMap<String, BusinessApplicationHit>,
) -> Result<()> {
    for hit in hits.values_mut() {
        let record = client
            .table(BUSINESS_APPLICATION_TABLE)
            .display_value(DisplayValue::Both)
            .exclude_reference_link(true)
            .get(&hit.sys_id)
            .await?;
        hit.number = text(&record, "number");
        hit.name = text(&record, "name").or_else(|| text(&record, "short_description"));
        hit.operational_state = text(&record, "operational_state");
    }
    Ok(())
}

async fn resolve_type_allowlist(
    client: &ServiceNowClient,
    labels: &[&str],
) -> Result<BTreeSet<String>> {
    let records = client
        .table("cmdb_rel_type")
        .in_list("name", labels)
        .fields(&["sys_id", "name"])
        .display_value(DisplayValue::Both)
        .exclude_reference_link(true)
        .no_count()
        .limit(labels.len() as u32)
        .execute()
        .await?
        .records;
    let mut allowed = labels
        .iter()
        .map(|value| (*value).to_string())
        .collect::<BTreeSet<_>>();
    for record in records {
        allowed.insert(record.sys_id.clone());
        if let Some(name) = text(&record, "name") {
            allowed.insert(name);
        }
    }
    Ok(allowed)
}

fn type_matches(record: &Record, allowed: &BTreeSet<String>) -> bool {
    if allowed.is_empty() {
        return true;
    }
    ref_sys_id(record, "type")
        .as_ref()
        .is_some_and(|value| allowed.contains(value))
        || text(record, "type")
            .as_ref()
            .is_some_and(|value| allowed.contains(value))
}

fn ref_sys_id(record: &Record, field: &str) -> Option<String> {
    record
        .get(field)
        .and_then(|field_value| field_value.value.as_ref())
        .and_then(Value::as_str)
        .or_else(|| record.get_raw(field))
        .or_else(|| record.get_str(field))
        .and_then(|value| normalize_record_lookup_sys_id(value).ok())
}

fn text(record: &Record, field: &str) -> Option<String> {
    record
        .get_display(field)
        .or_else(|| record.get_raw(field))
        .or_else(|| record.get_str(field))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}
