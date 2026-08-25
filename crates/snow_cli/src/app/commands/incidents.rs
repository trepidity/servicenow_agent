use super::super::*;

use crate::cli::IncidentCommand;
use std::io::{self, BufRead, Read, Write};
use std::path::Path;

/// Dispatch the `snow incident` subcommand family over the daemon client.
///
/// # Errors
///
/// Returns an error when the daemon is unreachable or returns a failure.
pub(crate) async fn cmd_incident(
    client: &DaemonRpcClient,
    action: IncidentCommand,
) -> Result<(), SnowError> {
    match action {
        IncidentCommand::Update {
            request,
            plan,
            apply,
            yes,
            json: _,
        } => match (request.as_deref(), plan.as_deref(), apply) {
            (Some(request), None, false) => {
                let result = client
                    .call_raw("incident_plan_update", read_json_input(request)?)
                    .await?;
                print_full_dump_or_inline(&result);
                Ok(())
            }
            (None, Some(plan_path), true) => {
                let bundle = read_json_input(plan_path)?;
                let preview = bundle.get("preview").cloned().ok_or_else(|| {
                    SnowError::Api("saved plan bundle is missing preview".to_string())
                })?;
                eprintln!(
                    "{}",
                    serde_json::to_string_pretty(&preview)
                        .map_err(|error| SnowError::Api(error.to_string()))?
                );
                if !yes {
                    eprint!("Apply this Incident update? [y/N] ");
                    io::stderr().flush()?;
                    if !confirm_from_terminal()? {
                        return Err(SnowError::Api("Incident apply cancelled".to_string()));
                    }
                }
                let result = client
                    .call_raw(
                        "incident_apply_update",
                        serde_json::json!({
                            "plan_id": required_bundle_string(&bundle, "plan_id")?,
                            "confirmation_token": required_bundle_string(&bundle, "confirmation_token")?,
                            "idempotency_key": required_bundle_string(&bundle, "idempotency_key")?,
                            "concurrency_token": bundle.get("concurrency_token").cloned().ok_or_else(|| {
                                SnowError::Api("saved plan bundle is missing concurrency_token".to_string())
                            })?,
                        }),
                    )
                    .await?;
                print_full_dump_or_inline(&result);
                Ok(())
            }
            _ => Err(SnowError::Api(
                "use --request to plan, or --plan with --apply to apply".to_string(),
            )),
        },
        IncidentCommand::BulkUpdate {
            request,
            plan,
            apply,
            yes,
            json: _,
        } => match (request.as_deref(), plan.as_deref(), apply) {
            (Some(request), None, false) => {
                let params = read_json_input(request)?;
                let result = client.call_raw("incident_bulk_plan_update", params).await?;
                print_full_dump_or_inline(&result);
                Ok(())
            }
            (None, Some(plan_path), true) => {
                let bundle = read_json_input(plan_path)?;
                let preview = bundle.get("preview").cloned().ok_or_else(|| {
                    SnowError::Api("saved plan bundle is missing preview".to_string())
                })?;
                eprintln!(
                    "{}",
                    serde_json::to_string_pretty(&preview)
                        .map_err(|error| SnowError::Api(error.to_string()))?
                );
                if !yes {
                    eprint!("Apply this Incident bulk plan? [y/N] ");
                    io::stderr().flush()?;
                    if !confirm_from_terminal()? {
                        return Err(SnowError::Api("bulk apply cancelled".to_string()));
                    }
                }
                let concurrency_tokens = preview
                    .get("targets")
                    .and_then(serde_json::Value::as_array)
                    .ok_or_else(|| {
                        SnowError::Api("saved plan preview is missing targets".to_string())
                    })?
                    .iter()
                    .map(|target| {
                        serde_json::json!({
                            "sys_id": target["target"]["sys_id"],
                            "sys_updated_on": target["concurrency_token"]["sys_updated_on"],
                        })
                    })
                    .collect::<Vec<_>>();
                let params = serde_json::json!({
                    "plan_id": required_bundle_string(&bundle, "plan_id")?,
                    "confirmation_token": required_bundle_string(&bundle, "confirmation_token")?,
                    "idempotency_key": required_bundle_string(&bundle, "idempotency_key")?,
                    "concurrency_tokens": concurrency_tokens,
                });
                let result = client
                    .call_raw("incident_bulk_apply_update", params)
                    .await?;
                print_full_dump_or_inline(&result);
                Ok(())
            }
            _ => Err(SnowError::Api(
                "use --request to plan, or --plan with --apply to apply".to_string(),
            )),
        },
        IncidentCommand::Get {
            number,
            sys_id,
            json: _,
        } => {
            let mut params = serde_json::Map::new();
            if let Some(number) = number {
                params.insert("number".to_string(), serde_json::Value::String(number));
            }
            if let Some(sys_id) = sys_id {
                params.insert("sys_id".to_string(), serde_json::Value::String(sys_id));
            }
            let envelope = client
                .incident_get(serde_json::Value::Object(params))
                .await?;
            print_full_dump_or_inline(&envelope);
            Ok(())
        }
        IncidentCommand::Query {
            numbers,
            assignment_group,
            assigned_to,
            caller_id,
            cmdb_ci,
            states,
            priorities,
            active,
            opened_after,
            opened_before,
            updated_after,
            updated_before,
            limit,
            cursor,
            json: _,
        } => {
            let mut filters = serde_json::Map::new();
            if !numbers.is_empty() {
                filters.insert("numbers".to_string(), serde_json::json!(numbers));
            }
            if !states.is_empty() {
                filters.insert("states".to_string(), serde_json::json!(states));
            }
            if !priorities.is_empty() {
                filters.insert("priorities".to_string(), serde_json::json!(priorities));
            }
            for (name, value) in [
                ("assignment_group", assignment_group),
                ("assigned_to", assigned_to),
                ("caller_id", caller_id),
                ("cmdb_ci", cmdb_ci),
                ("opened_after", opened_after),
                ("opened_before", opened_before),
                ("updated_after", updated_after),
                ("updated_before", updated_before),
            ] {
                if let Some(value) = value {
                    filters.insert(name.to_string(), serde_json::Value::String(value));
                }
            }
            if let Some(active) = active {
                filters.insert("active".to_string(), serde_json::Value::Bool(active));
            }
            let params = serde_json::json!({
                "filters": serde_json::Value::Object(filters),
                "limit": limit,
                "cursor": cursor,
            });
            let envelope = client.incident_query(params).await?;
            print_full_dump_or_inline(&envelope);
            Ok(())
        }
        IncidentCommand::Fields { json } => {
            let envelope = client.incident_fields().await?;
            if json {
                print_full_dump_or_inline(&envelope);
            } else {
                print!("{}", display::format_resource_descriptor(&envelope));
            }
            Ok(())
        }
    }
}

fn read_json_input(path: &Path) -> Result<serde_json::Value, SnowError> {
    let mut input = String::new();
    if path == Path::new("-") {
        io::stdin().read_to_string(&mut input)?;
    } else {
        input = std::fs::read_to_string(path)?;
    }
    serde_json::from_str(&input).map_err(|error| SnowError::Api(error.to_string()))
}

fn confirm_from_terminal() -> Result<bool, SnowError> {
    #[cfg(unix)]
    let terminal = std::fs::OpenOptions::new()
        .read(true)
        .open("/dev/tty")
        .map_err(|error| {
            SnowError::Api(format!(
                "interactive confirmation requires a controlling terminal; use --yes for noninteractive apply: {error}"
            ))
        })?;
    #[cfg(windows)]
    let terminal = std::fs::OpenOptions::new()
        .read(true)
        .open("CONIN$")
        .map_err(|error| {
            SnowError::Api(format!(
                "interactive confirmation requires a console; use --yes for noninteractive apply: {error}"
            ))
        })?;
    #[cfg(not(any(unix, windows)))]
    let terminal = return Err(SnowError::Api(
        "interactive confirmation is unavailable on this platform; use --yes for noninteractive apply"
            .to_string(),
    ));

    let mut answer = String::new();
    io::BufReader::new(terminal).read_line(&mut answer)?;
    Ok(matches!(
        answer.trim().to_ascii_lowercase().as_str(),
        "y" | "yes"
    ))
}

fn required_bundle_string(bundle: &serde_json::Value, field: &str) -> Result<String, SnowError> {
    bundle
        .get(field)
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| SnowError::Api(format!("saved plan bundle is missing {field}")))
}
