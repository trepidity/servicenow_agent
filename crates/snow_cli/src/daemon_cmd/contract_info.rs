//! `snow daemon contract-info` — bounded installed daemon exposure report.
//!
//! This command deliberately invokes only the daemon's `contract_info` method.
//! It is not a generic JSON-RPC caller and the emitted JSON excludes runtime
//! endpoint, identity, policy, credential, and record data.

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::{client, paths::DaemonPaths};

const CONTRACT_VERSION: &str = "daemon-json-rpc-v1";

#[derive(Deserialize)]
struct DaemonContract {
    contract_version: String,
    daemon_version: String,
    supported_methods: Vec<String>,
    deprecated_aliases: Vec<DeprecatedAlias>,
    environment: Environment,
    mcp_availability: McpAvailability,
}

#[derive(Deserialize, Serialize)]
struct DeprecatedAlias {
    method: String,
    replacement: String,
}

#[derive(Deserialize, Serialize)]
struct Environment {
    label: String,
}

#[derive(Deserialize, Serialize)]
struct McpAvailability {
    mode: String,
    transport: String,
}

#[derive(Serialize)]
struct SanitizedContract {
    contract_version: String,
    daemon_version: String,
    supported_methods: Vec<String>,
    deprecated_aliases: Vec<DeprecatedAlias>,
    environment: Environment,
    mcp_availability: McpAvailability,
}

/// Request the fixed daemon contract endpoint and print its public-safe report.
///
/// # Errors
///
/// Returns an error when the daemon is unreachable, its response is malformed,
/// or its contract version is not supported by this CLI.
pub fn run() -> Result<()> {
    let paths = DaemonPaths::resolve()?;
    let response = client::request(&paths, "contract_info", json!({}))
        .map_err(|_| anyhow!("daemon unreachable"))?;
    let contract: DaemonContract =
        serde_json::from_value(response).context("malformed daemon contract response")?;
    if contract.contract_version != CONTRACT_VERSION {
        bail!(
            "incompatible daemon contract: expected {CONTRACT_VERSION}, got {}",
            contract.contract_version
        );
    }

    let sanitized = SanitizedContract {
        contract_version: contract.contract_version,
        daemon_version: contract.daemon_version,
        supported_methods: contract.supported_methods,
        deprecated_aliases: contract.deprecated_aliases,
        environment: contract.environment,
        mcp_availability: contract.mcp_availability,
    };
    println!("{}", serde_json::to_string(&sanitized)?);
    Ok(())
}
