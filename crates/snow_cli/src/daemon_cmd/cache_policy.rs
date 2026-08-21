//! Fixed cache-policy lifecycle commands.

use anyhow::Result;

use crate::cli::CachePolicyCommand;

use super::{client, paths::DaemonPaths};

pub fn run(action: CachePolicyCommand) -> Result<()> {
    let paths = DaemonPaths::resolve()?;
    let (method, json_output) = match action {
        CachePolicyCommand::Validate { json } => ("cache_policy_validate", json),
        CachePolicyCommand::Reload { json } => ("cache_policy_reload", json),
    };
    let result = client::request(&paths, method, serde_json::json!({}))?;
    if json_output {
        println!("{}", serde_json::to_string(&result)?);
    } else {
        println!("{}", serde_json::to_string_pretty(&result)?);
    }
    Ok(())
}
