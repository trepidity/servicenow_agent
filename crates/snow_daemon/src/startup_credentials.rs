//! Daemon-owned, bounded credential startup. Never put secrets, item identities,
//! or helper stderr into daemon logs. A stalled helper must not strand readiness.
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use snow_core::credential::{CredentialProvider, SecretString, prepare_one_password_command};
use tokio::io::AsyncReadExt;
use zeroize::{Zeroize, Zeroizing};

pub(super) async fn resolve(provider: &CredentialProvider) -> Result<(String, SecretString)> {
    let seconds = std::env::var("SNOW_DAEMON_CREDENTIAL_TIMEOUT_SECS")
        .ok()
        .map(|value| value.parse::<u64>())
        .transpose()
        .context("SNOW_DAEMON_CREDENTIAL_TIMEOUT_SECS must be an integer from 1 to 30")?
        .unwrap_or(30);
    if !(1..=30).contains(&seconds) {
        bail!("SNOW_DAEMON_CREDENTIAL_TIMEOUT_SECS must be an integer from 1 to 30");
    }
    let timeout = Duration::from_secs(seconds);
    let username = ["SERVICENOW_USERNAME", "SERVICENOW_USER", "SNOW_USER"]
        .iter()
        .find_map(|key| non_empty_env(key));

    if username.is_none()
        && let CredentialProvider::OnePassword {
            item_id,
            field,
            vault,
        } = provider
        && !item_id.starts_with("op://")
    {
        let mut args = item_args(item_id, vault.as_deref());
        args.extend([
            "--fields".into(),
            format!("label=username,label={field}"),
            "--reveal".into(),
            "--format".into(),
            "json".into(),
        ]);
        let output = run_op(args, timeout).await?;
        #[derive(Deserialize)]
        struct Field {
            label: String,
            value: String,
        }
        impl Drop for Field {
            fn drop(&mut self) {
                self.value.zeroize();
            }
        }
        let fields: Vec<Field> = serde_json::from_slice(&output)
            .map_err(|_| anyhow::anyhow!("invalid 1Password credential response"))?;
        let read = |label: &str| -> Result<SecretString> {
            let matches: Vec<_> = fields.iter().filter(|field| field.label == label).collect();
            if matches.len() != 1 || matches[0].value.trim().is_empty() {
                bail!("incomplete or ambiguous 1Password credential response");
            }
            Ok(SecretString::new(matches[0].value.trim().to_string()))
        };
        return Ok((read("username")?.to_string(), read(field)?));
    }

    let username = async {
        if let Some(username) = username {
            return Ok(username);
        }
        let item = non_empty_env("OP_ITEM_ID").context("ServiceNow username is not configured")?;
        read_field(
            &item,
            "username",
            non_empty_env("OP_VAULT").as_deref(),
            timeout,
        )
        .await
        .map(|value| value.to_string())
    };
    let password = async {
        match provider {
            CredentialProvider::Env => provider
                .resolve()
                .map_err(|_| anyhow::anyhow!("ServiceNow password is not configured")),
            CredentialProvider::OnePassword {
                item_id,
                field,
                vault,
            } => read_field(item_id, field, vault.as_deref(), timeout).await,
        }
    };
    tokio::try_join!(username, password)
}

fn non_empty_env(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

fn item_args(item: &str, vault: Option<&str>) -> Vec<String> {
    let mut args = vec!["item".into(), "get".into(), item.trim().into()];
    if let Some(vault) = vault {
        args.extend(["--vault".into(), vault.trim().into()]);
    }
    args
}

async fn read_field(
    item: &str,
    field: &str,
    vault: Option<&str>,
    timeout: Duration,
) -> Result<SecretString> {
    let args = if item.starts_with("op://") {
        vec![
            "read".into(),
            format!("{}/{field}", item.trim_end_matches('/')),
        ]
    } else {
        let mut args = item_args(item, vault);
        args.extend([
            "--fields".into(),
            format!("label={field}"),
            "--reveal".into(),
        ]);
        args
    };
    let output = run_op(args, timeout).await?;
    let value = std::str::from_utf8(&output)
        .ok()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .context("invalid or empty 1Password credential response")?;
    Ok(SecretString::new(value.to_string()))
}

async fn run_op(args: Vec<String>, timeout: Duration) -> Result<Zeroizing<Vec<u8>>> {
    const MAX_OUTPUT: u64 = 65_536;
    let mut command = std::process::Command::new("op");
    command
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    prepare_one_password_command(&mut command).map_err(|_| {
        anyhow::anyhow!("cannot prepare 1Password authentication; check credential configuration")
    })?;
    let mut child = tokio::process::Command::from(command)
        .kill_on_drop(true)
        .spawn()
        .map_err(|_| {
            anyhow::anyhow!("cannot start 1Password CLI; check installation and daemon PATH")
        })?;
    let mut stdout = child
        .stdout
        .take()
        .context("credential stdout unavailable")?
        .take(MAX_OUTPUT + 1);
    let mut stderr = child
        .stderr
        .take()
        .context("credential stderr unavailable")?
        .take(MAX_OUTPUT + 1);
    let mut output = Zeroizing::new(Vec::new());
    let mut diagnostic = Zeroizing::new(Vec::new());
    let outcome = tokio::time::timeout(timeout, async {
        tokio::try_join!(
            child.wait(),
            stdout.read_to_end(&mut output),
            stderr.read_to_end(&mut diagnostic)
        )
    })
    .await;
    match outcome {
        Ok(Ok((status, _, _))) if status.success() && output.len() <= MAX_OUTPUT as usize => {
            Ok(output)
        }
        Ok(_) => {
            let _ = child.kill().await;
            bail!(
                "1Password credential lookup failed; check daemon authentication and item field access"
            )
        }
        Err(_) => {
            let _ = child.kill().await;
            bail!(
                "1Password credential lookup timed out after {}s; check daemon authentication and connectivity",
                timeout.as_secs()
            )
        }
    }
}
