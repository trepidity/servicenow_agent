use serde::{Deserialize, Serialize};
use std::ffi::OsStr;
use std::path::Path;
use std::process::{Command, Output};
use thiserror::Error;
use zeroize::{Zeroize, Zeroizing};

const SERVICENOW_PASSWORD_ENV: &str = "SERVICENOW_PASSWORD";
const SNOW_PASSWORD_ENV: &str = "SNOW_PASSWORD";
const OP_SERVICE_ACCOUNT_TOKEN_ENV: &str = "OP_SERVICE_ACCOUNT_TOKEN";
const OP_SERVICE_ACCOUNT_TOKEN_FILE_ENV: &str = "OP_SERVICE_ACCOUNT_TOKEN_FILE";

const PASSWORD_ENV_VARS: &[&str] = &[SERVICENOW_PASSWORD_ENV, SNOW_PASSWORD_ENV];
const USER_ENV_VARS: &[&str] = &["SERVICENOW_USERNAME", "SERVICENOW_USER", "SNOW_USER"];

const SECRET_ENV_VARS: &[&str] = &[
    SERVICENOW_PASSWORD_ENV,
    SNOW_PASSWORD_ENV,
    "SERVICE_NOW_PASSWORD",
    "SERVICENOW_CLIENT_SECRET",
    "SNOW_CLIENT_SECRET",
    "OP_ITEM_ID",
    "OP_VAULT",
    OP_SERVICE_ACCOUNT_TOKEN_ENV,
    OP_SERVICE_ACCOUNT_TOKEN_FILE_ENV,
    "OP_CONNECT_TOKEN",
    "OP_CONNECT_HOST",
    "ONEPASSWORD_CONNECT_TOKEN",
];

const ONEPASSWORD_CLI_AUTH_ENV_VARS: &[&str] = &[
    OP_SERVICE_ACCOUNT_TOKEN_ENV,
    "OP_CONNECT_TOKEN",
    "OP_CONNECT_HOST",
    "ONEPASSWORD_CONNECT_TOKEN",
];

pub type SecretString = Zeroizing<String>;

/// Where the ServiceNow password is resolved from at runtime.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "provider", rename_all = "lowercase")]
pub enum CredentialProvider {
    /// Read the ServiceNow password from the environment.
    #[default]
    Env,
    /// Fetch from 1Password via the `op` CLI.
    OnePassword {
        item_id: String,
        #[serde(default = "default_op_field")]
        field: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        vault: Option<String>,
    },
}

impl CredentialProvider {
    pub fn resolve(&self) -> Result<SecretString, CredentialError> {
        match self {
            Self::Env => resolve_env_password(),
            Self::OnePassword {
                item_id,
                field,
                vault,
            } => resolve_one_password(item_id, field, vault.as_deref()),
        }
    }

    pub fn from_runtime_env() -> Self {
        if non_empty_env(SERVICENOW_PASSWORD_ENV).is_some()
            || non_empty_env(SNOW_PASSWORD_ENV).is_some()
        {
            return Self::Env;
        }

        match non_empty_env("OP_ITEM_ID") {
            Some(item_id) => Self::OnePassword {
                item_id,
                field: default_op_field(),
                vault: non_empty_env("OP_VAULT"),
            },
            None => Self::Env,
        }
    }
}

#[derive(Debug, Error)]
pub enum CredentialError {
    #[error(
        "No ServiceNow password found. Set SERVICENOW_PASSWORD or configure [instance.credential] with provider = \"onepassword\" and item_id = \"...\"."
    )]
    MissingEnv,
    #[error(
        "No ServiceNow username found. Set SERVICENOW_USERNAME or configure OP_ITEM_ID for a 1Password item with a username field."
    )]
    MissingUsername,
    #[error(
        "Failed to run 1Password CLI for item {item_id}. Install `op`, sign in, or set SERVICENOW_PASSWORD instead. ({source})"
    )]
    OnePasswordUnavailable {
        item_id: String,
        #[source]
        source: std::io::Error,
    },
    #[error("1Password lookup failed for item {item_id}, field {field}: {stderr}")]
    OnePasswordFailure {
        item_id: String,
        field: String,
        stderr: String,
    },
    #[error("1Password returned an empty password for item {item_id}, field {field}.")]
    OnePasswordEmpty { item_id: String, field: String },
    #[error("1Password item_id is empty. Set OP_ITEM_ID or configure [instance.credential].")]
    OnePasswordMissingItem,
    #[error("1Password field is empty for item {item_id}.")]
    OnePasswordMissingField { item_id: String },
    #[error("1Password service account token file {path} could not be read: {source}")]
    OnePasswordTokenFileRead {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("1Password service account token file {path} is empty.")]
    OnePasswordTokenFileEmpty { path: String },
}

pub fn op_item_get_args(item_id: &str, field: &str) -> Vec<String> {
    op_item_get_args_with_vault(item_id, field, None)
}

fn op_item_get_args_with_vault(item_id: &str, field: &str, vault: Option<&str>) -> Vec<String> {
    let mut args = vec![
        "item".to_string(),
        "get".to_string(),
        item_id.trim().to_string(),
    ];
    if let Some(vault) = vault.map(str::trim).filter(|vault| !vault.is_empty()) {
        args.push("--vault".to_string());
        args.push(vault.to_string());
    }
    args.extend([
        "--fields".to_string(),
        format!("label={}", field.trim()),
        "--reveal".to_string(),
    ]);
    args
}

pub fn op_credential_get_args(item_id: &str, field: &str) -> Vec<String> {
    op_credential_get_args_with_vault(item_id, field, None)
}

fn op_credential_get_args_with_vault(
    item_id: &str,
    field: &str,
    vault: Option<&str>,
) -> Vec<String> {
    if let Some(reference) = op_secret_reference(item_id, field) {
        return vec!["read".to_string(), reference];
    }

    op_item_get_args_with_vault(item_id, field, vault)
}

fn op_secret_reference(item_id: &str, field: &str) -> Option<String> {
    let item_id = item_id.trim();
    if !item_id.starts_with("op://") {
        return None;
    }

    Some(format!(
        "{}/{}",
        item_id.trim_end_matches('/'),
        field.trim()
    ))
}

pub fn strip_secret_env(command: &mut Command) -> &mut Command {
    strip_secret_env_except(command, &[], false)
}

pub fn strip_secret_env_for_one_password(command: &mut Command) -> &mut Command {
    strip_secret_env_except(command, ONEPASSWORD_CLI_AUTH_ENV_VARS, true)
}

pub fn prepare_one_password_command(
    command: &mut Command,
) -> Result<&mut Command, CredentialError> {
    strip_secret_env_for_one_password(command);
    if non_empty_env(OP_SERVICE_ACCOUNT_TOKEN_ENV).is_none()
        && let Some(path) = non_empty_env(OP_SERVICE_ACCOUNT_TOKEN_FILE_ENV)
    {
        let token = read_one_password_service_account_token_file(&path)?;
        command.env(OP_SERVICE_ACCOUNT_TOKEN_ENV, token.as_str());
    }
    Ok(command)
}

fn strip_secret_env_except<'a>(
    command: &'a mut Command,
    allowed_static: &[&str],
    allow_op_session_env: bool,
) -> &'a mut Command {
    for key in SECRET_ENV_VARS {
        if !allowed_static.contains(key) {
            command.env_remove(key);
        }
    }

    for (key, _) in std::env::vars_os() {
        if is_dynamic_secret_env_key(&key) && !allow_op_session_env {
            command.env_remove(key);
        }
    }

    command
}

fn default_op_field() -> String {
    "password".to_string()
}

fn resolve_env_password() -> Result<SecretString, CredentialError> {
    for key in PASSWORD_ENV_VARS {
        if let Some(password) = non_empty_env(key) {
            clear_password_env_vars();
            return Ok(Zeroizing::new(password));
        }
    }

    Err(CredentialError::MissingEnv)
}

fn resolve_one_password(
    item_id: &str,
    field: &str,
    vault: Option<&str>,
) -> Result<SecretString, CredentialError> {
    resolve_one_password_with_runner(item_id, field, vault, |args| {
        let mut command = Command::new("op");
        command.args(args);
        prepare_one_password_command(&mut command)?;
        command
            .output()
            .map_err(|source| CredentialError::OnePasswordUnavailable {
                item_id: item_id.to_string(),
                source,
            })
    })
}

fn resolve_one_password_with_runner<F>(
    item_id: &str,
    field: &str,
    vault: Option<&str>,
    runner: F,
) -> Result<SecretString, CredentialError>
where
    F: FnOnce(&[String]) -> Result<Output, CredentialError>,
{
    if item_id.trim().is_empty() {
        return Err(CredentialError::OnePasswordMissingItem);
    }
    if field.trim().is_empty() {
        return Err(CredentialError::OnePasswordMissingField {
            item_id: item_id.to_string(),
        });
    }

    let args = op_credential_get_args_with_vault(item_id, field, vault);
    let mut output = runner(&args)?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(CredentialError::OnePasswordFailure {
            item_id: item_id.to_string(),
            field: field.to_string(),
            stderr: if stderr.is_empty() {
                format!("op exited with status {}", output.status)
            } else {
                stderr
            },
        });
    }

    let password = Zeroizing::new(String::from_utf8_lossy(&output.stdout).trim().to_string());
    output.stdout.zeroize();
    if password.is_empty() {
        return Err(CredentialError::OnePasswordEmpty {
            item_id: item_id.to_string(),
            field: field.to_string(),
        });
    }

    Ok(password)
}

fn non_empty_env(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|value| !value.is_empty())
}

pub fn resolve_username_from_runtime_env() -> Result<String, CredentialError> {
    for key in USER_ENV_VARS {
        if let Some(username) = non_empty_env(key) {
            return Ok(username);
        }
    }

    let Some(item_id) = non_empty_env("OP_ITEM_ID") else {
        return Err(CredentialError::MissingUsername);
    };
    let username =
        resolve_one_password(&item_id, "username", non_empty_env("OP_VAULT").as_deref())?;
    Ok(username.to_string())
}

fn read_one_password_service_account_token_file(
    path: &str,
) -> Result<Zeroizing<String>, CredentialError> {
    let token = std::fs::read_to_string(Path::new(path)).map_err(|source| {
        CredentialError::OnePasswordTokenFileRead {
            path: path.to_string(),
            source,
        }
    })?;
    let token = Zeroizing::new(token.trim().to_string());
    if token.is_empty() {
        return Err(CredentialError::OnePasswordTokenFileEmpty {
            path: path.to_string(),
        });
    }
    Ok(token)
}

fn clear_password_env_vars() {
    // SAFETY: Callers resolve credentials during startup before long-lived
    // worker tasks or child processes are spawned. Removing only the password
    // keys prevents unrelated child commands from inheriting the secret.
    unsafe {
        for key in PASSWORD_ENV_VARS {
            std::env::remove_var(key);
        }
    }
}

fn is_dynamic_secret_env_key(key: &OsStr) -> bool {
    let Some(key) = key.to_str() else {
        return false;
    };

    key == "OP_SESSION" || key.starts_with("OP_SESSION_")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::process::ExitStatusExt;
    use std::sync::{Mutex, MutexGuard};

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    struct EnvGuard {
        _lock: MutexGuard<'static, ()>,
    }

    impl EnvGuard {
        fn new() -> Self {
            let guard = Self {
                _lock: ENV_LOCK.lock().expect("env lock"),
            };
            guard.clear();
            guard
        }

        fn clear(&self) {
            // SAFETY: tests serialize process-env mutation with ENV_LOCK.
            unsafe {
                for key in [
                    SERVICENOW_PASSWORD_ENV,
                    SNOW_PASSWORD_ENV,
                    "SERVICENOW_USERNAME",
                    "SERVICENOW_USER",
                    "SNOW_USER",
                    "OP_ITEM_ID",
                    "OP_VAULT",
                    OP_SERVICE_ACCOUNT_TOKEN_ENV,
                    OP_SERVICE_ACCOUNT_TOKEN_FILE_ENV,
                    "OP_CONNECT_TOKEN",
                    "OP_CONNECT_HOST",
                    "ONEPASSWORD_CONNECT_TOKEN",
                    "OP_SESSION_test",
                ] {
                    std::env::remove_var(key);
                }
            }
        }

        fn set(&self, key: &str, value: &str) {
            // SAFETY: tests serialize process-env mutation with ENV_LOCK.
            unsafe {
                std::env::set_var(key, value);
            }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            self.clear();
        }
    }

    #[test]
    fn env_resolves_servicenow_password_and_clears_password_env() {
        let env = EnvGuard::new();
        env.set(SERVICENOW_PASSWORD_ENV, "servicenow-secret");
        env.set(SNOW_PASSWORD_ENV, "fallback-secret");

        let password = CredentialProvider::Env.resolve().expect("password");

        assert_eq!(password.as_str(), "servicenow-secret");
        assert!(std::env::var(SERVICENOW_PASSWORD_ENV).is_err());
        assert!(std::env::var(SNOW_PASSWORD_ENV).is_err());
    }

    #[test]
    fn env_falls_back_to_snow_password_alias() {
        let env = EnvGuard::new();
        env.set(SNOW_PASSWORD_ENV, "snow-secret");

        let password = CredentialProvider::Env.resolve().expect("password");

        assert_eq!(password.as_str(), "snow-secret");
    }

    #[test]
    fn env_errors_when_password_vars_are_absent() {
        let _env = EnvGuard::new();

        let err = CredentialProvider::Env.resolve().expect_err("missing env");

        assert!(matches!(err, CredentialError::MissingEnv));
    }

    #[test]
    fn runtime_env_prefers_password_env_over_one_password() {
        let env = EnvGuard::new();
        env.set(SERVICENOW_PASSWORD_ENV, "servicenow-secret");
        env.set("OP_ITEM_ID", "vault-item");

        assert_eq!(
            CredentialProvider::from_runtime_env(),
            CredentialProvider::Env
        );
    }

    #[test]
    fn runtime_env_accepts_snow_password_alias_over_one_password() {
        let env = EnvGuard::new();
        env.set(SNOW_PASSWORD_ENV, "snow-secret");
        env.set("OP_ITEM_ID", "vault-item");

        assert_eq!(
            CredentialProvider::from_runtime_env(),
            CredentialProvider::Env
        );
    }

    #[test]
    fn runtime_env_falls_back_to_one_password_item_id() {
        let env = EnvGuard::new();
        env.set("OP_ITEM_ID", "vault-item");

        assert_eq!(
            CredentialProvider::from_runtime_env(),
            CredentialProvider::OnePassword {
                item_id: "vault-item".to_string(),
                field: "password".to_string(),
                vault: None,
            }
        );
    }

    #[test]
    fn runtime_env_carries_one_password_vault() {
        let env = EnvGuard::new();
        env.set("OP_ITEM_ID", "service-account-item");
        env.set("OP_VAULT", "shared-vault");

        assert_eq!(
            CredentialProvider::from_runtime_env(),
            CredentialProvider::OnePassword {
                item_id: "service-account-item".to_string(),
                field: "password".to_string(),
                vault: Some("shared-vault".to_string()),
            }
        );
    }

    #[test]
    fn runtime_env_uses_env_missing_path_when_no_provider_vars_exist() {
        let _env = EnvGuard::new();

        assert_eq!(
            CredentialProvider::from_runtime_env(),
            CredentialProvider::Env
        );
    }

    #[test]
    fn one_password_arg_builder_uses_item_and_field() {
        assert_eq!(
            op_item_get_args("item-123", "api-password"),
            vec![
                "item",
                "get",
                "item-123",
                "--fields",
                "label=api-password",
                "--reveal"
            ]
        );
    }

    #[test]
    fn one_password_credential_arg_builder_uses_op_read_for_secret_reference() {
        assert_eq!(
            op_credential_get_args("op://vault/item", "password"),
            vec!["read", "op://vault/item/password"]
        );
    }

    #[test]
    fn one_password_credential_arg_builder_adds_vault_for_plain_items() {
        assert_eq!(
            op_credential_get_args_with_vault("service-account-item", "password", Some("vault")),
            vec![
                "item",
                "get",
                "service-account-item",
                "--vault",
                "vault",
                "--fields",
                "label=password",
                "--reveal",
            ]
        );
    }

    fn command_env_setting(command: &Command, key: &str) -> Option<Option<String>> {
        command
            .get_envs()
            .find(|(name, _)| *name == OsStr::new(key))
            .map(|(_, value)| value.map(|value| value.to_string_lossy().into_owned()))
    }

    #[test]
    fn generic_secret_stripping_removes_one_password_auth_env() {
        let env = EnvGuard::new();
        env.set("OP_SERVICE_ACCOUNT_TOKEN", "service-token");

        let mut command = Command::new("true");
        strip_secret_env(&mut command);

        assert_eq!(
            command_env_setting(&command, "OP_SERVICE_ACCOUNT_TOKEN"),
            Some(None)
        );
    }

    #[test]
    fn one_password_secret_stripping_preserves_op_auth_env() {
        let env = EnvGuard::new();
        env.set(SNOW_PASSWORD_ENV, "snow-secret");
        env.set("OP_SERVICE_ACCOUNT_TOKEN", "service-token");
        env.set("OP_SESSION_test", "session-token");

        let mut command = Command::new("op");
        strip_secret_env_for_one_password(&mut command);

        assert_eq!(command_env_setting(&command, SNOW_PASSWORD_ENV), Some(None));
        assert_eq!(
            command_env_setting(&command, "OP_SERVICE_ACCOUNT_TOKEN"),
            None
        );
        assert_eq!(command_env_setting(&command, "OP_SESSION_test"), None);
    }

    #[test]
    fn one_password_command_loads_service_account_token_file() {
        let env = EnvGuard::new();
        let tmp = tempfile::tempdir().expect("tempdir");
        let token_path = tmp.path().join("op-sa-token");
        std::fs::write(&token_path, " service-token\n").expect("write token");
        env.set(
            OP_SERVICE_ACCOUNT_TOKEN_FILE_ENV,
            token_path.to_str().expect("utf-8 token path"),
        );

        let mut command = Command::new("op");
        prepare_one_password_command(&mut command).expect("prepare op command");

        assert_eq!(
            command_env_setting(&command, OP_SERVICE_ACCOUNT_TOKEN_ENV),
            Some(Some("service-token".to_string()))
        );
        assert_eq!(
            command_env_setting(&command, OP_SERVICE_ACCOUNT_TOKEN_FILE_ENV),
            Some(None)
        );
    }

    #[test]
    fn runtime_username_prefers_env_value() {
        let env = EnvGuard::new();
        env.set("OP_ITEM_ID", "service-account-item");
        env.set("SERVICENOW_USERNAME", "service-user");

        assert_eq!(
            resolve_username_from_runtime_env().expect("username"),
            "service-user"
        );
    }

    #[test]
    fn runtime_username_prefers_canonical_env_value_over_aliases() {
        let env = EnvGuard::new();
        env.set("SERVICENOW_USERNAME", "canonical-user");
        env.set("SERVICENOW_USER", "service-user");
        env.set("SNOW_USER", "snow-user");

        assert_eq!(
            resolve_username_from_runtime_env().expect("username"),
            "canonical-user"
        );
    }

    #[test]
    fn runtime_username_requires_env_or_one_password_item() {
        let _env = EnvGuard::new();

        let err = resolve_username_from_runtime_env().expect_err("missing username");

        assert!(matches!(err, CredentialError::MissingUsername));
    }

    #[test]
    fn one_password_resolver_uses_arg_builder_and_trims_output() {
        let password = resolve_one_password_with_runner("item-123", "api-password", None, |args| {
            assert_eq!(
                args,
                [
                    "item".to_string(),
                    "get".to_string(),
                    "item-123".to_string(),
                    "--fields".to_string(),
                    "label=api-password".to_string(),
                    "--reveal".to_string()
                ]
            );
            Ok(Output {
                status: std::process::ExitStatus::from_raw(0),
                stdout: b" secret-value\n".to_vec(),
                stderr: Vec::new(),
            })
        })
        .expect("password");

        assert_eq!(password.as_str(), "secret-value");
    }

    #[test]
    fn one_password_resolver_reads_secret_reference() {
        let password =
            resolve_one_password_with_runner("op://vault/item", "password", None, |args| {
                assert_eq!(
                    args,
                    ["read".to_string(), "op://vault/item/password".to_string(),]
                );
                Ok(Output {
                    status: std::process::ExitStatus::from_raw(0),
                    stdout: b" secret-value\n".to_vec(),
                    stderr: Vec::new(),
                })
            })
            .expect("password");

        assert_eq!(password.as_str(), "secret-value");
    }

    #[test]
    fn one_password_resolver_surfaces_command_failure() {
        let err = resolve_one_password_with_runner("item-123", "password", None, |_| {
            Ok(Output {
                status: std::process::ExitStatus::from_raw(1),
                stdout: Vec::new(),
                stderr: b"not signed in".to_vec(),
            })
        })
        .expect_err("op failure");

        assert!(matches!(err, CredentialError::OnePasswordFailure { .. }));
        assert!(err.to_string().contains("not signed in"));
    }

    #[test]
    fn credential_provider_serializes_provider_shapes() {
        let provider = CredentialProvider::OnePassword {
            item_id: "item-123".to_string(),
            field: "api-password".to_string(),
            vault: Some("shared".to_string()),
        };
        let encoded = toml::to_string(&provider).expect("serialize");

        let decoded: CredentialProvider = toml::from_str(&encoded).expect("deserialize");

        assert_eq!(decoded, provider);
    }

    #[test]
    fn credential_provider_env_round_trips_and_is_default() {
        let provider = CredentialProvider::default();
        let encoded = toml::to_string(&provider).expect("serialize");

        let decoded: CredentialProvider = toml::from_str(&encoded).expect("deserialize");

        assert_eq!(provider, CredentialProvider::Env);
        assert_eq!(decoded, CredentialProvider::Env);
    }

    #[test]
    fn one_password_deserialize_defaults_field() {
        let decoded: CredentialProvider = toml::from_str(
            r#"
provider = "onepassword"
item_id = "item-123"
"#,
        )
        .expect("deserialize");

        assert_eq!(
            decoded,
            CredentialProvider::OnePassword {
                item_id: "item-123".to_string(),
                field: "password".to_string(),
                vault: None,
            }
        );
    }
}
