use serde::{Deserialize, Serialize};
use std::ffi::OsStr;
use std::process::{Command, Output};
use thiserror::Error;
use zeroize::{Zeroize, Zeroizing};

const SNOW_PASSWORD_ENV: &str = "SNOW_PASSWORD";
const SERVICENOW_PASSWORD_ENV: &str = "SERVICENOW_PASSWORD";

const PASSWORD_ENV_VARS: &[&str] = &[SNOW_PASSWORD_ENV, SERVICENOW_PASSWORD_ENV];

const SECRET_ENV_VARS: &[&str] = &[
    SNOW_PASSWORD_ENV,
    SERVICENOW_PASSWORD_ENV,
    "SERVICE_NOW_PASSWORD",
    "SERVICENOW_CLIENT_SECRET",
    "SNOW_CLIENT_SECRET",
    "OP_ITEM_ID",
    "OP_SERVICE_ACCOUNT_TOKEN",
    "OP_CONNECT_TOKEN",
    "OP_CONNECT_HOST",
    "ONEPASSWORD_CONNECT_TOKEN",
];

pub type SecretString = Zeroizing<String>;

/// Where the ServiceNow password is resolved from at runtime.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "provider", rename_all = "lowercase")]
pub enum CredentialProvider {
    /// Read SNOW_PASSWORD, then SERVICENOW_PASSWORD, from the environment.
    #[default]
    Env,
    /// Fetch from 1Password via the `op` CLI.
    OnePassword {
        item_id: String,
        #[serde(default = "default_op_field")]
        field: String,
    },
}

impl CredentialProvider {
    pub fn resolve(&self) -> Result<SecretString, CredentialError> {
        match self {
            Self::Env => resolve_env_password(),
            Self::OnePassword { item_id, field } => resolve_one_password(item_id, field),
        }
    }

    pub fn from_runtime_env() -> Self {
        if non_empty_env(SNOW_PASSWORD_ENV).is_some()
            || non_empty_env(SERVICENOW_PASSWORD_ENV).is_some()
        {
            return Self::Env;
        }

        match non_empty_env("OP_ITEM_ID") {
            Some(item_id) => Self::OnePassword {
                item_id,
                field: default_op_field(),
            },
            None => Self::Env,
        }
    }
}

#[derive(Debug, Error)]
pub enum CredentialError {
    #[error(
        "No ServiceNow password found. Set SNOW_PASSWORD or SERVICENOW_PASSWORD, or configure [instance.credential] with provider = \"onepassword\" and item_id = \"...\"."
    )]
    MissingEnv,
    #[error(
        "Failed to run 1Password CLI for item {item_id}. Install `op`, sign in, or set SNOW_PASSWORD instead. ({source})"
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
}

pub fn op_item_get_args(item_id: &str, field: &str) -> Vec<String> {
    vec![
        "item".to_string(),
        "get".to_string(),
        item_id.to_string(),
        "--fields".to_string(),
        format!("label={field}"),
        "--reveal".to_string(),
    ]
}

pub fn strip_secret_env(command: &mut Command) -> &mut Command {
    for key in SECRET_ENV_VARS {
        command.env_remove(key);
    }

    for (key, _) in std::env::vars_os() {
        if is_dynamic_secret_env_key(&key) {
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

fn resolve_one_password(item_id: &str, field: &str) -> Result<SecretString, CredentialError> {
    resolve_one_password_with_runner(item_id, field, |args| {
        let mut command = Command::new("op");
        command.args(args);
        strip_secret_env(&mut command).output()
    })
}

fn resolve_one_password_with_runner<F>(
    item_id: &str,
    field: &str,
    runner: F,
) -> Result<SecretString, CredentialError>
where
    F: FnOnce(&[String]) -> std::io::Result<Output>,
{
    if item_id.trim().is_empty() {
        return Err(CredentialError::OnePasswordMissingItem);
    }
    if field.trim().is_empty() {
        return Err(CredentialError::OnePasswordMissingField {
            item_id: item_id.to_string(),
        });
    }

    let args = op_item_get_args(item_id, field);
    let mut output = runner(&args).map_err(|source| CredentialError::OnePasswordUnavailable {
        item_id: item_id.to_string(),
        source,
    })?;

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
                    SNOW_PASSWORD_ENV,
                    SERVICENOW_PASSWORD_ENV,
                    "OP_ITEM_ID",
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
    fn env_resolves_snow_password_and_clears_password_env() {
        let env = EnvGuard::new();
        env.set(SNOW_PASSWORD_ENV, "snow-secret");
        env.set(SERVICENOW_PASSWORD_ENV, "fallback-secret");

        let password = CredentialProvider::Env.resolve().expect("password");

        assert_eq!(password.as_str(), "snow-secret");
        assert!(std::env::var(SNOW_PASSWORD_ENV).is_err());
        assert!(std::env::var(SERVICENOW_PASSWORD_ENV).is_err());
    }

    #[test]
    fn env_falls_back_to_servicenow_password() {
        let env = EnvGuard::new();
        env.set(SERVICENOW_PASSWORD_ENV, "servicenow-secret");

        let password = CredentialProvider::Env.resolve().expect("password");

        assert_eq!(password.as_str(), "servicenow-secret");
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
        env.set(SNOW_PASSWORD_ENV, "snow-secret");
        env.set("OP_ITEM_ID", "vault-item");

        assert_eq!(
            CredentialProvider::from_runtime_env(),
            CredentialProvider::Env
        );
    }

    #[test]
    fn runtime_env_prefers_servicenow_password_over_one_password() {
        let env = EnvGuard::new();
        env.set(SERVICENOW_PASSWORD_ENV, "servicenow-secret");
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
    fn one_password_resolver_uses_arg_builder_and_trims_output() {
        let password = resolve_one_password_with_runner("item-123", "api-password", |args| {
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
    fn one_password_resolver_surfaces_command_failure() {
        let err = resolve_one_password_with_runner("item-123", "password", |_| {
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
            }
        );
    }
}
