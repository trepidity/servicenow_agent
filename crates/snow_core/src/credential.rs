// Compatibility re-export. Type is now defined in servicenow_rs.
pub use servicenow_rs::auth::credential::{
    CredentialError, CredentialProvider, SecretString, op_credential_get_args, op_item_get_args,
    prepare_one_password_command, resolve_username_from_runtime_env, strip_secret_env,
    strip_secret_env_for_one_password,
};
