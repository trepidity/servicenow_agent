use serde::de::Error as _;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::path::PathBuf;

use crate::credential::CredentialProvider;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct SnowConfig {
    pub instance: InstanceConfig,
    pub vault: VaultConfig,
    pub transport: TransportConfig,
    pub refresh: RefreshConfig,
    pub cache: CacheConfig,
    pub daemon: DaemonConfig,
    pub kb: KbConfig,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq, Default)]
pub struct InstanceConfig {
    pub url: String,
    pub user: String,
    pub credential: CredentialProvider,
    /// Service Portal page slug used to build human-facing record URLs
    /// (e.g. `<instance>/<portal>?id=ticket&...`). Instance-specific; leave
    /// empty to fall back to the out-of-box `sp` portal. Set the real value
    /// via the `SNOW_PORTAL` env var or `[instance] portal = "..."` in config.
    pub portal: String,
}

/// Out-of-box ServiceNow Service Portal page slug, used when no
/// instance-specific portal is configured.
pub fn default_service_portal() -> String {
    "sp".to_string()
}

impl<'de> Deserialize<'de> for InstanceConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct RawInstanceConfig {
            url: String,
            user: String,
            #[serde(default)]
            credential: CredentialProvider,
            #[serde(default)]
            portal: String,
            #[serde(default)]
            op_item_id: Option<String>,
        }

        let raw = RawInstanceConfig::deserialize(deserializer)?;
        if raw.op_item_id.is_some() {
            return Err(D::Error::custom(
                "`instance.op_item_id` is no longer accepted; replace it with \
                 [instance.credential] provider = \"onepassword\" item_id = \"...\"",
            ));
        }

        Ok(Self {
            url: raw.url,
            user: raw.user,
            credential: raw.credential,
            portal: raw.portal,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct VaultConfig {
    pub path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransportConfig {
    pub preferred: TransportPreference,
    pub graphql_fallback: bool,
    pub graphql_batch_threshold: usize,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            preferred: TransportPreference::Auto,
            graphql_fallback: true,
            graphql_batch_threshold: 3,
        }
    }
}

impl SnowConfig {
    pub fn from_toml_str(input: &str) -> anyhow::Result<Self> {
        let mut config: Self = toml::from_str(input)?;
        config.apply_defaults();
        Ok(config)
    }

    pub fn from_path(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let mut config: Self = toml::from_str(&fs::read_to_string(path)?)?;
        config.apply_defaults();
        Ok(config)
    }

    pub fn apply_defaults(&mut self) {
        if self.instance.portal.trim().is_empty() {
            self.instance.portal = default_service_portal();
        }
        if self.refresh.max_concurrent_requests == 0 {
            self.refresh.max_concurrent_requests = 6;
        }
        if self.cache.memory.capacity == 0 {
            self.cache.memory.capacity = 1000;
        }
        if self.cache.memory.ttl_active.is_empty() {
            self.cache.memory.ttl_active = "1h".to_string();
        }
        if self.cache.memory.ttl_resolved.is_empty() {
            self.cache.memory.ttl_resolved = "24h".to_string();
        }
        if self.cache.memory.ttl_closed.is_empty() {
            self.cache.memory.ttl_closed = "7d".to_string();
        }
        if self.kb.max_auto_tags == 0 {
            self.kb.max_auto_tags = 5;
        }
        if self.kb.llm_tags.timeout_seconds == 0 {
            self.kb.llm_tags.timeout_seconds = 10;
        }
        if self.kb.semantic_search.provider.is_empty() {
            self.kb.semantic_search.provider = "ollama".to_string();
        }
        if self.kb.semantic_search.timeout_seconds == 0 {
            self.kb.semantic_search.timeout_seconds = 20;
        }
        if self.kb.semantic_search.batch_size == 0 {
            self.kb.semantic_search.batch_size = 32;
        }
        if self.kb.semantic_search.query_max_chars == 0 {
            self.kb.semantic_search.query_max_chars = 4_000;
        }
        if self.kb.semantic_search.top_k == 0 {
            self.kb.semantic_search.top_k = 20;
        }
        if self.kb.semantic_search.candidate_pool == 0 {
            self.kb.semantic_search.candidate_pool = 80;
        }
        if self.kb.semantic_search.min_score_millis == 0 {
            self.kb.semantic_search.min_score_millis = 200;
        }
        ensure_resource_defaults(&mut self.refresh.resources);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum TransportPreference {
    #[default]
    Auto,
    Graphql,
    Rest,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct RefreshConfig {
    pub schedule: String,
    pub max_concurrent_requests: usize,
    #[serde(flatten)]
    pub resources: BTreeMap<String, ResourceRefreshConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ResourceRefreshConfig {
    pub enabled: bool,
    pub filter: String,
    pub states: Vec<String>,
    pub incremental: bool,
    pub fetch_children: bool,
    pub full_sync_interval: Option<String>,
}

fn ensure_resource_defaults(resources: &mut BTreeMap<String, ResourceRefreshConfig>) {
    for (name, defaults) in [
        (
            "incident",
            ResourceRefreshConfig {
                enabled: true,
                filter: "assigned_to={{user}}".to_string(),
                states: vec!["New".into(), "In Progress".into(), "On Hold".into()],
                incremental: true,
                fetch_children: false,
                full_sync_interval: None,
            },
        ),
        (
            "change",
            ResourceRefreshConfig {
                enabled: true,
                filter: "assigned_to={{user}}".to_string(),
                states: vec![
                    "New".into(),
                    "Assess".into(),
                    "Authorize".into(),
                    "Scheduled".into(),
                    "Implement".into(),
                ],
                incremental: true,
                fetch_children: true,
                full_sync_interval: None,
            },
        ),
        (
            "change_task",
            ResourceRefreshConfig {
                enabled: true,
                filter: "assigned_to={{user}}".to_string(),
                states: vec!["Open".into(), "Work in Progress".into(), "Pending".into()],
                incremental: true,
                fetch_children: false,
                full_sync_interval: None,
            },
        ),
        (
            "request",
            ResourceRefreshConfig {
                enabled: true,
                filter: "assigned_to={{user}}".to_string(),
                states: vec!["Open".into(), "Work in Progress".into()],
                incremental: true,
                fetch_children: true,
                full_sync_interval: None,
            },
        ),
        (
            "request_task",
            ResourceRefreshConfig {
                enabled: true,
                filter: "assigned_to={{user}}".to_string(),
                states: vec!["Open".into(), "Work in Progress".into(), "Pending".into()],
                incremental: true,
                fetch_children: false,
                full_sync_interval: None,
            },
        ),
        (
            "story",
            ResourceRefreshConfig {
                enabled: true,
                filter: "assigned_to={{user}}".to_string(),
                states: vec![
                    "Draft".into(),
                    "Ready".into(),
                    "In Progress".into(),
                    "In Review".into(),
                ],
                incremental: true,
                fetch_children: true,
                full_sync_interval: None,
            },
        ),
        (
            "scrum_task",
            ResourceRefreshConfig {
                enabled: true,
                filter: "assigned_to={{user}}".to_string(),
                states: vec!["Open".into(), "In Progress".into(), "Blocked".into()],
                incremental: true,
                fetch_children: false,
                full_sync_interval: None,
            },
        ),
        (
            "knowledge",
            ResourceRefreshConfig {
                enabled: true,
                filter: "workflow_state=published".to_string(),
                states: Vec::new(),
                incremental: true,
                fetch_children: false,
                full_sync_interval: Some("7d".to_string()),
            },
        ),
        (
            "approval",
            ResourceRefreshConfig {
                enabled: true,
                filter: "approver={{user}}^state=requested".to_string(),
                states: Vec::new(),
                incremental: true,
                fetch_children: false,
                full_sync_interval: None,
            },
        ),
        (
            "project",
            ResourceRefreshConfig {
                enabled: true,
                filter: "project_manager={{user}}".to_string(),
                states: Vec::new(),
                incremental: true,
                fetch_children: false,
                full_sync_interval: None,
            },
        ),
        (
            "demand",
            ResourceRefreshConfig {
                enabled: true,
                filter: "demand_manager={{user}}".to_string(),
                states: Vec::new(),
                incremental: true,
                fetch_children: false,
                full_sync_interval: None,
            },
        ),
    ] {
        resources.entry(name.to_string()).or_insert(defaults);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct CacheConfig {
    pub memory: MemoryCacheConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct MemoryCacheConfig {
    pub capacity: usize,
    pub ttl_active: String,
    pub ttl_resolved: String,
    pub ttl_closed: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct KbConfig {
    pub max_auto_tags: usize,
    #[serde(default)]
    pub llm_tags: KbLlmTagConfig,
    #[serde(default)]
    pub semantic_search: KbSemanticSearchConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct KbLlmTagConfig {
    pub enabled: bool,
    pub endpoint: String,
    pub model: String,
    pub timeout_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KbSemanticSearchConfig {
    pub enabled: bool,
    pub provider: String,
    pub endpoint: String,
    pub model: String,
    pub timeout_seconds: u64,
    pub batch_size: usize,
    pub query_max_chars: usize,
    pub top_k: usize,
    pub candidate_pool: usize,
    pub min_score_millis: u32,
    pub include_tags_in_embedding_input: bool,
    pub hybrid_fallback_to_lexical: bool,
}

impl Default for KbSemanticSearchConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            provider: "ollama".to_string(),
            endpoint: String::new(),
            model: String::new(),
            timeout_seconds: 20,
            batch_size: 32,
            query_max_chars: 4_000,
            top_k: 20,
            candidate_pool: 80,
            min_score_millis: 200,
            include_tags_in_embedding_input: true,
            hybrid_fallback_to_lexical: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct DaemonConfig {
    pub socket_path: PathBuf,
    pub mcp_transport: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, MutexGuard};

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    struct InstanceWrapper {
        instance: InstanceConfig,
    }

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    struct EnvGuard {
        _lock: MutexGuard<'static, ()>,
    }

    impl EnvGuard {
        fn new() -> Self {
            let guard = Self {
                _lock: ENV_LOCK.lock().expect("env lock"),
            };
            // SAFETY: tests serialize process-env mutation with ENV_LOCK.
            unsafe {
                std::env::remove_var("SNOW_PASSWORD");
            }
            guard
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
            // SAFETY: tests serialize process-env mutation with ENV_LOCK.
            unsafe {
                std::env::remove_var("SNOW_PASSWORD");
            }
        }
    }

    #[test]
    fn instance_credential_toml_parses_and_round_trips() {
        let input = r#"
[instance]
url = "https://example.service-now.com"
user = "service.user"

[instance.credential]
provider = "onepassword"
item_id = "item-123"
field = "api-password"
"#;

        let parsed: InstanceWrapper = toml::from_str(input).expect("parse instance");
        assert_eq!(
            parsed.instance.credential,
            CredentialProvider::OnePassword {
                item_id: "item-123".to_string(),
                field: "api-password".to_string(),
            }
        );

        let encoded = toml::to_string(&parsed).expect("serialize instance");
        let reparsed: InstanceWrapper = toml::from_str(&encoded).expect("reparse instance");

        assert_eq!(reparsed, parsed);
    }

    #[test]
    fn instance_credential_defaults_to_env() {
        let input = r#"
[instance]
url = "https://example.service-now.com"
user = "service.user"
"#;

        let parsed: InstanceWrapper = toml::from_str(input).expect("parse instance");

        assert_eq!(parsed.instance.credential, CredentialProvider::Env);
    }

    #[test]
    fn legacy_op_item_id_is_rejected_with_replacement_hint() {
        let input = r#"
[instance]
url = "https://example.service-now.com"
user = "service.user"
op_item_id = "legacy-item"
"#;

        let err = toml::from_str::<InstanceWrapper>(input).expect_err("legacy rejection");
        let message = err.to_string();

        assert!(message.contains("instance.op_item_id"), "{message}");
        assert!(message.contains("[instance.credential]"), "{message}");
        assert!(message.contains("onepassword"), "{message}");
    }

    #[test]
    fn legacy_op_item_id_with_ambient_password_is_not_silently_defaulted() {
        let env = EnvGuard::new();
        env.set("SNOW_PASSWORD", "ambient-secret");
        let input = r#"
[instance]
url = "https://example.service-now.com"
user = "service.user"
op_item_id = "legacy-item"
"#;

        let err = toml::from_str::<InstanceWrapper>(input).expect_err("legacy rejection");

        assert!(err.to_string().contains("instance.op_item_id"));
    }
}
