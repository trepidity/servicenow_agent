use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum EnvironmentLabelSource {
    ExplicitConfig,
    SnowEnv,
    #[default]
    Default,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct McpEnvironment {
    pub label: String,
    pub instance_timezone: String,
    #[serde(default)]
    pub label_source: EnvironmentLabelSource,
}

impl Default for McpEnvironment {
    fn default() -> Self {
        Self {
            label: "test".to_string(),
            instance_timezone: "America/Chicago".to_string(),
            label_source: EnvironmentLabelSource::Default,
        }
    }
}

impl McpEnvironment {
    pub fn explicit_config(label: impl Into<String>, instance_timezone: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            instance_timezone: instance_timezone.into(),
            label_source: EnvironmentLabelSource::ExplicitConfig,
        }
    }

    pub fn snow_env(label: impl Into<String>, instance_timezone: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            instance_timezone: instance_timezone.into(),
            label_source: EnvironmentLabelSource::SnowEnv,
        }
    }

    pub fn has_explicit_label_source(&self) -> bool {
        matches!(
            self.label_source,
            EnvironmentLabelSource::ExplicitConfig | EnvironmentLabelSource::SnowEnv
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct McpConfig {
    pub environment: McpEnvironment,
    pub policy: crate::domain::policy::PolicyConfig,
    pub audit: McpAuditConfig,
    pub redaction: McpRedactionConfig,
    pub rate_limit: McpRateLimitConfig,
}

impl Default for McpConfig {
    fn default() -> Self {
        Self {
            environment: McpEnvironment::default(),
            policy: crate::domain::policy::PolicyConfig::read_only_default(),
            audit: McpAuditConfig::default(),
            redaction: McpRedactionConfig::default(),
            rate_limit: McpRateLimitConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct McpAuditConfig {
    pub path: Option<PathBuf>,
    pub retention_days: u32,
}

impl Default for McpAuditConfig {
    fn default() -> Self {
        Self {
            path: None,
            retention_days: 365,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct McpRedactionConfig {
    pub rules_version: String,
    pub phi_in_work_notes: String,
}

impl Default for McpRedactionConfig {
    fn default() -> Self {
        Self {
            rules_version: "builtin-v1".to_string(),
            phi_in_work_notes: "deny".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct McpRateLimitConfig {
    pub read_per_minute: u32,
    pub write_per_minute: u32,
    pub failed_confirmations_per_minute: u32,
}

impl Default for McpRateLimitConfig {
    fn default() -> Self {
        Self {
            read_per_minute: 60,
            write_per_minute: 10,
            failed_confirmations_per_minute: 5,
        }
    }
}
