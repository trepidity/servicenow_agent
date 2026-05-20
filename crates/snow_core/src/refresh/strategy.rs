use crate::ResourceType;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefreshPolicy {
    pub enabled: bool,
    pub filter: String,
    pub states: Vec<String>,
    pub incremental: bool,
    pub fetch_children: bool,
    pub full_sync_interval: Option<String>,
}

impl RefreshPolicy {
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            filter: String::new(),
            states: Vec::new(),
            incremental: false,
            fetch_children: false,
            full_sync_interval: None,
        }
    }

    pub fn active(filter: impl Into<String>) -> Self {
        Self {
            enabled: true,
            filter: filter.into(),
            states: Vec::new(),
            incremental: true,
            fetch_children: false,
            full_sync_interval: None,
        }
    }

    pub fn with_states<I, S>(mut self, states: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.states = states.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_children(mut self, fetch_children: bool) -> Self {
        self.fetch_children = fetch_children;
        self
    }

    pub fn with_full_sync_interval(mut self, interval: impl Into<String>) -> Self {
        self.full_sync_interval = Some(interval.into());
        self
    }

    pub fn is_incremental(&self) -> bool {
        self.enabled && self.incremental
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefreshStrategy {
    pub resource_type: ResourceType,
    pub policy: RefreshPolicy,
}

impl RefreshStrategy {
    pub fn new(resource_type: ResourceType, policy: RefreshPolicy) -> Self {
        Self {
            resource_type,
            policy,
        }
    }

    pub fn enabled(resource_type: ResourceType, filter: impl Into<String>) -> Self {
        Self::new(resource_type, RefreshPolicy::active(filter))
    }

    pub fn disabled(resource_type: ResourceType) -> Self {
        Self::new(resource_type, RefreshPolicy::disabled())
    }

    pub fn with_children(mut self, fetch_children: bool) -> Self {
        self.policy = self.policy.with_children(fetch_children);
        self
    }

    pub fn supports_children(&self) -> bool {
        self.policy.fetch_children
    }

    pub fn full_sync_interval(&self) -> Option<Duration> {
        self.policy
            .full_sync_interval
            .as_deref()
            .and_then(parse_duration)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceRefreshState {
    pub resource_type: ResourceType,
    pub started_at: Option<chrono::DateTime<chrono::Utc>>,
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
    pub checkpoint: Option<super::RefreshCheckpoint>,
    pub last_reconciled_scope: Option<super::ScopeReconciliationPlan>,
    pub last_error: Option<String>,
}

impl ResourceRefreshState {
    pub fn is_running(&self) -> bool {
        self.started_at.is_some() && self.completed_at.is_none()
    }

    pub fn succeeded(&self) -> bool {
        self.last_error.is_none()
    }
}

fn parse_duration(input: &str) -> Option<Duration> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return None;
    }

    let (value_part, unit_part): (String, String) =
        trimmed.chars().partition(|ch| ch.is_ascii_digit());
    let value = value_part.parse::<u64>().ok()?;
    match unit_part.as_str() {
        "s" => Some(Duration::from_secs(value)),
        "m" => Some(Duration::from_secs(value * 60)),
        "h" => Some(Duration::from_secs(value * 60 * 60)),
        "d" => Some(Duration::from_secs(value * 60 * 60 * 24)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policy_supports_child_fetches() {
        let policy = RefreshPolicy::active("assigned_to={{user}}")
            .with_states(["New", "In Progress"])
            .with_children(true)
            .with_full_sync_interval("7d");
        assert!(policy.enabled);
        assert!(policy.fetch_children);
        assert!(policy.is_incremental());
    }

    #[test]
    fn strategy_reports_child_support() {
        let strategy = RefreshStrategy::enabled(ResourceType::Change, "assigned_to={{user}}")
            .with_children(true);
        assert!(strategy.supports_children());
    }
}
