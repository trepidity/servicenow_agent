use crate::ResourceType;
use crate::cache::store::{RecordLifecycle, RecordRow};
use chrono::{DateTime, Utc};
use std::collections::BTreeSet;

use super::SyncWindow;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScopeAction {
    Retain,
    Tombstone,
    Prune,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopeOutcome {
    pub sys_id: String,
    pub number: String,
    pub table_name: String,
    pub lifecycle_before: RecordLifecycle,
    pub action: ScopeAction,
}

impl ScopeOutcome {
    pub fn from_row(row: &RecordRow, action: ScopeAction) -> Self {
        Self {
            sys_id: row.sys_id.clone(),
            number: row.number.clone(),
            table_name: row.table_name.clone(),
            lifecycle_before: row.lifecycle(),
            action,
        }
    }

    pub fn should_tombstone(&self) -> bool {
        matches!(self.action, ScopeAction::Tombstone)
    }

    pub fn should_prune(&self) -> bool {
        matches!(self.action, ScopeAction::Prune)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopeReconciliationPlan {
    pub resource_type: ResourceType,
    pub outcomes: Vec<ScopeOutcome>,
}

impl ScopeReconciliationPlan {
    pub fn kept_count(&self) -> usize {
        self.outcomes
            .iter()
            .filter(|outcome| matches!(outcome.action, ScopeAction::Retain))
            .count()
    }

    pub fn tombstoned_count(&self) -> usize {
        self.outcomes
            .iter()
            .filter(|outcome| matches!(outcome.action, ScopeAction::Tombstone))
            .count()
    }

    pub fn pruned_count(&self) -> usize {
        self.outcomes
            .iter()
            .filter(|outcome| matches!(outcome.action, ScopeAction::Prune))
            .count()
    }

    pub fn outcome_for(&self, sys_id: &str) -> Option<&ScopeOutcome> {
        self.outcomes
            .iter()
            .find(|outcome| outcome.sys_id == sys_id)
    }

    pub fn tombstone_sys_ids(&self) -> impl Iterator<Item = &str> {
        self.outcomes
            .iter()
            .filter(|outcome| outcome.should_tombstone())
            .map(|outcome| outcome.sys_id.as_str())
    }

    pub fn prune_sys_ids(&self) -> impl Iterator<Item = &str> {
        self.outcomes
            .iter()
            .filter(|outcome| outcome.should_prune())
            .map(|outcome| outcome.sys_id.as_str())
    }

    pub fn retained_sys_ids(&self) -> impl Iterator<Item = &str> {
        self.outcomes
            .iter()
            .filter(|outcome| matches!(outcome.action, ScopeAction::Retain))
            .map(|outcome| outcome.sys_id.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncrementalDiff {
    pub window: SyncWindow,
    pub seen_sys_ids: BTreeSet<String>,
    pub new_high_watermark: Option<DateTime<Utc>>,
    pub changed: Vec<ScopeOutcome>,
    pub skipped: Vec<String>,
}

impl IncrementalDiff {
    pub fn new(window: SyncWindow) -> Self {
        Self {
            window,
            seen_sys_ids: BTreeSet::new(),
            new_high_watermark: None,
            changed: Vec::new(),
            skipped: Vec::new(),
        }
    }

    pub fn record_seen(&mut self, sys_id: impl Into<String>) {
        self.seen_sys_ids.insert(sys_id.into());
    }

    pub fn record_high_watermark(&mut self, value: DateTime<Utc>) {
        self.new_high_watermark = Some(
            self.new_high_watermark
                .map_or(value, |current| current.max(value)),
        );
    }

    pub fn record_change(&mut self, outcome: ScopeOutcome) {
        self.changed.push(outcome);
    }

    pub fn record_skip(&mut self, sys_id: impl Into<String>) {
        self.skipped.push(sys_id.into());
    }

    pub fn has_seen(&self, sys_id: &str) -> bool {
        self.seen_sys_ids.contains(sys_id)
    }

    pub fn observed_count(&self) -> usize {
        self.seen_sys_ids.len()
    }

    pub fn commit_high_watermark(&self) -> DateTime<Utc> {
        self.new_high_watermark
            .unwrap_or(self.window.upper_inclusive)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::refresh::SyncWindow;
    use chrono::TimeZone;

    #[test]
    fn diff_tracks_high_watermark_and_seen_rows() {
        let window = SyncWindow::new(
            Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
            Utc.timestamp_opt(1_712_650_000, 0).unwrap(),
        );
        let mut diff = IncrementalDiff::new(window);
        let watermark = Utc.timestamp_opt(1_712_649_900, 0).unwrap();
        diff.record_seen("sys-1");
        diff.record_high_watermark(watermark);

        assert!(diff.has_seen("sys-1"));
        assert_eq!(diff.commit_high_watermark(), watermark);
    }

    #[test]
    fn scope_outcomes_report_actions() {
        let row = RecordRow::active(
            "sys-1",
            "INC1",
            "incident",
            ResourceType::Incident,
            Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
        );
        let outcome = ScopeOutcome::from_row(&row, ScopeAction::Tombstone);
        assert!(outcome.should_tombstone());
        assert!(!outcome.should_prune());
        let plan = ScopeReconciliationPlan {
            resource_type: ResourceType::Incident,
            outcomes: vec![outcome],
        };
        assert_eq!(plan.tombstoned_count(), 1);
    }
}
