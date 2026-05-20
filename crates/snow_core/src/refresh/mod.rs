pub mod diff;
pub mod strategy;

use crate::ResourceType;
use crate::cache::store::{RecordRow, Store, SyncStateRow};
use chrono::{DateTime, Utc};
use std::collections::VecDeque;
use std::time::Duration;

pub use diff::{IncrementalDiff, ScopeAction, ScopeOutcome, ScopeReconciliationPlan};
pub use strategy::{RefreshPolicy, RefreshStrategy, ResourceRefreshState};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefreshState {
    Idle,
    Running,
    Paused,
    Stopping,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchedulerStatus {
    pub state: RefreshState,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub active_jobs: usize,
    pub queued_jobs: usize,
    pub last_error: Option<String>,
}

impl Default for SchedulerStatus {
    fn default() -> Self {
        Self {
            state: RefreshState::Idle,
            started_at: None,
            completed_at: None,
            active_jobs: 0,
            queued_jobs: 0,
            last_error: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefreshJob {
    pub resource_type: ResourceType,
    pub strategy: RefreshStrategy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefreshSchedulerConfig {
    pub schedule: String,
    pub max_concurrent_requests: usize,
}

impl Default for RefreshSchedulerConfig {
    fn default() -> Self {
        Self {
            schedule: "06:00".to_string(),
            max_concurrent_requests: 6,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RefreshScheduler {
    config: RefreshSchedulerConfig,
    status: SchedulerStatus,
    queue: VecDeque<RefreshJob>,
    active_jobs: Vec<RefreshJob>,
}

impl RefreshScheduler {
    pub fn new(config: RefreshSchedulerConfig) -> Self {
        Self {
            config,
            status: SchedulerStatus::default(),
            queue: VecDeque::new(),
            active_jobs: Vec::new(),
        }
    }

    pub fn config(&self) -> &RefreshSchedulerConfig {
        &self.config
    }

    pub fn status(&self) -> SchedulerStatus {
        let mut status = self.status.clone();
        status.active_jobs = self.active_jobs.len();
        status.queued_jobs = self.queue.len();
        status
    }

    pub fn is_running(&self) -> bool {
        matches!(self.status.state, RefreshState::Running)
    }

    pub fn set_state(&mut self, state: RefreshState, at: Option<DateTime<Utc>>) {
        self.status.state = state;
        match state {
            RefreshState::Running => self.status.started_at = at,
            RefreshState::Idle | RefreshState::Paused | RefreshState::Stopping => {
                self.status.completed_at = at
            }
        }
    }

    pub fn record_error(&mut self, error: impl Into<String>) {
        self.status.last_error = Some(error.into());
    }

    pub fn clear_error(&mut self) {
        self.status.last_error = None;
    }

    pub fn enqueue(&mut self, job: RefreshJob) {
        self.queue.push_back(job);
    }

    pub fn queue_resource(&mut self, resource_type: ResourceType, strategy: RefreshStrategy) {
        self.enqueue(RefreshJob {
            resource_type,
            strategy,
        });
    }

    pub fn next_job(&mut self) -> Option<RefreshJob> {
        let job = self.queue.pop_front()?;
        self.active_jobs.push(job.clone());
        Some(job)
    }

    pub fn complete_job(&mut self, resource_type: &ResourceType) {
        self.active_jobs
            .retain(|job| &job.resource_type != resource_type);
    }

    pub fn clear(&mut self) {
        self.queue.clear();
        self.active_jobs.clear();
        self.status = SchedulerStatus::default();
    }

    pub fn apply_checkpoint(
        &mut self,
        resource_type: ResourceType,
        checkpoint: &RefreshCheckpoint,
        last_reconciled_scope: Option<ScopeReconciliationPlan>,
    ) -> ResourceRefreshState {
        ResourceRefreshState {
            resource_type,
            started_at: self.status.started_at,
            completed_at: self.status.completed_at,
            checkpoint: Some(checkpoint.clone()),
            last_reconciled_scope,
            last_error: self.status.last_error.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncWindow {
    pub lower_exclusive: DateTime<Utc>,
    pub upper_inclusive: DateTime<Utc>,
}

impl SyncWindow {
    pub fn new(lower_exclusive: DateTime<Utc>, upper_inclusive: DateTime<Utc>) -> Self {
        Self {
            lower_exclusive,
            upper_inclusive,
        }
    }

    pub fn contains(&self, value: DateTime<Utc>) -> bool {
        value > self.lower_exclusive && value <= self.upper_inclusive
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefreshCheckpoint {
    pub window: SyncWindow,
    pub observed_high_watermark: DateTime<Utc>,
}

impl RefreshCheckpoint {
    pub fn new(window: SyncWindow) -> Self {
        let observed_high_watermark = window.upper_inclusive;
        Self {
            observed_high_watermark,
            window,
        }
    }

    pub fn from_sync_state(last_incr: Option<DateTime<Utc>>, window_end: DateTime<Utc>) -> Self {
        let lower_exclusive = last_incr.unwrap_or(window_end);
        Self::new(SyncWindow::new(lower_exclusive, window_end))
    }

    pub fn observe(&self, value: DateTime<Utc>) -> bool {
        self.window.contains(value)
    }

    pub fn with_observed_high_watermark(mut self, value: DateTime<Utc>) -> Self {
        if value > self.observed_high_watermark {
            self.observed_high_watermark = value;
        }
        self
    }

    pub fn commit_high_watermark(&self) -> DateTime<Utc> {
        self.observed_high_watermark
    }

    pub fn to_sync_state_row(
        &self,
        resource_type: impl Into<String>,
        last_full: Option<DateTime<Utc>>,
        cursor: Option<String>,
        filter_hash: Option<String>,
    ) -> SyncStateRow {
        SyncStateRow {
            resource_type: resource_type.into(),
            last_full,
            last_incr: Some(self.commit_high_watermark()),
            high_watermark: Some(self.observed_high_watermark),
            cursor,
            filter_hash,
        }
    }

    pub fn with_start_time(mut self, start_time: DateTime<Utc>) -> Self {
        self.window.lower_exclusive = start_time;
        self
    }

    pub fn lookback(&self) -> Duration {
        self.window
            .upper_inclusive
            .signed_duration_since(self.window.lower_exclusive)
            .to_std()
            .unwrap_or_else(|_| Duration::from_secs(0))
    }
}

impl From<&SyncStateRow> for RefreshCheckpoint {
    fn from(value: &SyncStateRow) -> Self {
        let upper = value
            .high_watermark
            .or(value.last_incr)
            .or(value.last_full)
            .unwrap_or_else(Utc::now);
        let lower = value.last_incr.or(value.last_full).unwrap_or(upper);
        Self::new(SyncWindow::new(lower, upper))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatermarkTracker {
    pub checkpoint: RefreshCheckpoint,
    pub observed_max: Option<DateTime<Utc>>,
}

impl WatermarkTracker {
    pub fn new(checkpoint: RefreshCheckpoint) -> Self {
        Self {
            checkpoint,
            observed_max: None,
        }
    }

    pub fn from_sync_state(row: &SyncStateRow) -> Self {
        Self::new(RefreshCheckpoint::from(row))
    }

    pub fn record_observation(&mut self, value: DateTime<Utc>) -> bool {
        if !self.checkpoint.observe(value) {
            return false;
        }
        self.observed_max = Some(
            self.observed_max
                .map_or(value, |current| current.max(value)),
        );
        true
    }

    pub fn finalize(self) -> RefreshCheckpoint {
        match self.observed_max {
            Some(max) => RefreshCheckpoint {
                observed_high_watermark: max,
                ..self.checkpoint
            },
            None => self.checkpoint,
        }
    }

    pub fn is_in_window(&self, value: DateTime<Utc>) -> bool {
        self.checkpoint.observe(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefreshScopeSummary {
    pub resource_type: ResourceType,
    pub kept: usize,
    pub tombstoned: usize,
    pub pruned: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefreshOutcome {
    pub checkpoint: Option<RefreshCheckpoint>,
    pub scope: Option<ScopeReconciliationPlan>,
    pub records_seen: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefreshRequest {
    pub resource_type: ResourceType,
    pub strategy: RefreshStrategy,
    pub checkpoint: Option<RefreshCheckpoint>,
}

impl RefreshRequest {
    pub fn new(resource_type: ResourceType, strategy: RefreshStrategy) -> Self {
        Self {
            resource_type,
            strategy,
            checkpoint: None,
        }
    }

    pub fn with_checkpoint(mut self, checkpoint: RefreshCheckpoint) -> Self {
        self.checkpoint = Some(checkpoint);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduledResource {
    pub resource_type: ResourceType,
    pub policy: RefreshPolicy,
}

impl ScheduledResource {
    pub fn new(resource_type: ResourceType, policy: RefreshPolicy) -> Self {
        Self {
            resource_type,
            policy,
        }
    }
}

pub fn default_schedule_time() -> &'static str {
    "06:00"
}

pub fn bounded_concurrency_limit(limit: usize) -> usize {
    limit.max(1)
}

pub fn reconcile_scope<I, S>(
    resource_type: ResourceType,
    existing: &[RecordRow],
    seen_sys_ids: I,
) -> ScopeReconciliationPlan
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let seen = seen_sys_ids
        .into_iter()
        .map(|item| item.as_ref().to_string())
        .collect::<std::collections::BTreeSet<_>>();

    let mut outcomes = Vec::with_capacity(existing.len());
    for row in existing {
        let action = if seen.contains(&row.sys_id) {
            ScopeAction::Retain
        } else if row.lifecycle() == crate::cache::store::RecordLifecycle::Pruned {
            ScopeAction::Prune
        } else {
            ScopeAction::Tombstone
        };
        outcomes.push(ScopeOutcome::from_row(row, action));
    }

    ScopeReconciliationPlan {
        resource_type,
        outcomes,
    }
}

/// Apply a [`ScopeReconciliationPlan`] to the store, tombstoning or pruning records as directed,
/// then clean up orphaned enrichments and relationships left behind by tombstoned records.
///
/// Cleanup errors are non-fatal and are logged as warnings so a partial cleanup does not abort
/// a refresh run.
pub fn apply_scope_plan(
    store: &Store,
    plan: &ScopeReconciliationPlan,
    when: DateTime<Utc>,
) -> crate::cache::store::Result<()> {
    for sys_id in plan.tombstone_sys_ids() {
        store.tombstone_record(sys_id, when)?;
    }
    for sys_id in plan.prune_sys_ids() {
        store.prune_record(sys_id, when)?;
    }

    if let Err(err) = store.cleanup_orphaned_relationships() {
        tracing::warn!(error = %err, "failed to clean orphaned relationships");
    }
    if let Err(err) = store.cleanup_orphaned_references() {
        tracing::warn!(error = %err, "failed to clean orphaned references");
    }
    if let Err(err) = store.cleanup_orphaned_enrichments() {
        tracing::warn!(error = %err, "failed to clean orphaned enrichments");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::store::RecordRow;
    use chrono::TimeZone;

    #[test]
    fn checkpoint_commits_high_watermark() {
        let window_end = Utc.timestamp_opt(1_712_650_000, 0).unwrap();
        let checkpoint = RefreshCheckpoint::from_sync_state(Some(window_end), window_end);
        assert_eq!(checkpoint.commit_high_watermark(), window_end);
    }

    #[test]
    fn tracker_records_only_in_window_observations() {
        let start = Utc.timestamp_opt(1_712_649_600, 0).unwrap();
        let end = Utc.timestamp_opt(1_712_650_000, 0).unwrap();
        let checkpoint = RefreshCheckpoint::new(SyncWindow::new(start, end));
        let mut tracker = WatermarkTracker::new(checkpoint);

        assert!(tracker.record_observation(Utc.timestamp_opt(1_712_649_800, 0).unwrap()));
        assert!(!tracker.record_observation(Utc.timestamp_opt(1_712_650_100, 0).unwrap()));
        let final_checkpoint = tracker.finalize();
        assert_eq!(
            final_checkpoint.commit_high_watermark(),
            Utc.timestamp_opt(1_712_649_800, 0).unwrap()
        );
    }

    #[test]
    fn reconcile_scope_marks_missing_rows_for_tombstone() {
        let row = RecordRow::active(
            "sys-1",
            "INC0001",
            "incident",
            ResourceType::Incident,
            Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
        );
        let plan = reconcile_scope(ResourceType::Incident, &[row], std::iter::empty::<&str>());
        assert_eq!(plan.tombstoned_count(), 1);
        assert_eq!(plan.kept_count(), 0);
        assert_eq!(plan.resource_type, ResourceType::Incident);
    }

    #[test]
    fn reconcile_scope_retains_seen_rows() {
        let row = RecordRow::active(
            "sys-1",
            "INC0001",
            "incident",
            ResourceType::Incident,
            Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
        );
        let plan = reconcile_scope(ResourceType::Incident, &[row], ["sys-1"]);
        assert_eq!(plan.kept_count(), 1);
        assert_eq!(plan.tombstoned_count(), 0);
    }
}
