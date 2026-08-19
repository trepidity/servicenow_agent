use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobKind {
    KbSync,
    KbSyncFull,
    VerifyVault,
    PruneOrphans,
    RepairVault,
    RefreshAll,
    SemanticIndexRebuild,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobProgress {
    pub current: u64,
    pub total: Option<u64>,
    pub stage: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: Uuid,
    pub kind: JobKind,
    pub status: JobStatus,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub progress: Option<JobProgress>,
    pub log_tail: VecDeque<String>,
    pub result: Option<serde_json::Value>,
    pub error: Option<String>,
}

const LOG_TAIL_CAP: usize = 200;
const FINISHED_RETENTION_SECS: i64 = 30 * 60;
const FINISHED_CAP: usize = 100;

#[derive(Debug, Clone, Default)]
pub struct ListJobsFilter {
    pub include_finished: bool,
    pub limit: Option<usize>,
}

#[derive(Debug, Default)]
pub struct JobRegistry {
    jobs: RwLock<HashMap<Uuid, Job>>,
    kind_locks: Mutex<HashMap<JobKind, Arc<Mutex<()>>>>,
    cancel_tokens: RwLock<HashMap<Uuid, CancellationToken>>,
}

impl JobRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn insert(&self, kind: JobKind) -> Uuid {
        let id = Uuid::new_v4();
        let job = Job {
            id,
            kind,
            status: JobStatus::Pending,
            started_at: Utc::now(),
            finished_at: None,
            progress: None,
            log_tail: VecDeque::with_capacity(LOG_TAIL_CAP),
            result: None,
            error: None,
        };
        self.jobs.write().await.insert(id, job);
        id
    }

    pub async fn get(&self, id: Uuid) -> Option<Job> {
        self.jobs.read().await.get(&id).cloned()
    }

    pub async fn mark_running(&self, id: Uuid) {
        if let Some(job) = self.jobs.write().await.get_mut(&id)
            && matches!(job.status, JobStatus::Pending)
        {
            job.status = JobStatus::Running;
        }
    }

    pub async fn record_progress(&self, id: Uuid, progress: JobProgress) {
        if let Some(job) = self.jobs.write().await.get_mut(&id) {
            job.progress = Some(progress);
        }
    }

    pub async fn append_log(&self, id: Uuid, line: String) {
        if let Some(job) = self.jobs.write().await.get_mut(&id) {
            if job.log_tail.len() == LOG_TAIL_CAP {
                job.log_tail.pop_front();
            }
            job.log_tail.push_back(line);
        }
    }

    pub async fn finish(
        &self,
        id: Uuid,
        status: JobStatus,
        result: Option<serde_json::Value>,
        error: Option<String>,
    ) {
        if let Some(job) = self.jobs.write().await.get_mut(&id) {
            if !matches!(job.status, JobStatus::Cancelled) || matches!(status, JobStatus::Cancelled)
            {
                job.status = status;
            }
            job.finished_at = Some(Utc::now());
            job.result = result;
            job.error = error;
        }
        self.cancel_tokens.write().await.remove(&id);
        self.sweep_retention().await;
    }

    pub async fn list(&self, filter: ListJobsFilter) -> Vec<Job> {
        let jobs = self.jobs.read().await;
        let mut out: Vec<Job> = jobs
            .values()
            .filter(|j| filter.include_finished || j.finished_at.is_none())
            .cloned()
            .collect();
        out.sort_by_key(|j| std::cmp::Reverse(j.started_at));
        if let Some(limit) = filter.limit {
            out.truncate(limit);
        }
        out
    }

    pub async fn register_token(&self, id: Uuid) -> CancellationToken {
        let token = CancellationToken::new();
        self.cancel_tokens.write().await.insert(id, token.clone());
        token
    }

    pub async fn cancel(&self, id: Uuid) -> bool {
        let cancellable = self
            .jobs
            .read()
            .await
            .get(&id)
            .map(|job| matches!(job.status, JobStatus::Pending | JobStatus::Running))
            .unwrap_or(false);
        if !cancellable {
            return false;
        }

        let token = self.cancel_tokens.read().await.get(&id).cloned();
        match token {
            Some(t) if !t.is_cancelled() => {
                t.cancel();
                if let Some(job) = self.jobs.write().await.get_mut(&id)
                    && matches!(job.status, JobStatus::Pending | JobStatus::Running)
                {
                    // Status flips to Cancelled when worker observes the token,
                    // but record intent so list_jobs can surface it.
                    job.status = JobStatus::Cancelled;
                    job.finished_at = Some(Utc::now());
                }
                true
            }
            _ => false,
        }
    }

    pub async fn kind_lock(&self, kind: JobKind) -> Arc<Mutex<()>> {
        self.kind_locks
            .lock()
            .await
            .entry(kind)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    async fn sweep_retention(&self) {
        let cutoff = Utc::now() - chrono::Duration::seconds(FINISHED_RETENTION_SECS);
        let mut removed = Vec::new();
        let mut jobs = self.jobs.write().await;
        let mut finished: Vec<(Uuid, DateTime<Utc>)> = jobs
            .iter()
            .filter_map(|(id, j)| j.finished_at.map(|t| (*id, t)))
            .collect();
        finished.retain(|(id, t)| {
            if *t < cutoff {
                jobs.remove(id);
                removed.push(*id);
                false
            } else {
                true
            }
        });
        if finished.len() > FINISHED_CAP {
            finished.sort_by_key(|(_, t)| *t);
            let excess = finished.len() - FINISHED_CAP;
            for (id, _) in finished.into_iter().take(excess) {
                jobs.remove(&id);
                removed.push(id);
            }
        }
        drop(jobs);

        if !removed.is_empty() {
            let mut tokens = self.cancel_tokens.write().await;
            for id in removed {
                tokens.remove(&id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn insert_then_get_roundtrips() {
        let reg = JobRegistry::new();
        let id = reg.insert(JobKind::KbSync).await;
        let job = reg.get(id).await.expect("job present");
        assert_eq!(job.kind, JobKind::KbSync);
        assert_eq!(job.status, JobStatus::Pending);
    }

    #[tokio::test]
    async fn get_missing_returns_none() {
        let reg = JobRegistry::new();
        assert!(reg.get(Uuid::new_v4()).await.is_none());
    }

    #[tokio::test]
    async fn status_progresses_through_running_to_succeeded() {
        let reg = JobRegistry::new();
        let id = reg.insert(JobKind::VerifyVault).await;
        reg.mark_running(id).await;
        assert_eq!(reg.get(id).await.unwrap().status, JobStatus::Running);
        reg.finish(
            id,
            JobStatus::Succeeded,
            Some(serde_json::json!({"ok": true})),
            None,
        )
        .await;
        let job = reg.get(id).await.unwrap();
        assert_eq!(job.status, JobStatus::Succeeded);
        assert!(job.finished_at.is_some());
        assert_eq!(job.result, Some(serde_json::json!({"ok": true})));
    }

    #[tokio::test]
    async fn log_tail_caps_at_limit() {
        let reg = JobRegistry::new();
        let id = reg.insert(JobKind::KbSync).await;
        for i in 0..(LOG_TAIL_CAP + 50) {
            reg.append_log(id, format!("line {i}")).await;
        }
        let job = reg.get(id).await.unwrap();
        assert_eq!(job.log_tail.len(), LOG_TAIL_CAP);
        assert_eq!(job.log_tail.front().unwrap(), "line 50");
        assert_eq!(
            job.log_tail.back().unwrap(),
            &format!("line {}", LOG_TAIL_CAP + 49)
        );
    }

    #[tokio::test]
    async fn list_excludes_finished_by_default() {
        let reg = JobRegistry::new();
        let active = reg.insert(JobKind::KbSync).await;
        let done = reg.insert(JobKind::VerifyVault).await;
        reg.finish(done, JobStatus::Succeeded, None, None).await;
        let active_only = reg.list(ListJobsFilter::default()).await;
        assert_eq!(active_only.len(), 1);
        assert_eq!(active_only[0].id, active);
        let all = reg
            .list(ListJobsFilter {
                include_finished: true,
                limit: None,
            })
            .await;
        assert_eq!(all.len(), 2);
    }

    #[tokio::test]
    async fn retention_drops_jobs_older_than_window() {
        let reg = JobRegistry::new();
        let id = reg.insert(JobKind::PruneOrphans).await;
        reg.finish(id, JobStatus::Succeeded, None, None).await;
        // Backdate the finished_at to force a sweep removal
        {
            let mut jobs = reg.jobs.write().await;
            let job = jobs.get_mut(&id).unwrap();
            job.finished_at =
                Some(Utc::now() - chrono::Duration::seconds(FINISHED_RETENTION_SECS + 60));
        }
        // Trigger a sweep by finishing another job.
        let other = reg.insert(JobKind::VerifyVault).await;
        reg.finish(other, JobStatus::Succeeded, None, None).await;
        assert!(reg.get(id).await.is_none());
        assert!(reg.get(other).await.is_some());
    }

    #[tokio::test]
    async fn cancel_marks_job_cancelled_and_fires_token() {
        let reg = JobRegistry::new();
        let id = reg.insert(JobKind::KbSync).await;
        let token = reg.register_token(id).await;
        assert!(reg.cancel(id).await);
        assert!(token.is_cancelled());
        assert_eq!(reg.get(id).await.unwrap().status, JobStatus::Cancelled);
    }

    #[tokio::test]
    async fn finish_preserves_cancelled_status() {
        let reg = JobRegistry::new();
        let id = reg.insert(JobKind::KbSync).await;
        reg.register_token(id).await;
        assert!(reg.cancel(id).await);
        reg.finish(id, JobStatus::Failed, None, Some("late error".into()))
            .await;
        let job = reg.get(id).await.unwrap();
        assert_eq!(job.status, JobStatus::Cancelled);
        assert_eq!(job.error.as_deref(), Some("late error"));
    }

    #[tokio::test]
    async fn finish_removes_cancel_token() {
        let reg = JobRegistry::new();
        let id = reg.insert(JobKind::KbSync).await;
        reg.register_token(id).await;
        reg.finish(id, JobStatus::Succeeded, None, None).await;
        assert!(!reg.cancel(id).await);
        assert!(!reg.cancel_tokens.read().await.contains_key(&id));
    }

    #[tokio::test]
    async fn cancel_unknown_returns_false() {
        let reg = JobRegistry::new();
        assert!(!reg.cancel(Uuid::new_v4()).await);
    }

    #[tokio::test]
    async fn kind_lock_is_shared_per_kind() {
        let reg = JobRegistry::new();
        let a = reg.kind_lock(JobKind::KbSync).await;
        let b = reg.kind_lock(JobKind::KbSync).await;
        let c = reg.kind_lock(JobKind::VerifyVault).await;
        assert!(Arc::ptr_eq(&a, &b));
        assert!(!Arc::ptr_eq(&a, &c));
    }
}
