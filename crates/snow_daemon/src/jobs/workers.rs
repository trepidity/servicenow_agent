//! Job worker dispatch and per-kind worker functions.
//!
//! A `JobContext` is the handle the worker uses to publish progress, append
//! log lines, and observe cooperative cancellation. `AppRunner` carries the
//! shared daemon state ([`DaemonState`]) into the workers; per-kind worker
//! functions call into the existing `SnowCore` handlers and report progress
//! through the [`JobRegistry`].
//!
//! Because `SnowCore` is `!Send` (the daemon runs on a `current_thread`
//! runtime inside a `LocalSet`), workers are spawned with
//! `tokio::task::spawn_local` rather than `tokio::spawn`, and synchronous
//! core methods are called directly rather than via `spawn_blocking` (which
//! would require the captured state to be `Send`).

use std::sync::Arc;

use anyhow::Result;
use serde_json::Value;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{JobKind, JobProgress, JobRegistry, JobStatus};
use crate::DaemonState;

/// Handle threaded into core handlers so they can report progress and observe
/// cooperative cancellation.
#[derive(Clone)]
pub struct JobContext {
    /// The id of the job this context belongs to.
    pub job_id: Uuid,
    /// The shared registry the job records progress / log lines / status into.
    pub registry: Arc<JobRegistry>,
    /// Cancellation token; workers should check `is_cancelled()` between
    /// natural batch boundaries and stop early if it fires.
    pub cancel: CancellationToken,
}

impl JobContext {
    /// Record a progress update on the underlying job entry.
    pub async fn progress(&self, stage: impl Into<String>, current: u64, total: Option<u64>) {
        self.registry
            .record_progress(
                self.job_id,
                JobProgress {
                    current,
                    total,
                    stage: stage.into(),
                },
            )
            .await;
    }

    /// Append a single log line to the job's bounded log tail.
    pub async fn log(&self, line: impl Into<String>) {
        self.registry.append_log(self.job_id, line.into()).await;
    }

    /// Whether the cooperative cancel token for this job has fired.
    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }
}

/// Carries shared application state into workers. Cloning is cheap — both
/// fields are `Arc`s.
#[derive(Clone)]
pub struct AppRunner {
    /// Shared daemon state, including `SnowCore` and the `JobRegistry`.
    pub state: Arc<DaemonState>,
}

/// Spawn a job by kind. Returns the new job id immediately; the work runs on
/// a Tokio local task and reports its outcome through the registry.
///
/// Per-kind serialization is enforced via [`JobRegistry::kind_lock`] — only
/// one job of a given kind may be running at a time across the daemon
/// process.
///
/// # Panics
/// Must be called from within a `LocalSet` context (the daemon's main
/// runtime); otherwise `spawn_local` will panic. This matches the rest of
/// the daemon's task spawning model.
pub async fn spawn(
    registry: Arc<JobRegistry>,
    kind: JobKind,
    params: Value,
    runner: AppRunner,
) -> Uuid {
    let id = registry.insert(kind).await;
    let token = registry.register_token(id).await;
    let kind_lock = registry.kind_lock(kind).await;
    let ctx = JobContext {
        job_id: id,
        registry: registry.clone(),
        cancel: token,
    };

    tokio::task::spawn_local(async move {
        // Per-kind serialization: only one job of `kind` runs at a time.
        let _guard = kind_lock.lock().await;
        if ctx.is_cancelled() {
            registry.finish(id, JobStatus::Cancelled, None, None).await;
            return;
        }
        registry.mark_running(id).await;
        let outcome = run_kind(kind, params, ctx.clone(), runner).await;
        match outcome {
            Ok(value) if !ctx.is_cancelled() => {
                registry
                    .finish(id, JobStatus::Succeeded, Some(value), None)
                    .await;
            }
            Ok(_) => {
                registry.finish(id, JobStatus::Cancelled, None, None).await;
            }
            Err(e) => {
                registry
                    .finish(id, JobStatus::Failed, None, Some(format!("{e:#}")))
                    .await;
            }
        }
    });

    id
}

/// Dispatch a `JobKind` to its concrete worker.
async fn run_kind(
    kind: JobKind,
    params: Value,
    ctx: JobContext,
    runner: AppRunner,
) -> Result<Value> {
    match kind {
        JobKind::VerifyVault => verify_vault_worker(params, ctx, runner).await,
        JobKind::KbSync => kb_sync_worker(params, ctx, runner, false).await,
        JobKind::KbSyncFull => kb_sync_worker(params, ctx, runner, true).await,
        JobKind::RebuildCache => rebuild_cache_worker(params, ctx, runner).await,
        JobKind::PruneOrphans => prune_orphans_worker(params, ctx, runner).await,
        JobKind::RepairVault => repair_vault_worker(params, ctx, runner).await,
        JobKind::RefreshAll => refresh_all_worker(params, ctx, runner).await,
        JobKind::SemanticIndexRebuild => semantic_index_rebuild_worker(params, ctx, runner).await,
    }
}

/// Wraps `SnowCore::verify_vault` (synchronous) and reports progress.
///
/// `verify_vault` does not currently expose a batch boundary, so cooperative
/// cancel granularity is whole-sync until core exposes one.
async fn verify_vault_worker(_params: Value, ctx: JobContext, runner: AppRunner) -> Result<Value> {
    ctx.progress("starting", 0, None).await;
    ctx.log("verify_vault: starting").await;

    if ctx.is_cancelled() {
        return Ok(serde_json::json!({"cancelled": true}));
    }

    // Direct call: `SnowCore` is `!Send`, so `spawn_blocking` is not viable.
    // The rpc.rs handler does the same thing today.
    // cooperative cancel granularity is whole-sync until core exposes batch boundary
    let report = runner.state.core.verify_vault()?;

    ctx.progress("complete", 1, Some(1)).await;
    ctx.log("verify_vault: complete").await;
    Ok(serde_json::json!({ "report": report }))
}

/// Wraps `SnowCore::sync_knowledge`. `full=true` corresponds to
/// [`JobKind::KbSyncFull`]; `full=false` to [`JobKind::KbSync`].
///
/// Reads `with_bodies: bool` from `params` (defaults to false).
async fn kb_sync_worker(
    params: Value,
    ctx: JobContext,
    runner: AppRunner,
    full: bool,
) -> Result<Value> {
    let with_bodies = params
        .get("with_bodies")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    ctx.progress("starting", 0, None).await;
    ctx.log(format!("kb_sync: full={full} with_bodies={with_bodies}"))
        .await;

    if ctx.is_cancelled() {
        return Ok(serde_json::json!({"cancelled": true}));
    }

    // cooperative cancel granularity is whole-sync until core exposes batch boundary
    let outcome = runner.state.core.sync_knowledge(full, with_bodies).await?;

    ctx.progress("complete", 1, Some(1)).await;
    ctx.log("kb_sync: complete").await;
    Ok(serde_json::json!({ "sync": outcome }))
}

/// Wraps `SnowCore::rebuild_cache` (synchronous). See `verify_vault_worker`
/// for the rationale on calling sync core methods directly rather than via
/// `spawn_blocking`.
async fn rebuild_cache_worker(_params: Value, ctx: JobContext, runner: AppRunner) -> Result<Value> {
    ctx.progress("starting", 0, None).await;
    ctx.log("rebuild_cache: starting").await;

    if ctx.is_cancelled() {
        return Ok(serde_json::json!({"cancelled": true}));
    }

    // cooperative cancel granularity is whole-sync until core exposes batch boundary
    let report = runner.state.core.rebuild_cache()?;

    ctx.progress("complete", 1, Some(1)).await;
    ctx.log("rebuild_cache: complete").await;
    Ok(serde_json::json!({ "report": report }))
}

/// Wraps `SnowCore::prune_orphans`. Reads `dry_run: bool` from `params`
/// (defaults to false to match the JSON-RPC behavior).
async fn prune_orphans_worker(params: Value, ctx: JobContext, runner: AppRunner) -> Result<Value> {
    let dry_run = params
        .get("dry_run")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    ctx.progress("starting", 0, None).await;
    ctx.log(format!("prune_orphans: dry_run={dry_run}")).await;

    if ctx.is_cancelled() {
        return Ok(serde_json::json!({"cancelled": true}));
    }

    // cooperative cancel granularity is whole-sync until core exposes batch boundary
    let report = runner.state.core.prune_orphans(dry_run).await?;

    ctx.progress("complete", 1, Some(1)).await;
    ctx.log("prune_orphans: complete").await;
    Ok(serde_json::json!({ "report": report }))
}

/// Wraps `SnowCore::repair_vault`.
async fn repair_vault_worker(_params: Value, ctx: JobContext, runner: AppRunner) -> Result<Value> {
    ctx.progress("starting", 0, None).await;
    ctx.log("repair_vault: starting").await;

    if ctx.is_cancelled() {
        return Ok(serde_json::json!({"cancelled": true}));
    }

    // cooperative cancel granularity is whole-sync until core exposes batch boundary
    let report = runner.state.core.repair_vault().await?;

    ctx.progress("complete", 1, Some(1)).await;
    ctx.log("repair_vault: complete").await;
    Ok(serde_json::json!({ "report": report }))
}

/// Composite refresh pipeline: incremental `kb_sync`, then `rebuild_cache`,
/// then `verify_vault`. Each phase reports its own progress stage so the
/// TUI can show a meaningful "what's happening now" indicator. Cancellation
/// is observed between phases.
async fn refresh_all_worker(params: Value, ctx: JobContext, runner: AppRunner) -> Result<Value> {
    let with_bodies = params
        .get("with_bodies")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    ctx.progress("starting", 0, Some(3)).await;
    ctx.log("refresh_all: starting").await;

    if ctx.is_cancelled() {
        return Ok(serde_json::json!({"cancelled": true}));
    }
    ctx.progress("kb_sync", 0, Some(3)).await;
    let sync = runner.state.core.sync_knowledge(false, with_bodies).await?;

    if ctx.is_cancelled() {
        return Ok(serde_json::json!({"cancelled": true, "phase": "after_kb_sync"}));
    }
    ctx.progress("rebuild_cache", 1, Some(3)).await;
    let rebuild = runner.state.core.rebuild_cache()?;

    if ctx.is_cancelled() {
        return Ok(serde_json::json!({"cancelled": true, "phase": "after_rebuild_cache"}));
    }
    ctx.progress("verify_vault", 2, Some(3)).await;
    let verify = runner.state.core.verify_vault()?;

    ctx.progress("complete", 3, Some(3)).await;
    ctx.log("refresh_all: complete").await;
    Ok(serde_json::json!({
        "sync": sync,
        "rebuild": rebuild,
        "verify": verify,
    }))
}

/// Wraps `SnowCore::rebuild_knowledge_semantic_index`. Reads `full: bool`
/// from `params` (defaults to false to match the JSON-RPC behavior of the
/// existing `kb_semantic_rebuild` method).
async fn semantic_index_rebuild_worker(
    params: Value,
    ctx: JobContext,
    runner: AppRunner,
) -> Result<Value> {
    let full = params
        .get("full")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    ctx.progress("starting", 0, None).await;
    ctx.log(format!("semantic_index_rebuild: full={full}"))
        .await;

    if ctx.is_cancelled() {
        return Ok(serde_json::json!({"cancelled": true}));
    }

    // cooperative cancel granularity is whole-sync until core exposes batch boundary
    let summary = runner
        .state
        .core
        .rebuild_knowledge_semantic_index(full)
        .await?;

    ctx.progress("complete", 1, Some(1)).await;
    ctx.log("semantic_index_rebuild: complete").await;
    Ok(serde_json::json!({ "summary": summary }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::build_fixture_state;
    use std::time::Duration;
    use tokio::task::LocalSet;

    /// Spawn a job and poll the registry until it reaches a terminal state
    /// or a hard timeout fires. Returns the final `Job` snapshot.
    async fn run_until_terminal(
        reg: Arc<JobRegistry>,
        id: Uuid,
        max_iters: usize,
    ) -> super::super::Job {
        for _ in 0..max_iters {
            if let Some(job) = reg.get(id).await
                && job.finished_at.is_some()
            {
                return job;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("job did not finish within timeout");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn verify_vault_job_runs_against_temp_state() {
        let local = LocalSet::new();
        local
            .run_until(async {
                let fixture = build_fixture_state().await.expect("fixture");
                let reg = fixture.state.jobs.clone();
                let id = spawn(
                    reg.clone(),
                    JobKind::VerifyVault,
                    serde_json::json!({}),
                    AppRunner {
                        state: fixture.state.clone(),
                    },
                )
                .await;
                let job = run_until_terminal(reg, id, 500).await;
                assert_eq!(
                    job.status,
                    JobStatus::Succeeded,
                    "verify_vault should succeed on a fresh fixture (error: {:?})",
                    job.error
                );
                assert!(job.result.is_some());
            })
            .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancel_before_run_observes_cancellation() {
        let local = LocalSet::new();
        local
            .run_until(async {
                let fixture = build_fixture_state().await.expect("fixture");
                let reg = fixture.state.jobs.clone();
                let id = spawn(
                    reg.clone(),
                    JobKind::VerifyVault,
                    serde_json::json!({}),
                    AppRunner {
                        state: fixture.state.clone(),
                    },
                )
                .await;
                // Cancel as fast as possible. Worker is small enough that this
                // race may land before or after completion — both terminal
                // states are acceptable; we only assert termination.
                reg.cancel(id).await;
                let job = run_until_terminal(reg, id, 500).await;
                assert!(matches!(
                    job.status,
                    JobStatus::Cancelled | JobStatus::Succeeded
                ));
            })
            .await;
    }
}
