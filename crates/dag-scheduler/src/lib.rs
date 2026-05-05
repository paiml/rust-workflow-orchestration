//! `dag-scheduler` — minimal `tokio-cron-scheduler` wrapper.
//!
//! Triggers DAG runs on a cron expression. The M2 lessons of the Workflow
//! Orchestration with Rust course use this crate to demonstrate the cron-vs-
//! interval-vs-event distinction without bringing in the full apalis worker
//! tree.
//!
//! Each call to [`DagScheduler::schedule`] returns a [`Uuid`] handle the
//! caller can use later to ask "when does this fire next?" or to remove
//! the job entirely.
//!
//! # Lifecycle
//!
//! ```no_run
//! use dag_scheduler::DagScheduler;
//!
//! # async fn run() -> anyhow::Result<()> {
//! let mut sched = DagScheduler::new().await?;
//! sched.start().await?;
//! let id = sched
//!     .schedule("etl-daily", "0 0 6 * * *", || Box::pin(async { Ok(()) }))
//!     .await?;
//! // ...
//! sched.unschedule(id).await?;
//! sched.shutdown().await?;
//! # Ok(()) }
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::{Context as _, Result};
use chrono::{DateTime, Utc};
use thiserror::Error;
use tokio_cron_scheduler::{Job, JobScheduler};
use tracing::{debug, info};
use uuid::Uuid;

/// Errors emitted by the scheduler.
#[derive(Debug, Error)]
pub enum SchedulerError {
    /// `unschedule` / `next_fire` was called with a job id we never minted
    /// (or that has already been removed).
    #[error("unknown job id: {0}")]
    UnknownJob(Uuid),
}

/// Pinned, boxed future returned by a scheduled job. Tokio-cron-scheduler
/// requires this exact shape; the alias keeps the trait bound off the
/// caller's mind.
pub type JobFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;

/// Wrapper around [`tokio_cron_scheduler::JobScheduler`] that mints
/// pre-allocated UUIDs and remembers the cron expression for each job
/// (so [`DagScheduler::next_fire`] can answer without round-tripping the
/// underlying scheduler's metadata).
pub struct DagScheduler {
    inner: Arc<JobScheduler>,
    /// Map from caller-visible job id → cron expression (for `next_fire`)
    /// and registered DAG name (for tracing logs).
    registry: Arc<tokio::sync::RwLock<JobRegistry>>,
}

/// Records what the scheduler knows about each registered job.
#[derive(Debug, Default)]
struct JobRegistry {
    by_id: std::collections::HashMap<Uuid, JobMeta>,
}

#[derive(Debug, Clone)]
struct JobMeta {
    dag_name: String,
    cron: String,
}

impl DagScheduler {
    /// Construct a scheduler. Does not start it — call [`Self::start`] before
    /// scheduling jobs to make sure the background tokio task is alive.
    pub async fn new() -> Result<Self> {
        let inner = JobScheduler::new()
            .await
            .context("failed to construct JobScheduler")?;
        Ok(Self {
            inner: Arc::new(inner),
            registry: Arc::new(tokio::sync::RwLock::new(JobRegistry::default())),
        })
    }

    /// Spin up the background tick loop.
    pub async fn start(&self) -> Result<()> {
        // Cheap clone — `JobScheduler` is internally Arc-y itself.
        Arc::as_ref(&self.inner)
            .clone()
            .start()
            .await
            .context("failed to start JobScheduler")?;
        info!("DagScheduler started");
        Ok(())
    }

    /// Schedule `dag_name` to fire on `cron_expr`, invoking `body` each time
    /// the cron matches. Returns the job id you'll use to unschedule.
    ///
    /// `body` is a function that returns a fresh future on each tick — this
    /// lets the caller capture per-tick state in a closure and rebuild the
    /// future as the scheduler triggers.
    pub async fn schedule<F>(
        &self,
        dag_name: impl Into<String>,
        cron_expr: impl Into<String>,
        mut body: F,
    ) -> Result<Uuid>
    where
        F: FnMut() -> JobFuture + Send + Sync + 'static,
    {
        let dag_name = dag_name.into();
        let cron_expr = cron_expr.into();
        let dag_name_cloned = dag_name.clone();

        let job = Job::new_async(cron_expr.as_str(), move |_uuid, _l| {
            let fut = body();
            let dag_name = dag_name_cloned.clone();
            Box::pin(async move {
                debug!(dag = %dag_name, "tick");
                if let Err(e) = fut.await {
                    tracing::error!(dag = %dag_name, error = ?e, "scheduled run failed");
                }
            })
        })
        .with_context(|| format!("invalid cron expression: {cron_expr}"))?;

        let id = self
            .inner
            .add(job)
            .await
            .context("failed to register job with scheduler")?;
        self.registry.write().await.by_id.insert(
            id,
            JobMeta {
                dag_name,
                cron: cron_expr,
            },
        );
        Ok(id)
    }

    /// Remove a previously-scheduled job.
    pub async fn unschedule(&self, id: Uuid) -> Result<()> {
        if !self.registry.read().await.by_id.contains_key(&id) {
            return Err(SchedulerError::UnknownJob(id).into());
        }
        self.inner
            .remove(&id)
            .await
            .context("failed to remove job from scheduler")?;
        self.registry.write().await.by_id.remove(&id);
        Ok(())
    }

    /// Return the next scheduled fire time for a job, computed against the
    /// stored cron expression. Returns `None` if the cron expression has
    /// no future fires (e.g. a one-shot Job::new_one_shot would).
    pub async fn next_fire(&self, id: Uuid) -> Result<Option<DateTime<Utc>>> {
        let reg = self.registry.read().await;
        let meta = reg
            .by_id
            .get(&id)
            .ok_or(SchedulerError::UnknownJob(id))?
            .clone();
        drop(reg);
        // `cron::Schedule::from_str` is the canonical 6-field-cron parser
        // tokio-cron-scheduler also uses internally.
        use std::str::FromStr;
        let schedule = cron::Schedule::from_str(&meta.cron)
            .with_context(|| format!("re-parsing cron '{}' failed", meta.cron))?;
        Ok(schedule.upcoming(Utc).next())
    }

    /// Return the DAG name registered for `id`. Useful for log + UI surfaces.
    pub async fn dag_name(&self, id: Uuid) -> Result<String> {
        let reg = self.registry.read().await;
        Ok(reg
            .by_id
            .get(&id)
            .ok_or(SchedulerError::UnknownJob(id))?
            .dag_name
            .clone())
    }

    /// Total number of registered jobs (live count from the local registry).
    pub async fn job_count(&self) -> usize {
        self.registry.read().await.by_id.len()
    }

    /// Graceful shutdown: stops the tick loop. Does not destroy the
    /// scheduler — you can re-`start` it afterwards.
    pub async fn shutdown(&self) -> Result<()> {
        // tokio-cron-scheduler exposes shutdown via the inner Arc<JobScheduler>.
        Arc::as_ref(&self.inner)
            .clone()
            .shutdown()
            .await
            .context("failed to shut down JobScheduler")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn schedule_and_unschedule_roundtrip() {
        let sched = DagScheduler::new().await.unwrap();
        let id = sched
            .schedule("etl-daily", "0 0 6 * * *", || Box::pin(async { Ok(()) }))
            .await
            .unwrap();
        assert_eq!(sched.job_count().await, 1);
        assert_eq!(sched.dag_name(id).await.unwrap(), "etl-daily");
        sched.unschedule(id).await.unwrap();
        assert_eq!(sched.job_count().await, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unschedule_unknown_errors() {
        let sched = DagScheduler::new().await.unwrap();
        let bogus = Uuid::new_v4();
        let err = sched.unschedule(bogus).await.unwrap_err();
        assert!(err.to_string().contains("unknown job id"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn invalid_cron_rejected() {
        let sched = DagScheduler::new().await.unwrap();
        let r = sched
            .schedule("bad", "this is not cron", || Box::pin(async { Ok(()) }))
            .await;
        assert!(r.is_err(), "expected invalid cron to error");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn next_fire_returns_a_future_time() {
        let sched = DagScheduler::new().await.unwrap();
        let id = sched
            .schedule("daily-6am", "0 0 6 * * *", || Box::pin(async { Ok(()) }))
            .await
            .unwrap();
        let fire = sched.next_fire(id).await.unwrap();
        let fire = fire.expect("daily-6am cron must have a future fire time");
        assert!(fire > Utc::now(), "next fire must be in the future");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn body_is_invoked_on_tick() {
        // Use a once-per-second cron and assert the body fires within ~3s.
        let sched = DagScheduler::new().await.unwrap();
        sched.start().await.unwrap();
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_for_body = counter.clone();
        let id = sched
            .schedule("tick", "*/1 * * * * *", move || {
                let c = counter_for_body.clone();
                Box::pin(async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            })
            .await
            .unwrap();
        // Wait up to 3 seconds for the body to fire at least once.
        for _ in 0..30 {
            if counter.load(Ordering::SeqCst) >= 1 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        sched.unschedule(id).await.unwrap();
        sched.shutdown().await.unwrap();
        assert!(
            counter.load(Ordering::SeqCst) >= 1,
            "body should have fired at least once within 3 seconds"
        );
    }
}
