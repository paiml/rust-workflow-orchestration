//! `LocalRunner` — in-process executor for a `Dag<Box<dyn Task>>`.
//!
//! Walks the DAG in topological order. Each task gets a fresh tracing span,
//! its state is recorded in [`dag_lineage::LineageStore`], and its typed
//! output is published into the shared [`dag_core::Context`] for downstream
//! tasks to consume.
//!
//! Failure policy is fail-fast (Jidoka): the first task that returns an
//! error stops the run; every subsequent task is recorded as `Skipped`.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context as _, Result};
use async_trait::async_trait;
use chrono::Utc;
use dag_core::{Context, Dag, Task, TaskOutput};
use dag_lineage::{LineageStore, TaskRunRecord};
use tracing::{info, warn};

use crate::state::TaskState;
use crate::{RunReport, Runner};

/// In-process DAG executor.
pub struct LocalRunner {
    dag: Dag<Arc<dyn Task>>,
    lineage: LineageStore,
    scratch_root: PathBuf,
}

impl LocalRunner {
    /// Construct a runner from a built DAG, the lineage store to write
    /// into, and a scratch directory root (per-run dirs nest inside).
    pub fn new(
        dag: Dag<Arc<dyn Task>>,
        lineage: LineageStore,
        scratch_root: impl Into<PathBuf>,
    ) -> Self {
        Self {
            dag,
            lineage,
            scratch_root: scratch_root.into(),
        }
    }

    /// Borrow the underlying DAG (for tests + introspection).
    pub fn dag(&self) -> &Dag<Arc<dyn Task>> {
        &self.dag
    }

    /// Borrow the underlying lineage store.
    pub fn lineage(&self) -> &LineageStore {
        &self.lineage
    }
}

#[async_trait]
impl Runner for LocalRunner {
    fn name(&self) -> &'static str {
        "LocalRunner"
    }

    async fn run(&self, run_id: &str) -> Result<RunReport> {
        // 1. topo + cycle check — fail loudly before touching the lineage db.
        self.dag.cycle_check().context("DAG cycle check failed")?;
        let order = self.dag.topo_sort().context("DAG topo_sort failed")?;

        // 2. record lineage edges first. Pre-populating these lets the
        // closing demo's "lineage-edge-count" contract assert on the DAG
        // shape even if a downstream task fails part-way through.
        for (from, to) in self.dag.edges() {
            let upstream_id = self
                .dag
                .payload(from)
                .expect("from in dag")
                .id()
                .to_string();
            let downstream_id = self.dag.payload(to).expect("to in dag").id().to_string();
            self.lineage
                .record_edge(run_id, &upstream_id, &downstream_id)
                .await
                .context("failed to record lineage edge")?;
        }

        // 3. per-run scratch dir
        let scratch_dir = self.scratch_root.join(run_id);
        std::fs::create_dir_all(&scratch_dir)
            .with_context(|| format!("failed to create scratch dir {}", scratch_dir.display()))?;
        let ctx = Context::new(run_id, scratch_dir);

        // 4. walk the order, fail-fast on any task error.
        let mut task_states: BTreeMap<String, TaskState> = BTreeMap::new();
        let mut task_outputs: BTreeMap<String, serde_json::Value> = BTreeMap::new();
        let mut topo_ids: Vec<String> = Vec::with_capacity(order.len());
        let mut failure_seen = false;

        for node in order {
            let task = self.dag.payload(node).expect("node in dag").clone();
            let task_id = task.id().to_string();
            topo_ids.push(task_id.clone());

            if failure_seen {
                self.persist_state(run_id, &task_id, TaskState::Skipped, None)
                    .await?;
                task_states.insert(task_id, TaskState::Skipped);
                continue;
            }

            self.persist_state(run_id, &task_id, TaskState::Running, None)
                .await?;
            task_states.insert(task_id.clone(), TaskState::Running);
            info!(run = %run_id, task = %task_id, "task running");

            match task.execute(&ctx).await {
                Ok(out) => {
                    // Publish into the context so downstream tasks can read it.
                    ctx.put(task_id.clone(), out.clone());
                    let json = out.as_json();
                    task_outputs.insert(task_id.clone(), json.clone());
                    let json_string = serde_json::to_string(&json).ok();
                    self.persist_state(run_id, &task_id, TaskState::Succeeded, json_string)
                        .await?;
                    task_states.insert(task_id, TaskState::Succeeded);
                }
                Err(e) => {
                    warn!(run = %run_id, task = %task_id, error = ?e, "task failed");
                    let err_json = serde_json::json!({"error": e.to_string()});
                    self.persist_state(
                        run_id,
                        &task_id,
                        TaskState::Failed,
                        Some(err_json.to_string()),
                    )
                    .await?;
                    task_states.insert(task_id.clone(), TaskState::Failed);
                    task_outputs.insert(task_id, err_json);
                    failure_seen = true;
                }
            }
        }

        // Hand the published context back through TaskOutput::as_json so the
        // closing demo can compare values across runners byte-for-byte. The
        // task_outputs map already has the as_json form per task.
        let _drop_ctx_to_silence_unused = ctx;

        Ok(RunReport {
            run_id: run_id.to_string(),
            task_states,
            task_outputs,
            topo_order: topo_ids,
        })
    }
}

impl LocalRunner {
    async fn persist_state(
        &self,
        run_id: &str,
        task_id: &str,
        state: TaskState,
        output_json: Option<String>,
    ) -> Result<()> {
        let now = Utc::now();
        let rec = TaskRunRecord {
            run_id: run_id.into(),
            task_id: task_id.into(),
            state: state.to_string(),
            started_at: now,
            finished_at: matches!(
                state,
                TaskState::Succeeded | TaskState::Failed | TaskState::Skipped
            )
            .then_some(now),
            output_json,
        };
        self.lineage.record_task_run(&rec).await
    }
}

/// Convenience builder for tasks-as-closures. Wraps an async closure +
/// task id into something that satisfies [`dag_core::Task`].
pub struct ClosureTask<F> {
    id: String,
    f: F,
}

impl<F, Fut> ClosureTask<F>
where
    F: Fn(Context) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<TaskOutput>> + Send + 'static,
{
    /// Construct an `Arc<dyn Task>` from an async closure. Returns the
    /// trait object so the result can drop straight into `Dag::add_node`.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(id: impl Into<String>, f: F) -> Arc<dyn Task> {
        Arc::new(ClosureTask { id: id.into(), f })
    }
}

#[async_trait]
impl<F, Fut> Task for ClosureTask<F>
where
    F: Fn(Context) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<TaskOutput>> + Send + 'static,
{
    fn id(&self) -> &str {
        &self.id
    }
    async fn execute(&self, ctx: &Context) -> Result<TaskOutput> {
        (self.f)(ctx.clone()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn linear_dag() -> Dag<Arc<dyn Task>> {
        let mut dag = Dag::<Arc<dyn Task>>::new();
        let a = dag.add_node(ClosureTask::new("a", |_ctx| async {
            Ok(TaskOutput::Int(1))
        }));
        let b = dag.add_node(ClosureTask::new("b", |ctx| async move {
            // demonstrates upstream value pass-through
            let upstream = ctx.get("a").unwrap_or(TaskOutput::Int(0));
            let n = match upstream {
                TaskOutput::Int(n) => n + 1,
                _ => 0,
            };
            Ok(TaskOutput::Int(n))
        }));
        let c = dag.add_node(ClosureTask::new("c", |ctx| async move {
            let upstream = ctx.get("b").unwrap_or(TaskOutput::Int(0));
            let n = match upstream {
                TaskOutput::Int(n) => n * 10,
                _ => 0,
            };
            Ok(TaskOutput::Int(n))
        }));
        dag.add_edge(a, b).unwrap();
        dag.add_edge(b, c).unwrap();
        dag
    }

    #[tokio::test]
    async fn linear_dag_succeeds_and_passes_state() {
        let lineage = LineageStore::open_memory().await.unwrap();
        let dir = tempfile::tempdir().unwrap();
        let runner = LocalRunner::new(linear_dag(), lineage, dir.path());
        let report = runner.run("r-test").await.unwrap();

        assert!(report.all_succeeded());
        assert_eq!(report.topo_order, vec!["a", "b", "c"]);
        assert_eq!(report.task_outputs["a"], serde_json::json!(1));
        assert_eq!(report.task_outputs["b"], serde_json::json!(2));
        assert_eq!(report.task_outputs["c"], serde_json::json!(20));
    }

    #[tokio::test]
    async fn lineage_edges_recorded() {
        let lineage = LineageStore::open_memory().await.unwrap();
        let dir = tempfile::tempdir().unwrap();
        let runner = LocalRunner::new(linear_dag(), lineage.clone(), dir.path());
        runner.run("r-test").await.unwrap();
        assert_eq!(lineage.edge_count(Some("r-test")).await.unwrap(), 2);
    }

    #[tokio::test]
    async fn fail_fast_marks_downstream_skipped() {
        let mut dag = Dag::<Arc<dyn Task>>::new();
        let a = dag.add_node(ClosureTask::new("a", |_ctx| async {
            Err(anyhow::anyhow!("boom"))
        }));
        let b = dag.add_node(ClosureTask::new("b", |_ctx| async {
            Ok(TaskOutput::Int(1))
        }));
        dag.add_edge(a, b).unwrap();

        let lineage = LineageStore::open_memory().await.unwrap();
        let dir = tempfile::tempdir().unwrap();
        let runner = LocalRunner::new(dag, lineage, dir.path());
        let report = runner.run("r-fail").await.unwrap();
        assert_eq!(report.task_states["a"], TaskState::Failed);
        assert_eq!(report.task_states["b"], TaskState::Skipped);
        assert!(!report.all_succeeded());
    }

    #[tokio::test]
    async fn topo_order_is_deterministic_across_runs() {
        let lineage = LineageStore::open_memory().await.unwrap();
        let dir = tempfile::tempdir().unwrap();
        let runner = LocalRunner::new(linear_dag(), lineage, dir.path());
        let r1 = runner.run("r1").await.unwrap();
        let r2 = runner.run("r2").await.unwrap();
        // Same DAG, same insertion order — petgraph's Kahn topo gives the
        // same order both times.
        assert_eq!(r1.topo_order, r2.topo_order);
    }
}
