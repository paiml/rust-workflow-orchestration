//! `Task` trait + execution context.
//!
//! Concrete tasks live in user code (and in the `etl_pipeline_dag` example
//! under `dag-cli/examples/`). Runners (`LocalRunner`, `ForjarRunner`) call
//! `Task::execute` once per node in topological order.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

/// Per-run, shared state passed to every task. Carries the run id, the
/// per-run scratch directory, and a small typed bag for upstream→downstream
/// values (the strongly-typed Airflow XCom replacement the M3 lessons
/// describe).
#[derive(Debug, Clone)]
pub struct Context {
    /// Stable id of this DAG run. Tasks should use this for log correlation.
    pub run_id: String,
    /// Per-run scratch directory. Tasks may write intermediate files here;
    /// the runner is free to delete it after the DAG completes.
    pub scratch_dir: std::path::PathBuf,
    /// Inter-task value passing. `Arc<Mutex<_>>` lets the runner snapshot
    /// the state mid-DAG for the lineage store and lets concurrent
    /// downstream tasks read upstream output safely.
    pub state: Arc<Mutex<HashMap<String, TaskOutput>>>,
}

impl Context {
    /// Construct a context with a fresh state bag.
    pub fn new(run_id: impl Into<String>, scratch_dir: std::path::PathBuf) -> Self {
        Self {
            run_id: run_id.into(),
            scratch_dir,
            state: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Read an upstream task's output. Returns `None` if no task has
    /// published a value under `key`.
    pub fn get(&self, key: &str) -> Option<TaskOutput> {
        self.state
            .lock()
            .expect("context state poisoned")
            .get(key)
            .cloned()
    }

    /// Publish this task's output under `key` for downstream tasks.
    pub fn put(&self, key: impl Into<String>, value: TaskOutput) {
        self.state
            .lock()
            .expect("context state poisoned")
            .insert(key.into(), value);
    }
}

/// Strongly-typed task output. Tasks return one of these from `execute`.
/// `Json` is the workhorse — the closing demo passes `serde_json::Value`
/// payloads between extract → transform → validate → load → notify.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", content = "value")]
pub enum TaskOutput {
    /// No-value tag. Used when a task is purely effectful.
    Unit,
    /// Single integer. Common for `validate` style tasks that return a row count.
    Int(i64),
    /// String — file paths, identifiers, single-line summaries.
    Text(String),
    /// Nested JSON. Used by `extract`/`transform` to hand structured data
    /// down the chain.
    Json(serde_json::Value),
}

impl TaskOutput {
    /// Convenience: return the JSON payload, treating `Unit`/`Int`/`Text`
    /// as their JSON-equivalent values. Used by the demo to compare two
    /// runners' outputs by canonical JSON shape.
    pub fn as_json(&self) -> serde_json::Value {
        match self {
            TaskOutput::Unit => serde_json::Value::Null,
            TaskOutput::Int(n) => serde_json::json!(n),
            TaskOutput::Text(s) => serde_json::json!(s),
            TaskOutput::Json(v) => v.clone(),
        }
    }
}

/// One unit of work in a DAG. The trait is `async_trait` because tokio
/// timers, sqlx queries, and HTTP fetches all return futures.
#[async_trait]
pub trait Task: Send + Sync {
    /// Stable identifier. Used for the lineage store key, the Mermaid
    /// label, and the tracing span.
    fn id(&self) -> &str;

    /// Execute the task. Implementations should:
    ///   - read upstream values via `ctx.get(...)`
    ///   - perform their work
    ///   - publish a typed output via `ctx.put(self.id(), out.clone())` or
    ///     by returning the output (the runner publishes for them)
    async fn execute(&self, ctx: &Context) -> anyhow::Result<TaskOutput>;
}

#[cfg(test)]
mod tests {
    use super::*;

    struct EchoTask {
        id: &'static str,
        value: i64,
    }

    #[async_trait]
    impl Task for EchoTask {
        fn id(&self) -> &str {
            self.id
        }
        async fn execute(&self, _ctx: &Context) -> anyhow::Result<TaskOutput> {
            Ok(TaskOutput::Int(self.value))
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn echo_task_runs() {
        let ctx = Context::new("run-1", std::env::temp_dir());
        let task = EchoTask {
            id: "echo",
            value: 42,
        };
        let out = task.execute(&ctx).await.unwrap();
        assert_eq!(out, TaskOutput::Int(42));
    }

    #[test]
    fn context_state_roundtrip() {
        let ctx = Context::new("run-1", std::env::temp_dir());
        ctx.put("k", TaskOutput::Int(7));
        assert_eq!(ctx.get("k"), Some(TaskOutput::Int(7)));
        assert_eq!(ctx.get("missing"), None);
    }

    #[test]
    fn task_output_as_json_normalizes() {
        assert_eq!(TaskOutput::Unit.as_json(), serde_json::Value::Null);
        assert_eq!(TaskOutput::Int(5).as_json(), serde_json::json!(5));
        assert_eq!(
            TaskOutput::Text("ok".into()).as_json(),
            serde_json::json!("ok")
        );
    }
}
