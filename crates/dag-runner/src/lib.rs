//! `dag-runner` — two execution backends for [`dag_core::Dag`] graphs.
//!
//! - [`local::LocalRunner`] is the in-process executor. It walks the topo
//!   order and calls [`dag_core::Task::execute`] on each node, persisting
//!   per-task state in a SQLite [`dag_lineage::LineageStore`].
//! - [`forjar::ForjarRunner`] (gated behind the `forjar` feature) wraps the
//!   `forjar` CLI to execute a DAG defined in `forjar.yaml` format. This
//!   demonstrates that the same DAG topology can be handed to a real
//!   production-grade engine without changing the orchestration layer.
//!
//! Both runners share a common [`Runner`] trait and produce the same
//! [`RunReport`] shape so the closing demo can directly compare outputs and
//! lineage.

use std::collections::BTreeMap;

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

pub mod local;
pub mod state;

#[cfg(feature = "forjar")]
pub mod forjar;

pub use local::LocalRunner;
pub use state::TaskState;

/// Result of a successful DAG run. Both runners produce one of these so the
/// demo can compare them apples-to-apples.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RunReport {
    pub run_id: String,
    /// Final state per task id (`Succeeded`, `Failed`, etc.).
    pub task_states: BTreeMap<String, TaskState>,
    /// Per-task JSON output, normalized through `TaskOutput::as_json` so two
    /// runners with different intermediate shapes can be compared.
    pub task_outputs: BTreeMap<String, serde_json::Value>,
    /// Topological order the runner used. Two runners on the same DAG must
    /// produce a topologically-valid order; identical small linear DAGs
    /// produce byte-identical orders.
    pub topo_order: Vec<String>,
}

impl RunReport {
    /// True iff every task is in the `Succeeded` state. The closing demo
    /// asserts this.
    pub fn all_succeeded(&self) -> bool {
        !self.task_states.is_empty()
            && self
                .task_states
                .values()
                .all(|s| matches!(s, TaskState::Succeeded))
    }

    /// Return only the JSON payload portion, in topo order. Used by the
    /// demo's runner-output-equivalence contract.
    pub fn outputs_in_order(&self) -> Vec<&serde_json::Value> {
        self.topo_order
            .iter()
            .filter_map(|id| self.task_outputs.get(id))
            .collect()
    }
}

/// Common runner surface. Concrete runners are created independently
/// (because their constructor inputs differ — `LocalRunner` takes a
/// `Vec<Box<dyn Task>>`, `ForjarRunner` takes a path to `forjar.yaml`),
/// but they all expose `run` returning a [`RunReport`].
#[async_trait]
pub trait Runner: Send + Sync {
    /// Execute the DAG end-to-end. The `run_id` correlates lineage rows
    /// across both task records and edges.
    async fn run(&self, run_id: &str) -> Result<RunReport>;

    /// Stable, human-readable name. The demo uses this in log lines and to
    /// disambiguate the two reports it compares.
    fn name(&self) -> &'static str;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_report_all_succeeded_works() {
        let mut r = RunReport {
            run_id: "r1".into(),
            task_states: BTreeMap::new(),
            task_outputs: BTreeMap::new(),
            topo_order: vec!["a".into(), "b".into()],
        };
        r.task_states.insert("a".into(), TaskState::Succeeded);
        r.task_states.insert("b".into(), TaskState::Succeeded);
        assert!(r.all_succeeded());

        r.task_states.insert("b".into(), TaskState::Failed);
        assert!(!r.all_succeeded());
    }

    #[test]
    fn outputs_in_order_uses_topo() {
        let mut r = RunReport {
            run_id: "r1".into(),
            task_states: BTreeMap::new(),
            task_outputs: BTreeMap::new(),
            topo_order: vec!["a".into(), "b".into(), "c".into()],
        };
        r.task_outputs.insert("a".into(), serde_json::json!(1));
        r.task_outputs.insert("b".into(), serde_json::json!(2));
        r.task_outputs.insert("c".into(), serde_json::json!(3));
        let v = r.outputs_in_order();
        assert_eq!(v.len(), 3);
        assert_eq!(v[0], &serde_json::json!(1));
        assert_eq!(v[2], &serde_json::json!(3));
    }
}
