//! `dag-core` — generic DAG type, `Task` trait, topological sort, cycle detection.
//!
//! This is the model crate for the Workflow Orchestration with Rust course
//! (Coursera Rust for Data Engineering, c16). Everything else in the workspace
//! (`dag-scheduler`, `dag-runner`, `dag-lineage`, `dag-cli`) consumes the
//! `Dag<T>` type defined here.
//!
//! The DAG is generic over the task payload `T`, so the same primitive can
//! carry a `Box<dyn Task>` for the in-process `LocalRunner` or a forjar
//! resource handle for the `ForjarRunner` — both flow through the same
//! `topo_sort` and `cycle_check` paths.
//!
//! # Example
//!
//! ```
//! use dag_core::Dag;
//!
//! let mut dag = Dag::<&str>::new();
//! let extract = dag.add_node("extract");
//! let transform = dag.add_node("transform");
//! let load = dag.add_node("load");
//!
//! dag.add_edge(extract, transform).unwrap();
//! dag.add_edge(transform, load).unwrap();
//! dag.cycle_check().unwrap();
//!
//! let order = dag.topo_sort().unwrap();
//! let names: Vec<&&str> = dag.payloads(&order);
//! assert_eq!(names, vec![&"extract", &"transform", &"load"]);
//! ```

pub mod task;

use std::collections::HashMap;

use petgraph::algo::{is_cyclic_directed, toposort};
use petgraph::graph::{DiGraph, NodeIndex};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub use task::{Context, Task, TaskOutput};

/// Errors emitted by [`Dag`] mutations and queries.
#[derive(Debug, Error)]
pub enum DagError {
    /// `add_edge` was called with a node that does not belong to the DAG.
    #[error("unknown node: {0:?}")]
    UnknownNode(NodeIndex),

    /// `topo_sort` or `cycle_check` failed because the graph contains a cycle.
    /// The offending node is reported for diagnostics.
    #[error("DAG contains a cycle through node {0:?}")]
    Cycle(NodeIndex),

    /// `add_edge` would have produced a duplicate edge between the same two
    /// nodes. We forbid this so the topology stays a simple DAG (multigraph
    /// edges add nothing to the schedule but bloat the lineage store).
    #[error("duplicate edge {from:?} -> {to:?}")]
    DuplicateEdge { from: NodeIndex, to: NodeIndex },
}

/// A directed acyclic graph generic over the task payload `T`.
///
/// Built on `petgraph::DiGraph<T, ()>`. The crate intentionally does not
/// expose petgraph types in its public API beyond `NodeIndex` so consumers
/// can treat the DAG as an opaque value.
#[derive(Debug, Clone)]
pub struct Dag<T> {
    graph: DiGraph<T, ()>,
}

impl<T> Default for Dag<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Dag<T> {
    /// Create an empty DAG.
    pub fn new() -> Self {
        Self {
            graph: DiGraph::new(),
        }
    }

    /// Add a task payload as a new node and return its handle.
    pub fn add_node(&mut self, payload: T) -> NodeIndex {
        self.graph.add_node(payload)
    }

    /// Add a directed edge `from -> to`. Errors if either endpoint is
    /// unknown or the edge would duplicate an existing one.
    pub fn add_edge(&mut self, from: NodeIndex, to: NodeIndex) -> Result<(), DagError> {
        if !self.graph.node_indices().any(|n| n == from) {
            return Err(DagError::UnknownNode(from));
        }
        if !self.graph.node_indices().any(|n| n == to) {
            return Err(DagError::UnknownNode(to));
        }
        if self.graph.find_edge(from, to).is_some() {
            return Err(DagError::DuplicateEdge { from, to });
        }
        self.graph.add_edge(from, to, ());
        Ok(())
    }

    /// Return a topological order of the DAG, or [`DagError::Cycle`].
    ///
    /// Petgraph's `toposort` is Kahn's algorithm; ties broken by
    /// node-insertion order, which gives us deterministic output across
    /// runs as long as `add_node` is called in the same order.
    pub fn topo_sort(&self) -> Result<Vec<NodeIndex>, DagError> {
        toposort(&self.graph, None).map_err(|cycle| DagError::Cycle(cycle.node_id()))
    }

    /// Cheaper guard: just answer "is this thing acyclic?" without
    /// allocating the full sort.
    pub fn cycle_check(&self) -> Result<(), DagError> {
        if is_cyclic_directed(&self.graph) {
            // Re-run toposort just to grab the offending node for the error.
            self.topo_sort().map(|_| ())
        } else {
            Ok(())
        }
    }

    /// Project an ordered list of node handles down to their payloads (by
    /// reference). Convenience for tests + downstream display code.
    pub fn payloads(&self, order: &[NodeIndex]) -> Vec<&T> {
        order.iter().map(|n| &self.graph[*n]).collect()
    }

    /// Lookup a single payload by handle.
    pub fn payload(&self, node: NodeIndex) -> Option<&T> {
        self.graph.node_weight(node)
    }

    /// Number of nodes in the DAG.
    pub fn node_count(&self) -> usize {
        self.graph.node_count()
    }

    /// Number of edges in the DAG.
    pub fn edge_count(&self) -> usize {
        self.graph.edge_count()
    }

    /// Iterate over every (from, to) edge in insertion order. Used by the
    /// runner crates to record lineage edges as the DAG walks.
    pub fn edges(&self) -> impl Iterator<Item = (NodeIndex, NodeIndex)> + '_ {
        self.graph
            .edge_indices()
            .filter_map(|e| self.graph.edge_endpoints(e))
    }

    /// Return the upstream (parent) nodes of `node`.
    pub fn parents(&self, node: NodeIndex) -> Vec<NodeIndex> {
        self.graph
            .neighbors_directed(node, petgraph::Direction::Incoming)
            .collect()
    }

    /// Return the downstream (child) nodes of `node`.
    pub fn children(&self, node: NodeIndex) -> Vec<NodeIndex> {
        self.graph
            .neighbors_directed(node, petgraph::Direction::Outgoing)
            .collect()
    }

    /// True iff every node has at least one in- or out-edge OR is the only
    /// node in the DAG. Catches "orphan task" mistakes.
    pub fn every_task_reachable(&self) -> bool {
        if self.graph.node_count() <= 1 {
            return true;
        }
        self.graph.node_indices().all(|n| {
            self.parents(n)
                .is_empty()
                .not_or(|| !self.children(n).is_empty())
        })
    }
}

/// Internal helper used inside `every_task_reachable` to keep the call site
/// readable without resorting to `||`.
trait BoolExt {
    fn not_or(self, other: impl FnOnce() -> bool) -> bool;
}

impl BoolExt for bool {
    fn not_or(self, other: impl FnOnce() -> bool) -> bool {
        // The intent reads: "if I am false, fall back to `other`".
        // i.e. `(!self) || other()` written as a method for clarity.
        !self || other()
    }
}

/// Lightweight on-disk representation of a DAG used by `dag-cli validate`
/// and the example runner. Each `TaskSpec` becomes one node; `depends_on`
/// becomes the inbound edges.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DagSpec {
    /// Human-readable name (used in lineage + tracing spans).
    pub name: String,
    /// Optional description that flows into the rendered Mermaid lineage.
    #[serde(default)]
    pub description: String,
    /// Tasks, in any order — the spec resolver builds the topology from
    /// the `depends_on` field on each task.
    pub tasks: Vec<TaskSpec>,
}

/// One task entry in a [`DagSpec`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskSpec {
    pub id: String,
    #[serde(default)]
    pub depends_on: Vec<String>,
}

impl DagSpec {
    /// Parse a YAML DAG specification.
    pub fn from_yaml(s: &str) -> anyhow::Result<Self> {
        Ok(serde_yaml::from_str(s)?)
    }

    /// Build a [`Dag<String>`] from this spec (string payload = task id).
    /// Errors on duplicate task ids, dangling `depends_on` references,
    /// and cycles.
    pub fn build(&self) -> anyhow::Result<Dag<String>> {
        let mut dag = Dag::<String>::new();
        let mut indices: HashMap<String, NodeIndex> = HashMap::new();
        for task in &self.tasks {
            if indices.contains_key(&task.id) {
                anyhow::bail!("duplicate task id: {}", task.id);
            }
            let idx = dag.add_node(task.id.clone());
            indices.insert(task.id.clone(), idx);
        }
        for task in &self.tasks {
            let to = indices[&task.id];
            for dep in &task.depends_on {
                let from = *indices.get(dep).ok_or_else(|| {
                    anyhow::anyhow!("task '{}' depends on unknown '{}'", task.id, dep)
                })?;
                dag.add_edge(from, to)?;
            }
        }
        dag.cycle_check()?;
        Ok(dag)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn linear_chain() -> Dag<&'static str> {
        let mut dag = Dag::new();
        let a = dag.add_node("extract");
        let b = dag.add_node("transform");
        let c = dag.add_node("load");
        dag.add_edge(a, b).unwrap();
        dag.add_edge(b, c).unwrap();
        dag
    }

    #[test]
    fn topo_sort_linear() {
        let dag = linear_chain();
        let order = dag.topo_sort().unwrap();
        let names: Vec<&&str> = dag.payloads(&order);
        assert_eq!(names, vec![&"extract", &"transform", &"load"]);
    }

    #[test]
    fn topo_sort_branch_then_join() {
        let mut dag = Dag::<&str>::new();
        let root = dag.add_node("root");
        let a = dag.add_node("a");
        let b = dag.add_node("b");
        let join = dag.add_node("join");
        dag.add_edge(root, a).unwrap();
        dag.add_edge(root, b).unwrap();
        dag.add_edge(a, join).unwrap();
        dag.add_edge(b, join).unwrap();

        let order = dag.topo_sort().unwrap();
        let names: Vec<&&str> = dag.payloads(&order);
        let pos = |n: &str| names.iter().position(|s| **s == n).unwrap();
        assert!(pos("root") < pos("a"));
        assert!(pos("root") < pos("b"));
        assert!(pos("a") < pos("join"));
        assert!(pos("b") < pos("join"));
    }

    #[test]
    fn cycle_detection() {
        let mut dag = Dag::<&str>::new();
        let a = dag.add_node("a");
        let b = dag.add_node("b");
        dag.add_edge(a, b).unwrap();
        dag.add_edge(b, a).unwrap(); // closes the loop

        assert!(matches!(dag.cycle_check(), Err(DagError::Cycle(_))));
        assert!(matches!(dag.topo_sort(), Err(DagError::Cycle(_))));
    }

    #[test]
    fn duplicate_edge_rejected() {
        let mut dag = Dag::<&str>::new();
        let a = dag.add_node("a");
        let b = dag.add_node("b");
        dag.add_edge(a, b).unwrap();
        let err = dag.add_edge(a, b).unwrap_err();
        assert!(matches!(err, DagError::DuplicateEdge { .. }));
    }

    #[test]
    fn topo_sort_is_deterministic() {
        let first = linear_chain().topo_sort().unwrap();
        for _ in 0..10 {
            let next = linear_chain().topo_sort().unwrap();
            assert_eq!(first, next, "topo_sort must be deterministic");
        }
    }

    #[test]
    fn parents_and_children() {
        let mut dag = Dag::<&str>::new();
        let a = dag.add_node("a");
        let b = dag.add_node("b");
        let c = dag.add_node("c");
        dag.add_edge(a, b).unwrap();
        dag.add_edge(b, c).unwrap();

        assert_eq!(dag.parents(b), vec![a]);
        assert_eq!(dag.children(b), vec![c]);
    }

    #[test]
    fn dag_spec_yaml_roundtrip() {
        let yaml = r#"
name: etl
description: tiny ETL
tasks:
  - id: extract
  - id: transform
    depends_on: [extract]
  - id: load
    depends_on: [transform]
"#;
        let spec = DagSpec::from_yaml(yaml).unwrap();
        assert_eq!(spec.tasks.len(), 3);
        let dag = spec.build().unwrap();
        let order = dag.topo_sort().unwrap();
        let names: Vec<String> = dag.payloads(&order).into_iter().cloned().collect();
        assert_eq!(names, vec!["extract", "transform", "load"]);
    }

    #[test]
    fn dag_spec_dangling_dep_errors() {
        let yaml = r#"
name: bad
tasks:
  - id: a
    depends_on: [ghost]
"#;
        let spec = DagSpec::from_yaml(yaml).unwrap();
        let err = spec.build().unwrap_err();
        assert!(err.to_string().contains("ghost"));
    }

    #[test]
    fn edge_count_tracks_inserts() {
        let dag = linear_chain();
        assert_eq!(dag.node_count(), 3);
        assert_eq!(dag.edge_count(), 2);
    }
}
