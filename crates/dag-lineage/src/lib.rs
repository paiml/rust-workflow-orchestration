//! `dag-lineage` — sqlx + SQLite store for DAG run lineage.
//!
//! Records one row per task execution and one row per upstream → downstream
//! edge as the runner walks the DAG. The M4 lessons of the Workflow
//! Orchestration with Rust course rely on this crate to demonstrate how
//! lineage queries work without an external service like OpenLineage or
//! Marquez — everything lives in a single SQLite file.
//!
//! Mermaid rendering of the lineage subgraph (`render_mermaid`) gives the
//! course README + lesson decks a deterministic visual to talk over.

use std::collections::{BTreeSet, HashMap};
use std::path::Path;

use anyhow::{Context as _, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Pool, Row, Sqlite};
use thiserror::Error;
use tracing::debug;

/// Errors emitted by the lineage store.
#[derive(Debug, Error)]
pub enum LineageError {
    /// The store was queried for a task that has never been recorded.
    #[error("unknown task id: {0}")]
    UnknownTask(String),
}

/// Newtype wrapper around an sqlx SQLite connection pool. Keeps the lineage
/// API independent of the underlying driver.
#[derive(Debug, Clone)]
pub struct LineageStore {
    pool: Pool<Sqlite>,
}

/// One recorded task execution. Mirrors the `task_runs` table.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskRunRecord {
    pub run_id: String,
    pub task_id: String,
    pub state: String,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub output_json: Option<String>,
}

/// One recorded edge from upstream → downstream task. Mirrors the
/// `lineage_edges` table.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LineageEdge {
    pub run_id: String,
    pub upstream_id: String,
    pub downstream_id: String,
    pub recorded_at: DateTime<Utc>,
}

impl LineageStore {
    /// Open (or create) a SQLite-backed lineage store at `path`. The
    /// `?mode=rwc` lets us bring up a fresh file in one call.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let p = path.as_ref();
        // Touch the file so SqliteConnectOptions does not panic on absent
        // parents. Mirrors the `init` Make target's behavior.
        if let Some(parent) = p.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("failed to create parent dir for {}", p.display()))?;
            }
        }
        let opts = SqliteConnectOptions::new()
            .filename(p)
            .create_if_missing(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(opts)
            .await
            .with_context(|| format!("failed to open SQLite at {}", p.display()))?;
        let store = Self { pool };
        store.init_schema().await?;
        Ok(store)
    }

    /// Open an in-memory store. Intended for tests + the example program
    /// when a clean lineage baseline is needed.
    pub async fn open_memory() -> Result<Self> {
        let opts = SqliteConnectOptions::new()
            .in_memory(true)
            .create_if_missing(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1) // in-memory DB must be one pool
            .connect_with(opts)
            .await?;
        let store = Self { pool };
        store.init_schema().await?;
        Ok(store)
    }

    /// Idempotent CREATE TABLE migration. Called from `open` /
    /// `open_memory`; safe to invoke explicitly.
    pub async fn init_schema(&self) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS task_runs (
                run_id        TEXT NOT NULL,
                task_id       TEXT NOT NULL,
                state         TEXT NOT NULL,
                started_at    TEXT NOT NULL,
                finished_at   TEXT,
                output_json   TEXT,
                PRIMARY KEY (run_id, task_id)
            )
            "#,
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS lineage_edges (
                run_id        TEXT NOT NULL,
                upstream_id   TEXT NOT NULL,
                downstream_id TEXT NOT NULL,
                recorded_at   TEXT NOT NULL,
                PRIMARY KEY (run_id, upstream_id, downstream_id)
            )
            "#,
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Insert (or update) a task run.
    pub async fn record_task_run(&self, rec: &TaskRunRecord) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO task_runs (run_id, task_id, state, started_at, finished_at, output_json)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id, task_id) DO UPDATE SET
                state = excluded.state,
                finished_at = excluded.finished_at,
                output_json = excluded.output_json
            "#,
        )
        .bind(&rec.run_id)
        .bind(&rec.task_id)
        .bind(&rec.state)
        .bind(rec.started_at.to_rfc3339())
        .bind(rec.finished_at.map(|t| t.to_rfc3339()))
        .bind(&rec.output_json)
        .execute(&self.pool)
        .await?;
        debug!(run_id = %rec.run_id, task = %rec.task_id, "recorded task run");
        Ok(())
    }

    /// Insert one upstream → downstream edge for this run. The PK on
    /// (run_id, upstream_id, downstream_id) makes the operation idempotent.
    pub async fn record_edge(
        &self,
        run_id: &str,
        upstream_id: &str,
        downstream_id: &str,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT OR IGNORE INTO lineage_edges (run_id, upstream_id, downstream_id, recorded_at)
            VALUES (?, ?, ?, ?)
            "#,
        )
        .bind(run_id)
        .bind(upstream_id)
        .bind(downstream_id)
        .bind(Utc::now().to_rfc3339())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Number of distinct edges recorded across all runs (or the latest run
    /// when `run_id` is set). Used by the demo's lineage-edge contract.
    pub async fn edge_count(&self, run_id: Option<&str>) -> Result<i64> {
        let row = if let Some(rid) = run_id {
            sqlx::query("SELECT COUNT(*) AS n FROM lineage_edges WHERE run_id = ?")
                .bind(rid)
                .fetch_one(&self.pool)
                .await?
        } else {
            sqlx::query("SELECT COUNT(*) AS n FROM lineage_edges")
                .fetch_one(&self.pool)
                .await?
        };
        Ok(row.try_get::<i64, _>("n")?)
    }

    /// Direct upstream task ids of `task_id` for `run_id`.
    pub async fn query_upstream(&self, run_id: &str, task_id: &str) -> Result<Vec<String>> {
        let rows = sqlx::query(
            "SELECT upstream_id FROM lineage_edges WHERE run_id = ? AND downstream_id = ? \
             ORDER BY upstream_id",
        )
        .bind(run_id)
        .bind(task_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| r.try_get::<String, _>("upstream_id").unwrap_or_default())
            .collect())
    }

    /// Direct downstream task ids of `task_id` for `run_id`.
    pub async fn query_downstream(&self, run_id: &str, task_id: &str) -> Result<Vec<String>> {
        let rows = sqlx::query(
            "SELECT downstream_id FROM lineage_edges WHERE run_id = ? AND upstream_id = ? \
             ORDER BY downstream_id",
        )
        .bind(run_id)
        .bind(task_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| r.try_get::<String, _>("downstream_id").unwrap_or_default())
            .collect())
    }

    /// Return all edges for a run (sorted alphabetically for deterministic
    /// snapshot output). Used by the demo + the cli `lineage` subcommand.
    pub async fn edges_for_run(&self, run_id: &str) -> Result<Vec<LineageEdge>> {
        let rows = sqlx::query(
            "SELECT run_id, upstream_id, downstream_id, recorded_at \
             FROM lineage_edges WHERE run_id = ? ORDER BY upstream_id, downstream_id",
        )
        .bind(run_id)
        .fetch_all(&self.pool)
        .await?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let recorded_at_s: String = r.try_get("recorded_at")?;
            let recorded_at = DateTime::parse_from_rfc3339(&recorded_at_s)?.with_timezone(&Utc);
            out.push(LineageEdge {
                run_id: r.try_get("run_id")?,
                upstream_id: r.try_get("upstream_id")?,
                downstream_id: r.try_get("downstream_id")?,
                recorded_at,
            });
        }
        Ok(out)
    }

    /// Return all task-run records for a run (sorted by task id).
    pub async fn task_runs_for_run(&self, run_id: &str) -> Result<Vec<TaskRunRecord>> {
        let rows = sqlx::query(
            "SELECT run_id, task_id, state, started_at, finished_at, output_json \
             FROM task_runs WHERE run_id = ? ORDER BY task_id",
        )
        .bind(run_id)
        .fetch_all(&self.pool)
        .await?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let started_s: String = r.try_get("started_at")?;
            let finished_s: Option<String> = r.try_get("finished_at")?;
            out.push(TaskRunRecord {
                run_id: r.try_get("run_id")?,
                task_id: r.try_get("task_id")?,
                state: r.try_get("state")?,
                started_at: DateTime::parse_from_rfc3339(&started_s)?.with_timezone(&Utc),
                finished_at: finished_s
                    .map(|s| DateTime::parse_from_rfc3339(&s).map(|t| t.with_timezone(&Utc)))
                    .transpose()?,
                output_json: r.try_get("output_json")?,
            });
        }
        Ok(out)
    }

    /// Render the lineage subgraph for `run_id` as a Mermaid `graph TD`
    /// block. Used by the README + the `dag-cli lineage` subcommand.
    pub async fn render_mermaid(&self, run_id: &str) -> Result<String> {
        let edges = self.edges_for_run(run_id).await?;
        let mut nodes: BTreeSet<String> = BTreeSet::new();
        for e in &edges {
            nodes.insert(e.upstream_id.clone());
            nodes.insert(e.downstream_id.clone());
        }
        // Also include task ids that have a run record but no edges, so a
        // single-node DAG still renders something.
        for t in self.task_runs_for_run(run_id).await? {
            nodes.insert(t.task_id);
        }

        let mut node_aliases: HashMap<String, String> = HashMap::new();
        for (i, n) in nodes.iter().enumerate() {
            node_aliases.insert(n.clone(), format!("t{i}"));
        }

        let mut out = String::from("graph TD\n");
        for n in &nodes {
            let alias = &node_aliases[n];
            out.push_str(&format!("    {alias}[\"{n}\"]\n"));
        }
        for e in &edges {
            let from = &node_aliases[&e.upstream_id];
            let to = &node_aliases[&e.downstream_id];
            out.push_str(&format!("    {from} --> {to}\n"));
        }
        Ok(out)
    }

    /// Borrow the underlying pool for clients that want to issue ad-hoc
    /// queries (e.g. the `dag-cli backfill` subcommand).
    pub fn pool(&self) -> &Pool<Sqlite> {
        &self.pool
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn fresh_store() -> LineageStore {
        LineageStore::open_memory().await.unwrap()
    }

    fn rec(run_id: &str, task_id: &str, state: &str) -> TaskRunRecord {
        TaskRunRecord {
            run_id: run_id.into(),
            task_id: task_id.into(),
            state: state.into(),
            started_at: Utc::now(),
            finished_at: Some(Utc::now()),
            output_json: Some("{}".into()),
        }
    }

    #[tokio::test]
    async fn record_and_query_roundtrip() {
        let store = fresh_store().await;
        store.record_task_run(&rec("r1", "extract", "Succeeded")).await.unwrap();
        store.record_task_run(&rec("r1", "transform", "Succeeded")).await.unwrap();
        store.record_edge("r1", "extract", "transform").await.unwrap();

        assert_eq!(store.edge_count(Some("r1")).await.unwrap(), 1);
        assert_eq!(
            store.query_upstream("r1", "transform").await.unwrap(),
            vec!["extract"]
        );
        assert_eq!(
            store.query_downstream("r1", "extract").await.unwrap(),
            vec!["transform"]
        );
    }

    #[tokio::test]
    async fn duplicate_edge_is_idempotent() {
        let store = fresh_store().await;
        store.record_edge("r1", "a", "b").await.unwrap();
        store.record_edge("r1", "a", "b").await.unwrap();
        store.record_edge("r1", "a", "b").await.unwrap();
        assert_eq!(store.edge_count(Some("r1")).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn upsert_overwrites_state() {
        let store = fresh_store().await;
        store.record_task_run(&rec("r1", "extract", "Running")).await.unwrap();
        store.record_task_run(&rec("r1", "extract", "Succeeded")).await.unwrap();
        let runs = store.task_runs_for_run("r1").await.unwrap();
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].state, "Succeeded");
    }

    #[tokio::test]
    async fn mermaid_render_includes_node_and_edge() {
        let store = fresh_store().await;
        store.record_task_run(&rec("r1", "extract", "Succeeded")).await.unwrap();
        store.record_task_run(&rec("r1", "transform", "Succeeded")).await.unwrap();
        store.record_edge("r1", "extract", "transform").await.unwrap();
        let mermaid = store.render_mermaid("r1").await.unwrap();
        assert!(mermaid.starts_with("graph TD"));
        assert!(mermaid.contains("[\"extract\"]"));
        assert!(mermaid.contains("[\"transform\"]"));
        assert!(mermaid.contains("--> "));
    }

    #[tokio::test]
    async fn open_disk_path_creates_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lineage.sqlite");
        let store = LineageStore::open(&path).await.unwrap();
        store.record_edge("r1", "a", "b").await.unwrap();
        assert!(path.exists());
        assert_eq!(store.edge_count(Some("r1")).await.unwrap(), 1);
    }
}
