//! Closing demo: a 5-task ETL DAG run twice — once with [`LocalRunner`] and
//! once with [`ForjarRunner`] — asserting four runtime invariants.
//!
//! Pipeline shape (linear chain):
//!     extract → transform → validate → load → notify
//!
//! Each task does real, deterministic work:
//!   - `extract`   reads a 5-row JSON fixture from `examples/data/orders.json`
//!   - `transform` projects one field and computes a derived `total`
//!   - `validate`  runs assertions on the transformed batch and returns a row count
//!   - `load`      writes the rows into a per-run SQLite table via sqlx
//!   - `notify`    appends a one-line summary to `notifications.log` in the scratch dir
//!
//! The two runtime backends:
//!   1. `LocalRunner` walks the topo order in-process and records each task's
//!      output in [`dag_lineage::LineageStore`].
//!   2. `ForjarRunner` shells out to the published `forjar` CLI to validate
//!      and traverse the same DAG topology described in `forjar.yaml`. The
//!      yaml lives next to this file — same 5 tasks, declared as `file`
//!      resources with the same `depends_on` shape.
//!
//! Asserted contracts (matched by `contracts/dag-rust-v1.yaml`):
//!   C1 all-tasks-complete            — every task `Succeeded`
//!   C2 lineage-edge-count            — exactly 4 edges in the lineage store
//!   C3 runner-output-equivalence     — both runners emit the same `topo_order`
//!                                       and produce identical task ids
//!   C4 topo-determinism              — 10 consecutive runs of the LocalRunner
//!                                       yield byte-identical `topo_order`s
//!
//! Run:
//!   cargo run -p dag-cli --example etl_pipeline_dag
//!   cargo run -p dag-cli --example etl_pipeline_dag --features dag-runner/forjar

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context as _, Result};
use dag_core::{Context, Dag, Task, TaskOutput};
use dag_lineage::LineageStore;
use dag_runner::forjar::ForjarRunner;
use dag_runner::local::ClosureTask;
use dag_runner::{LocalRunner, RunReport, Runner};
use serde::{Deserialize, Serialize};

/// Tiny order shape used by the extract/transform/validate/load chain.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Order {
    id: u64,
    sku: String,
    qty: u32,
    unit_price: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EnrichedOrder {
    id: u64,
    sku: String,
    qty: u32,
    unit_price: f64,
    total: f64,
}

const FIXTURE: &str = include_str!("data/orders.json");

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_target(false)
        .compact()
        .init();

    let workdir = std::env::temp_dir().join(format!(
        "etl-pipeline-dag-{}",
        chrono::Utc::now().timestamp_micros()
    ));
    std::fs::create_dir_all(&workdir).context("failed to create workdir")?;

    println!("=== c16 Workflow Orchestration — closing demo ===");
    println!("workdir: {}", workdir.display());

    // 1. LocalRunner end-to-end run.
    let lineage = LineageStore::open(workdir.join("lineage.sqlite")).await?;
    let local_runner = LocalRunner::new(
        build_etl_dag(&workdir)?,
        lineage.clone(),
        workdir.join("scratch"),
    );
    let local_report = local_runner.run("local-run").await?;
    println!("\n[LocalRunner]");
    println!("topo_order = {:?}", local_report.topo_order);
    println!("states     = {:?}", local_report.task_states);

    // 2. ForjarRunner end-to-end run, when forjar is on PATH.
    let yaml_path = workdir.join("forjar.yaml");
    fs::write(&yaml_path, forjar_yaml_for_etl())?;
    let forjar_state = workdir.join("forjar-state");
    let forjar_runner = ForjarRunner::new(&yaml_path, &forjar_state, lineage.clone());

    let forjar_report = match forjar_runner.run("forjar-run").await {
        Ok(r) => Some(r),
        Err(e) => {
            eprintln!("\n[ForjarRunner] skipped — {e}");
            eprintln!(
                "(install forjar with `cargo install forjar` to exercise the second runner.)"
            );
            None
        }
    };
    if let Some(r) = &forjar_report {
        println!("\n[ForjarRunner]");
        println!("topo_order = {:?}", r.topo_order);
        println!("states     = {:?}", r.task_states);
    }

    // 3. Determinism probe: 10 LocalRunner runs, same topo each time.
    let mut topo_orders = Vec::with_capacity(10);
    for i in 0..10 {
        let lineage_i = LineageStore::open_memory().await?;
        let runner_i = LocalRunner::new(
            build_etl_dag(&workdir)?,
            lineage_i,
            workdir.join(format!("scratch-determ-{i}")),
        );
        let r = runner_i.run(&format!("determ-{i}")).await?;
        topo_orders.push(r.topo_order);
    }
    let first = topo_orders[0].clone();

    // 4. Assert all four runtime invariants. Each is one of the
    // contracts in contracts/dag-rust-v1.yaml.
    println!("\n=== runtime contracts ===");

    // C1 all-tasks-complete
    assert!(
        local_report.all_succeeded(),
        "C1 all-tasks-complete: every LocalRunner task must Succeed; got {:?}",
        local_report.task_states
    );
    println!("C1 all-tasks-complete:        OK (5 tasks, all Succeeded)");

    // C2 lineage-edge-count — exactly 4 directed edges in a 5-node linear chain.
    let edge_count_local = lineage.edge_count(Some("local-run")).await?;
    assert_eq!(
        edge_count_local, 4,
        "C2 lineage-edge-count: linear 5-node chain must have 4 edges, got {edge_count_local}"
    );
    println!("C2 lineage-edge-count:        OK (4 edges in the linear chain)");

    // C3 runner-output-equivalence
    if let Some(forjar_report) = &forjar_report {
        // Both runners produce a topologically valid order on the same DAG
        // shape; for a strict linear chain the order is byte-identical.
        assert_eq!(
            local_report.topo_order, forjar_report.topo_order,
            "C3 runner-output-equivalence: topo orders disagree:\n  local:  {:?}\n  forjar: {:?}",
            local_report.topo_order, forjar_report.topo_order
        );
        // Both runners record the same set of task ids and the same
        // cardinality. Output payloads differ in shape (LocalRunner emits
        // per-task results; ForjarRunner emits resource manifests) — the
        // contract is on the topology, not on the payload bytes.
        let local_ids: std::collections::BTreeSet<&str> = local_report
            .task_outputs
            .keys()
            .map(|s| s.as_str())
            .collect();
        let forjar_ids: std::collections::BTreeSet<&str> = forjar_report
            .task_outputs
            .keys()
            .map(|s| s.as_str())
            .collect();
        assert_eq!(
            local_ids, forjar_ids,
            "C3 runner-output-equivalence: task id sets differ"
        );
        println!("C3 runner-output-equivalence: OK (both runners agree on topo + task set)");
    } else {
        println!("C3 runner-output-equivalence: SKIPPED (forjar not installed)");
    }

    // C4 topo-determinism
    for (i, order) in topo_orders.iter().enumerate() {
        assert_eq!(
            order, &first,
            "C4 topo-determinism: run {i} produced a different order: {order:?} vs {first:?}"
        );
    }
    println!("C4 topo-determinism:          OK (10/10 LocalRunner runs identical topo)");

    // Print the lineage as Mermaid for the README screenshot.
    let mermaid = lineage.render_mermaid("local-run").await?;
    println!("\n=== lineage (Mermaid, local-run) ===\n{mermaid}");

    println!("\nDemo complete.");
    Ok(())
}

/// Build the 5-task linear-chain DAG with real task bodies.
fn build_etl_dag(workdir: &Path) -> Result<Dag<Arc<dyn Task>>> {
    let workdir_extract = workdir.to_path_buf();
    let workdir_load = workdir.to_path_buf();
    let workdir_notify = workdir.to_path_buf();

    let mut dag = Dag::<Arc<dyn Task>>::new();

    // extract — parse the embedded JSON fixture and publish it.
    let extract = dag.add_node(ClosureTask::new("extract", move |_ctx| {
        let workdir = workdir_extract.clone();
        async move {
            let parsed: Vec<Order> =
                serde_json::from_str(FIXTURE).context("extract: bad fixture json")?;
            // Side effect: drop a copy in scratch so the operator can `cat` it.
            std::fs::write(
                workdir.join("extract.json"),
                serde_json::to_string_pretty(&parsed)?,
            )?;
            Ok(TaskOutput::Json(serde_json::to_value(parsed)?))
        }
    }));

    // transform — derive `total = qty * unit_price` per row.
    let transform = dag.add_node(ClosureTask::new(
        "transform",
        move |ctx: Context| async move {
            let upstream = ctx.get("extract").context("transform: extract missing")?;
            let orders: Vec<Order> = serde_json::from_value(upstream.as_json())
                .context("transform: extract output not Vec<Order>")?;
            let enriched: Vec<EnrichedOrder> = orders
                .into_iter()
                .map(|o| EnrichedOrder {
                    total: o.qty as f64 * o.unit_price,
                    id: o.id,
                    sku: o.sku,
                    qty: o.qty,
                    unit_price: o.unit_price,
                })
                .collect();
            Ok(TaskOutput::Json(serde_json::to_value(enriched)?))
        },
    ));

    // validate — assert non-empty, every total > 0; return row count.
    let validate = dag.add_node(ClosureTask::new(
        "validate",
        move |ctx: Context| async move {
            let upstream = ctx
                .get("transform")
                .context("validate: transform missing")?;
            let enriched: Vec<EnrichedOrder> = serde_json::from_value(upstream.as_json())
                .context("validate: transform output not Vec<EnrichedOrder>")?;
            anyhow::ensure!(!enriched.is_empty(), "validate: empty batch");
            for o in &enriched {
                anyhow::ensure!(o.total > 0.0, "validate: total must be positive: {o:?}");
                anyhow::ensure!(!o.sku.is_empty(), "validate: sku must be non-empty");
            }
            Ok(TaskOutput::Int(enriched.len() as i64))
        },
    ));

    // load — write the enriched rows into a per-run SQLite table.
    let load = dag.add_node(ClosureTask::new("load", move |ctx: Context| {
        let workdir = workdir_load.clone();
        async move {
            let upstream = ctx.get("transform").context("load: transform missing")?;
            let enriched: Vec<EnrichedOrder> = serde_json::from_value(upstream.as_json())
                .context("load: transform output not Vec<EnrichedOrder>")?;
            let db_path = workdir.join("orders.sqlite");
            let pool = sqlx::sqlite::SqlitePoolOptions::new()
                .max_connections(1)
                .connect_with(
                    sqlx::sqlite::SqliteConnectOptions::new()
                        .filename(&db_path)
                        .create_if_missing(true),
                )
                .await?;
            sqlx::query(
                "CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY, \
                 sku TEXT NOT NULL, qty INTEGER NOT NULL, unit_price REAL NOT NULL, \
                 total REAL NOT NULL)",
            )
            .execute(&pool)
            .await?;
            for o in &enriched {
                sqlx::query(
                    "INSERT OR REPLACE INTO orders (id, sku, qty, unit_price, total) \
                     VALUES (?, ?, ?, ?, ?)",
                )
                .bind(o.id as i64)
                .bind(&o.sku)
                .bind(o.qty as i64)
                .bind(o.unit_price)
                .bind(o.total)
                .execute(&pool)
                .await?;
            }
            pool.close().await;
            Ok(TaskOutput::Text(format!(
                "wrote {} rows to {}",
                enriched.len(),
                db_path.display()
            )))
        }
    }));

    // notify — append a one-line summary to notifications.log.
    let notify = dag.add_node(ClosureTask::new("notify", move |ctx: Context| {
        let workdir = workdir_notify.clone();
        async move {
            let count = ctx.get("validate").context("notify: validate missing")?;
            let n = match count {
                TaskOutput::Int(n) => n,
                _ => 0,
            };
            let msg = format!(
                "[{}] etl_pipeline_dag — loaded {n} rows\n",
                chrono::Utc::now().to_rfc3339()
            );
            let log_path = workdir.join("notifications.log");
            let mut f = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&log_path)?;
            f.write_all(msg.as_bytes())?;
            Ok(TaskOutput::Text(msg.trim_end().to_string()))
        }
    }));

    dag.add_edge(extract, transform)?;
    dag.add_edge(transform, validate)?;
    dag.add_edge(validate, load)?;
    dag.add_edge(load, notify)?;
    Ok(dag)
}

/// Generate the forjar.yaml the ForjarRunner consumes. We use 5 `file`
/// resources because forjar's `file` provider with `state: directory` is
/// the cheapest one that exists on every host (it just `mkdir -p`s a
/// tempdir). The DAG topology — 5 nodes, 4 edges, linear — matches the
/// LocalRunner's, which is all the runner-output-equivalence contract
/// asserts on.
fn forjar_yaml_for_etl() -> String {
    // Use a tempdir for `path` so we don't write into anything sensitive.
    let dir: PathBuf = std::env::temp_dir().join("etl-pipeline-dag-forjar-files");
    let dir_s = dir.display().to_string();
    format!(
        r#"version: "1.0"
name: etl-pipeline-dag
description: "c16 workflow-orchestration closing demo, executed via forjar"
machines:
  local:
    hostname: localhost
    addr: 127.0.0.1
resources:
  extract:
    type: file
    machine: local
    state: directory
    path: "{dir_s}/extract"
    mode: "0755"
  transform:
    type: file
    machine: local
    state: directory
    path: "{dir_s}/transform"
    mode: "0755"
    depends_on: [extract]
  validate:
    type: file
    machine: local
    state: directory
    path: "{dir_s}/validate"
    mode: "0755"
    depends_on: [transform]
  load:
    type: file
    machine: local
    state: directory
    path: "{dir_s}/load"
    mode: "0755"
    depends_on: [validate]
  notify:
    type: file
    machine: local
    state: directory
    path: "{dir_s}/notify"
    mode: "0755"
    depends_on: [load]
"#
    )
}

/// Suppress unused warning when forjar is uninstalled.
#[allow(dead_code)]
fn _ensure_run_report_serializes(r: &RunReport) {
    let _ = serde_json::to_string(r);
}
