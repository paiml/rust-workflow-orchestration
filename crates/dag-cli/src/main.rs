//! `dag` — workflow-orchestration command-line driver for the c16 demo.
//!
//! Subcommands:
//!   - `validate <dag.yaml>` — parse, topo, cycle-check
//!   - `run <dag.yaml> --runner local|forjar` — execute end-to-end
//!   - `schedule <dag.yaml> --cron "..."` — register a cron trigger
//!   - `lineage <task-id> [--run-id <id>]` — query the lineage store
//!   - `backfill <dag.yaml> --start <ts> --end <ts>` — re-run for a date range

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{bail, Context as _, Result};
use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand, ValueEnum};
use dag_core::{Context, Dag, DagSpec, Task, TaskOutput};
use dag_lineage::LineageStore;
use dag_runner::forjar::ForjarRunner;
use dag_runner::{LocalRunner, Runner};
use dag_scheduler::DagScheduler;

#[derive(Debug, Parser)]
#[command(name = "dag", version, about = "Workflow Orchestration CLI (c16)")]
struct Cli {
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// Parse a dag.yaml, build the topology, run cycle + topo checks.
    Validate {
        /// Path to a DAG yaml (DagSpec) file.
        path: PathBuf,
        /// Print the topo-ordered task ids on success.
        #[arg(long)]
        print_order: bool,
    },
    /// Execute a DAG end-to-end via the chosen runner.
    Run {
        path: PathBuf,
        /// Pick the executor.
        #[arg(long, value_enum, default_value_t = RunnerKind::Local)]
        runner: RunnerKind,
        /// SQLite file for lineage persistence (created if missing).
        #[arg(long, default_value = "lineage.sqlite")]
        lineage_db: PathBuf,
        /// Optional run id; auto-generated when omitted.
        #[arg(long)]
        run_id: Option<String>,
        /// Forjar state directory (only used by `--runner forjar`).
        #[arg(long, default_value = "state")]
        forjar_state_dir: PathBuf,
    },
    /// Register a cron trigger and exit. Not a long-running daemon — the
    /// scheduler is constructed, the cron expression is validated, and the
    /// next-fire timestamp is printed.
    Schedule {
        path: PathBuf,
        /// 6-field cron expression (sec min hour day month weekday).
        #[arg(long)]
        cron: String,
    },
    /// Query the lineage store for a task's upstream + downstream edges.
    Lineage {
        /// Task id to query.
        task_id: String,
        /// Restrict to a specific run.
        #[arg(long)]
        run_id: Option<String>,
        /// Lineage SQLite path.
        #[arg(long, default_value = "lineage.sqlite")]
        lineage_db: PathBuf,
        /// Render the run as a Mermaid graph instead of JSON.
        #[arg(long)]
        mermaid: bool,
    },
    /// Re-run the DAG once per logical date in the closed range
    /// [`start`, `end`]. Inclusive on both ends. Each run gets its own
    /// `run_id` of the form `backfill-YYYY-MM-DD`.
    Backfill {
        path: PathBuf,
        #[arg(long)]
        start: String,
        #[arg(long)]
        end: String,
        #[arg(long, default_value = "lineage.sqlite")]
        lineage_db: PathBuf,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum RunnerKind {
    Local,
    Forjar,
}

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

    let cli = Cli::parse();
    match cli.command {
        Cmd::Validate { path, print_order } => cmd_validate(path, print_order).await,
        Cmd::Run {
            path,
            runner,
            lineage_db,
            run_id,
            forjar_state_dir,
        } => cmd_run(path, runner, lineage_db, run_id, forjar_state_dir).await,
        Cmd::Schedule { path, cron } => cmd_schedule(path, cron).await,
        Cmd::Lineage {
            task_id,
            run_id,
            lineage_db,
            mermaid,
        } => cmd_lineage(task_id, run_id, lineage_db, mermaid).await,
        Cmd::Backfill {
            path,
            start,
            end,
            lineage_db,
        } => cmd_backfill(path, start, end, lineage_db).await,
    }
}

async fn cmd_validate(path: PathBuf, print_order: bool) -> Result<()> {
    let raw = std::fs::read_to_string(&path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    let spec = DagSpec::from_yaml(&raw)?;
    let dag = spec.build()?;
    let order = dag.topo_sort()?;
    println!(
        "OK: {} ({} tasks, {} edges)",
        spec.name,
        dag.node_count(),
        dag.edge_count()
    );
    if print_order {
        for id in dag.payloads(&order) {
            println!("  - {id}");
        }
    }
    Ok(())
}

async fn cmd_run(
    path: PathBuf,
    runner_kind: RunnerKind,
    lineage_db: PathBuf,
    run_id: Option<String>,
    forjar_state_dir: PathBuf,
) -> Result<()> {
    let lineage = LineageStore::open(&lineage_db).await?;
    let run_id = run_id.unwrap_or_else(|| format!("run-{}", uuid_short()));

    match runner_kind {
        RunnerKind::Local => {
            let raw = std::fs::read_to_string(&path)
                .with_context(|| format!("failed to read {}", path.display()))?;
            let spec = DagSpec::from_yaml(&raw)?;
            let dag = build_runnable_dag(&spec)?;
            let runner = LocalRunner::new(
                dag,
                lineage.clone(),
                std::env::temp_dir().join("dag-cli-scratch"),
            );
            let report = runner.run(&run_id).await?;
            print_report(&report);
        }
        RunnerKind::Forjar => {
            let runner = ForjarRunner::new(&path, &forjar_state_dir, lineage.clone());
            let report = runner.run(&run_id).await?;
            print_report(&report);
        }
    }
    Ok(())
}

/// `cmd_run --runner local` accepts a DagSpec yaml. Each task becomes a
/// no-op closure that emits its own id as a string output. This is the
/// minimum the CLI can do without a user-supplied task table; the
/// production path is the `etl_pipeline_dag` example which carries real
/// task bodies.
fn build_runnable_dag(spec: &DagSpec) -> Result<Dag<Arc<dyn Task>>> {
    use dag_runner::local::ClosureTask;

    let mut dag = Dag::<Arc<dyn Task>>::new();
    let mut indices = std::collections::HashMap::new();
    for ts in &spec.tasks {
        let id_for_closure = ts.id.clone();
        let task: Arc<dyn Task> = ClosureTask::new(ts.id.clone(), move |_ctx| {
            let id = id_for_closure.clone();
            async move { Ok(TaskOutput::Text(id)) }
        });
        let idx = dag.add_node(task);
        indices.insert(ts.id.clone(), idx);
    }
    for ts in &spec.tasks {
        let to = indices[&ts.id];
        for dep in &ts.depends_on {
            let from = *indices
                .get(dep)
                .ok_or_else(|| anyhow::anyhow!("'{}' depends on unknown '{}'", ts.id, dep))?;
            dag.add_edge(from, to)?;
        }
    }
    dag.cycle_check()?;
    // Quick smoke: build a Context just to make sure scratch dirs work.
    let _ = Context::new("smoke", std::env::temp_dir());
    Ok(dag)
}

fn print_report(report: &dag_runner::RunReport) {
    println!(
        "{}",
        serde_json::to_string_pretty(report).expect("RunReport serializes")
    );
}

async fn cmd_schedule(path: PathBuf, cron: String) -> Result<()> {
    let raw = std::fs::read_to_string(&path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    let spec = DagSpec::from_yaml(&raw)?;
    let _dag = spec.build()?; // validate before registering

    let scheduler = DagScheduler::new().await?;
    let dag_name = spec.name.clone();
    let job_id = scheduler
        .schedule(dag_name.clone(), cron, || {
            Box::pin(async {
                tracing::info!("(cron tick) — would trigger DAG run here");
                Ok(())
            })
        })
        .await?;
    let next = scheduler.next_fire(job_id).await?;
    println!(
        "Registered '{}' as job {} (next fire: {:?})",
        dag_name, job_id, next
    );
    // CLI is fire-and-forget — drop the scheduler immediately so the
    // process exits. Real users would `start()` and `await` a shutdown
    // signal.
    Ok(())
}

async fn cmd_lineage(
    task_id: String,
    run_id: Option<String>,
    lineage_db: PathBuf,
    mermaid: bool,
) -> Result<()> {
    let lineage = LineageStore::open(&lineage_db).await?;
    let rid = run_id.unwrap_or_else(|| "default".into());
    if mermaid {
        let mermaid = lineage.render_mermaid(&rid).await?;
        print!("{mermaid}");
        return Ok(());
    }
    let upstream = lineage.query_upstream(&rid, &task_id).await?;
    let downstream = lineage.query_downstream(&rid, &task_id).await?;
    let payload = serde_json::json!({
        "run_id": rid,
        "task_id": task_id,
        "upstream": upstream,
        "downstream": downstream,
    });
    println!("{}", serde_json::to_string_pretty(&payload)?);
    Ok(())
}

async fn cmd_backfill(
    path: PathBuf,
    start: String,
    end: String,
    lineage_db: PathBuf,
) -> Result<()> {
    let start: DateTime<Utc> = parse_date(&start)?;
    let end: DateTime<Utc> = parse_date(&end)?;
    if end < start {
        bail!("--end ({end}) is before --start ({start})");
    }

    let raw = std::fs::read_to_string(&path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    let spec = DagSpec::from_yaml(&raw)?;
    let lineage = LineageStore::open(&lineage_db).await?;

    // One run per day in the inclusive [start, end] range.
    let one_day = chrono::Duration::days(1);
    let mut cursor = start;
    let mut runs_executed = 0usize;
    while cursor <= end {
        let rid = format!("backfill-{}", cursor.format("%Y-%m-%d"));
        let dag = build_runnable_dag(&spec)?;
        let runner = LocalRunner::new(
            dag,
            lineage.clone(),
            std::env::temp_dir().join("dag-cli-scratch"),
        );
        let report = runner.run(&rid).await?;
        if !report.all_succeeded() {
            bail!(
                "backfill run {rid} did not succeed (states: {:?})",
                report.task_states
            );
        }
        runs_executed += 1;
        cursor += one_day;
    }
    println!(
        "OK: backfill complete — {} run(s) from {} to {}",
        runs_executed,
        start.format("%Y-%m-%d"),
        end.format("%Y-%m-%d"),
    );
    Ok(())
}

fn parse_date(s: &str) -> Result<DateTime<Utc>> {
    // Accept either `YYYY-MM-DD` or full RFC3339.
    if let Ok(d) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        let t = d
            .and_hms_opt(0, 0, 0)
            .context("midnight conversion failed")?;
        return Ok(DateTime::from_naive_utc_and_offset(t, Utc));
    }
    Ok(DateTime::parse_from_rfc3339(s)?.with_timezone(&Utc))
}

fn uuid_short() -> String {
    // Avoid pulling uuid into dag-cli — chrono is enough for an example id.
    format!("{}", Utc::now().timestamp_micros())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_date_accepts_date_only() {
        let d = parse_date("2026-05-04").unwrap();
        assert_eq!(d.format("%Y-%m-%d").to_string(), "2026-05-04");
    }

    #[test]
    fn parse_date_accepts_rfc3339() {
        let d = parse_date("2026-05-04T12:30:00Z").unwrap();
        assert_eq!(d.format("%H").to_string(), "12");
    }

    #[test]
    fn parse_date_rejects_bad_input() {
        assert!(parse_date("not-a-date").is_err());
    }

    #[test]
    fn build_runnable_dag_from_minimal_spec() {
        let spec = DagSpec::from_yaml(
            r#"
name: minimal
tasks:
  - id: a
  - id: b
    depends_on: [a]
"#,
        )
        .unwrap();
        let dag = build_runnable_dag(&spec).unwrap();
        assert_eq!(dag.node_count(), 2);
        assert_eq!(dag.edge_count(), 1);
    }
}
