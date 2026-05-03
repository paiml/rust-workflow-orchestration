# rust-workflow-orchestration

[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)
[![MSRV](https://img.shields.io/badge/MSRV-1.95-orange.svg)](rust-toolchain.toml)

Reference Rust workspace for course **c16 — Workflow Orchestration with Rust** in the Coursera
[Rust for Data Engineering](https://www.coursera.org/) specialization.

Native-Rust dataflow DAGs — model with `petgraph`, schedule with `tokio-cron-scheduler`, execute durably with `apalis`, persist lineage with `sqlx`. Replaces the parts of Airflow that matter (DAG semantics, backfill, retries) and uses types where Airflow uses strings.

## Workspace layout

- [`crates/dag-core`](crates/dag-core) — DAG model, Task trait, toposort, cycle detection
- [`crates/dag-scheduler`](crates/dag-scheduler) — Cron, interval, and event triggers
- [`crates/dag-runner`](crates/dag-runner) — apalis-backed task executor with typed state
- [`crates/dag-lineage`](crates/dag-lineage) — sqlx-backed run/lineage store
- [`crates/dag-cli`](crates/dag-cli) — clap binary including dag-check (cycle detector)

## Quick start

```bash
git clone https://github.com/paiml/rust-workflow-orchestration
cd rust-workflow-orchestration
cargo test --workspace
```

## Status

Scaffold. Lessons land as recordings ship. Track companion config at
[`paiml/course-studio`](https://github.com/paiml/course-studio).

## License

Dual-licensed under either of

- [Apache License, Version 2.0](LICENSE-APACHE)
- [MIT License](LICENSE-MIT)

at your option.
