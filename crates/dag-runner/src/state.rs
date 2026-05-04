//! Task state machine.
//!
//! The M3 lessons of the course walk through this enum in detail; this is
//! the Rust sum type that the lessons use to motivate "use a type, not a
//! string". The `From<&str>`/`AsRef<str>` impls make persistence to sqlx
//! a straight-forward TEXT column without `#[derive(sqlx::Type)]` (which
//! requires turning on the `derive` feature for sqlx + linking via macros).

use serde::{Deserialize, Serialize};

/// Lifecycle of a single task within a DAG run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum TaskState {
    /// Visible to the runner, not yet picked up.
    Queued,
    /// Worker has started executing.
    Running,
    /// Completed cleanly.
    Succeeded,
    /// Errored out. The runner records the error message in lineage.
    Failed,
    /// Skipped because an upstream task failed (used by the LocalRunner's
    /// fail-fast policy).
    Skipped,
}

impl TaskState {
    /// Stable string label used by the lineage TEXT column and by JSON
    /// serialization. Matches the variant name verbatim.
    pub fn as_label(&self) -> &'static str {
        match self {
            TaskState::Queued => "Queued",
            TaskState::Running => "Running",
            TaskState::Succeeded => "Succeeded",
            TaskState::Failed => "Failed",
            TaskState::Skipped => "Skipped",
        }
    }
}

impl AsRef<str> for TaskState {
    fn as_ref(&self) -> &str {
        self.as_label()
    }
}

impl std::fmt::Display for TaskState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_label())
    }
}

impl std::str::FromStr for TaskState {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Queued" => Ok(TaskState::Queued),
            "Running" => Ok(TaskState::Running),
            "Succeeded" => Ok(TaskState::Succeeded),
            "Failed" => Ok(TaskState::Failed),
            "Skipped" => Ok(TaskState::Skipped),
            other => Err(anyhow::anyhow!("unknown TaskState: {other}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn label_roundtrip() {
        for s in [
            TaskState::Queued,
            TaskState::Running,
            TaskState::Succeeded,
            TaskState::Failed,
            TaskState::Skipped,
        ] {
            let parsed: TaskState = TaskState::from_str(s.as_label()).unwrap();
            assert_eq!(parsed, s);
        }
    }

    #[test]
    fn unknown_label_errors() {
        assert!(TaskState::from_str("RogueState").is_err());
    }

    #[test]
    fn json_serialization_uses_pascal_case() {
        let json = serde_json::to_string(&TaskState::Succeeded).unwrap();
        assert_eq!(json, r#""Succeeded""#);
    }
}
