use std::{future::Future, time::Duration};

use sqlx::SqlitePool;
use uuid::Uuid;

use crate::app::{to_string, CommandResult};
use crate::ui_notifications::{
    enqueue_ui_agent_run_changed_in_tx, enqueue_ui_work_item_changed_in_tx,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WarmTurnOutcome {
    pub(crate) agent_status: &'static str,
    pub(crate) run_status: &'static str,
    pub(crate) should_reset_runtime: bool,
    pub(crate) runtime_session_status: &'static str,
    pub(crate) activity_kind: &'static str,
    pub(crate) activity_title: &'static str,
}

pub(crate) fn resolve_warm_turn_outcome(success: bool, was_cancelled: bool) -> WarmTurnOutcome {
    if was_cancelled {
        return WarmTurnOutcome {
            agent_status: "idle",
            run_status: "cancelled",
            should_reset_runtime: false,
            runtime_session_status: "idle",
            activity_kind: "run",
            activity_title: "Stopped",
        };
    }

    if success {
        WarmTurnOutcome {
            agent_status: "idle",
            run_status: "exited",
            should_reset_runtime: false,
            runtime_session_status: "idle",
            activity_kind: "run",
            activity_title: "Completed",
        }
    } else {
        WarmTurnOutcome {
            agent_status: "idle",
            run_status: "failed",
            should_reset_runtime: true,
            runtime_session_status: "stopped",
            activity_kind: "run_error",
            activity_title: "Failed",
        }
    }
}

/// SQLite answers some write conflicts with `database is locked` at once
/// instead of waiting out `busy_timeout` (a deferred transaction that read
/// first, for instance). Turn finalization retries those so a burst of agents
/// finishing together cannot leave a run open.
pub(crate) fn is_transient_sqlite_error(error: &str) -> bool {
    error.contains("database is locked")
        || error.contains("database table is locked")
        || error.contains("(code: 5)")
        || error.contains("(code: 6)")
}

const TRANSIENT_RETRY_ATTEMPTS: u32 = 8;
const TRANSIENT_RETRY_FIRST_DELAY: Duration = Duration::from_millis(50);
const TRANSIENT_RETRY_MAX_DELAY: Duration = Duration::from_secs(2);

pub(crate) async fn retry_transient_sqlite<T, F, Fut>(mut operation: F) -> CommandResult<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = CommandResult<T>>,
{
    let mut delay = TRANSIENT_RETRY_FIRST_DELAY;
    let mut attempt = 1;
    loop {
        match operation().await {
            Err(error)
                if attempt < TRANSIENT_RETRY_ATTEMPTS && is_transient_sqlite_error(&error) =>
            {
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(TRANSIENT_RETRY_MAX_DELAY);
                attempt += 1;
            }
            result => return result,
        }
    }
}

/// Keeps the first error of a best-effort step and lets finalization go on.
pub(crate) fn keep_first_error<T>(
    first: &mut Option<String>,
    result: CommandResult<T>,
) -> Option<T> {
    match result {
        Ok(value) => Some(value),
        Err(error) => {
            if first.is_none() {
                *first = Some(error);
            }
            None
        }
    }
}

/// Work-item status after a turn ends. A turn that failed because the
/// provider login expired never reached the model, so its request goes back to
/// the queue instead of failing.
pub(crate) fn finished_work_status(
    success: bool,
    was_cancelled: bool,
    auth_required: bool,
    current_status: Option<&str>,
) -> &'static str {
    if was_cancelled {
        "cancelled"
    } else if auth_required {
        "queued"
    } else if success && current_status == Some("silent") {
        "silent"
    } else if success && current_status == Some("held") {
        "held"
    } else if success {
        "done"
    } else {
        "failed"
    }
}

pub(crate) struct TurnTerminalState<'a> {
    pub(crate) agent_id: Uuid,
    pub(crate) agent_status: &'a str,
    pub(crate) run_id: Uuid,
    pub(crate) run_status: &'a str,
    pub(crate) log_line: &'a str,
    pub(crate) run_reason: &'static str,
    pub(crate) work: Option<(Uuid, &'a str)>,
}

/// Closes the run and settles its work item in one transaction, retrying
/// transient lock errors. A `queued` work item is handed back to the
/// scheduler rather than completed.
pub(crate) async fn write_turn_terminal_state(
    pool: &SqlitePool,
    state: &TurnTerminalState<'_>,
) -> CommandResult<()> {
    retry_transient_sqlite(|| write_turn_terminal_state_once(pool, state)).await
}

async fn write_turn_terminal_state_once(
    pool: &SqlitePool,
    state: &TurnTerminalState<'_>,
) -> CommandResult<()> {
    let mut transaction = pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .map_err(to_string)?;
    sqlx::query(
        r#"
        update agent_runs
        set status = $2,
            exit_code = null,
            log = substr(log || $3, -20000),
            stopped_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
        where id = $1
        "#,
    )
    .bind(state.run_id)
    .bind(state.run_status)
    .bind(state.log_line)
    .execute(&mut *transaction)
    .await
    .map_err(to_string)?;
    sqlx::query("update agents set status = $2 where id = $1")
        .bind(state.agent_id)
        .bind(state.agent_status)
        .execute(&mut *transaction)
        .await
        .map_err(to_string)?;
    enqueue_ui_agent_run_changed_in_tx(&mut transaction, state.run_id, state.run_reason).await?;
    if let Some((work_item_id, work_status)) = state.work {
        if work_status == "queued" {
            sqlx::query(
                r#"
                update agent_work_items
                set status = 'queued',
                    run_id = null,
                    completed_at = null,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
                where id = $1
                "#,
            )
            .bind(work_item_id)
            .execute(&mut *transaction)
            .await
            .map_err(to_string)?;
            enqueue_ui_work_item_changed_in_tx(
                &mut transaction,
                work_item_id,
                "work_item_requeued",
            )
            .await?;
        } else {
            sqlx::query(
                r#"
                update agent_work_items
                set status = $2,
                    completed_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now'),
                    updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
                where id = $1
                "#,
            )
            .bind(work_item_id)
            .bind(work_status)
            .execute(&mut *transaction)
            .await
            .map_err(to_string)?;
            enqueue_ui_work_item_changed_in_tx(
                &mut transaction,
                work_item_id,
                "work_item_finished",
            )
            .await?;
        }
    }
    transaction.commit().await.map_err(to_string)
}

#[cfg(test)]
mod tests {
    use super::{resolve_warm_turn_outcome, WarmTurnOutcome};

    #[test]
    fn warm_turn_outcome_matrix_keeps_agents_routable_and_resets_only_failures() {
        let cases = [
            (
                true,
                false,
                WarmTurnOutcome {
                    agent_status: "idle",
                    run_status: "exited",
                    should_reset_runtime: false,
                    runtime_session_status: "idle",
                    activity_kind: "run",
                    activity_title: "Completed",
                },
            ),
            (
                false,
                false,
                WarmTurnOutcome {
                    agent_status: "idle",
                    run_status: "failed",
                    should_reset_runtime: true,
                    runtime_session_status: "stopped",
                    activity_kind: "run_error",
                    activity_title: "Failed",
                },
            ),
            (
                true,
                true,
                WarmTurnOutcome {
                    agent_status: "idle",
                    run_status: "cancelled",
                    should_reset_runtime: false,
                    runtime_session_status: "idle",
                    activity_kind: "run",
                    activity_title: "Stopped",
                },
            ),
            (
                false,
                true,
                WarmTurnOutcome {
                    agent_status: "idle",
                    run_status: "cancelled",
                    should_reset_runtime: false,
                    runtime_session_status: "idle",
                    activity_kind: "run",
                    activity_title: "Stopped",
                },
            ),
        ];

        for (success, was_cancelled, expected) in cases {
            assert_eq!(
                resolve_warm_turn_outcome(success, was_cancelled),
                expected,
                "success={success} was_cancelled={was_cancelled}"
            );
        }
    }
}
