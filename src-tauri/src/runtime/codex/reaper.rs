use std::{sync::Arc, time::Duration};

use sqlx::SqlitePool;
use tokio::time::sleep;
use uuid::Uuid;

use super::{
    remove_warm_codex_runtime_if_same, turn::finish_warm_codex_active_turn, WarmCodexRegistry,
    WarmCodexRuntime, CODEX_TURN_START_TIMEOUT,
};
use crate::app::CommandResult;
use crate::events::activity::record_agent_activity;
use crate::runtime::process::terminate_process_group;
use crate::ui_notifications::notify_supervisor_wake;

const CODEX_IDLE_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const CODEX_IDLE_REAPER_INTERVAL: Duration = Duration::from_secs(30);

async fn reap_stuck_codex_runtime(
    pool: &SqlitePool,
    registry: &WarmCodexRegistry,
    agent_id: Uuid,
    runtime: &Arc<WarmCodexRuntime>,
    source: &str,
) -> CommandResult<bool> {
    let (pid, elapsed_ms) = {
        let mut state = runtime.state.lock().await;
        let Some(active) = state.active.as_ref() else {
            return Ok(false);
        };
        let elapsed = active.started_at.elapsed();
        if active.turn_id.is_some() || elapsed < CODEX_TURN_START_TIMEOUT {
            return Ok(false);
        }
        let elapsed_ms = elapsed.as_millis();
        state.alive = false;
        (runtime.pid, elapsed_ms)
    };

    let detail = format!(
        "no turn id after {elapsed_ms} ms; source={source}; process_group={}",
        pid.map(|pid| pid.to_string())
            .unwrap_or_else(|| "unavailable".to_owned())
    );
    finish_warm_codex_active_turn(pool, agent_id, runtime, false, Some(detail.clone())).await?;
    if let Some(pid) = pid {
        if let Err(err) = terminate_process_group(pid).await {
            record_agent_activity(
                pool,
                Some(agent_id),
                None,
                "run_error",
                "Codex zombie turn stop failed",
                err,
            )
            .await?;
        }
    }
    let _ = sqlx::query(
        "update runtime_sessions set status = 'failed', updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now') where agent_id = $1 and runtime = 'codex'",
    )
    .bind(agent_id)
    .execute(pool)
    .await;
    remove_warm_codex_runtime_if_same(registry, agent_id, runtime).await;
    record_agent_activity(
        pool,
        Some(agent_id),
        None,
        "run_error",
        "Codex zombie turn recovered",
        detail,
    )
    .await?;
    let _ = notify_supervisor_wake(pool).await;
    Ok(true)
}

pub(crate) async fn reap_stuck_codex_turn(
    pool: &SqlitePool,
    registry: &WarmCodexRegistry,
    agent_id: Uuid,
    source: &str,
) -> CommandResult<bool> {
    let runtime = {
        let runtimes = registry.runtimes.lock().await;
        runtimes.get(&agent_id).cloned()
    };
    let Some(runtime) = runtime else {
        return Ok(false);
    };
    reap_stuck_codex_runtime(pool, registry, agent_id, &runtime, source).await
}

pub(super) async fn codex_warm_idle_reaper(
    pool: SqlitePool,
    registry: WarmCodexRegistry,
    agent_id: Uuid,
    runtime: Arc<WarmCodexRuntime>,
) {
    loop {
        sleep(CODEX_IDLE_REAPER_INTERVAL).await;
        let stop_reason = {
            let mut state = runtime.state.lock().await;
            if !state.alive {
                return;
            }
            if let Some(active) = state.active.as_ref() {
                if active.turn_id.is_none()
                    && active.started_at.elapsed() >= CODEX_TURN_START_TIMEOUT
                {
                    Some("zombie")
                } else {
                    None
                }
            } else if state.last_activity.elapsed() >= CODEX_IDLE_TIMEOUT {
                state.alive = false;
                Some("idle")
            } else {
                None
            }
        };
        let Some(stop_reason) = stop_reason else {
            continue;
        };
        if stop_reason == "zombie" {
            let _ =
                reap_stuck_codex_runtime(&pool, &registry, agent_id, &runtime, "idle_reaper").await;
            return;
        }

        let Some(pid) = runtime.pid else {
            return;
        };
        match terminate_process_group(pid).await {
            Ok(()) => {
                let _ = sqlx::query(
                    "update runtime_sessions set status = 'stopping', updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now') where agent_id = $1 and runtime = 'codex'",
                )
                .bind(agent_id)
                .execute(&pool)
                .await;
                let _ = record_agent_activity(
                    &pool,
                    Some(agent_id),
                    None,
                    "run",
                    "Codex warm app-server idle timeout",
                    format!("sent SIGTERM to process_group={pid}"),
                )
                .await;
            }
            Err(err) => {
                {
                    let mut state = runtime.state.lock().await;
                    state.alive = true;
                    state.last_activity = std::time::Instant::now();
                }
                let _ = record_agent_activity(
                    &pool,
                    Some(agent_id),
                    None,
                    "run_error",
                    "Codex warm app-server idle stop failed",
                    err,
                )
                .await;
            }
        }
        return;
    }
}
