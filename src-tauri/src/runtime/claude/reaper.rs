use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use sqlx::SqlitePool;
use tokio::time::sleep;
use uuid::Uuid;

use super::WarmClaudeRuntime;
use crate::events::activity::record_agent_activity;
use crate::runtime::process::terminate_process_group;

const CLAUDE_IDLE_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const CLAUDE_IDLE_REAPER_INTERVAL: Duration = Duration::from_secs(30);

pub(super) async fn claude_warm_idle_reaper(
    pool: SqlitePool,
    agent_id: Uuid,
    runtime: Arc<WarmClaudeRuntime>,
) {
    loop {
        sleep(CLAUDE_IDLE_REAPER_INTERVAL).await;
        let should_stop = {
            let mut state = runtime.state.lock().await;
            let should_stop = state.alive
                && state.active.is_none()
                && state.last_activity.elapsed() >= CLAUDE_IDLE_TIMEOUT;
            if should_stop {
                state.alive = false;
            }
            should_stop
        };
        if !should_stop {
            if !runtime.state.lock().await.alive {
                return;
            }
            continue;
        }

        let Some(pid) = runtime.pid else {
            return;
        };
        match terminate_process_group(pid).await {
            Ok(()) => {
                let _ = sqlx::query(
                    "update runtime_sessions set status = 'stopping', updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now') where agent_id = $1 and runtime = 'claude'",
                )
                .bind(agent_id)
                .execute(&pool)
                .await;
                let _ = record_agent_activity(
                    &pool,
                    Some(agent_id),
                    None,
                    "run",
                    "Claude warm stream-json idle timeout",
                    format!("sent SIGTERM to process_group={pid}"),
                )
                .await;
            }
            Err(err) => {
                {
                    let mut state = runtime.state.lock().await;
                    state.alive = true;
                    state.last_activity = Instant::now();
                }
                let _ = record_agent_activity(
                    &pool,
                    Some(agent_id),
                    None,
                    "run_error",
                    "Claude warm stream-json idle stop failed",
                    err,
                )
                .await;
            }
        }
        return;
    }
}
