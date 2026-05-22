use sqlx::Row;
use tauri::State;
use uuid::Uuid;

use crate::{
    agent_work_dispatch::dispatch_agent_restart_backlog,
    app::{to_string, AppState, CommandResult},
    events::activity::record_agent_activity,
    launch_agent,
    models::LaunchAgentStatus,
    ui_notifications::{notify_supervisor_wake, notify_ui_agent_run_changed, notify_ui_refresh},
};

#[tauri::command]
pub(crate) async fn start_agent(agent_id: Uuid, state: State<'_, AppState>) -> CommandResult<()> {
    let active_run: Option<Uuid> = sqlx::query_scalar(
        r#"
        select id
        from agent_runs
        where agent_id = $1
          and stopped_at is null
          and status in ('starting', 'running', 'stopping')
        limit 1
        "#,
    )
    .bind(agent_id)
    .fetch_optional(&state.pool)
    .await
    .map_err(to_string)?;

    if active_run.is_some() {
        return Err("agent already has an active run".to_owned());
    }

    let pending_start: Option<Uuid> = sqlx::query_scalar(
        r#"
        select id
        from supervisor_commands
        where command_type = 'start_agent'
          and agent_id = $1
          and status in ('pending', 'running')
        limit 1
        "#,
    )
    .bind(agent_id)
    .fetch_optional(&state.pool)
    .await
    .map_err(to_string)?;

    if pending_start.is_some() {
        return Err("agent already has a pending start command".to_owned());
    }

    sqlx::query("update agents set status = 'queued' where id = $1")
        .bind(agent_id)
        .execute(&state.pool)
        .await
        .map_err(to_string)?;
    let (redispatched_tasks, inbox_wake_available) =
        dispatch_agent_restart_backlog(&state.pool, agent_id).await?;
    let pending_start_after_backlog: Option<Uuid> = sqlx::query_scalar(
        r#"
        select id
        from supervisor_commands
        where command_type = 'start_agent'
          and agent_id = $1
          and status in ('pending', 'running')
        limit 1
        "#,
    )
    .bind(agent_id)
    .fetch_optional(&state.pool)
    .await
    .map_err(to_string)?;
    if pending_start_after_backlog.is_none() {
        sqlx::query(
            r#"
            insert into supervisor_commands (command_type, agent_id)
            values ('start_agent', $1)
            "#,
        )
        .bind(agent_id)
        .execute(&state.pool)
        .await
        .map_err(to_string)?;
        let _ = notify_supervisor_wake(&state.pool).await;
        let _ = notify_ui_refresh(&state.pool, "supervisor_command").await;
    }
    record_agent_activity(
        &state.pool,
        Some(agent_id),
        None,
        "run",
        "Start queued",
        if redispatched_tasks > 0 || inbox_wake_available {
            format!(
                "Waiting for supervisor to launch the agent; redispatched {redispatched_tasks} unfinished task(s)"
            )
        } else {
            "Waiting for supervisor to launch the agent".to_owned()
        },
    )
    .await?;

    Ok(())
}

#[tauri::command]
pub(crate) async fn stop_agent(run_id: Uuid, state: State<'_, AppState>) -> CommandResult<()> {
    let row = sqlx::query(
        r#"
        select agent_id, pid, work_item_id
        from agent_runs
        where id = $1 and stopped_at is null
        "#,
    )
    .bind(run_id)
    .fetch_one(&state.pool)
    .await
    .map_err(to_string)?;

    let agent_id: Uuid = row.get("agent_id");
    sqlx::query(
        r#"
        insert into supervisor_commands (command_type, agent_id, run_id)
        values ('stop_run', $1, $2)
        "#,
    )
    .bind(agent_id)
    .bind(run_id)
    .execute(&state.pool)
    .await
    .map_err(to_string)?;
    let _ = notify_supervisor_wake(&state.pool).await;
    let _ = notify_ui_refresh(&state.pool, "supervisor_command").await;
    sqlx::query("update agent_runs set status = 'stopping' where id = $1")
        .bind(run_id)
        .execute(&state.pool)
        .await
        .map_err(to_string)?;
    notify_ui_agent_run_changed(&state.pool, run_id, "run_stopping").await;
    sqlx::query("update agents set status = 'stopping' where id = $1")
        .bind(agent_id)
        .execute(&state.pool)
        .await
        .map_err(to_string)?;
    record_agent_activity(
        &state.pool,
        Some(agent_id),
        Some(run_id),
        "run",
        "Stop requested",
        "Stop command queued for supervisor",
    )
    .await?;

    Ok(())
}

#[tauri::command]
pub(crate) async fn install_supervisor_service(
    state: State<'_, AppState>,
) -> CommandResult<LaunchAgentStatus> {
    let status = launch_agent::install_supervisor_service(state.db_url())?;
    let _ = notify_ui_refresh(&state.pool, "supervisor_service_installed").await;
    Ok(status)
}

#[tauri::command]
pub(crate) async fn uninstall_supervisor_service(
    state: State<'_, AppState>,
) -> CommandResult<LaunchAgentStatus> {
    let status = launch_agent::uninstall_supervisor_service()?;

    sqlx::query("update supervisor_state set status = 'offline', updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now') where id = 1")
        .execute(&state.pool)
        .await
        .map_err(to_string)?;

    let _ = notify_ui_refresh(&state.pool, "supervisor_service_uninstalled").await;
    Ok(status)
}
