use std::time::Duration;

use serde_json::json;
use sqlx::{Row, SqlitePool};
use tauri::Emitter;
use tokio::time::sleep;
use uuid::Uuid;

use crate::agent_inbox_wake::sync_inbox_for_work_item;
use crate::message_store::load_message;
use crate::models::{AgentActivity, AgentRunPatch, AgentWorkItemPatch, Artifact, Message};
use crate::{to_string, CommandResult};

pub(crate) const UI_REFRESH_CHANNEL: &str = "lantor_ui_refresh";
const SUPERVISOR_WAKE_CHANNEL: &str = "lantor_supervisor_wake";
const UI_REFRESH_EVENT: &str = "lantor://refresh";

pub(crate) async fn notify_database_event(
    pool: &SqlitePool,
    channel: &str,
    payload: &str,
) -> CommandResult<()> {
    if channel != UI_REFRESH_CHANNEL {
        return Ok(());
    }
    sqlx::query("insert into ui_events (event_json) values ($1)")
        .bind(payload)
        .execute(pool)
        .await
        .map_err(to_string)?;

    Ok(())
}

pub(crate) async fn notify_ui_refresh(pool: &SqlitePool, reason: &str) -> CommandResult<()> {
    notify_database_event(
        pool,
        UI_REFRESH_CHANNEL,
        &json!({ "type": "refresh", "reason": reason }).to_string(),
    )
    .await
}

pub(crate) async fn notify_ui_message_upsert(
    pool: &SqlitePool,
    message: &Message,
    reason: &str,
) -> CommandResult<()> {
    notify_database_event(
        pool,
        UI_REFRESH_CHANNEL,
        &json!({ "type": "message_upsert", "reason": reason, "message": message }).to_string(),
    )
    .await
}

pub(crate) async fn notify_ui_message_delta(
    pool: &SqlitePool,
    message_id: Uuid,
    append: &str,
    delivery_state: &str,
    reason: &str,
) -> CommandResult<()> {
    notify_database_event(
        pool,
        UI_REFRESH_CHANNEL,
        &json!({
            "type": "message_delta",
            "reason": reason,
            "message_id": message_id,
            "append": append,
            "delivery_state": delivery_state
        })
        .to_string(),
    )
    .await
}

pub(crate) async fn notify_ui_message_delete(
    pool: &SqlitePool,
    message_id: Uuid,
    reason: &str,
) -> CommandResult<()> {
    notify_database_event(
        pool,
        UI_REFRESH_CHANNEL,
        &json!({ "type": "message_delete", "reason": reason, "message_id": message_id })
            .to_string(),
    )
    .await
}

pub(crate) async fn notify_ui_activity_upsert(
    pool: &SqlitePool,
    activity: &AgentActivity,
    reason: &str,
) -> CommandResult<()> {
    notify_database_event(
        pool,
        UI_REFRESH_CHANNEL,
        &json!({ "type": "activity_upsert", "reason": reason, "activity": activity }).to_string(),
    )
    .await
}

pub(crate) async fn notify_ui_agent_run_upsert(
    pool: &SqlitePool,
    run: &AgentRunPatch,
    reason: &str,
) -> CommandResult<()> {
    notify_database_event(
        pool,
        UI_REFRESH_CHANNEL,
        &json!({ "type": "agent_run_upsert", "reason": reason, "run": run }).to_string(),
    )
    .await
}

pub(crate) async fn notify_ui_work_item_upsert(
    pool: &SqlitePool,
    work_item: &AgentWorkItemPatch,
    reason: &str,
) -> CommandResult<()> {
    notify_database_event(
        pool,
        UI_REFRESH_CHANNEL,
        &json!({ "type": "work_item_upsert", "reason": reason, "work_item": work_item })
            .to_string(),
    )
    .await
}

pub(crate) async fn notify_ui_artifact_upsert(
    pool: &SqlitePool,
    artifact: &Artifact,
    reason: &str,
) -> CommandResult<()> {
    notify_database_event(
        pool,
        UI_REFRESH_CHANNEL,
        &json!({ "type": "artifact_upsert", "reason": reason, "artifact": artifact }).to_string(),
    )
    .await
}

pub(crate) async fn notify_ui_agent_run_changed(pool: &SqlitePool, run_id: Uuid, reason: &str) {
    if let Ok(run) = load_agent_run_patch(pool, run_id).await {
        let _ = notify_ui_agent_run_upsert(pool, &run, reason).await;
    } else {
        let _ = notify_ui_refresh(pool, reason).await;
    }
}

pub(crate) async fn notify_ui_work_item_changed(
    pool: &SqlitePool,
    work_item_id: Uuid,
    reason: &str,
) {
    let _ = sync_inbox_for_work_item(pool, work_item_id).await;
    if let Ok(work_item) = load_agent_work_item_patch(pool, work_item_id).await {
        let _ = notify_ui_work_item_upsert(pool, &work_item, reason).await;
        let _ = maybe_insert_work_item_system_message(pool, &work_item, reason).await;
    } else {
        let _ = notify_ui_refresh(pool, reason).await;
    }
}

pub(crate) async fn insert_system_message(
    pool: &SqlitePool,
    channel_id: Uuid,
    thread_root_id: Option<Uuid>,
    body: impl AsRef<str>,
) -> CommandResult<Uuid> {
    let body = body.as_ref().trim();
    if body.is_empty() {
        return Err("system message body is empty".to_owned());
    }
    let message_id: Uuid = sqlx::query_scalar(
        r#"
        insert into messages (channel_id, thread_root_id, sender_name, sender_role, body, is_task)
        values ($1, $2, 'Lantor', 'system', $3, false)
        returning id
        "#,
    )
    .bind(channel_id)
    .bind(thread_root_id)
    .bind(body)
    .fetch_one(pool)
    .await
    .map_err(to_string)?;

    if let Ok(message) = load_message(pool, message_id).await {
        let _ = notify_ui_message_upsert(pool, &message, "system_message").await;
    } else {
        let _ = notify_ui_refresh(pool, "system_message").await;
    }
    Ok(message_id)
}

async fn maybe_insert_work_item_system_message(
    pool: &SqlitePool,
    work_item: &AgentWorkItemPatch,
    reason: &str,
) -> CommandResult<()> {
    // Conversation-triggered agent turns are attention events, not timeline-level tasks.
    // Keep normal lifecycle messages for explicit task-backed work only; still surface
    // exceptional failures/cancellations for conversational turns.
    if work_item.task_number.is_none()
        && !matches!(reason, "work_item_failed" | "work_item_cancelled")
    {
        return Ok(());
    }
    if work_item.task_number.is_some()
        && matches!(
            reason,
            "work_item_created" | "work_item_queued" | "work_item_running"
        )
    {
        return Ok(());
    }
    if work_item.task_number.is_some() {
        if let Some(task_id) = work_item.task_id {
            let task_row = sqlx::query(
                "select coalesce(assignee_agent_id = $2, false) as is_assignee, status from tasks where id = $1",
            )
            .bind(task_id)
            .bind(work_item.agent_id)
            .fetch_optional(pool)
            .await
            .map_err(to_string)?;
            let Some(task_row) = task_row else {
                return Ok(());
            };
            if !task_row.get::<bool, _>("is_assignee") {
                return Ok(());
            }
            let task_status: String = task_row.get("status");
            if reason == "work_item_finished"
                && work_item.status == "done"
                && matches!(task_status.as_str(), "todo" | "in_progress")
            {
                return Ok(());
            }
        }
    }
    let Some(channel_id) = work_item.channel_id else {
        return Ok(());
    };
    let thread_root_id = work_item.thread_root_id.or(work_item.source_message_id);
    let object_label = work_item
        .task_number
        .map(|number| format!("task run for task #{number}"))
        .unwrap_or_else(|| "agent request".to_owned());
    let title = work_item.title.trim();
    let title_suffix = if title.is_empty() {
        String::new()
    } else {
        format!(": {title}")
    };
    let body = match reason {
        "work_item_created" | "work_item_queued" => {
            format!(
                "@{} queued {}{}",
                work_item.agent_handle, object_label, title_suffix
            )
        }
        "work_item_running" => {
            format!(
                "@{} started {}{}",
                work_item.agent_handle, object_label, title_suffix
            )
        }
        "work_item_cancelling" => {
            format!(
                "@{} is stopping {}{}",
                work_item.agent_handle, object_label, title_suffix
            )
        }
        "work_item_cancelled" => {
            format!(
                "@{} cancelled {}{}",
                work_item.agent_handle, object_label, title_suffix
            )
        }
        "work_item_failed" => {
            format!(
                "@{} failed {}{}",
                work_item.agent_handle, object_label, title_suffix
            )
        }
        "work_item_finished" => match work_item.status.as_str() {
            "done" => format!(
                "@{} completed {}{}",
                work_item.agent_handle, object_label, title_suffix
            ),
            "failed" => format!(
                "@{} failed {}{}",
                work_item.agent_handle, object_label, title_suffix
            ),
            "cancelled" => format!(
                "@{} cancelled {}{}",
                work_item.agent_handle, object_label, title_suffix
            ),
            "silent" => return Ok(()),
            _ => return Ok(()),
        },
        _ => return Ok(()),
    };
    insert_system_message(pool, channel_id, thread_root_id, body).await?;
    Ok(())
}

pub(crate) async fn notify_supervisor_wake(pool: &SqlitePool) -> CommandResult<()> {
    notify_database_event(pool, SUPERVISOR_WAKE_CHANNEL, "wake").await
}

pub(crate) fn spawn_ui_refresh_listener(app: tauri::AppHandle, pool: SqlitePool) {
    tauri::async_runtime::spawn(async move {
        let mut last_id: i64 = sqlx::query_scalar("select coalesce(max(id), 0) from ui_events")
            .fetch_one(&pool)
            .await
            .unwrap_or(0);
        loop {
            let rows = sqlx::query(
                r#"
                select id, event_json
                from ui_events
                where id > $1
                order by id asc
                limit 80
                "#,
            )
            .bind(last_id)
            .fetch_all(&pool)
            .await;
            match rows {
                Ok(rows) if rows.is_empty() => {
                    sleep(Duration::from_millis(150)).await;
                }
                Ok(rows) => {
                    let mut payloads = Vec::with_capacity(rows.len());
                    for row in rows {
                        last_id = row.get("id");
                        payloads.push(row.get::<String, _>("event_json"));
                    }
                    if payloads.len() == 1 {
                        if let Some(payload) = payloads.pop() {
                            let _ = app.emit(UI_REFRESH_EVENT, payload);
                        }
                    } else {
                        let _ = app.emit(
                            UI_REFRESH_EVENT,
                            json!({ "type": "batch", "events": payloads }).to_string(),
                        );
                    }
                }
                Err(err) => {
                    eprintln!("Lantor UI refresh poller failed: {err}");
                    sleep(Duration::from_secs(2)).await;
                }
            }
        }
    });
}

async fn load_agent_run_patch(pool: &SqlitePool, run_id: Uuid) -> CommandResult<AgentRunPatch> {
    let row = sqlx::query(
        r#"
        select
            r.id,
            r.agent_id,
            a.handle as agent_handle,
            r.work_item_id,
            r.command,
            r.working_directory,
            r.status,
            r.pid,
            r.exit_code,
            r.input_tokens,
            r.output_tokens,
            r.cost_micros,
            r.started_at,
            r.stopped_at
        from agent_runs r
        join agents a on a.id = r.agent_id
        where r.id = $1
        "#,
    )
    .bind(run_id)
    .fetch_one(pool)
    .await
    .map_err(to_string)?;

    Ok(AgentRunPatch {
        id: row.get("id"),
        agent_id: row.get("agent_id"),
        agent_handle: row.get("agent_handle"),
        work_item_id: row.get("work_item_id"),
        command: row.get("command"),
        working_directory: row.get("working_directory"),
        status: row.get("status"),
        pid: row.get("pid"),
        exit_code: row.get("exit_code"),
        input_tokens: row.get("input_tokens"),
        output_tokens: row.get("output_tokens"),
        cost_micros: row.get("cost_micros"),
        started_at: row.get("started_at"),
        stopped_at: row.get("stopped_at"),
    })
}

async fn load_agent_work_item_patch(
    pool: &SqlitePool,
    work_item_id: Uuid,
) -> CommandResult<AgentWorkItemPatch> {
    let row = sqlx::query(
        r#"
        select
            w.id,
            w.agent_id,
            a.handle as agent_handle,
            w.channel_id,
            c.name as channel_name,
            w.thread_root_id,
            w.source_message_id,
            w.inbox_item_id,
            w.task_id,
            t.number as task_number,
            w.source_kind,
            w.title,
            w.status,
            w.run_id,
            w.created_at,
            w.updated_at,
            w.completed_at
        from agent_work_items w
        join agents a on a.id = w.agent_id
        left join channels c on c.id = w.channel_id
        left join tasks t on t.id = w.task_id
        where w.id = $1
        "#,
    )
    .bind(work_item_id)
    .fetch_one(pool)
    .await
    .map_err(to_string)?;

    Ok(AgentWorkItemPatch {
        id: row.get("id"),
        agent_id: row.get("agent_id"),
        agent_handle: row.get("agent_handle"),
        channel_id: row.get("channel_id"),
        channel_name: row.get("channel_name"),
        thread_root_id: row.get("thread_root_id"),
        source_message_id: row.get("source_message_id"),
        inbox_item_id: row.get("inbox_item_id"),
        task_id: row.get("task_id"),
        task_number: row.get("task_number"),
        source_kind: row.get("source_kind"),
        title: row.get("title"),
        status: row.get("status"),
        run_id: row.get("run_id"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
        completed_at: row.get("completed_at"),
    })
}
