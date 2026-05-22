use chrono::{DateTime, Utc};
use serde_json::json;
use sqlx::{Row, SqlitePool};
use tauri::State;
use uuid::Uuid;

use crate::events::activity::record_agent_activity;
use crate::models::AgentSchedule;
use crate::ui_notifications::{insert_system_message, notify_ui_refresh};
use crate::{
    app::{to_string, AppState, CommandResult},
    create_agent_inbox_item, ensure_agent_inbox_wake_work_item, AgentInboxItemInput,
};

use super::parse_due_at;

pub(super) async fn process_due_agent_schedules(pool: &SqlitePool) -> CommandResult<()> {
    let rows = sqlx::query(
        r#"
        update agent_schedules
        set last_run_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now'),
            next_run_at = case
                when cadence = 'hourly' then strftime('%Y-%m-%dT%H:%M:%f+00:00','now','+1 hour')
                when cadence = 'daily' then strftime('%Y-%m-%dT%H:%M:%f+00:00','now','+1 day')
                when cadence = 'weekly' then strftime('%Y-%m-%dT%H:%M:%f+00:00','now','+7 days')
                else strftime('%Y-%m-%dT%H:%M:%f+00:00','now','+1 day')
            end,
            updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
        where id in (
            select id
            from agent_schedules
            where status = 'active'
              and next_run_at <= strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
            order by next_run_at asc
            limit 8
        )
        returning
            id,
            agent_id,
            (select handle from agents where id = agent_schedules.agent_id) as agent_handle,
            channel_id,
            (select name from channels where id = agent_schedules.channel_id) as channel_name,
            thread_root_id,
            title,
            prompt,
            cadence,
            next_run_at
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(to_string)?;

    let fired_any = !rows.is_empty();
    for row in rows {
        let schedule_id: Uuid = row.get("id");
        let agent_id: Uuid = row.get("agent_id");
        let agent_handle: String = row.get("agent_handle");
        let channel_id: Uuid = row.get("channel_id");
        let channel_name: String = row.get("channel_name");
        let thread_root_id: Option<Uuid> = row.get("thread_root_id");
        let title: String = row.get("title");
        let prompt: String = row.get("prompt");
        let cadence: String = row.get("cadence");
        let next_run_at: DateTime<Utc> = row.get("next_run_at");

        let system_body = format!(
            "Scheduled routine for @{agent_handle}: {title}\nNext run: {}",
            next_run_at.to_rfc3339()
        );
        let source_message_id =
            insert_system_message(pool, channel_id, thread_root_id, system_body).await?;
        let work_thread_root_id = thread_root_id.or(Some(source_message_id));
        let inbox_item_id = create_agent_inbox_item(
            pool,
            AgentInboxItemInput {
                agent_id,
                channel_id: Some(channel_id),
                thread_root_id: work_thread_root_id,
                source_message_id: Some(source_message_id),
                task_id: None,
                kind: "schedule_due",
                priority: 75,
                title: &title,
                body_preview: &prompt,
                payload: json!({"schedule_id": schedule_id, "cadence": &cadence}),
            },
        )
        .await?;
        let wake = ensure_agent_inbox_wake_work_item(pool, agent_id).await?;
        let scheduled = wake.as_ref().is_some_and(|(_, scheduled)| *scheduled);
        let work_item_id = wake.map(|(work_item_id, _)| work_item_id);

        sqlx::query(
            "update agent_schedules set last_work_item_id = $2, updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now') where id = $1",
        )
        .bind(schedule_id)
        .bind(work_item_id)
        .execute(pool)
        .await
        .map_err(to_string)?;

        record_agent_activity(
            pool,
            Some(agent_id),
            None,
            "schedule",
            if scheduled {
                "Scheduled routine dispatched"
            } else {
                "Scheduled routine queued"
            },
            json!({
                "schedule_id": schedule_id,
                "work_item_id": work_item_id,
                "inbox_item_id": inbox_item_id,
                "channel": format!("#{channel_name}"),
                "cadence": cadence,
                "next_run_at": next_run_at.to_rfc3339()
            })
            .to_string(),
        )
        .await?;
    }
    if fired_any {
        let _ = notify_ui_refresh(pool, "agent_schedule_due").await;
    }
    Ok(())
}

fn normalize_schedule_cadence(value: &str) -> CommandResult<String> {
    let cadence = value.trim();
    if matches!(cadence, "hourly" | "daily" | "weekly") {
        Ok(cadence.to_owned())
    } else {
        Err(format!("unsupported schedule cadence: {cadence}"))
    }
}

#[tauri::command]
pub(crate) async fn create_agent_schedule(
    agent_id: Uuid,
    channel_id: Uuid,
    thread_root_id: Option<Uuid>,
    title: String,
    prompt: String,
    cadence: String,
    next_run_at: String,
    state: State<'_, AppState>,
) -> CommandResult<Uuid> {
    let title = title.trim();
    let prompt = prompt.trim();
    if title.is_empty() {
        return Err("schedule title is empty".to_owned());
    }
    if prompt.is_empty() {
        return Err("schedule prompt is empty".to_owned());
    }
    let next_run_at = parse_due_at(&next_run_at)?;
    let cadence = normalize_schedule_cadence(&cadence)?;

    let agent_exists: bool =
        sqlx::query_scalar("select exists(select 1 from agents where id = $1)")
            .bind(agent_id)
            .fetch_one(&state.pool)
            .await
            .map_err(to_string)?;
    if !agent_exists {
        return Err("agent does not exist".to_owned());
    }

    let channel_row = sqlx::query("select kind, dm_agent_id from channels where id = $1")
        .bind(channel_id)
        .fetch_optional(&state.pool)
        .await
        .map_err(to_string)?;
    let Some(channel_row) = channel_row else {
        return Err("channel does not exist".to_owned());
    };
    let channel_kind: String = channel_row.get("kind");
    let dm_agent_id: Option<Uuid> = channel_row.get("dm_agent_id");
    if channel_kind == "dm" && dm_agent_id != Some(agent_id) {
        return Err("direct message schedules must target their DM agent".to_owned());
    }

    if let Some(thread_root_id) = thread_root_id {
        let root_channel: Option<Uuid> = sqlx::query_scalar(
            "select channel_id from messages where id = $1 and thread_root_id is null",
        )
        .bind(thread_root_id)
        .fetch_optional(&state.pool)
        .await
        .map_err(to_string)?;
        if root_channel != Some(channel_id) {
            return Err("thread root does not belong to target channel".to_owned());
        }
    }

    if channel_kind != "dm" {
        sqlx::query(
            r#"
            insert into channel_members (channel_id, agent_id)
            values ($1, $2)
            on conflict (channel_id, agent_id) do nothing
            "#,
        )
        .bind(channel_id)
        .bind(agent_id)
        .execute(&state.pool)
        .await
        .map_err(to_string)?;
    }

    let schedule_id: Uuid = sqlx::query_scalar(
        r#"
        insert into agent_schedules (
            agent_id, channel_id, thread_root_id, title, prompt, cadence, status, next_run_at
        )
        values ($1, $2, $3, $4, $5, $6, 'active', $7)
        returning id
        "#,
    )
    .bind(agent_id)
    .bind(channel_id)
    .bind(thread_root_id)
    .bind(title)
    .bind(prompt)
    .bind(&cadence)
    .bind(next_run_at)
    .fetch_one(&state.pool)
    .await
    .map_err(to_string)?;

    record_agent_activity(
        &state.pool,
        Some(agent_id),
        None,
        "schedule",
        "Scheduled routine created",
        json!({
            "schedule_id": schedule_id,
            "cadence": cadence,
            "next_run_at": next_run_at.to_rfc3339()
        })
        .to_string(),
    )
    .await?;
    let _ = notify_ui_refresh(&state.pool, "agent_schedule_created").await;
    Ok(schedule_id)
}

#[tauri::command]
pub(crate) async fn update_agent_schedule_status(
    schedule_id: Uuid,
    status: String,
    state: State<'_, AppState>,
) -> CommandResult<()> {
    let status = status.trim();
    if !matches!(status, "active" | "paused" | "cancelled") {
        return Err(format!("unsupported schedule status: {status}"));
    }
    let row = sqlx::query(
        r#"
        update agent_schedules
        set status = $2,
            updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
        where id = $1 and status <> 'cancelled'
        returning agent_id
        "#,
    )
    .bind(schedule_id)
    .bind(status)
    .fetch_optional(&state.pool)
    .await
    .map_err(to_string)?;
    let Some(row) = row else {
        return Err("schedule does not exist or is already cancelled".to_owned());
    };
    let agent_id: Uuid = row.get("agent_id");
    record_agent_activity(
        &state.pool,
        Some(agent_id),
        None,
        "schedule",
        match status {
            "active" => "Scheduled routine resumed",
            "paused" => "Scheduled routine paused",
            "cancelled" => "Scheduled routine cancelled",
            _ => "Scheduled routine updated",
        },
        schedule_id.to_string(),
    )
    .await?;
    let _ = notify_ui_refresh(&state.pool, "agent_schedule_updated").await;
    Ok(())
}

pub(crate) async fn load_agent_schedules(pool: &SqlitePool) -> CommandResult<Vec<AgentSchedule>> {
    let rows = sqlx::query(
        r#"
        select
            s.id,
            s.agent_id,
            a.handle as agent_handle,
            s.channel_id,
            c.name as channel_name,
            c.kind as channel_kind,
            s.thread_root_id,
            s.title,
            s.prompt,
            s.cadence,
            s.status,
            s.next_run_at,
            s.last_run_at,
            s.last_work_item_id,
            s.created_at,
            s.updated_at
        from agent_schedules s
        join agents a on a.id = s.agent_id
        join channels c on c.id = s.channel_id
        where s.status in ('active', 'paused')
        order by
            case s.status when 'active' then 0 else 1 end,
            s.next_run_at asc
        limit 100
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(to_string)?;

    Ok(rows
        .into_iter()
        .map(|row| AgentSchedule {
            id: row.get("id"),
            agent_id: row.get("agent_id"),
            agent_handle: row.get("agent_handle"),
            channel_id: row.get("channel_id"),
            channel_name: row.get("channel_name"),
            channel_kind: row.get("channel_kind"),
            thread_root_id: row.get("thread_root_id"),
            title: row.get("title"),
            prompt: row.get("prompt"),
            cadence: row.get("cadence"),
            status: row.get("status"),
            next_run_at: row.get("next_run_at"),
            last_run_at: row.get("last_run_at"),
            last_work_item_id: row.get("last_work_item_id"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use std::fs as std_fs;

    use chrono::{DateTime, Utc};
    use sqlx::{Row, SqlitePool};
    use uuid::Uuid;

    use super::process_due_agent_schedules;
    use crate::db::{db_connect_with_url, migrate};

    #[tokio::test]
    async fn due_agent_schedule_dispatches_work_item() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let agent_id = insert_test_agent(&pool, "scheduler").await?;
            let channel_id = insert_test_channel(&pool, "scheduled").await?;
            let schedule_id: Uuid = sqlx::query_scalar(
                r#"
                insert into agent_schedules (
                    agent_id, channel_id, title, prompt, cadence, next_run_at, status
                )
                values ($1, $2, 'Daily check', 'Summarize open work', 'daily', strftime('%Y-%m-%dT%H:%M:%f+00:00','now','-1 minute'), 'active')
                returning id
                "#,
            )
            .bind(agent_id)
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;

            process_due_agent_schedules(&pool).await?;

            let schedule = sqlx::query(
                "select last_run_at, last_work_item_id, next_run_at > strftime('%Y-%m-%dT%H:%M:%f+00:00','now') as future from agent_schedules where id = $1",
            )
            .bind(schedule_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let last_run_at: Option<DateTime<Utc>> = schedule.get("last_run_at");
            let last_work_item_id: Option<Uuid> = schedule.get("last_work_item_id");
            let future: bool = schedule.get("future");
            assert!(last_run_at.is_some());
            assert!(last_work_item_id.is_some());
            assert!(future);

            let work_items: i64 = sqlx::query_scalar(
                r#"
                select count(*)
                from agent_work_items w
                join agent_inbox_items i on i.work_item_id = w.id
                where w.agent_id = $1
                  and w.channel_id = $2
                  and w.title = 'Process inbox: Daily check'
                  and w.context like '%Lantor agent inbox wake%'
                  and i.kind = 'schedule_due'
                "#,
            )
            .bind(agent_id)
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(work_items, 1);

            let system_messages: i64 = sqlx::query_scalar(
                r#"
                select count(*)
                from messages
                where channel_id = $1
                  and sender_role = 'system'
                  and body like 'Scheduled routine for @scheduler:%'
                "#,
            )
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(system_messages, 1);
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    async fn test_pool() -> Option<(SqlitePool, String)> {
        let database_path =
            std::env::temp_dir().join(format!("lantor-test-{}.sqlite", Uuid::new_v4().simple()));
        let database_path = database_path.to_string_lossy().into_owned();
        let database_url = format!("sqlite://{database_path}");
        let pool = match db_connect_with_url(&database_url, 1).await {
            Ok(pool) => pool,
            Err(err) => {
                eprintln!("skipping SQLite-backed Lantor test: {err}");
                return None;
            }
        };
        if let Err(err) = migrate(&pool).await {
            eprintln!("skipping SQLite-backed Lantor test: {err}");
            pool.close().await;
            drop_sqlite_test_files(&database_path);
            return None;
        }
        Some((pool, database_path))
    }

    fn drop_sqlite_test_files(database_path: &str) {
        let _ = std_fs::remove_file(database_path);
        let _ = std_fs::remove_file(format!("{database_path}-wal"));
        let _ = std_fs::remove_file(format!("{database_path}-shm"));
    }

    async fn drop_test_schema(pool: SqlitePool, database_path: String) {
        pool.close().await;
        drop_sqlite_test_files(&database_path);
    }

    async fn insert_test_agent(pool: &SqlitePool, handle: &str) -> Result<Uuid, String> {
        sqlx::query_scalar(
            r#"
            insert into agents (handle, display_name, role, status, runtime, model, avatar, description)
            values ($1, $2, 'agent', 'idle', 'codex', 'gpt-5.5', 'D', 'test agent')
            returning id
            "#,
        )
        .bind(handle)
        .bind(handle)
        .fetch_one(pool)
        .await
        .map_err(|err| err.to_string())
    }

    async fn insert_test_channel(pool: &SqlitePool, name: &str) -> Result<Uuid, String> {
        sqlx::query_scalar(
            r#"
            insert into channels (name, description, kind)
            values ($1, 'test channel', 'channel')
            returning id
            "#,
        )
        .bind(name)
        .fetch_one(pool)
        .await
        .map_err(|err| err.to_string())
    }
}
