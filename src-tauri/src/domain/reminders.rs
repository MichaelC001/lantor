use chrono::{DateTime, Utc};
use serde_json::json;
use sqlx::{Executor, Row, Sqlite, SqlitePool};
use tauri::State;
use uuid::Uuid;

use crate::events::activity::record_agent_activity;
use crate::models::Reminder;
use crate::ui_notifications::{enqueue_ui_event_in_tx, insert_system_message, UiEvent};
use crate::{
    app::{to_string, AppState, CommandResult},
    create_agent_inbox_item, ensure_agent_inbox_wake_work_item, AgentInboxItemInput,
};

use super::parse_due_at;

pub(super) async fn process_due_reminders(pool: &SqlitePool) -> CommandResult<()> {
    let mut transaction = pool.begin().await.map_err(to_string)?;
    let rows = sqlx::query(
        r#"
        update reminders
        set status = case when recurrence = 'none' then 'fired' else 'scheduled' end,
            fired_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now'),
            -- Anchor the next occurrence to the original due time instead of
            -- the processing time, so late processing (sleep, app closed)
            -- does not permanently drift the schedule. Skips missed periods.
            due_at = case
                when recurrence = 'daily' then
                    case
                        when julianday(coalesce(recurrence_anchor_at, due_at)) > julianday('now')
                            then coalesce(recurrence_anchor_at, due_at)
                        else strftime('%Y-%m-%dT%H:%M:%f+00:00',
                            coalesce(recurrence_anchor_at, due_at),
                            '+' || (
                                cast(
                                    julianday('now')
                                        - julianday(coalesce(recurrence_anchor_at, due_at))
                                    as integer
                                ) + 1
                            ) || ' days')
                    end
                when recurrence = 'weekly' then
                    case
                        when julianday(coalesce(recurrence_anchor_at, due_at)) > julianday('now')
                            then coalesce(recurrence_anchor_at, due_at)
                        else strftime('%Y-%m-%dT%H:%M:%f+00:00',
                            coalesce(recurrence_anchor_at, due_at),
                            '+' || (
                                (
                                    cast(
                                        (
                                            julianday('now')
                                                - julianday(coalesce(recurrence_anchor_at, due_at))
                                        ) / 7
                                        as integer
                                    ) + 1
                                ) * 7
                            ) || ' days')
                    end
                else due_at
            end,
            updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
        where id in (
            select id
            from reminders
            where status = 'scheduled'
              and due_at <= strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
            order by due_at asc
            limit 12
        )
        returning id, channel_id, creator_agent_id, thread_root_id, title, note, recurrence, status, due_at
        "#,
    )
    .fetch_all(&mut *transaction)
    .await
    .map_err(to_string)?;

    if !rows.is_empty() {
        enqueue_ui_event_in_tx(
            &mut transaction,
            &UiEvent::Refresh {
                reason: "reminder_due",
            },
        )
        .await?;
    }
    transaction.commit().await.map_err(to_string)?;
    for row in rows {
        let reminder_id: Uuid = row.get("id");
        let channel_id: Option<Uuid> = row.get("channel_id");
        let creator_agent_id: Option<Uuid> = row.get("creator_agent_id");
        let thread_root_id: Option<Uuid> = row.get("thread_root_id");
        let title: String = row.get("title");
        let note: String = row.get("note");
        let recurrence: String = row.get("recurrence");
        let status: String = row.get("status");
        let next_due_at: DateTime<Utc> = row.get("due_at");
        let fired_detail = if recurrence == "none" {
            String::new()
        } else {
            format!("next_due_at={}", next_due_at.to_rfc3339())
        };
        insert_reminder_event(pool, reminder_id, "fired", &fired_detail).await?;

        if let Some(channel_id) = channel_id {
            let mut body = format!("Reminder: {title}");
            if !note.trim().is_empty() {
                body.push_str(&format!("\n{}", note.trim()));
            }
            if recurrence != "none" && status == "scheduled" {
                body.push_str(&format!("\nNext reminder: {}", next_due_at.to_rfc3339()));
            }
            if let Ok(message_id) =
                insert_system_message(pool, channel_id, thread_root_id, body).await
            {
                if let Some(agent_id) = creator_agent_id {
                    let _ = dispatch_due_reminder_to_agent(
                        pool,
                        reminder_id,
                        agent_id,
                        channel_id,
                        thread_root_id,
                        message_id,
                        &title,
                        &note,
                    )
                    .await;
                }
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn dispatch_due_reminder_to_agent(
    pool: &SqlitePool,
    reminder_id: Uuid,
    agent_id: Uuid,
    channel_id: Uuid,
    thread_root_id: Option<Uuid>,
    source_message_id: Uuid,
    title: &str,
    note: &str,
) -> CommandResult<()> {
    sqlx::query(
        r#"
        insert into channel_members (channel_id, agent_id)
        values ($1, $2)
        on conflict (channel_id, agent_id) do nothing
        "#,
    )
    .bind(channel_id)
    .bind(agent_id)
    .execute(pool)
    .await
    .map_err(to_string)?;

    let work_thread_root_id = thread_root_id.or(Some(source_message_id));
    let inbox_item_id = create_agent_inbox_item(
        pool,
        AgentInboxItemInput {
            agent_id,
            channel_id: Some(channel_id),
            thread_root_id: work_thread_root_id,
            source_message_id: Some(source_message_id),
            task_id: None,
            kind: "reminder_due",
            priority: 90,
            title,
            body_preview: note,
            payload: json!({"reminder_id": reminder_id}),
        },
    )
    .await?;
    let wake = ensure_agent_inbox_wake_work_item(pool, agent_id).await?;
    let scheduled = wake.is_some_and(|(_, scheduled)| scheduled);
    record_agent_activity(
        pool,
        Some(agent_id),
        None,
        "reminder",
        if scheduled {
            "Reminder follow-up dispatched"
        } else {
            "Reminder follow-up queued"
        },
        json!({
            "reminder_id": reminder_id,
            "inbox_item_id": inbox_item_id,
            "source_message_id": source_message_id
        })
        .to_string(),
    )
    .await?;
    Ok(())
}

fn normalize_recurrence(value: &str) -> CommandResult<String> {
    let recurrence = value.trim();
    if matches!(recurrence, "none" | "daily" | "weekly") {
        Ok(recurrence.to_owned())
    } else if matches!(recurrence, "one_shot" | "one-shot" | "once") {
        Ok("none".to_owned())
    } else {
        Err(format!("unsupported reminder recurrence: {recurrence}"))
    }
}

async fn insert_reminder_event<'e, E>(
    executor: E,
    reminder_id: Uuid,
    event_type: &str,
    detail: &str,
) -> CommandResult<()>
where
    E: Executor<'e, Database = Sqlite>,
{
    sqlx::query(
        r#"
        insert into reminder_events (reminder_id, event_type, detail)
        values ($1, $2, $3)
        "#,
    )
    .bind(reminder_id)
    .bind(event_type)
    .bind(detail)
    .execute(executor)
    .await
    .map_err(to_string)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn create_reminder_in_pool(
    pool: &SqlitePool,
    creator_agent_id: Option<Uuid>,
    channel_id: Option<Uuid>,
    thread_root_id: Option<Uuid>,
    message_id: Option<Uuid>,
    title: &str,
    note: &str,
    due_at: DateTime<Utc>,
    recurrence: &str,
) -> CommandResult<Uuid> {
    let title = title.trim();
    if title.is_empty() {
        return Err("reminder title is empty".to_owned());
    }
    let recurrence = normalize_recurrence(recurrence)?;

    if let Some(thread_root_id) = thread_root_id {
        let exists: bool = sqlx::query_scalar(
            "select exists(select 1 from messages where id = $1 and thread_root_id is null)",
        )
        .bind(thread_root_id)
        .fetch_one(pool)
        .await
        .map_err(to_string)?;
        if !exists {
            return Err("thread root does not exist".to_owned());
        }
    }

    let mut transaction = pool.begin().await.map_err(to_string)?;
    let reminder_id: Uuid = sqlx::query_scalar(
        r#"
        insert into reminders (
            channel_id, creator_agent_id, thread_root_id, message_id, title, note,
            due_at, recurrence_anchor_at, recurrence, status
        )
        values ($1, $2, $3, $4, $5, $6, $7, $7, $8, 'scheduled')
        returning id
        "#,
    )
    .bind(channel_id)
    .bind(creator_agent_id)
    .bind(thread_root_id)
    .bind(message_id)
    .bind(title)
    .bind(note.trim())
    .bind(due_at)
    .bind(&recurrence)
    .fetch_one(&mut *transaction)
    .await
    .map_err(to_string)?;
    let created_detail = due_at.to_rfc3339();
    insert_reminder_event(&mut *transaction, reminder_id, "created", &created_detail).await?;
    enqueue_ui_event_in_tx(
        &mut transaction,
        &UiEvent::Refresh {
            reason: "reminder_created",
        },
    )
    .await?;
    transaction.commit().await.map_err(to_string)?;
    Ok(reminder_id)
}

pub(crate) async fn cancel_reminder_in_pool(
    pool: &SqlitePool,
    reminder_id: Uuid,
) -> CommandResult<()> {
    let mut transaction = pool.begin().await.map_err(to_string)?;
    let affected = sqlx::query(
        r#"
        update reminders
        set status = 'cancelled',
            completed_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now'),
            updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
        where id = $1 and status in ('scheduled', 'fired')
        "#,
    )
    .bind(reminder_id)
    .execute(&mut *transaction)
    .await
    .map_err(to_string)?
    .rows_affected();
    if affected == 0 {
        return Err("reminder does not exist or is not active".to_owned());
    }
    insert_reminder_event(&mut *transaction, reminder_id, "cancelled", "").await?;
    enqueue_ui_event_in_tx(
        &mut transaction,
        &UiEvent::Refresh {
            reason: "reminder_cancelled",
        },
    )
    .await?;
    transaction.commit().await.map_err(to_string)?;
    Ok(())
}

#[tauri::command]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn create_reminder(
    channel_id: Option<Uuid>,
    thread_root_id: Option<Uuid>,
    message_id: Option<Uuid>,
    title: String,
    note: String,
    due_at: String,
    recurrence: String,
    state: State<'_, AppState>,
) -> CommandResult<Uuid> {
    let due_at = parse_due_at(&due_at)?;
    create_reminder_in_pool(
        &state.pool,
        None,
        channel_id,
        thread_root_id,
        message_id,
        &title,
        &note,
        due_at,
        &recurrence,
    )
    .await
}

#[tauri::command]
pub(crate) async fn snooze_reminder(
    reminder_id: Uuid,
    minutes: i64,
    state: State<'_, AppState>,
) -> CommandResult<()> {
    if !(1..=10_080).contains(&minutes) {
        return Err("snooze minutes must be between 1 and 10080".to_owned());
    }
    let mut transaction = state.pool.begin().await.map_err(to_string)?;
    sqlx::query(
        r#"
        update reminders
        set status = 'scheduled',
            due_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now','+' || $2 || ' minutes'),
            updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
        where id = $1 and status in ('scheduled', 'fired')
        "#,
    )
    .bind(reminder_id)
    .bind(minutes)
    .execute(&mut *transaction)
    .await
    .map_err(to_string)?;
    let snooze_detail = format!("{minutes} minutes");
    insert_reminder_event(&mut *transaction, reminder_id, "snoozed", &snooze_detail).await?;
    enqueue_ui_event_in_tx(
        &mut transaction,
        &UiEvent::Refresh {
            reason: "reminder_snoozed",
        },
    )
    .await?;
    transaction.commit().await.map_err(to_string)?;
    Ok(())
}

#[tauri::command]
pub(crate) async fn complete_reminder(
    reminder_id: Uuid,
    state: State<'_, AppState>,
) -> CommandResult<()> {
    complete_reminder_in_pool(&state.pool, reminder_id).await
}

pub(crate) async fn complete_reminder_in_pool(
    pool: &SqlitePool,
    reminder_id: Uuid,
) -> CommandResult<()> {
    let mut transaction = pool.begin().await.map_err(to_string)?;
    sqlx::query(
        r#"
        update reminders
        set status = 'done',
            completed_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now'),
            updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
        where id = $1 and status in ('scheduled', 'fired')
        "#,
    )
    .bind(reminder_id)
    .execute(&mut *transaction)
    .await
    .map_err(to_string)?;
    insert_reminder_event(&mut *transaction, reminder_id, "completed", "").await?;
    enqueue_ui_event_in_tx(
        &mut transaction,
        &UiEvent::Refresh {
            reason: "reminder_completed",
        },
    )
    .await?;
    transaction.commit().await.map_err(to_string)?;
    Ok(())
}

#[tauri::command]
pub(crate) async fn cancel_reminder(
    reminder_id: Uuid,
    state: State<'_, AppState>,
) -> CommandResult<()> {
    cancel_reminder_in_pool(&state.pool, reminder_id).await
}

pub(crate) async fn load_reminders(pool: &SqlitePool) -> CommandResult<Vec<Reminder>> {
    let rows = sqlx::query(
        r#"
        select
            r.id,
            r.channel_id,
            c.name as channel_name,
            r.creator_agent_id,
            a.handle as creator_agent_handle,
            r.thread_root_id,
            r.message_id,
            r.title,
            r.note,
            r.status,
            r.recurrence,
            r.due_at,
            r.fired_at,
            r.completed_at,
            r.created_at,
            r.updated_at
        from reminders r
        left join channels c on c.id = r.channel_id
        left join agents a on a.id = r.creator_agent_id
        where r.status in ('scheduled', 'fired')
        order by
            case r.status when 'fired' then 0 else 1 end,
            r.due_at asc
        limit 100
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(to_string)?;

    Ok(rows
        .into_iter()
        .map(|row| Reminder {
            id: row.get("id"),
            channel_id: row.get("channel_id"),
            channel_name: row.get("channel_name"),
            creator_agent_id: row.get("creator_agent_id"),
            creator_agent_handle: row.get("creator_agent_handle"),
            thread_root_id: row.get("thread_root_id"),
            message_id: row.get("message_id"),
            title: row.get("title"),
            note: row.get("note"),
            status: row.get("status"),
            recurrence: row.get("recurrence"),
            due_at: row.get("due_at"),
            fired_at: row.get("fired_at"),
            completed_at: row.get("completed_at"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use std::fs as std_fs;

    use sqlx::SqlitePool;
    use uuid::Uuid;

    use super::process_due_reminders;
    use crate::db::{db_connect_with_url, migrate};

    #[tokio::test]
    async fn due_reminder_fires_system_message() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let channel_id = insert_test_channel(&pool, "reminders").await?;
            let reminder_id: Uuid = sqlx::query_scalar(
                r#"
                insert into reminders (channel_id, title, note, due_at, recurrence, status)
                values ($1, 'Check thread', 'Follow up with Dylan', strftime('%Y-%m-%dT%H:%M:%f+00:00','now','-1 minute'), 'none', 'scheduled')
                returning id
                "#,
            )
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;

            process_due_reminders(&pool).await?;

            let status: String = sqlx::query_scalar("select status from reminders where id = $1")
                .bind(reminder_id)
                .fetch_one(&pool)
                .await
                .map_err(|err| err.to_string())?;
            assert_eq!(status, "fired");
            let system_messages: i64 = sqlx::query_scalar(
                r#"
                select count(*)
                from messages
                where channel_id = $1
                  and sender_role = 'system'
                  and body like 'Reminder:%'
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

    #[tokio::test]
    async fn recurring_reminder_next_due_stays_anchored_to_original_time() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let channel_id = insert_test_channel(&pool, "reminder-anchor").await?;
            // Due 3 days ago at a fixed wall-clock time: late processing must
            // keep the original time-of-day and land strictly in the future.
            let reminder_id: Uuid = sqlx::query_scalar(
                r#"
                insert into reminders (
                    channel_id, title, note, due_at, recurrence_anchor_at, recurrence, status
                )
                values (
                    $1,
                    'Daily memory pass',
                    '',
                    strftime('%Y-%m-%dT%H:%M:%f+00:00','now','-1 minute'),
                    strftime('%Y-%m-%dT03:00:00.000+00:00','now','-3 days'),
                    'daily',
                    'scheduled'
                )
                returning id
                "#,
            )
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;

            process_due_reminders(&pool).await?;

            let (status, due_at): (String, String) =
                sqlx::query_as("select status, due_at from reminders where id = $1")
                    .bind(reminder_id)
                    .fetch_one(&pool)
                    .await
                    .map_err(|err| err.to_string())?;
            assert_eq!(status, "scheduled");
            assert!(
                due_at.contains("T03:00:00"),
                "next due should keep original time-of-day, got {due_at}"
            );
            let now: String =
                sqlx::query_scalar("select strftime('%Y-%m-%dT%H:%M:%f+00:00','now')")
                    .fetch_one(&pool)
                    .await
                    .map_err(|err| err.to_string())?;
            assert!(due_at > now, "next due {due_at} should be after now {now}");
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
