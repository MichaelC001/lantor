use sqlx::{Row, SqlitePool};
use uuid::Uuid;

use crate::message_store::insert_agent_message;
use crate::ui_notifications::notify_ui_refresh;
use crate::{to_string, CommandResult};

pub(crate) async fn create_agent_task_thread(
    pool: &SqlitePool,
    agent_id: Uuid,
    channel_id: Uuid,
    title: &str,
    body: Option<&str>,
    thread_body: Option<&str>,
    assign_self: bool,
    status: Option<&str>,
) -> CommandResult<(i64, Uuid, Option<Uuid>)> {
    let title = title.trim();
    if title.is_empty() {
        return Err("task_create title is required".to_owned());
    }
    let final_status = status
        .map(str::trim)
        .filter(|status| !status.is_empty())
        .unwrap_or(if assign_self { "in_progress" } else { "todo" });
    if !matches!(final_status, "todo" | "in_progress" | "in_review" | "done") {
        return Err(format!("unsupported task status: {final_status}"));
    }
    let root_body = body
        .map(str::trim)
        .filter(|body| !body.is_empty())
        .unwrap_or(title);
    let root_message_id =
        insert_agent_message(pool, agent_id, channel_id, None, root_body, true).await?;
    let task_row = sqlx::query(
        r#"
        update tasks
        set title = $2,
            status = $3,
            assignee_agent_id = case when $4 then $5 else null end,
            version = version + 1,
            updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
        where message_id = $1
        returning number
        "#,
    )
    .bind(root_message_id)
    .bind(title)
    .bind(final_status)
    .bind(assign_self)
    .bind(agent_id)
    .fetch_one(pool)
    .await
    .map_err(to_string)?;
    let task_number: i64 = task_row.get("number");
    let thread_reply_id = match thread_body.map(str::trim).filter(|body| !body.is_empty()) {
        Some(thread_body) => Some(
            insert_agent_message(
                pool,
                agent_id,
                channel_id,
                Some(root_message_id),
                thread_body,
                false,
            )
            .await?,
        ),
        None => None,
    };
    let _ = notify_ui_refresh(pool, "task_create").await;
    Ok((task_number, root_message_id, thread_reply_id))
}
