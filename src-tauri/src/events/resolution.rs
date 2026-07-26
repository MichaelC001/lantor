use sqlx::{Row, SqlitePool};
use uuid::Uuid;

use crate::{
    app::{to_string, CommandResult},
    channels::normalize_channel_name,
};

pub(crate) async fn resolve_run_reminder_anchor(
    pool: &SqlitePool,
    agent_id: Uuid,
    run_id: Uuid,
) -> CommandResult<(Option<Uuid>, Option<Uuid>, Option<Uuid>)> {
    let row = sqlx::query(
        r#"
        select w.channel_id, w.thread_root_id, w.source_message_id
        from agent_runs r
        left join agent_work_items w on w.id = r.work_item_id
        where r.id = $1 and r.agent_id = $2
        "#,
    )
    .bind(run_id)
    .bind(agent_id)
    .fetch_optional(pool)
    .await
    .map_err(to_string)?;
    Ok(row
        .map(|row| {
            (
                row.get("channel_id"),
                row.get("thread_root_id"),
                row.get("source_message_id"),
            )
        })
        .unwrap_or((None, None, None)))
}

pub(crate) async fn resolve_event_channel(
    pool: &SqlitePool,
    channel_id: Option<Uuid>,
    channel_name: Option<&str>,
) -> CommandResult<Uuid> {
    if let Some(channel_id) = channel_id {
        let resolved: Option<Uuid> = sqlx::query_scalar(
            r#"
            select id
            from channels
            where id = $1 or (kind = 'dm' and dm_agent_id = $1)
            order by case when id = $1 then 0 else 1 end
            limit 1
            "#,
        )
        .bind(channel_id)
        .fetch_optional(pool)
        .await
        .map_err(to_string)?;
        return resolved.ok_or_else(|| format!("channel {channel_id} does not exist"));
    }

    let Some(name) = channel_name else {
        return Err("message event requires channel or channel_id".to_owned());
    };
    let normalized = normalize_channel_name(name);
    if normalized.is_empty() {
        return Err("message event channel is empty".to_owned());
    }
    sqlx::query_scalar("select id from channels where name = $1")
        .bind(&normalized)
        .fetch_optional(pool)
        .await
        .map_err(to_string)?
        .ok_or_else(|| format!("channel #{normalized} does not exist"))
}
