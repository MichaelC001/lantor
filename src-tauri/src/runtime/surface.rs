use std::time::Duration;

use sqlx::{Row, SqlitePool};
use uuid::Uuid;

use crate::app::{to_string, CommandResult};
use crate::context_tool::short_id;
use crate::text::compact_chars_middle;

const CLAUDE_THREAD_CONTEXT_MESSAGE_LIMIT: i64 = 16;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CodexActiveTurnScheduleState {
    ReadyForSteer,
    WaitingForTurnId,
    StuckBeforeTurnId,
}

pub(crate) fn codex_active_turn_schedule_state(
    turn_id: Option<&str>,
    steer_disabled: bool,
    elapsed: Duration,
    turn_start_timeout: Duration,
) -> CodexActiveTurnScheduleState {
    if turn_id.is_some() && !steer_disabled {
        return CodexActiveTurnScheduleState::ReadyForSteer;
    }
    if turn_id.is_none() && elapsed >= turn_start_timeout {
        return CodexActiveTurnScheduleState::StuckBeforeTurnId;
    }
    CodexActiveTurnScheduleState::WaitingForTurnId
}

pub(crate) async fn same_codex_surface(
    _pool: &SqlitePool,
    channel_id: Option<Uuid>,
    thread_root_id: Option<Uuid>,
    active_channel_id: Option<Uuid>,
    active_thread_root_id: Option<Uuid>,
) -> CommandResult<bool> {
    if channel_id.is_none() || channel_id != active_channel_id {
        return Ok(false);
    }
    Ok(thread_root_id == active_thread_root_id)
}

pub(crate) async fn append_claude_thread_context(
    pool: &SqlitePool,
    context: &str,
    channel_id: Option<Uuid>,
    channel_name: Option<&str>,
    thread_root_id: Option<Uuid>,
) -> CommandResult<String> {
    let Some(channel_id) = channel_id else {
        return Ok(context.to_owned());
    };
    let rows = if let Some(thread_root_id) = thread_root_id {
        sqlx::query(
            r#"
            select id, sender_name, sender_role, body, thread_root_id
            from messages
            where channel_id = $1
              and (id = $2 or thread_root_id = $2)
              and length(trim(body)) > 0
            order by created_at desc
            limit $3
            "#,
        )
        .bind(channel_id)
        .bind(thread_root_id)
        .bind(CLAUDE_THREAD_CONTEXT_MESSAGE_LIMIT)
        .fetch_all(pool)
        .await
        .map_err(to_string)?
    } else {
        sqlx::query(
            r#"
            select id, sender_name, sender_role, body, thread_root_id
            from messages
            where channel_id = $1
              and thread_root_id is null
              and length(trim(body)) > 0
            order by created_at desc
            limit $2
            "#,
        )
        .bind(channel_id)
        .bind(CLAUDE_THREAD_CONTEXT_MESSAGE_LIMIT)
        .fetch_all(pool)
        .await
        .map_err(to_string)?
    };
    if rows.is_empty() {
        return Ok(context.to_owned());
    }

    let mut lines = vec![
        String::new(),
        "Same-thread recent context (auto-injected by Lantor):".to_owned(),
        "Use these messages as current-surface evidence. Resolve contextual follow-ups from this block when possible; if it is insufficient, use the Lantor history/search context tools before answering. Do not mix in older warm-runtime turns from other channels or threads unless they are explicitly quoted here.".to_owned(),
    ];
    for row in rows.iter().rev() {
        let message_id: Uuid = row.get("id");
        let row_thread_root_id: Option<Uuid> = row.get("thread_root_id");
        let sender_name: String = row.get("sender_name");
        let sender_role: String = row.get("sender_role");
        let body: String = row.get("body");
        let body = compact_chars_middle(body.trim(), 1_200).replace('\n', " ");
        lines.push(format!(
            "[target={} msg={} type={}] {}: {}",
            claude_context_target(channel_name, channel_id, thread_root_id, row_thread_root_id),
            short_id(message_id),
            sender_role,
            sender_name,
            body,
        ));
    }

    let mut enriched = context.trim().to_owned();
    if !enriched.is_empty() {
        enriched.push('\n');
    }
    enriched.push_str(&lines.join("\n"));
    Ok(enriched)
}

fn claude_context_target(
    channel_name: Option<&str>,
    channel_id: Uuid,
    prompt_thread_root_id: Option<Uuid>,
    row_thread_root_id: Option<Uuid>,
) -> String {
    let channel_label = channel_name
        .filter(|name| !name.trim().is_empty())
        .map(|name| format!("#{name}"))
        .unwrap_or_else(|| channel_id.to_string());
    prompt_thread_root_id
        .or(row_thread_root_id)
        .map(|thread_root_id| format!("{channel_label}:{}", short_id(thread_root_id)))
        .unwrap_or(channel_label)
}

#[cfg(test)]
#[path = "../tests/runtime_surface.rs"]
mod relocated_tests;
