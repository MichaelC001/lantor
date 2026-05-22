use serde_json::{json, Value};
use sqlx::SqlitePool;
use uuid::Uuid;

use crate::app::{to_string, CommandResult};
use crate::message_store::{insert_agent_message, load_artifact, load_message};
use crate::text::compact_chars_middle;
use crate::ui_notifications::{
    notify_ui_artifact_upsert, notify_ui_message_upsert, notify_ui_refresh,
};

pub(crate) fn normalize_artifact_kind(kind: &str) -> CommandResult<String> {
    let normalized = kind.trim().to_lowercase().replace('_', "-");
    let normalized = match normalized.as_str() {
        "md" | "markdown" => "markdown",
        other => {
            return Err(format!(
                "unsupported artifact kind: {other}; supported: markdown"
            ));
        }
    };
    Ok(normalized.to_owned())
}

pub(crate) async fn create_agent_artifact(
    pool: &SqlitePool,
    agent_id: Uuid,
    channel_id: Uuid,
    thread_root_id: Option<Uuid>,
    kind: &str,
    title: &str,
    summary: Option<&str>,
    content: &str,
    metadata: Option<Value>,
) -> CommandResult<(Uuid, Uuid)> {
    let kind = normalize_artifact_kind(kind)?;
    let title = title.trim();
    if title.is_empty() {
        return Err("artifact_create title is required".to_owned());
    }
    let content = content.trim();
    if content.is_empty() {
        return Err("artifact_create content is required".to_owned());
    }
    let summary = summary
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| content.lines().next().unwrap_or(""))
        .to_owned();
    let summary = compact_chars_middle(&summary, 320).replace('\n', " ");
    let body = if summary.is_empty() {
        format!("Created artifact: {title}")
    } else {
        format!("Created artifact: {title}\n\n{summary}")
    };
    let message_id =
        insert_agent_message(pool, agent_id, channel_id, thread_root_id, &body, false).await?;
    let artifact_id: Uuid = sqlx::query_scalar(
        r#"
        insert into artifacts (
            message_id, channel_id, thread_root_id, creator_agent_id,
            kind, title, summary, content, metadata
        )
        values ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        returning id
        "#,
    )
    .bind(message_id)
    .bind(channel_id)
    .bind(thread_root_id)
    .bind(agent_id)
    .bind(&kind)
    .bind(title)
    .bind(&summary)
    .bind(content)
    .bind(metadata.unwrap_or_else(|| json!({})))
    .fetch_one(pool)
    .await
    .map_err(to_string)?;
    if let Ok(artifact) = load_artifact(pool, artifact_id).await {
        let _ = notify_ui_artifact_upsert(pool, &artifact, "artifact_created").await;
    }
    if let Ok(message) = load_message(pool, message_id).await {
        let _ = notify_ui_message_upsert(pool, &message, "artifact_created").await;
    } else {
        let _ = notify_ui_refresh(pool, "artifact_created").await;
    }
    Ok((artifact_id, message_id))
}
