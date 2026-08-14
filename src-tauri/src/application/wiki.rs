use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{Row, SqlitePool};
use uuid::Uuid;

use crate::{
    app::{to_string, CommandResult},
    channel_wiki::{
        announce_channel_wiki_publish, list_channel_wiki_revisions, load_channel_wiki_head,
        publish_channel_wiki_revision, short_revision_id, ChannelWikiPublishOutcome,
        ChannelWikiRevision, CHANNEL_WIKI_MAX_BYTES,
    },
    context_tool::escape_like_pattern,
};

const WIKI_HISTORY_LIMIT: i64 = 50;

/// Author recorded for wiki revisions published from the owner-facing UI.
/// Matches the label the context tool records for owner terminal edits.
const OWNER_WIKI_AUTHOR: &str = "owner";

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct LoadChannelWikiRequest {
    pub(crate) channel_id: Uuid,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PublishChannelWikiRequest {
    pub(crate) channel_id: Uuid,
    pub(crate) parent_id: Option<Uuid>,
    pub(crate) content: String,
    pub(crate) note: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct ChannelWikiRevisionView {
    pub(crate) id: Uuid,
    pub(crate) short_id: String,
    pub(crate) parent_short_id: Option<String>,
    pub(crate) content: String,
    pub(crate) author: String,
    pub(crate) note: String,
    pub(crate) created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ChannelWikiOverview {
    pub(crate) head: Option<ChannelWikiRevisionView>,
    pub(crate) revisions: Vec<ChannelWikiRevisionView>,
    pub(crate) max_bytes: usize,
}

#[derive(Debug, Serialize)]
pub(crate) struct PublishChannelWikiResult {
    /// "published" when the head advanced, "conflict" when `parent_id` no
    /// longer matched the head. On conflict the caller should show the
    /// returned overview's head, let the editor merge, and retry.
    pub(crate) outcome: &'static str,
    pub(crate) overview: ChannelWikiOverview,
}

fn revision_view(revision: ChannelWikiRevision) -> ChannelWikiRevisionView {
    ChannelWikiRevisionView {
        short_id: short_revision_id(revision.id),
        parent_short_id: revision.parent_id.map(short_revision_id),
        id: revision.id,
        content: revision.content,
        author: revision.author,
        note: revision.note,
        created_at: revision.created_at,
    }
}

pub(crate) async fn load_channel_wiki(
    pool: &SqlitePool,
    request: LoadChannelWikiRequest,
) -> CommandResult<ChannelWikiOverview> {
    let head = load_channel_wiki_head(pool, request.channel_id).await?;
    let revisions =
        list_channel_wiki_revisions(pool, request.channel_id, WIKI_HISTORY_LIMIT).await?;
    Ok(ChannelWikiOverview {
        head: head.map(revision_view),
        revisions: revisions.into_iter().map(revision_view).collect(),
        max_bytes: CHANNEL_WIKI_MAX_BYTES,
    })
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SearchChannelWikisRequest {
    pub(crate) query: String,
    #[serde(default)]
    pub(crate) limit: Option<i64>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ChannelWikiSearchHit {
    pub(crate) channel_id: Uuid,
    pub(crate) channel_name: String,
    pub(crate) channel_kind: String,
    pub(crate) rev_short_id: String,
    pub(crate) author: String,
    pub(crate) updated_at: DateTime<Utc>,
    pub(crate) snippet: String,
}

/// Search current wiki heads across all channels. Old revisions never match;
/// the wiki contract is that only the head is authoritative.
pub(crate) async fn search_channel_wikis(
    pool: &SqlitePool,
    request: SearchChannelWikisRequest,
) -> CommandResult<Vec<ChannelWikiSearchHit>> {
    let query = request.query.trim();
    if query.is_empty() {
        return Ok(Vec::new());
    }
    let limit = request.limit.unwrap_or(20).clamp(1, 50);
    let pattern = format!("%{}%", escape_like_pattern(query));
    let rows = sqlx::query(
        r#"
        select h.channel_id, c.name as channel_name, c.kind as channel_kind,
               r.id as rev_id, r.author, r.created_at, r.content
        from channel_wiki_heads h
        join channel_wiki_revisions r on r.id = h.head_id
        join channels c on c.id = h.channel_id
        where lower(r.content) like lower($1) escape '\'
        order by r.created_at desc
        limit $2
        "#,
    )
    .bind(&pattern)
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(to_string)?;

    let needle = query.to_lowercase();
    Ok(rows
        .iter()
        .map(|row| {
            let content: String = row.get("content");
            let snippet = content
                .lines()
                .find(|line| line.to_lowercase().contains(&needle))
                .or_else(|| content.lines().find(|line| !line.trim().is_empty()))
                .unwrap_or_default()
                .trim()
                .chars()
                .take(200)
                .collect::<String>();
            ChannelWikiSearchHit {
                channel_id: row.get("channel_id"),
                channel_name: row.get("channel_name"),
                channel_kind: row.get("channel_kind"),
                rev_short_id: short_revision_id(row.get("rev_id")),
                author: row.get("author"),
                updated_at: row.get("created_at"),
                snippet,
            }
        })
        .collect())
}

pub(crate) async fn publish_channel_wiki(
    pool: &SqlitePool,
    request: PublishChannelWikiRequest,
) -> CommandResult<PublishChannelWikiResult> {
    let outcome = publish_channel_wiki_revision(
        pool,
        request.channel_id,
        request.parent_id,
        &request.content,
        OWNER_WIKI_AUTHOR,
        &request.note,
    )
    .await?;
    let outcome_label = match &outcome {
        ChannelWikiPublishOutcome::Published(revision) => {
            announce_channel_wiki_publish(pool, request.channel_id, revision).await;
            "published"
        }
        ChannelWikiPublishOutcome::Conflict(_) => "conflict",
    };
    let overview = load_channel_wiki(
        pool,
        LoadChannelWikiRequest {
            channel_id: request.channel_id,
        },
    )
    .await?;
    Ok(PublishChannelWikiResult {
        outcome: outcome_label,
        overview,
    })
}
