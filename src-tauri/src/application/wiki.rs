use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use uuid::Uuid;

use crate::{
    app::CommandResult,
    channel_wiki::{
        announce_channel_wiki_publish, list_channel_wiki_revisions, load_channel_wiki_head,
        publish_channel_wiki_revision, short_revision_id, ChannelWikiPublishOutcome,
        ChannelWikiRevision, CHANNEL_WIKI_MAX_BYTES,
    },
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
