use tauri::State;
use uuid::Uuid;

use crate::{
    app::{AppState, CommandResult},
    application::wiki::{
        self as application, ChannelWikiOverview, LoadChannelWikiRequest,
        PublishChannelWikiRequest, PublishChannelWikiResult,
    },
};

#[tauri::command]
pub(crate) async fn load_channel_wiki(
    channel_id: Uuid,
    state: State<'_, AppState>,
) -> CommandResult<ChannelWikiOverview> {
    application::load_channel_wiki(&state.pool, LoadChannelWikiRequest { channel_id }).await
}

#[tauri::command]
pub(crate) async fn publish_channel_wiki(
    channel_id: Uuid,
    parent_id: Option<Uuid>,
    content: String,
    note: String,
    state: State<'_, AppState>,
) -> CommandResult<PublishChannelWikiResult> {
    application::publish_channel_wiki(
        &state.pool,
        PublishChannelWikiRequest {
            channel_id,
            parent_id,
            content,
            note,
        },
    )
    .await
}
