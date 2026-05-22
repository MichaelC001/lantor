use chrono::{DateTime, Utc};
use serde::Deserialize;
use tauri::State;
use uuid::Uuid;

use crate::{
    app::{AppState, CommandResult},
    owner_inbox::{
        dismiss_inbox_items_in_pool, mark_all_owner_inbox_read_in_pool, mark_channel_read_in_pool,
        mark_inbox_items_read_in_pool, update_thread_followed_in_pool,
    },
};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DismissInboxItemInput {
    item_id: String,
    dismissed_until: DateTime<Utc>,
}

#[tauri::command]
pub(crate) async fn mark_channel_read(
    channel_id: Uuid,
    state: State<'_, AppState>,
) -> CommandResult<()> {
    mark_channel_read_in_pool(&state.pool, channel_id).await
}

#[tauri::command]
pub(crate) async fn dismiss_inbox_items(
    items: Vec<DismissInboxItemInput>,
    state: State<'_, AppState>,
) -> CommandResult<()> {
    dismiss_inbox_items_in_pool(
        &state.pool,
        items
            .into_iter()
            .map(|item| (item.item_id, item.dismissed_until)),
    )
    .await
}

#[tauri::command]
pub(crate) async fn mark_inbox_items_read(
    items: Vec<DismissInboxItemInput>,
    state: State<'_, AppState>,
) -> CommandResult<()> {
    mark_inbox_items_read_in_pool(
        &state.pool,
        items
            .into_iter()
            .map(|item| (item.item_id, item.dismissed_until)),
    )
    .await
}

#[tauri::command]
pub(crate) async fn mark_all_inbox_read(state: State<'_, AppState>) -> CommandResult<()> {
    mark_all_owner_inbox_read_in_pool(&state.pool).await
}

#[tauri::command]
pub(crate) async fn update_thread_followed(
    thread_root_id: Uuid,
    followed: bool,
    state: State<'_, AppState>,
) -> CommandResult<()> {
    update_thread_followed_in_pool(&state.pool, thread_root_id, followed).await
}
