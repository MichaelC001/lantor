use serde::Serialize;
use tauri::State;
use uuid::Uuid;

use crate::{
    app::{AppState, CommandResult},
    channels::{
        create_channel_with_members, delete_channel_in_pool, open_dm_with_agent_in_pool,
        set_channel_agent_membership_in_pool, update_channel_in_pool,
    },
};

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CreateChannelResult {
    channel_id: Uuid,
}

#[tauri::command]
pub(crate) async fn create_channel(
    name: String,
    description: Option<String>,
    agent_ids: Option<Vec<Uuid>>,
    state: State<'_, AppState>,
) -> CommandResult<CreateChannelResult> {
    let channel_id = create_channel_with_members(
        &state.pool,
        &name,
        description.as_deref().unwrap_or(""),
        agent_ids,
    )
    .await?;
    Ok(CreateChannelResult { channel_id })
}

#[tauri::command]
pub(crate) async fn update_channel(
    channel_id: Uuid,
    name: String,
    description: String,
    state: State<'_, AppState>,
) -> CommandResult<()> {
    update_channel_in_pool(&state.pool, channel_id, name, description).await
}

#[tauri::command]
pub(crate) async fn set_channel_agent_membership(
    channel_id: Uuid,
    agent_id: Uuid,
    member: bool,
    state: State<'_, AppState>,
) -> CommandResult<()> {
    set_channel_agent_membership_in_pool(&state.pool, channel_id, agent_id, member).await
}

#[tauri::command]
pub(crate) async fn delete_channel(
    channel_id: Uuid,
    state: State<'_, AppState>,
) -> CommandResult<()> {
    delete_channel_in_pool(&state.pool, channel_id).await
}

#[tauri::command]
pub(crate) async fn open_dm_with_agent(
    agent_id: Uuid,
    state: State<'_, AppState>,
) -> CommandResult<String> {
    open_dm_with_agent_in_pool(&state.pool, agent_id).await
}
