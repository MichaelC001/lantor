use tauri::State;
use uuid::Uuid;

use crate::{
    app::{AppState, CommandResult},
    application::channels::{
        self as application, ChannelIdRequest, CreateChannelRequest, CreateChannelResult,
        SetChannelAgentMembershipRequest, UpdateChannelRequest,
    },
    application::AgentIdRequest,
};

#[tauri::command]
pub(crate) async fn create_channel(
    name: String,
    description: Option<String>,
    agent_ids: Option<Vec<Uuid>>,
    state: State<'_, AppState>,
) -> CommandResult<CreateChannelResult> {
    application::create_channel(
        &state.pool,
        CreateChannelRequest {
            name,
            description,
            agent_ids,
        },
    )
    .await
}

#[tauri::command]
pub(crate) async fn update_channel(
    channel_id: Uuid,
    name: String,
    description: String,
    state: State<'_, AppState>,
) -> CommandResult<()> {
    application::update_channel(
        &state.pool,
        UpdateChannelRequest {
            channel_id,
            name,
            description,
        },
    )
    .await
}

#[tauri::command]
pub(crate) async fn set_channel_agent_membership(
    channel_id: Uuid,
    agent_id: Uuid,
    member: bool,
    state: State<'_, AppState>,
) -> CommandResult<()> {
    application::set_channel_agent_membership(
        &state.pool,
        SetChannelAgentMembershipRequest {
            channel_id,
            agent_id,
            member,
        },
    )
    .await
}

#[tauri::command]
pub(crate) async fn delete_channel(
    channel_id: Uuid,
    state: State<'_, AppState>,
) -> CommandResult<()> {
    application::delete_channel(&state.pool, ChannelIdRequest { channel_id }).await
}

#[tauri::command]
pub(crate) async fn open_dm_with_agent(
    agent_id: Uuid,
    state: State<'_, AppState>,
) -> CommandResult<String> {
    application::open_dm_with_agent(&state.pool, AgentIdRequest { agent_id }).await
}
