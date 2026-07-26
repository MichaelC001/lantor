use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use uuid::Uuid;

use crate::{
    app::CommandResult,
    channels::{
        create_channel_with_members, delete_channel_in_pool, open_dm_with_agent_in_pool,
        set_channel_agent_membership_in_pool, update_channel_in_pool,
    },
};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CreateChannelRequest {
    pub(crate) name: String,
    #[serde(default)]
    pub(crate) description: Option<String>,
    #[serde(default)]
    pub(crate) agent_ids: Option<Vec<Uuid>>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CreateChannelResult {
    pub(crate) channel_id: Uuid,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct UpdateChannelRequest {
    pub(crate) channel_id: Uuid,
    pub(crate) name: String,
    pub(crate) description: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ChannelIdRequest {
    pub(crate) channel_id: Uuid,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SetChannelAgentMembershipRequest {
    pub(crate) channel_id: Uuid,
    pub(crate) agent_id: Uuid,
    pub(crate) member: bool,
}

pub(crate) async fn create_channel(
    pool: &SqlitePool,
    request: CreateChannelRequest,
) -> CommandResult<CreateChannelResult> {
    let channel_id = create_channel_with_members(
        pool,
        &request.name,
        request.description.as_deref().unwrap_or(""),
        request.agent_ids,
    )
    .await?;
    Ok(CreateChannelResult { channel_id })
}

pub(crate) async fn update_channel(
    pool: &SqlitePool,
    request: UpdateChannelRequest,
) -> CommandResult<()> {
    update_channel_in_pool(pool, request.channel_id, request.name, request.description).await
}

pub(crate) async fn set_channel_agent_membership(
    pool: &SqlitePool,
    request: SetChannelAgentMembershipRequest,
) -> CommandResult<()> {
    set_channel_agent_membership_in_pool(pool, request.channel_id, request.agent_id, request.member)
        .await
}

pub(crate) async fn delete_channel(
    pool: &SqlitePool,
    request: ChannelIdRequest,
) -> CommandResult<()> {
    delete_channel_in_pool(pool, request.channel_id).await
}

pub(crate) async fn open_dm_with_agent(
    pool: &SqlitePool,
    request: super::AgentIdRequest,
) -> CommandResult<String> {
    open_dm_with_agent_in_pool(pool, request.agent_id).await
}
