use serde::Deserialize;
use sqlx::SqlitePool;
use uuid::Uuid;

use crate::{
    agent_profile::{
        create_agent_in_pool, delete_agent_in_pool, update_agent_in_pool,
        update_owner_profile_in_pool,
    },
    app::CommandResult,
};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OwnerProfileRequest {
    pub(crate) display_name: String,
    pub(crate) avatar: String,
    pub(crate) description: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CreateAgentRequest {
    pub(crate) handle: String,
    pub(crate) display_name: String,
    pub(crate) role: Option<String>,
    pub(crate) runtime: String,
    pub(crate) model: String,
    pub(crate) reasoning_effort: Option<String>,
    pub(crate) service_tier: Option<String>,
    pub(crate) avatar: Option<String>,
    pub(crate) description: Option<String>,
    pub(crate) launch_command: String,
    pub(crate) environment_variables: Option<String>,
    pub(crate) working_directory: String,
    pub(crate) daily_budget_micros: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct UpdateAgentRequest {
    pub(crate) agent_id: Uuid,
    pub(crate) handle: String,
    pub(crate) display_name: String,
    pub(crate) role: Option<String>,
    pub(crate) runtime: String,
    pub(crate) model: String,
    pub(crate) reasoning_effort: Option<String>,
    pub(crate) service_tier: Option<String>,
    pub(crate) avatar: Option<String>,
    pub(crate) description: String,
    pub(crate) launch_command: String,
    pub(crate) environment_variables: Option<String>,
    pub(crate) working_directory: String,
    pub(crate) daily_budget_micros: Option<i64>,
}

pub(crate) async fn update_owner_profile(
    pool: &SqlitePool,
    request: OwnerProfileRequest,
) -> CommandResult<()> {
    update_owner_profile_in_pool(
        pool,
        request.display_name,
        request.avatar,
        request.description,
    )
    .await
}

pub(crate) async fn create_agent(
    pool: &SqlitePool,
    request: CreateAgentRequest,
) -> CommandResult<String> {
    create_agent_in_pool(
        pool,
        request.handle,
        request.display_name,
        request.role,
        request.runtime,
        request.model,
        request.reasoning_effort,
        request.service_tier,
        request.avatar,
        request.description,
        request.launch_command,
        request.environment_variables,
        request.working_directory,
        request.daily_budget_micros,
    )
    .await
    .map(|agent_id| agent_id.to_string())
}

pub(crate) async fn update_agent(
    pool: &SqlitePool,
    request: UpdateAgentRequest,
) -> CommandResult<()> {
    update_agent_in_pool(
        pool,
        request.agent_id,
        request.handle,
        request.display_name,
        request.role,
        request.runtime,
        request.model,
        request.reasoning_effort,
        request.service_tier,
        request.avatar,
        request.description,
        request.launch_command,
        request.environment_variables,
        request.working_directory,
        request.daily_budget_micros,
    )
    .await
}

pub(crate) async fn delete_agent(
    pool: &SqlitePool,
    request: super::AgentIdRequest,
) -> CommandResult<()> {
    delete_agent_in_pool(pool, request.agent_id).await
}
