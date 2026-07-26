use tauri::State;
use uuid::Uuid;

use crate::{
    app::{AppState, CommandResult},
    application::agents::{
        self as application, CreateAgentRequest, OwnerProfileRequest, UpdateAgentRequest,
    },
    application::AgentIdRequest,
};

#[tauri::command]
pub(crate) async fn update_owner_profile(
    display_name: String,
    avatar: String,
    description: String,
    state: State<'_, AppState>,
) -> CommandResult<()> {
    application::update_owner_profile(
        &state.pool,
        OwnerProfileRequest {
            display_name,
            avatar,
            description,
        },
    )
    .await
}

#[tauri::command]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn create_agent(
    handle: String,
    display_name: String,
    role: Option<String>,
    runtime: String,
    model: String,
    reasoning_effort: Option<String>,
    service_tier: Option<String>,
    avatar: Option<String>,
    description: Option<String>,
    launch_command: String,
    environment_variables: Option<String>,
    working_directory: String,
    daily_budget_micros: Option<i64>,
    state: State<'_, AppState>,
) -> CommandResult<String> {
    application::create_agent(
        &state.pool,
        CreateAgentRequest {
            handle,
            display_name,
            role,
            runtime,
            model,
            reasoning_effort,
            service_tier,
            avatar,
            description,
            launch_command,
            environment_variables,
            working_directory,
            daily_budget_micros,
        },
    )
    .await
}

#[tauri::command]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn update_agent(
    agent_id: Uuid,
    handle: String,
    display_name: String,
    role: Option<String>,
    runtime: String,
    model: String,
    reasoning_effort: Option<String>,
    service_tier: Option<String>,
    avatar: Option<String>,
    description: String,
    launch_command: String,
    environment_variables: Option<String>,
    working_directory: String,
    daily_budget_micros: Option<i64>,
    state: State<'_, AppState>,
) -> CommandResult<()> {
    application::update_agent(
        &state.pool,
        UpdateAgentRequest {
            agent_id,
            handle,
            display_name,
            role,
            runtime,
            model,
            reasoning_effort,
            service_tier,
            avatar,
            description,
            launch_command,
            environment_variables,
            working_directory,
            daily_budget_micros,
        },
    )
    .await
}

#[tauri::command]
pub(crate) async fn delete_agent(agent_id: Uuid, state: State<'_, AppState>) -> CommandResult<()> {
    application::delete_agent(&state.pool, AgentIdRequest { agent_id }).await
}
