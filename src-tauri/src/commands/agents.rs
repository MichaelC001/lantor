use tauri::State;
use uuid::Uuid;

use crate::{
    agent_profile::{
        create_agent_in_pool, delete_agent_in_pool, update_agent_in_pool,
        update_owner_profile_in_pool,
    },
    app::{AppState, CommandResult},
};

#[tauri::command]
pub(crate) async fn update_owner_profile(
    display_name: String,
    avatar: String,
    description: String,
    state: State<'_, AppState>,
) -> CommandResult<()> {
    update_owner_profile_in_pool(&state.pool, display_name, avatar, description).await
}

#[tauri::command]
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
    working_directory: String,
    daily_budget_micros: Option<i64>,
    state: State<'_, AppState>,
) -> CommandResult<String> {
    create_agent_in_pool(
        &state.pool,
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
        working_directory,
        daily_budget_micros,
    )
    .await
    .map(|agent_id| agent_id.to_string())
}

#[tauri::command]
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
    working_directory: String,
    daily_budget_micros: Option<i64>,
    state: State<'_, AppState>,
) -> CommandResult<()> {
    update_agent_in_pool(
        &state.pool,
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
        working_directory,
        daily_budget_micros,
    )
    .await
}

#[tauri::command]
pub(crate) async fn delete_agent(agent_id: Uuid, state: State<'_, AppState>) -> CommandResult<()> {
    delete_agent_in_pool(&state.pool, agent_id).await
}
