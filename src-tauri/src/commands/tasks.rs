use tauri::State;
use uuid::Uuid;

use crate::{
    app::{AppState, CommandResult},
    task_store::{update_task_status_in_pool, update_task_title_in_pool},
};

#[tauri::command]
pub(crate) async fn update_task_status(
    task_id: Uuid,
    status: String,
    state: State<'_, AppState>,
) -> CommandResult<()> {
    update_task_status_in_pool(&state.pool, task_id, status).await
}

#[tauri::command]
pub(crate) async fn update_task_title(
    task_id: Uuid,
    title: String,
    state: State<'_, AppState>,
) -> CommandResult<()> {
    update_task_title_in_pool(&state.pool, task_id, title).await
}
