use tauri::State;
use uuid::Uuid;

use crate::{
    app::{AppState, CommandResult},
    application::tasks::{self as application, UpdateTaskStatusRequest, UpdateTaskTitleRequest},
};

#[tauri::command]
pub(crate) async fn update_task_status(
    task_id: Uuid,
    status: String,
    state: State<'_, AppState>,
) -> CommandResult<()> {
    application::update_task_status(&state.pool, UpdateTaskStatusRequest { task_id, status }).await
}

#[tauri::command]
pub(crate) async fn update_task_title(
    task_id: Uuid,
    title: String,
    state: State<'_, AppState>,
) -> CommandResult<()> {
    application::update_task_title(&state.pool, UpdateTaskTitleRequest { task_id, title }).await
}
