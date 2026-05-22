use tauri::State;
use uuid::Uuid;

use crate::{
    app::{AppState, CommandResult},
    message_store::load_artifact,
    models::Artifact,
};

#[tauri::command]
pub(crate) async fn artifact_read(
    artifact_id: Uuid,
    state: State<'_, AppState>,
) -> CommandResult<Artifact> {
    load_artifact(&state.pool, artifact_id).await
}
