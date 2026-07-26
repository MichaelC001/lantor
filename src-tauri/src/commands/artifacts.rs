use tauri::State;
use uuid::Uuid;

use crate::{
    app::{AppState, CommandResult},
    application::artifacts::{self as application, ArtifactReadRequest},
    models::Artifact,
};

#[tauri::command]
pub(crate) async fn artifact_read(
    artifact_id: Uuid,
    state: State<'_, AppState>,
) -> CommandResult<Artifact> {
    application::artifact_read(&state.pool, ArtifactReadRequest { artifact_id }).await
}
