use tauri::State;
use uuid::Uuid;

use crate::{
    app::{AppState, CommandResult},
    bootstrap::load_tauri_bootstrap,
    models::Bootstrap,
};

#[tauri::command]
pub(crate) async fn bootstrap(
    channel_id: Option<Uuid>,
    current_channel_only: Option<bool>,
    state: State<'_, AppState>,
) -> CommandResult<Bootstrap> {
    load_tauri_bootstrap(
        &state.pool,
        state.db_url().to_owned(),
        channel_id,
        current_channel_only.unwrap_or(false),
    )
    .await
}
