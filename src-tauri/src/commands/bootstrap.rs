use tauri::State;
use uuid::Uuid;

use crate::{
    app::{AppState, CommandResult},
    application::bootstrap::{self as application, BootstrapRequest, BootstrapSurface},
    models::Bootstrap,
};

#[tauri::command]
pub(crate) async fn bootstrap(
    channel_id: Option<Uuid>,
    current_channel_only: Option<bool>,
    state: State<'_, AppState>,
) -> CommandResult<Bootstrap> {
    application::bootstrap(
        &state.pool,
        state.db_url().to_owned(),
        BootstrapSurface::Tauri,
        BootstrapRequest {
            channel_id,
            current_channel_only: current_channel_only.unwrap_or(false),
        },
    )
    .await
}
