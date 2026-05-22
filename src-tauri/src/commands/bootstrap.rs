use tauri::State;

use crate::{
    app::{AppState, CommandResult},
    bootstrap::load_bootstrap,
    models::Bootstrap,
};

#[tauri::command]
pub(crate) async fn bootstrap(state: State<'_, AppState>) -> CommandResult<Bootstrap> {
    load_bootstrap(&state.pool, state.db_url().to_owned()).await
}
