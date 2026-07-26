use sqlx::SqlitePool;
use uuid::Uuid;

use crate::{
    app::CommandResult,
    bootstrap::{load_tauri_bootstrap, load_web_bootstrap},
    models::Bootstrap,
};

#[derive(Debug, Clone, Copy)]
pub(crate) enum BootstrapSurface {
    Tauri,
    Web,
}

#[derive(Debug)]
pub(crate) struct BootstrapRequest {
    pub(crate) channel_id: Option<Uuid>,
    pub(crate) current_channel_only: bool,
}

pub(crate) async fn bootstrap(
    pool: &SqlitePool,
    db_url: String,
    surface: BootstrapSurface,
    request: BootstrapRequest,
) -> CommandResult<Bootstrap> {
    match surface {
        BootstrapSurface::Tauri => {
            load_tauri_bootstrap(
                pool,
                db_url,
                request.channel_id,
                request.current_channel_only,
            )
            .await
        }
        BootstrapSurface::Web => {
            load_web_bootstrap(
                pool,
                db_url,
                request.channel_id,
                request.current_channel_only,
            )
            .await
        }
    }
}
