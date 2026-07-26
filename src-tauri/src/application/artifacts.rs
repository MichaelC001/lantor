use serde::Deserialize;
use sqlx::SqlitePool;
use uuid::Uuid;

use crate::{app::CommandResult, message_store::load_artifact, models::Artifact};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ArtifactReadRequest {
    pub(crate) artifact_id: Uuid,
}

pub(crate) async fn artifact_read(
    pool: &SqlitePool,
    request: ArtifactReadRequest,
) -> CommandResult<Artifact> {
    load_artifact(pool, request.artifact_id).await
}
