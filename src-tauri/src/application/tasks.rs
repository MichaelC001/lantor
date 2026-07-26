use serde::Deserialize;
use sqlx::SqlitePool;
use uuid::Uuid;

use crate::{
    app::CommandResult,
    task_store::{update_task_status_in_pool, update_task_title_in_pool},
};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct UpdateTaskStatusRequest {
    pub(crate) task_id: Uuid,
    pub(crate) status: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct UpdateTaskTitleRequest {
    pub(crate) task_id: Uuid,
    pub(crate) title: String,
}

pub(crate) async fn update_task_status(
    pool: &SqlitePool,
    request: UpdateTaskStatusRequest,
) -> CommandResult<()> {
    update_task_status_in_pool(pool, request.task_id, request.status).await
}

pub(crate) async fn update_task_title(
    pool: &SqlitePool,
    request: UpdateTaskTitleRequest,
) -> CommandResult<()> {
    update_task_title_in_pool(pool, request.task_id, request.title).await
}
