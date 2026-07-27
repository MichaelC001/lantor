use serde::Deserialize;
use sqlx::SqlitePool;
use uuid::Uuid;

use crate::{
    agent_inbox_wake::agent_accepts_new_work,
    agent_work_dispatch::dispatch_task_assignment_to_agent,
    app::{to_string, CommandResult},
    github::{
        bind_github_repository_in_pool, create_github_review_task_record, github_account,
        load_cached_github_channel_overview, load_existing_github_review_task, load_github_binding,
        load_github_pull_request, refresh_github_channel_overview, GithubChannelOverview,
        GithubRepositoryBinding, GithubReviewTaskResult,
    },
};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GithubChannelRequest {
    pub(crate) channel_id: Uuid,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BindGithubRepositoryRequest {
    pub(crate) channel_id: Uuid,
    pub(crate) repository: String,
    #[serde(default)]
    pub(crate) local_path: Option<String>,
    #[serde(default)]
    pub(crate) review_login: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CreateGithubReviewTaskRequest {
    pub(crate) channel_id: Uuid,
    pub(crate) pull_number: i64,
    pub(crate) agent_id: Uuid,
}

pub(crate) async fn load_github_review_queue(
    pool: &SqlitePool,
    request: GithubChannelRequest,
) -> CommandResult<GithubChannelOverview> {
    load_cached_github_channel_overview(pool, request.channel_id).await
}

pub(crate) async fn refresh_github_review_queue(
    pool: &SqlitePool,
    request: GithubChannelRequest,
) -> CommandResult<GithubChannelOverview> {
    refresh_github_channel_overview(pool, request.channel_id).await
}

pub(crate) async fn bind_github_repository(
    pool: &SqlitePool,
    request: BindGithubRepositoryRequest,
) -> CommandResult<GithubRepositoryBinding> {
    bind_github_repository_in_pool(
        pool,
        request.channel_id,
        &request.repository,
        request.local_path.as_deref(),
        request.review_login.as_deref(),
    )
    .await
}

pub(crate) async fn create_github_review_task(
    pool: &SqlitePool,
    request: CreateGithubReviewTaskRequest,
) -> CommandResult<GithubReviewTaskResult> {
    let binding = load_github_binding(pool, request.channel_id)
        .await?
        .ok_or_else(|| "channel has no GitHub repository binding".to_owned())?;
    if let Some(existing) = load_existing_github_review_task(
        pool,
        request.channel_id,
        &binding.repository_id,
        request.pull_number,
    )
    .await?
    {
        return Ok(existing);
    }

    let agent_handle: Option<String> =
        sqlx::query_scalar("select handle from agents where id = $1")
            .bind(request.agent_id)
            .fetch_optional(pool)
            .await
            .map_err(to_string)?;
    let Some(agent_handle) = agent_handle else {
        return Err("agent does not exist".to_owned());
    };
    if !agent_accepts_new_work(pool, request.agent_id).await? {
        return Err(format!(
            "agent @{agent_handle} is in error state and cannot accept new work"
        ));
    }

    // Re-check the authenticated account before fetching PR metadata so auth
    // failures are reported before any local task state is created.
    let _ = github_account().await?;
    let pull_request = load_github_pull_request(&binding, request.pull_number).await?;
    if !pull_request.is_open() {
        return Err("pull request is no longer open".to_owned());
    }
    let result = create_github_review_task_record(
        pool,
        request.channel_id,
        request.agent_id,
        &binding,
        &pull_request,
    )
    .await?;
    if result.created {
        dispatch_task_assignment_to_agent(pool, result.task_id, request.agent_id, "github_review")
            .await?;
    }
    Ok(result)
}
