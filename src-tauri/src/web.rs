use std::{
    convert::Infallible,
    env,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
};

use axum::{
    body::Body,
    extract::{DefaultBodyLimit, Path as AxumPath, State},
    http::{header, HeaderValue, StatusCode},
    response::{
        sse::{Event, KeepAlive},
        IntoResponse, Response, Sse,
    },
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::{postgres::PgListener, PgPool, Row};
use tokio::net::TcpListener;
use tower_http::services::{ServeDir, ServeFile};
use uuid::Uuid;

use crate::models::AttachmentUpload;
use crate::{
    add_agent_to_channel, agent_workspace_list_in_pool, agent_workspace_read_file_in_pool,
    complete_reminder_in_pool, create_agent_in_pool, create_channel_in_pool, delete_agent_in_pool,
    delete_channel_in_pool, load_artifact, load_bootstrap, mark_channel_read_in_pool,
    open_dm_with_agent_in_pool, send_owner_message_in_pool, set_channel_agent_membership_in_pool,
    set_message_saved_in_pool, to_string, update_agent_in_pool, update_channel_in_pool,
    update_owner_profile_in_pool, UI_REFRESH_CHANNEL,
};

const WEB_SEND_MESSAGE_BODY_LIMIT: usize = 128 * 1024 * 1024;

#[derive(Clone)]
struct WebState {
    pool: PgPool,
    db_url: String,
}

#[derive(Serialize)]
struct ApiError {
    ok: bool,
    message: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SendMessageRequest {
    channel_id: Uuid,
    thread_root_id: Option<Uuid>,
    body: String,
    as_task: bool,
    attachments: Option<Vec<AttachmentUpload>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ChannelIdRequest {
    channel_id: Uuid,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateChannelRequest {
    name: String,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    agent_ids: Option<Vec<Uuid>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct UpdateChannelRequest {
    channel_id: Uuid,
    name: String,
    description: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SetChannelAgentMembershipRequest {
    channel_id: Uuid,
    agent_id: Uuid,
    member: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReminderIdRequest {
    reminder_id: Uuid,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ArtifactReadRequest {
    artifact_id: Uuid,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SetMessageSavedRequest {
    message_id: Uuid,
    saved: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AgentIdRequest {
    agent_id: Uuid,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateAgentRequest {
    handle: String,
    display_name: String,
    role: Option<String>,
    runtime: String,
    model: String,
    reasoning_effort: Option<String>,
    service_tier: Option<String>,
    avatar: Option<String>,
    description: Option<String>,
    launch_command: String,
    working_directory: String,
    daily_budget_micros: Option<i64>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct UpdateAgentRequest {
    agent_id: Uuid,
    handle: String,
    display_name: String,
    role: Option<String>,
    runtime: String,
    model: String,
    reasoning_effort: Option<String>,
    service_tier: Option<String>,
    avatar: Option<String>,
    description: String,
    launch_command: String,
    working_directory: String,
    daily_budget_micros: Option<i64>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AgentWorkspaceRequest {
    agent_id: Uuid,
    path: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OwnerProfileRequest {
    display_name: String,
    avatar: String,
    description: String,
}

pub(crate) const DEFAULT_LANTOR_WEB_BIND: &str = "0.0.0.0:8787";

pub(crate) fn resolve_web_bind() -> Option<String> {
    match env::var("LANTOR_WEB_BIND") {
        Ok(value) => {
            let trimmed = value.trim().to_owned();
            if trimmed.is_empty() {
                return Some(DEFAULT_LANTOR_WEB_BIND.to_owned());
            }
            if matches!(
                trimmed.to_ascii_lowercase().as_str(),
                "off" | "none" | "disabled" | "false" | "0"
            ) {
                return None;
            }
            Some(trimmed)
        }
        Err(_) => Some(DEFAULT_LANTOR_WEB_BIND.to_owned()),
    }
}

pub(crate) fn spawn_web_server_if_configured(pool: PgPool, db_url: String) {
    let Some(bind) = resolve_web_bind() else {
        return;
    };
    let Ok(addr) = bind.parse::<SocketAddr>() else {
        eprintln!("Lantor web access disabled: invalid LANTOR_WEB_BIND={bind}");
        return;
    };

    let dist_dir = web_dist_dir();
    tauri::async_runtime::spawn(async move {
        let state = Arc::new(WebState { pool, db_url });
        let app = web_router(state, dist_dir);
        match TcpListener::bind(addr).await {
            Ok(listener) => {
                eprintln!("Lantor web access listening on http://{addr}");
                if let Err(err) = axum::serve(
                    listener,
                    app.into_make_service_with_connect_info::<SocketAddr>(),
                )
                .await
                {
                    eprintln!("Lantor web access stopped: {err}");
                }
            }
            Err(err) => {
                eprintln!("Lantor web access failed to bind {addr}: {err}");
            }
        }
    });
}

fn web_router(state: Arc<WebState>, dist_dir: PathBuf) -> Router {
    let index = dist_dir.join("index.html");
    let app = Router::new()
        .route("/api/health", get(api_health))
        .route("/api/bootstrap", get(api_bootstrap))
        .route("/api/events", get(api_events))
        .route("/api/attachments/{attachment_id}", get(api_attachment))
        .route(
            "/api/send_message",
            post(api_send_message).layer(DefaultBodyLimit::max(WEB_SEND_MESSAGE_BODY_LIMIT)),
        )
        .route("/api/create_channel", post(api_create_channel))
        .route("/api/update_channel", post(api_update_channel))
        .route("/api/delete_channel", post(api_delete_channel))
        .route("/api/create_agent", post(api_create_agent))
        .route("/api/update_agent", post(api_update_agent))
        .route("/api/delete_agent", post(api_delete_agent))
        .route(
            "/api/set_channel_agent_membership",
            post(api_set_channel_agent_membership),
        )
        .route("/api/set_message_saved", post(api_set_message_saved))
        .route("/api/update_owner_profile", post(api_update_owner_profile))
        .route("/api/mark_channel_read", post(api_mark_channel_read))
        .route("/api/complete_reminder", post(api_complete_reminder))
        .route("/api/artifact_read", post(api_artifact_read))
        .route("/api/open_dm_with_agent", post(api_open_dm_with_agent))
        .route("/api/agent_workspace_list", post(api_agent_workspace_list))
        .route(
            "/api/agent_workspace_read_file",
            post(api_agent_workspace_read_file),
        )
        .with_state(state);

    if index.is_file() {
        app.fallback_service(ServeDir::new(&dist_dir).fallback(ServeFile::new(index)))
    } else {
        app.fallback(get(move || missing_dist(dist_dir)))
    }
}

fn web_dist_dir() -> PathBuf {
    if let Ok(path) = env::var("LANTOR_WEB_DIST") {
        let path = PathBuf::from(path);
        if path.is_dir() {
            return path;
        }
    }
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let candidates = [
        manifest_dir.join("../dist"),
        env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join("dist"),
    ];
    candidates
        .into_iter()
        .find(|path| path.join("index.html").is_file())
        .unwrap_or_else(|| manifest_dir.join("../dist"))
}

async fn missing_dist(dist_dir: PathBuf) -> impl IntoResponse {
    let body = format!(
        r#"<!doctype html>
<html>
  <head><title>Lantor Web</title></head>
  <body style="font-family: -apple-system, BlinkMacSystemFont, sans-serif; padding: 32px;">
    <h1>Lantor Web build not found</h1>
    <p>Expected <code>{}</code>.</p>
    <p>Run <code>npm run build</code>, then restart Lantor.</p>
  </body>
</html>"#,
        dist_dir.display()
    );
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        body,
    )
}

async fn api_health() -> impl IntoResponse {
    Json(json!({ "ok": true }))
}

async fn api_bootstrap(State(state): State<Arc<WebState>>) -> Result<impl IntoResponse, Response> {
    load_bootstrap(&state.pool, state.db_url.clone())
        .await
        .map(Json)
        .map_err(api_error)
}

async fn api_send_message(
    State(state): State<Arc<WebState>>,
    Json(request): Json<SendMessageRequest>,
) -> Result<impl IntoResponse, Response> {
    send_owner_message_in_pool(
        &state.pool,
        request.channel_id,
        request.thread_root_id,
        &request.body,
        request.as_task,
        request.attachments.unwrap_or_default(),
    )
    .await
    .map(|_| Json(json!({ "ok": true })))
    .map_err(api_error)
}

async fn api_create_channel(
    State(state): State<Arc<WebState>>,
    Json(request): Json<CreateChannelRequest>,
) -> Result<impl IntoResponse, Response> {
    let description = request.description.unwrap_or_default();
    let channel_id = create_channel_in_pool(&state.pool, &request.name, &description)
        .await
        .map_err(api_error)?;
    if let Some(ids) = request.agent_ids {
        let mut seen = std::collections::HashSet::new();
        for agent_id in ids {
            if !seen.insert(agent_id) {
                continue;
            }
            add_agent_to_channel(&state.pool, channel_id, agent_id)
                .await
                .map_err(api_error)?;
        }
    }
    Ok(Json(json!({ "ok": true, "channelId": channel_id })))
}

async fn api_update_channel(
    State(state): State<Arc<WebState>>,
    Json(request): Json<UpdateChannelRequest>,
) -> Result<impl IntoResponse, Response> {
    update_channel_in_pool(
        &state.pool,
        request.channel_id,
        request.name,
        request.description,
    )
    .await
    .map(|_| Json(json!({ "ok": true })))
    .map_err(api_error)
}

async fn api_delete_channel(
    State(state): State<Arc<WebState>>,
    Json(request): Json<ChannelIdRequest>,
) -> Result<impl IntoResponse, Response> {
    delete_channel_in_pool(&state.pool, request.channel_id)
        .await
        .map(|_| Json(json!({ "ok": true })))
        .map_err(api_error)
}

async fn api_create_agent(
    State(state): State<Arc<WebState>>,
    Json(request): Json<CreateAgentRequest>,
) -> Result<impl IntoResponse, Response> {
    create_agent_in_pool(
        &state.pool,
        request.handle,
        request.display_name,
        request.role,
        request.runtime,
        request.model,
        request.reasoning_effort,
        request.service_tier,
        request.avatar,
        request.description,
        request.launch_command,
        request.working_directory,
        request.daily_budget_micros,
    )
    .await
    .map(|agent_id| Json(agent_id.to_string()))
    .map_err(api_error)
}

async fn api_update_agent(
    State(state): State<Arc<WebState>>,
    Json(request): Json<UpdateAgentRequest>,
) -> Result<impl IntoResponse, Response> {
    update_agent_in_pool(
        &state.pool,
        request.agent_id,
        request.handle,
        request.display_name,
        request.role,
        request.runtime,
        request.model,
        request.reasoning_effort,
        request.service_tier,
        request.avatar,
        request.description,
        request.launch_command,
        request.working_directory,
        request.daily_budget_micros,
    )
    .await
    .map(|_| Json(json!({ "ok": true })))
    .map_err(api_error)
}

async fn api_delete_agent(
    State(state): State<Arc<WebState>>,
    Json(request): Json<AgentIdRequest>,
) -> Result<impl IntoResponse, Response> {
    delete_agent_in_pool(&state.pool, request.agent_id)
        .await
        .map(|_| Json(json!({ "ok": true })))
        .map_err(api_error)
}

async fn api_set_channel_agent_membership(
    State(state): State<Arc<WebState>>,
    Json(request): Json<SetChannelAgentMembershipRequest>,
) -> Result<impl IntoResponse, Response> {
    set_channel_agent_membership_in_pool(
        &state.pool,
        request.channel_id,
        request.agent_id,
        request.member,
    )
    .await
    .map(|_| Json(json!({ "ok": true })))
    .map_err(api_error)
}

async fn api_update_owner_profile(
    State(state): State<Arc<WebState>>,
    Json(request): Json<OwnerProfileRequest>,
) -> Result<impl IntoResponse, Response> {
    update_owner_profile_in_pool(
        &state.pool,
        request.display_name,
        request.avatar,
        request.description,
    )
    .await
    .map(|_| Json(json!({ "ok": true })))
    .map_err(api_error)
}

async fn api_mark_channel_read(
    State(state): State<Arc<WebState>>,
    Json(request): Json<ChannelIdRequest>,
) -> Result<impl IntoResponse, Response> {
    mark_channel_read_in_pool(&state.pool, request.channel_id)
        .await
        .map(|_| Json(json!({ "ok": true })))
        .map_err(api_error)
}

async fn api_set_message_saved(
    State(state): State<Arc<WebState>>,
    Json(request): Json<SetMessageSavedRequest>,
) -> Result<impl IntoResponse, Response> {
    set_message_saved_in_pool(&state.pool, request.message_id, request.saved)
        .await
        .map(|_| Json(json!({ "ok": true })))
        .map_err(api_error)
}

async fn api_complete_reminder(
    State(state): State<Arc<WebState>>,
    Json(request): Json<ReminderIdRequest>,
) -> Result<impl IntoResponse, Response> {
    complete_reminder_in_pool(&state.pool, request.reminder_id)
        .await
        .map(|_| Json(json!({ "ok": true })))
        .map_err(api_error)
}

async fn api_artifact_read(
    State(state): State<Arc<WebState>>,
    Json(request): Json<ArtifactReadRequest>,
) -> Result<impl IntoResponse, Response> {
    load_artifact(&state.pool, request.artifact_id)
        .await
        .map(Json)
        .map_err(api_error)
}

async fn api_open_dm_with_agent(
    State(state): State<Arc<WebState>>,
    Json(request): Json<AgentIdRequest>,
) -> Result<impl IntoResponse, Response> {
    open_dm_with_agent_in_pool(&state.pool, request.agent_id)
        .await
        .map(Json)
        .map_err(api_error)
}

async fn api_agent_workspace_list(
    State(state): State<Arc<WebState>>,
    Json(request): Json<AgentWorkspaceRequest>,
) -> Result<impl IntoResponse, Response> {
    agent_workspace_list_in_pool(&state.pool, request.agent_id, &request.path)
        .await
        .map(Json)
        .map_err(api_error)
}

async fn api_agent_workspace_read_file(
    State(state): State<Arc<WebState>>,
    Json(request): Json<AgentWorkspaceRequest>,
) -> Result<impl IntoResponse, Response> {
    agent_workspace_read_file_in_pool(&state.pool, request.agent_id, &request.path)
        .await
        .map(Json)
        .map_err(api_error)
}

async fn api_events(State(state): State<Arc<WebState>>) -> Result<impl IntoResponse, Response> {
    let db_url = state.db_url.clone();
    let stream = async_stream::stream! {
        match PgListener::connect(&db_url).await {
            Ok(mut listener) => {
                if let Err(err) = listener.listen(UI_REFRESH_CHANNEL).await {
                    yield Ok::<Event, Infallible>(Event::default().event("error").data(err.to_string()));
                    return;
                }
                loop {
                    match listener.recv().await {
                        Ok(notification) => {
                            yield Ok(Event::default().event("lantor").data(notification.payload().to_owned()));
                        }
                        Err(err) => {
                            yield Ok(Event::default().event("error").data(err.to_string()));
                            return;
                        }
                    }
                }
            }
            Err(err) => {
                yield Ok(Event::default().event("error").data(err.to_string()));
            }
        }
    };
    let response = Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response();
    let (mut parts, body) = response.into_parts();
    parts.headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache, no-transform"),
    );
    parts
        .headers
        .insert(header::CONNECTION, HeaderValue::from_static("keep-alive"));
    parts
        .headers
        .insert("x-accel-buffering", HeaderValue::from_static("no"));
    Ok(Response::from_parts(parts, body))
}

async fn api_attachment(
    State(state): State<Arc<WebState>>,
    AxumPath(attachment_id): AxumPath<Uuid>,
) -> Result<Response, Response> {
    let row = sqlx::query(
        r#"
        select original_name, mime_type, storage_path
        from message_attachments
        where id = $1
        "#,
    )
    .bind(attachment_id)
    .fetch_optional(&state.pool)
    .await
    .map_err(to_string)
    .map_err(api_error)?
    .ok_or_else(|| api_error("attachment does not exist".to_owned()))?;

    let original_name: String = row.get("original_name");
    let mime_type: String = row.get("mime_type");
    let storage_path: String = row.get("storage_path");
    let bytes = tokio::fs::read(Path::new(&storage_path))
        .await
        .map_err(to_string)
        .map_err(api_error)?;
    let content_type = if mime_type.trim().is_empty() {
        mime_guess::from_path(&storage_path)
            .first_or_octet_stream()
            .to_string()
    } else {
        mime_type
    };
    let mut response = Response::new(Body::from(bytes));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(&content_type)
            .unwrap_or(HeaderValue::from_static("application/octet-stream")),
    );
    response.headers_mut().insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_str(&format!(
            "inline; filename=\"{}\"",
            original_name.replace('"', "")
        ))
        .unwrap_or(HeaderValue::from_static("inline")),
    );
    Ok(response)
}

fn api_error(message: String) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(ApiError { ok: false, message }),
    )
        .into_response()
}
