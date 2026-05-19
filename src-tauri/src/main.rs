#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod agent_event;
mod attachments;
mod context_tool;
mod launch_agent;
mod models;
mod prompts;
mod text;
mod usage;
mod web;

use std::{
    collections::{HashMap, HashSet},
    env, fs,
    net::{IpAddr, SocketAddr},
    path::{Component, Path, PathBuf},
    process::{Command as StdCommand, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{
    sqlite::{
        SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow, SqliteSynchronous,
    },
    Row, SqlitePool,
};
#[cfg(unix)]
use std::os::fd::AsRawFd;
use std::str::FromStr;
use tauri::{Emitter, Manager, State};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncWriteExt, BufReader},
    process::Command,
    sync::{Mutex as AsyncMutex, Semaphore},
    time::sleep,
};
use uuid::Uuid;

use agent_event::{AgentAttachmentFile, AgentEvent};
use attachments::{write_attachment_file, ATTACHMENT_SIZE_LIMIT};
use context_tool::{run_agent_context_tool, short_id};
use models::{
    Agent, AgentActivity, AgentRun, AgentRunPatch, AgentSchedule, AgentWorkItem,
    AgentWorkItemPatch, AgentWorkspaceEntry, AgentWorkspaceFile, AgentWorkspaceListing, Artifact,
    AttachmentUpload, Bootstrap, Channel, ChannelMember, LaunchAgentStatus, Message,
    MessageAttachment, OwnerProfile, Reminder, RuntimeCheck, SavedMessage, SupervisorCommand,
    SupervisorStatus, Task,
};
use prompts::{
    build_claude_streaming_prompt, build_codex_streaming_prompt, build_streaming_work_item_prompt,
    build_work_item_prompt, claude_system_prompt, codex_developer_instructions,
    ensure_agent_workspace, load_agent_memory_context, prepend_memory_context,
};
#[cfg(test)]
use prompts::{AGENT_MEMORY_CONTEXT_LIMIT, WORK_ITEM_FINISH_PROMPT};
use text::compact_chars_middle;
use usage::{
    agent_budget_exhausted, backfill_agent_run_usage_from_logs, record_run_usage,
    usage_from_runtime_event,
};

const DEFAULT_DATABASE_URL: &str = "sqlite://~/Library/Application Support/Lantor/lantor.sqlite";
const AGENT_EVENT_PREFIX: &str = "LANTOR_EVENT ";
const SILENT_REPLY_PREFIX: &str = "LANTOR_SILENT_REPLY";
pub(crate) const UI_REFRESH_CHANNEL: &str = "lantor_ui_refresh";
const SUPERVISOR_WAKE_CHANNEL: &str = "lantor_supervisor_wake";
const UI_REFRESH_EVENT: &str = "lantor://refresh";
const LANTOR_CONTEXT_TOOL_ENV: &str = "LANTOR_CONTEXT_TOOL";
const STREAMING_MESSAGE_BODY_LIMIT: usize = 200_000;
const CLAUDE_MAX_RETRIES_ENV: &str = "ANTHROPIC_MAX_RETRIES";
const DEFAULT_CLAUDE_MAX_RETRIES: &str = "3";
const STREAMING_TRUNCATION_MARKER: &str = "\n\n[stream truncated by Lantor]";
const DISPATCH_MESSAGE_BODY_LIMIT: usize = 4 * 1024;
const CLAUDE_THREAD_CONTEXT_MESSAGE_LIMIT: i64 = 16;
const INBOX_WAKE_BATCH_LIMIT: i64 = 8;
const INBOX_WAKE_OTHER_SUMMARY_LIMIT: i64 = 6;
const AGENT_CONTEXT_TOOL_MESSAGE_LIMIT: usize = 2_000;
const CODEX_IDLE_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const CODEX_IDLE_REAPER_INTERVAL: Duration = Duration::from_secs(30);
const CODEX_TURN_START_TIMEOUT: Duration = Duration::from_secs(90);
const SUPERVISOR_COMMAND_CONCURRENCY: usize = 4;
const CODEX_CONTEXT_ROTATE_DEFAULT_INPUT_TOKENS: i64 = 180_000;
const CODEX_CONTEXT_ROTATE_MIN_INPUT_TOKENS: i64 = 50_000;
const CODEX_CONTEXT_ROTATE_ENV: &str = "LANTOR_CODEX_CONTEXT_ROTATE_INPUT_TOKENS";
const AGENT_WORKSPACE_PREVIEW_LIMIT: u64 = 256 * 1024;
const DEFAULT_OWNER_DISPLAY_NAME: &str = "Me";
const DEFAULT_OWNER_AVATAR: &str = "dicebear:dylan:owner";
const DEFAULT_OWNER_DESCRIPTION: &str = "local owner";

fn expand_home_path(value: &str) -> String {
    let value = value.trim();
    if value == "~" {
        return env::var("HOME").unwrap_or_else(|_| value.to_owned());
    }
    if let Some(rest) = value.strip_prefix("~/") {
        if let Ok(home) = env::var("HOME") {
            return PathBuf::from(home).join(rest).to_string_lossy().to_string();
        }
    }
    value.to_owned()
}

#[derive(Clone)]
struct AppState {
    pool: SqlitePool,
    db_url: String,
}

#[derive(Clone, Default)]
struct WarmCodexRegistry {
    runtimes: Arc<AsyncMutex<HashMap<Uuid, Arc<WarmCodexRuntime>>>>,
}

#[derive(Clone, Default)]
struct WarmClaudeRegistry {
    runtimes: Arc<AsyncMutex<HashMap<Uuid, Arc<WarmClaudeRuntime>>>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DismissInboxItemInput {
    item_id: String,
    dismissed_until: DateTime<Utc>,
}

struct WarmCodexRuntime {
    stdin: AsyncMutex<tokio::process::ChildStdin>,
    state: AsyncMutex<WarmCodexState>,
    thread_id: String,
    pid: Option<i32>,
}

struct WarmCodexState {
    alive: bool,
    active: Option<CodexActiveTurn>,
    next_request_id: i64,
    last_activity: Instant,
}

struct CodexActiveTurn {
    run_id: Uuid,
    turn_request_id: i64,
    turn_id: Option<String>,
    started_at: Instant,
    first_delta_at: Option<Instant>,
    work_item_id: Option<Uuid>,
    channel_id: Option<Uuid>,
    thread_root_id: Option<Uuid>,
    stream_keys: HashSet<String>,
    steer_requests: HashMap<i64, CodexSteerRequest>,
    steer_disabled: bool,
    interrupt_request_id: Option<i64>,
}

struct CodexSteerRequest {
    work_item_id: Uuid,
    run_id: Uuid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CodexActiveTurnScheduleState {
    ReadyForSteer,
    WaitingForTurnId,
    StuckBeforeTurnId,
}

struct WarmClaudeRuntime {
    stdin: AsyncMutex<tokio::process::ChildStdin>,
    state: AsyncMutex<WarmClaudeState>,
    pid: Option<i32>,
}

struct WarmClaudeState {
    alive: bool,
    active: Option<ClaudeActiveTurn>,
    session_id: Option<String>,
    last_surface: Option<ClaudeSurface>,
    last_activity: Instant,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ClaudeSurface {
    channel_id: Option<Uuid>,
    thread_root_id: Option<Uuid>,
}

struct ClaudeActiveTurn {
    run_id: Uuid,
    started_at: Instant,
    first_delta_at: Option<Instant>,
    work_item_id: Option<Uuid>,
    channel_id: Option<Uuid>,
    thread_root_id: Option<Uuid>,
    stream_key: String,
}

type CommandResult<T> = Result<T, String>;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CreateChannelResult {
    channel_id: Uuid,
}

fn db_url() -> String {
    let configured = env::var("LANTOR_DATABASE_URL").unwrap_or_else(|_| {
        env::var("DATABASE_URL")
            .ok()
            .filter(|url| url.trim_start().starts_with("sqlite:"))
            .unwrap_or_else(|| DEFAULT_DATABASE_URL.to_owned())
    });
    if let Some(path) = configured.strip_prefix("sqlite://") {
        return format!("sqlite://{}", expand_home_path(path));
    }
    if let Some(path) = configured.strip_prefix("sqlite:") {
        return format!("sqlite:{}", expand_home_path(path));
    }
    configured
}

pub(crate) async fn db_connect_with_url(
    database_url: &str,
    max_connections: u32,
) -> Result<SqlitePool, sqlx::Error> {
    let options = SqliteConnectOptions::from_str(database_url)?
        .create_if_missing(true)
        .foreign_keys(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        .busy_timeout(Duration::from_secs(10));
    if !database_url.contains(":memory:") {
        if let Some(parent) = options.get_filename().parent() {
            fs::create_dir_all(parent)?;
        }
    }
    SqlitePoolOptions::new()
        .max_connections(max_connections)
        .connect_with(options)
        .await
}

pub(crate) async fn db_connect(max_connections: u32) -> Result<SqlitePool, sqlx::Error> {
    db_connect_with_url(&db_url(), max_connections).await
}

fn sqlite_database_file_path(database_url: &str) -> CommandResult<Option<PathBuf>> {
    if database_url.contains(":memory:") {
        return Ok(None);
    }
    let options = SqliteConnectOptions::from_str(database_url).map_err(to_string)?;
    Ok(Some(options.get_filename().to_path_buf()))
}

#[cfg(unix)]
fn try_lock_supervisor_file(file: &fs::File) -> std::io::Result<()> {
    const LOCK_EX: i32 = 2;
    const LOCK_NB: i32 = 4;
    extern "C" {
        fn flock(fd: i32, operation: i32) -> i32;
    }
    let result = unsafe { flock(file.as_raw_fd(), LOCK_EX | LOCK_NB) };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(not(unix))]
fn try_lock_supervisor_file(_file: &fs::File) -> std::io::Result<()> {
    Ok(())
}

fn acquire_supervisor_lock(database_url: &str) -> CommandResult<Option<fs::File>> {
    let Some(database_path) = sqlite_database_file_path(database_url)? else {
        return Ok(None);
    };
    let lock_path = PathBuf::from(format!("{}.supervisor.lock", database_path.display()));
    if let Some(parent) = lock_path.parent() {
        fs::create_dir_all(parent).map_err(to_string)?;
    }
    let file = fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&lock_path)
        .map_err(to_string)?;
    match try_lock_supervisor_file(&file) {
        Ok(()) => Ok(Some(file)),
        Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => Err(format!(
            "another Lantor supervisor is already running for {}",
            database_path.display()
        )),
        Err(err) => Err(err.to_string()),
    }
}

pub(crate) async fn notify_database_event(
    pool: &SqlitePool,
    channel: &str,
    payload: &str,
) -> CommandResult<()> {
    if channel != UI_REFRESH_CHANNEL {
        return Ok(());
    }
    sqlx::query("insert into ui_events (event_json) values ($1)")
        .bind(payload)
        .execute(pool)
        .await
        .map_err(to_string)?;

    Ok(())
}

pub(crate) async fn notify_ui_refresh(pool: &SqlitePool, reason: &str) -> CommandResult<()> {
    notify_database_event(
        pool,
        UI_REFRESH_CHANNEL,
        &json!({ "type": "refresh", "reason": reason }).to_string(),
    )
    .await
}

async fn notify_ui_message_upsert(
    pool: &SqlitePool,
    message: &Message,
    reason: &str,
) -> CommandResult<()> {
    notify_database_event(
        pool,
        UI_REFRESH_CHANNEL,
        &json!({ "type": "message_upsert", "reason": reason, "message": message }).to_string(),
    )
    .await
}

async fn notify_ui_message_delta(
    pool: &SqlitePool,
    message_id: Uuid,
    append: &str,
    delivery_state: &str,
    reason: &str,
) -> CommandResult<()> {
    notify_database_event(
        pool,
        UI_REFRESH_CHANNEL,
        &json!({
            "type": "message_delta",
            "reason": reason,
            "message_id": message_id,
            "append": append,
            "delivery_state": delivery_state
        })
        .to_string(),
    )
    .await
}

async fn notify_ui_message_delete(
    pool: &SqlitePool,
    message_id: Uuid,
    reason: &str,
) -> CommandResult<()> {
    notify_database_event(
        pool,
        UI_REFRESH_CHANNEL,
        &json!({ "type": "message_delete", "reason": reason, "message_id": message_id })
            .to_string(),
    )
    .await
}

async fn notify_ui_activity_upsert(
    pool: &SqlitePool,
    activity: &AgentActivity,
    reason: &str,
) -> CommandResult<()> {
    notify_database_event(
        pool,
        UI_REFRESH_CHANNEL,
        &json!({ "type": "activity_upsert", "reason": reason, "activity": activity }).to_string(),
    )
    .await
}

async fn notify_ui_agent_run_upsert(
    pool: &SqlitePool,
    run: &AgentRunPatch,
    reason: &str,
) -> CommandResult<()> {
    notify_database_event(
        pool,
        UI_REFRESH_CHANNEL,
        &json!({ "type": "agent_run_upsert", "reason": reason, "run": run }).to_string(),
    )
    .await
}

async fn notify_ui_work_item_upsert(
    pool: &SqlitePool,
    work_item: &AgentWorkItemPatch,
    reason: &str,
) -> CommandResult<()> {
    notify_database_event(
        pool,
        UI_REFRESH_CHANNEL,
        &json!({ "type": "work_item_upsert", "reason": reason, "work_item": work_item })
            .to_string(),
    )
    .await
}

async fn notify_ui_artifact_upsert(
    pool: &SqlitePool,
    artifact: &Artifact,
    reason: &str,
) -> CommandResult<()> {
    notify_database_event(
        pool,
        UI_REFRESH_CHANNEL,
        &json!({ "type": "artifact_upsert", "reason": reason, "artifact": artifact }).to_string(),
    )
    .await
}

async fn notify_ui_agent_run_changed(pool: &SqlitePool, run_id: Uuid, reason: &str) {
    if let Ok(run) = load_agent_run_patch(pool, run_id).await {
        let _ = notify_ui_agent_run_upsert(pool, &run, reason).await;
    } else {
        let _ = notify_ui_refresh(pool, reason).await;
    }
}

async fn notify_ui_work_item_changed(pool: &SqlitePool, work_item_id: Uuid, reason: &str) {
    let _ = sync_inbox_for_work_item(pool, work_item_id).await;
    if let Ok(work_item) = load_agent_work_item_patch(pool, work_item_id).await {
        let _ = notify_ui_work_item_upsert(pool, &work_item, reason).await;
        let _ = maybe_insert_work_item_system_message(pool, &work_item, reason).await;
    } else {
        let _ = notify_ui_refresh(pool, reason).await;
    }
}

async fn insert_system_message(
    pool: &SqlitePool,
    channel_id: Uuid,
    thread_root_id: Option<Uuid>,
    body: impl AsRef<str>,
) -> CommandResult<Uuid> {
    let body = body.as_ref().trim();
    if body.is_empty() {
        return Err("system message body is empty".to_owned());
    }
    let message_id: Uuid = sqlx::query_scalar(
        r#"
        insert into messages (channel_id, thread_root_id, sender_name, sender_role, body, is_task)
        values ($1, $2, 'Lantor', 'system', $3, false)
        returning id
        "#,
    )
    .bind(channel_id)
    .bind(thread_root_id)
    .bind(body)
    .fetch_one(pool)
    .await
    .map_err(to_string)?;

    if let Ok(message) = load_message(pool, message_id).await {
        let _ = notify_ui_message_upsert(pool, &message, "system_message").await;
    } else {
        let _ = notify_ui_refresh(pool, "system_message").await;
    }
    Ok(message_id)
}

async fn maybe_insert_work_item_system_message(
    pool: &SqlitePool,
    work_item: &AgentWorkItemPatch,
    reason: &str,
) -> CommandResult<()> {
    // Conversation-triggered agent turns are attention events, not timeline-level tasks.
    // Keep normal lifecycle messages for explicit task-backed work only; still surface
    // exceptional failures/cancellations for conversational turns.
    if work_item.task_number.is_none()
        && !matches!(reason, "work_item_failed" | "work_item_cancelled")
    {
        return Ok(());
    }
    if work_item.task_number.is_some()
        && matches!(
            reason,
            "work_item_created" | "work_item_queued" | "work_item_running"
        )
    {
        return Ok(());
    }
    if work_item.task_number.is_some() {
        if let Some(task_id) = work_item.task_id {
            let task_row = sqlx::query(
                "select coalesce(assignee_agent_id = $2, false) as is_assignee, status from tasks where id = $1",
            )
            .bind(task_id)
            .bind(work_item.agent_id)
            .fetch_optional(pool)
            .await
            .map_err(to_string)?;
            let Some(task_row) = task_row else {
                return Ok(());
            };
            if !task_row.get::<bool, _>("is_assignee") {
                return Ok(());
            }
            let task_status: String = task_row.get("status");
            if reason == "work_item_finished"
                && work_item.status == "done"
                && matches!(task_status.as_str(), "todo" | "in_progress")
            {
                return Ok(());
            }
        }
    }
    let Some(channel_id) = work_item.channel_id else {
        return Ok(());
    };
    let thread_root_id = work_item.thread_root_id.or(work_item.source_message_id);
    let object_label = work_item
        .task_number
        .map(|number| format!("task run for task #{number}"))
        .unwrap_or_else(|| "agent request".to_owned());
    let title = work_item.title.trim();
    let title_suffix = if title.is_empty() {
        String::new()
    } else {
        format!(": {title}")
    };
    let body = match reason {
        "work_item_created" | "work_item_queued" => {
            format!(
                "@{} queued {}{}",
                work_item.agent_handle, object_label, title_suffix
            )
        }
        "work_item_running" => {
            format!(
                "@{} started {}{}",
                work_item.agent_handle, object_label, title_suffix
            )
        }
        "work_item_cancelling" => {
            format!(
                "@{} is stopping {}{}",
                work_item.agent_handle, object_label, title_suffix
            )
        }
        "work_item_cancelled" => {
            format!(
                "@{} cancelled {}{}",
                work_item.agent_handle, object_label, title_suffix
            )
        }
        "work_item_failed" => {
            format!(
                "@{} failed {}{}",
                work_item.agent_handle, object_label, title_suffix
            )
        }
        "work_item_finished" => match work_item.status.as_str() {
            "done" => format!(
                "@{} completed {}{}",
                work_item.agent_handle, object_label, title_suffix
            ),
            "failed" => format!(
                "@{} failed {}{}",
                work_item.agent_handle, object_label, title_suffix
            ),
            "cancelled" => format!(
                "@{} cancelled {}{}",
                work_item.agent_handle, object_label, title_suffix
            ),
            "silent" => return Ok(()),
            _ => return Ok(()),
        },
        _ => return Ok(()),
    };
    insert_system_message(pool, channel_id, thread_root_id, body).await?;
    Ok(())
}

async fn notify_supervisor_wake(pool: &SqlitePool) -> CommandResult<()> {
    notify_database_event(pool, SUPERVISOR_WAKE_CHANNEL, "wake").await
}

fn spawn_reminder_worker(pool: SqlitePool) {
    tauri::async_runtime::spawn(async move {
        loop {
            if let Err(err) = process_due_reminders(&pool).await {
                eprintln!("Lantor reminder worker failed: {err}");
            }
            if let Err(err) = process_due_agent_schedules(&pool).await {
                eprintln!("Lantor schedule worker failed: {err}");
            }
            sleep(Duration::from_secs(15)).await;
        }
    });
}

async fn process_due_reminders(pool: &SqlitePool) -> CommandResult<()> {
    let rows = sqlx::query(
        r#"
        update reminders
        set status = case when recurrence = 'none' then 'fired' else 'scheduled' end,
            fired_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now'),
            due_at = case
                when recurrence = 'daily' then strftime('%Y-%m-%dT%H:%M:%f+00:00','now','+1 day')
                when recurrence = 'weekly' then strftime('%Y-%m-%dT%H:%M:%f+00:00','now','+7 days')
                else due_at
            end,
            updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
        where id in (
            select id
            from reminders
            where status = 'scheduled'
              and due_at <= strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
            order by due_at asc
            limit 12
        )
        returning id, channel_id, creator_agent_id, thread_root_id, title, note, recurrence, status, due_at
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(to_string)?;

    let fired_any = !rows.is_empty();
    for row in rows {
        let reminder_id: Uuid = row.get("id");
        let channel_id: Option<Uuid> = row.get("channel_id");
        let creator_agent_id: Option<Uuid> = row.get("creator_agent_id");
        let thread_root_id: Option<Uuid> = row.get("thread_root_id");
        let title: String = row.get("title");
        let note: String = row.get("note");
        let recurrence: String = row.get("recurrence");
        let status: String = row.get("status");
        let next_due_at: DateTime<Utc> = row.get("due_at");
        insert_reminder_event(
            pool,
            reminder_id,
            "fired",
            if recurrence == "none" {
                String::new()
            } else {
                format!("next_due_at={}", next_due_at.to_rfc3339())
            },
        )
        .await?;

        if let Some(channel_id) = channel_id {
            let mut body = format!("Reminder: {title}");
            if !note.trim().is_empty() {
                body.push_str(&format!("\n{}", note.trim()));
            }
            if recurrence != "none" && status == "scheduled" {
                body.push_str(&format!("\nNext reminder: {}", next_due_at.to_rfc3339()));
            }
            if let Ok(message_id) =
                insert_system_message(pool, channel_id, thread_root_id, body).await
            {
                if let Some(agent_id) = creator_agent_id {
                    let _ = dispatch_due_reminder_to_agent(
                        pool,
                        reminder_id,
                        agent_id,
                        channel_id,
                        thread_root_id,
                        message_id,
                        &title,
                        &note,
                    )
                    .await;
                }
            }
        }
    }
    if fired_any {
        let _ = notify_ui_refresh(pool, "reminder_due").await;
    }
    Ok(())
}

async fn dispatch_due_reminder_to_agent(
    pool: &SqlitePool,
    reminder_id: Uuid,
    agent_id: Uuid,
    channel_id: Uuid,
    thread_root_id: Option<Uuid>,
    source_message_id: Uuid,
    title: &str,
    note: &str,
) -> CommandResult<()> {
    sqlx::query(
        r#"
        insert into channel_members (channel_id, agent_id)
        values ($1, $2)
        on conflict (channel_id, agent_id) do nothing
        "#,
    )
    .bind(channel_id)
    .bind(agent_id)
    .execute(pool)
    .await
    .map_err(to_string)?;

    let work_thread_root_id = thread_root_id.or(Some(source_message_id));
    let inbox_item_id = create_agent_inbox_item(
        pool,
        AgentInboxItemInput {
            agent_id,
            channel_id: Some(channel_id),
            thread_root_id: work_thread_root_id,
            source_message_id: Some(source_message_id),
            task_id: None,
            kind: "reminder_due",
            priority: 90,
            title,
            body_preview: note,
            payload: json!({"reminder_id": reminder_id}),
        },
    )
    .await?;
    let wake = ensure_agent_inbox_wake_work_item(pool, agent_id).await?;
    let scheduled = wake.is_some_and(|(_, scheduled)| scheduled);
    record_agent_activity(
        pool,
        Some(agent_id),
        None,
        "reminder",
        if scheduled {
            "Reminder follow-up dispatched"
        } else {
            "Reminder follow-up queued"
        },
        json!({
            "reminder_id": reminder_id,
            "inbox_item_id": inbox_item_id,
            "source_message_id": source_message_id
        })
        .to_string(),
    )
    .await?;
    Ok(())
}

async fn process_due_agent_schedules(pool: &SqlitePool) -> CommandResult<()> {
    let rows = sqlx::query(
        r#"
        update agent_schedules
        set last_run_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now'),
            next_run_at = case
                when cadence = 'hourly' then strftime('%Y-%m-%dT%H:%M:%f+00:00','now','+1 hour')
                when cadence = 'daily' then strftime('%Y-%m-%dT%H:%M:%f+00:00','now','+1 day')
                when cadence = 'weekly' then strftime('%Y-%m-%dT%H:%M:%f+00:00','now','+7 days')
                else strftime('%Y-%m-%dT%H:%M:%f+00:00','now','+1 day')
            end,
            updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
        where id in (
            select id
            from agent_schedules
            where status = 'active'
              and next_run_at <= strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
            order by next_run_at asc
            limit 8
        )
        returning
            id,
            agent_id,
            (select handle from agents where id = agent_schedules.agent_id) as agent_handle,
            channel_id,
            (select name from channels where id = agent_schedules.channel_id) as channel_name,
            thread_root_id,
            title,
            prompt,
            cadence,
            next_run_at
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(to_string)?;

    let fired_any = !rows.is_empty();
    for row in rows {
        let schedule_id: Uuid = row.get("id");
        let agent_id: Uuid = row.get("agent_id");
        let agent_handle: String = row.get("agent_handle");
        let channel_id: Uuid = row.get("channel_id");
        let channel_name: String = row.get("channel_name");
        let thread_root_id: Option<Uuid> = row.get("thread_root_id");
        let title: String = row.get("title");
        let prompt: String = row.get("prompt");
        let cadence: String = row.get("cadence");
        let next_run_at: DateTime<Utc> = row.get("next_run_at");

        let system_body = format!(
            "Scheduled routine for @{agent_handle}: {title}\nNext run: {}",
            next_run_at.to_rfc3339()
        );
        let source_message_id =
            insert_system_message(pool, channel_id, thread_root_id, system_body).await?;
        let work_thread_root_id = thread_root_id.or(Some(source_message_id));
        let inbox_item_id = create_agent_inbox_item(
            pool,
            AgentInboxItemInput {
                agent_id,
                channel_id: Some(channel_id),
                thread_root_id: work_thread_root_id,
                source_message_id: Some(source_message_id),
                task_id: None,
                kind: "schedule_due",
                priority: 75,
                title: &title,
                body_preview: &prompt,
                payload: json!({"schedule_id": schedule_id, "cadence": &cadence}),
            },
        )
        .await?;
        let wake = ensure_agent_inbox_wake_work_item(pool, agent_id).await?;
        let scheduled = wake.as_ref().is_some_and(|(_, scheduled)| *scheduled);
        let work_item_id = wake.map(|(work_item_id, _)| work_item_id);

        sqlx::query(
            "update agent_schedules set last_work_item_id = $2, updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now') where id = $1",
        )
        .bind(schedule_id)
        .bind(work_item_id)
        .execute(pool)
        .await
        .map_err(to_string)?;

        record_agent_activity(
            pool,
            Some(agent_id),
            None,
            "schedule",
            if scheduled {
                "Scheduled routine dispatched"
            } else {
                "Scheduled routine queued"
            },
            json!({
                "schedule_id": schedule_id,
                "work_item_id": work_item_id,
                "inbox_item_id": inbox_item_id,
                "channel": format!("#{channel_name}"),
                "cadence": cadence,
                "next_run_at": next_run_at.to_rfc3339()
            })
            .to_string(),
        )
        .await?;
    }
    if fired_any {
        let _ = notify_ui_refresh(pool, "agent_schedule_due").await;
    }
    Ok(())
}

fn spawn_ui_refresh_listener(app: tauri::AppHandle, pool: SqlitePool) {
    tauri::async_runtime::spawn(async move {
        let mut last_id: i64 = sqlx::query_scalar("select coalesce(max(id), 0) from ui_events")
            .fetch_one(&pool)
            .await
            .unwrap_or(0);
        loop {
            let rows = sqlx::query(
                r#"
                select id, event_json
                from ui_events
                where id > $1
                order by id asc
                limit 80
                "#,
            )
            .bind(last_id)
            .fetch_all(&pool)
            .await;
            match rows {
                Ok(rows) if rows.is_empty() => {
                    sleep(Duration::from_millis(150)).await;
                }
                Ok(rows) => {
                    let mut payloads = Vec::with_capacity(rows.len());
                    for row in rows {
                        last_id = row.get("id");
                        payloads.push(row.get::<String, _>("event_json"));
                    }
                    if payloads.len() == 1 {
                        if let Some(payload) = payloads.pop() {
                            let _ = app.emit(UI_REFRESH_EVENT, payload);
                        }
                    } else {
                        let _ = app.emit(
                            UI_REFRESH_EVENT,
                            json!({ "type": "batch", "events": payloads }).to_string(),
                        );
                    }
                }
                Err(err) => {
                    eprintln!("Lantor UI refresh poller failed: {err}");
                    sleep(Duration::from_secs(2)).await;
                }
            }
        }
    });
}

async fn migrate(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    for statement in [
        r#"
        create table if not exists owner_profile (
            id integer primary key default 1 check (id = 1),
            display_name text not null default 'Me',
            avatar text not null default 'dicebear:dylan:owner',
            description text not null default 'local owner',
            updated_at text not null default (strftime('%Y-%m-%dT%H:%M:%f+00:00','now'))
        )
        "#,
        r#"
        create table if not exists agents (
            id blob primary key not null default (randomblob(16)),
            handle text not null unique,
            display_name text not null,
            role text not null default 'agent',
            status text not null default 'idle',
            runtime text not null default 'codex',
            model text not null default 'gpt-5.5',
            avatar text not null default '',
            description text not null default '',
            launch_command text not null default '',
            working_directory text not null default '',
            daily_budget_micros integer not null default 0,
            reasoning_effort text not null default 'medium',
            service_tier text not null default '',
            created_at text not null default (strftime('%Y-%m-%dT%H:%M:%f+00:00','now'))
        )
        "#,
        r#"
        create table if not exists channels (
            id blob primary key not null default (randomblob(16)),
            name text not null unique,
            description text not null default '',
            unread_count integer not null default 0,
            kind text not null default 'channel',
            dm_agent_id blob references agents(id) on delete cascade,
            created_at text not null default (strftime('%Y-%m-%dT%H:%M:%f+00:00','now'))
        )
        "#,
        r#"
        create table if not exists messages (
            id blob primary key not null default (randomblob(16)),
            channel_id blob not null references channels(id) on delete cascade,
            thread_root_id blob references messages(id) on delete cascade,
            sender_agent_id blob references agents(id) on delete set null,
            sender_name text not null,
            sender_role text not null default 'human',
            body text not null,
            is_task boolean not null default 0,
            thread_followed boolean not null default 1,
            delivery_state text not null default 'complete',
            stream_key text not null default '',
            created_at text not null default (strftime('%Y-%m-%dT%H:%M:%f+00:00','now')),
            updated_at text not null default (strftime('%Y-%m-%dT%H:%M:%f+00:00','now'))
        )
        "#,
        r#"
        create table if not exists message_attachments (
            id blob primary key not null default (randomblob(16)),
            message_id blob not null references messages(id) on delete cascade,
            original_name text not null,
            mime_type text not null,
            size_bytes integer not null,
            storage_path text not null,
            created_at text not null default (strftime('%Y-%m-%dT%H:%M:%f+00:00','now'))
        )
        "#,
        r#"
        create table if not exists saved_messages (
            id blob primary key not null default (randomblob(16)),
            message_id blob not null unique references messages(id) on delete cascade,
            created_at text not null default (strftime('%Y-%m-%dT%H:%M:%f+00:00','now'))
        )
        "#,
        r#"
        create table if not exists artifacts (
            id blob primary key not null default (randomblob(16)),
            message_id blob not null references messages(id) on delete cascade,
            channel_id blob not null references channels(id) on delete cascade,
            thread_root_id blob references messages(id) on delete set null,
            creator_agent_id blob references agents(id) on delete set null,
            kind text not null,
            title text not null,
            summary text not null default '',
            content text not null,
            metadata text not null default '{}',
            created_at text not null default (strftime('%Y-%m-%dT%H:%M:%f+00:00','now')),
            updated_at text not null default (strftime('%Y-%m-%dT%H:%M:%f+00:00','now'))
        )
        "#,
        r#"
        create table if not exists runtime_sessions (
            id blob primary key not null default (randomblob(16)),
            agent_id blob not null references agents(id) on delete cascade,
            runtime text not null,
            provider_thread_id text not null,
            status text not null default 'idle',
            created_at text not null default (strftime('%Y-%m-%dT%H:%M:%f+00:00','now')),
            updated_at text not null default (strftime('%Y-%m-%dT%H:%M:%f+00:00','now')),
            unique(agent_id, runtime)
        )
        "#,
        r#"
        create table if not exists tasks (
            number integer primary key autoincrement,
            id blob not null unique default (randomblob(16)),
            message_id blob not null unique references messages(id) on delete cascade,
            channel_id blob not null references channels(id) on delete cascade,
            title text not null,
            status text not null default 'todo',
            assignee_agent_id blob references agents(id) on delete set null,
            version integer not null default 0,
            created_at text not null default (strftime('%Y-%m-%dT%H:%M:%f+00:00','now')),
            updated_at text not null default (strftime('%Y-%m-%dT%H:%M:%f+00:00','now'))
        )
        "#,
        r#"
        create table if not exists reminders (
            id blob primary key not null default (randomblob(16)),
            channel_id blob references channels(id) on delete set null,
            creator_agent_id blob references agents(id) on delete set null,
            thread_root_id blob references messages(id) on delete set null,
            message_id blob references messages(id) on delete set null,
            title text not null,
            note text not null default '',
            status text not null default 'scheduled',
            recurrence text not null default 'none',
            due_at text not null,
            fired_at text,
            completed_at text,
            created_at text not null default (strftime('%Y-%m-%dT%H:%M:%f+00:00','now')),
            updated_at text not null default (strftime('%Y-%m-%dT%H:%M:%f+00:00','now'))
        )
        "#,
        r#"
        create table if not exists reminder_events (
            id blob primary key not null default (randomblob(16)),
            reminder_id blob not null references reminders(id) on delete cascade,
            event_type text not null,
            detail text not null default '',
            created_at text not null default (strftime('%Y-%m-%dT%H:%M:%f+00:00','now'))
        )
        "#,
        r#"
        create table if not exists agent_schedules (
            id blob primary key not null default (randomblob(16)),
            agent_id blob not null references agents(id) on delete cascade,
            channel_id blob not null references channels(id) on delete cascade,
            thread_root_id blob references messages(id) on delete set null,
            title text not null,
            prompt text not null default '',
            cadence text not null default 'daily',
            status text not null default 'active',
            next_run_at text not null,
            last_run_at text,
            last_work_item_id blob,
            created_at text not null default (strftime('%Y-%m-%dT%H:%M:%f+00:00','now')),
            updated_at text not null default (strftime('%Y-%m-%dT%H:%M:%f+00:00','now'))
        )
        "#,
        r#"
        create table if not exists agent_runs (
            id blob primary key not null default (randomblob(16)),
            agent_id blob not null references agents(id) on delete cascade,
            work_item_id blob references agent_work_items(id) on delete set null,
            command text not null,
            working_directory text not null default '',
            status text not null default 'starting',
            pid integer,
            exit_code integer,
            log text not null default '',
            input_tokens integer not null default 0,
            output_tokens integer not null default 0,
            cost_micros integer not null default 0,
            started_at text not null default (strftime('%Y-%m-%dT%H:%M:%f+00:00','now')),
            stopped_at text
        )
        "#,
        r#"
        create table if not exists agent_activities (
            id blob primary key not null default (randomblob(16)),
            agent_id blob references agents(id) on delete set null,
            agent_handle text not null default '',
            run_id blob references agent_runs(id) on delete set null,
            kind text not null,
            phase text not null default 'event',
            status text not null default 'info',
            title text not null,
            summary text not null default '',
            detail text not null default '',
            metadata text not null default '{}',
            created_at text not null default (strftime('%Y-%m-%dT%H:%M:%f+00:00','now'))
        )
        "#,
        r#"
        create table if not exists agent_work_items (
            id blob primary key not null default (randomblob(16)),
            agent_id blob not null references agents(id) on delete cascade,
            channel_id blob references channels(id) on delete set null,
            thread_root_id blob references messages(id) on delete set null,
            source_message_id blob references messages(id) on delete set null,
            inbox_item_id blob,
            task_id blob references tasks(id) on delete set null,
            source_kind text not null default 'manual',
            title text not null,
            context text not null default '',
            status text not null default 'queued',
            run_id blob references agent_runs(id) on delete set null,
            created_at text not null default (strftime('%Y-%m-%dT%H:%M:%f+00:00','now')),
            updated_at text not null default (strftime('%Y-%m-%dT%H:%M:%f+00:00','now')),
            completed_at text
        )
        "#,
        r#"
        create table if not exists agent_inbox_items (
            id blob primary key not null default (randomblob(16)),
            agent_id blob not null references agents(id) on delete cascade,
            channel_id blob references channels(id) on delete set null,
            thread_root_id blob references messages(id) on delete set null,
            source_message_id blob references messages(id) on delete set null,
            task_id blob references tasks(id) on delete set null,
            kind text not null,
            priority integer not null default 50,
            state text not null default 'unread',
            title text not null,
            body_preview text not null default '',
            payload text not null default '{}',
            work_item_id blob references agent_work_items(id) on delete set null,
            created_at text not null default (strftime('%Y-%m-%dT%H:%M:%f+00:00','now')),
            updated_at text not null default (strftime('%Y-%m-%dT%H:%M:%f+00:00','now')),
            archived_at text
        )
        "#,
        r#"
        create table if not exists agent_thread_subscriptions (
            agent_id blob not null references agents(id) on delete cascade,
            channel_id blob not null references channels(id) on delete cascade,
            thread_root_id blob not null references messages(id) on delete cascade,
            source_kind text not null default 'manual',
            last_source_message_id blob references messages(id) on delete set null,
            created_at text not null default (strftime('%Y-%m-%dT%H:%M:%f+00:00','now')),
            updated_at text not null default (strftime('%Y-%m-%dT%H:%M:%f+00:00','now')),
            primary key (agent_id, thread_root_id)
        )
        "#,
        r#"
        create table if not exists agent_event_receipts (
            run_id blob not null references agent_runs(id) on delete cascade,
            event_json text not null,
            event_hash text not null,
            created_at text not null default (strftime('%Y-%m-%dT%H:%M:%f+00:00','now')),
            primary key (run_id, event_hash)
        )
        "#,
        r#"
        create table if not exists supervisor_commands (
            id blob primary key not null default (randomblob(16)),
            command_type text not null,
            agent_id blob references agents(id) on delete cascade,
            run_id blob references agent_runs(id) on delete cascade,
            work_item_id blob references agent_work_items(id) on delete set null,
            status text not null default 'pending',
            error text not null default '',
            created_at text not null default (strftime('%Y-%m-%dT%H:%M:%f+00:00','now')),
            updated_at text not null default (strftime('%Y-%m-%dT%H:%M:%f+00:00','now'))
        )
        "#,
        r#"
        create table if not exists supervisor_state (
            id integer primary key default 1 check (id = 1),
            pid integer,
            status text not null default 'offline',
            updated_at text not null default (strftime('%Y-%m-%dT%H:%M:%f+00:00','now'))
        )
        "#,
        r#"
        create table if not exists channel_read_state (
            channel_id blob primary key references channels(id) on delete cascade,
            last_read_at text not null default '0001-01-01T00:00:00+00:00'
        )
        "#,
        r#"
        create table if not exists owner_inbox_dismissals (
            item_id text primary key,
            dismissed_until text not null,
            dismissed_at text not null default (strftime('%Y-%m-%dT%H:%M:%f+00:00','now'))
        )
        "#,
        r#"
        create table if not exists owner_inbox_read_state (
            item_id text primary key,
            read_until text not null,
            read_at text not null default (strftime('%Y-%m-%dT%H:%M:%f+00:00','now'))
        )
        "#,
        r#"
        create table if not exists owner_inbox_hidden_items (
            item_id text primary key,
            hidden_until text not null,
            hidden_at text not null default (strftime('%Y-%m-%dT%H:%M:%f+00:00','now'))
        )
        "#,
        r#"
        create table if not exists channel_members (
            channel_id blob not null references channels(id) on delete cascade,
            agent_id blob not null references agents(id) on delete cascade,
            created_at text not null default (strftime('%Y-%m-%dT%H:%M:%f+00:00','now')),
            primary key (channel_id, agent_id)
        )
        "#,
        r#"
        create table if not exists ui_events (
            id integer primary key autoincrement,
            event_json text not null,
            created_at text not null default (strftime('%Y-%m-%dT%H:%M:%f+00:00','now'))
        )
        "#,
    ] {
        sqlx::query(statement).execute(pool).await?;
    }

    for statement in [
        "create unique index if not exists channels_dm_unique on channels(dm_agent_id) where kind = 'dm' and dm_agent_id is not null",
        "create unique index if not exists messages_stream_key_unique on messages(stream_key) where stream_key <> ''",
        "create index if not exists message_attachments_message_id_idx on message_attachments(message_id)",
        "create index if not exists saved_messages_created_at_idx on saved_messages(created_at desc)",
        "create index if not exists artifacts_message_id_idx on artifacts(message_id)",
        "create index if not exists artifacts_channel_id_idx on artifacts(channel_id)",
        "create index if not exists reminders_due_idx on reminders(status, due_at)",
        "create index if not exists agent_schedules_due_idx on agent_schedules(status, next_run_at)",
        "create index if not exists agent_inbox_items_agent_state_idx on agent_inbox_items(agent_id, state, priority desc, created_at)",
        "create unique index if not exists agent_inbox_items_source_unique on agent_inbox_items(agent_id, source_message_id, kind) where source_message_id is not null",
        "create index if not exists ui_events_created_idx on ui_events(created_at)",
    ] {
        sqlx::query(statement).execute(pool).await?;
    }

    sqlx::query(
        r#"
        insert into owner_profile (id, display_name, avatar, description)
        values (1, 'Me', 'dicebear:dylan:owner', 'local owner')
        on conflict (id) do nothing
        "#,
    )
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        update owner_profile
        set avatar = 'dicebear:dylan:owner'
        where id = 1 and avatar = 'M'
        "#,
    )
    .execute(pool)
    .await?;
    backfill_agent_run_usage_from_logs(pool).await?;

    Ok(())
}

#[tauri::command]
async fn bootstrap(state: State<'_, AppState>) -> CommandResult<Bootstrap> {
    load_bootstrap(&state.pool, state.db_url.clone()).await
}

fn configured_web_base_url() -> Option<String> {
    if let Ok(value) = env::var("LANTOR_WEB_PUBLIC_URL") {
        let trimmed = value.trim().trim_end_matches('/').to_owned();
        if !trimmed.is_empty() {
            return Some(trimmed);
        }
    }
    let bind = crate::web::resolve_web_bind()?;
    let addr = bind.parse::<SocketAddr>().ok()?;
    let host = match addr.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => "127.0.0.1".to_owned(),
        IpAddr::V4(ip) => ip.to_string(),
        IpAddr::V6(ip) if ip.is_unspecified() => "[::1]".to_owned(),
        IpAddr::V6(ip) => format!("[{ip}]"),
    };
    Some(format!("http://{host}:{}", addr.port()))
}

fn normalize_external_url(url: &str) -> Option<String> {
    let (scheme, rest) = url.split_once(':')?;
    let scheme = scheme.to_ascii_lowercase();
    match scheme.as_str() {
        "http" | "https" if rest.starts_with("//") => Some(url.to_owned()),
        "mailto" if !rest.is_empty() => Some(url.to_owned()),
        "file" if rest.starts_with("//") => Some(url.to_owned()),
        _ => None,
    }
}

fn strip_local_path_line_suffix(value: &str) -> &str {
    if Path::new(value).exists() {
        return value;
    }

    if let Some((path, line)) = value.rsplit_once(':') {
        if !line.is_empty() && line.chars().all(|c| c.is_ascii_digit()) && Path::new(path).exists()
        {
            return path;
        }
    }

    if let Some((path, line)) = value.rsplit_once("#L") {
        if !line.is_empty() && line.chars().all(|c| c.is_ascii_digit()) && Path::new(path).exists()
        {
            return path;
        }
    }

    value
}

fn normalize_local_path_link(url: &str) -> Option<String> {
    let expanded = expand_home_path(url);
    let without_line_suffix = strip_local_path_line_suffix(&expanded);
    let path = Path::new(without_line_suffix);
    if path.is_absolute() && path.exists() {
        return Some(path.to_string_lossy().to_string());
    }
    None
}

fn normalize_open_link_target(url: &str) -> Option<String> {
    let trimmed = url.trim();
    if trimmed.is_empty() || trimmed.len() > 4096 || trimmed.chars().any(char::is_control) {
        return None;
    }

    normalize_external_url(trimmed).or_else(|| normalize_local_path_link(trimmed))
}

fn open_link_target_with_system(target: &str) -> CommandResult<()> {
    #[cfg(target_os = "macos")]
    let status = StdCommand::new("open")
        .arg(target)
        .status()
        .map_err(to_string)?;

    #[cfg(target_os = "windows")]
    let status = StdCommand::new("cmd")
        .args(["/C", "start", "", target])
        .status()
        .map_err(to_string)?;

    #[cfg(all(unix, not(target_os = "macos")))]
    let status = StdCommand::new("xdg-open")
        .arg(target)
        .status()
        .map_err(to_string)?;

    if status.success() {
        Ok(())
    } else {
        Err(format!("failed to open link target: {status}"))
    }
}

#[tauri::command]
async fn open_external_url(url: String) -> CommandResult<()> {
    let target = normalize_open_link_target(&url).ok_or_else(|| {
        "only http, https, mailto, file, and existing local file links can be opened".to_owned()
    })?;
    tauri::async_runtime::spawn_blocking(move || open_link_target_with_system(&target))
        .await
        .map_err(to_string)?
}

pub(crate) async fn load_bootstrap(pool: &SqlitePool, db_url: String) -> CommandResult<Bootstrap> {
    let owner_profile = load_owner_profile(pool).await?;
    let channels = load_channels(pool).await?;
    let channel_members = load_channel_members(pool).await?;
    let agents = load_agents(pool).await?;
    let messages = load_messages(pool).await?;
    let saved_messages = load_saved_messages(pool).await?;
    let dismissed_inbox_items = load_dismissed_inbox_items(pool).await?;
    let read_inbox_items = load_read_inbox_items(pool).await?;
    let artifacts = load_artifacts(pool).await?;
    let tasks = load_tasks(pool).await?;
    let reminders = load_reminders(pool).await?;
    let agent_schedules = load_agent_schedules(pool).await?;
    let agent_runs = load_agent_runs(pool).await?;
    let agent_work_items = load_agent_work_items(pool).await?;
    let agent_activities = load_agent_activities(pool).await?;
    let supervisor = load_supervisor_status(pool).await?;
    let launch_agent = launch_agent::load_launch_agent_status()?;

    Ok(Bootstrap {
        db_url,
        web_base_url: configured_web_base_url(),
        owner_profile,
        channels,
        channel_members,
        agents,
        messages,
        saved_messages,
        dismissed_inbox_items,
        read_inbox_items,
        artifacts,
        tasks,
        reminders,
        agent_schedules,
        agent_runs,
        agent_work_items,
        agent_activities,
        supervisor,
        launch_agent,
    })
}

#[tauri::command]
async fn check_runtime(runtime: String) -> CommandResult<RuntimeCheck> {
    check_runtime_in_env(runtime).await
}

pub(crate) async fn check_runtime_in_env(runtime: String) -> CommandResult<RuntimeCheck> {
    let runtime = runtime.trim().to_owned();
    let command = match runtime.as_str() {
        "codex" => "codex",
        "claude" => "claude",
        _ => {
            return Ok(RuntimeCheck {
                runtime,
                command: String::new(),
                available: false,
                detail: "Unknown runtime".to_owned(),
            });
        }
    };

    let script = format!(
        "if command -v {command} >/dev/null 2>&1; then {command} --version 2>&1 | head -n 1; else echo '{command} not found in PATH' >&2; exit 127; fi"
    );
    let output = StdCommand::new("/bin/zsh")
        .arg("-lc")
        .arg(script)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(to_string)?;

    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
    let detail = if !stdout.is_empty() {
        stdout
    } else if !stderr.is_empty() {
        stderr
    } else if output.status.success() {
        format!("{command} found")
    } else {
        format!("{command} unavailable")
    };

    Ok(RuntimeCheck {
        runtime,
        command: command.to_owned(),
        available: output.status.success(),
        detail,
    })
}

#[tauri::command]
async fn create_channel(
    name: String,
    description: Option<String>,
    agent_ids: Option<Vec<Uuid>>,
    state: State<'_, AppState>,
) -> CommandResult<CreateChannelResult> {
    let channel_id =
        create_channel_in_pool(&state.pool, &name, description.as_deref().unwrap_or("")).await?;
    if let Some(ids) = agent_ids {
        let mut seen = std::collections::HashSet::new();
        for agent_id in ids {
            if !seen.insert(agent_id) {
                continue;
            }
            add_agent_to_channel(&state.pool, channel_id, agent_id).await?;
        }
    }
    Ok(CreateChannelResult { channel_id })
}

#[tauri::command]
async fn update_channel(
    channel_id: Uuid,
    name: String,
    description: String,
    state: State<'_, AppState>,
) -> CommandResult<()> {
    update_channel_in_pool(&state.pool, channel_id, name, description).await
}

pub(crate) async fn update_channel_in_pool(
    pool: &SqlitePool,
    channel_id: Uuid,
    name: String,
    description: String,
) -> CommandResult<()> {
    let normalized = normalize_channel_name(&name);
    if normalized.is_empty() {
        return Err("channel name is empty".to_owned());
    }

    let kind: Option<String> = sqlx::query_scalar("select kind from channels where id = $1")
        .bind(channel_id)
        .fetch_optional(pool)
        .await
        .map_err(to_string)?;
    match kind.as_deref() {
        Some("dm") => return Err("direct messages cannot be renamed".to_owned()),
        Some(_) => {}
        None => return Err("channel does not exist".to_owned()),
    }

    sqlx::query(
        r#"
        update channels
        set name = $2, description = $3
        where id = $1
        "#,
    )
    .bind(channel_id)
    .bind(normalized)
    .bind(description.trim())
    .execute(pool)
    .await
    .map_err(to_string)?;

    let _ = notify_ui_refresh(pool, "channel_updated").await;
    Ok(())
}

#[tauri::command]
async fn set_channel_agent_membership(
    channel_id: Uuid,
    agent_id: Uuid,
    member: bool,
    state: State<'_, AppState>,
) -> CommandResult<()> {
    set_channel_agent_membership_in_pool(&state.pool, channel_id, agent_id, member).await
}

pub(crate) async fn set_channel_agent_membership_in_pool(
    pool: &SqlitePool,
    channel_id: Uuid,
    agent_id: Uuid,
    member: bool,
) -> CommandResult<()> {
    let channel_row = sqlx::query("select name, kind from channels where id = $1")
        .bind(channel_id)
        .fetch_optional(pool)
        .await
        .map_err(to_string)?;
    let Some(channel_row) = channel_row else {
        return Err("channel does not exist".to_owned());
    };
    let channel_name: String = channel_row.get("name");
    let channel_kind: String = channel_row.get("kind");
    if channel_kind == "dm" {
        return Err("direct message membership is fixed".to_owned());
    }

    let agent_handle: Option<String> =
        sqlx::query_scalar("select handle from agents where id = $1")
            .bind(agent_id)
            .fetch_optional(pool)
            .await
            .map_err(to_string)?;
    if agent_handle.is_none() {
        return Err("agent does not exist".to_owned());
    }

    if member {
        sqlx::query(
            r#"
            insert into channel_members (channel_id, agent_id)
            values ($1, $2)
            on conflict (channel_id, agent_id) do nothing
            "#,
        )
        .bind(channel_id)
        .bind(agent_id)
        .execute(pool)
        .await
        .map_err(to_string)?;
    } else {
        sqlx::query("delete from channel_members where channel_id = $1 and agent_id = $2")
            .bind(channel_id)
            .bind(agent_id)
            .execute(pool)
            .await
            .map_err(to_string)?;
    }

    record_agent_activity(
        pool,
        Some(agent_id),
        None,
        "membership",
        if member {
            "Agent joined channel"
        } else {
            "Agent left channel"
        },
        format!("#{channel_name}"),
    )
    .await?;

    let _ = notify_ui_refresh(pool, "channel_membership_updated").await;
    Ok(())
}

#[tauri::command]
async fn update_owner_profile(
    display_name: String,
    avatar: String,
    description: String,
    state: State<'_, AppState>,
) -> CommandResult<()> {
    update_owner_profile_in_pool(&state.pool, display_name, avatar, description).await
}

pub(crate) async fn update_owner_profile_in_pool(
    pool: &SqlitePool,
    display_name: String,
    avatar: String,
    description: String,
) -> CommandResult<()> {
    let display_name = display_name.trim();
    if display_name.is_empty() {
        return Err("display name is empty".to_owned());
    }

    sqlx::query(
        r#"
        insert into owner_profile (id, display_name, avatar, description, updated_at)
        values (1, $1, $2, $3, strftime('%Y-%m-%dT%H:%M:%f+00:00','now'))
        on conflict (id) do update set
            display_name = excluded.display_name,
            avatar = excluded.avatar,
            description = excluded.description,
            updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
        "#,
    )
    .bind(display_name)
    .bind(avatar.trim())
    .bind(description.trim())
    .execute(pool)
    .await
    .map_err(to_string)?;

    let _ = notify_ui_refresh(pool, "owner_profile_updated").await;
    Ok(())
}

#[tauri::command]
async fn delete_channel(channel_id: Uuid, state: State<'_, AppState>) -> CommandResult<()> {
    delete_channel_in_pool(&state.pool, channel_id).await
}

pub(crate) async fn delete_channel_in_pool(
    pool: &SqlitePool,
    channel_id: Uuid,
) -> CommandResult<()> {
    let result = sqlx::query("delete from channels where id = $1")
        .bind(channel_id)
        .execute(pool)
        .await
        .map_err(to_string)?;
    if result.rows_affected() == 0 {
        return Err("channel does not exist".to_owned());
    }

    let _ = notify_ui_refresh(pool, "channel_deleted").await;

    Ok(())
}

#[tauri::command]
async fn open_dm_with_agent(agent_id: Uuid, state: State<'_, AppState>) -> CommandResult<String> {
    open_dm_with_agent_in_pool(&state.pool, agent_id).await
}

#[tauri::command]
async fn artifact_read(artifact_id: Uuid, state: State<'_, AppState>) -> CommandResult<Artifact> {
    load_artifact(&state.pool, artifact_id).await
}

pub(crate) async fn open_dm_with_agent_in_pool(
    pool: &SqlitePool,
    agent_id: Uuid,
) -> CommandResult<String> {
    let mut tx = pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .map_err(to_string)?;
    let agent_row = sqlx::query("select handle from agents where id = $1")
        .bind(agent_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(to_string)?;
    let Some(agent_row) = agent_row else {
        return Err("agent does not exist".to_owned());
    };
    let agent_handle: String = agent_row.get("handle");

    let existing: Option<Uuid> = sqlx::query_scalar(
        "select id from channels where kind = 'dm' and dm_agent_id = $1 limit 1",
    )
    .bind(agent_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(to_string)?;
    if let Some(channel_id) = existing {
        tx.commit().await.map_err(to_string)?;
        return Ok(channel_id.to_string());
    }

    let channel_id: Uuid = sqlx::query_scalar(
        r#"
        insert into channels (name, description, kind, dm_agent_id)
        values ($1, $2, 'dm', $3)
        returning id
        "#,
    )
    .bind(format!("dm:{agent_id}"))
    .bind(format!("Direct message with @{agent_handle}"))
    .bind(agent_id)
    .fetch_one(&mut *tx)
    .await
    .map_err(to_string)?;

    sqlx::query(
        r#"
        insert into channel_members (channel_id, agent_id)
        values ($1, $2)
        on conflict (channel_id, agent_id) do nothing
        "#,
    )
    .bind(channel_id)
    .bind(agent_id)
    .execute(&mut *tx)
    .await
    .map_err(to_string)?;

    tx.commit().await.map_err(to_string)?;
    record_agent_activity(
        pool,
        Some(agent_id),
        None,
        "dm",
        "Direct message opened",
        format!("@{agent_handle}"),
    )
    .await?;

    let _ = notify_ui_refresh(pool, "dm_opened").await;
    Ok(channel_id.to_string())
}

fn normalize_reasoning_effort(runtime: &str, value: Option<&str>) -> CommandResult<String> {
    if !runtime.eq_ignore_ascii_case("codex") {
        return Ok(String::new());
    }
    let effort = value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("medium")
        .to_ascii_lowercase();
    match effort.as_str() {
        "low" | "medium" | "high" | "xhigh" => Ok(effort),
        _ => Err(format!("invalid Codex reasoning effort: {effort}")),
    }
}

fn normalize_service_tier(runtime: &str, value: Option<&str>) -> CommandResult<String> {
    if !runtime.eq_ignore_ascii_case("codex") {
        return Ok(String::new());
    }
    let tier = value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or_default()
        .to_ascii_lowercase();
    match tier.as_str() {
        "" | "default" => Ok(String::new()),
        "standard" => Ok(String::new()),
        "fast" => Ok(tier),
        _ => Err(format!("invalid Codex speed tier: {tier}")),
    }
}

#[tauri::command]
async fn create_agent(
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
    state: State<'_, AppState>,
) -> CommandResult<String> {
    create_agent_in_pool(
        &state.pool,
        handle,
        display_name,
        role,
        runtime,
        model,
        reasoning_effort,
        service_tier,
        avatar,
        description,
        launch_command,
        working_directory,
        daily_budget_micros,
    )
    .await
    .map(|agent_id| agent_id.to_string())
}

pub(crate) async fn create_agent_in_pool(
    pool: &SqlitePool,
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
) -> CommandResult<Uuid> {
    let normalized_handle = handle.trim().trim_start_matches('@');
    if normalized_handle.is_empty() {
        return Err("agent handle is empty".to_owned());
    }
    let display_name = if display_name.trim().is_empty() {
        normalized_handle
    } else {
        display_name.trim()
    };
    let role = role
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("agent");
    let avatar = avatar
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| {
            normalized_handle
                .chars()
                .next()
                .map(|c| c.to_uppercase().to_string())
                .unwrap_or_else(|| "A".to_owned())
        });
    let description = description
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("Local agent");
    let daily_budget_micros = daily_budget_micros.unwrap_or_default().max(0);
    let working_directory = expand_home_path(&working_directory);
    ensure_agent_workspace(&working_directory, normalized_handle)?;
    let runtime = runtime.trim();
    let model = model.trim();
    let reasoning_effort = normalize_reasoning_effort(runtime, reasoning_effort.as_deref())?;
    let service_tier = normalize_service_tier(runtime, service_tier.as_deref())?;

    let agent_id: Uuid = sqlx::query_scalar(
        r#"
        insert into agents (
            handle, display_name, role, status, runtime, model, avatar, description,
            launch_command, working_directory, daily_budget_micros, reasoning_effort, service_tier
        )
        values ($1, $2, $3, 'idle', $4, $5, $6, $7, $8, $9, $10, $11, $12)
        on conflict (handle) do update set
            display_name = excluded.display_name,
            role = excluded.role,
            runtime = excluded.runtime,
            model = excluded.model,
            avatar = excluded.avatar,
            description = excluded.description,
            launch_command = excluded.launch_command,
            working_directory = excluded.working_directory,
            daily_budget_micros = excluded.daily_budget_micros,
            reasoning_effort = excluded.reasoning_effort,
            service_tier = excluded.service_tier
        returning id
        "#,
    )
    .bind(normalized_handle)
    .bind(display_name)
    .bind(role)
    .bind(runtime)
    .bind(model)
    .bind(avatar)
    .bind(description)
    .bind(launch_command.trim())
    .bind(&working_directory)
    .bind(daily_budget_micros)
    .bind(&reasoning_effort)
    .bind(&service_tier)
    .fetch_one(pool)
    .await
    .map_err(to_string)?;
    record_agent_activity(
        pool,
        Some(agent_id),
        None,
        "profile",
        "Agent profile saved",
        format!(
            "runtime={} model={} reasoning_effort={} service_tier={}",
            runtime, model, reasoning_effort, service_tier
        ),
    )
    .await?;

    let _ = notify_ui_refresh(pool, "agent_created").await;
    Ok(agent_id)
}

#[tauri::command]
async fn update_agent(
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
    state: State<'_, AppState>,
) -> CommandResult<()> {
    update_agent_in_pool(
        &state.pool,
        agent_id,
        handle,
        display_name,
        role,
        runtime,
        model,
        reasoning_effort,
        service_tier,
        avatar,
        description,
        launch_command,
        working_directory,
        daily_budget_micros,
    )
    .await
}

pub(crate) async fn update_agent_in_pool(
    pool: &SqlitePool,
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
) -> CommandResult<()> {
    let normalized_handle = handle.trim().trim_start_matches('@');
    if normalized_handle.is_empty() {
        return Err("agent handle is empty".to_owned());
    }
    let display_name = if display_name.trim().is_empty() {
        normalized_handle
    } else {
        display_name.trim()
    };
    let role = role
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("agent");
    let avatar = avatar
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| {
            normalized_handle
                .chars()
                .next()
                .map(|c| c.to_uppercase().to_string())
                .unwrap_or_else(|| "A".to_owned())
        });
    let daily_budget_micros = daily_budget_micros.unwrap_or_default().max(0);
    let working_directory = expand_home_path(&working_directory);
    ensure_agent_workspace(&working_directory, normalized_handle)?;
    let runtime = runtime.trim();
    let model = model.trim();
    let reasoning_effort = normalize_reasoning_effort(runtime, reasoning_effort.as_deref())?;
    let service_tier = normalize_service_tier(runtime, service_tier.as_deref())?;

    sqlx::query(
        r#"
        update agents
        set handle = $2,
            display_name = $3,
            role = $4,
            runtime = $5,
            model = $6,
            avatar = $7,
            description = $8,
            launch_command = $9,
            working_directory = $10,
            daily_budget_micros = $11,
            reasoning_effort = $12,
            service_tier = $13
        where id = $1
        "#,
    )
    .bind(agent_id)
    .bind(normalized_handle)
    .bind(display_name)
    .bind(role)
    .bind(runtime)
    .bind(model)
    .bind(avatar)
    .bind(description.trim())
    .bind(launch_command.trim())
    .bind(&working_directory)
    .bind(daily_budget_micros)
    .bind(&reasoning_effort)
    .bind(&service_tier)
    .execute(pool)
    .await
    .map_err(to_string)?;
    record_agent_activity(
        pool,
        Some(agent_id),
        None,
        "profile",
        "Agent profile updated",
        format!(
            "runtime={} model={} reasoning_effort={} service_tier={}",
            runtime, model, reasoning_effort, service_tier
        ),
    )
    .await?;

    let _ = notify_ui_refresh(pool, "agent_updated").await;
    Ok(())
}

#[tauri::command]
async fn delete_agent(agent_id: Uuid, state: State<'_, AppState>) -> CommandResult<()> {
    delete_agent_in_pool(&state.pool, agent_id).await
}

pub(crate) async fn delete_agent_in_pool(pool: &SqlitePool, agent_id: Uuid) -> CommandResult<()> {
    let active_run: Option<Uuid> = sqlx::query_scalar(
        r#"
        select id
        from agent_runs
        where agent_id = $1
          and stopped_at is null
          and status in ('starting', 'running', 'stopping')
        limit 1
        "#,
    )
    .bind(agent_id)
    .fetch_optional(pool)
    .await
    .map_err(to_string)?;

    if active_run.is_some() {
        return Err("stop the agent before deleting it".to_owned());
    }

    let agent_handle: Option<String> =
        sqlx::query_scalar("select handle from agents where id = $1")
            .bind(agent_id)
            .fetch_optional(pool)
            .await
            .map_err(to_string)?;
    let Some(agent_handle) = agent_handle else {
        return Err("agent does not exist".to_owned());
    };

    record_agent_activity(
        pool,
        Some(agent_id),
        None,
        "profile",
        "Agent profile deleted",
        format!("@{agent_handle}"),
    )
    .await?;

    let result = sqlx::query("delete from agents where id = $1")
        .bind(agent_id)
        .execute(pool)
        .await
        .map_err(to_string)?;
    if result.rows_affected() == 0 {
        return Err("agent does not exist".to_owned());
    }

    let _ = notify_ui_refresh(pool, "agent_deleted").await;

    Ok(())
}

#[tauri::command]
async fn start_agent(agent_id: Uuid, state: State<'_, AppState>) -> CommandResult<()> {
    let active_run: Option<Uuid> = sqlx::query_scalar(
        r#"
        select id
        from agent_runs
        where agent_id = $1
          and stopped_at is null
          and status in ('starting', 'running', 'stopping')
        limit 1
        "#,
    )
    .bind(agent_id)
    .fetch_optional(&state.pool)
    .await
    .map_err(to_string)?;

    if active_run.is_some() {
        return Err("agent already has an active run".to_owned());
    }

    let pending_start: Option<Uuid> = sqlx::query_scalar(
        r#"
        select id
        from supervisor_commands
        where command_type = 'start_agent'
          and agent_id = $1
          and status in ('pending', 'running')
        limit 1
        "#,
    )
    .bind(agent_id)
    .fetch_optional(&state.pool)
    .await
    .map_err(to_string)?;

    if pending_start.is_some() {
        return Err("agent already has a pending start command".to_owned());
    }

    sqlx::query(
        r#"
        insert into supervisor_commands (command_type, agent_id)
        values ('start_agent', $1)
        "#,
    )
    .bind(agent_id)
    .execute(&state.pool)
    .await
    .map_err(to_string)?;
    let _ = notify_supervisor_wake(&state.pool).await;
    let _ = notify_ui_refresh(&state.pool, "supervisor_command").await;
    sqlx::query("update agents set status = 'queued' where id = $1")
        .bind(agent_id)
        .execute(&state.pool)
        .await
        .map_err(to_string)?;
    record_agent_activity(
        &state.pool,
        Some(agent_id),
        None,
        "run",
        "Start queued",
        "Waiting for supervisor to launch the agent",
    )
    .await?;

    Ok(())
}

#[tauri::command]
async fn dispatch_agent_work(
    agent_id: Uuid,
    channel_id: Option<Uuid>,
    thread_root_id: Option<Uuid>,
    task_id: Option<Uuid>,
    title: String,
    context: String,
    state: State<'_, AppState>,
) -> CommandResult<Uuid> {
    let agent_handle: Option<String> =
        sqlx::query_scalar("select handle from agents where id = $1")
            .bind(agent_id)
            .fetch_optional(&state.pool)
            .await
            .map_err(to_string)?;
    let Some(agent_handle) = agent_handle else {
        return Err("agent does not exist".to_owned());
    };

    let mut resolved_channel_id = channel_id;
    let mut resolved_thread_root_id = thread_root_id;
    let mut resolved_title = title.trim().to_owned();

    if let Some(task_id) = task_id {
        let row = sqlx::query(
            r#"
            select channel_id, message_id, title
            from tasks
            where id = $1
            "#,
        )
        .bind(task_id)
        .fetch_one(&state.pool)
        .await
        .map_err(to_string)?;
        resolved_channel_id = Some(row.get("channel_id"));
        resolved_thread_root_id = Some(row.get("message_id"));
        if resolved_title.is_empty() {
            resolved_title = row.get("title");
        }
    }

    if resolved_channel_id.is_none() {
        if let Some(thread_root_id) = resolved_thread_root_id {
            resolved_channel_id =
                sqlx::query_scalar("select channel_id from messages where id = $1")
                    .bind(thread_root_id)
                    .fetch_optional(&state.pool)
                    .await
                    .map_err(to_string)?;
        }
    }

    if resolved_title.is_empty() {
        resolved_title = match resolved_thread_root_id {
            Some(thread_root_id) => {
                let body: Option<String> =
                    sqlx::query_scalar("select body from messages where id = $1")
                        .bind(thread_root_id)
                        .fetch_optional(&state.pool)
                        .await
                        .map_err(to_string)?;
                body.and_then(|body| {
                    body.lines()
                        .next()
                        .map(|line| line.chars().take(120).collect())
                })
                .filter(|line: &String| !line.trim().is_empty())
                .unwrap_or_else(|| "Lantor agent request".to_owned())
            }
            None => "Lantor agent request".to_owned(),
        };
    }

    let source_kind = if task_id.is_some() { "task" } else { "manual" };
    let inbox_kind = if task_id.is_some() {
        "task_assigned"
    } else {
        "manual"
    };
    let inbox_item_id = create_agent_inbox_item(
        &state.pool,
        AgentInboxItemInput {
            agent_id,
            channel_id: resolved_channel_id,
            thread_root_id: resolved_thread_root_id,
            source_message_id: resolved_thread_root_id,
            task_id,
            kind: inbox_kind,
            priority: if task_id.is_some() { 90 } else { 60 },
            title: &resolved_title,
            body_preview: context.trim(),
            payload: json!({"source_kind": source_kind, "explicit_dispatch": true}),
        },
    )
    .await?;
    let work_context = prepend_inbox_context(inbox_item_id, inbox_kind, context.trim());
    let work_item_id: Uuid = sqlx::query_scalar(
        r#"
        insert into agent_work_items (
            agent_id, channel_id, thread_root_id, inbox_item_id, task_id, source_kind, title, context, status
        )
        values ($1, $2, $3, $4, $5, $6, $7, $8, 'queued')
        returning id
        "#,
    )
    .bind(agent_id)
    .bind(resolved_channel_id)
    .bind(resolved_thread_root_id)
    .bind(inbox_item_id)
    .bind(task_id)
    .bind(source_kind)
    .bind(&resolved_title)
    .bind(&work_context)
    .fetch_one(&state.pool)
    .await
    .map_err(to_string)?;
    attach_work_item_to_inbox(&state.pool, inbox_item_id, work_item_id).await?;
    notify_ui_work_item_changed(&state.pool, work_item_id, "work_item_created").await;

    if let Some(task_id) = task_id {
        sqlx::query(
            r#"
            update tasks
            set assignee_agent_id = $2,
                status = 'in_progress',
                version = version + 1,
                updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
            where id = $1
            "#,
        )
        .bind(task_id)
        .bind(agent_id)
        .execute(&state.pool)
        .await
        .map_err(to_string)?;
    }

    if let (Some(channel_id), Some(thread_root_id)) = (resolved_channel_id, resolved_thread_root_id)
    {
        upsert_agent_thread_subscription(
            &state.pool,
            agent_id,
            channel_id,
            thread_root_id,
            source_kind,
            None,
        )
        .await?;
    }

    let scheduled = enqueue_agent_work_if_available(&state.pool, agent_id, work_item_id).await?;
    record_agent_activity(
        &state.pool,
        Some(agent_id),
        None,
        "dispatch",
        if scheduled {
            "Agent request dispatched"
        } else {
            "Agent request queued"
        },
        format!("#{work_item_id} to @{agent_handle}: {resolved_title}"),
    )
    .await?;

    Ok(work_item_id)
}

async fn dispatch_task_assignment_to_agent(
    pool: &SqlitePool,
    task_id: Uuid,
    agent_id: Uuid,
    reason: &str,
) -> CommandResult<()> {
    let row = sqlx::query(
        r#"
        select t.channel_id, t.message_id, t.title, m.body
        from tasks t
        join messages m on m.id = t.message_id
        where t.id = $1
        "#,
    )
    .bind(task_id)
    .fetch_one(pool)
    .await
    .map_err(to_string)?;
    let channel_id: Uuid = row.get("channel_id");
    let message_id: Uuid = row.get("message_id");
    let title: String = row.get("title");
    let body: String = row.get("body");

    sqlx::query(
        r#"
        insert into channel_members (channel_id, agent_id)
        values ($1, $2)
        on conflict (channel_id, agent_id) do nothing
        "#,
    )
    .bind(channel_id)
    .bind(agent_id)
    .execute(pool)
    .await
    .map_err(to_string)?;

    let inbox_item_id = create_agent_inbox_item(
        pool,
        AgentInboxItemInput {
            agent_id,
            channel_id: Some(channel_id),
            thread_root_id: Some(message_id),
            source_message_id: Some(message_id),
            task_id: Some(task_id),
            kind: "task_assigned",
            priority: 95,
            title: &title,
            body_preview: body.trim(),
            payload: json!({"source_kind": "task", "reason": reason}),
        },
    )
    .await?;
    upsert_agent_thread_subscription(
        pool,
        agent_id,
        channel_id,
        message_id,
        "task",
        Some(message_id),
    )
    .await?;
    let scheduled = ensure_agent_inbox_wake_work_item(pool, agent_id)
        .await?
        .is_some_and(|(_, scheduled)| scheduled);
    let agent_handle = resolve_agent_handle(pool, agent_id)
        .await
        .unwrap_or_else(|_| "unknown".to_owned());
    record_agent_activity(
        pool,
        Some(agent_id),
        None,
        "task",
        if scheduled {
            "Task assignment delivered to inbox"
        } else {
            "Task assignment queued in inbox"
        },
        json!({
            "target_agent": format!("@{agent_handle}"),
            "inbox_item_id": inbox_item_id,
            "task_id": task_id,
            "title": title,
        })
        .to_string(),
    )
    .await?;
    Ok(())
}

async fn dispatch_unassigned_task_availability(
    pool: &SqlitePool,
    task_id: Uuid,
) -> CommandResult<()> {
    let Some(row) = sqlx::query(
        r#"
        select t.channel_id, t.message_id, t.title, t.status, t.assignee_agent_id, m.body, c.name as channel_name
        from tasks t
        join messages m on m.id = t.message_id
        join channels c on c.id = t.channel_id
        where t.id = $1
        "#,
    )
    .bind(task_id)
    .fetch_optional(pool)
    .await
    .map_err(to_string)?
    else {
        return Ok(());
    };
    let status: String = row.get("status");
    let assignee_agent_id: Option<Uuid> = row.get("assignee_agent_id");
    if status != "todo" || assignee_agent_id.is_some() {
        return Ok(());
    }

    let channel_id: Uuid = row.get("channel_id");
    let message_id: Uuid = row.get("message_id");
    let title: String = row.get("title");
    let body: String = row.get("body");
    let channel_name: String = row.get("channel_name");
    let agents = sqlx::query(
        r#"
        select a.id, a.handle
        from channel_members cm
        join agents a on a.id = cm.agent_id
        where cm.channel_id = $1
        order by cm.created_at, a.handle
        "#,
    )
    .bind(channel_id)
    .fetch_all(pool)
    .await
    .map_err(to_string)?;

    for agent in agents {
        let agent_id: Uuid = agent.get("id");
        let agent_handle: String = agent.get("handle");
        let inbox_item_id = create_agent_inbox_item(
            pool,
            AgentInboxItemInput {
                agent_id,
                channel_id: Some(channel_id),
                thread_root_id: Some(message_id),
                source_message_id: Some(message_id),
                task_id: Some(task_id),
                kind: "task_available",
                priority: 70,
                title: &title,
                body_preview: body.trim(),
                payload: json!({
                    "channel_name": &channel_name,
                    "source_kind": "task_available",
                    "claim_contract": "Emit LANTOR_EVENT task_claim only if you can start this task now. The backend will atomically accept one claimant and ignore stale claims.",
                }),
            },
        )
        .await?;
        let scheduled = ensure_agent_inbox_wake_work_item(pool, agent_id)
            .await?
            .is_some_and(|(_, scheduled)| scheduled);
        record_agent_activity(
            pool,
            Some(agent_id),
            None,
            "task",
            if scheduled {
                "Task claim opportunity delivered"
            } else {
                "Task claim opportunity queued"
            },
            json!({
                "target_agent": format!("@{agent_handle}"),
                "inbox_item_id": inbox_item_id,
                "task_id": task_id,
                "title": &title,
            })
            .to_string(),
        )
        .await?;
    }

    Ok(())
}

async fn try_claim_unassigned_task(
    pool: &SqlitePool,
    task_id: Uuid,
    agent_id: Uuid,
    expected_version: Option<i64>,
    reason: &str,
) -> CommandResult<Option<i64>> {
    let claimed = sqlx::query(
        r#"
        update tasks as t
        set assignee_agent_id = $2,
            status = 'in_progress',
            version = t.version + 1,
            updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
        where t.id = $1
          and t.assignee_agent_id is null
          and t.status = 'todo'
          and ($3 is null or t.version = $3)
          and exists (
              select 1
              from channel_members cm
              where cm.channel_id = t.channel_id
                and cm.agent_id = $2
          )
          and not exists (
              select 1
              from tasks active
              where active.assignee_agent_id = $2
                and active.status in ('todo', 'in_progress')
                and active.id <> t.id
          )
        returning number
        "#,
    )
    .bind(task_id)
    .bind(agent_id)
    .bind(expected_version)
    .fetch_optional(pool)
    .await
    .map_err(to_string)?;

    let Some(row) = claimed else {
        return Ok(None);
    };
    let task_number: i64 = row.get("number");
    sqlx::query(
        r#"
        update agent_inbox_items
        set state = 'archived',
            archived_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now'),
            updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
        where task_id = $1
          and kind = 'task_available'
          and state <> 'archived'
        "#,
    )
    .bind(task_id)
    .execute(pool)
    .await
    .map_err(to_string)?;
    dispatch_task_assignment_to_agent(pool, task_id, agent_id, reason).await?;
    record_agent_activity(
        pool,
        Some(agent_id),
        None,
        "task",
        format!("Task #{task_number} claimed"),
        json!({
            "task_id": task_id,
            "reason": reason,
            "optimistic": expected_version.is_some(),
        })
        .to_string(),
    )
    .await?;
    let _ = notify_ui_refresh(pool, "task_claimed").await;
    Ok(Some(task_number))
}

async fn mark_task_after_work_item_finished(
    pool: &SqlitePool,
    work_item_id: Uuid,
    agent_id: Uuid,
    run_id: Uuid,
    work_status: &str,
) -> CommandResult<()> {
    let task_row = sqlx::query(
        r#"
        select t.id, t.number, t.title, t.status
        from agent_work_items w
        join tasks t on t.id = w.task_id
        where w.id = $1
        "#,
    )
    .bind(work_item_id)
    .fetch_optional(pool)
    .await
    .map_err(to_string)?;
    let Some(task_row) = task_row else {
        return Ok(());
    };
    let task_id: Uuid = task_row.get("id");
    let task_number: i64 = task_row.get("number");
    let title: String = task_row.get("title");
    let current_status: String = task_row.get("status");

    if work_status == "done" && matches!(current_status.as_str(), "todo" | "in_progress") {
        sqlx::query(
            r#"
            update tasks
            set status = 'in_review',
                updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
            where id = $1 and status in ('todo', 'in_progress')
            "#,
        )
        .bind(task_id)
        .execute(pool)
        .await
        .map_err(to_string)?;
        let _ = notify_ui_refresh(pool, "task_ready_for_review").await;
        record_agent_activity(
            pool,
            Some(agent_id),
            Some(run_id),
            "task",
            "Task ready for review",
            json!({
                "task_id": task_id,
                "task_number": task_number,
                "work_item_id": work_item_id,
                "title": title,
            })
            .to_string(),
        )
        .await?;
    } else if matches!(work_status, "failed" | "cancelled") {
        record_agent_activity(
            pool,
            Some(agent_id),
            Some(run_id),
            "task",
            if work_status == "failed" {
                "Task run failed"
            } else {
                "Task run cancelled"
            },
            json!({
                "task_id": task_id,
                "task_number": task_number,
                "work_item_id": work_item_id,
                "title": title,
            })
            .to_string(),
        )
        .await?;
    }

    Ok(())
}

#[tauri::command]
async fn cancel_agent_work(work_item_id: Uuid, state: State<'_, AppState>) -> CommandResult<()> {
    cancel_agent_work_in_pool(&state.pool, work_item_id).await
}

pub(crate) async fn cancel_agent_work_in_pool(
    pool: &SqlitePool,
    work_item_id: Uuid,
) -> CommandResult<()> {
    let row = sqlx::query(
        r#"
        select agent_id, run_id, status
        from agent_work_items
        where id = $1
        "#,
    )
    .bind(work_item_id)
    .fetch_one(pool)
    .await
    .map_err(to_string)?;
    let agent_id: Uuid = row.get("agent_id");
    let run_id: Option<Uuid> = row.get("run_id");
    let status: String = row.get("status");

    match status.as_str() {
        "queued" => {
            sqlx::query(
                r#"
                update agent_work_items
                set status = 'cancelled',
                    completed_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now'),
                    updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
                where id = $1
                "#,
            )
            .bind(work_item_id)
            .execute(pool)
            .await
            .map_err(to_string)?;
            notify_ui_work_item_changed(pool, work_item_id, "work_item_cancelled").await;
            sqlx::query(
                r#"
                update supervisor_commands
                set status = 'done',
                    error = 'cancelled',
                    updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
                where work_item_id = $1 and status = 'pending'
                "#,
            )
            .bind(work_item_id)
            .execute(pool)
            .await
            .map_err(to_string)?;
        }
        "running" => {
            let Some(run_id) = run_id else {
                return Err("running agent request does not have a run id".to_owned());
            };
            sqlx::query(
                r#"
                update agent_work_items
                set status = 'cancelling',
                    updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
                where id = $1
                "#,
            )
            .bind(work_item_id)
            .execute(pool)
            .await
            .map_err(to_string)?;
            notify_ui_work_item_changed(pool, work_item_id, "work_item_cancelling").await;
            let pending_stop: Option<Uuid> = sqlx::query_scalar(
                r#"
                select id
                from supervisor_commands
                where command_type = 'stop_run'
                  and run_id = $1
                  and status in ('pending', 'running')
                limit 1
                "#,
            )
            .bind(run_id)
            .fetch_optional(pool)
            .await
            .map_err(to_string)?;
            if pending_stop.is_none() {
                sqlx::query(
                    r#"
                    insert into supervisor_commands (command_type, agent_id, run_id, work_item_id)
                    values ('stop_run', $1, $2, $3)
                    "#,
                )
                .bind(agent_id)
                .bind(run_id)
                .bind(work_item_id)
                .execute(pool)
                .await
                .map_err(to_string)?;
                let _ = notify_supervisor_wake(pool).await;
                let _ = notify_ui_refresh(pool, "supervisor_command").await;
            }
        }
        "cancelling" => return Ok(()),
        other => return Err(format!("cannot cancel agent request with status {other}")),
    }

    record_agent_activity(
        pool,
        Some(agent_id),
        run_id,
        "dispatch",
        "Agent request cancel requested",
        work_item_id.to_string(),
    )
    .await?;

    Ok(())
}

#[tauri::command]
async fn retry_agent_work(work_item_id: Uuid, state: State<'_, AppState>) -> CommandResult<Uuid> {
    retry_agent_work_in_pool(&state.pool, work_item_id).await
}

pub(crate) async fn retry_agent_work_in_pool(
    pool: &SqlitePool,
    work_item_id: Uuid,
) -> CommandResult<Uuid> {
    let row = sqlx::query(
        r#"
        select agent_id, channel_id, thread_root_id, source_message_id, inbox_item_id, task_id, source_kind, title, context, status
        from agent_work_items
        where id = $1
        "#,
    )
    .bind(work_item_id)
    .fetch_one(pool)
    .await
    .map_err(to_string)?;
    let old_status: String = row.get("status");
    if matches!(old_status.as_str(), "queued" | "running" | "cancelling") {
        return Err(format!(
            "cannot retry agent request with status {old_status}"
        ));
    }

    let agent_id: Uuid = row.get("agent_id");
    let title: String = row.get("title");
    let context: String = row.get("context");
    let new_work_item_id: Uuid = sqlx::query_scalar(
        r#"
        insert into agent_work_items (
            agent_id, channel_id, thread_root_id, source_message_id, inbox_item_id, task_id, source_kind, title, context, status
        )
        values ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'queued')
        returning id
        "#,
    )
    .bind(agent_id)
    .bind(row.get::<Option<Uuid>, _>("channel_id"))
    .bind(row.get::<Option<Uuid>, _>("thread_root_id"))
    .bind(row.get::<Option<Uuid>, _>("source_message_id"))
    .bind(row.get::<Option<Uuid>, _>("inbox_item_id"))
    .bind(row.get::<Option<Uuid>, _>("task_id"))
    .bind(row.get::<String, _>("source_kind"))
    .bind(&title)
    .bind(&context)
    .fetch_one(pool)
    .await
    .map_err(to_string)?;
    if let Some(inbox_item_id) = row.get::<Option<Uuid>, _>("inbox_item_id") {
        attach_work_item_to_inbox(pool, inbox_item_id, new_work_item_id).await?;
    }
    notify_ui_work_item_changed(pool, new_work_item_id, "work_item_created").await;

    let scheduled = enqueue_agent_work_if_available(pool, agent_id, new_work_item_id).await?;
    record_agent_activity(
        pool,
        Some(agent_id),
        None,
        "dispatch",
        if scheduled {
            "Agent request retried"
        } else {
            "Retried agent request queued"
        },
        format!("{work_item_id} -> {new_work_item_id}: {title}"),
    )
    .await?;

    Ok(new_work_item_id)
}

async fn agent_runtime(pool: &SqlitePool, agent_id: Uuid) -> CommandResult<Option<String>> {
    sqlx::query_scalar("select runtime from agents where id = $1")
        .bind(agent_id)
        .fetch_optional(pool)
        .await
        .map_err(to_string)
}

async fn agent_has_active_run(pool: &SqlitePool, agent_id: Uuid) -> CommandResult<bool> {
    let active_run: Option<Uuid> = sqlx::query_scalar(
        r#"
        select id
        from agent_runs
        where agent_id = $1
          and stopped_at is null
          and status in ('starting', 'running', 'stopping')
        limit 1
        "#,
    )
    .bind(agent_id)
    .fetch_optional(pool)
    .await
    .map_err(to_string)?;

    Ok(active_run.is_some())
}

async fn agent_has_active_or_pending_start(
    pool: &SqlitePool,
    agent_id: Uuid,
) -> CommandResult<bool> {
    if agent_has_active_run(pool, agent_id).await? {
        return Ok(true);
    }

    let pending_start: Option<Uuid> = sqlx::query_scalar(
        r#"
        select id
        from supervisor_commands
        where command_type = 'start_agent'
          and agent_id = $1
          and status in ('pending', 'running')
        limit 1
        "#,
    )
    .bind(agent_id)
    .fetch_optional(pool)
    .await
    .map_err(to_string)?;

    Ok(pending_start.is_some())
}

fn codex_context_rotate_input_tokens_from_env(value: Option<&str>) -> i64 {
    value
        .and_then(|value| value.trim().parse::<i64>().ok())
        .filter(|tokens| *tokens >= CODEX_CONTEXT_ROTATE_MIN_INPUT_TOKENS)
        .unwrap_or(CODEX_CONTEXT_ROTATE_DEFAULT_INPUT_TOKENS)
}

fn codex_context_rotate_input_tokens() -> i64 {
    codex_context_rotate_input_tokens_from_env(env::var(CODEX_CONTEXT_ROTATE_ENV).ok().as_deref())
}

async fn codex_context_rotation_candidate(
    pool: &SqlitePool,
    agent_id: Uuid,
    threshold: i64,
) -> CommandResult<Option<(Uuid, i64)>> {
    let row = sqlx::query(
        r#"
        select id, input_tokens
        from agent_runs
        where agent_id = $1
          and stopped_at is not null
        order by stopped_at desc
        limit 1
        "#,
    )
    .bind(agent_id)
    .fetch_optional(pool)
    .await
    .map_err(to_string)?;

    Ok(row.and_then(|row| {
        let input_tokens = row.get("input_tokens");
        (input_tokens >= threshold).then(|| (row.get("id"), input_tokens))
    }))
}

async fn enqueue_agent_work_if_available(
    pool: &SqlitePool,
    agent_id: Uuid,
    work_item_id: Uuid,
) -> CommandResult<bool> {
    let status: Option<String> =
        sqlx::query_scalar("select status from agent_work_items where id = $1 and agent_id = $2")
            .bind(work_item_id)
            .bind(agent_id)
            .fetch_optional(pool)
            .await
            .map_err(to_string)?;
    if status.as_deref() != Some("queued") {
        return Ok(false);
    }
    let runtime = agent_runtime(pool, agent_id).await?;
    let is_codex = runtime
        .as_deref()
        .is_some_and(|runtime| runtime.eq_ignore_ascii_case("codex"));
    let has_active_run = agent_has_active_run(pool, agent_id).await?;
    if !is_codex && agent_has_active_or_pending_start(pool, agent_id).await? {
        return Ok(false);
    }

    let pending_for_work: Option<Uuid> = sqlx::query_scalar(
        r#"
        select id
        from supervisor_commands
        where command_type = 'start_agent'
          and work_item_id = $1
          and status in ('pending', 'running')
        limit 1
        "#,
    )
    .bind(work_item_id)
    .fetch_optional(pool)
    .await
    .map_err(to_string)?;
    if pending_for_work.is_some() {
        return Ok(true);
    }

    sqlx::query(
        r#"
        insert into supervisor_commands (command_type, agent_id, work_item_id)
        values ('start_agent', $1, $2)
        "#,
    )
    .bind(agent_id)
    .bind(work_item_id)
    .execute(pool)
    .await
    .map_err(to_string)?;
    let _ = notify_supervisor_wake(pool).await;
    let _ = notify_ui_refresh(pool, "supervisor_command").await;
    sqlx::query("update agents set status = 'queued' where id = $1")
        .bind(agent_id)
        .execute(pool)
        .await
        .map_err(to_string)?;
    if is_codex && has_active_run {
        sqlx::query("update agents set status = 'running' where id = $1")
            .bind(agent_id)
            .execute(pool)
            .await
            .map_err(to_string)?;
    }

    Ok(true)
}

fn extract_agent_mentions(body: &str) -> Vec<String> {
    let mut handles = Vec::new();
    let mut chars = body.char_indices().peekable();
    while let Some((idx, ch)) = chars.next() {
        if ch != '@' {
            continue;
        }
        if body[..idx]
            .chars()
            .next_back()
            .map(|prev| prev.is_ascii_alphanumeric() || prev == '_' || prev == '-' || prev == '.')
            .unwrap_or(false)
        {
            continue;
        }
        let mut handle = String::new();
        while let Some((_, next)) = chars.peek().copied() {
            if next.is_ascii_alphanumeric() || next == '_' || next == '-' {
                handle.push(next);
                chars.next();
            } else {
                break;
            }
        }
        if !handle.is_empty() && !handles.contains(&handle) {
            handles.push(handle);
        }
    }
    handles
}

#[derive(Clone, Copy)]
enum MentionDispatchOrigin {
    Owner,
    Agent {
        sender_agent_id: Uuid,
        allow_channel_member_invite: bool,
    },
}

impl MentionDispatchOrigin {
    fn sender_agent_id(self) -> Option<Uuid> {
        match self {
            MentionDispatchOrigin::Owner => None,
            MentionDispatchOrigin::Agent {
                sender_agent_id, ..
            } => Some(sender_agent_id),
        }
    }

    fn allows_dm_auto_dispatch(self) -> bool {
        matches!(self, MentionDispatchOrigin::Owner)
    }

    fn is_agent(self) -> bool {
        matches!(self, MentionDispatchOrigin::Agent { .. })
    }

    fn allows_channel_member_invite(self) -> bool {
        match self {
            MentionDispatchOrigin::Owner => true,
            MentionDispatchOrigin::Agent {
                allow_channel_member_invite,
                ..
            } => allow_channel_member_invite,
        }
    }
}

const INTER_AGENT_THREAD_MESSAGE_LIMIT: i64 = 10;

#[derive(Clone, Copy, PartialEq, Eq)]
enum DispatchKind {
    ChannelMessage,
    Mention,
    Dm,
    ThreadFollowUp,
}

struct AgentInboxItemInput<'a> {
    agent_id: Uuid,
    channel_id: Option<Uuid>,
    thread_root_id: Option<Uuid>,
    source_message_id: Option<Uuid>,
    task_id: Option<Uuid>,
    kind: &'a str,
    priority: i32,
    title: &'a str,
    body_preview: &'a str,
    payload: Value,
}

#[derive(Clone)]
struct InboxWakeItem {
    id: Uuid,
    channel_id: Option<Uuid>,
    channel_name: Option<String>,
    channel_kind: Option<String>,
    thread_root_id: Option<Uuid>,
    source_message_id: Option<Uuid>,
    task_id: Option<Uuid>,
    kind: String,
    priority: i32,
    title: String,
    body_preview: String,
    message_created_at: Option<DateTime<Utc>>,
    sender_name: Option<String>,
    sender_role: Option<String>,
}

impl InboxWakeItem {
    fn target(&self) -> String {
        format_inbox_target(
            self.channel_kind.as_deref(),
            self.channel_name.as_deref(),
            self.thread_root_id,
        )
    }

    fn message_header(&self) -> String {
        let msg = self
            .source_message_id
            .map(short_id)
            .unwrap_or_else(|| short_id(self.id));
        let time = self
            .message_created_at
            .map(|time| time.to_rfc3339())
            .unwrap_or_else(|| "-".to_owned());
        let sender_role = self.sender_role.as_deref().unwrap_or("unknown");
        let sender = self
            .sender_name
            .as_deref()
            .filter(|name| !name.trim().is_empty())
            .unwrap_or("unknown");
        let preview = compact_chars_middle(&self.body_preview, DISPATCH_MESSAGE_BODY_LIMIT)
            .replace('\n', " ");
        format!(
            "[target={} msg={} time={} type={}] {}: {}",
            self.target(),
            msg,
            time,
            sender_role,
            sender,
            preview
        )
    }
}

struct InboxWakeSummary {
    target: String,
    count: i64,
}

async fn create_agent_inbox_item(
    pool: &SqlitePool,
    input: AgentInboxItemInput<'_>,
) -> CommandResult<Uuid> {
    if let Some(source_message_id) = input.source_message_id {
        let existing_id: Option<Uuid> = sqlx::query_scalar(
            r#"
            select id
            from agent_inbox_items
            where agent_id = $1 and source_message_id = $2 and kind = $3
            limit 1
            "#,
        )
        .bind(input.agent_id)
        .bind(source_message_id)
        .bind(input.kind)
        .fetch_optional(pool)
        .await
        .map_err(to_string)?;
        if let Some(existing_id) = existing_id {
            sqlx::query(
                r#"
                update agent_inbox_items
                set channel_id = $2,
                    thread_root_id = $3,
                    task_id = $4,
                    priority = max(priority, $5),
                    state = case when state = 'archived' then 'unread' else state end,
                    title = $6,
                    body_preview = $7,
                    payload = $8,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now'),
                    archived_at = case when state = 'archived' then null else archived_at end
                where id = $1
                "#,
            )
            .bind(existing_id)
            .bind(input.channel_id)
            .bind(input.thread_root_id)
            .bind(input.task_id)
            .bind(input.priority)
            .bind(input.title)
            .bind(compact_chars_middle(
                input.body_preview,
                DISPATCH_MESSAGE_BODY_LIMIT,
            ))
            .bind(input.payload)
            .execute(pool)
            .await
            .map_err(to_string)?;
            return Ok(existing_id);
        }
    }

    let inbox_item_id: Uuid = sqlx::query_scalar(
        r#"
        insert into agent_inbox_items (
            agent_id, channel_id, thread_root_id, source_message_id, task_id,
            kind, priority, state, title, body_preview, payload
        )
        values ($1, $2, $3, $4, $5, $6, $7, 'unread', $8, $9, $10)
        returning id
        "#,
    )
    .bind(input.agent_id)
    .bind(input.channel_id)
    .bind(input.thread_root_id)
    .bind(input.source_message_id)
    .bind(input.task_id)
    .bind(input.kind)
    .bind(input.priority)
    .bind(input.title)
    .bind(compact_chars_middle(
        input.body_preview,
        DISPATCH_MESSAGE_BODY_LIMIT,
    ))
    .bind(input.payload)
    .fetch_one(pool)
    .await
    .map_err(to_string)?;
    Ok(inbox_item_id)
}

async fn attach_work_item_to_inbox(
    pool: &SqlitePool,
    inbox_item_id: Uuid,
    work_item_id: Uuid,
) -> CommandResult<()> {
    attach_work_item_to_inboxes(pool, &[inbox_item_id], work_item_id).await
}

async fn attach_work_item_to_inboxes(
    pool: &SqlitePool,
    inbox_item_ids: &[Uuid],
    work_item_id: Uuid,
) -> CommandResult<()> {
    if inbox_item_ids.is_empty() {
        return Ok(());
    }
    for inbox_item_id in inbox_item_ids {
        sqlx::query(
            r#"
            update agent_inbox_items
            set work_item_id = $2,
                state = 'processing',
                updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now'),
                archived_at = null
            where id = $1
            "#,
        )
        .bind(*inbox_item_id)
        .bind(work_item_id)
        .execute(pool)
        .await
        .map_err(to_string)?;
    }
    Ok(())
}

fn format_inbox_target(
    channel_kind: Option<&str>,
    channel_name: Option<&str>,
    thread_root_id: Option<Uuid>,
) -> String {
    match (channel_kind, channel_name, thread_root_id) {
        (Some("dm"), Some(name), Some(thread_root_id)) => {
            format!("dm:{name}:{}", short_id(thread_root_id))
        }
        (Some("dm"), Some(name), None) => format!("dm:{name}"),
        (_, Some(name), Some(thread_root_id)) => format!("#{name}:{}", short_id(thread_root_id)),
        (_, Some(name), None) => format!("#{name}"),
        _ => "unknown target".to_owned(),
    }
}

fn inbox_wake_item_from_row(row: &SqliteRow) -> InboxWakeItem {
    InboxWakeItem {
        id: row.get("id"),
        channel_id: row.get("channel_id"),
        channel_name: row.get("channel_name"),
        channel_kind: row.get("channel_kind"),
        thread_root_id: row.get("thread_root_id"),
        source_message_id: row.get("source_message_id"),
        task_id: row.get("task_id"),
        kind: row.get("kind"),
        priority: row.get("priority"),
        title: row.get("title"),
        body_preview: row.get("body_preview"),
        message_created_at: row.get("message_created_at"),
        sender_name: row.get("sender_name"),
        sender_role: row.get("sender_role"),
    }
}

async fn next_unread_inbox_wake_item(
    pool: &SqlitePool,
    agent_id: Uuid,
) -> CommandResult<Option<InboxWakeItem>> {
    let row = sqlx::query(
        r#"
        select
            i.id,
            i.channel_id,
            c.name as channel_name,
            c.kind as channel_kind,
            i.thread_root_id,
            i.source_message_id,
            i.task_id,
            i.kind,
            i.priority,
            i.title,
            i.body_preview,
            m.created_at as message_created_at,
            m.sender_name,
            m.sender_role
        from agent_inbox_items i
        left join channels c on c.id = i.channel_id
        left join messages m on m.id = i.source_message_id
        where i.agent_id = $1
          and i.state = 'unread'
        order by i.priority desc, i.created_at asc
        limit 1
        "#,
    )
    .bind(agent_id)
    .fetch_optional(pool)
    .await
    .map_err(to_string)?;

    Ok(row.as_ref().map(inbox_wake_item_from_row))
}

async fn load_unread_inbox_wake_batch(
    pool: &SqlitePool,
    agent_id: Uuid,
    channel_id: Option<Uuid>,
    thread_root_id: Option<Uuid>,
) -> CommandResult<Vec<InboxWakeItem>> {
    let rows = sqlx::query(
        r#"
        select
            i.id,
            i.channel_id,
            c.name as channel_name,
            c.kind as channel_kind,
            i.thread_root_id,
            i.source_message_id,
            i.task_id,
            i.kind,
            i.priority,
            i.title,
            i.body_preview,
            m.created_at as message_created_at,
            m.sender_name,
            m.sender_role
        from agent_inbox_items i
        left join channels c on c.id = i.channel_id
        left join messages m on m.id = i.source_message_id
        where i.agent_id = $1
          and i.state = 'unread'
          and i.channel_id is not distinct from $2
          and i.thread_root_id is not distinct from $3
        order by i.priority desc, i.created_at asc
        limit $4
        "#,
    )
    .bind(agent_id)
    .bind(channel_id)
    .bind(thread_root_id)
    .bind(INBOX_WAKE_BATCH_LIMIT)
    .fetch_all(pool)
    .await
    .map_err(to_string)?;

    Ok(rows.iter().map(inbox_wake_item_from_row).collect())
}

async fn load_inbox_wake_items_for_work_item(
    pool: &SqlitePool,
    work_item_id: Uuid,
) -> CommandResult<Vec<InboxWakeItem>> {
    let rows = sqlx::query(
        r#"
        select
            i.id,
            i.channel_id,
            c.name as channel_name,
            c.kind as channel_kind,
            i.thread_root_id,
            i.source_message_id,
            i.task_id,
            i.kind,
            i.priority,
            i.title,
            i.body_preview,
            m.created_at as message_created_at,
            m.sender_name,
            m.sender_role
        from agent_inbox_items i
        left join channels c on c.id = i.channel_id
        left join messages m on m.id = i.source_message_id
        where i.work_item_id = $1
        order by i.priority desc, i.created_at asc
        "#,
    )
    .bind(work_item_id)
    .fetch_all(pool)
    .await
    .map_err(to_string)?;

    Ok(rows.iter().map(inbox_wake_item_from_row).collect())
}

async fn load_other_active_inbox_summary(
    pool: &SqlitePool,
    agent_id: Uuid,
    channel_id: Option<Uuid>,
    thread_root_id: Option<Uuid>,
) -> CommandResult<Vec<InboxWakeSummary>> {
    let rows = sqlx::query(
        r#"
        select
            i.channel_id,
            c.name as channel_name,
            c.kind as channel_kind,
            i.thread_root_id,
            count(*) as item_count,
            max(i.priority) as max_priority,
            min(i.created_at) as oldest_created_at
        from agent_inbox_items i
        left join channels c on c.id = i.channel_id
        where i.agent_id = $1
          and i.state in ('unread', 'processing')
          and not (
              i.channel_id is not distinct from $2
              and i.thread_root_id is not distinct from $3
          )
        group by i.channel_id, c.name, c.kind, i.thread_root_id
        order by max_priority desc, oldest_created_at asc
        limit $4
        "#,
    )
    .bind(agent_id)
    .bind(channel_id)
    .bind(thread_root_id)
    .bind(INBOX_WAKE_OTHER_SUMMARY_LIMIT)
    .fetch_all(pool)
    .await
    .map_err(to_string)?;

    Ok(rows
        .iter()
        .map(|row| {
            let channel_name: Option<String> = row.get("channel_name");
            let channel_kind: Option<String> = row.get("channel_kind");
            let thread_root_id: Option<Uuid> = row.get("thread_root_id");
            InboxWakeSummary {
                target: format_inbox_target(
                    channel_kind.as_deref(),
                    channel_name.as_deref(),
                    thread_root_id,
                ),
                count: row.get("item_count"),
            }
        })
        .collect())
}

async fn find_queued_inbox_wake_work_item_for_surface(
    pool: &SqlitePool,
    agent_id: Uuid,
    channel_id: Option<Uuid>,
    thread_root_id: Option<Uuid>,
) -> CommandResult<Option<Uuid>> {
    sqlx::query_scalar(
        r#"
        select id
        from agent_work_items
        where agent_id = $1
          and source_kind = 'inbox_wake'
          and status = 'queued'
          and channel_id is not distinct from $2
          and thread_root_id is not distinct from $3
        order by created_at asc
        limit 1
        "#,
    )
    .bind(agent_id)
    .bind(channel_id)
    .bind(thread_root_id)
    .fetch_optional(pool)
    .await
    .map_err(to_string)
}

fn inbox_wake_work_item_title(items: &[InboxWakeItem]) -> String {
    let Some(primary) = items.first() else {
        return "Process inbox".to_owned();
    };
    if items.len() == 1 {
        format!("Process inbox: {}", primary.title)
    } else {
        format!(
            "Process inbox: {} (+{} more)",
            primary.title,
            items.len() - 1
        )
    }
}

fn inbox_wake_context(items: &[InboxWakeItem], other_active: &[InboxWakeSummary]) -> String {
    let Some(primary) = items.first() else {
        return "Lantor agent inbox wake.".to_owned();
    };
    let target = primary.target();
    let has_task_available = items.iter().any(|item| item.kind == "task_available");
    let mut lines = vec![
        "Lantor agent inbox wake.".to_owned(),
        if items.len() == 1 {
            "The default inbox item below is already selected for this turn. Handle it directly from this context when enough detail is present.".to_owned()
        } else {
            format!(
                "This turn batches {} inbox items from the same channel/thread target. Handle them together when possible.",
                items.len()
            )
        },
        "The message headers below include target, source message id, created time, sender type/name, and preview. Handle directly from them when enough detail is present.".to_owned(),
        "Warm-runtime guard: the inbox item and its thread are authoritative over older context from other channels or tasks.".to_owned(),
        "For thread follow-ups or contextual references like continue/this fix/that change/above/same issue/继续/这样修/上面/这个, use history-read on the default reply target before answering unless the needed same-thread context is already present.".to_owned(),
        "Use \"$LANTOR_CONTEXT_TOOL\" --agent-context-tool inbox-read --inbox-id <id> only if the preview/header is insufficient and you need a full source message or metadata.".to_owned(),
        "Use \"$LANTOR_CONTEXT_TOOL\" --agent-context-tool inbox-list --state active --limit 20 only if you need to inspect or choose among other active inbox items.".to_owned(),
        "Current work-item inbox item(s) are archived automatically when this work item finishes; use inbox-archive only for unrelated or extra active items you intentionally clear.".to_owned(),
        String::new(),
        format!("Default reply target for normal assistant text: {target}"),
        "If you handle another inbox item in this same turn with a different target, post to that item's channel/thread with channel_message_create instead of relying on the default route.".to_owned(),
        String::new(),
    ];
    if has_task_available {
        lines.extend([
            "Task claim opportunity mode:".to_owned(),
            "- This is a competitive, unassigned task opportunity sent to multiple agents.".to_owned(),
            "- If you can start now, emit only the standalone `LANTOR_EVENT {\"type\":\"task_claim\",\"task_number\":...}` control line, then finish with `LANTOR_SILENT_REPLY: claim attempted`.".to_owned(),
            "- Do not post a visible reply, do not narrate that you are queued/starting, and do not emit activity events for the claim attempt.".to_owned(),
            "- Lantor atomically accepts one claimant, ignores stale claims, and will send a separate task_assigned inbox turn to the winning agent to do the work visibly.".to_owned(),
            String::new(),
        ]);
    }

    if items.len() == 1 {
        lines.push("Default inbox item:".to_owned());
    } else {
        lines.push("Batched inbox items:".to_owned());
    }
    for (index, item) in items.iter().enumerate() {
        if items.len() > 1 {
            lines.push(format!("{}. {}", index + 1, item.message_header()));
        } else {
            lines.push(item.message_header());
        }
        lines.push(format!("   inbox_id: {}", item.id));
        lines.push(format!(
            "   kind: {}, priority: {}, title: {}",
            item.kind, item.priority, item.title
        ));
        if index + 1 < items.len() {
            lines.push(String::new());
        }
    }

    if !other_active.is_empty() {
        lines.push(String::new());
        lines.push("Other active inbox targets:".to_owned());
        for summary in other_active {
            lines.push(format!("- {}: {} active", summary.target, summary.count));
        }
        lines.push("Stay focused on the selected item(s) above unless another active target is clearly higher priority.".to_owned());
    }

    lines.join("\n")
}

fn build_steer_followup_prompt(items: &[InboxWakeItem]) -> String {
    let Some(primary) = items.first() else {
        return "Same-channel/thread live inbox follow-up.".to_owned();
    };
    let target = primary.target();
    let mut lines = vec![
        "Same-channel/thread live inbox follow-up.".to_owned(),
        "Treat the message header(s) below as newer input for the active turn.".to_owned(),
        "If the latest owner message explicitly mentions another agent and does not mention you, stop that newly assigned work and reply silently unless directly asked to acknowledge.".to_owned(),
        format!("Default reply target for normal assistant text: {target}"),
        "Current work-item inbox item(s) are archived automatically when the active turn finishes; use inbox-archive only for unrelated or extra active items you intentionally clear.".to_owned(),
        String::new(),
    ];

    if items.len() == 1 {
        lines.push("New inbox message:".to_owned());
    } else {
        lines.push("New inbox messages:".to_owned());
    }
    for (index, item) in items.iter().enumerate() {
        if items.len() > 1 {
            lines.push(format!("{}. {}", index + 1, item.message_header()));
        } else {
            lines.push(item.message_header());
        }
        lines.push(format!("   inbox_id: {}", item.id));
        if index + 1 < items.len() {
            lines.push(String::new());
        }
    }

    lines.join("\n")
}

async fn refresh_inbox_wake_work_item(
    pool: &SqlitePool,
    agent_id: Uuid,
    work_item_id: Uuid,
    items: &[InboxWakeItem],
) -> CommandResult<()> {
    let Some(primary) = items.first() else {
        return Ok(());
    };
    let other_active =
        load_other_active_inbox_summary(pool, agent_id, primary.channel_id, primary.thread_root_id)
            .await?;
    sqlx::query(
        r#"
        update agent_work_items
        set channel_id = $2,
            thread_root_id = $3,
            source_message_id = $4,
            inbox_item_id = $5,
            task_id = $6,
            title = $7,
            context = $8,
            updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
        where id = $1
        "#,
    )
    .bind(work_item_id)
    .bind(primary.channel_id)
    .bind(primary.thread_root_id)
    .bind(primary.source_message_id)
    .bind(primary.id)
    .bind(primary.task_id)
    .bind(inbox_wake_work_item_title(items))
    .bind(inbox_wake_context(items, &other_active))
    .execute(pool)
    .await
    .map_err(to_string)?;
    Ok(())
}

fn prepend_inbox_context(inbox_item_id: Uuid, kind: &str, context: &str) -> String {
    let mut lines = vec![
        "Agent inbox item:".to_owned(),
        format!("id: {inbox_item_id}"),
        format!("kind: {kind}"),
        "decision: decide whether this inbox item needs a visible reply, a task claim/create, a reminder, a handoff, or a silent_reply.".to_owned(),
    ];
    if !context.trim().is_empty() {
        lines.push(String::new());
        lines.push(context.trim().to_owned());
    }
    lines.join("\n")
}

async fn sync_inbox_for_work_item(pool: &SqlitePool, work_item_id: Uuid) -> CommandResult<()> {
    let row = sqlx::query(
        r#"
        select agent_id, source_kind, status
        from agent_work_items
        where id = $1
        "#,
    )
    .bind(work_item_id)
    .fetch_optional(pool)
    .await
    .map_err(to_string)?;
    let Some(row) = row else {
        return Ok(());
    };
    let agent_id: Uuid = row.get("agent_id");
    let source_kind: String = row.get("source_kind");
    let status: String = row.get("status");
    let inbox_state = match status.as_str() {
        "queued" | "running" | "cancelling" => "processing",
        "done" | "failed" | "cancelled" | "silent" => "archived",
        _ => "processing",
    };
    sqlx::query(
        r#"
        update agent_inbox_items
        set state = $2,
            updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now'),
            archived_at = case when $2 = 'archived' then strftime('%Y-%m-%dT%H:%M:%f+00:00','now') else null end
        where work_item_id = $1
        "#,
    )
    .bind(work_item_id)
    .bind(inbox_state)
    .execute(pool)
    .await
    .map_err(to_string)?;
    if source_kind == "inbox_wake" && matches!(status.as_str(), "done" | "failed" | "cancelled") {
        let _ = Box::pin(ensure_agent_inbox_wake_work_item(pool, agent_id)).await?;
    }
    Ok(())
}

async fn ensure_agent_inbox_wake_work_item(
    pool: &SqlitePool,
    agent_id: Uuid,
) -> CommandResult<Option<(Uuid, bool)>> {
    let Some(primary) = next_unread_inbox_wake_item(pool, agent_id).await? else {
        return Ok(None);
    };
    let mut batch =
        load_unread_inbox_wake_batch(pool, agent_id, primary.channel_id, primary.thread_root_id)
            .await?;
    if batch.is_empty() {
        batch.push(primary);
    }
    let inbox_item_ids: Vec<Uuid> = batch.iter().map(|item| item.id).collect();

    if let Some(existing_work_item_id) = find_queued_inbox_wake_work_item_for_surface(
        pool,
        agent_id,
        batch[0].channel_id,
        batch[0].thread_root_id,
    )
    .await?
    {
        attach_work_item_to_inboxes(pool, &inbox_item_ids, existing_work_item_id).await?;
        let items = load_inbox_wake_items_for_work_item(pool, existing_work_item_id).await?;
        refresh_inbox_wake_work_item(pool, agent_id, existing_work_item_id, &items).await?;
        notify_ui_work_item_changed(pool, existing_work_item_id, "work_item_merged").await;
        let scheduled =
            enqueue_agent_work_if_available(pool, agent_id, existing_work_item_id).await?;
        let detail = format!(
            "{}: {}",
            items
                .first()
                .map(InboxWakeItem::target)
                .unwrap_or_else(|| "unknown target".to_owned()),
            inbox_wake_work_item_title(&items)
        );
        record_agent_activity(
            pool,
            Some(agent_id),
            None,
            "inbox",
            if scheduled {
                "Inbox wake merged and dispatched"
            } else {
                "Inbox wake merged"
            },
            detail,
        )
        .await?;
        return Ok(Some((existing_work_item_id, scheduled)));
    }

    let other_active = load_other_active_inbox_summary(
        pool,
        agent_id,
        batch[0].channel_id,
        batch[0].thread_root_id,
    )
    .await?;
    let work_item_id: Uuid = sqlx::query_scalar(
        r#"
        insert into agent_work_items (
            agent_id, channel_id, thread_root_id, source_message_id, inbox_item_id, task_id,
            source_kind, title, context, status
        )
        values ($1, $2, $3, $4, $5, $6, 'inbox_wake', $7, $8, 'queued')
        returning id
        "#,
    )
    .bind(agent_id)
    .bind(batch[0].channel_id)
    .bind(batch[0].thread_root_id)
    .bind(batch[0].source_message_id)
    .bind(batch[0].id)
    .bind(batch[0].task_id)
    .bind(inbox_wake_work_item_title(&batch))
    .bind(inbox_wake_context(&batch, &other_active))
    .fetch_one(pool)
    .await
    .map_err(to_string)?;
    attach_work_item_to_inboxes(pool, &inbox_item_ids, work_item_id).await?;
    notify_ui_work_item_changed(pool, work_item_id, "work_item_created").await;
    let scheduled = enqueue_agent_work_if_available(pool, agent_id, work_item_id).await?;
    let target = batch
        .first()
        .map(InboxWakeItem::target)
        .unwrap_or_else(|| "unknown target".to_owned());
    record_agent_activity(
        pool,
        Some(agent_id),
        None,
        "inbox",
        if scheduled {
            "Inbox wake dispatched"
        } else {
            "Inbox wake queued"
        },
        format!("{target}: {}", inbox_wake_work_item_title(&batch)),
    )
    .await?;
    Ok(Some((work_item_id, scheduled)))
}

async fn upsert_agent_thread_subscription(
    pool: &SqlitePool,
    agent_id: Uuid,
    channel_id: Uuid,
    thread_root_id: Uuid,
    source_kind: &str,
    source_message_id: Option<Uuid>,
) -> CommandResult<()> {
    sqlx::query(
        r#"
        insert into agent_thread_subscriptions (
            agent_id, channel_id, thread_root_id, source_kind, last_source_message_id
        )
        values ($1, $2, $3, $4, $5)
        on conflict (agent_id, thread_root_id) do update
        set channel_id = excluded.channel_id,
            source_kind = excluded.source_kind,
            last_source_message_id = coalesce(excluded.last_source_message_id, agent_thread_subscriptions.last_source_message_id),
            updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
        "#,
    )
    .bind(agent_id)
    .bind(channel_id)
    .bind(thread_root_id)
    .bind(source_kind)
    .bind(source_message_id)
    .execute(pool)
    .await
    .map_err(to_string)?;
    Ok(())
}

async fn queue_mentions_as_work_items(
    pool: &SqlitePool,
    channel_id: Uuid,
    thread_root_id: Option<Uuid>,
    message_id: Uuid,
    task_id: Option<Uuid>,
    body: &str,
    origin: MentionDispatchOrigin,
) -> CommandResult<()> {
    let mentions = extract_agent_mentions(body);
    let channel_row = sqlx::query("select name, kind, dm_agent_id from channels where id = $1")
        .bind(channel_id)
        .fetch_one(pool)
        .await
        .map_err(to_string)?;
    let channel_name: String = channel_row.get("name");
    let channel_kind: String = channel_row.get("kind");
    let dm_agent_id: Option<Uuid> = channel_row.get("dm_agent_id");

    let mut targets = Vec::new();
    let mut dispatch_kind = DispatchKind::Mention;
    if channel_kind == "dm" {
        dispatch_kind = DispatchKind::Dm;
        if !origin.allows_dm_auto_dispatch() {
            return Ok(());
        }
        if let Some(agent_id) = dm_agent_id {
            let agent_handle: Option<String> =
                sqlx::query_scalar("select handle from agents where id = $1")
                    .bind(agent_id)
                    .fetch_optional(pool)
                    .await
                    .map_err(to_string)?;
            if let Some(agent_handle) = agent_handle {
                targets.push((agent_id, agent_handle));
            }
        }
    } else {
        for handle in &mentions {
            let agent_id: Option<Uuid> =
                sqlx::query_scalar("select id from agents where handle = $1")
                    .bind(handle)
                    .fetch_optional(pool)
                    .await
                    .map_err(to_string)?;
            let Some(agent_id) = agent_id else {
                continue;
            };
            if Some(agent_id) == origin.sender_agent_id() {
                continue;
            }
            if origin.is_agent() && !origin.allows_channel_member_invite() {
                let is_channel_member: bool = sqlx::query_scalar(
                    r#"
                    select exists (
                        select 1
                        from channel_members
                        where channel_id = $1 and agent_id = $2
                    )
                    "#,
                )
                .bind(channel_id)
                .bind(agent_id)
                .fetch_one(pool)
                .await
                .map_err(to_string)?;
                if !is_channel_member {
                    continue;
                }
            }
            targets.push((agent_id, handle.clone()));
        }
        if mentions.is_empty()
            && targets.is_empty()
            && matches!(origin, MentionDispatchOrigin::Owner)
        {
            if let Some(thread_root_id) = thread_root_id {
                targets = load_thread_followup_targets(pool, channel_id, thread_root_id).await?;
                if !targets.is_empty() {
                    dispatch_kind = DispatchKind::ThreadFollowUp;
                }
            } else if task_id.is_none() {
                targets = load_channel_root_delivery_targets(pool, channel_id).await?;
                if !targets.is_empty() {
                    dispatch_kind = DispatchKind::ChannelMessage;
                }
            } else {
                let channel_targets = load_channel_root_delivery_targets(pool, channel_id).await?;
                if channel_targets.len() == 1 {
                    targets = channel_targets;
                    dispatch_kind = DispatchKind::ChannelMessage;
                }
            }
        }
    }

    if targets.is_empty() {
        return Ok(());
    }
    if task_id.is_some() {
        targets.truncate(1);
    }
    let reply_thread_root_id = thread_root_id.unwrap_or(message_id);
    if origin.is_agent()
        && inter_agent_thread_message_count_since_last_owner(pool, channel_id, reply_thread_root_id)
            .await?
            >= INTER_AGENT_THREAD_MESSAGE_LIMIT
    {
        insert_system_message(
            pool,
            channel_id,
            Some(reply_thread_root_id),
            format!(
                "Inter-agent collaboration paused: this thread reached {INTER_AGENT_THREAD_MESSAGE_LIMIT} agent messages. Add a human reply to continue."
            ),
        )
        .await?;
        return Ok(());
    }

    let title = body
        .lines()
        .next()
        .map(|line| line.chars().take(120).collect::<String>())
        .filter(|line| !line.trim().is_empty())
        .unwrap_or_else(|| match dispatch_kind {
            DispatchKind::ChannelMessage => format!("Channel message in #{channel_name}"),
            DispatchKind::Dm => format!("DM in #{channel_name}"),
            DispatchKind::Mention => format!("Mention in #{channel_name}"),
            DispatchKind::ThreadFollowUp => format!("Thread follow-up in #{channel_name}"),
        });

    if let (Some(task_id), Some((agent_id, _))) = (task_id, targets.first()) {
        sqlx::query(
            r#"
            update tasks
            set assignee_agent_id = $2,
                status = 'in_progress',
                version = version + 1,
                updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
            where id = $1 and assignee_agent_id is null
            "#,
        )
        .bind(task_id)
        .bind(*agent_id)
        .execute(pool)
        .await
        .map_err(to_string)?;
    }

    for (agent_id, agent_handle) in targets {
        let already_queued: bool = sqlx::query_scalar(
            r#"
            select exists (
                select 1
                from agent_work_items
                where source_message_id = $1 and agent_id = $2
            )
            "#,
        )
        .bind(message_id)
        .bind(agent_id)
        .fetch_one(pool)
        .await
        .map_err(to_string)?;
        if already_queued {
            continue;
        }

        sqlx::query(
            r#"
            insert into channel_members (channel_id, agent_id)
            values ($1, $2)
            on conflict (channel_id, agent_id) do nothing
            "#,
        )
        .bind(channel_id)
        .bind(agent_id)
        .execute(pool)
        .await
        .map_err(to_string)?;

        let source_kind = if task_id.is_some() {
            "task"
        } else {
            match (dispatch_kind, origin.is_agent()) {
                (DispatchKind::ChannelMessage, _) => "channel_message",
                (DispatchKind::Dm, _) => "dm",
                (DispatchKind::ThreadFollowUp, _) => "thread_followup",
                (DispatchKind::Mention, true) => "collaboration",
                (DispatchKind::Mention, false) => "mention",
            }
        };
        let inbox_kind = if task_id.is_some() {
            "task_assigned"
        } else {
            source_kind
        };
        let priority = match (dispatch_kind, task_id.is_some(), origin.is_agent()) {
            (_, true, _) => 95,
            (DispatchKind::Dm, _, _) => 85,
            (DispatchKind::Mention, _, false) => 80,
            (DispatchKind::Mention, _, true) => 70,
            (DispatchKind::ThreadFollowUp, _, _) => 60,
            (DispatchKind::ChannelMessage, _, _) => 35,
        };
        let inbox_item_id = create_agent_inbox_item(
            pool,
            AgentInboxItemInput {
                agent_id,
                channel_id: Some(channel_id),
                thread_root_id: Some(reply_thread_root_id),
                source_message_id: Some(message_id),
                task_id,
                kind: inbox_kind,
                priority,
                title: &title,
                body_preview: body,
                payload: json!({
                    "channel_name": &channel_name,
                    "source_kind": source_kind,
                    "origin": if origin.is_agent() { "agent" } else { "owner" },
                }),
            },
        )
        .await?;
        upsert_agent_thread_subscription(
            pool,
            agent_id,
            channel_id,
            reply_thread_root_id,
            source_kind,
            Some(message_id),
        )
        .await?;
        let scheduled = ensure_agent_inbox_wake_work_item(pool, agent_id)
            .await?
            .is_some_and(|(_, scheduled)| scheduled);
        record_agent_activity(
            pool,
            Some(agent_id),
            None,
            match dispatch_kind {
                DispatchKind::ChannelMessage => "channel",
                DispatchKind::Dm => "dm",
                DispatchKind::Mention => "mention",
                DispatchKind::ThreadFollowUp => "thread",
            },
            match (dispatch_kind, scheduled, origin.is_agent()) {
                (DispatchKind::ChannelMessage, true, _) => "Channel message delivered to inbox",
                (DispatchKind::ChannelMessage, false, _) => "Channel message queued in inbox",
                (DispatchKind::Dm, true, _) => "DM delivered to inbox",
                (DispatchKind::Dm, false, _) => "DM queued in inbox",
                (DispatchKind::ThreadFollowUp, true, _) => "Thread follow-up delivered to inbox",
                (DispatchKind::ThreadFollowUp, false, _) => "Thread follow-up queued in inbox",
                (DispatchKind::Mention, true, true) => "Collaboration delivered to inbox",
                (DispatchKind::Mention, false, true) => "Collaboration queued in inbox",
                (DispatchKind::Mention, true, false) => "Mention delivered to inbox",
                (DispatchKind::Mention, false, false) => "Mention queued in inbox",
            },
            json!({
                "channel": format!("#{channel_name}"),
                "target_agent": format!("@{agent_handle}"),
                "inbox_item_id": inbox_item_id,
                "title": title,
            })
            .to_string(),
        )
        .await?;
    }

    Ok(())
}

async fn load_thread_followup_targets(
    pool: &SqlitePool,
    channel_id: Uuid,
    thread_root_id: Uuid,
) -> CommandResult<Vec<(Uuid, String)>> {
    let rows = sqlx::query(
        r#"
        select a.id, a.handle
        from (
            select agent_id, max(last_at) as last_at
            from (
                select sender_agent_id as agent_id, max(created_at) as last_at
                from messages
                where channel_id = $1
                  and (id = $2 or thread_root_id = $2)
                  and sender_agent_id is not null
                group by sender_agent_id
                union all
                select agent_id, max(created_at) as last_at
                from agent_work_items
                where channel_id = $1
                  and thread_root_id = $2
                group by agent_id
                union all
                select agent_id, max(updated_at) as last_at
                from agent_thread_subscriptions
                where channel_id = $1
                  and thread_root_id = $2
                group by agent_id
            ) candidates
            where agent_id is not null
            group by agent_id
        ) candidates
        join agents a on a.id = candidates.agent_id
        join channel_members cm on cm.channel_id = $1 and cm.agent_id = a.id
        order by candidates.last_at desc, lower(a.handle)
        limit 8
        "#,
    )
    .bind(channel_id)
    .bind(thread_root_id)
    .fetch_all(pool)
    .await
    .map_err(to_string)?;

    Ok(rows
        .into_iter()
        .map(|row| (row.get("id"), row.get("handle")))
        .collect())
}

async fn load_channel_root_delivery_targets(
    pool: &SqlitePool,
    channel_id: Uuid,
) -> CommandResult<Vec<(Uuid, String)>> {
    let rows = sqlx::query(
        r#"
        select a.id, a.handle
        from channel_members cm
        join agents a on a.id = cm.agent_id
        where cm.channel_id = $1
        order by lower(a.handle)
        "#,
    )
    .bind(channel_id)
    .fetch_all(pool)
    .await
    .map_err(to_string)?;
    Ok(rows
        .into_iter()
        .map(|row| (row.get("id"), row.get("handle")))
        .collect())
}

async fn inter_agent_thread_message_count_since_last_owner(
    pool: &SqlitePool,
    channel_id: Uuid,
    thread_root_id: Uuid,
) -> CommandResult<i64> {
    let last_owner_created_at: Option<DateTime<Utc>> = sqlx::query_scalar(
        r#"
        select max(created_at)
        from messages
        where channel_id = $1
          and (id = $2 or thread_root_id = $2)
          and sender_role = 'owner'
        "#,
    )
    .bind(channel_id)
    .bind(thread_root_id)
    .fetch_one(pool)
    .await
    .map_err(to_string)?;

    let count = if let Some(last_owner_created_at) = last_owner_created_at {
        sqlx::query_scalar(
            r#"
            select count(*)
            from messages
            where channel_id = $1
              and (id = $2 or thread_root_id = $2)
              and sender_agent_id is not null
              and created_at > $3
            "#,
        )
        .bind(channel_id)
        .bind(thread_root_id)
        .bind(last_owner_created_at)
        .fetch_one(pool)
        .await
        .map_err(to_string)?
    } else {
        sqlx::query_scalar(
            r#"
            select count(*)
            from messages
            where channel_id = $1
              and (id = $2 or thread_root_id = $2)
              and sender_agent_id is not null
            "#,
        )
        .bind(channel_id)
        .bind(thread_root_id)
        .fetch_one(pool)
        .await
        .map_err(to_string)?
    };
    Ok(count)
}

async fn queue_agent_message_mentions(pool: &SqlitePool, message_id: Uuid) -> CommandResult<()> {
    let Some(row) = sqlx::query(
        r#"
        select channel_id, thread_root_id, sender_agent_id, body, is_task
        from messages
        where id = $1
        "#,
    )
    .bind(message_id)
    .fetch_optional(pool)
    .await
    .map_err(to_string)?
    else {
        return Ok(());
    };

    let Some(sender_agent_id) = row.get::<Option<Uuid>, _>("sender_agent_id") else {
        return Ok(());
    };
    let is_task: bool = row.get("is_task");
    if is_task {
        return Ok(());
    }

    let channel_id: Uuid = row.get("channel_id");
    let thread_root_id: Option<Uuid> = row.get("thread_root_id");
    let body: String = row.get("body");
    queue_mentions_as_work_items(
        pool,
        channel_id,
        thread_root_id,
        message_id,
        None,
        body.trim(),
        MentionDispatchOrigin::Agent {
            sender_agent_id,
            allow_channel_member_invite: false,
        },
    )
    .await
}

#[tauri::command]
async fn stop_agent(run_id: Uuid, state: State<'_, AppState>) -> CommandResult<()> {
    let row = sqlx::query(
        r#"
        select agent_id, pid, work_item_id
        from agent_runs
        where id = $1 and stopped_at is null
        "#,
    )
    .bind(run_id)
    .fetch_one(&state.pool)
    .await
    .map_err(to_string)?;

    let agent_id: Uuid = row.get("agent_id");
    sqlx::query(
        r#"
        insert into supervisor_commands (command_type, agent_id, run_id)
        values ('stop_run', $1, $2)
        "#,
    )
    .bind(agent_id)
    .bind(run_id)
    .execute(&state.pool)
    .await
    .map_err(to_string)?;
    let _ = notify_supervisor_wake(&state.pool).await;
    let _ = notify_ui_refresh(&state.pool, "supervisor_command").await;
    sqlx::query("update agent_runs set status = 'stopping' where id = $1")
        .bind(run_id)
        .execute(&state.pool)
        .await
        .map_err(to_string)?;
    notify_ui_agent_run_changed(&state.pool, run_id, "run_stopping").await;
    sqlx::query("update agents set status = 'stopping' where id = $1")
        .bind(agent_id)
        .execute(&state.pool)
        .await
        .map_err(to_string)?;
    record_agent_activity(
        &state.pool,
        Some(agent_id),
        Some(run_id),
        "run",
        "Stop requested",
        "Stop command queued for supervisor",
    )
    .await?;

    Ok(())
}

#[tauri::command]
async fn install_supervisor_service(
    state: State<'_, AppState>,
) -> CommandResult<LaunchAgentStatus> {
    let status = launch_agent::install_supervisor_service(&state.db_url)?;
    let _ = notify_ui_refresh(&state.pool, "supervisor_service_installed").await;
    Ok(status)
}

#[tauri::command]
async fn uninstall_supervisor_service(
    state: State<'_, AppState>,
) -> CommandResult<LaunchAgentStatus> {
    let status = launch_agent::uninstall_supervisor_service()?;

    sqlx::query("update supervisor_state set status = 'offline', updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now') where id = 1")
        .execute(&state.pool)
        .await
        .map_err(to_string)?;

    let _ = notify_ui_refresh(&state.pool, "supervisor_service_uninstalled").await;
    Ok(status)
}

#[tauri::command]
async fn send_message(
    channel_id: Uuid,
    thread_root_id: Option<Uuid>,
    body: String,
    as_task: bool,
    attachments: Option<Vec<AttachmentUpload>>,
    state: State<'_, AppState>,
) -> CommandResult<()> {
    send_owner_message_in_pool(
        &state.pool,
        channel_id,
        thread_root_id,
        &body,
        as_task,
        attachments.unwrap_or_default(),
    )
    .await
}

pub(crate) async fn send_owner_message_in_pool(
    pool: &SqlitePool,
    channel_id: Uuid,
    thread_root_id: Option<Uuid>,
    body: &str,
    as_task: bool,
    attachments: Vec<AttachmentUpload>,
) -> CommandResult<()> {
    if body.trim().is_empty() && attachments.is_empty() {
        return Err("message body or attachment is required".to_owned());
    }
    let mut tx = pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .map_err(to_string)?;
    let channel_kind: Option<String> =
        sqlx::query_scalar("select kind from channels where id = $1")
            .bind(channel_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(to_string)?;
    let Some(channel_kind) = channel_kind else {
        return Err("channel does not exist".to_owned());
    };
    if as_task && channel_kind == "dm" {
        return Err("direct messages do not support tasks".to_owned());
    }

    let owner_display_name =
        sqlx::query_scalar::<_, String>("select display_name from owner_profile where id = 1")
            .fetch_optional(&mut *tx)
            .await
            .map_err(to_string)?
            .unwrap_or_else(|| DEFAULT_OWNER_DISPLAY_NAME.to_owned());

    let msg_id: Uuid = sqlx::query_scalar(
        r#"
        insert into messages (channel_id, thread_root_id, sender_name, sender_role, body, is_task)
        values ($1, $2, $3, 'owner', $4, $5)
        returning id
        "#,
    )
    .bind(channel_id)
    .bind(thread_root_id)
    .bind(owner_display_name)
    .bind(body.trim())
    .bind(as_task)
    .fetch_one(&mut *tx)
    .await
    .map_err(to_string)?;

    insert_message_attachments_tx(&mut tx, msg_id, attachments).await?;

    let mut task_id = None;
    if as_task {
        task_id = Some(
            sqlx::query_scalar(
                r#"
            insert into tasks (message_id, channel_id, title, status)
            values ($1, $2, $3, 'todo')
            returning id
            "#,
            )
            .bind(msg_id)
            .bind(channel_id)
            .bind(body.lines().next().unwrap_or("Untitled task"))
            .fetch_one(&mut *tx)
            .await
            .map_err(to_string)?,
        );
    }

    tx.commit().await.map_err(to_string)?;
    queue_mentions_as_work_items(
        pool,
        channel_id,
        thread_root_id,
        msg_id,
        task_id,
        body.trim(),
        MentionDispatchOrigin::Owner,
    )
    .await?;
    if let Some(task_id) = task_id {
        dispatch_unassigned_task_availability(pool, task_id).await?;
    }
    let _ = notify_ui_refresh(pool, "message").await;
    Ok(())
}

async fn insert_message_attachments_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    message_id: Uuid,
    attachments: Vec<AttachmentUpload>,
) -> CommandResult<usize> {
    let mut inserted = 0;
    for attachment in attachments {
        if attachment.bytes.is_empty() {
            continue;
        }
        if attachment.bytes.len() > ATTACHMENT_SIZE_LIMIT {
            return Err(format!(
                "attachment {} is larger than 25MB",
                attachment.original_name
            ));
        }
        let attachment_id = Uuid::new_v4();
        let original_name = attachment.original_name.trim();
        let original_name = if original_name.is_empty() {
            "attachment"
        } else {
            original_name
        };
        let mime_type = attachment.mime_type.trim();
        let mime_type = if mime_type.is_empty() {
            "application/octet-stream"
        } else {
            mime_type
        };
        let storage_path =
            write_attachment_file(message_id, attachment_id, original_name, &attachment.bytes)?;
        sqlx::query(
            r#"
            insert into message_attachments (
                id,
                message_id,
                original_name,
                mime_type,
                size_bytes,
                storage_path
            )
            values ($1, $2, $3, $4, $5, $6)
            "#,
        )
        .bind(attachment_id)
        .bind(message_id)
        .bind(original_name)
        .bind(mime_type)
        .bind(attachment.bytes.len() as i64)
        .bind(storage_path)
        .execute(&mut **tx)
        .await
        .map_err(to_string)?;
        inserted += 1;
    }
    Ok(inserted)
}

#[tauri::command]
async fn update_message(
    message_id: Uuid,
    body: String,
    state: State<'_, AppState>,
) -> CommandResult<()> {
    let body = body.trim();
    if body.is_empty() {
        return Err("message body is empty".to_owned());
    }

    let mut tx = state
        .pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .map_err(to_string)?;
    let result = sqlx::query("update messages set body = $2, updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now') where id = $1")
        .bind(message_id)
        .bind(body)
        .execute(&mut *tx)
        .await
        .map_err(to_string)?;
    if result.rows_affected() == 0 {
        return Err("message does not exist".to_owned());
    }

    sqlx::query(
        r#"
        update tasks
        set title = $2, updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
        where message_id = $1
        "#,
    )
    .bind(message_id)
    .bind(body.lines().next().unwrap_or("Untitled task"))
    .execute(&mut *tx)
    .await
    .map_err(to_string)?;

    tx.commit().await.map_err(to_string)?;
    Ok(())
}

#[tauri::command]
async fn delete_message(message_id: Uuid, state: State<'_, AppState>) -> CommandResult<()> {
    let result = sqlx::query("delete from messages where id = $1")
        .bind(message_id)
        .execute(&state.pool)
        .await
        .map_err(to_string)?;
    if result.rows_affected() == 0 {
        return Err("message does not exist".to_owned());
    }
    Ok(())
}

#[tauri::command]
async fn set_message_saved(
    message_id: Uuid,
    saved: bool,
    state: State<'_, AppState>,
) -> CommandResult<()> {
    set_message_saved_in_pool(&state.pool, message_id, saved).await
}

pub(crate) async fn set_message_saved_in_pool(
    pool: &SqlitePool,
    message_id: Uuid,
    saved: bool,
) -> CommandResult<()> {
    let exists: bool = sqlx::query_scalar("select exists(select 1 from messages where id = $1)")
        .bind(message_id)
        .fetch_one(pool)
        .await
        .map_err(to_string)?;
    if !exists {
        return Err("message does not exist".to_owned());
    }

    if saved {
        sqlx::query(
            r#"
            insert into saved_messages (message_id)
            values ($1)
            on conflict (message_id) do nothing
            "#,
        )
        .bind(message_id)
        .execute(pool)
        .await
        .map_err(to_string)?;
    } else {
        sqlx::query("delete from saved_messages where message_id = $1")
            .bind(message_id)
            .execute(pool)
            .await
            .map_err(to_string)?;
    }
    let _ = notify_ui_refresh(pool, "saved_message_updated").await;
    Ok(())
}

#[tauri::command]
async fn claim_task(
    task_id: Uuid,
    agent_id: Option<Uuid>,
    expected_version: Option<i64>,
    state: State<'_, AppState>,
) -> CommandResult<()> {
    claim_task_in_pool(&state.pool, task_id, agent_id, expected_version).await
}

pub(crate) async fn claim_task_in_pool(
    pool: &SqlitePool,
    task_id: Uuid,
    agent_id: Option<Uuid>,
    expected_version: Option<i64>,
) -> CommandResult<()> {
    let current_status: Option<String> =
        sqlx::query_scalar("select status from tasks where id = $1")
            .bind(task_id)
            .fetch_optional(pool)
            .await
            .map_err(to_string)?;
    let current_status = current_status.ok_or_else(|| "task does not exist".to_owned())?;
    if current_status == "done" {
        return Err("done tasks cannot be reassigned".to_owned());
    }

    if let (Some(agent_id), Some(expected_version)) = (agent_id, expected_version) {
        return try_claim_unassigned_task(
            pool,
            task_id,
            agent_id,
            Some(expected_version),
            "manual_claim",
        )
        .await?
        .map(|_| ())
        .ok_or_else(|| "task was already claimed or is no longer available".to_owned());
    }

    sqlx::query_scalar::<_, Uuid>(
        r#"
        update tasks
        set assignee_agent_id = $2,
            status = case when $2 is null then status else 'in_progress' end,
            version = version + 1,
            updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
        where id = $1
        returning id
        "#,
    )
    .bind(task_id)
    .bind(agent_id)
    .fetch_optional(pool)
    .await
    .map_err(to_string)?
    .ok_or_else(|| "task does not exist".to_owned())?;

    if let Some(agent_id) = agent_id {
        dispatch_task_assignment_to_agent(pool, task_id, agent_id, "manual_claim").await?;
    } else {
        record_agent_activity(
            pool,
            None,
            None,
            "task",
            "Task unassigned",
            json!({ "task_id": task_id }).to_string(),
        )
        .await?;
    }

    let _ = notify_ui_refresh(pool, "task_claimed").await;
    Ok(())
}

#[tauri::command]
async fn update_task_status(
    task_id: Uuid,
    status: String,
    state: State<'_, AppState>,
) -> CommandResult<()> {
    update_task_status_in_pool(&state.pool, task_id, status).await
}

pub(crate) async fn update_task_status_in_pool(
    pool: &SqlitePool,
    task_id: Uuid,
    status: String,
) -> CommandResult<()> {
    let status = status.trim();
    if !matches!(status, "todo" | "in_progress" | "in_review" | "done") {
        return Err(format!("unsupported task status: {status}"));
    }

    let affected = sqlx::query(
        r#"
        update tasks
        set status = $2, version = version + 1, updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
        where id = $1
        "#,
    )
    .bind(task_id)
    .bind(status)
    .execute(pool)
    .await
    .map_err(to_string)?
    .rows_affected();
    if affected == 0 {
        return Err("task does not exist".to_owned());
    }
    record_agent_activity(
        pool,
        None,
        None,
        "task",
        "Task status updated",
        json!({ "task_id": task_id, "status": status }).to_string(),
    )
    .await?;

    let _ = notify_ui_refresh(pool, "task_status_updated").await;
    Ok(())
}

#[tauri::command]
async fn update_task_title(
    task_id: Uuid,
    title: String,
    state: State<'_, AppState>,
) -> CommandResult<()> {
    update_task_title_in_pool(&state.pool, task_id, title).await
}

pub(crate) async fn update_task_title_in_pool(
    pool: &SqlitePool,
    task_id: Uuid,
    title: String,
) -> CommandResult<()> {
    let title = title.trim();
    if title.is_empty() {
        return Err("task title is empty".to_owned());
    }

    let mut tx = pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .map_err(to_string)?;
    let message_id: Uuid = sqlx::query_scalar(
        r#"
        update tasks
        set title = $2, version = version + 1, updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
        where id = $1
        returning message_id
        "#,
    )
    .bind(task_id)
    .bind(title)
    .fetch_one(&mut *tx)
    .await
    .map_err(to_string)?;

    sqlx::query("update messages set body = $2, updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now') where id = $1")
        .bind(message_id)
        .bind(title)
        .execute(&mut *tx)
        .await
        .map_err(to_string)?;

    tx.commit().await.map_err(to_string)?;
    record_agent_activity(
        pool,
        None,
        None,
        "task",
        "Task title updated",
        json!({ "task_id": task_id, "title": title }).to_string(),
    )
    .await?;
    let _ = notify_ui_refresh(pool, "task_title_updated").await;
    Ok(())
}

fn parse_due_at(value: &str) -> CommandResult<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|err| format!("invalid reminder due_at: {err}"))
}

fn normalize_recurrence(value: &str) -> CommandResult<String> {
    let recurrence = value.trim();
    if matches!(recurrence, "none" | "daily" | "weekly") {
        Ok(recurrence.to_owned())
    } else if matches!(recurrence, "one_shot" | "one-shot" | "once") {
        Ok("none".to_owned())
    } else {
        Err(format!("unsupported reminder recurrence: {recurrence}"))
    }
}

fn normalize_schedule_cadence(value: &str) -> CommandResult<String> {
    let cadence = value.trim();
    if matches!(cadence, "hourly" | "daily" | "weekly") {
        Ok(cadence.to_owned())
    } else {
        Err(format!("unsupported schedule cadence: {cadence}"))
    }
}

async fn insert_reminder_event(
    pool: &SqlitePool,
    reminder_id: Uuid,
    event_type: &str,
    detail: impl AsRef<str>,
) -> CommandResult<()> {
    sqlx::query(
        r#"
        insert into reminder_events (reminder_id, event_type, detail)
        values ($1, $2, $3)
        "#,
    )
    .bind(reminder_id)
    .bind(event_type)
    .bind(detail.as_ref())
    .execute(pool)
    .await
    .map_err(to_string)?;
    Ok(())
}

async fn create_reminder_in_pool(
    pool: &SqlitePool,
    creator_agent_id: Option<Uuid>,
    channel_id: Option<Uuid>,
    thread_root_id: Option<Uuid>,
    message_id: Option<Uuid>,
    title: &str,
    note: &str,
    due_at: DateTime<Utc>,
    recurrence: &str,
) -> CommandResult<Uuid> {
    let title = title.trim();
    if title.is_empty() {
        return Err("reminder title is empty".to_owned());
    }
    let recurrence = normalize_recurrence(recurrence)?;

    if let Some(thread_root_id) = thread_root_id {
        let exists: bool = sqlx::query_scalar(
            "select exists(select 1 from messages where id = $1 and thread_root_id is null)",
        )
        .bind(thread_root_id)
        .fetch_one(pool)
        .await
        .map_err(to_string)?;
        if !exists {
            return Err("thread root does not exist".to_owned());
        }
    }

    let reminder_id: Uuid = sqlx::query_scalar(
        r#"
        insert into reminders (
            channel_id, creator_agent_id, thread_root_id, message_id, title, note, due_at, recurrence, status
        )
        values ($1, $2, $3, $4, $5, $6, $7, $8, 'scheduled')
        returning id
        "#,
    )
    .bind(channel_id)
    .bind(creator_agent_id)
    .bind(thread_root_id)
    .bind(message_id)
    .bind(title)
    .bind(note.trim())
    .bind(due_at)
    .bind(&recurrence)
    .fetch_one(pool)
    .await
    .map_err(to_string)?;
    insert_reminder_event(pool, reminder_id, "created", due_at.to_rfc3339()).await?;
    let _ = notify_ui_refresh(pool, "reminder_created").await;
    Ok(reminder_id)
}

async fn cancel_reminder_in_pool(pool: &SqlitePool, reminder_id: Uuid) -> CommandResult<()> {
    let affected = sqlx::query(
        r#"
        update reminders
        set status = 'cancelled',
            completed_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now'),
            updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
        where id = $1 and status in ('scheduled', 'fired')
        "#,
    )
    .bind(reminder_id)
    .execute(pool)
    .await
    .map_err(to_string)?
    .rows_affected();
    if affected == 0 {
        return Err("reminder does not exist or is not active".to_owned());
    }
    insert_reminder_event(pool, reminder_id, "cancelled", "").await?;
    let _ = notify_ui_refresh(pool, "reminder_cancelled").await;
    Ok(())
}

#[tauri::command]
async fn create_reminder(
    channel_id: Option<Uuid>,
    thread_root_id: Option<Uuid>,
    message_id: Option<Uuid>,
    title: String,
    note: String,
    due_at: String,
    recurrence: String,
    state: State<'_, AppState>,
) -> CommandResult<Uuid> {
    let due_at = parse_due_at(&due_at)?;
    create_reminder_in_pool(
        &state.pool,
        None,
        channel_id,
        thread_root_id,
        message_id,
        &title,
        &note,
        due_at,
        &recurrence,
    )
    .await
}

#[tauri::command]
async fn snooze_reminder(
    reminder_id: Uuid,
    minutes: i64,
    state: State<'_, AppState>,
) -> CommandResult<()> {
    if !(1..=10_080).contains(&minutes) {
        return Err("snooze minutes must be between 1 and 10080".to_owned());
    }
    sqlx::query(
        r#"
        update reminders
        set status = 'scheduled',
            due_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now','+' || $2 || ' minutes'),
            updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
        where id = $1 and status in ('scheduled', 'fired')
        "#,
    )
    .bind(reminder_id)
    .bind(minutes)
    .execute(&state.pool)
    .await
    .map_err(to_string)?;
    insert_reminder_event(
        &state.pool,
        reminder_id,
        "snoozed",
        format!("{minutes} minutes"),
    )
    .await?;
    let _ = notify_ui_refresh(&state.pool, "reminder_snoozed").await;
    Ok(())
}

#[tauri::command]
async fn complete_reminder(reminder_id: Uuid, state: State<'_, AppState>) -> CommandResult<()> {
    complete_reminder_in_pool(&state.pool, reminder_id).await
}

pub(crate) async fn complete_reminder_in_pool(
    pool: &SqlitePool,
    reminder_id: Uuid,
) -> CommandResult<()> {
    sqlx::query(
        r#"
        update reminders
        set status = 'done',
            completed_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now'),
            updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
        where id = $1 and status in ('scheduled', 'fired')
        "#,
    )
    .bind(reminder_id)
    .execute(pool)
    .await
    .map_err(to_string)?;
    insert_reminder_event(pool, reminder_id, "completed", "").await?;
    let _ = notify_ui_refresh(pool, "reminder_completed").await;
    Ok(())
}

#[tauri::command]
async fn cancel_reminder(reminder_id: Uuid, state: State<'_, AppState>) -> CommandResult<()> {
    cancel_reminder_in_pool(&state.pool, reminder_id).await
}

#[tauri::command]
async fn create_agent_schedule(
    agent_id: Uuid,
    channel_id: Uuid,
    thread_root_id: Option<Uuid>,
    title: String,
    prompt: String,
    cadence: String,
    next_run_at: String,
    state: State<'_, AppState>,
) -> CommandResult<Uuid> {
    let title = title.trim();
    let prompt = prompt.trim();
    if title.is_empty() {
        return Err("schedule title is empty".to_owned());
    }
    if prompt.is_empty() {
        return Err("schedule prompt is empty".to_owned());
    }
    let next_run_at = parse_due_at(&next_run_at)?;
    let cadence = normalize_schedule_cadence(&cadence)?;

    let agent_exists: bool =
        sqlx::query_scalar("select exists(select 1 from agents where id = $1)")
            .bind(agent_id)
            .fetch_one(&state.pool)
            .await
            .map_err(to_string)?;
    if !agent_exists {
        return Err("agent does not exist".to_owned());
    }

    let channel_row = sqlx::query("select kind, dm_agent_id from channels where id = $1")
        .bind(channel_id)
        .fetch_optional(&state.pool)
        .await
        .map_err(to_string)?;
    let Some(channel_row) = channel_row else {
        return Err("channel does not exist".to_owned());
    };
    let channel_kind: String = channel_row.get("kind");
    let dm_agent_id: Option<Uuid> = channel_row.get("dm_agent_id");
    if channel_kind == "dm" && dm_agent_id != Some(agent_id) {
        return Err("direct message schedules must target their DM agent".to_owned());
    }

    if let Some(thread_root_id) = thread_root_id {
        let root_channel: Option<Uuid> = sqlx::query_scalar(
            "select channel_id from messages where id = $1 and thread_root_id is null",
        )
        .bind(thread_root_id)
        .fetch_optional(&state.pool)
        .await
        .map_err(to_string)?;
        if root_channel != Some(channel_id) {
            return Err("thread root does not belong to target channel".to_owned());
        }
    }

    if channel_kind != "dm" {
        sqlx::query(
            r#"
            insert into channel_members (channel_id, agent_id)
            values ($1, $2)
            on conflict (channel_id, agent_id) do nothing
            "#,
        )
        .bind(channel_id)
        .bind(agent_id)
        .execute(&state.pool)
        .await
        .map_err(to_string)?;
    }

    let schedule_id: Uuid = sqlx::query_scalar(
        r#"
        insert into agent_schedules (
            agent_id, channel_id, thread_root_id, title, prompt, cadence, status, next_run_at
        )
        values ($1, $2, $3, $4, $5, $6, 'active', $7)
        returning id
        "#,
    )
    .bind(agent_id)
    .bind(channel_id)
    .bind(thread_root_id)
    .bind(title)
    .bind(prompt)
    .bind(&cadence)
    .bind(next_run_at)
    .fetch_one(&state.pool)
    .await
    .map_err(to_string)?;

    record_agent_activity(
        &state.pool,
        Some(agent_id),
        None,
        "schedule",
        "Scheduled routine created",
        json!({
            "schedule_id": schedule_id,
            "cadence": cadence,
            "next_run_at": next_run_at.to_rfc3339()
        })
        .to_string(),
    )
    .await?;
    let _ = notify_ui_refresh(&state.pool, "agent_schedule_created").await;
    Ok(schedule_id)
}

#[tauri::command]
async fn update_agent_schedule_status(
    schedule_id: Uuid,
    status: String,
    state: State<'_, AppState>,
) -> CommandResult<()> {
    let status = status.trim();
    if !matches!(status, "active" | "paused" | "cancelled") {
        return Err(format!("unsupported schedule status: {status}"));
    }
    let row = sqlx::query(
        r#"
        update agent_schedules
        set status = $2,
            updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
        where id = $1 and status <> 'cancelled'
        returning agent_id
        "#,
    )
    .bind(schedule_id)
    .bind(status)
    .fetch_optional(&state.pool)
    .await
    .map_err(to_string)?;
    let Some(row) = row else {
        return Err("schedule does not exist or is already cancelled".to_owned());
    };
    let agent_id: Uuid = row.get("agent_id");
    record_agent_activity(
        &state.pool,
        Some(agent_id),
        None,
        "schedule",
        match status {
            "active" => "Scheduled routine resumed",
            "paused" => "Scheduled routine paused",
            "cancelled" => "Scheduled routine cancelled",
            _ => "Scheduled routine updated",
        },
        schedule_id.to_string(),
    )
    .await?;
    let _ = notify_ui_refresh(&state.pool, "agent_schedule_updated").await;
    Ok(())
}

#[tauri::command]
async fn mark_channel_read(channel_id: Uuid, state: State<'_, AppState>) -> CommandResult<()> {
    mark_channel_read_in_pool(&state.pool, channel_id).await
}

#[tauri::command]
async fn dismiss_inbox_items(
    items: Vec<DismissInboxItemInput>,
    state: State<'_, AppState>,
) -> CommandResult<()> {
    dismiss_inbox_items_in_pool(
        &state.pool,
        items
            .into_iter()
            .map(|item| (item.item_id, item.dismissed_until)),
    )
    .await
}

#[tauri::command]
async fn mark_inbox_items_read(
    items: Vec<DismissInboxItemInput>,
    state: State<'_, AppState>,
) -> CommandResult<()> {
    mark_inbox_items_read_in_pool(
        &state.pool,
        items
            .into_iter()
            .map(|item| (item.item_id, item.dismissed_until)),
    )
    .await
}

pub(crate) async fn dismiss_inbox_items_in_pool<I>(pool: &SqlitePool, items: I) -> CommandResult<()>
where
    I: IntoIterator<Item = (String, DateTime<Utc>)>,
{
    let mut updated = false;
    for item in items {
        let item_id = item.0.trim();
        if item_id.is_empty() {
            continue;
        }
        sqlx::query(
            r#"
            insert into owner_inbox_hidden_items (item_id, hidden_until, hidden_at)
            values ($1, $2, strftime('%Y-%m-%dT%H:%M:%f+00:00','now'))
            on conflict (item_id) do update set
                hidden_until = max(
                    owner_inbox_hidden_items.hidden_until,
                    excluded.hidden_until
                ),
                hidden_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
            "#,
        )
        .bind(item_id)
        .bind(item.1)
        .execute(pool)
        .await
        .map_err(to_string)?;
        updated = true;
    }

    if updated {
        notify_ui_refresh(pool, "owner_inbox_dismissed").await?;
    }
    Ok(())
}

pub(crate) async fn mark_inbox_items_read_in_pool<I>(
    pool: &SqlitePool,
    items: I,
) -> CommandResult<()>
where
    I: IntoIterator<Item = (String, DateTime<Utc>)>,
{
    let mut updated = false;
    for item in items {
        let item_id = item.0.trim();
        if item_id.is_empty() {
            continue;
        }
        sqlx::query(
            r#"
            insert into owner_inbox_read_state (item_id, read_until, read_at)
            values ($1, $2, strftime('%Y-%m-%dT%H:%M:%f+00:00','now'))
            on conflict (item_id) do update set
                read_until = max(
                    owner_inbox_read_state.read_until,
                    excluded.read_until
                ),
                read_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
            "#,
        )
        .bind(item_id)
        .bind(item.1)
        .execute(pool)
        .await
        .map_err(to_string)?;
        updated = true;
    }

    if updated {
        notify_ui_refresh(pool, "owner_inbox_read").await?;
    }
    Ok(())
}

pub(crate) async fn mark_all_owner_inbox_read_in_pool(pool: &SqlitePool) -> CommandResult<()> {
    sqlx::query(
        r#"
        insert into owner_inbox_read_state (item_id, read_until, read_at)
        select 'task:' || lower(
            substr(hex(id), 1, 8) || '-' ||
            substr(hex(id), 9, 4) || '-' ||
            substr(hex(id), 13, 4) || '-' ||
            substr(hex(id), 17, 4) || '-' ||
            substr(hex(id), 21, 12)
        ), strftime('%Y-%m-%dT%H:%M:%f+00:00','now'), strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
        from tasks
        where status = 'in_review'
        on conflict (item_id) do update set
            read_until = max(owner_inbox_read_state.read_until, excluded.read_until),
            read_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
        "#,
    )
    .execute(pool)
    .await
    .map_err(to_string)?;

    sqlx::query(
        r#"
        insert into owner_inbox_read_state (item_id, read_until, read_at)
        select 'reminder:' || lower(
            substr(hex(id), 1, 8) || '-' ||
            substr(hex(id), 9, 4) || '-' ||
            substr(hex(id), 13, 4) || '-' ||
            substr(hex(id), 17, 4) || '-' ||
            substr(hex(id), 21, 12)
        ), strftime('%Y-%m-%dT%H:%M:%f+00:00','now'), strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
        from reminders
        where status = 'fired'
        on conflict (item_id) do update set
            read_until = max(owner_inbox_read_state.read_until, excluded.read_until),
            read_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
        "#,
    )
    .execute(pool)
    .await
    .map_err(to_string)?;

    sqlx::query(
        r#"
        insert into channel_read_state (channel_id, last_read_at)
        select id, strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
        from channels
        where true
        on conflict (channel_id) do update set last_read_at = excluded.last_read_at
        "#,
    )
    .execute(pool)
    .await
    .map_err(to_string)?;

    notify_ui_refresh(pool, "owner_inbox_mark_all_read").await?;
    Ok(())
}

#[tauri::command]
async fn mark_all_inbox_read(state: State<'_, AppState>) -> CommandResult<()> {
    mark_all_owner_inbox_read_in_pool(&state.pool).await
}

pub(crate) async fn mark_channel_read_in_pool(
    pool: &SqlitePool,
    channel_id: Uuid,
) -> CommandResult<()> {
    sqlx::query(
        r#"
        insert into channel_read_state (channel_id, last_read_at)
        values ($1, strftime('%Y-%m-%dT%H:%M:%f+00:00','now'))
        on conflict (channel_id) do update set last_read_at = excluded.last_read_at
        "#,
    )
    .bind(channel_id)
    .execute(pool)
    .await
    .map_err(to_string)?;

    let _ = notify_ui_refresh(pool, "channel_read").await;
    Ok(())
}

#[tauri::command]
async fn update_thread_followed(
    thread_root_id: Uuid,
    followed: bool,
    state: State<'_, AppState>,
) -> CommandResult<()> {
    sqlx::query(
        r#"
        update messages
        set thread_followed = $2
        where id = $1 and thread_root_id is null
        "#,
    )
    .bind(thread_root_id)
    .bind(followed)
    .execute(&state.pool)
    .await
    .map_err(to_string)?;

    Ok(())
}

async fn load_owner_profile(pool: &SqlitePool) -> CommandResult<OwnerProfile> {
    let row = sqlx::query(
        r#"
        select display_name, avatar, description
        from owner_profile
        where id = 1
        "#,
    )
    .fetch_optional(pool)
    .await
    .map_err(to_string)?;

    Ok(row
        .map(|row| OwnerProfile {
            display_name: row.get("display_name"),
            avatar: row.get("avatar"),
            description: row.get("description"),
        })
        .unwrap_or_else(|| OwnerProfile {
            display_name: DEFAULT_OWNER_DISPLAY_NAME.to_owned(),
            avatar: DEFAULT_OWNER_AVATAR.to_owned(),
            description: DEFAULT_OWNER_DESCRIPTION.to_owned(),
        }))
}

async fn load_channels(pool: &SqlitePool) -> CommandResult<Vec<Channel>> {
    let rows = sqlx::query(
        r#"
        select
            c.id,
            c.name,
            c.description,
            c.kind,
            c.dm_agent_id,
            cast(count(m.id) filter (
                where m.created_at > coalesce(r.last_read_at, '-infinity')
                  and m.delivery_state <> 'streaming'
            ) as integer) as unread_count
        from channels c
        left join channel_read_state r on r.channel_id = c.id
        left join messages m on m.channel_id = c.id
        group by c.id, c.name, c.description, c.kind, c.dm_agent_id
        order by
          case
            when c.kind = 'channel' and c.name = 'lantor' then 0
            when c.kind = 'channel' then 1
            else 2
          end,
          c.name
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(to_string)?;

    Ok(rows
        .into_iter()
        .map(|row| Channel {
            id: row.get("id"),
            name: row.get("name"),
            description: row.get("description"),
            kind: row.get("kind"),
            dm_agent_id: row.get("dm_agent_id"),
            unread_count: row.get("unread_count"),
        })
        .collect())
}

async fn load_channel_members(pool: &SqlitePool) -> CommandResult<Vec<ChannelMember>> {
    let rows = sqlx::query(
        r#"
        select
            m.channel_id,
            m.agent_id,
            a.handle as agent_handle,
            a.display_name as agent_display_name,
            m.created_at
        from channel_members m
        join agents a on a.id = m.agent_id
        join channels c on c.id = m.channel_id
        order by c.name, a.handle
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(to_string)?;

    Ok(rows
        .into_iter()
        .map(|row| ChannelMember {
            channel_id: row.get("channel_id"),
            agent_id: row.get("agent_id"),
            agent_handle: row.get("agent_handle"),
            agent_display_name: row.get("agent_display_name"),
            created_at: row.get("created_at"),
        })
        .collect())
}

async fn load_agents(pool: &SqlitePool) -> CommandResult<Vec<Agent>> {
    let rows = sqlx::query(
        r#"
        select
            id,
            handle,
            display_name,
            role,
            status,
            runtime,
            model,
            reasoning_effort,
            service_tier,
            avatar,
            description,
            launch_command,
            working_directory,
            daily_budget_micros
        from agents
        order by case when handle = 'Hancock' then 0 else 1 end, display_name
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(to_string)?;

    Ok(rows
        .into_iter()
        .map(|row| {
            let working_directory: String = row.get("working_directory");
            let workspace = load_agent_workspace_summary(&working_directory);
            Agent {
                id: row.get("id"),
                handle: row.get("handle"),
                display_name: row.get("display_name"),
                role: row.get("role"),
                status: row.get("status"),
                runtime: row.get("runtime"),
                model: row.get("model"),
                reasoning_effort: row.get("reasoning_effort"),
                service_tier: row.get("service_tier"),
                avatar: row.get("avatar"),
                description: row.get("description"),
                launch_command: row.get("launch_command"),
                working_directory,
                workspace_exists: workspace.exists,
                workspace_memory_path: workspace.memory_path,
                workspace_memory_exists: workspace.memory_exists,
                workspace_entries: workspace.entries,
                daily_budget_micros: row.get("daily_budget_micros"),
            }
        })
        .collect())
}

struct AgentWorkspaceSummary {
    exists: bool,
    memory_path: String,
    memory_exists: bool,
    entries: Vec<AgentWorkspaceEntry>,
}

fn load_agent_workspace_summary(working_directory: &str) -> AgentWorkspaceSummary {
    let working_directory = working_directory.trim();
    if working_directory.is_empty() {
        return AgentWorkspaceSummary {
            exists: false,
            memory_path: String::new(),
            memory_exists: false,
            entries: Vec::new(),
        };
    }

    let workspace = PathBuf::from(working_directory);
    let memory_path = workspace.join("MEMORY.md");
    let memory_path_string = memory_path.to_string_lossy().to_string();
    let exists = workspace.is_dir();
    let memory_exists = memory_path.is_file();
    let mut entries = Vec::new();

    if exists {
        if let Ok(read_dir) = fs::read_dir(&workspace) {
            for entry in read_dir.flatten() {
                let file_name = entry.file_name().to_string_lossy().to_string();
                if should_hide_workspace_entry(&file_name) {
                    continue;
                }
                if let Ok(metadata) = entry.metadata() {
                    let kind = if metadata.is_dir() {
                        "dir"
                    } else if metadata.is_file() {
                        "file"
                    } else {
                        "other"
                    };
                    let path = entry.path();
                    entries.push(workspace_entry_from_path(
                        &workspace, &path, file_name, kind, &metadata,
                    ));
                }
            }
        }
    }

    entries.sort_by(
        |left, right| match (left.kind.as_str(), right.kind.as_str()) {
            ("dir", "file") | ("dir", "other") | ("file", "other") => std::cmp::Ordering::Less,
            ("file", "dir") | ("other", "dir") | ("other", "file") => std::cmp::Ordering::Greater,
            _ => left.name.to_lowercase().cmp(&right.name.to_lowercase()),
        },
    );
    entries.truncate(48);

    AgentWorkspaceSummary {
        exists,
        memory_path: memory_path_string,
        memory_exists,
        entries,
    }
}

fn should_hide_workspace_entry(name: &str) -> bool {
    matches!(
        name,
        ".git" | "node_modules" | "target" | "dist" | ".next" | ".turbo"
    )
}

fn workspace_entry_from_path(
    workspace: &Path,
    path: &Path,
    name: String,
    kind: &str,
    metadata: &fs::Metadata,
) -> AgentWorkspaceEntry {
    AgentWorkspaceEntry {
        name,
        path: path.to_string_lossy().to_string(),
        relative_path: path
            .strip_prefix(workspace)
            .ok()
            .map(path_to_slash_string)
            .unwrap_or_default(),
        kind: kind.to_owned(),
        size_bytes: metadata.is_file().then_some(metadata.len() as i64),
    }
}

fn path_to_slash_string(path: &Path) -> String {
    path.components()
        .filter_map(|component| match component {
            Component::Normal(value) => Some(value.to_string_lossy().to_string()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("/")
}

async fn agent_workspace_root(pool: &SqlitePool, agent_id: Uuid) -> CommandResult<PathBuf> {
    let row = sqlx::query("select working_directory from agents where id = $1")
        .bind(agent_id)
        .fetch_optional(pool)
        .await
        .map_err(to_string)?
        .ok_or_else(|| "agent not found".to_owned())?;
    let working_directory: String = row.get("working_directory");
    let working_directory = working_directory.trim();
    if working_directory.is_empty() {
        return Err("agent working directory is not configured".to_owned());
    }
    let workspace = PathBuf::from(working_directory);
    let canonical = workspace
        .canonicalize()
        .map_err(|err| format!("workspace is not available: {err}"))?;
    if !canonical.is_dir() {
        return Err("agent workspace is not a directory".to_owned());
    }
    Ok(canonical)
}

fn safe_workspace_path(workspace: &Path, relative_path: &str) -> CommandResult<PathBuf> {
    let relative_path = relative_path.trim();
    let relative = Path::new(relative_path);
    if relative.is_absolute() {
        return Err("workspace path must be relative".to_owned());
    }

    let mut clean = PathBuf::new();
    for component in relative.components() {
        match component {
            Component::Normal(value) => clean.push(value),
            Component::CurDir => {}
            _ => return Err("workspace path cannot escape the workspace".to_owned()),
        }
    }

    let target = workspace.join(clean);
    let canonical = target
        .canonicalize()
        .map_err(|err| format!("workspace path is not available: {err}"))?;
    if !canonical.starts_with(workspace) {
        return Err("workspace path cannot escape the workspace".to_owned());
    }
    Ok(canonical)
}

fn list_workspace_entries(
    workspace: &Path,
    directory: &Path,
) -> CommandResult<Vec<AgentWorkspaceEntry>> {
    let mut entries = Vec::new();
    let read_dir = fs::read_dir(directory).map_err(to_string)?;
    for entry in read_dir.flatten() {
        let file_name = entry.file_name().to_string_lossy().to_string();
        if should_hide_workspace_entry(&file_name) {
            continue;
        }
        let Ok(metadata) = entry.metadata() else {
            continue;
        };
        let kind = if metadata.is_dir() {
            "dir"
        } else if metadata.is_file() {
            "file"
        } else {
            "other"
        };
        entries.push(workspace_entry_from_path(
            workspace,
            &entry.path(),
            file_name,
            kind,
            &metadata,
        ));
    }
    entries.sort_by(
        |left, right| match (left.kind.as_str(), right.kind.as_str()) {
            ("dir", "file") | ("dir", "other") | ("file", "other") => std::cmp::Ordering::Less,
            ("file", "dir") | ("other", "dir") | ("other", "file") => std::cmp::Ordering::Greater,
            _ => left.name.to_lowercase().cmp(&right.name.to_lowercase()),
        },
    );
    entries.truncate(200);
    Ok(entries)
}

#[tauri::command]
async fn agent_workspace_list(
    agent_id: Uuid,
    path: String,
    state: State<'_, AppState>,
) -> CommandResult<AgentWorkspaceListing> {
    agent_workspace_list_in_pool(&state.pool, agent_id, &path).await
}

pub(crate) async fn agent_workspace_list_in_pool(
    pool: &SqlitePool,
    agent_id: Uuid,
    path: &str,
) -> CommandResult<AgentWorkspaceListing> {
    let workspace = agent_workspace_root(pool, agent_id).await?;
    let directory = safe_workspace_path(&workspace, path)?;
    if !directory.is_dir() {
        return Err("workspace path is not a directory".to_owned());
    }
    Ok(AgentWorkspaceListing {
        path: path_to_slash_string(
            directory
                .strip_prefix(&workspace)
                .unwrap_or_else(|_| Path::new("")),
        ),
        entries: list_workspace_entries(&workspace, &directory)?,
    })
}

#[tauri::command]
async fn agent_workspace_read_file(
    agent_id: Uuid,
    path: String,
    state: State<'_, AppState>,
) -> CommandResult<AgentWorkspaceFile> {
    agent_workspace_read_file_in_pool(&state.pool, agent_id, &path).await
}

pub(crate) async fn agent_workspace_read_file_in_pool(
    pool: &SqlitePool,
    agent_id: Uuid,
    path: &str,
) -> CommandResult<AgentWorkspaceFile> {
    let workspace = agent_workspace_root(pool, agent_id).await?;
    let file_path = safe_workspace_path(&workspace, path)?;
    if !file_path.is_file() {
        return Err("workspace path is not a file".to_owned());
    }

    let metadata = fs::metadata(&file_path).map_err(to_string)?;
    let mut content = fs::read_to_string(&file_path)
        .map_err(|err| format!("workspace preview only supports UTF-8 text files: {err}"))?;
    let truncated = metadata.len() > AGENT_WORKSPACE_PREVIEW_LIMIT;
    if truncated {
        let mut boundary = AGENT_WORKSPACE_PREVIEW_LIMIT as usize;
        while boundary > 0 && !content.is_char_boundary(boundary) {
            boundary -= 1;
        }
        content.truncate(boundary);
        content.push_str("\n\n[preview truncated by Lantor]");
    }

    let name = file_path
        .file_name()
        .map(|value| value.to_string_lossy().to_string())
        .unwrap_or_else(|| path.to_owned());
    let relative_path = path_to_slash_string(
        file_path
            .strip_prefix(&workspace)
            .unwrap_or_else(|_| Path::new("")),
    );
    Ok(AgentWorkspaceFile {
        name,
        path: file_path.to_string_lossy().to_string(),
        relative_path,
        size_bytes: metadata.len() as i64,
        language: workspace_preview_language(&file_path),
        content,
        truncated,
    })
}

fn workspace_preview_language(path: &Path) -> String {
    match path
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "md" | "markdown" => "markdown",
        "json" => "json",
        "toml" => "toml",
        "yaml" | "yml" => "yaml",
        "rs" => "rust",
        "ts" | "tsx" => "typescript",
        "js" | "jsx" => "javascript",
        "css" => "css",
        "html" => "html",
        "sql" => "sql",
        "py" => "python",
        "sh" | "zsh" | "bash" => "shell",
        _ => "text",
    }
    .to_owned()
}

async fn load_messages(pool: &SqlitePool) -> CommandResult<Vec<Message>> {
    let rows = sqlx::query(
        r#"
        select
            m.id,
            m.channel_id,
            m.thread_root_id,
            m.sender_agent_id,
            m.sender_name,
            m.sender_role,
            m.body,
            m.is_task,
            m.thread_followed,
            m.delivery_state,
            m.stream_key,
            t.number as task_number,
            t.status as task_status,
            m.created_at,
            m.updated_at
        from messages m
        left join tasks t on t.message_id = m.id
        order by m.created_at asc
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(to_string)?;

    let mut messages: Vec<Message> = rows
        .into_iter()
        .map(|row| Message {
            id: row.get("id"),
            channel_id: row.get("channel_id"),
            thread_root_id: row.get("thread_root_id"),
            sender_agent_id: row.get("sender_agent_id"),
            sender_name: row.get("sender_name"),
            sender_role: row.get("sender_role"),
            body: row.get("body"),
            is_task: row.get("is_task"),
            thread_followed: row.get("thread_followed"),
            delivery_state: row.get("delivery_state"),
            stream_key: row.get("stream_key"),
            task_number: row.get("task_number"),
            task_status: row.get("task_status"),
            attachments: Vec::new(),
            artifacts: Vec::new(),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        })
        .collect();
    attach_message_attachments(pool, &mut messages).await?;
    attach_message_artifacts(pool, &mut messages).await?;
    Ok(messages)
}

async fn load_saved_messages(pool: &SqlitePool) -> CommandResult<Vec<SavedMessage>> {
    let rows = sqlx::query(
        r#"
        select
            sm.id,
            sm.message_id,
            m.channel_id,
            c.name as channel_name,
            m.thread_root_id,
            m.sender_name,
            m.sender_role,
            m.body,
            m.created_at as message_created_at,
            sm.created_at
        from saved_messages sm
        join messages m on m.id = sm.message_id
        join channels c on c.id = m.channel_id
        order by sm.created_at desc
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(to_string)?;

    Ok(rows
        .into_iter()
        .map(|row| SavedMessage {
            id: row.get("id"),
            message_id: row.get("message_id"),
            channel_id: row.get("channel_id"),
            channel_name: row.get("channel_name"),
            thread_root_id: row.get("thread_root_id"),
            sender_name: row.get("sender_name"),
            sender_role: row.get("sender_role"),
            body: row.get("body"),
            message_created_at: row.get("message_created_at"),
            created_at: row.get("created_at"),
        })
        .collect())
}

async fn load_dismissed_inbox_items(
    pool: &SqlitePool,
) -> CommandResult<HashMap<String, DateTime<Utc>>> {
    let rows = sqlx::query(
        r#"
        select item_id, hidden_until
        from owner_inbox_hidden_items
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(to_string)?;

    Ok(rows
        .into_iter()
        .map(|row| (row.get("item_id"), row.get("hidden_until")))
        .collect())
}

async fn load_read_inbox_items(pool: &SqlitePool) -> CommandResult<HashMap<String, DateTime<Utc>>> {
    let rows = sqlx::query(
        r#"
        select item_id, max(read_until) as read_until
        from (
            select item_id, read_until
            from owner_inbox_read_state
            union all
            select item_id, dismissed_until as read_until
            from owner_inbox_dismissals
        ) reads
        group by item_id
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(to_string)?;

    Ok(rows
        .into_iter()
        .map(|row| (row.get("item_id"), row.get("read_until")))
        .collect())
}

async fn load_message(pool: &SqlitePool, message_id: Uuid) -> CommandResult<Message> {
    let row = sqlx::query(
        r#"
        select
            m.id,
            m.channel_id,
            m.thread_root_id,
            m.sender_agent_id,
            m.sender_name,
            m.sender_role,
            m.body,
            m.is_task,
            m.thread_followed,
            m.delivery_state,
            m.stream_key,
            t.number as task_number,
            t.status as task_status,
            m.created_at,
            m.updated_at
        from messages m
        left join tasks t on t.message_id = m.id
        where m.id = $1
        "#,
    )
    .bind(message_id)
    .fetch_one(pool)
    .await
    .map_err(to_string)?;

    let mut message = Message {
        id: row.get("id"),
        channel_id: row.get("channel_id"),
        thread_root_id: row.get("thread_root_id"),
        sender_agent_id: row.get("sender_agent_id"),
        sender_name: row.get("sender_name"),
        sender_role: row.get("sender_role"),
        body: row.get("body"),
        is_task: row.get("is_task"),
        thread_followed: row.get("thread_followed"),
        delivery_state: row.get("delivery_state"),
        stream_key: row.get("stream_key"),
        task_number: row.get("task_number"),
        task_status: row.get("task_status"),
        attachments: Vec::new(),
        artifacts: Vec::new(),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    };
    attach_message_attachments(pool, std::slice::from_mut(&mut message)).await?;
    attach_message_artifacts(pool, std::slice::from_mut(&mut message)).await?;
    Ok(message)
}

async fn attach_message_attachments(
    pool: &SqlitePool,
    messages: &mut [Message],
) -> CommandResult<()> {
    if messages.is_empty() {
        return Ok(());
    }
    let ids: Vec<Uuid> = messages.iter().map(|message| message.id).collect();
    let placeholders = (0..ids.len())
        .map(|index| format!("${}", index + 1))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        r#"
        select id, message_id, original_name, mime_type, size_bytes, storage_path, created_at
        from message_attachments
        where message_id in ({placeholders})
        order by created_at asc
        "#,
    );
    let mut query = sqlx::query(&sql);
    for id in &ids {
        query = query.bind(*id);
    }
    let rows = query.fetch_all(pool).await.map_err(to_string)?;
    let mut attachments_by_message: HashMap<Uuid, Vec<MessageAttachment>> = HashMap::new();
    for row in rows {
        let attachment = MessageAttachment {
            id: row.get("id"),
            message_id: row.get("message_id"),
            original_name: row.get("original_name"),
            mime_type: row.get("mime_type"),
            size_bytes: row.get("size_bytes"),
            storage_path: row.get("storage_path"),
            created_at: row.get("created_at"),
        };
        attachments_by_message
            .entry(attachment.message_id)
            .or_default()
            .push(attachment);
    }
    for message in messages {
        message.attachments = attachments_by_message
            .remove(&message.id)
            .unwrap_or_default();
    }
    Ok(())
}

fn artifact_from_row(row: &sqlx::sqlite::SqliteRow) -> Artifact {
    Artifact {
        id: row.get("id"),
        message_id: row.get("message_id"),
        channel_id: row.get("channel_id"),
        thread_root_id: row.get("thread_root_id"),
        creator_agent_id: row.get("creator_agent_id"),
        creator_agent_handle: row.get("creator_agent_handle"),
        kind: row.get("kind"),
        title: row.get("title"),
        summary: row.get("summary"),
        content: row.get("content"),
        metadata: row.get("metadata"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

async fn load_artifacts(pool: &SqlitePool) -> CommandResult<Vec<Artifact>> {
    let rows = sqlx::query(
        r#"
        select
            ar.id,
            ar.message_id,
            ar.channel_id,
            ar.thread_root_id,
            ar.creator_agent_id,
            a.handle as creator_agent_handle,
            ar.kind,
            ar.title,
            ar.summary,
            ar.content,
            ar.metadata,
            ar.created_at,
            ar.updated_at
        from artifacts ar
        left join agents a on a.id = ar.creator_agent_id
        order by ar.created_at asc
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(to_string)?;
    Ok(rows.iter().map(artifact_from_row).collect())
}

pub(crate) async fn load_artifact(pool: &SqlitePool, artifact_id: Uuid) -> CommandResult<Artifact> {
    let row = sqlx::query(
        r#"
        select
            ar.id,
            ar.message_id,
            ar.channel_id,
            ar.thread_root_id,
            ar.creator_agent_id,
            a.handle as creator_agent_handle,
            ar.kind,
            ar.title,
            ar.summary,
            ar.content,
            ar.metadata,
            ar.created_at,
            ar.updated_at
        from artifacts ar
        left join agents a on a.id = ar.creator_agent_id
        where ar.id = $1
        "#,
    )
    .bind(artifact_id)
    .fetch_one(pool)
    .await
    .map_err(to_string)?;
    Ok(artifact_from_row(&row))
}

async fn attach_message_artifacts(
    pool: &SqlitePool,
    messages: &mut [Message],
) -> CommandResult<()> {
    if messages.is_empty() {
        return Ok(());
    }
    let ids: Vec<Uuid> = messages.iter().map(|message| message.id).collect();
    let placeholders = (0..ids.len())
        .map(|index| format!("${}", index + 1))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        r#"
        select
            ar.id,
            ar.message_id,
            ar.channel_id,
            ar.thread_root_id,
            ar.creator_agent_id,
            a.handle as creator_agent_handle,
            ar.kind,
            ar.title,
            ar.summary,
            ar.content,
            ar.metadata,
            ar.created_at,
            ar.updated_at
        from artifacts ar
        left join agents a on a.id = ar.creator_agent_id
        where ar.message_id in ({placeholders})
        order by ar.created_at asc
        "#,
    );
    let mut query = sqlx::query(&sql);
    for id in &ids {
        query = query.bind(*id);
    }
    let rows = query.fetch_all(pool).await.map_err(to_string)?;
    let mut artifacts_by_message: HashMap<Uuid, Vec<Artifact>> = HashMap::new();
    for row in rows {
        let artifact = artifact_from_row(&row);
        artifacts_by_message
            .entry(artifact.message_id)
            .or_default()
            .push(artifact);
    }
    for message in messages {
        message.artifacts = artifacts_by_message.remove(&message.id).unwrap_or_default();
    }
    Ok(())
}

async fn load_tasks(pool: &SqlitePool) -> CommandResult<Vec<Task>> {
    let rows = sqlx::query(
        r#"
        select
            t.id,
            t.number,
            t.message_id,
            t.channel_id,
            t.title,
            t.status,
            t.version,
            c.name as channel_name,
            t.assignee_agent_id as assignee_id,
            a.display_name as assignee_name,
            t.created_at,
            t.updated_at
        from tasks t
        join channels c on c.id = t.channel_id
        left join agents a on a.id = t.assignee_agent_id
        order by t.number desc
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(to_string)?;

    Ok(rows
        .into_iter()
        .map(|row| Task {
            id: row.get("id"),
            number: row.get("number"),
            message_id: row.get("message_id"),
            channel_id: row.get("channel_id"),
            title: row.get("title"),
            status: row.get("status"),
            version: row.get("version"),
            channel_name: row.get("channel_name"),
            assignee_id: row.get("assignee_id"),
            assignee_name: row.get("assignee_name"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        })
        .collect())
}

async fn load_reminders(pool: &SqlitePool) -> CommandResult<Vec<Reminder>> {
    let rows = sqlx::query(
        r#"
        select
            r.id,
            r.channel_id,
            c.name as channel_name,
            r.creator_agent_id,
            a.handle as creator_agent_handle,
            r.thread_root_id,
            r.message_id,
            r.title,
            r.note,
            r.status,
            r.recurrence,
            r.due_at,
            r.fired_at,
            r.completed_at,
            r.created_at,
            r.updated_at
        from reminders r
        left join channels c on c.id = r.channel_id
        left join agents a on a.id = r.creator_agent_id
        where r.status in ('scheduled', 'fired')
        order by
            case r.status when 'fired' then 0 else 1 end,
            r.due_at asc
        limit 100
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(to_string)?;

    Ok(rows
        .into_iter()
        .map(|row| Reminder {
            id: row.get("id"),
            channel_id: row.get("channel_id"),
            channel_name: row.get("channel_name"),
            creator_agent_id: row.get("creator_agent_id"),
            creator_agent_handle: row.get("creator_agent_handle"),
            thread_root_id: row.get("thread_root_id"),
            message_id: row.get("message_id"),
            title: row.get("title"),
            note: row.get("note"),
            status: row.get("status"),
            recurrence: row.get("recurrence"),
            due_at: row.get("due_at"),
            fired_at: row.get("fired_at"),
            completed_at: row.get("completed_at"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        })
        .collect())
}

async fn load_agent_schedules(pool: &SqlitePool) -> CommandResult<Vec<AgentSchedule>> {
    let rows = sqlx::query(
        r#"
        select
            s.id,
            s.agent_id,
            a.handle as agent_handle,
            s.channel_id,
            c.name as channel_name,
            c.kind as channel_kind,
            s.thread_root_id,
            s.title,
            s.prompt,
            s.cadence,
            s.status,
            s.next_run_at,
            s.last_run_at,
            s.last_work_item_id,
            s.created_at,
            s.updated_at
        from agent_schedules s
        join agents a on a.id = s.agent_id
        join channels c on c.id = s.channel_id
        where s.status in ('active', 'paused')
        order by
            case s.status when 'active' then 0 else 1 end,
            s.next_run_at asc
        limit 100
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(to_string)?;

    Ok(rows
        .into_iter()
        .map(|row| AgentSchedule {
            id: row.get("id"),
            agent_id: row.get("agent_id"),
            agent_handle: row.get("agent_handle"),
            channel_id: row.get("channel_id"),
            channel_name: row.get("channel_name"),
            channel_kind: row.get("channel_kind"),
            thread_root_id: row.get("thread_root_id"),
            title: row.get("title"),
            prompt: row.get("prompt"),
            cadence: row.get("cadence"),
            status: row.get("status"),
            next_run_at: row.get("next_run_at"),
            last_run_at: row.get("last_run_at"),
            last_work_item_id: row.get("last_work_item_id"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        })
        .collect())
}

async fn load_agent_runs(pool: &SqlitePool) -> CommandResult<Vec<AgentRun>> {
    let rows = sqlx::query(
        r#"
        select
            r.id,
            r.agent_id,
            a.handle as agent_handle,
            r.work_item_id,
            r.command,
            r.working_directory,
            r.status,
            r.pid,
            r.exit_code,
            r.log,
            r.input_tokens,
            r.output_tokens,
            r.cost_micros,
            r.started_at,
            r.stopped_at
        from agent_runs r
        join agents a on a.id = r.agent_id
        order by r.started_at desc
        limit 30
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(to_string)?;

    Ok(rows
        .into_iter()
        .map(|row| AgentRun {
            id: row.get("id"),
            agent_id: row.get("agent_id"),
            agent_handle: row.get("agent_handle"),
            work_item_id: row.get("work_item_id"),
            command: row.get("command"),
            working_directory: row.get("working_directory"),
            status: row.get("status"),
            pid: row.get("pid"),
            exit_code: row.get("exit_code"),
            log: row.get("log"),
            input_tokens: row.get("input_tokens"),
            output_tokens: row.get("output_tokens"),
            cost_micros: row.get("cost_micros"),
            started_at: row.get("started_at"),
            stopped_at: row.get("stopped_at"),
        })
        .collect())
}

async fn load_agent_run_patch(pool: &SqlitePool, run_id: Uuid) -> CommandResult<AgentRunPatch> {
    let row = sqlx::query(
        r#"
        select
            r.id,
            r.agent_id,
            a.handle as agent_handle,
            r.work_item_id,
            r.command,
            r.working_directory,
            r.status,
            r.pid,
            r.exit_code,
            r.input_tokens,
            r.output_tokens,
            r.cost_micros,
            r.started_at,
            r.stopped_at
        from agent_runs r
        join agents a on a.id = r.agent_id
        where r.id = $1
        "#,
    )
    .bind(run_id)
    .fetch_one(pool)
    .await
    .map_err(to_string)?;

    Ok(AgentRunPatch {
        id: row.get("id"),
        agent_id: row.get("agent_id"),
        agent_handle: row.get("agent_handle"),
        work_item_id: row.get("work_item_id"),
        command: row.get("command"),
        working_directory: row.get("working_directory"),
        status: row.get("status"),
        pid: row.get("pid"),
        exit_code: row.get("exit_code"),
        input_tokens: row.get("input_tokens"),
        output_tokens: row.get("output_tokens"),
        cost_micros: row.get("cost_micros"),
        started_at: row.get("started_at"),
        stopped_at: row.get("stopped_at"),
    })
}

async fn load_agent_work_items(pool: &SqlitePool) -> CommandResult<Vec<AgentWorkItem>> {
    let rows = sqlx::query(
        r#"
        select
            w.id,
            w.agent_id,
            a.handle as agent_handle,
            w.channel_id,
            c.name as channel_name,
            w.thread_root_id,
            w.source_message_id,
            w.inbox_item_id,
            w.task_id,
            t.number as task_number,
            w.source_kind,
            w.title,
            w.context,
            w.status,
            w.run_id,
            w.created_at,
            w.updated_at,
            w.completed_at
        from agent_work_items w
        join agents a on a.id = w.agent_id
        left join channels c on c.id = w.channel_id
        left join tasks t on t.id = w.task_id
        order by w.created_at desc
        limit 80
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(to_string)?;

    Ok(rows
        .into_iter()
        .map(|row| AgentWorkItem {
            id: row.get("id"),
            agent_id: row.get("agent_id"),
            agent_handle: row.get("agent_handle"),
            channel_id: row.get("channel_id"),
            channel_name: row.get("channel_name"),
            thread_root_id: row.get("thread_root_id"),
            source_message_id: row.get("source_message_id"),
            inbox_item_id: row.get("inbox_item_id"),
            task_id: row.get("task_id"),
            task_number: row.get("task_number"),
            source_kind: row.get("source_kind"),
            title: row.get("title"),
            context: row.get("context"),
            status: row.get("status"),
            run_id: row.get("run_id"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
            completed_at: row.get("completed_at"),
        })
        .collect())
}

async fn load_agent_work_item_patch(
    pool: &SqlitePool,
    work_item_id: Uuid,
) -> CommandResult<AgentWorkItemPatch> {
    let row = sqlx::query(
        r#"
        select
            w.id,
            w.agent_id,
            a.handle as agent_handle,
            w.channel_id,
            c.name as channel_name,
            w.thread_root_id,
            w.source_message_id,
            w.inbox_item_id,
            w.task_id,
            t.number as task_number,
            w.source_kind,
            w.title,
            w.status,
            w.run_id,
            w.created_at,
            w.updated_at,
            w.completed_at
        from agent_work_items w
        join agents a on a.id = w.agent_id
        left join channels c on c.id = w.channel_id
        left join tasks t on t.id = w.task_id
        where w.id = $1
        "#,
    )
    .bind(work_item_id)
    .fetch_one(pool)
    .await
    .map_err(to_string)?;

    Ok(AgentWorkItemPatch {
        id: row.get("id"),
        agent_id: row.get("agent_id"),
        agent_handle: row.get("agent_handle"),
        channel_id: row.get("channel_id"),
        channel_name: row.get("channel_name"),
        thread_root_id: row.get("thread_root_id"),
        source_message_id: row.get("source_message_id"),
        inbox_item_id: row.get("inbox_item_id"),
        task_id: row.get("task_id"),
        task_number: row.get("task_number"),
        source_kind: row.get("source_kind"),
        title: row.get("title"),
        status: row.get("status"),
        run_id: row.get("run_id"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
        completed_at: row.get("completed_at"),
    })
}

async fn load_agent_activities(pool: &SqlitePool) -> CommandResult<Vec<AgentActivity>> {
    let rows = sqlx::query(
        r#"
        select
            id,
            agent_id,
            agent_handle,
            run_id,
            kind,
            phase,
            status,
            title,
            summary,
            detail,
            metadata as metadata,
            created_at
        from (
            select
                agent_activities.*,
                row_number() over (
                    partition by coalesce(case when agent_id is null then null else lower(hex(agent_id)) end, nullif(agent_handle, ''), 'unknown')
                    order by created_at desc
                ) as activity_rank
            from agent_activities
        ) ranked
        where activity_rank <= 80
        order by created_at desc
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(to_string)?;

    Ok(rows
        .into_iter()
        .map(|row| AgentActivity {
            id: row.get("id"),
            agent_id: row.get("agent_id"),
            agent_handle: row.get("agent_handle"),
            run_id: row.get("run_id"),
            kind: row.get("kind"),
            phase: row.get("phase"),
            status: row.get("status"),
            title: row.get("title"),
            summary: row.get("summary"),
            detail: row.get("detail"),
            metadata: parse_json_value(row.get("metadata")),
            created_at: row.get("created_at"),
        })
        .collect())
}

async fn load_agent_activity(pool: &SqlitePool, activity_id: Uuid) -> CommandResult<AgentActivity> {
    let row = sqlx::query(
        r#"
        select
            id,
            agent_id,
            agent_handle,
            run_id,
            kind,
            phase,
            status,
            title,
            summary,
            detail,
            metadata as metadata,
            created_at
        from agent_activities
        where id = $1
        "#,
    )
    .bind(activity_id)
    .fetch_one(pool)
    .await
    .map_err(to_string)?;

    Ok(AgentActivity {
        id: row.get("id"),
        agent_id: row.get("agent_id"),
        agent_handle: row.get("agent_handle"),
        run_id: row.get("run_id"),
        kind: row.get("kind"),
        phase: row.get("phase"),
        status: row.get("status"),
        title: row.get("title"),
        summary: row.get("summary"),
        detail: row.get("detail"),
        metadata: parse_json_value(row.get("metadata")),
        created_at: row.get("created_at"),
    })
}

async fn load_supervisor_status(pool: &SqlitePool) -> CommandResult<SupervisorStatus> {
    let row = sqlx::query(
        r#"
        select pid, status, updated_at
        from supervisor_state
        where id = 1
        "#,
    )
    .fetch_optional(pool)
    .await
    .map_err(to_string)?;

    let Some(row) = row else {
        return Ok(SupervisorStatus {
            pid: None,
            status: "offline".to_owned(),
            updated_at: None,
        });
    };

    let updated_at: DateTime<Utc> = row.get("updated_at");
    let status = if Utc::now().signed_duration_since(updated_at).num_seconds() > 10 {
        "stale".to_owned()
    } else {
        row.get("status")
    };

    Ok(SupervisorStatus {
        pid: row.get("pid"),
        status,
        updated_at: Some(updated_at),
    })
}

fn parse_json_value(raw: String) -> Value {
    serde_json::from_str(&raw).unwrap_or_else(|_| json!({}))
}

fn activity_phase(kind: &str) -> &'static str {
    match kind {
        "thinking" => "thinking",
        "command" => "command",
        "file_edit" => "file_edit",
        "tools" => "tools",
        "error" | "event_error" | "run_error" => "error",
        "run" | "usage" => "runtime",
        "dispatch" | "mention" | "dm" | "task" | "schedule" | "channel" | "membership" => "work",
        "profile" | "memory" => "profile",
        _ => "acting",
    }
}

fn normalize_agent_activity_kind(kind: Option<&str>) -> &'static str {
    match kind.map(str::trim).filter(|kind| !kind.is_empty()) {
        Some("thinking") => "thinking",
        Some("command") | Some("running_command") => "command",
        Some("file_edit") | Some("editing_file") => "file_edit",
        Some("tools") | Some("tool") => "tools",
        Some("error") => "error",
        Some("task") => "task",
        Some("message") => "message",
        Some("dispatch") => "dispatch",
        Some("reminder") => "schedule",
        Some("schedule") => "schedule",
        Some("usage") => "usage",
        Some("memory") => "memory",
        Some("channel") => "channel",
        Some("membership") => "membership",
        _ => "acting",
    }
}

fn activity_status(kind: &str, title: &str) -> &'static str {
    let lowered = title.to_lowercase();
    let is_terminal_error_kind = matches!(kind, "error" | "event_error" | "run_error");
    let can_infer_error_from_title =
        matches!(kind, "command" | "run" | "dispatch" | "task" | "schedule");
    if is_terminal_error_kind
        || (can_infer_error_from_title
            && (lowered.contains("failed")
                || lowered.contains("error")
                || lowered.contains("rejected")))
    {
        "error"
    } else if lowered.contains("warning") {
        "warning"
    } else if lowered.contains("cancel") || lowered.contains("stop") || lowered.contains("stopping")
    {
        "warning"
    } else if lowered.contains("completed")
        || lowered.contains("complete")
        || lowered.contains("done")
        || lowered.contains("exited")
        || lowered.contains("finished")
        || lowered.contains("ready")
        || lowered.contains("accepted")
    {
        "success"
    } else if matches!(
        kind,
        "thinking" | "command" | "file_edit" | "tools" | "acting"
    ) {
        "active"
    } else if lowered.contains("running")
        || lowered.contains("started")
        || lowered.contains("queued")
        || lowered.contains("dispatched")
        || lowered.contains("responding")
        || lowered.contains("thinking")
        || lowered.contains("editing")
        || lowered.contains("using")
    {
        "active"
    } else {
        "info"
    }
}

fn work_status_title(status: &str) -> &'static str {
    match status {
        "running" => "Request started",
        "done" => "Request completed",
        "silent" => "No visible reply needed",
        "cancelled" => "Request cancelled",
        "failed" => "Request failed",
        "queued" => "Request queued",
        _ => "Request updated",
    }
}

fn parse_activity_metadata(detail: &str) -> Value {
    let detail = detail.trim();
    if detail.is_empty() {
        return json!({});
    }
    if let Ok(value) = serde_json::from_str::<Value>(detail) {
        if value.is_object() {
            return value;
        }
    }

    let mut metadata = serde_json::Map::new();
    for segment in detail.split([',', '\n']) {
        let Some((key, value)) = segment.split_once('=') else {
            continue;
        };
        let key = key.trim();
        let value = value.trim();
        if key.is_empty() || value.is_empty() {
            continue;
        }
        metadata.insert(key.to_owned(), json!(value));
        if key.ends_with("duration") || key == "duration" {
            if let Some(ms) = value
                .split_whitespace()
                .next()
                .and_then(|value| value.parse::<u64>().ok())
            {
                metadata.insert("duration_ms".to_owned(), json!(ms));
            }
        }
    }

    if metadata.is_empty() {
        if Uuid::parse_str(detail).is_ok() {
            metadata.insert("reference_id".to_owned(), json!(detail));
        } else {
            metadata.insert("detail".to_owned(), json!(detail));
        }
    }

    Value::Object(metadata)
}

fn memory_path_for_workspace(working_directory: &str) -> CommandResult<PathBuf> {
    let working_directory = working_directory.trim();
    if working_directory.is_empty() {
        return Err("agent working_directory is not configured".to_owned());
    }
    let workspace = PathBuf::from(working_directory);
    fs::create_dir_all(&workspace).map_err(to_string)?;
    Ok(workspace.join("MEMORY.md"))
}

async fn agent_memory_path(pool: &SqlitePool, agent_id: Uuid) -> CommandResult<PathBuf> {
    let row = sqlx::query("select handle, working_directory from agents where id = $1")
        .bind(agent_id)
        .fetch_one(pool)
        .await
        .map_err(to_string)?;
    let handle: String = row.get("handle");
    let working_directory: String = row.get("working_directory");
    ensure_agent_workspace(working_directory.trim(), &handle)?;
    memory_path_for_workspace(&working_directory)
}

#[cfg(test)]
fn format_memory_index_entry(body: &str) -> String {
    let body = body.trim();
    let body = body
        .lines()
        .map(str::trim_end)
        .collect::<Vec<_>>()
        .join("\n");
    let body = body
        .strip_prefix("- ")
        .or_else(|| body.strip_prefix("* "))
        .unwrap_or(&body)
        .trim();
    let mut lines = body.lines();
    let first = lines.next().unwrap_or("").trim();
    let mut entry = format!("- {first}");
    for line in lines {
        let line = line.trim();
        if !line.is_empty() {
            entry.push_str("\n  ");
            entry.push_str(line);
        }
    }
    entry
}

fn insert_memory_index_entry(memory: &str, entry: &str) -> String {
    let memory = memory.trim_end();
    let entry = entry.trim();
    if memory
        .lines()
        .any(|line| line.trim() == entry || line.trim() == entry.trim_start_matches("- ").trim())
    {
        return format!("{memory}\n");
    }

    let section_start = memory
        .find("\n## Key Knowledge")
        .or_else(|| memory.starts_with("## Key Knowledge").then_some(0));
    let Some(section_start) = section_start else {
        return format!("{memory}\n\n## Key Knowledge\n{entry}\n");
    };

    let content_start = if section_start == 0 {
        "## Key Knowledge".len()
    } else {
        section_start + "\n## Key Knowledge".len()
    };
    let section_end = memory[content_start..]
        .find("\n## ")
        .map(|offset| content_start + offset)
        .unwrap_or(memory.len());
    let section = &memory[content_start..section_end];

    let placeholder = section
        .trim()
        .lines()
        .all(|line| line.trim().is_empty() || line.trim_start().starts_with("- Add "));

    let mut updated = String::new();
    updated.push_str(&memory[..content_start]);
    updated.push('\n');
    if !placeholder {
        let existing = section.trim();
        if !existing.is_empty() {
            updated.push_str(existing);
            updated.push('\n');
        }
    }
    updated.push_str(entry);
    updated.push('\n');
    updated.push_str(memory[section_end..].trim_start_matches('\n'));
    updated.push('\n');
    updated
}

async fn append_agent_memory(pool: &SqlitePool, agent_id: Uuid, body: &str) -> CommandResult<()> {
    let body = body.trim();
    if body.is_empty() {
        return Err("memory_append body is empty".to_owned());
    }
    let path = agent_memory_path(pool, agent_id).await?;
    let workspace = path
        .parent()
        .ok_or_else(|| "agent memory path has no parent".to_owned())?;
    let notes_dir = workspace.join("notes");
    fs::create_dir_all(&notes_dir).map_err(to_string)?;

    let memory = fs::read_to_string(&path).unwrap_or_default();
    if !memory.contains("notes/work-log.md") {
        let index_entry = "- `notes/work-log.md`: chronological durable updates staged by `memory_append`; keep `MEMORY.md` as the compact recovery index.";
        fs::write(&path, insert_memory_index_entry(&memory, index_entry)).map_err(to_string)?;
    }

    let note_path = notes_dir.join("work-log.md");
    if !note_path.exists() {
        fs::write(
            &note_path,
            "# Work Log\n\nChronological durable updates staged by `memory_append`. Promote only stable, reusable facts into `MEMORY.md` with `memory_compact`.\n",
        )
        .map_err(to_string)?;
    }
    let entry = format!(
        "\n\n## Memory update {}\n{}\n",
        Utc::now().to_rfc3339(),
        body
    );
    fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(note_path)
        .and_then(|mut file| std::io::Write::write_all(&mut file, entry.as_bytes()))
        .map_err(to_string)
}

async fn compact_agent_memory(pool: &SqlitePool, agent_id: Uuid, body: &str) -> CommandResult<()> {
    let body = body.trim();
    if body.is_empty() {
        return Err("memory_compact body is empty".to_owned());
    }
    let path = agent_memory_path(pool, agent_id).await?;
    if path.exists() {
        let backup = path.with_extension(format!("md.bak-{}", Utc::now().format("%Y%m%d%H%M%S")));
        let _ = fs::copy(&path, backup);
    }
    fs::write(path, format!("{body}\n")).map_err(to_string)
}

pub(crate) async fn create_channel_in_pool(
    pool: &SqlitePool,
    name: &str,
    description: &str,
) -> CommandResult<Uuid> {
    let normalized = normalize_channel_name(name);
    if normalized.is_empty() {
        return Err("channel name is empty".to_owned());
    }
    let channel_id = sqlx::query_scalar(
        r#"
        insert into channels (name, description, kind)
        values ($1, $2, 'channel')
        on conflict (name) do update
            set description = case
                when excluded.description <> '' then excluded.description
                else channels.description
            end
        returning id
        "#,
    )
    .bind(normalized)
    .bind(description.trim())
    .fetch_one(pool)
    .await
    .map_err(to_string)?;

    let _ = notify_ui_refresh(pool, "channel_created").await;
    Ok(channel_id)
}

pub(crate) async fn add_agent_to_channel(
    pool: &SqlitePool,
    channel_id: Uuid,
    agent_id: Uuid,
) -> CommandResult<()> {
    let kind: Option<String> = sqlx::query_scalar("select kind from channels where id = $1")
        .bind(channel_id)
        .fetch_optional(pool)
        .await
        .map_err(to_string)?;
    if kind.as_deref() == Some("dm") {
        return Err("direct message membership is fixed".to_owned());
    }
    sqlx::query(
        r#"
        insert into channel_members (channel_id, agent_id)
        values ($1, $2)
        on conflict (channel_id, agent_id) do nothing
        "#,
    )
    .bind(channel_id)
    .bind(agent_id)
    .execute(pool)
    .await
    .map_err(to_string)?;
    let _ = notify_ui_refresh(pool, "channel_membership_updated").await;
    Ok(())
}

async fn record_agent_activity(
    pool: &SqlitePool,
    agent_id: Option<Uuid>,
    run_id: Option<Uuid>,
    kind: &str,
    title: impl AsRef<str>,
    detail: impl AsRef<str>,
) -> CommandResult<()> {
    let agent_handle = match agent_id {
        Some(agent_id) => sqlx::query_scalar("select handle from agents where id = $1")
            .bind(agent_id)
            .fetch_optional(pool)
            .await
            .map_err(to_string)?
            .unwrap_or_else(|| "unknown".to_owned()),
        None => String::new(),
    };
    let title = title.as_ref();
    let detail = detail.as_ref();
    let phase = activity_phase(kind);
    let status = activity_status(kind, title);
    let summary = title;
    let metadata = parse_activity_metadata(detail);

    let activity_id: Uuid = sqlx::query_scalar(
        r#"
        insert into agent_activities (
            agent_id,
            agent_handle,
            run_id,
            kind,
            phase,
            status,
            title,
            summary,
            detail,
            metadata
        )
        values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        returning id
        "#,
    )
    .bind(agent_id)
    .bind(agent_handle)
    .bind(run_id)
    .bind(kind)
    .bind(phase)
    .bind(status)
    .bind(title)
    .bind(summary)
    .bind(detail)
    .bind(metadata.to_string())
    .fetch_one(pool)
    .await
    .map_err(to_string)?;
    if let Ok(activity) = load_agent_activity(pool, activity_id).await {
        let _ = notify_ui_activity_upsert(pool, &activity, "activity").await;
    } else {
        let _ = notify_ui_refresh(pool, "activity").await;
    }

    Ok(())
}

async fn record_agent_activity_throttled(
    pool: &SqlitePool,
    agent_id: Option<Uuid>,
    run_id: Option<Uuid>,
    kind: &str,
    title: impl AsRef<str>,
    detail: impl AsRef<str>,
) -> CommandResult<()> {
    let title = title.as_ref();
    let detail = detail.as_ref();
    let recently_recorded: bool = sqlx::query_scalar(
        r#"
        select exists (
            select 1
            from agent_activities
            where agent_id is not distinct from $1
              and run_id is not distinct from $2
              and kind = $3
              and title = $4
              and detail = $5
              and created_at > strftime('%Y-%m-%dT%H:%M:%f+00:00','now','-1 second')
        )
        "#,
    )
    .bind(agent_id)
    .bind(run_id)
    .bind(kind)
    .bind(title)
    .bind(detail)
    .fetch_one(pool)
    .await
    .map_err(to_string)?;

    if recently_recorded {
        return Ok(());
    }

    record_agent_activity(pool, agent_id, run_id, kind, title, detail).await
}

async fn append_run_log(pool: &SqlitePool, run_id: Uuid, line: String) -> CommandResult<()> {
    sqlx::query("update agent_runs set log = substr(log || $2, -20000) where id = $1")
        .bind(run_id)
        .bind(line)
        .execute(pool)
        .await
        .map_err(to_string)?;

    Ok(())
}

fn truncate_activity_detail(value: &str) -> String {
    let trimmed = value.trim();
    let mut chars = trimmed.chars();
    let mut out: String = chars.by_ref().take(600).collect();
    if chars.next().is_some() {
        out.push_str("...");
    }
    out
}

fn structured_log_string<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value.get(key).and_then(Value::as_str)
}

fn structured_log_message(value: &Value) -> Option<&str> {
    value
        .pointer("/fields/message")
        .and_then(Value::as_str)
        .or_else(|| structured_log_string(value, "message"))
}

fn structured_log_field_string<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value
        .get("fields")
        .and_then(|fields| fields.get(key))
        .and_then(Value::as_str)
        .or_else(|| structured_log_string(value, key))
}

fn structured_log_detail(
    value: &Value,
    level: &str,
    target: Option<&str>,
    message: Option<&str>,
) -> String {
    let mut detail = serde_json::Map::new();
    detail.insert("level".to_owned(), json!(level));
    if let Some(target) = target.map(str::trim).filter(|target| !target.is_empty()) {
        detail.insert("target".to_owned(), json!(target));
    }
    if let Some(message) = message.map(str::trim).filter(|message| !message.is_empty()) {
        detail.insert("message".to_owned(), json!(message));
    }
    if detail.len() > 1 {
        return Value::Object(detail).to_string();
    }
    truncate_activity_detail(&value.to_string())
}

fn is_ignored_codex_manifest_warning(
    level: &str,
    target: Option<&str>,
    message: Option<&str>,
) -> bool {
    matches!(level.to_ascii_uppercase().as_str(), "WARN" | "WARNING")
        && target == Some("codex_core_plugins::manifest")
        && message
            .map(str::trim)
            .is_some_and(|message| message.starts_with("ignoring interface.defaultPrompt:"))
}

fn is_ignored_codex_skill_loader_warning(
    level: &str,
    target: Option<&str>,
    message: Option<&str>,
) -> bool {
    matches!(level.to_ascii_uppercase().as_str(), "WARN" | "WARNING")
        && target == Some("codex_core_skills::loader")
        && message.map(str::trim).is_some_and(|message| {
            matches!(
                message,
                "ignoring interface.icon_small: icon path must not contain '..'"
                    | "ignoring interface.icon_large: icon path must not contain '..'"
            )
        })
}

fn is_ignored_codex_legacy_notify_warning(
    value: &Value,
    level: &str,
    target: Option<&str>,
    message: Option<&str>,
) -> bool {
    matches!(level.to_ascii_uppercase().as_str(), "WARN" | "WARNING")
        && target == Some("codex_core::session::turn")
        && message.map(str::trim) == Some("after_agent hook failed; continuing")
        && structured_log_field_string(value, "hook_name") == Some("legacy_notify")
        && structured_log_field_string(value, "error")
            .map(str::trim)
            .is_some_and(|error| error.contains("No such file or directory"))
}

fn strip_ansi_escape_sequences(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    let mut chars = value.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '\u{1b}' {
            output.push(ch);
            continue;
        }
        if chars.peek() != Some(&'[') {
            continue;
        }
        chars.next();
        for next in chars.by_ref() {
            if next.is_ascii_alphabetic() {
                break;
            }
        }
    }
    output
}

fn is_retryable_codex_stderr_error(line: &str) -> bool {
    let cleaned = strip_ansi_escape_sequences(line).to_lowercase();
    (cleaned.contains("codex_api::endpoint::responses_websocket")
        && cleaned.contains("failed to connect to websocket"))
        || (cleaned.contains("codex_models_manager::manager")
            && cleaned.contains("failed to refresh available models"))
        || (cleaned.contains("rmcp::transport::worker")
            && cleaned.contains("https://chatgpt.com/backend-api/wham/apps"))
}

fn classify_structured_stderr_log(
    line: &str,
) -> Option<Option<(&'static str, &'static str, String)>> {
    let Ok(value) = serde_json::from_str::<Value>(line) else {
        return None;
    };
    if !value.is_object() {
        return None;
    }
    let Some(level) = structured_log_string(&value, "level") else {
        return None;
    };

    let level = level.trim();
    let target = structured_log_string(&value, "target");
    let message = structured_log_message(&value);
    if is_ignored_codex_manifest_warning(level, target, message) {
        return Some(None);
    }
    if is_ignored_codex_skill_loader_warning(level, target, message) {
        return Some(None);
    }
    if is_ignored_codex_legacy_notify_warning(&value, level, target, message) {
        return Some(None);
    }

    let detail = structured_log_detail(&value, level, target, message);
    match level.to_ascii_uppercase().as_str() {
        "ERROR" => Some(Some(("error", "Runtime error", detail))),
        "WARN" | "WARNING" => Some(Some(("run", "Runtime warning", detail))),
        "DEBUG" | "TRACE" | "INFO" => Some(None),
        _ => Some(Some(("run", "Runtime log", detail))),
    }
}

fn classify_agent_output_activity(
    label: &str,
    line: &str,
) -> Option<(&'static str, &'static str, String)> {
    let trimmed = line.trim();
    if trimmed.is_empty() || extract_agent_event_json(trimmed).is_some() {
        return None;
    }

    let lower = trimmed.to_lowercase();
    if let Some(activity) = classify_structured_stderr_log(trimmed) {
        return activity;
    }

    if label == "stderr" && is_retryable_codex_stderr_error(trimmed) {
        return Some(("run", "Runtime warning", truncate_activity_detail(trimmed)));
    }

    let is_error = label == "stderr"
        && [
            "error",
            "failed",
            "panic",
            "exception",
            "traceback",
            "permission denied",
            "not found",
        ]
        .iter()
        .any(|needle| lower.contains(needle));
    if is_error {
        return Some(("error", "Error output", truncate_activity_detail(trimmed)));
    }

    let is_warning = label == "stderr"
        && (lower.contains("warning")
            || lower.contains("warn:")
            || lower.contains("level=warn")
            || lower.contains("\"level\":\"warn\""));
    if is_warning {
        return Some(("run", "Runtime warning", truncate_activity_detail(trimmed)));
    }

    let is_tool = [
        "tool",
        "exec_command",
        "apply_patch",
        "running command",
        "cargo ",
        "npm ",
        "git ",
        "psql ",
        "slock ",
        "rg ",
        "sed ",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
        || trimmed.starts_with("$ ");
    if is_tool {
        return Some(("tools", "Using tools", truncate_activity_detail(trimmed)));
    }

    let is_action = [
        "message sent",
        "created",
        "updated",
        "deleted",
        "fixed",
        "committed",
        "commit ",
        "done",
        "completed",
        "finished",
    ]
    .iter()
    .any(|needle| lower.contains(needle));
    if is_action {
        return Some(("acting", "Acting", truncate_activity_detail(trimmed)));
    }

    if label == "stderr" {
        return Some(("run", "Runtime output", truncate_activity_detail(trimmed)));
    }

    Some(("thinking", "Thinking", truncate_activity_detail(trimmed)))
}

async fn pipe_run_output<R>(
    pool: SqlitePool,
    agent_id: Uuid,
    run_id: Uuid,
    stream: R,
    label: &'static str,
    parse_agent_events: bool,
) where
    R: AsyncRead + Unpin,
{
    let mut lines = BufReader::new(stream).lines();
    loop {
        match lines.next_line().await {
            Ok(Some(line)) => {
                let _ = append_run_log(&pool, run_id, format!("[{label}] {line}\n")).await;
                if let Some((kind, title, detail)) = classify_agent_output_activity(label, &line) {
                    let _ = record_agent_activity_throttled(
                        &pool,
                        Some(agent_id),
                        Some(run_id),
                        kind,
                        title,
                        detail,
                    )
                    .await;
                }
                if parse_agent_events {
                    if let Some(reason) = silent_reply_reason(&line) {
                        let _ = mark_run_work_item_silent(&pool, agent_id, run_id, &reason).await;
                        continue;
                    }
                    match ingest_agent_event_line(&pool, agent_id, run_id, &line).await {
                        Ok(Some(note)) => {
                            let _ =
                                append_run_log(&pool, run_id, format!("[event] {note}\n")).await;
                            let _ = record_agent_activity(
                                &pool,
                                Some(agent_id),
                                Some(run_id),
                                "event",
                                "Stdout event accepted",
                                note,
                            )
                            .await;
                        }
                        Ok(None) => {}
                        Err(err) => {
                            let _ =
                                append_run_log(&pool, run_id, format!("[event] rejected: {err}\n"))
                                    .await;
                            let _ = record_agent_activity(
                                &pool,
                                Some(agent_id),
                                Some(run_id),
                                "event_error",
                                "Stdout event rejected",
                                err.to_string(),
                            )
                            .await;
                        }
                    }
                }
            }
            Ok(None) => break,
            Err(err) => {
                let _ =
                    append_run_log(&pool, run_id, format!("[{label}] read error: {err}\n")).await;
                break;
            }
        }
    }
}

async fn ingest_agent_event_line(
    pool: &SqlitePool,
    agent_id: Uuid,
    run_id: Uuid,
    line: &str,
) -> CommandResult<Option<String>> {
    let Some(json) = extract_agent_event_json(line) else {
        return Ok(None);
    };
    if !claim_agent_event(pool, run_id, json).await? {
        return Ok(None);
    }
    let event: AgentEvent = serde_json::from_str(json).map_err(to_string)?;
    handle_agent_event(pool, agent_id, run_id, event)
        .await
        .map(Some)
}

async fn claim_agent_event(pool: &SqlitePool, run_id: Uuid, json: &str) -> CommandResult<bool> {
    let event_hash = format!("{:x}", Sha256::digest(json.as_bytes()));
    let inserted: Option<bool> = sqlx::query_scalar(
        r#"
        insert into agent_event_receipts (run_id, event_json, event_hash)
        values ($1, $2, $3)
        on conflict (run_id, event_hash) do nothing
        returning true
        "#,
    )
    .bind(run_id)
    .bind(json)
    .bind(event_hash)
    .fetch_optional(pool)
    .await
    .map_err(to_string)?;

    Ok(inserted.unwrap_or(false))
}

async fn replay_agent_events_from_run_log_if_needed(
    pool: &SqlitePool,
    agent_id: Uuid,
    run_id: Uuid,
) -> CommandResult<usize> {
    let accepted_events: i64 = sqlx::query_scalar(
        r#"
        select count(*)
        from agent_activities
        where run_id = $1
          and kind = 'event'
          and title = 'Stdout event accepted'
        "#,
    )
    .bind(run_id)
    .fetch_one(pool)
    .await
    .map_err(to_string)?;
    if accepted_events > 0 {
        return Ok(0);
    }

    let Some(log): Option<String> = sqlx::query_scalar("select log from agent_runs where id = $1")
        .bind(run_id)
        .fetch_optional(pool)
        .await
        .map_err(to_string)?
    else {
        return Ok(0);
    };

    let mut replayed = 0;
    let mut seen = HashSet::new();
    for line in log.lines() {
        let Some(json) = extract_agent_event_json(line) else {
            continue;
        };
        if !seen.insert(json.to_owned()) {
            continue;
        }
        let result = match serde_json::from_str::<AgentEvent>(json).map_err(to_string) {
            Ok(event) => {
                if claim_agent_event(pool, run_id, json).await? {
                    handle_agent_event(pool, agent_id, run_id, event).await
                } else {
                    continue;
                }
            }
            Err(err) => Err(err),
        };
        match result {
            Ok(note) => {
                replayed += 1;
                append_run_log(pool, run_id, format!("[event-replay] {note}\n")).await?;
                record_agent_activity(
                    pool,
                    Some(agent_id),
                    Some(run_id),
                    "event",
                    "Run log event replayed",
                    note,
                )
                .await?;
            }
            Err(err) => {
                append_run_log(pool, run_id, format!("[event-replay] rejected: {err}\n")).await?;
                record_agent_activity(
                    pool,
                    Some(agent_id),
                    Some(run_id),
                    "event_error",
                    "Run log event rejected",
                    err,
                )
                .await?;
            }
        }
    }

    Ok(replayed)
}

fn extract_agent_event_json(line: &str) -> Option<&str> {
    extract_agent_event_json_with_remainder(line).map(|(json, _)| json)
}

fn extract_agent_event_json_with_remainder(line: &str) -> Option<(&str, &str)> {
    let mut trimmed = line.trim();
    for prefix in ["[stdout] ", "[stderr] "] {
        if let Some(rest) = trimmed.strip_prefix(prefix) {
            trimmed = rest.trim_start();
            break;
        }
    }
    let payload = trimmed.strip_prefix(AGENT_EVENT_PREFIX)?.trim_start();
    match complete_json_object_end(payload) {
        Some(end) => Some((&payload[..end], &payload[end..])),
        None => Some((payload.trim(), "")),
    }
}

fn complete_json_object_end(value: &str) -> Option<usize> {
    if !value.starts_with('{') {
        return None;
    }

    let mut depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    for (index, ch) in value.char_indices() {
        if in_string {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }

        match ch {
            '"' => in_string = true,
            '{' => depth += 1,
            '}' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return Some(index + ch.len_utf8());
                }
            }
            _ => {}
        }
    }

    None
}

async fn handle_agent_event(
    pool: &SqlitePool,
    agent_id: Uuid,
    run_id: Uuid,
    event: AgentEvent,
) -> CommandResult<String> {
    match event {
        AgentEvent::Message {
            channel,
            channel_id,
            thread_root_id,
            body,
            as_task,
        } => {
            let channel_id = resolve_event_channel(pool, channel_id, channel.as_deref()).await?;
            let msg_id = insert_agent_message(
                pool,
                agent_id,
                channel_id,
                thread_root_id,
                body.trim(),
                as_task.unwrap_or(false),
            )
            .await?;
            record_agent_activity(
                pool,
                Some(agent_id),
                None,
                if as_task.unwrap_or(false) {
                    "task"
                } else {
                    "message"
                },
                if as_task.unwrap_or(false) {
                    "Task created from stdout event"
                } else {
                    "Message posted from stdout event"
                },
                format!("message_id={msg_id}"),
            )
            .await?;
            Ok(format!("message accepted {msg_id}"))
        }
        AgentEvent::ChannelMessageCreate {
            channel,
            channel_id,
            thread_root_id,
            body,
        } => {
            let channel_id = resolve_event_channel(pool, channel_id, channel.as_deref()).await?;
            ensure_agent_channel_member(pool, agent_id, channel_id, "channel_message_create")
                .await?;
            let body = body.trim();
            if body.is_empty() {
                return Err("channel_message_create body is required".to_owned());
            }
            let msg_id = insert_agent_message_with_options(
                pool,
                agent_id,
                channel_id,
                thread_root_id,
                body,
                false,
                false,
            )
            .await?;
            queue_mentions_as_work_items(
                pool,
                channel_id,
                thread_root_id,
                msg_id,
                None,
                body,
                MentionDispatchOrigin::Agent {
                    sender_agent_id: agent_id,
                    allow_channel_member_invite: true,
                },
            )
            .await?;
            record_agent_activity(
                pool,
                Some(agent_id),
                None,
                "message",
                "Channel message posted from control event",
                json!({
                    "message_id": msg_id,
                    "channel_id": channel_id,
                    "thread_root_id": thread_root_id
                })
                .to_string(),
            )
            .await?;
            Ok(format!("channel message accepted {msg_id}"))
        }
        AgentEvent::Activity {
            kind,
            title,
            detail,
        } => {
            let title = title.trim();
            if title.is_empty() {
                return Err("activity title is required".to_owned());
            }
            let kind = normalize_agent_activity_kind(kind.as_deref());
            record_agent_activity(
                pool,
                Some(agent_id),
                Some(run_id),
                kind,
                title,
                detail.unwrap_or_default(),
            )
            .await?;
            Ok("activity accepted".to_owned())
        }
        AgentEvent::TaskCreate {
            channel,
            channel_id,
            title,
            body,
            thread_body,
            assign_self,
            status,
        } => {
            let channel_id = resolve_event_channel(pool, channel_id, channel.as_deref()).await?;
            let (task_number, root_message_id, thread_reply_id) = create_agent_task_thread(
                pool,
                agent_id,
                channel_id,
                &title,
                body.as_deref(),
                thread_body.as_deref(),
                assign_self.unwrap_or(true),
                status.as_deref(),
            )
            .await?;
            record_agent_activity(
                pool,
                Some(agent_id),
                Some(run_id),
                "task",
                format!("Task #{task_number} created"),
                json!({
                    "task_number": task_number,
                    "message_id": root_message_id,
                    "thread_reply_id": thread_reply_id,
                })
                .to_string(),
            )
            .await?;
            Ok(format!("task #{task_number} created {root_message_id}"))
        }
        AgentEvent::TaskStatus {
            task_number,
            status,
        } => {
            let status = status.trim();
            if !matches!(status, "todo" | "in_progress" | "in_review" | "done") {
                return Err(format!("unsupported task status: {status}"));
            }
            let affected = sqlx::query(
                r#"
                update tasks
                set status = $2, version = version + 1, updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
                where number = $1
                "#,
            )
            .bind(task_number)
            .bind(status)
            .execute(pool)
            .await
            .map_err(to_string)?
            .rows_affected();
            if affected == 0 {
                return Err(format!("task #{task_number} does not exist"));
            }
            record_agent_activity(
                pool,
                Some(agent_id),
                None,
                "task",
                format!("Task #{task_number} status changed"),
                format!("status={status}"),
            )
            .await?;
            Ok(format!("task #{task_number} status set to {status}"))
        }
        AgentEvent::TaskClaim {
            task_number,
            assignee_handle,
        } => {
            let assignee = match assignee_handle.as_deref().map(str::trim) {
                Some("") | Some("null") | Some("unassigned") => None,
                Some(handle) => Some(resolve_agent_by_handle(pool, handle).await?),
                None => Some(agent_id),
            };
            let task_id: Option<Uuid> =
                sqlx::query_scalar("select id from tasks where number = $1")
                    .bind(task_number)
                    .fetch_optional(pool)
                    .await
                    .map_err(to_string)?;
            let Some(task_id) = task_id else {
                return Err(format!("task #{task_number} does not exist"));
            };
            if assignee.is_none() {
                let affected = sqlx::query(
                    r#"
                    update tasks
                    set assignee_agent_id = null,
                        version = version + 1,
                        updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
                    where id = $1
                      and assignee_agent_id = $2
                      and status <> 'done'
                    "#,
                )
                .bind(task_id)
                .bind(agent_id)
                .execute(pool)
                .await
                .map_err(to_string)?
                .rows_affected();
                if affected == 0 {
                    return Ok(format!("task #{task_number} unclaim ignored"));
                }
                record_agent_activity(
                    pool,
                    Some(agent_id),
                    None,
                    "task",
                    format!("Task #{task_number} unclaimed"),
                    "agent_claim",
                )
                .await?;
            } else if assignee != Some(agent_id) {
                return Err("task_claim can only claim for the current agent".to_owned());
            } else if try_claim_unassigned_task(pool, task_id, agent_id, None, "agent_claim")
                .await?
                .is_none()
            {
                return Ok(format!("task #{task_number} claim ignored"));
            }
            Ok(format!("task #{task_number} assignee updated"))
        }
        AgentEvent::TaskHandoff {
            target_agent,
            task_number,
            reason,
            body,
        } => {
            let (task_id, resolved_task_number, title, channel_id, thread_root_id) =
                resolve_task_for_handoff(pool, agent_id, run_id, task_number).await?;
            let target_agent_id = resolve_agent_by_handle(pool, &target_agent).await?;
            if target_agent_id == agent_id {
                return Err("task_handoff target_agent must be a different agent".to_owned());
            }
            let target_handle = resolve_agent_handle(pool, target_agent_id).await?;
            let source_handle = resolve_agent_handle(pool, agent_id).await?;
            let reason = reason.trim();
            if reason.is_empty() {
                return Err("task_handoff reason is required".to_owned());
            }
            let handoff_body = body
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
                .unwrap_or_else(|| {
                    format!("@{target_handle} taking over task #{resolved_task_number}: {reason}")
                });

            let affected = sqlx::query(
                r#"
                update tasks
                set assignee_agent_id = $2,
                    status = 'in_progress',
                    version = version + 1,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
                where id = $1
                  and assignee_agent_id = $3
                  and status <> 'done'
                "#,
            )
            .bind(task_id)
            .bind(target_agent_id)
            .bind(agent_id)
            .execute(pool)
            .await
            .map_err(to_string)?
            .rows_affected();
            if affected == 0 {
                return Err(format!(
                    "task #{resolved_task_number} can only be handed off by its current assignee"
                ));
            }

            let handoff_message_id = insert_agent_handoff_message(
                pool,
                agent_id,
                channel_id,
                thread_root_id,
                &handoff_body,
            )
            .await?;
            dispatch_task_assignment_to_agent(pool, task_id, target_agent_id, reason).await?;
            let _ = notify_ui_refresh(pool, "task_handoff").await;
            record_agent_activity(
                pool,
                Some(agent_id),
                Some(run_id),
                "task",
                format!("Task #{resolved_task_number} handed off"),
                json!({
                    "task_id": task_id,
                    "task_number": resolved_task_number,
                    "title": title,
                    "from": format!("@{source_handle}"),
                    "target_agent": format!("@{target_handle}"),
                    "reason": reason,
                    "message_id": handoff_message_id,
                })
                .to_string(),
            )
            .await?;
            Ok(format!(
                "task #{resolved_task_number} handed off to @{target_handle}"
            ))
        }
        AgentEvent::Silent { reason } => {
            let reason =
                reason.unwrap_or_else(|| "Agent judged no visible reply was needed.".to_owned());
            mark_run_work_item_silent(pool, agent_id, run_id, &reason).await?;
            Ok("silent reply accepted".to_owned())
        }
        AgentEvent::ReminderCreate {
            channel,
            channel_id,
            thread_root_id,
            message_id,
            title,
            note,
            due_at,
            recurrence,
        } => {
            let due_at = due_at.ok_or_else(|| {
                "reminder_create requires a when or due_at ISO8601 timestamp".to_owned()
            })?;
            let due_at = parse_due_at(&due_at)?;
            let (default_channel_id, default_thread_root_id, default_message_id) =
                resolve_run_reminder_anchor(pool, agent_id, run_id).await?;
            let resolved_channel_id = if channel_id.is_some() || channel.is_some() {
                Some(resolve_event_channel(pool, channel_id, channel.as_deref()).await?)
            } else {
                default_channel_id
            };
            let reminder_id = create_reminder_in_pool(
                pool,
                Some(agent_id),
                resolved_channel_id,
                thread_root_id.or(default_thread_root_id),
                message_id.or(default_message_id),
                &title,
                note.as_deref().unwrap_or(""),
                due_at,
                recurrence.as_deref().unwrap_or("none"),
            )
            .await?;
            record_agent_activity(
                pool,
                Some(agent_id),
                Some(run_id),
                "reminder",
                "Reminder scheduled",
                json!({
                    "reminder_id": reminder_id,
                    "title": title.trim(),
                    "due_at": due_at.to_rfc3339(),
                    "recurrence": recurrence.unwrap_or_else(|| "none".to_owned())
                })
                .to_string(),
            )
            .await?;
            Ok(format!("reminder created {reminder_id}"))
        }
        AgentEvent::ReminderCancel { reminder_id } => {
            cancel_reminder_in_pool(pool, reminder_id).await?;
            record_agent_activity(
                pool,
                Some(agent_id),
                Some(run_id),
                "reminder",
                "Reminder cancelled",
                json!({ "reminder_id": reminder_id }).to_string(),
            )
            .await?;
            Ok(format!("reminder cancelled {reminder_id}"))
        }
        AgentEvent::Usage {
            input_tokens,
            output_tokens,
            total_tokens,
            cost_micros,
            cost_usd,
        } => {
            let input_tokens = input_tokens.unwrap_or_default().max(0);
            let mut output_tokens = output_tokens.unwrap_or_default().max(0);
            if output_tokens == 0 {
                if let Some(total_tokens) = total_tokens {
                    output_tokens = (total_tokens - input_tokens).max(0);
                }
            }
            let event_cost_micros = cost_micros
                .or_else(|| cost_usd.map(|value| (value.max(0.0) * 1_000_000.0).round() as i64));
            record_run_usage(
                pool,
                agent_id,
                run_id,
                input_tokens,
                output_tokens,
                event_cost_micros,
            )
            .await?;
            record_agent_activity(
                pool,
                Some(agent_id),
                Some(run_id),
                "usage",
                "Usage recorded",
                json!({
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens,
                    "cost_micros": event_cost_micros
                })
                .to_string(),
            )
            .await?;
            Ok("usage accepted".to_owned())
        }
        AgentEvent::MemoryAppend { body } => {
            append_agent_memory(pool, agent_id, &body).await?;
            record_agent_activity(
                pool,
                Some(agent_id),
                Some(run_id),
                "memory",
                "Memory updated",
                json!({ "operation": "append" }).to_string(),
            )
            .await?;
            Ok("memory appended".to_owned())
        }
        AgentEvent::MemoryCompact { body } => {
            compact_agent_memory(pool, agent_id, &body).await?;
            record_agent_activity(
                pool,
                Some(agent_id),
                Some(run_id),
                "memory",
                "Memory compacted",
                json!({ "operation": "compact" }).to_string(),
            )
            .await?;
            Ok("memory compacted".to_owned())
        }
        AgentEvent::ChannelCreate {
            name,
            description,
            agent_handles,
        } => {
            let channel_id =
                create_channel_in_pool(pool, &name, description.as_deref().unwrap_or("")).await?;
            add_agent_to_channel(pool, channel_id, agent_id).await?;
            let mut invited = Vec::new();
            for handle in agent_handles.unwrap_or_default() {
                let invited_agent_id = resolve_agent_by_handle(pool, &handle).await?;
                add_agent_to_channel(pool, channel_id, invited_agent_id).await?;
                invited.push(handle.trim().trim_start_matches('@').to_owned());
            }
            insert_system_message(
                pool,
                channel_id,
                None,
                format!(
                    "@{} created #{}{}",
                    sqlx::query_scalar::<_, String>("select handle from agents where id = $1")
                        .bind(agent_id)
                        .fetch_one(pool)
                        .await
                        .map_err(to_string)?,
                    normalize_channel_name(&name),
                    if invited.is_empty() {
                        String::new()
                    } else {
                        format!(
                            " and invited {}",
                            invited
                                .iter()
                                .map(|h| format!("@{h}"))
                                .collect::<Vec<_>>()
                                .join(", ")
                        )
                    }
                ),
            )
            .await?;
            record_agent_activity(
                pool,
                Some(agent_id),
                Some(run_id),
                "channel",
                "Channel created",
                json!({
                    "channel_id": channel_id,
                    "name": normalize_channel_name(&name),
                    "invited": invited
                })
                .to_string(),
            )
            .await?;
            Ok(format!("channel created {channel_id}"))
        }
        AgentEvent::ChannelInvite {
            channel,
            channel_id,
            agent_handles,
        } => {
            if agent_handles.is_empty() {
                return Err("channel_invite requires agent_handles".to_owned());
            }
            let channel_id = resolve_event_channel(pool, channel_id, channel.as_deref()).await?;
            let mut invited = Vec::new();
            for handle in agent_handles {
                let invited_agent_id = resolve_agent_by_handle(pool, &handle).await?;
                add_agent_to_channel(pool, channel_id, invited_agent_id).await?;
                invited.push(handle.trim().trim_start_matches('@').to_owned());
            }
            record_agent_activity(
                pool,
                Some(agent_id),
                Some(run_id),
                "membership",
                "Agents invited",
                json!({
                    "channel_id": channel_id,
                    "invited": invited
                })
                .to_string(),
            )
            .await?;
            let _ = notify_ui_refresh(pool, "channel_invite").await;
            Ok("agents invited".to_owned())
        }
        AgentEvent::ProfileUpdate {
            display_name,
            role,
            avatar,
            description,
        } => {
            let display_name = display_name
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty());
            let role = role
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty());
            let avatar = avatar
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty());
            let description = description
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty());
            if display_name.is_none() && role.is_none() && avatar.is_none() && description.is_none()
            {
                return Err("profile_update requires at least one non-empty field".to_owned());
            }
            sqlx::query(
                r#"
                update agents
                set display_name = coalesce($2, display_name),
                    role = coalesce($3, role),
                    avatar = coalesce($4, avatar),
                    description = coalesce($5, description)
                where id = $1
                "#,
            )
            .bind(agent_id)
            .bind(display_name)
            .bind(role)
            .bind(avatar)
            .bind(description)
            .execute(pool)
            .await
            .map_err(to_string)?;
            record_agent_activity(
                pool,
                Some(agent_id),
                Some(run_id),
                "profile",
                "Profile updated",
                json!({
                    "display_name": display_name,
                    "role": role,
                    "avatar": avatar,
                    "description": description
                })
                .to_string(),
            )
            .await?;
            let _ = notify_ui_refresh(pool, "profile_update").await;
            Ok("profile updated".to_owned())
        }
        AgentEvent::ArtifactCreate {
            channel,
            channel_id,
            thread_root_id,
            kind,
            title,
            summary,
            content,
            metadata,
        } => {
            let channel_id = resolve_event_channel(pool, channel_id, channel.as_deref()).await?;
            let (artifact_id, message_id) = create_agent_artifact(
                pool,
                agent_id,
                channel_id,
                thread_root_id,
                &kind,
                &title,
                summary.as_deref(),
                &content,
                metadata,
            )
            .await?;
            record_agent_activity(
                pool,
                Some(agent_id),
                Some(run_id),
                "artifact",
                "Artifact created",
                json!({
                    "artifact_id": artifact_id,
                    "message_id": message_id,
                    "kind": kind,
                    "title": title
                })
                .to_string(),
            )
            .await?;
            Ok(format!("artifact created: {artifact_id}"))
        }
        AgentEvent::AttachmentCreate {
            channel,
            channel_id,
            thread_root_id,
            body,
            files,
        } => {
            let channel_id = resolve_event_channel(pool, channel_id, channel.as_deref()).await?;
            let uploads = load_agent_attachment_uploads(files)?;
            let upload_count = uploads.len();
            let body = body
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
                .unwrap_or_else(|| default_attachment_message_body(&uploads));
            let message_id = insert_agent_attachment_message(
                pool,
                agent_id,
                channel_id,
                thread_root_id,
                &body,
                uploads,
            )
            .await?;
            record_agent_activity(
                pool,
                Some(agent_id),
                Some(run_id),
                "attachment",
                "Attachment message created",
                json!({
                    "message_id": message_id,
                    "file_count": upload_count
                })
                .to_string(),
            )
            .await?;
            Ok(format!("attachment message created: {message_id}"))
        }
        AgentEvent::HandoffCreate {
            target_agent,
            channel,
            channel_id,
            thread_root_id,
            reason,
            body,
        } => {
            let channel_id = resolve_event_channel(pool, channel_id, channel.as_deref()).await?;
            let (target_agent_id, target_handle, work_item_id, handoff_message_id) =
                create_agent_handoff(
                    pool,
                    agent_id,
                    channel_id,
                    thread_root_id,
                    &target_agent,
                    reason.as_deref(),
                    &body,
                )
                .await?;
            record_agent_activity(
                pool,
                Some(agent_id),
                Some(run_id),
                "handoff",
                "Handoff created",
                json!({
                    "target_agent_id": target_agent_id,
                    "target_handle": target_handle,
                    "work_item_id": work_item_id,
                    "message_id": handoff_message_id,
                    "thread_root_id": thread_root_id
                })
                .to_string(),
            )
            .await?;
            Ok(format!(
                "handoff created for @{target_handle}: {work_item_id}"
            ))
        }
    }
}

async fn resolve_run_reminder_anchor(
    pool: &SqlitePool,
    agent_id: Uuid,
    run_id: Uuid,
) -> CommandResult<(Option<Uuid>, Option<Uuid>, Option<Uuid>)> {
    let row = sqlx::query(
        r#"
        select w.channel_id, w.thread_root_id, w.source_message_id
        from agent_runs r
        left join agent_work_items w on w.id = r.work_item_id
        where r.id = $1 and r.agent_id = $2
        "#,
    )
    .bind(run_id)
    .bind(agent_id)
    .fetch_optional(pool)
    .await
    .map_err(to_string)?;
    Ok(row
        .map(|row| {
            (
                row.get("channel_id"),
                row.get("thread_root_id"),
                row.get("source_message_id"),
            )
        })
        .unwrap_or((None, None, None)))
}

async fn resolve_event_channel(
    pool: &SqlitePool,
    channel_id: Option<Uuid>,
    channel_name: Option<&str>,
) -> CommandResult<Uuid> {
    if let Some(channel_id) = channel_id {
        let exists: Option<Uuid> = sqlx::query_scalar("select id from channels where id = $1")
            .bind(channel_id)
            .fetch_optional(pool)
            .await
            .map_err(to_string)?;
        return exists.ok_or_else(|| format!("channel {channel_id} does not exist"));
    }

    let Some(name) = channel_name else {
        return Err("message event requires channel or channel_id".to_owned());
    };
    let normalized = normalize_channel_name(name);
    if normalized.is_empty() {
        return Err("message event channel is empty".to_owned());
    }
    sqlx::query_scalar("select id from channels where name = $1")
        .bind(&normalized)
        .fetch_optional(pool)
        .await
        .map_err(to_string)?
        .ok_or_else(|| format!("channel #{normalized} does not exist"))
}

async fn resolve_task_for_handoff(
    pool: &SqlitePool,
    agent_id: Uuid,
    run_id: Uuid,
    task_number: Option<i64>,
) -> CommandResult<(Uuid, i64, String, Uuid, Uuid)> {
    let row = if let Some(task_number) = task_number {
        sqlx::query(
            r#"
            select id, number, title, channel_id, message_id, assignee_agent_id, status
            from tasks
            where number = $1
            "#,
        )
        .bind(task_number)
        .fetch_optional(pool)
        .await
        .map_err(to_string)?
    } else {
        sqlx::query(
            r#"
            select t.id, t.number, t.title, t.channel_id, t.message_id, t.assignee_agent_id, t.status
            from agent_work_items w
            join tasks t on t.id = w.task_id
            where w.run_id = $1 and w.agent_id = $2
            order by w.updated_at desc
            limit 1
            "#,
        )
        .bind(run_id)
        .bind(agent_id)
        .fetch_optional(pool)
        .await
        .map_err(to_string)?
    };
    let row = row.ok_or_else(|| {
        task_number
            .map(|task_number| format!("task #{task_number} does not exist"))
            .unwrap_or_else(|| {
                "task_handoff needs task_number when the current run is not tied to a task"
                    .to_owned()
            })
    })?;
    let resolved_task_number: i64 = row.get("number");
    let status: String = row.get("status");
    if status == "done" {
        return Err(format!("task #{resolved_task_number} is already done"));
    }
    let assignee_agent_id: Option<Uuid> = row.get("assignee_agent_id");
    if assignee_agent_id != Some(agent_id) {
        return Err(format!(
            "task #{resolved_task_number} can only be handed off by its current assignee"
        ));
    }
    Ok((
        row.get("id"),
        resolved_task_number,
        row.get("title"),
        row.get("channel_id"),
        row.get("message_id"),
    ))
}

async fn resolve_agent_by_handle(pool: &SqlitePool, handle: &str) -> CommandResult<Uuid> {
    let normalized = handle.trim().trim_start_matches('@');
    if normalized.is_empty() {
        return Err("assignee handle is empty".to_owned());
    }
    sqlx::query_scalar("select id from agents where lower(handle) = lower($1)")
        .bind(normalized)
        .fetch_optional(pool)
        .await
        .map_err(to_string)?
        .ok_or_else(|| format!("agent @{normalized} does not exist"))
}

async fn resolve_agent_handle(pool: &SqlitePool, agent_id: Uuid) -> CommandResult<String> {
    sqlx::query_scalar("select handle from agents where id = $1")
        .bind(agent_id)
        .fetch_one(pool)
        .await
        .map_err(to_string)
}

async fn ensure_agent_channel_member(
    pool: &SqlitePool,
    agent_id: Uuid,
    channel_id: Uuid,
    event_name: &str,
) -> CommandResult<()> {
    let is_member: bool = sqlx::query_scalar(
        r#"
        select exists (
            select 1 from channel_members
            where channel_id = $1 and agent_id = $2
        )
        "#,
    )
    .bind(channel_id)
    .bind(agent_id)
    .fetch_one(pool)
    .await
    .map_err(to_string)?;
    if is_member {
        Ok(())
    } else {
        Err(format!(
            "{event_name} requires source agent channel membership"
        ))
    }
}

async fn create_agent_handoff(
    pool: &SqlitePool,
    source_agent_id: Uuid,
    channel_id: Uuid,
    thread_root_id: Uuid,
    target_agent: &str,
    reason: Option<&str>,
    body: &str,
) -> CommandResult<(Uuid, String, Uuid, Uuid)> {
    let target_agent_id = resolve_agent_by_handle(pool, target_agent).await?;
    if target_agent_id == source_agent_id {
        return Err("handoff_create target_agent must be a different agent".to_owned());
    }
    let target_handle = resolve_agent_handle(pool, target_agent_id).await?;
    let source_handle = resolve_agent_handle(pool, source_agent_id).await?;
    let root_channel: Option<Uuid> = sqlx::query_scalar(
        "select channel_id from messages where id = $1 and thread_root_id is null",
    )
    .bind(thread_root_id)
    .fetch_optional(pool)
    .await
    .map_err(to_string)?;
    if root_channel != Some(channel_id) {
        return Err("thread_root_id does not belong to target channel".to_owned());
    }
    ensure_agent_channel_member(pool, source_agent_id, channel_id, "handoff_create").await?;

    let request_body = body.trim();
    if request_body.is_empty() {
        return Err("handoff_create body is required".to_owned());
    }
    let reason = reason
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("Agent handoff requested");
    let handoff_message_id = insert_agent_handoff_message(
        pool,
        source_agent_id,
        channel_id,
        thread_root_id,
        request_body,
    )
    .await?;

    sqlx::query(
        r#"
        insert into channel_members (channel_id, agent_id)
        values ($1, $2)
        on conflict (channel_id, agent_id) do nothing
        "#,
    )
    .bind(channel_id)
    .bind(target_agent_id)
    .execute(pool)
    .await
    .map_err(to_string)?;
    upsert_agent_thread_subscription(
        pool,
        target_agent_id,
        channel_id,
        thread_root_id,
        "handoff",
        Some(handoff_message_id),
    )
    .await?;
    let title = request_body
        .lines()
        .next()
        .map(|line| line.chars().take(120).collect::<String>())
        .filter(|line| !line.trim().is_empty())
        .unwrap_or_else(|| format!("Handoff from @{source_handle}"));
    let inbox_item_id = create_agent_inbox_item(
        pool,
        AgentInboxItemInput {
            agent_id: target_agent_id,
            channel_id: Some(channel_id),
            thread_root_id: Some(thread_root_id),
            source_message_id: Some(handoff_message_id),
            task_id: None,
            kind: "handoff",
            priority: 80,
            title: &title,
            body_preview: request_body,
            payload: json!({"source_agent_id": source_agent_id, "reason": reason}),
        },
    )
    .await?;
    let wake = ensure_agent_inbox_wake_work_item(pool, target_agent_id).await?;
    let Some((work_item_id, scheduled)) = wake else {
        return Err("handoff inbox item was not wakeable".to_owned());
    };
    record_agent_activity(
        pool,
        Some(target_agent_id),
        None,
        "handoff",
        if scheduled {
            "Handoff dispatched"
        } else {
            "Handoff queued"
        },
        json!({
            "from": source_handle,
            "reason": reason,
            "inbox_item_id": inbox_item_id,
            "work_item_id": work_item_id,
            "message_id": handoff_message_id,
            "thread_root_id": thread_root_id
        })
        .to_string(),
    )
    .await?;
    Ok((
        target_agent_id,
        target_handle,
        work_item_id,
        handoff_message_id,
    ))
}

fn infer_attachment_mime_type(path: &Path, original_name: &str) -> String {
    let extension = Path::new(original_name)
        .extension()
        .or_else(|| path.extension())
        .and_then(|value| value.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    match extension.as_str() {
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "svg" => "image/svg+xml",
        "txt" => "text/plain",
        "md" | "markdown" => "text/markdown",
        "json" => "application/json",
        "csv" => "text/csv",
        "html" | "htm" => "text/html",
        "pdf" => "application/pdf",
        _ => "application/octet-stream",
    }
    .to_owned()
}

fn load_agent_attachment_uploads(
    files: Vec<AgentAttachmentFile>,
) -> CommandResult<Vec<AttachmentUpload>> {
    if files.is_empty() {
        return Err("attachment_create requires at least one file".to_owned());
    }
    let mut uploads = Vec::with_capacity(files.len());
    for file in files {
        let raw_path = file.path.trim();
        if raw_path.is_empty() {
            return Err("attachment_create file path is empty".to_owned());
        }
        let path = PathBuf::from(raw_path);
        let metadata = fs::metadata(&path)
            .map_err(|err| format!("cannot read attachment file {}: {err}", path.display()))?;
        if !metadata.is_file() {
            return Err(format!("attachment path is not a file: {}", path.display()));
        }
        if metadata.len() > ATTACHMENT_SIZE_LIMIT as u64 {
            return Err(format!(
                "attachment file {} is larger than 25MB",
                path.display()
            ));
        }
        let bytes = fs::read(&path)
            .map_err(|err| format!("cannot read attachment file {}: {err}", path.display()))?;
        if bytes.is_empty() {
            return Err(format!("attachment file is empty: {}", path.display()));
        }
        let original_name = file
            .name
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
            .or_else(|| {
                path.file_name()
                    .and_then(|value| value.to_str())
                    .map(str::to_owned)
            })
            .unwrap_or_else(|| "attachment".to_owned());
        let mime_type = file
            .mime_type
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
            .unwrap_or_else(|| infer_attachment_mime_type(&path, &original_name));
        uploads.push(AttachmentUpload {
            original_name,
            mime_type,
            bytes,
        });
    }
    Ok(uploads)
}

fn default_attachment_message_body(uploads: &[AttachmentUpload]) -> String {
    if uploads.len() == 1 {
        format!("Attached file: {}", uploads[0].original_name.trim())
    } else {
        format!("Attached {} files.", uploads.len())
    }
}

async fn insert_agent_message(
    pool: &SqlitePool,
    agent_id: Uuid,
    channel_id: Uuid,
    thread_root_id: Option<Uuid>,
    body: &str,
    as_task: bool,
) -> CommandResult<Uuid> {
    insert_agent_message_with_options(
        pool,
        agent_id,
        channel_id,
        thread_root_id,
        body,
        as_task,
        true,
    )
    .await
}

async fn insert_agent_handoff_message(
    pool: &SqlitePool,
    agent_id: Uuid,
    channel_id: Uuid,
    thread_root_id: Uuid,
    body: &str,
) -> CommandResult<Uuid> {
    insert_agent_message_with_options(
        pool,
        agent_id,
        channel_id,
        Some(thread_root_id),
        body,
        false,
        false,
    )
    .await
}

async fn insert_agent_message_with_options(
    pool: &SqlitePool,
    agent_id: Uuid,
    channel_id: Uuid,
    thread_root_id: Option<Uuid>,
    body: &str,
    as_task: bool,
    dispatch_mentions: bool,
) -> CommandResult<Uuid> {
    if body.is_empty() {
        return Err("message event body is empty".to_owned());
    }
    if as_task && thread_root_id.is_some() {
        return Err("task message events must be root messages".to_owned());
    }
    if as_task {
        let channel_kind: Option<String> =
            sqlx::query_scalar("select kind from channels where id = $1")
                .bind(channel_id)
                .fetch_optional(pool)
                .await
                .map_err(to_string)?;
        if channel_kind.as_deref() == Some("dm") {
            return Err("direct messages do not support tasks".to_owned());
        }
    }
    if let Some(thread_root_id) = thread_root_id {
        let root_channel: Option<Uuid> = sqlx::query_scalar(
            "select channel_id from messages where id = $1 and thread_root_id is null",
        )
        .bind(thread_root_id)
        .fetch_optional(pool)
        .await
        .map_err(to_string)?;
        if root_channel != Some(channel_id) {
            return Err("thread_root_id does not belong to target channel".to_owned());
        }
    }

    let sender = sqlx::query("select display_name, role from agents where id = $1")
        .bind(agent_id)
        .fetch_one(pool)
        .await
        .map_err(to_string)?;
    let sender_name: String = sender.get("display_name");
    let sender_role: String = sender.get("role");

    let mut tx = pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .map_err(to_string)?;
    let msg_id: Uuid = sqlx::query_scalar(
        r#"
        insert into messages (
            channel_id, thread_root_id, sender_agent_id, sender_name, sender_role, body, is_task
        )
        values ($1, $2, $3, $4, $5, $6, $7)
        returning id
        "#,
    )
    .bind(channel_id)
    .bind(thread_root_id)
    .bind(agent_id)
    .bind(sender_name)
    .bind(sender_role)
    .bind(body)
    .bind(as_task)
    .fetch_one(&mut *tx)
    .await
    .map_err(to_string)?;

    if as_task {
        sqlx::query(
            r#"
            insert into tasks (message_id, channel_id, title, status, assignee_agent_id)
            values ($1, $2, $3, 'todo', $4)
            "#,
        )
        .bind(msg_id)
        .bind(channel_id)
        .bind(body.lines().next().unwrap_or("Untitled task"))
        .bind(agent_id)
        .execute(&mut *tx)
        .await
        .map_err(to_string)?;
    }

    tx.commit().await.map_err(to_string)?;
    let conversation_thread_root_id = thread_root_id.unwrap_or(msg_id);
    upsert_agent_thread_subscription(
        pool,
        agent_id,
        channel_id,
        conversation_thread_root_id,
        if as_task {
            "task_message"
        } else {
            "agent_message"
        },
        Some(msg_id),
    )
    .await?;
    if !as_task && dispatch_mentions {
        queue_agent_message_mentions(pool, msg_id).await?;
    }
    let _ = notify_ui_refresh(pool, "message").await;
    Ok(msg_id)
}

async fn insert_agent_attachment_message(
    pool: &SqlitePool,
    agent_id: Uuid,
    channel_id: Uuid,
    thread_root_id: Option<Uuid>,
    body: &str,
    attachments: Vec<AttachmentUpload>,
) -> CommandResult<Uuid> {
    if attachments.is_empty() {
        return Err("attachment_create requires at least one file".to_owned());
    }
    let body = body.trim();
    if body.is_empty() {
        return Err("attachment_create body is empty".to_owned());
    }
    if let Some(thread_root_id) = thread_root_id {
        let root_channel: Option<Uuid> = sqlx::query_scalar(
            "select channel_id from messages where id = $1 and thread_root_id is null",
        )
        .bind(thread_root_id)
        .fetch_optional(pool)
        .await
        .map_err(to_string)?;
        if root_channel != Some(channel_id) {
            return Err("thread_root_id does not belong to target channel".to_owned());
        }
    }

    let sender = sqlx::query("select display_name, role from agents where id = $1")
        .bind(agent_id)
        .fetch_one(pool)
        .await
        .map_err(to_string)?;
    let sender_name: String = sender.get("display_name");
    let sender_role: String = sender.get("role");

    let mut tx = pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .map_err(to_string)?;
    let msg_id: Uuid = sqlx::query_scalar(
        r#"
        insert into messages (
            channel_id, thread_root_id, sender_agent_id, sender_name, sender_role, body, is_task
        )
        values ($1, $2, $3, $4, $5, $6, false)
        returning id
        "#,
    )
    .bind(channel_id)
    .bind(thread_root_id)
    .bind(agent_id)
    .bind(sender_name)
    .bind(sender_role)
    .bind(body)
    .fetch_one(&mut *tx)
    .await
    .map_err(to_string)?;

    let inserted = insert_message_attachments_tx(&mut tx, msg_id, attachments).await?;
    if inserted == 0 {
        return Err("attachment_create produced no attachments".to_owned());
    }
    tx.commit().await.map_err(to_string)?;

    let conversation_thread_root_id = thread_root_id.unwrap_or(msg_id);
    upsert_agent_thread_subscription(
        pool,
        agent_id,
        channel_id,
        conversation_thread_root_id,
        "agent_attachment_message",
        Some(msg_id),
    )
    .await?;
    queue_agent_message_mentions(pool, msg_id).await?;
    if let Ok(message) = load_message(pool, msg_id).await {
        let _ = notify_ui_message_upsert(pool, &message, "attachment_created").await;
    } else {
        let _ = notify_ui_refresh(pool, "attachment_created").await;
    }
    Ok(msg_id)
}

async fn create_agent_task_thread(
    pool: &SqlitePool,
    agent_id: Uuid,
    channel_id: Uuid,
    title: &str,
    body: Option<&str>,
    thread_body: Option<&str>,
    assign_self: bool,
    status: Option<&str>,
) -> CommandResult<(i64, Uuid, Option<Uuid>)> {
    let title = title.trim();
    if title.is_empty() {
        return Err("task_create title is required".to_owned());
    }
    let final_status = status
        .map(str::trim)
        .filter(|status| !status.is_empty())
        .unwrap_or(if assign_self { "in_progress" } else { "todo" });
    if !matches!(final_status, "todo" | "in_progress" | "in_review" | "done") {
        return Err(format!("unsupported task status: {final_status}"));
    }
    let root_body = body
        .map(str::trim)
        .filter(|body| !body.is_empty())
        .unwrap_or(title);
    let root_message_id =
        insert_agent_message(pool, agent_id, channel_id, None, root_body, true).await?;
    let task_row = sqlx::query(
        r#"
        update tasks
        set title = $2,
            status = $3,
            assignee_agent_id = case when $4 then $5 else null end,
            version = version + 1,
            updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
        where message_id = $1
        returning number
        "#,
    )
    .bind(root_message_id)
    .bind(title)
    .bind(final_status)
    .bind(assign_self)
    .bind(agent_id)
    .fetch_one(pool)
    .await
    .map_err(to_string)?;
    let task_number: i64 = task_row.get("number");
    let thread_reply_id = match thread_body.map(str::trim).filter(|body| !body.is_empty()) {
        Some(thread_body) => Some(
            insert_agent_message(
                pool,
                agent_id,
                channel_id,
                Some(root_message_id),
                thread_body,
                false,
            )
            .await?,
        ),
        None => None,
    };
    let _ = notify_ui_refresh(pool, "task_create").await;
    Ok((task_number, root_message_id, thread_reply_id))
}

fn normalize_artifact_kind(kind: &str) -> CommandResult<String> {
    let normalized = kind.trim().to_lowercase().replace('_', "-");
    let normalized = match normalized.as_str() {
        "md" | "markdown" => "markdown",
        other => {
            return Err(format!(
                "unsupported artifact kind: {other}; supported: markdown"
            ))
        }
    };
    Ok(normalized.to_owned())
}

async fn create_agent_artifact(
    pool: &SqlitePool,
    agent_id: Uuid,
    channel_id: Uuid,
    thread_root_id: Option<Uuid>,
    kind: &str,
    title: &str,
    summary: Option<&str>,
    content: &str,
    metadata: Option<Value>,
) -> CommandResult<(Uuid, Uuid)> {
    let kind = normalize_artifact_kind(kind)?;
    let title = title.trim();
    if title.is_empty() {
        return Err("artifact_create title is required".to_owned());
    }
    let content = content.trim();
    if content.is_empty() {
        return Err("artifact_create content is required".to_owned());
    }
    let summary = summary
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| content.lines().next().unwrap_or(""))
        .to_owned();
    let summary = compact_chars_middle(&summary, 320).replace('\n', " ");
    let body = if summary.is_empty() {
        format!("Created artifact: {title}")
    } else {
        format!("Created artifact: {title}\n\n{summary}")
    };
    let message_id =
        insert_agent_message(pool, agent_id, channel_id, thread_root_id, &body, false).await?;
    let artifact_id: Uuid = sqlx::query_scalar(
        r#"
        insert into artifacts (
            message_id, channel_id, thread_root_id, creator_agent_id,
            kind, title, summary, content, metadata
        )
        values ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        returning id
        "#,
    )
    .bind(message_id)
    .bind(channel_id)
    .bind(thread_root_id)
    .bind(agent_id)
    .bind(&kind)
    .bind(title)
    .bind(&summary)
    .bind(content)
    .bind(metadata.unwrap_or_else(|| json!({})))
    .fetch_one(pool)
    .await
    .map_err(to_string)?;
    if let Ok(artifact) = load_artifact(pool, artifact_id).await {
        let _ = notify_ui_artifact_upsert(pool, &artifact, "artifact_created").await;
    }
    if let Ok(message) = load_message(pool, message_id).await {
        let _ = notify_ui_message_upsert(pool, &message, "artifact_created").await;
    } else {
        let _ = notify_ui_refresh(pool, "artifact_created").await;
    }
    Ok((artifact_id, message_id))
}

fn capped_stream_delta(delta: &str, current_len: usize) -> (String, bool) {
    if current_len >= STREAMING_MESSAGE_BODY_LIMIT {
        return (String::new(), true);
    }
    let remaining = STREAMING_MESSAGE_BODY_LIMIT - current_len;
    let delta_len = delta.chars().count();
    if delta_len <= remaining {
        return (delta.to_owned(), false);
    }

    let marker_len = STREAMING_TRUNCATION_MARKER.chars().count();
    let keep = remaining.saturating_sub(marker_len);
    let mut capped: String = delta.chars().take(keep).collect();
    if remaining >= marker_len {
        capped.push_str(STREAMING_TRUNCATION_MARKER);
    }
    (capped, true)
}

async fn append_streaming_agent_message(
    pool: &SqlitePool,
    agent_id: Uuid,
    channel_id: Uuid,
    thread_root_id: Option<Uuid>,
    stream_key: &str,
    delta: &str,
) -> CommandResult<Uuid> {
    if stream_key.trim().is_empty() {
        return Err("stream_key is empty".to_owned());
    }
    if delta.is_empty() {
        return ensure_streaming_agent_message(
            pool,
            agent_id,
            channel_id,
            thread_root_id,
            stream_key,
        )
        .await;
    }

    if let Some(row) = sqlx::query(
        "select id, delivery_state, length(body) as body_len from messages where stream_key = $1",
    )
    .bind(stream_key)
    .fetch_optional(pool)
    .await
    .map_err(to_string)?
    {
        let message_id: Uuid = row.get("id");
        let delivery_state: String = row.get("delivery_state");
        if delivery_state != "streaming" {
            return Ok(message_id);
        }
        let body_len: i32 = row.get("body_len");
        let (append_delta, truncated) = capped_stream_delta(delta, body_len.max(0) as usize);
        if append_delta.is_empty() && truncated {
            finish_streaming_agent_message(pool, stream_key, "complete").await?;
            return Ok(message_id);
        }
        sqlx::query("update messages set body = body || $2, delivery_state = $3 where id = $1")
            .bind(message_id)
            .bind(&append_delta)
            .bind(if truncated { "complete" } else { "streaming" })
            .execute(pool)
            .await
            .map_err(to_string)?;
        let delivery_state = if truncated { "complete" } else { "streaming" };
        let _ = notify_ui_message_delta(
            pool,
            message_id,
            &append_delta,
            delivery_state,
            "stream_delta",
        )
        .await;
        if let Some((control_agent_id, run_id, _)) =
            load_streaming_control_context(pool, stream_key).await?
        {
            let _ = consume_complete_streaming_agent_control_lines(
                pool,
                control_agent_id,
                run_id,
                stream_key,
            )
            .await;
        }
        if truncated {
            queue_agent_message_mentions(pool, message_id).await?;
        }
        return Ok(message_id);
    }

    delete_superseded_empty_run_progress_messages(
        pool,
        agent_id,
        channel_id,
        thread_root_id,
        stream_key,
    )
    .await?;

    let sender = sqlx::query("select display_name, role from agents where id = $1")
        .bind(agent_id)
        .fetch_one(pool)
        .await
        .map_err(to_string)?;
    let sender_name: String = sender.get("display_name");
    let sender_role: String = sender.get("role");
    let (initial_body, truncated) = capped_stream_delta(delta, 0);

    let message_id: Uuid = sqlx::query_scalar(
        r#"
        insert into messages (
            channel_id,
            thread_root_id,
            sender_agent_id,
            sender_name,
            sender_role,
            body,
            is_task,
            delivery_state,
            stream_key
        )
        values ($1, $2, $3, $4, $5, $6, false, $7, $8)
        returning id
        "#,
    )
    .bind(channel_id)
    .bind(thread_root_id)
    .bind(agent_id)
    .bind(sender_name)
    .bind(sender_role)
    .bind(initial_body)
    .bind(if truncated { "complete" } else { "streaming" })
    .bind(stream_key)
    .fetch_one(pool)
    .await
    .map_err(to_string)?;

    if let Ok(message) = load_message(pool, message_id).await {
        let _ = notify_ui_message_upsert(pool, &message, "stream_start").await;
    } else {
        let _ = notify_ui_refresh(pool, "stream_start").await;
    }
    if let Some((control_agent_id, run_id, _)) =
        load_streaming_control_context(pool, stream_key).await?
    {
        let _ = consume_complete_streaming_agent_control_lines(
            pool,
            control_agent_id,
            run_id,
            stream_key,
        )
        .await;
    }
    if truncated {
        queue_agent_message_mentions(pool, message_id).await?;
    }
    Ok(message_id)
}

async fn ensure_streaming_agent_message(
    pool: &SqlitePool,
    agent_id: Uuid,
    channel_id: Uuid,
    thread_root_id: Option<Uuid>,
    stream_key: &str,
) -> CommandResult<Uuid> {
    if stream_key.trim().is_empty() {
        return Err("stream_key is empty".to_owned());
    }

    if let Some(existing) = sqlx::query_scalar("select id from messages where stream_key = $1")
        .bind(stream_key)
        .fetch_optional(pool)
        .await
        .map_err(to_string)?
    {
        return Ok(existing);
    }

    let sender = sqlx::query("select display_name, role from agents where id = $1")
        .bind(agent_id)
        .fetch_one(pool)
        .await
        .map_err(to_string)?;
    let sender_name: String = sender.get("display_name");
    let sender_role: String = sender.get("role");
    let message_id: Uuid = sqlx::query_scalar(
        r#"
        insert into messages (
            channel_id,
            thread_root_id,
            sender_agent_id,
            sender_name,
            sender_role,
            body,
            is_task,
            delivery_state,
            stream_key
        )
        values ($1, $2, $3, $4, $5, '', false, 'streaming', $6)
        returning id
        "#,
    )
    .bind(channel_id)
    .bind(thread_root_id)
    .bind(agent_id)
    .bind(sender_name)
    .bind(sender_role)
    .bind(stream_key)
    .fetch_one(pool)
    .await
    .map_err(to_string)?;

    if let Ok(message) = load_message(pool, message_id).await {
        let _ = notify_ui_message_upsert(pool, &message, "stream_placeholder").await;
    } else {
        let _ = notify_ui_refresh(pool, "stream_placeholder").await;
    }
    Ok(message_id)
}

async fn adopt_streaming_agent_message_key(
    pool: &SqlitePool,
    pending_stream_key: &str,
    stream_key: &str,
) -> CommandResult<Option<Uuid>> {
    if pending_stream_key == stream_key {
        return Ok(None);
    }
    if streaming_message_exists(pool, stream_key).await? {
        return Ok(None);
    }

    let message_id: Option<Uuid> = sqlx::query_scalar(
        r#"
        update messages
        set stream_key = $2,
            updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
        where stream_key = $1
          and delivery_state = 'streaming'
          and body = ''
        returning id
        "#,
    )
    .bind(pending_stream_key)
    .bind(stream_key)
    .fetch_optional(pool)
    .await
    .map_err(to_string)?;

    if let Some(message_id) = message_id {
        if let Ok(message) = load_message(pool, message_id).await {
            let _ = notify_ui_message_upsert(pool, &message, "stream_key_adopted").await;
        } else {
            let _ = notify_ui_refresh(pool, "stream_key_adopted").await;
        }
    }
    Ok(message_id)
}

async fn streaming_message_body_is_empty(
    pool: &SqlitePool,
    stream_key: &str,
) -> CommandResult<bool> {
    let body: Option<String> =
        sqlx::query_scalar("select body from messages where stream_key = $1")
            .bind(stream_key)
            .fetch_optional(pool)
            .await
            .map_err(to_string)?;
    Ok(body.map(|body| body.is_empty()).unwrap_or(true))
}

async fn delete_streaming_agent_message(
    pool: &SqlitePool,
    message_id: Uuid,
    reason: &str,
) -> CommandResult<()> {
    sqlx::query("delete from messages where id = $1")
        .bind(message_id)
        .execute(pool)
        .await
        .map_err(to_string)?;
    let _ = notify_ui_message_delete(pool, message_id, reason).await;
    Ok(())
}

async fn delete_superseded_empty_run_progress_messages(
    pool: &SqlitePool,
    agent_id: Uuid,
    channel_id: Uuid,
    thread_root_id: Option<Uuid>,
    stream_key: &str,
) -> CommandResult<()> {
    let Some(run_prefix) = stream_key
        .split(':')
        .next()
        .filter(|value| !value.is_empty())
    else {
        return Ok(());
    };
    if Uuid::parse_str(run_prefix).is_err() {
        return Ok(());
    }

    let superseded_ids: Vec<Uuid> = sqlx::query_scalar(
        r#"
        select id
        from messages
        where sender_agent_id = $1
          and channel_id = $2
          and thread_root_id is not distinct from $3
          and stream_key <> $4
          and stream_key like $5
          and body = ''
          and delivery_state in ('streaming', 'complete')
        "#,
    )
    .bind(agent_id)
    .bind(channel_id)
    .bind(thread_root_id)
    .bind(stream_key)
    .bind(format!("{run_prefix}:%"))
    .fetch_all(pool)
    .await
    .map_err(to_string)?;

    for message_id in superseded_ids {
        delete_streaming_agent_message(pool, message_id, "superseded_progress_status").await?;
    }
    Ok(())
}

async fn finish_streaming_agent_message(
    pool: &SqlitePool,
    stream_key: &str,
    delivery_state: &str,
) -> CommandResult<()> {
    if delivery_state == "complete" {
        if let Some((agent_id, run_id, work_item_id)) =
            load_streaming_control_context(pool, stream_key).await?
        {
            if consume_streaming_agent_control_lines(
                pool,
                agent_id,
                run_id,
                work_item_id,
                stream_key,
            )
            .await?
            {
                return Ok(());
            }
        }
    }

    let affected = sqlx::query(
        r#"
        update messages
        set delivery_state = $2
        where stream_key = $1
          and delivery_state = 'streaming'
        "#,
    )
    .bind(stream_key)
    .bind(delivery_state)
    .execute(pool)
    .await
    .map_err(to_string)?
    .rows_affected();
    if affected > 0 {
        let message_id: Option<Uuid> =
            sqlx::query_scalar("select id from messages where stream_key = $1")
                .bind(stream_key)
                .fetch_optional(pool)
                .await
                .map_err(to_string)?;
        if let Some(message_id) = message_id {
            if let Ok(message) = load_message(pool, message_id).await {
                let _ = notify_ui_message_upsert(pool, &message, "stream_finish").await;
            } else {
                let _ = notify_ui_refresh(pool, "stream_finish").await;
            }
            if delivery_state == "complete" {
                queue_agent_message_mentions(pool, message_id).await?;
            }
        }
    }
    Ok(())
}

async fn load_streaming_control_context(
    pool: &SqlitePool,
    stream_key: &str,
) -> CommandResult<Option<(Uuid, Uuid, Option<Uuid>)>> {
    let Some(run_prefix) = stream_key
        .split(':')
        .next()
        .filter(|value| !value.is_empty())
    else {
        return Ok(None);
    };
    let Ok(run_id) = Uuid::parse_str(run_prefix) else {
        return Ok(None);
    };
    let Some(row) = sqlx::query("select agent_id, work_item_id from agent_runs where id = $1")
        .bind(run_id)
        .fetch_optional(pool)
        .await
        .map_err(to_string)?
    else {
        return Ok(None);
    };
    let agent_id: Uuid = row.get("agent_id");
    let work_item_id: Option<Uuid> = row.get("work_item_id");
    Ok(Some((agent_id, run_id, work_item_id)))
}

fn silent_reply_reason(body: &str) -> Option<String> {
    let first_line = body.trim().lines().next()?.trim().trim_matches('`').trim();
    let rest = first_line.strip_prefix(SILENT_REPLY_PREFIX)?;
    if !rest.is_empty()
        && !rest.starts_with(':')
        && !rest
            .chars()
            .next()
            .map(char::is_whitespace)
            .unwrap_or(false)
    {
        return None;
    }
    let reason = rest.trim_start_matches(':').trim();
    Some(reason.to_owned())
}

async fn mark_work_item_silent(
    pool: &SqlitePool,
    agent_id: Uuid,
    run_id: Uuid,
    work_item_id: Uuid,
    reason: &str,
) -> CommandResult<()> {
    sqlx::query(
        r#"
        update agent_work_items
        set status = 'silent',
            completed_at = coalesce(completed_at, strftime('%Y-%m-%dT%H:%M:%f+00:00','now')),
            updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
        where id = $1
          and status not in ('cancelled', 'failed')
        "#,
    )
    .bind(work_item_id)
    .execute(pool)
    .await
    .map_err(to_string)?;
    notify_ui_work_item_changed(pool, work_item_id, "work_item_silent").await;
    record_agent_activity(
        pool,
        Some(agent_id),
        Some(run_id),
        "decision",
        "No visible reply needed",
        json!({
            "work_item_id": work_item_id,
            "reason": if reason.trim().is_empty() {
                "Agent judged the message as non-actionable."
            } else {
                reason.trim()
            }
        })
        .to_string(),
    )
    .await?;
    Ok(())
}

async fn mark_run_work_item_silent(
    pool: &SqlitePool,
    agent_id: Uuid,
    run_id: Uuid,
    reason: &str,
) -> CommandResult<()> {
    let work_item_id: Option<Uuid> =
        sqlx::query_scalar("select work_item_id from agent_runs where id = $1 and agent_id = $2")
            .bind(run_id)
            .bind(agent_id)
            .fetch_optional(pool)
            .await
            .map_err(to_string)?
            .flatten();
    if let Some(work_item_id) = work_item_id {
        mark_work_item_silent(pool, agent_id, run_id, work_item_id, reason).await?;
    } else {
        record_agent_activity(
            pool,
            Some(agent_id),
            Some(run_id),
            "decision",
            "No visible reply needed",
            reason.trim(),
        )
        .await?;
    }
    Ok(())
}

async fn maybe_hide_silent_streaming_reply(
    pool: &SqlitePool,
    agent_id: Uuid,
    run_id: Uuid,
    work_item_id: Option<Uuid>,
    stream_key: &str,
) -> CommandResult<bool> {
    let Some(row) = sqlx::query("select id, body from messages where stream_key = $1")
        .bind(stream_key)
        .fetch_optional(pool)
        .await
        .map_err(to_string)?
    else {
        return Ok(false);
    };
    let message_id: Uuid = row.get("id");
    let body: String = row.get("body");
    let Some(reason) = silent_reply_reason(&body) else {
        return Ok(false);
    };

    delete_streaming_agent_message(pool, message_id, "silent_reply").await?;
    if let Some(work_item_id) = work_item_id {
        mark_work_item_silent(pool, agent_id, run_id, work_item_id, &reason).await?;
    } else {
        record_agent_activity(
            pool,
            Some(agent_id),
            Some(run_id),
            "decision",
            "No visible reply needed",
            reason.trim(),
        )
        .await?;
    }
    Ok(true)
}

fn split_streaming_agent_event_lines(body: &str) -> (String, Vec<String>) {
    let mut visible_lines = Vec::new();
    let mut events = Vec::new();
    for line in body.lines() {
        if let Some((json, remainder)) = extract_agent_event_json_with_remainder(line) {
            events.push(json.to_owned());
            if !remainder.trim().is_empty() {
                visible_lines.push(remainder);
            }
        } else {
            visible_lines.push(line);
        }
    }
    (visible_lines.join("\n").trim().to_owned(), events)
}

fn split_complete_streaming_agent_event_lines(body: &str) -> (String, Vec<String>) {
    let mut visible = String::new();
    let mut events = Vec::new();
    for segment in body.split_inclusive('\n') {
        if !segment.ends_with('\n') {
            visible.push_str(segment);
            continue;
        }
        let line = segment.trim_end_matches(['\r', '\n']);
        if let Some((json, remainder)) = extract_agent_event_json_with_remainder(line) {
            events.push(json.to_owned());
            if !remainder.trim().is_empty() {
                visible.push_str(remainder);
                visible.push('\n');
            }
        } else {
            visible.push_str(segment);
        }
    }
    (visible.trim().to_owned(), events)
}

fn control_event_creates_visible_chat_message(json: &str) -> bool {
    let Ok(value) = serde_json::from_str::<Value>(json) else {
        return false;
    };
    match value.get("type").and_then(Value::as_str) {
        Some(
            "message"
            | "channel_message_create"
            | "task_create"
            | "task_handoff"
            | "attachment_create"
            | "handoff_create",
        ) => true,
        Some("artifact_create") => {
            let kind_supported = value
                .get("kind")
                .and_then(Value::as_str)
                .is_some_and(|kind| normalize_artifact_kind(kind).is_ok());
            let has_title = value
                .get("title")
                .and_then(Value::as_str)
                .is_some_and(|title| !title.trim().is_empty());
            let has_content = value
                .get("content")
                .and_then(Value::as_str)
                .is_some_and(|content| !content.trim().is_empty());
            kind_supported && has_title && has_content
        }
        _ => false,
    }
}

fn control_event_hides_empty_streaming_reply(json: &str) -> bool {
    let Ok(value) = serde_json::from_str::<Value>(json) else {
        return false;
    };
    value.get("type").and_then(Value::as_str) == Some("silent")
        || control_event_creates_visible_chat_message(json)
}

async fn handle_streaming_agent_event_json(
    pool: &SqlitePool,
    agent_id: Uuid,
    run_id: Uuid,
    json: &str,
) -> CommandResult<()> {
    match serde_json::from_str::<AgentEvent>(json).map_err(to_string) {
        Ok(event) => {
            if !claim_agent_event(pool, run_id, json).await? {
                return Ok(());
            }
            match handle_agent_event(pool, agent_id, run_id, event).await {
                Ok(note) => {
                    append_run_log(pool, run_id, format!("[stream-event] {note}\n")).await?;
                    record_agent_activity(
                        pool,
                        Some(agent_id),
                        Some(run_id),
                        "event",
                        "Stream event accepted",
                        note,
                    )
                    .await?;
                }
                Err(err) => {
                    append_run_log(pool, run_id, format!("[stream-event] rejected: {err}\n"))
                        .await?;
                    record_agent_activity(
                        pool,
                        Some(agent_id),
                        Some(run_id),
                        "event_error",
                        "Stream event rejected",
                        err,
                    )
                    .await?;
                }
            }
        }
        Err(err) => {
            append_run_log(pool, run_id, format!("[stream-event] rejected: {err}\n")).await?;
            record_agent_activity(
                pool,
                Some(agent_id),
                Some(run_id),
                "event_error",
                "Stream event rejected",
                err,
            )
            .await?;
        }
    }
    Ok(())
}

async fn consume_complete_streaming_agent_control_lines(
    pool: &SqlitePool,
    agent_id: Uuid,
    run_id: Uuid,
    stream_key: &str,
) -> CommandResult<bool> {
    let Some(row) = sqlx::query("select id, body from messages where stream_key = $1")
        .bind(stream_key)
        .fetch_optional(pool)
        .await
        .map_err(to_string)?
    else {
        return Ok(false);
    };
    let message_id: Uuid = row.get("id");
    let body: String = row.get("body");
    let (visible_body, event_jsons) = split_complete_streaming_agent_event_lines(&body);
    if event_jsons.is_empty() {
        return Ok(false);
    }

    for json in &event_jsons {
        handle_streaming_agent_event_json(pool, agent_id, run_id, json).await?;
    }

    if visible_body.is_empty()
        && event_jsons
            .iter()
            .any(|json| control_event_hides_empty_streaming_reply(json))
    {
        delete_streaming_agent_message(pool, message_id, "stream_event_consumed").await?;
        return Ok(true);
    }

    sqlx::query("update messages set body = $2, updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now') where id = $1")
        .bind(message_id)
        .bind(&visible_body)
        .execute(pool)
        .await
        .map_err(to_string)?;
    if let Ok(message) = load_message(pool, message_id).await {
        let _ = notify_ui_message_upsert(pool, &message, "stream_event_consumed").await;
    } else {
        let _ = notify_ui_refresh(pool, "stream_event_consumed").await;
    }
    Ok(true)
}

async fn consume_streaming_agent_control_lines(
    pool: &SqlitePool,
    agent_id: Uuid,
    run_id: Uuid,
    work_item_id: Option<Uuid>,
    stream_key: &str,
) -> CommandResult<bool> {
    if maybe_hide_silent_streaming_reply(pool, agent_id, run_id, work_item_id, stream_key).await? {
        return Ok(true);
    }

    let Some(row) = sqlx::query("select id, body from messages where stream_key = $1")
        .bind(stream_key)
        .fetch_optional(pool)
        .await
        .map_err(to_string)?
    else {
        return Ok(false);
    };
    let message_id: Uuid = row.get("id");
    let body: String = row.get("body");
    let (visible_body, event_jsons) = split_streaming_agent_event_lines(&body);
    if event_jsons.is_empty() {
        return Ok(false);
    }

    for json in &event_jsons {
        handle_streaming_agent_event_json(pool, agent_id, run_id, json).await?;
    }

    if visible_body.is_empty()
        && event_jsons
            .iter()
            .any(|json| control_event_hides_empty_streaming_reply(json))
    {
        delete_streaming_agent_message(pool, message_id, "stream_event_consumed").await?;
        return Ok(true);
    }

    sqlx::query("update messages set body = $2 where id = $1")
        .bind(message_id)
        .bind(&visible_body)
        .execute(pool)
        .await
        .map_err(to_string)?;
    if let Ok(message) = load_message(pool, message_id).await {
        let _ = notify_ui_message_upsert(pool, &message, "stream_event_consumed").await;
    } else {
        let _ = notify_ui_refresh(pool, "stream_event_consumed").await;
    }
    Ok(false)
}

async fn load_runtime_thread_id(
    pool: &SqlitePool,
    agent_id: Uuid,
    runtime: &str,
) -> CommandResult<Option<String>> {
    let thread_id: Option<String> = sqlx::query_scalar(
        r#"
        select provider_thread_id
        from runtime_sessions
        where agent_id = $1
          and runtime = $2
        "#,
    )
    .bind(agent_id)
    .bind(runtime)
    .fetch_optional(pool)
    .await
    .map_err(to_string)?;
    Ok(thread_id.filter(|thread_id| !thread_id.trim().is_empty()))
}

async fn upsert_runtime_thread_id(
    pool: &SqlitePool,
    agent_id: Uuid,
    runtime: &str,
    provider_thread_id: &str,
    status: &str,
) -> CommandResult<()> {
    sqlx::query(
        r#"
        insert into runtime_sessions (agent_id, runtime, provider_thread_id, status)
        values ($1, $2, $3, $4)
        on conflict (agent_id, runtime) do update set
            provider_thread_id = excluded.provider_thread_id,
            status = excluded.status,
            updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
        "#,
    )
    .bind(agent_id)
    .bind(runtime)
    .bind(provider_thread_id)
    .bind(status)
    .execute(pool)
    .await
    .map_err(to_string)?;
    Ok(())
}

async fn wait_for_agent_run(
    pool: SqlitePool,
    agent_id: Uuid,
    run_id: Uuid,
    work_item_id: Option<Uuid>,
    pipe_tasks: Vec<tokio::task::JoinHandle<()>>,
    mut child: tokio::process::Child,
) {
    let result = child.wait().await;
    for task in pipe_tasks {
        let _ = task.await;
    }
    let _ = replay_agent_events_from_run_log_if_needed(&pool, agent_id, run_id).await;
    let current_run_status: Option<String> =
        sqlx::query_scalar("select status from agent_runs where id = $1")
            .bind(run_id)
            .fetch_optional(&pool)
            .await
            .ok()
            .flatten();
    let (run_status, agent_status, exit_code, log_line) = match result {
        _ if current_run_status.as_deref() == Some("failed") => (
            "failed",
            "error",
            None,
            "process exited after runtime error\n".to_owned(),
        ),
        Ok(status) if status.success() => (
            "exited",
            "idle",
            status.code(),
            format!("process exited successfully: {status}\n"),
        ),
        Ok(status) => (
            "stopped",
            "idle",
            status.code(),
            format!("process stopped: {status}\n"),
        ),
        Err(err) => (
            "failed",
            "error",
            None,
            format!("failed while waiting for process: {err}\n"),
        ),
    };

    let _ = sqlx::query(
        r#"
        update agent_runs
        set status = $2,
            exit_code = $3,
            log = substr(log || $4, -20000),
            stopped_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
        where id = $1
        "#,
    )
    .bind(run_id)
    .bind(run_status)
    .bind(exit_code)
    .bind(&log_line)
    .execute(&pool)
    .await;
    notify_ui_agent_run_changed(&pool, run_id, "run_finished").await;

    let _ = sqlx::query("update agents set status = $2 where id = $1")
        .bind(agent_id)
        .bind(agent_status)
        .execute(&pool)
        .await;

    if let Some(work_item_id) = work_item_id {
        let current_status: Option<String> =
            sqlx::query_scalar("select status from agent_work_items where id = $1")
                .bind(work_item_id)
                .fetch_optional(&pool)
                .await
                .ok()
                .flatten();
        let work_status = if current_status.as_deref() == Some("cancelling") {
            "cancelled"
        } else if current_status.as_deref() == Some("silent") && run_status == "exited" {
            "silent"
        } else if run_status == "exited" {
            "done"
        } else {
            "failed"
        };
        let _ = sqlx::query(
            r#"
            update agent_work_items
            set status = $2,
                completed_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now'),
                updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
            where id = $1
            "#,
        )
        .bind(work_item_id)
        .bind(work_status)
        .execute(&pool)
        .await;
        let _ =
            mark_task_after_work_item_finished(&pool, work_item_id, agent_id, run_id, work_status)
                .await;
        notify_ui_work_item_changed(&pool, work_item_id, "work_item_finished").await;
        let _ = record_agent_activity(
            &pool,
            Some(agent_id),
            Some(run_id),
            "dispatch",
            work_status_title(work_status),
            work_item_id.to_string(),
        )
        .await;
    }

    let _ = record_agent_activity(
        &pool,
        Some(agent_id),
        Some(run_id),
        "run",
        format!("Run {run_status}"),
        log_line.trim(),
    )
    .await;
}

fn effective_launch_command(
    launch_command: String,
    _runtime: String,
    _model: String,
    _handle: String,
) -> String {
    if !launch_command.trim().is_empty() {
        return launch_command.trim().to_owned();
    }

    "printf 'Lantor placeholder runtime. Configure launch_command to run a real agent.\\n'; sleep 3600"
        .to_owned()
}

async fn load_channel_agent_roster(
    pool: &SqlitePool,
    channel_id: Option<Uuid>,
    current_agent_id: Uuid,
) -> CommandResult<Vec<String>> {
    let Some(channel_id) = channel_id else {
        return Ok(vec![]);
    };
    let rows = sqlx::query(
        r#"
        select a.handle, a.display_name, a.runtime, a.model, a.status
        from channels c
        join channel_members cm on cm.channel_id = c.id
        join agents a on a.id = cm.agent_id
        where c.id = $1
          and c.kind = 'channel'
          and a.id <> $2
        order by lower(a.handle)
        "#,
    )
    .bind(channel_id)
    .bind(current_agent_id)
    .fetch_all(pool)
    .await
    .map_err(to_string)?;

    Ok(rows
        .into_iter()
        .map(|row| {
            let handle: String = row.get("handle");
            let display_name: String = row.get("display_name");
            let runtime: String = row.get("runtime");
            let model: String = row.get("model");
            let status: String = row.get("status");
            format!("@{handle} - {display_name} - {runtime}/{model} - {status}")
        })
        .collect())
}

fn configure_agent_context_tool_env(command: &mut Command) {
    if let Ok(exe_path) = env::current_exe() {
        command.env(LANTOR_CONTEXT_TOOL_ENV, exe_path);
    }
    command.env("LANTOR_DATABASE_URL", db_url());
}

fn configure_agent_identity_env(command: &mut Command, agent_id: Uuid, handle: &str) {
    command.env("LANTOR_AGENT_ID", agent_id.to_string());
    command.env("LANTOR_AGENT_HANDLE", handle);
}

fn codex_stream_key(run_id: Uuid, item_id: &str) -> String {
    format!("{run_id}:{item_id}")
}

fn codex_pending_stream_key(run_id: Uuid) -> String {
    format!("{run_id}:pending")
}

async fn codex_write_json(
    stdin: &mut tokio::process::ChildStdin,
    value: Value,
) -> CommandResult<()> {
    let mut line = serde_json::to_vec(&value).map_err(to_string)?;
    line.push(b'\n');
    stdin.write_all(&line).await.map_err(to_string)?;
    stdin.flush().await.map_err(to_string)?;
    Ok(())
}

fn codex_request_error(value: &Value) -> Option<String> {
    value.get("error").map(|error| {
        error
            .get("message")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .unwrap_or_else(|| error.to_string())
    })
}

fn codex_error_notification_detail(value: &Value) -> Option<String> {
    if value.pointer("/params/willRetry").and_then(Value::as_bool) == Some(true) {
        return None;
    }
    value
        .pointer("/params/error/message")
        .or_else(|| value.pointer("/params/message"))
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or_else(|| Some("codex emitted error notification".to_owned()))
}

fn codex_thread_id_from_response(value: &Value) -> Option<String> {
    value
        .pointer("/result/thread/id")
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn codex_turn_id_from_value(value: &Value) -> Option<String> {
    value
        .pointer("/result/turn/id")
        .or_else(|| value.pointer("/params/turn/id"))
        .or_else(|| value.pointer("/params/turnId"))
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn codex_item_type(value: &Value) -> Option<&str> {
    value.pointer("/params/item/type").and_then(Value::as_str)
}

fn codex_item_id(value: &Value) -> Option<&str> {
    value.pointer("/params/item/id").and_then(Value::as_str)
}

fn codex_item_summary(value: &Value) -> String {
    let Some(item) = value.pointer("/params/item") else {
        return "item".to_owned();
    };
    match item.get("type").and_then(Value::as_str).unwrap_or("item") {
        "commandExecution" => item
            .get("command")
            .and_then(Value::as_str)
            .unwrap_or("shell command")
            .to_owned(),
        "mcpToolCall" => format!(
            "{}.{}",
            item.get("server").and_then(Value::as_str).unwrap_or("mcp"),
            item.get("tool").and_then(Value::as_str).unwrap_or("tool")
        ),
        "dynamicToolCall" => format!(
            "{}{}",
            item.get("namespace")
                .and_then(Value::as_str)
                .map(|namespace| format!("{namespace}."))
                .unwrap_or_default(),
            item.get("tool").and_then(Value::as_str).unwrap_or("tool")
        ),
        "webSearch" => item
            .get("query")
            .and_then(Value::as_str)
            .unwrap_or("web search")
            .to_owned(),
        "fileChange" => "File change".to_owned(),
        "reasoning" => "Thinking".to_owned(),
        "agentMessage" => "Writing response".to_owned(),
        other => other.to_owned(),
    }
}

fn first_nonempty_item_value<'a>(item: &'a Value, fields: &[&str]) -> Option<&'a str> {
    fields
        .iter()
        .find_map(|field| item.pointer(field).or_else(|| item.get(*field)))
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
}

fn codex_item_file_summary(item: &Value) -> String {
    let path = first_nonempty_item_value(
        item,
        &[
            "path",
            "file",
            "filePath",
            "filename",
            "/path",
            "/file",
            "/filePath",
            "/changes/0/path",
            "/changes/0/file",
        ],
    )
    .unwrap_or("file");
    let operation = first_nonempty_item_value(
        item,
        &["operation", "action", "change", "/operation", "/action"],
    )
    .unwrap_or("edit");
    json!({ "file": path, "operation": operation }).to_string()
}

fn codex_item_started_activity(value: &Value) -> (&'static str, &'static str, String) {
    let Some(item) = value.pointer("/params/item") else {
        return ("activity", "Codex activity", "item".to_owned());
    };
    match item.get("type").and_then(Value::as_str).unwrap_or("item") {
        "reasoning" => ("thinking", "Thinking", "Thinking".to_owned()),
        "commandExecution" => (
            "command",
            "Running command",
            json!({ "command": codex_item_summary(value) }).to_string(),
        ),
        "fileChange" => ("file_edit", "Editing file", codex_item_file_summary(item)),
        "mcpToolCall" | "dynamicToolCall" | "webSearch" => {
            ("tools", "Using tool", codex_item_summary(value))
        }
        "agentMessage" => ("acting", "Writing response", "Writing response".to_owned()),
        _ => ("activity", "Codex activity", codex_item_summary(value)),
    }
}

fn first_nonempty_item_string<'a>(item: &'a Value, fields: &[&str]) -> Option<&'a str> {
    fields
        .iter()
        .find_map(|field| item.get(*field).and_then(Value::as_str))
        .filter(|value| !value.trim().is_empty())
}

fn codex_tool_completion_activity(value: &Value) -> Option<(&'static str, &'static str, String)> {
    let item = value.pointer("/params/item")?;
    let item_type = item.get("type").and_then(Value::as_str).unwrap_or("item");
    if !matches!(
        item_type,
        "commandExecution" | "mcpToolCall" | "dynamicToolCall" | "webSearch" | "fileChange"
    ) {
        return None;
    }

    let (kind, title, mut metadata) = match item_type {
        "commandExecution" => (
            "command",
            "Command finished",
            json!({ "command": codex_item_summary(value) }),
        ),
        "fileChange" => (
            "file_edit",
            "File edit finished",
            serde_json::from_str(&codex_item_file_summary(item))
                .unwrap_or_else(|_| json!({ "file": "file" })),
        ),
        _ => (
            "tools",
            "Tool completed",
            json!({ "tool": codex_item_summary(value) }),
        ),
    };

    if let Some(object) = metadata.as_object_mut() {
        if let Some(exit_code) = item.get("exitCode").and_then(Value::as_i64) {
            object.insert("exit_code".to_owned(), json!(exit_code));
        }
        if let Some(status) = first_nonempty_item_string(item, &["status", "state"]) {
            object.insert("status".to_owned(), json!(status));
        }
        if let Some(output) =
            first_nonempty_item_string(item, &["output", "stdout", "stderr", "result", "error"])
        {
            object.insert("output".to_owned(), json!(truncate_activity_detail(output)));
        }
    }

    Some((kind, title, metadata.to_string()))
}

fn effective_codex_cwd(working_directory: &str) -> CommandResult<String> {
    if working_directory.trim().is_empty() {
        Ok(env::current_dir()
            .map_err(to_string)?
            .to_string_lossy()
            .to_string())
    } else {
        Ok(working_directory.trim().to_owned())
    }
}

fn codex_model_value(model: &str) -> Value {
    if model.trim().is_empty() {
        Value::Null
    } else {
        json!(model.trim())
    }
}

fn apply_codex_runtime_options(params: &mut Value, reasoning_effort: &str, service_tier: &str) {
    if let Some(object) = params.as_object_mut() {
        let reasoning_effort = reasoning_effort.trim();
        if !reasoning_effort.is_empty() {
            object.insert("reasoningEffort".to_owned(), json!(reasoning_effort));
        }
        let service_tier = service_tier.trim();
        if !service_tier.is_empty() {
            object.insert("serviceTier".to_owned(), json!(service_tier));
        }
    }
}

fn claude_stream_key(run_id: Uuid) -> String {
    format!("{run_id}:claude-assistant")
}

fn claude_text_delta(value: &Value) -> Option<&str> {
    if value.get("type").and_then(Value::as_str) != Some("stream_event") {
        return None;
    }
    if value.pointer("/event/delta/type").and_then(Value::as_str) != Some("text_delta") {
        return None;
    }
    value.pointer("/event/delta/text").and_then(Value::as_str)
}

fn claude_session_id(value: &Value) -> Option<&str> {
    value.get("session_id").and_then(Value::as_str)
}

fn claude_message_text(value: &Value) -> Option<String> {
    if value.get("type").and_then(Value::as_str) != Some("assistant") {
        return None;
    }
    let content = value.pointer("/message/content")?.as_array()?;
    let text = content
        .iter()
        .filter_map(|block| {
            if block.get("type").and_then(Value::as_str) == Some("text") {
                block.get("text").and_then(Value::as_str)
            } else {
                None
            }
        })
        .collect::<Vec<_>>()
        .join("");
    (!text.trim().is_empty()).then_some(text)
}

fn claude_result_text(value: &Value) -> Option<String> {
    if value.get("type").and_then(Value::as_str) != Some("result") {
        return None;
    }
    value
        .get("result")
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())
        .map(str::to_owned)
}

fn claude_result_error(value: &Value) -> Option<String> {
    if value.get("type").and_then(Value::as_str) != Some("result") {
        return None;
    }
    let is_error = value
        .get("is_error")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let api_error_status = value
        .get("api_error_status")
        .and_then(Value::as_str)
        .filter(|status| !status.trim().is_empty());
    if !is_error && api_error_status.is_none() {
        return None;
    }
    value
        .get("error")
        .and_then(Value::as_str)
        .or(api_error_status)
        .or_else(|| value.get("result").and_then(Value::as_str))
        .map(str::to_owned)
        .or_else(|| Some("Claude stream-json result reported an error".to_owned()))
}

fn claude_api_retry_detail(value: &Value) -> String {
    let attempt = value
        .get("attempt")
        .or_else(|| value.pointer("/retry/attempt"))
        .and_then(Value::as_i64);
    let max_retries = value
        .get("max_retries")
        .or_else(|| value.pointer("/retry/max_retries"))
        .and_then(Value::as_i64);
    let status = value
        .get("error_status")
        .or_else(|| value.pointer("/error/status"))
        .and_then(Value::as_i64)
        .map(|status| status.to_string());
    let error = value
        .get("error")
        .or_else(|| value.pointer("/error/type"))
        .or_else(|| value.pointer("/error/error"))
        .and_then(Value::as_str);

    let mut parts = vec!["Claude provider request failed; retrying automatically".to_owned()];
    match (attempt, max_retries) {
        (Some(attempt), Some(max_retries)) => {
            parts.push(format!("attempt {attempt}/{max_retries}"));
        }
        (Some(attempt), None) => {
            parts.push(format!("attempt {attempt}"));
        }
        _ => {}
    }
    if let Some(status) = status {
        parts.push(format!("status {status}"));
    }
    if let Some(error) = error {
        parts.push(format!("error {error}"));
    }

    truncate_activity_detail(&parts.join(" · "))
}

fn claude_stream_event_activity(value: &Value) -> Option<(&'static str, &'static str, String)> {
    match value.get("type").and_then(Value::as_str)? {
        "system" => match value.get("subtype").and_then(Value::as_str) {
            Some("init") => Some(("run", "Runtime ready", "Claude stream connected".to_owned())),
            Some("api_retry") => Some((
                "run_error",
                "Claude provider retrying",
                claude_api_retry_detail(value),
            )),
            Some(_) => None,
            None => None,
        },
        "rate_limit_event" => {
            let status = value
                .pointer("/rate_limit_info/status")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            if status.eq_ignore_ascii_case("allowed") {
                None
            } else {
                Some((
                    "run_error",
                    "Waiting on rate limit",
                    format!("status={status}"),
                ))
            }
        }
        "stream_event" => {
            let event_type = value.pointer("/event/type").and_then(Value::as_str)?;
            match event_type {
                "content_block_start" => {
                    let block_type = value
                        .pointer("/event/content_block/type")
                        .and_then(Value::as_str)
                        .unwrap_or("content");
                    if block_type == "tool_use" {
                        let name = value
                            .pointer("/event/content_block/name")
                            .and_then(Value::as_str)
                            .unwrap_or("tool");
                        match name {
                            "Bash" => Some((
                                "command",
                                "Running command",
                                json!({ "tool": name }).to_string(),
                            )),
                            "Edit" | "MultiEdit" | "Write" | "NotebookEdit" => Some((
                                "file_edit",
                                "Editing file",
                                json!({ "tool": name }).to_string(),
                            )),
                            _ => Some(("tools", "Using tool", name.to_owned())),
                        }
                    } else if block_type == "thinking" {
                        Some(("thinking", "Thinking", "Claude is thinking".to_owned()))
                    } else {
                        None
                    }
                }
                "content_block_stop" | "message_stop" => None,
                _ => None,
            }
        }
        _ => None,
    }
}

async fn streaming_message_exists(pool: &SqlitePool, stream_key: &str) -> CommandResult<bool> {
    let exists: bool =
        sqlx::query_scalar("select exists(select 1 from messages where stream_key = $1)")
            .bind(stream_key)
            .fetch_one(pool)
            .await
            .map_err(to_string)?;
    Ok(exists)
}

async fn get_or_spawn_warm_claude_runtime(
    pool: &SqlitePool,
    registry: &WarmClaudeRegistry,
    agent_id: Uuid,
    handle: &str,
    model: &str,
    working_directory: &str,
    memory_context: Option<&str>,
) -> CommandResult<Arc<WarmClaudeRuntime>> {
    if let Some(runtime) = {
        let runtimes = registry.runtimes.lock().await;
        runtimes.get(&agent_id).cloned()
    } {
        if runtime.state.lock().await.alive {
            return Ok(runtime);
        }
        registry.runtimes.lock().await.remove(&agent_id);
    }

    let runtime = spawn_warm_claude_runtime(
        pool,
        registry.clone(),
        agent_id,
        handle,
        model,
        working_directory,
        memory_context,
    )
    .await?;
    registry
        .runtimes
        .lock()
        .await
        .insert(agent_id, runtime.clone());
    Ok(runtime)
}

fn claude_streaming_command_text(model: &str) -> String {
    format!(
        "{CLAUDE_MAX_RETRIES_ENV}={DEFAULT_CLAUDE_MAX_RETRIES} claude -p --model {model} --output-format stream-json --input-format stream-json --include-partial-messages --verbose --permission-mode bypassPermissions"
    )
}

fn claude_user_input(prompt: &str) -> Value {
    json!({
        "type": "user",
        "message": {
            "role": "user",
            "content": [{
                "type": "text",
                "text": prompt
            }]
        }
    })
}

async fn claude_write_input(
    stdin: &mut tokio::process::ChildStdin,
    value: Value,
) -> CommandResult<()> {
    let mut line = serde_json::to_vec(&value).map_err(to_string)?;
    line.push(b'\n');
    stdin.write_all(&line).await.map_err(to_string)?;
    stdin.flush().await.map_err(to_string)
}

async fn spawn_warm_claude_runtime(
    pool: &SqlitePool,
    registry: WarmClaudeRegistry,
    agent_id: Uuid,
    handle: &str,
    model: &str,
    working_directory: &str,
    memory_context: Option<&str>,
) -> CommandResult<Arc<WarmClaudeRuntime>> {
    let model = if model.trim().is_empty() {
        "sonnet".to_owned()
    } else {
        model.trim().to_owned()
    };
    let mut command = Command::new("claude");
    command
        .arg("-p")
        .arg("--system-prompt")
        .arg(claude_system_prompt(handle, memory_context))
        .arg("--model")
        .arg(&model)
        .arg("--output-format")
        .arg("stream-json")
        .arg("--input-format")
        .arg("stream-json")
        .arg("--include-partial-messages")
        .arg("--verbose")
        .arg("--permission-mode")
        .arg("bypassPermissions")
        .env(CLAUDE_MAX_RETRIES_ENV, DEFAULT_CLAUDE_MAX_RETRIES);
    configure_agent_identity_env(&mut command, agent_id, handle);
    configure_agent_context_tool_env(&mut command);
    #[cfg(unix)]
    command.process_group(0);
    if !working_directory.is_empty() {
        command.current_dir(working_directory);
    }
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(err) => {
            sqlx::query("update agents set status = 'error' where id = $1")
                .bind(agent_id)
                .execute(pool)
                .await
                .map_err(to_string)?;
            record_agent_activity(
                pool,
                Some(agent_id),
                None,
                "run_error",
                "Claude warm stream-json failed to start",
                err.to_string(),
            )
            .await?;
            return Err(err.to_string());
        }
    };

    let pid = child.id().map(|id| id as i32);
    let Some(stdin) = child.stdin.take() else {
        return Err("Claude stream-json stdin unavailable".to_owned());
    };
    let Some(stdout) = child.stdout.take() else {
        return Err("Claude stream-json stdout unavailable".to_owned());
    };
    let stderr = child.stderr.take();

    let runtime = Arc::new(WarmClaudeRuntime {
        stdin: AsyncMutex::new(stdin),
        state: AsyncMutex::new(WarmClaudeState {
            alive: true,
            active: None,
            session_id: None,
            last_surface: None,
            last_activity: Instant::now(),
        }),
        pid,
    });

    upsert_runtime_thread_id(
        pool,
        agent_id,
        "claude",
        &pid.map(|pid| format!("pid:{pid}"))
            .unwrap_or_else(|| "warming".to_owned()),
        "idle",
    )
    .await?;
    record_agent_activity(
        pool,
        Some(agent_id),
        None,
        "run",
        "Claude warm stream-json ready",
        pid.map(|pid| format!("pid={pid}"))
            .unwrap_or_else(|| "pid unavailable".to_owned()),
    )
    .await?;

    tokio::spawn(claude_warm_stdout_reader(
        pool.clone(),
        registry.clone(),
        agent_id,
        runtime.clone(),
        stdout,
    ));
    if let Some(stderr) = stderr {
        tokio::spawn(claude_warm_stderr_reader(
            pool.clone(),
            agent_id,
            runtime.clone(),
            stderr,
        ));
    }
    tokio::spawn(wait_for_warm_claude_process(
        pool.clone(),
        registry,
        agent_id,
        runtime.clone(),
        child,
    ));
    tokio::spawn(claude_warm_idle_reaper(
        pool.clone(),
        agent_id,
        runtime.clone(),
    ));

    Ok(runtime)
}

async fn get_or_spawn_warm_codex_runtime(
    pool: &SqlitePool,
    registry: &WarmCodexRegistry,
    agent_id: Uuid,
    handle: &str,
    model: &str,
    reasoning_effort: &str,
    service_tier: &str,
    working_directory: &str,
    memory_context: Option<&str>,
) -> CommandResult<Arc<WarmCodexRuntime>> {
    let context_rotate_threshold = codex_context_rotate_input_tokens();
    let rotation_candidate =
        codex_context_rotation_candidate(pool, agent_id, context_rotate_threshold).await?;
    if let Some(runtime) = {
        let runtimes = registry.runtimes.lock().await;
        runtimes.get(&agent_id).cloned()
    } {
        let mut state = runtime.state.lock().await;
        if state.alive && rotation_candidate.is_some() && state.active.is_none() {
            state.alive = false;
            drop(state);
            if let Some(pid) = runtime.pid {
                let _ = terminate_process_group(pid).await;
            }
            remove_warm_codex_runtime_if_same(registry, agent_id, &runtime).await;
        } else if state.alive {
            drop(state);
            return Ok(runtime);
        } else {
            drop(state);
            registry.runtimes.lock().await.remove(&agent_id);
        }
    }

    let runtime = spawn_warm_codex_runtime(
        pool,
        registry.clone(),
        agent_id,
        handle,
        model,
        reasoning_effort,
        service_tier,
        working_directory,
        memory_context,
        context_rotate_threshold,
    )
    .await?;
    registry
        .runtimes
        .lock()
        .await
        .insert(agent_id, runtime.clone());
    Ok(runtime)
}

async fn spawn_warm_codex_runtime(
    pool: &SqlitePool,
    registry: WarmCodexRegistry,
    agent_id: Uuid,
    handle: &str,
    model: &str,
    reasoning_effort: &str,
    service_tier: &str,
    working_directory: &str,
    memory_context: Option<&str>,
    context_rotate_threshold: i64,
) -> CommandResult<Arc<WarmCodexRuntime>> {
    let cwd = effective_codex_cwd(working_directory)?;
    let mut command = Command::new("/bin/zsh");
    command
        .arg("-lc")
        .arg("exec codex app-server --listen stdio:// -c 'notify=[]'");
    configure_agent_identity_env(&mut command, agent_id, handle);
    configure_agent_context_tool_env(&mut command);
    #[cfg(unix)]
    command.process_group(0);
    command.current_dir(&cwd);
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(err) => {
            sqlx::query("update agents set status = 'error' where id = $1")
                .bind(agent_id)
                .execute(pool)
                .await
                .map_err(to_string)?;
            record_agent_activity(
                pool,
                Some(agent_id),
                None,
                "run_error",
                "Codex warm app-server failed to start",
                err.to_string(),
            )
            .await?;
            return Err(err.to_string());
        }
    };

    let pid = child.id().map(|id| id as i32);
    let Some(mut stdin) = child.stdin.take() else {
        return Err("codex app-server stdin unavailable".to_owned());
    };
    let Some(stdout) = child.stdout.take() else {
        return Err("codex app-server stdout unavailable".to_owned());
    };
    let stderr = child.stderr.take();
    let mut reader = BufReader::new(stdout);
    let developer_instructions = codex_developer_instructions(handle, memory_context);
    let model_value = codex_model_value(model);
    let mut next_request_id = 1_i64;
    let initialize_id = next_request_id;
    next_request_id += 1;
    let mut thread_request_id = next_request_id;
    next_request_id += 1;

    codex_write_json(
        &mut stdin,
        json!({
            "method": "initialize",
            "id": initialize_id,
            "params": {
                "clientInfo": {
                    "name": "lantor",
                    "title": "Lantor",
                    "version": env!("CARGO_PKG_VERSION")
                },
                "capabilities": {
                    "experimentalApi": true
                }
            }
        }),
    )
    .await?;
    codex_write_json(&mut stdin, json!({ "method": "initialized" })).await?;

    let rotation_candidate =
        codex_context_rotation_candidate(pool, agent_id, context_rotate_threshold).await?;
    let existing_thread_id = if rotation_candidate.is_some() {
        None
    } else {
        load_runtime_thread_id(pool, agent_id, "codex").await?
    };
    let mut attempted_resume = existing_thread_id.is_some();
    if let Some((run_id, input_tokens)) = rotation_candidate {
        record_agent_activity(
            pool,
            Some(agent_id),
            None,
            "run",
            "Codex context rotated",
            json!({
                "previous_run_id": run_id,
                "input_tokens": input_tokens,
                "threshold": context_rotate_threshold,
                "env": CODEX_CONTEXT_ROTATE_ENV
            })
            .to_string(),
        )
        .await?;
    }
    if let Some(thread_id) = existing_thread_id {
        let mut params = json!({
            "threadId": thread_id.clone(),
            "model": model_value.clone(),
            "cwd": cwd.clone(),
            "approvalPolicy": "never",
            "sandbox": "danger-full-access",
            "developerInstructions": developer_instructions.clone(),
            "persistExtendedHistory": true
        });
        apply_codex_runtime_options(&mut params, reasoning_effort, service_tier);
        codex_write_json(
            &mut stdin,
            json!({
                "method": "thread/resume",
                "id": thread_request_id,
                "params": params
            }),
        )
        .await?;
    } else {
        let mut params = json!({
            "model": model_value.clone(),
            "cwd": cwd.clone(),
            "approvalPolicy": "never",
            "sandbox": "danger-full-access",
            "developerInstructions": developer_instructions.clone(),
            "experimentalRawEvents": false,
            "persistExtendedHistory": true
        });
        apply_codex_runtime_options(&mut params, reasoning_effort, service_tier);
        codex_write_json(
            &mut stdin,
            json!({
                "method": "thread/start",
                "id": thread_request_id,
                "params": params
            }),
        )
        .await?;
    }

    let thread_id = loop {
        let mut line = String::new();
        let bytes = reader.read_line(&mut line).await.map_err(to_string)?;
        if bytes == 0 {
            return Err("codex app-server closed during warm initialization".to_owned());
        }
        let line = line.trim_end_matches(['\r', '\n']);
        let value: Value = serde_json::from_str(line).map_err(to_string)?;
        if value.get("id").and_then(Value::as_i64) == Some(thread_request_id) {
            if let Some(error) = codex_request_error(&value) {
                if attempted_resume {
                    attempted_resume = false;
                    thread_request_id = next_request_id;
                    next_request_id += 1;
                    let mut params = json!({
                        "model": model_value.clone(),
                        "cwd": cwd.clone(),
                        "approvalPolicy": "never",
                        "sandbox": "danger-full-access",
                        "developerInstructions": developer_instructions.clone(),
                        "experimentalRawEvents": false,
                        "persistExtendedHistory": true
                    });
                    apply_codex_runtime_options(&mut params, reasoning_effort, service_tier);
                    codex_write_json(
                        &mut stdin,
                        json!({
                            "method": "thread/start",
                            "id": thread_request_id,
                            "params": params
                        }),
                    )
                    .await?;
                    record_agent_activity(
                        pool,
                        Some(agent_id),
                        None,
                        "run",
                        "Codex thread resume failed; starting new thread",
                        error,
                    )
                    .await?;
                    continue;
                }
                return Err(format!("codex thread request failed: {error}"));
            }
            let Some(thread_id) = codex_thread_id_from_response(&value) else {
                return Err("codex thread response missing thread id".to_owned());
            };
            break thread_id;
        }
    };

    upsert_runtime_thread_id(pool, agent_id, "codex", &thread_id, "idle").await?;
    record_agent_activity(
        pool,
        Some(agent_id),
        None,
        "run",
        "Codex warm app-server ready",
        pid.map(|pid| format!("pid={pid}, thread_id={thread_id}"))
            .unwrap_or_else(|| format!("thread_id={thread_id}")),
    )
    .await?;

    let runtime = Arc::new(WarmCodexRuntime {
        stdin: AsyncMutex::new(stdin),
        state: AsyncMutex::new(WarmCodexState {
            alive: true,
            active: None,
            next_request_id,
            last_activity: Instant::now(),
        }),
        thread_id,
        pid,
    });

    tokio::spawn(codex_warm_stdout_reader(
        pool.clone(),
        registry.clone(),
        agent_id,
        runtime.clone(),
        reader,
    ));
    if let Some(stderr) = stderr {
        tokio::spawn(codex_warm_stderr_reader(
            pool.clone(),
            agent_id,
            runtime.clone(),
            stderr,
        ));
    }
    tokio::spawn(wait_for_warm_codex_process(
        pool.clone(),
        registry.clone(),
        agent_id,
        runtime.clone(),
        child,
    ));
    tokio::spawn(codex_warm_idle_reaper(
        pool.clone(),
        registry.clone(),
        agent_id,
        runtime.clone(),
    ));

    Ok(runtime)
}

async fn run_supervisor() -> CommandResult<()> {
    let database_url = db_url();
    let _supervisor_lock = acquire_supervisor_lock(&database_url)?;
    let pool = db_connect_with_url(&database_url, 5)
        .await
        .map_err(to_string)?;
    migrate(&pool).await.map_err(to_string)?;

    mark_orphaned_agent_runs(&pool).await?;
    recover_supervisor_commands_at_startup(&pool).await?;
    let codex_registry = WarmCodexRegistry::default();
    let claude_registry = WarmClaudeRegistry::default();
    let command_semaphore = Arc::new(Semaphore::new(SUPERVISOR_COMMAND_CONCURRENCY));
    let mut last_command_cleanup = Instant::now() - Duration::from_secs(3600);

    loop {
        write_supervisor_heartbeat(&pool).await?;
        if last_command_cleanup.elapsed() >= Duration::from_secs(3600) {
            cleanup_supervisor_commands(&pool).await?;
            last_command_cleanup = Instant::now();
        }
        schedule_queued_work_items(&pool, &codex_registry).await?;
        let mut processed_command = false;
        loop {
            let Ok(permit) = command_semaphore.clone().try_acquire_owned() else {
                break;
            };
            let Some(command) = claim_next_supervisor_command(&pool).await? else {
                drop(permit);
                break;
            };
            processed_command = true;
            let command_id = command.id;
            let pool = pool.clone();
            let codex_registry = codex_registry.clone();
            let claude_registry = claude_registry.clone();
            tokio::spawn(async move {
                let _permit = permit;
                let result =
                    process_supervisor_command(&pool, &codex_registry, &claude_registry, command)
                        .await;
                if let Err(err) = finish_supervisor_command(&pool, command_id, result.err()).await {
                    eprintln!("failed to finish supervisor command {command_id}: {err}");
                }
            });
        }
        if processed_command {
            continue;
        }
        sleep(Duration::from_secs(2)).await;
    }
}

async fn active_codex_turn_surface(
    registry: &WarmCodexRegistry,
    agent_id: Uuid,
) -> Option<(Option<Uuid>, Option<Uuid>, CodexActiveTurnScheduleState)> {
    let runtime = {
        let runtimes = registry.runtimes.lock().await;
        runtimes.get(&agent_id).cloned()
    }?;
    let state = runtime.state.lock().await;
    let active = state.active.as_ref()?;
    Some((
        active.channel_id,
        active.thread_root_id,
        codex_active_turn_schedule_state(
            active.turn_id.as_deref(),
            active.steer_disabled,
            active.started_at.elapsed(),
        ),
    ))
}

fn codex_active_turn_schedule_state(
    turn_id: Option<&str>,
    steer_disabled: bool,
    elapsed: Duration,
) -> CodexActiveTurnScheduleState {
    if turn_id.is_some() && !steer_disabled {
        return CodexActiveTurnScheduleState::ReadyForSteer;
    }
    if turn_id.is_none() && elapsed >= CODEX_TURN_START_TIMEOUT {
        return CodexActiveTurnScheduleState::StuckBeforeTurnId;
    }
    CodexActiveTurnScheduleState::WaitingForTurnId
}

async fn same_codex_surface(
    pool: &SqlitePool,
    channel_id: Option<Uuid>,
    thread_root_id: Option<Uuid>,
    active_channel_id: Option<Uuid>,
    active_thread_root_id: Option<Uuid>,
) -> CommandResult<bool> {
    if channel_id.is_none() || channel_id != active_channel_id {
        return Ok(false);
    }
    let Some(channel_id) = channel_id else {
        return Ok(false);
    };
    let kind: Option<String> = sqlx::query_scalar("select kind from channels where id = $1")
        .bind(channel_id)
        .fetch_optional(pool)
        .await
        .map_err(to_string)?;
    if kind.as_deref() == Some("dm") {
        return Ok(true);
    }
    Ok(thread_root_id == active_thread_root_id)
}

async fn should_schedule_queued_work_item(
    pool: &SqlitePool,
    registry: &WarmCodexRegistry,
    agent_id: Uuid,
    channel_id: Option<Uuid>,
    thread_root_id: Option<Uuid>,
) -> CommandResult<bool> {
    let runtime = agent_runtime(pool, agent_id).await?;
    if !runtime
        .as_deref()
        .is_some_and(|runtime| runtime.eq_ignore_ascii_case("codex"))
    {
        return Ok(!agent_has_active_or_pending_start(pool, agent_id).await?);
    }

    let Some((active_channel_id, active_thread_root_id, schedule_state)) =
        active_codex_turn_surface(registry, agent_id).await
    else {
        return Ok(true);
    };
    match schedule_state {
        CodexActiveTurnScheduleState::ReadyForSteer => {}
        CodexActiveTurnScheduleState::WaitingForTurnId => return Ok(false),
        CodexActiveTurnScheduleState::StuckBeforeTurnId => {
            reap_stuck_codex_turn(pool, registry, agent_id, "scheduler").await?;
            return Ok(true);
        }
    }
    same_codex_surface(
        pool,
        channel_id,
        thread_root_id,
        active_channel_id,
        active_thread_root_id,
    )
    .await
}

async fn schedule_queued_work_items(
    pool: &SqlitePool,
    registry: &WarmCodexRegistry,
) -> CommandResult<()> {
    let rows = sqlx::query(
        r#"
        select w.id, w.agent_id, w.channel_id, w.thread_root_id
        from agent_work_items w
        where w.status = 'queued'
          and not exists (
              select 1
              from supervisor_commands c
              where c.command_type = 'start_agent'
                and c.work_item_id = w.id
                and c.status in ('pending', 'running')
          )
        order by w.created_at asc
        limit 16
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(to_string)?;

    for row in rows {
        let work_item_id: Uuid = row.get("id");
        let agent_id: Uuid = row.get("agent_id");
        let channel_id: Option<Uuid> = row.get("channel_id");
        let thread_root_id: Option<Uuid> = row.get("thread_root_id");
        if !should_schedule_queued_work_item(pool, registry, agent_id, channel_id, thread_root_id)
            .await?
        {
            continue;
        }
        if enqueue_agent_work_if_available(pool, agent_id, work_item_id).await? {
            record_agent_activity(
                pool,
                Some(agent_id),
                None,
                "dispatch",
                "Backlog agent request scheduled",
                work_item_id.to_string(),
            )
            .await?;
        }
    }

    Ok(())
}

async fn mark_orphaned_agent_runs(pool: &SqlitePool) -> CommandResult<()> {
    sqlx::query(
        r#"
        update agent_runs
        set status = 'unknown', stopped_at = coalesce(stopped_at, strftime('%Y-%m-%dT%H:%M:%f+00:00','now'))
        where stopped_at is null and status in ('starting', 'running', 'stopping')
        "#,
    )
    .execute(pool)
    .await
    .map_err(to_string)?;
    sqlx::query(
        "update agents set status = 'idle' where status in ('queued', 'running', 'stopping')",
    )
    .execute(pool)
    .await
    .map_err(to_string)?;
    sqlx::query(
        r#"
        update agent_work_items
        set status = 'failed',
            completed_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now'),
            updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
        where status = 'running'
        "#,
    )
    .execute(pool)
    .await
    .map_err(to_string)?;

    Ok(())
}

async fn recover_supervisor_commands_at_startup(pool: &SqlitePool) -> CommandResult<()> {
    sqlx::query(
        r#"
        update supervisor_commands
        set status = 'done',
            error = 'skipped stale command for terminal work item',
            updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
        where status in ('pending', 'running')
          and exists (
              select 1
              from agent_work_items w
              where w.id = supervisor_commands.work_item_id
          and w.status in ('cancelled', 'failed', 'done', 'silent')
          )
        "#,
    )
    .execute(pool)
    .await
    .map_err(to_string)?;

    sqlx::query(
        r#"
        update supervisor_commands
        set status = 'pending',
            error = 'requeued after supervisor restart',
            updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
        where status = 'running'
        "#,
    )
    .execute(pool)
    .await
    .map_err(to_string)?;

    Ok(())
}

async fn write_supervisor_heartbeat(pool: &SqlitePool) -> CommandResult<()> {
    sqlx::query(
        r#"
        insert into supervisor_state (id, pid, status, updated_at)
        values (1, $1, 'running', strftime('%Y-%m-%dT%H:%M:%f+00:00','now'))
        on conflict (id) do update set
            pid = excluded.pid,
            status = excluded.status,
            updated_at = excluded.updated_at
        "#,
    )
    .bind(std::process::id() as i32)
    .execute(pool)
    .await
    .map_err(to_string)?;

    Ok(())
}

async fn claim_next_supervisor_command(
    pool: &SqlitePool,
) -> CommandResult<Option<SupervisorCommand>> {
    let row = sqlx::query(
        r#"
        update supervisor_commands
        set status = 'running',
            updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
        where id = (
            select id
            from supervisor_commands
            where status = 'pending'
            order by created_at asc
            limit 1
        )
        returning id, command_type, agent_id, run_id, work_item_id
        "#,
    )
    .fetch_optional(pool)
    .await
    .map_err(to_string)?;

    let Some(row) = row else {
        return Ok(None);
    };

    let command = SupervisorCommand {
        id: row.get("id"),
        command_type: row.get("command_type"),
        agent_id: row.get("agent_id"),
        run_id: row.get("run_id"),
        work_item_id: row.get("work_item_id"),
    };

    Ok(Some(command))
}

async fn finish_supervisor_command(
    pool: &SqlitePool,
    command_id: Uuid,
    error: Option<String>,
) -> CommandResult<()> {
    let (status, error) = match error {
        Some(error) => ("failed", error),
        None => ("done", String::new()),
    };

    sqlx::query(
        r#"
        update supervisor_commands
        set status = $2, error = $3, updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
        where id = $1
        "#,
    )
    .bind(command_id)
    .bind(status)
    .bind(error)
    .execute(pool)
    .await
    .map_err(to_string)?;

    Ok(())
}

async fn cleanup_supervisor_commands(pool: &SqlitePool) -> CommandResult<()> {
    sqlx::query(
        r#"
        delete from supervisor_commands
        where status in ('done', 'failed')
          and updated_at < strftime('%Y-%m-%dT%H:%M:%f+00:00','now','-7 days')
        "#,
    )
    .execute(pool)
    .await
    .map_err(to_string)?;
    Ok(())
}

async fn process_supervisor_command(
    pool: &SqlitePool,
    codex_registry: &WarmCodexRegistry,
    claude_registry: &WarmClaudeRegistry,
    command: SupervisorCommand,
) -> CommandResult<()> {
    match command.command_type.as_str() {
        "start_agent" => {
            let Some(agent_id) = command.agent_id else {
                return Err("start_agent command missing agent_id".to_owned());
            };
            supervisor_start_agent(
                pool,
                codex_registry,
                claude_registry,
                agent_id,
                command.work_item_id,
            )
            .await
        }
        "stop_run" => {
            let Some(run_id) = command.run_id else {
                return Err("stop_run command missing run_id".to_owned());
            };
            supervisor_stop_run(pool, codex_registry, run_id).await
        }
        other => Err(format!("unknown supervisor command: {other}")),
    }
}

async fn interrupt_warm_codex_turn(
    pool: &SqlitePool,
    agent_id: Uuid,
    runtime: &Arc<WarmCodexRuntime>,
    run_id: Uuid,
) -> CommandResult<bool> {
    let (request_id, turn_id) = {
        let mut state = runtime.state.lock().await;
        let Some(active) = state.active.as_ref() else {
            return Ok(false);
        };
        if active.run_id != run_id {
            return Ok(false);
        }
        let Some(turn_id) = active.turn_id.clone() else {
            return Ok(false);
        };
        if active.interrupt_request_id.is_some() {
            return Ok(true);
        }
        let request_id = state.next_request_id;
        state.next_request_id += 1;
        state
            .active
            .as_mut()
            .expect("active turn checked")
            .interrupt_request_id = Some(request_id);
        state.last_activity = Instant::now();
        (request_id, turn_id)
    };

    {
        let mut stdin = runtime.stdin.lock().await;
        codex_write_json(
            &mut stdin,
            json!({
                "method": "turn/interrupt",
                "id": request_id,
                "params": {
                    "threadId": runtime.thread_id.clone(),
                    "turnId": turn_id
                }
            }),
        )
        .await?;
    }

    append_run_log(
        pool,
        run_id,
        format!("[codex] turn/interrupt requested id={request_id}\n"),
    )
    .await?;
    record_agent_activity(
        pool,
        Some(agent_id),
        Some(run_id),
        "run",
        "Stop requested",
        turn_id,
    )
    .await?;
    Ok(true)
}

async fn steer_warm_codex_turn_if_same_surface(
    pool: &SqlitePool,
    agent_id: Uuid,
    runtime: &Arc<WarmCodexRuntime>,
    work_item_id: Uuid,
    codex_prompt: &str,
) -> CommandResult<bool> {
    let row = sqlx::query(
        r#"
        select channel_id, thread_root_id, source_kind
        from agent_work_items
        where id = $1 and agent_id = $2 and status = 'queued'
        "#,
    )
    .bind(work_item_id)
    .bind(agent_id)
    .fetch_optional(pool)
    .await
    .map_err(to_string)?;
    let Some(row) = row else {
        return Ok(false);
    };
    let channel_id: Option<Uuid> = row.get("channel_id");
    let thread_root_id: Option<Uuid> = row.get("thread_root_id");
    let source_kind: String = row.get("source_kind");

    let (active_run_id, active_channel_id, active_thread_root_id, active_turn_id) = {
        let state = runtime.state.lock().await;
        let Some(active) = state.active.as_ref() else {
            return Ok(false);
        };
        if active.steer_disabled {
            return Ok(false);
        }
        (
            active.run_id,
            active.channel_id,
            active.thread_root_id,
            active.turn_id.clone(),
        )
    };
    if !same_codex_surface(
        pool,
        channel_id,
        thread_root_id,
        active_channel_id,
        active_thread_root_id,
    )
    .await?
    {
        return Ok(false);
    }
    let Some(turn_id) = active_turn_id else {
        return Ok(false);
    };
    let steer_prompt = if source_kind == "inbox_wake" {
        let items = load_inbox_wake_items_for_work_item(pool, work_item_id).await?;
        if items.is_empty() {
            codex_prompt.to_owned()
        } else {
            build_steer_followup_prompt(&items)
        }
    } else {
        codex_prompt.to_owned()
    };

    let request_id = {
        let mut state = runtime.state.lock().await;
        let Some(active) = state.active.as_mut() else {
            return Ok(false);
        };
        if active.steer_disabled {
            return Ok(false);
        }
        if active.run_id != active_run_id || active.turn_id.as_deref() != Some(turn_id.as_str()) {
            return Ok(false);
        }
        if active.channel_id != active_channel_id || active.thread_root_id != active_thread_root_id
        {
            return Ok(false);
        }
        let run_id = active.run_id;
        let request_id = state.next_request_id;
        state.next_request_id += 1;
        state.last_activity = Instant::now();
        state
            .active
            .as_mut()
            .expect("active turn checked")
            .steer_requests
            .insert(
                request_id,
                CodexSteerRequest {
                    work_item_id,
                    run_id,
                },
            );
        request_id
    };

    sqlx::query(
        r#"
        update agent_work_items
        set status = 'running',
            run_id = $2,
            updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
        where id = $1
        "#,
    )
    .bind(work_item_id)
    .bind(active_run_id)
    .execute(pool)
    .await
    .map_err(to_string)?;
    notify_ui_work_item_changed(pool, work_item_id, "work_item_running").await;

    let write_result = {
        let mut stdin = runtime.stdin.lock().await;
        codex_write_json(
            &mut stdin,
            json!({
                "method": "turn/steer",
                "id": request_id,
                "params": {
                    "threadId": runtime.thread_id.clone(),
                    "expectedTurnId": turn_id,
                    "input": [{
                        "type": "text",
                        "text": steer_prompt,
                        "text_elements": []
                    }]
                }
            }),
        )
        .await
    };

    if let Err(err) = write_result {
        {
            let mut state = runtime.state.lock().await;
            if let Some(active) = state.active.as_mut() {
                active.steer_requests.remove(&request_id);
            }
        }
        sqlx::query(
            r#"
            update agent_work_items
            set status = 'queued',
                run_id = null,
                updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
            where id = $1
            "#,
        )
        .bind(work_item_id)
        .execute(pool)
        .await
        .map_err(to_string)?;
        notify_ui_work_item_changed(pool, work_item_id, "work_item_queued").await;
        return Err(err);
    }

    append_run_log(
        pool,
        active_run_id,
        format!("[codex] turn/steer work_item={work_item_id}\n"),
    )
    .await?;
    Ok(true)
}

async fn supervisor_start_codex_streaming_agent(
    pool: &SqlitePool,
    codex_registry: &WarmCodexRegistry,
    agent_id: Uuid,
    work_item_id: Option<Uuid>,
    handle: String,
    model: String,
    reasoning_effort: String,
    service_tier: String,
    working_directory: String,
    work_item_prompt: String,
    memory_context: Option<String>,
) -> CommandResult<()> {
    let command_text = "codex app-server --listen stdio://".to_owned();
    let codex_prompt = build_codex_streaming_prompt(&work_item_prompt);
    let runtime = get_or_spawn_warm_codex_runtime(
        pool,
        codex_registry,
        agent_id,
        &handle,
        &model,
        &reasoning_effort,
        &service_tier,
        &working_directory,
        memory_context.as_deref(),
    )
    .await?;

    {
        let state = runtime.state.lock().await;
        if !state.alive {
            return Err("codex warm runtime is not alive".to_owned());
        }
        if state.active.is_some() {
            drop(state);
            if let Some(work_item_id) = work_item_id {
                if steer_warm_codex_turn_if_same_surface(
                    pool,
                    agent_id,
                    &runtime,
                    work_item_id,
                    &codex_prompt,
                )
                .await?
                {
                    return Ok(());
                }
            }
            record_agent_activity_throttled(
                pool,
                Some(agent_id),
                None,
                "dispatch",
                "Agent busy",
                work_item_id
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| "no agent request".to_owned()),
            )
            .await?;
            return Ok(());
        }
    }

    let initial_log = if codex_prompt.is_empty() {
        format!("$ {command_text}\n[warm process reused]\n")
    } else {
        format!(
            "$ {command_text}\n[warm process reused]\n\n[streaming agent request]\n{codex_prompt}\n"
        )
    };

    let run_id: Uuid = sqlx::query_scalar(
        r#"
        insert into agent_runs (agent_id, work_item_id, command, working_directory, status, log)
        values ($1, $2, $3, $4, 'starting', $5)
        returning id
        "#,
    )
    .bind(agent_id)
    .bind(work_item_id)
    .bind(&command_text)
    .bind(&working_directory)
    .bind(initial_log)
    .fetch_one(pool)
    .await
    .map_err(to_string)?;
    notify_ui_agent_run_changed(pool, run_id, "run_created").await;

    let (channel_id, thread_root_id) = if let Some(work_item_id) = work_item_id {
        let row = sqlx::query(
            r#"
            select channel_id, thread_root_id
            from agent_work_items
            where id = $1 and agent_id = $2
            "#,
        )
        .bind(work_item_id)
        .bind(agent_id)
        .fetch_one(pool)
        .await
        .map_err(to_string)?;
        sqlx::query(
            r#"
            update agent_work_items
            set status = 'running',
                run_id = $2,
                updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
            where id = $1
            "#,
        )
        .bind(work_item_id)
        .bind(run_id)
        .execute(pool)
        .await
        .map_err(to_string)?;
        notify_ui_work_item_changed(pool, work_item_id, "work_item_running").await;
        record_agent_activity(
            pool,
            Some(agent_id),
            Some(run_id),
            "dispatch",
            "Request started",
            work_item_id.to_string(),
        )
        .await?;
        (
            row.get::<Option<Uuid>, _>("channel_id"),
            row.get::<Option<Uuid>, _>("thread_root_id"),
        )
    } else {
        (None, None)
    };

    sqlx::query("update agent_runs set status = 'running', pid = $2 where id = $1")
        .bind(run_id)
        .bind(runtime.pid)
        .execute(pool)
        .await
        .map_err(to_string)?;
    notify_ui_agent_run_changed(pool, run_id, "run_running").await;
    sqlx::query("update agents set status = 'running' where id = $1")
        .bind(agent_id)
        .execute(pool)
        .await
        .map_err(to_string)?;
    record_agent_activity(
        pool,
        Some(agent_id),
        Some(run_id),
        "run",
        "Started working",
        runtime
            .pid
            .map(|pid| format!("pid={pid}, thread_id={}", runtime.thread_id))
            .unwrap_or_else(|| format!("thread_id={}", runtime.thread_id)),
    )
    .await?;

    let pending_stream_key = codex_pending_stream_key(run_id);
    if let Some(channel_id) = channel_id {
        ensure_streaming_agent_message(
            pool,
            agent_id,
            channel_id,
            thread_root_id,
            &pending_stream_key,
        )
        .await?;
    }

    let request_id = {
        let mut state = runtime.state.lock().await;
        if !state.alive {
            return Err("codex warm runtime exited before turn start".to_owned());
        }
        if state.active.is_some() {
            return Err("codex warm runtime became busy before turn start".to_owned());
        }
        let request_id = state.next_request_id;
        state.next_request_id += 1;
        state.last_activity = Instant::now();
        let mut stream_keys = HashSet::new();
        if channel_id.is_some() {
            stream_keys.insert(pending_stream_key.clone());
        }
        state.active = Some(CodexActiveTurn {
            run_id,
            turn_request_id: request_id,
            turn_id: None,
            started_at: Instant::now(),
            first_delta_at: None,
            work_item_id,
            channel_id,
            thread_root_id,
            stream_keys,
            steer_requests: HashMap::new(),
            steer_disabled: false,
            interrupt_request_id: None,
        });
        request_id
    };

    let cwd = effective_codex_cwd(&working_directory)?;
    let model_value = codex_model_value(&model);
    let write_result = {
        let mut stdin = runtime.stdin.lock().await;
        let mut params = json!({
            "threadId": runtime.thread_id.clone(),
            "input": [{
                "type": "text",
                "text": codex_prompt,
                "text_elements": []
            }],
            "cwd": cwd,
            "approvalPolicy": "never",
            "model": model_value
        });
        apply_codex_runtime_options(&mut params, &reasoning_effort, &service_tier);
        codex_write_json(
            &mut stdin,
            json!({
                "method": "turn/start",
                "id": request_id,
                "params": params
            }),
        )
        .await
    };

    if let Err(err) = write_result {
        finish_warm_codex_active_turn(pool, agent_id, &runtime, false, Some(err.clone())).await?;
        return Err(err);
    }

    Ok(())
}

async fn supervisor_start_claude_streaming_agent(
    pool: &SqlitePool,
    claude_registry: &WarmClaudeRegistry,
    agent_id: Uuid,
    work_item_id: Option<Uuid>,
    handle: String,
    model: String,
    working_directory: String,
    work_item_prompt: String,
    memory_context: Option<String>,
) -> CommandResult<()> {
    let claude_prompt = build_claude_streaming_prompt(&work_item_prompt);
    let model = if model.trim().is_empty() {
        "sonnet".to_owned()
    } else {
        model.trim().to_owned()
    };
    let command_text = claude_streaming_command_text(&model);
    let runtime = match get_or_spawn_warm_claude_runtime(
        pool,
        claude_registry,
        agent_id,
        &handle,
        &model,
        &working_directory,
        memory_context.as_deref(),
    )
    .await
    {
        Ok(runtime) => runtime,
        Err(err) => {
            if let Some(work_item_id) = work_item_id {
                sqlx::query(
                    r#"
                    update agent_work_items
                    set status = 'failed',
                        completed_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now'),
                        updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
                    where id = $1
                    "#,
                )
                .bind(work_item_id)
                .execute(pool)
                .await
                .map_err(to_string)?;
                notify_ui_work_item_changed(pool, work_item_id, "work_item_failed").await;
            }
            return Err(err);
        }
    };

    {
        let state = runtime.state.lock().await;
        if !state.alive {
            return Err("Claude warm runtime is not alive".to_owned());
        }
        if state.active.is_some() {
            drop(state);
            record_agent_activity_throttled(
                pool,
                Some(agent_id),
                None,
                "dispatch",
                "Agent busy",
                work_item_id
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| "no agent request".to_owned()),
            )
            .await?;
            return Ok(());
        }
    }
    let (channel_id, thread_root_id) = if let Some(work_item_id) = work_item_id {
        let row = sqlx::query(
            r#"
            select channel_id, thread_root_id
            from agent_work_items
            where id = $1 and agent_id = $2
            "#,
        )
        .bind(work_item_id)
        .bind(agent_id)
        .fetch_one(pool)
        .await
        .map_err(to_string)?;
        (
            row.get::<Option<Uuid>, _>("channel_id"),
            row.get::<Option<Uuid>, _>("thread_root_id"),
        )
    } else {
        (None, None)
    };
    let current_surface = ClaudeSurface {
        channel_id,
        thread_root_id,
    };
    let surface_boundary = {
        let state = runtime.state.lock().await;
        claude_surface_boundary_marker(state.last_surface, current_surface).unwrap_or_default()
    };
    let claude_prompt = if surface_boundary.is_empty() {
        claude_prompt
    } else {
        format!("{surface_boundary}{claude_prompt}")
    };

    let initial_log = if claude_prompt.is_empty() {
        format!("$ {command_text}\n[warm process reused]\n")
    } else {
        format!(
            "$ {command_text}\n[warm process reused]\n\n[streaming agent request]\n{claude_prompt}\n"
        )
    };

    let run_id: Uuid = sqlx::query_scalar(
        r#"
        insert into agent_runs (agent_id, work_item_id, command, working_directory, status, log)
        values ($1, $2, $3, $4, 'starting', $5)
        returning id
        "#,
    )
    .bind(agent_id)
    .bind(work_item_id)
    .bind(&command_text)
    .bind(&working_directory)
    .bind(initial_log)
    .fetch_one(pool)
    .await
    .map_err(to_string)?;
    notify_ui_agent_run_changed(pool, run_id, "run_created").await;

    if let Some(work_item_id) = work_item_id {
        sqlx::query(
            r#"
            update agent_work_items
            set status = 'running',
                run_id = $2,
                updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
            where id = $1
            "#,
        )
        .bind(work_item_id)
        .bind(run_id)
        .execute(pool)
        .await
        .map_err(to_string)?;
        notify_ui_work_item_changed(pool, work_item_id, "work_item_running").await;
        record_agent_activity(
            pool,
            Some(agent_id),
            Some(run_id),
            "dispatch",
            "Request started",
            work_item_id.to_string(),
        )
        .await?;
    }

    sqlx::query("update agent_runs set status = 'running', pid = $2 where id = $1")
        .bind(run_id)
        .bind(runtime.pid)
        .execute(pool)
        .await
        .map_err(to_string)?;
    notify_ui_agent_run_changed(pool, run_id, "run_running").await;
    sqlx::query("update agents set status = 'running' where id = $1")
        .bind(agent_id)
        .execute(pool)
        .await
        .map_err(to_string)?;
    record_agent_activity(
        pool,
        Some(agent_id),
        Some(run_id),
        "run",
        "Started working",
        runtime
            .pid
            .map(|pid| format!("pid={pid}"))
            .unwrap_or_else(|| "pid unavailable".to_owned()),
    )
    .await?;

    let stream_key = claude_stream_key(run_id);
    if let Some(channel_id) = channel_id {
        ensure_streaming_agent_message(pool, agent_id, channel_id, thread_root_id, &stream_key)
            .await?;
    }
    {
        let mut state = runtime.state.lock().await;
        if !state.alive {
            return Err("Claude warm runtime exited before turn start".to_owned());
        }
        if state.active.is_some() {
            return Err("Claude warm runtime became busy before turn start".to_owned());
        }
        state.last_activity = Instant::now();
        state.active = Some(ClaudeActiveTurn {
            run_id,
            started_at: Instant::now(),
            first_delta_at: None,
            work_item_id,
            channel_id,
            thread_root_id,
            stream_key,
        });
    }

    let write_result = {
        let mut stdin = runtime.stdin.lock().await;
        claude_write_input(&mut stdin, claude_user_input(&claude_prompt)).await
    };

    if let Err(err) = write_result {
        finish_warm_claude_active_turn(pool, agent_id, &runtime, false, Some(err.clone())).await?;
        return Err(err);
    }

    Ok(())
}

fn claude_surface_boundary_marker(
    previous: Option<ClaudeSurface>,
    current: ClaudeSurface,
) -> Option<String> {
    let previous = previous?;
    if previous == current {
        return None;
    }
    Some(format!(
        "\n\n--- Lantor thread boundary ---\nPrevious surface: channel_id={}, thread_root_id={}\nCurrent surface: channel_id={}, thread_root_id={}\nThe warm Claude conversation may contain older turns from the previous surface. Treat this current surface and the injected current-thread context as authoritative. Do not use details from the previous surface unless the current prompt explicitly includes them.\n--- end boundary ---\n\n",
        display_optional_uuid(previous.channel_id),
        display_optional_uuid(previous.thread_root_id),
        display_optional_uuid(current.channel_id),
        display_optional_uuid(current.thread_root_id),
    ))
}

fn display_optional_uuid(id: Option<Uuid>) -> String {
    id.map(|id| id.to_string())
        .unwrap_or_else(|| "none".to_owned())
}

async fn append_claude_thread_context(
    pool: &SqlitePool,
    context: &str,
    channel_id: Option<Uuid>,
    channel_name: Option<&str>,
    thread_root_id: Option<Uuid>,
) -> CommandResult<String> {
    let Some(channel_id) = channel_id else {
        return Ok(context.to_owned());
    };
    let rows = if let Some(thread_root_id) = thread_root_id {
        sqlx::query(
            r#"
            select id, sender_name, sender_role, body, thread_root_id
            from messages
            where channel_id = $1
              and (id = $2 or thread_root_id = $2)
              and length(trim(body)) > 0
            order by created_at desc
            limit $3
            "#,
        )
        .bind(channel_id)
        .bind(thread_root_id)
        .bind(CLAUDE_THREAD_CONTEXT_MESSAGE_LIMIT)
        .fetch_all(pool)
        .await
        .map_err(to_string)?
    } else {
        sqlx::query(
            r#"
            select id, sender_name, sender_role, body, thread_root_id
            from messages
            where channel_id = $1
              and thread_root_id is null
              and length(trim(body)) > 0
            order by created_at desc
            limit $2
            "#,
        )
        .bind(channel_id)
        .bind(CLAUDE_THREAD_CONTEXT_MESSAGE_LIMIT)
        .fetch_all(pool)
        .await
        .map_err(to_string)?
    };
    if rows.is_empty() {
        return Ok(context.to_owned());
    }

    let mut lines = vec![
        String::new(),
        "Same-thread recent context (auto-injected by Lantor):".to_owned(),
        "Use these messages as current-surface evidence. Resolve contextual follow-ups from this block when possible; if it is insufficient, use the Lantor history/search context tools before answering. Do not mix in older warm-runtime turns from other channels or threads unless they are explicitly quoted here.".to_owned(),
    ];
    for row in rows.iter().rev() {
        let message_id: Uuid = row.get("id");
        let row_thread_root_id: Option<Uuid> = row.get("thread_root_id");
        let sender_name: String = row.get("sender_name");
        let sender_role: String = row.get("sender_role");
        let body: String = row.get("body");
        let body = compact_chars_middle(body.trim(), 1_200).replace('\n', " ");
        lines.push(format!(
            "[target={} msg={} type={}] {}: {}",
            claude_context_target(channel_name, channel_id, thread_root_id, row_thread_root_id),
            short_id(message_id),
            sender_role,
            sender_name,
            body,
        ));
    }

    let mut enriched = context.trim().to_owned();
    if !enriched.is_empty() {
        enriched.push('\n');
    }
    enriched.push_str(&lines.join("\n"));
    Ok(enriched)
}

fn claude_context_target(
    channel_name: Option<&str>,
    channel_id: Uuid,
    prompt_thread_root_id: Option<Uuid>,
    row_thread_root_id: Option<Uuid>,
) -> String {
    let channel_label = channel_name
        .filter(|name| !name.trim().is_empty())
        .map(|name| format!("#{name}"))
        .unwrap_or_else(|| channel_id.to_string());
    prompt_thread_root_id
        .or(row_thread_root_id)
        .map(|thread_root_id| format!("{channel_label}:{}", short_id(thread_root_id)))
        .unwrap_or(channel_label)
}

async fn supervisor_start_agent(
    pool: &SqlitePool,
    codex_registry: &WarmCodexRegistry,
    claude_registry: &WarmClaudeRegistry,
    agent_id: Uuid,
    work_item_id: Option<Uuid>,
) -> CommandResult<()> {
    if let Some(work_item_id) = work_item_id {
        let status: Option<String> =
            sqlx::query_scalar("select status from agent_work_items where id = $1")
                .bind(work_item_id)
                .fetch_optional(pool)
                .await
                .map_err(to_string)?;
        if status.as_deref() == Some("cancelled") {
            record_agent_activity(
                pool,
                Some(agent_id),
                None,
                "dispatch",
                "Cancelled agent request skipped",
                work_item_id.to_string(),
            )
            .await?;
            return Ok(());
        }
    }

    if let Some(reason) = agent_budget_exhausted(pool, agent_id).await? {
        if let Some(work_item_id) = work_item_id {
            sqlx::query(
                r#"
                update agent_work_items
                set status = 'failed',
                    completed_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now'),
                    updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
                where id = $1
                "#,
            )
            .bind(work_item_id)
            .execute(pool)
            .await
            .map_err(to_string)?;
            notify_ui_work_item_changed(pool, work_item_id, "budget_exhausted").await;
        }
        record_agent_activity(
            pool,
            Some(agent_id),
            None,
            "usage",
            "Budget reached",
            reason,
        )
        .await?;
        return Ok(());
    }

    let row = sqlx::query(
        r#"
        select handle, runtime, model, reasoning_effort, service_tier, launch_command, working_directory, avatar
        from agents
        where id = $1
        "#,
    )
    .bind(agent_id)
    .fetch_one(pool)
    .await
    .map_err(to_string)?;

    let handle: String = row.get("handle");
    let runtime: String = row.get("runtime");
    let model: String = row.get("model");
    let reasoning_effort: String = row.get("reasoning_effort");
    let service_tier: String = row.get("service_tier");
    let launch_command: String = row.get("launch_command");
    let working_directory: String = row.get::<String, _>("working_directory").trim().to_owned();
    let avatar: Option<String> = row.get("avatar");
    let is_warm_streaming_runtime =
        runtime.eq_ignore_ascii_case("codex") || runtime.eq_ignore_ascii_case("claude");
    let memory_context = match load_agent_memory_context(&working_directory) {
        Ok(memory_context) => memory_context,
        Err(err) => {
            record_agent_activity(
                pool,
                Some(agent_id),
                None,
                "profile",
                "Memory context skipped",
                err,
            )
            .await?;
            None
        }
    };
    let work_item_prompt = match work_item_id {
        Some(work_item_id) => {
            let row = sqlx::query(
                r#"
                select
                    w.channel_id,
                    w.title,
                    w.context,
                    c.name as channel_name,
                    t.number as task_number,
                    w.thread_root_id
                from agent_work_items w
                left join channels c on c.id = w.channel_id
                left join tasks t on t.id = w.task_id
                where w.id = $1 and w.agent_id = $2
                "#,
            )
            .bind(work_item_id)
            .bind(agent_id)
            .fetch_one(pool)
            .await
            .map_err(to_string)?;
            let title: String = row.get("title");
            let context: String = row.get("context");
            let channel_id: Option<Uuid> = row.get("channel_id");
            let channel_name: Option<String> = row.get("channel_name");
            let task_number: Option<i64> = row.get("task_number");
            let thread_root_id: Option<Uuid> = row.get("thread_root_id");
            let context = if runtime.eq_ignore_ascii_case("claude") {
                append_claude_thread_context(
                    pool,
                    &context,
                    channel_id,
                    channel_name.as_deref(),
                    thread_root_id,
                )
                .await?
            } else {
                context
            };
            let available_agents = load_channel_agent_roster(pool, channel_id, agent_id).await?;
            let agent_profile_hint = avatar
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .is_none()
                .then(|| {
                    format!(
                        "Your agent profile currently has no avatar. If your handle or MEMORY.md gives you a stable identity, you may emit one standalone LANTOR_EVENT profile_update with an avatar like `dicebear:dylan:{handle}`. Keep handling the user's request normally and do not send visible chat only for avatar setup."
                    )
                });
            if is_warm_streaming_runtime {
                build_streaming_work_item_prompt(
                    work_item_id,
                    &title,
                    &context,
                    channel_name.as_deref(),
                    task_number,
                    thread_root_id,
                    &available_agents,
                    agent_profile_hint.as_deref(),
                )
            } else {
                build_work_item_prompt(
                    work_item_id,
                    &title,
                    &context,
                    channel_name.as_deref(),
                    task_number,
                    thread_root_id,
                    &available_agents,
                    agent_profile_hint.as_deref(),
                )
            }
        }
        None => String::new(),
    };
    if runtime.eq_ignore_ascii_case("codex") {
        return supervisor_start_codex_streaming_agent(
            pool,
            codex_registry,
            agent_id,
            work_item_id,
            handle,
            model,
            reasoning_effort,
            service_tier,
            working_directory,
            work_item_prompt,
            memory_context,
        )
        .await;
    }
    if runtime.eq_ignore_ascii_case("claude") {
        return supervisor_start_claude_streaming_agent(
            pool,
            claude_registry,
            agent_id,
            work_item_id,
            handle,
            model,
            working_directory,
            work_item_prompt,
            memory_context,
        )
        .await;
    }
    let work_item_prompt = prepend_memory_context(work_item_prompt, memory_context.as_deref());
    let command_text = effective_launch_command(launch_command, runtime, model, handle.clone());
    let initial_log = if work_item_prompt.is_empty() {
        format!("$ {command_text}\n")
    } else {
        format!("$ {command_text}\n\n[agent request]\n{work_item_prompt}\n")
    };

    let run_id: Uuid = sqlx::query_scalar(
        r#"
        insert into agent_runs (agent_id, work_item_id, command, working_directory, status, log)
        values ($1, $2, $3, $4, 'starting', $5)
        returning id
        "#,
    )
    .bind(agent_id)
    .bind(work_item_id)
    .bind(&command_text)
    .bind(&working_directory)
    .bind(initial_log)
    .fetch_one(pool)
    .await
    .map_err(to_string)?;
    notify_ui_agent_run_changed(pool, run_id, "run_created").await;
    if let Some(work_item_id) = work_item_id {
        sqlx::query(
            r#"
            update agent_work_items
            set status = 'running',
                run_id = $2,
                updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
            where id = $1
            "#,
        )
        .bind(work_item_id)
        .bind(run_id)
        .execute(pool)
        .await
        .map_err(to_string)?;
        notify_ui_work_item_changed(pool, work_item_id, "work_item_running").await;
        record_agent_activity(
            pool,
            Some(agent_id),
            Some(run_id),
            "dispatch",
            "Request started",
            work_item_id.to_string(),
        )
        .await?;
    }
    record_agent_activity(
        pool,
        Some(agent_id),
        Some(run_id),
        "run",
        "Run created",
        "Supervisor is preparing the launch command",
    )
    .await?;

    let mut command = Command::new("/bin/zsh");
    command.arg("-lc").arg(&command_text);
    configure_agent_identity_env(&mut command, agent_id, &handle);
    configure_agent_context_tool_env(&mut command);
    let run_id_value = run_id.to_string();
    command.env("LANTOR_RUN_ID", run_id_value);
    let work_item_id_value = work_item_id
        .map(|id| id.to_string())
        .unwrap_or_else(String::new);
    command.env("LANTOR_WORK_ITEM_ID", work_item_id_value);
    command.env("LANTOR_WORK_ITEM_PROMPT", &work_item_prompt);
    #[cfg(unix)]
    command.process_group(0);
    if !working_directory.is_empty() {
        command.current_dir(&working_directory);
    }
    command.stdout(Stdio::piped()).stderr(Stdio::piped());

    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(err) => {
            let error_log = format!("failed to start process: {err}\n");
            sqlx::query(
                r#"
                update agent_runs
                set status = 'failed', log = substr(log || $2, -20000), stopped_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
                where id = $1
                "#,
            )
            .bind(run_id)
            .bind(error_log)
            .execute(pool)
            .await
            .map_err(to_string)?;
            notify_ui_agent_run_changed(pool, run_id, "run_failed").await;
            sqlx::query("update agents set status = 'error' where id = $1")
                .bind(agent_id)
                .execute(pool)
                .await
                .map_err(to_string)?;
            if let Some(work_item_id) = work_item_id {
                sqlx::query(
                    r#"
                    update agent_work_items
                    set status = 'failed',
                        completed_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now'),
                        updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
                    where id = $1
                    "#,
                )
                .bind(work_item_id)
                .execute(pool)
                .await
                .map_err(to_string)?;
                notify_ui_work_item_changed(pool, work_item_id, "work_item_failed").await;
            }
            record_agent_activity(
                pool,
                Some(agent_id),
                Some(run_id),
                "run_error",
                "Run failed to start",
                err.to_string(),
            )
            .await?;
            return Err(err.to_string());
        }
    };

    let pid = child.id().map(|id| id as i32);
    sqlx::query("update agent_runs set status = 'running', pid = $2 where id = $1")
        .bind(run_id)
        .bind(pid)
        .execute(pool)
        .await
        .map_err(to_string)?;
    notify_ui_agent_run_changed(pool, run_id, "run_running").await;
    sqlx::query("update agents set status = 'running' where id = $1")
        .bind(agent_id)
        .execute(pool)
        .await
        .map_err(to_string)?;
    record_agent_activity(
        pool,
        Some(agent_id),
        Some(run_id),
        "run",
        "Run started",
        pid.map(|pid| format!("pid={pid}"))
            .unwrap_or_else(|| "pid unavailable".to_owned()),
    )
    .await?;

    let mut pipe_tasks = Vec::new();
    if let Some(stdout) = child.stdout.take() {
        pipe_tasks.push(tokio::spawn(pipe_run_output(
            pool.clone(),
            agent_id,
            run_id,
            stdout,
            "stdout",
            true,
        )));
    }
    if let Some(stderr) = child.stderr.take() {
        pipe_tasks.push(tokio::spawn(pipe_run_output(
            pool.clone(),
            agent_id,
            run_id,
            stderr,
            "stderr",
            true,
        )));
    }

    tokio::spawn(wait_for_agent_run(
        pool.clone(),
        agent_id,
        run_id,
        work_item_id,
        pipe_tasks,
        child,
    ));

    Ok(())
}

async fn codex_warm_stdout_reader(
    pool: SqlitePool,
    registry: WarmCodexRegistry,
    agent_id: Uuid,
    runtime: Arc<WarmCodexRuntime>,
    reader: BufReader<tokio::process::ChildStdout>,
) {
    let mut lines = reader.lines();
    loop {
        match lines.next_line().await {
            Ok(Some(line)) => {
                if let Err(err) =
                    handle_codex_warm_stdout_line(&pool, agent_id, &runtime, &line).await
                {
                    let _ = record_agent_activity(
                        &pool,
                        Some(agent_id),
                        None,
                        "run_error",
                        "Codex stream event failed",
                        err,
                    )
                    .await;
                }
            }
            Ok(None) => break,
            Err(err) => {
                let _ = record_agent_activity(
                    &pool,
                    Some(agent_id),
                    None,
                    "run_error",
                    "Codex stdout read failed",
                    err.to_string(),
                )
                .await;
                break;
            }
        }
    }

    {
        let mut state = runtime.state.lock().await;
        state.alive = false;
    }
    let _ = finish_warm_codex_active_turn(
        &pool,
        agent_id,
        &runtime,
        false,
        Some("stdout closed".into()),
    )
    .await;
    remove_warm_codex_runtime_if_same(&registry, agent_id, &runtime).await;
}

async fn finish_codex_steer_request(
    pool: &SqlitePool,
    agent_id: Uuid,
    steer: CodexSteerRequest,
    success: bool,
    error: Option<String>,
) -> CommandResult<()> {
    let (status, completed_at, run_id) = if success {
        (
            "done",
            "strftime('%Y-%m-%dT%H:%M:%f+00:00','now')",
            Some(steer.run_id),
        )
    } else {
        ("queued", "null", None)
    };
    sqlx::query(&format!(
        r#"
        update agent_work_items
        set status = $2,
            run_id = $3,
            completed_at = {completed_at},
            updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
        where id = $1
        "#
    ))
    .bind(steer.work_item_id)
    .bind(status)
    .bind(run_id)
    .execute(pool)
    .await
    .map_err(to_string)?;
    notify_ui_work_item_changed(pool, steer.work_item_id, "codex_turn_steer_result").await;

    record_agent_activity(
        pool,
        Some(agent_id),
        Some(steer.run_id),
        if success { "dispatch" } else { "run_error" },
        if success {
            "Follow-up added"
        } else {
            "Follow-up queued"
        },
        error.unwrap_or_else(|| steer.work_item_id.to_string()),
    )
    .await?;
    Ok(())
}

async fn claude_warm_stdout_reader<R>(
    pool: SqlitePool,
    registry: WarmClaudeRegistry,
    agent_id: Uuid,
    runtime: Arc<WarmClaudeRuntime>,
    stream: R,
) where
    R: AsyncRead + Unpin,
{
    let mut lines = BufReader::new(stream).lines();
    loop {
        match lines.next_line().await {
            Ok(Some(line)) => {
                if let Err(err) =
                    handle_claude_warm_stdout_line(&pool, agent_id, &runtime, &line).await
                {
                    let _ = record_agent_activity(
                        &pool,
                        Some(agent_id),
                        None,
                        "run_error",
                        "Claude stream event failed",
                        err,
                    )
                    .await;
                }
            }
            Ok(None) => break,
            Err(err) => {
                let _ = record_agent_activity(
                    &pool,
                    Some(agent_id),
                    None,
                    "run_error",
                    "Claude stdout read failed",
                    err.to_string(),
                )
                .await;
                break;
            }
        }
    }

    {
        let mut state = runtime.state.lock().await;
        state.alive = false;
    }
    let _ = finish_warm_claude_active_turn(
        &pool,
        agent_id,
        &runtime,
        false,
        Some("stdout closed".to_owned()),
    )
    .await;
    remove_warm_claude_runtime_if_same(&registry, agent_id, &runtime).await;
}

async fn handle_claude_warm_stdout_line(
    pool: &SqlitePool,
    agent_id: Uuid,
    runtime: &Arc<WarmClaudeRuntime>,
    line: &str,
) -> CommandResult<()> {
    let active_run_id = {
        runtime
            .state
            .lock()
            .await
            .active
            .as_ref()
            .map(|active| active.run_id)
    };
    if let Some(run_id) = active_run_id {
        append_run_log(pool, run_id, format!("[claude] {line}\n")).await?;
    }
    let value: Value = serde_json::from_str(line).map_err(to_string)?;

    if let (Some(run_id), Some((input_tokens, output_tokens))) =
        (active_run_id, usage_from_runtime_event(&value))
    {
        let _ = record_run_usage(pool, agent_id, run_id, input_tokens, output_tokens, None).await;
    }

    if let Some(session_id) = claude_session_id(&value) {
        {
            let mut state = runtime.state.lock().await;
            state.session_id = Some(session_id.to_owned());
            state.last_activity = Instant::now();
        }
        let status = if active_run_id.is_some() {
            "running"
        } else {
            "idle"
        };
        let _ = upsert_runtime_thread_id(pool, agent_id, "claude", session_id, status).await;
    }

    if let Some((kind, title, detail)) = claude_stream_event_activity(&value) {
        if let Some(run_id) = active_run_id {
            record_agent_activity_throttled(
                pool,
                Some(agent_id),
                Some(run_id),
                kind,
                title,
                detail,
            )
            .await?;
        }
    }

    if let Some(delta) = claude_text_delta(&value) {
        let (active, first_delta_elapsed) = {
            let mut state = runtime.state.lock().await;
            let (active, first_delta_elapsed) = {
                let Some(active) = state.active.as_mut() else {
                    return Ok(());
                };
                let first_delta_elapsed = if active.first_delta_at.is_none() {
                    let elapsed = active.started_at.elapsed();
                    active.first_delta_at = Some(Instant::now());
                    Some(elapsed)
                } else {
                    None
                };
                let active = (
                    active.run_id,
                    active.channel_id,
                    active.thread_root_id,
                    active.stream_key.clone(),
                );
                (active, first_delta_elapsed)
            };
            state.last_activity = Instant::now();
            (active, first_delta_elapsed)
        };
        if let Some(elapsed) = first_delta_elapsed {
            record_agent_activity(
                pool,
                Some(agent_id),
                Some(active.0),
                "acting",
                "Responding",
                format!("first_token_ms={}", elapsed.as_millis()),
            )
            .await?;
        }
        if let Some(channel_id) = active.1 {
            append_streaming_agent_message(pool, agent_id, channel_id, active.2, &active.3, delta)
                .await?;
        }
        return Ok(());
    }

    if let Some(text) = claude_message_text(&value).or_else(|| claude_result_text(&value)) {
        let active = {
            let state = runtime.state.lock().await;
            state.active.as_ref().map(|active| {
                (
                    active.run_id,
                    active.channel_id,
                    active.thread_root_id,
                    active.stream_key.clone(),
                )
            })
        };
        if let Some((_, Some(channel_id), thread_root_id, stream_key)) = active {
            if !streaming_message_exists(pool, &stream_key).await? {
                append_streaming_agent_message(
                    pool,
                    agent_id,
                    channel_id,
                    thread_root_id,
                    &stream_key,
                    &text,
                )
                .await?;
            }
        }
    }

    if let Some(error) = claude_result_error(&value) {
        finish_warm_claude_active_turn(pool, agent_id, runtime, false, Some(error)).await?;
    } else if value.get("type").and_then(Value::as_str) == Some("result") {
        finish_warm_claude_active_turn(pool, agent_id, runtime, true, None).await?;
    }

    Ok(())
}

async fn claude_warm_stderr_reader<R>(
    pool: SqlitePool,
    agent_id: Uuid,
    runtime: Arc<WarmClaudeRuntime>,
    stream: R,
) where
    R: AsyncRead + Unpin,
{
    let mut lines = BufReader::new(stream).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let active_run_id = {
            runtime
                .state
                .lock()
                .await
                .active
                .as_ref()
                .map(|active| active.run_id)
        };
        if let Some(run_id) = active_run_id {
            let _ = append_run_log(&pool, run_id, format!("[stderr] {line}\n")).await;
            if let Some((kind, title, detail)) = classify_agent_output_activity("stderr", &line) {
                let _ = record_agent_activity_throttled(
                    &pool,
                    Some(agent_id),
                    Some(run_id),
                    kind,
                    title,
                    detail,
                )
                .await;
            }
        }
    }
}

async fn handle_codex_warm_stdout_line(
    pool: &SqlitePool,
    agent_id: Uuid,
    runtime: &Arc<WarmCodexRuntime>,
    line: &str,
) -> CommandResult<()> {
    let value: Value = serde_json::from_str(line).map_err(to_string)?;
    let active_run_id = {
        runtime
            .state
            .lock()
            .await
            .active
            .as_ref()
            .map(|active| active.run_id)
    };
    if let Some(run_id) = active_run_id {
        append_run_log(pool, run_id, format!("[codex] {line}\n")).await?;
    }

    if let (Some(run_id), Some((input_tokens, output_tokens))) =
        (active_run_id, usage_from_runtime_event(&value))
    {
        let _ = record_run_usage(pool, agent_id, run_id, input_tokens, output_tokens, None).await;
    }

    if let Some(response_id) = value.get("id").and_then(Value::as_i64) {
        let matched = {
            let mut state = runtime.state.lock().await;
            let Some(active) = state.active.as_mut() else {
                return Ok(());
            };
            if active.turn_request_id == response_id {
                if let Some(turn_id) = codex_turn_id_from_value(&value) {
                    active.turn_id = Some(turn_id);
                    state.last_activity = Instant::now();
                }
                Some((true, None, false))
            } else if let Some(steer) = active.steer_requests.remove(&response_id) {
                if codex_request_error(&value).is_some() {
                    active.steer_disabled = true;
                }
                state.last_activity = Instant::now();
                Some((false, Some(steer), false))
            } else if active.interrupt_request_id == Some(response_id) {
                active.interrupt_request_id = None;
                state.last_activity = Instant::now();
                Some((false, None, true))
            } else {
                None
            }
        };
        if let Some((is_turn_start, steer, is_interrupt)) = matched {
            if is_turn_start {
                if let Some(error) = codex_request_error(&value) {
                    finish_warm_codex_active_turn(pool, agent_id, runtime, false, Some(error))
                        .await?;
                } else if let Some(run_id) = active_run_id {
                    record_agent_activity(
                        pool,
                        Some(agent_id),
                        Some(run_id),
                        "run",
                        "Request acknowledged",
                        response_id.to_string(),
                    )
                    .await?;
                }
                return Ok(());
            }
            if is_interrupt {
                if let Some(error) = codex_request_error(&value) {
                    finish_warm_codex_active_turn(pool, agent_id, runtime, false, Some(error))
                        .await?;
                } else if let Some(run_id) = active_run_id {
                    record_agent_activity(
                        pool,
                        Some(agent_id),
                        Some(run_id),
                        "run",
                        "Stop acknowledged",
                        response_id.to_string(),
                    )
                    .await?;
                }
                return Ok(());
            }
            if let Some(steer) = steer {
                if let Some(error) = codex_request_error(&value) {
                    finish_codex_steer_request(pool, agent_id, steer, false, Some(error)).await?;
                } else {
                    finish_codex_steer_request(pool, agent_id, steer, true, None).await?;
                }
                return Ok(());
            }
        }
    }

    match value.get("method").and_then(Value::as_str) {
        Some("turn/started") => {
            if value.pointer("/params/threadId").and_then(Value::as_str)
                != Some(runtime.thread_id.as_str())
            {
                return Ok(());
            }
            if let Some(turn_id) = codex_turn_id_from_value(&value) {
                let mut state = runtime.state.lock().await;
                if let Some(active) = state.active.as_mut() {
                    active.turn_id = Some(turn_id);
                    state.last_activity = Instant::now();
                }
            }
        }
        Some("item/agentMessage/delta") => {
            let item_id = value
                .pointer("/params/itemId")
                .and_then(Value::as_str)
                .unwrap_or("agent-message");
            let delta = value
                .pointer("/params/delta")
                .and_then(Value::as_str)
                .unwrap_or("");
            if delta.is_empty() {
                return Ok(());
            }
            let (active, first_delta_elapsed) = {
                let mut state = runtime.state.lock().await;
                let Some(active) = state.active.as_mut() else {
                    return Ok(());
                };
                let first_delta_elapsed = if active.first_delta_at.is_none() {
                    let elapsed = active.started_at.elapsed();
                    active.first_delta_at = Some(Instant::now());
                    Some(elapsed)
                } else {
                    None
                };
                let pending_stream_key = codex_pending_stream_key(active.run_id);
                let stream_key = codex_stream_key(active.run_id, item_id);
                active.stream_keys.remove(&pending_stream_key);
                active.stream_keys.insert(stream_key.clone());
                let active = (
                    active.run_id,
                    active.channel_id,
                    active.thread_root_id,
                    pending_stream_key,
                    stream_key,
                );
                (active, first_delta_elapsed)
            };
            if let Some(elapsed) = first_delta_elapsed {
                record_agent_activity(
                    pool,
                    Some(agent_id),
                    Some(active.0),
                    "acting",
                    "Responding",
                    format!("first_token_ms={}", elapsed.as_millis()),
                )
                .await?;
            }
            if let Some(channel_id) = active.1 {
                adopt_streaming_agent_message_key(pool, &active.3, &active.4).await?;
                append_streaming_agent_message(
                    pool, agent_id, channel_id, active.2, &active.4, delta,
                )
                .await?;
            }
        }
        Some("item/completed") if codex_item_type(&value) == Some("agentMessage") => {
            let Some(item_id) = codex_item_id(&value) else {
                return Ok(());
            };
            let active = {
                let mut state = runtime.state.lock().await;
                let Some(active) = state.active.as_mut() else {
                    return Ok(());
                };
                let pending_stream_key = codex_pending_stream_key(active.run_id);
                let stream_key = codex_stream_key(active.run_id, item_id);
                active.stream_keys.remove(&pending_stream_key);
                active.stream_keys.remove(&stream_key);
                (
                    active.run_id,
                    active.channel_id,
                    active.thread_root_id,
                    active.work_item_id,
                    pending_stream_key,
                    stream_key,
                )
            };
            if let Some(channel_id) = active.1 {
                adopt_streaming_agent_message_key(pool, &active.4, &active.5).await?;
                if streaming_message_body_is_empty(pool, &active.5).await? {
                    if let Some(text) = value
                        .pointer("/params/item/text")
                        .and_then(Value::as_str)
                        .filter(|text| !text.is_empty())
                    {
                        append_streaming_agent_message(
                            pool, agent_id, channel_id, active.2, &active.5, text,
                        )
                        .await?;
                    }
                }
                let hidden = consume_streaming_agent_control_lines(
                    pool, agent_id, active.0, active.3, &active.5,
                )
                .await?;
                if !hidden {
                    finish_streaming_agent_message(pool, &active.5, "complete").await?;
                }
            }
        }
        Some("item/completed") => {
            let Some(run_id) = active_run_id else {
                return Ok(());
            };
            if let Some((kind, title, detail)) = codex_tool_completion_activity(&value) {
                record_agent_activity_throttled(
                    pool,
                    Some(agent_id),
                    Some(run_id),
                    kind,
                    title,
                    detail,
                )
                .await?;
            }
        }
        Some("item/started") => {
            let Some(run_id) = active_run_id else {
                return Ok(());
            };
            let (kind, title, detail) = codex_item_started_activity(&value);
            record_agent_activity_throttled(
                pool,
                Some(agent_id),
                Some(run_id),
                kind,
                title,
                detail,
            )
            .await?;
        }
        Some("item/reasoning/textDelta") | Some("item/reasoning/summaryTextDelta") => {
            let Some(run_id) = active_run_id else {
                return Ok(());
            };
            if let Some(delta) = value.pointer("/params/delta").and_then(Value::as_str) {
                if !delta.trim().is_empty() {
                    append_run_log(pool, run_id, format!("[thinking] {delta}\n")).await?;
                }
            }
        }
        Some("turn/completed") => {
            if value.pointer("/params/threadId").and_then(Value::as_str)
                == Some(runtime.thread_id.as_str())
            {
                finish_warm_codex_active_turn(pool, agent_id, runtime, true, None).await?;
            }
        }
        Some("error") => {
            if let Some(detail) = codex_error_notification_detail(&value) {
                finish_warm_codex_active_turn(pool, agent_id, runtime, false, Some(detail)).await?;
            } else {
                let mut state = runtime.state.lock().await;
                state.last_activity = Instant::now();
            }
        }
        _ => {}
    }

    Ok(())
}

async fn codex_warm_stderr_reader<R>(
    pool: SqlitePool,
    agent_id: Uuid,
    runtime: Arc<WarmCodexRuntime>,
    stream: R,
) where
    R: AsyncRead + Unpin,
{
    let mut lines = BufReader::new(stream).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let active_run_id = {
            runtime
                .state
                .lock()
                .await
                .active
                .as_ref()
                .map(|active| active.run_id)
        };
        if let Some(run_id) = active_run_id {
            let _ = append_run_log(&pool, run_id, format!("[stderr] {line}\n")).await;
            if let Some((kind, title, detail)) = classify_agent_output_activity("stderr", &line) {
                let _ = record_agent_activity_throttled(
                    &pool,
                    Some(agent_id),
                    Some(run_id),
                    kind,
                    title,
                    detail,
                )
                .await;
            }
        }
    }
}

async fn wait_for_warm_codex_process(
    pool: SqlitePool,
    registry: WarmCodexRegistry,
    agent_id: Uuid,
    runtime: Arc<WarmCodexRuntime>,
    mut child: tokio::process::Child,
) {
    let wait_result = child.wait().await;
    {
        let mut state = runtime.state.lock().await;
        state.alive = false;
    }
    let detail = match wait_result {
        Ok(status) => format!("codex app-server exited: {status}"),
        Err(err) => format!("codex app-server wait failed: {err}"),
    };
    let _ =
        finish_warm_codex_active_turn(&pool, agent_id, &runtime, false, Some(detail.clone())).await;
    let _ = sqlx::query(
        r#"
        update runtime_sessions
        set status = 'stopped', updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
        where agent_id = $1
          and runtime = 'codex'
          and provider_thread_id = $2
        "#,
    )
    .bind(agent_id)
    .bind(&runtime.thread_id)
    .execute(&pool)
    .await;
    let _ = record_agent_activity(
        &pool,
        Some(agent_id),
        None,
        "run",
        "Codex warm app-server exited",
        detail,
    )
    .await;
    remove_warm_codex_runtime_if_same(&registry, agent_id, &runtime).await;
    let _ = notify_supervisor_wake(&pool).await;
}

async fn remove_warm_codex_runtime_if_same(
    registry: &WarmCodexRegistry,
    agent_id: Uuid,
    runtime: &Arc<WarmCodexRuntime>,
) {
    let mut runtimes = registry.runtimes.lock().await;
    if runtimes
        .get(&agent_id)
        .map(|current| Arc::ptr_eq(current, runtime))
        .unwrap_or(false)
    {
        runtimes.remove(&agent_id);
    }
}

async fn terminate_process_group(pid: i32) -> CommandResult<()> {
    let status = Command::new("kill")
        .arg("-TERM")
        .arg(format!("-{pid}"))
        .status()
        .await
        .map_err(to_string)?;

    if !status.success() {
        return Err(format!("failed to terminate process group {pid}: {status}"));
    }

    Ok(())
}

async fn reap_stuck_codex_runtime(
    pool: &SqlitePool,
    registry: &WarmCodexRegistry,
    agent_id: Uuid,
    runtime: &Arc<WarmCodexRuntime>,
    source: &str,
) -> CommandResult<bool> {
    let (pid, elapsed_ms) = {
        let mut state = runtime.state.lock().await;
        let Some(active) = state.active.as_ref() else {
            return Ok(false);
        };
        let elapsed = active.started_at.elapsed();
        if active.turn_id.is_some() || elapsed < CODEX_TURN_START_TIMEOUT {
            return Ok(false);
        }
        let elapsed_ms = elapsed.as_millis();
        state.alive = false;
        (runtime.pid, elapsed_ms)
    };

    let detail = format!(
        "no turn id after {elapsed_ms} ms; source={source}; process_group={}",
        pid.map(|pid| pid.to_string())
            .unwrap_or_else(|| "unavailable".to_owned())
    );
    finish_warm_codex_active_turn(pool, agent_id, runtime, false, Some(detail.clone())).await?;
    if let Some(pid) = pid {
        if let Err(err) = terminate_process_group(pid).await {
            record_agent_activity(
                pool,
                Some(agent_id),
                None,
                "run_error",
                "Codex zombie turn stop failed",
                err,
            )
            .await?;
        }
    }
    let _ = sqlx::query(
        "update runtime_sessions set status = 'failed', updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now') where agent_id = $1 and runtime = 'codex'",
    )
    .bind(agent_id)
    .execute(pool)
    .await;
    remove_warm_codex_runtime_if_same(registry, agent_id, runtime).await;
    record_agent_activity(
        pool,
        Some(agent_id),
        None,
        "run_error",
        "Codex zombie turn recovered",
        detail,
    )
    .await?;
    let _ = notify_supervisor_wake(pool).await;
    Ok(true)
}

async fn reap_stuck_codex_turn(
    pool: &SqlitePool,
    registry: &WarmCodexRegistry,
    agent_id: Uuid,
    source: &str,
) -> CommandResult<bool> {
    let runtime = {
        let runtimes = registry.runtimes.lock().await;
        runtimes.get(&agent_id).cloned()
    };
    let Some(runtime) = runtime else {
        return Ok(false);
    };
    reap_stuck_codex_runtime(pool, registry, agent_id, &runtime, source).await
}

async fn codex_warm_idle_reaper(
    pool: SqlitePool,
    registry: WarmCodexRegistry,
    agent_id: Uuid,
    runtime: Arc<WarmCodexRuntime>,
) {
    loop {
        sleep(CODEX_IDLE_REAPER_INTERVAL).await;
        let stop_reason = {
            let mut state = runtime.state.lock().await;
            if !state.alive {
                return;
            }
            if let Some(active) = state.active.as_ref() {
                if active.turn_id.is_none()
                    && active.started_at.elapsed() >= CODEX_TURN_START_TIMEOUT
                {
                    Some("zombie")
                } else {
                    None
                }
            } else if state.last_activity.elapsed() >= CODEX_IDLE_TIMEOUT {
                state.alive = false;
                Some("idle")
            } else {
                None
            }
        };
        let Some(stop_reason) = stop_reason else {
            continue;
        };
        if stop_reason == "zombie" {
            let _ =
                reap_stuck_codex_runtime(&pool, &registry, agent_id, &runtime, "idle_reaper").await;
            return;
        }

        let Some(pid) = runtime.pid else {
            return;
        };
        match terminate_process_group(pid).await {
            Ok(()) => {
                let _ = sqlx::query(
                    "update runtime_sessions set status = 'stopping', updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now') where agent_id = $1 and runtime = 'codex'",
                )
                .bind(agent_id)
                .execute(&pool)
                .await;
                let _ = record_agent_activity(
                    &pool,
                    Some(agent_id),
                    None,
                    "run",
                    "Codex warm app-server idle timeout",
                    format!("sent SIGTERM to process_group={pid}"),
                )
                .await;
            }
            Err(err) => {
                {
                    let mut state = runtime.state.lock().await;
                    state.alive = true;
                    state.last_activity = Instant::now();
                }
                let _ = record_agent_activity(
                    &pool,
                    Some(agent_id),
                    None,
                    "run_error",
                    "Codex warm app-server idle stop failed",
                    err,
                )
                .await;
            }
        }
        return;
    }
}

async fn wait_for_warm_claude_process(
    pool: SqlitePool,
    registry: WarmClaudeRegistry,
    agent_id: Uuid,
    runtime: Arc<WarmClaudeRuntime>,
    mut child: tokio::process::Child,
) {
    let wait_result = child.wait().await;
    {
        let mut state = runtime.state.lock().await;
        state.alive = false;
    }
    let detail = match wait_result {
        Ok(status) => format!("claude stream-json exited: {status}"),
        Err(err) => format!("claude stream-json wait failed: {err}"),
    };
    let _ = finish_warm_claude_active_turn(&pool, agent_id, &runtime, false, Some(detail.clone()))
        .await;
    let _ = sqlx::query(
        "update runtime_sessions set status = 'stopped', updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now') where agent_id = $1 and runtime = 'claude'",
    )
    .bind(agent_id)
    .execute(&pool)
    .await;
    let _ = record_agent_activity(
        &pool,
        Some(agent_id),
        None,
        "run",
        "Claude warm stream-json exited",
        detail,
    )
    .await;
    remove_warm_claude_runtime_if_same(&registry, agent_id, &runtime).await;
    let _ = notify_supervisor_wake(&pool).await;
}

async fn remove_warm_claude_runtime_if_same(
    registry: &WarmClaudeRegistry,
    agent_id: Uuid,
    runtime: &Arc<WarmClaudeRuntime>,
) {
    let mut runtimes = registry.runtimes.lock().await;
    if runtimes
        .get(&agent_id)
        .map(|current| Arc::ptr_eq(current, runtime))
        .unwrap_or(false)
    {
        runtimes.remove(&agent_id);
    }
}

async fn claude_warm_idle_reaper(
    pool: SqlitePool,
    agent_id: Uuid,
    runtime: Arc<WarmClaudeRuntime>,
) {
    loop {
        sleep(CODEX_IDLE_REAPER_INTERVAL).await;
        let should_stop = {
            let mut state = runtime.state.lock().await;
            let should_stop = state.alive
                && state.active.is_none()
                && state.last_activity.elapsed() >= CODEX_IDLE_TIMEOUT;
            if should_stop {
                state.alive = false;
            }
            should_stop
        };
        if !should_stop {
            if !runtime.state.lock().await.alive {
                return;
            }
            continue;
        }

        let Some(pid) = runtime.pid else {
            return;
        };
        match terminate_process_group(pid).await {
            Ok(()) => {
                let _ = sqlx::query(
                    "update runtime_sessions set status = 'stopping', updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now') where agent_id = $1 and runtime = 'claude'",
                )
                .bind(agent_id)
                .execute(&pool)
                .await;
                let _ = record_agent_activity(
                    &pool,
                    Some(agent_id),
                    None,
                    "run",
                    "Claude warm stream-json idle timeout",
                    format!("sent SIGTERM to process_group={pid}"),
                )
                .await;
            }
            Err(err) => {
                {
                    let mut state = runtime.state.lock().await;
                    state.alive = true;
                    state.last_activity = Instant::now();
                }
                let _ = record_agent_activity(
                    &pool,
                    Some(agent_id),
                    None,
                    "run_error",
                    "Claude warm stream-json idle stop failed",
                    err,
                )
                .await;
            }
        }
        return;
    }
}

async fn finish_warm_claude_active_turn(
    pool: &SqlitePool,
    agent_id: Uuid,
    runtime: &Arc<WarmClaudeRuntime>,
    success: bool,
    error: Option<String>,
) -> CommandResult<()> {
    let (active, session_id) = {
        let mut state = runtime.state.lock().await;
        state.last_activity = Instant::now();
        let active = state.active.take();
        if let Some(active) = active.as_ref() {
            state.last_surface = Some(ClaudeSurface {
                channel_id: active.channel_id,
                thread_root_id: active.thread_root_id,
            });
        }
        let session_id = state.session_id.clone();
        (active, session_id)
    };
    let Some(active) = active else {
        return Ok(());
    };
    let elapsed_ms = active.started_at.elapsed().as_millis();
    let current_work_status: Option<String> = match active.work_item_id {
        Some(work_item_id) => {
            sqlx::query_scalar("select status from agent_work_items where id = $1")
                .bind(work_item_id)
                .fetch_optional(pool)
                .await
                .map_err(to_string)?
        }
        None => None,
    };
    let was_cancelled = current_work_status.as_deref() == Some("cancelling");

    let hidden = if success && !was_cancelled {
        consume_streaming_agent_control_lines(
            pool,
            agent_id,
            active.run_id,
            active.work_item_id,
            &active.stream_key,
        )
        .await?
    } else {
        false
    };
    if !hidden {
        finish_streaming_agent_message(
            pool,
            &active.stream_key,
            if success && !was_cancelled {
                "complete"
            } else {
                "error"
            },
        )
        .await?;
    }

    let run_status = if was_cancelled {
        "cancelled"
    } else if success {
        "exited"
    } else {
        "failed"
    };
    let agent_status = if success || was_cancelled {
        "idle"
    } else {
        "error"
    };
    let log_line = if was_cancelled {
        "claude warm turn cancelled\n".to_owned()
    } else {
        error
            .as_ref()
            .map(|error| format!("claude warm turn failed: {error}\n"))
            .unwrap_or_else(|| format!("claude warm turn completed in {elapsed_ms} ms\n"))
    };
    sqlx::query(
        r#"
        update agent_runs
        set status = $2,
            exit_code = null,
            log = substr(log || $3, -20000),
            stopped_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
        where id = $1
        "#,
    )
    .bind(active.run_id)
    .bind(run_status)
    .bind(&log_line)
    .execute(pool)
    .await
    .map_err(to_string)?;
    notify_ui_agent_run_changed(pool, active.run_id, "claude_turn_finished").await;

    sqlx::query("update agents set status = $2 where id = $1")
        .bind(agent_id)
        .bind(agent_status)
        .execute(pool)
        .await
        .map_err(to_string)?;

    if let Some(work_item_id) = active.work_item_id {
        let current_work_status: Option<String> =
            sqlx::query_scalar("select status from agent_work_items where id = $1")
                .bind(work_item_id)
                .fetch_optional(pool)
                .await
                .map_err(to_string)?;
        let was_silent = current_work_status.as_deref() == Some("silent");
        let work_status = if was_cancelled {
            "cancelled"
        } else if was_silent && success {
            "silent"
        } else if success {
            "done"
        } else {
            "failed"
        };
        sqlx::query(
            r#"
            update agent_work_items
            set status = $2,
                completed_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now'),
                updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
            where id = $1
            "#,
        )
        .bind(work_item_id)
        .bind(work_status)
        .execute(pool)
        .await
        .map_err(to_string)?;
        mark_task_after_work_item_finished(
            pool,
            work_item_id,
            agent_id,
            active.run_id,
            work_status,
        )
        .await?;
        notify_ui_work_item_changed(pool, work_item_id, "work_item_finished").await;
        record_agent_activity(
            pool,
            Some(agent_id),
            Some(active.run_id),
            "dispatch",
            work_status_title(work_status),
            work_item_id.to_string(),
        )
        .await?;
    }

    let provider_thread_id = session_id.unwrap_or_else(|| {
        runtime
            .pid
            .map(|pid| format!("pid:{pid}"))
            .unwrap_or_else(|| "unknown".to_owned())
    });
    upsert_runtime_thread_id(
        pool,
        agent_id,
        "claude",
        &provider_thread_id,
        if success || was_cancelled {
            "idle"
        } else {
            "failed"
        },
    )
    .await?;
    record_agent_activity(
        pool,
        Some(agent_id),
        Some(active.run_id),
        if success || was_cancelled {
            "run"
        } else {
            "run_error"
        },
        if was_cancelled {
            "Stopped"
        } else if success {
            "Completed"
        } else {
            "Failed"
        },
        if success || was_cancelled {
            format!("duration={elapsed_ms} ms")
        } else {
            log_line.trim().to_owned()
        },
    )
    .await?;
    let _ = notify_supervisor_wake(pool).await;
    Ok(())
}

async fn finish_warm_codex_active_turn(
    pool: &SqlitePool,
    agent_id: Uuid,
    runtime: &Arc<WarmCodexRuntime>,
    success: bool,
    error: Option<String>,
) -> CommandResult<()> {
    let active = {
        let mut state = runtime.state.lock().await;
        state.last_activity = Instant::now();
        state.active.take()
    };
    let Some(active) = active else {
        return Ok(());
    };
    let elapsed_ms = active.started_at.elapsed().as_millis();
    let current_work_status: Option<String> = match active.work_item_id {
        Some(work_item_id) => {
            sqlx::query_scalar("select status from agent_work_items where id = $1")
                .bind(work_item_id)
                .fetch_optional(pool)
                .await
                .map_err(to_string)?
        }
        None => None,
    };
    let was_cancelled = current_work_status.as_deref() == Some("cancelling");

    for stream_key in active.stream_keys {
        let hidden = if success && !was_cancelled {
            consume_streaming_agent_control_lines(
                pool,
                agent_id,
                active.run_id,
                active.work_item_id,
                &stream_key,
            )
            .await?
        } else {
            false
        };
        if !hidden {
            finish_streaming_agent_message(
                pool,
                &stream_key,
                if success && !was_cancelled {
                    "complete"
                } else {
                    "error"
                },
            )
            .await?;
        }
    }

    for steer in active.steer_requests.into_values() {
        finish_codex_steer_request(
            pool,
            agent_id,
            steer,
            success && !was_cancelled,
            if was_cancelled {
                Some("cancelled".to_owned())
            } else {
                error.clone()
            },
        )
        .await?;
    }

    let run_status = if was_cancelled {
        "cancelled"
    } else if success {
        "exited"
    } else {
        "failed"
    };
    let agent_status = if success || was_cancelled {
        "idle"
    } else {
        "error"
    };
    let log_line = if was_cancelled {
        "codex warm turn cancelled\n".to_owned()
    } else {
        error
            .as_ref()
            .map(|error| format!("codex warm turn failed: {error}\n"))
            .unwrap_or_else(|| format!("codex warm turn completed in {elapsed_ms} ms\n"))
    };
    sqlx::query(
        r#"
        update agent_runs
        set status = $2,
            exit_code = null,
            log = substr(log || $3, -20000),
            stopped_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
        where id = $1
        "#,
    )
    .bind(active.run_id)
    .bind(run_status)
    .bind(&log_line)
    .execute(pool)
    .await
    .map_err(to_string)?;
    notify_ui_agent_run_changed(pool, active.run_id, "codex_turn_finished").await;

    sqlx::query("update agents set status = $2 where id = $1")
        .bind(agent_id)
        .bind(agent_status)
        .execute(pool)
        .await
        .map_err(to_string)?;

    if let Some(work_item_id) = active.work_item_id {
        let current_work_status: Option<String> =
            sqlx::query_scalar("select status from agent_work_items where id = $1")
                .bind(work_item_id)
                .fetch_optional(pool)
                .await
                .map_err(to_string)?;
        let was_silent = current_work_status.as_deref() == Some("silent");
        let work_status = if was_cancelled {
            "cancelled"
        } else if was_silent && success {
            "silent"
        } else if success {
            "done"
        } else {
            "failed"
        };
        sqlx::query(
            r#"
            update agent_work_items
            set status = $2,
                completed_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now'),
                updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
            where id = $1
            "#,
        )
        .bind(work_item_id)
        .bind(work_status)
        .execute(pool)
        .await
        .map_err(to_string)?;
        mark_task_after_work_item_finished(
            pool,
            work_item_id,
            agent_id,
            active.run_id,
            work_status,
        )
        .await?;
        notify_ui_work_item_changed(pool, work_item_id, "work_item_finished").await;
        record_agent_activity(
            pool,
            Some(agent_id),
            Some(active.run_id),
            "dispatch",
            work_status_title(work_status),
            work_item_id.to_string(),
        )
        .await?;
    }

    upsert_runtime_thread_id(
        pool,
        agent_id,
        "codex",
        &runtime.thread_id,
        if success || was_cancelled {
            "idle"
        } else {
            "failed"
        },
    )
    .await?;
    record_agent_activity(
        pool,
        Some(agent_id),
        Some(active.run_id),
        if success || was_cancelled {
            "run"
        } else {
            "run_error"
        },
        if was_cancelled {
            "Stopped"
        } else if success {
            "Completed"
        } else {
            "Failed"
        },
        if success || was_cancelled {
            format!("duration={elapsed_ms} ms")
        } else {
            log_line.trim().to_owned()
        },
    )
    .await?;
    let _ = notify_supervisor_wake(pool).await;
    Ok(())
}

async fn supervisor_stop_run(
    pool: &SqlitePool,
    codex_registry: &WarmCodexRegistry,
    run_id: Uuid,
) -> CommandResult<()> {
    let row = sqlx::query(
        r#"
        select r.agent_id, r.pid, r.work_item_id, a.runtime
        from agent_runs r
        join agents a on a.id = r.agent_id
        where r.id = $1 and r.stopped_at is null
        "#,
    )
    .bind(run_id)
    .fetch_one(pool)
    .await
    .map_err(to_string)?;

    let agent_id: Uuid = row.get("agent_id");
    let pid: Option<i32> = row.get("pid");
    let work_item_id: Option<Uuid> = row.get("work_item_id");
    let runtime: String = row.get("runtime");
    let Some(pid) = pid else {
        return Err("agent run does not have a pid yet".to_owned());
    };

    sqlx::query("update agent_runs set status = 'stopping' where id = $1")
        .bind(run_id)
        .execute(pool)
        .await
        .map_err(to_string)?;
    notify_ui_agent_run_changed(pool, run_id, "run_stopping").await;
    sqlx::query("update agents set status = 'stopping' where id = $1")
        .bind(agent_id)
        .execute(pool)
        .await
        .map_err(to_string)?;
    if let Some(work_item_id) = work_item_id {
        sqlx::query(
            r#"
            update agent_work_items
            set status = 'cancelling',
                updated_at = strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
            where id = $1 and status in ('queued', 'running')
            "#,
        )
        .bind(work_item_id)
        .execute(pool)
        .await
        .map_err(to_string)?;
        notify_ui_work_item_changed(pool, work_item_id, "work_item_cancelling").await;
    }

    if runtime.eq_ignore_ascii_case("codex") {
        let warm_runtime = {
            let runtimes = codex_registry.runtimes.lock().await;
            runtimes.get(&agent_id).cloned()
        };
        if let Some(runtime) = warm_runtime {
            if interrupt_warm_codex_turn(pool, agent_id, &runtime, run_id).await? {
                record_agent_activity(
                    pool,
                    Some(agent_id),
                    Some(run_id),
                    "run",
                    "Stop requested",
                    "Codex accepted stop request",
                )
                .await?;
                return Ok(());
            }
        }
    }

    terminate_process_group(pid).await?;

    append_run_log(
        pool,
        run_id,
        format!("sent SIGTERM to process group {pid}\n"),
    )
    .await?;
    record_agent_activity(
        pool,
        Some(agent_id),
        Some(run_id),
        "run",
        "Stop requested",
        format!("process_group={pid}"),
    )
    .await?;
    Ok(())
}

fn normalize_channel_name(name: &str) -> String {
    name.trim()
        .trim_start_matches('#')
        .to_lowercase()
        .replace(' ', "-")
}

pub(crate) fn to_string(error: impl std::fmt::Display) -> String {
    error.to_string()
}

pub fn run() {
    let database_url = db_url();
    let pool = tauri::async_runtime::block_on(db_connect_with_url(&database_url, 5))
        .expect("failed to connect Lantor SQLite database");

    tauri::async_runtime::block_on(migrate(&pool)).expect("failed to initialize Lantor schema");
    launch_agent::spawn_supervisor_process(&database_url);
    let state_db_url = database_url.clone();
    let reminder_pool = pool.clone();

    tauri::Builder::default()
        .manage(AppState {
            pool,
            db_url: state_db_url,
        })
        .setup(move |app| {
            spawn_ui_refresh_listener(app.handle().clone(), reminder_pool.clone());
            web::spawn_web_server_if_configured(reminder_pool.clone(), database_url.clone());
            spawn_reminder_worker(reminder_pool.clone());
            if let Some(window) = app.get_webview_window("main") {
                let _ = window.set_title("Lantor");
            }
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            agent_workspace_list,
            agent_workspace_read_file,
            bootstrap,
            artifact_read,
            cancel_agent_work,
            cancel_reminder,
            check_runtime,
            complete_reminder,
            create_agent,
            create_agent_schedule,
            create_channel,
            create_reminder,
            claim_task,
            delete_agent,
            delete_channel,
            delete_message,
            dispatch_agent_work,
            install_supervisor_service,
            dismiss_inbox_items,
            mark_inbox_items_read,
            mark_all_inbox_read,
            mark_channel_read,
            open_dm_with_agent,
            open_external_url,
            retry_agent_work,
            send_message,
            set_message_saved,
            set_channel_agent_membership,
            snooze_reminder,
            start_agent,
            stop_agent,
            uninstall_supervisor_service,
            update_agent,
            update_agent_schedule_status,
            update_channel,
            update_message,
            update_owner_profile,
            update_thread_followed,
            update_task_title,
            update_task_status
        ])
        .run(tauri::generate_context!())
        .expect("error while running Lantor");
}

fn main() {
    let args = env::args().collect::<Vec<_>>();
    if let Some(tool_arg_index) = args.iter().position(|arg| arg == "--agent-context-tool") {
        let tool_args = args
            .get(tool_arg_index + 1..)
            .map(|args| args.to_vec())
            .unwrap_or_default();
        match tauri::async_runtime::block_on(run_agent_context_tool(&tool_args)) {
            Ok(output) => {
                println!("{output}");
                return;
            }
            Err(err) => {
                eprintln!("{err}");
                std::process::exit(1);
            }
        }
    }

    if args.iter().any(|arg| arg == "--supervisor") {
        if let Err(err) = tauri::async_runtime::block_on(run_supervisor()) {
            eprintln!("Lantor supervisor stopped: {err}");
            std::process::exit(1);
        }
    } else {
        run();
    }
}

#[cfg(test)]
mod tests {
    use super::{
        activity_status, adopt_streaming_agent_message_key, append_claude_thread_context,
        append_streaming_agent_message, build_codex_streaming_prompt, build_steer_followup_prompt,
        build_streaming_work_item_prompt, build_work_item_prompt, capped_stream_delta,
        claim_agent_event, claim_next_supervisor_command, classify_agent_output_activity,
        claude_message_text, claude_result_error, claude_stream_event_activity,
        claude_surface_boundary_marker, claude_system_prompt, claude_text_delta,
        codex_active_turn_schedule_state, codex_context_rotate_input_tokens_from_env,
        codex_error_notification_detail, codex_item_started_activity, codex_pending_stream_key,
        codex_turn_id_from_value, consume_streaming_agent_control_lines,
        context_tool::{
            agent_context_agent_inspect, agent_context_artifact_read_in_pool,
            agent_context_attachment_info, agent_context_history_read, agent_context_inbox_archive,
            agent_context_inbox_list, agent_context_inbox_read, agent_context_memory_read,
            agent_context_message_search, agent_context_workspace_info,
            agent_context_workspace_list, short_id,
        },
        create_agent_inbox_item, db_connect_with_url, delete_agent_in_pool, delete_channel_in_pool,
        dismiss_inbox_items_in_pool, ensure_agent_workspace, ensure_streaming_agent_message,
        extract_agent_event_json, extract_agent_mentions, finish_streaming_agent_message,
        format_memory_index_entry, handle_agent_event, inbox_wake_context, insert_agent_message,
        insert_memory_index_entry, load_agent_memory_context, load_channel_agent_roster,
        load_messages, load_reminders, load_runtime_thread_id, mark_all_owner_inbox_read_in_pool,
        mark_inbox_items_read_in_pool, maybe_hide_silent_streaming_reply, migrate,
        normalize_open_link_target, notify_ui_work_item_changed, open_dm_with_agent_in_pool,
        parse_activity_metadata, process_due_agent_schedules, process_due_reminders,
        queue_mentions_as_work_items, record_agent_activity,
        recover_supervisor_commands_at_startup, send_owner_message_in_pool, silent_reply_reason,
        split_streaming_agent_event_lines, streaming_message_body_is_empty,
        try_claim_unassigned_task, upsert_agent_thread_subscription, upsert_runtime_thread_id,
        usage::{usage_from_run_log, usage_from_runtime_event},
        AgentAttachmentFile, AgentEvent, AgentInboxItemInput, ClaudeSurface,
        CodexActiveTurnScheduleState, InboxWakeItem, InboxWakeSummary, MentionDispatchOrigin,
        AGENT_MEMORY_CONTEXT_LIMIT, CODEX_CONTEXT_ROTATE_DEFAULT_INPUT_TOKENS,
        CODEX_TURN_START_TIMEOUT, STREAMING_MESSAGE_BODY_LIMIT, STREAMING_TRUNCATION_MARKER,
        WORK_ITEM_FINISH_PROMPT,
    };
    use chrono::{DateTime, Duration as ChronoDuration, Utc};
    use serde_json::{json, Value};
    use sqlx::{Row, SqlitePool};
    use std::{fs as std_fs, time::Duration};
    use uuid::Uuid;

    #[test]
    fn extracts_unique_agent_mentions() {
        let mentions = extract_agent_mentions("ping @Hancock and @agent-2, then @Hancock again");
        assert_eq!(mentions, vec!["Hancock", "agent-2"]);
    }

    #[test]
    fn ignores_empty_or_email_like_at_signs() {
        let mentions = extract_agent_mentions("email a@b.com and a lone @ sign");
        assert!(mentions.is_empty());
    }

    #[test]
    fn open_link_target_normalization_allows_safe_schemes() {
        assert_eq!(
            normalize_open_link_target(" https://example.com/path?q=1 "),
            Some("https://example.com/path?q=1".to_owned())
        );
        assert_eq!(
            normalize_open_link_target("mailto:hello@example.com"),
            Some("mailto:hello@example.com".to_owned())
        );
        assert_eq!(
            normalize_open_link_target("file:///tmp/report.txt"),
            Some("file:///tmp/report.txt".to_owned())
        );
        assert!(normalize_open_link_target("javascript:alert(1)").is_none());
        assert!(normalize_open_link_target("https://example.com/\nopen").is_none());
    }

    #[test]
    fn open_link_target_normalization_allows_existing_local_paths_with_line_suffixes() {
        let dir = std::env::temp_dir().join(format!("lantor-link-test-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("main.rs");
        std::fs::write(&file, "fn main() {}\n").unwrap();
        let file = file.to_string_lossy().to_string();

        assert_eq!(normalize_open_link_target(&file), Some(file.clone()));
        assert_eq!(
            normalize_open_link_target(&format!("{file}:42")),
            Some(file.clone())
        );
        assert_eq!(
            normalize_open_link_target(&format!("{file}#L42")),
            Some(file.clone())
        );
        assert!(normalize_open_link_target("/definitely/not/a/lantor/file.rs:1").is_none());

        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn memory_context_is_bounded_and_preserves_tail() {
        let dir = std::env::temp_dir().join(format!("lantor-memory-test-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("create temp memory dir");
        let memory_path = dir.join("MEMORY.md");
        let memory = format!(
            "# Agent\n\n{}\n\n## Active Context\nimportant tail survives",
            "context ".repeat(AGENT_MEMORY_CONTEXT_LIMIT)
        );
        std::fs::write(&memory_path, memory).expect("write memory");

        let context =
            load_agent_memory_context(dir.to_str().expect("utf8 temp dir")).expect("load memory");
        std::fs::remove_dir_all(&dir).ok();

        let context = context.expect("memory should load");
        assert!(context.contains("Lantor omitted"));
        assert!(context.contains("important tail survives"));
        assert!(context.chars().count() < AGENT_MEMORY_CONTEXT_LIMIT + 1_000);
    }

    #[test]
    fn runtime_standing_prompt_carries_memory_once() {
        let prompt =
            claude_system_prompt("tester", Some("Persistent memory: prefer concise replies"));
        assert!(prompt.contains("one warm runtime session per agent"));
        assert!(prompt.contains("channel and thread are delivered as message envelope fields"));
        assert!(prompt.contains("Treat messages as conversation"));
        assert!(prompt.contains("Activity events are the short progress notes"));
        assert!(prompt.contains("MEMORY.md is the compact index"));
        assert!(prompt.contains("raw conversation/tool logs should stay out of memory"));
        assert!(prompt.contains("notes/<topic>.md"));
        assert!(prompt.contains("not replay past turns"));
        assert!(prompt.contains("timestamp-log-like"));
        assert!(prompt.contains("stable user preferences"));
        assert!(prompt.contains("Before long-running work, update Active Context"));
        assert!(prompt.contains("Turn startup sequence:"));
        assert!(
            prompt.contains("Use history-read or message-search when older channel/thread context")
        );
        assert!(prompt.contains("Reply briefly to direct greetings"));
        assert!(prompt.contains("Agent context tools"));
        assert!(prompt.contains("inbox-list"));
        assert!(prompt.contains("[target=... msg=... time=... type=...]"));
        assert!(prompt.contains("Live inbox delivery"));
        assert!(prompt.contains("Persistent memory: prefer concise replies"));
    }

    #[test]
    fn ensure_agent_workspace_creates_index_memory_template_and_notes_dir() {
        let dir = std::env::temp_dir().join(format!("lantor-memory-template-{}", Uuid::new_v4()));
        ensure_agent_workspace(dir.to_str().expect("utf8 temp dir"), "template-agent")
            .expect("ensure workspace");

        let memory = std::fs::read_to_string(dir.join("MEMORY.md")).expect("read memory");
        assert!(dir.join("notes").is_dir());
        assert!(memory.contains("# @template-agent"));
        assert!(memory.contains("## Key Knowledge"));
        assert!(memory.contains("## Memory Map"));
        assert!(memory.contains("notes/user-preferences.md"));
        assert!(memory.contains("notes/work-log.md"));
        assert!(memory.contains("## Active Context"));
        assert!(memory.contains("Keep this file concise and index-like"));
        assert!(memory.contains("Do not use MEMORY.md as a chronological log"));

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn memory_append_can_add_work_log_link_without_timestamp_log() {
        let memory = "# @agent\n\n## Role\nLantor agent.\n\n## Key Knowledge\n- Add stable facts and links that help a restarted agent recover quickly.\n\n## Active Context\n- Currently working on: none.";

        let updated = insert_memory_index_entry(
            memory,
            &format_memory_index_entry("`notes/work-log.md` - staged durable updates."),
        );

        assert!(
            updated.contains("## Key Knowledge\n- `notes/work-log.md` - staged durable updates.")
        );
        assert!(updated.contains("\n## Active Context"));
        assert!(!updated.contains("Memory update"));
        assert!(!updated.contains("Add stable facts and links"));
    }

    #[test]
    fn codex_context_rotation_threshold_is_configurable_with_floor() {
        assert_eq!(
            codex_context_rotate_input_tokens_from_env(None),
            CODEX_CONTEXT_ROTATE_DEFAULT_INPUT_TOKENS
        );
        assert_eq!(
            codex_context_rotate_input_tokens_from_env(Some("220000")),
            220_000
        );
        assert_eq!(
            codex_context_rotate_input_tokens_from_env(Some("49999")),
            CODEX_CONTEXT_ROTATE_DEFAULT_INPUT_TOKENS
        );
        assert_eq!(
            codex_context_rotate_input_tokens_from_env(Some("not-a-number")),
            CODEX_CONTEXT_ROTATE_DEFAULT_INPUT_TOKENS
        );
    }

    #[test]
    fn streaming_prompt_replaces_stdout_finish_contract() {
        let prompt = build_work_item_prompt(
            Uuid::nil(),
            "Review the change",
            "Latest user message: please review",
            Some("lantor"),
            None,
            Some(Uuid::nil()),
            &[],
            None,
        );
        assert!(prompt.contains("Treat messages as conversation"));
        assert!(prompt.contains(WORK_ITEM_FINISH_PROMPT));

        let streaming = build_codex_streaming_prompt(&prompt);
        assert!(streaming.contains("will stream your Codex assistant text"));
        assert!(streaming.contains("Reply briefly to direct greetings"));
        assert!(streaming.contains("pure acknowledgement"));
        assert!(streaming.contains("you may emit standalone LANTOR_EVENT control lines"));
        assert!(streaming.contains("Activity progress: before your final reply"));
        assert!(streaming.contains("activity is not only for reasoning"));
        assert!(streaming.contains("what you are doing or what you just learned"));
        assert!(streaming.contains("artifact_create"));
        assert!(streaming.contains("attachment_create"));
        assert!(streaming.contains("channel_message_create"));
        assert!(streaming.contains("handoff_create"));
        assert!(streaming.contains("task_handoff"));
        assert!(streaming.contains("task_claim"));
        assert!(streaming.contains("Do not narrate every intermediate step in chat"));
        assert!(!streaming.contains(WORK_ITEM_FINISH_PROMPT));
    }

    #[test]
    fn streaming_work_item_prompt_omits_repeated_standing_context() {
        let prompt = build_streaming_work_item_prompt(
            Uuid::nil(),
            "Review the change",
            "Latest user message: please review",
            Some("lantor"),
            None,
            Some(Uuid::nil()),
            &[],
            None,
        );

        assert!(prompt.contains("Standing instructions are already installed"));
        assert!(prompt.contains("authoritative over older warm-runtime context"));
        assert!(prompt.contains("Same-channel/thread follow-ups may be delivered"));
        assert!(prompt.contains("Latest user message: please review"));
        assert!(prompt.contains(WORK_ITEM_FINISH_PROMPT));
        assert!(!prompt.contains("Operating policy:"));
        assert!(!prompt.contains("Agent context tools:"));
        assert!(!prompt.contains("Standalone LANTOR_EVENT control lines:"));
    }

    #[test]
    fn work_item_prompt_includes_agent_profile_hint_when_present() {
        let prompt = build_work_item_prompt(
            Uuid::nil(),
            "Handle inbox",
            "Latest user message: hello",
            Some("lantor"),
            None,
            Some(Uuid::nil()),
            &[],
            Some("Pick a stable DiceBear avatar if the profile is empty."),
        );

        assert!(prompt.contains("agent_profile_hint:"));
        assert!(prompt.contains("Pick a stable DiceBear avatar if the profile is empty."));
    }

    #[test]
    fn inbox_wake_context_includes_message_headers_and_other_active_summary() {
        let channel_id = Uuid::new_v4();
        let thread_root_id = Uuid::new_v4();
        let source_message_id = Uuid::new_v4();
        let inbox_id = Uuid::new_v4();
        let context = inbox_wake_context(
            &[InboxWakeItem {
                id: inbox_id,
                channel_id: Some(channel_id),
                channel_name: Some("support".to_owned()),
                channel_kind: Some("channel".to_owned()),
                thread_root_id: Some(thread_root_id),
                source_message_id: Some(source_message_id),
                task_id: None,
                kind: "owner_thread_followup".to_owned(),
                priority: 90,
                title: "Handle follow-up".to_owned(),
                body_preview: "please use the latest numbers\nand reply directly".to_owned(),
                message_created_at: Some(Utc::now()),
                sender_name: Some("Dylan".to_owned()),
                sender_role: Some("owner".to_owned()),
            }],
            &[InboxWakeSummary {
                target: "dm:Hancock".to_owned(),
                count: 2,
            }],
        );

        assert!(context.contains("[target=#support:"));
        assert!(context.contains(&format!("msg={}", short_id(source_message_id))));
        assert!(context.contains("type=owner"));
        assert!(context.contains("Dylan: please use the latest numbers and reply directly"));
        assert!(context.contains("Warm-runtime guard"));
        assert!(context.contains("use history-read on the default reply target"));
        assert!(context.contains(&format!("inbox_id: {inbox_id}")));
        assert!(context.contains("Other active inbox targets:"));
        assert!(context.contains("- dm:Hancock: 2 active"));
    }

    #[test]
    fn inbox_wake_context_tells_task_available_agents_to_claim_silently() {
        let context = inbox_wake_context(
            &[InboxWakeItem {
                id: Uuid::new_v4(),
                channel_id: Some(Uuid::new_v4()),
                channel_name: Some("builders".to_owned()),
                channel_kind: Some("channel".to_owned()),
                thread_root_id: Some(Uuid::new_v4()),
                source_message_id: Some(Uuid::new_v4()),
                task_id: Some(Uuid::new_v4()),
                kind: "task_available".to_owned(),
                priority: 70,
                title: "Implement queue behavior".to_owned(),
                body_preview: "Implement queue behavior".to_owned(),
                message_created_at: Some(Utc::now()),
                sender_name: Some("Dylan".to_owned()),
                sender_role: Some("owner".to_owned()),
            }],
            &[],
        );

        assert!(context.contains("Task claim opportunity mode:"));
        assert!(context.contains("competitive, unassigned task opportunity"));
        assert!(context.contains(r#"LANTOR_EVENT {"type":"task_claim","task_number":...}"#));
        assert!(context.contains("LANTOR_SILENT_REPLY: claim attempted"));
        assert!(context.contains("do not emit activity events"));
        assert!(context.contains("task_assigned inbox turn"));
    }

    #[test]
    fn steer_followup_prompt_uses_compact_inbox_headers() {
        let thread_root_id = Uuid::new_v4();
        let source_message_id = Uuid::new_v4();
        let inbox_id = Uuid::new_v4();
        let prompt = build_steer_followup_prompt(&[InboxWakeItem {
            id: inbox_id,
            channel_id: Some(Uuid::new_v4()),
            channel_name: Some("support".to_owned()),
            channel_kind: Some("channel".to_owned()),
            thread_root_id: Some(thread_root_id),
            source_message_id: Some(source_message_id),
            task_id: None,
            kind: "owner_thread_followup".to_owned(),
            priority: 90,
            title: "Handle follow-up".to_owned(),
            body_preview: "please use the latest numbers\nand reply directly".to_owned(),
            message_created_at: Some(Utc::now()),
            sender_name: Some("Dylan".to_owned()),
            sender_role: Some("owner".to_owned()),
        }]);

        assert!(prompt.contains("Same-channel/thread live inbox follow-up."));
        assert!(prompt.contains("Default reply target for normal assistant text: #support:"));
        assert!(prompt.contains(&format!("msg={}", short_id(source_message_id))));
        assert!(prompt.contains(&format!("inbox_id: {inbox_id}")));
        assert!(prompt.contains("archived automatically"));
        assert!(!prompt.contains("inbox-archive --inbox-id <id>"));
        assert!(!prompt.contains("Current Lantor inbox processing turn:"));
        assert!(!prompt.contains("title: Handle follow-up"));
        assert!(!prompt.contains("kind: owner_thread_followup"));
        assert!(!prompt.contains(WORK_ITEM_FINISH_PROMPT));
    }

    #[test]
    fn claude_surface_boundary_only_appears_when_surface_changes() {
        let channel_id = Uuid::new_v4();
        let thread_a = Uuid::new_v4();
        let thread_b = Uuid::new_v4();
        let previous = ClaudeSurface {
            channel_id: Some(channel_id),
            thread_root_id: Some(thread_a),
        };

        assert!(claude_surface_boundary_marker(None, previous).is_none());
        assert!(claude_surface_boundary_marker(Some(previous), previous).is_none());

        let marker = claude_surface_boundary_marker(
            Some(previous),
            ClaudeSurface {
                channel_id: Some(channel_id),
                thread_root_id: Some(thread_b),
            },
        )
        .expect("changed surface should produce a boundary");
        assert!(marker.contains("Lantor thread boundary"));
        assert!(marker.contains("Previous surface:"));
        assert!(marker.contains("Current surface:"));
    }

    #[tokio::test]
    async fn claude_thread_context_injects_only_current_thread_messages() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let channel_id = insert_test_channel(&pool, "claude-context").await?;
            let thread_a: Uuid = sqlx::query_scalar(
                r#"
                insert into messages (channel_id, sender_name, sender_role, body, is_task)
                values ($1, 'Dylan', 'owner', 'thread A root', false)
                returning id
                "#,
            )
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let thread_b: Uuid = sqlx::query_scalar(
                r#"
                insert into messages (channel_id, sender_name, sender_role, body, is_task)
                values ($1, 'Dylan', 'owner', 'thread B root with forbidden bleed', false)
                returning id
                "#,
            )
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            sqlx::query(
                r#"
                insert into messages (channel_id, thread_root_id, sender_name, sender_role, body, is_task)
                values ($1, $2, 'Agent', 'agent', 'thread A reply evidence', false),
                       ($1, $3, 'Agent', 'agent', 'thread B reply forbidden bleed', false)
                "#,
            )
            .bind(channel_id)
            .bind(thread_a)
            .bind(thread_b)
            .execute(&pool)
            .await
            .map_err(|err| err.to_string())?;

            let context = append_claude_thread_context(
                &pool,
                "Default inbox item: summarize",
                Some(channel_id),
                Some("claude-context"),
                Some(thread_a),
            )
            .await?;
            assert!(context.contains("Same-thread recent context"));
            assert!(context.contains("Resolve contextual follow-ups from this block"));
            assert!(context.contains("thread A root"));
            assert!(context.contains("thread A reply evidence"));
            assert!(!context.contains("thread B root with forbidden bleed"));
            assert!(!context.contains("thread B reply forbidden bleed"));
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        result.unwrap();
    }

    #[tokio::test]
    async fn agent_context_tool_reads_thread_history_and_searches_messages() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let channel_id = insert_test_channel(&pool, "context-tools").await?;
            let root_id: Uuid = sqlx::query_scalar(
                r#"
                insert into messages (channel_id, sender_name, sender_role, body, is_task)
                values ($1, 'Dylan', 'owner', 'root message with needle', false)
                returning id
                "#,
            )
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            sqlx::query(
                r#"
                insert into messages (channel_id, thread_root_id, sender_name, sender_role, body, is_task)
                values ($1, $2, 'agent', 'agent', 'reply inside thread', false),
                       ($1, null, 'Dylan', 'owner', 'separate root message', false)
                "#,
            )
            .bind(channel_id)
            .bind(root_id)
            .execute(&pool)
            .await
            .map_err(|err| err.to_string())?;

            let history_args = vec![
                "history-read".to_owned(),
                "--target".to_owned(),
                format!("#context-tools:{}", short_id(root_id)),
                "--limit".to_owned(),
                "10".to_owned(),
            ];
            let history = agent_context_history_read(&pool, &history_args).await?;
            assert!(history.contains("root message with needle"));
            assert!(history.contains("reply inside thread"));
            assert!(history.contains("[target=#context-tools:"));
            assert!(history.contains(" type=owner] Dylan: root message with needle"));
            assert!(!history.contains("separate root message"));

            let search_args = vec![
                "message-search".to_owned(),
                "--query".to_owned(),
                "needle".to_owned(),
                "--target".to_owned(),
                "#context-tools".to_owned(),
            ];
            let search = agent_context_message_search(&pool, &search_args).await?;
            assert!(search.contains("root message with needle"));
            assert!(search.contains("[target=#context-tools msg="));
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn agent_context_tool_reads_workspace_and_memory() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let workspace =
            std::env::temp_dir().join(format!("lantor-workspace-tool-{}", Uuid::new_v4()));
        let result: Result<(), String> = async {
            std::fs::create_dir_all(workspace.join("notes")).map_err(|err| err.to_string())?;
            std::fs::write(
                workspace.join("MEMORY.md"),
                "# @workspace-agent\n\n## Role\nWorkspace-aware test agent.\n",
            )
            .map_err(|err| err.to_string())?;
            std::fs::write(workspace.join("notes").join("handoff.md"), "handoff note")
                .map_err(|err| err.to_string())?;

            let agent_id = insert_test_agent(&pool, "workspace-agent").await?;
            sqlx::query("update agents set working_directory = $2 where id = $1")
                .bind(agent_id)
                .bind(workspace.to_string_lossy().to_string())
                .execute(&pool)
                .await
                .map_err(|err| err.to_string())?;

            let target_args = vec![
                "workspace-info".to_owned(),
                "--target".to_owned(),
                "@workspace-agent".to_owned(),
            ];
            let workspace_info = agent_context_workspace_info(&pool, &target_args).await?;
            assert!(workspace_info.contains("Lantor workspace for @workspace-agent"));
            assert!(workspace_info.contains("memory_exists=true"));
            assert!(workspace_info.contains("MEMORY.md"));

            let memory_args = vec![
                "memory-read".to_owned(),
                "--target".to_owned(),
                "@workspace-agent".to_owned(),
            ];
            let memory = agent_context_memory_read(&pool, &memory_args).await?;
            assert!(memory.contains("Workspace-aware test agent"));

            let list_args = vec![
                "workspace-list".to_owned(),
                "--target".to_owned(),
                "@workspace-agent".to_owned(),
                "--max-depth".to_owned(),
                "2".to_owned(),
            ];
            let listing = agent_context_workspace_list(&pool, &list_args).await?;
            assert!(listing.contains("notes/"));
            assert!(listing.contains("notes/handoff.md"));
            Ok(())
        }
        .await;
        let _ = std::fs::remove_dir_all(&workspace);
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn artifact_create_event_persists_artifact_and_message_card() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let agent_id = insert_test_agent(&pool, "artifact-agent").await?;
            let channel_id = insert_test_channel(&pool, "artifacts").await?;
            let run_id: Uuid = sqlx::query_scalar(
                r#"
                insert into agent_runs (agent_id, command, status)
                values ($1, 'codex app-server', 'running')
                returning id
                "#,
            )
            .bind(agent_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;

            handle_agent_event(
                &pool,
                agent_id,
                run_id,
                AgentEvent::ArtifactCreate {
                    channel: None,
                    channel_id: Some(channel_id),
                    thread_root_id: None,
                    kind: "markdown".to_owned(),
                    title: "Review report".to_owned(),
                    summary: Some("Two findings and one follow-up.".to_owned()),
                    content: "# Review report\n\n- finding".to_owned(),
                    metadata: Some(json!({"source": "test"})),
                },
            )
            .await?;

            let artifact = sqlx::query(
                r#"
                select id, message_id, kind, title, summary, content, metadata
                from artifacts
                where channel_id = $1
                "#,
            )
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let artifact_id: Uuid = artifact.get("id");
            let message_id: Uuid = artifact.get("message_id");
            assert_eq!(artifact.get::<String, _>("kind"), "markdown");
            assert_eq!(artifact.get::<String, _>("title"), "Review report");
            assert_eq!(
                artifact.get::<String, _>("summary"),
                "Two findings and one follow-up."
            );

            let messages = load_messages(&pool).await?;
            let message = messages
                .iter()
                .find(|message| message.id == message_id)
                .expect("artifact message should be loaded");
            assert!(message.body.contains("Created artifact: Review report"));
            assert_eq!(message.artifacts.len(), 1);
            assert_eq!(message.artifacts[0].id, artifact_id);

            let tool_output = agent_context_artifact_read_in_pool(
                &pool,
                &[
                    "artifact-read".to_owned(),
                    "--artifact-id".to_owned(),
                    artifact_id.to_string(),
                ],
            )
            .await?;
            assert!(tool_output.contains("kind=markdown"));
            assert!(tool_output.contains("# Review report"));
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn attachment_create_event_imports_local_file_as_message_attachment() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let agent_id = insert_test_agent(&pool, "attachment-agent").await?;
            let channel_id = insert_test_channel(&pool, "attachments").await?;
            let run_id: Uuid = sqlx::query_scalar(
                r#"
                insert into agent_runs (agent_id, command, status)
                values ($1, 'codex app-server', 'running')
                returning id
                "#,
            )
            .bind(agent_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;

            let dir =
                std::env::temp_dir().join(format!("lantor-attachment-test-{}", Uuid::new_v4()));
            std::fs::create_dir_all(&dir).map_err(|err| err.to_string())?;
            let source_path = dir.join("generated.png");
            let source_bytes = b"\x89PNG\r\n\x1a\nlantor-test-image";
            std::fs::write(&source_path, source_bytes).map_err(|err| err.to_string())?;

            handle_agent_event(
                &pool,
                agent_id,
                run_id,
                AgentEvent::AttachmentCreate {
                    channel: None,
                    channel_id: Some(channel_id),
                    thread_root_id: None,
                    body: Some("Generated architecture diagram".to_owned()),
                    files: vec![AgentAttachmentFile {
                        path: source_path.to_string_lossy().to_string(),
                        name: Some("architecture.png".to_owned()),
                        mime_type: Some("image/png".to_owned()),
                    }],
                },
            )
            .await?;

            let messages = load_messages(&pool).await?;
            let message = messages
                .iter()
                .find(|message| message.body == "Generated architecture diagram")
                .expect("attachment message should be loaded");
            assert_eq!(message.attachments.len(), 1);
            let attachment = &message.attachments[0];
            assert_eq!(attachment.original_name, "architecture.png");
            assert_eq!(attachment.mime_type, "image/png");
            assert_eq!(attachment.size_bytes, source_bytes.len() as i64);
            assert_ne!(
                attachment.storage_path.as_str(),
                source_path.to_string_lossy().as_ref()
            );
            assert_eq!(
                std::fs::read(&attachment.storage_path).map_err(|err| err.to_string())?,
                source_bytes
            );

            let stored_path = std::path::PathBuf::from(&attachment.storage_path);
            let _ = std::fs::remove_file(&stored_path);
            if let Some(parent) = stored_path.parent() {
                let _ = std::fs::remove_dir(parent);
            }
            let _ = std::fs::remove_dir_all(&dir);
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn handoff_create_dispatches_target_agent_to_existing_thread() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let source_agent_id = insert_test_agent(&pool, "source").await?;
            let target_agent_id = insert_test_agent(&pool, "target").await?;
            let channel_id = insert_test_channel(&pool, "handoff").await?;
            sqlx::query(
                r#"
                insert into channel_members (channel_id, agent_id)
                values ($1, $2)
                "#,
            )
            .bind(channel_id)
            .bind(source_agent_id)
            .execute(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let root_message_id: Uuid = sqlx::query_scalar(
                r#"
                insert into messages (channel_id, sender_name, sender_role, body, is_task)
                values ($1, 'Dylan', 'owner', 'Please investigate this thread', false)
                returning id
                "#,
            )
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let run_id: Uuid = sqlx::query_scalar(
                r#"
                insert into agent_runs (agent_id, command, status)
                values ($1, 'codex app-server', 'running')
                returning id
                "#,
            )
            .bind(source_agent_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;

            handle_agent_event(
                &pool,
                source_agent_id,
                run_id,
                AgentEvent::HandoffCreate {
                    target_agent: "@target".to_owned(),
                    channel: None,
                    channel_id: Some(channel_id),
                    thread_root_id: root_message_id,
                    reason: Some("User asked target to continue".to_owned()),
                    body: "@target Please continue the implementation from this thread.".to_owned(),
                },
            )
            .await?;

            let handoff_message = sqlx::query(
                r#"
                select id, sender_agent_id, sender_name, sender_role, body
                from messages
                where channel_id = $1
                  and thread_root_id = $2
                  and sender_agent_id = $3
                  and sender_role = 'agent'
                "#,
            )
            .bind(channel_id)
            .bind(root_message_id)
            .bind(source_agent_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let handoff_message_id: Uuid = handoff_message.get("id");
            let handoff_body: String = handoff_message.get("body");
            assert_eq!(handoff_message.get::<String, _>("sender_name"), "source");
            assert_eq!(
                handoff_body,
                "@target Please continue the implementation from this thread."
            );
            assert!(!handoff_body.contains("Reason:"));

            let target_is_member: bool = sqlx::query_scalar(
                r#"
                select exists (
                    select 1 from channel_members
                    where channel_id = $1 and agent_id = $2
                )
                "#,
            )
            .bind(channel_id)
            .bind(target_agent_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert!(target_is_member);

            let work_item = sqlx::query(
                r#"
                select
                    w.agent_id,
                    w.thread_root_id,
                    w.source_message_id,
                    w.source_kind,
                    w.title,
                    w.context,
                    w.status,
                    i.kind as inbox_kind,
                    i.state as inbox_state
                from agent_work_items w
                join agent_inbox_items i on i.work_item_id = w.id
                where w.source_message_id = $1
                "#,
            )
            .bind(handoff_message_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(work_item.get::<Uuid, _>("agent_id"), target_agent_id);
            assert_eq!(
                work_item.get::<Option<Uuid>, _>("thread_root_id"),
                Some(root_message_id)
            );
            assert_eq!(
                work_item.get::<Option<Uuid>, _>("source_message_id"),
                Some(handoff_message_id)
            );
            assert_eq!(work_item.get::<String, _>("source_kind"), "inbox_wake");
            assert_eq!(work_item.get::<String, _>("status"), "queued");
            assert!(work_item
                .get::<String, _>("context")
                .contains("Lantor agent inbox wake"));
            assert_eq!(work_item.get::<String, _>("inbox_kind"), "handoff");
            assert_eq!(work_item.get::<String, _>("inbox_state"), "processing");
            let target_work_items: i64 =
                sqlx::query_scalar("select count(*) from agent_work_items where agent_id = $1")
                    .bind(target_agent_id)
                    .fetch_one(&pool)
                    .await
                    .map_err(|err| err.to_string())?;
            assert_eq!(target_work_items, 1);

            let pending_start: i64 = sqlx::query_scalar(
                r#"
                select count(*)
                from supervisor_commands
                where agent_id = $1 and work_item_id in (
                    select id from agent_work_items where source_message_id = $2
                )
                "#,
            )
            .bind(target_agent_id)
            .bind(handoff_message_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(pending_start, 1);
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn task_handoff_reassigns_task_and_wakes_new_assignee() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let source_agent_id = insert_test_agent(&pool, "task-source").await?;
            let target_agent_id = insert_test_agent(&pool, "task-target").await?;
            let channel_id = insert_test_channel(&pool, "task-handoff").await?;
            sqlx::query(
                r#"
                insert into channel_members (channel_id, agent_id)
                values ($1, $2)
                "#,
            )
            .bind(channel_id)
            .bind(source_agent_id)
            .execute(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let root_message_id: Uuid = sqlx::query_scalar(
                r#"
                insert into messages (channel_id, sender_name, sender_role, body, is_task)
                values ($1, 'Dylan', 'owner', 'Implement task handoff', true)
                returning id
                "#,
            )
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let task_row = sqlx::query(
                r#"
                insert into tasks (message_id, channel_id, title, status, assignee_agent_id)
                values ($1, $2, 'Implement task handoff', 'in_progress', $3)
                returning id, number
                "#,
            )
            .bind(root_message_id)
            .bind(channel_id)
            .bind(source_agent_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let task_id: Uuid = task_row.get("id");
            let task_number: i64 = task_row.get("number");
            let run_id: Uuid = sqlx::query_scalar(
                r#"
                insert into agent_runs (agent_id, command, status)
                values ($1, 'codex app-server', 'running')
                returning id
                "#,
            )
            .bind(source_agent_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            sqlx::query(
                r#"
                insert into agent_work_items (
                    agent_id, channel_id, thread_root_id, task_id, source_kind, title, context, status, run_id
                )
                values ($1, $2, $3, $4, 'task', 'Implement task handoff', '', 'running', $5)
                "#,
            )
            .bind(source_agent_id)
            .bind(channel_id)
            .bind(root_message_id)
            .bind(task_id)
            .bind(run_id)
            .execute(&pool)
            .await
            .map_err(|err| err.to_string())?;

            handle_agent_event(
                &pool,
                source_agent_id,
                run_id,
                AgentEvent::TaskHandoff {
                    target_agent: "@task-target".to_owned(),
                    task_number: None,
                    reason: "Target owns the UI side".to_owned(),
                    body: Some("@task-target please continue the UI wiring.".to_owned()),
                },
            )
            .await?;

            let task = sqlx::query("select status, assignee_agent_id from tasks where id = $1")
                .bind(task_id)
                .fetch_one(&pool)
                .await
                .map_err(|err| err.to_string())?;
            assert_eq!(task.get::<String, _>("status"), "in_progress");
            assert_eq!(
                task.get::<Option<Uuid>, _>("assignee_agent_id"),
                Some(target_agent_id)
            );

            let handoff_message_body: String = sqlx::query_scalar(
                r#"
                select body
                from messages
                where channel_id = $1
                  and thread_root_id = $2
                  and sender_agent_id = $3
                "#,
            )
            .bind(channel_id)
            .bind(root_message_id)
            .bind(source_agent_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(
                handoff_message_body,
                "@task-target please continue the UI wiring."
            );

            let inbox = sqlx::query(
                r#"
                select kind, task_id, payload->>'reason' as reason
                from agent_inbox_items
                where agent_id = $1
                order by created_at desc
                limit 1
                "#,
            )
            .bind(target_agent_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(inbox.get::<String, _>("kind"), "task_assigned");
            assert_eq!(inbox.get::<Option<Uuid>, _>("task_id"), Some(task_id));
            assert_eq!(
                inbox.get::<Option<String>, _>("reason").as_deref(),
                Some("Target owns the UI side")
            );
            let target_is_member: bool = sqlx::query_scalar(
                r#"
                select exists (
                    select 1 from channel_members
                    where channel_id = $1 and agent_id = $2
                )
                "#,
            )
            .bind(channel_id)
            .bind(target_agent_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert!(target_is_member);
            let target_work_items: i64 = sqlx::query_scalar(
                "select count(*) from agent_work_items where agent_id = $1 and task_id = $2",
            )
            .bind(target_agent_id)
            .bind(task_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(target_work_items, 1);
            let activity_count: i64 = sqlx::query_scalar(
                r#"
                select count(*)
                from agent_activities
                where agent_id = $1
                  and title = $2
                  and metadata->>'target_agent' = '@task-target'
                "#,
            )
            .bind(source_agent_id)
            .bind(format!("Task #{task_number} handed off"))
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(activity_count, 1);
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn channel_message_create_posts_agent_message_and_dispatches_mentions() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let source_agent_id = insert_test_agent(&pool, "source").await?;
            let target_agent_id = insert_test_agent(&pool, "target").await?;
            let channel_id = insert_test_channel(&pool, "channel-message").await?;
            sqlx::query(
                r#"
                insert into channel_members (channel_id, agent_id)
                values ($1, $2)
                "#,
            )
            .bind(channel_id)
            .bind(source_agent_id)
            .execute(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let run_id: Uuid = sqlx::query_scalar(
                r#"
                insert into agent_runs (agent_id, command, status)
                values ($1, 'codex app-server', 'running')
                returning id
                "#,
            )
            .bind(source_agent_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;

            handle_agent_event(
                &pool,
                source_agent_id,
                run_id,
                AgentEvent::ChannelMessageCreate {
                    channel: None,
                    channel_id: Some(channel_id),
                    thread_root_id: None,
                    body: "@target please start this task in this channel.".to_owned(),
                },
            )
            .await?;

            let message = sqlx::query(
                r#"
                select id, thread_root_id, sender_agent_id, sender_name, sender_role, body, is_task
                from messages
                where channel_id = $1 and sender_agent_id = $2
                "#,
            )
            .bind(channel_id)
            .bind(source_agent_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let message_id: Uuid = message.get("id");
            assert_eq!(message.get::<Option<Uuid>, _>("thread_root_id"), None);
            assert_eq!(
                message.get::<Option<Uuid>, _>("sender_agent_id"),
                Some(source_agent_id)
            );
            assert_eq!(message.get::<String, _>("sender_name"), "source");
            assert_eq!(message.get::<String, _>("sender_role"), "agent");
            assert_eq!(
                message.get::<String, _>("body"),
                "@target please start this task in this channel."
            );
            assert!(!message.get::<bool, _>("is_task"));

            let target_is_member: bool = sqlx::query_scalar(
                r#"
                select exists (
                    select 1 from channel_members
                    where channel_id = $1 and agent_id = $2
                )
                "#,
            )
            .bind(channel_id)
            .bind(target_agent_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert!(target_is_member);

            let work_item = sqlx::query(
                r#"
                select w.agent_id, w.thread_root_id, w.source_message_id, w.source_kind, i.kind as inbox_kind
                from agent_work_items w
                join agent_inbox_items i on i.work_item_id = w.id
                where w.source_message_id = $1 and w.agent_id = $2
                "#,
            )
            .bind(message_id)
            .bind(target_agent_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(work_item.get::<Uuid, _>("agent_id"), target_agent_id);
            assert_eq!(
                work_item.get::<Option<Uuid>, _>("thread_root_id"),
                Some(message_id)
            );
            assert_eq!(
                work_item.get::<Option<Uuid>, _>("source_message_id"),
                Some(message_id)
            );
            assert_eq!(work_item.get::<String, _>("source_kind"), "inbox_wake");
            assert_eq!(work_item.get::<String, _>("inbox_kind"), "collaboration");
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn channel_message_create_requires_source_channel_membership() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> =
            async {
                let source_agent_id = insert_test_agent(&pool, "source").await?;
                let channel_id = insert_test_channel(&pool, "channel-message-denied").await?;
                let run_id: Uuid = sqlx::query_scalar(
                    r#"
                insert into agent_runs (agent_id, command, status)
                values ($1, 'codex app-server', 'running')
                returning id
                "#,
                )
                .bind(source_agent_id)
                .fetch_one(&pool)
                .await
                .map_err(|err| err.to_string())?;

                let err = handle_agent_event(
                    &pool,
                    source_agent_id,
                    run_id,
                    AgentEvent::ChannelMessageCreate {
                        channel: None,
                        channel_id: Some(channel_id),
                        thread_root_id: None,
                        body: "I should not be posted.".to_owned(),
                    },
                )
                .await
                .expect_err("non-member channel_message_create should be rejected");
                assert!(
                    err.contains("channel_message_create requires source agent channel membership")
                );

                let message_count: i64 =
                    sqlx::query_scalar("select count(*) from messages where channel_id = $1")
                        .bind(channel_id)
                        .fetch_one(&pool)
                        .await
                        .map_err(|err| err.to_string())?;
                assert_eq!(message_count, 0);
                Ok(())
            }
            .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn agent_event_receipts_hash_large_control_events() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let agent_id = insert_test_agent(&pool, "large-event-agent").await?;
            let run_id: Uuid = sqlx::query_scalar(
                r#"
                insert into agent_runs (agent_id, command, status)
                values ($1, 'codex app-server', 'running')
                returning id
                "#,
            )
            .bind(agent_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let markdown = format!(
                "# Large report\n\n{}",
                "- large artifact payload\n".repeat(400)
            );
            let event = json!({
                "type": "artifact_create",
                "channel_id": Uuid::new_v4(),
                "kind": "markdown",
                "title": "Large markdown artifact",
                "content": markdown
            })
            .to_string();

            assert!(claim_agent_event(&pool, run_id, &event).await?);
            assert!(!claim_agent_event(&pool, run_id, &event).await?);
            let (count, max_len): (i64, i64) = sqlx::query_as(
                "select count(*), max(length(event_json)) from agent_event_receipts where run_id = $1",
            )
            .bind(run_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(count, 1);
            assert!(max_len > 2704);
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[test]
    fn extracts_plain_agent_event_lines() {
        assert_eq!(
            extract_agent_event_json(r#"LANTOR_EVENT {"type":"message","body":"ok"}"#),
            Some(r#"{"type":"message","body":"ok"}"#)
        );
    }

    #[test]
    fn extracts_codex_wrapped_agent_event_lines() {
        assert_eq!(
            extract_agent_event_json(
                r#"[stderr] LANTOR_EVENT {"type":"message","body":"from tool output"}"#
            ),
            Some(r#"{"type":"message","body":"from tool output"}"#)
        );
    }

    #[test]
    fn extracts_stdout_wrapped_agent_event_lines() {
        assert_eq!(
            extract_agent_event_json(
                r#"[stdout] LANTOR_EVENT {"type":"message","body":"from final output"}"#
            ),
            Some(r#"{"type":"message","body":"from final output"}"#)
        );
    }

    #[test]
    fn extracts_agent_event_json_before_trailing_text() {
        assert_eq!(
            extract_agent_event_json(
                r#"LANTOR_EVENT {"type":"activity","title":"Done","detail":"ok"} ## Review"#
            ),
            Some(r#"{"type":"activity","title":"Done","detail":"ok"}"#)
        );
    }

    #[test]
    fn extracts_agent_event_json_with_braces_inside_strings() {
        assert_eq!(
            extract_agent_event_json(
                r#"LANTOR_EVENT {"type":"message","body":"text with { braces } and \"quotes\""} trailing"#
            ),
            Some(r#"{"type":"message","body":"text with { braces } and \"quotes\""}"#)
        );
    }

    #[test]
    fn keeps_trailing_stream_text_after_control_event() {
        let (visible, events) = split_streaming_agent_event_lines(
            r#"LANTOR_EVENT {"type":"activity","title":"Done","detail":"ok"} ## Review"#,
        );

        assert_eq!(
            events,
            vec![r#"{"type":"activity","title":"Done","detail":"ok"}"#]
        );
        assert_eq!(visible, "## Review");
    }

    #[test]
    fn ignores_event_examples_embedded_in_instructions() {
        assert!(extract_agent_event_json(
            r#"[stderr] Reply with: LANTOR_EVENT {"type":"message","body":"..."}"#
        )
        .is_none());
    }

    #[test]
    fn caps_streaming_deltas_with_marker() {
        let remaining = STREAMING_MESSAGE_BODY_LIMIT - 4;
        let delta = "x".repeat(remaining + 16);
        let (capped, truncated) = capped_stream_delta(&delta, 4);
        assert!(truncated);
        assert!(capped.ends_with(STREAMING_TRUNCATION_MARKER));
        assert_eq!(capped.chars().count() + 4, STREAMING_MESSAGE_BODY_LIMIT);
    }

    #[test]
    fn detects_silent_reply_marker() {
        assert_eq!(
            silent_reply_reason("LANTOR_SILENT_REPLY: greeting only"),
            Some("greeting only".to_owned())
        );
        assert_eq!(
            silent_reply_reason("LANTOR_SILENT_REPLY"),
            Some(String::new())
        );
        assert_eq!(silent_reply_reason("LANTOR_SILENT_REPLYING"), None);
    }

    #[test]
    fn structures_activity_metadata_from_detail() {
        let metadata = parse_activity_metadata("pid=123, thread_id=abc, duration=42 ms");
        assert_eq!(metadata["pid"], "123");
        assert_eq!(metadata["thread_id"], "abc");
        assert_eq!(metadata["duration_ms"], 42);
    }

    #[test]
    fn ignores_known_codex_manifest_default_prompt_warning() {
        let line = json!({
            "timestamp": "2026-05-14T13:05:55.340546Z",
            "level": "WARN",
            "fields": {
                "message": "ignoring interface.defaultPrompt: maximum length exceeded"
            },
            "target": "codex_core_plugins::manifest"
        })
        .to_string();

        assert_eq!(classify_agent_output_activity("stderr", &line), None);
        assert_eq!(classify_agent_output_activity("stdout", &line), None);
    }

    #[test]
    fn ignores_known_codex_skill_loader_icon_warning() {
        for message in [
            "ignoring interface.icon_small: icon path must not contain '..'",
            "ignoring interface.icon_large: icon path must not contain '..'",
        ] {
            let line = json!({
                "timestamp": "2026-05-14T13:05:55.340546Z",
                "level": "WARN",
                "fields": {
                    "message": message
                },
                "target": "codex_core_skills::loader"
            })
            .to_string();

            assert_eq!(classify_agent_output_activity("stderr", &line), None);
            assert_eq!(classify_agent_output_activity("stdout", &line), None);
        }
    }

    #[test]
    fn maps_structured_stderr_warning_to_runtime_warning() {
        let line = json!({
            "timestamp": "2026-05-14T13:05:55.340546Z",
            "level": "WARN",
            "fields": {
                "message": "plugin manifest used a deprecated field"
            },
            "target": "codex_core_plugins::manifest"
        })
        .to_string();
        let activity =
            classify_agent_output_activity("stderr", &line).expect("warning should be classified");

        assert_eq!(activity.0, "run");
        assert_eq!(activity.1, "Runtime warning");
        let detail: Value = serde_json::from_str(&activity.2).expect("structured detail");
        assert_eq!(detail["level"], "WARN");
        assert_eq!(detail["target"], "codex_core_plugins::manifest");
        assert_eq!(detail["message"], "plugin manifest used a deprecated field");

        let stdout_activity =
            classify_agent_output_activity("stdout", &line).expect("warning should be classified");
        assert_eq!(stdout_activity.0, "run");
        assert_eq!(stdout_activity.1, "Runtime warning");
    }

    #[test]
    fn ignores_known_codex_legacy_notify_hook_warning() {
        let line = json!({
            "timestamp": "2026-05-14T13:16:30.388210Z",
            "level": "WARN",
            "fields": {
                "error": "No such file or directory (os error 2)",
                "hook_name": "legacy_notify",
                "message": "after_agent hook failed; continuing",
                "turn_id": "019e26a1-7ad5-7642-af8a-e042a0738a84"
            },
            "target": "codex_core::session::turn"
        })
        .to_string();

        assert_eq!(classify_agent_output_activity("stderr", &line), None);
    }

    #[test]
    fn maps_structured_warning_with_error_words_to_runtime_warning() {
        let line = json!({
            "timestamp": "2026-05-14T13:16:30.388210Z",
            "level": "WARN",
            "fields": {
                "error": "retryable operation failed once",
                "message": "operation failed; continuing"
            },
            "target": "codex_core::session::turn"
        })
        .to_string();
        let activity =
            classify_agent_output_activity("stderr", &line).expect("warning should be classified");

        assert_eq!(activity.0, "run");
        assert_eq!(activity.1, "Runtime warning");
        assert_eq!(activity_status(activity.0, activity.1), "warning");
    }

    #[test]
    fn marks_runtime_warning_activity_as_warning_status() {
        assert_eq!(activity_status("run", "Runtime warning"), "warning");
        assert_eq!(activity_status("error", "Error output"), "error");
        assert_eq!(
            activity_status("thinking", "Investigating Activity ERROR"),
            "active"
        );
        assert_eq!(
            activity_status("tools", "Checking current changes"),
            "active"
        );
        assert_eq!(
            activity_status("file_edit", "Adjusting progress display"),
            "active"
        );
        assert_eq!(activity_status("command", "Command finished"), "success");
    }

    #[test]
    fn downgrades_retryable_codex_infra_stderr_to_warning() {
        for line in [
            "\u{1b}[2m2026-05-18T14:32:02.340702Z\u{1b}[0m \u{1b}[31mERROR\u{1b}[0m \u{1b}[2mcodex_api::endpoint::responses_websocket\u{1b}[0m\u{1b}[2m:\u{1b}[0m failed to connect to websocket: IO error: tls handshake eof",
            "\u{1b}[2m2026-05-18T14:32:21.393770Z\u{1b}[0m \u{1b}[31mERROR\u{1b}[0m \u{1b}[2mcodex_models_manager::manager\u{1b}[0m\u{1b}[2m:\u{1b}[0m failed to refresh available models: timeout waiting for child process to exit",
            "\u{1b}[2m2026-05-18T14:31:57.219245Z\u{1b}[0m \u{1b}[31mERROR\u{1b}[0m \u{1b}[2mrmcp::transport::worker\u{1b}[0m\u{1b}[2m:\u{1b}[0m worker quit with fatal: Transport channel closed, when Client(HttpRequest(HttpRequest(\"http/request failed: error sending request for url (https://chatgpt.com/backend-api/wham/apps)\")))",
        ] {
            let activity = classify_agent_output_activity("stderr", line)
                .expect("retryable stderr should remain visible");
            assert_eq!(activity.0, "run");
            assert_eq!(activity.1, "Runtime warning");
            assert_eq!(activity_status(activity.0, activity.1), "warning");
        }
    }

    #[test]
    fn maps_unclassified_stderr_to_runtime_output_not_thinking() {
        assert_eq!(
            classify_agent_output_activity("stderr", "runtime heartbeat"),
            Some(("run", "Runtime output", "runtime heartbeat".to_owned()))
        );
        assert_eq!(
            classify_agent_output_activity("stdout", "model is considering options"),
            Some((
                "thinking",
                "Thinking",
                "model is considering options".to_owned()
            ))
        );
    }

    #[test]
    fn extracts_codex_turn_ids_from_response_and_notification() {
        assert_eq!(
            codex_turn_id_from_value(&json!({"result": {"turn": {"id": "turn-1"}}})),
            Some("turn-1".to_owned())
        );
        assert_eq!(
            codex_turn_id_from_value(&json!({"params": {"turn": {"id": "turn-2"}}})),
            Some("turn-2".to_owned())
        );
        assert_eq!(
            codex_turn_id_from_value(&json!({"params": {"turnId": "turn-3"}})),
            Some("turn-3".to_owned())
        );
    }

    #[test]
    fn ignores_retryable_codex_error_notifications() {
        assert_eq!(
            codex_error_notification_detail(&json!({
                "method": "error",
                "params": {
                    "error": {"message": "Reconnecting... 2/5"},
                    "willRetry": true
                }
            })),
            None
        );
        assert_eq!(
            codex_error_notification_detail(&json!({
                "method": "error",
                "params": {
                    "error": {"message": "stream disconnected"},
                    "willRetry": false
                }
            })),
            Some("stream disconnected".to_owned())
        );
    }

    #[test]
    fn maps_codex_command_and_file_activity() {
        assert_eq!(
            codex_item_started_activity(&json!({
                "params": {
                    "item": {
                        "type": "commandExecution",
                        "command": "cargo test"
                    }
                }
            })),
            (
                "command",
                "Running command",
                json!({"command": "cargo test"}).to_string()
            )
        );
        assert_eq!(
            codex_item_started_activity(&json!({
                "params": {
                    "item": {
                        "type": "fileChange",
                        "path": "src/main.rs",
                        "operation": "update"
                    }
                }
            })),
            (
                "file_edit",
                "Editing file",
                json!({"file": "src/main.rs", "operation": "update"}).to_string()
            )
        );
    }

    #[test]
    fn parses_codex_thread_token_usage_events() {
        let value = json!({
            "method": "thread/tokenUsage/updated",
            "params": {
                "tokenUsage": {
                    "total": {
                        "inputTokens": 11488567,
                        "outputTokens": 36332
                    },
                    "last": {
                        "inputTokens": 33569,
                        "cachedInputTokens": 31616,
                        "outputTokens": 1278
                    }
                }
            }
        });
        assert_eq!(usage_from_runtime_event(&value), Some((33569, 1278)));

        let log = format!(
            "[codex] {{\"method\":\"item/agentMessage/delta\",\"params\":{{\"delta\":\"hi\"}}}}\n[codex] {value}"
        );
        assert_eq!(usage_from_run_log(&log), Some((33569, 1278)));
    }

    #[test]
    fn parses_claude_stream_json_events() {
        assert_eq!(
            claude_text_delta(
                &json!({"type": "stream_event", "event": {"delta": {"type": "text_delta", "text": "hi"}}})
            ),
            Some("hi")
        );
        assert_eq!(
            claude_message_text(&json!({
                "type": "assistant",
                "message": {
                    "content": [
                        {"type": "text", "text": "hello"},
                        {"type": "tool_use", "name": "Read"}
                    ]
                }
            })),
            Some("hello".to_owned())
        );
        assert_eq!(
            claude_result_error(
                &json!({"type": "result", "is_error": true, "result": "rate limited"})
            ),
            Some("rate limited".to_owned())
        );
        assert_eq!(
            claude_result_error(
                &json!({"type": "result", "is_error": false, "api_error_status": null, "result": "ok"})
            ),
            None
        );
        assert_eq!(
            claude_result_error(
                &json!({"type": "result", "is_error": false, "api_error_status": "rate_limited", "result": "busy"})
            ),
            Some("rate_limited".to_owned())
        );
        assert_eq!(
            claude_stream_event_activity(&json!({"type": "system", "subtype": "init"})),
            Some(("run", "Runtime ready", "Claude stream connected".to_owned()))
        );
        assert_eq!(
            claude_stream_event_activity(&json!({
                "type": "system",
                "subtype": "api_retry",
                "attempt": 2,
                "max_retries": 3,
                "error_status": 529,
                "error": "rate_limit"
            })),
            Some((
                "run_error",
                "Claude provider retrying",
                "Claude provider request failed; retrying automatically · attempt 2/3 · status 529 · error rate_limit"
                    .to_owned()
            ))
        );
        assert_eq!(
            claude_stream_event_activity(
                &json!({"type": "rate_limit_event", "rate_limit_info": {"status": "allowed"}})
            ),
            None
        );
        assert_eq!(
            claude_stream_event_activity(
                &json!({"type": "stream_event", "event": {"type": "message_stop"}})
            ),
            None
        );
        assert_eq!(
            claude_stream_event_activity(
                &json!({"type": "stream_event", "event": {"type": "content_block_start", "content_block": {"type": "tool_use", "name": "Bash"}}})
            ),
            Some((
                "command",
                "Running command",
                json!({"tool": "Bash"}).to_string()
            ))
        );
        assert_eq!(
            claude_stream_event_activity(
                &json!({"type": "stream_event", "event": {"type": "content_block_start", "content_block": {"type": "tool_use", "name": "Edit"}}})
            ),
            Some((
                "file_edit",
                "Editing file",
                json!({"tool": "Edit"}).to_string()
            ))
        );
    }

    async fn test_pool() -> Option<(SqlitePool, String)> {
        test_pool_with_connections(1).await
    }

    async fn test_pool_with_connections(max_connections: u32) -> Option<(SqlitePool, String)> {
        let database_path =
            std::env::temp_dir().join(format!("lantor-test-{}.sqlite", Uuid::new_v4().simple()));
        let database_path = database_path.to_string_lossy().into_owned();
        let database_url = format!("sqlite://{database_path}");
        let pool = match db_connect_with_url(&database_url, max_connections).await {
            Ok(pool) => pool,
            Err(err) => {
                eprintln!("skipping SQLite-backed Lantor test: {err}");
                return None;
            }
        };
        if let Err(err) = migrate(&pool).await {
            eprintln!("skipping SQLite-backed Lantor test: {err}");
            pool.close().await;
            drop_sqlite_test_files(&database_path);
            return None;
        }
        Some((pool, database_path))
    }

    fn drop_sqlite_test_files(database_path: &str) {
        let _ = std_fs::remove_file(database_path);
        let _ = std_fs::remove_file(format!("{database_path}-wal"));
        let _ = std_fs::remove_file(format!("{database_path}-shm"));
    }

    async fn drop_test_schema(pool: SqlitePool, database_path: String) {
        pool.close().await;
        drop_sqlite_test_files(&database_path);
    }

    async fn latest_ui_event_id(pool: &SqlitePool) -> Result<i64, String> {
        sqlx::query_scalar("select coalesce(max(id), 0) from ui_events")
            .fetch_one(pool)
            .await
            .map_err(|err| err.to_string())
    }

    async fn wait_for_ui_refresh_reason(
        pool: &SqlitePool,
        last_event_id: &mut i64,
        reason: &str,
    ) -> Result<(), String> {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let rows = sqlx::query(
                    "select id, event_json from ui_events where id > $1 order by id asc",
                )
                .bind(*last_event_id)
                .fetch_all(pool)
                .await
                .map_err(|err| err.to_string())?;
                for row in rows {
                    *last_event_id = row.get::<i64, _>("id");
                    let payload: String = row.get("event_json");
                    let value: Value =
                        serde_json::from_str(&payload).map_err(|err| err.to_string())?;
                    if value.get("reason").and_then(Value::as_str) == Some(reason) {
                        return Ok(());
                    }
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .map_err(|_| format!("timed out waiting for {reason} notification"))?
    }

    async fn insert_test_agent(pool: &SqlitePool, handle: &str) -> Result<Uuid, String> {
        sqlx::query_scalar(
            r#"
            insert into agents (handle, display_name, role, status, runtime, model, avatar, description)
            values ($1, $2, 'agent', 'idle', 'codex', 'gpt-5.5', 'D', 'test agent')
            returning id
            "#,
        )
        .bind(handle)
        .bind(handle)
        .fetch_one(pool)
        .await
        .map_err(|err| err.to_string())
    }

    async fn insert_test_channel(pool: &SqlitePool, name: &str) -> Result<Uuid, String> {
        sqlx::query_scalar(
            r#"
            insert into channels (name, description, kind)
            values ($1, 'test channel', 'channel')
            returning id
            "#,
        )
        .bind(name)
        .fetch_one(pool)
        .await
        .map_err(|err| err.to_string())
    }

    #[tokio::test]
    async fn usage_event_updates_run_tokens_and_cost() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let agent_id = insert_test_agent(&pool, "usage-agent").await?;
            let run_id: Uuid = sqlx::query_scalar(
                r#"
                insert into agent_runs (agent_id, command, status)
                values ($1, 'codex app-server', 'running')
                returning id
                "#,
            )
            .bind(agent_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            handle_agent_event(
                &pool,
                agent_id,
                run_id,
                AgentEvent::Usage {
                    input_tokens: Some(1000),
                    output_tokens: Some(200),
                    total_tokens: None,
                    cost_micros: Some(1234),
                    cost_usd: None,
                },
            )
            .await?;
            let row = sqlx::query(
                "select input_tokens, output_tokens, cost_micros from agent_runs where id = $1",
            )
            .bind(run_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(row.get::<i64, _>("input_tokens"), 1000);
            assert_eq!(row.get::<i64, _>("output_tokens"), 200);
            assert_eq!(row.get::<i64, _>("cost_micros"), 1234);
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn memory_events_append_and_compact_memory_file() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let agent_id = insert_test_agent(&pool, "memory-agent").await?;
            let run_id: Uuid = sqlx::query_scalar(
                r#"
                insert into agent_runs (agent_id, command, status)
                values ($1, 'codex app-server', 'running')
                returning id
                "#,
            )
            .bind(agent_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let dir = std::env::temp_dir().join(format!("lantor-memory-write-{}", Uuid::new_v4()));
            sqlx::query("update agents set working_directory = $2 where id = $1")
                .bind(agent_id)
                .bind(dir.to_string_lossy().to_string())
                .execute(&pool)
                .await
                .map_err(|err| err.to_string())?;
            handle_agent_event(
                &pool,
                agent_id,
                run_id,
                AgentEvent::MemoryAppend {
                    body: "Remember: concise replies.".to_owned(),
                },
            )
            .await?;
            let memory_path = dir.join("MEMORY.md");
            let memory = std::fs::read_to_string(&memory_path).map_err(|err| err.to_string())?;
            let work_log_path = dir.join("notes").join("work-log.md");
            let work_log =
                std::fs::read_to_string(&work_log_path).map_err(|err| err.to_string())?;
            assert!(memory.contains("notes/work-log.md"));
            assert!(memory.contains("## Memory Map"));
            assert!(!memory.contains("## Memory update"));
            assert!(work_log.contains("## Memory update"));
            assert!(work_log.contains("Remember: concise replies."));
            handle_agent_event(
                &pool,
                agent_id,
                run_id,
                AgentEvent::MemoryCompact {
                    body: "# @memory-agent\n\n## Role\nCompact memory.\n".to_owned(),
                },
            )
            .await?;
            let memory = std::fs::read_to_string(&memory_path).map_err(|err| err.to_string())?;
            assert_eq!(memory, "# @memory-agent\n\n## Role\nCompact memory.\n");
            let _ = std::fs::remove_dir_all(dir);
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn channel_events_create_channel_and_invite_agents() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let agent_id = insert_test_agent(&pool, "creator-agent").await?;
            let reviewer_id = insert_test_agent(&pool, "reviewer-agent").await?;
            let run_id: Uuid = sqlx::query_scalar(
                r#"
                insert into agent_runs (agent_id, command, status)
                values ($1, 'codex app-server', 'running')
                returning id
                "#,
            )
            .bind(agent_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            handle_agent_event(
                &pool,
                agent_id,
                run_id,
                AgentEvent::ChannelCreate {
                    name: "Feature Room".to_owned(),
                    description: Some("long-lived feature coordination".to_owned()),
                    agent_handles: Some(vec!["@reviewer-agent".to_owned()]),
                },
            )
            .await?;
            let channel_id: Uuid =
                sqlx::query_scalar("select id from channels where name = 'feature-room'")
                    .fetch_one(&pool)
                    .await
                    .map_err(|err| err.to_string())?;
            let members: i64 = sqlx::query_scalar(
                r#"
                select count(*)
                from channel_members
                where channel_id = $1 and agent_id in ($2, $3)
                "#,
            )
            .bind(channel_id)
            .bind(agent_id)
            .bind(reviewer_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(members, 2);
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn profile_update_event_updates_agent_profile() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let agent_id = insert_test_agent(&pool, "profile-agent").await?;
            let run_id: Uuid = sqlx::query_scalar(
                r#"
                insert into agent_runs (agent_id, command, status)
                values ($1, 'codex app-server', 'running')
                returning id
                "#,
            )
            .bind(agent_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;

            handle_agent_event(
                &pool,
                agent_id,
                run_id,
                AgentEvent::ProfileUpdate {
                    display_name: Some("Profile Agent".to_owned()),
                    role: Some("vision reviewer".to_owned()),
                    avatar: Some("P".to_owned()),
                    description: Some("Reviews screenshots and agent handoffs.".to_owned()),
                },
            )
            .await?;

            let row = sqlx::query(
                "select display_name, role, avatar, description from agents where id = $1",
            )
            .bind(agent_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(row.get::<String, _>("display_name"), "Profile Agent");
            assert_eq!(row.get::<String, _>("role"), "vision reviewer");
            assert_eq!(row.get::<String, _>("avatar"), "P");
            assert_eq!(
                row.get::<String, _>("description"),
                "Reviews screenshots and agent handoffs."
            );
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn history_context_and_attachment_tool_expose_attachment_paths() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let channel_id = insert_test_channel(&pool, "vision-channel").await?;
            let message_id: Uuid = sqlx::query_scalar(
                r#"
                insert into messages (channel_id, sender_name, sender_role, body, is_task)
                values ($1, 'Dylan', 'owner', 'Please inspect this screenshot', false)
                returning id
                "#,
            )
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let image_path =
                std::env::temp_dir().join(format!("lantor-vision-{}.png", Uuid::new_v4()));
            std::fs::write(&image_path, b"fake image bytes").map_err(|err| err.to_string())?;
            let attachment_id: Uuid = sqlx::query_scalar(
                r#"
                insert into message_attachments (
                    message_id, original_name, mime_type, size_bytes, storage_path
                )
                values ($1, 'screen.png', 'image/png', 16, $2)
                returning id
                "#,
            )
            .bind(message_id)
            .bind(image_path.to_string_lossy().to_string())
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;

            let history = agent_context_history_read(
                &pool,
                &[
                    "history-read".to_owned(),
                    "--target".to_owned(),
                    "#vision-channel".to_owned(),
                ],
            )
            .await?;
            assert!(history.contains(&attachment_id.to_string()));
            assert!(history.contains("local_path="));
            assert!(history.contains("attachment-info"));

            let info = agent_context_attachment_info(
                &pool,
                &[
                    "attachment-info".to_owned(),
                    "--attachment-id".to_owned(),
                    attachment_id.to_string(),
                ],
            )
            .await?;
            assert!(info.contains("mime=image/png"));
            assert!(info.contains("file_exists=true"));
            assert!(info.contains("vision_hint="));
            let _ = std::fs::remove_file(image_path);
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn agent_inspect_tool_summarizes_profile_runs_requests_and_activity() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let agent_id = insert_test_agent(&pool, "inspectable").await?;
            let channel_id = insert_test_channel(&pool, "inspect-channel").await?;
            sqlx::query(
                r#"
                update agents
                set display_name = 'Inspectable Agent',
                    role = 'review specialist',
                    description = 'Reviews work from other agents.',
                    daily_budget_micros = 2500000
                where id = $1
                "#,
            )
            .bind(agent_id)
            .execute(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let run_id: Uuid = sqlx::query_scalar(
                r#"
                insert into agent_runs (
                    agent_id, command, status, input_tokens, output_tokens, cost_micros
                )
                values ($1, 'codex app-server', 'complete', 100, 20, 500)
                returning id
                "#,
            )
            .bind(agent_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            sqlx::query(
                r#"
                insert into agent_work_items (agent_id, channel_id, title, context, status, source_kind)
                values ($1, $2, 'Review implementation', 'context', 'done', 'collaboration')
                "#,
            )
            .bind(agent_id)
            .bind(channel_id)
            .execute(&pool)
            .await
            .map_err(|err| err.to_string())?;
            record_agent_activity(
                &pool,
                Some(agent_id),
                Some(run_id),
                "thinking",
                "Reading recent context",
                "{}".to_owned(),
            )
            .await?;

            let output = agent_context_agent_inspect(
                &pool,
                &[
                    "agent-inspect".to_owned(),
                    "--target".to_owned(),
                    "@inspectable".to_owned(),
                ],
            )
            .await?;
            assert!(output.contains("Agent @inspectable"));
            assert!(output.contains("display_name=Inspectable Agent"));
            assert!(output.contains("role=review specialist"));
            assert!(output.contains("recent_runs:"));
            assert!(output.contains("recent_requests:"));
            assert!(output.contains("recent_activity:"));
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn streaming_agent_messages_append_and_finish() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let agent_id = insert_test_agent(&pool, "streamer").await?;
            let channel_id = insert_test_channel(&pool, "streaming").await?;
            let stream_key = "run-1:item-1";

            let message_id = append_streaming_agent_message(
                &pool, agent_id, channel_id, None, stream_key, "Hel",
            )
            .await?;
            let same_message_id =
                append_streaming_agent_message(&pool, agent_id, channel_id, None, stream_key, "lo")
                    .await?;
            assert_eq!(message_id, same_message_id);
            finish_streaming_agent_message(&pool, stream_key, "complete").await?;

            let messages = load_messages(&pool).await?;
            let message = messages
                .iter()
                .find(|message| message.id == message_id)
                .expect("streaming message should be visible in bootstrap payload");
            assert_eq!(message.body, "Hello");
            assert_eq!(message.delivery_state, "complete");
            assert_eq!(message.stream_key, stream_key);

            upsert_runtime_thread_id(&pool, agent_id, "codex", "thread-1", "idle").await?;
            assert_eq!(
                load_runtime_thread_id(&pool, agent_id, "codex").await?,
                Some("thread-1".to_owned())
            );
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        result.unwrap();
    }

    #[tokio::test]
    async fn streaming_placeholder_is_reused_for_visible_reply() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let agent_id = insert_test_agent(&pool, "placeholder-agent").await?;
            let channel_id = insert_test_channel(&pool, "placeholder-channel").await?;
            let run_id = Uuid::new_v4();
            let pending_stream_key = codex_pending_stream_key(run_id);
            let final_stream_key = format!("{run_id}:item-1");

            let placeholder_id = ensure_streaming_agent_message(
                &pool,
                agent_id,
                channel_id,
                None,
                &pending_stream_key,
            )
            .await?;
            let messages = load_messages(&pool).await?;
            let placeholder = messages
                .iter()
                .find(|message| message.id == placeholder_id)
                .expect("placeholder should be visible in bootstrap payload");
            assert_eq!(placeholder.body, "");
            assert_eq!(placeholder.delivery_state, "streaming");
            assert_eq!(placeholder.stream_key, pending_stream_key);

            let adopted_id =
                adopt_streaming_agent_message_key(&pool, &pending_stream_key, &final_stream_key)
                    .await?;
            assert_eq!(adopted_id, Some(placeholder_id));
            assert!(streaming_message_body_is_empty(&pool, &final_stream_key).await?);

            let message_id = append_streaming_agent_message(
                &pool,
                agent_id,
                channel_id,
                None,
                &final_stream_key,
                "Done",
            )
            .await?;
            assert_eq!(message_id, placeholder_id);
            finish_streaming_agent_message(&pool, &final_stream_key, "complete").await?;

            let messages = load_messages(&pool).await?;
            let message = messages
                .iter()
                .find(|message| message.id == placeholder_id)
                .expect("final reply should reuse placeholder message");
            assert_eq!(message.body, "Done");
            assert_eq!(message.delivery_state, "complete");
            assert_eq!(message.stream_key, final_stream_key);
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        result.unwrap();
    }

    #[tokio::test]
    async fn activity_only_streaming_reply_keeps_status_message() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let agent_id = insert_test_agent(&pool, "activity-only-agent").await?;
            let channel_id = insert_test_channel(&pool, "activity-only-channel").await?;
            let run_id: Uuid = sqlx::query_scalar(
                r#"
                insert into agent_runs (agent_id, command, status)
                values ($1, 'codex app-server', 'running')
                returning id
                "#,
            )
            .bind(agent_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let stream_key = format!("{run_id}:item-activity");
            let event = json!({
                "type": "activity",
                "kind": "thinking",
                "title": "Checking source",
                "detail": "Tracing the code path"
            });

            let message_id = append_streaming_agent_message(
                &pool,
                agent_id,
                channel_id,
                None,
                &stream_key,
                &format!("LANTOR_EVENT {event}\n"),
            )
            .await?;
            finish_streaming_agent_message(&pool, &stream_key, "complete").await?;

            let row = sqlx::query("select body, delivery_state from messages where id = $1")
                .bind(message_id)
                .fetch_one(&pool)
                .await
                .map_err(|err| err.to_string())?;
            assert_eq!(row.get::<String, _>("body"), "");
            assert_eq!(row.get::<String, _>("delivery_state"), "complete");

            let activity_count: i64 = sqlx::query_scalar(
                "select count(*) from agent_activities where run_id = $1 and title = 'Checking source'",
            )
            .bind(run_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(activity_count, 1);

            let leaked_messages: i64 = sqlx::query_scalar(
                "select count(*) from messages where body like '%LANTOR_EVENT%'",
            )
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(leaked_messages, 0);
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        result.unwrap();
    }

    #[tokio::test]
    async fn visible_reply_replaces_prior_activity_only_status_message() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let agent_id = insert_test_agent(&pool, "visible-after-progress-agent").await?;
            let channel_id = insert_test_channel(&pool, "visible-after-progress-channel").await?;
            let run_id: Uuid = sqlx::query_scalar(
                r#"
                insert into agent_runs (agent_id, command, status)
                values ($1, 'codex app-server', 'running')
                returning id
                "#,
            )
            .bind(agent_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let progress_stream_key = format!("{run_id}:item-progress");
            let final_stream_key = format!("{run_id}:item-final");
            let event = json!({
                "type": "activity",
                "kind": "thinking",
                "title": "Checking source",
                "detail": "Tracing the code path"
            });

            let progress_message_id = append_streaming_agent_message(
                &pool,
                agent_id,
                channel_id,
                None,
                &progress_stream_key,
                &format!("LANTOR_EVENT {event}\n"),
            )
            .await?;
            finish_streaming_agent_message(&pool, &progress_stream_key, "complete").await?;

            let final_message_id = append_streaming_agent_message(
                &pool,
                agent_id,
                channel_id,
                None,
                &final_stream_key,
                "Done",
            )
            .await?;
            finish_streaming_agent_message(&pool, &final_stream_key, "complete").await?;

            assert_ne!(progress_message_id, final_message_id);
            let progress_message_count: i64 =
                sqlx::query_scalar("select count(*) from messages where id = $1")
                    .bind(progress_message_id)
                    .fetch_one(&pool)
                    .await
                    .map_err(|err| err.to_string())?;
            assert_eq!(progress_message_count, 0);

            let messages = load_messages(&pool).await?;
            let final_message = messages
                .iter()
                .find(|message| message.id == final_message_id)
                .expect("final reply should remain visible");
            assert_eq!(final_message.body, "Done");
            assert_eq!(final_message.delivery_state, "complete");
            assert_eq!(final_message.stream_key, final_stream_key);
            assert_eq!(
                messages
                    .iter()
                    .filter(|message| message.stream_key.starts_with(&run_id.to_string()))
                    .count(),
                1
            );
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        result.unwrap();
    }

    #[tokio::test]
    async fn silent_streaming_reply_hides_message_and_marks_work_item() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let agent_id = insert_test_agent(&pool, "silent-agent").await?;
            let channel_id = insert_test_channel(&pool, "silent-channel").await?;
            let run_id: Uuid = sqlx::query_scalar(
                r#"
                insert into agent_runs (agent_id, command, status)
                values ($1, 'codex app-server', 'running')
                returning id
                "#,
            )
            .bind(agent_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let work_item_id: Uuid = sqlx::query_scalar(
                r#"
                insert into agent_work_items (agent_id, channel_id, title, context, status, run_id)
                values ($1, $2, 'hello', 'hi', 'running', $3)
                returning id
                "#,
            )
            .bind(agent_id)
            .bind(channel_id)
            .bind(run_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let inbox_item_id: Uuid = sqlx::query_scalar(
                r#"
                insert into agent_inbox_items (
                    agent_id, channel_id, kind, state, title, body_preview, work_item_id
                )
                values ($1, $2, 'dm', 'processing', 'hello', 'hi', $3)
                returning id
                "#,
            )
            .bind(agent_id)
            .bind(channel_id)
            .bind(work_item_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let stream_key = "silent-run:item-1";
            let message_id = append_streaming_agent_message(
                &pool,
                agent_id,
                channel_id,
                None,
                stream_key,
                "LANTOR_SILENT_REPLY: greeting only",
            )
            .await?;
            assert!(
                maybe_hide_silent_streaming_reply(
                    &pool,
                    agent_id,
                    run_id,
                    Some(work_item_id),
                    stream_key,
                )
                .await?
            );

            let remaining: i64 = sqlx::query_scalar("select count(*) from messages where id = $1")
                .bind(message_id)
                .fetch_one(&pool)
                .await
                .map_err(|err| err.to_string())?;
            assert_eq!(remaining, 0);
            let status: String =
                sqlx::query_scalar("select status from agent_work_items where id = $1")
                    .bind(work_item_id)
                    .fetch_one(&pool)
                    .await
                    .map_err(|err| err.to_string())?;
            assert_eq!(status, "silent");
            let inbox_state: String =
                sqlx::query_scalar("select state from agent_inbox_items where id = $1")
                    .bind(inbox_item_id)
                    .fetch_one(&pool)
                    .await
                    .map_err(|err| err.to_string())?;
            assert_eq!(inbox_state, "archived");
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn streaming_reminder_control_line_is_consumed() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let agent_id = insert_test_agent(&pool, "reminder-agent").await?;
            let channel_id = insert_test_channel(&pool, "reminder-control").await?;
            let root_id: Uuid = sqlx::query_scalar(
                r#"
                insert into messages (channel_id, sender_name, sender_role, body, is_task)
                values ($1, 'Dylan', 'owner', 'remind me later', false)
                returning id
                "#,
            )
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let work_item_id: Uuid = sqlx::query_scalar(
                r#"
                insert into agent_work_items (
                    agent_id, channel_id, thread_root_id, source_message_id, title, context, status
                )
                values ($1, $2, $3, $3, 'set reminder', 'remind me later', 'running')
                returning id
                "#,
            )
            .bind(agent_id)
            .bind(channel_id)
            .bind(root_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let run_id: Uuid = sqlx::query_scalar(
                r#"
                insert into agent_runs (agent_id, work_item_id, command, status)
                values ($1, $2, 'codex app-server', 'running')
                returning id
                "#,
            )
            .bind(agent_id)
            .bind(work_item_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let due_at = (Utc::now() + ChronoDuration::minutes(30)).to_rfc3339();
            let event = json!({
                "type": "reminder_create",
                "when": due_at,
                "title": "Check PR",
                "note": "Look at CI"
            });
            let stream_key = "reminder-run:item-1";
            let body = format!("I'll remind you.\nLANTOR_EVENT {event}");
            let message_id = append_streaming_agent_message(
                &pool,
                agent_id,
                channel_id,
                Some(root_id),
                stream_key,
                &body,
            )
            .await?;

            let hidden = consume_streaming_agent_control_lines(
                &pool,
                agent_id,
                run_id,
                Some(work_item_id),
                stream_key,
            )
            .await?;
            assert!(!hidden);

            let visible_body: String =
                sqlx::query_scalar("select body from messages where id = $1")
                    .bind(message_id)
                    .fetch_one(&pool)
                    .await
                    .map_err(|err| err.to_string())?;
            assert_eq!(visible_body, "I'll remind you.");

            let reminders = load_reminders(&pool).await?;
            assert_eq!(reminders.len(), 1);
            assert_eq!(reminders[0].title, "Check PR");
            assert_eq!(reminders[0].note, "Look at CI");
            assert_eq!(reminders[0].creator_agent_id, Some(agent_id));
            assert_eq!(reminders[0].channel_id, Some(channel_id));
            assert_eq!(reminders[0].thread_root_id, Some(root_id));
            assert_eq!(reminders[0].message_id, Some(root_id));
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn streaming_artifact_control_line_is_consumed_and_hidden() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let agent_id = insert_test_agent(&pool, "artifact-stream-agent").await?;
            let channel_id = insert_test_channel(&pool, "artifact-stream-control").await?;
            let root_id: Uuid = sqlx::query_scalar(
                r#"
                insert into messages (channel_id, sender_name, sender_role, body, is_task)
                values ($1, 'Dylan', 'owner', 'make an architecture artifact', false)
                returning id
                "#,
            )
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let work_item_id: Uuid = sqlx::query_scalar(
                r#"
                insert into agent_work_items (
                    agent_id, channel_id, thread_root_id, source_message_id, title, context, status
                )
                values ($1, $2, $3, $3, 'create artifact', 'make an architecture artifact', 'running')
                returning id
                "#,
            )
            .bind(agent_id)
            .bind(channel_id)
            .bind(root_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let run_id: Uuid = sqlx::query_scalar(
                r#"
                insert into agent_runs (agent_id, work_item_id, command, status)
                values ($1, $2, 'codex app-server', 'running')
                returning id
                "#,
            )
            .bind(agent_id)
            .bind(work_item_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let event = json!({
                "type": "artifact_create",
                "channel_id": channel_id,
                "thread_root_id": root_id,
                "kind": "markdown",
                "title": "Architecture report",
                "summary": "Markdown architecture summary.",
                "content": "# Architecture\n\n- UI\n- Backend\n- Postgres"
            });
            let stream_key = "artifact-run:item-1";
            let raw_control_message_id = append_streaming_agent_message(
                &pool,
                agent_id,
                channel_id,
                Some(root_id),
                stream_key,
                &format!("LANTOR_EVENT {event}"),
            )
            .await?;

            let hidden = consume_streaming_agent_control_lines(
                &pool,
                agent_id,
                run_id,
                Some(work_item_id),
                stream_key,
            )
            .await?;
            assert!(hidden);

            let raw_remaining: i64 =
                sqlx::query_scalar("select count(*) from messages where id = $1")
                    .bind(raw_control_message_id)
                    .fetch_one(&pool)
                    .await
                    .map_err(|err| err.to_string())?;
            assert_eq!(raw_remaining, 0);

            let artifact = sqlx::query(
                r#"
                select kind, title, content
                from artifacts
                where channel_id = $1 and thread_root_id = $2
                "#,
            )
            .bind(channel_id)
            .bind(root_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(artifact.get::<String, _>("kind"), "markdown");
            assert_eq!(artifact.get::<String, _>("title"), "Architecture report");
            assert!(artifact
                .get::<String, _>("content")
                .contains("# Architecture"));

            let visible_messages = load_messages(&pool).await?;
            assert!(!visible_messages
                .iter()
                .any(|message| message.body.contains("LANTOR_EVENT")));
            assert!(visible_messages.iter().any(|message| message
                .body
                .contains("Created artifact: Architecture report")));
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn streaming_finish_consumes_channel_create_control_line() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let agent_id = insert_test_agent(&pool, "creator-agent").await?;
            let reviewer_id = insert_test_agent(&pool, "Hancock").await?;
            let source_channel_id = insert_test_channel(&pool, "source-channel").await?;
            let run_id: Uuid = sqlx::query_scalar(
                r#"
                insert into agent_runs (agent_id, command, status)
                values ($1, 'claude stream-json', 'running')
                returning id
                "#,
            )
            .bind(agent_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let event = json!({
                "type": "channel_create",
                "name": "lantor-ui-design",
                "description": "讨论 SLock UI 设计后续工作",
                "agent_handles": ["hancock"]
            });
            let stream_key = format!("{run_id}:claude-assistant");
            let message_id = append_streaming_agent_message(
                &pool,
                agent_id,
                source_channel_id,
                None,
                &stream_key,
                &format!("好的，我来创建。\n\nLANTOR_EVENT {event}"),
            )
            .await?;

            finish_streaming_agent_message(&pool, &stream_key, "complete").await?;

            let visible_body: String =
                sqlx::query_scalar("select body from messages where id = $1")
                    .bind(message_id)
                    .fetch_one(&pool)
                    .await
                    .map_err(|err| err.to_string())?;
            assert_eq!(visible_body, "好的，我来创建。");

            let channel_id: Uuid =
                sqlx::query_scalar("select id from channels where name = 'lantor-ui-design'")
                    .fetch_one(&pool)
                    .await
                    .map_err(|err| err.to_string())?;
            let member_count: i64 = sqlx::query_scalar(
                r#"
                select count(*)
                from channel_members
                where channel_id = $1 and agent_id in ($2, $3)
                "#,
            )
            .bind(channel_id)
            .bind(agent_id)
            .bind(reviewer_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(member_count, 2);

            let leaked_messages: i64 = sqlx::query_scalar(
                "select count(*) from messages where body like '%LANTOR_EVENT%'",
            )
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(leaked_messages, 0);
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn streaming_unsupported_artifact_control_line_keeps_status_message() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let agent_id = insert_test_agent(&pool, "unsupported-artifact-agent").await?;
            let channel_id = insert_test_channel(&pool, "unsupported-artifact-control").await?;
            let run_id: Uuid = sqlx::query_scalar(
                r#"
                insert into agent_runs (agent_id, command, status)
                values ($1, 'codex app-server', 'running')
                returning id
                "#,
            )
            .bind(agent_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let event = json!({
                "type": "artifact_create",
                "channel_id": channel_id,
                "kind": "html",
                "title": "Unsupported HTML",
                "content": "<main>not supported</main>"
            });
            let stream_key = "unsupported-artifact-run:item-1";
            let raw_control_message_id = append_streaming_agent_message(
                &pool,
                agent_id,
                channel_id,
                None,
                stream_key,
                &format!("LANTOR_EVENT {event}"),
            )
            .await?;

            let hidden =
                consume_streaming_agent_control_lines(&pool, agent_id, run_id, None, stream_key)
                    .await?;
            assert!(!hidden);

            let raw_remaining: i64 =
                sqlx::query_scalar("select count(*) from messages where id = $1")
                    .bind(raw_control_message_id)
                    .fetch_one(&pool)
                    .await
                    .map_err(|err| err.to_string())?;
            assert_eq!(raw_remaining, 1);

            finish_streaming_agent_message(&pool, stream_key, "complete").await?;
            let raw_body: String = sqlx::query_scalar("select body from messages where id = $1")
                .bind(raw_control_message_id)
                .fetch_one(&pool)
                .await
                .map_err(|err| err.to_string())?;
            assert_eq!(raw_body, "");

            let artifact_count: i64 =
                sqlx::query_scalar("select count(*) from artifacts where channel_id = $1")
                    .bind(channel_id)
                    .fetch_one(&pool)
                    .await
                    .map_err(|err| err.to_string())?;
            assert_eq!(artifact_count, 0);
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn dm_open_is_idempotent_and_cascades_with_agent() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let agent_id = insert_test_agent(&pool, "dm-idempotent").await?;
            let dm1 = open_dm_with_agent_in_pool(&pool, agent_id).await?;
            let dm2 = open_dm_with_agent_in_pool(&pool, agent_id).await?;
            assert_eq!(dm1, dm2);

            let dm_channel_id = Uuid::parse_str(&dm1).map_err(|err| err.to_string())?;
            let row = sqlx::query(
                r#"
                select c.kind, c.dm_agent_id, count(m.agent_id) as members
                from channels c
                left join channel_members m on m.channel_id = c.id
                where c.id = $1
                group by c.kind, c.dm_agent_id
                "#,
            )
            .bind(dm_channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let kind: String = row.get("kind");
            let dm_agent_id: Uuid = row.get("dm_agent_id");
            let members: i64 = row.get("members");
            assert_eq!(kind, "dm");
            assert_eq!(dm_agent_id, agent_id);
            assert_eq!(members, 1);

            sqlx::query("delete from agents where id = $1")
                .bind(agent_id)
                .execute(&pool)
                .await
                .map_err(|err| err.to_string())?;
            let remaining: i64 = sqlx::query_scalar("select count(*) from channels where id = $1")
                .bind(dm_channel_id)
                .fetch_one(&pool)
                .await
                .map_err(|err| err.to_string())?;
            assert_eq!(remaining, 0);
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn delete_agent_cascades_dm_and_preserves_sender_text() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let agent_id = insert_test_agent(&pool, "delete-me").await?;
            let channel_id = insert_test_channel(&pool, "delete-agent").await?;
            let dm_channel_id =
                Uuid::parse_str(&open_dm_with_agent_in_pool(&pool, agent_id).await?)
                    .map_err(|err| err.to_string())?;
            insert_agent_message(&pool, agent_id, dm_channel_id, None, "before delete", false)
                .await?;
            let channel_message_id =
                insert_agent_message(&pool, agent_id, channel_id, None, "channel message", false)
                    .await?;
            delete_agent_in_pool(&pool, agent_id).await?;

            let agent_count: i64 =
                sqlx::query_scalar("select count(*) from agents where id = $1")
                    .bind(agent_id)
                    .fetch_one(&pool)
                    .await
                    .map_err(|err| err.to_string())?;
            assert_eq!(agent_count, 0);

            let dm_count: i64 =
                sqlx::query_scalar("select count(*) from channels where id = $1")
                    .bind(dm_channel_id)
                    .fetch_one(&pool)
                    .await
                    .map_err(|err| err.to_string())?;
            assert_eq!(dm_count, 0);

            let deleted_activity: i64 = sqlx::query_scalar(
                "select count(*) from agent_activities where agent_handle = 'delete-me' and title = 'Agent profile deleted'",
            )
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(deleted_activity, 1);

            let message = sqlx::query(
                "select sender_agent_id, sender_name, body from messages where id = $1",
            )
            .bind(channel_message_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(message.get::<Option<Uuid>, _>("sender_agent_id"), None);
            assert_eq!(message.get::<String, _>("sender_name"), "delete-me");
            assert_eq!(message.get::<String, _>("body"), "channel message");
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn delete_channel_removes_timeline_and_unlinks_requests() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let agent_id = insert_test_agent(&pool, "channel-delete-agent").await?;
            let channel_id = insert_test_channel(&pool, "channel-delete").await?;
            sqlx::query("insert into channel_members (channel_id, agent_id) values ($1, $2)")
                .bind(channel_id)
                .bind(agent_id)
                .execute(&pool)
                .await
                .map_err(|err| err.to_string())?;
            let message_id: Uuid = sqlx::query_scalar(
                r#"
                insert into messages (channel_id, sender_name, sender_role, body, is_task)
                values ($1, 'Dylan', 'owner', 'delete channel body', true)
                returning id
                "#,
            )
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            sqlx::query(
                r#"
                insert into tasks (message_id, channel_id, title, status, assignee_agent_id)
                values ($1, $2, 'delete channel task', 'in_progress', $3)
                "#,
            )
            .bind(message_id)
            .bind(channel_id)
            .bind(agent_id)
            .execute(&pool)
            .await
            .map_err(|err| err.to_string())?;
            sqlx::query(
                r#"
                insert into reminders (channel_id, creator_agent_id, title, due_at)
                values ($1, $2, 'channel reminder', strftime('%Y-%m-%dT%H:%M:%f+00:00','now','+1 hour'))
                "#,
            )
            .bind(channel_id)
            .bind(agent_id)
            .execute(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let work_item_id: Uuid = sqlx::query_scalar(
                r#"
                insert into agent_work_items (agent_id, channel_id, source_message_id, title, context, status)
                values ($1, $2, $3, 'request', 'context', 'queued')
                returning id
                "#,
            )
            .bind(agent_id)
            .bind(channel_id)
            .bind(message_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;

            delete_channel_in_pool(&pool, channel_id).await?;

            let channel_count: i64 =
                sqlx::query_scalar("select count(*) from channels where id = $1")
                    .bind(channel_id)
                    .fetch_one(&pool)
                    .await
                    .map_err(|err| err.to_string())?;
            assert_eq!(channel_count, 0);
            let message_count: i64 =
                sqlx::query_scalar("select count(*) from messages where id = $1")
                    .bind(message_id)
                    .fetch_one(&pool)
                    .await
                    .map_err(|err| err.to_string())?;
            assert_eq!(message_count, 0);
            let unlinked_work_item_channel: Option<Uuid> =
                sqlx::query_scalar("select channel_id from agent_work_items where id = $1")
                    .bind(work_item_id)
                    .fetch_one(&pool)
                    .await
                    .map_err(|err| err.to_string())?;
            assert_eq!(unlinked_work_item_channel, None);
            let reminder_channel_count: i64 = sqlx::query_scalar(
                "select count(*) from reminders where title = 'channel reminder' and channel_id is null",
            )
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(reminder_channel_count, 1);
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn dm_rejects_tasks_and_auto_dispatches_owner_messages() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let agent_id = insert_test_agent(&pool, "dm-dispatch").await?;
            let dm_channel_id =
                Uuid::parse_str(&open_dm_with_agent_in_pool(&pool, agent_id).await?)
                    .map_err(|err| err.to_string())?;

            let owner_task_err =
                send_owner_message_in_pool(&pool, dm_channel_id, None, "task body", true, vec![])
                    .await
                    .unwrap_err();
            assert!(owner_task_err.contains("direct messages do not support tasks"));

            let agent_task_err =
                insert_agent_message(&pool, agent_id, dm_channel_id, None, "task body", true)
                    .await
                    .unwrap_err();
            assert!(agent_task_err.contains("direct messages do not support tasks"));

            send_owner_message_in_pool(
                &pool,
                dm_channel_id,
                None,
                "please inspect this",
                false,
                vec![],
            )
            .await?;
            let work_items: i64 = sqlx::query_scalar(
                r#"
                select count(*)
                from agent_work_items
                where channel_id = $1 and agent_id = $2 and status = 'queued'
                "#,
            )
            .bind(dm_channel_id)
            .bind(agent_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(work_items, 1);
            let source_kind: String = sqlx::query_scalar(
                "select source_kind from agent_work_items where channel_id = $1 and agent_id = $2",
            )
            .bind(dm_channel_id)
            .bind(agent_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(source_kind, "inbox_wake");
            let inbox = sqlx::query(
                r#"
                select i.id as inbox_item_id, i.kind, i.state, i.work_item_id, w.id as linked_work_item_id, w.inbox_item_id as linked_inbox_item_id
                from agent_inbox_items i
                join agent_work_items w on w.id = i.work_item_id
                where i.channel_id = $1 and i.agent_id = $2
                "#,
            )
            .bind(dm_channel_id)
            .bind(agent_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(inbox.get::<String, _>("kind"), "dm");
            assert_eq!(inbox.get::<String, _>("state"), "processing");
            assert_eq!(
                inbox.get::<Option<Uuid>, _>("work_item_id"),
                Some(inbox.get::<Uuid, _>("linked_work_item_id"))
            );
            assert_eq!(
                inbox.get::<Option<Uuid>, _>("linked_inbox_item_id"),
                Some(inbox.get::<Uuid, _>("inbox_item_id"))
            );
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn inbox_wake_creates_work_items_without_serializing_unread_items() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let agent_id = insert_test_agent(&pool, "inbox-reschedule").await?;
            let dm_channel_id =
                Uuid::parse_str(&open_dm_with_agent_in_pool(&pool, agent_id).await?)
                    .map_err(|err| err.to_string())?;

            send_owner_message_in_pool(
                &pool,
                dm_channel_id,
                None,
                "first inbox item",
                false,
                vec![],
            )
            .await?;
            send_owner_message_in_pool(
                &pool,
                dm_channel_id,
                None,
                "second inbox item",
                false,
                vec![],
            )
            .await?;

            let initial_work_items: Vec<Uuid> = sqlx::query_scalar(
                "select id from agent_work_items where agent_id = $1 order by created_at asc",
            )
            .bind(agent_id)
            .fetch_all(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(initial_work_items.len(), 2);

            let state_counts = sqlx::query(
                r#"
                select
                    count(*) filter (where state = 'processing') as processing,
                    count(*) filter (where state = 'unread') as unread
                from agent_inbox_items
                where agent_id = $1
                "#,
            )
            .bind(agent_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(state_counts.get::<Option<i64>, _>("processing"), Some(2));
            assert_eq!(state_counts.get::<Option<i64>, _>("unread"), Some(0));

            sqlx::query("update agent_work_items set status = 'done' where id = $1")
                .bind(initial_work_items[0])
                .execute(&pool)
                .await
                .map_err(|err| err.to_string())?;
            notify_ui_work_item_changed(&pool, initial_work_items[0], "test_done").await;

            let final_work_items: Vec<Uuid> = sqlx::query_scalar(
                "select id from agent_work_items where agent_id = $1 order by created_at asc",
            )
            .bind(agent_id)
            .fetch_all(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(final_work_items.len(), 2);
            let state_counts = sqlx::query(
                r#"
                select
                    count(*) filter (where state = 'archived') as archived,
                    count(*) filter (where state = 'processing') as processing,
                    count(*) filter (where state = 'unread') as unread
                from agent_inbox_items
                where agent_id = $1
                "#,
            )
            .bind(agent_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(state_counts.get::<Option<i64>, _>("archived"), Some(1));
            assert_eq!(state_counts.get::<Option<i64>, _>("processing"), Some(1));
            assert_eq!(state_counts.get::<Option<i64>, _>("unread"), Some(0));
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn inbox_wake_batches_unread_items_for_same_thread() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let agent_id = insert_test_agent(&pool, "inbox-batch").await?;
            let channel_id = insert_test_channel(&pool, "batch-thread").await?;
            sqlx::query(
                r#"
                insert into channel_members (channel_id, agent_id)
                values ($1, $2)
                "#,
            )
            .bind(channel_id)
            .bind(agent_id)
            .execute(&pool)
            .await
            .map_err(|err| err.to_string())?;

            let root_id: Uuid = sqlx::query_scalar(
                r#"
                insert into messages (channel_id, sender_name, sender_role, body, is_task)
                values ($1, 'Dylan', 'owner', '@inbox-batch please investigate', false)
                returning id
                "#,
            )
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            sqlx::query(
                r#"
                insert into agent_work_items (
                    agent_id, channel_id, thread_root_id, source_message_id, title, context, status
                )
                values ($1, $2, $3, $3, 'Initial dispatch', 'Initial dispatch', 'done')
                "#,
            )
            .bind(agent_id)
            .bind(channel_id)
            .bind(root_id)
            .execute(&pool)
            .await
            .map_err(|err| err.to_string())?;

            send_owner_message_in_pool(
                &pool,
                channel_id,
                Some(root_id),
                "first pending follow-up",
                false,
                vec![],
            )
            .await?;
            send_owner_message_in_pool(
                &pool,
                channel_id,
                Some(root_id),
                "second pending follow-up",
                false,
                vec![],
            )
            .await?;

            let rows = sqlx::query(
                r#"
                select
                    w.id,
                    w.title,
                    w.context,
                    count(i.id) as inbox_count,
                    count(*) filter (where i.state = 'processing') as processing_count
                from agent_work_items w
                join agent_inbox_items i on i.work_item_id = w.id
                where w.agent_id = $1
                  and w.channel_id = $2
                  and w.thread_root_id = $3
                  and w.source_kind = 'inbox_wake'
                group by w.id, w.title, w.context
                "#,
            )
            .bind(agent_id)
            .bind(channel_id)
            .bind(root_id)
            .fetch_all(&pool)
            .await
            .map_err(|err| err.to_string())?;

            assert_eq!(rows.len(), 1);
            let work_item_id: Uuid = rows[0].get("id");
            assert_eq!(rows[0].get::<i64, _>("inbox_count"), 2);
            assert_eq!(rows[0].get::<i64, _>("processing_count"), 2);
            assert_eq!(
                rows[0].get::<String, _>("title"),
                "Process inbox: first pending follow-up (+1 more)"
            );
            let context: String = rows[0].get("context");
            assert!(context.contains("batches 2 inbox items"));
            assert!(context.contains("first pending follow-up"));
            assert!(context.contains("second pending follow-up"));

            let start_commands: i64 = sqlx::query_scalar(
                r#"
                select count(*)
                from supervisor_commands
                where agent_id = $1 and command_type = 'start_agent'
                "#,
            )
            .bind(agent_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(start_commands, 1);

            sqlx::query("update agent_work_items set status = 'done' where id = $1")
                .bind(work_item_id)
                .execute(&pool)
                .await
                .map_err(|err| err.to_string())?;
            notify_ui_work_item_changed(&pool, work_item_id, "test_done").await;
            let archived: i64 = sqlx::query_scalar(
                r#"
                select count(*)
                from agent_inbox_items
                where agent_id = $1 and work_item_id = $2 and state = 'archived'
                "#,
            )
            .bind(agent_id)
            .bind(work_item_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(archived, 2);
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn agent_context_inbox_tools_list_read_and_archive_items() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let agent_id = insert_test_agent(&pool, "inbox-tool").await?;
            let dm_channel_id =
                Uuid::parse_str(&open_dm_with_agent_in_pool(&pool, agent_id).await?)
                    .map_err(|err| err.to_string())?;
            send_owner_message_in_pool(
                &pool,
                dm_channel_id,
                None,
                "please inspect inbox tools",
                false,
                vec![],
            )
            .await?;
            let inbox_id: Uuid = sqlx::query_scalar(
                "select id from agent_inbox_items where agent_id = $1 and kind = 'dm'",
            )
            .bind(agent_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;

            let list = agent_context_inbox_list(
                &pool,
                &[
                    "inbox-list".to_owned(),
                    "--target".to_owned(),
                    "@inbox-tool".to_owned(),
                    "--state".to_owned(),
                    "active".to_owned(),
                ],
            )
            .await?;
            assert!(list.contains("Lantor inbox for @inbox-tool"));
            assert!(list.contains("kind=dm"));
            assert!(list.contains("please inspect inbox tools"));

            let read = agent_context_inbox_read(
                &pool,
                &[
                    "inbox-read".to_owned(),
                    "--target".to_owned(),
                    "@inbox-tool".to_owned(),
                    "--inbox-id".to_owned(),
                    short_id(inbox_id),
                ],
            )
            .await?;
            assert!(read.contains("source_message:"));
            assert!(read.contains("please inspect inbox tools"));
            assert!(read.contains(&format!("--target \"{dm_channel_id}")));
            assert!(!read.contains("--target \"dm:"));
            assert!(read
                .contains("archive_note=linked work-item inbox items are archived automatically"));
            assert!(!read.contains("archive_when_done="));

            let archived = agent_context_inbox_archive(
                &pool,
                &[
                    "inbox-archive".to_owned(),
                    "--target".to_owned(),
                    "@inbox-tool".to_owned(),
                    "--inbox-id".to_owned(),
                    short_id(inbox_id),
                ],
            )
            .await?;
            assert!(archived.contains("Archived Lantor inbox item"));
            let state: String =
                sqlx::query_scalar("select state from agent_inbox_items where id = $1")
                    .bind(inbox_id)
                    .fetch_one(&pool)
                    .await
                    .map_err(|err| err.to_string())?;
            assert_eq!(state, "archived");
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn owner_channel_root_message_without_mentions_delivers_to_member_agent_inbox() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let agent_id = insert_test_agent(&pool, "channel-listener").await?;
            let channel_id = insert_test_channel(&pool, "channel-root-delivery").await?;
            sqlx::query(
                r#"
                insert into channel_members (channel_id, agent_id)
                values ($1, $2)
                "#,
            )
            .bind(channel_id)
            .bind(agent_id)
            .execute(&pool)
            .await
            .map_err(|err| err.to_string())?;

            send_owner_message_in_pool(
                &pool,
                channel_id,
                None,
                "Lantor README needs a quick review",
                false,
                vec![],
            )
            .await?;
            let message_id: Uuid =
                sqlx::query_scalar("select id from messages where channel_id = $1 and body = $2")
                    .bind(channel_id)
                    .bind("Lantor README needs a quick review")
                    .fetch_one(&pool)
                    .await
                    .map_err(|err| err.to_string())?;
            let row = sqlx::query(
                r#"
                select
                    w.source_kind,
                    w.thread_root_id,
                    w.source_message_id,
                    i.kind as inbox_kind,
                    i.priority,
                    i.body_preview
                from agent_work_items w
                join agent_inbox_items i on i.work_item_id = w.id
                where w.agent_id = $1 and w.source_message_id = $2
                "#,
            )
            .bind(agent_id)
            .bind(message_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(row.get::<String, _>("source_kind"), "inbox_wake");
            assert_eq!(row.get::<String, _>("inbox_kind"), "channel_message");
            assert_eq!(row.get::<i32, _>("priority"), 35);
            assert_eq!(
                row.get::<Option<Uuid>, _>("thread_root_id"),
                Some(message_id)
            );
            assert_eq!(
                row.get::<Option<Uuid>, _>("source_message_id"),
                Some(message_id)
            );
            assert!(row
                .get::<String, _>("body_preview")
                .contains("README needs"));
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn owner_thread_followup_dispatches_to_thread_agents_without_mentions() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let agent_id = insert_test_agent(&pool, "thread-agent").await?;
            let channel_id = insert_test_channel(&pool, "thread-followup").await?;
            sqlx::query(
                r#"
                insert into channel_members (channel_id, agent_id)
                values ($1, $2)
                "#,
            )
            .bind(channel_id)
            .bind(agent_id)
            .execute(&pool)
            .await
            .map_err(|err| err.to_string())?;

            let root_id: Uuid = sqlx::query_scalar(
                r#"
                insert into messages (channel_id, sender_name, sender_role, body, is_task)
                values ($1, 'Dylan', 'owner', '@thread-agent please investigate', false)
                returning id
                "#,
            )
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            sqlx::query(
                r#"
                insert into agent_work_items (
                    agent_id, channel_id, thread_root_id, source_message_id, title, context, status
                )
                values ($1, $2, $3, $3, 'Initial dispatch', 'Initial dispatch', 'done')
                "#,
            )
            .bind(agent_id)
            .bind(channel_id)
            .bind(root_id)
            .execute(&pool)
            .await
            .map_err(|err| err.to_string())?;

            send_owner_message_in_pool(
                &pool,
                channel_id,
                Some(root_id),
                "我补充一下：这个复现只在 thread 里出现",
                false,
                vec![],
            )
            .await?;

            let work_items = sqlx::query(
                r#"
                select
                    w.source_message_id,
                    w.source_kind,
                    w.title,
                    w.context,
                    i.kind as inbox_kind,
                    i.body_preview
                from agent_work_items w
                join agent_inbox_items i on i.work_item_id = w.id
                where w.channel_id = $1 and w.agent_id = $2 and w.source_message_id <> $3
                "#,
            )
            .bind(channel_id)
            .bind(agent_id)
            .bind(root_id)
            .fetch_all(&pool)
            .await
            .map_err(|err| err.to_string())?;

            assert_eq!(work_items.len(), 1);
            assert_eq!(work_items[0].get::<String, _>("source_kind"), "inbox_wake");
            assert_eq!(
                work_items[0].get::<String, _>("inbox_kind"),
                "thread_followup"
            );
            assert_eq!(
                work_items[0]
                    .get::<Option<String>, _>("title")
                    .unwrap_or_default(),
                "Process inbox: 我补充一下：这个复现只在 thread 里出现"
            );
            let context: String = work_items[0].get("context");
            assert!(context.contains("Lantor agent inbox wake"));
            assert!(context.contains("inbox-read"));
            assert!(work_items[0]
                .get::<String, _>("body_preview")
                .contains("这个复现"));
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn owner_thread_followup_with_explicit_mention_does_not_fan_out_to_thread_agents() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let mentioned_agent_id = insert_test_agent(&pool, "mentioned-agent").await?;
            let bystander_agent_id = insert_test_agent(&pool, "bystander-agent").await?;
            let channel_id = insert_test_channel(&pool, "thread-explicit-owner").await?;
            for agent_id in [mentioned_agent_id, bystander_agent_id] {
                sqlx::query(
                    r#"
                    insert into channel_members (channel_id, agent_id)
                    values ($1, $2)
                    "#,
                )
                .bind(channel_id)
                .bind(agent_id)
                .execute(&pool)
                .await
                .map_err(|err| err.to_string())?;
            }

            let root_id: Uuid = sqlx::query_scalar(
                r#"
                insert into messages (channel_id, sender_name, sender_role, body, is_task)
                values ($1, 'Dylan', 'owner', 'root request', false)
                returning id
                "#,
            )
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            upsert_agent_thread_subscription(
                &pool,
                mentioned_agent_id,
                channel_id,
                root_id,
                "mention",
                Some(root_id),
            )
            .await?;
            upsert_agent_thread_subscription(
                &pool,
                bystander_agent_id,
                channel_id,
                root_id,
                "mention",
                Some(root_id),
            )
            .await?;

            send_owner_message_in_pool(
                &pool,
                channel_id,
                Some(root_id),
                "@mentioned-agent 这个后续只给被点名的 agent",
                false,
                vec![],
            )
            .await?;

            let mentioned_inboxes: i64 = sqlx::query_scalar(
                r#"
                select count(*)
                from agent_inbox_items
                where agent_id = $1
                  and source_message_id <> $2
                  and kind = 'mention'
                "#,
            )
            .bind(mentioned_agent_id)
            .bind(root_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let bystander_inboxes: i64 = sqlx::query_scalar(
                r#"
                select count(*)
                from agent_inbox_items
                where agent_id = $1
                  and source_message_id <> $2
                "#,
            )
            .bind(bystander_agent_id)
            .bind(root_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;

            assert_eq!(mentioned_inboxes, 1);
            assert_eq!(bystander_inboxes, 0);
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn owner_thread_followup_with_unknown_mention_does_not_fall_back_to_thread_agents() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let agent_id = insert_test_agent(&pool, "known-thread-agent").await?;
            let channel_id = insert_test_channel(&pool, "thread-unknown-mention").await?;
            sqlx::query(
                r#"
                insert into channel_members (channel_id, agent_id)
                values ($1, $2)
                "#,
            )
            .bind(channel_id)
            .bind(agent_id)
            .execute(&pool)
            .await
            .map_err(|err| err.to_string())?;

            let root_id: Uuid = sqlx::query_scalar(
                r#"
                insert into messages (channel_id, sender_name, sender_role, body, is_task)
                values ($1, 'Dylan', 'owner', 'root request', false)
                returning id
                "#,
            )
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            upsert_agent_thread_subscription(
                &pool,
                agent_id,
                channel_id,
                root_id,
                "mention",
                Some(root_id),
            )
            .await?;

            send_owner_message_in_pool(
                &pool,
                channel_id,
                Some(root_id),
                "@missing-agent 这个后续不应该 fallback 给 thread 参与者",
                false,
                vec![],
            )
            .await?;

            let inboxes: i64 = sqlx::query_scalar(
                r#"
                select count(*)
                from agent_inbox_items
                where agent_id = $1
                  and source_message_id <> $2
                "#,
            )
            .bind(agent_id)
            .bind(root_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;

            assert_eq!(inboxes, 0);
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn owner_thread_followup_uses_agent_thread_subscription_after_work_done() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let agent_id = insert_test_agent(&pool, "sub-agent").await?;
            let channel_id = insert_test_channel(&pool, "thread-subscription").await?;
            sqlx::query(
                r#"
                insert into channel_members (channel_id, agent_id)
                values ($1, $2)
                "#,
            )
            .bind(channel_id)
            .bind(agent_id)
            .execute(&pool)
            .await
            .map_err(|err| err.to_string())?;

            let root_id: Uuid = sqlx::query_scalar(
                r#"
                insert into messages (channel_id, sender_name, sender_role, body, is_task)
                values ($1, 'Dylan', 'owner', 'root request', false)
                returning id
                "#,
            )
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            upsert_agent_thread_subscription(
                &pool,
                agent_id,
                channel_id,
                root_id,
                "mention",
                Some(root_id),
            )
            .await?;

            send_owner_message_in_pool(
                &pool,
                channel_id,
                Some(root_id),
                "继续补充，不需要重新 @ agent",
                false,
                vec![],
            )
            .await?;

            let row = sqlx::query(
                r#"
                select w.source_kind, w.thread_root_id, w.task_id, i.kind as inbox_kind
                from agent_work_items w
                join agent_inbox_items i on i.work_item_id = w.id
                where w.agent_id = $1 and w.source_message_id <> $2
                order by w.created_at desc
                limit 1
                "#,
            )
            .bind(agent_id)
            .bind(root_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(row.get::<String, _>("source_kind"), "inbox_wake");
            assert_eq!(row.get::<String, _>("inbox_kind"), "thread_followup");
            assert_eq!(row.get::<Option<Uuid>, _>("thread_root_id"), Some(root_id));
            assert_eq!(row.get::<Option<Uuid>, _>("task_id"), None);
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn mentions_create_agent_requests_but_only_task_mode_creates_global_tasks() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let agent_id = insert_test_agent(&pool, "task-agent").await?;
            let channel_id = insert_test_channel(&pool, "task-semantics").await?;

            send_owner_message_in_pool(
                &pool,
                channel_id,
                None,
                "@task-agent please look at this",
                false,
                vec![],
            )
            .await?;
            let task_count: i64 = sqlx::query_scalar("select count(*) from tasks")
                .fetch_one(&pool)
                .await
                .map_err(|err| err.to_string())?;
            assert_eq!(task_count, 0);
            let mention_inbox_kind: String = sqlx::query_scalar(
                "select kind from agent_inbox_items where agent_id = $1 order by created_at desc limit 1",
            )
            .bind(agent_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(mention_inbox_kind, "mention");

            send_owner_message_in_pool(
                &pool,
                channel_id,
                None,
                "@task-agent implement the tracked feature",
                true,
                vec![],
            )
            .await?;
            let task_count: i64 = sqlx::query_scalar("select count(*) from tasks")
                .fetch_one(&pool)
                .await
                .map_err(|err| err.to_string())?;
            assert_eq!(task_count, 1);
            let task = sqlx::query("select status, assignee_agent_id from tasks limit 1")
                .fetch_one(&pool)
                .await
                .map_err(|err| err.to_string())?;
            assert_eq!(task.get::<String, _>("status"), "in_progress");
            assert_eq!(
                task.get::<Option<Uuid>, _>("assignee_agent_id"),
                Some(agent_id)
            );
            let task_inbox_kind: String = sqlx::query_scalar(
                "select kind from agent_inbox_items where agent_id = $1 order by created_at desc limit 1",
            )
            .bind(agent_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(task_inbox_kind, "task_assigned");
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn owner_task_without_mentions_auto_assigns_single_channel_agent() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let agent_id = insert_test_agent(&pool, "solo-task-agent").await?;
            let channel_id = insert_test_channel(&pool, "solo-task").await?;
            sqlx::query(
                r#"
                insert into channel_members (channel_id, agent_id)
                values ($1, $2)
                "#,
            )
            .bind(channel_id)
            .bind(agent_id)
            .execute(&pool)
            .await
            .map_err(|err| err.to_string())?;

            send_owner_message_in_pool(
                &pool,
                channel_id,
                None,
                "Implement the compact task flow",
                true,
                vec![],
            )
            .await?;

            let task = sqlx::query("select status, assignee_agent_id from tasks limit 1")
                .fetch_one(&pool)
                .await
                .map_err(|err| err.to_string())?;
            assert_eq!(task.get::<String, _>("status"), "in_progress");
            assert_eq!(
                task.get::<Option<Uuid>, _>("assignee_agent_id"),
                Some(agent_id)
            );
            let inbox_kind: String = sqlx::query_scalar(
                "select kind from agent_inbox_items where agent_id = $1 order by created_at desc limit 1",
            )
            .bind(agent_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(inbox_kind, "task_assigned");
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn owner_task_without_mentions_stays_unassigned_with_multiple_channel_agents() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let first_agent_id = insert_test_agent(&pool, "multi-task-a").await?;
            let second_agent_id = insert_test_agent(&pool, "multi-task-b").await?;
            let channel_id = insert_test_channel(&pool, "multi-task").await?;
            for agent_id in [first_agent_id, second_agent_id] {
                sqlx::query(
                    r#"
                    insert into channel_members (channel_id, agent_id)
                    values ($1, $2)
                    "#,
                )
                .bind(channel_id)
                .bind(agent_id)
                .execute(&pool)
                .await
                .map_err(|err| err.to_string())?;
            }

            send_owner_message_in_pool(
                &pool,
                channel_id,
                None,
                "Implement the unassigned queue",
                true,
                vec![],
            )
            .await?;

            let task = sqlx::query("select status, assignee_agent_id from tasks limit 1")
                .fetch_one(&pool)
                .await
                .map_err(|err| err.to_string())?;
            assert_eq!(task.get::<String, _>("status"), "todo");
            assert_eq!(task.get::<Option<Uuid>, _>("assignee_agent_id"), None);
            let inbox_count: i64 = sqlx::query_scalar(
                r#"
                select count(*)
                from agent_inbox_items
                where task_id = (select id from tasks limit 1)
                  and kind = 'task_available'
                "#,
            )
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(inbox_count, 2);
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn unassigned_task_claim_is_atomic_across_agents() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let first_agent_id = insert_test_agent(&pool, "claim-race-a").await?;
            let second_agent_id = insert_test_agent(&pool, "claim-race-b").await?;
            let channel_id = insert_test_channel(&pool, "claim-race").await?;
            for agent_id in [first_agent_id, second_agent_id] {
                sqlx::query(
                    r#"
                    insert into channel_members (channel_id, agent_id)
                    values ($1, $2)
                    "#,
                )
                .bind(channel_id)
                .bind(agent_id)
                .execute(&pool)
                .await
                .map_err(|err| err.to_string())?;
            }
            let message_id: Uuid = sqlx::query_scalar(
                r#"
                insert into messages (channel_id, sender_name, sender_role, body, is_task)
                values ($1, 'Dylan', 'owner', 'Race to claim', true)
                returning id
                "#,
            )
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let task_id: Uuid = sqlx::query_scalar(
                r#"
                insert into tasks (message_id, channel_id, title, status)
                values ($1, $2, 'Race to claim', 'todo')
                returning id
                "#,
            )
            .bind(message_id)
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;

            let first = try_claim_unassigned_task(&pool, task_id, first_agent_id, Some(0), "test");
            let second =
                try_claim_unassigned_task(&pool, task_id, second_agent_id, Some(0), "test");
            let (first, second) = tokio::join!(first, second);
            let wins = [first?, second?]
                .into_iter()
                .filter(|claim| claim.is_some())
                .count();
            assert_eq!(wins, 1);

            let task = sqlx::query("select status, assignee_agent_id, version from tasks where id = $1")
                .bind(task_id)
                .fetch_one(&pool)
                .await
                .map_err(|err| err.to_string())?;
            assert_eq!(task.get::<String, _>("status"), "in_progress");
            assert!(task.get::<Option<Uuid>, _>("assignee_agent_id").is_some());
            assert_eq!(task.get::<i64, _>("version"), 1);

            let assigned_inboxes: i64 = sqlx::query_scalar(
                "select count(*) from agent_inbox_items where task_id = $1 and kind = 'task_assigned'",
            )
            .bind(task_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(assigned_inboxes, 1);
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn stale_task_claim_event_is_ignored_without_dispatch_noise() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let winner_agent_id = insert_test_agent(&pool, "claim-winner").await?;
            let stale_agent_id = insert_test_agent(&pool, "claim-stale").await?;
            let channel_id = insert_test_channel(&pool, "claim-stale").await?;
            for agent_id in [winner_agent_id, stale_agent_id] {
                sqlx::query(
                    r#"
                    insert into channel_members (channel_id, agent_id)
                    values ($1, $2)
                    "#,
                )
                .bind(channel_id)
                .bind(agent_id)
                .execute(&pool)
                .await
                .map_err(|err| err.to_string())?;
            }
            let message_id: Uuid = sqlx::query_scalar(
                r#"
                insert into messages (channel_id, sender_name, sender_role, body, is_task)
                values ($1, 'Dylan', 'owner', 'Claim once', true)
                returning id
                "#,
            )
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let task_row = sqlx::query(
                r#"
                insert into tasks (message_id, channel_id, title, status)
                values ($1, $2, 'Claim once', 'todo')
                returning id, number
                "#,
            )
            .bind(message_id)
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let task_id: Uuid = task_row.get("id");
            let task_number: i64 = task_row.get("number");
            assert!(
                try_claim_unassigned_task(&pool, task_id, winner_agent_id, None, "test")
                    .await?
                    .is_some()
            );

            let run_id: Uuid = sqlx::query_scalar(
                r#"
                insert into agent_runs (agent_id, command, status)
                values ($1, 'codex app-server', 'running')
                returning id
                "#,
            )
            .bind(stale_agent_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let result = handle_agent_event(
                &pool,
                stale_agent_id,
                run_id,
                AgentEvent::TaskClaim {
                    task_number,
                    assignee_handle: None,
                },
            )
            .await?;
            assert_eq!(result, format!("task #{task_number} claim ignored"));

            let task = sqlx::query("select assignee_agent_id from tasks where id = $1")
                .bind(task_id)
                .fetch_one(&pool)
                .await
                .map_err(|err| err.to_string())?;
            assert_eq!(
                task.get::<Option<Uuid>, _>("assignee_agent_id"),
                Some(winner_agent_id)
            );
            let stale_inboxes: i64 = sqlx::query_scalar(
                "select count(*) from agent_inbox_items where task_id = $1 and agent_id = $2 and kind = 'task_assigned'",
            )
            .bind(task_id)
            .bind(stale_agent_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(stale_inboxes, 0);
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn task_status_and_claim_events_do_not_insert_system_messages() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let agent_id = insert_test_agent(&pool, "task-noise-agent").await?;
            let channel_id = insert_test_channel(&pool, "task-noise").await?;
            sqlx::query(
                r#"
                insert into channel_members (channel_id, agent_id)
                values ($1, $2)
                "#,
            )
            .bind(channel_id)
            .bind(agent_id)
            .execute(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let message_id: Uuid = sqlx::query_scalar(
                r#"
                insert into messages (channel_id, sender_name, sender_role, body, is_task)
                values ($1, 'Dylan', 'owner', 'Track this task', true)
                returning id
                "#,
            )
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let task_number: i64 = sqlx::query_scalar(
                r#"
                insert into tasks (message_id, channel_id, title, status)
                values ($1, $2, 'Track this task', 'todo')
                returning number
                "#,
            )
            .bind(message_id)
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let run_id: Uuid = sqlx::query_scalar(
                r#"
                insert into agent_runs (agent_id, command, status)
                values ($1, 'codex app-server', 'running')
                returning id
                "#,
            )
            .bind(agent_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;

            handle_agent_event(
                &pool,
                agent_id,
                run_id,
                AgentEvent::TaskClaim {
                    task_number,
                    assignee_handle: None,
                },
            )
            .await?;
            handle_agent_event(
                &pool,
                agent_id,
                run_id,
                AgentEvent::TaskStatus {
                    task_number,
                    status: "done".to_owned(),
                },
            )
            .await?;

            let task = sqlx::query("select status, assignee_agent_id from tasks where number = $1")
                .bind(task_number)
                .fetch_one(&pool)
                .await
                .map_err(|err| err.to_string())?;
            assert_eq!(task.get::<String, _>("status"), "done");
            assert_eq!(
                task.get::<Option<Uuid>, _>("assignee_agent_id"),
                Some(agent_id)
            );

            let system_messages: i64 = sqlx::query_scalar(
                r#"
                select count(*)
                from messages
                where channel_id = $1
                  and sender_role = 'system'
                  and (body like '%claimed task%' or body like 'Task #%moved%')
                "#,
            )
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(system_messages, 0);
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn task_work_item_queue_and_start_do_not_insert_system_messages() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let agent_id = insert_test_agent(&pool, "task-lifecycle-agent").await?;
            let channel_id = insert_test_channel(&pool, "task-lifecycle").await?;
            let message_id: Uuid = sqlx::query_scalar(
                r#"
                insert into messages (channel_id, sender_name, sender_role, body, is_task)
                values ($1, 'Dylan', 'owner', 'Handle this task', true)
                returning id
                "#,
            )
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let task_id: Uuid = sqlx::query_scalar(
                r#"
                insert into tasks (message_id, channel_id, title, status, assignee_agent_id)
                values ($1, $2, 'Handle this task', 'in_progress', $3)
                returning id
                "#,
            )
            .bind(message_id)
            .bind(channel_id)
            .bind(agent_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let work_item_id: Uuid = sqlx::query_scalar(
                r#"
                insert into agent_work_items (
                    agent_id, channel_id, thread_root_id, source_message_id,
                    task_id, source_kind, title, context, status
                )
                values ($1, $2, $3, $3, $4, 'task_assigned', 'Handle this task', 'context', 'queued')
                returning id
                "#,
            )
            .bind(agent_id)
            .bind(channel_id)
            .bind(message_id)
            .bind(task_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;

            notify_ui_work_item_changed(&pool, work_item_id, "work_item_created").await;
            notify_ui_work_item_changed(&pool, work_item_id, "work_item_queued").await;
            sqlx::query("update agent_work_items set status = 'running' where id = $1")
                .bind(work_item_id)
                .execute(&pool)
                .await
                .map_err(|err| err.to_string())?;
            notify_ui_work_item_changed(&pool, work_item_id, "work_item_running").await;

            let system_messages: i64 = sqlx::query_scalar(
                r#"
                select count(*)
                from messages
                where channel_id = $1
                  and thread_root_id = $2
                  and sender_role = 'system'
                  and (body like '%queued task run%' or body like '%started task run%')
                "#,
            )
            .bind(channel_id)
            .bind(message_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(system_messages, 0);
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn task_claim_opportunity_finish_does_not_insert_system_message() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let loser_agent_id = insert_test_agent(&pool, "claim-loser").await?;
            let winner_agent_id = insert_test_agent(&pool, "claim-winner-finish").await?;
            let channel_id = insert_test_channel(&pool, "claim-finish").await?;
            let available_message_id: Uuid = sqlx::query_scalar(
                r#"
                insert into messages (channel_id, sender_name, sender_role, body, is_task)
                values ($1, 'Dylan', 'owner', 'Race this task', true)
                returning id
                "#,
            )
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let available_task_id: Uuid = sqlx::query_scalar(
                r#"
                insert into tasks (message_id, channel_id, title, status)
                values ($1, $2, 'Race this task', 'todo')
                returning id
                "#,
            )
            .bind(available_message_id)
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let available_inbox_id = create_agent_inbox_item(
                &pool,
                AgentInboxItemInput {
                    agent_id: loser_agent_id,
                    channel_id: Some(channel_id),
                    thread_root_id: Some(available_message_id),
                    source_message_id: Some(available_message_id),
                    task_id: Some(available_task_id),
                    kind: "task_available",
                    priority: 70,
                    title: "Race this task",
                    body_preview: "Race this task",
                    payload: json!({}),
                },
            )
            .await?;
            let available_work_item_id: Uuid = sqlx::query_scalar(
                r#"
                insert into agent_work_items (
                    agent_id, channel_id, thread_root_id, source_message_id, inbox_item_id,
                    task_id, source_kind, title, context, status, completed_at
                )
                values ($1, $2, $3, $3, $4, $5, 'inbox_wake', 'Race this task', 'context', 'done', strftime('%Y-%m-%dT%H:%M:%f+00:00','now'))
                returning id
                "#,
            )
            .bind(loser_agent_id)
            .bind(channel_id)
            .bind(available_message_id)
            .bind(available_inbox_id)
            .bind(available_task_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            sqlx::query("update agent_inbox_items set work_item_id = $2 where id = $1")
                .bind(available_inbox_id)
                .bind(available_work_item_id)
                .execute(&pool)
                .await
                .map_err(|err| err.to_string())?;
            notify_ui_work_item_changed(&pool, available_work_item_id, "work_item_finished").await;

            let assigned_message_id: Uuid = sqlx::query_scalar(
                r#"
                insert into messages (channel_id, sender_name, sender_role, body, is_task)
                values ($1, 'Dylan', 'owner', 'Run this task', true)
                returning id
                "#,
            )
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let assigned_task_id: Uuid = sqlx::query_scalar(
                r#"
                insert into tasks (message_id, channel_id, title, status, assignee_agent_id)
                values ($1, $2, 'Run this task', 'in_progress', $3)
                returning id
                "#,
            )
            .bind(assigned_message_id)
            .bind(channel_id)
            .bind(winner_agent_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let assigned_inbox_id = create_agent_inbox_item(
                &pool,
                AgentInboxItemInput {
                    agent_id: winner_agent_id,
                    channel_id: Some(channel_id),
                    thread_root_id: Some(assigned_message_id),
                    source_message_id: Some(assigned_message_id),
                    task_id: Some(assigned_task_id),
                    kind: "task_assigned",
                    priority: 95,
                    title: "Run this task",
                    body_preview: "Run this task",
                    payload: json!({}),
                },
            )
            .await?;
            let assigned_work_item_id: Uuid = sqlx::query_scalar(
                r#"
                insert into agent_work_items (
                    agent_id, channel_id, thread_root_id, source_message_id, inbox_item_id,
                    task_id, source_kind, title, context, status, completed_at
                )
                values ($1, $2, $3, $3, $4, $5, 'inbox_wake', 'Run this task', 'context', 'done', strftime('%Y-%m-%dT%H:%M:%f+00:00','now'))
                returning id
                "#,
            )
            .bind(winner_agent_id)
            .bind(channel_id)
            .bind(assigned_message_id)
            .bind(assigned_inbox_id)
            .bind(assigned_task_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            sqlx::query("update agent_inbox_items set work_item_id = $2 where id = $1")
                .bind(assigned_inbox_id)
                .bind(assigned_work_item_id)
                .execute(&pool)
                .await
                .map_err(|err| err.to_string())?;
            notify_ui_work_item_changed(&pool, assigned_work_item_id, "work_item_finished").await;

            let claim_opportunity_messages: i64 = sqlx::query_scalar(
                r#"
                select count(*)
                from messages
                where channel_id = $1
                  and thread_root_id = $2
                  and sender_role = 'system'
                  and body like '@claim-loser completed task run%'
                "#,
            )
            .bind(channel_id)
            .bind(available_message_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(claim_opportunity_messages, 0);

            let assigned_messages: i64 = sqlx::query_scalar(
                r#"
                select count(*)
                from messages
                where channel_id = $1
                  and thread_root_id = $2
                  and sender_role = 'system'
                  and body like '@claim-winner-finish completed task run%'
                "#,
            )
            .bind(channel_id)
            .bind(assigned_message_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(assigned_messages, 0);

            sqlx::query("update tasks set status = 'in_review' where id = $1")
                .bind(assigned_task_id)
                .execute(&pool)
                .await
                .map_err(|err| err.to_string())?;
            notify_ui_work_item_changed(&pool, assigned_work_item_id, "work_item_finished").await;

            let assigned_messages: i64 = sqlx::query_scalar(
                r#"
                select count(*)
                from messages
                where channel_id = $1
                  and thread_root_id = $2
                  and sender_role = 'system'
                  and body like '@claim-winner-finish completed task run%'
                "#,
            )
            .bind(channel_id)
            .bind(assigned_message_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(assigned_messages, 1);
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn mark_all_inbox_read_uses_current_cutoff_without_dismissing_tasks() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let channel_id = insert_test_channel(&pool, "mark-all-read").await?;
            let message_id: Uuid = sqlx::query_scalar(
                r#"
                insert into messages (channel_id, sender_name, sender_role, body, is_task)
                values ($1, 'Dylan', 'owner', 'Review this task', true)
                returning id
                "#,
            )
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let task_id: Uuid = sqlx::query_scalar(
                r#"
                insert into tasks (message_id, channel_id, title, status, updated_at)
                values ($1, $2, 'Review this task', 'in_review', strftime('%Y-%m-%dT%H:%M:%f+00:00','now','-1 hour'))
                returning id
                "#,
            )
            .bind(message_id)
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let task_updated_at: DateTime<Utc> =
                sqlx::query_scalar("select updated_at from tasks where id = $1")
                    .bind(task_id)
                    .fetch_one(&pool)
                    .await
                    .map_err(|err| err.to_string())?;
            let before_mark_all: DateTime<Utc> = sqlx::query_scalar("select strftime('%Y-%m-%dT%H:%M:%f+00:00','now')")
                .fetch_one(&pool)
                .await
                .map_err(|err| err.to_string())?;

            mark_all_owner_inbox_read_in_pool(&pool).await?;

            let read_until: DateTime<Utc> = sqlx::query_scalar(
                "select read_until from owner_inbox_read_state where item_id = $1",
            )
            .bind(format!("task:{task_id}"))
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let hidden: bool = sqlx::query_scalar(
                "select exists(select 1 from owner_inbox_hidden_items where item_id = $1)",
            )
            .bind(format!("task:{task_id}"))
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;

            assert!(read_until > task_updated_at);
            assert!(read_until >= before_mark_all);
            assert!(!hidden);
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn dismiss_inbox_item_hides_without_marking_read() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let item_id = "thread:root:latest".to_owned();
            let hidden_until: DateTime<Utc> =
                sqlx::query_scalar("select strftime('%Y-%m-%dT%H:%M:%f+00:00','now','+5 seconds')")
                    .fetch_one(&pool)
                    .await
                    .map_err(|err| err.to_string())?;

            dismiss_inbox_items_in_pool(&pool, [(item_id.clone(), hidden_until)]).await?;

            let stored_hidden_until: DateTime<Utc> = sqlx::query_scalar(
                "select hidden_until from owner_inbox_hidden_items where item_id = $1",
            )
            .bind(&item_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let read_exists: bool = sqlx::query_scalar(
                "select exists(select 1 from owner_inbox_read_state where item_id = $1)",
            )
            .bind(&item_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;

            assert_eq!(stored_hidden_until, hidden_until);
            assert!(!read_exists);
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn inbox_read_and_dismiss_emit_ui_refresh() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let mut last_event_id = latest_ui_event_id(&pool).await?;
            let cutoff: DateTime<Utc> =
                sqlx::query_scalar("select strftime('%Y-%m-%dT%H:%M:%f+00:00','now')")
                    .fetch_one(&pool)
                    .await
                    .map_err(|err| err.to_string())?;

            dismiss_inbox_items_in_pool(&pool, [("thread:root".to_owned(), cutoff)]).await?;
            wait_for_ui_refresh_reason(&pool, &mut last_event_id, "owner_inbox_dismissed").await?;

            mark_inbox_items_read_in_pool(&pool, [("thread:root".to_owned(), cutoff)]).await?;
            wait_for_ui_refresh_reason(&pool, &mut last_event_id, "owner_inbox_read").await?;
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn conversational_work_item_finish_does_not_insert_system_message() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let agent_id = insert_test_agent(&pool, "conversation-agent").await?;
            let channel_id = insert_test_channel(&pool, "conversation-finish").await?;
            let source_message_id: Uuid = sqlx::query_scalar(
                r#"
                insert into messages (channel_id, sender_name, sender_role, body, is_task)
                values ($1, 'Dylan', 'owner', 'Please answer in thread', false)
                returning id
                "#,
            )
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let work_item_id: Uuid = sqlx::query_scalar(
                r#"
                insert into agent_work_items (
                    agent_id, channel_id, thread_root_id, source_message_id,
                    source_kind, title, context, status, completed_at
                )
                values ($1, $2, $3, $3, 'mention', 'Please answer in thread', 'context', 'done', strftime('%Y-%m-%dT%H:%M:%f+00:00','now'))
                returning id
                "#,
            )
            .bind(agent_id)
            .bind(channel_id)
            .bind(source_message_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;

            notify_ui_work_item_changed(&pool, work_item_id, "work_item_finished").await;

            let system_messages: i64 = sqlx::query_scalar(
                r#"
                select count(*)
                from messages
                where channel_id = $1
                  and thread_root_id = $2
                  and sender_role = 'system'
                  and body like '@conversation-agent completed agent request%'
                "#,
            )
            .bind(channel_id)
            .bind(source_message_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(system_messages, 0);
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn task_create_event_creates_root_task_and_execution_thread() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let agent_id = insert_test_agent(&pool, "task-thread-agent").await?;
            let channel_id = insert_test_channel(&pool, "task-thread-api").await?;
            let run_id: Uuid = sqlx::query_scalar(
                r#"
                insert into agent_runs (agent_id, command, working_directory, status)
                values ($1, 'test', '', 'running')
                returning id
                "#,
            )
            .bind(agent_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;

            handle_agent_event(
                &pool,
                agent_id,
                run_id,
                AgentEvent::TaskCreate {
                    channel: None,
                    channel_id: Some(channel_id),
                    title: "Investigate task thread API".to_owned(),
                    body: Some("Track this as a durable task".to_owned()),
                    thread_body: Some("Starting execution in the task thread".to_owned()),
                    assign_self: Some(true),
                    status: Some("in_progress".to_owned()),
                },
            )
            .await?;

            let task_row = sqlx::query(
                r#"
                select t.number, t.status, t.assignee_agent_id, m.id as message_id, m.body, m.thread_root_id
                from tasks t
                join messages m on m.id = t.message_id
                where t.channel_id = $1
                "#,
            )
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let root_message_id: Uuid = task_row.get("message_id");
            assert_eq!(task_row.get::<String, _>("status"), "in_progress");
            assert_eq!(
                task_row.get::<Option<Uuid>, _>("assignee_agent_id"),
                Some(agent_id)
            );
            assert_eq!(task_row.get::<String, _>("body"), "Track this as a durable task");
            assert_eq!(task_row.get::<Option<Uuid>, _>("thread_root_id"), None);

            let reply_body: String = sqlx::query_scalar(
                "select body from messages where channel_id = $1 and thread_root_id = $2",
            )
            .bind(channel_id)
            .bind(root_message_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(reply_body, "Starting execution in the task thread");

            let subscribed: bool = sqlx::query_scalar(
                r#"
                select exists (
                    select 1
                    from agent_thread_subscriptions
                    where agent_id = $1 and channel_id = $2 and thread_root_id = $3
                )
                "#,
            )
            .bind(agent_id)
            .bind(channel_id)
            .bind(root_message_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert!(subscribed);
            assert!(task_row.get::<i64, _>("number") > 0);
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn activity_event_records_progress_without_chat_message() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let agent_id = insert_test_agent(&pool, "activity-agent").await?;
            let run_id: Uuid = sqlx::query_scalar(
                r#"
                insert into agent_runs (agent_id, command, working_directory, status)
                values ($1, 'test', '', 'running')
                returning id
                "#,
            )
            .bind(agent_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;

            handle_agent_event(
                &pool,
                agent_id,
                run_id,
                AgentEvent::Activity {
                    kind: Some("running_command".to_owned()),
                    title: "Running tests".to_owned(),
                    detail: Some("cargo test".to_owned()),
                },
            )
            .await?;

            let row = sqlx::query(
                r#"
                select kind, phase, status, title, detail
                from agent_activities
                where agent_id = $1 and run_id = $2
                "#,
            )
            .bind(agent_id)
            .bind(run_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(row.get::<String, _>("kind"), "command");
            assert_eq!(row.get::<String, _>("phase"), "command");
            assert_eq!(row.get::<String, _>("status"), "active");
            assert_eq!(row.get::<String, _>("title"), "Running tests");
            assert_eq!(row.get::<String, _>("detail"), "cargo test");
            let messages: i64 = sqlx::query_scalar("select count(*) from messages")
                .fetch_one(&pool)
                .await
                .map_err(|err| err.to_string())?;
            assert_eq!(messages, 0);
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn agent_mentions_dispatch_other_agents_once_in_same_thread() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let author_id = insert_test_agent(&pool, "author").await?;
            let reviewer_id = insert_test_agent(&pool, "reviewer").await?;
            let outsider_id = insert_test_agent(&pool, "outsider").await?;
            let channel_id = insert_test_channel(&pool, "collab").await?;
            sqlx::query(
                r#"
                insert into channel_members (channel_id, agent_id)
                values ($1, $2), ($1, $3)
                "#,
            )
            .bind(channel_id)
            .bind(author_id)
            .bind(reviewer_id)
            .execute(&pool)
            .await
            .map_err(|err| err.to_string())?;

            let message_id = insert_agent_message(
                &pool,
                author_id,
                channel_id,
                None,
                "@author ignore self, @reviewer please review this, @outsider ignore non-member",
                false,
            )
            .await?;
            queue_mentions_as_work_items(
                &pool,
                channel_id,
                None,
                message_id,
                None,
                "@reviewer please review this again",
                MentionDispatchOrigin::Agent {
                    sender_agent_id: author_id,
                    allow_channel_member_invite: false,
                },
            )
            .await?;

            let work_items = sqlx::query(
                r#"
                select agent_id, thread_root_id, source_message_id
                from agent_work_items
                where source_message_id = $1
                "#,
            )
            .bind(message_id)
            .fetch_all(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(work_items.len(), 1);
            assert_eq!(work_items[0].get::<Uuid, _>("agent_id"), reviewer_id);
            assert_ne!(work_items[0].get::<Uuid, _>("agent_id"), outsider_id);
            assert_eq!(
                work_items[0].get::<Option<Uuid>, _>("thread_root_id"),
                Some(message_id)
            );
            assert_eq!(
                work_items[0].get::<Option<Uuid>, _>("source_message_id"),
                Some(message_id)
            );
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn agent_mentions_do_not_cross_dispatch_from_dm() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let dm_agent_id = insert_test_agent(&pool, "dm-agent").await?;
            let reviewer_id = insert_test_agent(&pool, "dm-reviewer").await?;
            let dm_channel_id =
                Uuid::parse_str(&open_dm_with_agent_in_pool(&pool, dm_agent_id).await?)
                    .map_err(|err| err.to_string())?;

            insert_agent_message(
                &pool,
                dm_agent_id,
                dm_channel_id,
                None,
                "@dm-reviewer please join this DM",
                false,
            )
            .await?;
            let work_items: i64 = sqlx::query_scalar(
                r#"
                select count(*)
                from agent_work_items
                where channel_id = $1 and agent_id = $2
                "#,
            )
            .bind(dm_channel_id)
            .bind(reviewer_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(work_items, 0);
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn channel_agent_roster_excludes_current_agent() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let current_id = insert_test_agent(&pool, "current").await?;
            let peer_id = insert_test_agent(&pool, "peer").await?;
            let channel_id = insert_test_channel(&pool, "roster").await?;
            sqlx::query(
                r#"
                insert into channel_members (channel_id, agent_id)
                values ($1, $2), ($1, $3)
                "#,
            )
            .bind(channel_id)
            .bind(current_id)
            .bind(peer_id)
            .execute(&pool)
            .await
            .map_err(|err| err.to_string())?;

            let roster = load_channel_agent_roster(&pool, Some(channel_id), current_id).await?;
            assert_eq!(roster.len(), 1);
            assert!(roster[0].contains("@peer"));
            assert!(!roster[0].contains("@current"));
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn agent_collaboration_pauses_after_thread_limit() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let author_id = insert_test_agent(&pool, "loop-author").await?;
            let reviewer_id = insert_test_agent(&pool, "loop-reviewer").await?;
            let channel_id = insert_test_channel(&pool, "loop-guard").await?;
            sqlx::query(
                r#"
                insert into channel_members (channel_id, agent_id)
                values ($1, $2), ($1, $3)
                "#,
            )
            .bind(channel_id)
            .bind(author_id)
            .bind(reviewer_id)
            .execute(&pool)
            .await
            .map_err(|err| err.to_string())?;

            let root_id: Uuid = sqlx::query_scalar(
                r#"
                insert into messages (channel_id, sender_name, sender_role, body, is_task)
                values ($1, 'Dylan', 'owner', 'start collaboration', false)
                returning id
                "#,
            )
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            for index in 0..10 {
                insert_agent_message(
                    &pool,
                    author_id,
                    channel_id,
                    Some(root_id),
                    &format!("agent loop message {index}"),
                    false,
                )
                .await?;
            }
            insert_agent_message(
                &pool,
                author_id,
                channel_id,
                Some(root_id),
                "@loop-reviewer continue the loop",
                false,
            )
            .await?;

            let work_items: i64 = sqlx::query_scalar(
                r#"
                select count(*)
                from agent_work_items
                where channel_id = $1 and agent_id = $2
                "#,
            )
            .bind(channel_id)
            .bind(reviewer_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(work_items, 0);
            let system_messages: i64 = sqlx::query_scalar(
                r#"
                select count(*)
                from messages
                where channel_id = $1
                  and thread_root_id = $2
                  and sender_role = 'system'
                  and body like 'Inter-agent collaboration paused:%'
                "#,
            )
            .bind(channel_id)
            .bind(root_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(system_messages, 1);
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn due_reminder_fires_system_message() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let channel_id = insert_test_channel(&pool, "reminders").await?;
            let reminder_id: Uuid = sqlx::query_scalar(
                r#"
                insert into reminders (channel_id, title, note, due_at, recurrence, status)
                values ($1, 'Check thread', 'Follow up with Dylan', strftime('%Y-%m-%dT%H:%M:%f+00:00','now','-1 minute'), 'none', 'scheduled')
                returning id
                "#,
            )
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;

            process_due_reminders(&pool).await?;

            let status: String = sqlx::query_scalar("select status from reminders where id = $1")
                .bind(reminder_id)
                .fetch_one(&pool)
                .await
                .map_err(|err| err.to_string())?;
            assert_eq!(status, "fired");
            let system_messages: i64 = sqlx::query_scalar(
                r#"
                select count(*)
                from messages
                where channel_id = $1
                  and sender_role = 'system'
                  and body like 'Reminder:%'
                "#,
            )
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(system_messages, 1);
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn mark_all_owner_inbox_read_writes_db_snapshot() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let channel_id = insert_test_channel(&pool, "mark-all-inbox").await?;
            let root_id: Uuid = sqlx::query_scalar(
                r#"
                insert into messages (channel_id, sender_name, sender_role, body, is_task)
                values ($1, 'Dylan', 'owner', 'thread root', false)
                returning id
                "#,
            )
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let reply_id: Uuid = sqlx::query_scalar(
                r#"
                insert into messages (channel_id, thread_root_id, sender_name, sender_role, body, is_task)
                values ($1, $2, 'Agent', 'agent', '@Dylan latest reply', false)
                returning id
                "#,
            )
            .bind(channel_id)
            .bind(root_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let task_message_id: Uuid = sqlx::query_scalar(
                r#"
                insert into messages (channel_id, sender_name, sender_role, body, is_task)
                values ($1, 'Dylan', 'owner', 'task root', true)
                returning id
                "#,
            )
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let task_id: Uuid = sqlx::query_scalar(
                r#"
                insert into tasks (message_id, channel_id, title, status)
                values ($1, $2, 'Review this', 'in_review')
                returning id
                "#,
            )
            .bind(task_message_id)
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let active_task_message_id: Uuid = sqlx::query_scalar(
                r#"
                insert into messages (channel_id, sender_name, sender_role, body, is_task)
                values ($1, 'Dylan', 'owner', 'active task root', true)
                returning id
                "#,
            )
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let active_task_id: Uuid = sqlx::query_scalar(
                r#"
                insert into tasks (message_id, channel_id, title, status)
                values ($1, $2, 'In progress task', 'in_progress')
                returning id
                "#,
            )
            .bind(active_task_message_id)
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let reminder_id: Uuid = sqlx::query_scalar(
                r#"
                insert into reminders (channel_id, title, note, due_at, fired_at, recurrence, status)
                values ($1, 'Due reminder', '', strftime('%Y-%m-%dT%H:%M:%f+00:00','now','-1 minute'), strftime('%Y-%m-%dT%H:%M:%f+00:00','now'), 'none', 'fired')
                returning id
                "#,
            )
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let read_channel_id = insert_test_channel(&pool, "mark-all-read-thread").await?;
            let read_root_id: Uuid = sqlx::query_scalar(
                r#"
                insert into messages (channel_id, sender_name, sender_role, body, is_task)
                values ($1, 'Dylan', 'owner', 'already read thread root', false)
                returning id
                "#,
            )
            .bind(read_channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let read_reply_id: Uuid = sqlx::query_scalar(
                r#"
                insert into messages (channel_id, thread_root_id, sender_name, sender_role, body, is_task)
                values ($1, $2, 'Agent', 'agent', '@Dylan already read reply', false)
                returning id
                "#,
            )
            .bind(read_channel_id)
            .bind(read_root_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            sqlx::query(
                "insert into channel_read_state (channel_id, last_read_at) values ($1, strftime('%Y-%m-%dT%H:%M:%f+00:00','now'))",
            )
            .bind(read_channel_id)
            .execute(&pool)
            .await
            .map_err(|err| err.to_string())?;

            mark_all_owner_inbox_read_in_pool(&pool).await?;

            let read_state_count: i64 = sqlx::query_scalar(
                "select count(*) from channel_read_state where channel_id = $1",
            )
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(read_state_count, 1);

            for item_id in [format!("task:{task_id}"), format!("reminder:{reminder_id}")] {
                let exists: bool = sqlx::query_scalar(
                    "select exists(select 1 from owner_inbox_read_state where item_id = $1)",
                )
                .bind(item_id)
                .fetch_one(&pool)
                .await
                .map_err(|err| err.to_string())?;
                assert!(exists);
            }
            for item_id in [
                format!("thread:{root_id}:{reply_id}"),
                format!("mention:{reply_id}"),
                format!("task:{active_task_id}"),
                format!("reminder:{reminder_id}"),
                format!("thread:{read_root_id}:{read_reply_id}"),
                format!("mention:{read_reply_id}"),
            ] {
                let exists: bool = sqlx::query_scalar(
                    "select exists(select 1 from owner_inbox_hidden_items where item_id = $1)",
                )
                .bind(item_id)
                .fetch_one(&pool)
                .await
                .map_err(|err| err.to_string())?;
                assert!(!exists);
            }

            let reminder_status: String =
                sqlx::query_scalar("select status from reminders where id = $1")
                    .bind(reminder_id)
                    .fetch_one(&pool)
                    .await
                    .map_err(|err| err.to_string())?;
            assert_eq!(reminder_status, "fired");
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn due_agent_schedule_dispatches_work_item() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let agent_id = insert_test_agent(&pool, "scheduler").await?;
            let channel_id = insert_test_channel(&pool, "scheduled").await?;
            let schedule_id: Uuid = sqlx::query_scalar(
                r#"
                insert into agent_schedules (
                    agent_id, channel_id, title, prompt, cadence, next_run_at, status
                )
                values ($1, $2, 'Daily check', 'Summarize open work', 'daily', strftime('%Y-%m-%dT%H:%M:%f+00:00','now','-1 minute'), 'active')
                returning id
                "#,
            )
            .bind(agent_id)
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;

            process_due_agent_schedules(&pool).await?;

            let schedule = sqlx::query(
                "select last_run_at, last_work_item_id, next_run_at > strftime('%Y-%m-%dT%H:%M:%f+00:00','now') as future from agent_schedules where id = $1",
            )
            .bind(schedule_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let last_run_at: Option<DateTime<Utc>> = schedule.get("last_run_at");
            let last_work_item_id: Option<Uuid> = schedule.get("last_work_item_id");
            let future: bool = schedule.get("future");
            assert!(last_run_at.is_some());
            assert!(last_work_item_id.is_some());
            assert!(future);

            let work_items: i64 = sqlx::query_scalar(
                r#"
                select count(*)
                from agent_work_items w
                join agent_inbox_items i on i.work_item_id = w.id
                where w.agent_id = $1
                  and w.channel_id = $2
                  and w.title = 'Process inbox: Daily check'
                  and w.context like '%Lantor agent inbox wake%'
                  and i.kind = 'schedule_due'
                "#,
            )
            .bind(agent_id)
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(work_items, 1);

            let system_messages: i64 = sqlx::query_scalar(
                r#"
                select count(*)
                from messages
                where channel_id = $1
                  and sender_role = 'system'
                  and body like 'Scheduled routine for @scheduler:%'
                "#,
            )
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            assert_eq!(system_messages, 1);
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[tokio::test]
    async fn startup_recovery_requeues_running_supervisor_commands_and_skips_terminal_work() {
        let Some((pool, schema)) = test_pool().await else {
            return;
        };
        let result: Result<(), String> = async {
            let agent_id = insert_test_agent(&pool, "recovery-agent").await?;
            let channel_id = insert_test_channel(&pool, "recovery").await?;
            let queued_work_item_id: Uuid = sqlx::query_scalar(
                r#"
                insert into agent_work_items (agent_id, channel_id, title, context, status)
                values ($1, $2, 'queued request', 'context', 'queued')
                returning id
                "#,
            )
            .bind(agent_id)
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let cancelled_work_item_id: Uuid = sqlx::query_scalar(
                r#"
                insert into agent_work_items (agent_id, channel_id, title, context, status)
                values ($1, $2, 'cancelled request', 'context', 'cancelled')
                returning id
                "#,
            )
            .bind(agent_id)
            .bind(channel_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;

            let running_command_id: Uuid = sqlx::query_scalar(
                r#"
                insert into supervisor_commands (command_type, agent_id, work_item_id, status)
                values ('start_agent', $1, $2, 'running')
                returning id
                "#,
            )
            .bind(agent_id)
            .bind(queued_work_item_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;
            let terminal_command_id: Uuid = sqlx::query_scalar(
                r#"
                insert into supervisor_commands (command_type, agent_id, work_item_id, status)
                values ('start_agent', $1, $2, 'pending')
                returning id
                "#,
            )
            .bind(agent_id)
            .bind(cancelled_work_item_id)
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;

            recover_supervisor_commands_at_startup(&pool).await?;

            let running_status: String =
                sqlx::query_scalar("select status from supervisor_commands where id = $1")
                    .bind(running_command_id)
                    .fetch_one(&pool)
                    .await
                    .map_err(|err| err.to_string())?;
            let terminal_status: String =
                sqlx::query_scalar("select status from supervisor_commands where id = $1")
                    .bind(terminal_command_id)
                    .fetch_one(&pool)
                    .await
                    .map_err(|err| err.to_string())?;

            assert_eq!(running_status, "pending");
            assert_eq!(terminal_status, "done");
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[test]
    fn codex_active_turn_without_turn_id_times_out_for_scheduling() {
        assert_eq!(
            codex_active_turn_schedule_state(
                Some("turn-1"),
                false,
                CODEX_TURN_START_TIMEOUT + Duration::from_secs(1)
            ),
            CodexActiveTurnScheduleState::ReadyForSteer
        );
        assert_eq!(
            codex_active_turn_schedule_state(None, false, Duration::from_secs(5)),
            CodexActiveTurnScheduleState::WaitingForTurnId
        );
        assert_eq!(
            codex_active_turn_schedule_state(
                None,
                false,
                CODEX_TURN_START_TIMEOUT + Duration::from_secs(1)
            ),
            CodexActiveTurnScheduleState::StuckBeforeTurnId
        );
    }

    #[tokio::test]
    async fn supervisor_command_claim_is_single_consumer() {
        let Some((pool, schema)) = test_pool_with_connections(8).await else {
            return;
        };
        let result: Result<(), String> = async {
            let command_id: Uuid = sqlx::query_scalar(
                r#"
                insert into supervisor_commands (command_type)
                values ('start_agent')
                returning id
                "#,
            )
            .fetch_one(&pool)
            .await
            .map_err(|err| err.to_string())?;

            let (r1, r2, r3, r4) = tokio::join!(
                claim_next_supervisor_command(&pool),
                claim_next_supervisor_command(&pool),
                claim_next_supervisor_command(&pool),
                claim_next_supervisor_command(&pool)
            );
            let mut claimed = Vec::new();
            for result in [r1, r2, r3, r4] {
                if let Some(command) = result? {
                    claimed.push(command.id);
                }
            }

            assert_eq!(claimed, vec![command_id]);
            let status: String =
                sqlx::query_scalar("select status from supervisor_commands where id = $1")
                    .bind(command_id)
                    .fetch_one(&pool)
                    .await
                    .map_err(|err| err.to_string())?;
            assert_eq!(status, "running");
            Ok(())
        }
        .await;
        drop_test_schema(pool, schema).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }
}
