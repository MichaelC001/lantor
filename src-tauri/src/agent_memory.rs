use std::{fs, io::Write, path::PathBuf};

use chrono::Utc;
use sqlx::{Row, SqlitePool};
use uuid::Uuid;

use crate::{prompts::ensure_agent_workspace, to_string, CommandResult};

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
pub(crate) fn format_memory_index_entry(body: &str) -> String {
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

pub(crate) fn insert_memory_index_entry(memory: &str, entry: &str) -> String {
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

pub(crate) async fn append_agent_memory(
    pool: &SqlitePool,
    agent_id: Uuid,
    body: &str,
) -> CommandResult<()> {
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
        .and_then(|mut file| file.write_all(entry.as_bytes()))
        .map_err(to_string)
}

pub(crate) async fn compact_agent_memory(
    pool: &SqlitePool,
    agent_id: Uuid,
    body: &str,
) -> CommandResult<()> {
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

pub(crate) async fn append_run_log(
    pool: &SqlitePool,
    run_id: Uuid,
    line: String,
) -> CommandResult<()> {
    sqlx::query("update agent_runs set log = substr(log || $2, -20000) where id = $1")
        .bind(run_id)
        .bind(line)
        .execute(pool)
        .await
        .map_err(to_string)?;

    Ok(())
}
