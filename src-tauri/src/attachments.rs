use std::{
    env, fs,
    path::{Path, PathBuf},
};

use serde::Deserialize;
use uuid::Uuid;

use crate::{models::AttachmentUpload, CommandResult};

pub(crate) const ATTACHMENT_SIZE_LIMIT: usize = 25 * 1024 * 1024;

#[derive(Debug, Deserialize)]
pub(crate) struct AgentAttachmentFile {
    #[serde(alias = "local_path")]
    pub(crate) path: String,
    pub(crate) name: Option<String>,
    #[serde(alias = "mime")]
    pub(crate) mime_type: Option<String>,
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

pub(crate) fn load_agent_attachment_uploads(
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

pub(crate) fn default_attachment_message_body(uploads: &[AttachmentUpload]) -> String {
    if uploads.len() == 1 {
        format!("Attached file: {}", uploads[0].original_name.trim())
    } else {
        format!("Attached {} files.", uploads.len())
    }
}

fn attachment_root_dir() -> CommandResult<PathBuf> {
    if let Ok(path) = env::var("LANTOR_ATTACHMENT_DIR") {
        return Ok(PathBuf::from(path));
    }
    let home = env::var("HOME").map_err(|_| "HOME is not set".to_owned())?;
    Ok(PathBuf::from(home)
        .join("Library")
        .join("Application Support")
        .join("Lantor")
        .join("attachments"))
}

fn attachment_extension(original_name: &str) -> String {
    let path = PathBuf::from(original_name);
    let Some(extension) = path.extension().and_then(|value| value.to_str()) else {
        return String::new();
    };
    let sanitized: String = extension
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .take(12)
        .collect();
    if sanitized.is_empty() {
        String::new()
    } else {
        format!(".{}", sanitized.to_ascii_lowercase())
    }
}

pub(crate) fn write_attachment_file(
    message_id: Uuid,
    attachment_id: Uuid,
    original_name: &str,
    bytes: &[u8],
) -> CommandResult<String> {
    let root = attachment_root_dir()?;
    let message_dir = root.join(message_id.to_string());
    fs::create_dir_all(&message_dir).map_err(|err| err.to_string())?;
    let path = message_dir.join(format!(
        "{}{}",
        attachment_id,
        attachment_extension(original_name)
    ));
    fs::write(&path, bytes).map_err(|err| err.to_string())?;
    Ok(path.to_string_lossy().to_string())
}

pub(crate) fn format_attachment_size(size_bytes: i64) -> String {
    if size_bytes >= 1_000_000 {
        format!("{:.1}MB", size_bytes as f64 / 1_000_000.0)
    } else if size_bytes >= 1_000 {
        format!("{:.1}KB", size_bytes as f64 / 1_000.0)
    } else {
        format!("{size_bytes}B")
    }
}

pub(crate) fn attachment_summary_sql() -> &'static str {
    r#"
    coalesce((
        select group_concat(
            'attachment_id=' ||
                lower(
                    substr(hex(ma.id), 1, 8) || '-' ||
                    substr(hex(ma.id), 9, 4) || '-' ||
                    substr(hex(ma.id), 13, 4) || '-' ||
                    substr(hex(ma.id), 17, 4) || '-' ||
                    substr(hex(ma.id), 21, 12)
                ) ||
            ' name=' || quote(ma.original_name) ||
            ' mime=' || ma.mime_type ||
            ' size=' || ma.size_bytes ||
            ' local_path=' || quote(ma.storage_path),
            char(10)
        )
        from (
            select *
            from message_attachments
            where message_id = m.id
            order by created_at asc
        ) ma
    ), '') as attachment_summary
    "#
}
