use std::{
    collections::HashSet,
    env, fs,
    io::ErrorKind,
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

use serde::Deserialize;
use sqlx::SqlitePool;
use uuid::Uuid;

use crate::{
    app::{to_string, CommandResult},
    models::AttachmentUpload,
};

pub(crate) const ATTACHMENT_SIZE_LIMIT: usize = 25 * 1024 * 1024;
const ATTACHMENT_ORPHAN_GRACE: Duration = Duration::from_secs(60 * 60);

#[derive(Default)]
pub(crate) struct PendingAttachmentWrites {
    paths: Vec<PathBuf>,
    committed: bool,
}

impl PendingAttachmentWrites {
    pub(crate) fn track(&mut self, storage_path: &str) {
        self.paths.push(PathBuf::from(storage_path));
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.paths.is_empty()
    }

    pub(crate) fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for PendingAttachmentWrites {
    fn drop(&mut self) {
        if self.committed || self.paths.is_empty() {
            return;
        }
        let report = remove_attachment_paths(&self.paths, None);
        for error in report.errors {
            eprintln!("Lantor attachment rollback cleanup failed: {error}");
        }
    }
}

#[derive(Default)]
struct AttachmentCleanupReport {
    removed_files: usize,
    removed_directories: usize,
    errors: Vec<String>,
}

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

fn is_uuid_component(value: Option<&std::ffi::OsStr>) -> bool {
    value
        .and_then(|value| value.to_str())
        .is_some_and(|value| Uuid::parse_str(value).is_ok())
}

fn has_managed_attachment_shape(path: &Path) -> bool {
    let Some(message_dir) = path.parent() else {
        return false;
    };
    is_uuid_component(message_dir.file_name()) && is_uuid_component(path.file_stem())
}

fn is_managed_attachment_path(root: &Path, path: &Path) -> bool {
    has_managed_attachment_shape(path)
        && path
            .parent()
            .and_then(Path::parent)
            .is_some_and(|parent| parent == root)
}

fn remove_attachment_paths(
    paths: &[PathBuf],
    required_root: Option<&Path>,
) -> AttachmentCleanupReport {
    let mut report = AttachmentCleanupReport::default();
    let mut parent_directories = HashSet::new();
    for path in paths {
        let managed = required_root
            .map(|root| is_managed_attachment_path(root, path))
            .unwrap_or_else(|| has_managed_attachment_shape(path));
        if !managed {
            report.errors.push(format!(
                "refused to remove unmanaged attachment path {}",
                path.display()
            ));
            continue;
        }
        if let Some(parent) = path.parent() {
            parent_directories.insert(parent.to_path_buf());
        }
        match fs::remove_file(path) {
            Ok(()) => report.removed_files += 1,
            Err(err) if err.kind() == ErrorKind::NotFound => {}
            Err(err) => report
                .errors
                .push(format!("cannot remove {}: {err}", path.display())),
        }
    }
    for directory in parent_directories {
        match fs::remove_dir(&directory) {
            Ok(()) => report.removed_directories += 1,
            Err(err)
                if matches!(
                    err.kind(),
                    ErrorKind::NotFound | ErrorKind::DirectoryNotEmpty
                ) => {}
            Err(err) => report.errors.push(format!(
                "cannot remove attachment directory {}: {err}",
                directory.display()
            )),
        }
    }
    report
}

pub(crate) fn remove_attachment_files(storage_paths: &[String]) {
    let root = match attachment_root_dir() {
        Ok(root) => root,
        Err(err) => {
            eprintln!("Lantor attachment cleanup skipped: {err}");
            return;
        }
    };
    let paths = storage_paths.iter().map(PathBuf::from).collect::<Vec<_>>();
    let report = remove_attachment_paths(&paths, Some(&root));
    for error in report.errors {
        eprintln!("Lantor attachment cleanup failed: {error}");
    }
}

fn sweep_orphan_attachment_files(
    root: &Path,
    referenced_paths: &HashSet<PathBuf>,
    orphan_cutoff: SystemTime,
) -> AttachmentCleanupReport {
    let mut report = AttachmentCleanupReport::default();
    let directory_entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(err) if err.kind() == ErrorKind::NotFound => return report,
        Err(err) => {
            report.errors.push(format!(
                "cannot scan attachment root {}: {err}",
                root.display()
            ));
            return report;
        }
    };

    for message_entry in directory_entries {
        let message_entry = match message_entry {
            Ok(entry) => entry,
            Err(err) => {
                report
                    .errors
                    .push(format!("cannot read attachment directory entry: {err}"));
                continue;
            }
        };
        let message_dir = message_entry.path();
        let is_managed_directory = message_entry
            .file_type()
            .is_ok_and(|file_type| file_type.is_dir())
            && is_uuid_component(message_dir.file_name());
        if !is_managed_directory {
            continue;
        }
        let attachment_entries = match fs::read_dir(&message_dir) {
            Ok(entries) => entries,
            Err(err) => {
                report.errors.push(format!(
                    "cannot scan attachment directory {}: {err}",
                    message_dir.display()
                ));
                continue;
            }
        };
        let mut orphan_paths = Vec::new();
        for attachment_entry in attachment_entries {
            let attachment_entry = match attachment_entry {
                Ok(entry) => entry,
                Err(err) => {
                    report.errors.push(format!(
                        "cannot read entry in attachment directory {}: {err}",
                        message_dir.display()
                    ));
                    continue;
                }
            };
            let path = attachment_entry.path();
            if !attachment_entry
                .file_type()
                .is_ok_and(|file_type| file_type.is_file())
            {
                continue;
            }
            let metadata = match attachment_entry.metadata() {
                Ok(metadata) => metadata,
                Err(err) => {
                    report.errors.push(format!(
                        "cannot inspect attachment file {}: {err}",
                        path.display()
                    ));
                    continue;
                }
            };
            if !is_managed_attachment_path(root, &path)
                || referenced_paths.contains(&path)
                || !metadata
                    .modified()
                    .is_ok_and(|modified| modified <= orphan_cutoff)
            {
                continue;
            }
            orphan_paths.push(path);
        }
        let removed = remove_attachment_paths(&orphan_paths, Some(root));
        report.removed_files += removed.removed_files;
        report.removed_directories += removed.removed_directories;
        report.errors.extend(removed.errors);
    }
    report
}

async fn garbage_collect_orphan_attachments(
    pool: &SqlitePool,
) -> CommandResult<AttachmentCleanupReport> {
    let referenced_paths =
        sqlx::query_scalar::<_, String>("select storage_path from message_attachments")
            .fetch_all(pool)
            .await
            .map_err(to_string)?
            .into_iter()
            .map(PathBuf::from)
            .collect::<HashSet<_>>();
    let root = attachment_root_dir()?;
    let orphan_cutoff = SystemTime::now()
        .checked_sub(ATTACHMENT_ORPHAN_GRACE)
        .unwrap_or(SystemTime::UNIX_EPOCH);
    tokio::task::spawn_blocking(move || {
        sweep_orphan_attachment_files(&root, &referenced_paths, orphan_cutoff)
    })
    .await
    .map_err(|err| format!("attachment garbage collector task failed: {err}"))
}

pub(crate) fn spawn_attachment_garbage_collector(pool: SqlitePool) {
    tauri::async_runtime::spawn(async move {
        match garbage_collect_orphan_attachments(&pool).await {
            Ok(report) => {
                if report.removed_files > 0 {
                    eprintln!(
                        "Lantor attachment garbage collector removed {} orphan files",
                        report.removed_files
                    );
                }
                for error in report.errors {
                    eprintln!("Lantor attachment garbage collector failed: {error}");
                }
            }
            Err(err) => eprintln!("Lantor attachment garbage collector failed: {err}"),
        }
    });
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
    if let Err(err) = fs::write(&path, bytes) {
        let _ = fs::remove_file(&path);
        let _ = fs::remove_dir(&message_dir);
        return Err(err.to_string());
    }
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

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        fs,
        path::{Path, PathBuf},
        time::{Duration, SystemTime},
    };

    use uuid::Uuid;

    use super::{sweep_orphan_attachment_files, PendingAttachmentWrites};

    fn attachment_test_root(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!("lantor-{label}-{}", Uuid::new_v4()))
    }

    fn managed_test_path(root: &Path) -> PathBuf {
        root.join(Uuid::new_v4().to_string())
            .join(format!("{}.txt", Uuid::new_v4()))
    }

    #[test]
    fn pending_attachment_writes_are_removed_unless_committed() {
        let root = attachment_test_root("attachment-rollback");
        let path = managed_test_path(&root);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, b"pending").unwrap();

        let mut pending = PendingAttachmentWrites::default();
        pending.track(&path.to_string_lossy());
        drop(pending);

        assert!(!path.exists());
        assert!(!path.parent().unwrap().exists());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn orphan_sweep_preserves_referenced_and_unmanaged_files() {
        let root = attachment_test_root("attachment-gc");
        let referenced = managed_test_path(&root);
        let orphan = managed_test_path(&root);
        let unmanaged = root.join("keep-me.txt");
        for path in [&referenced, &orphan, &unmanaged] {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).unwrap();
            }
            fs::write(path, b"attachment").unwrap();
        }

        let report = sweep_orphan_attachment_files(
            &root,
            &HashSet::from([referenced.clone()]),
            SystemTime::now() + Duration::from_secs(1),
        );

        assert_eq!(report.removed_files, 1);
        assert!(referenced.exists());
        assert!(!orphan.exists());
        assert!(unmanaged.exists());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn orphan_sweep_preserves_recent_uncommitted_files() {
        let root = attachment_test_root("attachment-gc-grace");
        let recent = managed_test_path(&root);
        fs::create_dir_all(recent.parent().unwrap()).unwrap();
        fs::write(&recent, b"pending transaction").unwrap();

        let report = sweep_orphan_attachment_files(
            &root,
            &HashSet::new(),
            SystemTime::now() - Duration::from_secs(60),
        );

        assert_eq!(report.removed_files, 0);
        assert!(recent.exists());
        let _ = fs::remove_dir_all(root);
    }
}
