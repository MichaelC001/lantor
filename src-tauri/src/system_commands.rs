use std::{
    path::Path,
    process::{Command as StdCommand, Stdio},
};

use crate::{
    app::{to_string, CommandResult},
    db::expand_home_path,
    models::RuntimeCheck,
};

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
pub(crate) async fn open_external_url(url: String) -> CommandResult<()> {
    let target = normalize_open_link_target(&url).ok_or_else(|| {
        "only http, https, mailto, file, and existing local file links can be opened".to_owned()
    })?;
    tauri::async_runtime::spawn_blocking(move || open_link_target_with_system(&target))
        .await
        .map_err(to_string)?
}

#[tauri::command]
pub(crate) async fn check_runtime(runtime: String) -> CommandResult<RuntimeCheck> {
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

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::normalize_open_link_target;

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
}
