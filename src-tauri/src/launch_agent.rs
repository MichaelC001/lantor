use std::{
    env, fs,
    path::{Path, PathBuf},
    process::{Command as StdCommand, Stdio},
};

use crate::{
    app::{to_string, CommandResult},
    models::LaunchAgentStatus,
};

const LAUNCH_AGENT_LABEL: &str = "local.lantor.supervisor";

pub(crate) fn spawn_supervisor_process(database_url: &str) {
    let Ok(exe) = env::current_exe() else {
        eprintln!("failed to resolve current executable for Lantor supervisor");
        return;
    };

    if let Err(err) = StdCommand::new(exe)
        .arg("--supervisor")
        .env("LANTOR_DATABASE_URL", database_url)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    {
        eprintln!("failed to spawn Lantor supervisor: {err}");
    }
}

pub(crate) fn load_launch_agent_status() -> CommandResult<LaunchAgentStatus> {
    let plist_path = launch_agent_plist_path()?;
    let installed = plist_path.exists();
    let loaded = launch_agent_domain()
        .map(|domain| {
            StdCommand::new("launchctl")
                .arg("print")
                .arg(launch_agent_service_target(&domain))
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .map(|status| status.success())
                .unwrap_or(false)
        })
        .unwrap_or(false);

    Ok(LaunchAgentStatus {
        label: LAUNCH_AGENT_LABEL.to_owned(),
        plist_path: plist_path.to_string_lossy().to_string(),
        installed,
        loaded,
    })
}

pub(crate) fn install_supervisor_service(database_url: &str) -> CommandResult<LaunchAgentStatus> {
    let plist_path = launch_agent_plist_path()?;
    let exe_path = env::current_exe().map_err(to_string)?;
    let plist = render_launch_agent_plist(&exe_path, database_url);

    if let Some(parent) = plist_path.parent() {
        fs::create_dir_all(parent).map_err(to_string)?;
    }
    fs::write(&plist_path, plist).map_err(to_string)?;

    let domain = launch_agent_domain()?;
    let service = launch_agent_service_target(&domain);
    let _ = StdCommand::new("launchctl")
        .arg("bootout")
        .arg(&service)
        .output();

    run_launchctl(&["bootstrap", &domain, &plist_path.to_string_lossy()])?;
    run_launchctl(&["kickstart", "-k", &service])?;

    load_launch_agent_status()
}

pub(crate) fn uninstall_supervisor_service() -> CommandResult<LaunchAgentStatus> {
    let domain = launch_agent_domain()?;
    let service = launch_agent_service_target(&domain);
    let _ = StdCommand::new("launchctl")
        .arg("bootout")
        .arg(&service)
        .output();

    remove_plist_if_exists(&launch_agent_plist_path()?)?;

    load_launch_agent_status()
}

fn remove_plist_if_exists(plist_path: &Path) -> CommandResult<()> {
    match fs::remove_file(plist_path) {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(err.to_string()),
    }
    Ok(())
}

fn launch_agent_plist_path() -> CommandResult<PathBuf> {
    launch_agent_plist_path_for_label(LAUNCH_AGENT_LABEL)
}

fn launch_agent_plist_path_for_label(label: &str) -> CommandResult<PathBuf> {
    let home = env::var_os("HOME").ok_or_else(|| "HOME is not set".to_owned())?;
    Ok(PathBuf::from(home)
        .join("Library")
        .join("LaunchAgents")
        .join(format!("{label}.plist")))
}

fn launch_agent_domain() -> CommandResult<String> {
    let output = StdCommand::new("id")
        .arg("-u")
        .output()
        .map_err(to_string)?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).trim().to_owned());
    }
    let uid = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    if uid.is_empty() {
        return Err("failed to resolve current uid".to_owned());
    }
    Ok(format!("gui/{uid}"))
}

fn launch_agent_service_target(domain: &str) -> String {
    launch_agent_service_target_for_label(domain, LAUNCH_AGENT_LABEL)
}

fn launch_agent_service_target_for_label(domain: &str, label: &str) -> String {
    format!("{domain}/{label}")
}

fn run_launchctl(args: &[&str]) -> CommandResult<()> {
    let output = StdCommand::new("launchctl")
        .args(args)
        .output()
        .map_err(to_string)?;
    if output.status.success() {
        return Ok(());
    }

    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    Err(format!(
        "launchctl {} failed: {}{}",
        args.join(" "),
        stderr.trim(),
        if stdout.trim().is_empty() {
            String::new()
        } else {
            format!(" {}", stdout.trim())
        }
    ))
}

fn render_launch_agent_plist(exe_path: &Path, database_url: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>{}</string>
  <key>ProgramArguments</key>
  <array>
    <string>{}</string>
    <string>--supervisor</string>
  </array>
  <key>EnvironmentVariables</key>
  <dict>
    <key>LANTOR_DATABASE_URL</key>
    <string>{}</string>
  </dict>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>StandardOutPath</key>
  <string>{}</string>
  <key>StandardErrorPath</key>
  <string>{}</string>
</dict>
</plist>
"#,
        xml_escape(LAUNCH_AGENT_LABEL),
        xml_escape(&exe_path.to_string_lossy()),
        xml_escape(database_url),
        xml_escape("/tmp/lantor-supervisor.out.log"),
        xml_escape("/tmp/lantor-supervisor.err.log"),
    )
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
