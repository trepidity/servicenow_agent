//! Hidden `snow daemon __serve` entrypoint.

use std::path::PathBuf;

use anyhow::{Context, Result};

use super::paths::DaemonPaths;

pub fn run(env_name: &str, no_idle_timeout: bool) -> Result<()> {
    set_selected_env(env_name);
    if no_idle_timeout {
        set_no_idle_timeout();
    }
    let paths = DaemonPaths::resolve()?;
    let env_file = load_env_file(env_name);
    write_runtime_files(&paths, env_name, env_file.as_ref())?;

    let result = snow_daemon::run_blocking_with_endpoint(
        paths.endpoint.clone(),
        paths.config_dir.clone(),
        None,
    );

    let _ = std::fs::remove_file(&paths.pidfile);
    let _ = std::fs::remove_file(&paths.statusfile);

    result
}

fn write_runtime_files(
    paths: &DaemonPaths,
    env_name: &str,
    env_file: Option<&PathBuf>,
) -> Result<()> {
    let pid = std::process::id();
    std::fs::write(&paths.pidfile, format!("{pid}\n"))
        .with_context(|| format!("writing {}", paths.pidfile.display()))?;

    let status = serde_json::json!({
        "pid": pid,
        "started_at": chrono::Utc::now().to_rfc3339(),
        "version": env!("CARGO_PKG_VERSION"),
        "environment": env_name,
        "env_file": env_file.map(|path| path.display().to_string()),
        "socket": paths.socket.display().to_string(),
        "endpoint": paths.endpoint.status_string(),
    });
    std::fs::write(&paths.statusfile, serde_json::to_string_pretty(&status)?)
        .with_context(|| format!("writing {}", paths.statusfile.display()))?;
    Ok(())
}

fn load_env_file(env_name: &str) -> Option<PathBuf> {
    let filename = format!(".env.{env_name}");
    let path = super::paths::config_path(&filename);
    if path.exists() {
        let _ = dotenvy::from_path(&path);
        return Some(path);
    }
    None
}

fn set_selected_env(env_name: &str) {
    // SAFETY: `__serve` runs before the daemon's Tokio runtime and worker tasks
    // exist, so this process-wide env write cannot race with other threads.
    unsafe {
        std::env::set_var("SNOW_ENV", env_name);
    }
}

fn set_no_idle_timeout() {
    // The daemon resolves its idle window from `SNOW_DAEMON_IDLE_SECS` at
    // startup; `0` disables idle self-shutdown. SAFETY: same single-threaded
    // pre-runtime context as `set_selected_env`.
    unsafe {
        std::env::set_var("SNOW_DAEMON_IDLE_SECS", "0");
    }
}
