use std::path::PathBuf;

use anyhow::{Context, Result};
use snow_core::ipc::IpcEndpoint;

const DEFAULT_ENV: &str = "test";
const DEFAULT_DAEMON_START_ENV: &str = "prd";

pub struct DaemonPaths {
    pub config_dir: PathBuf,
    pub pidfile: PathBuf,
    // Only the Unix daemon lifecycle modules read these; unused on Windows.
    #[cfg_attr(not(unix), allow(dead_code))]
    pub statusfile: PathBuf,
    pub logfile: PathBuf,
    #[cfg_attr(not(unix), allow(dead_code))]
    pub logfile_rotated: PathBuf,
    pub socket: PathBuf,
    pub endpoint: IpcEndpoint,
}

impl DaemonPaths {
    pub fn resolve() -> Result<Self> {
        let config_dir = resolve_config_dir().context("resolving snow config dir")?;
        std::fs::create_dir_all(&config_dir)
            .with_context(|| format!("creating {}", config_dir.display()))?;
        let endpoint = IpcEndpoint::for_config_dir(&config_dir);
        let socket = endpoint
            .filesystem_path()
            .map(PathBuf::from)
            .unwrap_or_else(|| config_dir.join("daemon.sock"));
        Ok(Self {
            pidfile: config_dir.join("daemon.pid"),
            statusfile: config_dir.join("daemon.status"),
            logfile: config_dir.join("daemon.log"),
            logfile_rotated: config_dir.join("daemon.log.1"),
            socket,
            endpoint,
            config_dir,
        })
    }

    #[cfg(test)]
    pub fn under(root: PathBuf) -> Self {
        let endpoint = IpcEndpoint::for_config_dir(&root);
        let socket = endpoint
            .filesystem_path()
            .map(PathBuf::from)
            .unwrap_or_else(|| root.join("daemon.sock"));
        Self {
            pidfile: root.join("daemon.pid"),
            statusfile: root.join("daemon.status"),
            logfile: root.join("daemon.log"),
            logfile_rotated: root.join("daemon.log.1"),
            socket,
            endpoint,
            config_dir: root,
        }
    }
}

pub fn selected_env(explicit: Option<&str>) -> String {
    selected_env_with_default(explicit, DEFAULT_ENV)
}

pub fn selected_daemon_start_env(explicit: Option<&str>) -> String {
    selected_env_with_default(explicit, DEFAULT_DAEMON_START_ENV)
}

fn selected_env_with_default(explicit: Option<&str>, default_env: &str) -> String {
    let env_var = std::env::var("SNOW_ENV").ok();
    let persisted = persisted_env_selection();
    select_env(
        explicit,
        env_var.as_deref(),
        persisted.as_deref(),
        default_env,
    )
}

fn select_env(
    explicit: Option<&str>,
    env_var: Option<&str>,
    persisted: Option<&str>,
    default_env: &str,
) -> String {
    explicit
        .map(ToOwned::to_owned)
        .or_else(|| env_var.map(ToOwned::to_owned))
        .or_else(|| {
            persisted
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
        })
        .unwrap_or_else(|| default_env.to_string())
}

fn persisted_env_selection() -> Option<String> {
    DaemonPaths::resolve()
        .ok()
        .and_then(|paths| std::fs::read_to_string(paths.config_dir.join("env")).ok())
}

pub fn config_path(filename: &str) -> PathBuf {
    if let Some(p) = std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|d| d.join(filename)))
        && p.exists()
    {
        return p;
    }

    if let Ok(config_dir) = resolve_config_dir() {
        let p = config_dir.join(filename);
        if p.exists() {
            return p;
        }
    }

    PathBuf::from(filename)
}

pub fn config_dir_hint() -> String {
    resolve_config_dir()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|_| ".snow".to_string())
}

pub fn resolve_config_dir() -> Result<PathBuf> {
    snow_core::ipc::resolve_config_dir().context("resolving shared snow config dir")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn under_builds_consistent_paths() {
        let tmp = tempfile::tempdir().unwrap();
        let p = DaemonPaths::under(tmp.path().to_path_buf());
        assert_eq!(p.pidfile.file_name().unwrap(), "daemon.pid");
        assert_eq!(p.socket.file_name().unwrap(), "daemon.sock");
        assert_eq!(p.logfile.file_name().unwrap(), "daemon.log");
        assert_eq!(p.statusfile.file_name().unwrap(), "daemon.status");
    }

    #[test]
    fn select_env_respects_source_precedence() {
        assert_eq!(
            select_env(Some("training"), Some("prd"), Some("test"), "prd"),
            "training"
        );
        assert_eq!(
            select_env(None, Some("training"), Some("test"), "prd"),
            "training"
        );
        assert_eq!(select_env(None, None, Some("test"), "prd"), "test");
    }

    #[test]
    fn select_env_allows_different_fallback_defaults() {
        assert_eq!(select_env(None, None, None, "test"), "test");
        assert_eq!(select_env(None, None, None, "prd"), "prd");
        assert_eq!(select_env(None, None, Some("  "), "prd"), "prd");
    }
}
