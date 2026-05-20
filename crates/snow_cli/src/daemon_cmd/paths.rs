use std::path::PathBuf;

use anyhow::{Context, Result};

pub struct DaemonPaths {
    pub config_dir: PathBuf,
    pub pidfile: PathBuf,
    pub statusfile: PathBuf,
    pub logfile: PathBuf,
    pub logfile_rotated: PathBuf,
    pub socket: PathBuf,
}

impl DaemonPaths {
    pub fn resolve() -> Result<Self> {
        let config_dir = resolve_config_dir().context("resolving snow config dir")?;
        std::fs::create_dir_all(&config_dir)
            .with_context(|| format!("creating {}", config_dir.display()))?;
        Ok(Self {
            pidfile: config_dir.join("daemon.pid"),
            statusfile: config_dir.join("daemon.status"),
            logfile: config_dir.join("daemon.log"),
            logfile_rotated: config_dir.join("daemon.log.1"),
            socket: config_dir.join("daemon.sock"),
            config_dir,
        })
    }

    #[cfg(test)]
    pub fn under(root: PathBuf) -> Self {
        Self {
            pidfile: root.join("daemon.pid"),
            statusfile: root.join("daemon.status"),
            logfile: root.join("daemon.log"),
            logfile_rotated: root.join("daemon.log.1"),
            socket: root.join("daemon.sock"),
            config_dir: root,
        }
    }
}

pub fn selected_env(explicit: Option<&str>) -> String {
    explicit
        .map(ToOwned::to_owned)
        .or_else(|| std::env::var("SNOW_ENV").ok())
        .or_else(|| {
            DaemonPaths::resolve()
                .ok()
                .and_then(|paths| std::fs::read_to_string(paths.config_dir.join("env")).ok())
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
        })
        .unwrap_or_else(|| "test".to_string())
}

pub fn config_path(filename: &str) -> PathBuf {
    if let Some(p) = std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|d| d.join(filename)))
        && p.exists()
    {
        return p;
    }

    if let Some(home) = dirs::home_dir() {
        let p = home.join(".config").join("snow").join(filename);
        if p.exists() {
            return p;
        }
    }

    PathBuf::from(filename)
}

fn resolve_config_dir() -> Result<PathBuf> {
    // Match the existing snow config-dir pattern documented in
    // memory/reference_config_dir_pattern.md: exe -> ~/.config/snow -> cwd.
    if let Ok(exe) = std::env::current_exe()
        && let Some(dir) = exe.parent()
    {
        let candidate = dir.join("snow_config");
        if candidate.exists() {
            return Ok(candidate);
        }
    }
    if let Some(home) = dirs::home_dir() {
        let candidate = home.join(".config").join("snow");
        return Ok(candidate);
    }
    Ok(std::env::current_dir()?.join(".snow"))
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
}
