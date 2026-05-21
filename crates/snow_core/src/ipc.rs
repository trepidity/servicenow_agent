use std::env;
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};

use interprocess::local_socket::tokio::{Listener, Stream};
use interprocess::local_socket::{GenericFilePath, GenericNamespaced, ListenerOptions, Name};
use interprocess::local_socket::{ToFsName, ToNsName};

const DAEMON_SOCKET_FILENAME: &str = "daemon.sock";
#[cfg(windows)]
const DAEMON_PIPE_PREFIX: &str = "snow-daemon";
#[cfg(windows)]
const DAEMON_PIPE_SCOPE_VERSION: &str = "v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigDirResolver {
    exe_dir: Option<PathBuf>,
    home_dir: Option<PathBuf>,
    appdata_dir: Option<PathBuf>,
    current_dir: PathBuf,
}

impl ConfigDirResolver {
    pub fn from_env() -> io::Result<Self> {
        Ok(Self {
            exe_dir: env::current_exe()
                .ok()
                .and_then(|path| path.parent().map(Path::to_path_buf)),
            home_dir: dirs::home_dir(),
            appdata_dir: env::var_os("APPDATA").map(PathBuf::from),
            current_dir: env::current_dir()?,
        })
    }

    pub fn new(
        exe_dir: Option<PathBuf>,
        home_dir: Option<PathBuf>,
        appdata_dir: Option<PathBuf>,
        current_dir: PathBuf,
    ) -> Self {
        Self {
            exe_dir,
            home_dir,
            appdata_dir,
            current_dir,
        }
    }

    pub fn resolve(&self) -> PathBuf {
        if let Some(exe_dir) = &self.exe_dir {
            let candidate = exe_dir.join("snow_config");
            if candidate.exists() {
                return candidate;
            }
        }

        #[cfg(windows)]
        {
            if let Some(appdata_dir) = &self.appdata_dir {
                return appdata_dir.join("snow");
            }
        }

        #[cfg(unix)]
        {
            if let Some(home_dir) = &self.home_dir {
                return home_dir.join(".config").join("snow");
            }
        }

        self.current_dir.join(".snow")
    }
}

pub fn resolve_config_dir() -> io::Result<PathBuf> {
    ConfigDirResolver::from_env().map(|resolver| resolver.resolve())
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum IpcEndpoint {
    Filesystem { path: PathBuf },
    Namespaced { name: String },
}

pub type IpcStream = Stream;

impl IpcEndpoint {
    pub fn filesystem(path: impl Into<PathBuf>) -> Self {
        Self::Filesystem { path: path.into() }
    }

    pub fn namespaced(name: impl Into<String>) -> Self {
        Self::Namespaced { name: name.into() }
    }

    pub fn for_config_dir(config_dir: impl AsRef<Path>) -> Self {
        let config_dir = config_dir.as_ref();

        #[cfg(windows)]
        {
            return Self::Namespaced {
                name: daemon_pipe_name(config_dir),
            };
        }

        #[cfg(not(windows))]
        {
            Self::Filesystem {
                path: config_dir.join(DAEMON_SOCKET_FILENAME),
            }
        }
    }

    pub fn parse(value: &str) -> Self {
        if let Some(rest) = value
            .strip_prefix("fs:")
            .or_else(|| value.strip_prefix("file:"))
        {
            return Self::filesystem(expand_home(rest));
        }
        if let Some(rest) = value
            .strip_prefix("ns:")
            .or_else(|| value.strip_prefix("name:"))
        {
            return Self::namespaced(rest);
        }

        #[cfg(unix)]
        {
            if value == "~" || value.starts_with("~/") || value.contains('/') {
                return Self::filesystem(expand_home(value));
            }
        }

        #[cfg(windows)]
        {
            if value.contains('\\') || value.contains('/') || looks_like_windows_drive_path(value) {
                return Self::filesystem(expand_home(value));
            }
        }

        Self::namespaced(value)
    }

    pub fn from_socket_path(path: impl Into<PathBuf>) -> Self {
        Self::filesystem(path)
    }

    pub fn filesystem_path(&self) -> Option<&Path> {
        match self {
            Self::Filesystem { path } => Some(path.as_path()),
            Self::Namespaced { .. } => None,
        }
    }

    pub fn socket_path(&self) -> Option<&Path> {
        self.filesystem_path()
    }

    pub fn status_string(&self) -> String {
        match self {
            Self::Filesystem { path } => path.display().to_string(),
            Self::Namespaced { name } => namespaced_status_string(name),
        }
    }

    pub fn to_local_socket_name(&self) -> io::Result<Name<'static>> {
        match self {
            Self::Filesystem { path } => path.clone().to_fs_name::<GenericFilePath>(),
            Self::Namespaced { name } => name.clone().to_ns_name::<GenericNamespaced>(),
        }
    }

    pub fn listener_options(&self) -> io::Result<ListenerOptions<'static>> {
        Ok(ListenerOptions::new().name(self.to_local_socket_name()?))
    }

    pub fn listen(&self) -> io::Result<Listener> {
        self.listener_options()?.create_tokio()
    }

    pub async fn connect(&self) -> io::Result<Stream> {
        use interprocess::local_socket::tokio::prelude::*;

        Stream::connect(self.to_local_socket_name()?).await
    }
}

pub fn default_endpoint_from_env() -> io::Result<IpcEndpoint> {
    if let Some(endpoint) = env::var_os("SNOW_DAEMON_ENDPOINT") {
        return Ok(IpcEndpoint::parse(&endpoint.to_string_lossy()));
    }
    if let Some(socket) = env::var_os("SNOW_DAEMON_SOCKET") {
        return Ok(IpcEndpoint::filesystem(expand_home(
            &socket.to_string_lossy(),
        )));
    }
    Ok(IpcEndpoint::for_config_dir(resolve_config_dir()?))
}

impl fmt::Display for IpcEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.status_string())
    }
}

#[cfg(windows)]
fn daemon_pipe_name(config_dir: &Path) -> String {
    format!(
        "{DAEMON_PIPE_PREFIX}-{DAEMON_PIPE_SCOPE_VERSION}-{}",
        config_dir_scope(config_dir)
    )
}

#[cfg(windows)]
fn config_dir_scope(config_dir: &Path) -> String {
    let mut source = config_dir.as_os_str().to_string_lossy().into_owned();
    source = source.replace('\\', "/").to_ascii_lowercase();

    blake3::hash(source.as_bytes()).to_hex()[..20].to_string()
}

fn namespaced_status_string(name: &str) -> String {
    #[cfg(windows)]
    {
        format!(r"\\.\pipe\{name}")
    }

    #[cfg(not(windows))]
    {
        name.to_string()
    }
}

fn expand_home(value: &str) -> PathBuf {
    if value == "~" {
        return dirs::home_dir().unwrap_or_else(|| PathBuf::from(value));
    }
    if let Some(rest) = value.strip_prefix("~/")
        && let Some(home) = dirs::home_dir()
    {
        return home.join(rest);
    }
    PathBuf::from(value)
}

#[cfg(windows)]
fn looks_like_windows_drive_path(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() >= 3 && bytes[1] == b':' && (bytes[2] == b'\\' || bytes[2] == b'/')
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, MutexGuard};

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    struct EnvGuard {
        _guard: MutexGuard<'static, ()>,
        snow_env: Option<String>,
    }

    impl EnvGuard {
        fn new() -> Self {
            let guard = Self {
                _guard: ENV_LOCK.lock().expect("env lock"),
                snow_env: env::var("SNOW_ENV").ok(),
            };
            // SAFETY: tests serialize process-env mutation with ENV_LOCK.
            unsafe {
                env::remove_var("SNOW_ENV");
            }
            guard
        }

        fn set_snow_env(&self, value: &str) {
            // SAFETY: tests serialize process-env mutation with ENV_LOCK.
            unsafe {
                env::set_var("SNOW_ENV", value);
            }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            // SAFETY: tests serialize process-env mutation with ENV_LOCK.
            unsafe {
                match &self.snow_env {
                    Some(value) => env::set_var("SNOW_ENV", value),
                    None => env::remove_var("SNOW_ENV"),
                }
            }
        }
    }

    #[test]
    fn exe_dir_snow_config_overrides_platform_default() {
        let temp = tempfile::tempdir().expect("tempdir");
        let exe_dir = temp.path().join("bin");
        let config_dir = exe_dir.join("snow_config");
        std::fs::create_dir_all(&config_dir).expect("config dir");

        let resolver = ConfigDirResolver::new(
            Some(exe_dir),
            Some(temp.path().join("home")),
            Some(temp.path().join("appdata")),
            temp.path().join("cwd"),
        );

        assert_eq!(resolver.resolve(), config_dir);
    }

    #[cfg(unix)]
    #[test]
    fn unix_config_dir_uses_home_dot_config_snow() {
        let temp = tempfile::tempdir().expect("tempdir");
        let home = temp.path().join("home");
        let cwd = temp.path().join("cwd");
        let resolver = ConfigDirResolver::new(None, Some(home.clone()), None, cwd);

        assert_eq!(resolver.resolve(), home.join(".config").join("snow"));
    }

    #[cfg(windows)]
    #[test]
    fn windows_config_dir_uses_appdata_snow() {
        let temp = tempfile::tempdir().expect("tempdir");
        let home = temp.path().join("home");
        let appdata = temp.path().join("AppData").join("Roaming");
        let cwd = temp.path().join("cwd");
        let resolver = ConfigDirResolver::new(None, Some(home), Some(appdata.clone()), cwd);

        assert_eq!(resolver.resolve(), appdata.join("snow"));
    }

    #[test]
    fn config_dir_falls_back_to_cwd_dot_snow() {
        let temp = tempfile::tempdir().expect("tempdir");
        let cwd = temp.path().join("cwd");
        let resolver = ConfigDirResolver::new(None, None, None, cwd.clone());

        assert_eq!(resolver.resolve(), cwd.join(".snow"));
    }

    #[cfg(unix)]
    #[test]
    fn unix_endpoint_uses_config_dir_daemon_sock() {
        let config_dir = PathBuf::from("/tmp/snow-test");
        let endpoint = IpcEndpoint::for_config_dir(&config_dir);

        assert_eq!(
            endpoint.filesystem_path(),
            Some(config_dir.join(DAEMON_SOCKET_FILENAME).as_path())
        );
        assert_eq!(
            endpoint.status_string(),
            config_dir
                .join(DAEMON_SOCKET_FILENAME)
                .display()
                .to_string()
        );
    }

    #[cfg(windows)]
    #[test]
    fn windows_endpoint_uses_stable_config_dir_pipe_scope() {
        let endpoint = IpcEndpoint::for_config_dir(r"C:\Users\jared\AppData\Roaming\snow");
        let same_endpoint = IpcEndpoint::for_config_dir(r"c:\users\jared\appdata\roaming\snow");
        let other_endpoint =
            IpcEndpoint::for_config_dir(r"C:\Users\jared\AppData\Roaming\snow-alt");

        assert_eq!(endpoint, same_endpoint);
        assert_ne!(endpoint, other_endpoint);
        assert!(endpoint.filesystem_path().is_none());
        assert!(
            endpoint
                .status_string()
                .starts_with(r"\\.\pipe\snow-daemon-v1-")
        );
    }

    #[test]
    fn env_label_does_not_change_endpoint_identity() {
        let env = EnvGuard::new();
        let config_dir = PathBuf::from("/tmp/snow-config-root");

        env.set_snow_env("test");
        let test_endpoint = IpcEndpoint::for_config_dir(&config_dir);

        env.set_snow_env("prd");
        let prd_endpoint = IpcEndpoint::for_config_dir(&config_dir);

        assert_eq!(test_endpoint, prd_endpoint);
    }

    #[test]
    fn parse_preserves_legacy_filesystem_endpoint_prefix() {
        assert_eq!(
            IpcEndpoint::parse("fs:/tmp/snow.sock"),
            IpcEndpoint::filesystem("/tmp/snow.sock")
        );
    }

    #[test]
    fn parse_supports_explicit_namespaced_endpoint_prefix() {
        assert_eq!(
            IpcEndpoint::parse("ns:snow-daemon-test"),
            IpcEndpoint::namespaced("snow-daemon-test")
        );
    }
}
