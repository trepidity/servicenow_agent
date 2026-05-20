//! Tab routing for the admin TUI.

pub mod cache_vault;
pub mod config;
pub mod daemon;
pub mod sync;

/// The four top-level tabs of the admin TUI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(usize)]
pub enum Tab {
    Daemon = 0,
    Sync = 1,
    CacheVault = 2,
    Config = 3,
}

impl Tab {
    /// Cycle to the next tab (wrapping).
    pub fn next(self) -> Self {
        match self {
            Tab::Daemon => Tab::Sync,
            Tab::Sync => Tab::CacheVault,
            Tab::CacheVault => Tab::Config,
            Tab::Config => Tab::Daemon,
        }
    }

    /// Cycle to the previous tab (wrapping).
    pub fn prev(self) -> Self {
        match self {
            Tab::Daemon => Tab::Config,
            Tab::Sync => Tab::Daemon,
            Tab::CacheVault => Tab::Sync,
            Tab::Config => Tab::CacheVault,
        }
    }
}
