use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Sql(#[from] rusqlite::Error),
    #[error("invalid params: {0}")]
    InvalidParams(String),
    #[error("policy denied: {0}")]
    PolicyDenied(String),
    #[error("not implemented: {0}")]
    NotImplemented(String),
    #[error("daemon request failed: {message}")]
    DaemonJsonRpc {
        code: i64,
        message: String,
        data: Option<serde_json::Value>,
    },
}
