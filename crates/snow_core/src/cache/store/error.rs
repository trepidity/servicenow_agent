use thiserror::Error;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid resource type: {0}")]
    InvalidResourceType(String),
    #[error("invalid schema version: {0}")]
    InvalidSchemaVersion(String),
    #[error(
        "incompatible cache format: found {found}; expected {expected}. Run `snow rebuild-cache` to replace the disposable cache"
    )]
    IncompatibleCacheFormat { found: String, expected: String },
    #[error("invalid query: {0}")]
    InvalidQuery(String),
    #[error("invalid timestamp: {0}")]
    InvalidTimestamp(i64),
    #[error("serde json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid embedding vector length: expected {expected} bytes, got {actual}")]
    InvalidEmbeddingVectorLength { expected: usize, actual: usize },
    #[error("embedding vector was not unit-length")]
    NonUnitEmbeddingVector,
}

pub type Result<T> = std::result::Result<T, StoreError>;
