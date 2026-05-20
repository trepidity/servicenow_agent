use chrono::{DateTime, Duration, Utc};
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use std::path::Path;
use uuid::Uuid;

use crate::Result;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConfirmationRecord {
    pub token_id: String,
    pub plan_id: String,
    pub actor: String,
    pub requester: String,
    pub tool: String,
    pub op_hash: String,
    pub environment: String,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub consumed: bool,
    pub revoked: bool,
}

pub type ConfirmationToken = ConfirmationRecord;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfirmationBinding {
    pub actor: String,
    pub requester: String,
    pub tool: String,
    pub op_hash: String,
    pub environment: String,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ConfirmationConsumeError {
    #[error("token not found")]
    NotFound,
    #[error("token expired")]
    Expired,
    #[error("token already consumed")]
    AlreadyConsumed,
    #[error("token revoked")]
    Revoked,
    #[error("binding mismatch on field `{field}`")]
    BindingMismatch { field: &'static str },
}

#[async_trait::async_trait(?Send)]
pub trait ConfirmationStore {
    async fn issue(
        &self,
        plan_id: &str,
        binding: ConfirmationBinding,
        ttl_seconds: u64,
    ) -> Result<ConfirmationToken>;

    async fn consume(
        &self,
        token_id: &str,
        binding: &ConfirmationBinding,
    ) -> std::result::Result<ConfirmationToken, ConfirmationConsumeError>;
}

pub struct SqliteConfirmationStore {
    conn: Connection,
}

impl SqliteConfirmationStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let conn = Connection::open(path)?;
        let store = Self { conn };
        store.bootstrap()?;
        Ok(store)
    }

    pub fn open_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        let store = Self { conn };
        store.bootstrap()?;
        Ok(store)
    }

    pub fn in_memory() -> Result<Self> {
        Self::open_memory()
    }

    fn bootstrap(&self) -> Result<()> {
        self.conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS mcp_confirmations (
              token_id TEXT PRIMARY KEY,
              plan_id TEXT NOT NULL,
              actor TEXT NOT NULL,
              requester TEXT NOT NULL,
              tool TEXT NOT NULL,
              op_hash TEXT NOT NULL,
              environment TEXT NOT NULL,
              issued_at TEXT NOT NULL,
              expires_at TEXT NOT NULL,
              consumed INTEGER NOT NULL DEFAULT 0,
              revoked INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_mcp_confirmations_expires
              ON mcp_confirmations(expires_at);
            "#,
        )?;
        Ok(())
    }

    fn find(&self, token_id: &str) -> Result<Option<ConfirmationRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT token_id, plan_id, actor, requester, tool, op_hash, environment, issued_at, expires_at, consumed, revoked FROM mcp_confirmations WHERE token_id = ?1",
        )?;
        let result = stmt.query_row([token_id], |row| {
            let issued_at: String = row.get(7)?;
            let expires_at: String = row.get(8)?;
            Ok(ConfirmationRecord {
                token_id: row.get(0)?,
                plan_id: row.get(1)?,
                actor: row.get(2)?,
                requester: row.get(3)?,
                tool: row.get(4)?,
                op_hash: row.get(5)?,
                environment: row.get(6)?,
                issued_at: DateTime::parse_from_rfc3339(&issued_at)
                    .map(|dt| dt.with_timezone(&Utc))
                    .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
                expires_at: DateTime::parse_from_rfc3339(&expires_at)
                    .map(|dt| dt.with_timezone(&Utc))
                    .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
                consumed: row.get::<_, i64>(9)? != 0,
                revoked: row.get::<_, i64>(10)? != 0,
            })
        });
        match result {
            Ok(record) => Ok(Some(record)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    pub fn lookup(&self, token_id: &str) -> Result<Option<ConfirmationRecord>> {
        self.find(token_id)
    }
}

#[async_trait::async_trait(?Send)]
impl ConfirmationStore for SqliteConfirmationStore {
    async fn issue(
        &self,
        plan_id: &str,
        binding: ConfirmationBinding,
        ttl_seconds: u64,
    ) -> Result<ConfirmationToken> {
        let issued_at = Utc::now();
        let record = ConfirmationRecord {
            token_id: Uuid::new_v4().to_string(),
            plan_id: plan_id.to_string(),
            actor: binding.actor,
            requester: binding.requester,
            tool: binding.tool,
            op_hash: binding.op_hash,
            environment: binding.environment,
            issued_at,
            expires_at: issued_at + Duration::seconds(ttl_seconds as i64),
            consumed: false,
            revoked: false,
        };
        self.conn.execute(
            "INSERT INTO mcp_confirmations VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0, 0)",
            params![
                record.token_id,
                record.plan_id,
                record.actor,
                record.requester,
                record.tool,
                record.op_hash,
                record.environment,
                record.issued_at.to_rfc3339(),
                record.expires_at.to_rfc3339()
            ],
        )?;
        Ok(record)
    }

    async fn consume(
        &self,
        token_id: &str,
        binding: &ConfirmationBinding,
    ) -> std::result::Result<ConfirmationToken, ConfirmationConsumeError> {
        let Some(mut record) = self
            .find(token_id)
            .map_err(|_| ConfirmationConsumeError::NotFound)?
        else {
            return Err(ConfirmationConsumeError::NotFound);
        };

        if record.revoked {
            return Err(ConfirmationConsumeError::Revoked);
        }
        if record.consumed {
            return Err(ConfirmationConsumeError::AlreadyConsumed);
        }
        if record.expires_at <= Utc::now() {
            return Err(ConfirmationConsumeError::Expired);
        }

        for (field, stored, incoming) in [
            ("actor", record.actor.as_str(), binding.actor.as_str()),
            (
                "requester",
                record.requester.as_str(),
                binding.requester.as_str(),
            ),
            ("tool", record.tool.as_str(), binding.tool.as_str()),
            ("op_hash", record.op_hash.as_str(), binding.op_hash.as_str()),
            (
                "environment",
                record.environment.as_str(),
                binding.environment.as_str(),
            ),
        ] {
            if stored != incoming {
                return Err(ConfirmationConsumeError::BindingMismatch { field });
            }
        }

        self.conn
            .execute(
                "UPDATE mcp_confirmations SET consumed = 1 WHERE token_id = ?1",
                [token_id],
            )
            .map_err(|_| ConfirmationConsumeError::NotFound)?;
        record.consumed = true;
        Ok(record)
    }
}
