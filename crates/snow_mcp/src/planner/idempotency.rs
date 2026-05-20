use chrono::{DateTime, Duration, Utc};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::path::Path;

use crate::Result;
use crate::domain::primitives::IdempotencyKey;
use crate::planner::operation_plan::OperationReceipt;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IdempotencyRecord {
    pub key: String,
    pub op_hash: String,
    pub tool: String,
    pub receipt: Option<OperationReceipt>,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub apply_started_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum IdempotencyOutcome {
    NewKey,
    Pending { retryable: bool, transient: bool },
    // Boxed: `OperationReceipt` is far larger than the other variants, so
    // indirection keeps `IdempotencyOutcome` small (clippy::large_enum_variant).
    Replay(Box<OperationReceipt>),
    Conflict { reason: String },
}

#[async_trait::async_trait(?Send)]
pub trait IdempotencyStore {
    async fn check_and_record(
        &self,
        key: &IdempotencyKey,
        tool: &str,
        op_hash: &str,
        window_seconds: u64,
    ) -> Result<IdempotencyOutcome>;

    async fn save_receipt(&self, key: &IdempotencyKey, receipt: &OperationReceipt) -> Result<()>;

    async fn lookup_receipt(
        &self,
        key: &IdempotencyKey,
        tool: &str,
        op_hash: &str,
    ) -> Result<Option<OperationReceipt>>;
}

pub struct SqliteIdempotencyStore {
    conn: Connection,
}

impl SqliteIdempotencyStore {
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
            CREATE TABLE IF NOT EXISTS mcp_idempotency (
              key TEXT NOT NULL,
              tool TEXT NOT NULL,
              op_hash TEXT NOT NULL,
              receipt_json TEXT,
              created_at TEXT NOT NULL,
              expires_at TEXT NOT NULL,
              apply_started_at TEXT,
              PRIMARY KEY (key, tool)
            );
            CREATE INDEX IF NOT EXISTS idx_mcp_idempotency_expires
              ON mcp_idempotency(expires_at);
            "#,
        )?;
        self.add_column_if_missing("mcp_idempotency", "apply_started_at", "TEXT")?;
        Ok(())
    }

    fn get_record(&self, key: &IdempotencyKey, tool: &str) -> Result<Option<IdempotencyRecord>> {
        self.conn
            .query_row(
                "SELECT key, tool, op_hash, receipt_json, created_at, expires_at, apply_started_at FROM mcp_idempotency WHERE key = ?1 AND tool = ?2",
                params![key.value, tool],
                |row| {
                    let receipt_json: Option<String> = row.get(3)?;
                    let created_at: String = row.get(4)?;
                    let expires_at: String = row.get(5)?;
                    let apply_started_at: Option<String> = row.get(6)?;
                    Ok(IdempotencyRecord {
                        key: row.get(0)?,
                        tool: row.get(1)?,
                        op_hash: row.get(2)?,
                        receipt: receipt_json
                            .map(|value| serde_json::from_str::<OperationReceipt>(&value))
                            .transpose()
                            .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
                        created_at: DateTime::parse_from_rfc3339(&created_at)
                            .map(|dt| dt.with_timezone(&Utc))
                            .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
                        expires_at: DateTime::parse_from_rfc3339(&expires_at)
                            .map(|dt| dt.with_timezone(&Utc))
                            .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
                        apply_started_at: apply_started_at
                            .map(|value| DateTime::parse_from_rfc3339(&value))
                            .transpose()
                            .map(|value| value.map(|dt| dt.with_timezone(&Utc)))
                            .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn lookup_record(
        &self,
        key: &IdempotencyKey,
        tool: &str,
    ) -> Result<Option<IdempotencyRecord>> {
        self.get_record(key, tool)
    }

    pub async fn mark_apply_started(&self, key: &IdempotencyKey, tool: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE mcp_idempotency SET apply_started_at = COALESCE(apply_started_at, ?1) WHERE key = ?2 AND tool = ?3",
            params![Utc::now().to_rfc3339(), key.value, tool],
        )?;
        Ok(())
    }

    fn add_column_if_missing(&self, table: &str, column: &str, definition: &str) -> Result<()> {
        if self.table_has_column(table, column)? {
            return Ok(());
        }
        self.conn.execute(
            &format!("ALTER TABLE {table} ADD COLUMN {column} {definition}"),
            [],
        )?;
        Ok(())
    }

    fn table_has_column(&self, table: &str, column: &str) -> Result<bool> {
        let mut stmt = self.conn.prepare(&format!("PRAGMA table_info({table})"))?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
        for name in rows {
            if name? == column {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

#[async_trait::async_trait(?Send)]
impl IdempotencyStore for SqliteIdempotencyStore {
    async fn check_and_record(
        &self,
        key: &IdempotencyKey,
        tool: &str,
        op_hash: &str,
        window_seconds: u64,
    ) -> Result<IdempotencyOutcome> {
        let now = Utc::now();
        if let Some(record) = self.get_record(key, tool)? {
            if record.expires_at <= now {
                self.conn.execute(
                    "DELETE FROM mcp_idempotency WHERE key = ?1 AND tool = ?2",
                    params![key.value, tool],
                )?;
            } else if record.op_hash != op_hash {
                return Ok(IdempotencyOutcome::Conflict {
                    reason: "idempotency key reused with different op_hash".to_string(),
                });
            } else if let Some(mut receipt) = record.receipt {
                receipt.idempotency_replay = true;
                return Ok(IdempotencyOutcome::Replay(Box::new(receipt)));
            } else {
                return Ok(IdempotencyOutcome::Pending {
                    retryable: true,
                    transient: true,
                });
            }
        }

        let expires_at = now + Duration::seconds(window_seconds as i64);
        self.conn.execute(
            "INSERT OR REPLACE INTO mcp_idempotency (key, tool, op_hash, receipt_json, created_at, expires_at) VALUES (?1, ?2, ?3, NULL, ?4, ?5)",
            params![
                key.value,
                tool,
                op_hash,
                now.to_rfc3339(),
                expires_at.to_rfc3339()
            ],
        )?;
        Ok(IdempotencyOutcome::NewKey)
    }

    async fn save_receipt(&self, key: &IdempotencyKey, receipt: &OperationReceipt) -> Result<()> {
        self.conn.execute(
            "UPDATE mcp_idempotency SET receipt_json = ?1 WHERE key = ?2 AND tool = ?3",
            params![serde_json::to_string(receipt)?, key.value, receipt.tool],
        )?;
        Ok(())
    }

    async fn lookup_receipt(
        &self,
        key: &IdempotencyKey,
        tool: &str,
        op_hash: &str,
    ) -> Result<Option<OperationReceipt>> {
        let mut stmt = self
            .conn
            .prepare("SELECT receipt_json FROM mcp_idempotency WHERE key = ?1 AND tool = ?2 AND op_hash = ?3")?;
        let receipt_json = stmt
            .query_row([key.value.as_str(), tool, op_hash], |row| {
                row.get::<_, Option<String>>(0)
            })
            .optional()?
            .flatten();
        receipt_json
            .map(|value| serde_json::from_str::<OperationReceipt>(&value).map_err(Into::into))
            .transpose()
    }
}
