use chrono::Utc;
use rusqlite::{Connection, params};
use serde_json::Value;

use crate::audit::chain::{AuditChainBreak, AuditChainRange, compute_row_hash, verify_chain_range};
use crate::domain::audit::AuditEvent;
use crate::{Error, Result};

#[async_trait::async_trait(?Send)]
pub trait AuditSink {
    async fn append(&self, event: AuditEvent) -> Result<AuditEvent>;
    async fn get(&self, audit_id: &str) -> Result<Option<AuditEvent>>;
    async fn list(&self, limit: usize) -> Result<Vec<AuditEvent>>;
    async fn verify_chain(&self, range: AuditChainRange) -> Result<Vec<AuditChainBreak>>;
}

pub struct SqliteAuditSink {
    conn: Connection,
}

impl SqliteAuditSink {
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let conn = Connection::open(path)?;
        let sink = Self { conn };
        sink.bootstrap()?;
        Ok(sink)
    }

    pub fn open_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        let sink = Self { conn };
        sink.bootstrap()?;
        Ok(sink)
    }

    pub fn in_memory() -> Result<Self> {
        Self::open_memory()
    }

    fn bootstrap(&self) -> Result<()> {
        self.conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS mcp_audit_events (
              audit_id TEXT PRIMARY KEY,
              timestamp TEXT NOT NULL,
              prev_hash TEXT NOT NULL,
              row_hash TEXT NOT NULL,
              event_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_mcp_audit_events_timestamp
              ON mcp_audit_events(timestamp);
            "#,
        )?;
        Ok(())
    }

    fn last_hash(&self) -> Result<String> {
        let mut stmt = self.conn.prepare(
            "SELECT row_hash FROM mcp_audit_events ORDER BY timestamp DESC, audit_id DESC LIMIT 1",
        )?;
        let hash = stmt.query_row([], |row| row.get::<_, String>(0));
        match hash {
            Ok(hash) => Ok(hash),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok("GENESIS".to_string()),
            Err(err) => Err(err.into()),
        }
    }
}

impl SqliteAuditSink {
    fn list_in_range(&self, range: &AuditChainRange) -> Result<Vec<AuditEvent>> {
        let mut stmt = self.conn.prepare(
            "SELECT event_json FROM mcp_audit_events ORDER BY timestamp ASC, audit_id ASC",
        )?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        let mut events = Vec::new();
        let mut include = range.from_audit_id.is_none();
        for row in rows {
            let event = serde_json::from_str::<AuditEvent>(&row?)?;
            if range
                .from_audit_id
                .as_ref()
                .is_some_and(|from| from == &event.audit_id)
            {
                include = true;
            }
            if include {
                let is_to = range
                    .to_audit_id
                    .as_ref()
                    .is_some_and(|to| to == &event.audit_id);
                events.push(event);
                if is_to {
                    break;
                }
            }
        }
        Ok(events)
    }

    pub fn tamper_raw_arguments_for_test(&self, audit_id: &str, payload: &Value) -> Result<()> {
        let Some(mut event) = self.get_blocking(audit_id)? else {
            return Err(Error::InvalidParams(format!(
                "audit event {audit_id} not found"
            )));
        };
        event.raw_arguments_redacted = payload.clone();
        let event_json = serde_json::to_string(&event)?;
        self.conn.execute(
            "UPDATE mcp_audit_events SET event_json = ?1 WHERE audit_id = ?2",
            params![event_json, audit_id],
        )?;
        Ok(())
    }

    fn get_blocking(&self, audit_id: &str) -> Result<Option<AuditEvent>> {
        let mut stmt = self
            .conn
            .prepare("SELECT event_json FROM mcp_audit_events WHERE audit_id = ?1")?;
        let result = stmt.query_row([audit_id], |row| row.get::<_, String>(0));
        match result {
            Ok(json) => Ok(Some(serde_json::from_str(&json)?)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(err) => Err(Error::from(err)),
        }
    }
}

#[async_trait::async_trait(?Send)]
impl AuditSink for SqliteAuditSink {
    async fn append(&self, mut event: AuditEvent) -> Result<AuditEvent> {
        if event.timestamp.timestamp() == 0 {
            event.timestamp = Utc::now();
        }
        let prev_hash = self.last_hash()?;
        event.prev_hash = prev_hash.clone();
        let value = serde_json::to_value(&event)?;
        event.row_hash = compute_row_hash(&prev_hash, &value);
        let event_json = serde_json::to_string(&event)?;
        self.conn.execute(
            "INSERT INTO mcp_audit_events (audit_id, timestamp, prev_hash, row_hash, event_json) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![event.audit_id, event.timestamp.to_rfc3339(), event.prev_hash, event.row_hash, event_json],
        )?;
        Ok(event)
    }

    async fn get(&self, audit_id: &str) -> Result<Option<AuditEvent>> {
        self.get_blocking(audit_id)
    }

    async fn list(&self, limit: usize) -> Result<Vec<AuditEvent>> {
        let mut stmt = self.conn.prepare(
            "SELECT event_json FROM mcp_audit_events ORDER BY timestamp ASC, audit_id ASC LIMIT ?1",
        )?;
        let rows = stmt.query_map([limit as i64], |row| row.get::<_, String>(0))?;
        let mut events = Vec::new();
        for row in rows {
            events.push(serde_json::from_str::<AuditEvent>(&row?)?);
        }
        Ok(events)
    }

    async fn verify_chain(&self, range: AuditChainRange) -> Result<Vec<AuditChainBreak>> {
        let events = self.list_in_range(&range)?;
        Ok(match verify_chain_range(&events) {
            Ok(()) => Vec::new(),
            Err(chain_break) => vec![chain_break],
        })
    }
}
