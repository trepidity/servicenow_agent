use std::path::Path;

use chrono::{DateTime, Utc};
use rusqlite::types::Type;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::Result;
use crate::planner::operation_plan::ConcurrencyToken;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PlanStoreRecord {
    pub plan_id: String,
    pub tool: String,
    pub actor: String,
    pub op_hash: String,
    pub plan_json: Value,
    pub concurrency_token: Option<ConcurrencyToken>,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub state: PlanLifecycleState,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PlanLifecycleState {
    Pending,
    Consumed,
    Expired,
}

impl PlanLifecycleState {
    fn as_storage(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Consumed => "consumed",
            Self::Expired => "expired",
        }
    }

    fn from_storage(value: &str) -> std::result::Result<Self, String> {
        match value {
            "pending" => Ok(Self::Pending),
            "consumed" => Ok(Self::Consumed),
            "expired" => Ok(Self::Expired),
            other => Err(format!("unknown plan lifecycle state `{other}`")),
        }
    }
}

#[async_trait::async_trait(?Send)]
pub trait PlanStore {
    async fn put(&self, record: PlanStoreRecord) -> Result<()>;
    async fn get(&self, plan_id: &str) -> Result<Option<PlanStoreRecord>>;
    async fn mark_consumed(&self, plan_id: &str) -> Result<()>;
    async fn mark_expired(&self, plan_id: &str) -> Result<()>;
    async fn sweep_expired(&self, now: DateTime<Utc>) -> Result<u64>;
}

pub struct SqlitePlanStore {
    conn: Connection,
}

impl SqlitePlanStore {
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
            CREATE TABLE IF NOT EXISTS mcp_plans (
              plan_id TEXT PRIMARY KEY,
              tool TEXT NOT NULL,
              actor TEXT NOT NULL,
              op_hash TEXT NOT NULL,
              plan_json TEXT NOT NULL,
              concurrency_token TEXT,
              created_at TEXT NOT NULL,
              expires_at TEXT NOT NULL,
              state TEXT NOT NULL DEFAULT 'pending'
            );
            CREATE INDEX IF NOT EXISTS idx_mcp_plans_expires
              ON mcp_plans(expires_at);
            CREATE INDEX IF NOT EXISTS idx_mcp_plans_actor
              ON mcp_plans(actor);
            CREATE INDEX IF NOT EXISTS idx_mcp_plans_op_hash
              ON mcp_plans(op_hash);
            "#,
        )?;
        Ok(())
    }
}

#[async_trait::async_trait(?Send)]
impl PlanStore for SqlitePlanStore {
    async fn put(&self, record: PlanStoreRecord) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO mcp_plans (plan_id, tool, actor, op_hash, plan_json, concurrency_token, created_at, expires_at, state) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                record.plan_id,
                record.tool,
                record.actor,
                record.op_hash,
                serde_json::to_string(&record.plan_json)?,
                record
                    .concurrency_token
                    .as_ref()
                    .map(serde_json::to_string)
                    .transpose()?,
                record.created_at.to_rfc3339(),
                record.expires_at.to_rfc3339(),
                record.state.as_storage(),
            ],
        )?;
        Ok(())
    }

    async fn get(&self, plan_id: &str) -> Result<Option<PlanStoreRecord>> {
        self.conn
            .query_row(
                "SELECT plan_id, tool, actor, op_hash, plan_json, concurrency_token, created_at, expires_at, state FROM mcp_plans WHERE plan_id = ?1",
                [plan_id],
                |row| {
                    let plan_json: String = row.get(4)?;
                    let concurrency_token: Option<String> = row.get(5)?;
                    let created_at: String = row.get(6)?;
                    let expires_at: String = row.get(7)?;
                    let state: String = row.get(8)?;
                    Ok(PlanStoreRecord {
                        plan_id: row.get(0)?,
                        tool: row.get(1)?,
                        actor: row.get(2)?,
                        op_hash: row.get(3)?,
                        plan_json: serde_json::from_str(&plan_json).map_err(to_sql_conversion_failure)?,
                        concurrency_token: concurrency_token
                            .map(|value| serde_json::from_str::<ConcurrencyToken>(&value))
                            .transpose()
                            .map_err(to_sql_conversion_failure)?,
                        created_at: DateTime::parse_from_rfc3339(&created_at)
                            .map(|dt| dt.with_timezone(&Utc))
                            .map_err(to_sql_conversion_failure)?,
                        expires_at: DateTime::parse_from_rfc3339(&expires_at)
                            .map(|dt| dt.with_timezone(&Utc))
                            .map_err(to_sql_conversion_failure)?,
                        state: PlanLifecycleState::from_storage(&state).map_err(|err| {
                            to_sql_conversion_failure(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                err,
                            ))
                        })?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    async fn mark_consumed(&self, plan_id: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE mcp_plans SET state = 'consumed' WHERE plan_id = ?1",
            [plan_id],
        )?;
        Ok(())
    }

    async fn mark_expired(&self, plan_id: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE mcp_plans SET state = 'expired' WHERE plan_id = ?1 AND state != 'consumed'",
            [plan_id],
        )?;
        Ok(())
    }

    async fn sweep_expired(&self, now: DateTime<Utc>) -> Result<u64> {
        let changed = self.conn.execute(
            "UPDATE mcp_plans SET state = 'expired' WHERE state = 'pending' AND expires_at <= ?1",
            [now.to_rfc3339()],
        )?;
        Ok(changed as u64)
    }
}

fn to_sql_conversion_failure<E>(err: E) -> rusqlite::Error
where
    E: std::error::Error + Send + Sync + 'static,
{
    rusqlite::Error::FromSqlConversionFailure(0, Type::Text, Box::new(err))
}

#[cfg(test)]
mod tests {
    use chrono::Duration;
    use serde_json::json;

    use super::*;

    fn record(plan_id: &str, expires_at: DateTime<Utc>) -> PlanStoreRecord {
        let created_at = Utc::now();
        PlanStoreRecord {
            plan_id: plan_id.to_string(),
            tool: "story_plan_create".to_string(),
            actor: "actor-1".to_string(),
            op_hash: "hash-1".to_string(),
            plan_json: json!({
                "plan_id": plan_id,
                "tool": "story_plan_create",
                "op_hash": "hash-1",
            }),
            concurrency_token: Some(ConcurrencyToken {
                sys_updated_on: "2026-05-19 12:00:00".to_string(),
                sys_mod_count: Some(3),
            }),
            created_at,
            expires_at,
            state: PlanLifecycleState::Pending,
        }
    }

    #[tokio::test]
    async fn plan_store_round_trips_record() {
        let store = SqlitePlanStore::in_memory().expect("store");
        let expected = record("plan-1", Utc::now() + Duration::minutes(10));

        store.put(expected.clone()).await.expect("put");
        let actual = store.get("plan-1").await.expect("get").expect("record");

        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn plan_store_marks_and_sweeps_expired_records() {
        let store = SqlitePlanStore::in_memory().expect("store");
        store
            .put(record("old-plan", Utc::now() - Duration::seconds(1)))
            .await
            .expect("put old");
        store
            .put(record("new-plan", Utc::now() + Duration::minutes(10)))
            .await
            .expect("put new");

        let expired = store.sweep_expired(Utc::now()).await.expect("sweep");
        assert_eq!(expired, 1);
        assert_eq!(
            store
                .get("old-plan")
                .await
                .expect("get old")
                .expect("old")
                .state,
            PlanLifecycleState::Expired
        );
        assert_eq!(
            store
                .get("new-plan")
                .await
                .expect("get new")
                .expect("new")
                .state,
            PlanLifecycleState::Pending
        );

        store.mark_expired("new-plan").await.expect("mark expired");
        assert_eq!(
            store
                .get("new-plan")
                .await
                .expect("get new")
                .expect("new")
                .state,
            PlanLifecycleState::Expired
        );

        store
            .put(record("consumed-plan", Utc::now() + Duration::minutes(10)))
            .await
            .expect("put consumed");
        store
            .mark_consumed("consumed-plan")
            .await
            .expect("mark consumed");
        store
            .mark_expired("consumed-plan")
            .await
            .expect("mark expired consumed");
        assert_eq!(
            store
                .get("consumed-plan")
                .await
                .expect("get consumed")
                .expect("consumed")
                .state,
            PlanLifecycleState::Consumed
        );
    }

    #[tokio::test]
    async fn plan_store_missing_get_returns_none() {
        let store = SqlitePlanStore::in_memory().expect("store");
        assert!(store.get("missing-plan").await.expect("get").is_none());
    }
}
