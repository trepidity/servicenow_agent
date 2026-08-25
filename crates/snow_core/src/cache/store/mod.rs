use super::policy::stable_reference_ttl;
use crate::{FieldValue, KnowledgeEmbeddingCoverage, ResourceType, SnowRecord};
use chrono::{DateTime, Duration, Utc};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::path::{Path, PathBuf};
pub const CACHE_FORMAT_ID: &str = "snow-cache-v2";

mod business_applications;
mod catalog_products;
mod error;
mod helpers;
mod knowledge;
mod models;
mod records;
mod relationships;
mod schema;
mod sync;
#[cfg(test)]
mod tests;
mod users;

pub use error::{Result, StoreError};
use helpers::*;
pub use models::*;

pub struct Store {
    path: PathBuf,
    conn: Connection,
}

impl fmt::Debug for Store {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Store").field("path", &self.path).finish()
    }
}

impl Store {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let in_memory = path == Path::new(":memory:");
        let existed = !in_memory && path.exists();
        if !in_memory
            && !existed
            && let Some(parent) = path.parent()
        {
            std::fs::create_dir_all(parent)?;
        }

        if existed {
            match Self::inspect_format(path)? {
                CacheFormat::Current => {}
                CacheFormat::Absent => unreachable!("an existing cache cannot be absent"),
                CacheFormat::Incompatible { found } => {
                    return Err(StoreError::IncompatibleCacheFormat {
                        found,
                        expected: CACHE_FORMAT_ID.to_string(),
                    });
                }
            }
        }

        let conn = if in_memory {
            Connection::open_in_memory()?
        } else {
            Connection::open(path)?
        };

        let store = Self {
            path: path.to_path_buf(),
            conn,
        };
        if existed {
            store.configure_connection()?;
            store.migrate_schema()?;
        } else {
            store.bootstrap_new()?;
        }
        Ok(store)
    }

    pub fn open_in_memory() -> Result<Self> {
        Self::open(":memory:")
    }

    pub fn inspect_format(path: impl AsRef<Path>) -> Result<CacheFormat> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(CacheFormat::Absent);
        }
        let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        let marker = conn
            .query_row(
                "SELECT value FROM schema_meta WHERE key = 'cache_format'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional();
        match marker {
            Ok(Some(marker)) if marker == CACHE_FORMAT_ID => {
                let projection_tables = conn.query_row(
                    r#"
                    SELECT COUNT(*)
                    FROM sqlite_master
                    WHERE type = 'table'
                      AND name IN ('catalog_products_complete', 'catalog_products_narrowed')
                    "#,
                    [],
                    |row| row.get::<_, i64>(0),
                )?;
                if projection_tables == 2 {
                    Ok(CacheFormat::Current)
                } else {
                    Ok(CacheFormat::Incompatible {
                        found: format!("{CACHE_FORMAT_ID} missing typed catalog projection"),
                    })
                }
            }
            Ok(Some(marker)) => Ok(CacheFormat::Incompatible {
                found: format!("cache format marker {marker:?}"),
            }),
            Ok(None) => Ok(CacheFormat::Incompatible {
                found: "missing cache format marker".to_string(),
            }),
            Err(rusqlite::Error::SqliteFailure(_, Some(message)))
                if message.contains("no such table") =>
            {
                Ok(CacheFormat::Incompatible {
                    found: "missing schema metadata".to_string(),
                })
            }
            Err(error) => Err(StoreError::Sqlite(error)),
        }
    }

    fn configure_connection(&self) -> Result<()> {
        self.conn.pragma_update(None, "foreign_keys", "ON")?;
        self.conn.pragma_update(None, "synchronous", "NORMAL")?;
        Ok(())
    }

    fn migrate_schema(&self) -> Result<()> {
        let mut statement = self.conn.prepare("PRAGMA table_info(records)")?;
        let columns = statement
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<std::result::Result<BTreeSet<_>, _>>()?;
        if !columns.contains("vault_provenance") {
            self.conn.execute_batch(
                "ALTER TABLE records ADD COLUMN vault_provenance TEXT NOT NULL DEFAULT 'legacy_unknown' CHECK (vault_provenance IN ('vault_backed', 'cache_only', 'legacy_unknown'));",
            )?;
        }
        Ok(())
    }

    fn bootstrap_new(&self) -> Result<()> {
        self.configure_connection()?;
        self.conn.pragma_update(None, "journal_mode", "WAL")?;
        self.conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS schema_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            "#,
        )?;
        self.create_schema_objects()?;
        self.conn.execute(
            "INSERT INTO schema_meta(key, value) VALUES ('cache_format', ?1)",
            [CACHE_FORMAT_ID],
        )?;
        Ok(())
    }

    pub fn get_meta_value(&self, key: &str) -> Result<Option<String>> {
        self.conn
            .query_row(
                "SELECT value FROM schema_meta WHERE key = ?1",
                params![key],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(Into::into)
    }

    #[cfg(test)]
    fn table_exists(&self, table: &str) -> Result<bool> {
        let exists = self.conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type IN ('table', 'view') AND name = ?1",
            [table],
            |row| row.get::<_, i64>(0),
        )?;
        Ok(exists > 0)
    }

    #[cfg(test)]
    fn index_exists(&self, index: &str) -> Result<bool> {
        let exists = self.conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?1",
            [index],
            |row| row.get::<_, i64>(0),
        )?;
        Ok(exists > 0)
    }

    pub fn set_meta_value(&self, key: &str, value: Option<&str>) -> Result<()> {
        match value {
            Some(value) => {
                self.conn.execute(
                    "INSERT INTO schema_meta(key, value) VALUES (?1, ?2)
                     ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                    params![key, value],
                )?;
            }
            None => {
                self.conn
                    .execute("DELETE FROM schema_meta WHERE key = ?1", params![key])?;
            }
        }
        Ok(())
    }
}
