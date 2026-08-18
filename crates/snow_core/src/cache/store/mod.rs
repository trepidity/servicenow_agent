use super::policy::stable_reference_ttl;
use crate::{FieldValue, KnowledgeEmbeddingCoverage, ResourceType, SnowRecord};
use chrono::{DateTime, Duration, Utc};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::path::{Path, PathBuf};
use thiserror::Error;

pub const CACHE_FORMAT_ID: &str = "snow-cache-v1";

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

mod helpers;
mod models;
mod schema;
#[cfg(test)]
mod tests;

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
            Ok(Some(marker)) if marker == CACHE_FORMAT_ID => Ok(CacheFormat::Current),
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

    pub fn upsert_record(&self, record: &RecordRow, work_notes: &str, content: &str) -> Result<()> {
        self.upsert_record_with_tags(record, work_notes, content, "")
    }

    pub fn upsert_record_with_tags(
        &self,
        record: &RecordRow,
        work_notes: &str,
        content: &str,
        tag_tokens: &str,
    ) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO records (
                sys_id, number, table_name, resource_type, state, short_desc, description,
                assigned_to, parent_id, file_path, synced_at, sys_updated_on, etag,
                in_scope, last_seen_at, tombstoned_at, pruned_at, raw_json
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7,
                ?8, ?9, ?10, ?11, ?12, ?13,
                ?14, ?15, ?16, ?17, ?18
            )
            ON CONFLICT(sys_id) DO UPDATE SET
                number = excluded.number,
                table_name = excluded.table_name,
                resource_type = excluded.resource_type,
                state = excluded.state,
                short_desc = excluded.short_desc,
                description = excluded.description,
                assigned_to = excluded.assigned_to,
                parent_id = excluded.parent_id,
                file_path = excluded.file_path,
                synced_at = excluded.synced_at,
                sys_updated_on = excluded.sys_updated_on,
                etag = excluded.etag,
                in_scope = excluded.in_scope,
                last_seen_at = excluded.last_seen_at,
                tombstoned_at = excluded.tombstoned_at,
                pruned_at = excluded.pruned_at,
                raw_json = excluded.raw_json
            "#,
            params![
                &record.sys_id,
                &record.number,
                &record.table_name,
                resource_type_to_str(&record.resource_type),
                &record.state,
                &record.short_desc,
                &record.description,
                &record.assigned_to,
                &record.parent_id,
                &record.file_path,
                to_ts(record.synced_at),
                to_ts(record.sys_updated_on),
                &record.etag,
                bool_to_i64(record.in_scope),
                to_ts(record.last_seen_at),
                opt_ts(record.tombstoned_at),
                opt_ts(record.pruned_at),
                &record.raw_json,
            ],
        )?;

        let rowid: i64 = self.conn.query_row(
            "SELECT rowid FROM records WHERE sys_id = ?1",
            params![&record.sys_id],
            |row| row.get(0),
        )?;

        self.conn
            .execute("DELETE FROM fts_records WHERE rowid = ?1", params![rowid])?;
        self.conn.execute(
            "INSERT INTO fts_records(rowid, number, short_desc, description, work_notes, content, tag_tokens)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                rowid,
                &record.number,
                record.short_desc.as_deref().unwrap_or(""),
                record.description.as_deref().unwrap_or(""),
                work_notes,
                content,
                tag_tokens,
            ],
        )?;
        Ok(())
    }

    /// Batch-upsert multiple records in a single SQLite transaction.
    ///
    /// Each entry is `(record_row, work_notes_text, fts_content, fts_tag_tokens)`.
    pub fn upsert_records(&self, entries: &[(&RecordRow, &str, &str, &str)]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        self.conn.execute_batch("BEGIN IMMEDIATE")?;
        let result = entries
            .iter()
            .try_for_each(|(row, work_notes, content, tag_tokens)| {
                self.upsert_record_with_tags(row, work_notes, content, tag_tokens)
            });
        match result {
            Ok(()) => {
                self.conn.execute_batch("COMMIT")?;
                Ok(())
            }
            Err(err) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(err)
            }
        }
    }

    pub fn get_primary_record_by_number(&self, number: &str) -> Result<Option<RecordRow>> {
        self.conn
            .query_row(
                r#"
                SELECT sys_id, number, table_name, resource_type, state, short_desc, description,
                       assigned_to, parent_id, file_path, synced_at, sys_updated_on, etag,
                       in_scope, last_seen_at, tombstoned_at, pruned_at, raw_json
                FROM records
                WHERE number = ?1
                ORDER BY CASE WHEN resource_type = 'sysapproval_approver' THEN 1 ELSE 0 END,
                         CASE WHEN in_scope = 1 THEN 0 ELSE 1 END,
                         synced_at DESC,
                         sys_id ASC
                LIMIT 1
                "#,
                params![number],
                row_to_record_row,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn get_record_by_number(&self, number: &str) -> Result<Option<RecordRow>> {
        self.get_primary_record_by_number(number)
    }

    pub fn get_record_by_number_and_type(
        &self,
        number: &str,
        resource_type: ResourceType,
    ) -> Result<Option<RecordRow>> {
        self.conn
            .query_row(
                r#"
                SELECT sys_id, number, table_name, resource_type, state, short_desc, description,
                       assigned_to, parent_id, file_path, synced_at, sys_updated_on, etag,
                       in_scope, last_seen_at, tombstoned_at, pruned_at, raw_json
                FROM records
                WHERE number = ?1
                  AND resource_type = ?2
                ORDER BY CASE WHEN in_scope = 1 THEN 0 ELSE 1 END,
                         synced_at DESC,
                         sys_id ASC
                LIMIT 1
                "#,
                params![number, resource_type_to_str(&resource_type)],
                row_to_record_row,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn get_record_by_sys_id(&self, sys_id: &str) -> Result<Option<RecordRow>> {
        self.conn
            .query_row(
                r#"
                SELECT sys_id, number, table_name, resource_type, state, short_desc, description,
                       assigned_to, parent_id, file_path, synced_at, sys_updated_on, etag,
                       in_scope, last_seen_at, tombstoned_at, pruned_at, raw_json
                FROM records
                WHERE sys_id = ?1
                "#,
                params![sys_id],
                row_to_record_row,
            )
            .optional()
            .map_err(Into::into)
    }

    /// Get the stored ETag for a record by its number, if any.
    pub fn get_etag_by_number(&self, number: &str) -> Result<Option<String>> {
        self.conn
            .query_row(
                "SELECT etag FROM records WHERE number = ?1 AND in_scope = 1 LIMIT 1",
                params![number],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()
            .map(|opt| opt.flatten())
            .map_err(Into::into)
    }

    pub fn list_active_records(
        &self,
        resource_type: Option<ResourceType>,
    ) -> Result<Vec<RecordRow>> {
        let mut stmt = match resource_type {
            Some(_) => self.conn.prepare(
                r#"
                SELECT sys_id, number, table_name, resource_type, state, short_desc, description,
                       assigned_to, parent_id, file_path, synced_at, sys_updated_on, etag,
                       in_scope, last_seen_at, tombstoned_at, pruned_at, raw_json
                FROM records
                WHERE in_scope = 1 AND resource_type = ?1
                ORDER BY number
                "#,
            )?,
            None => self.conn.prepare(
                r#"
                SELECT sys_id, number, table_name, resource_type, state, short_desc, description,
                       assigned_to, parent_id, file_path, synced_at, sys_updated_on, etag,
                       in_scope, last_seen_at, tombstoned_at, pruned_at, raw_json
                FROM records
                WHERE in_scope = 1
                ORDER BY number
                "#,
            )?,
        };

        let rows = match resource_type {
            Some(kind) => {
                stmt.query_map(params![resource_type_to_str(&kind)], row_to_record_row)?
            }
            None => stmt.query_map([], row_to_record_row)?,
        };
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    pub fn list_records_by_resource_type(
        &self,
        resource_type: ResourceType,
    ) -> Result<Vec<RecordRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT sys_id, number, table_name, resource_type, state, short_desc, description,
                   assigned_to, parent_id, file_path, synced_at, sys_updated_on, etag,
                   in_scope, last_seen_at, tombstoned_at, pruned_at, raw_json
            FROM records
            WHERE resource_type = ?1
            ORDER BY number
            "#,
        )?;
        let rows = stmt.query_map(
            params![resource_type_to_str(&resource_type)],
            row_to_record_row,
        )?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    /// List active records with LIMIT and OFFSET for pagination.
    pub fn list_active_records_paginated(
        &self,
        resource_type: Option<ResourceType>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<RecordRow>> {
        let (sql, bind_resource_type) = match resource_type {
            Some(ref kind) => (
                r#"
                SELECT sys_id, number, table_name, resource_type, state, short_desc, description,
                       assigned_to, parent_id, file_path, synced_at, sys_updated_on, etag,
                       in_scope, last_seen_at, tombstoned_at, pruned_at, raw_json
                FROM records
                WHERE in_scope = 1 AND resource_type = ?1
                ORDER BY number
                LIMIT ?2 OFFSET ?3
                "#,
                Some(resource_type_to_str(kind).to_string()),
            ),
            None => (
                r#"
                SELECT sys_id, number, table_name, resource_type, state, short_desc, description,
                       assigned_to, parent_id, file_path, synced_at, sys_updated_on, etag,
                       in_scope, last_seen_at, tombstoned_at, pruned_at, raw_json
                FROM records
                WHERE in_scope = 1
                ORDER BY number
                LIMIT ?1 OFFSET ?2
                "#,
                None,
            ),
        };

        let mut stmt = self.conn.prepare(sql)?;
        let rows = match bind_resource_type {
            Some(ref kind) => stmt.query_map(
                params![kind, limit as i64, offset as i64],
                row_to_record_row,
            )?,
            None => stmt.query_map(params![limit as i64, offset as i64], row_to_record_row)?,
        };
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    pub fn upsert_reference(&self, reference: &ReferenceRow) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO "references" (
                sys_id, table_name, display_name, extra_json, synced_at, expires_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            ON CONFLICT(sys_id) DO UPDATE SET
                table_name = excluded.table_name,
                display_name = excluded.display_name,
                extra_json = excluded.extra_json,
                synced_at = excluded.synced_at,
                expires_at = excluded.expires_at
            "#,
            params![
                &reference.sys_id,
                &reference.table_name,
                &reference.display_name,
                &reference.extra_json,
                to_ts(reference.synced_at),
                opt_ts(reference.expires_at),
            ],
        )?;
        Ok(())
    }

    pub fn replace_references(&self, references: &[ReferenceRow]) -> Result<()> {
        for reference in references {
            self.upsert_reference(reference)?;
        }
        Ok(())
    }

    pub fn upsert_relationship(&self, relationship: &RelationshipRow) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO relationships (source_id, target_id, rel_type, field_name)
            VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT(source_id, target_id, rel_type, field_name)
            DO UPDATE SET
                source_id = excluded.source_id,
                target_id = excluded.target_id,
                rel_type = excluded.rel_type,
                field_name = excluded.field_name
            "#,
            params![
                &relationship.source_id,
                &relationship.target_id,
                &relationship.rel_type,
                &relationship.field_name,
            ],
        )?;
        Ok(())
    }

    pub fn replace_relationships_for_source(
        &self,
        source_id: &str,
        relationships: &[RelationshipRow],
    ) -> Result<()> {
        self.conn.execute(
            "DELETE FROM relationships WHERE source_id = ?1",
            params![source_id],
        )?;
        for relationship in relationships {
            self.upsert_relationship(relationship)?;
        }
        Ok(())
    }

    pub fn list_relationships_for_source(&self, source_id: &str) -> Result<Vec<RelationshipRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT source_id, target_id, rel_type, field_name
            FROM relationships
            WHERE source_id = ?1
            ORDER BY field_name, target_id
            "#,
        )?;
        let rows = stmt.query_map(params![source_id], |row| {
            Ok(RelationshipRow {
                source_id: row.get(0)?,
                target_id: row.get(1)?,
                rel_type: row.get(2)?,
                field_name: row.get(3)?,
            })
        })?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    pub fn upsert_business_application_server_membership(
        &self,
        membership: &BusinessApplicationServerMembershipRow,
    ) -> Result<()> {
        self.conn
            .execute_batch("SAVEPOINT ba_server_membership_upsert")?;
        let result = (|| -> Result<()> {
            if membership.tombstoned_at.is_none() {
                self.conn.execute(
                    r#"
                    UPDATE business_application_servers
                    SET tombstoned_at = ?3
                    WHERE ba_sys_id = ?1
                      AND server_sys_id = ?2
                      AND provenance <> ?4
                      AND tombstoned_at IS NULL
                    "#,
                    params![
                        &membership.ba_sys_id,
                        &membership.server_sys_id,
                        to_ts(membership.last_seen_at),
                        &membership.provenance,
                    ],
                )?;
            }

            self.conn.execute(
                r#"
                INSERT INTO business_application_servers (
                    ba_sys_id, server_sys_id, server_table, provenance, min_depth,
                    paths_json, discovered_at, last_seen_at, tombstoned_at
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6,
                    COALESCE((
                        SELECT MIN(discovered_at)
                        FROM business_application_servers
                        WHERE ba_sys_id = ?1
                          AND server_sys_id = ?2
                    ), ?7),
                    ?8, ?9
                )
                ON CONFLICT(ba_sys_id, server_sys_id, provenance) DO UPDATE SET
                    server_table = excluded.server_table,
                    min_depth = excluded.min_depth,
                    paths_json = excluded.paths_json,
                    discovered_at = MIN(business_application_servers.discovered_at, excluded.discovered_at),
                    last_seen_at = excluded.last_seen_at,
                    tombstoned_at = excluded.tombstoned_at
                "#,
                params![
                    &membership.ba_sys_id,
                    &membership.server_sys_id,
                    &membership.server_table,
                    &membership.provenance,
                    membership.min_depth as i64,
                    &membership.paths_json,
                    to_ts(membership.discovered_at),
                    to_ts(membership.last_seen_at),
                    opt_ts(membership.tombstoned_at),
                ],
            )?;

            let duplicate_live_pairs: i64 = self.conn.query_row(
                r#"
                SELECT COUNT(*)
                FROM (
                    SELECT 1
                    FROM business_application_servers
                    WHERE ba_sys_id = ?1
                      AND server_sys_id = ?2
                      AND tombstoned_at IS NULL
                    GROUP BY ba_sys_id, server_sys_id
                    HAVING COUNT(*) > 1
                )
                "#,
                params![&membership.ba_sys_id, &membership.server_sys_id],
                |row| row.get(0),
            )?;
            if duplicate_live_pairs > 0 {
                return Err(StoreError::InvalidQuery(format!(
                    "duplicate live BA/server membership rows for ba_sys_id={} server_sys_id={}",
                    membership.ba_sys_id, membership.server_sys_id
                )));
            }
            Ok(())
        })();
        match result {
            Ok(()) => {
                self.conn
                    .execute_batch("RELEASE ba_server_membership_upsert")?;
                Ok(())
            }
            Err(err) => {
                let _ = self.conn.execute_batch(
                    "ROLLBACK TO ba_server_membership_upsert; RELEASE ba_server_membership_upsert",
                );
                Err(err)
            }
        }
    }

    pub fn tombstone_stale_business_application_server_memberships(
        &self,
        ba_sys_id: &str,
        last_seen_before: DateTime<Utc>,
        tombstoned_at: DateTime<Utc>,
    ) -> Result<usize> {
        let updated = self.conn.execute(
            r#"
            UPDATE business_application_servers
            SET tombstoned_at = ?2
            WHERE ba_sys_id = ?1
              AND tombstoned_at IS NULL
              AND last_seen_at < ?3
            "#,
            params![
                ba_sys_id,
                opt_ts(Some(tombstoned_at)),
                to_ts(last_seen_before)
            ],
        )?;
        Ok(updated)
    }

    pub fn upsert_business_application_server_inventory_health(
        &self,
        health: &BusinessApplicationServerInventoryHealthRow,
    ) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO business_application_server_inventory_health (
                ba_sys_id, run_started_at, run_completed_at, service_membership_status,
                relationship_status, inventory_status, summary_json
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            ON CONFLICT(ba_sys_id) DO UPDATE SET
                run_started_at = excluded.run_started_at,
                run_completed_at = excluded.run_completed_at,
                service_membership_status = excluded.service_membership_status,
                relationship_status = excluded.relationship_status,
                inventory_status = excluded.inventory_status,
                summary_json = excluded.summary_json
            "#,
            params![
                &health.ba_sys_id,
                to_ts(health.run_started_at),
                to_ts(health.run_completed_at),
                &health.service_membership_status,
                &health.relationship_status,
                &health.inventory_status,
                &health.summary_json,
            ],
        )?;
        Ok(())
    }

    pub fn get_business_application_server_inventory_health(
        &self,
        ba_sys_id: &str,
    ) -> Result<Option<BusinessApplicationServerInventoryHealthRow>> {
        self.conn
            .query_row(
                r#"
                SELECT ba_sys_id, run_started_at, run_completed_at,
                       service_membership_status, relationship_status, inventory_status,
                       summary_json
                FROM business_application_server_inventory_health
                WHERE ba_sys_id = ?1
                "#,
                params![ba_sys_id],
                row_to_business_application_server_inventory_health,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn list_business_application_server_memberships_for_ba(
        &self,
        ba_sys_id: &str,
        include_tombstoned: bool,
    ) -> Result<Vec<BusinessApplicationServerMembershipRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT ba_sys_id, server_sys_id, server_table, provenance, min_depth,
                   paths_json, discovered_at, last_seen_at, tombstoned_at
            FROM business_application_servers
            WHERE ba_sys_id = ?1
              AND (?2 != 0 OR tombstoned_at IS NULL)
            ORDER BY min_depth ASC, server_table ASC, server_sys_id ASC, provenance ASC
            "#,
        )?;
        let rows = stmt.query_map(
            params![ba_sys_id, bool_to_i64(include_tombstoned)],
            row_to_business_application_server_membership,
        )?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    pub fn list_business_application_server_memberships_for_server(
        &self,
        server_sys_id: &str,
        include_tombstoned: bool,
    ) -> Result<Vec<BusinessApplicationServerMembershipRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT ba_sys_id, server_sys_id, server_table, provenance, min_depth,
                   paths_json, discovered_at, last_seen_at, tombstoned_at
            FROM business_application_servers
            WHERE server_sys_id = ?1
              AND (?2 != 0 OR tombstoned_at IS NULL)
            ORDER BY min_depth ASC, ba_sys_id ASC, provenance ASC
            "#,
        )?;
        let rows = stmt.query_map(
            params![server_sys_id, bool_to_i64(include_tombstoned)],
            row_to_business_application_server_membership,
        )?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    pub fn list_references_for_source(&self, source_id: &str) -> Result<Vec<RecordReferenceRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT relationships.source_id,
                   relationships.field_name,
                   "references".sys_id,
                   "references".table_name,
                   "references".display_name,
                   "references".extra_json,
                   "references".synced_at,
                   "references".expires_at
            FROM relationships
            INNER JOIN "references" ON "references".sys_id = relationships.target_id
            WHERE relationships.source_id = ?1
              AND relationships.rel_type = 'reference'
            ORDER BY relationships.field_name, "references".sys_id
            "#,
        )?;
        let rows = stmt.query_map(params![source_id], |row| {
            Ok(RecordReferenceRow {
                source_id: row.get(0)?,
                field_name: row.get(1)?,
                reference: ReferenceRow {
                    sys_id: row.get(2)?,
                    table_name: row.get(3)?,
                    display_name: row.get(4)?,
                    extra_json: row.get(5)?,
                    synced_at: from_ts(row.get(6)?).map_err(to_sqlite_err)?,
                    expires_at: row
                        .get::<_, Option<i64>>(7)?
                        .map(from_ts)
                        .transpose()
                        .map_err(to_sqlite_err)?,
                },
            })
        })?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    pub fn replace_relationships(
        &self,
        source_id: &str,
        relationships: &[RelationshipRow],
    ) -> Result<()> {
        self.conn.execute(
            "DELETE FROM relationships WHERE source_id = ?1",
            params![source_id],
        )?;
        for relationship in relationships {
            self.upsert_relationship(relationship)?;
        }
        Ok(())
    }

    pub fn set_sync_state(&self, row: &SyncStateRow) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO sync_state (
                resource_type, last_full, last_incr, high_watermark, cursor, filter_hash, updated_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, strftime('%s', 'now'))
            ON CONFLICT(resource_type) DO UPDATE SET
                last_full = excluded.last_full,
                last_incr = excluded.last_incr,
                high_watermark = excluded.high_watermark,
                cursor = excluded.cursor,
                filter_hash = excluded.filter_hash,
                updated_at = excluded.updated_at
            "#,
            params![
                &row.resource_type,
                opt_ts(row.last_full),
                opt_ts(row.last_incr),
                opt_ts(row.high_watermark),
                &row.cursor,
                &row.filter_hash,
            ],
        )?;
        Ok(())
    }

    pub fn list_references(&self) -> Result<Vec<ReferenceRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT sys_id, table_name, display_name, extra_json, synced_at, expires_at
            FROM "references"
            ORDER BY table_name, display_name, sys_id
            "#,
        )?;
        let rows = stmt.query_map([], |row| {
            let synced_at = from_ts(row.get::<_, i64>(4)?).map_err(to_sqlite_err)?;
            Ok(ReferenceRow {
                sys_id: row.get(0)?,
                table_name: row.get(1)?,
                display_name: row.get(2)?,
                extra_json: row.get(3)?,
                synced_at,
                expires_at: row
                    .get::<_, Option<i64>>(5)?
                    .map(from_ts)
                    .transpose()
                    .map_err(to_sqlite_err)?,
            })
        })?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    pub fn list_relationships(&self) -> Result<Vec<RelationshipRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT source_id, target_id, rel_type, field_name
            FROM relationships
            ORDER BY source_id, rel_type, field_name, target_id
            "#,
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(RelationshipRow {
                source_id: row.get(0)?,
                target_id: row.get(1)?,
                rel_type: row.get(2)?,
                field_name: row.get(3)?,
            })
        })?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    pub fn upsert_business_application_projection(
        &self,
        record: &SnowRecord,
        raw_fields: Option<&Value>,
    ) -> Result<()> {
        if record.resource_type != ResourceType::BusinessApplication
            && record.table != "cmdb_ci_business_app"
        {
            return Err(StoreError::InvalidQuery(format!(
                "record {} is not a Business Application",
                record.sys_id
            )));
        }

        let raw_map = raw_fields
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_else(|| field_values_to_json_map(&record.fields));
        let dictionary = self.business_application_dictionary_map()?;
        let now = Utc::now();
        let fields = projected_fields_from_json(
            &record.sys_id,
            &raw_map,
            Some(&record.fields),
            &dictionary,
            now,
        )?;
        let projection = business_application_projection_from_fields(record, &fields);

        self.conn.execute_batch("BEGIN IMMEDIATE")?;
        let result = (|| -> Result<()> {
            self.upsert_business_application_projection_row(&projection)?;
            self.conn.execute(
                "DELETE FROM business_application_fields WHERE record_sys_id = ?1",
                params![&record.sys_id],
            )?;
            for field in &fields {
                self.upsert_business_application_field(field)?;
            }
            Ok(())
        })();
        match result {
            Ok(()) => {
                self.conn.execute_batch("COMMIT")?;
                Ok(())
            }
            Err(err) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(err)
            }
        }
    }

    pub fn rebuild_business_application_projection_from_raw_json(
        &self,
        row: &RecordRow,
    ) -> Result<bool> {
        if row.resource_type != ResourceType::BusinessApplication
            && row.table_name != "cmdb_ci_business_app"
        {
            return Ok(false);
        }
        let raw = serde_json::from_str::<Value>(&row.raw_json)?;
        let Some(raw_map) = raw.as_object() else {
            return Ok(false);
        };
        let fields_map = raw_map
            .iter()
            .map(|(key, value)| (key.clone(), field_value_from_raw_json(value)))
            .collect::<HashMap<_, _>>();
        let record = SnowRecord {
            sys_id: row.sys_id.clone(),
            number: row.number.clone(),
            table: row.table_name.clone(),
            resource_type: ResourceType::BusinessApplication,
            state: row.state.clone().unwrap_or_default(),
            short_description: row.short_desc.clone().unwrap_or_default(),
            description: row.description.clone().unwrap_or_default(),
            fields: fields_map,
            work_notes: Vec::new(),
            comments: Vec::new(),
            parent: None,
            children: Vec::new(),
            references: HashMap::new(),
            synced_at: row.synced_at,
            source: crate::CacheSource::Disk,
        };
        self.upsert_business_application_projection(&record, Some(&raw))?;
        Ok(true)
    }

    pub fn get_business_application_projection(
        &self,
        record_sys_id: &str,
    ) -> Result<Option<BusinessApplicationProjectionRow>> {
        self.conn
            .query_row(
                r#"
                SELECT record_sys_id, name, number, business_owner_sys_id, business_owner_name,
                       is_owner_sys_id, is_owner_name, ci_owner_group_sys_id, ci_owner_group_name,
                       primary_support_group_sys_id, primary_support_group_name,
                       operational_status_value, operational_status_display,
                       primary_portfolio_sys_id, primary_portfolio_name, primary_portfolio_table,
                       attested_date, sys_updated_on, field_count, reference_count,
                       unresolved_reference_count
                FROM business_applications
                WHERE record_sys_id = ?1
                "#,
                params![record_sys_id],
                row_to_business_application_projection,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn list_business_application_fields(
        &self,
        record_sys_id: &str,
    ) -> Result<Vec<ProjectedFieldRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT record_sys_id, field_name, field_label, field_type, value_text,
                   display_value, value_number, value_date, value_bool, reference_sys_id,
                   reference_table, raw_json, updated_at
            FROM business_application_fields
            WHERE record_sys_id = ?1
            ORDER BY field_name
            "#,
        )?;
        let rows = stmt.query_map(params![record_sys_id], row_to_projected_field)?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    pub fn upsert_business_application_field_dictionary(
        &self,
        field: &BusinessApplicationFieldDictionaryRow,
    ) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO business_application_field_dictionary (
                table_name, field_name, field_label, field_type, reference_table, choice,
                mandatory, read_only, max_length, active, synced_at, raw_json
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
            ON CONFLICT(table_name, field_name) DO UPDATE SET
                field_label = excluded.field_label,
                field_type = excluded.field_type,
                reference_table = excluded.reference_table,
                choice = excluded.choice,
                mandatory = excluded.mandatory,
                read_only = excluded.read_only,
                max_length = excluded.max_length,
                active = excluded.active,
                synced_at = excluded.synced_at,
                raw_json = excluded.raw_json
            "#,
            params![
                &field.table_name,
                &field.field_name,
                &field.field_label,
                &field.field_type,
                &field.reference_table,
                bool_to_i64(field.choice),
                bool_to_i64(field.mandatory),
                bool_to_i64(field.read_only),
                &field.max_length,
                bool_to_i64(field.active),
                to_ts(field.synced_at),
                &field.raw_json,
            ],
        )?;
        Ok(())
    }

    pub fn upsert_primitive_object(&self, object: &PrimitiveObjectRow) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO primitive_objects (
                sys_id, table_name, resource_type, display_name, number, file_path, raw_json,
                synced_at, sys_updated_on, resolution_status, last_error
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
            ON CONFLICT(sys_id) DO UPDATE SET
                table_name = excluded.table_name,
                resource_type = excluded.resource_type,
                display_name = excluded.display_name,
                number = excluded.number,
                file_path = excluded.file_path,
                raw_json = excluded.raw_json,
                synced_at = excluded.synced_at,
                sys_updated_on = excluded.sys_updated_on,
                resolution_status = excluded.resolution_status,
                last_error = excluded.last_error
            "#,
            params![
                &object.sys_id,
                &object.table_name,
                &object.resource_type,
                &object.display_name,
                &object.number,
                &object.file_path,
                &object.raw_json,
                to_ts(object.synced_at),
                &object.sys_updated_on,
                primitive_resolution_status_to_str(object.resolution_status),
                &object.last_error,
            ],
        )?;
        Ok(())
    }

    pub fn upsert_unresolved_primitive_stub(
        &self,
        sys_id: impl Into<String>,
        table_name: impl Into<String>,
        display_name: impl Into<String>,
        resolution_status: PrimitiveResolutionStatus,
        last_error: Option<String>,
    ) -> Result<()> {
        let table_name = table_name.into();
        let sys_id = sys_id.into();
        let display_name = display_name.into();
        let raw_json = serde_json::json!({
            "sys_id": sys_id,
            "table": table_name,
            "display_value": display_name,
            "resolution_status": primitive_resolution_status_to_str(resolution_status),
        })
        .to_string();
        self.upsert_primitive_object(&PrimitiveObjectRow {
            sys_id,
            table_name,
            resource_type: "referenced_record".to_string(),
            display_name,
            number: None,
            file_path: None,
            raw_json,
            synced_at: Utc::now(),
            sys_updated_on: None,
            resolution_status,
            last_error,
        })
    }

    pub fn upsert_primitive_object_field(
        &self,
        primitive_sys_id: &str,
        field: &ProjectedFieldRow,
    ) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO primitive_object_fields (
                primitive_sys_id, field_name, field_label, field_type, value_text,
                display_value, value_number, value_date, value_bool, reference_sys_id,
                reference_table, raw_json, updated_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
            ON CONFLICT(primitive_sys_id, field_name) DO UPDATE SET
                field_label = excluded.field_label,
                field_type = excluded.field_type,
                value_text = excluded.value_text,
                display_value = excluded.display_value,
                value_number = excluded.value_number,
                value_date = excluded.value_date,
                value_bool = excluded.value_bool,
                reference_sys_id = excluded.reference_sys_id,
                reference_table = excluded.reference_table,
                raw_json = excluded.raw_json,
                updated_at = excluded.updated_at
            "#,
            params![
                primitive_sys_id,
                &field.field_name,
                &field.field_label,
                &field.field_type,
                &field.value_text,
                &field.display_value,
                &field.value_number,
                &field.value_date,
                field.value_bool.map(bool_to_i64),
                &field.reference_sys_id,
                &field.reference_table,
                &field.raw_json,
                to_ts(field.updated_at),
            ],
        )?;
        Ok(())
    }

    pub fn get_primitive_object(&self, sys_id: &str) -> Result<Option<PrimitiveObjectRow>> {
        self.conn
            .query_row(
                r#"
                SELECT sys_id, table_name, resource_type, display_name, number, file_path,
                       raw_json, synced_at, sys_updated_on, resolution_status, last_error
                FROM primitive_objects
                WHERE sys_id = ?1
                "#,
                params![sys_id],
                |row| {
                    Ok(PrimitiveObjectRow {
                        sys_id: row.get(0)?,
                        table_name: row.get(1)?,
                        resource_type: row.get(2)?,
                        display_name: row.get(3)?,
                        number: row.get(4)?,
                        file_path: row.get(5)?,
                        raw_json: row.get(6)?,
                        synced_at: from_ts(row.get(7)?).map_err(to_sqlite_err)?,
                        sys_updated_on: row.get(8)?,
                        resolution_status: primitive_resolution_status_from_str(
                            &row.get::<_, String>(9)?,
                        )
                        .map_err(to_sqlite_err)?,
                        last_error: row.get(10)?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn upsert_cached_user(&self, user: &CachedUserRow) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO cached_users (
                sys_id, user_name, name, first_name, last_name, email, employee_number,
                active, department, location, title, raw_json, synced_at, sys_updated_on
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7,
                ?8, ?9, ?10, ?11, ?12, ?13, ?14
            )
            ON CONFLICT(sys_id) DO UPDATE SET
                user_name = excluded.user_name,
                name = excluded.name,
                first_name = excluded.first_name,
                last_name = excluded.last_name,
                email = excluded.email,
                employee_number = excluded.employee_number,
                active = excluded.active,
                department = excluded.department,
                location = excluded.location,
                title = excluded.title,
                raw_json = excluded.raw_json,
                synced_at = excluded.synced_at,
                sys_updated_on = excluded.sys_updated_on
            "#,
            params![
                &user.sys_id,
                &user.user_name,
                &user.name,
                &user.first_name,
                &user.last_name,
                &user.email,
                &user.employee_number,
                user.active.map(bool_to_i64),
                &user.department,
                &user.location,
                &user.title,
                &user.raw_json,
                to_ts(user.synced_at),
                &user.sys_updated_on,
            ],
        )?;
        Ok(())
    }

    pub fn get_cached_user(&self, sys_id: &str) -> Result<Option<CachedUserRow>> {
        self.conn
            .query_row(
                r#"
                SELECT sys_id, user_name, name, first_name, last_name, email, employee_number,
                       active, department, location, title, raw_json, synced_at, sys_updated_on
                FROM cached_users
                WHERE sys_id = ?1
                "#,
                params![sys_id],
                row_to_cached_user,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn list_cached_users_by_sys_ids(&self, sys_ids: &[String]) -> Result<Vec<CachedUserRow>> {
        let mut users = Vec::new();
        for sys_id in sys_ids {
            if let Some(user) = self.get_cached_user(sys_id)? {
                users.push(user);
            }
        }
        Ok(users)
    }

    pub fn put_cached_user_query_result(
        &self,
        query_key: &str,
        result_sys_ids: &[String],
        synced_at: DateTime<Utc>,
    ) -> Result<CachedUserQueryRow> {
        self.put_cached_user_query_result_with_ttl(
            query_key,
            result_sys_ids,
            synced_at,
            cached_user_query_ttl(),
        )
    }

    pub fn put_cached_user_query_result_with_ttl(
        &self,
        query_key: &str,
        result_sys_ids: &[String],
        synced_at: DateTime<Utc>,
        ttl: Duration,
    ) -> Result<CachedUserQueryRow> {
        let row = CachedUserQueryRow {
            query_key: query_key.to_string(),
            result_sys_ids: result_sys_ids.to_vec(),
            synced_at,
            expires_at: synced_at + ttl,
        };
        self.upsert_cached_user_query_result(&row)?;
        Ok(row)
    }

    pub fn upsert_cached_user_query_result(&self, row: &CachedUserQueryRow) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO cached_user_queries (
                query_key, result_sys_ids_json, synced_at, expires_at
            ) VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT(query_key) DO UPDATE SET
                result_sys_ids_json = excluded.result_sys_ids_json,
                synced_at = excluded.synced_at,
                expires_at = excluded.expires_at
            "#,
            params![
                &row.query_key,
                serde_json::to_string(&row.result_sys_ids)?,
                to_ts(row.synced_at),
                to_ts(row.expires_at),
            ],
        )?;
        Ok(())
    }

    pub fn get_cached_user_query_result(
        &self,
        query_key: &str,
        now: DateTime<Utc>,
    ) -> Result<Option<CachedUserQueryRow>> {
        let row = self
            .conn
            .query_row(
                r#"
                SELECT query_key, result_sys_ids_json, synced_at, expires_at
                FROM cached_user_queries
                WHERE query_key = ?1
                "#,
                params![query_key],
                row_to_cached_user_query,
            )
            .optional()?;
        Ok(row.filter(|row| row.expires_at > now))
    }

    fn upsert_business_application_projection_row(
        &self,
        projection: &BusinessApplicationProjectionRow,
    ) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO business_applications (
                record_sys_id, name, number, business_owner_sys_id, business_owner_name,
                is_owner_sys_id, is_owner_name, ci_owner_group_sys_id, ci_owner_group_name,
                primary_support_group_sys_id, primary_support_group_name,
                operational_status_value, operational_status_display,
                primary_portfolio_sys_id, primary_portfolio_name, primary_portfolio_table,
                attested_date, sys_updated_on, field_count, reference_count,
                unresolved_reference_count
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11,
                ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21
            )
            ON CONFLICT(record_sys_id) DO UPDATE SET
                name = excluded.name,
                number = excluded.number,
                business_owner_sys_id = excluded.business_owner_sys_id,
                business_owner_name = excluded.business_owner_name,
                is_owner_sys_id = excluded.is_owner_sys_id,
                is_owner_name = excluded.is_owner_name,
                ci_owner_group_sys_id = excluded.ci_owner_group_sys_id,
                ci_owner_group_name = excluded.ci_owner_group_name,
                primary_support_group_sys_id = excluded.primary_support_group_sys_id,
                primary_support_group_name = excluded.primary_support_group_name,
                operational_status_value = excluded.operational_status_value,
                operational_status_display = excluded.operational_status_display,
                primary_portfolio_sys_id = excluded.primary_portfolio_sys_id,
                primary_portfolio_name = excluded.primary_portfolio_name,
                primary_portfolio_table = excluded.primary_portfolio_table,
                attested_date = excluded.attested_date,
                sys_updated_on = excluded.sys_updated_on,
                field_count = excluded.field_count,
                reference_count = excluded.reference_count,
                unresolved_reference_count = excluded.unresolved_reference_count
            "#,
            params![
                &projection.record_sys_id,
                &projection.name,
                &projection.number,
                &projection.business_owner_sys_id,
                &projection.business_owner_name,
                &projection.is_owner_sys_id,
                &projection.is_owner_name,
                &projection.ci_owner_group_sys_id,
                &projection.ci_owner_group_name,
                &projection.primary_support_group_sys_id,
                &projection.primary_support_group_name,
                &projection.operational_status_value,
                &projection.operational_status_display,
                &projection.primary_portfolio_sys_id,
                &projection.primary_portfolio_name,
                &projection.primary_portfolio_table,
                &projection.attested_date,
                &projection.sys_updated_on,
                projection.field_count as i64,
                projection.reference_count as i64,
                projection.unresolved_reference_count as i64,
            ],
        )?;
        Ok(())
    }

    fn upsert_business_application_field(&self, field: &ProjectedFieldRow) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO business_application_fields (
                record_sys_id, field_name, field_label, field_type, value_text,
                display_value, value_number, value_date, value_bool, reference_sys_id,
                reference_table, raw_json, updated_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
            ON CONFLICT(record_sys_id, field_name) DO UPDATE SET
                field_label = excluded.field_label,
                field_type = excluded.field_type,
                value_text = excluded.value_text,
                display_value = excluded.display_value,
                value_number = excluded.value_number,
                value_date = excluded.value_date,
                value_bool = excluded.value_bool,
                reference_sys_id = excluded.reference_sys_id,
                reference_table = excluded.reference_table,
                raw_json = excluded.raw_json,
                updated_at = excluded.updated_at
            "#,
            params![
                &field.owner_sys_id,
                &field.field_name,
                &field.field_label,
                &field.field_type,
                &field.value_text,
                &field.display_value,
                &field.value_number,
                &field.value_date,
                field.value_bool.map(bool_to_i64),
                &field.reference_sys_id,
                &field.reference_table,
                &field.raw_json,
                to_ts(field.updated_at),
            ],
        )?;
        Ok(())
    }

    pub fn replace_tags(&self, record_sys_id: &str, tags: &[TagRow]) -> Result<()> {
        self.conn.execute(
            "DELETE FROM record_tags WHERE record_sys_id = ?1",
            params![record_sys_id],
        )?;
        for tag in tags {
            self.conn.execute(
                r#"
                INSERT INTO record_tags (record_sys_id, tag, source, weight)
                VALUES (?1, ?2, ?3, ?4)
                ON CONFLICT(record_sys_id, tag) DO UPDATE SET
                    source = excluded.source,
                    weight = excluded.weight
                "#,
                params![&tag.record_sys_id, &tag.tag, &tag.source, tag.weight],
            )?;
        }
        Ok(())
    }

    pub fn list_tags(&self, record_sys_id: &str) -> Result<Vec<TagRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT record_sys_id, tag, source, weight
            FROM record_tags
            WHERE record_sys_id = ?1
            ORDER BY tag
            "#,
        )?;
        let rows = stmt.query_map(params![record_sys_id], |row| {
            Ok(TagRow {
                record_sys_id: row.get(0)?,
                tag: row.get(1)?,
                source: row.get(2)?,
                weight: row.get(3)?,
            })
        })?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    pub fn replace_keywords(&self, record_sys_id: &str, keywords: &[KeywordRow]) -> Result<()> {
        self.conn.execute(
            "DELETE FROM record_keywords WHERE record_sys_id = ?1",
            params![record_sys_id],
        )?;
        for keyword in keywords {
            self.conn.execute(
                r#"
                INSERT INTO record_keywords (record_sys_id, keyword, source, weight)
                VALUES (?1, ?2, ?3, ?4)
                ON CONFLICT(record_sys_id, keyword) DO UPDATE SET
                    source = excluded.source,
                    weight = excluded.weight
                "#,
                params![
                    &keyword.record_sys_id,
                    &keyword.keyword,
                    &keyword.source,
                    keyword.weight
                ],
            )?;
        }
        Ok(())
    }

    pub fn list_keywords(&self, record_sys_id: &str) -> Result<Vec<KeywordRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT record_sys_id, keyword, source, weight
            FROM record_keywords
            WHERE record_sys_id = ?1
            ORDER BY keyword
            "#,
        )?;
        let rows = stmt.query_map(params![record_sys_id], |row| {
            Ok(KeywordRow {
                record_sys_id: row.get(0)?,
                keyword: row.get(1)?,
                source: row.get(2)?,
                weight: row.get(3)?,
            })
        })?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    pub fn replace_aliases(&self, record_sys_id: &str, aliases: &[AliasRow]) -> Result<()> {
        self.conn.execute(
            "DELETE FROM record_aliases WHERE record_sys_id = ?1",
            params![record_sys_id],
        )?;
        for alias in aliases {
            self.conn.execute(
                r#"
                INSERT INTO record_aliases (record_sys_id, alias, kind, source)
                VALUES (?1, ?2, ?3, ?4)
                ON CONFLICT(record_sys_id, alias) DO UPDATE SET
                    kind = excluded.kind,
                    source = excluded.source
                "#,
                params![
                    &alias.record_sys_id,
                    &alias.alias,
                    &alias.kind,
                    &alias.source
                ],
            )?;
        }
        Ok(())
    }

    pub fn list_aliases(&self, record_sys_id: &str) -> Result<Vec<AliasRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT record_sys_id, alias, kind, source
            FROM record_aliases
            WHERE record_sys_id = ?1
            ORDER BY alias
            "#,
        )?;
        let rows = stmt.query_map(params![record_sys_id], |row| {
            Ok(AliasRow {
                record_sys_id: row.get(0)?,
                alias: row.get(1)?,
                kind: row.get(2)?,
                source: row.get(3)?,
            })
        })?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    pub fn upsert_knowledge_article(&self, article: &KnowledgeArticleRow) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO knowledge_articles (
                record_sys_id, number, title, workflow_state, knowledge_base_sys_id,
                knowledge_base_name, category_sys_id, category_name, author_sys_id,
                author_name, published_at, valid_to, article_type, sys_updated_on,
                sn_tags, auto_tags, user_tags, body_cached
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5,
                ?6, ?7, ?8, ?9,
                ?10, ?11, ?12, ?13, ?14,
                ?15, ?16, ?17, ?18
            )
            ON CONFLICT(record_sys_id) DO UPDATE SET
                number = excluded.number,
                title = excluded.title,
                workflow_state = excluded.workflow_state,
                knowledge_base_sys_id = excluded.knowledge_base_sys_id,
                knowledge_base_name = excluded.knowledge_base_name,
                category_sys_id = excluded.category_sys_id,
                category_name = excluded.category_name,
                author_sys_id = excluded.author_sys_id,
                author_name = excluded.author_name,
                published_at = excluded.published_at,
                valid_to = excluded.valid_to,
                article_type = excluded.article_type,
                sys_updated_on = excluded.sys_updated_on,
                sn_tags = excluded.sn_tags,
                auto_tags = excluded.auto_tags,
                user_tags = excluded.user_tags,
                body_cached = excluded.body_cached
            "#,
            params![
                &article.record_sys_id,
                &article.number,
                &article.title,
                &article.workflow_state,
                &article.knowledge_base_sys_id,
                &article.knowledge_base_name,
                &article.category_sys_id,
                &article.category_name,
                &article.author_sys_id,
                &article.author_name,
                &article.published_at,
                &article.valid_to,
                &article.article_type,
                &article.sys_updated_on,
                serde_json::to_string(&article.sn_tags)?,
                serde_json::to_string(&article.auto_tags)?,
                serde_json::to_string(&article.user_tags)?,
                bool_to_i64(article.body_cached),
            ],
        )?;
        Ok(())
    }

    pub fn get_knowledge_article(
        &self,
        record_sys_id: &str,
    ) -> Result<Option<KnowledgeArticleRow>> {
        self.conn
            .query_row(
                r#"
                SELECT record_sys_id, number, title, workflow_state, knowledge_base_sys_id,
                       knowledge_base_name, category_sys_id, category_name, author_sys_id,
                       author_name, published_at, valid_to, article_type, sys_updated_on,
                       sn_tags, auto_tags, user_tags, body_cached
                FROM knowledge_articles
                WHERE record_sys_id = ?1
                "#,
                params![record_sys_id],
                |row| {
                    Ok(KnowledgeArticleRow {
                        record_sys_id: row.get(0)?,
                        number: row.get(1)?,
                        title: row.get(2)?,
                        workflow_state: row.get(3)?,
                        knowledge_base_sys_id: row.get(4)?,
                        knowledge_base_name: row.get(5)?,
                        category_sys_id: row.get(6)?,
                        category_name: row.get(7)?,
                        author_sys_id: row.get(8)?,
                        author_name: row.get(9)?,
                        published_at: row.get(10)?,
                        valid_to: row.get(11)?,
                        article_type: row.get(12)?,
                        sys_updated_on: row.get(13)?,
                        sn_tags: parse_string_vec_column(row, 14)?,
                        auto_tags: parse_string_vec_column(row, 15)?,
                        user_tags: parse_string_vec_column(row, 16)?,
                        body_cached: i64_to_bool(row.get(17)?),
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn list_knowledge_bases(&self) -> Result<Vec<KnowledgeBaseSummaryRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT ka.knowledge_base_sys_id, ka.knowledge_base_name, COUNT(*)
            FROM knowledge_articles ka
            INNER JOIN records r ON r.sys_id = ka.record_sys_id
            WHERE r.in_scope = 1
            GROUP BY knowledge_base_sys_id, knowledge_base_name
            ORDER BY knowledge_base_name, knowledge_base_sys_id
            "#,
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(KnowledgeBaseSummaryRow {
                knowledge_base_sys_id: row.get(0)?,
                knowledge_base_name: row.get(1)?,
                article_count: row.get::<_, i64>(2)? as usize,
            })
        })?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    pub fn list_knowledge_categories(
        &self,
        knowledge_base_sys_id: &str,
    ) -> Result<Vec<KnowledgeCategorySummaryRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT ka.category_sys_id, ka.category_name, ka.knowledge_base_sys_id, COUNT(*)
            FROM knowledge_articles ka
            INNER JOIN records r ON r.sys_id = ka.record_sys_id
            WHERE ka.knowledge_base_sys_id = ?1
              AND r.in_scope = 1
            GROUP BY category_sys_id, category_name, knowledge_base_sys_id
            ORDER BY category_name, category_sys_id
            "#,
        )?;
        let rows = stmt.query_map(params![knowledge_base_sys_id], |row| {
            Ok(KnowledgeCategorySummaryRow {
                category_sys_id: row.get(0)?,
                category_name: row.get(1)?,
                knowledge_base_sys_id: row.get(2)?,
                article_count: row.get::<_, i64>(3)? as usize,
            })
        })?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    pub fn count_knowledge_articles(&self) -> Result<usize> {
        self.conn
            .query_row(
                r#"
                SELECT COUNT(*)
                FROM knowledge_articles ka
                INNER JOIN records r ON r.sys_id = ka.record_sys_id
                WHERE r.in_scope = 1
                "#,
                [],
                |row| row.get::<_, i64>(0),
            )
            .map(|count| count as usize)
            .map_err(Into::into)
    }

    pub fn count_knowledge_articles_with_cached_body(&self) -> Result<usize> {
        self.conn
            .query_row(
                r#"
                SELECT COUNT(*)
                FROM knowledge_articles ka
                INNER JOIN records r ON r.sys_id = ka.record_sys_id
                WHERE r.in_scope = 1
                  AND ka.body_cached = 1
                "#,
                [],
                |row| row.get::<_, i64>(0),
            )
            .map(|count| count as usize)
            .map_err(Into::into)
    }

    pub fn upsert_knowledge_embedding(&self, row: &KnowledgeEmbeddingRow) -> Result<()> {
        let vector_blob = encode_embedding_vector(&row.vector)?;
        self.conn.execute(
            r#"
            INSERT INTO knowledge_article_embeddings (
                record_sys_id, model, provider, dimensions, coverage, content_hash,
                vector_blob, updated_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            ON CONFLICT(record_sys_id) DO UPDATE SET
                model = excluded.model,
                provider = excluded.provider,
                dimensions = excluded.dimensions,
                coverage = excluded.coverage,
                content_hash = excluded.content_hash,
                vector_blob = excluded.vector_blob,
                updated_at = excluded.updated_at
            "#,
            params![
                &row.record_sys_id,
                &row.model,
                &row.provider,
                row.dimensions as i64,
                coverage_to_str(row.coverage),
                &row.content_hash,
                vector_blob,
                to_ts(row.updated_at),
            ],
        )?;
        Ok(())
    }

    pub fn get_knowledge_embedding(
        &self,
        record_sys_id: &str,
    ) -> Result<Option<KnowledgeEmbeddingRow>> {
        self.conn
            .query_row(
                r#"
                SELECT record_sys_id, model, provider, dimensions, coverage, content_hash,
                       vector_blob, updated_at
                FROM knowledge_article_embeddings
                WHERE record_sys_id = ?1
                "#,
                params![record_sys_id],
                |row| {
                    let dimensions = row.get::<_, i64>(3)? as usize;
                    let blob: Vec<u8> = row.get(6)?;
                    Ok(KnowledgeEmbeddingRow {
                        record_sys_id: row.get(0)?,
                        model: row.get(1)?,
                        provider: row.get(2)?,
                        dimensions,
                        coverage: coverage_from_str(&row.get::<_, String>(4)?)
                            .map_err(to_sqlite_err)?,
                        content_hash: row.get(5)?,
                        vector: decode_embedding_vector(&blob, dimensions)
                            .map_err(to_sqlite_err)?,
                        updated_at: from_ts(row.get(7)?).map_err(to_sqlite_err)?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn list_knowledge_embeddings(&self) -> Result<Vec<KnowledgeEmbeddingRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT record_sys_id, model, provider, dimensions, coverage, content_hash,
                   vector_blob, updated_at
            FROM knowledge_article_embeddings
            ORDER BY record_sys_id
            "#,
        )?;
        let rows = stmt.query_map([], |row| {
            let dimensions = row.get::<_, i64>(3)? as usize;
            let blob: Vec<u8> = row.get(6)?;
            Ok(KnowledgeEmbeddingRow {
                record_sys_id: row.get(0)?,
                model: row.get(1)?,
                provider: row.get(2)?,
                dimensions,
                coverage: coverage_from_str(&row.get::<_, String>(4)?).map_err(to_sqlite_err)?,
                content_hash: row.get(5)?,
                vector: decode_embedding_vector(&blob, dimensions).map_err(to_sqlite_err)?,
                updated_at: from_ts(row.get(7)?).map_err(to_sqlite_err)?,
            })
        })?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    pub fn delete_knowledge_embedding(&self, record_sys_id: &str) -> Result<()> {
        self.conn.execute(
            "DELETE FROM knowledge_article_embeddings WHERE record_sys_id = ?1",
            params![record_sys_id],
        )?;
        Ok(())
    }

    pub fn prune_orphan_knowledge_embeddings(&self) -> Result<usize> {
        let removed = self.conn.execute(
            r#"
            DELETE FROM knowledge_article_embeddings
            WHERE NOT EXISTS (
                SELECT 1
                FROM knowledge_articles ka
                WHERE ka.record_sys_id = knowledge_article_embeddings.record_sys_id
            )
            "#,
            [],
        )?;
        Ok(removed)
    }

    pub fn count_knowledge_embeddings_by_coverage(
        &self,
        model: &str,
        coverage: KnowledgeEmbeddingCoverage,
    ) -> Result<usize> {
        self.conn
            .query_row(
                r#"
                SELECT COUNT(*)
                FROM knowledge_article_embeddings
                WHERE model = ?1
                  AND coverage = ?2
                "#,
                params![model, coverage_to_str(coverage)],
                |row| row.get::<_, i64>(0),
            )
            .map(|count| count as usize)
            .map_err(Into::into)
    }

    pub fn count_orphan_knowledge_embeddings(&self) -> Result<usize> {
        self.conn
            .query_row(
                r#"
                SELECT COUNT(*)
                FROM knowledge_article_embeddings kae
                LEFT JOIN knowledge_articles ka
                  ON ka.record_sys_id = kae.record_sys_id
                WHERE ka.record_sys_id IS NULL
                "#,
                [],
                |row| row.get::<_, i64>(0),
            )
            .map(|count| count as usize)
            .map_err(Into::into)
    }

    pub fn knowledge_semantic_meta(&self) -> Result<KnowledgeSemanticMeta> {
        let last_rebuild_at = self
            .get_meta_value("kb_semantic_last_rebuild_at")?
            .map(|value| {
                value
                    .parse::<i64>()
                    .map_err(|_| StoreError::InvalidSchemaVersion(value.clone()))
                    .and_then(from_ts)
            })
            .transpose()?;
        let last_error = self.get_meta_value("kb_semantic_last_error")?;
        Ok(KnowledgeSemanticMeta {
            last_rebuild_at,
            last_error,
        })
    }

    pub fn set_knowledge_semantic_meta(
        &self,
        last_rebuild_at: Option<DateTime<Utc>>,
        last_error: Option<&str>,
    ) -> Result<()> {
        self.set_meta_value(
            "kb_semantic_last_rebuild_at",
            last_rebuild_at
                .map(|value| value.timestamp().to_string())
                .as_deref(),
        )?;
        self.set_meta_value("kb_semantic_last_error", last_error)?;
        Ok(())
    }

    pub fn list_active_knowledge_index_rows(&self) -> Result<Vec<KnowledgeIndexRow>> {
        self.list_active_knowledge_index_rows_filtered(None)
    }

    pub fn list_active_knowledge_index_rows_for_base(
        &self,
        knowledge_base_sys_id: &str,
    ) -> Result<Vec<KnowledgeIndexRow>> {
        self.list_active_knowledge_index_rows_filtered(Some(knowledge_base_sys_id))
    }

    fn list_active_knowledge_index_rows_filtered(
        &self,
        knowledge_base_sys_id: Option<&str>,
    ) -> Result<Vec<KnowledgeIndexRow>> {
        let (sql, params_vec) = if let Some(knowledge_base_sys_id) = knowledge_base_sys_id {
            (
                r#"
                SELECT ka.record_sys_id, ka.number, ka.title, ka.knowledge_base_sys_id,
                       ka.knowledge_base_name, ka.category_sys_id, ka.category_name, r.file_path,
                       ka.sn_tags, ka.auto_tags, ka.user_tags
                FROM knowledge_articles ka
                INNER JOIN records r ON r.sys_id = ka.record_sys_id
                WHERE r.in_scope = 1
                  AND r.file_path IS NOT NULL
                  AND ka.knowledge_base_sys_id = ?1
                ORDER BY ka.knowledge_base_name, ka.category_name, ka.title, ka.number
                "#,
                vec![knowledge_base_sys_id.to_string()],
            )
        } else {
            (
                r#"
                SELECT ka.record_sys_id, ka.number, ka.title, ka.knowledge_base_sys_id,
                       ka.knowledge_base_name, ka.category_sys_id, ka.category_name, r.file_path,
                       ka.sn_tags, ka.auto_tags, ka.user_tags
                FROM knowledge_articles ka
                INNER JOIN records r ON r.sys_id = ka.record_sys_id
                WHERE r.in_scope = 1
                  AND r.file_path IS NOT NULL
                ORDER BY ka.knowledge_base_name, ka.category_name, ka.title, ka.number
                "#,
                Vec::new(),
            )
        };

        let mut stmt = self.conn.prepare(sql)?;
        if let Some(value) = params_vec.first() {
            let rows = stmt.query_map(params![value], |row| {
                Ok(KnowledgeIndexRow {
                    record_sys_id: row.get(0)?,
                    number: row.get(1)?,
                    title: row.get(2)?,
                    knowledge_base_sys_id: row.get(3)?,
                    knowledge_base_name: row.get(4)?,
                    category_sys_id: row.get(5)?,
                    category_name: row.get(6)?,
                    file_path: row.get(7)?,
                    sn_tags: parse_string_vec_column(row, 8)?,
                    auto_tags: parse_string_vec_column(row, 9)?,
                    user_tags: parse_string_vec_column(row, 10)?,
                })
            })?;
            Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
        } else {
            let rows = stmt.query_map([], |row| {
                Ok(KnowledgeIndexRow {
                    record_sys_id: row.get(0)?,
                    number: row.get(1)?,
                    title: row.get(2)?,
                    knowledge_base_sys_id: row.get(3)?,
                    knowledge_base_name: row.get(4)?,
                    category_sys_id: row.get(5)?,
                    category_name: row.get(6)?,
                    file_path: row.get(7)?,
                    sn_tags: parse_string_vec_column(row, 8)?,
                    auto_tags: parse_string_vec_column(row, 9)?,
                    user_tags: parse_string_vec_column(row, 10)?,
                })
            })?;
            Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
        }
    }

    pub fn list_active_knowledge_local_scan_rows(&self) -> Result<Vec<KnowledgeLocalScanRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT r.sys_id, r.number, r.file_path, l.modified_at_ms
            FROM records r
            LEFT JOIN kb_local_file_state l ON l.record_sys_id = r.sys_id
            WHERE r.in_scope = 1
              AND r.resource_type = 'kb_knowledge'
              AND r.file_path IS NOT NULL
            ORDER BY r.number
            "#,
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(KnowledgeLocalScanRow {
                record_sys_id: row.get(0)?,
                number: row.get(1)?,
                file_path: row.get(2)?,
                modified_at_ms: row.get(3)?,
            })
        })?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    pub fn update_knowledge_local_state(
        &self,
        record_sys_id: &str,
        user_tags: &[String],
        body_cached: bool,
    ) -> Result<()> {
        self.conn.execute(
            r#"
            UPDATE knowledge_articles
            SET user_tags = ?2,
                body_cached = ?3
            WHERE record_sys_id = ?1
            "#,
            params![
                record_sys_id,
                serde_json::to_string(user_tags)?,
                bool_to_i64(body_cached),
            ],
        )?;
        Ok(())
    }

    pub fn load_kb_term_stats(&self) -> Result<HashMap<String, usize>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT term, doc_freq
            FROM kb_term_stats
            ORDER BY term
            "#,
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as usize))
        })?;
        let mut stats = HashMap::new();
        for row in rows {
            let (term, doc_freq) = row?;
            stats.insert(term, doc_freq);
        }
        Ok(stats)
    }

    pub fn replace_kb_term_stats(&self, stats: &HashMap<String, usize>) -> Result<()> {
        self.conn.execute_batch("BEGIN IMMEDIATE")?;
        let result = (|| -> Result<()> {
            self.conn.execute("DELETE FROM kb_term_stats", [])?;
            for (term, doc_freq) in stats {
                self.conn.execute(
                    r#"
                    INSERT INTO kb_term_stats (term, doc_freq)
                    VALUES (?1, ?2)
                    "#,
                    params![term, *doc_freq as i64],
                )?;
            }
            Ok(())
        })();
        match result {
            Ok(()) => {
                self.conn.execute_batch("COMMIT")?;
                Ok(())
            }
            Err(err) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(err)
            }
        }
    }

    pub fn get_kb_article_terms(&self, record_sys_id: &str) -> Result<Vec<String>> {
        self.conn
            .query_row(
                r#"
                SELECT terms_json
                FROM kb_article_terms
                WHERE record_sys_id = ?1
                "#,
                params![record_sys_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .map(|raw| serde_json::from_str::<Vec<String>>(&raw).map_err(Into::into))
            .transpose()
            .map(|terms| terms.unwrap_or_default())
    }

    pub fn replace_all_kb_article_terms(&self, entries: &[(String, Vec<String>)]) -> Result<()> {
        self.conn.execute_batch("BEGIN IMMEDIATE")?;
        let result = (|| -> Result<()> {
            self.conn.execute("DELETE FROM kb_article_terms", [])?;
            for (record_sys_id, terms) in dedupe_kb_term_entries(entries) {
                self.conn.execute(
                    r#"
                    INSERT INTO kb_article_terms (record_sys_id, terms_json)
                    VALUES (?1, ?2)
                    "#,
                    params![record_sys_id, serde_json::to_string(&terms)?],
                )?;
            }
            Ok(())
        })();
        match result {
            Ok(()) => {
                self.conn.execute_batch("COMMIT")?;
                Ok(())
            }
            Err(err) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(err)
            }
        }
    }

    pub fn replace_kb_article_terms_entries(
        &self,
        entries: &[(String, Vec<String>)],
    ) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        self.conn.execute_batch("BEGIN IMMEDIATE")?;
        let result = (|| -> Result<()> {
            for (record_sys_id, terms) in dedupe_kb_term_entries(entries) {
                self.conn.execute(
                    r#"
                    INSERT INTO kb_article_terms (record_sys_id, terms_json)
                    VALUES (?1, ?2)
                    ON CONFLICT(record_sys_id) DO UPDATE SET
                        terms_json = excluded.terms_json
                    "#,
                    params![record_sys_id, serde_json::to_string(&terms)?],
                )?;
            }
            Ok(())
        })();
        match result {
            Ok(()) => {
                self.conn.execute_batch("COMMIT")?;
                Ok(())
            }
            Err(err) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(err)
            }
        }
    }

    pub fn delete_kb_article_terms(&self, record_sys_ids: &[String]) -> Result<()> {
        if record_sys_ids.is_empty() {
            return Ok(());
        }
        self.conn.execute_batch("BEGIN IMMEDIATE")?;
        let result = (|| -> Result<()> {
            for record_sys_id in record_sys_ids {
                self.conn.execute(
                    "DELETE FROM kb_article_terms WHERE record_sys_id = ?1",
                    params![record_sys_id],
                )?;
            }
            Ok(())
        })();
        match result {
            Ok(()) => {
                self.conn.execute_batch("COMMIT")?;
                Ok(())
            }
            Err(err) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(err)
            }
        }
    }

    pub fn upsert_kb_local_file_states(&self, entries: &[(String, String, i64)]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        self.conn.execute_batch("BEGIN IMMEDIATE")?;
        let result = (|| -> Result<()> {
            for (record_sys_id, file_path, modified_at_ms) in entries {
                self.conn.execute(
                    r#"
                    INSERT INTO kb_local_file_state (record_sys_id, file_path, modified_at_ms)
                    VALUES (?1, ?2, ?3)
                    ON CONFLICT(record_sys_id) DO UPDATE SET
                        file_path = excluded.file_path,
                        modified_at_ms = excluded.modified_at_ms
                    "#,
                    params![record_sys_id, file_path, modified_at_ms],
                )?;
            }
            Ok(())
        })();
        match result {
            Ok(()) => {
                self.conn.execute_batch("COMMIT")?;
                Ok(())
            }
            Err(err) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(err)
            }
        }
    }

    pub fn get_kb_sync_state(&self) -> Result<KbSyncStateRow> {
        self.conn
            .query_row(
                r#"
                SELECT last_full_at, last_incr_at, watermark_updated_at, watermark_sys_id, kb_sync_lock
                FROM kb_sync_state
                WHERE id = 1
                "#,
                [],
                |row| {
                    Ok(KbSyncStateRow {
                        last_full_at: row
                            .get::<_, Option<i64>>(0)?
                            .map(from_ts)
                            .transpose()
                            .map_err(to_sqlite_err)?,
                        last_incr_at: row
                            .get::<_, Option<i64>>(1)?
                            .map(from_ts)
                            .transpose()
                            .map_err(to_sqlite_err)?,
                        watermark_updated_at: row.get(2)?,
                        watermark_sys_id: row.get(3)?,
                        kb_sync_lock: row.get(4)?,
                    })
                },
            )
            .map_err(Into::into)
    }

    pub fn set_kb_sync_state(&self, row: &KbSyncStateRow) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO kb_sync_state (
                id, last_full_at, last_incr_at, watermark_updated_at, watermark_sys_id, kb_sync_lock
            ) VALUES (1, ?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(id) DO UPDATE SET
                last_full_at = excluded.last_full_at,
                last_incr_at = excluded.last_incr_at,
                watermark_updated_at = excluded.watermark_updated_at,
                watermark_sys_id = excluded.watermark_sys_id,
                kb_sync_lock = excluded.kb_sync_lock
            "#,
            params![
                opt_ts(row.last_full_at),
                opt_ts(row.last_incr_at),
                &row.watermark_updated_at,
                &row.watermark_sys_id,
                &row.kb_sync_lock,
            ],
        )?;
        Ok(())
    }

    pub fn acquire_kb_sync_lock(&self, now_ms: i64, stale_after_ms: i64) -> Result<bool> {
        self.conn
            .execute("INSERT OR IGNORE INTO kb_sync_state (id) VALUES (1)", [])?;
        let changed = self.conn.execute(
            r#"
            UPDATE kb_sync_state
            SET kb_sync_lock = ?1
            WHERE id = 1
              AND (kb_sync_lock IS NULL OR kb_sync_lock < ?2)
            "#,
            params![now_ms, now_ms - stale_after_ms],
        )?;
        Ok(changed > 0)
    }

    pub fn release_kb_sync_lock(&self) -> Result<()> {
        self.conn.execute(
            "UPDATE kb_sync_state SET kb_sync_lock = NULL WHERE id = 1",
            [],
        )?;
        Ok(())
    }

    pub fn list_knowledge_tags(
        &self,
        layer: &str,
        min_count: usize,
    ) -> Result<Vec<KnowledgeTagCountRow>> {
        let (sql, params_vec) = match layer {
            "sn" => (
                r#"
                SELECT tag, 'sn' AS layer, COUNT(*) AS article_count
                FROM (
                    SELECT ka.record_sys_id, LOWER(TRIM(json_each.value)) AS tag
                    FROM knowledge_articles ka
                    INNER JOIN records r ON r.sys_id = ka.record_sys_id
                    INNER JOIN json_each(ka.sn_tags)
                    WHERE r.in_scope = 1
                )
                WHERE tag <> ''
                GROUP BY tag
                HAVING COUNT(*) >= ?1
                ORDER BY article_count DESC, tag ASC
                "#,
                vec![min_count as i64],
            ),
            "auto" => (
                r#"
                SELECT tag, 'auto' AS layer, COUNT(*) AS article_count
                FROM (
                    SELECT ka.record_sys_id, LOWER(TRIM(json_each.value)) AS tag
                    FROM knowledge_articles ka
                    INNER JOIN records r ON r.sys_id = ka.record_sys_id
                    INNER JOIN json_each(ka.auto_tags)
                    WHERE r.in_scope = 1
                )
                WHERE tag <> ''
                GROUP BY tag
                HAVING COUNT(*) >= ?1
                ORDER BY article_count DESC, tag ASC
                "#,
                vec![min_count as i64],
            ),
            "user" => (
                r#"
                SELECT tag, 'user' AS layer, COUNT(*) AS article_count
                FROM (
                    SELECT ka.record_sys_id, LOWER(TRIM(json_each.value)) AS tag
                    FROM knowledge_articles ka
                    INNER JOIN records r ON r.sys_id = ka.record_sys_id
                    INNER JOIN json_each(ka.user_tags)
                    WHERE r.in_scope = 1
                )
                WHERE tag <> ''
                GROUP BY tag
                HAVING COUNT(*) >= ?1
                ORDER BY article_count DESC, tag ASC
                "#,
                vec![min_count as i64],
            ),
            _ => (
                r#"
                SELECT tag, 'all' AS layer, COUNT(*) AS article_count
                FROM (
                    SELECT ka.record_sys_id, LOWER(TRIM(json_each.value)) AS tag
                    FROM knowledge_articles ka
                    INNER JOIN records r ON r.sys_id = ka.record_sys_id
                    INNER JOIN json_each(ka.sn_tags)
                    WHERE r.in_scope = 1
                    UNION
                    SELECT ka.record_sys_id, LOWER(TRIM(json_each.value)) AS tag
                    FROM knowledge_articles ka
                    INNER JOIN records r ON r.sys_id = ka.record_sys_id
                    INNER JOIN json_each(ka.auto_tags)
                    WHERE r.in_scope = 1
                    UNION
                    SELECT ka.record_sys_id, LOWER(TRIM(json_each.value)) AS tag
                    FROM knowledge_articles ka
                    INNER JOIN records r ON r.sys_id = ka.record_sys_id
                    INNER JOIN json_each(ka.user_tags)
                    WHERE r.in_scope = 1
                )
                WHERE tag <> ''
                GROUP BY tag
                HAVING COUNT(*) >= ?1
                ORDER BY article_count DESC, tag ASC
                "#,
                vec![min_count as i64],
            ),
        };

        let mut stmt = self.conn.prepare(sql)?;
        let rows = stmt.query_map(params![params_vec[0]], |row| {
            Ok(KnowledgeTagCountRow {
                tag: row.get(0)?,
                layer: row.get(1)?,
                article_count: row.get::<_, i64>(2)? as usize,
            })
        })?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    pub fn find_record_sys_ids_by_tag(&self, tag: &str, limit: usize) -> Result<Vec<String>> {
        self.find_record_sys_ids_by_enrichment("record_tags", "tag", tag, limit)
    }

    pub fn find_record_sys_ids_by_keyword(
        &self,
        keyword: &str,
        limit: usize,
    ) -> Result<Vec<String>> {
        self.find_record_sys_ids_by_enrichment("record_keywords", "keyword", keyword, limit)
    }

    pub fn find_record_sys_ids_by_alias(&self, alias: &str, limit: usize) -> Result<Vec<String>> {
        self.find_record_sys_ids_by_enrichment("record_aliases", "alias", alias, limit)
    }

    pub fn get_sync_state(&self, resource_type: &str) -> Result<Option<SyncStateRow>> {
        self.conn
            .query_row(
                r#"
                SELECT resource_type, last_full, last_incr, high_watermark, cursor, filter_hash
                FROM sync_state
                WHERE resource_type = ?1
                "#,
                params![resource_type],
                |row| {
                    Ok(SyncStateRow {
                        resource_type: row.get(0)?,
                        last_full: row
                            .get::<_, Option<i64>>(1)?
                            .map(from_ts)
                            .transpose()
                            .map_err(to_sqlite_err)?,
                        last_incr: row
                            .get::<_, Option<i64>>(2)?
                            .map(from_ts)
                            .transpose()
                            .map_err(to_sqlite_err)?,
                        high_watermark: row
                            .get::<_, Option<i64>>(3)?
                            .map(from_ts)
                            .transpose()
                            .map_err(to_sqlite_err)?,
                        cursor: row.get(4)?,
                        filter_hash: row.get(5)?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn tombstone_record(&self, sys_id: &str, when: DateTime<Utc>) -> Result<()> {
        self.conn.execute(
            r#"
            UPDATE records
            SET in_scope = 0,
                tombstoned_at = ?2,
                last_seen_at = ?2
            WHERE sys_id = ?1
            "#,
            params![sys_id, to_ts(when)],
        )?;
        Ok(())
    }

    pub fn prune_record(&self, sys_id: &str, when: DateTime<Utc>) -> Result<()> {
        self.conn.execute(
            r#"
            UPDATE records
            SET pruned_at = ?2
            WHERE sys_id = ?1
            "#,
            params![sys_id, to_ts(when)],
        )?;
        self.conn.execute(
            "DELETE FROM fts_records WHERE rowid = (SELECT rowid FROM records WHERE sys_id = ?1)",
            params![sys_id],
        )?;
        self.conn.execute(
            "DELETE FROM relationships WHERE source_id = ?1 OR target_id = ?1",
            params![sys_id],
        )?;
        self.conn.execute(
            "DELETE FROM record_tags WHERE record_sys_id = ?1",
            params![sys_id],
        )?;
        self.conn.execute(
            "DELETE FROM record_keywords WHERE record_sys_id = ?1",
            params![sys_id],
        )?;
        self.conn.execute(
            "DELETE FROM record_aliases WHERE record_sys_id = ?1",
            params![sys_id],
        )?;
        self.conn.execute(
            "DELETE FROM knowledge_articles WHERE record_sys_id = ?1",
            params![sys_id],
        )?;
        self.conn.execute(
            "DELETE FROM kb_article_terms WHERE record_sys_id = ?1",
            params![sys_id],
        )?;
        self.conn.execute(
            "DELETE FROM kb_local_file_state WHERE record_sys_id = ?1",
            params![sys_id],
        )?;
        self.conn
            .execute("DELETE FROM records WHERE sys_id = ?1", params![sys_id])?;
        Ok(())
    }

    pub fn count_active_records(&self) -> Result<i64> {
        let count = self.conn.query_row(
            "SELECT COUNT(*) FROM records WHERE in_scope = 1",
            [],
            |row| row.get(0),
        )?;
        Ok(count)
    }

    pub fn search_fts(&self, query: &str, limit: usize) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT records.number
            FROM fts_records
            JOIN records ON records.rowid = fts_records.rowid
            WHERE fts_records MATCH ?1 AND records.in_scope = 1
            ORDER BY bm25(fts_records)
            LIMIT ?2
            "#,
        )?;
        let rows = stmt.query_map(params![query, limit as i64], |row| row.get::<_, String>(0))?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    pub fn query_business_application_records(
        &self,
        query: &crate::query::filter::BusinessApplicationQuery,
    ) -> Result<Vec<RecordRow>> {
        let limit = query.limit.unwrap_or(20);
        if limit > 500 {
            return Err(StoreError::InvalidQuery(
                "Business Application local query limit cannot exceed 500".to_string(),
            ));
        }

        for filter in &query.filters {
            if !query.allow_unknown_fields
                && !self.business_application_field_exists(&filter.field)?
            {
                return Err(StoreError::InvalidQuery(format!(
                    "unknown Business Application field `{}`",
                    filter.field
                )));
            }
        }
        for sort in &query.sort {
            if !query.allow_unknown_fields
                && !self.business_application_field_exists(&sort.field)?
            {
                return Err(StoreError::InvalidQuery(format!(
                    "unknown Business Application sort field `{}`",
                    sort.field
                )));
            }
        }

        let sql = if query.include_tombstoned {
            r#"
            SELECT sys_id, number, table_name, resource_type, state, short_desc, description,
                   assigned_to, parent_id, file_path, synced_at, sys_updated_on, etag,
                   in_scope, last_seen_at, tombstoned_at, pruned_at, raw_json
            FROM records
            WHERE resource_type IN ('business_application', 'cmdb_ci_business_app')
               OR table_name = 'cmdb_ci_business_app'
            "#
        } else {
            r#"
            SELECT sys_id, number, table_name, resource_type, state, short_desc, description,
                   assigned_to, parent_id, file_path, synced_at, sys_updated_on, etag,
                   in_scope, last_seen_at, tombstoned_at, pruned_at, raw_json
            FROM records
            WHERE in_scope = 1
              AND (resource_type IN ('business_application', 'cmdb_ci_business_app')
                   OR table_name = 'cmdb_ci_business_app')
            "#
        };
        let mut stmt = self.conn.prepare(sql)?;
        let rows = stmt.query_map([], row_to_record_row)?;
        let mut candidates = Vec::new();
        for row in rows {
            let row = row?;
            if self
                .list_business_application_fields(&row.sys_id)?
                .is_empty()
            {
                let _ = self.rebuild_business_application_projection_from_raw_json(&row)?;
            }
            let projection = self.get_business_application_projection(&row.sys_id)?;
            let fields = self
                .list_business_application_fields(&row.sys_id)?
                .into_iter()
                .map(|field| (field.field_name.clone(), field))
                .collect::<BTreeMap<_, _>>();
            if !business_application_matches_query(&row, projection.as_ref(), &fields, query) {
                continue;
            }
            candidates.push((row, projection, fields));
        }

        candidates.sort_by(|left, right| {
            compare_business_application_candidates(left, right, &query.sort)
                .then_with(|| {
                    left.1
                        .as_ref()
                        .map(|row| row.name.as_str())
                        .unwrap_or(left.0.short_desc.as_deref().unwrap_or(""))
                        .cmp(
                            right
                                .1
                                .as_ref()
                                .map(|row| row.name.as_str())
                                .unwrap_or(right.0.short_desc.as_deref().unwrap_or("")),
                        )
                })
                .then_with(|| left.0.sys_id.cmp(&right.0.sys_id))
        });

        Ok(candidates
            .into_iter()
            .skip(query.offset.unwrap_or(0))
            .take(limit)
            .map(|(row, _, _)| row)
            .collect())
    }

    fn find_record_sys_ids_by_enrichment(
        &self,
        table: &str,
        column: &str,
        value: &str,
        limit: usize,
    ) -> Result<Vec<String>> {
        let sql = format!(
            r#"
            SELECT DISTINCT records.sys_id
            FROM {table}
            INNER JOIN records ON records.sys_id = {table}.record_sys_id
            WHERE {table}.{column} = ?1
              AND records.in_scope = 1
            ORDER BY records.number, records.sys_id
            LIMIT ?2
            "#
        );

        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map(params![value, limit as i64], |row| row.get::<_, String>(0))?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    fn business_application_field_exists(&self, field: &str) -> Result<bool> {
        if matches!(
            field,
            "sys_id"
                | "number"
                | "name"
                | "short_description"
                | "description"
                | "business_owner"
                | "it_application_owner"
                | "managed_by_group"
                | "support_group"
                | "operational_status"
                | "portfolio"
                | "attested_date"
                | "sys_updated_on"
        ) {
            return Ok(true);
        }
        let count = self.conn.query_row(
            r#"
            SELECT
                (SELECT COUNT(*) FROM business_application_fields WHERE field_name = ?1)
              + (SELECT COUNT(*) FROM business_application_field_dictionary WHERE field_name = ?1)
            "#,
            params![field],
            |row| row.get::<_, i64>(0),
        )?;
        Ok(count > 0)
    }

    fn business_application_dictionary_map(
        &self,
    ) -> Result<HashMap<String, BusinessApplicationFieldDictionaryRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT table_name, field_name, field_label, field_type, reference_table, choice,
                   mandatory, read_only, max_length, active, synced_at, raw_json
            FROM business_application_field_dictionary
            WHERE table_name = 'cmdb_ci_business_app'
               OR table_name = 'task'
               OR table_name = 'cmdb_ci'
            "#,
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(BusinessApplicationFieldDictionaryRow {
                table_name: row.get(0)?,
                field_name: row.get(1)?,
                field_label: row.get(2)?,
                field_type: row.get(3)?,
                reference_table: row.get(4)?,
                choice: i64_to_bool(row.get(5)?),
                mandatory: i64_to_bool(row.get(6)?),
                read_only: i64_to_bool(row.get(7)?),
                max_length: row.get(8)?,
                active: i64_to_bool(row.get(9)?),
                synced_at: from_ts(row.get(10)?).map_err(to_sqlite_err)?,
                raw_json: row.get(11)?,
            })
        })?;
        let mut map = HashMap::new();
        for row in rows {
            let row = row?;
            map.insert(row.field_name.clone(), row);
        }
        Ok(map)
    }

    /// List all cached dictionary rows for a single table, ordered by field name.
    ///
    /// Returns an empty vector when the dictionary has never been synced for the
    /// table. Callers treat an empty result as a dictionary cache miss and fall
    /// back to baseline/observed behavior (degraded-read mode).
    pub fn list_business_application_field_dictionary(
        &self,
        table_name: &str,
    ) -> Result<Vec<BusinessApplicationFieldDictionaryRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT table_name, field_name, field_label, field_type, reference_table, choice,
                   mandatory, read_only, max_length, active, synced_at, raw_json
            FROM business_application_field_dictionary
            WHERE table_name = ?1
            ORDER BY field_name
            "#,
        )?;
        let rows = stmt.query_map(params![table_name], |row| {
            Ok(BusinessApplicationFieldDictionaryRow {
                table_name: row.get(0)?,
                field_name: row.get(1)?,
                field_label: row.get(2)?,
                field_type: row.get(3)?,
                reference_table: row.get(4)?,
                choice: i64_to_bool(row.get(5)?),
                mandatory: i64_to_bool(row.get(6)?),
                read_only: i64_to_bool(row.get(7)?),
                max_length: row.get(8)?,
                active: i64_to_bool(row.get(9)?),
                synced_at: from_ts(row.get(10)?).map_err(to_sqlite_err)?,
                raw_json: row.get(11)?,
            })
        })?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    /// Merged dictionary across the Business Application table and its inherited
    /// tables, keyed by field name. Child-table rows win over ancestor rows when
    /// the same field name appears at multiple levels (closest definition wins).
    ///
    /// Returns an empty map on a full dictionary cache miss.
    pub fn business_application_dictionary_for_tables(
        &self,
        tables: &[String],
    ) -> Result<HashMap<String, BusinessApplicationFieldDictionaryRow>> {
        // Walk from the most-derived table (index 0) to the base so earlier
        // entries are not overwritten by ancestor definitions.
        let mut map: HashMap<String, BusinessApplicationFieldDictionaryRow> = HashMap::new();
        for table in tables {
            for row in self.list_business_application_field_dictionary(table)? {
                map.entry(row.field_name.clone()).or_insert(row);
            }
        }
        Ok(map)
    }

    /// Delete relationships where the source record is tombstoned or pruned.
    pub fn cleanup_orphaned_relationships(&self) -> Result<usize> {
        let count = self.conn.execute(
            r#"
            DELETE FROM relationships
            WHERE source_id IN (
                SELECT sys_id FROM records WHERE in_scope = 0
            )
            "#,
            [],
        )?;
        Ok(count)
    }

    /// Delete references not linked to any active record's relationships.
    pub fn cleanup_orphaned_references(&self) -> Result<usize> {
        let count = self.conn.execute(
            r#"
            DELETE FROM "references"
            WHERE sys_id NOT IN (
                SELECT target_id FROM relationships
            )
            "#,
            [],
        )?;
        Ok(count)
    }

    /// Delete tags, keywords, and aliases for tombstoned or pruned records.
    pub fn cleanup_orphaned_enrichments(&self) -> Result<usize> {
        let mut total = 0;
        for table in ["record_tags", "record_keywords", "record_aliases"] {
            let count = self.conn.execute(
                &format!(
                    r#"DELETE FROM "{table}" WHERE record_sys_id IN (
                        SELECT sys_id FROM records WHERE in_scope = 0
                    )"#
                ),
                [],
            )?;
            total += count;
        }
        Ok(total)
    }
}
