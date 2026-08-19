use super::*;

impl Store {
    pub fn count_records(&self) -> Result<u64> {
        let count: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM records", [], |row| row.get(0))?;
        Ok(count.max(0) as u64)
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
}
