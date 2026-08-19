use super::*;

impl Store {
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
