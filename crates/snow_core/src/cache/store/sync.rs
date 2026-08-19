use super::*;

impl Store {
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
}
