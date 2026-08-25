use super::*;
use crate::CatalogItem;

impl Store {
    pub fn upsert_complete_catalog_product(
        &self,
        item: &CatalogItem,
        last_refreshed_at: DateTime<Utc>,
    ) -> Result<()> {
        let item_json = serde_json::to_string(item)?;
        self.conn.execute(
            r#"
            INSERT INTO catalog_products_complete (sys_id, item_json, last_refreshed_at)
            VALUES (?1, ?2, ?3)
            ON CONFLICT(sys_id) DO UPDATE SET
                item_json = excluded.item_json,
                last_refreshed_at = excluded.last_refreshed_at
            "#,
            params![&item.sys_id, item_json, to_ts(last_refreshed_at)],
        )?;
        Ok(())
    }

    pub fn get_complete_catalog_product(
        &self,
        sys_id: &str,
    ) -> Result<Option<CatalogProductProjectionRow>> {
        let stored = self
            .conn
            .query_row(
                r#"
                SELECT item_json, last_refreshed_at
                FROM catalog_products_complete
                WHERE sys_id = ?1
                "#,
                params![sys_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
            )
            .optional()?;
        stored
            .map(|(item_json, last_refreshed_at)| {
                Ok(CatalogProductProjectionRow {
                    item: serde_json::from_str(&item_json)?,
                    last_refreshed_at: from_ts(last_refreshed_at)?,
                })
            })
            .transpose()
    }

    pub fn upsert_narrowed_catalog_product(
        &self,
        item: &CatalogItem,
        last_refreshed_at: DateTime<Utc>,
    ) -> Result<()> {
        let item_json = serde_json::to_string(item)?;
        self.conn.execute(
            r#"
            INSERT INTO catalog_products_narrowed (
                sys_id, name, short_description, item_json, last_refreshed_at
            ) VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(sys_id) DO UPDATE SET
                name = excluded.name,
                short_description = excluded.short_description,
                item_json = excluded.item_json,
                last_refreshed_at = excluded.last_refreshed_at
            "#,
            params![
                &item.sys_id,
                &item.name,
                &item.short_description,
                item_json,
                to_ts(last_refreshed_at)
            ],
        )?;
        Ok(())
    }

    pub fn search_narrowed_catalog_products(
        &self,
        query: &str,
        limit: usize,
    ) -> Result<Vec<CatalogProductProjectionRow>> {
        let pattern = format!("%{}%", query.trim());
        let mut statement = self.conn.prepare(
            r#"
            SELECT item_json, last_refreshed_at
            FROM catalog_products_narrowed
            WHERE lower(name) LIKE lower(?1)
               OR lower(short_description) LIKE lower(?1)
            ORDER BY name, sys_id
            LIMIT ?2
            "#,
        )?;
        let rows = statement.query_map(params![pattern, limit as i64], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?;
        rows.map(|stored| {
            let (item_json, last_refreshed_at) = stored?;
            Ok(CatalogProductProjectionRow {
                item: serde_json::from_str(&item_json)?,
                last_refreshed_at: from_ts(last_refreshed_at)?,
            })
        })
        .collect()
    }

    pub fn delete_catalog_product_projections(&self, sys_id: &str) -> Result<()> {
        self.conn.execute(
            "DELETE FROM catalog_products_complete WHERE sys_id = ?1",
            params![sys_id],
        )?;
        self.conn.execute(
            "DELETE FROM catalog_products_narrowed WHERE sys_id = ?1",
            params![sys_id],
        )?;
        Ok(())
    }
}
