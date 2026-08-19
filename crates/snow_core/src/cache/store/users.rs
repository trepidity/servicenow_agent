use super::*;

impl Store {
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
}
