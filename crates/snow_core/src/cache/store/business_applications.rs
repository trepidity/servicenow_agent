use super::*;

impl Store {
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
}
