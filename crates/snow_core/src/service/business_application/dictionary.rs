use super::*;

pub(super) fn is_business_application_reference_table_resolvable(table: &str) -> bool {
    matches!(table, "sys_user" | "sys_user_group" | "cmdb_ci")
        || table == BUSINESS_APPLICATION_TABLE
        || table.contains("portfolio")
}

/// Build a cached dictionary row from a `sys_dictionary` record.
///
/// Returns `None` for rows without a usable `element` (the ServiceNow field
/// name), which can happen for collection/placeholder dictionary rows. The
/// `table_name` is the table the query was scoped to so inherited fields are
/// attributed to the level that defines them.
pub(super) fn dictionary_row_from_record(
    table_name: &str,
    record: &Record,
    synced_at: DateTime<Utc>,
) -> Option<BusinessApplicationFieldDictionaryRow> {
    let field_name = non_empty_owned(record.get_raw("element"))
        .or_else(|| non_empty_owned(record.get_str("element")))?;
    let internal_type = record_field_raw_or_display(record, "internal_type");
    let reference_table = non_empty_owned(record.get_raw("reference"))
        .or_else(|| record_field_display_or_raw(record, "reference"));
    let raw_json = serde_json::json!({
        "element": field_name,
        "column_label": record_field_display_or_raw(record, "column_label"),
        "internal_type": internal_type,
        "reference": reference_table,
        "choice": record_field_raw_or_display(record, "choice"),
        "mandatory": record_field_raw_or_display(record, "mandatory"),
        "read_only": record_field_raw_or_display(record, "read_only"),
        "max_length": record_field_raw_or_display(record, "max_length"),
        "active": record_field_raw_or_display(record, "active"),
    })
    .to_string();
    Some(BusinessApplicationFieldDictionaryRow {
        table_name: table_name.to_string(),
        field_name,
        field_label: record_field_display_or_raw(record, "column_label"),
        field_type: internal_type,
        // `choice` in sys_dictionary is a numeric flag ("1"/"3" => choice list);
        // treat any non-empty, non-zero value as a choice field.
        reference_table,
        choice: dictionary_flag_is_set(record, "choice"),
        mandatory: record_bool(record, "mandatory"),
        read_only: record_bool(record, "read_only"),
        max_length: parse_i64(record_field_raw_or_display(record, "max_length").as_deref()),
        active: record_bool(record, "active"),
        synced_at,
        raw_json,
    })
}

/// Interpret a `sys_dictionary` flag field. `choice` is numeric (0/1/2/3); any
/// non-empty, non-"0" value counts as set. Boolean-style flags ("true") also
/// count.
pub(super) fn dictionary_flag_is_set(record: &Record, field: &str) -> bool {
    match record_field_raw_or_display(record, field) {
        Some(value) => {
            let value = value.trim().to_ascii_lowercase();
            !value.is_empty() && value != "0" && value != "false" && value != "no"
        }
        None => false,
    }
}

/// Promote the baseline alias map to dictionary-verified fields.
///
/// For each typed product label we keep the baseline ServiceNow field name when
/// the dictionary confirms it exists, otherwise we keep the baseline (the
/// dictionary may not expose every field to the authenticated account). The
/// Primary Portfolio reference target table is taken from the dictionary
/// `reference` value when present. `dictionary_version` is set to the latest
/// `synced_at` so callers can tell typed aliases were dictionary-verified.
pub(super) fn business_application_aliases_from_dictionary(
    dictionary: &HashMap<String, BusinessApplicationFieldDictionaryRow>,
) -> BusinessApplicationFieldAliases {
    let mut aliases = BusinessApplicationFieldAliases::baseline();

    // Helper: keep the baseline field name if the dictionary knows it; otherwise
    // fall back to the first dictionary field whose label matches the product
    // label. This lets instance-specific custom `u_*` fields supersede the baseline.
    let resolve = |baseline: &str, labels: &[&str]| -> String {
        if dictionary.contains_key(baseline) {
            return baseline.to_string();
        }
        dictionary
            .values()
            .find(|row| {
                row.field_label
                    .as_deref()
                    .map(|label| {
                        let label = label.trim().to_ascii_lowercase();
                        labels.iter().any(|candidate| label == *candidate)
                    })
                    .unwrap_or(false)
            })
            .map(|row| row.field_name.clone())
            .unwrap_or_else(|| baseline.to_string())
    };

    aliases.business_owner = resolve("business_owner", &["business owner"]);
    aliases.is_owner = resolve(
        "it_application_owner",
        &["is owner", "it application owner"],
    );
    aliases.ci_owner_group = resolve("managed_by_group", &["ci owner group"]);
    aliases.primary_support_group = resolve("support_group", &["primary support group"]);
    aliases.operational_state = resolve("operational_status", &["operational state"]);
    aliases.primary_portfolio = resolve("portfolio", &["primary portfolio"]);
    aliases.attested_date = resolve("attested_date", &["attested date"]);

    // Discover the Primary Portfolio reference target table from the dictionary.
    aliases.primary_portfolio_table = dictionary
        .get(&aliases.primary_portfolio)
        .and_then(|row| row.reference_table.clone())
        .filter(|table| !table.is_empty());

    aliases.dictionary_version = dictionary
        .values()
        .map(|row| row.synced_at)
        .max()
        .map(|synced_at| synced_at.to_rfc3339());

    aliases
}

/// Fetch live `sys_dictionary` metadata for `cmdb_ci_business_app` and its
/// inherited tables, then upsert the active rows into the
/// `business_application_field_dictionary` cache.
///
/// Returns the number of dictionary rows persisted. A failure to reach the
/// dictionary (or an empty result) is surfaced as an error/zero so callers
/// can stay in degraded-read mode; it must never abort a normal BA read.
impl BusinessApplicationService {
    pub async fn refresh_business_application_dictionary(&self) -> Result<usize> {
        let tables = self.business_application_dictionary_tables().await?;
        let synced_at = Utc::now();
        let mut persisted = 0usize;
        for table in &tables {
            // One query per table keeps each `name=<table>` scoped and lets a
            // single failing table degrade independently of the others.
            let records = self
                .ctx
                .client
                .table("sys_dictionary")
                .equals("name", table)
                .equals("active", "true")
                .display_value(DisplayValue::Both)
                .exclude_reference_link(true)
                .limit(2000)
                .execute()
                .await?
                .records;
            for record in records {
                let Some(row) = dictionary_row_from_record(table, &record, synced_at) else {
                    continue;
                };
                self.ctx
                    .query
                    .store()
                    .upsert_business_application_field_dictionary(&row)?;
                persisted += 1;
            }
        }
        Ok(persisted)
    }

    /// Read the cached, dictionary-verified field metadata for the Business
    /// Application table and its ancestors, keyed by ServiceNow field name.
    ///
    /// Returns an empty map on a dictionary cache miss (degraded-read mode).
    pub async fn business_application_dictionary(
        &self,
    ) -> Result<HashMap<String, BusinessApplicationFieldDictionaryRow>> {
        let tables = self.business_application_dictionary_tables().await?;
        Ok(self
            .ctx
            .query
            .store()
            .business_application_dictionary_for_tables(&tables)?)
    }

    /// Build the typed alias map for the Business Application primitive,
    /// promoting baseline aliases to dictionary-verified fields when cached
    /// `sys_dictionary` metadata is present.
    ///
    /// On a dictionary cache miss this returns
    /// [`BusinessApplicationFieldAliases::baseline_degraded`], which carries a
    /// `DictionaryUnavailable` diagnostic so the degradation is never silent.
    pub async fn business_application_aliases(&self) -> Result<BusinessApplicationFieldAliases> {
        let dictionary = self.business_application_dictionary().await?;
        if dictionary.is_empty() {
            return Ok(BusinessApplicationFieldAliases::baseline_degraded());
        }
        Ok(business_application_aliases_from_dictionary(&dictionary))
    }
}

impl BusinessApplicationService {
    /// The Business Application table and all of its inherited tables, most
    /// derived first. Used to scope `sys_dictionary` queries and dictionary
    /// cache lookups. Inheritance traversal is bounded to 8 levels by
    /// [`Self::table_ancestors`].
    pub(super) async fn business_application_dictionary_tables(&self) -> Result<Vec<String>> {
        let mut tables = vec![BUSINESS_APPLICATION_TABLE.to_string()];
        tables.extend(self.ctx.table_ancestors(BUSINESS_APPLICATION_TABLE).await?);
        Ok(tables)
    }

    /// Resolve the Business Application alias map for a hydration run, optionally
    /// refreshing the dictionary first.
    ///
    /// When `refresh_dictionary` is set, a best-effort live dictionary fetch runs
    /// before resolving so freshly verified instance field names take effect. A
    /// failure to refresh or an empty cache yields the degraded baseline aliases
    /// (carrying a `DictionaryUnavailable` diagnostic) so reads never fail.
    pub(super) async fn resolve_business_application_aliases(
        &self,
        refresh_dictionary: bool,
    ) -> BusinessApplicationFieldAliases {
        if refresh_dictionary {
            let _ = self.refresh_business_application_dictionary().await;
        }
        self.business_application_aliases()
            .await
            .unwrap_or_else(|_| BusinessApplicationFieldAliases::baseline_degraded())
    }
}
