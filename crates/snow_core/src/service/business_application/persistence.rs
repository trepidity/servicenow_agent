use super::*;

impl BusinessApplicationService {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn persist_business_application_server_traversal(
        &self,
        application: &SnowRecord,
        servers: &[Server],
        server_provenance: &BTreeMap<String, BusinessApplicationServerProvenance>,
        server_paths: &BTreeMap<String, Vec<BusinessApplicationServerPath>>,
        run_started_at: DateTime<Utc>,
        options: &BusinessApplicationServersOptions,
        summary: &mut BusinessApplicationServersSummary,
    ) -> Result<()> {
        self.ctx
            .persist_snow_records(std::slice::from_ref(application))?;
        let server_records = servers
            .iter()
            .map(|server| server.record.clone())
            .collect::<Vec<_>>();
        summary.persisted_servers = self.ctx.persist_snow_records(&server_records)?;

        let store = self.ctx.query.store();
        let now = Utc::now();
        for server in servers {
            let paths = server_paths
                .get(&server.record.sys_id)
                .cloned()
                .unwrap_or_default();
            let min_depth = paths
                .iter()
                .map(BusinessApplicationServerPath::depth)
                .min()
                .unwrap_or(0);
            let provenance = server_provenance
                .get(&server.record.sys_id)
                .copied()
                .unwrap_or(BusinessApplicationServerProvenance::Relationship);
            let server_table = server
                .class_name
                .as_deref()
                .and_then(|value| non_empty_owned(Some(value)))
                .or_else(|| non_empty_owned(Some(server.record.table.as_str())))
                .unwrap_or_else(|| SERVER_TABLE.to_string());
            let membership = BusinessApplicationServerMembershipRow {
                ba_sys_id: application.sys_id.clone(),
                server_sys_id: server.record.sys_id.clone(),
                server_table,
                provenance: provenance.as_str().to_string(),
                min_depth,
                paths_json: serde_json::to_string(&paths)?,
                discovered_at: now,
                last_seen_at: now,
                tombstoned_at: None,
            };
            store.upsert_business_application_server_membership(&membership)?;
            summary.membership_upserts += 1;
        }

        if options.prune_stale {
            summary.membership_pruned = store
                .tombstone_stale_business_application_server_memberships(
                    application.sys_id.as_str(),
                    run_started_at,
                    Utc::now(),
                )?;
        }
        let run_completed_at = Utc::now();
        let service_membership_status =
            Self::business_application_service_membership_health_status(summary);
        let relationship_status = Self::business_application_relationship_health_status(summary);
        let inventory_status = Self::business_application_inventory_health_status(
            relationship_status,
            &service_membership_status,
            summary,
        );
        summary.service_membership_status = Some(service_membership_status.to_string());
        summary.relationship_status = Some(relationship_status.to_string());
        summary.inventory_status = Some(inventory_status.to_string());
        store.upsert_business_application_server_inventory_health(
            &BusinessApplicationServerInventoryHealthRow {
                ba_sys_id: application.sys_id.clone(),
                run_started_at,
                run_completed_at,
                service_membership_status: service_membership_status.to_string(),
                relationship_status: relationship_status.to_string(),
                inventory_status: inventory_status.to_string(),
                summary_json: serde_json::to_string(summary)?,
            },
        )?;
        Ok(())
    }

    pub(super) async fn persist_business_application_reference_primitives(
        &self,
        business_application: &mut BusinessApplication,
        options: &BusinessApplicationHydrationOptions,
    ) -> Result<()> {
        if !options.persist {
            return Ok(());
        }

        let mut diagnostics = Vec::new();
        let max_direct_references = 100usize;
        let total_references = business_application.references.len();

        for descriptor in business_application
            .references
            .iter_mut()
            .take(max_direct_references)
        {
            let can_resolve = options.resolve_references
                && options.reference_depth > 0
                && is_business_application_reference_table_resolvable(
                    descriptor.reference_table.as_str(),
                );

            if !can_resolve {
                if descriptor.resolution_status == ReferenceResolutionStatus::Resolved {
                    descriptor.resolution_status =
                        if is_business_application_reference_table_resolvable(
                            descriptor.reference_table.as_str(),
                        ) {
                            ReferenceResolutionStatus::Unresolved
                        } else {
                            ReferenceResolutionStatus::UnknownTable
                        };
                }
                let diagnostic = self.persist_reference_primitive_stub(descriptor, None)?;
                diagnostics.push(diagnostic);
                continue;
            }

            match self
                .ctx
                .client
                .table(descriptor.reference_table.as_str())
                .display_value(DisplayValue::Both)
                .exclude_reference_link(true)
                .get(descriptor.reference_sys_id.as_str())
                .await
            {
                Ok(record) => {
                    descriptor.resolution_status = ReferenceResolutionStatus::Resolved;
                    descriptor.diagnostic = None;
                    self.persist_resolved_reference_primitive(descriptor, &record)?;
                }
                Err(SnowApiError::Api { status: 404, .. }) => {
                    descriptor.resolution_status = ReferenceResolutionStatus::NotFound;
                    descriptor.diagnostic = Some("referenced record was not found".to_string());
                    diagnostics.push(self.persist_reference_primitive_stub(descriptor, None)?);
                }
                Err(err) => {
                    descriptor.resolution_status = ReferenceResolutionStatus::Error;
                    let message = err.to_string();
                    descriptor.diagnostic = Some(message.clone());
                    diagnostics
                        .push(self.persist_reference_primitive_stub(descriptor, Some(message))?);
                }
            }
        }

        if total_references > max_direct_references {
            diagnostics.push(ReferenceResolutionDiagnostic {
                field: "*".to_string(),
                reference_table: "*".to_string(),
                reference_sys_id: business_application.record.sys_id.clone(),
                display_value: Some(business_application.name.clone()),
                reason: ReferenceResolutionReason::FanoutLimitExceeded,
                message: Some(format!(
                    "resolved first {max_direct_references} Business Application references out of {total_references}"
                )),
            });
        }

        for diagnostic in diagnostics {
            push_unique_reference_diagnostic(
                &mut business_application.unresolved_references,
                diagnostic,
            );
        }

        Ok(())
    }

    pub(super) fn persist_resolved_reference_primitive(
        &self,
        descriptor: &ReferencePrimitiveDescriptor,
        record: &Record,
    ) -> Result<()> {
        let raw_json = serialize_record_document(record);
        let display_name = primitive_display_name(record, descriptor);
        let relative_path = self.persist_reference_primitive_markdown(
            descriptor,
            &display_name,
            ReferenceResolutionStatus::Resolved,
            &raw_json,
            None,
        )?;
        let synced_at = Utc::now();
        self.ctx
            .query
            .store()
            .upsert_primitive_object(&PrimitiveObjectRow {
                sys_id: descriptor.reference_sys_id.clone(),
                table_name: descriptor.reference_table.clone(),
                resource_type: primitive_resource_type_name(&descriptor.primitive_type).to_string(),
                display_name,
                number: record_first_value(record, &["number", "user_name"]).or_else(|| {
                    record
                        .get_raw("number")
                        .or_else(|| record.get_display("number"))
                        .map(ToOwned::to_owned)
                }),
                file_path: Some(relative_path.to_string_lossy().into_owned()),
                raw_json: raw_json.to_string(),
                synced_at,
                sys_updated_on: record
                    .get_raw("sys_updated_on")
                    .or_else(|| record.get_display("sys_updated_on"))
                    .or_else(|| record.get_str("sys_updated_on"))
                    .map(ToOwned::to_owned),
                resolution_status: PrimitiveResolutionStatus::Resolved,
                last_error: None,
            })?;

        if let Some(raw_map) = raw_json.as_object() {
            for (field_name, raw_value) in raw_map {
                let field = primitive_projected_field(
                    &descriptor.reference_sys_id,
                    field_name,
                    raw_value,
                    synced_at,
                );
                self.ctx
                    .query
                    .store()
                    .upsert_primitive_object_field(&descriptor.reference_sys_id, &field)?;
            }
        }
        Ok(())
    }

    pub(super) fn persist_reference_primitive_stub(
        &self,
        descriptor: &ReferencePrimitiveDescriptor,
        last_error: Option<String>,
    ) -> Result<ReferenceResolutionDiagnostic> {
        let status = descriptor.resolution_status;
        let display_name = descriptor
            .display_value
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(descriptor.reference_sys_id.as_str())
            .to_string();
        let raw_json = serde_json::json!({
            "sys_id": descriptor.reference_sys_id,
            "table": descriptor.reference_table,
            "display_value": display_name,
            "source_field": descriptor.field,
            "resolution_status": reference_resolution_status_name(status),
            "diagnostic": descriptor.diagnostic.as_deref().or(last_error.as_deref()),
        });
        let relative_path = self.persist_reference_primitive_markdown(
            descriptor,
            &display_name,
            status,
            &raw_json,
            descriptor.diagnostic.as_deref().or(last_error.as_deref()),
        )?;
        self.ctx
            .query
            .store()
            .upsert_primitive_object(&PrimitiveObjectRow {
                sys_id: descriptor.reference_sys_id.clone(),
                table_name: descriptor.reference_table.clone(),
                resource_type: primitive_resource_type_name(&descriptor.primitive_type).to_string(),
                display_name: display_name.clone(),
                number: None,
                file_path: Some(relative_path.to_string_lossy().into_owned()),
                raw_json: raw_json.to_string(),
                synced_at: Utc::now(),
                sys_updated_on: None,
                resolution_status: primitive_status_from_reference_status(status),
                last_error,
            })?;

        Ok(ReferenceResolutionDiagnostic {
            field: descriptor.field.clone(),
            reference_table: descriptor.reference_table.clone(),
            reference_sys_id: descriptor.reference_sys_id.clone(),
            display_value: descriptor.display_value.clone(),
            reason: reason_from_reference_status(status),
            message: descriptor.diagnostic.clone(),
        })
    }

    pub(super) fn persist_reference_primitive_markdown(
        &self,
        descriptor: &ReferencePrimitiveDescriptor,
        display_name: &str,
        status: ReferenceResolutionStatus,
        raw_json: &Value,
        diagnostic: Option<&str>,
    ) -> Result<PathBuf> {
        let relative_path = reference_primitive_relative_path(descriptor, display_name);
        let absolute_path = self.ctx.vault_path.join(&relative_path);
        let contents = render_reference_primitive_markdown(
            descriptor,
            display_name,
            status,
            raw_json,
            diagnostic,
        );
        let persisted = self
            .ctx
            .vault
            .write_markdown_file(absolute_path, &contents)?;
        Ok(persisted.relative_path)
    }
}
