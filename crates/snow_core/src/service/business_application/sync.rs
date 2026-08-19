use super::*;

impl BusinessApplicationService {
    /// Run a live Business Application search+persist and aggregate a sync summary.
    ///
    /// Reuses [`Self::search_business_applications_live`] for the actual fetch and
    /// persistence (honoring `options.persist`), then rolls up reference
    /// resolution and dictionary-degradation status across the returned
    /// applications. `params` is optional; `None` produces the default bounded
    /// Business Application search ordered by `name`.
    ///
    /// This is a degraded-tolerant read: unresolved references and an
    /// unavailable dictionary are reported in the summary, never errors.
    pub async fn sync_business_applications(
        &self,
        params: Option<BusinessApplicationSearchParams>,
        options: BusinessApplicationHydrationOptions,
    ) -> Result<BusinessApplicationSyncSummary> {
        let params = params.unwrap_or_default();
        let page_size = params.validated_limit()?;
        // When the caller explicitly asks for a dictionary refresh, attempt it
        // up front so degraded status reflects the freshest metadata. The fetch
        // is best-effort: a failure leaves us in baseline/degraded mode. We do
        // the refresh here (and clear the flag for the live search) so it runs
        // exactly once per sync.
        let mut dictionary_refreshed = false;
        let mut live_options = options.clone();
        if options.refresh_dictionary {
            dictionary_refreshed = self.refresh_business_application_dictionary().await.is_ok();
            live_options.refresh_dictionary = false;
        }

        let applications = self
            .search_business_applications_live(params, live_options)
            .await?;

        let mut summary = BusinessApplicationSyncSummary {
            all: false,
            table: BUSINESS_APPLICATION_TABLE.to_string(),
            page_size,
            pages: usize::from(!applications.is_empty()),
            total_returned: applications.len(),
            total_applications: applications.len(),
            persisted: if options.persist {
                applications.len()
            } else {
                0
            },
            dictionary_refreshed,
            ..Default::default()
        };

        roll_up_business_application_summary(&mut summary, &applications);

        Ok(summary)
    }

    /// Drain every live Business Application page and persist each page before
    /// requesting the next one.
    ///
    /// This explicit full-inventory path uses the `servicenow_rs` paginator
    /// directly. It deliberately avoids `execute_all()` so persistence remains
    /// durable page by page and the whole live result set is never required in
    /// memory.
    pub async fn sync_all_business_applications(
        &self,
        options: BusinessApplicationHydrationOptions,
    ) -> Result<BusinessApplicationSyncSummary> {
        let mut dictionary_refreshed = false;
        let mut live_options = options.clone();
        if options.refresh_dictionary {
            dictionary_refreshed = self.refresh_business_application_dictionary().await.is_ok();
            live_options.refresh_dictionary = false;
        }

        let aliases = self
            .resolve_business_application_aliases(live_options.refresh_dictionary)
            .await;
        let mut paginator = self
            .ctx
            .client
            .table(BUSINESS_APPLICATION_TABLE)
            .equals("sys_class_name", BUSINESS_APPLICATION_TABLE)
            .display_value(DisplayValue::Both)
            .exclude_reference_link(true)
            .limit(BUSINESS_APPLICATION_SYNC_ALL_PAGE_SIZE as u32)
            .order_by("name", Order::Asc)
            .order_by("sys_id", Order::Asc)
            .paginate()?;

        let mut summary = BusinessApplicationSyncSummary {
            all: true,
            table: BUSINESS_APPLICATION_TABLE.to_string(),
            page_size: BUSINESS_APPLICATION_SYNC_ALL_PAGE_SIZE,
            dictionary_refreshed,
            ..Default::default()
        };

        while let Some(page) = paginator.next_page().await? {
            if page.records.is_empty() {
                continue;
            }
            let applications = self
                .hydrate_business_application_page(page.records, &aliases, &live_options)
                .await?;

            summary.pages += 1;
            summary.total_returned += applications.len();
            summary.total_applications += applications.len();
            if options.persist {
                summary.persisted += applications.len();
            }
            roll_up_business_application_summary(&mut summary, &applications);
        }

        Ok(summary)
    }
}

impl BusinessApplicationService {
    pub(super) async fn hydrate_business_application_page(
        &self,
        records: Vec<Record>,
        aliases: &BusinessApplicationFieldAliases,
        options: &BusinessApplicationHydrationOptions,
    ) -> Result<Vec<BusinessApplication>> {
        let mut business_applications = Vec::with_capacity(records.len());
        for record in records {
            let mut business_application = BusinessApplication::from_servicenow(&record, aliases)?;
            if options.persist {
                self.ctx.persist_record(&record)?;
                self.persist_business_application_reference_primitives(
                    &mut business_application,
                    options,
                )
                .await?;
            }
            business_applications.push(business_application);
        }
        Ok(business_applications)
    }
}
