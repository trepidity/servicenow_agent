//! `DescriptorService` — live typed-resource metadata discovery (FND-OPS-001).
//!
//! Discovery is **fail-closed**: every value returned here came from the
//! configured instance's `sys_dictionary` or `sys_choice`. Nothing is inferred
//! from a bundled schema, a display name, or a hardcoded field list. When the
//! instance returns nothing, or ACLs deny the read, the affected category is
//! reported as [`FieldSupport::Unavailable`] with a typed reason rather than as
//! an empty success.
//!
//! This distinction matters more than it looks: `Available(vec![])` tells a
//! consumer "this table has no writable fields", while `Unavailable` tells it
//! "we could not find out". Conflating them would let a permissions problem
//! masquerade as a factual answer.

use anyhow::Result;
use servicenow_rs::prelude::{DisplayValue, Error as SnowApiError};

use crate::context::CoreContext;
use crate::helpers::{
    is_servicenow_acl_error, non_empty_owned, record_bool, record_field_display_or_raw,
    record_field_raw_or_display,
};
use crate::resource::descriptor::{
    FieldDescriptor, FieldSupport, OperationEnvelope, PagingSupport, ResourceDescriptor,
    UnavailableReason,
};
use crate::{FieldChoice, ResourceType};

/// The Incident table, fixed for the `incident_fields` operation.
///
/// Bound as a constant rather than a parameter: a caller-supplied table would
/// turn typed metadata discovery into the generic table browser this contract
/// explicitly forbids. Internal callers may make a record-derived field-support
/// probe only; that narrow capability does not expose table browsing on any
/// public transport.
pub const INCIDENT_TABLE: &str = "incident";

/// Named operation for Incident metadata discovery.
pub const INCIDENT_FIELDS_OPERATION: &str = "incident_fields";

/// Default rows requested per page by `incident_query`.
pub const INCIDENT_DEFAULT_PAGE_LIMIT: u16 = 50;

/// Largest page `incident_query` will request.
pub const INCIDENT_MAX_PAGE_LIMIT: u16 = 200;

/// Upper bound on dictionary rows fetched per table level.
const DICTIONARY_ROW_LIMIT: u32 = 2000;

/// Live typed-resource metadata discovery.
#[derive(Clone)]
pub(crate) struct DescriptorService {
    ctx: CoreContext,
}

impl DescriptorService {
    pub(crate) fn new(ctx: CoreContext) -> Self {
        Self { ctx }
    }

    /// Discover the Incident resource contract from the configured instance.
    ///
    /// Always live: metadata is not a cache-eligible object under
    /// `#cache-policy-decisions`, so this performs no cache read or write.
    ///
    /// # Errors
    ///
    /// Returns an error only when the transport itself fails. An ACL denial or
    /// an empty dictionary is reported inside the descriptor as an unavailable
    /// category, not as an error, so a partially-readable instance still yields
    /// a truthful answer.
    pub(crate) async fn incident_descriptor(
        &self,
    ) -> Result<OperationEnvelope<ResourceDescriptor>> {
        let descriptor = self
            .descriptor_for(
                ResourceType::Incident,
                INCIDENT_TABLE,
                PagingSupport::Cursor {
                    default_limit: INCIDENT_DEFAULT_PAGE_LIMIT,
                    max_limit: INCIDENT_MAX_PAGE_LIMIT,
                },
            )
            .await?;
        // Metadata discovery is never served from cache, so the envelope is
        // unconditionally live and complete: there is no projection to narrow
        // and no page limit to truncate a descriptor.
        Ok(OperationEnvelope::live_complete(
            INCIDENT_FIELDS_OPERATION,
            descriptor,
        ))
    }

    /// Determine whether a record-derived table supports one field.
    ///
    /// This is deliberately narrower than exposing full descriptor discovery:
    /// callers receive only the requested support fact, and must never accept
    /// the table from an RPC or MCP request.
    pub(crate) async fn supports_field(
        &self,
        table: &str,
        field: &str,
    ) -> Result<FieldSupport<bool>> {
        let mut discovered_any_field = false;
        let mut current = table.to_string();
        let mut seen = std::collections::HashSet::from([current.clone()]);

        for _ in 0..8 {
            let records = match self
                .ctx
                .client
                .table("sys_dictionary")
                .equals("name", &current)
                .equals("active", "true")
                .display_value(DisplayValue::Both)
                .exclude_reference_link(true)
                .limit(DICTIONARY_ROW_LIMIT)
                .execute()
                .await
            {
                Ok(response) => response.records,
                Err(err) if is_acl_error(&err) => {
                    return Ok(FieldSupport::Unavailable {
                        reason: UnavailableReason::AclDenied,
                    });
                }
                Err(err) => return Err(err.into()),
            };

            for record in records {
                let Some(name) = non_empty_owned(record.get_raw("element"))
                    .or_else(|| non_empty_owned(record.get_str("element")))
                else {
                    continue;
                };
                discovered_any_field = true;
                if name == field {
                    return Ok(FieldSupport::available_value(true));
                }
            }

            let Some(parent) = self.ctx.table_parent(&current).await? else {
                break;
            };
            if !seen.insert(parent.clone()) {
                break;
            }
            current = parent;
        }

        if discovered_any_field {
            Ok(FieldSupport::available_value(false))
        } else {
            Ok(FieldSupport::Unavailable {
                reason: UnavailableReason::NotReturnedByInstance,
            })
        }
    }

    /// Build a descriptor for one typed family by reading its dictionary.
    async fn descriptor_for(
        &self,
        resource_type: ResourceType,
        table: &str,
        paging: PagingSupport,
    ) -> Result<ResourceDescriptor> {
        let fields = self.discover_fields(table).await?;
        let (readable_fields, writable_fields) = match fields {
            FieldSupport::Unavailable { reason } => (
                FieldSupport::Unavailable {
                    reason: reason.clone(),
                },
                FieldSupport::Unavailable { reason },
            ),
            FieldSupport::Available { value: rows } => {
                // Writable candidates are a strict subset of the discovered
                // dictionary fields, derived from the instance's own
                // `read_only` flag rather than from a curated allowlist. This
                // is structural metadata, not an attestation that record ACLs
                // authorize a later write.
                let writable = rows
                    .iter()
                    .filter(|row| !row.read_only)
                    .map(|row| row.descriptor.clone())
                    .collect::<Vec<_>>();
                let readable = rows
                    .into_iter()
                    .map(|row| row.descriptor)
                    .collect::<Vec<_>>();
                (
                    FieldSupport::available_value(readable),
                    FieldSupport::available_value(writable),
                )
            }
        };
        Ok(ResourceDescriptor {
            resource_type,
            table: table.to_string(),
            readable_fields,
            writable_fields,
            paging,
        })
    }

    /// Read `sys_dictionary` for a table and every table it inherits from.
    ///
    /// Inherited fields are attributed to the level that defines them and the
    /// most-derived definition wins, matching ServiceNow's own override
    /// semantics for a field redefined on a child table.
    async fn discover_fields(&self, table: &str) -> Result<FieldSupport<Vec<DiscoveredField>>> {
        let mut tables = vec![table.to_string()];
        tables.extend(self.ctx.table_ancestors(table).await?);

        let mut discovered: Vec<DiscoveredField> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for level in &tables {
            let records = match self
                .ctx
                .client
                .table("sys_dictionary")
                .equals("name", level)
                .equals("active", "true")
                .display_value(DisplayValue::Both)
                .exclude_reference_link(true)
                .limit(DICTIONARY_ROW_LIMIT)
                .execute()
                .await
            {
                Ok(response) => response.records,
                Err(err) if is_acl_error(&err) => {
                    return Ok(FieldSupport::Unavailable {
                        reason: UnavailableReason::AclDenied,
                    });
                }
                Err(err) => return Err(err.into()),
            };

            for record in records {
                // Collection/placeholder dictionary rows carry no `element`.
                let Some(name) = non_empty_owned(record.get_raw("element"))
                    .or_else(|| non_empty_owned(record.get_str("element")))
                else {
                    continue;
                };
                // Most-derived wins: the first level to define a field owns it.
                if !seen.insert(name.clone()) {
                    continue;
                }
                let Some(kind) = record_field_raw_or_display(&record, "internal_type") else {
                    // Without an internal type we cannot describe the field
                    // truthfully, and guessing `string` would be a fabrication.
                    continue;
                };
                let reference_table = non_empty_owned(record.get_raw("reference"))
                    .or_else(|| record_field_display_or_raw(&record, "reference"));
                discovered.push(DiscoveredField {
                    read_only: record_bool(&record, "read_only"),
                    is_choice: dictionary_flag_is_set(&record, "choice"),
                    descriptor: FieldDescriptor {
                        name,
                        label: record_field_display_or_raw(&record, "column_label"),
                        kind,
                        reference_table,
                        // Filled in below for choice fields only.
                        choices: FieldSupport::Unavailable {
                            reason: UnavailableReason::NotSupportedByOperation,
                        },
                    },
                });
            }
        }

        if discovered.is_empty() {
            return Ok(FieldSupport::Unavailable {
                reason: UnavailableReason::NotReturnedByInstance,
            });
        }

        // Choices are fetched only for fields the dictionary flags as choice
        // fields. Querying `sys_choice` for every field would be both wasteful
        // and misleading: an empty result for a non-choice field is not the
        // same fact as an empty choice list on a real choice field.
        for field in &mut discovered {
            if !field.is_choice {
                continue;
            }
            field.descriptor.choices = self
                .discover_choices(&tables, &field.descriptor.name)
                .await?;
        }

        discovered.sort_by(|left, right| left.descriptor.name.cmp(&right.descriptor.name));
        Ok(FieldSupport::available_value(discovered))
    }

    /// Read the active choice list for one field across its table hierarchy.
    ///
    /// The most-derived non-empty definition wins. ServiceNow commonly defines
    /// fields such as `state` on `task`, so stopping after an empty child-table
    /// result would incorrectly report inherited choices as unavailable.
    async fn discover_choices(
        &self,
        tables: &[String],
        field: &str,
    ) -> Result<FieldSupport<Vec<FieldChoice>>> {
        for table in tables {
            match self.ctx.field_choices_for_table(table, field).await {
                Ok(choices) if choices.is_empty() => continue,
                Ok(choices) => return Ok(FieldSupport::available_value(choices)),
                Err(err) => {
                    if let Some(api) = err.downcast_ref::<SnowApiError>()
                        && is_servicenow_acl_error(api)
                    {
                        return Ok(FieldSupport::Unavailable {
                            reason: UnavailableReason::AclDenied,
                        });
                    }
                    return Err(err);
                }
            }
        }

        // No hierarchy level returned a choice definition. This is not the
        // same fact as a choice field with a genuinely empty available list.
        Ok(FieldSupport::Unavailable {
            reason: UnavailableReason::NotReturnedByInstance,
        })
    }
}

/// A dictionary row plus the flags needed to classify it.
struct DiscoveredField {
    read_only: bool,
    is_choice: bool,
    descriptor: FieldDescriptor,
}

/// Interpret a `sys_dictionary` flag field.
///
/// `choice` is numeric (0/1/2/3) rather than boolean, so any non-empty,
/// non-zero value counts as set. Boolean-style flags are accepted too.
fn dictionary_flag_is_set(record: &servicenow_rs::prelude::Record, field: &str) -> bool {
    match record_field_raw_or_display(record, field) {
        Some(value) => {
            let value = value.trim().to_ascii_lowercase();
            !value.is_empty() && value != "0" && value != "false" && value != "no"
        }
        None => false,
    }
}

/// Whether a transport error is an ACL denial.
fn is_acl_error(err: &SnowApiError) -> bool {
    is_servicenow_acl_error(err)
}

#[cfg(test)]
#[path = "descriptor_tests.rs"]
mod tests;
