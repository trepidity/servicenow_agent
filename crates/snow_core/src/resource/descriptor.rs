//! Transport-neutral typed operation descriptor and response envelope.
//!
//! These are the FND-OPS-001 shared shapes named in
//! `docs/spec-servicenow-operational-capabilities.md#t-ops-01-metadata-envelope-and-operation-contract`.
//! Every selected operation returns the same envelope through CLI, daemon
//! JSON-RPC, direct MCP, and daemon-backed MCP, so a consumer sees identical
//! semantics regardless of transport.
//!
//! Two invariants are carried by the types rather than by prose, because prose
//! does not survive a refactor:
//!
//! - [`Source::Cache`] cannot be constructed without its refresh timestamp, so
//!   a cached response can never omit how stale it is.
//! - [`FieldSupport::Unavailable`] is a distinct state from
//!   `Available(vec![])`. "The instance did not return this category" and "the
//!   category is genuinely empty" are different facts and must never collapse
//!   into one another.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{FieldChoice, ResourceType};

/// Why a metadata category could not be discovered.
///
/// Discovery is fail-closed: Snow reports what the configured ServiceNow API
/// actually returned and never guesses support on the instance's behalf.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnavailableReason {
    /// The instance returned no metadata for this category, e.g. an empty
    /// `sys_dictionary` result for the table.
    NotReturnedByInstance,
    /// ServiceNow ACLs denied the metadata read for the configured identity.
    AclDenied,
    /// The category does not apply to this operation, e.g. choices on a field
    /// that is not a choice field.
    NotSupportedByOperation,
}

/// A discovered metadata category, or a typed reason it is unavailable.
///
/// Deliberately not `Option<T>` and not a bare `Vec<T>`: an empty vector means
/// "discovered, and genuinely empty", while [`FieldSupport::Unavailable`] means
/// "not discoverable". Collapsing the two is the specific failure the T-OPS-01
/// parity tests exist to catch.
///
/// Both variants are struct-shaped so the enum can be *internally* tagged. That
/// keeps the wire form flat — `{"status":"unavailable","reason":"acl_denied"}`
/// rather than burying `reason` inside a `value` object — which is what the
/// published capability contract documents.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "status")]
pub enum FieldSupport<T> {
    /// The instance returned this category.
    Available {
        /// The discovered value. May legitimately be empty.
        value: T,
    },
    /// The instance did not return this category, with the reason why.
    Unavailable {
        /// Why discovery could not produce a value.
        reason: UnavailableReason,
    },
}

impl<T> FieldSupport<T> {
    /// Wrap a discovered value.
    pub fn available_value(value: T) -> Self {
        Self::Available { value }
    }

    /// Borrow the discovered value, if this category was available.
    pub fn available(&self) -> Option<&T> {
        match self {
            Self::Available { value } => Some(value),
            Self::Unavailable { .. } => None,
        }
    }

    /// Whether this category was discovered at all.
    ///
    /// Note that an available category may still be empty; this answers
    /// "did the instance tell us?", not "is there anything in it?".
    pub fn is_available(&self) -> bool {
        matches!(self, Self::Available { .. })
    }
}

/// One discovered ServiceNow field.
///
/// Every value here comes from `sys_dictionary` or `sys_choice`. Fields the
/// instance did not return are omitted from the containing vector rather than
/// emitted with empty values, so a consumer can distinguish "absent" from
/// "present but blank".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldDescriptor {
    /// Native ServiceNow field name, never renamed or case-converted.
    pub name: String,
    /// Human-readable column label, omitted when the instance returned none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    /// Native ServiceNow internal type, e.g. `string`, `reference`, `glide_date_time`.
    pub kind: String,
    /// Target table for a reference field; absent for non-reference fields.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reference_table: Option<String>,
    /// Discovered choice list, or a typed reason there is none.
    pub choices: FieldSupport<Vec<FieldChoice>>,
}

/// Native ServiceNow pagination support for an operation.
///
/// Snow never fabricates pagination. An operation whose upstream offers none
/// reports [`PagingSupport::None`] rather than simulating pages locally.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "mode")]
pub enum PagingSupport {
    /// Cursor pagination with the row-count bounds the operation enforces.
    Cursor {
        /// Rows requested per page when the caller does not specify.
        default_limit: u16,
        /// Largest page the operation will request.
        max_limit: u16,
    },
    /// The operation is not paginated.
    None,
}

/// The discovered contract for one typed resource family.
///
/// `table` is fixed per named operation and is never caller-supplied; this is
/// what keeps metadata discovery from becoming a generic table browser.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceDescriptor {
    /// Typed resource family, not a free-form string.
    pub resource_type: ResourceType,
    /// The ServiceNow table this operation is permanently bound to.
    pub table: String,
    /// Fields visible through live dictionary metadata discovery.
    ///
    /// This is a structural candidate list, not proof that record-level ACLs
    /// authorize reading every field on every record.
    pub readable_fields: FieldSupport<Vec<FieldDescriptor>>,
    /// Dictionary-visible fields not marked `read_only` by the instance.
    ///
    /// This is a structural candidate list, not write authorization. The
    /// governed operation policy and ServiceNow ACLs still decide whether a
    /// later write is permitted.
    pub writable_fields: FieldSupport<Vec<FieldDescriptor>>,
    /// Native pagination support for the family's query operation.
    pub paging: PagingSupport,
}

/// Where a response's data came from.
///
/// [`Source::Cache`] carries a mandatory refresh timestamp. This is a type-level
/// guarantee, not a convention: there is no way to report a cached response
/// while omitting how old it is.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum Source {
    /// Read directly from ServiceNow.
    Live,
    /// Served from the local cache, with its last successful live refresh.
    Cache {
        /// When this cache entry last refreshed from ServiceNow.
        last_refreshed_at: DateTime<Utc>,
    },
}

/// Why a response does not represent the complete result set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartialReason {
    /// The local cache projection is intentionally narrower than live access.
    NarrowedProjection,
    /// The operation's page limit truncated the result.
    PageLimitReached,
    /// ServiceNow itself reported a truncated result.
    UpstreamTruncated,
}

/// Whether a response represents everything the caller asked for.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum Completeness {
    /// The response is the whole result.
    Complete,
    /// The response is partial, with the reason why.
    Partial {
        /// Why the result is incomplete.
        reason: PartialReason,
    },
}

/// The Snow-owned wrapper around a native ServiceNow payload.
///
/// Snow owns `operation`, `source`, and `completeness`. `data` stays native:
/// no transport may rename, flatten, case-convert, or default anything inside
/// it, because a consumer comparing two transports must see identical records.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationEnvelope<T> {
    /// The named operation that produced this response.
    pub operation: String,
    /// Where the data came from.
    pub source: Source,
    /// Whether the data is the complete result.
    pub completeness: Completeness,
    /// Native ServiceNow payload.
    pub data: T,
}

impl<T> OperationEnvelope<T> {
    /// Wrap a complete live result.
    pub fn live_complete(operation: impl Into<String>, data: T) -> Self {
        Self {
            operation: operation.into(),
            source: Source::Live,
            completeness: Completeness::Complete,
            data,
        }
    }

    /// Wrap a live result that the operation's page limit truncated.
    pub fn live_partial(operation: impl Into<String>, reason: PartialReason, data: T) -> Self {
        Self {
            operation: operation.into(),
            source: Source::Live,
            completeness: Completeness::Partial { reason },
            data,
        }
    }

    /// Wrap a cached result, which must state when it last refreshed.
    pub fn cached(
        operation: impl Into<String>,
        last_refreshed_at: DateTime<Utc>,
        completeness: Completeness,
        data: T,
    ) -> Self {
        Self {
            operation: operation.into(),
            source: Source::Cache { last_refreshed_at },
            completeness,
            data,
        }
    }
}
