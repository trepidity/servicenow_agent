use std::collections::{BTreeMap, HashMap};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use servicenow_rs::prelude::Record;

use crate::{
    CacheSource, FieldValue, Reference, SnowRecord, choose_reference_display_name,
    normalize_record_lookup_sys_id,
};

pub const SERVER_TABLE: &str = "cmdb_ci_server";
pub const LINUX_SERVER_TABLE: &str = "cmdb_ci_linux_server";
pub const WINDOWS_SERVER_TABLE: &str = "cmdb_ci_win_server";
pub const SERVER_RESOURCE_TYPE: &str = "server";
pub const SERVER_DEFAULT_LIMIT: usize = 20;
pub const SERVER_MAX_LIMIT: usize = 100;

pub const SERVER_TABLES: &[&str] = &[SERVER_TABLE, LINUX_SERVER_TABLE, WINDOWS_SERVER_TABLE];
pub const SERVER_LEAF_TABLES: &[&str] = &[LINUX_SERVER_TABLE, WINDOWS_SERVER_TABLE];

/// Common `cmdb_ci_server` subclasses shipped in baseline CMDB plus widely-used
/// OOTB extensions. These are the descendants of `cmdb_ci_server` that the
/// graph traversal must recognize as servers even though they are not in the
/// narrow [`SERVER_TABLES`] hydration alias list. The list is not meant to be
/// exhaustive of every custom subclass an instance may define; see
/// [`is_server_class`] for how the structural naming convention covers the long
/// tail without a per-class CMDB hierarchy lookup.
pub const SERVER_SUBCLASS_TABLES: &[&str] = &[
    "cmdb_ci_unix_server",
    "cmdb_ci_aix_server",
    "cmdb_ci_solaris_server",
    "cmdb_ci_hpux_server",
    "cmdb_ci_osx_server",
    "cmdb_ci_esx_server",
    "cmdb_ci_ucs_blade",
    "cmdb_ci_mainframe_hardware",
    "cmdb_ci_mainframe_lpar",
];

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Server {
    pub record: SnowRecord,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ip_address: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub class_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ci_owner_group: Option<Reference>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub support_group: Option<Reference>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operational_status: Option<FieldValue>,
    #[serde(default)]
    pub fields: BTreeMap<String, FieldValue>,
}

impl Server {
    pub fn from_servicenow(record: &Record) -> Result<Self> {
        let mut snow_record = SnowRecord::from_servicenow(record);
        snow_record.references = collect_server_references(record);
        snow_record.source = CacheSource::Api;

        Ok(Self {
            name: server_display_name(record),
            ip_address: first_record_value(record, &["ip_address"]),
            class_name: first_record_value(record, &["sys_class_name"]),
            ci_owner_group: snow_record.references.get("managed_by_group").cloned(),
            support_group: snow_record.references.get("support_group").cloned(),
            operational_status: snow_record.fields.get("operational_status").cloned(),
            fields: snow_record.fields.clone().into_iter().collect(),
            record: snow_record,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ServerLookup {
    SysId(String),
    ExactName(String),
    IpAddress(String),
}

impl ServerLookup {
    pub fn sys_id(sys_id: impl AsRef<str>) -> Result<Self> {
        Ok(Self::SysId(normalize_record_lookup_sys_id(
            sys_id.as_ref(),
        )?))
    }

    pub fn exact_name(name: impl Into<String>) -> Self {
        Self::ExactName(name.into())
    }

    pub fn ip_address(ip_address: impl Into<String>) -> Self {
        Self::IpAddress(ip_address.into())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct ServerSearchParams {
    /// Substring match against cmdb_ci_server.name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Exact match against cmdb_ci_server.ip_address.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ip_address: Option<String>,
    /// CI owner group display name or sys_id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ci_owner_group: Option<String>,
    /// Server class selector: linux, windows, or a supported cmdb_ci_* table.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub class: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
}

impl ServerSearchParams {
    pub fn validate(&self) -> Result<()> {
        self.validated_limit()?;
        if let Some(class) = self.class.as_deref() {
            canonical_server_class(class)?;
        }
        Ok(())
    }

    pub fn validated_limit(&self) -> Result<usize> {
        let limit = self.limit.unwrap_or(SERVER_DEFAULT_LIMIT);
        if limit == 0 {
            anyhow::bail!("`limit` must be at least 1");
        }
        if limit > SERVER_MAX_LIMIT {
            anyhow::bail!("`limit` must be at most {SERVER_MAX_LIMIT}");
        }
        Ok(limit)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct ServerQuery {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ip_address: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ci_owner_group: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub class: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<usize>,
}

impl ServerQuery {
    pub fn validate(&self) -> Result<()> {
        let limit = self.limit.unwrap_or(SERVER_DEFAULT_LIMIT);
        if limit == 0 {
            anyhow::bail!("`limit` must be at least 1");
        }
        if limit > 500 {
            anyhow::bail!("`limit` must be at most 500");
        }
        if let Some(class) = self.class.as_deref() {
            canonical_server_class(class)?;
        }
        Ok(())
    }
}

/// True when `value` names one of the canonical server hydration aliases
/// (`cmdb_ci_server`, `cmdb_ci_linux_server`, `cmdb_ci_win_server`).
///
/// This is the narrow exact-name check used where the canonical alias matters
/// (e.g. building hydration field projections). For deciding whether an
/// arbitrary CMDB class encountered during graph traversal is a server, use
/// [`is_server_class`], which is hierarchy-aware.
pub fn is_server_table(value: &str) -> bool {
    SERVER_TABLES.contains(&canonical_server_table_alias(value).as_str())
}

/// Hierarchy-aware server-class detection for CMDB graph traversal.
///
/// ServiceNow models CIs as a table-per-hierarchy rooted at `cmdb_ci`. Every
/// physical/virtual server class extends `cmdb_ci_server`, but the concrete
/// `sys_class_name` is the *leaf* subclass (`cmdb_ci_linux_server`,
/// `cmdb_ci_esx_server`, `cmdb_ci_aix_server`, ...). An exact-name allowlist
/// therefore silently drops real servers and traverses *through* them.
///
/// The fully general solution is to walk the `sys_db_object` super_class chain
/// for the encountered class and test descent from `cmdb_ci_server`. That costs
/// an extra CMDB metadata query per distinct class. To stay hierarchy-aware
/// without that per-class lookup, this function recognizes a server class when
/// any of the following hold:
///
/// 1. it is the base server table or a canonical alias ([`is_server_table`]);
/// 2. it is a known baseline/OOTB subclass ([`SERVER_SUBCLASS_TABLES`]); or
/// 3. it matches the structural naming convention used by server subclasses —
///    a `cmdb_ci_*` class whose name ends in `_server` (e.g. a custom
///    `cmdb_ci_acme_server`).
///
/// Rule (3) is the hierarchy-shaped backstop that covers the long tail of
/// instance-specific subclasses. The downstream hydration query targets the
/// base `cmdb_ci_server` table by `sys_id`, so a false positive here is
/// self-correcting: a non-server CI that slips through is simply not returned by
/// the base-table read (recorded as a missing CI) rather than corrupting results.
pub fn is_server_class(value: &str) -> bool {
    let normalized = canonical_server_table_alias(value);
    if SERVER_TABLES.contains(&normalized.as_str())
        || SERVER_SUBCLASS_TABLES.contains(&normalized.as_str())
    {
        return true;
    }
    // Structural convention: server subclasses are cmdb_ci_* tables whose name
    // ends in `_server`. Guard against the bare `cmdb_ci_server` having already
    // been handled above and against unrelated `cmdb_ci` tables.
    normalized.starts_with("cmdb_ci_") && normalized.ends_with("_server")
}

pub fn is_server_alias(value: &str) -> bool {
    let normalized = canonical_server_table_alias(value);
    normalized == SERVER_RESOURCE_TYPE || SERVER_TABLES.contains(&normalized.as_str())
}

pub fn canonical_server_table_alias(value: &str) -> String {
    match value
        .trim()
        .to_ascii_lowercase()
        .replace([' ', '-'], "_")
        .as_str()
    {
        "server" | "servers" => SERVER_RESOURCE_TYPE.to_string(),
        "linux" | "linux_server" | "linux_servers" => LINUX_SERVER_TABLE.to_string(),
        "windows" | "window" | "windows_server" | "windows_servers" | "win_server"
        | "win_servers" => WINDOWS_SERVER_TABLE.to_string(),
        other => other.to_string(),
    }
}

pub fn canonical_server_class(value: &str) -> Result<&'static str> {
    match canonical_server_table_alias(value).as_str() {
        LINUX_SERVER_TABLE => Ok(LINUX_SERVER_TABLE),
        WINDOWS_SERVER_TABLE => Ok(WINDOWS_SERVER_TABLE),
        SERVER_TABLE | SERVER_RESOURCE_TYPE => Ok(SERVER_TABLE),
        other => anyhow::bail!("unsupported server class `{other}`"),
    }
}

pub fn server_display_name(record: &Record) -> String {
    first_record_value(record, &["name", "fqdn", "dns_domain"])
        .unwrap_or_else(|| record.sys_id.clone())
}

pub fn server_number(record: &Record) -> String {
    record
        .get_raw("number")
        .or_else(|| record.get_display("number"))
        .or_else(|| record.get_str("number"))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| format!("SERVER:{}", record.sys_id))
}

pub fn server_state(record: &Record) -> String {
    first_record_value(record, &["operational_status", "install_status"]).unwrap_or_default()
}

pub fn server_description(record: &Record) -> String {
    first_record_value(record, &["short_description", "description", "fqdn"]).unwrap_or_default()
}

pub fn collect_server_references(record: &Record) -> HashMap<String, Reference> {
    let mut references = HashMap::new();
    for (field_name, table) in [
        ("managed_by_group", "sys_user_group"),
        ("support_group", "sys_user_group"),
        ("assigned_to", "sys_user"),
        ("owned_by", "sys_user"),
        ("location", "cmn_location"),
    ] {
        if let Some(reference) = reference_from_server_field(record, field_name, table) {
            references.insert(field_name.to_string(), reference);
        }
    }
    references
}

fn reference_from_server_field(
    record: &Record,
    field_name: &str,
    table: &str,
) -> Option<Reference> {
    let field_value = record.get(field_name)?;
    let sys_id = field_value
        .value
        .as_ref()
        .and_then(|value| value.as_str())
        .or_else(|| record.get_raw(field_name))
        .filter(|value| looks_like_sys_id(value))?
        .to_string();
    let display_name = choose_reference_display_name([
        field_value.display_value.as_deref(),
        record.get_str(&format!("{field_name}.name")),
        record.get_str(&format!("{field_name}.number")),
        record.get_str(&format!("{field_name}.user_name")),
        Some(sys_id.as_str()),
    ]);
    Some(Reference {
        sys_id,
        table: table.to_string(),
        display_name,
        extra: HashMap::new(),
    })
}

fn first_record_value(record: &Record, fields: &[&str]) -> Option<String> {
    fields.iter().find_map(|field| {
        record
            .get_display(field)
            .or_else(|| record.get_raw(field))
            .or_else(|| record.get_str(field))
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
    })
}

fn looks_like_sys_id(value: &str) -> bool {
    let value = value.trim();
    value.len() == 32 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}
