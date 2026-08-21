//! Fixed-source, operation/object cache policy.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::RwLock;

use chrono::Duration;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

pub const CACHE_POLICY_FILENAME: &str = "cache-policy.toml";
pub const STABLE_REFERENCE_CACHE_TTL_DAYS: i64 = 7;
pub const WORK_RECORD_CACHE_TTL_MINUTES: i64 = 60;
pub const DEFAULT_STABLE_REFERENCE_CACHE_TTL: &str = "7d";
pub const DEFAULT_WORK_RECORD_CACHE_TTL: &str = "60m";
const MIN_TTL_SECONDS: i64 = 60;
const MAX_TTL_SECONDS: i64 = 365 * 24 * 60 * 60;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheMode {
    Live,
    ReadThrough,
    CacheOnly,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheRule {
    pub mode: CacheMode,
    pub ttl: Option<Duration>,
}
impl CacheRule {
    fn live() -> Self {
        Self {
            mode: CacheMode::Live,
            ttl: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CachePolicySource {
    BuiltInDefaults,
    BuiltInPlusFile,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CachePolicySummary {
    pub version: u32,
    pub source: CachePolicySource,
    pub rule_count: usize,
    pub fingerprint: String,
}

#[derive(Debug, Clone)]
pub struct EffectiveCachePolicy {
    source: CachePolicySource,
    objects: BTreeMap<String, CacheRule>,
    operations: BTreeMap<String, OperationRule>,
    knowledge_rebuild_base_sys_id: Option<String>,
    fingerprint: String,
}

#[derive(Debug, Clone)]
struct OperationRule {
    object: String,
    rule: CacheRule,
}

impl EffectiveCachePolicy {
    pub fn built_in() -> Self {
        Self::materialize(
            CachePolicySource::BuiltInDefaults,
            builtin_objects(),
            BTreeMap::new(),
            None,
        )
    }

    pub fn load(path: &Path) -> Result<Self, CachePolicyError> {
        let input = match fs::read_to_string(path) {
            Ok(input) => input,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Self::built_in()),
            Err(err) => {
                return Err(CachePolicyError::Io {
                    kind: io_kind(&err),
                    source: err,
                });
            }
        };
        let document: PolicyDocument =
            toml::from_str(&input).map_err(|err| CachePolicyError::Invalid {
                field: None,
                rule: None,
                reason: err.to_string(),
            })?;
        if document.version != 1 {
            return Err(invalid_field(
                "version",
                "only cache-policy version 1 is supported",
            ));
        }
        let known_objects = registered_objects();
        let operation_registry = registered_operations();
        let mut objects = builtin_objects();
        for (object, raw) in document.objects {
            if object.contains('*') || !known_objects.contains(object.as_str()) {
                return Err(invalid_rule(&object, "unknown object or wildcard"));
            }
            objects.insert(
                object.clone(),
                materialize_rule(&format!("objects.{object}"), raw)?,
            );
        }
        let mut operations = BTreeMap::new();
        for (operation, raw) in document.operations {
            if operation.contains('*') {
                return Err(invalid_rule(&operation, "wildcards are forbidden"));
            }
            let Some(expected_object) = operation_registry.get(operation.as_str()) else {
                return Err(invalid_rule(&operation, "unknown operation"));
            };
            if raw.object != *expected_object {
                return Err(invalid_rule(&operation, "operation/object mismatch"));
            }
            let rule = materialize_rule(&format!("operations.{operation}"), raw.rule)?;
            operations.insert(
                operation,
                OperationRule {
                    object: raw.object,
                    rule,
                },
            );
        }
        let knowledge_rebuild_base_sys_id = document
            .rebuild
            .knowledge
            .map(|scope| {
                normalize_sys_id(
                    "rebuild.knowledge.knowledge_base_sys_id",
                    &scope.knowledge_base_sys_id,
                )
            })
            .transpose()?;
        Ok(Self::materialize(
            CachePolicySource::BuiltInPlusFile,
            objects,
            operations,
            knowledge_rebuild_base_sys_id,
        ))
    }

    fn materialize(
        source: CachePolicySource,
        objects: BTreeMap<String, CacheRule>,
        operations: BTreeMap<String, OperationRule>,
        knowledge_rebuild_base_sys_id: Option<String>,
    ) -> Self {
        let canonical = canonical_rules(
            &objects,
            &operations,
            knowledge_rebuild_base_sys_id.as_deref(),
        );
        let fingerprint = Sha256::digest(canonical.as_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        Self {
            source,
            objects,
            operations,
            knowledge_rebuild_base_sys_id,
            fingerprint,
        }
    }

    pub fn summary(&self) -> CachePolicySummary {
        CachePolicySummary {
            version: 1,
            source: self.source,
            rule_count: self.objects.len()
                + self.operations.len()
                + usize::from(self.knowledge_rebuild_base_sys_id.is_some()),
            fingerprint: self.fingerprint.clone(),
        }
    }

    pub fn rule_for(&self, operation: &str, object: &str) -> CacheRule {
        self.operations
            .get(operation)
            .filter(|entry| entry.object == object)
            .map(|entry| entry.rule.clone())
            .or_else(|| self.objects.get(object).cloned())
            .unwrap_or_else(CacheRule::live)
    }

    pub fn knowledge_rebuild_base_sys_id(&self) -> Option<&str> {
        self.knowledge_rebuild_base_sys_id.as_deref()
    }
}

#[derive(Debug)]
pub struct CachePolicyManager {
    path: PathBuf,
    active: RwLock<EffectiveCachePolicy>,
}

impl CachePolicyManager {
    pub fn open(config_dir: &Path) -> Result<Self, CachePolicyError> {
        let path = config_dir.join(CACHE_POLICY_FILENAME);
        let active = EffectiveCachePolicy::load(&path)?;
        Ok(Self {
            path,
            active: RwLock::new(active),
        })
    }
    pub fn built_in(config_dir: &Path) -> Self {
        Self {
            path: config_dir.join(CACHE_POLICY_FILENAME),
            active: RwLock::new(EffectiveCachePolicy::built_in()),
        }
    }
    pub fn validate(&self) -> Result<CachePolicySummary, CachePolicyError> {
        Ok(EffectiveCachePolicy::load(&self.path)?.summary())
    }
    pub fn reload(&self) -> Result<CachePolicyReloadSummary, CachePolicyError> {
        let candidate = EffectiveCachePolicy::load(&self.path)?;
        let summary = candidate.summary();
        let mut active = self.active.write().map_err(|_| CachePolicyError::Invalid {
            field: None,
            rule: None,
            reason: "cache-policy state lock poisoned".to_string(),
        })?;
        let previous_fingerprint = active.fingerprint.clone();
        let changed = previous_fingerprint != summary.fingerprint;
        *active = candidate;
        Ok(CachePolicyReloadSummary {
            version: summary.version,
            source: summary.source,
            rule_count: summary.rule_count,
            previous_fingerprint,
            fingerprint: summary.fingerprint,
            changed,
        })
    }
    pub fn active(&self) -> EffectiveCachePolicy {
        self.active
            .read()
            .map(|value| value.clone())
            .unwrap_or_else(|_| EffectiveCachePolicy::built_in())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CachePolicyReloadSummary {
    pub version: u32,
    pub source: CachePolicySource,
    pub rule_count: usize,
    pub previous_fingerprint: String,
    pub fingerprint: String,
    pub changed: bool,
}

#[derive(Debug, Error)]
pub enum CachePolicyError {
    #[error("invalid cache policy: {reason}")]
    Invalid {
        field: Option<String>,
        rule: Option<String>,
        reason: String,
    },
    #[error("cache policy I/O failed: {source}")]
    Io {
        kind: String,
        #[source]
        source: std::io::Error,
    },
}

#[derive(Debug, Error)]
#[error("cache miss for {operation}/{object}")]
pub struct CacheMiss {
    pub operation: &'static str,
    pub object: &'static str,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PolicyDocument {
    version: u32,
    #[serde(default)]
    objects: BTreeMap<String, RawRule>,
    #[serde(default)]
    operations: BTreeMap<String, RawOperationRule>,
    #[serde(default)]
    rebuild: RawRebuildPolicy,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRebuildPolicy {
    knowledge: Option<RawKnowledgeRebuildScope>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawKnowledgeRebuildScope {
    knowledge_base_sys_id: String,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRule {
    mode: CacheMode,
    ttl: Option<String>,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawOperationRule {
    object: String,
    #[serde(flatten)]
    rule: RawRule,
}

fn materialize_rule(name: &str, raw: RawRule) -> Result<CacheRule, CachePolicyError> {
    match raw.mode {
        CacheMode::Live => {
            if raw.ttl.is_some() {
                return Err(invalid_rule(name, "live mode forbids ttl"));
            }
            Ok(CacheRule::live())
        }
        CacheMode::ReadThrough | CacheMode::CacheOnly => {
            let ttl = raw
                .ttl
                .ok_or_else(|| invalid_rule(name, "cached mode requires ttl"))?;
            let duration = parse_cache_ttl(&ttl).ok_or_else(|| {
                invalid_rule(
                    name,
                    "ttl must be an integer s/m/h/d duration from 1m through 365d",
                )
            })?;
            Ok(CacheRule {
                mode: raw.mode,
                ttl: Some(duration),
            })
        }
    }
}

pub fn parse_cache_ttl(input: &str) -> Option<Duration> {
    if input.len() < 2 {
        return None;
    }
    let (amount, unit) = input.split_at(input.len() - 1);
    if amount.is_empty() || !amount.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let amount = amount.parse::<i64>().ok()?;
    let seconds = match unit {
        "s" => amount,
        "m" => amount.checked_mul(60)?,
        "h" => amount.checked_mul(3600)?,
        "d" => amount.checked_mul(86400)?,
        _ => return None,
    };
    (MIN_TTL_SECONDS..=MAX_TTL_SECONDS)
        .contains(&seconds)
        .then(|| Duration::seconds(seconds))
}

fn builtin_objects() -> BTreeMap<String, CacheRule> {
    [
        ("business_application", "30d"),
        ("knowledge", "7d"),
        ("server", "24h"),
        ("service_catalog_product", "30d"),
    ]
    .into_iter()
    .map(|(name, ttl)| {
        (
            name.to_string(),
            CacheRule {
                mode: CacheMode::ReadThrough,
                ttl: parse_cache_ttl(ttl),
            },
        )
    })
    .collect()
}
fn registered_objects() -> BTreeSet<&'static str> {
    [
        "approval",
        "business_application",
        "change_request",
        "change_task",
        "knowledge",
        "resource_plan",
        "server",
        "service_catalog_product",
        "story",
        "story_task",
        "timecard",
        "user",
    ]
    .into_iter()
    .collect()
}
fn registered_operations() -> BTreeMap<&'static str, &'static str> {
    [
        ("business_application_get", "business_application"),
        ("business_application_search", "business_application"),
        ("business_application_query", "business_application"),
        ("get_article", "knowledge"),
        ("search_knowledge", "knowledge"),
        ("list_knowledge_articles", "knowledge"),
        ("server_get", "server"),
        ("server_search", "server"),
        ("server_query", "server"),
        ("catalog_items_search", "service_catalog_product"),
        ("catalog_item_get", "service_catalog_product"),
    ]
    .into_iter()
    .collect()
}
fn canonical_rules(
    objects: &BTreeMap<String, CacheRule>,
    operations: &BTreeMap<String, OperationRule>,
    knowledge_rebuild_base_sys_id: Option<&str>,
) -> String {
    let mut output = String::new();
    for (name, rule) in objects {
        output.push_str(&format!(
            "object\t{name}\t{}\t{}\n",
            mode_name(rule.mode),
            ttl_seconds(rule)
        ));
    }
    for (name, operation) in operations {
        output.push_str(&format!(
            "operation\t{name}\t{}\t{}\t{}\n",
            operation.object,
            mode_name(operation.rule.mode),
            ttl_seconds(&operation.rule)
        ));
    }
    if let Some(sys_id) = knowledge_rebuild_base_sys_id {
        output.push_str(&format!(
            "rebuild\tknowledge\tknowledge_base_sys_id\t{sys_id}\n"
        ));
    }
    output
}

fn normalize_sys_id(field: &str, value: &str) -> Result<String, CachePolicyError> {
    if value.len() != 32 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(invalid_field(
            field,
            "must be exactly 32 hexadecimal characters",
        ));
    }
    Ok(value.to_ascii_lowercase())
}
fn mode_name(mode: CacheMode) -> &'static str {
    match mode {
        CacheMode::Live => "live",
        CacheMode::ReadThrough => "read_through",
        CacheMode::CacheOnly => "cache_only",
    }
}
fn ttl_seconds(rule: &CacheRule) -> i64 {
    rule.ttl.map_or(0, |ttl| ttl.num_seconds())
}
fn invalid_field(field: &str, reason: &str) -> CachePolicyError {
    CachePolicyError::Invalid {
        field: Some(field.to_string()),
        rule: None,
        reason: reason.to_string(),
    }
}
fn invalid_rule(rule: &str, reason: &str) -> CachePolicyError {
    CachePolicyError::Invalid {
        field: None,
        rule: Some(rule.to_string()),
        reason: reason.to_string(),
    }
}
fn io_kind(error: &std::io::Error) -> String {
    format!("{:?}", error.kind()).to_ascii_lowercase()
}

// Retained while the legacy cache readers are being routed through the named
// operation policy. They are not the source of the new effective policy.
pub fn stable_reference_ttl() -> Duration {
    Duration::days(STABLE_REFERENCE_CACHE_TTL_DAYS)
}
pub fn work_record_ttl() -> Duration {
    Duration::minutes(WORK_RECORD_CACHE_TTL_MINUTES)
}
pub fn is_within_ttl(synced_at: chrono::DateTime<chrono::Utc>, ttl: Duration) -> bool {
    synced_at + ttl > chrono::Utc::now()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheTtlPolicy {
    stable_reference_ttl: Duration,
    work_record_ttl: Duration,
}
impl CacheTtlPolicy {
    pub fn from_ttl_strings(stable: &str, work: &str) -> anyhow::Result<Self> {
        let stable = parse_legacy_ttl(stable, DEFAULT_STABLE_REFERENCE_CACHE_TTL)?;
        if stable < stable_reference_ttl() {
            anyhow::bail!(
                "cache.policy.stable_reference_ttl must be at least {DEFAULT_STABLE_REFERENCE_CACHE_TTL}"
            );
        }
        Ok(Self {
            stable_reference_ttl: stable,
            work_record_ttl: parse_legacy_ttl(work, DEFAULT_WORK_RECORD_CACHE_TTL)?,
        })
    }
    pub fn stable_reference_ttl(&self) -> Duration {
        self.stable_reference_ttl
    }
    pub fn work_record_ttl(&self) -> Duration {
        self.work_record_ttl
    }
}
impl Default for CacheTtlPolicy {
    fn default() -> Self {
        Self {
            stable_reference_ttl: stable_reference_ttl(),
            work_record_ttl: work_record_ttl(),
        }
    }
}
fn parse_legacy_ttl(input: &str, default: &str) -> anyhow::Result<Duration> {
    let value = if input.trim().is_empty() {
        default
    } else {
        input.trim()
    };
    let normalized = value.to_ascii_lowercase();
    let (digits, suffix) = normalized.split_at(normalized.len().saturating_sub(1));
    let amount = digits
        .parse::<i64>()
        .map_err(|_| anyhow::anyhow!("cache ttl must be a positive s/m/h/d duration"))?;
    let duration = match suffix {
        "s" => Duration::seconds(amount),
        "m" => Duration::minutes(amount),
        "h" => Duration::hours(amount),
        "d" => Duration::days(amount),
        _ => anyhow::bail!("cache ttl must be a positive s/m/h/d duration"),
    };
    if amount <= 0 {
        anyhow::bail!("cache ttl must be a positive s/m/h/d duration");
    }
    Ok(duration)
}
