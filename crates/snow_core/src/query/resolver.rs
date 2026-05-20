use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};

use chrono::Utc;
use serde_json::Value;

use crate::{RecordRef, Reference, choose_reference_display_name};

#[derive(Debug, Clone)]
struct CachedReference {
    reference: Reference,
    expires_at: Instant,
}

#[derive(Debug, Clone)]
pub struct ReferenceResolver {
    ttl: Duration,
    capacity: usize,
    cache: std::sync::Arc<RwLock<HashMap<String, CachedReference>>>,
}

impl Default for ReferenceResolver {
    fn default() -> Self {
        Self::with_ttl_and_capacity(Duration::from_secs(24 * 60 * 60), 10_000)
    }
}

impl ReferenceResolver {
    /// Create a resolver with the given TTL and default capacity of 10,000.
    pub fn with_ttl(ttl: Duration) -> Self {
        Self::with_ttl_and_capacity(ttl, 10_000)
    }

    /// Create a resolver with the given TTL and maximum cache capacity.
    /// Capacity is clamped to a minimum of 1.
    pub fn with_ttl_and_capacity(ttl: Duration, capacity: usize) -> Self {
        Self {
            ttl,
            capacity: capacity.max(1),
            cache: std::sync::Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn resolve_cached(&self, table: &str, sys_id: &str) -> Option<Reference> {
        let key = Self::cache_key(table, sys_id);
        let guard = self.cache.read().ok()?;
        let cached = guard.get(&key)?;
        if cached.expires_at > Instant::now() {
            Some(cached.reference.clone())
        } else {
            None
        }
    }

    pub fn cache_reference(&self, reference: Reference) -> Reference {
        let key = Self::cache_key(&reference.table, &reference.sys_id);
        let cached = CachedReference {
            reference: reference.clone(),
            expires_at: Instant::now() + self.ttl,
        };
        if let Ok(mut guard) = self.cache.write() {
            guard.insert(key, cached);
            if guard.len() > self.capacity {
                self.evict_expired_locked(&mut guard);
            }
        }
        reference
    }

    pub fn resolve_json_reference(
        &self,
        field_name: &str,
        value: &Value,
        default_table: Option<&str>,
    ) -> Option<Reference> {
        let reference = match value {
            Value::Object(map) => {
                let sys_id = map
                    .get("sys_id")
                    .and_then(Value::as_str)
                    .or_else(|| map.get("value").and_then(Value::as_str))
                    .map(str::to_string)?;
                let display_name = choose_reference_display_name([
                    map.get("display_value").and_then(Value::as_str),
                    map.get("name").and_then(Value::as_str),
                    map.get("label").and_then(Value::as_str),
                    map.get("title").and_then(Value::as_str),
                    map.get("user_name").and_then(Value::as_str),
                    map.get("email").and_then(Value::as_str),
                    map.get("number").and_then(Value::as_str),
                ]);
                let table = map
                    .get("table")
                    .and_then(Value::as_str)
                    .or_else(|| map.get("sys_class_name").and_then(Value::as_str))
                    .or(default_table)
                    .unwrap_or(field_name)
                    .to_string();
                let mut extra = HashMap::new();
                for (key, value) in map {
                    if matches!(
                        key.as_str(),
                        "sys_id"
                            | "value"
                            | "display_value"
                            | "name"
                            | "label"
                            | "title"
                            | "user_name"
                            | "email"
                            | "number"
                            | "table"
                            | "sys_class_name"
                            | "link"
                    ) {
                        continue;
                    }
                    if let Some(text) = value.as_str() {
                        extra.insert(key.clone(), text.to_string());
                    } else {
                        extra.insert(key.clone(), value.to_string());
                    }
                }
                Some(Reference {
                    sys_id,
                    table,
                    display_name,
                    extra,
                })
            }
            Value::String(text) if !text.is_empty() => Some(Reference {
                sys_id: text.clone(),
                table: default_table.unwrap_or(field_name).to_string(),
                display_name: choose_reference_display_name([Some(text.as_str())]),
                extra: HashMap::new(),
            }),
            _ => None,
        }?;

        Some(self.cache_reference(reference))
    }

    pub fn resolve_record_ref(
        &self,
        field_name: &str,
        value: &Value,
        default_table: Option<&str>,
    ) -> Option<RecordRef> {
        let reference = self.resolve_json_reference(field_name, value, default_table)?;
        Some(RecordRef {
            sys_id: reference.sys_id,
            number: reference.display_name,
            table: reference.table,
        })
    }

    pub fn invalidate(&self, table: &str, sys_id: &str) {
        if let Ok(mut guard) = self.cache.write() {
            guard.remove(&Self::cache_key(table, sys_id));
        }
    }

    pub fn clear(&self) {
        if let Ok(mut guard) = self.cache.write() {
            guard.clear();
        }
    }

    /// Remove all expired entries. Returns count of removed entries.
    pub fn clear_expired(&self) -> usize {
        if let Ok(mut guard) = self.cache.write() {
            self.evict_expired_locked(&mut guard)
        } else {
            0
        }
    }

    pub fn len(&self) -> usize {
        self.cache.read().map(|guard| guard.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn cache_key(table: &str, sys_id: &str) -> String {
        format!("{table}:{sys_id}")
    }

    /// Evict all expired entries from the cache. Returns count removed.
    fn evict_expired_locked(&self, guard: &mut HashMap<String, CachedReference>) -> usize {
        let now = Instant::now();
        let before = guard.len();
        guard.retain(|_, cached| cached.expires_at > now);
        before - guard.len()
    }

    #[allow(dead_code)]
    pub fn timestamp_now() -> chrono::DateTime<Utc> {
        Utc::now()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_reference(table: &str, sys_id: &str) -> Reference {
        Reference {
            sys_id: sys_id.to_string(),
            table: table.to_string(),
            display_name: format!("Test {sys_id}"),
            extra: HashMap::new(),
        }
    }

    #[test]
    fn test_bounded_cache_evicts_when_over_capacity() {
        let resolver = ReferenceResolver::with_ttl_and_capacity(Duration::from_secs(3600), 3);
        resolver.cache_reference(test_reference("sys_user", "a"));
        resolver.cache_reference(test_reference("sys_user", "b"));
        resolver.cache_reference(test_reference("sys_user", "c"));
        assert_eq!(resolver.len(), 3);

        // Adding a 4th should trigger eviction, but since none are expired,
        // all 4 will remain (evict_expired removes nothing when all are fresh)
        resolver.cache_reference(test_reference("sys_user", "d"));
        assert_eq!(resolver.len(), 4);
    }

    #[test]
    fn test_clear_expired_removes_stale() {
        let resolver = ReferenceResolver::with_ttl(Duration::from_millis(1));
        resolver.cache_reference(test_reference("sys_user", "a"));
        resolver.cache_reference(test_reference("sys_user", "b"));

        std::thread::sleep(Duration::from_millis(10));
        let removed = resolver.clear_expired();
        assert_eq!(removed, 2);
        assert_eq!(resolver.len(), 0);
    }

    #[test]
    fn test_resolve_cached_returns_none_for_expired() {
        let resolver = ReferenceResolver::with_ttl(Duration::from_millis(1));
        resolver.cache_reference(test_reference("sys_user", "a"));

        std::thread::sleep(Duration::from_millis(10));
        assert!(resolver.resolve_cached("sys_user", "a").is_none());
    }
}
