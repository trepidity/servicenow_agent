use anyhow::{Result, bail};
use chrono::Duration;

pub const STABLE_REFERENCE_CACHE_TTL_DAYS: i64 = 7;
pub const WORK_RECORD_CACHE_TTL_MINUTES: i64 = 60;
pub const DEFAULT_STABLE_REFERENCE_CACHE_TTL: &str = "7d";
pub const DEFAULT_WORK_RECORD_CACHE_TTL: &str = "60m";

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
    pub fn from_ttl_strings(stable_reference_ttl: &str, work_record_ttl: &str) -> Result<Self> {
        let stable_reference_ttl = parse_ttl_or_default(
            "cache.policy.stable_reference_ttl",
            stable_reference_ttl,
            DEFAULT_STABLE_REFERENCE_CACHE_TTL,
        )?;
        if stable_reference_ttl < stable_reference_ttl_minimum() {
            bail!(
                "cache.policy.stable_reference_ttl must be at least {}",
                DEFAULT_STABLE_REFERENCE_CACHE_TTL
            );
        }

        let work_record_ttl = parse_ttl_or_default(
            "cache.policy.work_record_ttl",
            work_record_ttl,
            DEFAULT_WORK_RECORD_CACHE_TTL,
        )?;

        Ok(Self {
            stable_reference_ttl,
            work_record_ttl,
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

fn stable_reference_ttl_minimum() -> Duration {
    stable_reference_ttl()
}

fn parse_ttl_or_default(name: &str, input: &str, default: &str) -> Result<Duration> {
    let value = if input.trim().is_empty() {
        default
    } else {
        input
    };
    parse_cache_ttl(value).ok_or_else(|| {
        anyhow::anyhow!(
            "{name} must be a positive duration ending in s, m, h, or d, e.g. 60m or 7d"
        )
    })
}

pub fn parse_cache_ttl(input: &str) -> Option<Duration> {
    let input = input.trim();
    if input.is_empty() {
        return None;
    }

    let digit_count = input
        .char_indices()
        .take_while(|(_, ch)| ch.is_ascii_digit())
        .map(|(idx, ch)| idx + ch.len_utf8())
        .last()?;
    let amount = input[..digit_count].parse::<i64>().ok()?;
    if amount <= 0 {
        return None;
    }
    let unit = input[digit_count..]
        .trim()
        .to_ascii_lowercase()
        .replace(' ', "");

    match unit.as_str() {
        "s" | "sec" | "secs" | "second" | "seconds" => Some(Duration::seconds(amount)),
        "m" | "min" | "mins" | "minute" | "minutes" => Some(Duration::minutes(amount)),
        "h" | "hr" | "hrs" | "hour" | "hours" => Some(Duration::hours(amount)),
        "d" | "day" | "days" => Some(Duration::days(amount)),
        _ => None,
    }
}
