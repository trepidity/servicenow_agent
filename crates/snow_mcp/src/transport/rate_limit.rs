use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TokenBucketConfig {
    pub read_per_minute: u32,
    pub write_per_minute: u32,
}

impl Default for TokenBucketConfig {
    fn default() -> Self {
        Self {
            read_per_minute: 60,
            write_per_minute: 10,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RateLimitClass {
    Read,
    Write,
    FailedConfirmation,
}

#[derive(Debug, Clone, Default)]
pub struct RateLimiterRegistry {
    pub config: TokenBucketConfig,
}
