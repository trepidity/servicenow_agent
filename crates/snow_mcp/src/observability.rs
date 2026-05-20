use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HealthState {
    pub ready: bool,
    pub environment: String,
}

#[derive(Debug, Clone, Default)]
pub struct Metrics;
