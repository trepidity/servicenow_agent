use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HttpTransport {
    pub bind: HttpBindMode,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HttpBindMode {
    Loopback,
    Private,
}

impl Default for HttpTransport {
    fn default() -> Self {
        Self {
            bind: HttpBindMode::Loopback,
        }
    }
}
