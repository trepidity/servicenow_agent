use std::fmt;

#[derive(Debug)]
pub enum SnowError {
    AuthFailed(String),
    NotFound(String),
    UserNotFound(String),
    Network(String),
    Api(String),
}

impl fmt::Display for SnowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SnowError::AuthFailed(msg) => write!(f, "{msg}"),
            SnowError::NotFound(msg) => write!(f, "{msg}"),
            SnowError::UserNotFound(msg) => write!(f, "{msg}"),
            SnowError::Network(msg) => write!(f, "{msg}"),
            SnowError::Api(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for SnowError {}

impl From<servicenow_rs::error::Error> for SnowError {
    fn from(e: servicenow_rs::error::Error) -> Self {
        match e {
            servicenow_rs::error::Error::Auth { message, .. } => SnowError::AuthFailed(message),
            servicenow_rs::error::Error::Http(e) => SnowError::Network(e.to_string()),
            servicenow_rs::error::Error::RateLimited { retry_after } => {
                let msg = match retry_after {
                    Some(secs) => format!("Rate limited. Retry after {secs}s."),
                    None => "Rate limited by ServiceNow.".to_string(),
                };
                SnowError::Api(msg)
            }
            other => SnowError::Api(other.to_string()),
        }
    }
}

impl From<std::io::Error> for SnowError {
    fn from(e: std::io::Error) -> Self {
        SnowError::Api(e.to_string())
    }
}

impl From<anyhow::Error> for SnowError {
    fn from(e: anyhow::Error) -> Self {
        SnowError::Api(e.to_string())
    }
}
