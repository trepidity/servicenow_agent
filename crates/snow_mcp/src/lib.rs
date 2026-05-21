#![allow(clippy::arc_with_non_send_sync)]

pub mod audit;
pub mod config;
pub mod daemon_bridge;
pub mod domain;
pub mod error;
pub mod observability;
pub mod planner;
pub mod protocol;
pub mod redact;
pub mod server;
pub mod tools;
pub mod transport;

pub use config::{McpConfig, McpEnvironment};
pub use daemon_bridge::{
    DaemonBackedMcpBridge, DaemonEndpoint, DaemonJsonRpcClient, LocalSocketDaemonJsonRpcClient,
    ProcessDaemonAutoSpawn, UnixDaemonJsonRpcClient, default_daemon_endpoint,
};
pub use error::{Error, Result};
pub use protocol::schema::{
    JsonRpcError, JsonRpcRequest, JsonRpcResponse, McpResourceDescriptor, McpToolDescriptor,
    PolicyDescribeReport, ToolCapabilitiesReport,
};
pub use server::{McpServer, McpServerBuilder, ServeOptions};
pub use transport::McpTransport;

pub mod prelude {
    pub use crate::config::{McpConfig, McpEnvironment};
    pub use crate::domain::prelude::*;
    pub use crate::server::{McpServer, McpServerBuilder, ServeOptions};
    pub use crate::transport::McpTransport;
}
