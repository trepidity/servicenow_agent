use anyhow::{Context, Result, anyhow};
use interprocess::local_socket::tokio::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use snow_core::ipc::IpcEndpoint;
use snow_core::{
    KnowledgeSemanticSearchFilters, RecordLookup, ResourceType, SearchScope, SnowCore, SnowRecord,
    TaskSlaParentRef, query::filter::ListQuery,
};
use std::future::Future;
use std::io::ErrorKind;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::fs;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::signal;
use tokio::sync::Notify;
use tokio_util::task::TaskTracker;

use crate::transport::{
    DaemonBusinessApplicationDiagnostic, DaemonKnowledgeSemanticStatus, DaemonKnowledgeStatus,
    DaemonKnowledgeSyncOutcome, DaemonKnowledgeTagSummary, DaemonSemanticIndexSummary,
};
use crate::{DaemonState, transport::DaemonTransport};
use snow_core::vault::markdown::{
    render_approval_record, render_knowledge_article, render_snow_record,
};

mod method;
mod wire;

pub use method::RpcMethod;
pub use wire::{JsonRpcError, JsonRpcRequest, JsonRpcResponse};

mod router;
mod server;

pub(crate) use router::dispatch;
#[cfg(test)]
pub(crate) use server::handle_connection;
pub use server::{DEFAULT_IDLE_TIMEOUT, JsonRpcServer};

pub(crate) mod handlers;

#[cfg(test)]
mod business_application_cache_policy_tests;
#[cfg(test)]
mod cache_policy_tests;
#[cfg(test)]
mod catalog_cache_policy_tests;
#[cfg(test)]
mod incident_bulk_write_tests;
#[cfg(test)]
mod incident_fields_parity_tests;
#[cfg(test)]
mod incident_read_parity_tests;
#[cfg(test)]
mod resource_cache_policy_tests;
#[cfg(test)]
mod tests;
