//! `kb` — public compatibility shim for the knowledge domain.
//!
//! The knowledge implementation (the `KnowledgeService`, its domain types, and
//! the sync/tag helper graph) moved to `crate::service::knowledge` in Task 11 of
//! the library boundary migration. This module re-exports the previously-public
//! knowledge status/sync types so the existing `snow_core::kb::*` paths — checked
//! by `tests/api_compat.rs` — remain valid.

pub use crate::service::knowledge::{
    KnowledgeStatus, KnowledgeSyncMode, KnowledgeSyncOutcome, KnowledgeTagLayer,
    KnowledgeTagSummary,
};
