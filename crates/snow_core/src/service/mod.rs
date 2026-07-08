//! Per-domain services extracted from the `SnowCore` god-object.
//!
//! Each service holds a cloned [`crate::context::CoreContext`] and exposes the
//! public API methods for its domain. `SnowCore` composes the services and
//! delegates to them (see `lib.rs`). This module is the home for the
//! extractions performed across Tasks 8-11 of the library boundary migration;
//! `user` (Task 8) is the first, `approval` (Task 9) is the second.

pub(crate) mod approval;
pub(crate) mod user;
pub(crate) use approval::ApprovalService;
pub(crate) use user::UserService;
