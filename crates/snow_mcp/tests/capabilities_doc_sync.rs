//! Drift guards keeping `CAPABILITIES.md` and `policy.example.toml` honest
//! against the policy code. These fail CI when a write tool is added or a
//! default changes without the docs being updated.

use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;

use snow_mcp::domain::policy::{PolicyConfig, is_write_tool};
use snow_mcp::tools::ToolRegistry;

/// Tool names that `is_write_tool` matches explicitly (beyond the
/// `_apply_`/`_submit_` name patterns). Keep in sync with `is_write_tool`;
/// the test below proves every entry is still classified as a write.
const EXPLICIT_WRITE_TOOLS: &[&str] = &[
    "catalog_submit_request",
    "catalog_cancel_request",
    "work_note_apply_add",
    "approval_approve",
    "approval_reject",
    "plan_cancel",
];

fn crate_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn read_capabilities_doc() -> String {
    let path = crate_root().join("CAPABILITIES.md");
    fs::read_to_string(&path).unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()))
}

/// Every tool the code treats as a write must be named in CAPABILITIES.md, so
/// the human-facing write table can never silently omit a transaction. The
/// universe of write tools is: explicit matches in `is_write_tool` plus any
/// write-classified key shipped in the default policy.
#[test]
fn capabilities_doc_lists_every_write_tool() {
    let doc = read_capabilities_doc();

    let mut write_tools: BTreeSet<&str> = EXPLICIT_WRITE_TOOLS.iter().copied().collect();
    let default_tools = PolicyConfig::default().tools;
    for name in default_tools.keys() {
        if is_write_tool(name) {
            write_tools.insert(name.as_str());
        }
    }

    let missing: Vec<&str> = write_tools
        .iter()
        .copied()
        .filter(|name| !doc.contains(name))
        .collect();

    assert!(
        missing.is_empty(),
        "CAPABILITIES.md is missing write tool(s): {missing:?}. \
         Add them to crates/snow_mcp/CAPABILITIES.md when introducing or \
         renaming a write transaction."
    );
}

/// CAPABILITIES.md claims to be a complete inventory, so EVERY registered tool
/// must be named somewhere in it. This fails CI when a tool is added without a
/// corresponding doc entry (read or write).
#[test]
fn capabilities_doc_lists_every_registered_tool() {
    let doc = read_capabilities_doc();
    let registry = ToolRegistry::new();

    let missing: Vec<String> = registry
        .metadata()
        .iter()
        .map(|m| m.name.clone())
        .filter(|name| !doc.contains(name))
        .collect();

    assert!(
        missing.is_empty(),
        "CAPABILITIES.md is missing registered tool(s): {missing:?}. \
         Every tool must appear in crates/snow_mcp/CAPABILITIES.md."
    );
}

/// Guard the explicit list above against `is_write_tool` drifting.
#[test]
fn explicit_write_tools_are_classified_as_writes() {
    for tool in EXPLICIT_WRITE_TOOLS {
        assert!(
            is_write_tool(tool),
            "{tool} is in EXPLICIT_WRITE_TOOLS but is_write_tool() no longer classifies it as a write"
        );
    }
}

/// The shipped example policy must remain parseable by the real loader so the
/// deployer template never goes stale relative to the schema.
#[test]
fn example_policy_parses() {
    let path = crate_root().join("policy.example.toml");
    let input = fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));
    let cfg = PolicyConfig::from_toml_str(&input)
        .unwrap_or_else(|e| panic!("policy.example.toml failed to parse: {e}"));

    // Spot-check it exercises a write-tool override so the template stays useful.
    assert!(
        cfg.tools.contains_key("story_apply_create"),
        "example policy should demonstrate enabling a write tool"
    );

    for tool in ["change_request_apply_create", "change_request_apply_update"] {
        let policy = cfg
            .tools
            .get(tool)
            .unwrap_or_else(|| panic!("example policy missing {tool}"));
        assert!(
            policy.field_allowlist.contains("requested_by_date"),
            "example policy {tool} allowlist missing requested_by_date"
        );
    }

    let read_only_agent = cfg
        .roles
        .get("read_only_agent")
        .expect("example policy missing read_only_agent role");
    for tool in [
        "business_application_servers",
        "business_application_servers_cached",
        "business_applications_for_server",
    ] {
        assert!(
            read_only_agent.read_tools.contains(tool),
            "example policy read_only_agent role missing cache-safe BA relationship read tool {tool}"
        );
    }
}
