use crate::Result;
use crate::domain::audit::AuditEvent;

pub struct JsonLinesExporter;
pub struct MarkdownExporter;

impl JsonLinesExporter {
    pub fn render(events: &[AuditEvent]) -> Result<String> {
        let mut out = String::new();
        for event in events {
            out.push_str(&serde_json::to_string(event)?);
            out.push('\n');
        }
        Ok(out)
    }
}

impl MarkdownExporter {
    pub fn render(events: &[AuditEvent]) -> String {
        let mut out = String::from("# MCP Audit Events\n\n");
        for event in events {
            out.push_str(&format!(
                "## {}\n\n- tool: `{}`\n- status: `{:?}`\n- actor: `{}`\n\n",
                event.audit_id, event.tool, event.result_status, event.actor.subject
            ));
        }
        out
    }
}
