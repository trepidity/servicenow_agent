use serde_json::{Value, json};

use crate::domain::policy::is_write_tool;
use crate::protocol::schema::{McpResourceDescriptor, McpToolDescriptor, ToolCapability};

#[derive(Debug, Clone, PartialEq)]
pub struct ToolMetadata {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
    pub output_schema: Value,
    pub default_enabled: bool,
    pub requires_confirmation: bool,
}

impl ToolMetadata {
    pub fn descriptor(&self) -> McpToolDescriptor {
        McpToolDescriptor {
            name: self.name.clone(),
            description: self.description.clone(),
            input_schema: self.input_schema.clone(),
            output_schema: self.output_schema.clone(),
            schema_version: "1.0".to_string(),
        }
    }

    pub fn capability(&self, enabled: bool) -> ToolCapability {
        ToolCapability {
            name: self.name.clone(),
            enabled,
            mode: if is_write_tool(&self.name) {
                "write".to_string()
            } else {
                "read".to_string()
            },
            read_only: !is_write_tool(&self.name),
            requires_confirmation: self.requires_confirmation,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ToolRegistry {
    tools: Vec<ToolMetadata>,
    resources: Vec<McpResourceDescriptor>,
}

impl Default for ToolRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ToolRegistry {
    pub fn new() -> Self {
        let mut registry = Self {
            tools: Vec::new(),
            resources: Vec::new(),
        };
        crate::tools::records::register(&mut registry);
        crate::tools::knowledge::register(&mut registry);
        crate::tools::catalog::register(&mut registry);
        crate::tools::change::register(&mut registry);
        crate::tools::resource_plan::register(&mut registry);
        crate::tools::story::register(&mut registry);
        crate::tools::timecard::register(&mut registry);
        crate::tools::work_note::register(&mut registry);
        crate::tools::plan_lifecycle::register(&mut registry);
        crate::tools::audit::register(&mut registry);
        crate::tools::governance::register(&mut registry);
        registry.resources = vec![
            McpResourceDescriptor {
                uri: "snow://records/{number}".to_string(),
                description: "Record as markdown".to_string(),
                mime_type: "text/markdown".to_string(),
            },
            McpResourceDescriptor {
                uri: "snow://knowledge/{number}".to_string(),
                description: "Knowledge article as markdown".to_string(),
                mime_type: "text/markdown".to_string(),
            },
            McpResourceDescriptor {
                uri: "snow://dashboard".to_string(),
                description: "Current user's task summary".to_string(),
                mime_type: "application/json".to_string(),
            },
        ];
        registry
    }

    pub fn add(&mut self, metadata: ToolMetadata) {
        if !self.tools.iter().any(|tool| tool.name == metadata.name) {
            self.tools.push(metadata);
        }
    }

    pub fn descriptors(&self) -> Vec<McpToolDescriptor> {
        self.tools.iter().map(ToolMetadata::descriptor).collect()
    }

    pub fn resources(&self) -> Vec<McpResourceDescriptor> {
        self.resources.clone()
    }

    pub fn metadata(&self) -> &[ToolMetadata] {
        &self.tools
    }
}

pub fn object_schema() -> Value {
    json!({"type":"object"})
}

pub fn number_arg_schema() -> Value {
    json!({"type":"object","properties":{"number":{"type":"string"}},"required":["number"]})
}
