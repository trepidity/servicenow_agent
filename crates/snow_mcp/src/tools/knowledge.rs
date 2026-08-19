use serde_json::json;

use crate::tools::registry::{ToolMetadata, ToolRegistry, object_schema};

pub fn register(registry: &mut ToolRegistry) {
    let tools = vec![
        ToolMetadata {
            name: "search_knowledge".to_string(),
            description: "Compatibility alias for knowledge_search; prefer knowledge_search for product-facing MCP calls".to_string(),
            input_schema: json!({"type":"object","properties":{"query":{"type":"string"},"knowledge_base":{"type":"string"},"category":{"type":"string"},"limit":{"type":"integer","minimum":1}},"required":["query"]}),
            output_schema: object_schema(),
            default_enabled: true,
            requires_confirmation: false,
        },
        ToolMetadata {
            name: "knowledge_search".to_string(),
            description: "Search knowledge articles with the canonical product MCP nomenclature".to_string(),
            input_schema: json!({"type":"object","properties":{"query":{"type":"string"},"knowledge_base":{"type":"string"},"category":{"type":"string"},"limit":{"type":"integer","minimum":1},"mode":{"type":"string","enum":["lexical","semantic","hybrid"]}},"required":["query"]}),
            output_schema: object_schema(),
            default_enabled: true,
            requires_confirmation: false,
        },
        ToolMetadata {
            name: "get_article".to_string(),
            description: "Compatibility alias for knowledge_fetch; prefer knowledge_fetch for product-facing MCP calls".to_string(),
            input_schema: json!({"type":"object","properties":{"number":{"type":"string"},"fresh":{"type":"boolean"}},"required":["number"]}),
            output_schema: object_schema(),
            default_enabled: true,
            requires_confirmation: false,
        },
        ToolMetadata {
            name: "knowledge_fetch".to_string(),
            description: "Fetch a full KB article by number with the canonical product MCP nomenclature".to_string(),
            input_schema: json!({"type":"object","properties":{"number":{"type":"string"},"fresh":{"type":"boolean"}},"required":["number"]}),
            output_schema: object_schema(),
            default_enabled: true,
            requires_confirmation: false,
        },
        ToolMetadata {
            name: "kb_semantic_search".to_string(),
            description: "Search knowledge through the semantic KB surface.".to_string(),
            input_schema: json!({"type":"object","properties":{"query":{"type":"string"},"knowledge_base":{"type":"string"},"category":{"type":"string"},"limit":{"type":"integer","minimum":1},"mode":{"type":"string","enum":["lexical","semantic","hybrid"]},"min_score_millis":{"type":"integer","minimum":0}},"required":["query"]}),
            output_schema: object_schema(),
            default_enabled: true,
            requires_confirmation: false,
        },
        ToolMetadata {
            name: "knowledge_answer".to_string(),
            description:
                "Return KB-grounded article excerpts and citation hashes for an answer draft"
                    .to_string(),
            input_schema: json!({"type":"object","properties":{"query":{"type":"string"},"limit":{"type":"integer","minimum":1}},"required":["query"]}),
            output_schema: object_schema(),
            default_enabled: true,
            requires_confirmation: false,
        },
        ToolMetadata {
            name: "knowledge_grounded_plan".to_string(),
            description: "Build an operation plan only when KB evidence is sufficient".to_string(),
            input_schema: json!({"type":"object","properties":{"query":{"type":"string"},"intent":{"type":"string"}},"required":["query"]}),
            output_schema: object_schema(),
            default_enabled: true,
            requires_confirmation: false,
        },
        ToolMetadata {
            name: "list_knowledge_bases".to_string(),
            description: "List knowledge bases available in the local cache".to_string(),
            input_schema: object_schema(),
            output_schema: object_schema(),
            default_enabled: true,
            requires_confirmation: false,
        },
        ToolMetadata {
            name: "list_categories".to_string(),
            description: "List categories for a knowledge base".to_string(),
            input_schema: json!({"type":"object","properties":{"knowledge_base_sys_id":{"type":"string"}},"required":["knowledge_base_sys_id"]}),
            output_schema: object_schema(),
            default_enabled: true,
            requires_confirmation: false,
        },
        ToolMetadata {
            name: "list_knowledge_articles".to_string(),
            description:
                "List knowledge articles with optional knowledge base and category filters"
                    .to_string(),
            input_schema: json!({"type":"object","properties":{"knowledge_base_sys_id":{"type":"string"},"category_sys_id":{"type":"string"},"limit":{"type":"integer","minimum":1}}}),
            output_schema: object_schema(),
            default_enabled: true,
            requires_confirmation: false,
        },
        ToolMetadata {
            name: "vault_path".to_string(),
            description: "Return the daemon vault root path".to_string(),
            input_schema: object_schema(),
            output_schema: object_schema(),
            default_enabled: true,
            requires_confirmation: false,
        },
        ToolMetadata {
            name: "kb_sync".to_string(),
            description: "Invoke the KB sync surface with full or incremental options.".to_string(),
            input_schema: json!({"type":"object","properties":{"full":{"type":"boolean"},"with_bodies":{"type":"boolean"}}}),
            output_schema: object_schema(),
            default_enabled: true,
            requires_confirmation: false,
        },
        ToolMetadata {
            name: "kb_list_tags".to_string(),
            description: "List aggregated KB tags from the local cache.".to_string(),
            input_schema: json!({"type":"object","properties":{"layer":{"type":"string","enum":["all","sn","auto","user"]},"min_count":{"type":"integer","minimum":1}}}),
            output_schema: object_schema(),
            default_enabled: true,
            requires_confirmation: false,
        },
        ToolMetadata {
            name: "kb_status".to_string(),
            description:
                "Show KB sync timestamps and cache coverage from the local SQLite catalog."
                    .to_string(),
            input_schema: object_schema(),
            output_schema: object_schema(),
            default_enabled: true,
            requires_confirmation: false,
        },
        ToolMetadata {
            name: "kb_semantic_status".to_string(),
            description: "Show semantic KB index coverage and rebuild health.".to_string(),
            input_schema: object_schema(),
            output_schema: object_schema(),
            default_enabled: true,
            requires_confirmation: false,
        },
        ToolMetadata {
            name: "kb_semantic_rebuild".to_string(),
            description: "Rebuild the semantic KB index.".to_string(),
            input_schema: json!({"type":"object","properties":{"full":{"type":"boolean"}}}),
            output_schema: object_schema(),
            default_enabled: true,
            requires_confirmation: false,
        },
        ToolMetadata {
            name: "repair_vault".to_string(),
            description: "Repair missing vault files from cached runtime data".to_string(),
            input_schema: object_schema(),
            output_schema: object_schema(),
            default_enabled: true,
            requires_confirmation: false,
        },
        ToolMetadata {
            name: "verify_vault".to_string(),
            description: "Verify vault and cache parity".to_string(),
            input_schema: object_schema(),
            output_schema: object_schema(),
            default_enabled: true,
            requires_confirmation: false,
        },
    ];
    for tool in tools {
        registry.add(tool);
    }
}
