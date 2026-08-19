pub fn expected_page() -> serde_json::Value {
    serde_json::json!({
        "records": [{
            "sys_id": "00000000000000000000000000000001",
            "number": "STRY1",
            "table": "rm_story",
            "resource_type": "Story",
            "state": "New",
            "short_description": "Typed story",
            "description": "",
            "fields": {
                "sys_id": { "value": "00000000000000000000000000000001", "display_value": null },
                "number": { "value": "STRY1", "display_value": null },
                "short_description": { "value": "Typed story", "display_value": null },
                "state": { "value": "1", "display_value": "New" }
            },
            "work_notes": [],
            "comments": [],
            "parent": null,
            "children": [],
            "references": {},
            "source": "Api"
        }],
        "next_cursor": null,
        "complete": true,
        "source": "live",
        "limit": 2,
        "rows_inspected": 1
    })
}
