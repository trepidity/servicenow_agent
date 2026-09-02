//! Exact-record export command and durable format renderers.

use super::super::*;
use rust_xlsxwriter::Workbook;
use serde_json::{Map, Value};
use std::fmt::Write as _;
use std::io::Write as _;
use std::path::Path;

/// Reads one explicitly selected ACL-readable record and writes it atomically
/// in the requested durable format.
pub(crate) async fn cmd_export(
    client: &ServiceNowClient,
    table: ExportTable,
    number: Option<&str>,
    sys_id: Option<&str>,
    format: ExportFormat,
    output: &Path,
) -> Result<(), SnowError> {
    let record = match (number, sys_id) {
        (Some(number), None) => client
            .table(table.table_name())
            .equals("number", number)
            .display_value(DisplayValue::Both)
            .limit(1)
            .first()
            .await?
            .ok_or_else(|| {
                SnowError::NotFound(format!("{number} not found in {}", table.table_name()))
            })?,
        (None, Some(sys_id)) => {
            client
                .table(table.table_name())
                .display_value(DisplayValue::Both)
                .get(sys_id)
                .await?
        }
        _ => {
            return Err(SnowError::Api(
                "export requires exactly one of --number or --sys-id".to_string(),
            ));
        }
    };
    let value = display::record_to_json(&record);
    let bytes = serialize_export(table, &value, format)?;
    write_export_file(output, &bytes)?;
    println!(
        "Exported {} {} from {} to {}",
        table.label(),
        value
            .get("number")
            .and_then(Value::as_str)
            .unwrap_or(record.sys_id.as_str()),
        table.table_name(),
        output.display()
    );
    Ok(())
}

fn serialize_export(
    table: ExportTable,
    record: &Value,
    format: ExportFormat,
) -> Result<Vec<u8>, SnowError> {
    let fields = record_fields(record)?;
    match format {
        ExportFormat::Json => serde_json::to_vec_pretty(record)
            .map_err(|error| SnowError::Api(format!("serializing JSON export: {error}"))),
        ExportFormat::Jsonl => {
            let mut output = serde_json::to_vec(record)
                .map_err(|error| SnowError::Api(format!("serializing JSONL export: {error}")))?;
            output.push(b'\n');
            Ok(output)
        }
        ExportFormat::Csv => Ok(serialize_csv(&fields).into_bytes()),
        ExportFormat::Markdown => Ok(serialize_markdown(table, &fields).into_bytes()),
        ExportFormat::Xlsx => serialize_xlsx(table, &fields),
    }
}

fn record_fields(record: &Value) -> Result<&Map<String, Value>, SnowError> {
    record
        .as_object()
        .ok_or_else(|| SnowError::Api("export source record was not a JSON object".to_string()))
}

fn serialize_csv(fields: &Map<String, Value>) -> String {
    let mut output = String::from("field,value\n");
    for (field, value) in fields {
        write_csv_cell(&mut output, field);
        output.push(',');
        write_csv_cell(&mut output, &value_text(value));
        output.push('\n');
    }
    output
}

fn serialize_markdown(table: ExportTable, fields: &Map<String, Value>) -> String {
    let identity = fields
        .get("number")
        .and_then(Value::as_str)
        .or_else(|| fields.get("sys_id").and_then(Value::as_str))
        .unwrap_or("record");
    let mut output = format!("# {} {identity}\n\n", table.label());
    output.push_str("| Field | Value |\n| --- | --- |\n");
    for (field, value) in fields {
        let _ = writeln!(
            output,
            "| {} | {} |",
            markdown_cell(field),
            markdown_cell(&value_text(value))
        );
    }
    output
}

fn serialize_xlsx(table: ExportTable, fields: &Map<String, Value>) -> Result<Vec<u8>, SnowError> {
    let mut workbook = Workbook::new();
    let worksheet = workbook.add_worksheet();
    worksheet
        .set_name(table.label())
        .map_err(|error| SnowError::Api(format!("naming XLSX worksheet: {error}")))?;
    worksheet
        .write_string(0, 0, "Field")
        .map_err(|error| SnowError::Api(format!("writing XLSX field header: {error}")))?;
    worksheet
        .write_string(0, 1, "Value")
        .map_err(|error| SnowError::Api(format!("writing XLSX value header: {error}")))?;
    for (index, (field, value)) in fields.iter().enumerate() {
        let row = u32::try_from(index + 1)
            .map_err(|_| SnowError::Api("export has too many fields for XLSX".to_string()))?;
        worksheet
            .write_string(row, 0, field)
            .map_err(|error| SnowError::Api(format!("writing XLSX field `{field}`: {error}")))?;
        worksheet
            .write_string(row, 1, value_text(value))
            .map_err(|error| {
                SnowError::Api(format!("writing XLSX value for `{field}`: {error}"))
            })?;
    }
    workbook
        .save_to_buffer()
        .map_err(|error| SnowError::Api(format!("serializing XLSX export: {error}")))
}

fn value_text(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::String(text) => text.clone(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::Array(_) | Value::Object(_) => serde_json::to_string(value).unwrap_or_default(),
    }
}

fn write_csv_cell(output: &mut String, value: &str) {
    if value
        .chars()
        .any(|ch| matches!(ch, ',' | '"' | '\n' | '\r'))
    {
        output.push('"');
        for ch in value.chars() {
            if ch == '"' {
                output.push_str("\"\"");
            } else {
                output.push(ch);
            }
        }
        output.push('"');
    } else {
        output.push_str(value);
    }
}

fn markdown_cell(value: &str) -> String {
    value.replace('|', "\\|").replace(['\r', '\n'], "<br>")
}

fn write_export_file(output: &Path, bytes: &[u8]) -> Result<(), SnowError> {
    validate_export_output(output)?;
    let parent = output
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let file_name = output
        .file_name()
        .ok_or_else(|| SnowError::Api("export --output must name a file".to_string()))?;
    let temporary = parent.join(format!(
        ".{}.{}.tmp",
        file_name.to_string_lossy(),
        uuid::Uuid::new_v4()
    ));
    let write_result = (|| -> Result<(), SnowError> {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        Ok(())
    })();
    if let Err(error) = write_result {
        let _ = std::fs::remove_file(&temporary);
        return Err(error);
    }
    if let Err(error) = std::fs::rename(&temporary, output) {
        let _ = std::fs::remove_file(&temporary);
        return Err(SnowError::from(error));
    }
    Ok(())
}

fn validate_export_output(output: &Path) -> Result<(), SnowError> {
    if output.file_name().is_none() || output.is_dir() {
        return Err(SnowError::Api(
            "export --output must name a file".to_string(),
        ));
    }
    let parent = output
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    if !parent.is_dir() {
        return Err(SnowError::Api(format!(
            "export output parent is not a directory: {}",
            parent.display()
        )));
    }
    Ok(())
}
