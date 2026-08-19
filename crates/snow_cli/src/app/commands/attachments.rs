use super::super::*;

pub(crate) async fn cmd_attachment(
    core: &SnowCore,
    action: AttachmentCommand,
) -> Result<(), SnowError> {
    match action {
        AttachmentCommand::List { number } => {
            let Some(attachments) = core.list_attachments(&number).await? else {
                return Err(SnowError::NotFound(format!("{number} not found.")));
            };
            print_attachments(&number, &attachments);
            Ok(())
        }
        AttachmentCommand::Upload {
            number,
            path,
            file_name,
            content_type,
            dry_run,
            yes,
        } => {
            let metadata = std::fs::metadata(&path)?;
            if !metadata.is_file() {
                return Err(SnowError::Api(format!(
                    "{} is not a regular file",
                    path.display()
                )));
            }
            if metadata.len() == 0 {
                return Err(SnowError::Api(format!("{} is empty", path.display())));
            }

            let attachment_name = match file_name.as_deref() {
                Some(name) if !name.trim().is_empty() => name.to_string(),
                _ => path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .ok_or_else(|| {
                        SnowError::Api(format!(
                            "{} does not have a valid file name",
                            path.display()
                        ))
                    })?
                    .to_string(),
            };
            let content_type =
                content_type.unwrap_or_else(|| infer_content_type(&path).to_string());

            let target = core
                .get_record_fresh(&number)
                .await?
                .ok_or_else(|| SnowError::NotFound(format!("{number} not found.")))?;

            if dry_run {
                println!("Dry run — no attachment will be uploaded.");
                println!("  Number:       {number}");
                println!("  Table:        {}", target.table);
                println!("  Sys ID:       {}", target.sys_id);
                println!("  File:         {}", path.display());
                println!("  File name:    {attachment_name}");
                println!("  Content-Type: {content_type}");
                println!("  Size:         {} bytes", metadata.len());
                return Ok(());
            }

            if !yes
                && !confirm_action(&format!(
                    "Upload {attachment_name} ({} bytes) to {number}?",
                    metadata.len()
                ))?
            {
                println!("Cancelled.");
                return Ok(());
            }

            let Some(attachment) = core
                .upload_attachment_file(&number, &path, Some(&attachment_name), Some(&content_type))
                .await?
            else {
                return Err(SnowError::NotFound(format!("{number} not found.")));
            };

            println!(
                "Uploaded {} to {} as attachment {}.",
                attachment.file_name, number, attachment.sys_id
            );
            Ok(())
        }
    }
}

pub(crate) fn print_attachments(number: &str, attachments: &[snow_core::AttachmentMetadata]) {
    if attachments.is_empty() {
        println!("No attachments found for {number}.");
        return;
    }

    println!("Attachments for {number}:");
    for attachment in attachments {
        let size = attachment
            .size_bytes
            .map(|bytes| bytes.to_string())
            .unwrap_or_else(|| "-".to_string());
        let created = attachment.sys_created_on.as_deref().unwrap_or("-");
        println!(
            "  {}  {}  {} bytes  {}  {}",
            attachment.sys_id, attachment.file_name, size, attachment.content_type, created
        );
    }
}

pub(crate) fn infer_content_type(path: &std::path::Path) -> &'static str {
    match path
        .extension()
        .and_then(|extension| extension.to_str())
        .map(|extension| extension.to_ascii_lowercase())
        .as_deref()
    {
        Some("jpg") | Some("jpeg") => "image/jpeg",
        Some("png") => "image/png",
        Some("gif") => "image/gif",
        Some("pdf") => "application/pdf",
        Some("txt") | Some("log") => "text/plain",
        Some("json") => "application/json",
        Some("csv") => "text/csv",
        _ => "application/octet-stream",
    }
}
