use super::super::*;

pub(crate) fn print_approval_record(approval: &ApprovalRecord) {
    println!(
        "{} [{}] {}",
        approval.record.number, approval.record.state, approval.record.short_description
    );
    println!("approver: {}", approval.approver.display_name);
    println!("target: {}", approval.target.number);
    match approval.routed_via {
        snow_core::ApprovalRoutedVia::Direct => println!("routed via: direct"),
        snow_core::ApprovalRoutedVia::Group => {
            let group = approval
                .approver_group
                .as_ref()
                .map(|group| group.display_name.as_str())
                .unwrap_or(approval.approver.display_name.as_str());
            println!("routed via: group ({group})");
        }
    }
    println!("requested at: {}", approval.requested_at);
    if let Some(due_date) = approval.due_date {
        println!("due date: {due_date}");
    }
}

pub(crate) async fn cmd_approve(
    client: &ServiceNowClient,
    username: &str,
    number: &str,
    skip_confirm: bool,
) -> Result<(), SnowError> {
    let (table, record) = get_by_number(client, number).await?;

    let title = record
        .get_str("short_description")
        .unwrap_or("(no description)");
    println!("{} — {}", number.bold(), title);

    let user_record = client
        .table("sys_user")
        .equals("user_name", username)
        .fields(&["sys_id"])
        .limit(1)
        .first()
        .await?
        .ok_or_else(|| {
            SnowError::UserNotFound(format!("User {username} not found in ServiceNow."))
        })?;

    if !skip_confirm {
        print!("Approve {number}? [y/N] ");
        io::stdout().flush().unwrap();
        let mut input = String::new();
        io::stdin().read_line(&mut input).unwrap();
        if input.trim().to_lowercase() != "y" {
            println!("Cancelled.");
            return Ok(());
        }
    }

    client
        .approve(&table, &record.sys_id, &user_record.sys_id)
        .execute()
        .await?;
    println!("{}", "Approved.".green().bold());
    Ok(())
}

pub(crate) async fn cmd_note(
    client: &ServiceNowClient,
    number: &str,
    message: &str,
    dry_run: bool,
) -> Result<(), SnowError> {
    let (table, record) = get_by_number(client, number).await?;

    if dry_run {
        println!("{}", "Dry run — no changes will be made.".yellow().bold());
        println!("  Table:  {table}");
        println!("  Sys ID: {}", record.sys_id);
        println!("  Number: {number}");
        println!("  Action: PATCH /api/now/table/{table}/{}", record.sys_id);
        println!("  Body:   {{\"work_notes\": {:?}}}", message);
        return Ok(());
    }

    client
        .add_work_note(&table, &record.sys_id, message)
        .await?;
    println!("{} Work note added to {}.", "Done.".green().bold(), number);
    Ok(())
}

pub(crate) async fn cmd_show_approval_runtime(
    core: &SnowCore,
    number: &str,
) -> Result<(), SnowError> {
    match core.get_approval(number).await? {
        Some(approval) => print_approval_record(&approval),
        None => println!("Approval not found: {number}"),
    }
    Ok(())
}
