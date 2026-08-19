use super::super::*;

pub(crate) async fn cmd_tasks_core(core: &SnowCore, number: &str) -> Result<(), SnowError> {
    let children = core.get_children(number).await?;
    if children.is_empty() {
        println!("No tasks found.");
        return Ok(());
    }
    for child in children {
        println!(
            "{}  {}  {}",
            child.number, child.state, child.short_description
        );
    }
    Ok(())
}

pub(crate) async fn cmd_sla(core: &SnowCore, number: &str) -> Result<(), SnowError> {
    let status = core.task_sla_status_for_number(number).await?;
    print_task_sla_status(&status);
    Ok(())
}

pub(crate) async fn cmd_approve_core(
    core: &SnowCore,
    number: &str,
    yes: bool,
) -> Result<(), SnowError> {
    if !yes && !confirm_action(&format!("Approve {number}?"))? {
        println!("Cancelled.");
        return Ok(());
    }
    core.approve(number, None).await?;
    println!("Approved {number}.");
    Ok(())
}

pub(crate) async fn cmd_reject_core(
    core: &SnowCore,
    number: &str,
    reason: String,
    yes: bool,
) -> Result<(), SnowError> {
    if !yes && !confirm_action(&format!("Reject {number}?"))? {
        println!("Cancelled.");
        return Ok(());
    }
    core.reject(number, &reason).await?;
    println!("Rejected {number}.");
    Ok(())
}

pub(crate) async fn cmd_note_core(
    core: &SnowCore,
    number: &str,
    message: &str,
    dry_run: bool,
) -> Result<(), SnowError> {
    if dry_run {
        println!("Dry run — no changes will be made.");
        println!("Would add note to {number}: {message}");
        return Ok(());
    }
    core.add_work_note(number, message).await?;
    println!("Added work note to {number}.");
    Ok(())
}
