use super::*;

pub(super) async fn cmd_show(
    core: &SnowCore,
    client: &ServiceNowClient,
    username: &str,
    number: &str,
    extras: &[String],
    resource_plan_state: Option<&str>,
    smart: bool,
    full: bool,
) -> Result<(), SnowError> {
    if is_show_sla_alias(extras) {
        return cmd_sla(core, number).await;
    }

    match classify_show_target(number) {
        ShowTarget::Project => {
            cmd_show_project(client, number, extras, resource_plan_state, full).await
        }
        ShowTarget::Demand => cmd_show_demand(client, number, extras, full).await,
        ShowTarget::Incident => cmd_show_incident(client, number, extras, full).await,
        ShowTarget::Request => cmd_show_request(client, number, extras, full).await,
        ShowTarget::RequestItem => cmd_show_request_item(client, number, extras, full).await,
        ShowTarget::Story => cmd_show_story(client, number, extras, full).await,
        ShowTarget::StoryTask => cmd_show_story_task(client, number, extras, full).await,
        ShowTarget::Task => cmd_show_task(client, number, extras, full).await,
        ShowTarget::Knowledge => {
            if full || !extras.is_empty() {
                cmd_show_knowledge(client, number, extras, full).await
            } else {
                cmd_show_knowledge_runtime(core, number, false).await
            }
        }
        ShowTarget::ResourcePlan => cmd_show_resource_plan(client, number, extras, full).await,
        ShowTarget::PrivateTask => cmd_show_private_task(client, number, extras, full).await,
        ShowTarget::Change => cmd_show_change(client, username, number, extras, smart, full).await,
    }
}

pub(super) fn is_show_sla_alias(extras: &[String]) -> bool {
    matches!(extras, [extra] if extra.eq_ignore_ascii_case("sla"))
}

pub(super) fn classify_show_target(number: &str) -> ShowTarget {
    if number.starts_with("PRJ") {
        ShowTarget::Project
    } else if number.starts_with("DMND") {
        ShowTarget::Demand
    } else if number.starts_with("INC") {
        ShowTarget::Incident
    } else if number.starts_with("REQ") {
        ShowTarget::Request
    } else if number.starts_with("RITM") {
        ShowTarget::RequestItem
    } else if number.starts_with("STRY") {
        ShowTarget::Story
    } else if number.starts_with("STSK") {
        ShowTarget::StoryTask
    } else if number.starts_with("SCTASK") || number.starts_with("TASK") {
        ShowTarget::Task
    } else if number.starts_with("KB") {
        ShowTarget::Knowledge
    } else if number.starts_with("RPLN") {
        ShowTarget::ResourcePlan
    } else if number.starts_with("PTSK") {
        ShowTarget::PrivateTask
    } else {
        ShowTarget::Change
    }
}

/// Show a Visual Task Board private task (`vtb_task` / PTSK*).
///
/// Board/lane/checklist context comes from
/// [`snow_core::enrich::enrich_vtb_context`], which is best-effort: an ACL
/// miss on `vtb_card`/`checklist` degrades to omitted lines rather than
/// failing the whole `show` command.
pub(super) async fn cmd_show_private_task(
    client: &ServiceNowClient,
    number: &str,
    extras: &[String],
    full: bool,
) -> Result<(), SnowError> {
    if full {
        let task = client
            .table("vtb_task")
            .equals("number", number)
            .display_value(DisplayValue::Display)
            .limit(1)
            .first()
            .await?
            .ok_or_else(|| SnowError::NotFound(format!("{number} not found.")))?;
        display::print_full_dump(&display::record_to_json(&task));
        return Ok(());
    }

    let fields = &[
        "sys_id",
        "number",
        "short_description",
        "state",
        "priority",
        "assigned_to",
        "assignment_group",
        "owner",
        "opened_at",
        "description",
    ];
    let task = client
        .table("vtb_task")
        .equals("number", number)
        .fields(fields)
        .display_value(DisplayValue::Display)
        .limit(1)
        .first()
        .await?
        .ok_or_else(|| SnowError::NotFound(format!("{number} not found.")))?;

    // Reuse generic task-ish summary until a dedicated private-task layout lands.
    display::print_task_summary(&task);
    println!("resource_type: private_task");
    println!("table: vtb_task");

    let vtb_context = enrich_vtb_context(client, &VtbSchema::GATE0, &task.sys_id).await;
    print_vtb_context(&vtb_context);

    fetch_and_print_extras(client, &task, "vtb_task", extras).await?;
    Ok(())
}

/// Render the board/lane/checklist lines for [`cmd_show_private_task`].
///
/// Reuses the command's standard field styling. Board/lane/swim-lane lines are
/// omitted when unset, and the checklist block is omitted entirely when empty.
/// [`enrich_vtb_context`] already collapsed "ACL 403"/"timeout"/"genuinely not
/// set" into the same `None`/empty shape, so there is nothing left to
/// distinguish at render time.
pub(super) fn print_vtb_context(context: &VtbContext) {
    if let Some(board_name) = context.board_name.as_deref() {
        display::print_field("board", Some(board_name));
    }
    if let Some(lane_name) = context.lane_name.as_deref() {
        display::print_field("lane", Some(lane_name));
    }
    if let Some(swim_lane_name) = context.swim_lane_name.as_deref() {
        display::print_field("swim lane", Some(swim_lane_name));
    }
    for item in &context.checklist_items {
        let mark = if item.complete { "x" } else { " " };
        let value = format!("[{mark}] {}", item.name);
        display::print_field("checklist", Some(&value));
    }
}

pub(super) async fn cmd_show_story_task(
    client: &ServiceNowClient,
    number: &str,
    extras: &[String],
    full: bool,
) -> Result<(), SnowError> {
    if full {
        let task = client
            .table("rm_scrum_task")
            .equals("number", number)
            .display_value(DisplayValue::Display)
            .limit(1)
            .first()
            .await?
            .ok_or_else(|| SnowError::NotFound(format!("{number} not found.")))?;
        display::print_full_dump(&display::record_to_json(&task));
        return Ok(());
    }

    let fields = &[
        "sys_id",
        "number",
        "short_description",
        "state",
        "priority",
        "assigned_to",
        "assignment_group",
        "story",
        "opened_at",
        "due_date",
        "description",
    ];
    let task = client
        .table("rm_scrum_task")
        .equals("number", number)
        .fields(fields)
        .display_value(DisplayValue::Display)
        .limit(1)
        .first()
        .await?
        .ok_or_else(|| SnowError::NotFound(format!("{number} not found.")))?;
    display::print_story_task_summary(&task);
    fetch_and_print_extras(client, &task, "rm_scrum_task", extras).await?;
    Ok(())
}

/// Resolve a record number to its ServiceNow table name.
pub(super) fn resolve_table(number: &str) -> Result<String, SnowError> {
    let registry = PrefixRegistry::default();
    registry
        .table_for_number(number)
        .map(|s| s.to_string())
        .ok_or_else(|| SnowError::Api(format!("Unknown record prefix in '{number}'.")))
}

/// Look up a record by number, resolving the table from the prefix.
pub(super) async fn get_by_number(
    client: &ServiceNowClient,
    number: &str,
) -> Result<(String, Record), SnowError> {
    let table = resolve_table(number)?;
    let record = client
        .table(&table)
        .equals("number", number)
        .fields(&["sys_id", "number", "short_description", "state"])
        .display_value(DisplayValue::Display)
        .limit(1)
        .first()
        .await?
        .ok_or_else(|| SnowError::NotFound(format!("{number} not found.")))?;
    Ok((table, record))
}

/// Resolve friendly extra names to ServiceNow journal field names.
/// Returns (journal_fields, raw_fields) — journal fields are fetched from sys_journal_field,
/// raw fields are fetched as additional fields on the record itself.
pub(super) fn resolve_extras(extras: &[String]) -> (Vec<&'static str>, Vec<String>) {
    let mut journal_fields = Vec::new();
    let mut raw_fields = Vec::new();
    for extra in extras {
        match extra.to_lowercase().as_str() {
            "activity" => {
                journal_fields.push("work_notes");
                journal_fields.push("comments");
            }
            "notes" | "worknotes" | "work_notes" => {
                journal_fields.push("work_notes");
            }
            "comments" => {
                journal_fields.push("comments");
            }
            _ => raw_fields.push(extra.clone()),
        }
    }
    journal_fields.dedup();
    (journal_fields, raw_fields)
}

pub(super) async fn fetch_and_print_extras(
    client: &ServiceNowClient,
    record: &Record,
    table: &str,
    extras: &[String],
) -> Result<(), SnowError> {
    if extras.is_empty() {
        return Ok(());
    }
    let (journal_fields, raw_fields) = resolve_extras(extras);

    // Fetch journal fields using journal_inline (reads directly from record table,
    // avoids ACL-restricted sys_journal_field).
    if !journal_fields.is_empty() {
        let field_names: Vec<&str> = journal_fields.to_vec();
        let journal_record = client
            .journal_inline(table, &record.sys_id, &field_names)
            .first()
            .await?;
        if let Some(rec) = journal_record {
            let mut found = false;
            for jf in &journal_fields {
                if let Some(val) = rec.get_str(jf)
                    && !val.trim().is_empty()
                {
                    found = true;
                    display::print_multiline_field_pub(jf, Some(val));
                }
            }
            if !found {
                println!("\n{}", "No journal entries found.".dimmed());
            }
        } else {
            println!("\n{}", "No journal entries found.".dimmed());
        }
    }

    // Fetch raw fields by re-querying the record with those fields
    if !raw_fields.is_empty() {
        let field_names: Vec<&str> = raw_fields.iter().map(|s| s.as_str()).collect();
        let extra_record = client
            .table(table)
            .equals("sys_id", &record.sys_id)
            .fields(&field_names)
            .display_value(DisplayValue::Display)
            .limit(1)
            .first()
            .await?;
        if let Some(rec) = extra_record {
            for field in &raw_fields {
                display::print_multiline_field_pub(field, rec.get_str(field));
            }
        }
    }

    Ok(())
}

pub(super) async fn cmd_show_change(
    client: &ServiceNowClient,
    username: &str,
    number: &str,
    extras: &[String],
    smart: bool,
    full: bool,
) -> Result<(), SnowError> {
    let (table, summary_record) = get_by_number(client, number).await?;
    if table != "change_request" {
        return Err(SnowError::NotFound(format!("{number} not found.")));
    }

    if full {
        let cr = client
            .table("change_request")
            .display_value(DisplayValue::Display)
            .get(&summary_record.sys_id)
            .await?;

        let tasks = client
            .table("change_task")
            .equals("change_request", &cr.sys_id)
            .display_value(DisplayValue::Display)
            .execute()
            .await?;

        let mut full_output = display::record_to_json(&cr);
        full_output["_tasks"] = serde_json::to_value(
            tasks
                .records
                .iter()
                .map(display::record_to_json)
                .collect::<Vec<_>>(),
        )
        .unwrap_or_default();
        display::print_full_dump(&full_output);
        return Ok(());
    }

    let fields = &[
        "sys_id",
        "number",
        "short_description",
        "state",
        "category",
        "start_date",
        "end_date",
        "assigned_to",
        "description",
        "change_plan",
        "implementation_plan",
        "backout_plan",
    ];
    let cr = client
        .table("change_request")
        .fields(fields)
        .display_value(DisplayValue::Display)
        .get(&summary_record.sys_id)
        .await?;
    display::print_change_summary(&cr);
    fetch_and_print_extras(client, &cr, "change_request", extras).await?;

    if smart {
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
        let user_sys_id = &user_record.sys_id;

        let task_result = client
            .table("change_task")
            .equals("change_request", &cr.sys_id)
            .display_value(DisplayValue::Display)
            .execute()
            .await?;
        let my_tasks: Vec<&Record> = task_result
            .records
            .iter()
            .filter(|r| {
                r.get_str("assigned_to")
                    .is_some_and(|a| a.contains(username))
            })
            .collect();

        if !my_tasks.is_empty() {
            println!("\n{}", "Your Tasks:".bold().underline());
            let refs: Vec<Record> = my_tasks.into_iter().cloned().collect();
            display::print_tasks(&refs);
        }

        let approval_result = client
            .table("sysapproval_approver")
            .equals("document_id", &cr.sys_id)
            .equals("approver", user_sys_id)
            .display_value(DisplayValue::Display)
            .execute()
            .await?;
        display::print_approval_records(&approval_result.records);
    }

    // Interactive prompt
    println!("\n[a] Approve  [d] More details  [n] Add note  [q] Quit");
    print!("> ");
    io::stdout().flush().unwrap();
    let mut input = String::new();
    io::stdin().read_line(&mut input).unwrap();
    match input.trim().to_lowercase().as_str() {
        "a" => cmd_approve(client, username, number, false).await?,
        "d" => {
            let cr = client
                .table("change_request")
                .display_value(DisplayValue::Display)
                .get(&summary_record.sys_id)
                .await?;
            display::print_full_dump(&display::record_to_json(&cr));
        }
        "n" => {
            print!("Note: ");
            io::stdout().flush().unwrap();
            let mut note = String::new();
            io::stdin().read_line(&mut note).unwrap();
            let note = note.trim();
            if !note.is_empty() {
                cmd_note(client, number, note, false).await?;
            }
        }
        _ => {}
    }

    Ok(())
}

pub(super) async fn cmd_show_project(
    client: &ServiceNowClient,
    number: &str,
    extras: &[String],
    resource_plan_state: Option<&str>,
    full: bool,
) -> Result<(), SnowError> {
    if full {
        let project = client
            .table("pm_project")
            .equals("number", number)
            .display_value(DisplayValue::Display)
            .limit(1)
            .first()
            .await?
            .ok_or_else(|| SnowError::NotFound(format!("{number} not found.")))?;
        display::print_full_dump(&display::record_to_json(&project));
        return Ok(());
    }

    let fields = &[
        "sys_id",
        "number",
        "name",
        "short_description",
        "state",
        "demand",
        "project_manager",
        "start_date",
        "end_date",
        "percent_complete",
        "description",
        "goals",
        "business_case",
    ];
    let project = client
        .table("pm_project")
        .equals("number", number)
        .fields(fields)
        .display_value(DisplayValue::Display)
        .limit(1)
        .first()
        .await?
        .ok_or_else(|| SnowError::NotFound(format!("{number} not found.")))?;
    display::print_project_summary(&project);
    let resource_plans =
        fetch_project_resource_plans(client, &project, resource_plan_state).await?;
    print_project_resource_plans(&resource_plans, resource_plan_state);
    fetch_and_print_extras(client, &project, "pm_project", extras).await?;
    Ok(())
}

pub(super) async fn fetch_project_resource_plans(
    client: &ServiceNowClient,
    project: &Record,
    state_filter: Option<&str>,
) -> Result<Vec<Record>, SnowError> {
    let fields = &[
        "sys_id",
        "number",
        "short_description",
        "state",
        "task",
        "resource_type",
        "user_resource",
        "group_resource",
        "start_date",
        "end_date",
        "planned_hours",
        "allocated_hours",
        "confirmed_hours",
    ];
    let plans = client
        .table("resource_plan")
        .equals("task", &project.sys_id)
        .fields(fields)
        .display_value(DisplayValue::Both)
        .order_by("number", Order::Asc)
        .limit(500)
        .execute()
        .await?
        .records
        .into_iter()
        .filter(|plan| resource_plan_matches_state(plan, state_filter))
        .collect();
    Ok(plans)
}

pub(super) fn resource_plan_matches_state(plan: &Record, state_filter: Option<&str>) -> bool {
    let Some(state_filter) = state_filter
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return true;
    };
    [
        plan.get_str("state"),
        plan.get_display("state"),
        plan.get_raw("state"),
    ]
    .into_iter()
    .flatten()
    .any(|state| state.eq_ignore_ascii_case(state_filter))
}

pub(super) fn print_project_resource_plans(plans: &[Record], state_filter: Option<&str>) {
    let filter = state_filter
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("All");
    println!("\nResource Plans (state: {filter})");
    if plans.is_empty() {
        println!("  No resource plans found.");
        return;
    }

    for plan in plans {
        let number = plan.get_str("number").unwrap_or("-");
        let state = plan
            .get_display("state")
            .or(plan.get_str("state"))
            .unwrap_or("-");
        let resource = plan
            .get_str("user_resource")
            .or(plan.get_str("group_resource"))
            .or(plan.get_str("resource_type"))
            .unwrap_or("-");
        let start = plan.get_str("start_date").unwrap_or("-");
        let end = plan.get_str("end_date").unwrap_or("-");
        let planned = plan.get_str("planned_hours").unwrap_or("-");
        let allocated = plan.get_str("allocated_hours").unwrap_or("-");
        println!(
            "  {number} [{state}] resource:{resource} window:{start}..{end} planned:{planned} allocated:{allocated}"
        );
    }
}

pub(super) async fn cmd_show_demand(
    client: &ServiceNowClient,
    number: &str,
    extras: &[String],
    full: bool,
) -> Result<(), SnowError> {
    if full {
        let demand = client
            .table("dmn_demand")
            .equals("number", number)
            .display_value(DisplayValue::Display)
            .limit(1)
            .first()
            .await?
            .ok_or_else(|| SnowError::NotFound(format!("{number} not found.")))?;
        display::print_full_dump(&display::record_to_json(&demand));
        return Ok(());
    }

    let fields = &[
        "sys_id",
        "number",
        "short_description",
        "state",
        "priority",
        "requested_by",
        "start_date",
        "end_date",
        "description",
        "business_case",
    ];
    let demand = client
        .table("dmn_demand")
        .equals("number", number)
        .fields(fields)
        .display_value(DisplayValue::Display)
        .limit(1)
        .first()
        .await?
        .ok_or_else(|| SnowError::NotFound(format!("{number} not found.")))?;
    display::print_demand_summary(&demand);
    fetch_and_print_extras(client, &demand, "dmn_demand", extras).await?;
    Ok(())
}

pub(super) async fn cmd_show_incident(
    client: &ServiceNowClient,
    number: &str,
    extras: &[String],
    full: bool,
) -> Result<(), SnowError> {
    let (table, summary_record) = get_by_number(client, number).await?;
    if table != "incident" {
        return Err(SnowError::NotFound(format!("{number} not found.")));
    }

    if full {
        let incident = client
            .table("incident")
            .display_value(DisplayValue::Display)
            .get(&summary_record.sys_id)
            .await?;
        display::print_full_dump(&display::record_to_json(&incident));
        return Ok(());
    }

    let fields = &[
        "sys_id",
        "number",
        "short_description",
        "state",
        "priority",
        "severity",
        "category",
        "subcategory",
        "assigned_to",
        "assignment_group",
        "caller_id",
        "opened_at",
        "resolved_at",
        "close_code",
        "description",
    ];
    let incident = client
        .table("incident")
        .fields(fields)
        .display_value(DisplayValue::Display)
        .get(&summary_record.sys_id)
        .await?;
    display::print_incident_summary(&incident);
    fetch_and_print_extras(client, &incident, "incident", extras).await?;
    Ok(())
}

pub(super) async fn cmd_show_request_item(
    client: &ServiceNowClient,
    number: &str,
    extras: &[String],
    full: bool,
) -> Result<(), SnowError> {
    if full {
        let ritm = client
            .table("sc_req_item")
            .equals("number", number)
            .display_value(DisplayValue::Display)
            .limit(1)
            .first()
            .await?
            .ok_or_else(|| SnowError::NotFound(format!("{number} not found.")))?;
        display::print_full_dump(&display::record_to_json(&ritm));
        return Ok(());
    }

    let fields = &[
        "sys_id",
        "number",
        "short_description",
        "state",
        "priority",
        "assigned_to",
        "assignment_group",
        "request",
        "requested_for",
        "cat_item",
        "opened_at",
        "due_date",
        "stage",
        "approval",
        "description",
    ];
    let ritm = client
        .table("sc_req_item")
        .equals("number", number)
        .fields(fields)
        .display_value(DisplayValue::Display)
        .limit(1)
        .first()
        .await?
        .ok_or_else(|| SnowError::NotFound(format!("{number} not found.")))?;
    display::print_request_item_summary(&ritm);

    // Fetch and display catalog variables, resolving reference sys_ids to names
    let sys_id = ritm.get_str("sys_id").unwrap_or_default();
    let mut variables = client.catalog_variables(sys_id).await?;
    client.resolve_catalog_variables(&mut variables).await?;
    display::print_variables(&variables);

    // Fetch and display last 5 activity entries (Additional Comments only, no Email sent)
    let journal_rec = client
        .journal_inline("sc_req_item", sys_id, &["comments"])
        .first()
        .await?;
    if let Some(rec) = &journal_rec {
        let entries: Vec<JournalEntry> = rec
            .parse_journal("comments")
            .into_iter()
            .filter(|e| !e.is_email())
            .take(5)
            .collect();
        display::print_activity(&entries);
    }

    fetch_and_print_extras(client, &ritm, "sc_req_item", extras).await?;
    Ok(())
}

pub(super) async fn cmd_show_request(
    client: &ServiceNowClient,
    number: &str,
    extras: &[String],
    full: bool,
) -> Result<(), SnowError> {
    if full {
        let req = client
            .table("sc_request")
            .equals("number", number)
            .display_value(DisplayValue::Display)
            .limit(1)
            .first()
            .await?
            .ok_or_else(|| SnowError::NotFound(format!("{number} not found.")))?;
        display::print_full_dump(&display::record_to_json(&req));
        return Ok(());
    }

    let fields = &[
        "sys_id",
        "number",
        "short_description",
        "state",
        "requested_for",
        "requested_by",
        "opened_at",
        "due_date",
        "stage",
        "approval",
        "description",
    ];
    let req = client
        .table("sc_request")
        .equals("number", number)
        .fields(fields)
        .display_value(DisplayValue::Display)
        .limit(1)
        .first()
        .await?
        .ok_or_else(|| SnowError::NotFound(format!("{number} not found.")))?;

    println!("Number: {}", req.get_str("number").unwrap_or(number));
    println!(
        "Title: {}",
        req.get_str("short_description")
            .unwrap_or("(no description)")
    );
    println!("State: {}", req.get_str("state").unwrap_or("-"));
    println!(
        "Requested For: {}",
        req.get_str("requested_for").unwrap_or("-")
    );
    println!(
        "Requested By: {}",
        req.get_str("requested_by").unwrap_or("-")
    );
    if let Some(opened_at) = req.get_str("opened_at") {
        println!("Opened: {opened_at}");
    }
    if let Some(due_date) = req.get_str("due_date") {
        println!("Due Date: {due_date}");
    }
    if let Some(stage) = req.get_str("stage") {
        println!("Stage: {stage}");
    }
    if let Some(approval) = req.get_str("approval") {
        println!("Approval: {approval}");
    }

    let description = req
        .get_str("description")
        .map(display::strip_html)
        .unwrap_or_default();
    if !description.trim().is_empty() {
        println!("\nDescription:\n{description}");
    }

    fetch_and_print_extras(client, &req, "sc_request", extras).await?;
    Ok(())
}

pub(super) async fn cmd_show_task(
    client: &ServiceNowClient,
    number: &str,
    extras: &[String],
    full: bool,
) -> Result<(), SnowError> {
    if full {
        let task = client
            .table("sc_task")
            .equals("number", number)
            .display_value(DisplayValue::Display)
            .limit(1)
            .first()
            .await?
            .ok_or_else(|| SnowError::NotFound(format!("{number} not found.")))?;
        display::print_full_dump(&display::record_to_json(&task));
        return Ok(());
    }

    let fields = &[
        "sys_id",
        "number",
        "short_description",
        "state",
        "priority",
        "assigned_to",
        "assignment_group",
        "request_item",
        "opened_at",
        "due_date",
        "description",
    ];
    let task = client
        .table("sc_task")
        .equals("number", number)
        .fields(fields)
        .display_value(DisplayValue::Display)
        .limit(1)
        .first()
        .await?
        .ok_or_else(|| SnowError::NotFound(format!("{number} not found.")))?;
    display::print_task_summary(&task);
    fetch_and_print_extras(client, &task, "sc_task", extras).await?;
    Ok(())
}

pub(super) async fn cmd_show_story(
    client: &ServiceNowClient,
    number: &str,
    extras: &[String],
    full: bool,
) -> Result<(), SnowError> {
    if full {
        let story = client
            .table("rm_story")
            .equals("number", number)
            .display_value(DisplayValue::Display)
            .limit(1)
            .first()
            .await?
            .ok_or_else(|| SnowError::NotFound(format!("{number} not found.")))?;
        display::print_full_dump(&display::record_to_json(&story));
        return Ok(());
    }

    let fields = &[
        "sys_id",
        "number",
        "short_description",
        "state",
        "priority",
        "assigned_to",
        "story_points",
        "blocked",
        "sprint",
        "product",
        "epic",
        "acceptance_criteria",
        "description",
    ];
    let story = client
        .table("rm_story")
        .equals("number", number)
        .fields(fields)
        .display_value(DisplayValue::Display)
        .limit(1)
        .first()
        .await?
        .ok_or_else(|| SnowError::NotFound(format!("{number} not found.")))?;
    display::print_story_summary(&story);

    // Fetch story tasks (rm_scrum_task) linked to this story
    let sys_id = story.get_str("sys_id").unwrap_or_default();
    let task_result = client
        .table("rm_scrum_task")
        .equals("story", sys_id)
        .fields(&[
            "sys_id",
            "number",
            "short_description",
            "state",
            "assigned_to",
        ])
        .display_value(DisplayValue::Display)
        .execute()
        .await?;

    if !task_result.records.is_empty() {
        // Fetch work notes for each task
        let mut tasks_with_notes: Vec<(Record, Vec<JournalEntry>)> = Vec::new();
        for task in &task_result.records {
            let journal_rec = client
                .journal_inline("rm_scrum_task", &task.sys_id, &["work_notes"])
                .first()
                .await?;
            let entries = if let Some(rec) = &journal_rec {
                rec.parse_journal("work_notes")
                    .into_iter()
                    .filter(|e| !e.is_email())
                    .take(3)
                    .collect()
            } else {
                Vec::new()
            };
            tasks_with_notes.push((task.clone(), entries));
        }
        display::print_story_tasks(&tasks_with_notes);
    }

    fetch_and_print_extras(client, &story, "rm_story", extras).await?;
    Ok(())
}

pub(super) async fn cmd_show_knowledge(
    client: &ServiceNowClient,
    number: &str,
    extras: &[String],
    full: bool,
) -> Result<(), SnowError> {
    if full {
        let article = client
            .table("kb_knowledge")
            .equals("number", number)
            .display_value(DisplayValue::Display)
            .limit(1)
            .first()
            .await?
            .ok_or_else(|| SnowError::NotFound(format!("{number} not found.")))?;
        display::print_full_dump(&display::record_to_json(&article));
        return Ok(());
    }

    let fields = &[
        "sys_id",
        "number",
        "short_description",
        "workflow_state",
        "kb_category",
        "kb_knowledge_base",
        "author",
        "published",
        "valid_to",
        "text",
        "article_body",
        "description",
    ];
    let article = client
        .table("kb_knowledge")
        .equals("number", number)
        .fields(fields)
        .display_value(DisplayValue::Display)
        .limit(1)
        .first()
        .await?
        .ok_or_else(|| SnowError::NotFound(format!("{number} not found.")))?;

    println!("Number: {}", article.get_str("number").unwrap_or(number));
    println!(
        "Title: {}",
        article
            .get_str("short_description")
            .unwrap_or("(no description)")
    );
    println!(
        "State: {}",
        article
            .get_str("workflow_state")
            .or(article.get_str("state"))
            .unwrap_or("-")
    );
    println!(
        "Knowledge Base: {}",
        article.get_str("kb_knowledge_base").unwrap_or("-")
    );
    println!(
        "Category: {}",
        article.get_str("kb_category").unwrap_or("-")
    );
    println!("Author: {}", article.get_str("author").unwrap_or("-"));
    if let Some(published) = article.get_str("published") {
        println!("Published: {published}");
    }
    if let Some(valid_to) = article.get_str("valid_to") {
        println!("Valid To: {valid_to}");
    }

    let body = article
        .get_str("article_body")
        .or(article.get_str("text"))
        .or(article.get_str("description"))
        .map(display::strip_html)
        .unwrap_or_default();
    if !body.trim().is_empty() {
        println!("\nArticle:\n{body}");
    }

    fetch_and_print_extras(client, &article, "kb_knowledge", extras).await?;
    Ok(())
}

pub(super) async fn cmd_show_resource_plan(
    client: &ServiceNowClient,
    number: &str,
    extras: &[String],
    full: bool,
) -> Result<(), SnowError> {
    if full {
        let plan = client
            .table("resource_plan")
            .equals("number", number)
            .display_value(DisplayValue::Display)
            .limit(1)
            .first()
            .await?
            .ok_or_else(|| SnowError::NotFound(format!("{number} not found.")))?;
        display::print_full_dump(&display::record_to_json(&plan));
        return Ok(());
    }

    let fields = &[
        "sys_id",
        "number",
        "short_description",
        "state",
        "task",
        "resource_type",
        "user_resource",
        "group_resource",
        "start_date",
        "end_date",
        "planned_hours",
        "allocated_hours",
        "confirmed_hours",
        "description",
    ];
    let plan = client
        .table("resource_plan")
        .equals("number", number)
        .fields(fields)
        .display_value(DisplayValue::Display)
        .limit(1)
        .first()
        .await?
        .ok_or_else(|| SnowError::NotFound(format!("{number} not found.")))?;

    println!("Number: {}", plan.get_str("number").unwrap_or(number));
    println!(
        "Title: {}",
        plan.get_str("short_description")
            .unwrap_or("(no description)")
    );
    println!("State: {}", plan.get_str("state").unwrap_or("-"));
    println!("Task: {}", plan.get_str("task").unwrap_or("-"));
    println!(
        "Resource Type: {}",
        plan.get_str("resource_type").unwrap_or("-")
    );
    println!(
        "User Resource: {}",
        plan.get_str("user_resource").unwrap_or("-")
    );
    println!(
        "Group Resource: {}",
        plan.get_str("group_resource").unwrap_or("-")
    );
    if let Some(start_date) = plan.get_str("start_date") {
        println!("Start Date: {start_date}");
    }
    if let Some(end_date) = plan.get_str("end_date") {
        println!("End Date: {end_date}");
    }
    if let Some(planned_hours) = plan.get_str("planned_hours") {
        println!("Planned Hours: {planned_hours}");
    }
    if let Some(allocated_hours) = plan.get_str("allocated_hours") {
        println!("Allocated Hours: {allocated_hours}");
    }
    if let Some(confirmed_hours) = plan.get_str("confirmed_hours") {
        println!("Confirmed Hours: {confirmed_hours}");
    }

    let description = plan
        .get_str("description")
        .map(display::strip_html)
        .unwrap_or_default();
    if !description.trim().is_empty() {
        println!("\nDescription:\n{description}");
    }

    fetch_and_print_extras(client, &plan, "resource_plan", extras).await?;
    Ok(())
}
