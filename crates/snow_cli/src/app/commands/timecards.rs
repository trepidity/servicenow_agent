use super::super::*;

pub(crate) async fn cmd_timecard(
    core: Arc<SnowCore>,
    env_name: &str,
    instance: &str,
    username: &str,
    action: TimecardCommand,
) -> Result<(), SnowError> {
    match action {
        TimecardCommand::List { week } => {
            let week_selector = parse_week_selector(week.as_deref())?;
            let sheet = core.list_my_timecards(week_selector).await?;
            display::print_timecard_sheet(&sheet);
            write_timecard_index_cache(&sheet, env_name, instance, username)?;
            Ok(())
        }
        TimecardCommand::Set {
            card,
            day,
            hours,
            add,
            week,
            dry_run,
            yes,
            category,
            sun,
            mon,
            tue,
            wed,
            thu,
            fri,
            sat,
        } => {
            let updates = collect_timecard_updates(
                day.as_deref(),
                hours.as_deref(),
                [
                    (Weekday::Sun, sun.as_deref()),
                    (Weekday::Mon, mon.as_deref()),
                    (Weekday::Tue, tue.as_deref()),
                    (Weekday::Wed, wed.as_deref()),
                    (Weekday::Thu, thu.as_deref()),
                    (Weekday::Fri, fri.as_deref()),
                    (Weekday::Sat, sat.as_deref()),
                ],
            )?;
            let week = parse_week_selector(week.as_deref())?;
            let sheet = core.list_my_timecards(week).await?;
            let resolved = resolve_timecard_selector(
                &sheet,
                &card,
                category.as_deref(),
                env_name,
                instance,
                username,
            )?;
            let card_snapshot = sheet.cards[resolved.index].clone();
            print_timecard_update_preview(&card_snapshot, &updates, add, dry_run);
            warn_if_day_totals_exceed_24(&sheet, &card_snapshot.sys_id, &updates, add);
            if dry_run {
                println!("Dry run - no changes were made.");
                return Ok(());
            }
            if !yes && !confirm_action("Apply these timecard hour updates?")? {
                println!("Cancelled.");
                return Ok(());
            }

            for update in updates {
                let value = parse_time_value(&update.hours)?;
                let updated = core
                    .set_timecard_hours(&resolved.sys_id, update.day, value, set_mode(add))
                    .await?;
                println!(
                    "Updated {} {} to {} (total {}).",
                    display::timecard_task_display(&updated),
                    weekday_label(update.day),
                    display_hour(&updated.hours[weekday_index(update.day)]),
                    display_hour(&updated.total)
                );
            }
            let refreshed = core.list_my_timecards(week).await?;
            write_timecard_index_cache(&refreshed, env_name, instance, username)?;
            Ok(())
        }
        TimecardCommand::Edit { week } => {
            let week = parse_week_selector(week.as_deref())?;
            let editor_client = Arc::new(TuiClient::local(Arc::clone(&core)));
            let sheet = tui_app::run_timecard_editor(editor_client, week).await?;
            write_timecard_index_cache(&sheet, env_name, instance, username)?;
            Ok(())
        }
    }
}

#[derive(Debug)]
pub(crate) struct TimecardUpdate {
    pub(crate) day: Weekday,
    pub(crate) hours: String,
}

#[derive(Debug)]
pub(crate) struct ResolvedTimecard {
    sys_id: String,
    index: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TimecardSelectorShape {
    SysId,
    Index(usize),
    Task,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TimecardIndexCache {
    entries: Vec<TimecardIndexCacheEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TimecardIndexCacheEntry {
    key: TimecardIndexCacheKey,
    sys_id: String,
    fingerprint: TimecardFingerprint,
    expires_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TimecardIndexCacheKey {
    env: String,
    instance: String,
    username: String,
    actor_user_sys_id: String,
    week_starts_on: String,
    sheet_sys_id: String,
    index: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TimecardFingerprint {
    task_sys_id: String,
    task_display: String,
    category: String,
    project_time_category: String,
    week_starts_on: String,
}

pub(crate) fn parse_week_selector(value: Option<&str>) -> Result<WeekSelector, SnowError> {
    match value {
        Some(value) => NaiveDate::parse_from_str(value, "%Y-%m-%d")
            .map(WeekSelector::Date)
            .map_err(|_| SnowError::Api("--week must be formatted as YYYY-MM-DD".to_string())),
        None => Ok(WeekSelector::Current),
    }
}

pub(crate) fn collect_timecard_updates(
    day: Option<&str>,
    hours: Option<&str>,
    day_flags: [(Weekday, Option<&str>); 7],
) -> Result<Vec<TimecardUpdate>, SnowError> {
    let mut updates = Vec::new();
    if day.is_some() || hours.is_some() {
        let day = day.ok_or_else(|| {
            SnowError::Api("single-day form requires both <day> and <hours>".to_string())
        })?;
        let hours = hours.ok_or_else(|| {
            SnowError::Api("single-day form requires both <day> and <hours>".to_string())
        })?;
        updates.push(TimecardUpdate {
            day: parse_weekday(day)?,
            hours: normalize_hours(hours)?,
        });
    }

    let mut flag_updates = Vec::new();
    for (day, value) in day_flags {
        if let Some(value) = value {
            flag_updates.push(TimecardUpdate {
                day,
                hours: normalize_hours(value)?,
            });
        }
    }

    if !updates.is_empty() && !flag_updates.is_empty() {
        return Err(SnowError::Api(
            "use either positional <day> <hours> or --sun/--mon/... flags, not both".to_string(),
        ));
    }
    updates.extend(flag_updates);
    if updates.is_empty() {
        return Err(SnowError::Api(
            "provide a day and hours, or one or more --sun/--mon/... flags".to_string(),
        ));
    }
    Ok(updates)
}

pub(crate) fn parse_weekday(value: &str) -> Result<Weekday, SnowError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "sun" | "sunday" => Ok(Weekday::Sun),
        "mon" | "monday" => Ok(Weekday::Mon),
        "tue" | "tues" | "tuesday" => Ok(Weekday::Tue),
        "wed" | "wednesday" => Ok(Weekday::Wed),
        "thu" | "thur" | "thurs" | "thursday" => Ok(Weekday::Thu),
        "fri" | "friday" => Ok(Weekday::Fri),
        "sat" | "saturday" => Ok(Weekday::Sat),
        _ => Err(SnowError::Api(
            "unknown day; use sun, mon, tue, wed, thu, fri, or sat".to_string(),
        )),
    }
}

pub(crate) fn normalize_hours(value: &str) -> Result<String, SnowError> {
    let parsed = value
        .trim()
        .parse::<f64>()
        .map_err(|_| SnowError::Api(format!("invalid hours value {value:?}")))?;
    if !parsed.is_finite() || !(0.0..=24.0).contains(&parsed) {
        return Err(SnowError::Api(
            "hours must be a decimal value from 0 through 24".to_string(),
        ));
    }
    Ok(format_hours(parsed))
}

pub(crate) fn parse_time_value(hours: &str) -> Result<TimeValue, SnowError> {
    hours
        .parse::<TimeValue>()
        .map_err(|_| SnowError::Api(format!("invalid hours value {hours:?}")))
}

pub(crate) fn set_mode(add: bool) -> SetMode {
    if add { SetMode::Add } else { SetMode::Set }
}

pub(crate) fn resolve_timecard_selector(
    sheet: &TimecardSheet,
    selector: &str,
    category: Option<&str>,
    env_name: &str,
    instance: &str,
    username: &str,
) -> Result<ResolvedTimecard, SnowError> {
    match classify_timecard_selector(selector) {
        TimecardSelectorShape::SysId => sheet
            .cards
            .iter()
            .enumerate()
            .find(|(_, card)| card.sys_id.eq_ignore_ascii_case(selector))
            .map(|(index, card)| ResolvedTimecard {
                sys_id: card.sys_id.clone(),
                index,
            })
            .ok_or_else(|| {
                SnowError::NotFound(format!(
                    "time card {selector} is not present in the selected week"
                ))
            }),
        TimecardSelectorShape::Index(index) => {
            resolve_timecard_index(sheet, index, env_name, instance, username)
        }
        TimecardSelectorShape::Task => resolve_timecard_task(sheet, selector, category),
    }
}

pub(crate) fn classify_timecard_selector(selector: &str) -> TimecardSelectorShape {
    let trimmed = selector.trim();
    if trimmed.len() == 32 && trimmed.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return TimecardSelectorShape::SysId;
    }
    if !trimmed.is_empty()
        && trimmed.chars().all(|ch| ch.is_ascii_digit())
        && let Ok(index) = trimmed.parse::<usize>()
    {
        return TimecardSelectorShape::Index(index);
    }
    TimecardSelectorShape::Task
}

pub(crate) fn resolve_timecard_task(
    sheet: &TimecardSheet,
    selector: &str,
    category: Option<&str>,
) -> Result<ResolvedTimecard, SnowError> {
    let mut matches = sheet
        .cards
        .iter()
        .enumerate()
        .filter(|(_, card)| task_matches(card, selector))
        .collect::<Vec<_>>();
    if let Some(category) = category {
        matches.retain(|(_, card)| category_matches(card, category));
    }
    match matches.as_slice() {
        [] => Err(SnowError::NotFound(format!(
            "no time card matched task {selector:?} in the selected week"
        ))),
        [(index, card)] => Ok(ResolvedTimecard {
            sys_id: card.sys_id.clone(),
            index: *index,
        }),
        _ => {
            let mut message =
                format!("task {selector:?} matched multiple time cards; pass --category:\n");
            for (index, card) in matches {
                let category = if card.category.trim().is_empty() {
                    card.category_label.as_str()
                } else {
                    card.category.as_str()
                };
                let _ = writeln!(
                    message,
                    "  {}  {}  category={}  sys_id={}",
                    index + 1,
                    display::timecard_task_display(card),
                    category,
                    card.sys_id
                );
            }
            Err(SnowError::Api(message))
        }
    }
}

pub(crate) fn resolve_timecard_index(
    sheet: &TimecardSheet,
    index: usize,
    env_name: &str,
    instance: &str,
    username: &str,
) -> Result<ResolvedTimecard, SnowError> {
    if index == 0 {
        return Err(SnowError::Api(
            "timecard list indexes start at 1".to_string(),
        ));
    }
    let key = cache_key_for(sheet, index, env_name, instance, username)?;
    let cache = read_timecard_index_cache();
    let now = Utc::now().timestamp();
    let Some(entry) = cache
        .entries
        .iter()
        .find(|entry| entry.key == key && entry.expires_at > now)
    else {
        return Err(SnowError::Api(format!(
            "time card index {index} is not in the short-lived cache; rerun `snow timecard list`"
        )));
    };

    let Some((fresh_index, card)) = sheet
        .cards
        .iter()
        .enumerate()
        .find(|(_, card)| card.sys_id == entry.sys_id)
    else {
        return Err(SnowError::Api(format!(
            "cached time card index {index} no longer exists; rerun `snow timecard list`"
        )));
    };
    let fresh_fingerprint = fingerprint_timecard(card);
    if fresh_fingerprint != entry.fingerprint {
        return Err(SnowError::Api(format!(
            "cached time card index {index} changed; rerun `snow timecard list`"
        )));
    }
    Ok(ResolvedTimecard {
        sys_id: card.sys_id.clone(),
        index: fresh_index,
    })
}

pub(crate) fn task_matches(card: &TimeCard, selector: &str) -> bool {
    let selector = selector.trim();
    card.task
        .as_ref()
        .map(|task| {
            task.number.eq_ignore_ascii_case(selector)
                || task.sys_id.eq_ignore_ascii_case(selector)
                || display::timecard_task_display(card).eq_ignore_ascii_case(selector)
        })
        .unwrap_or(false)
}

pub(crate) fn category_matches(card: &TimeCard, category: &str) -> bool {
    card.category.eq_ignore_ascii_case(category)
        || card.category_label.eq_ignore_ascii_case(category)
}

pub(crate) fn write_timecard_index_cache(
    sheet: &TimecardSheet,
    env_name: &str,
    instance: &str,
    username: &str,
) -> Result<(), SnowError> {
    if sheet.cards.is_empty() {
        return Ok(());
    }
    let now = Utc::now().timestamp();
    let expires_at = now + 10 * 60;
    let mut cache = read_timecard_index_cache();
    cache.entries.retain(|entry| entry.expires_at > now);

    let sheet_sys_id = sheet_sys_id(sheet);
    let actor_user_sys_id = actor_user_sys_id(sheet, username);
    cache.entries.retain(|entry| {
        !(entry.key.env == env_name
            && entry.key.instance == normalize_cache_token(instance)
            && entry.key.username == username
            && entry.key.actor_user_sys_id == actor_user_sys_id
            && entry.key.week_starts_on == sheet.week_starts_on
            && entry.key.sheet_sys_id == sheet_sys_id)
    });

    for (index, card) in sheet.cards.iter().enumerate() {
        cache.entries.push(TimecardIndexCacheEntry {
            key: TimecardIndexCacheKey {
                env: env_name.to_string(),
                instance: normalize_cache_token(instance),
                username: username.to_string(),
                actor_user_sys_id: actor_user_sys_id.clone(),
                week_starts_on: sheet.week_starts_on.clone(),
                sheet_sys_id: sheet_sys_id.clone(),
                index: index + 1,
            },
            sys_id: card.sys_id.clone(),
            fingerprint: fingerprint_timecard(card),
            expires_at,
        });
    }

    let path = timecard_index_cache_path();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let bytes = serde_json::to_vec_pretty(&cache).map_err(|err| SnowError::Api(err.to_string()))?;
    std::fs::write(path, bytes)?;
    Ok(())
}

pub(crate) fn read_timecard_index_cache() -> TimecardIndexCache {
    let path = timecard_index_cache_path();
    std::fs::read(path)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<TimecardIndexCache>(&bytes).ok())
        .unwrap_or_else(|| TimecardIndexCache {
            entries: Vec::new(),
        })
}

pub(crate) fn timecard_index_cache_path() -> PathBuf {
    runtime_paths().root.join("timecard-index-cache.json")
}

pub(crate) fn cache_key_for(
    sheet: &TimecardSheet,
    index: usize,
    env_name: &str,
    instance: &str,
    username: &str,
) -> Result<TimecardIndexCacheKey, SnowError> {
    if sheet.cards.is_empty() {
        return Err(SnowError::NotFound(
            "no time cards found in the selected week".to_string(),
        ));
    }
    Ok(TimecardIndexCacheKey {
        env: env_name.to_string(),
        instance: normalize_cache_token(instance),
        username: username.to_string(),
        actor_user_sys_id: actor_user_sys_id(sheet, username),
        week_starts_on: sheet.week_starts_on.clone(),
        sheet_sys_id: sheet_sys_id(sheet),
        index,
    })
}

pub(crate) fn sheet_sys_id(sheet: &TimecardSheet) -> String {
    sheet
        .sheet
        .as_ref()
        .map(|sheet| sheet.sys_id.clone())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "none".to_string())
}

pub(crate) fn actor_user_sys_id(sheet: &TimecardSheet, fallback: &str) -> String {
    sheet
        .cards
        .first()
        .map(|card| card.user.sys_id.clone())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| fallback.to_string())
}

pub(crate) fn normalize_cache_token(value: &str) -> String {
    value
        .trim()
        .trim_end_matches('/')
        .trim_start_matches("https://")
        .trim_start_matches("http://")
        .to_ascii_lowercase()
}

pub(crate) fn fingerprint_timecard(card: &TimeCard) -> TimecardFingerprint {
    let (task_sys_id, task_display) = card
        .task
        .as_ref()
        .map(|task| (task.sys_id.clone(), task.number.clone()))
        .unwrap_or_else(|| ("".to_string(), "".to_string()));
    TimecardFingerprint {
        task_sys_id,
        task_display,
        category: card.category.clone(),
        project_time_category: card.project_time_category.clone().unwrap_or_default(),
        week_starts_on: card.week_starts_on.clone(),
    }
}

pub(crate) fn print_timecard_update_preview(
    card: &TimeCard,
    updates: &[TimecardUpdate],
    add: bool,
    dry_run: bool,
) {
    if dry_run {
        println!("Dry run preview:");
    } else {
        println!("Timecard update preview:");
    }
    println!(
        "Card: {}  category={}  sys_id={}",
        display::timecard_task_display(card),
        if card.category.trim().is_empty() {
            card.category_label.as_str()
        } else {
            card.category.as_str()
        },
        card.sys_id
    );

    let mut projected_hours = card.hours.clone();
    for update in updates {
        let day_index = weekday_index(update.day);
        let current = parse_hour_value(&projected_hours[day_index]).unwrap_or(0.0);
        let requested = parse_hour_value(&update.hours).unwrap_or(0.0);
        let new_value = if add { current + requested } else { requested };
        println!(
            "  {:<3} {} -> {}",
            weekday_label(update.day),
            format_hours(current),
            format_hours(new_value)
        );
        projected_hours[day_index] = format_hours(new_value);
    }
    let projected_total = projected_hours
        .iter()
        .filter_map(|value| parse_hour_value(value))
        .sum::<f64>();
    println!(
        "  Total {} -> {}",
        display_hour(&card.total),
        format_hours(projected_total)
    );
}

pub(crate) fn warn_if_day_totals_exceed_24(
    sheet: &TimecardSheet,
    target_sys_id: &str,
    updates: &[TimecardUpdate],
    add: bool,
) {
    for update in updates {
        let day_index = weekday_index(update.day);
        let mut total = 0.0;
        for card in &sheet.cards {
            let mut value = parse_hour_value(&card.hours[day_index]).unwrap_or(0.0);
            if card.sys_id == target_sys_id {
                let requested = parse_hour_value(&update.hours).unwrap_or(0.0);
                value = if add { value + requested } else { requested };
            }
            total += value;
        }
        if total > 24.0 {
            println!(
                "Warning: {} total across listed cards would be {} hours.",
                weekday_label(update.day),
                format_hours(total)
            );
        }
    }
}

pub(crate) fn weekday_index(day: Weekday) -> usize {
    match day {
        Weekday::Sun => 0,
        Weekday::Mon => 1,
        Weekday::Tue => 2,
        Weekday::Wed => 3,
        Weekday::Thu => 4,
        Weekday::Fri => 5,
        Weekday::Sat => 6,
    }
}

pub(crate) fn weekday_label(day: Weekday) -> &'static str {
    match day {
        Weekday::Sun => "Sun",
        Weekday::Mon => "Mon",
        Weekday::Tue => "Tue",
        Weekday::Wed => "Wed",
        Weekday::Thu => "Thu",
        Weekday::Fri => "Fri",
        Weekday::Sat => "Sat",
    }
}

pub(crate) fn parse_hour_value(value: &str) -> Option<f64> {
    let value = value.trim();
    if value.is_empty() {
        return Some(0.0);
    }
    value.parse::<f64>().ok()
}

pub(crate) fn display_hour(value: &str) -> String {
    parse_hour_value(value)
        .map(format_hours)
        .unwrap_or_else(|| value.to_string())
}

pub(crate) fn format_hours(value: f64) -> String {
    let rounded = (value * 100.0).round() / 100.0;
    if rounded.fract().abs() < f64::EPSILON {
        format!("{}", rounded as i64)
    } else {
        let mut value = format!("{rounded:.2}");
        while value.contains('.') && value.ends_with('0') {
            value.pop();
        }
        if value.ends_with('.') {
            value.pop();
        }
        value
    }
}
