/// Format a Task SLA business elapsed percentage for humans.
///
/// `None`, non-finite values, and missing values render as `-`. Finite values
/// render with at most one decimal place and a trailing percent sign.
pub fn format_business_elapsed(value: Option<f64>) -> String {
    let Some(value) = value.filter(|value| value.is_finite()) else {
        return "-".to_string();
    };

    let mut formatted = format!("{value:.1}");
    if formatted.ends_with(".0") {
        formatted.truncate(formatted.len() - 2);
    }
    format!("{formatted}%")
}

/// Format a raw ServiceNow Task SLA duration field for display.
///
/// ServiceNow duration strings are passed through unchanged after trimming.
/// Missing or blank values render as `-`.
pub fn format_time_left(value: Option<&str>) -> String {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("-")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_sla_display_helpers_format_missing_values() {
        assert_eq!(format_business_elapsed(None), "-");
        assert_eq!(format_business_elapsed(Some(f64::NAN)), "-");
        assert_eq!(format_time_left(None), "-");
        assert_eq!(format_time_left(Some("   ")), "-");
    }

    #[test]
    fn task_sla_display_helpers_format_present_values() {
        assert_eq!(format_business_elapsed(Some(75.0)), "75%");
        assert_eq!(format_business_elapsed(Some(65.25)), "65.2%");
        assert_eq!(
            format_time_left(Some(" 1970-01-01 04:00:00 ")),
            "1970-01-01 04:00:00"
        );
    }
}
