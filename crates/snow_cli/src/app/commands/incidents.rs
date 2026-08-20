use super::super::*;

use crate::cli::IncidentCommand;

/// Dispatch the `snow incident` subcommand family over the daemon client.
///
/// # Errors
///
/// Returns an error when the daemon is unreachable or returns a failure.
pub(crate) async fn cmd_incident(
    client: &DaemonRpcClient,
    action: IncidentCommand,
) -> Result<(), SnowError> {
    match action {
        IncidentCommand::Fields { json } => {
            let envelope = client.incident_fields().await?;
            if json {
                print_full_dump_or_inline(&envelope);
            } else {
                print!("{}", display::format_resource_descriptor(&envelope));
            }
            Ok(())
        }
    }
}
