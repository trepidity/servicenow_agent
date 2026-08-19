use super::super::*;

/// Dispatch the `snow server` subcommand family over the daemon client.
pub(crate) async fn cmd_server(
    client: &DaemonRpcClient,
    action: ServerCommand,
) -> Result<(), SnowError> {
    match action {
        ServerCommand::Get {
            sys_id,
            name,
            ip_address,
            fresh,
            json,
            full,
        } => {
            let selector_count = [sys_id.is_some(), name.is_some(), ip_address.is_some()]
                .into_iter()
                .filter(|selected| *selected)
                .count();
            if selector_count != 1 {
                return Err(SnowError::Api(
                    "server get requires exactly one of --sys-id, --name, or --ip-address"
                        .to_string(),
                ));
            }
            let server = client
                .server_get(
                    sys_id.as_deref(),
                    name.as_deref(),
                    ip_address.as_deref(),
                    fresh,
                )
                .await?;
            match server {
                Some(server) => {
                    if json {
                        print_json(&server)?;
                    } else {
                        print!("{}", display::format_server(&server, full));
                    }
                }
                None => println!("Server not found."),
            }
            Ok(())
        }
        ServerCommand::Search {
            name,
            ip_address,
            ci_owner_group,
            class,
            limit,
            json,
            full,
        } => {
            let servers = client
                .server_search(ServerQueryArgs {
                    text: None,
                    name: name.as_deref(),
                    ip_address: ip_address.as_deref(),
                    ci_owner_group: ci_owner_group.as_deref(),
                    class: class.as_deref(),
                    limit,
                })
                .await?;
            if json {
                return print_json(&servers);
            }
            if servers.is_empty() {
                println!("No Servers found.");
                return Ok(());
            }
            for server in &servers {
                if full {
                    print!("{}", display::format_server(server, true));
                } else {
                    println!("{}", display::format_server_summary(server));
                }
                println!();
            }
            Ok(())
        }
        ServerCommand::Query {
            text,
            name,
            ip_address,
            ci_owner_group,
            class,
            limit,
            json,
            full,
        } => {
            let servers = client
                .server_query(ServerQueryArgs {
                    text: text.as_deref(),
                    name: name.as_deref(),
                    ip_address: ip_address.as_deref(),
                    ci_owner_group: ci_owner_group.as_deref(),
                    class: class.as_deref(),
                    limit,
                })
                .await?;
            if json {
                return print_json(&servers);
            }
            if servers.is_empty() {
                println!("No Servers found.");
                return Ok(());
            }
            for server in &servers {
                if full {
                    print!("{}", display::format_server(server, true));
                } else {
                    println!("{}", display::format_server_summary(server));
                }
                println!();
            }
            Ok(())
        }
        ServerCommand::Fields { json } => {
            let fields = client.server_fields().await?;
            if json {
                print_full_dump_or_inline(&fields);
            } else {
                print!("{}", display::format_server_fields(&fields));
            }
            Ok(())
        }
    }
}
