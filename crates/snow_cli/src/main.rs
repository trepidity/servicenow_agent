#![allow(clippy::arc_with_non_send_sync)]

mod admin;
mod app;
mod auth;
mod cli;
mod daemon_cmd;
mod display;
mod error;
#[path = "tui/mod.rs"]
mod tui_app;
mod tui_client;

use clap::Parser;
use colored::Colorize;

use cli::Cli;
fn main() {
    let cli = Cli::parse();

    if let Err(error) = app::run_entry(cli) {
        eprintln!("{} {error}", "Error:".red().bold());
        std::process::exit(1);
    }
}
