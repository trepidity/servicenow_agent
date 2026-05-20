//! `snow daemon logs` — print and optionally tail the daemon logfile.
//!
//! Modes:
//!   - `--lines N` (no follow): print the last N lines and exit.
//!   - `--lines N --follow`:    print last N, then tail.
//!   - `--no-follow` (no lines): print the entire file and exit.
//!   - default (`--follow`):    tail from current end, polling for new lines.

use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::time::Duration;

use anyhow::{Context, Result};

use super::paths::DaemonPaths;

/// Run `snow daemon logs`. `follow` defaults to true at the CLI layer.
/// If `lines` is set, prints the last `lines` lines first.
pub fn run(follow: bool, lines: Option<usize>) -> Result<()> {
    let paths = DaemonPaths::resolve()?;
    let mut file = std::fs::File::open(&paths.logfile)
        .with_context(|| format!("opening {}", paths.logfile.display()))?;

    if let Some(n) = lines {
        let content = std::fs::read_to_string(&paths.logfile)?;
        let collected: Vec<&str> = content.lines().collect();
        let start = collected.len().saturating_sub(n);
        for line in &collected[start..] {
            println!("{line}");
        }
        if !follow {
            return Ok(());
        }
        file.seek(SeekFrom::End(0))?;
    }

    if !follow {
        let mut content = String::new();
        std::io::Read::read_to_string(&mut file, &mut content)?;
        print!("{content}");
        return Ok(());
    }

    let mut reader = BufReader::new(file);
    loop {
        let mut buf = String::new();
        let read = reader.read_line(&mut buf)?;
        if read == 0 {
            std::thread::sleep(Duration::from_millis(200));
            continue;
        }
        print!("{buf}");
    }
}
