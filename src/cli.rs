use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Debug, Clone, Parser)]
#[command(name = "bandwith", about = "Lightweight bandwidth monitor", version)]
pub struct Args {
    /// Run without GUI/tray (capture + DB only, useful for servers and CI).
    #[arg(long)]
    pub headless: bool,

    /// Synthesize 1000 fake rows, write them, print top domains, exit.
    /// Smoke test for the storage layer.
    #[arg(long)]
    pub demo: bool,

    /// Path to config file (defaults to %APPDATA%/bandwith/config.toml on Windows,
    /// ~/.config/bandwith/config.toml elsewhere).
    #[arg(long)]
    pub config: Option<PathBuf>,

    #[command(subcommand)]
    pub command: Option<QueryCmd>,
}

#[derive(Debug, Clone, Subcommand)]
pub enum QueryCmd {
    /// Print the top N processes by total bytes.
    TopProcesses {
        /// Time window in seconds (default 600 = 10 min).
        #[arg(long, default_value = "600")]
        last: i64,
        /// Max rows (default 20).
        #[arg(long, default_value_t = 20)]
        limit: usize,
    },
    /// Print the top N domains by total bytes.
    TopDomains {
        #[arg(long, default_value = "600")]
        last: i64,
        #[arg(long, default_value_t = 20)]
        limit: usize,
    },
    /// Print total bytes in/out for the time window.
    Totals {
        #[arg(long, default_value = "600")]
        last: i64,
    },
    /// Print total row count and DB size.
    Stats,
}
