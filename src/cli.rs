use clap::Parser;
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

    /// Automatically shut down headless mode after N seconds (useful for CI/testing).
    #[arg(long)]
    pub shutdown_after: Option<u64>,
}
