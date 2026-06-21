use clap::Parser;

#[derive(Debug, Clone, Parser)]
#[command(name = "bandwith", about = "Lightweight bandwidth monitor", version)]
pub struct Args {
    /// Run without GUI/tray (capture + DB only, useful for servers and CI).
    #[arg(long)]
    pub headless: bool,

    /// Path to config file (defaults to %APPDATA%/bandwith/config.toml on Windows,
    /// ~/.config/bandwith/config.toml elsewhere).
    #[arg(long)]
    pub config: Option<std::path::PathBuf>,
}
