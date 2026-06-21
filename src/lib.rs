//! bandwith — lightweight Windows bandwidth monitor
//!
//! Cross-platform skeleton; ETW capture is gated to `cfg(windows)`.

pub mod app;
pub mod capture;
pub mod cli;
pub mod config;
pub mod dns;
pub mod paths;
pub mod store;
pub mod ui;

use anyhow::Result;

/// Library version (mirrors Cargo.toml).
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Application entry point used by `main.rs` and the integration test harness.
pub fn run(args: cli::Args) -> Result<()> {
    // Tracing init: filter from RUST_LOG, default to "info".
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
    let _ = tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(tracing_subscriber::fmt::layer().with_target(false))
        .try_init();

    tracing::info!(version = VERSION, ?args, "bandwith starting");

    if args.headless {
        // Phase 1 stub: just confirm we got here and exit.
        tracing::info!("headless mode — exiting cleanly");
        return Ok(());
    }

    // Phase 1 stub: don't open a window yet, just return.
    tracing::info!("GUI not yet implemented in phase 1");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_is_set() {
        assert!(!VERSION.is_empty());
    }
}
