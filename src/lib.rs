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

    if args.demo {
        return run_demo();
    }

    if args.headless {
        // Phase 1 stub: just confirm we got here and exit.
        tracing::info!("headless mode — exiting cleanly");
        return Ok(());
    }

    // Phase 1 stub: don't open a window yet, just return.
    tracing::info!("GUI not yet implemented in phase 1");
    Ok(())
}

fn run_demo() -> Result<()> {
    use crate::capture::Proto;
    use crate::paths::db_path;
    use crate::store::{spawn, Query, Row, WriterConfig};
    use std::net::Ipv4Addr;
    use std::time::{SystemTime, UNIX_EPOCH};

    let db = db_path();
    tracing::info!(?db, "demo: writing 1000 fake rows");
    let handle = spawn(&db, WriterConfig::default())?;

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let procs = ["chrome.exe", "powershell.exe", "firefox.exe", "code.exe"];
    let domains = [
        "cloudflare.com",
        "google.com",
        "github.com",
        "stackoverflow.com",
        "rust-lang.org",
    ];
    let mut rng_state: u64 = 0x9E3779B97F4A7C15;

    for i in 0..1000 {
        // xorshift64
        rng_state ^= rng_state << 13;
        rng_state ^= rng_state >> 7;
        rng_state ^= rng_state << 17;
        let r1 = rng_state;
        rng_state ^= rng_state << 13;
        rng_state ^= rng_state >> 7;
        rng_state ^= rng_state << 17;
        let r2 = rng_state;

        let proc = procs[(r1 >> 32) as usize % procs.len()];
        let domain = domains[(r1 as usize) % domains.len()];
        let ip = Ipv4Addr::new(142, 250, (r2 >> 32) as u8, r2 as u8);
        let port = 443u16;
        let bytes_in = (r2 >> 8) % 50000;
        let bytes_out = (r2 >> 16) % 5000;

        handle.submit(Row {
            ts: now_ms - (1000 - i) * 10, // spread over last 10s
            pid: 1000 + (r1 as u32 % 100),
            proc_name: proc.to_string(),
            remote_ip: std::net::IpAddr::V4(ip),
            domain: Some(domain.to_string()),
            dport: port,
            proto: Proto::Tcp,
            bytes_in,
            bytes_out,
        });
    }

    handle.flush();
    handle.shutdown()?;

    let q = Query::open(&db)?;
    let total = q.total_since(now_ms - 60_000)?;
    tracing::info!(
        bytes_in = total.0,
        bytes_out = total.1,
        "demo: total bytes last 60s"
    );

    println!("Top 5 domains (last 60s):");
    for row in q.top_domains(now_ms - 60_000, 5)? {
        println!(
            "  {:30} in={:>10} out={:>10}",
            row.key, row.bytes_in, row.bytes_out
        );
    }
    println!("\nTop 5 processes (last 60s):");
    for row in q.top_processes(now_ms - 60_000, 5)? {
        println!(
            "  {:30} in={:>10} out={:>10}",
            row.key, row.bytes_in, row.bytes_out
        );
    }

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
