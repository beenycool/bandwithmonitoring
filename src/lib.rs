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

    std::panic::set_hook(Box::new(|info| {
        tracing::error!(panic = %info, "thread panicked");
    }));

    tracing::info!(version = VERSION, ?args, "bandwith starting");

    if let Some(cmd) = args.command {
        return run_query(cmd);
    }

    if args.demo {
        return run_demo();
    }

    if args.headless {
        return run_headless();
    }

    // Phase 1 stub: don't open a window yet, just return.
    tracing::info!("GUI not yet implemented in phase 1");
    Ok(())
}

/// `bandwith --query ...`: read-side queries against the on-disk DB.
///
/// Lets users verify what was captured without installing `sqlite3.exe`.
fn run_query(cmd: cli::QueryCmd) -> Result<()> {
    use crate::cli::QueryCmd;
    use crate::paths::db_path;
    use crate::store::Query;
    use std::time::{SystemTime, UNIX_EPOCH};

    let db = db_path();
    let q = Query::open(&db)?;
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);

    match cmd {
        QueryCmd::TopProcesses { last, limit } => {
            let rows = q.top_processes(now_ms - last * 1000, limit)?;
            println!("Top {} processes (last {}s):", rows.len().min(limit), last);
            for r in rows {
                println!(
                    "  {:30} in={:>12} out={:>12}",
                    r.key, r.bytes_in, r.bytes_out
                );
            }
        }
        QueryCmd::TopDomains { last, limit } => {
            let rows = q.top_domains(now_ms - last * 1000, limit)?;
            println!("Top {} domains (last {}s):", rows.len().min(limit), last);
            for r in rows {
                println!(
                    "  {:30} in={:>12} out={:>12}",
                    r.key, r.bytes_in, r.bytes_out
                );
            }
        }
        QueryCmd::Totals { last } => {
            let (bin, bout) = q.total_since(now_ms - last * 1000)?;
            println!(
                "Last {}s: in={} out={} total={}",
                last,
                bin,
                bout,
                bin + bout
            );
        }
        QueryCmd::Stats => {
            let count = q.count()?;
            let size = std::fs::metadata(&db).map(|m| m.len()).unwrap_or(0);
            println!("Rows: {}", count);
            println!("DB:   {} ({} bytes)", db.display(), size);
        }
    }
    Ok(())
}

/// `bandwith --headless`: full capture pipeline on Windows.
///
/// Wires together the ETW consumer (phase 4), the flow aggregator (phase 2)
/// and the SQLite writer (phase 3). On non-Windows hosts this returns
/// `Err` — the headless mode is Windows-only by design.
#[cfg(windows)]
fn run_headless() -> Result<()> {
    use crate::capture::flow::FlowAggregator;
    use crate::capture::process::ProcessCache;
    use crate::capture::EtwCapture;
    use crate::dns::{Resolver, ResolverConfig2};
    use crate::paths::db_path;
    use crate::store::{spawn, WriterConfig};
    use crossbeam_channel::bounded;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    let db = db_path();
    tracing::info!(?db, "headless: starting capture pipeline");
    let handle = spawn(&db, WriterConfig::default())?;

    let (tx, rx) = bounded::<crate::capture::ConnEvent>(50_000);

    let etw = EtwCapture::new()?;

    let shutdown = Arc::new(AtomicBool::new(false));

    let handle_for_agg = handle.clone();
    let agg_thread = std::thread::Builder::new()
        .name("bandwith-aggregator".into())
        .spawn(move || {
            let dns_rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to build DNS runtime");
            let (resolver, mut dns_rx) =
                Resolver::spawn(ResolverConfig2::default(), dns_rt.handle())
                    .expect("failed to spawn DNS resolver");

            let mut agg = FlowAggregator::new();
            let mut proc_cache = ProcessCache::new();
            let _ = proc_cache.refresh();

            loop {
                dns_rt.block_on(async {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                });
                while let Ok(result) = dns_rx.try_recv() {
                    if let Some(name) = result.name {
                        agg.update_domain(result.ip, name);
                    }
                }

                match rx.recv_timeout(Duration::from_secs(2)) {
                    Ok(ev) => {
                        resolver.request(ev.remote_ip);
                        agg.observe(ev);
                    }
                    Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                        if let Ok(()) = proc_cache.refresh() {
                            agg.update_proc_names(proc_cache.snapshot_map());
                        }
                        agg.maybe_flush(&handle_for_agg);
                    }
                    Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                        tracing::info!("aggregator: event channel closed, flushing and exiting");
                        if let Ok(()) = proc_cache.refresh() {
                            agg.update_proc_names(proc_cache.snapshot_map());
                        }
                        agg.flush_now(&handle_for_agg);
                        return;
                    }
                }
            }
        })?;

    let etw_shutdown = shutdown.clone();
    let etw_thread = std::thread::Builder::new()
        .name("bandwith-etw".into())
        .spawn(move || {
            if let Err(e) = etw.run(tx, etw_shutdown) {
                tracing::error!(error = %e, "ETW capture failed");
            }
        })?;

    let shutdown_timer = std::thread::Builder::new()
        .name("bandwith-shutdown-signal".into())
        .spawn(move || {
            let secs: u64 = match std::env::var("BANDWITH_HEADLESS_SECS") {
                Ok(s) => match s.parse() {
                    Ok(n) => n,
                    Err(e) => {
                        tracing::warn!(
                            value = %s,
                            error = %e,
                            "BANDWITH_HEADLESS_SECS is not a valid u64, ignoring"
                        );
                        return;
                    }
                },
                Err(_) => return,
            };
            tracing::info!(
                secs,
                "BANDWITH_HEADLESS_SECS set; auto-shutting down after this many seconds"
            );
            std::thread::sleep(Duration::from_secs(secs));
            shutdown.store(true, Ordering::Relaxed);
        })?;

    let _ = etw_thread.join();
    let _ = agg_thread.join();
    let _ = shutdown_timer.join();
    handle.shutdown()?;
    tracing::info!("headless: clean shutdown");
    Ok(())
}

#[cfg(not(windows))]
fn run_headless() -> Result<()> {
    anyhow::bail!("--headless mode is Windows-only (requires ETW)")
}

fn run_demo() -> Result<()> {
    use crate::capture::Proto;
    use crate::dns::{Resolver, ResolverConfig2};
    use crate::paths::db_path;
    use crate::store::{spawn, Query, Row, WriterConfig};
    use std::net::Ipv4Addr;
    use std::time::{SystemTime, UNIX_EPOCH};

    println!(
        "[phase 4] ETW capture wired in --headless mode; run `bandwith --headless` on Windows to capture live traffic"
    );

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

    // Phase 3 sanity check: feed events through the aggregator and verify
    // they end up in the DB. Runs BEFORE shutdown so the rows hit the writer
    // through the same channel as the 1000 rows above.
    use crate::capture::flow::FlowAggregator;
    use crate::capture::{ConnEvent, Direction};
    use std::net::IpAddr;
    let mut agg = FlowAggregator::new();
    for i in 0..10 {
        agg.observe(ConnEvent {
            ts_ms: now_ms,
            pid: 9000,
            proto: Proto::Tcp,
            remote_ip: IpAddr::V4(std::net::Ipv4Addr::new(8, 8, 8, 8)),
            remote_port: 443,
            bytes: 1000,
            direction: if i % 2 == 0 {
                Direction::In
            } else {
                Direction::Out
            },
        });
    }
    agg.update_proc_name(9000, "demo_proc.exe".to_string());
    let flushed = agg.flush_now(&handle);
    println!(
        "\n[phase 3] aggregator flushed {} rows through the writer",
        flushed
    );

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

    let top = q.top_processes(now_ms - 60_000, 5)?;
    let demo_row = top.iter().find(|r| r.key == "demo_proc.exe");
    assert!(demo_row.is_some(), "aggregator row should be in DB");
    println!("[phase 3] verified: demo_proc.exe in top_processes");

    {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let _guard = rt.enter();
        let (resolver, mut results) = Resolver::spawn(ResolverConfig2::default(), rt.handle())?;
        resolver.request("1.1.1.1".parse().unwrap());
        let _ = rt.block_on(async {
            tokio::time::timeout(std::time::Duration::from_secs(5), results.recv()).await
        });
        let name = resolver.cached_name("1.1.1.1".parse().unwrap());
        match name {
            Some(n) => println!("[phase 5] resolved 1.1.1.1 -> {}", n),
            None => println!("[phase 5] no cached name for 1.1.1.1 (network offline?)"),
        }
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
