//! Integration smoke test: the library compiles, CLI parses, and
//! `run` returns Ok in headless mode.

use bandwith::cli::Args;
use clap::Parser;

#[test]
fn cli_default_args() {
    let args = Args::parse_from(["bandwith", "--headless"]);
    assert!(args.headless);
    assert!(args.config.is_none());
}

#[test]
fn cli_with_config_path() {
    let args = Args::parse_from(["bandwith", "--headless", "--config", "/tmp/c.toml"]);
    assert!(args.headless);
    assert_eq!(args.config.unwrap().to_str(), Some("/tmp/c.toml"));
}

#[test]
fn run_headless_succeeds() {
    let args = Args::parse_from(["bandwith", "--headless", "--shutdown-after", "1"]);
    let result = bandwith::run(args);
    // --headless is Windows-only (requires ETW). On non-Windows, run() returns
    // Err. On Windows, it should succeed (we just want a clean exit code path).
    #[cfg(windows)]
    result.expect("headless run should succeed on Windows");
    #[cfg(not(windows))]
    {
        let _ = result; // suppress unused warning
    }
}
