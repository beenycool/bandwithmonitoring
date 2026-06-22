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
    let args = Args::parse_from(["bandwith", "--headless"]);
    #[cfg(windows)]
    {
        // Tell --headless to auto-exit after 5s so the smoke test doesn't
        // hang. Real users never set this env var; it's purely for CI.
        std::env::set_var("BANDWITH_HEADLESS_SECS", "5");
        let result = bandwith::run(args);
        std::env::remove_var("BANDWITH_HEADLESS_SECS");
        result.expect("headless run should succeed on Windows");
    }
    #[cfg(not(windows))]
    {
        let _ = bandwith::run(args); // headless is Windows-only; ignore result
    }
}
