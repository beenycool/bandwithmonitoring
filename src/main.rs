use anyhow::Result;
use bandwith::cli::Args;
use clap::Parser;

fn main() -> Result<()> {
    let args = Args::parse();
    bandwith::run(args)
}
