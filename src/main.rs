//! Riffle entry point — dispatches between subcommands `stream` and `demo`.

use anyhow::Result;
use clap::{Parser, Subcommand};

use riffle::cli_demo;
use riffle::cli_stream::{self, StreamArgs};
use riffle::config::Config;

#[derive(Parser, Debug)]
#[command(
    name = "riffle",
    about = "Delta Lake CDC toolkit: streaming transformation + producer/consumer demo, both with web dashboard",
    version
)]
struct Cli {
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Streaming Delta-to-Delta transformation (append/overwrite/merge) with live web dashboard.
    /// Run with no source/target to configure & start jobs from the dashboard UI.
    Stream(StreamArgs),
    /// Synthetic CDC producer + adaptive consumer demo with live web dashboard.
    Demo(Config),
}

#[tokio::main]
async fn main() -> Result<()> {
    let filter = std::env::var("RUST_LOG").unwrap_or_else(|_| "riffle=debug,warn".to_string());
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(filter))
        .with_target(false)
        .init();

    let cli = Cli::parse();
    match cli.command {
        Cmd::Stream(args) => cli_stream::run(args).await,
        Cmd::Demo(cfg) => cli_demo::run(cfg).await,
    }
}
