//! Riffle entry point — dispatches between subcommands `stream` and `demo`.

use anyhow::Result;
use clap::{Parser, Subcommand};

use riffle::cli_create::{self, CreateTableArgs};
use riffle::cli_demo;
use riffle::cli_enable_cdf::{self, EnableCdfArgs};
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
    /// Create a small Delta table with N commits of synthetic data (useful for
    /// iterating on stream/merge performance against a controlled source).
    CreateTable(CreateTableArgs),
    /// Enable Delta Change Data Feed (`delta.enableChangeDataFeed=true`) on an
    /// existing table. Only future commits will be replayable as a change feed.
    EnableCdf(EnableCdfArgs),
    /// Synthetic CDC producer + adaptive consumer demo with live web dashboard.
    Demo(Config),
}

// Use a larger worker pool than the default (= num CPUs) to give DataFusion's
// CPU-bound query execution headroom while still leaving cycles for the web
// server, SSE feed, and source-version polling. The OS-thread heartbeat in
// the stream command is independent of this and never gets starved.
#[tokio::main(flavor = "multi_thread", worker_threads = 32)]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    // For Stream the dashboard installs its own tracing subscriber (layered
    // with the log-buffer for the web tail). Other subcommands use the
    // basic fmt subscriber here.
    if !matches!(cli.command, Cmd::Stream(_)) {
        let filter = std::env::var("RUST_LOG").unwrap_or_else(|_| "riffle=debug,warn".to_string());
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::new(filter))
            .with_target(false)
            .try_init();
    }

    match cli.command {
        Cmd::Stream(args) => cli_stream::run(args).await,
        Cmd::CreateTable(args) => cli_create::run(args).await,
        Cmd::EnableCdf(args) => cli_enable_cdf::run(args).await,
        Cmd::Demo(cfg) => cli_demo::run(cfg).await,
    }
}
