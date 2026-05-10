//! `riffle enable-cdf` — enable Delta Change Data Feed on an existing table.
//!
//! Sets `delta.enableChangeDataFeed=true`. delta-rs auto-bumps the protocol
//! and adds the `changeDataFeed` writer feature when needed (writer v7+).
//!
//! IMPORTANT: ALTER-style enabling only takes effect for FUTURE commits — the
//! Change Data Feed cannot replay versions written before CDF was enabled.

use anyhow::Result;
use clap::Args;

use crate::tables_inspect;

#[derive(Args, Debug, Clone)]
pub struct EnableCdfArgs {
    /// Target Delta table URI (e.g. file:///c:/path/to/table or
    /// abfss://container@account.dfs.core.windows.net/path/to/table/).
    #[arg(long)]
    pub uri: String,

    /// Azure auth: auto | sp | msi | cli.
    #[arg(long, default_value = "auto")]
    pub azure_auth: String,
}

pub async fn run(args: EnableCdfArgs) -> Result<()> {
    println!("=== riffle enable-cdf ===");
    println!("Target  : {}", args.uri);

    let t0 = std::time::Instant::now();
    let res = tables_inspect::enable_cdf(&args.uri, &args.azure_auth).await?;
    let dur_ms = t0.elapsed().as_millis();

    if res.already_enabled {
        println!(
            "delta.enableChangeDataFeed is already true on this table (v{}). No commit written.",
            res.starting_version
        );
        tracing::info!(
            "[enable-cdf] target={} already has CDF enabled at v{}; no-op",
            args.uri,
            res.starting_version
        );
        return Ok(());
    }

    tracing::info!(
        "[enable-cdf] target={} v{} -> v{} (delta.enableChangeDataFeed=true) in {}ms",
        args.uri,
        res.starting_version,
        res.new_version,
        dur_ms
    );
    println!(
        "Enabled delta.enableChangeDataFeed=true (v{} -> v{}, {}ms).",
        res.starting_version, res.new_version, dur_ms
    );
    println!(
        "NOTE: CDF replay is only available for commits AT OR AFTER v{}; \
         older commits are not replayable as a change feed.",
        res.new_version
    );
    Ok(())
}
