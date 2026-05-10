//! `riffle enable-cdf` — enable Delta Change Data Feed on an existing table.
//!
//! Sets `delta.enableChangeDataFeed=true`. delta-rs auto-bumps the protocol
//! and adds the `changeDataFeed` writer feature when needed (writer v7+).
//!
//! IMPORTANT: ALTER-style enabling only takes effect for FUTURE commits — the
//! Change Data Feed cannot replay versions written before CDF was enabled.

use anyhow::{Context, Result};
use clap::Args;
use deltalake::{open_table_with_storage_options, DeltaOps};
use std::collections::HashMap;

use crate::config::{build_storage_options, register_handlers_for, Backend};
use crate::util::{parse_table_uri, version_to_i64};
use deltalake::table::config::TablePropertiesExt as _;

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

    let backend = Backend::detect(&args.uri);
    register_handlers_for(&[backend]);
    let storage_options = build_storage_options(&args.uri, &args.azure_auth)?;

    let table_url = parse_table_uri(&args.uri)?;
    let table = open_table_with_storage_options(table_url, storage_options)
        .await
        .with_context(|| format!("open table {}", args.uri))?;
    let starting_version = version_to_i64(table.version());

    let already_enabled = table
        .snapshot()
        .ok()
        .map(|s| s.table_config().enable_change_data_feed())
        .unwrap_or(false);

    if already_enabled {
        println!(
            "delta.enableChangeDataFeed is already true on this table (v{}). No commit written.",
            starting_version
        );
        tracing::info!(
            "[enable-cdf] target={} already has CDF enabled at v{}; no-op",
            args.uri,
            starting_version
        );
        return Ok(());
    }

    let t0 = std::time::Instant::now();
    let mut props = HashMap::new();
    props.insert(
        "delta.enableChangeDataFeed".to_string(),
        "true".to_string(),
    );
    let updated = DeltaOps(table)
        .set_tbl_properties()
        .with_properties(props)
        .with_raise_if_not_exists(false)
        .await
        .with_context(|| format!("set table properties on {}", args.uri))?;

    let new_version = version_to_i64(updated.version());
    let dur_ms = t0.elapsed().as_millis();
    tracing::info!(
        "[enable-cdf] target={} v{} -> v{} (delta.enableChangeDataFeed=true) in {}ms",
        args.uri,
        starting_version,
        new_version,
        dur_ms
    );
    println!(
        "Enabled delta.enableChangeDataFeed=true (v{} -> v{}, {}ms).",
        starting_version, new_version, dur_ms
    );
    println!(
        "NOTE: CDF replay is only available for commits AT OR AFTER v{}; \
         older commits are not replayable as a change feed.",
        new_version
    );
    Ok(())
}
