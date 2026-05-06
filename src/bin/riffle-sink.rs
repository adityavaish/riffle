//! `riffle-sink` — standalone Delta-to-Delta transfer CLI.
//!
//! Reads new commits from a source Delta table and applies them to a target
//! Delta table using one of three semantics: `append`, `overwrite`, `merge`.
//!
//! Auth: by default uses the same `auto` chain as the dashboard for Azure
//! sources/targets — service principal env vars → managed identity →
//! Azure CLI / VS Code Azure Account extension.

use anyhow::{Context, Result};
use clap::Parser;
use deltalake::{open_table_with_storage_options, DeltaTable};
use std::path::PathBuf;
use std::time::Duration;

use riffle::config::{build_storage_options, register_handlers_for, Backend};
use riffle::sink::{self, SinkConfig};

#[derive(Parser, Debug, Clone)]
#[command(
    name = "riffle-sink",
    about = "Generic Delta-to-Delta transfer with append / overwrite / merge semantics",
    version
)]
struct Args {
    /// Source Delta table URI (file path, abfss://, s3://, gs://).
    #[arg(long)]
    source_uri: String,

    /// Target Delta table URI. Created on first use if it does not exist.
    #[arg(long)]
    target_uri: String,

    /// Transfer mode: append | overwrite | merge.
    #[arg(long, default_value = "merge")]
    sink_mode: String,

    /// Comma-separated key columns for MERGE join. Required when --sink-mode merge.
    #[arg(long, default_value = "")]
    merge_keys: String,

    /// Comma-separated columns to UPDATE on match. Default = all non-key source columns.
    #[arg(long, default_value = "")]
    merge_update_columns: String,

    /// Optional SQL predicate gating WHEN MATCHED UPDATE (against `source.*` / `target.*`).
    #[arg(long)]
    merge_update_predicate: Option<String>,

    /// Optional SQL predicate triggering WHEN MATCHED DELETE.
    #[arg(long)]
    merge_delete_predicate: Option<String>,

    /// Optional SQL predicate gating WHEN NOT MATCHED INSERT.
    #[arg(long)]
    merge_insert_predicate: Option<String>,

    /// Start version (inclusive). If omitted, resumes from checkpoint or starts at 0.
    #[arg(long)]
    start_version: Option<i64>,

    /// End version (inclusive). If omitted, runs to the latest visible version
    /// (and keeps polling unless --once is given).
    #[arg(long)]
    end_version: Option<i64>,

    /// Process one batch of new versions and exit (no polling).
    #[arg(long)]
    once: bool,

    /// Polling interval in seconds when running in follow mode.
    #[arg(long, default_value_t = 5)]
    poll_interval_secs: u64,

    /// Path to JSON checkpoint file (stores last successfully sunk version).
    #[arg(long, default_value = "./riffle-sink-ckpt.json")]
    checkpoint_file: PathBuf,

    /// Maximum number of source versions coalesced into a single sink call.
    #[arg(long, default_value_t = 10)]
    max_versions_per_batch: usize,

    /// Azure auth method when source or target is `abfss://`.
    /// auto = service principal env → MSI → Azure CLI / VS Code.
    #[arg(long, default_value = "auto")]
    azure_auth: String,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Default, Clone)]
struct Checkpoint {
    last_sunk_version: i64,
}

fn load_checkpoint(path: &std::path::Path) -> Option<Checkpoint> {
    let bytes = std::fs::read(path).ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn save_checkpoint(path: &std::path::Path, ckpt: &Checkpoint) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(ckpt)?;
    std::fs::write(path, bytes).with_context(|| format!("write {}", path.display()))?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let filter = std::env::var("RUST_LOG").unwrap_or_else(|_| "riffle=info,warn".to_string());
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(filter))
        .with_target(false)
        .init();

    let args = Args::parse();

    let source_backend = Backend::detect(&args.source_uri);
    let target_backend = Backend::detect(&args.target_uri);
    register_handlers_for(&[source_backend, target_backend]);

    let source_storage_options = build_storage_options(&args.source_uri, &args.azure_auth)?;
    let target_storage_options = build_storage_options(&args.target_uri, &args.azure_auth)?;

    let mode = sink::parse_mode(
        &args.sink_mode,
        &args.merge_keys,
        &args.merge_update_columns,
        args.merge_update_predicate.clone(),
        args.merge_delete_predicate.clone(),
        args.merge_insert_predicate.clone(),
    )?;

    let sink_cfg = SinkConfig {
        source_uri: args.source_uri.clone(),
        target_uri: args.target_uri.clone(),
        source_storage_options: source_storage_options.clone(),
        target_storage_options,
        mode,
    };

    println!("=== riffle-sink ===");
    println!("Source : {}  ({:?})", args.source_uri, source_backend);
    println!("Target : {}  ({:?})", args.target_uri, target_backend);
    println!("Mode   : {}", sink_cfg.mode.name());
    println!();

    // Determine start version: explicit flag → checkpoint → 0.
    let mut last_sunk: i64 = match args.start_version {
        Some(v) => v - 1,
        None => load_checkpoint(&args.checkpoint_file)
            .map(|c| c.last_sunk_version)
            .unwrap_or(-1),
    };
    tracing::info!("[sink-cli] resuming after v{}", last_sunk);

    loop {
        let source: DeltaTable =
            open_table_with_storage_options(&args.source_uri, source_storage_options.clone())
                .await
                .with_context(|| format!("open source {}", args.source_uri))?;
        let current_version = source.version();
        let cap = args
            .end_version
            .unwrap_or(current_version)
            .min(current_version);

        if last_sunk >= cap {
            if args.once {
                println!("Up to date (v{}). Exiting (--once).", current_version);
                return Ok(());
            }
            tokio::time::sleep(Duration::from_secs(args.poll_interval_secs)).await;
            continue;
        }

        // Coalesce up to max_versions_per_batch new versions per sink call.
        let mut group_start = last_sunk + 1;
        while group_start <= cap {
            let group_end =
                (group_start + args.max_versions_per_batch as i64 - 1).min(cap);
            let versions: Vec<i64> = (group_start..=group_end).collect();
            tracing::info!(
                "[sink-cli] applying versions {}..={} (mode={})",
                group_start,
                group_end,
                sink_cfg.mode.name()
            );
            let outcome = sink::apply(&sink_cfg, &source, &versions).await?;
            println!(
                "v{}-v{} | source_rows={} | inserts={} updates={} deletes={} appended={} | tgt v{} | {}ms",
                group_start,
                group_end,
                outcome.source_rows,
                outcome.inserts,
                outcome.updates,
                outcome.deletes,
                outcome.appended,
                outcome.target_version,
                outcome.duration_ms
            );
            last_sunk = group_end;
            save_checkpoint(
                &args.checkpoint_file,
                &Checkpoint {
                    last_sunk_version: last_sunk,
                },
            )
            .ok();
            group_start = group_end + 1;

            if args.once {
                return Ok(());
            }
        }

        if args.once {
            return Ok(());
        }
    }
}
