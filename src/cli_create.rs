//! `riffle create-table` — create a small Delta table with N commits of synthetic data.
//!
//! Useful for iterating on stream/merge performance without needing a large
//! pre-existing source table. Builds a wide configurable schema (default 30
//! columns) with a timestamp column, then optionally runs OPTIMIZE ZORDER BY.

use anyhow::{Context, Result};
use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{TimeDelta, Utc};
use clap::Args;
use deltalake::operations::optimize::OptimizeType;
use deltalake::protocol::SaveMode;
use deltalake::{open_table_with_storage_options, DeltaOps};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;

use crate::config::{build_storage_options, register_handlers_for, Backend};
use crate::state::{CreateTableLaunchConfig, SharedState};
use crate::util::{parse_table_uri, version_to_i64};

#[derive(Args, Debug, Clone)]
pub struct CreateTableArgs {
    /// Target Delta table URI (e.g. abfss://container@account.dfs.core.windows.net/path/to/table/).
    #[arg(long)]
    pub uri: String,

    /// Total number of rows to write across all commits.
    #[arg(long, default_value_t = 200_000)]
    pub rows: usize,

    /// Number of commits (versions) to produce. Rows are split evenly.
    #[arg(long, default_value_t = 1)]
    pub commits: usize,

    /// Total number of columns in the schema (>= 2). The first two are always
    /// `event_id` (Utf8) and `event_timestamp` (Timestamp µs); the remainder
    /// are filled with mixed-type random columns.
    #[arg(long, default_value_t = 30)]
    pub columns: usize,

    /// Column name to ZORDER BY after writing (set empty to skip).
    #[arg(long, default_value = "event_timestamp")]
    pub zorder_by: String,

    /// Overwrite the target if it already exists (default appends to v0 if exists).
    #[arg(long)]
    pub overwrite: bool,

    /// Azure auth: auto | sp | msi | cli.
    #[arg(long, default_value = "auto")]
    pub azure_auth: String,
}

fn build_wide_schema(num_columns: usize) -> Arc<Schema> {
    assert!(num_columns >= 2, "columns must be >= 2");
    let mut fields: Vec<Field> = vec![
        Field::new("event_id", DataType::Utf8, false),
        Field::new(
            "event_timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ];
    // Cycle through types: Utf8, Int64, Float64, Bool, Int32.
    for i in 0..(num_columns - 2) {
        let (name, dtype) = match i % 5 {
            0 => (format!("str_{:02}", i), DataType::Utf8),
            1 => (format!("int64_{:02}", i), DataType::Int64),
            2 => (format!("dbl_{:02}", i), DataType::Float64),
            3 => (format!("bool_{:02}", i), DataType::Boolean),
            _ => (format!("int32_{:02}", i), DataType::Int32),
        };
        fields.push(Field::new(&name, dtype, false));
    }
    Arc::new(Schema::new(fields))
}

fn generate_wide_batch(
    schema: &Arc<Schema>,
    num_rows: usize,
    rng: &mut StdRng,
) -> Result<RecordBatch> {
    let now = Utc::now().naive_utc();

    let event_ids: Vec<String> = (0..num_rows).map(|_| Uuid::new_v4().to_string()).collect();
    let timestamps: Vec<i64> = (0..num_rows)
        .map(|_| {
            // Spread timestamps across ~30 days so Z-ORDER has range to work with.
            (now - TimeDelta::seconds(rng.gen_range(0..30 * 24 * 3600)))
                .and_utc()
                .timestamp_micros()
        })
        .collect();

    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    arrays.push(Arc::new(StringArray::from(
        event_ids.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
    )));
    arrays.push(Arc::new(TimestampMicrosecondArray::from(timestamps)));

    for field in schema.fields().iter().skip(2) {
        let arr: ArrayRef = match field.data_type() {
            DataType::Utf8 => {
                let v: Vec<String> =
                    (0..num_rows).map(|_| Uuid::new_v4().simple().to_string()).collect();
                Arc::new(StringArray::from(
                    v.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                ))
            }
            DataType::Int64 => {
                let v: Vec<i64> = (0..num_rows).map(|_| rng.gen::<i64>()).collect();
                Arc::new(Int64Array::from(v))
            }
            DataType::Int32 => {
                let v: Vec<i32> = (0..num_rows).map(|_| rng.gen::<i32>()).collect();
                Arc::new(Int32Array::from(v))
            }
            DataType::Float64 => {
                let v: Vec<f64> =
                    (0..num_rows).map(|_| rng.gen::<f64>() * 1_000_000.0).collect();
                Arc::new(Float64Array::from(v))
            }
            DataType::Boolean => {
                let v: Vec<bool> = (0..num_rows).map(|_| rng.gen_bool(0.5)).collect();
                Arc::new(BooleanArray::from(v))
            }
            other => anyhow::bail!("unsupported synthetic column type: {:?}", other),
        };
        arrays.push(arr);
    }

    Ok(RecordBatch::try_new(schema.clone(), arrays)?)
}

pub async fn run(args: CreateTableArgs) -> Result<()> {
    println!("=== riffle create-table ===");
    println!("Target  : {}", args.uri);
    println!(
        "Rows    : {} across {} commit(s) ({} columns)",
        args.rows, args.commits, args.columns
    );
    if !args.zorder_by.is_empty() {
        println!("ZORDER  : {}", args.zorder_by);
    }
    let cfg = CreateTableLaunchConfig {
        uri: args.uri,
        rows: args.rows,
        commits: args.commits,
        columns: args.columns,
        zorder_by: args.zorder_by,
        overwrite: args.overwrite,
        azure_auth: args.azure_auth,
    };
    let cancel = Arc::new(std::sync::atomic::AtomicBool::new(false));
    run_inner(cfg, None, cancel).await
}

/// Run a create-table job. If `state` is provided, progress is mirrored into
/// `DashboardState::create_table` so the web UI can show it live.
pub async fn run_inner(
    cfg: CreateTableLaunchConfig,
    state: Option<SharedState>,
    cancel: Arc<std::sync::atomic::AtomicBool>,
) -> Result<()> {
    use std::sync::atomic::Ordering;

    if cfg.commits == 0 {
        anyhow::bail!("commits must be >= 1");
    }
    if cfg.rows == 0 {
        anyhow::bail!("rows must be >= 1");
    }
    if cfg.columns < 2 {
        anyhow::bail!("columns must be >= 2");
    }

    let backend = Backend::detect(&cfg.uri);
    register_handlers_for(&[backend]);

    std::env::set_var("AZURE_AUTH", &cfg.azure_auth);
    let storage_options = build_storage_options(&cfg.uri, &cfg.azure_auth)
        .with_context(|| format!("build storage options for {}", cfg.uri))?;

    let schema = build_wide_schema(cfg.columns);
    let mut rng = StdRng::seed_from_u64(Utc::now().timestamp_millis() as u64);

    let rows_per_commit = (cfg.rows + cfg.commits - 1) / cfg.commits;
    let mut total_rows_written: u64 = 0;
    let t0 = Instant::now();

    if let Some(s) = &state {
        let mut g = s.lock().await;
        g.create_table.running = true;
        g.create_table.last_error.clear();
        g.create_table.target_uri = cfg.uri.clone();
        g.create_table.commits_done = 0;
        g.create_table.total_commits = cfg.commits;
        g.create_table.rows_written = 0;
        g.create_table.total_rows = cfg.rows as u64;
        g.create_table.optimize_status = String::new();
        g.create_table.status = format!("Writing {} commit(s)...", cfg.commits);
        g.create_table.launch_config = Some(cfg.clone());
    }

    tracing::info!(
        "[create-table] target={} rows={} commits={} columns={} zorder_by={} overwrite={}",
        cfg.uri,
        cfg.rows,
        cfg.commits,
        cfg.columns,
        cfg.zorder_by,
        cfg.overwrite
    );

    for commit_idx in 0..cfg.commits {
        if cancel.load(Ordering::Relaxed) {
            tracing::info!("[create-table] cancellation requested, stopping");
            if let Some(s) = &state {
                let mut g = s.lock().await;
                g.create_table.status = "Cancelled".to_string();
                g.create_table.running = false;
            }
            return Ok(());
        }

        let rows_this_commit = if commit_idx == cfg.commits - 1 {
            cfg.rows.saturating_sub(total_rows_written as usize).max(1)
        } else {
            rows_per_commit
        };

        let batch = generate_wide_batch(&schema, rows_this_commit, &mut rng)?;

        if let Some(s) = &state {
            let mut g = s.lock().await;
            g.create_table.status = format!(
                "Writing commit {}/{} ({} rows)...",
                commit_idx + 1,
                cfg.commits,
                rows_this_commit
            );
        }

        let write_start = Instant::now();
        let version = if commit_idx == 0 {
            let mode = if cfg.overwrite {
                SaveMode::Overwrite
            } else {
                SaveMode::Append
            };
            let table_url = parse_table_uri(&cfg.uri)?;
            let ops = DeltaOps::try_from_url_with_storage_options(
                table_url,
                storage_options.clone(),
            )
            .await
            .with_context(|| format!("open/create target {}", cfg.uri))?;
            let res = ops.write(vec![batch]).with_save_mode(mode).await?;
            version_to_i64(res.version())
        } else {
            let table_url = parse_table_uri(&cfg.uri)?;
            let table =
                open_table_with_storage_options(table_url, storage_options.clone())
                    .await
                    .with_context(|| format!("re-open target {}", cfg.uri))?;
            let res = DeltaOps(table)
                .write(vec![batch])
                .with_save_mode(SaveMode::Append)
                .await?;
            version_to_i64(res.version())
        };

        let dur_ms = write_start.elapsed().as_millis() as u64;
        total_rows_written += rows_this_commit as u64;
        tracing::info!(
            "[create-table] commit {}/{}: +{} rows -> v{} in {}ms",
            commit_idx + 1,
            cfg.commits,
            rows_this_commit,
            version,
            dur_ms
        );

        if let Some(s) = &state {
            let mut g = s.lock().await;
            g.create_table.commits_done = commit_idx + 1;
            g.create_table.rows_written = total_rows_written;
            g.create_table.last_commit_ms = dur_ms;
        }
    }

    let total_ms = t0.elapsed().as_millis();
    tracing::info!(
        "[create-table] wrote {} rows across {} commit(s) in {}ms",
        total_rows_written,
        cfg.commits,
        total_ms
    );

    if !cfg.zorder_by.is_empty() {
        if schema.field_with_name(&cfg.zorder_by).is_err() {
            anyhow::bail!(
                "zorder_by column '{}' not found in schema",
                cfg.zorder_by
            );
        }
        if let Some(s) = &state {
            let mut g = s.lock().await;
            g.create_table.status = format!("OPTIMIZE ZORDER BY ({})...", cfg.zorder_by);
            g.create_table.optimize_status = "running".to_string();
        }
        tracing::info!("[create-table] OPTIMIZE ZORDER BY ({})", cfg.zorder_by);
        let z0 = Instant::now();
        let table_url = parse_table_uri(&cfg.uri)?;
        let table = open_table_with_storage_options(table_url, storage_options.clone())
            .await
            .with_context(|| format!("re-open target for optimize {}", cfg.uri))?;
        let (_table, metrics) = DeltaOps(table)
            .optimize()
            .with_type(OptimizeType::ZOrder(vec![cfg.zorder_by.clone()]))
            .await
            .context("run OPTIMIZE ZORDER BY")?;
        tracing::info!(
            "[create-table] optimize done in {}ms: files_added={} files_removed={} partitions_optimized={}",
            z0.elapsed().as_millis(),
            metrics.num_files_added,
            metrics.num_files_removed,
            metrics.partitions_optimized
        );
        if let Some(s) = &state {
            let mut g = s.lock().await;
            g.create_table.optimize_status = format!(
                "done in {}ms (files_added={}, files_removed={})",
                z0.elapsed().as_millis(),
                metrics.num_files_added,
                metrics.num_files_removed
            );
        }
    }

    if let Some(s) = &state {
        let mut g = s.lock().await;
        g.create_table.status = format!("Done — {} rows in {}ms", total_rows_written, total_ms);
        g.create_table.running = false;
    }

    Ok(())
}
