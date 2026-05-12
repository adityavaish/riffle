//! Delta table maintenance: compact (`OPTIMIZE`), z-order, and `VACUUM`.
//!
//! All three operations open the table with the user-supplied storage options
//! (Azure auth honored) and return a compact JSON-friendly result struct so
//! the dashboard can render before/after metrics.

use anyhow::{anyhow, Context, Result};
use deltalake::operations::optimize::OptimizeType;
use deltalake::{open_table_with_storage_options, DeltaOps};
use serde::Serialize;
use std::num::NonZeroU64;
use std::time::Instant;

use crate::config::{build_storage_options, register_handlers_for, Backend};
use crate::util::{parse_table_uri, version_to_i64};

#[derive(Serialize)]
pub struct OptimizeResult {
    pub uri: String,
    pub mode: String,                // "compact" | "zorder"
    pub starting_version: i64,
    pub new_version: i64,
    pub num_files_added: u64,
    pub num_files_removed: u64,
    pub partitions_optimized: u64,
    pub duration_ms: u64,
    /// Z-order columns, empty for compact mode.
    pub zorder_columns: Vec<String>,
    /// Target file size in bytes, when set by the caller.
    pub target_file_size: Option<u64>,
}

#[derive(Serialize)]
pub struct VacuumResult {
    pub uri: String,
    pub dry_run: bool,
    pub starting_version: i64,
    pub new_version: i64,
    pub retention_hours: u64,
    pub num_files_deleted: usize,
    /// Up to 100 file paths for display; full list is in logs.
    pub sample_deleted_paths: Vec<String>,
    pub duration_ms: u64,
}

/// Run `OPTIMIZE` on a table. If `zorder_columns` is non-empty, uses
/// `OptimizeType::ZOrder`; otherwise plain compaction.
pub async fn optimize(
    uri: &str,
    azure_auth: &str,
    zorder_columns: Vec<String>,
    target_file_size_bytes: Option<u64>,
    max_concurrent_tasks: Option<usize>,
) -> Result<OptimizeResult> {
    let backend = Backend::detect(uri);
    register_handlers_for(&[backend]);
    std::env::set_var("AZURE_AUTH", azure_auth);
    let storage_options = build_storage_options(uri, azure_auth)
        .with_context(|| format!("storage opts for {}", uri))?;

    let table_url = parse_table_uri(uri)?;
    let table = open_table_with_storage_options(table_url, storage_options.clone())
        .await
        .with_context(|| format!("open table {}", uri))?;
    let starting_version = version_to_i64(table.version());

    let optimize_type = if zorder_columns.is_empty() {
        OptimizeType::Compact
    } else {
        OptimizeType::ZOrder(zorder_columns.clone())
    };
    let mode = if zorder_columns.is_empty() { "compact" } else { "zorder" };
    tracing::info!(
        "[maintenance] OPTIMIZE start: uri={} mode={} cols={:?} target_size={:?}",
        uri,
        mode,
        zorder_columns,
        target_file_size_bytes
    );

    let mut builder = DeltaOps(table).optimize().with_type(optimize_type);
    if let Some(sz) = target_file_size_bytes {
        if let Some(nz) = NonZeroU64::new(sz) {
            builder = builder.with_target_size(nz);
        }
    }
    if let Some(n) = max_concurrent_tasks {
        if n > 0 {
            builder = builder.with_max_concurrent_tasks(n);
        }
    }
    let t0 = Instant::now();
    let (table_out, metrics) = builder
        .await
        .with_context(|| format!("OPTIMIZE failed on {}", uri))?;
    let dur_ms = t0.elapsed().as_millis() as u64;
    let new_version = version_to_i64(table_out.version());
    tracing::info!(
        "[maintenance] OPTIMIZE done: uri={} v{}->v{} added={} removed={} partitions={} in {}ms",
        uri,
        starting_version,
        new_version,
        metrics.num_files_added,
        metrics.num_files_removed,
        metrics.partitions_optimized,
        dur_ms
    );
    Ok(OptimizeResult {
        uri: uri.to_string(),
        mode: mode.to_string(),
        starting_version,
        new_version,
        num_files_added: metrics.num_files_added,
        num_files_removed: metrics.num_files_removed,
        partitions_optimized: metrics.partitions_optimized,
        duration_ms: dur_ms,
        zorder_columns,
        target_file_size: target_file_size_bytes,
    })
}

/// Run `VACUUM`.
///
/// `retention_hours` is the minimum age for a tombstoned file to be eligible
/// for deletion. The Delta protocol's default safe minimum is 168 hours
/// (7 days); to go below, callers must set `enforce_retention=false`.
///
/// `dry_run=true` returns the files that WOULD be deleted without actually
/// deleting anything (and without writing a commit).
pub async fn vacuum(
    uri: &str,
    azure_auth: &str,
    retention_hours: u64,
    dry_run: bool,
    enforce_retention: bool,
) -> Result<VacuumResult> {
    let backend = Backend::detect(uri);
    register_handlers_for(&[backend]);
    std::env::set_var("AZURE_AUTH", azure_auth);
    let storage_options = build_storage_options(uri, azure_auth)
        .with_context(|| format!("storage opts for {}", uri))?;

    let table_url = parse_table_uri(uri)?;
    let table = open_table_with_storage_options(table_url, storage_options.clone())
        .await
        .with_context(|| format!("open table {}", uri))?;
    let starting_version = version_to_i64(table.version());

    tracing::info!(
        "[maintenance] VACUUM start: uri={} retention={}h dry_run={} enforce={}",
        uri,
        retention_hours,
        dry_run,
        enforce_retention
    );

    let t0 = Instant::now();
    let (table_out, metrics) = DeltaOps(table)
        .vacuum()
        .with_retention_period(chrono::Duration::seconds(retention_hours.saturating_mul(3600) as i64))
        .with_dry_run(dry_run)
        .with_enforce_retention_duration(enforce_retention)
        .await
        .map_err(|e| anyhow!("VACUUM failed on {}: {}", uri, e))?;
    let dur_ms = t0.elapsed().as_millis() as u64;
    let new_version = version_to_i64(table_out.version());
    let num = metrics.files_deleted.len();
    let sample = metrics.files_deleted.iter().take(100).cloned().collect();
    tracing::info!(
        "[maintenance] VACUUM done: uri={} v{}->v{} dry_run={} files_deleted={} in {}ms",
        uri,
        starting_version,
        new_version,
        dry_run,
        num,
        dur_ms
    );
    Ok(VacuumResult {
        uri: uri.to_string(),
        dry_run,
        starting_version,
        new_version,
        retention_hours,
        num_files_deleted: num,
        sample_deleted_paths: sample,
        duration_ms: dur_ms,
    })
}
