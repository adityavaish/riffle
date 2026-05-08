//! Lightweight Delta-table inspection used by the dashboard "Tables" tab.
//!
//! This is read-only: opens the table, returns version, schema, file count,
//! row-count estimate (sum of stats), and a slice of recent commits.

use anyhow::{Context, Result};
use deltalake::open_table_with_storage_options;
use serde::Serialize;

use crate::config::{build_storage_options, register_handlers_for, Backend};
use crate::util::{parse_table_uri, version_to_i64};

#[derive(Serialize)]
pub struct InspectResult {
    pub uri: String,
    pub backend: String,
    pub version: i64,
    pub num_files: usize,
    pub estimated_rows: u64,
    pub schema: Vec<ColumnInfo>,
    pub partition_columns: Vec<String>,
    pub commits: Vec<CommitInfo>,
}

#[derive(Serialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

#[derive(Serialize)]
pub struct CommitInfo {
    pub version: i64,
    pub timestamp: String,
    pub operation: String,
}

pub async fn inspect(uri: &str, azure_auth: &str) -> Result<InspectResult> {
    let backend = Backend::detect(uri);
    register_handlers_for(&[backend]);
    std::env::set_var("AZURE_AUTH", azure_auth);
    let storage_options = build_storage_options(uri, azure_auth)
        .with_context(|| format!("storage opts for {}", uri))?;

    let table_url = parse_table_uri(uri)?;
    let table = open_table_with_storage_options(table_url, storage_options.clone())
        .await
        .with_context(|| format!("open table {}", uri))?;
    let version = version_to_i64(table.version());

    let snapshot = table.snapshot().context("table snapshot")?;

    // Schema
    let schema = snapshot.schema();
    let cols: Vec<ColumnInfo> = schema
        .fields()
        .map(|f| ColumnInfo {
            name: f.name().to_string(),
            data_type: format!("{:?}", f.data_type()),
            nullable: f.is_nullable(),
        })
        .collect();

    let partition_columns: Vec<String> = snapshot.metadata().partition_columns().to_vec();

    // File-level stats: count files + sum num_records for an estimate.
    // delta-rs 0.32 dropped `file_actions_iter`; instead we project the
    // flattened add-actions table and read the per-file `num_records` column.
    let mut num_files = 0usize;
    let mut estimated_rows = 0u64;
    if let Ok(state) = table.snapshot() {
        if let Ok(batch) = state.add_actions_table(true) {
            use deltalake::arrow::array::Array;
            num_files = batch.num_rows();
            if let Some(arr) = batch.column_by_name("num_records") {
                if let Some(int_arr) = arr.as_any().downcast_ref::<deltalake::arrow::array::Int64Array>() {
                    for i in 0..int_arr.len() {
                        if !int_arr.is_null(i) {
                            estimated_rows += int_arr.value(i).max(0) as u64;
                        }
                    }
                }
            }
        }
    }

    // Recent commits via history (limit ~10). History returns newest-first;
    // the version field isn't carried so we derive it from `current_version`.
    let mut commits: Vec<CommitInfo> = Vec::new();
    if let Ok(history) = table.history(Some(10)).await {
        for (i, h) in history.take(10).enumerate() {
            commits.push(CommitInfo {
                version: version - i as i64,
                timestamp: h
                    .timestamp
                    .map(|t| {
                        chrono::DateTime::<chrono::Utc>::from_timestamp_millis(t)
                            .map(|d| d.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                            .unwrap_or_else(|| t.to_string())
                    })
                    .unwrap_or_default(),
                operation: h.operation.clone().unwrap_or_default(),
            });
        }
    }

    Ok(InspectResult {
        uri: uri.to_string(),
        backend: format!("{:?}", backend),
        version,
        num_files,
        estimated_rows,
        schema: cols,
        partition_columns,
        commits,
    })
}
