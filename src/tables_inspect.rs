//! Lightweight Delta-table inspection used by the dashboard "Tables" tab.
//!
//! This is read-only: opens the table, returns version, schema, file count,
//! row-count estimate (sum of stats), a slice of recent commits, and on
//! request a preview of the first N rows.

use anyhow::{anyhow, Context, Result};
use deltalake::datafusion::arrow::record_batch::RecordBatch;
use deltalake::datafusion::prelude::SessionContext;
use deltalake::delta_datafusion::{DeltaScanConfigBuilder, DeltaTableProvider};
use deltalake::{open_table_with_storage_options, DeltaOps};
use futures::StreamExt;
use serde::Serialize;
use std::sync::Arc;

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

#[derive(Serialize)]
pub struct PreviewResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
    pub returned: usize,
    pub limit: usize,
    pub truncated: bool,
}

/// Read up to `limit` rows from the table, optionally filtered by a SQL WHERE clause.
/// `where_clause` is the body of the WHERE (e.g. `bool_03 = true AND int64_01 > 0`).
pub async fn preview(
    uri: &str,
    azure_auth: &str,
    limit: usize,
    where_clause: Option<&str>,
) -> Result<PreviewResult> {
    let backend = Backend::detect(uri);
    register_handlers_for(&[backend]);
    std::env::set_var("AZURE_AUTH", azure_auth);
    let storage_options = build_storage_options(uri, azure_auth)
        .with_context(|| format!("storage opts for {}", uri))?;

    let table_url = parse_table_uri(uri)?;
    let table = open_table_with_storage_options(table_url, storage_options.clone())
        .await
        .with_context(|| format!("open table {}", uri))?;

    let where_trim = where_clause
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| {
            // Be lenient: accept either "col = 1" or "WHERE col = 1".
            if s.len() >= 5 && s[..5].eq_ignore_ascii_case("WHERE") {
                s[5..].trim_start()
            } else {
                s
            }
        })
        .filter(|s| !s.is_empty());
    let mut batches: Vec<RecordBatch> = Vec::new();

    if let Some(wc) = where_trim {
        // Use SessionContext + SQL so we can filter (DataFusion pushes predicates into the scan).
        let snapshot = table
            .snapshot()
            .map_err(|e| anyhow!("snapshot: {}", e))?
            .snapshot()
            .clone();
        let log_store = table.log_store();
        let config = DeltaScanConfigBuilder::new()
            .build(&snapshot)
            .map_err(|e| anyhow!("scan config: {}", e))?;
        let provider = DeltaTableProvider::try_new(snapshot, log_store, config)
            .map_err(|e| anyhow!("provider: {}", e))?;
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(provider))
            .map_err(|e| anyhow!("register: {}", e))?;
        let sql = format!("SELECT * FROM t WHERE {} LIMIT {}", wc, limit);
        let df = ctx.sql(&sql).await.map_err(|e| anyhow!("sql: {}", e))?;
        batches = df.collect().await.map_err(|e| anyhow!("collect: {}", e))?;
    } else {
        // Fast path: stream the table and stop once we hit `limit` rows.
        let (_t, mut stream) = DeltaOps(table)
            .load()
            .await
            .map_err(|e| anyhow!("load: {}", e))?;
        let mut fetched = 0usize;
        while fetched < limit {
            match stream.next().await {
                Some(Ok(b)) => {
                    fetched += b.num_rows();
                    batches.push(b);
                }
                Some(Err(e)) => return Err(anyhow!("stream: {}", e)),
                None => break,
            }
        }
    }

    use deltalake::arrow::util::display::{ArrayFormatter, FormatOptions};
    let opts = FormatOptions::default().with_null("NULL");
    let mut columns: Vec<String> = Vec::new();
    let mut rows: Vec<Vec<String>> = Vec::new();
    let mut total = 0usize;
    for batch in &batches {
        if columns.is_empty() {
            columns = batch.schema().fields().iter().map(|f| f.name().clone()).collect();
        }
        let formatters: Vec<ArrayFormatter> = batch
            .columns()
            .iter()
            .map(|c| ArrayFormatter::try_new(c.as_ref(), &opts))
            .collect::<Result<_, _>>()
            .map_err(|e| anyhow!("formatter: {}", e))?;
        for r in 0..batch.num_rows() {
            if total >= limit {
                break;
            }
            let row: Vec<String> = formatters.iter().map(|f| f.value(r).to_string()).collect();
            rows.push(row);
            total += 1;
        }
        if total >= limit {
            break;
        }
    }
    Ok(PreviewResult {
        returned: rows.len(),
        truncated: rows.len() >= limit,
        columns,
        rows,
        limit,
    })
}
