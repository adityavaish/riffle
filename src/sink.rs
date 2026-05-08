//! Generic Delta-to-Delta sink.
//!
//! Reads newly-added rows from a source Delta table (by version) and transfers them
//! into a target Delta table using one of three semantics:
//!
//! - **Append**    — `DeltaOps(target).write(...).with_save_mode(Append)`
//! - **Overwrite** — same with `SaveMode::Overwrite` (full replace per batch).
//! - **Merge**     — `DeltaOps(target).merge(...)` keyed by user-specified columns,
//!                   with optional SQL predicates for matched-update / matched-delete /
//!                   not-matched-insert clauses. Predicate strings are parsed by
//!                   delta-rs against the merged `source ∪ target` schema.
//!
//! Reading source rows uses the source table's already-authenticated `ObjectStore`,
//! so cloud credentials configured for the source apply transparently.

use anyhow::{anyhow, Context, Result};
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef as ArrowSchemaRef;
use deltalake::datafusion::common::DataFusionError;
use deltalake::datafusion::execution::context::SessionContext;
use deltalake::datafusion::logical_expr::col;
use deltalake::operations::create::CreateBuilder;
use deltalake::operations::merge::MergeMetrics;
use deltalake::protocol::SaveMode;
use deltalake::{open_table_with_storage_options, DeltaOps, DeltaTable, ObjectStore};
use futures::stream::{StreamExt, TryStreamExt};
use object_store::path::Path as ObjectStorePath;
use object_store::ObjectStoreExt;
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use crate::util::{parse_table_uri, version_to_i64};

#[derive(Debug, Clone)]
pub enum SinkMode {
    Append,
    Overwrite,
    Merge(MergeSpec),
}

impl SinkMode {
    pub fn name(&self) -> &'static str {
        match self {
            SinkMode::Append => "append",
            SinkMode::Overwrite => "overwrite",
            SinkMode::Merge(_) => "merge",
        }
    }
}

/// Configuration for a `MERGE` operation. Predicate fields are SQL strings parsed
/// against the merged `source ∪ target` schema (via delta-rs's
/// `Expression::String`).
#[derive(Debug, Clone, Default)]
pub struct MergeSpec {
    /// Equi-join columns; the join predicate becomes
    /// `target.k1 = source.k1 AND target.k2 = source.k2 AND ...`.
    pub keys: Vec<String>,
    /// Columns updated by `WHEN MATCHED UPDATE`. If empty, all non-key source columns are updated.
    pub update_columns: Vec<String>,
    /// Optional extra predicate gating `WHEN MATCHED UPDATE`.
    pub when_matched_update_predicate: Option<String>,
    /// Optional `WHEN MATCHED DELETE` predicate. If set, this delete clause is added
    /// BEFORE the update clause (delta-rs evaluates match clauses in order).
    pub when_matched_delete_predicate: Option<String>,
    /// Optional predicate gating `WHEN NOT MATCHED INSERT`.
    pub when_not_matched_insert_predicate: Option<String>,
    /// Optional column to order by when deduplicating source rows that share a merge key
    /// (descending — most recent kept). If empty, source dedup keeps an arbitrary row per key.
    pub dedupe_order_by: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SinkConfig {
    pub source_uri: String,
    pub target_uri: String,
    pub source_storage_options: HashMap<String, String>,
    pub target_storage_options: HashMap<String, String>,
    pub mode: SinkMode,
    /// Number of source parquet files to read in parallel. Defaults to 8 if 0.
    pub read_concurrency: usize,
}

#[derive(Debug, Default, Clone)]
pub struct SinkOutcome {
    pub source_rows: u64,
    pub inserts: u64,
    pub updates: u64,
    pub deletes: u64,
    pub appended: u64,
    pub target_version: i64,
    pub duration_ms: u64,
}

// ---------------------------------------------------------------------------
// Read newly-added rows from source commits
// ---------------------------------------------------------------------------

async fn read_added_paths(table: &DeltaTable, version: i64) -> Result<Vec<String>> {
    use deltalake::logstore::commit_uri_from_version;

    let log_store = table.log_store();
    let commit_path = commit_uri_from_version(Some(version as u64));
    let bytes = log_store
        .object_store(None)
        .get(&commit_path)
        .await?
        .bytes()
        .await?;
    let s = std::str::from_utf8(&bytes)?;

    let mut out = Vec::new();
    for line in s.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let action: serde_json::Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if let Some(add) = action.get("add") {
            if let Some(path) = add.get("path").and_then(|p| p.as_str()) {
                out.push(path.to_string());
            }
        }
    }
    Ok(out)
}

async fn read_parquet_file(
    object_store: Arc<dyn ObjectStore>,
    rel_path: &str,
) -> Result<Vec<RecordBatch>> {
    let path = ObjectStorePath::from_url_path(rel_path)
        .or_else(|_| Ok::<_, object_store::path::Error>(ObjectStorePath::from(rel_path)))?;
    let meta = object_store
        .head(&path)
        .await
        .with_context(|| format!("HEAD failed for {}", rel_path))?;

    let reader = ParquetObjectReader::new(object_store, path).with_file_size(meta.size);
    let stream = ParquetRecordBatchStreamBuilder::new(reader)
        .await?
        .build()?;
    let batches = stream.try_collect::<Vec<_>>().await?;
    Ok(batches)
}

/// Read every parquet file added by the given source versions.
///
/// `read_concurrency` controls how many parquet files are fetched in parallel
/// (clamped to `[1, all_paths.len()]`).
pub async fn read_added_rows(
    source: &DeltaTable,
    versions: &[i64],
    read_concurrency: usize,
) -> Result<(Vec<RecordBatch>, ArrowSchemaRef, u64)> {
    let object_store = source.log_store().object_store(None);
    let mut all_paths: Vec<String> = Vec::new();
    let t_paths = Instant::now();
    for v in versions {
        let mut p = read_added_paths(source, *v).await?;
        tracing::debug!("[sink] v{} contributes {} added file(s)", v, p.len());
        all_paths.append(&mut p);
    }
    let total_files = all_paths.len();
    tracing::info!(
        "[sink] discovered {} added file(s) across {} version(s) in {}ms",
        total_files,
        versions.len(),
        t_paths.elapsed().as_millis()
    );

    let concurrency = read_concurrency.max(1).min(total_files.max(1));
    tracing::info!(
        "[sink] reading {} file(s) with concurrency={}",
        total_files,
        concurrency
    );

    // Fan out file reads in parallel while preserving completion ordering for logs.
    let store = object_store.clone();
    let read_results: Vec<(usize, String, Vec<RecordBatch>, u64, u128)> =
        futures::stream::iter(all_paths.into_iter().enumerate())
            .map(|(idx, path)| {
                let store = store.clone();
                async move {
                    let t_file = Instant::now();
                    let batches = read_parquet_file(store, &path).await?;
                    let rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
                    Ok::<_, anyhow::Error>((idx, path, batches, rows, t_file.elapsed().as_millis()))
                }
            })
            .buffer_unordered(concurrency)
            .try_collect()
            .await?;

    let mut all_batches: Vec<RecordBatch> = Vec::new();
    let mut schema: Option<ArrowSchemaRef> = None;
    let mut total_rows: u64 = 0;
    for (idx, path, batches, rows, ms) in read_results {
        for b in batches {
            if schema.is_none() {
                schema = Some(b.schema());
            }
            all_batches.push(b);
        }
        total_rows += rows;
        tracing::debug!(
            "[sink] read file {}/{} ({}) -> {} rows in {}ms",
            idx + 1,
            total_files,
            path,
            rows,
            ms
        );
    }
    tracing::info!(
        "[sink] read total {} row(s) from {} file(s)",
        total_rows,
        total_files
    );

    let schema = schema.ok_or_else(|| anyhow!("no rows added in versions {:?}", versions))?;
    Ok((all_batches, schema, total_rows))
}

// ---------------------------------------------------------------------------
// Open / create target
// ---------------------------------------------------------------------------

async fn open_or_create_target(
    target_uri: &str,
    storage_opts: &HashMap<String, String>,
    source_schema: &ArrowSchemaRef,
) -> Result<DeltaTable> {
    let target_url = parse_table_uri(target_uri)?;
    match open_table_with_storage_options(target_url, storage_opts.clone()).await {
        Ok(t) => Ok(t),
        Err(_) => {
            tracing::info!("[sink] target table not found at {}; creating", target_uri);
            let struct_fields: Vec<deltalake::kernel::StructField> = source_schema
                .fields()
                .iter()
                .map(|f| {
                    use deltalake::kernel::engine::arrow_conversion::TryIntoKernel;
                    let dt: deltalake::kernel::DataType =
                        (&deltalake::arrow::datatypes::DataType::from(f.data_type().clone()))
                            .try_into_kernel()
                            .map_err(|e| {
                                anyhow!("schema convert failed for {}: {}", f.name(), e)
                            })?;
                    Ok::<_, anyhow::Error>(deltalake::kernel::StructField::new(
                        f.name().clone(),
                        dt,
                        f.is_nullable(),
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            let table = CreateBuilder::new()
                .with_location(target_uri)
                .with_storage_options(storage_opts.clone())
                .with_columns(struct_fields)
                .with_save_mode(SaveMode::ErrorIfExists)
                .with_table_name("riffle_sink_target")
                .await?;
            Ok(table)
        }
    }
}

// ---------------------------------------------------------------------------
// Apply
// ---------------------------------------------------------------------------

/// Run the configured sink mode for a group of source versions.
///
/// `source` should already be opened and reflect the latest visible state of the
/// source table (caller is expected to refresh/poll). Versions are read from the
/// source via its ObjectStore.
pub async fn apply(
    cfg: &SinkConfig,
    source: &DeltaTable,
    versions: &[i64],
) -> Result<SinkOutcome> {
    let t0 = Instant::now();
    tracing::debug!(
        "[sink] apply start: mode={} versions={:?} target={}",
        cfg.mode.name(),
        versions,
        cfg.target_uri
    );

    let concurrency = if cfg.read_concurrency == 0 { 8 } else { cfg.read_concurrency };
    let (batches, schema, source_rows) = read_added_rows(source, versions, concurrency).await?;
    if batches.is_empty() {
        tracing::info!("[sink] no batches to apply (empty source contributions)");
        return Ok(SinkOutcome {
            duration_ms: t0.elapsed().as_millis() as u64,
            ..Default::default()
        });
    }
    tracing::debug!(
        "[sink] read complete: {} batches, {} rows; opening target {}",
        batches.len(),
        source_rows,
        cfg.target_uri
    );

    let t_open = Instant::now();
    let target =
        open_or_create_target(&cfg.target_uri, &cfg.target_storage_options, &schema).await?;
    tracing::debug!(
        "[sink] target opened (v{}) in {}ms",
        version_to_i64(target.version()),
        t_open.elapsed().as_millis()
    );

    let t_write = Instant::now();
    let outcome = match &cfg.mode {
        SinkMode::Append => apply_append(target, batches, source_rows).await?,
        SinkMode::Overwrite => apply_overwrite(target, batches, source_rows).await?,
        SinkMode::Merge(spec) => apply_merge(target, batches, &schema, source_rows, spec).await?,
    };
    tracing::debug!(
        "[sink] {} write complete in {}ms (target_version=v{})",
        cfg.mode.name(),
        t_write.elapsed().as_millis(),
        outcome.target_version
    );

    Ok(SinkOutcome {
        duration_ms: t0.elapsed().as_millis() as u64,
        ..outcome
    })
}

async fn apply_append(
    target: DeltaTable,
    batches: Vec<RecordBatch>,
    source_rows: u64,
) -> Result<SinkOutcome> {
    let merged = DeltaOps(target)
        .write(batches)
        .with_save_mode(SaveMode::Append)
        .await?;
    Ok(SinkOutcome {
        source_rows,
        appended: source_rows,
        target_version: version_to_i64(merged.version()),
        ..Default::default()
    })
}

async fn apply_overwrite(
    target: DeltaTable,
    batches: Vec<RecordBatch>,
    source_rows: u64,
) -> Result<SinkOutcome> {
    let merged = DeltaOps(target)
        .write(batches)
        .with_save_mode(SaveMode::Overwrite)
        .await?;
    Ok(SinkOutcome {
        source_rows,
        appended: source_rows,
        target_version: version_to_i64(merged.version()),
        ..Default::default()
    })
}

async fn apply_merge(
    target: DeltaTable,
    batches: Vec<RecordBatch>,
    schema: &ArrowSchemaRef,
    source_rows: u64,
    spec: &MergeSpec,
) -> Result<SinkOutcome> {
    if spec.keys.is_empty() {
        return Err(anyhow!("merge mode requires at least one key column"));
    }

    let ctx = SessionContext::new();
    let df = ctx
        .read_batches(batches)
        .map_err(|e: DataFusionError| anyhow!("read_batches failed: {}", e))?;

    // Source-side dedup on merge keys to avoid the "MERGE matched a target row with multiple
    // source rows" error. Use ROW_NUMBER() OVER (PARTITION BY keys ORDER BY <order_col DESC | NULL>)
    // and keep rn = 1.
    let df = {
        let view_name = "__riffle_merge_src";
        ctx.register_table(view_name, df.into_view())
            .map_err(|e: DataFusionError| anyhow!("register source view failed: {}", e))?;
        let part_by = spec
            .keys
            .iter()
            .map(|k| format!("\"{}\"", k))
            .collect::<Vec<_>>()
            .join(", ");
        let order_by = match &spec.dedupe_order_by {
            Some(c) if !c.is_empty() => format!("\"{}\" DESC", c),
            _ => format!("\"{}\"", spec.keys[0]),
        };
        let cols = schema
            .fields()
            .iter()
            .map(|f| format!("\"{}\"", f.name()))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT {cols} FROM (SELECT {cols}, ROW_NUMBER() OVER (PARTITION BY {part_by} ORDER BY {order_by}) AS __rn FROM {view_name}) WHERE __rn = 1"
        );
        tracing::debug!("[sink] merge source dedup sql: {}", sql);
        ctx.sql(&sql)
            .await
            .map_err(|e: DataFusionError| anyhow!("dedup sql failed: {}", e))?
    };

    // Determine which columns are updated by WHEN MATCHED UPDATE.
    let key_set: std::collections::HashSet<&str> = spec.keys.iter().map(|s| s.as_str()).collect();
    let all_cols: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    let update_cols: Vec<String> = if spec.update_columns.is_empty() {
        all_cols.iter().filter(|c| !key_set.contains(c.as_str())).cloned().collect()
    } else {
        spec.update_columns.clone()
    };

    // Join predicate: target."k1" = source."k1" AND ...
    // Quoted to preserve case-sensitive column names (e.g. "BalanceId").
    let predicate_str = spec
        .keys
        .iter()
        .map(|k| format!("target.\"{0}\" = source.\"{0}\"", k))
        .collect::<Vec<_>>()
        .join(" AND ");

    let mut merge_op = DeltaOps(target)
        .merge(df, predicate_str.clone())
        .with_source_alias("source")
        .with_target_alias("target")
        .with_safe_cast(true);
    tracing::debug!(
        "[sink] merge predicate: {} | update_cols=[{}]",
        predicate_str,
        update_cols.join(", ")
    );

    // WHEN MATCHED DELETE — added before update so it takes precedence per delta-rs ordering.
    if let Some(del_pred) = &spec.when_matched_delete_predicate {
        let pred = del_pred.clone();
        merge_op = merge_op.when_matched_delete(|d| d.predicate(pred))?;
    }

    // WHEN MATCHED UPDATE
    {
        let cols = update_cols.clone();
        let upd_pred = spec.when_matched_update_predicate.clone();
        merge_op = merge_op.when_matched_update(|mut u| {
            if let Some(p) = upd_pred {
                u = u.predicate(p);
            }
            for c in &cols {
                u = u.update(c.clone(), col(format!("source.\"{}\"", c)));
            }
            u
        })?;
    }

    // WHEN NOT MATCHED INSERT
    {
        let cols = all_cols.clone();
        let ins_pred = spec.when_not_matched_insert_predicate.clone();
        merge_op = merge_op.when_not_matched_insert(|mut i| {
            if let Some(p) = ins_pred {
                i = i.predicate(p);
            }
            for c in &cols {
                i = i.set(c.clone(), col(format!("source.\"{}\"", c)));
            }
            i
        })?;
    }

    let (merged_table, metrics): (DeltaTable, MergeMetrics) = merge_op.await?;
    Ok(SinkOutcome {
        source_rows,
        inserts: metrics.num_target_rows_inserted as u64,
        updates: metrics.num_target_rows_updated as u64,
        deletes: metrics.num_target_rows_deleted as u64,
        target_version: version_to_i64(merged_table.version()),
        ..Default::default()
    })
}

// ---------------------------------------------------------------------------
// Mode parsing helpers (used by both binaries)
// ---------------------------------------------------------------------------

pub fn parse_mode(
    mode: &str,
    keys: &str,
    update_columns: &str,
    when_matched_update_predicate: Option<String>,
    when_matched_delete_predicate: Option<String>,
    when_not_matched_insert_predicate: Option<String>,
    dedupe_order_by: Option<String>,
) -> Result<SinkMode> {
    match mode.to_ascii_lowercase().as_str() {
        "append" => Ok(SinkMode::Append),
        "overwrite" => Ok(SinkMode::Overwrite),
        "merge" => {
            let keys: Vec<String> = keys
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            if keys.is_empty() {
                return Err(anyhow!("--sink-mode merge requires --merge-keys"));
            }
            let update_columns: Vec<String> = update_columns
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            Ok(SinkMode::Merge(MergeSpec {
                keys,
                update_columns,
                when_matched_update_predicate,
                when_matched_delete_predicate,
                when_not_matched_insert_predicate,
                dedupe_order_by: dedupe_order_by.filter(|s| !s.is_empty()),
            }))
        }
        other => Err(anyhow!(
            "Unknown --sink-mode '{}'. Use append | overwrite | merge.",
            other
        )),
    }
}
