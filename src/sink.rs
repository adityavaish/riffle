//! Generic Delta-to-Delta sink.
//!
//! Reads change rows from a source Delta table (via Delta Change Data Feed)
//! and transfers them into a target Delta table using one of three semantics:
//!
//! - **Append**    — `DeltaOps(target).write(...).with_save_mode(Append)` of post-images.
//! - **Overwrite** — same with `SaveMode::Overwrite` (full replace per batch).
//! - **Merge**     — `DeltaOps(target).merge(...)` keyed by user-specified columns,
//!                   with `_change_type='delete'` rows automatically routed to
//!                   `WHEN MATCHED THEN DELETE`. User predicates for matched-update /
//!                   matched-delete / not-matched-insert are still honoured.
//!
//! Riffle requires the source table to have `delta.enableChangeDataFeed=true`.
//! `update_preimage` rows are dropped on read (they describe pre-update state and
//! are never useful for a downstream materialized view). `_change_type`,
//! `_commit_version`, and `_commit_timestamp` columns are exposed to user-supplied
//! transforms via the `__src` view so SQL like `WHERE _change_type='insert'` works.
//!
//! Reading source rows uses the source table's already-authenticated `ObjectStore`,
//! so cloud credentials configured for the source apply transparently.

use anyhow::{anyhow, Result};
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef as ArrowSchemaRef;
use deltalake::datafusion::common::DataFusionError;
use deltalake::datafusion::datasource::TableProvider;
use deltalake::datafusion::execution::context::SessionContext;
use deltalake::datafusion::logical_expr::col;
use deltalake::delta_datafusion::cdf::scan::DeltaCdfTableProvider;
use deltalake::operations::create::CreateBuilder;
use deltalake::operations::merge::MergeMetrics;
use deltalake::protocol::SaveMode;
use deltalake::table::config::TablePropertiesExt;
use deltalake::{open_table_with_storage_options, DeltaOps, DeltaTable};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use crate::util::{parse_table_uri, version_to_i64};

/// CDF metadata columns appended by `scan_cdf`. These are stripped from the
/// data we write/merge into the target, but `_change_type` is kept available
/// in the `__src` view so user transforms can reference it.
const CDF_CHANGE_TYPE_COL: &str = "_change_type";
const CDF_COMMIT_VERSION_COL: &str = "_commit_version";
const CDF_COMMIT_TIMESTAMP_COL: &str = "_commit_timestamp";

fn is_cdf_metadata_col(name: &str) -> bool {
    matches!(
        name,
        CDF_CHANGE_TYPE_COL | CDF_COMMIT_VERSION_COL | CDF_COMMIT_TIMESTAMP_COL
    )
}

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
    /// Optional user-provided SQL applied to the source dataframe before MERGE.
    /// The dedup'd source is registered as `__src`; the SQL must reference it.
    /// Example: `SELECT id, UPPER(name) AS name, amount * 1.1 AS amount FROM __src WHERE status <> 'deleted'`.
    pub transform_sql: Option<String>,
    /// Optional compiled-and-loaded Rust transform applied to the dedup'd
    /// source RecordBatches before MERGE. Mutually exclusive with `transform_sql`.
    pub transform_rust: Option<Arc<crate::transform_rust::TransformLib>>,
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
// Read change rows from source via Delta CDF
// ---------------------------------------------------------------------------

/// Read change rows from `[start_version..=end_version]` of `source` via Delta
/// Change Data Feed.
///
/// - Pre-flight requires `delta.enableChangeDataFeed=true` on the source.
/// - `update_preimage` rows are dropped (they describe pre-update state).
/// - The returned schema includes all source data columns plus the CDF columns
///   `_change_type`, `_commit_version`, `_commit_timestamp` (in that order at
///   the end). Downstream callers strip them before writing as needed.
pub async fn read_cdf_rows(
    source: &DeltaTable,
    start_version: i64,
    end_version: i64,
    _read_concurrency: usize,
) -> Result<(Vec<RecordBatch>, ArrowSchemaRef, u64)> {
    let snap = source
        .snapshot()
        .map_err(|e| anyhow!("read source snapshot: {}", e))?;
    if !snap.table_config().enable_change_data_feed() {
        return Err(anyhow!(
            "source table does not have delta.enableChangeDataFeed=true. \
             Riffle reads exclusively via Delta Change Data Feed. \
             Recreate the source table with CDF enabled, or run `ALTER TABLE ... \
             SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')`. Note that \
             ALTER only enables CDF for FUTURE commits — existing versions will not \
             be replayable as a change feed."
        ));
    }

    if end_version < start_version {
        return Err(anyhow!(
            "read_cdf_rows: end_version v{} < start_version v{}",
            end_version,
            start_version
        ));
    }

    let t0 = Instant::now();
    let builder = source
        .clone()
        .scan_cdf()
        .with_starting_version(start_version as u64)
        .with_ending_version(end_version as u64);
    let provider = DeltaCdfTableProvider::try_new(builder)
        .map_err(|e| anyhow!("build CDF provider: {}", e))?;
    let provider_schema = provider.schema();
    let ctx = SessionContext::new();
    let df = ctx
        .read_table(Arc::new(provider))
        .map_err(|e: DataFusionError| anyhow!("read CDF table: {}", e))?;
    let raw_batches = df
        .collect()
        .await
        .map_err(|e: DataFusionError| anyhow!("collect CDF batches: {}", e))?;
    let raw_rows: u64 = raw_batches.iter().map(|b| b.num_rows() as u64).sum();
    tracing::info!(
        "[sink] read CDF v{}..=v{}: {} batch(es), {} row(s) (incl. preimages) in {}ms",
        start_version,
        end_version,
        raw_batches.len(),
        raw_rows,
        t0.elapsed().as_millis()
    );

    if raw_batches.is_empty() {
        return Ok((Vec::new(), provider_schema, 0));
    }

    // Drop update_preimage rows. They describe values BEFORE an update and are
    // never useful for a downstream materialized state — keeping them would
    // either double-count or fight the post-image in the merge.
    let ctx2 = SessionContext::new();
    let df = ctx2
        .read_batches(raw_batches)
        .map_err(|e: DataFusionError| anyhow!("read_batches for preimage filter: {}", e))?;
    ctx2.register_table("__cdf_raw", df.into_view())
        .map_err(|e: DataFusionError| anyhow!("register __cdf_raw: {}", e))?;
    let df = ctx2
        .sql(
            "SELECT * FROM __cdf_raw WHERE \"_change_type\" IS NULL OR \"_change_type\" <> 'update_preimage'",
        )
        .await
        .map_err(|e: DataFusionError| anyhow!("preimage filter sql: {}", e))?;
    let batches = df
        .collect()
        .await
        .map_err(|e: DataFusionError| anyhow!("collect after preimage filter: {}", e))?;
    let post_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
    tracing::info!(
        "[sink] after dropping update_preimage: {} row(s) ({} dropped)",
        post_rows,
        raw_rows.saturating_sub(post_rows)
    );

    let schema = batches
        .first()
        .map(|b| b.schema())
        .unwrap_or(provider_schema);
    Ok((batches, schema, post_rows))
}

/// Drop CDF metadata columns (`_change_type`, `_commit_version`,
/// `_commit_timestamp`) from a dataframe via SQL projection. Used for
/// append/overwrite paths where the target schema is data-only.
async fn drop_cdf_cols(
    ctx: &SessionContext,
    df: deltalake::datafusion::dataframe::DataFrame,
) -> Result<deltalake::datafusion::dataframe::DataFrame> {
    let arrow_schema = df.schema().as_arrow().clone();
    let cols: Vec<String> = arrow_schema
        .fields()
        .iter()
        .filter(|f| !is_cdf_metadata_col(f.name()))
        .map(|f| format!("\"{}\"", f.name()))
        .collect();
    let view = "__riffle_cdf_strip";
    let _ = ctx.deregister_table(view);
    ctx.register_table(view, df.into_view())
        .map_err(|e: DataFusionError| anyhow!("register {}: {}", view, e))?;
    let sql = format!("SELECT {} FROM {}", cols.join(", "), view);
    let df = ctx
        .sql(&sql)
        .await
        .map_err(|e: DataFusionError| anyhow!("strip CDF cols sql failed: {}", e))?;
    Ok(df)
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
            tracing::info!("[sink] target table not found at {}; creating with CDF enabled", target_uri);
            let struct_fields: Vec<deltalake::kernel::StructField> = source_schema
                .fields()
                .iter()
                .filter(|f| !is_cdf_metadata_col(f.name()))
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
                .with_configuration([(
                    "delta.enableChangeDataFeed".to_string(),
                    Some("true".to_string()),
                )])
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
/// source via Delta Change Data Feed (`delta.enableChangeDataFeed=true` required).
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

    if versions.is_empty() {
        return Ok(SinkOutcome {
            duration_ms: t0.elapsed().as_millis() as u64,
            ..Default::default()
        });
    }
    let v_start = *versions.first().unwrap();
    let v_end = *versions.last().unwrap();

    let concurrency = if cfg.read_concurrency == 0 { 8 } else { cfg.read_concurrency };
    let (batches, schema, source_rows) =
        read_cdf_rows(source, v_start, v_end, concurrency).await?;
    if batches.is_empty() {
        tracing::info!("[sink] no change rows to apply (empty CDF for v{}..=v{})", v_start, v_end);
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

    let outcome = apply_with_batches(cfg, batches, schema, source_rows).await?;

    Ok(SinkOutcome {
        duration_ms: t0.elapsed().as_millis() as u64,
        ..outcome
    })
}

/// Apply pre-collected RecordBatches to the configured sink mode.
///
/// Used by both:
///  * the Delta-CDF source (`apply()`) after reading change rows;
///  * the Event Hub source path (which already has a RecordBatch stream).
///
/// Append/overwrite paths drop CDF metadata columns when present (no-op for
/// non-CDF sources). Merge auto-routes `_change_type='delete'` rows when the
/// column is present, and otherwise treats every row as an upsert.
pub async fn apply_with_batches(
    cfg: &SinkConfig,
    batches: Vec<RecordBatch>,
    schema: ArrowSchemaRef,
    source_rows: u64,
) -> Result<SinkOutcome> {
    let t_write = Instant::now();
    let t_open = Instant::now();
    let outcome = match &cfg.mode {
        SinkMode::Append => {
            let target = open_or_create_target(&cfg.target_uri, &cfg.target_storage_options, &schema).await?;
            tracing::debug!("[sink] target opened (v{}) in {}ms", version_to_i64(target.version()), t_open.elapsed().as_millis());
            apply_append(target, batches, source_rows).await?
        }
        SinkMode::Overwrite => {
            let target = open_or_create_target(&cfg.target_uri, &cfg.target_storage_options, &schema).await?;
            tracing::debug!("[sink] target opened (v{}) in {}ms", version_to_i64(target.version()), t_open.elapsed().as_millis());
            apply_overwrite(target, batches, source_rows).await?
        }
        SinkMode::Merge(spec) => {
            // For merge, defer target open until after the transform so a fresh
            // target is created with the post-transform schema.
            apply_merge(
                &cfg.target_uri,
                &cfg.target_storage_options,
                batches,
                &schema,
                source_rows,
                spec,
            )
            .await?
        }
    };
    tracing::debug!(
        "[sink] {} write complete in {}ms (target_version=v{})",
        cfg.mode.name(),
        t_write.elapsed().as_millis(),
        outcome.target_version
    );
    Ok(outcome)
}

async fn apply_append(
    target: DeltaTable,
    batches: Vec<RecordBatch>,
    source_rows: u64,
) -> Result<SinkOutcome> {
    // Strip CDF metadata cols + drop delete tombstones so append only writes
    // post-image data rows.
    let ctx = SessionContext::new();
    let df = ctx
        .read_batches(batches)
        .map_err(|e: DataFusionError| anyhow!("read_batches: {}", e))?;
    ctx.register_table("__cdf", df.into_view())
        .map_err(|e: DataFusionError| anyhow!("register __cdf: {}", e))?;
    let df = ctx
        .sql(
            "SELECT * FROM __cdf WHERE \"_change_type\" IS NULL OR \"_change_type\" <> 'delete'",
        )
        .await
        .map_err(|e: DataFusionError| anyhow!("filter deletes for append: {}", e))?;
    let df = drop_cdf_cols(&ctx, df).await?;
    let out_batches = df
        .collect()
        .await
        .map_err(|e: DataFusionError| anyhow!("collect for append: {}", e))?;
    let appended: u64 = out_batches.iter().map(|b| b.num_rows() as u64).sum();
    let merged = DeltaOps(target)
        .write(out_batches)
        .with_save_mode(SaveMode::Append)
        .await?;
    Ok(SinkOutcome {
        source_rows,
        appended,
        target_version: version_to_i64(merged.version()),
        ..Default::default()
    })
}

async fn apply_overwrite(
    target: DeltaTable,
    batches: Vec<RecordBatch>,
    source_rows: u64,
) -> Result<SinkOutcome> {
    let ctx = SessionContext::new();
    let df = ctx
        .read_batches(batches)
        .map_err(|e: DataFusionError| anyhow!("read_batches: {}", e))?;
    ctx.register_table("__cdf", df.into_view())
        .map_err(|e: DataFusionError| anyhow!("register __cdf: {}", e))?;
    let df = ctx
        .sql(
            "SELECT * FROM __cdf WHERE \"_change_type\" IS NULL OR \"_change_type\" <> 'delete'",
        )
        .await
        .map_err(|e: DataFusionError| anyhow!("filter deletes for overwrite: {}", e))?;
    let df = drop_cdf_cols(&ctx, df).await?;
    let out_batches = df
        .collect()
        .await
        .map_err(|e: DataFusionError| anyhow!("collect for overwrite: {}", e))?;
    let appended: u64 = out_batches.iter().map(|b| b.num_rows() as u64).sum();
    let merged = DeltaOps(target)
        .write(out_batches)
        .with_save_mode(SaveMode::Overwrite)
        .await?;
    Ok(SinkOutcome {
        source_rows,
        appended,
        target_version: version_to_i64(merged.version()),
        ..Default::default()
    })
}

async fn apply_merge(
    target_uri: &str,
    target_storage_options: &HashMap<String, String>,
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
    // source rows" error. Use ROW_NUMBER() OVER (PARTITION BY keys ORDER BY <order_col DESC>)
    // and keep rn = 1. With CDF, the natural order is `_commit_version DESC` so the most
    // recent change per key wins (e.g. an `insert` followed by `delete` becomes just `delete`).
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
            _ => "\"_commit_version\" DESC".to_string(),
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

    // Optional user Rust transform: collect dedup'd source -> apply -> rebuild df.
    let df = if let Some(rust_lib) = spec.transform_rust.as_ref() {
        tracing::debug!("[sink] applying user-supplied Rust transform");
        let in_batches = df
            .collect()
            .await
            .map_err(|e: DataFusionError| anyhow!("collect for rust transform: {}", e))?;
        let mut out_batches = Vec::with_capacity(in_batches.len());
        for b in &in_batches {
            let nb = rust_lib.apply(b).map_err(|e| anyhow!("rust transform apply: {:#}", e))?;
            out_batches.push(nb);
        }
        ctx.read_batches(out_batches)
            .map_err(|e: DataFusionError| anyhow!("read_batches after rust transform failed: {}", e))?
    } else {
        df
    };

    // Optional user SQL transform: run it against the dedup'd source as `__src`.
    // The view exposes the CDF columns (`_change_type`, `_commit_version`,
    // `_commit_timestamp`) so users can filter on them, e.g.
    //   SELECT id, name FROM __src WHERE _change_type IN ('insert','update_postimage')
    let df = if let Some(user_sql) = spec.transform_sql.as_deref().filter(|s| !s.trim().is_empty()) {
        let view = "__src";
        let _ = ctx.deregister_table(view);
        ctx.register_table(view, df.into_view())
            .map_err(|e: DataFusionError| anyhow!("register __src failed: {}", e))?;
        tracing::debug!("[sink] applying transform_sql: {}", user_sql.trim());
        ctx.sql(user_sql)
            .await
            .map_err(|e: DataFusionError| anyhow!("transform_sql failed: {}", e))?
    } else {
        df
    };

    // Determine which columns are updated/inserted. Use the (possibly-transformed)
    // dataframe's schema so transforms that rename/drop columns are honoured.
    // CDF metadata columns are excluded from the data we project into the target
    // (target only stores user data); `_change_type` is kept available for clause
    // predicates but never written.
    let key_set: std::collections::HashSet<&str> = spec.keys.iter().map(|s| s.as_str()).collect();
    let df_arrow_schema: ArrowSchemaRef = Arc::new(df.schema().as_arrow().clone());
    let all_cols: Vec<String> = df_arrow_schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let has_change_type = all_cols.iter().any(|c| c == CDF_CHANGE_TYPE_COL);
    let data_cols: Vec<String> = all_cols
        .iter()
        .filter(|c| !is_cdf_metadata_col(c))
        .cloned()
        .collect();
    // Sanity-check: every merge key must still be present after the transform.
    for k in &spec.keys {
        if !data_cols.iter().any(|c| c == k) {
            return Err(anyhow!(
                "merge key '{}' missing from source after transform; columns: [{}]",
                k,
                data_cols.join(", ")
            ));
        }
    }
    let update_cols: Vec<String> = if spec.update_columns.is_empty() {
        data_cols.iter().filter(|c| !key_set.contains(c.as_str())).cloned().collect()
    } else {
        spec.update_columns.clone()
    };

    // Target-facing schema: drop CDF metadata cols. This is what the target will
    // be created with (if it doesn't exist) and what additive evolution diffs against.
    let target_schema: ArrowSchemaRef = {
        let fields: Vec<arrow::datatypes::FieldRef> = df_arrow_schema
            .fields()
            .iter()
            .filter(|f| !is_cdf_metadata_col(f.name()))
            .cloned()
            .collect();
        Arc::new(arrow::datatypes::Schema::new(fields))
    };

    // Join predicate: target."k1" = source."k1" AND ...
    // Quoted to preserve case-sensitive column names (e.g. "BalanceId").
    let predicate_str = spec
        .keys
        .iter()
        .map(|k| format!("target.\"{0}\" = source.\"{0}\"", k))
        .collect::<Vec<_>>()
        .join(" AND ");

    // Open / create target using the data-only schema (no CDF metadata cols).
    let t_open = Instant::now();
    let mut target = open_or_create_target(target_uri, target_storage_options, &target_schema).await?;
    tracing::debug!(
        "[sink] target opened (v{}) in {}ms",
        version_to_i64(target.version()),
        t_open.elapsed().as_millis()
    );

    // Additive schema evolution: if the post-transform dataframe has data
    // columns the target doesn't, ALTER TABLE ADD COLUMN before merging.
    // CDF metadata columns are NEVER added to the target.
    {
        use deltalake::kernel::engine::arrow_conversion::TryIntoKernel;
        let target_existing = target
            .snapshot()
            .map_err(|e| anyhow!("read target snapshot: {}", e))?
            .schema();
        let target_field_names: std::collections::HashSet<String> = target_existing
            .fields()
            .map(|f| f.name().to_string())
            .collect();
        let mut new_fields: Vec<deltalake::kernel::StructField> = Vec::new();
        for f in target_schema.fields() {
            if !target_field_names.contains(f.name()) {
                let dt: deltalake::kernel::DataType =
                    (&deltalake::arrow::datatypes::DataType::from(f.data_type().clone()))
                        .try_into_kernel()
                        .map_err(|e| {
                            anyhow!(
                                "additive evolution: cannot convert type for new column {}: {}",
                                f.name(),
                                e
                            )
                        })?;
                new_fields.push(deltalake::kernel::StructField::new(
                    f.name().clone(),
                    dt,
                    true,
                ));
            }
        }
        if !new_fields.is_empty() {
            let added: Vec<String> = new_fields.iter().map(|f| f.name().clone()).collect();
            tracing::info!(
                "[sink] additive schema evolution: adding column(s) [{}] to target before merge",
                added.join(", ")
            );
            target = DeltaOps(target)
                .add_columns()
                .with_fields(new_fields)
                .await
                .map_err(|e| anyhow!("ALTER TABLE ADD COLUMN failed: {}", e))?;
        }
    }

    let mut merge_op = DeltaOps(target)
        .merge(df, predicate_str.clone())
        .with_source_alias("source")
        .with_target_alias("target")
        .with_safe_cast(true);
    tracing::debug!(
        "[sink] merge predicate: {} | update_cols=[{}] | has_change_type={}",
        predicate_str,
        update_cols.join(", "),
        has_change_type
    );

    // CDF auto-routing: if `_change_type` survived the transform, route delete
    // change rows to MATCHED-DELETE first (delta-rs evaluates matched clauses
    // in order; first match wins). Update/insert clauses then get gated against
    // non-delete change types so they don't fire for delete tombstones.
    if has_change_type {
        merge_op = merge_op.when_matched_delete(|d| {
            d.predicate("source.\"_change_type\" = 'delete'")
        })?;
    }

    // User-supplied WHEN MATCHED DELETE — added after the auto-route.
    if let Some(del_pred) = &spec.when_matched_delete_predicate {
        let pred = del_pred.clone();
        merge_op = merge_op.when_matched_delete(|d| d.predicate(pred))?;
    }

    // WHEN MATCHED UPDATE
    {
        let cols = update_cols.clone();
        let upd_pred_user = spec.when_matched_update_predicate.clone();
        merge_op = merge_op.when_matched_update(|mut u| {
            // Combine user predicate (if any) with the CDF non-delete gate.
            let pred = match (upd_pred_user, has_change_type) {
                (Some(p), true) => Some(format!(
                    "({}) AND (source.\"_change_type\" IS NULL OR source.\"_change_type\" <> 'delete')",
                    p
                )),
                (Some(p), false) => Some(p),
                (None, true) => Some(
                    "source.\"_change_type\" IS NULL OR source.\"_change_type\" <> 'delete'"
                        .to_string(),
                ),
                (None, false) => None,
            };
            if let Some(p) = pred {
                u = u.predicate(p);
            }
            for c in &cols {
                u = u.update(c.clone(), col(format!("source.\"{}\"", c)));
            }
            u
        })?;
    }

    // WHEN NOT MATCHED INSERT — never insert delete tombstones.
    {
        let cols = data_cols.clone();
        let ins_pred_user = spec.when_not_matched_insert_predicate.clone();
        merge_op = merge_op.when_not_matched_insert(|mut i| {
            let pred = match (ins_pred_user, has_change_type) {
                (Some(p), true) => Some(format!(
                    "({}) AND (source.\"_change_type\" IS NULL OR source.\"_change_type\" <> 'delete')",
                    p
                )),
                (Some(p), false) => Some(p),
                (None, true) => Some(
                    "source.\"_change_type\" IS NULL OR source.\"_change_type\" <> 'delete'"
                        .to_string(),
                ),
                (None, false) => None,
            };
            if let Some(p) = pred {
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
    transform_sql: Option<String>,
    transform_rust: Option<Arc<crate::transform_rust::TransformLib>>,
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
            let transform_sql = transform_sql.filter(|s| !s.trim().is_empty());
            if transform_sql.is_some() && transform_rust.is_some() {
                return Err(anyhow!(
                    "--merge-transform-sql and --merge-transform-rust-file are mutually exclusive"
                ));
            }
            Ok(SinkMode::Merge(MergeSpec {
                keys,
                update_columns,
                when_matched_update_predicate,
                when_matched_delete_predicate,
                when_not_matched_insert_predicate,
                dedupe_order_by: dedupe_order_by.filter(|s| !s.is_empty()),
                transform_sql,
                transform_rust,
            }))
        }
        other => Err(anyhow!(
            "Unknown --sink-mode '{}'. Use append | overwrite | merge.",
            other
        )),
    }
}
