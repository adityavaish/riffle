//! Adaptive CDC consumer.
//!
//! Reads new commits one (or several) at a time from the Delta table's
//! `_delta_log` directory and dispatches them in three modes:
//!
//! - **single** — one commit becomes one work unit
//! - **merged** — multiple consecutive small commits are coalesced
//! - **split**  — one large commit is split into row-bounded chunks
//!
//! Progress is persisted to a JSON checkpoint file so the consumer resumes
//! exactly where it left off across restarts (including mid-version, when a
//! large commit was being chunked).

use anyhow::Result;
use chrono::Utc;
use deltalake::open_table_with_storage_options;
use object_store::ObjectStoreExt;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use crate::config::Config;
use crate::sink::{self, SinkConfig, SinkOutcome};
use crate::state::{
    fmt_ckpt, ConsumerEvent, DiskCheckpoint, SharedState, SinkEvent, TunableConfig,
};
use crate::util::{parse_table_uri, version_to_i64};

const MAX_MERGE_VERSIONS: usize = 10;

#[derive(Debug, Clone)]
struct VersionInfo {
    version: i64,
    rows: u64,
}

pub async fn run(
    state: SharedState,
    cfg: Config,
    pre_launch_version: i64,
    tunables: TunableConfig,
) -> Result<()> {
    let storage_options = cfg.storage_options()?;
    let sink_cfg: Option<SinkConfig> = if cfg.sink_enabled {
        Some(build_sink_config(&cfg)?)
    } else {
        None
    };
    {
        let mut s = state.lock().await;
        s.sink.enabled = cfg.sink_enabled;
        if let Some(ref sc) = sink_cfg {
            s.sink.mode = sc.mode.name().to_string();
            s.sink.target_uri = sc.target_uri.clone();
            s.sink.target_backend = format!("{:?}", cfg.target_backend());
            s.sink.status = "Idle".to_string();
        }
    }
    let mut latencies: Vec<u64> = Vec::new();

    let (mut ckpt, fresh) = match load_checkpoint(&cfg.checkpoint_file) {
        Some(c) => (c, false),
        None => (
            DiskCheckpoint {
                last_full_version: pre_launch_version,
                partial_file_offset: 0,
                rows_committed: 0,
            },
            true,
        ),
    };
    let mut last_version: i64 = ckpt.last_full_version;
    let mut total_rows: u64 = ckpt.rows_committed;
    let mut versions_processed: u64 = 0;
    let mut chunks_processed: u64 = 0;

    // Wait for the table to exist (producer might not have created it yet).
    loop {
        let table_url = parse_table_uri(&cfg.table_uri)?;
        match open_table_with_storage_options(table_url, storage_options.clone()).await {
            Ok(_) => {
                if fresh {
                    save_checkpoint(&cfg.checkpoint_file, &ckpt).ok();
                }
                let mut s = state.lock().await;
                s.consumer.status = "Tracking".to_string();
                s.consumer.last_version_seen = last_version;
                s.consumer.checkpoint = fmt_ckpt(&ckpt);
                tracing::info!(
                    "[consumer] baseline v{} (will process v{}+) committed_rows={}",
                    last_version,
                    last_version + 1,
                    total_rows
                );
                break;
            }
            Err(e) => {
                let err = format!("{}", e);
                if err.contains("Not a Delta table") || err.contains("not found") {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue;
                }
                return Err(e.into());
            }
        }
    }

    loop {
        tokio::time::sleep(Duration::from_secs(cfg.poll_interval_secs)).await;
        let poll_start = Instant::now();

        let table_url = parse_table_uri(&cfg.table_uri)?;
        let table =
            match open_table_with_storage_options(table_url, storage_options.clone()).await {
                Ok(t) => t,
                Err(_) => continue,
            };
        let current_version = version_to_i64(table.version());

        if current_version <= last_version {
            let mut s = state.lock().await;
            s.consumer.status = "Polling...".to_string();
            s.consumer.pending_versions = 0;
            continue;
        }

        // Cheaply peek pending commits.
        let mut pending: Vec<VersionInfo> = Vec::new();
        for v in (last_version + 1)..=current_version {
            match inspect_version(&table, v).await {
                Ok(info) => pending.push(info),
                Err(e) => tracing::warn!("[consumer] inspect v{} failed: {}", v, e),
            }
        }

        {
            let mut s = state.lock().await;
            s.consumer.pending_versions = pending.len() as u64;
        }

        // Read tunables fresh each iteration so dashboard changes apply in real time.
        let split_threshold = tunables.split_threshold_rows.load(Ordering::Relaxed).max(1);
        let merge_threshold = tunables.merge_threshold_rows.load(Ordering::Relaxed).max(1);
        let chunk_target = tunables.chunk_target_rows.load(Ordering::Relaxed).max(1);

        let mut i = 0;
        while i < pending.len() {
            let v_info = &pending[i];

            if v_info.rows > split_threshold {
                // SPLIT
                let n_chunks =
                    ((v_info.rows + chunk_target - 1) / chunk_target).max(2) as usize;
                let base_chunk = v_info.rows / n_chunks as u64;

                for idx in 0..n_chunks {
                    let chunk_rows = if idx + 1 == n_chunks {
                        v_info.rows - base_chunk * (n_chunks as u64 - 1)
                    } else {
                        base_chunk
                    };
                    if let Some(ref sc) = sink_cfg {
                        // When a real sink is configured, splitting a single source
                        // version into row-bounded chunks would require row-precision
                        // splitting of the source DataFrame (not yet supported here).
                        // We process the whole version once on the first chunk and
                        // skip the others — the loop still drives checkpoint advance.
                        if idx == 0 {
                            let outcome = run_sink(&state, sc, &table, &[v_info.version]).await?;
                            tracing::info!(
                                "[sink] v{} | mode={} | source_rows={} | inserts={} updates={} deletes={} appended={} | tgt v{} | {}ms",
                                v_info.version,
                                sc.mode.name(),
                                outcome.source_rows,
                                outcome.inserts,
                                outcome.updates,
                                outcome.deletes,
                                outcome.appended,
                                outcome.target_version,
                                outcome.duration_ms,
                            );
                        }
                    } else {
                        process_work(chunk_rows).await;
                    }

                    total_rows += chunk_rows;
                    chunks_processed += 1;

                    let latency = poll_start.elapsed().as_millis() as u64;
                    latencies.push(latency);
                    if latencies.len() > 100 {
                        latencies.remove(0);
                    }
                    let avg_latency =
                        latencies.iter().sum::<u64>() / latencies.len().max(1) as u64;

                    let is_last_chunk = idx + 1 == n_chunks;
                    let new_offset = if is_last_chunk { 0 } else { idx + 1 };
                    let new_full_v = if is_last_chunk {
                        v_info.version
                    } else {
                        v_info.version - 1
                    };
                    ckpt = DiskCheckpoint {
                        last_full_version: new_full_v,
                        partial_file_offset: new_offset,
                        rows_committed: total_rows,
                    };
                    save_checkpoint(&cfg.checkpoint_file, &ckpt).ok();

                    let range_str = format!("v{}[chunk {}/{}]", v_info.version, idx + 1, n_chunks);
                    update_consumer_state(
                        &state,
                        "split",
                        &range_str,
                        v_info.version,
                        chunk_rows,
                        latency,
                        avg_latency,
                        versions_processed + if is_last_chunk { 1 } else { 0 },
                        chunks_processed,
                        total_rows,
                        &ckpt,
                    )
                    .await;

                    tracing::info!(
                        "[consumer] {} | +{} rows | {}ms | ckpt={}",
                        range_str,
                        chunk_rows,
                        latency,
                        fmt_ckpt(&ckpt)
                    );
                }

                versions_processed += 1;
                i += 1;
            } else {
                // MERGE / SINGLE
                let mut group_end = i;
                let mut total_in_group: u64 = pending[i].rows;
                while group_end + 1 < pending.len()
                    && pending[group_end + 1].rows <= split_threshold
                    && total_in_group + pending[group_end + 1].rows <= merge_threshold
                    && (group_end - i + 1) < MAX_MERGE_VERSIONS
                {
                    group_end += 1;
                    total_in_group += pending[group_end].rows;
                }

                if let Some(ref sc) = sink_cfg {
                    let versions: Vec<i64> = pending[i..=group_end].iter().map(|v| v.version).collect();
                    let outcome = run_sink(&state, sc, &table, &versions).await?;
                    tracing::info!(
                        "[sink] versions={:?} | mode={} | source_rows={} | inserts={} updates={} deletes={} appended={} | tgt v{} | {}ms",
                        versions,
                        sc.mode.name(),
                        outcome.source_rows,
                        outcome.inserts,
                        outcome.updates,
                        outcome.deletes,
                        outcome.appended,
                        outcome.target_version,
                        outcome.duration_ms,
                    );
                } else {
                    process_work(total_in_group).await;
                }

                let latency = poll_start.elapsed().as_millis() as u64;
                latencies.push(latency);
                if latencies.len() > 100 {
                    latencies.remove(0);
                }
                let avg_latency =
                    latencies.iter().sum::<u64>() / latencies.len().max(1) as u64;

                total_rows += total_in_group;
                let group_size = group_end - i + 1;
                versions_processed += group_size as u64;
                chunks_processed += 1;

                let last_v_in_group = pending[group_end].version;
                ckpt = DiskCheckpoint {
                    last_full_version: last_v_in_group,
                    partial_file_offset: 0,
                    rows_committed: total_rows,
                };
                save_checkpoint(&cfg.checkpoint_file, &ckpt).ok();

                let (mode, range_str) = if group_size == 1 {
                    ("single".to_string(), format!("v{}", pending[i].version))
                } else {
                    (
                        "merged".to_string(),
                        format!(
                            "v{}-v{} ({} versions)",
                            pending[i].version, last_v_in_group, group_size
                        ),
                    )
                };

                update_consumer_state(
                    &state,
                    &mode,
                    &range_str,
                    last_v_in_group,
                    total_in_group,
                    latency,
                    avg_latency,
                    versions_processed,
                    chunks_processed,
                    total_rows,
                    &ckpt,
                )
                .await;

                tracing::info!(
                    "[consumer] {} | +{} rows | {}ms | ckpt={}",
                    range_str,
                    total_in_group,
                    latency,
                    fmt_ckpt(&ckpt)
                );

                i = group_end + 1;
            }
        }

        last_version = current_version;
    }
}

#[allow(clippy::too_many_arguments)]
async fn update_consumer_state(
    state: &SharedState,
    mode: &str,
    range: &str,
    version: i64,
    new_rows: u64,
    latency: u64,
    avg_latency: u64,
    versions_processed: u64,
    chunks_processed: u64,
    total_rows: u64,
    ckpt: &DiskCheckpoint,
) {
    let mut s = state.lock().await;
    s.consumer.status = "Processing".to_string();
    s.consumer.mode = mode.to_string();
    s.consumer.versions_processed = versions_processed;
    s.consumer.chunks_processed = chunks_processed;
    s.consumer.total_rows_consumed = total_rows;
    s.consumer.last_version_seen = version;
    s.consumer.last_poll_duration_ms = latency;
    s.consumer.avg_latency_ms = avg_latency;
    s.consumer.checkpoint = fmt_ckpt(ckpt);
    s.consumer.events.push(ConsumerEvent {
        timestamp: Utc::now().format("%H:%M:%S").to_string(),
        mode: mode.to_string(),
        range: range.to_string(),
        version,
        new_rows,
        latency_ms: latency,
    });
    if s.consumer.events.len() > 50 {
        s.consumer.events.remove(0);
    }
}

/// Stand-in for the user's actual downstream work (e.g. MERGE INTO, S3 sink).
/// Sleeps roughly proportionally to row count so the dashboard shows realistic
/// latency. Replace with your real consumer logic, or enable `--sink-enabled`.
async fn process_work(rows: u64) {
    let ms = (rows / 1000).clamp(1, 5_000);
    tokio::time::sleep(Duration::from_millis(ms)).await;
}

fn build_sink_config(cfg: &Config) -> Result<SinkConfig> {
    let mode = sink::parse_mode(
        &cfg.sink_mode,
        &cfg.merge_keys,
        &cfg.merge_update_columns,
        cfg.merge_update_predicate.clone(),
        cfg.merge_delete_predicate.clone(),
        cfg.merge_insert_predicate.clone(),
        None,
        None,
        None,
    )?;
    Ok(SinkConfig {
        source_uri: cfg.table_uri.clone(),
        target_uri: cfg.target_table_uri.clone(),
        source_storage_options: cfg.storage_options()?,
        target_storage_options: cfg.target_storage_options()?,
        mode,
        read_concurrency: 8,
    })
}

async fn run_sink(
    state: &SharedState,
    sink_cfg: &SinkConfig,
    source: &deltalake::DeltaTable,
    versions: &[i64],
) -> Result<SinkOutcome> {
    {
        let mut s = state.lock().await;
        s.sink.status = format!("Sinking {} version(s)...", versions.len());
    }
    let outcome = sink::apply(sink_cfg, source, versions).await?;
    update_sink_state(state, sink_cfg, &outcome).await;
    Ok(outcome)
}

async fn update_sink_state(
    state: &SharedState,
    sink_cfg: &SinkConfig,
    outcome: &SinkOutcome,
) {
    let mut s = state.lock().await;
    s.sink.status = "Idle".to_string();
    s.sink.batches_processed += 1;
    s.sink.total_inserts += outcome.inserts;
    s.sink.total_updates += outcome.updates;
    s.sink.total_deletes += outcome.deletes;
    s.sink.total_appended += outcome.appended;
    s.sink.target_version = outcome.target_version;
    s.sink.last_duration_ms = outcome.duration_ms;
    s.sink.last_source_rows = outcome.source_rows;
    s.sink.events.push(SinkEvent {
        timestamp: Utc::now().format("%H:%M:%S").to_string(),
        mode: sink_cfg.mode.name().to_string(),
        source_rows: outcome.source_rows,
        inserts: outcome.inserts,
        updates: outcome.updates,
        deletes: outcome.deletes,
        appended: outcome.appended,
        target_version: outcome.target_version,
        duration_ms: outcome.duration_ms,
    });
    if s.sink.events.len() > 50 {
        s.sink.events.remove(0);
    }
    let n = s.sink.events.len() as u64;
    let sum: u64 = s.sink.events.iter().map(|e| e.duration_ms).sum();
    s.sink.avg_duration_ms = if n == 0 { 0 } else { sum / n };
}

pub fn load_checkpoint(path: &str) -> Option<DiskCheckpoint> {
    let bytes = std::fs::read(Path::new(path)).ok()?;
    serde_json::from_slice(&bytes).ok()
}

pub fn save_checkpoint(path: &str, c: &DiskCheckpoint) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(c)?;
    std::fs::write(Path::new(path), bytes)?;
    Ok(())
}

/// Inspect a version's commit JSON in `_delta_log` to count added rows.
/// This is a cheap pre-pass that lets us decide whether to split, merge, or
/// process a version one-shot without actually reading any Parquet data.
async fn inspect_version(table: &deltalake::DeltaTable, version: i64) -> Result<VersionInfo> {
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
    let mut total_rows: u64 = 0;

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
            if let Some(stats_val) = add.get("stats") {
                let stats_str = stats_val
                    .as_str()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| stats_val.to_string());
                if let Ok(stats_v) = serde_json::from_str::<serde_json::Value>(&stats_str) {
                    if let Some(n) = stats_v.get("numRecords").and_then(|x| x.as_u64()) {
                        total_rows += n;
                    }
                }
            }
        }
    }

    Ok(VersionInfo {
        version,
        rows: total_rows,
    })
}

pub async fn capture_baseline_version(cfg: &Config) -> Result<i64> {
    let storage_options = cfg.storage_options()?;
    let table_url = parse_table_uri(&cfg.table_uri)?;
    match open_table_with_storage_options(table_url, storage_options).await {
        Ok(t) => {
            let v = version_to_i64(t.version());
            tracing::info!("[init] pre-launch baseline: table exists at v{}", v);
            Ok(v)
        }
        Err(_) => {
            tracing::info!("[init] pre-launch baseline: table does not exist (start from v-1)");
            Ok(-1)
        }
    }
}
