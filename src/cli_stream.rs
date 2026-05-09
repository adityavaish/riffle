//! `riffle stream` subcommand — Delta-to-Delta streaming transformation with built-in web dashboard.
//!
//! Two modes of operation:
//!
//! 1. **CLI mode** — pass `--source-uri` and `--target-uri` (plus mode/keys)
//!    on the command line. The job starts immediately and the dashboard is
//!    available at the bind address for live progress.
//!
//! 2. **Dashboard mode** — invoke with no source/target. The dashboard opens
//!    with an input form. Submit the form to start a job; press Stop to
//!    cancel. Multiple jobs can be run sequentially without restarting.
//!
//! The dashboard always runs (default `127.0.0.1:3001`).

use anyhow::{Context, Result};
use chrono::Utc;
use deltalake::{open_table_with_storage_options, DeltaTable};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch, Mutex};
use tokio::task::JoinHandle;

use crate::config::{build_storage_options, register_handlers_for, Backend};
use crate::sink::{self, SinkConfig, SinkOutcome};
use crate::state::{
    CreateTableCommand, CreateTableLaunchConfig, DashboardState, LogBuffer, SharedState,
    SinkCommand, SinkEvent, SinkLaunchConfig, TunableConfig,
};
use crate::{cli_create, log_layer, web};
use crate::util::{parse_table_uri, version_to_i64};

#[derive(clap::Args, Debug, Clone)]
pub struct StreamArgs {
    /// Source Delta table URI. Optional — if omitted, configure via dashboard.
    #[arg(long)]
    source_uri: Option<String>,

    /// Target Delta table URI. Optional — if omitted, configure via dashboard.
    #[arg(long)]
    target_uri: Option<String>,

    /// Transfer mode: append | overwrite | merge.
    #[arg(long, default_value = "merge")]
    sink_mode: String,

    /// Comma-separated key columns for MERGE join.
    #[arg(long, default_value = "")]
    merge_keys: String,

    /// Comma-separated columns to UPDATE on match. Default = all non-key source columns.
    #[arg(long, default_value = "")]
    merge_update_columns: String,

    #[arg(long)]
    merge_update_predicate: Option<String>,

    #[arg(long)]
    merge_delete_predicate: Option<String>,

    #[arg(long)]
    merge_insert_predicate: Option<String>,

    /// Optional column to order by when deduping source rows that share a merge key (DESC, latest wins).
    #[arg(long)]
    merge_dedupe_order_by: Option<String>,

    /// Optional SQL applied to the source dataframe before MERGE. The dedup'd source is
    /// available as `__src`. Example:
    ///   --merge-transform-sql "SELECT id, UPPER(name) AS name, amount*1.1 AS amount FROM __src WHERE status <> 'deleted'"
    #[arg(long)]
    merge_transform_sql: Option<String>,

    /// Optional path to a file containing the body of a Rust function
    /// `fn transform(batch: RecordBatch) -> Result<RecordBatch, String>`.
    /// Compiled to a cdylib via cargo and dynamically loaded at stream start.
    /// Mutually exclusive with --merge-transform-sql.
    #[arg(long)]
    merge_transform_rust_file: Option<PathBuf>,

    #[arg(long)]
    start_version: Option<i64>,

    #[arg(long)]
    end_version: Option<i64>,

    /// Process one batch of new versions and exit (no polling).
    #[arg(long)]
    once: bool,

    #[arg(long, default_value_t = 5)]
    poll_interval_secs: u64,

    #[arg(long, default_value = "./riffle-stream-ckpt.json")]
    checkpoint_file: PathBuf,

    #[arg(long, default_value_t = 10)]
    max_versions_per_batch: usize,

    /// Number of source parquet files to read in parallel during apply.
    #[arg(long, default_value_t = 8)]
    read_concurrency: usize,

    /// Azure auth: auto | sp | msi | cli.
    #[arg(long, default_value = "auto")]
    azure_auth: String,

    /// Bind address for the web dashboard. Default: 127.0.0.1:3001.
    #[arg(long, default_value = "127.0.0.1:3001")]
    dashboard_bind: String,
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

/// In-process state shared between the web layer and the running sink job.
struct Controller {
    state: SharedState,
    handle: Option<JoinHandle<()>>,
    cancel: Option<Arc<AtomicBool>>,
    backends_registered: HashSet<Backend>,
    gate: ActiveJobGate,
}

impl Controller {
    fn new(state: SharedState, gate: ActiveJobGate) -> Self {
        Self {
            state,
            handle: None,
            cancel: None,
            backends_registered: HashSet::new(),
            gate,
        }
    }

    fn is_running(&self) -> bool {
        self.handle.as_ref().map(|h| !h.is_finished()).unwrap_or(false)
    }

    async fn start(&mut self, cfg: SinkLaunchConfig) -> Result<(), String> {
        if self.is_running() {
            return Err("a sink job is already running; stop it first".into());
        }
        if cfg.source_uri.is_empty() || cfg.target_uri.is_empty() {
            return Err("source_uri and target_uri are required".into());
        }
        self.gate.acquire("stream").await?;

        let source_backend = Backend::detect(&cfg.source_uri);
        let target_backend = Backend::detect(&cfg.target_uri);
        let mut to_register = vec![];
        if !self.backends_registered.contains(&source_backend) {
            to_register.push(source_backend);
        }
        if source_backend != target_backend && !self.backends_registered.contains(&target_backend) {
            to_register.push(target_backend);
        }
        if !to_register.is_empty() {
            register_handlers_for(&to_register);
            for b in &to_register {
                self.backends_registered.insert(*b);
            }
        }

        let source_storage = build_storage_options(&cfg.source_uri, &cfg.azure_auth)
            .map_err(|e| format!("source storage opts: {}", e))?;
        let target_storage = build_storage_options(&cfg.target_uri, &cfg.azure_auth)
            .map_err(|e| format!("target storage opts: {}", e))?;
        let transform_rust_lib = match cfg.merge_transform_rust.as_deref() {
            Some(src) if !src.trim().is_empty() => {
                tracing::info!("[stream] compiling user Rust transform (this may take a while on first run)...");
                Some(crate::transform_rust::compile_and_load(src)
                    .map_err(|e| format!("rust transform: {}", e))?)
            }
            _ => None,
        };
        let mode = sink::parse_mode(
            &cfg.sink_mode,
            &cfg.merge_keys,
            &cfg.merge_update_columns,
            cfg.merge_update_predicate.clone(),
            cfg.merge_delete_predicate.clone(),
            cfg.merge_insert_predicate.clone(),
            cfg.merge_dedupe_order_by.clone(),
            cfg.merge_transform_sql.clone(),
            transform_rust_lib,
        )
        .map_err(|e| format!("parse_mode: {}", e))?;

        let sink_cfg = SinkConfig {
            source_uri: cfg.source_uri.clone(),
            target_uri: cfg.target_uri.clone(),
            source_storage_options: source_storage.clone(),
            target_storage_options: target_storage,
            mode,
            read_concurrency: cfg.read_concurrency,
        };

        // Reset / seed dashboard state.
        {
            let mut s = self.state.lock().await;
            s.table_uri = cfg.source_uri.clone();
            s.backend = format!("{:?}", source_backend);
            s.sink.enabled = true;
            s.sink.running = true;
            s.sink.last_error = String::new();
            s.sink.mode = sink_cfg.mode.name().to_string();
            s.sink.target_uri = cfg.target_uri.clone();
            s.sink.target_backend = format!("{:?}", target_backend);
            s.sink.status = "Starting...".to_string();
            s.sink.launch_config = Some(cfg.clone());
        }

        let cancel = Arc::new(AtomicBool::new(false));
        let cancel_b = cancel.clone();
        let state_b = self.state.clone();
        let cfg_b = cfg.clone();
        let gate_b = self.gate.clone();

        let handle = tokio::spawn(async move {
            if let Err(e) = run_sink_loop(state_b.clone(), sink_cfg, cfg_b, source_storage, cancel_b).await {
                tracing::error!("[sink] job failed: {:#}", e);
                let mut s = state_b.lock().await;
                s.sink.last_error = format!("{:#}", e);
                s.sink.status = format!("Error: {}", e);
                s.sink.running = false;
            } else {
                let mut s = state_b.lock().await;
                s.sink.status = "Stopped".to_string();
                s.sink.running = false;
            }
            gate_b.release("stream").await;
        });

        self.handle = Some(handle);
        self.cancel = Some(cancel);
        Ok(())
    }

    fn stop(&mut self) -> Result<(), String> {
        if !self.is_running() {
            return Err("no sink job is currently running".into());
        }
        if let Some(c) = &self.cancel {
            c.store(true, Ordering::Relaxed);
        }
        // Mark as stopping immediately so the UI can react.
        let state = self.state.clone();
        let handle = self.handle.take();
        self.cancel = None;
        tokio::spawn(async move {
            {
                let mut s = state.lock().await;
                s.sink.status = "Stopping...".to_string();
            }
            if let Some(h) = handle {
                tracing::info!("[stream] stop requested; waiting up to 10s for cooperative cancel");
                let abort_handle = h.abort_handle();
                if tokio::time::timeout(Duration::from_secs(10), h).await.is_err() {
                    tracing::warn!("[stream] sink loop did not exit within 10s (likely mid-merge); aborting hard");
                    abort_handle.abort();
                    // Wait long enough for the orphaned heartbeat OS thread to
                    // see its cancellation flag and exit (5s sleep + slack).
                    tokio::time::sleep(Duration::from_secs(6)).await;
                } else {
                    tracing::info!("[stream] sink loop exited cleanly");
                }
            }
            let mut s = state.lock().await;
            s.sink.status = "Stopped".to_string();
            s.sink.running = false;
        });
        Ok(())
    }
}

async fn run_sink_loop(
    state: SharedState,
    sink_cfg: SinkConfig,
    launch: SinkLaunchConfig,
    source_storage_options: std::collections::HashMap<String, String>,
    cancel: Arc<AtomicBool>,
) -> Result<()> {
    let ckpt_path = PathBuf::from(&launch.checkpoint_file);
    let mut last_sunk: i64 = match launch.start_version {
        Some(v) => v - 1,
        None => load_checkpoint(&ckpt_path)
            .map(|c| c.last_sunk_version)
            .unwrap_or(-1),
    };
    tracing::info!(
        "[stream] starting loop: source={} target={} mode={} poll_interval={}s max_versions_per_batch={} resume_after_v{}",
        launch.source_uri,
        sink_cfg.target_uri,
        sink_cfg.mode.name(),
        launch.poll_interval_secs,
        launch.max_versions_per_batch,
        last_sunk
    );

    loop {
        if cancel.load(Ordering::Relaxed) {
            tracing::info!("[stream] cancellation requested, exiting loop");
            return Ok(());
        }

        tracing::debug!("[stream] opening source table to detect latest version: {}", launch.source_uri);
        let t_open = std::time::Instant::now();
        let source_url = parse_table_uri(&launch.source_uri)?;
        let source: DeltaTable =
            open_table_with_storage_options(source_url, source_storage_options.clone())
                .await
                .with_context(|| format!("open source {}", launch.source_uri))?;
        let current_version = version_to_i64(source.version());
        tracing::debug!(
            "[stream] source opened in {}ms; current_version=v{} last_sunk=v{}",
            t_open.elapsed().as_millis(),
            current_version,
            last_sunk
        );
        let cap = launch
            .end_version
            .unwrap_or(current_version)
            .min(current_version);

        if last_sunk >= cap {
            if launch.once {
                tracing::info!("[stream] up to date (v{}). Exiting (--once).", current_version);
                return Ok(());
            }
            tracing::debug!("[stream] caught up at v{}; sleeping {}s before next poll", current_version, launch.poll_interval_secs);
            {
                let mut s = state.lock().await;
                s.sink.status = format!("Idle (caught up at v{})", current_version);
            }
            // Sleep in small slices so cancellation is responsive.
            let mut elapsed = 0u64;
            while elapsed < launch.poll_interval_secs {
                if cancel.load(Ordering::Relaxed) {
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
                elapsed += 1;
            }
            continue;
        }

        let mut group_start = last_sunk + 1;
        while group_start <= cap {
            if cancel.load(Ordering::Relaxed) {
                return Ok(());
            }
            let group_end =
                (group_start + launch.max_versions_per_batch as i64 - 1).min(cap);
            let versions: Vec<i64> = (group_start..=group_end).collect();
            tracing::info!(
                "[stream] applying versions v{}..=v{} ({} versions, mode={})",
                group_start,
                group_end,
                versions.len(),
                sink_cfg.mode.name()
            );
            {
                let mut s = state.lock().await;
                s.sink.status = format!("Streaming v{}..=v{}", group_start, group_end);
            }
            let t_apply = std::time::Instant::now();
            // Heartbeat on a dedicated OS thread so it cannot be starved by
            // CPU-bound work that DataFusion does on tokio worker threads
            // during sink::apply (especially merge planning/execution).
            let hb_state = state.clone();
            let hb_label = format!("v{}..=v{}", group_start, group_end);
            let hb_mode = sink_cfg.mode.name().to_string();
            let hb_done = Arc::new(AtomicBool::new(false));
            let hb_done_b = hb_done.clone();
            let hb_thread = std::thread::Builder::new()
                .name(format!("riffle-hb-{}", group_start))
                .spawn(move || {
                    let start = std::time::Instant::now();
                    let mut tick: u64 = 0;
                    while !hb_done_b.load(Ordering::Relaxed) {
                        std::thread::sleep(Duration::from_secs(5));
                        if hb_done_b.load(Ordering::Relaxed) {
                            break;
                        }
                        tick += 1;
                        let secs = start.elapsed().as_secs();
                        tracing::info!(
                            "[stream] {} {} in progress... {}s elapsed (tick #{})",
                            hb_mode,
                            hb_label,
                            secs,
                            tick
                        );
                        // Update dashboard state via blocking_lock — this thread is
                        // not a tokio worker, so blocking is fine here.
                        let mut s = hb_state.blocking_lock();
                        s.sink.status = format!(
                            "{} {} in progress ({}s, tick #{})",
                            hb_mode, hb_label, secs, tick
                        );
                    }
                })
                .expect("spawn heartbeat thread");
            // RAII guard: if this scope unwinds (task aborted, panic, ?-bubble),
            // signal the heartbeat thread to exit. Otherwise the OS thread keeps
            // running indefinitely after a hard abort and the dashboard never
            // reflects "stopped".
            // Note: we deliberately do NOT join the OS thread here — Drop runs
            // on a tokio worker, and a blocking join could stall it. The thread
            // exits within 5s on its own once it sees the flag.
            struct HbGuard {
                done: Arc<AtomicBool>,
            }
            impl Drop for HbGuard {
                fn drop(&mut self) {
                    self.done.store(true, Ordering::Relaxed);
                }
            }
            let _hb_guard = HbGuard { done: hb_done.clone() };
            let outcome_res = sink::apply(&sink_cfg, &source, &versions).await;
            // Drop the guard explicitly so the heartbeat stops emitting before
            // we log the per-batch summary line below.
            drop(_hb_guard);
            // Best-effort wait for the heartbeat OS thread to exit so it can't
            // overwrite the "Stopped" status set by the controller.
            let _ = hb_thread.join();
            let outcome = outcome_res?;
            tracing::info!(
                "[stream] applied v{}..=v{} in {}ms | src_rows={} ins={} upd={} del={} app={} | tgt=v{}",
                group_start,
                group_end,
                t_apply.elapsed().as_millis(),
                outcome.source_rows,
                outcome.inserts,
                outcome.updates,
                outcome.deletes,
                outcome.appended,
                outcome.target_version
            );
            update_sink_state(&state, &sink_cfg, &outcome).await;
            last_sunk = group_end;
            save_checkpoint(
                &ckpt_path,
                &Checkpoint {
                    last_sunk_version: last_sunk,
                },
            )
            .ok();
            group_start = group_end + 1;
            if launch.once {
                return Ok(());
            }
        }

        if launch.once {
            return Ok(());
        }
    }
}

async fn update_sink_state(state: &SharedState, sink_cfg: &SinkConfig, outcome: &SinkOutcome) {
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

/// Single-slot "who's running right now" gate. Used to enforce that only one
/// of `stream`, `create-table`, or `demo` runs at a time.
#[derive(Clone, Default)]
struct ActiveJobGate {
    inner: Arc<Mutex<Option<&'static str>>>,
}

impl ActiveJobGate {
    async fn acquire(&self, kind: &'static str) -> Result<(), String> {
        let mut g = self.inner.lock().await;
        match *g {
            Some(k) if k == kind => Err(format!("a {} job is already running; stop it first", k)),
            Some(k) => Err(format!(
                "another job ({}) is already running; stop it before starting {}",
                k, kind
            )),
            None => {
                *g = Some(kind);
                Ok(())
            }
        }
    }

    async fn release(&self, kind: &'static str) {
        let mut g = self.inner.lock().await;
        if matches!(*g, Some(k) if k == kind) {
            *g = None;
        }
    }

    async fn current(&self) -> Option<&'static str> {
        *self.inner.lock().await
    }
}

/// Controller for the dashboard "Create Table" tab.
struct CreateTableController {
    state: SharedState,
    handle: Option<JoinHandle<()>>,
    cancel: Option<Arc<AtomicBool>>,
    gate: ActiveJobGate,
}

impl CreateTableController {
    fn new(state: SharedState, gate: ActiveJobGate) -> Self {
        Self {
            state,
            handle: None,
            cancel: None,
            gate,
        }
    }

    fn is_running(&self) -> bool {
        self.handle.as_ref().map(|h| !h.is_finished()).unwrap_or(false)
    }

    async fn start(&mut self, cfg: CreateTableLaunchConfig) -> Result<(), String> {
        if self.is_running() {
            return Err("a create-table job is already running".into());
        }
        if cfg.uri.is_empty() {
            return Err("uri is required".into());
        }
        self.gate.acquire("create-table").await?;
        let cancel = Arc::new(AtomicBool::new(false));
        let cancel_b = cancel.clone();
        let state_b = self.state.clone();
        let gate_b = self.gate.clone();
        let cfg_b = cfg.clone();

        // Reset state so old run's metrics don't bleed over.
        {
            let mut s = self.state.lock().await;
            s.create_table = Default::default();
            s.create_table.running = true;
            s.create_table.target_uri = cfg_b.uri.clone();
            s.create_table.status = "Starting...".to_string();
            s.create_table.total_commits = cfg_b.commits;
            s.create_table.total_rows = cfg_b.rows as u64;
            s.create_table.launch_config = Some(cfg_b.clone());
            s.active_job = Some("create-table".to_string());
        }

        let handle = tokio::spawn(async move {
            let result = cli_create::run_inner(cfg_b, Some(state_b.clone()), cancel_b).await;
            let mut s = state_b.lock().await;
            match result {
                Ok(()) => {}
                Err(e) => {
                    tracing::error!("[create-table] failed: {:#}", e);
                    s.create_table.last_error = format!("{:#}", e);
                    s.create_table.status = format!("Error: {}", e);
                }
            }
            s.create_table.running = false;
            s.active_job = None;
            drop(s);
            gate_b.release("create-table").await;
        });

        self.handle = Some(handle);
        self.cancel = Some(cancel);
        Ok(())
    }

    fn stop(&mut self) -> Result<(), String> {
        if !self.is_running() {
            return Err("no create-table job is currently running".into());
        }
        if let Some(c) = &self.cancel {
            c.store(true, Ordering::Relaxed);
        }
        let state = self.state.clone();
        let handle = self.handle.take();
        self.cancel = None;
        tokio::spawn(async move {
            {
                let mut s = state.lock().await;
                s.create_table.status = "Stopping...".to_string();
            }
            if let Some(h) = handle {
                let abort_handle = h.abort_handle();
                if tokio::time::timeout(Duration::from_secs(10), h).await.is_err() {
                    tracing::warn!("[create-table] not exited in 10s; aborting");
                    abort_handle.abort();
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
            let mut s = state.lock().await;
            s.create_table.status = "Stopped".to_string();
            s.create_table.running = false;
        });
        Ok(())
    }
}

pub async fn run(args: StreamArgs) -> Result<()> {
    println!("=== riffle stream ===");
    println!("Dashboard : http://{}", args.dashboard_bind);
    println!();

    // Shared dashboard state and per-job gate.
    let state: SharedState = Arc::new(Mutex::new(DashboardState::default()));
    {
        let mut s = state.lock().await;
        s.app_mode = "stream-cli".to_string();
        s.sink.enabled = true;
        s.sink.status = "Idle (no job running)".to_string();
        s.sink.running = false;
    }
    let tunables = TunableConfig::new(50_000, 30_000, 200_000);
    let gate = ActiveJobGate::default();

    let stream_controller = Arc::new(Mutex::new(Controller::new(state.clone(), gate.clone())));
    let create_controller =
        Arc::new(Mutex::new(CreateTableController::new(state.clone(), gate.clone())));

    // Log ring buffer + tracing layer.
    let log_buffer = LogBuffer::new(2000);
    {
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::util::SubscriberInitExt;
        use tracing_subscriber::EnvFilter;
        let env_filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new("info,riffle=info,object_store=warn,hyper=warn,reqwest=warn"));
        let layer = log_layer::DashboardLogLayer::new(log_buffer.clone());
        let _ = tracing_subscriber::registry()
            .with(env_filter)
            .with(tracing_subscriber::fmt::layer())
            .with(layer)
            .try_init();
    }

    // Stream command channel.
    let (sink_tx, mut sink_rx) = mpsc::channel::<SinkCommand>(8);
    {
        let controller_b = stream_controller.clone();
        tokio::spawn(async move {
            while let Some(cmd) = sink_rx.recv().await {
                match cmd {
                    SinkCommand::Start(cfg, reply) => {
                        let mut c = controller_b.lock().await;
                        let r = c.start(cfg).await;
                        let _ = reply.send(r);
                    }
                    SinkCommand::Stop(reply) => {
                        let mut c = controller_b.lock().await;
                        let r = c.stop();
                        let _ = reply.send(r);
                    }
                }
            }
        });
    }

    // Create-table command channel.
    let (create_tx, mut create_rx) = mpsc::channel::<CreateTableCommand>(8);
    {
        let controller_b = create_controller.clone();
        tokio::spawn(async move {
            while let Some(cmd) = create_rx.recv().await {
                match cmd {
                    CreateTableCommand::Start(cfg, reply) => {
                        let mut c = controller_b.lock().await;
                        let r = c.start(cfg).await;
                        let _ = reply.send(r);
                    }
                    CreateTableCommand::Stop(reply) => {
                        let mut c = controller_b.lock().await;
                        let r = c.stop();
                        let _ = reply.send(r);
                    }
                }
            }
        });
    }

    // SSE broadcaster.
    let (snap_tx, snap_rx) = watch::channel(String::from("{}"));
    {
        let state_b = state.clone();
        let gate_b = gate.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(500)).await;
                {
                    let mut s = state_b.lock().await;
                    s.active_job = gate_b.current().await.map(|k| k.to_string());
                }
                let s = state_b.lock().await;
                if let Ok(json) = serde_json::to_string(&*s) {
                    let _ = snap_tx.send(json);
                }
            }
        });
    }

    // Web server.
    {
        let bind = args.dashboard_bind.clone();
        let rx = snap_rx.clone();
        let tun = tunables.clone();
        let stx = sink_tx.clone();
        let ctx = create_tx.clone();
        let lb = log_buffer.clone();
        tokio::spawn(async move {
            if let Err(e) = web::run(bind, rx, tun, Some(stx), Some(ctx), Some(lb)).await {
                eprintln!("[web error] {}", e);
            }
        });
    }

    // If CLI args provided, auto-start a stream job.
    let auto_started = match (args.source_uri.clone(), args.target_uri.clone()) {
        (Some(src), Some(tgt)) => {
            let cfg = SinkLaunchConfig {
                source_uri: src,
                target_uri: tgt,
                sink_mode: args.sink_mode.clone(),
                merge_keys: args.merge_keys.clone(),
                merge_update_columns: args.merge_update_columns.clone(),
                merge_update_predicate: args.merge_update_predicate.clone(),
                merge_delete_predicate: args.merge_delete_predicate.clone(),
                merge_insert_predicate: args.merge_insert_predicate.clone(),
                merge_dedupe_order_by: args.merge_dedupe_order_by.clone(),
                merge_transform_sql: args.merge_transform_sql.clone(),
                merge_transform_rust: match args.merge_transform_rust_file.as_ref() {
                    Some(p) => match std::fs::read_to_string(p) {
                        Ok(s) => Some(s),
                        Err(e) => {
                            eprintln!("Failed to read {}: {}", p.display(), e);
                            return Ok(());
                        }
                    },
                    None => None,
                },
                start_version: args.start_version,
                end_version: args.end_version,
                once: args.once,
                poll_interval_secs: args.poll_interval_secs,
                max_versions_per_batch: args.max_versions_per_batch,
                read_concurrency: args.read_concurrency,
                azure_auth: args.azure_auth.clone(),
                checkpoint_file: args.checkpoint_file.to_string_lossy().to_string(),
            };
            let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
            sink_tx.send(SinkCommand::Start(cfg, reply_tx)).await.ok();
            match reply_rx.await {
                Ok(Ok(())) => {
                    println!("Auto-started sink job from CLI args.");
                    true
                }
                Ok(Err(e)) => {
                    eprintln!("Failed to auto-start: {}", e);
                    false
                }
                Err(_) => false,
            }
        }
        _ => {
            println!(
                "No --source-uri/--target-uri provided. Configure & start the job from the dashboard."
            );
            false
        }
    };

    if auto_started && args.once {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let c = stream_controller.lock().await;
            if !c.is_running() {
                break;
            }
        }
        println!("Job finished (--once).");
        return Ok(());
    }

    tokio::signal::ctrl_c().await.ok();
    println!("\nShutting down...");
    {
        let mut c = stream_controller.lock().await;
        if c.is_running() {
            let _ = c.stop();
        }
    }
    {
        let mut c = create_controller.lock().await;
        if c.is_running() {
            let _ = c.stop();
        }
    }
    Ok(())
}
