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
    DashboardState, SharedState, SinkCommand, SinkEvent, SinkLaunchConfig, TunableConfig,
};
use crate::web;

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
}

impl Controller {
    fn new(state: SharedState) -> Self {
        Self {
            state,
            handle: None,
            cancel: None,
            backends_registered: HashSet::new(),
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
        let mode = sink::parse_mode(
            &cfg.sink_mode,
            &cfg.merge_keys,
            &cfg.merge_update_columns,
            cfg.merge_update_predicate.clone(),
            cfg.merge_delete_predicate.clone(),
            cfg.merge_insert_predicate.clone(),
        )
        .map_err(|e| format!("parse_mode: {}", e))?;

        let sink_cfg = SinkConfig {
            source_uri: cfg.source_uri.clone(),
            target_uri: cfg.target_uri.clone(),
            source_storage_options: source_storage.clone(),
            target_storage_options: target_storage,
            mode,
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
        });

        self.handle = Some(handle);
        self.cancel = Some(cancel);
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), String> {
        if !self.is_running() {
            return Err("no sink job is currently running".into());
        }
        if let Some(c) = &self.cancel {
            c.store(true, Ordering::Relaxed);
        }
        if let Some(h) = self.handle.take() {
            tracing::info!("[stream] stop requested; waiting up to 30s for cooperative cancel");
            let abort_handle = h.abort_handle();
            match tokio::time::timeout(Duration::from_secs(30), h).await {
                Ok(_) => {
                    tracing::info!("[stream] sink loop exited cleanly");
                }
                Err(_) => {
                    tracing::warn!(
                        "[stream] sink loop did not exit within 30s (likely mid-merge); aborting hard"
                    );
                    abort_handle.abort();
                    // Give the runtime a beat to actually drop the task before we return.
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
        self.cancel = None;
        let mut s = self.state.lock().await;
        s.sink.status = "Stopped".to_string();
        s.sink.running = false;
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
        let source: DeltaTable =
            open_table_with_storage_options(&launch.source_uri, source_storage_options.clone())
                .await
                .with_context(|| format!("open source {}", launch.source_uri))?;
        let current_version = source.version();
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
            // Heartbeat task: emit progress every 5s while sink::apply runs so users see the job is alive.
            let hb_state = state.clone();
            let hb_label = format!("v{}..=v{}", group_start, group_end);
            let hb_mode = sink_cfg.mode.name().to_string();
            let hb_done = Arc::new(AtomicBool::new(false));
            let hb_done_b = hb_done.clone();
            let hb = tokio::spawn(async move {
                let start = std::time::Instant::now();
                let mut tick: u64 = 0;
                while !hb_done_b.load(Ordering::Relaxed) {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    if hb_done_b.load(Ordering::Relaxed) {
                        break;
                    }
                    tick += 1;
                    let secs = start.elapsed().as_secs();
                    tracing::info!(
                        "[stream] {} {} in progress... {}s elapsed",
                        hb_mode,
                        hb_label,
                        secs
                    );
                    let mut s = hb_state.lock().await;
                    s.sink.status = format!(
                        "{} {} in progress ({}s, tick #{})",
                        hb_mode, hb_label, secs, tick
                    );
                }
            });
            let outcome_res = sink::apply(&sink_cfg, &source, &versions).await;
            hb_done.store(true, Ordering::Relaxed);
            let _ = hb.await;
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

pub async fn run(args: StreamArgs) -> Result<()> {
    println!("=== riffle stream ===");
    println!("Dashboard : http://{}", args.dashboard_bind);
    println!();

    // Shared dashboard state, always stream-cli mode.
    let state: SharedState = Arc::new(Mutex::new(DashboardState::default()));
    {
        let mut s = state.lock().await;
        s.app_mode = "stream-cli".to_string();
        s.sink.enabled = true;
        s.sink.status = "Idle (no job running)".to_string();
        s.sink.running = false;
    }
    let tunables = TunableConfig::new(50_000, 30_000, 200_000);
    let controller = Arc::new(Mutex::new(Controller::new(state.clone())));

    // Command channel from web → controller.
    let (cmd_tx, mut cmd_rx) = mpsc::channel::<SinkCommand>(8);

    // Controller task processes Start/Stop commands.
    {
        let controller_b = controller.clone();
        tokio::spawn(async move {
            while let Some(cmd) = cmd_rx.recv().await {
                match cmd {
                    SinkCommand::Start(cfg, reply) => {
                        let mut c = controller_b.lock().await;
                        let r = c.start(cfg).await;
                        let _ = reply.send(r);
                    }
                    SinkCommand::Stop(reply) => {
                        let mut c = controller_b.lock().await;
                        let r = c.stop().await;
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
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(500)).await;
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
        let tx = cmd_tx.clone();
        tokio::spawn(async move {
            if let Err(e) = web::run(bind, rx, tun, Some(tx)).await {
                eprintln!("[web error] {}", e);
            }
        });
    }

    // If CLI args provided, auto-start a job.
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
                start_version: args.start_version,
                end_version: args.end_version,
                once: args.once,
                poll_interval_secs: args.poll_interval_secs,
                max_versions_per_batch: args.max_versions_per_batch,
                azure_auth: args.azure_auth.clone(),
                checkpoint_file: args.checkpoint_file.to_string_lossy().to_string(),
            };
            let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
            cmd_tx.send(SinkCommand::Start(cfg, reply_tx)).await.ok();
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

    // Wait. If --once and a job auto-started, exit when it finishes.
    // Otherwise run forever (Ctrl-C to quit).
    if auto_started && args.once {
        // Poll for completion.
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let c = controller.lock().await;
            if !c.is_running() {
                break;
            }
        }
        println!("Job finished (--once).");
        return Ok(());
    }

    // Park forever; the dashboard keeps serving and the user can start/stop jobs.
    tokio::signal::ctrl_c().await.ok();
    println!("\nShutting down...");
    {
        let mut c = controller.lock().await;
        if c.is_running() {
            let _ = c.stop().await;
        }
    }
    Ok(())
}
