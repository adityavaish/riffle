//! Shared dashboard state, tunable consumer config, and SSE payload types.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Tunable thresholds the consumer reads on every poll iteration.
/// Atomic so HTTP handlers can mutate them with no locking.
#[derive(Clone)]
pub struct TunableConfig {
    pub chunk_target_rows: Arc<AtomicU64>,
    pub merge_threshold_rows: Arc<AtomicU64>,
    pub split_threshold_rows: Arc<AtomicU64>,
}

impl TunableConfig {
    pub fn new(chunk_target: u64, merge_threshold: u64, split_threshold: u64) -> Self {
        Self {
            chunk_target_rows: Arc::new(AtomicU64::new(chunk_target)),
            merge_threshold_rows: Arc::new(AtomicU64::new(merge_threshold)),
            split_threshold_rows: Arc::new(AtomicU64::new(split_threshold)),
        }
    }

    pub fn snapshot(&self) -> ConfigSnapshot {
        ConfigSnapshot {
            chunk_target_rows: self.chunk_target_rows.load(Ordering::Relaxed),
            merge_threshold_rows: self.merge_threshold_rows.load(Ordering::Relaxed),
            split_threshold_rows: self.split_threshold_rows.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct ConfigSnapshot {
    pub chunk_target_rows: u64,
    pub merge_threshold_rows: u64,
    pub split_threshold_rows: u64,
}

#[derive(Clone, serde::Serialize, Default)]
pub struct DashboardState {
    pub producer: ProducerState,
    pub consumer: ConsumerState,
    pub sink: SinkState,
    pub create_table: CreateTableState,
    pub config: ConfigSnapshot,
    pub table_uri: String,
    pub backend: String,
    /// "dashboard" (producer + consumer + optional sink) or "stream-cli" (sink only).
    pub app_mode: String,
    /// Identifier of the currently-running job, if any: "stream" | "create-table" | "demo".
    pub active_job: Option<String>,
}

#[derive(Clone, serde::Serialize, Default)]
pub struct ProducerState {
    pub status: String,
    pub batches_written: u32,
    pub total_batches: u32,
    pub total_rows_written: u64,
    pub current_version: i64,
    pub last_write_duration_ms: u64,
    pub avg_write_duration_ms: u64,
    pub rows_per_sec: f64,
    pub events: Vec<ProducerEvent>,
}

#[derive(Clone, serde::Serialize)]
pub struct ProducerEvent {
    pub timestamp: String,
    pub batch_id: u32,
    pub rows: u64,
    pub version: i64,
    pub duration_ms: u64,
}

#[derive(Clone, serde::Serialize, Default)]
pub struct ConsumerState {
    pub status: String,
    pub mode: String,
    pub versions_processed: u64,
    pub chunks_processed: u64,
    pub total_rows_consumed: u64,
    pub last_version_seen: i64,
    pub pending_versions: u64,
    pub last_poll_duration_ms: u64,
    pub avg_latency_ms: u64,
    pub checkpoint: String,
    pub events: Vec<ConsumerEvent>,
}

#[derive(Clone, serde::Serialize)]
pub struct ConsumerEvent {
    pub timestamp: String,
    pub mode: String,
    pub range: String,
    pub version: i64,
    pub new_rows: u64,
    pub latency_ms: u64,
}

#[derive(Clone, serde::Serialize, Default)]
pub struct SinkState {
    pub enabled: bool,
    pub mode: String,
    pub target_uri: String,
    pub target_backend: String,
    pub status: String,
    pub target_version: i64,
    pub batches_processed: u64,
    pub total_inserts: u64,
    pub total_updates: u64,
    pub total_deletes: u64,
    pub total_appended: u64,
    pub last_duration_ms: u64,
    pub avg_duration_ms: u64,
    pub last_source_rows: u64,
    pub events: Vec<SinkEvent>,
    /// Whether a sink job is currently running.
    pub running: bool,
    /// Last error message (cleared when a new job starts).
    pub last_error: String,
    /// Last-applied launch config (echoed back to UI to pre-fill form).
    pub launch_config: Option<SinkLaunchConfig>,
}

#[derive(Clone, serde::Serialize)]
pub struct SinkEvent {
    pub timestamp: String,
    pub mode: String,
    pub source_rows: u64,
    pub inserts: u64,
    pub updates: u64,
    pub deletes: u64,
    pub appended: u64,
    pub target_version: i64,
    pub duration_ms: u64,
}

/// Launch parameters for a sink job, mirroring the CLI flags.
/// Sent from the web UI (POST /api/sink/start) or constructed from CLI args.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct SinkLaunchConfig {
    pub source_uri: String,
    pub target_uri: String,
    pub sink_mode: String,
    #[serde(default)]
    pub merge_keys: String,
    #[serde(default)]
    pub merge_update_columns: String,
    #[serde(default)]
    pub merge_update_predicate: Option<String>,
    #[serde(default)]
    pub merge_delete_predicate: Option<String>,
    #[serde(default)]
    pub merge_insert_predicate: Option<String>,
    #[serde(default)]
    pub merge_dedupe_order_by: Option<String>,
    #[serde(default)]
    pub merge_transform_sql: Option<String>,
    /// User-provided Rust source code (function body); compiled and loaded at stream start.
    /// Mutually exclusive with `merge_transform_sql`.
    #[serde(default)]
    pub merge_transform_rust: Option<String>,
    #[serde(default)]
    pub start_version: Option<i64>,
    #[serde(default)]
    pub end_version: Option<i64>,
    #[serde(default)]
    pub once: bool,
    #[serde(default = "default_poll_interval")]
    pub poll_interval_secs: u64,
    #[serde(default = "default_max_versions")]
    pub max_versions_per_batch: usize,
    #[serde(default = "default_read_concurrency")]
    pub read_concurrency: usize,
    #[serde(default = "default_azure_auth")]
    pub azure_auth: String,
    #[serde(default = "default_checkpoint")]
    pub checkpoint_file: String,
}

fn default_poll_interval() -> u64 { 5 }
fn default_max_versions() -> usize { 10 }
fn default_read_concurrency() -> usize { 8 }
fn default_azure_auth() -> String { "auto".to_string() }
fn default_checkpoint() -> String { "./riffle-stream-ckpt.json".to_string() }

/// Commands sent from the web layer to the sink controller task.
#[derive(Debug)]
pub enum SinkCommand {
    Start(SinkLaunchConfig, tokio::sync::oneshot::Sender<Result<(), String>>),
    Stop(tokio::sync::oneshot::Sender<Result<(), String>>),
}

#[derive(serde::Serialize, serde::Deserialize, Default, Clone)]
pub struct DiskCheckpoint {
    pub last_full_version: i64,
    pub partial_file_offset: usize,
    pub rows_committed: u64,
}

pub type SharedState = Arc<tokio::sync::Mutex<DashboardState>>;

pub fn fmt_ckpt(c: &DiskCheckpoint) -> String {
    if c.partial_file_offset == 0 {
        format!("v{}:complete", c.last_full_version)
    } else {
        format!("v{}+chunk{}", c.last_full_version, c.partial_file_offset)
    }
}

// ---------------------------------------------------------------------------
// create-table dashboard tab
// ---------------------------------------------------------------------------

#[derive(Clone, serde::Serialize, Default)]
pub struct CreateTableState {
    pub running: bool,
    pub status: String,
    pub last_error: String,
    pub target_uri: String,
    pub commits_done: usize,
    pub total_commits: usize,
    pub rows_written: u64,
    pub total_rows: u64,
    pub last_commit_ms: u64,
    pub optimize_status: String,
    pub launch_config: Option<CreateTableLaunchConfig>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct CreateTableLaunchConfig {
    pub uri: String,
    #[serde(default = "default_create_rows")]
    pub rows: usize,
    #[serde(default = "default_create_commits")]
    pub commits: usize,
    #[serde(default = "default_create_columns")]
    pub columns: usize,
    #[serde(default = "default_zorder_by")]
    pub zorder_by: String,
    #[serde(default)]
    pub overwrite: bool,
    #[serde(default = "default_azure_auth")]
    pub azure_auth: String,
}

fn default_create_rows() -> usize { 200_000 }
fn default_create_commits() -> usize { 1 }
fn default_create_columns() -> usize { 30 }
fn default_zorder_by() -> String { "event_timestamp".to_string() }

#[derive(Debug)]
pub enum CreateTableCommand {
    Start(CreateTableLaunchConfig, tokio::sync::oneshot::Sender<Result<(), String>>),
    Stop(tokio::sync::oneshot::Sender<Result<(), String>>),
}

// ---------------------------------------------------------------------------
// Shared rolling log buffer
// ---------------------------------------------------------------------------

/// In-memory ring buffer of recent log lines, fed by a custom `tracing` layer
/// and exposed via `/api/logs` SSE so any tab can tail it.
#[derive(Clone)]
pub struct LogBuffer {
    inner: Arc<std::sync::Mutex<VecDeque<LogLine>>>,
    capacity: usize,
    seq: Arc<AtomicU64>,
    notify: Arc<tokio::sync::Notify>,
}

#[derive(Clone, serde::Serialize)]
pub struct LogLine {
    pub seq: u64,
    pub ts: String,
    pub level: String,
    pub target: String,
    pub message: String,
}

impl LogBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Arc::new(std::sync::Mutex::new(VecDeque::with_capacity(capacity))),
            capacity,
            seq: Arc::new(AtomicU64::new(0)),
            notify: Arc::new(tokio::sync::Notify::new()),
        }
    }

    pub fn push(&self, level: &str, target: &str, message: String) {
        let seq = self.seq.fetch_add(1, Ordering::Relaxed) + 1;
        let line = LogLine {
            seq,
            ts: chrono::Utc::now().format("%H:%M:%S%.3f").to_string(),
            level: level.to_string(),
            target: target.to_string(),
            message,
        };
        let mut g = self.inner.lock().unwrap();
        if g.len() == self.capacity {
            g.pop_front();
        }
        g.push_back(line);
        drop(g);
        self.notify.notify_waiters();
    }

    /// Snapshot of all currently-buffered lines.
    pub fn snapshot(&self) -> Vec<LogLine> {
        self.inner.lock().unwrap().iter().cloned().collect()
    }

    /// Lines after `since_seq` (exclusive).
    pub fn since(&self, since_seq: u64) -> Vec<LogLine> {
        self.inner
            .lock()
            .unwrap()
            .iter()
            .filter(|l| l.seq > since_seq)
            .cloned()
            .collect()
    }

    /// Wait until at least one new line is available (or timeout fires).
    pub async fn wait_change(&self) {
        self.notify.notified().await;
    }
}
