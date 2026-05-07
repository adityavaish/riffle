//! Shared dashboard state, tunable consumer config, and SSE payload types.

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
    pub config: ConfigSnapshot,
    pub table_uri: String,
    pub backend: String,
    /// "dashboard" (producer + consumer + optional sink) or "sink-cli" (sink only).
    pub app_mode: String,
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
    pub start_version: Option<i64>,
    #[serde(default)]
    pub end_version: Option<i64>,
    #[serde(default)]
    pub once: bool,
    #[serde(default = "default_poll_interval")]
    pub poll_interval_secs: u64,
    #[serde(default = "default_max_versions")]
    pub max_versions_per_batch: usize,
    #[serde(default = "default_azure_auth")]
    pub azure_auth: String,
    #[serde(default = "default_checkpoint")]
    pub checkpoint_file: String,
}

fn default_poll_interval() -> u64 { 5 }
fn default_max_versions() -> usize { 10 }
fn default_azure_auth() -> String { "auto".to_string() }
fn default_checkpoint() -> String { "./riffle-sink-ckpt.json".to_string() }

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
