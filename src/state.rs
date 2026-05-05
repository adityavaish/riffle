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
    pub config: ConfigSnapshot,
    pub table_uri: String,
    pub backend: String,
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
