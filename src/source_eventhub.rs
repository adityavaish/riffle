//! Azure Event Hubs source for the streaming sink.
//!
//! Consumes events from an Event Hub instance (one tokio task per partition,
//! all pushing into a shared `mpsc::channel<EventRow>`) and converts them
//! into Arrow `RecordBatch`es with a fixed 6-column schema:
//!
//! | column          | type                  |
//! |-----------------|-----------------------|
//! | partition_id    | Utf8                  |
//! | offset          | Utf8 (nullable)       |
//! | sequence_number | Int64                 |
//! | enqueued_time   | Timestamp(us, "UTC")  |
//! | body            | Utf8 (UTF-8 lossy)    |
//! | properties      | Utf8 (JSON)           |
//!
//! Body bytes are decoded as UTF-8 with replacement; for binary payloads,
//! the user transform can re-decode from the original bytes via the
//! `decode_*` helpers, but in v1 we emit a UTF-8 string only. (If you have
//! binary payloads, ask for a `body_bytes` BinaryArray column.)
//!
//! Auth: AAD only in v1, via `azure_identity::DeveloperToolsCredential`.
//! That single credential type chains: az-cli, environment-variable SP
//! (`AZURE_CLIENT_ID` + `AZURE_TENANT_ID` + `AZURE_CLIENT_SECRET`),
//! managed identity, and VS / VS Code credentials. Connection-string auth
//! is not yet supported (use `az login` or set the SP env vars).

use anyhow::{anyhow, Context, Result};
use arrow::array::{
    ArrayRef, Int64Builder, RecordBatch, StringBuilder, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef as ArrowSchemaRef, TimeUnit};
use azure_identity::DeveloperToolsCredential;
use azure_messaging_eventhubs::{
    ConsumerClient, OpenReceiverOptions, StartLocation, StartPosition,
};
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

/// Where to start reading per-partition when there is no checkpoint yet.
#[derive(Debug, Clone, Copy)]
pub enum InitialPosition {
    /// Start from the earliest event in each partition (replay everything).
    Earliest,
    /// Start from the latest event (skip backlog; only consume new events).
    Latest,
}

impl InitialPosition {
    pub fn parse(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "" | "latest" | "newest" => Ok(InitialPosition::Latest),
            "earliest" | "oldest" | "beginning" => Ok(InitialPosition::Earliest),
            other => Err(anyhow!(
                "invalid initial position '{}': expected 'earliest' or 'latest'",
                other
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct EventHubSourceConfig {
    /// Fully qualified namespace host, e.g. `mynamespace.servicebus.windows.net`.
    pub namespace: String,
    /// Event Hub instance (entity) name.
    pub event_hub: String,
    /// Consumer group. Pass `"$Default"` if unspecified.
    pub consumer_group: String,
    /// Where to start when there is no checkpoint. Resumed offsets, when
    /// present in the checkpoint, override this per partition.
    pub initial_position: InitialPosition,
    /// Maximum events accumulated before forcing a flush to the sink.
    pub max_events_per_batch: usize,
    /// Time-based flush — close the batch even if `max_events_per_batch`
    /// is not reached.
    pub batch_timeout_secs: u64,
    /// Optional explicit list of partition IDs to consume. When `None` the
    /// hub's full partition list is auto-discovered.
    pub partitions: Option<Vec<String>>,
    /// AMQP prefetch count per partition receiver. Higher = better throughput
    /// for backlog catch-up at the cost of memory per partition. Default 1000.
    pub prefetch: u32,
}

impl EventHubSourceConfig {
    /// Stable signature used for checkpoint identity. Changing the namespace,
    /// hub, or consumer group invalidates the checkpoint.
    pub fn checkpoint_signature(&self) -> String {
        format!("eh:{}|{}|{}", self.namespace, self.event_hub, self.consumer_group)
    }
}

/// Schema of the RecordBatch produced from EH events. This is the schema the
/// user transform sees as `__src`.
pub fn event_hub_arrow_schema() -> ArrowSchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("partition_id", DataType::Utf8, false),
        Field::new("offset", DataType::Utf8, true),
        Field::new("sequence_number", DataType::Int64, false),
        Field::new(
            "enqueued_time",
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
            true,
        ),
        Field::new("body", DataType::Utf8, true),
        Field::new("properties", DataType::Utf8, true),
    ]))
}

/// One row built from a `ReceivedEventData`. Lightweight to send across
/// the partition→main mpsc channel.
#[derive(Debug, Clone)]
struct EventRow {
    partition_id: String,
    offset: Option<String>,
    sequence_number: i64,
    enqueued_us: Option<i64>,
    body: String,
    properties: Option<String>,
}

fn system_time_to_us(ts: SystemTime) -> Option<i64> {
    ts.duration_since(UNIX_EPOCH).ok().and_then(|d| {
        let us = d.as_micros();
        if us > i64::MAX as u128 { None } else { Some(us as i64) }
    })
}

fn build_record_batch(rows: &[EventRow]) -> Result<RecordBatch> {
    let schema = event_hub_arrow_schema();
    let mut partition_id = StringBuilder::with_capacity(rows.len(), rows.len() * 4);
    let mut offset = StringBuilder::with_capacity(rows.len(), rows.len() * 16);
    let mut seq = Int64Builder::with_capacity(rows.len());
    let mut ts = TimestampMicrosecondBuilder::with_capacity(rows.len())
        .with_timezone("UTC");
    let mut body = StringBuilder::with_capacity(rows.len(), rows.len() * 64);
    let mut props = StringBuilder::with_capacity(rows.len(), rows.len() * 32);
    for r in rows {
        partition_id.append_value(&r.partition_id);
        match &r.offset {
            Some(o) => offset.append_value(o),
            None => offset.append_null(),
        }
        seq.append_value(r.sequence_number);
        match r.enqueued_us {
            Some(t) => ts.append_value(t),
            None => ts.append_null(),
        }
        body.append_value(&r.body);
        match &r.properties {
            Some(p) => props.append_value(p),
            None => props.append_null(),
        }
    }
    let columns: Vec<ArrayRef> = vec![
        Arc::new(partition_id.finish()),
        Arc::new(offset.finish()),
        Arc::new(seq.finish()),
        Arc::new(ts.finish()),
        Arc::new(body.finish()),
        Arc::new(props.finish()),
    ];
    Ok(RecordBatch::try_new(schema, columns)?)
}

/// Open a consumer client + discover partitions (or use the user-supplied list).
async fn open_client(
    cfg: &EventHubSourceConfig,
) -> Result<(ConsumerClient, Vec<String>)> {
    let credential = DeveloperToolsCredential::new(None)
        .map_err(|e| anyhow!("DeveloperToolsCredential: {}", e))?;
    let mut builder = ConsumerClient::builder();
    if !cfg.consumer_group.trim().is_empty() {
        builder = builder.with_consumer_group(cfg.consumer_group.clone());
    }
    // Be lenient about how the user types the namespace: accept any of
    //   myns
    //   myns.servicebus.windows.net
    //   sb://myns.servicebus.windows.net
    //   https://myns.servicebus.windows.net:443/
    // and reduce to the FQDN host.
    let ns_host = sanitize_namespace(&cfg.namespace);
    let consumer = builder
        .open(&ns_host, cfg.event_hub.clone(), credential)
        .await
        .map_err(|e| anyhow!("open EventHub consumer: {}", e))?;
    let partitions = if let Some(ps) = cfg.partitions.as_ref() {
        ps.clone()
    } else {
        consumer
            .get_eventhub_properties()
            .await
            .map_err(|e| anyhow!("get_eventhub_properties: {}", e))?
            .partition_ids
    };
    if partitions.is_empty() {
        return Err(anyhow!("no partitions discovered for {}", cfg.event_hub));
    }
    Ok((consumer, partitions))
}

/// Strip scheme, port, path, and whitespace from a user-supplied namespace
/// string. Returns the FQDN host that `ConsumerClient::open` expects.
fn sanitize_namespace(raw: &str) -> String {
    let mut s = raw.trim();
    for scheme in ["amqps://", "sb://", "https://", "http://"] {
        if let Some(rest) = s.strip_prefix(scheme) {
            s = rest;
            break;
        }
    }
    // Drop a path suffix if present (e.g. trailing '/' or '/hubname').
    let s = match s.find('/') {
        Some(i) => &s[..i],
        None => s,
    };
    // Drop a port suffix if present (e.g. ':443').
    let s = match s.rsplit_once(':') {
        Some((host, _port)) => host,
        None => s,
    };
    s.trim().to_string()
}

/// Resolve the per-partition `StartPosition` from checkpoint offsets and
/// the configured initial position.
fn start_position_for(
    partition: &str,
    resume_from: &HashMap<String, i64>,
    fallback: InitialPosition,
) -> StartPosition {
    if let Some(seq) = resume_from.get(partition) {
        // Resume strictly after the last checkpointed sequence number.
        StartPosition {
            location: StartLocation::SequenceNumber(*seq),
            inclusive: false,
            ..Default::default()
        }
    } else {
        StartPosition {
            location: match fallback {
                InitialPosition::Earliest => StartLocation::Earliest,
                InitialPosition::Latest => StartLocation::Latest,
            },
            ..Default::default()
        }
    }
}

/// Spawn one tokio task per partition that streams events into the shared
/// mpsc::Sender. Returns the consumer client + a `JoinSet` of tasks (held by
/// caller; tasks exit when `cancel` flips or the consumer client is dropped).
pub struct EventHubReader {
    client: ConsumerClient,
    rx: mpsc::Receiver<EventRow>,
    /// Task handles, kept for graceful shutdown.
    _tasks: Vec<tokio::task::JoinHandle<()>>,
    cancel: Arc<std::sync::atomic::AtomicBool>,
}

impl EventHubReader {
    pub async fn open(
        cfg: &EventHubSourceConfig,
        resume_from: &HashMap<String, i64>,
        cancel: Arc<std::sync::atomic::AtomicBool>,
    ) -> Result<Self> {
        let (consumer, partitions) = open_client(cfg).await?;
        tracing::info!(
            "[eh] consumer open: ns={} hub={} cg={} partitions=[{}]",
            cfg.namespace,
            cfg.event_hub,
            cfg.consumer_group,
            partitions.join(",")
        );
        let buffer_size = cfg.max_events_per_batch.max(1024).min(100_000);
        let (tx, rx) = mpsc::channel::<EventRow>(buffer_size);
        let mut tasks = Vec::with_capacity(partitions.len());
        for pid in partitions {
            let pos = start_position_for(&pid, resume_from, cfg.initial_position);
            let receiver = consumer
                .open_receiver_on_partition(
                    pid.clone(),
                    Some(OpenReceiverOptions {
                        start_position: Some(pos),
                        prefetch: Some(cfg.prefetch.max(1)),
                        ..Default::default()
                    }),
                )
                .await
                .map_err(|e| anyhow!("open_receiver_on_partition({}): {}", pid, e))?;
            let tx = tx.clone();
            let cancel_t = cancel.clone();
            let pid_t = pid.clone();
            let task = tokio::spawn(async move {
                let mut stream = receiver.stream_events();
                while !cancel_t.load(std::sync::atomic::Ordering::Relaxed) {
                    let next = tokio::time::timeout(
                        Duration::from_millis(500),
                        stream.next(),
                    )
                    .await;
                    let event = match next {
                        Ok(Some(Ok(ev))) => ev,
                        Ok(Some(Err(e))) => {
                            tracing::warn!(
                                "[eh] partition {} receive error: {}",
                                pid_t, e
                            );
                            continue;
                        }
                        Ok(None) => {
                            tracing::info!(
                                "[eh] partition {} stream ended",
                                pid_t
                            );
                            break;
                        }
                        Err(_) => continue, // poll-timeout, loop and check cancel
                    };
                    let row = received_to_row(&pid_t, &event);
                    if tx.send(row).await.is_err() {
                        // Receiver dropped; exit task.
                        break;
                    }
                }
                tracing::info!("[eh] partition {} consumer task exiting", pid_t);
            });
            tasks.push(task);
        }
        // Drop our local copy; tasks each own a clone.
        drop(tx);
        Ok(Self {
            client: consumer,
            rx,
            _tasks: tasks,
            cancel,
        })
    }

    /// Drain events for up to `timeout_secs` or until `max_events` are received,
    /// whichever first. Returns the assembled RecordBatch (or `None` if no
    /// events arrived) and the latest sequence number observed per partition.
    pub async fn next_batch(
        &mut self,
        max_events: usize,
        timeout: Duration,
    ) -> Result<Option<EventHubBatch>> {
        let deadline = Instant::now() + timeout;
        let mut rows: Vec<EventRow> = Vec::with_capacity(max_events.min(8192));
        let mut latest_seq: HashMap<String, i64> = HashMap::new();
        // Wait for the first event up to the full timeout (so we don't
        // spin when idle), then drain until count or deadline.
        loop {
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            let remaining = deadline - now;
            let recv = tokio::time::timeout(remaining, self.rx.recv()).await;
            match recv {
                Ok(Some(row)) => {
                    let pid = row.partition_id.clone();
                    let seq = row.sequence_number;
                    rows.push(row);
                    let entry = latest_seq.entry(pid).or_insert(seq);
                    if seq > *entry {
                        *entry = seq;
                    }
                    if rows.len() >= max_events {
                        break;
                    }
                }
                Ok(None) => {
                    // All senders dropped — partitions ended or cancelled.
                    break;
                }
                Err(_) => break, // timeout
            }
            if self.cancel.load(std::sync::atomic::Ordering::Relaxed) {
                break;
            }
        }
        if rows.is_empty() {
            return Ok(None);
        }
        let batch = build_record_batch(&rows)?;
        Ok(Some(EventHubBatch {
            batch,
            row_count: rows.len() as u64,
            latest_seq_by_partition: latest_seq,
        }))
    }

    pub async fn close(self) {
        // Bound the AMQP shutdown so a slow broker close doesn't blow past
        // the controller's 10s stop budget. 3s is plenty; partitions are
        // already draining via the cancel flag, and dropping the client is
        // safe even if the close handshake hasn't completed yet.
        let _ = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            self.client.close(),
        )
        .await;
    }
}

#[derive(Debug)]
pub struct EventHubBatch {
    pub batch: RecordBatch,
    pub row_count: u64,
    pub latest_seq_by_partition: HashMap<String, i64>,
}

fn received_to_row(
    partition_id: &str,
    ev: &azure_messaging_eventhubs::models::ReceivedEventData,
) -> EventRow {
    let event_data = ev.event_data();
    let body_bytes_opt = event_data.body();
    let body_str = match body_bytes_opt {
        None => String::new(),
        Some(b) => match std::str::from_utf8(b) {
            Ok(s) => s.to_string(),
            Err(_) => {
                use base64::Engine as _;
                // Tag base64 payloads so user transforms can detect them.
                format!(
                    "{{\"_riffle_b64\":\"{}\"}}",
                    base64::engine::general_purpose::STANDARD.encode(b)
                )
            }
        },
    };
    let props = event_data
        .properties()
        .map(|m| {
            let json: serde_json::Map<String, serde_json::Value> = m
                .iter()
                .map(|(k, v)| (k.clone(), serde_json::Value::String(format!("{:?}", v))))
                .collect();
            serde_json::Value::Object(json).to_string()
        });
    let offset = ev.offset().clone();
    let sequence_number = ev.sequence_number().unwrap_or(-1);
    let enqueued_us = ev.enqueued_time().and_then(system_time_to_us);
    EventRow {
        partition_id: partition_id.to_string(),
        offset,
        sequence_number,
        enqueued_us,
        body: body_str,
        properties: props,
    }
}

/// Public helper — used by the stream loop to validate the EH config at
/// start time (fails fast on bad credentials / unreachable namespace).
pub async fn preflight(cfg: &EventHubSourceConfig) -> Result<Vec<String>> {
    let (client, partitions) = open_client(cfg).await
        .with_context(|| format!("eventhub preflight {}/{}", cfg.namespace, cfg.event_hub))?;
    let _ = client.close().await;
    Ok(partitions)
}
