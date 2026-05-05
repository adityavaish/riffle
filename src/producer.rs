//! Demo producer that appends randomly-sized batches to the Delta table.
//!
//! In a real deployment you would replace this with your actual write pipeline
//! (or disable it entirely with `--no-producer-enabled`) and let Riffle just
//! consume changes another writer is producing.

use anyhow::Result;
use chrono::Utc;
use deltalake::protocol::SaveMode;
use deltalake::{open_table_with_storage_options, DeltaOps};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::time::Instant;

use crate::config::Config;
use crate::sample_data::{build_schema, generate_batch};
use crate::state::{ProducerEvent, SharedState};

pub async fn run(state: SharedState, cfg: Config) -> Result<()> {
    let storage_options = cfg.storage_options()?;
    let schema = build_schema();
    let mut rng = StdRng::seed_from_u64(Utc::now().timestamp_millis() as u64);
    let mut batch_idx: usize = 0;
    let mut total_rows_so_far: u64 = 0;
    let start_time = Instant::now();
    let mut write_durations: Vec<u64> = Vec::new();

    loop {
        let rows_this_batch = rng.gen_range(cfg.batch_min_rows..=cfg.batch_max_rows);
        let batch = generate_batch(&schema, rows_this_batch, batch_idx as i32, &mut rng)?;

        let write_start = Instant::now();
        let version = if batch_idx == 0 {
            // First write: create the table if it doesn't exist by overwriting.
            let ops = DeltaOps::try_from_uri_with_storage_options(
                &cfg.table_uri,
                storage_options.clone(),
            )
            .await?;
            let result = ops.write(vec![batch]).with_save_mode(SaveMode::Overwrite).await?;
            result.version()
        } else {
            let table =
                open_table_with_storage_options(&cfg.table_uri, storage_options.clone()).await?;
            let result = DeltaOps(table)
                .write(vec![batch])
                .with_save_mode(SaveMode::Append)
                .await?;
            result.version()
        };

        let write_dur = write_start.elapsed().as_millis() as u64;
        write_durations.push(write_dur);
        if write_durations.len() > 100 {
            write_durations.remove(0);
        }

        total_rows_so_far += rows_this_batch as u64;
        let elapsed = start_time.elapsed().as_secs_f64();
        let rows_per_sec = total_rows_so_far as f64 / elapsed.max(0.001);
        let avg_dur = write_durations.iter().sum::<u64>() / write_durations.len() as u64;

        {
            let mut s = state.lock().await;
            s.producer.status = "Writing".to_string();
            s.producer.batches_written = (batch_idx + 1) as u32;
            s.producer.total_rows_written = total_rows_so_far;
            s.producer.current_version = version;
            s.producer.last_write_duration_ms = write_dur;
            s.producer.avg_write_duration_ms = avg_dur;
            s.producer.rows_per_sec = rows_per_sec;
            s.producer.events.push(ProducerEvent {
                timestamp: Utc::now().format("%H:%M:%S").to_string(),
                batch_id: batch_idx as u32,
                rows: rows_this_batch as u64,
                version,
                duration_ms: write_dur,
            });
            if s.producer.events.len() > 50 {
                s.producer.events.remove(0);
            }
        }

        tracing::info!(
            "[producer] batch {} | v{} | rows={} | {}ms | {:.0} rows/sec",
            batch_idx,
            version,
            rows_this_batch,
            write_dur,
            rows_per_sec
        );

        batch_idx += 1;
        tokio::time::sleep(std::time::Duration::from_secs(cfg.write_interval_secs)).await;
    }
}
