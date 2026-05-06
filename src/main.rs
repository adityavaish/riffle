//! Riffle entry point.
//!
//! Wires together: config → web server → SSE broadcaster → optional producer
//! → adaptive consumer.

use anyhow::Result;
use clap::Parser;
use std::sync::Arc;
use tokio::sync::watch;

use riffle::config::Config;
use riffle::state::{DashboardState, SharedState, TunableConfig};
use riffle::{consumer, producer, web};

#[tokio::main]
async fn main() -> Result<()> {
    // Default tracing filter shows info+ from riffle, warn+ from deps.
    let filter = std::env::var("RUST_LOG").unwrap_or_else(|_| "riffle=info,warn".to_string());
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(filter))
        .with_target(false)
        .init();

    let cfg = Config::parse();
    cfg.register_handlers();

    let backend = format!("{:?}", cfg.backend());
    println!("=== RIFFLE — Delta Lake CDC Dashboard ===");
    println!("Backend     : {}", backend);
    println!("Table URI   : {}", cfg.table_uri);
    println!("Dashboard   : http://{}", cfg.bind_addr);
    println!("Producer    : {}", if cfg.producer_enabled { "enabled" } else { "disabled" });
    println!("Consumer    : {}", if cfg.consumer_enabled { "enabled" } else { "disabled" });
    if cfg.sink_enabled {
        println!("Sink        : enabled  mode={}  target={}", cfg.sink_mode, cfg.target_table_uri);
    } else {
        println!("Sink        : disabled");
    }
    println!();

    let (tx, rx) = watch::channel(String::from("{}"));
    let state: SharedState = Arc::new(tokio::sync::Mutex::new(DashboardState::default()));
    let tunables = TunableConfig::new(
        cfg.chunk_target_rows,
        cfg.merge_threshold_rows,
        cfg.split_threshold_rows,
    );

    {
        let mut s = state.lock().await;
        s.producer.status = if cfg.producer_enabled { "Starting..." } else { "Disabled" }.to_string();
        s.consumer.status = if cfg.consumer_enabled { "Waiting for table..." } else { "Disabled" }.to_string();
        s.config = tunables.snapshot();
        s.table_uri = cfg.table_uri.clone();
        s.backend = backend.clone();
    }

    // Web server
    {
        let bind = cfg.bind_addr.clone();
        let rx_for_web = rx.clone();
        let tunables_for_web = tunables.clone();
        tokio::spawn(async move {
            if let Err(e) = web::run(bind, rx_for_web, tunables_for_web).await {
                eprintln!("[web error] {}", e);
            }
        });
    }

    // Broadcast loop: snapshots state every 500ms.
    {
        let state_b = state.clone();
        let tx_b = tx.clone();
        let tunables_b = tunables.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                {
                    let mut s = state_b.lock().await;
                    s.config = tunables_b.snapshot();
                }
                let s = state_b.lock().await;
                if let Ok(json) = serde_json::to_string(&*s) {
                    let _ = tx_b.send(json);
                }
            }
        });
    }

    // Capture pre-launch baseline BEFORE the producer starts, so the producer's
    // first write (which uses Overwrite) is still detected by the consumer.
    let pre_launch_version = consumer::capture_baseline_version(&cfg).await?;

    let mut handles = Vec::new();

    if cfg.producer_enabled {
        let s = state.clone();
        let c = cfg.clone();
        handles.push(tokio::spawn(async move {
            if let Err(e) = producer::run(s, c).await {
                eprintln!("[producer error] {}", e);
            }
        }));
    }

    if cfg.consumer_enabled {
        // If a producer is also running, give it a head start so the table exists.
        if cfg.producer_enabled {
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        }
        let s = state.clone();
        let c = cfg.clone();
        let t = tunables.clone();
        handles.push(tokio::spawn(async move {
            if let Err(e) = consumer::run(s, c, pre_launch_version, t).await {
                eprintln!("[consumer error] {}", e);
            }
        }));
    }

    if handles.is_empty() {
        // Neither producer nor consumer enabled — just keep the dashboard alive.
        tracing::warn!("Both producer and consumer are disabled; dashboard only.");
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(3600)).await;
        }
    }

    for h in handles {
        let _ = h.await;
    }
    Ok(())
}
