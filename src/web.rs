//! Web server: HTML dashboard, SSE state stream, and tunables REST API.

use anyhow::Result;
use axum::extract::{Json as JsonExt, State};
use axum::response::sse::{Event, Sse};
use axum::response::Html;
use axum::routing::get;
use axum::Router;
use std::convert::Infallible;
use std::sync::atomic::Ordering;
use tokio::sync::watch;

use crate::state::{ConfigSnapshot, TunableConfig};

static DASHBOARD_HTML: &str = include_str!("dashboard.html");

#[derive(Clone)]
pub struct AppState {
    pub rx: watch::Receiver<String>,
    pub config: TunableConfig,
}

pub async fn run(bind_addr: String, rx: watch::Receiver<String>, config: TunableConfig) -> Result<()> {
    let app_state = AppState { rx, config };
    let app = Router::new()
        .route("/", get(serve_dashboard))
        .route("/events", get(sse_handler))
        .route("/api/config", get(get_config).post(update_config))
        .route("/api/health", get(|| async { "ok" }))
        .with_state(app_state);

    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    tracing::info!("[web] dashboard at http://{}", bind_addr);
    axum::serve(listener, app).await?;
    Ok(())
}

async fn serve_dashboard() -> Html<&'static str> {
    Html(DASHBOARD_HTML)
}

async fn sse_handler(
    State(app_state): State<AppState>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    let stream = async_stream::stream! {
        let mut rx = app_state.rx;
        loop {
            if rx.changed().await.is_ok() {
                let data = rx.borrow().clone();
                yield Ok::<_, Infallible>(Event::default().data(data));
            } else {
                break;
            }
        }
    };
    Sse::new(stream)
}

async fn get_config(State(app_state): State<AppState>) -> axum::Json<ConfigSnapshot> {
    axum::Json(app_state.config.snapshot())
}

#[derive(serde::Deserialize)]
pub struct ConfigUpdate {
    pub chunk_target_rows: Option<u64>,
    pub merge_threshold_rows: Option<u64>,
    pub split_threshold_rows: Option<u64>,
}

async fn update_config(
    State(app_state): State<AppState>,
    JsonExt(req): JsonExt<ConfigUpdate>,
) -> axum::Json<ConfigSnapshot> {
    if let Some(v) = req.chunk_target_rows {
        let v = v.clamp(1_000, 10_000_000);
        app_state.config.chunk_target_rows.store(v, Ordering::Relaxed);
        tracing::info!("[config] chunk_target_rows -> {}", v);
    }
    if let Some(v) = req.merge_threshold_rows {
        let v = v.clamp(1_000, 10_000_000);
        app_state.config.merge_threshold_rows.store(v, Ordering::Relaxed);
        tracing::info!("[config] merge_threshold_rows -> {}", v);
    }
    if let Some(v) = req.split_threshold_rows {
        let v = v.clamp(1_000, 10_000_000);
        app_state.config.split_threshold_rows.store(v, Ordering::Relaxed);
        tracing::info!("[config] split_threshold_rows -> {}", v);
    }
    axum::Json(app_state.config.snapshot())
}
