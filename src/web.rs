//! Web server: HTML dashboard, SSE state stream, and tunables REST API.

use anyhow::Result;
use axum::extract::{Json as JsonExt, Query, State};
use axum::response::sse::{Event, Sse};
use axum::response::Html;
use axum::routing::{get, post};
use axum::Router;
use std::convert::Infallible;
use std::sync::atomic::Ordering;
use tokio::sync::watch;

use crate::state::{
    ConfigSnapshot, CreateTableCommand, CreateTableLaunchConfig, LogBuffer, SinkCommand,
    SinkLaunchConfig, TunableConfig,
};
use crate::tables_inspect;

static DASHBOARD_HTML: &str = include_str!("dashboard.html");

#[derive(Clone)]
pub struct AppState {
    pub rx: watch::Receiver<String>,
    pub config: TunableConfig,
    pub sink_cmd_tx: Option<tokio::sync::mpsc::Sender<SinkCommand>>,
    pub create_cmd_tx: Option<tokio::sync::mpsc::Sender<CreateTableCommand>>,
    pub log_buffer: Option<LogBuffer>,
}

pub async fn run(
    bind_addr: String,
    rx: watch::Receiver<String>,
    config: TunableConfig,
    sink_cmd_tx: Option<tokio::sync::mpsc::Sender<SinkCommand>>,
    create_cmd_tx: Option<tokio::sync::mpsc::Sender<CreateTableCommand>>,
    log_buffer: Option<LogBuffer>,
) -> Result<()> {
    let app_state = AppState {
        rx,
        config,
        sink_cmd_tx,
        create_cmd_tx,
        log_buffer,
    };
    let app = Router::new()
        .route("/", get(serve_dashboard))
        .route("/events", get(sse_handler))
        .route("/api/config", get(get_config).post(update_config))
        .route("/api/sink/start", post(start_sink))
        .route("/api/sink/stop", post(stop_sink))
        .route("/api/create-table/start", post(start_create_table))
        .route("/api/create-table/stop", post(stop_create_table))
        .route("/api/tables/inspect", get(inspect_table))
        .route("/api/logs", get(logs_stream))
        .route("/api/logs/snapshot", get(logs_snapshot))
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

async fn start_sink(
    State(app_state): State<AppState>,
    JsonExt(cfg): JsonExt<SinkLaunchConfig>,
) -> Result<axum::Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let tx = app_state.sink_cmd_tx.as_ref().ok_or((
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        "sink controller not enabled".to_string(),
    ))?;
    let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
    tx.send(SinkCommand::Start(cfg, resp_tx))
        .await
        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("send failed: {}", e)))?;
    match resp_rx.await {
        Ok(Ok(())) => Ok(axum::Json(serde_json::json!({"ok": true}))),
        Ok(Err(e)) => Err((axum::http::StatusCode::CONFLICT, e)),
        Err(e) => Err((axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("recv failed: {}", e))),
    }
}

async fn stop_sink(
    State(app_state): State<AppState>,
) -> Result<axum::Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let tx = app_state.sink_cmd_tx.as_ref().ok_or((
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        "sink controller not enabled".to_string(),
    ))?;
    let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
    tx.send(SinkCommand::Stop(resp_tx))
        .await
        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("send failed: {}", e)))?;
    match resp_rx.await {
        Ok(Ok(())) => Ok(axum::Json(serde_json::json!({"ok": true}))),
        Ok(Err(e)) => Err((axum::http::StatusCode::BAD_REQUEST, e)),
        Err(e) => Err((axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("recv failed: {}", e))),
    }
}

async fn start_create_table(
    State(app_state): State<AppState>,
    JsonExt(cfg): JsonExt<CreateTableLaunchConfig>,
) -> Result<axum::Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let tx = app_state.create_cmd_tx.as_ref().ok_or((
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        "create-table controller not enabled".to_string(),
    ))?;
    let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
    tx.send(CreateTableCommand::Start(cfg, resp_tx))
        .await
        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("send failed: {}", e)))?;
    match resp_rx.await {
        Ok(Ok(())) => Ok(axum::Json(serde_json::json!({"ok": true}))),
        Ok(Err(e)) => Err((axum::http::StatusCode::CONFLICT, e)),
        Err(e) => Err((axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("recv failed: {}", e))),
    }
}

async fn stop_create_table(
    State(app_state): State<AppState>,
) -> Result<axum::Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let tx = app_state.create_cmd_tx.as_ref().ok_or((
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        "create-table controller not enabled".to_string(),
    ))?;
    let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
    tx.send(CreateTableCommand::Stop(resp_tx))
        .await
        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("send failed: {}", e)))?;
    match resp_rx.await {
        Ok(Ok(())) => Ok(axum::Json(serde_json::json!({"ok": true}))),
        Ok(Err(e)) => Err((axum::http::StatusCode::BAD_REQUEST, e)),
        Err(e) => Err((axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("recv failed: {}", e))),
    }
}

#[derive(serde::Deserialize)]
pub struct InspectQuery {
    pub uri: String,
    #[serde(default = "default_auth")]
    pub azure_auth: String,
}

fn default_auth() -> String {
    "auto".to_string()
}

async fn inspect_table(
    Query(q): Query<InspectQuery>,
) -> Result<axum::Json<tables_inspect::InspectResult>, (axum::http::StatusCode, String)> {
    if q.uri.is_empty() {
        return Err((axum::http::StatusCode::BAD_REQUEST, "uri is required".into()));
    }
    match tables_inspect::inspect(&q.uri, &q.azure_auth).await {
        Ok(r) => Ok(axum::Json(r)),
        Err(e) => Err((
            axum::http::StatusCode::BAD_REQUEST,
            format!("{:#}", e),
        )),
    }
}

async fn logs_snapshot(
    State(app_state): State<AppState>,
) -> axum::Json<serde_json::Value> {
    if let Some(buf) = app_state.log_buffer {
        let lines = buf.snapshot();
        axum::Json(serde_json::json!({"lines": lines}))
    } else {
        axum::Json(serde_json::json!({"lines": []}))
    }
}

async fn logs_stream(
    State(app_state): State<AppState>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    let buf = app_state.log_buffer;
    let stream = async_stream::stream! {
        let mut last_seq: u64 = 0;
        if let Some(b) = buf.as_ref() {
            // Send initial snapshot.
            let snap = b.snapshot();
            if let Some(last) = snap.last() {
                last_seq = last.seq;
            }
            if let Ok(json) = serde_json::to_string(&snap) {
                yield Ok::<_, Infallible>(Event::default().data(json));
            }
            loop {
                b.wait_change().await;
                let lines = b.since(last_seq);
                if let Some(last) = lines.last() {
                    last_seq = last.seq;
                }
                if !lines.is_empty() {
                    if let Ok(json) = serde_json::to_string(&lines) {
                        yield Ok::<_, Infallible>(Event::default().data(json));
                    }
                }
            }
        }
    };
    Sse::new(stream)
}

