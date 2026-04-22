use anyhow::Result;
use axum::{Router, extract::State, response::IntoResponse, routing::get};
use metrics_exporter_prometheus::PrometheusHandle;
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::broadcast;
use tracing::info;

#[derive(Clone)]
struct MetricsState {
    handle: PrometheusHandle,
}

pub async fn run_http_server(
    addr: String,
    handle: PrometheusHandle,
    mut shutdown_rx: broadcast::Receiver<()>,
) -> Result<()> {
    let state = Arc::new(MetricsState { handle });
    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
        .with_state(state);

    let addr: SocketAddr = addr.parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!(address=%addr, "http observability endpoint started");

    let server = axum::serve(listener, app);
    tokio::select! {
        result = server => {
            result?;
        }
        _ = shutdown_rx.recv() => {
            info!("http observability endpoint shutting down");
        }
    }
    Ok(())
}

async fn healthz() -> impl IntoResponse {
    "ok"
}

async fn readyz() -> impl IntoResponse {
    "ready"
}

async fn metrics(State(state): State<Arc<MetricsState>>) -> impl IntoResponse {
    state.handle.render()
}
