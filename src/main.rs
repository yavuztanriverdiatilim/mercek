use anyhow::Result;
use mercek::{config::AppConfig, db, http, listener, pipeline};
use metrics_exporter_prometheus::PrometheusBuilder;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<()> {
    let cfg = Arc::new(AppConfig::load()?);
    init_tracing(&cfg.log.level);

    let recorder = PrometheusBuilder::new().install_recorder()?;

    let db = db::PgWriter::new(&cfg).await?;
    let (ingress_tx, ingress_rx) = mpsc::channel(cfg.queue.ingress_capacity);
    let (db_tx, db_rx) = mpsc::channel(cfg.queue.db_capacity);
    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(8);

    let listeners_cfg = cfg.clone();
    let listeners_tx = ingress_tx.clone();
    let listeners_shutdown = shutdown_rx.resubscribe();
    let listener_task = tokio::spawn(async move {
        if let Err(e) =
            listener::spawn_listeners(listeners_cfg, listeners_tx, listeners_shutdown).await
        {
            error!(error=%e, "listeners exited with error");
        }
    });

    let pipeline_cfg = cfg.clone();
    let pipeline_shutdown = shutdown_rx.resubscribe();
    let pipeline_task = tokio::spawn(async move {
        pipeline::run_processors(
            pipeline_cfg,
            db,
            ingress_rx,
            db_tx,
            db_rx,
            pipeline_shutdown,
        )
        .await;
    });

    let metrics_shutdown = shutdown_rx.resubscribe();
    let metrics_addr = cfg.metrics.bind_addr.clone();
    let metrics_task = tokio::spawn(async move {
        if let Err(e) = http::run_http_server(metrics_addr, recorder, metrics_shutdown).await {
            error!(error=%e, "metrics endpoint failed");
        }
    });

    tokio::signal::ctrl_c().await?;
    info!("shutdown signal received");
    let _ = shutdown_tx.send(());

    let _ = tokio::join!(listener_task, pipeline_task, metrics_task);
    Ok(())
}

fn init_tracing(level: &str) {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(level));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}
