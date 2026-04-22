use crate::{
    config::{AppConfig, QueuePolicy},
    db::PgWriter,
    model::{RawMessage, SyslogEvent},
    parser,
};
use anyhow::Result;
use std::sync::Arc;
use tokio::{
    sync::{broadcast, mpsc},
    time::{Duration, Instant, interval},
};
use tracing::{info, warn};

pub async fn run_processors(
    cfg: Arc<AppConfig>,
    db: PgWriter,
    ingress_rx: mpsc::Receiver<RawMessage>,
    db_tx: mpsc::Sender<SyslogEvent>,
    db_rx: mpsc::Receiver<SyslogEvent>,
    shutdown_rx: broadcast::Receiver<()>,
) {
    let parser_task = tokio::spawn(run_parser_loop(
        cfg.clone(),
        ingress_rx,
        db_tx,
        shutdown_rx.resubscribe(),
    ));
    let writer_task = tokio::spawn(run_writer_loop(cfg, db, db_rx, shutdown_rx.resubscribe()));

    let _ = tokio::join!(parser_task, writer_task);
}

async fn run_parser_loop(
    cfg: Arc<AppConfig>,
    mut ingress_rx: mpsc::Receiver<RawMessage>,
    db_tx: mpsc::Sender<SyslogEvent>,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("parser loop shutting down");
                break;
            }
            maybe = ingress_rx.recv() => {
                match maybe {
                    Some(raw) => {
                        metrics::counter!("syslog_received_total").increment(1);
                        let parsed = parser::parse(raw);
                        if parsed.valid {
                            metrics::counter!("syslog_parsed_total", "status" => "ok").increment(1);
                        } else {
                            metrics::counter!("syslog_parsed_total", "status" => "invalid").increment(1);
                        }

                        if cfg.queue.policy == QueuePolicy::DropNewest {
                            if db_tx.try_send(parsed).is_err() {
                                metrics::counter!("syslog_dropped_total", "reason" => "db_queue_full", "policy" => "drop_newest").increment(1);
                            }
                        } else if db_tx.send(parsed).await.is_err() {
                            warn!("db queue closed");
                            break;
                        }
                    }
                    None => break,
                }
            }
        }
    }
}

async fn run_writer_loop(
    cfg: Arc<AppConfig>,
    db: PgWriter,
    mut db_rx: mpsc::Receiver<SyslogEvent>,
    mut shutdown_rx: broadcast::Receiver<()>,
) -> Result<()> {
    let mut ticker = interval(Duration::from_millis(cfg.batching.flush_interval_ms));
    let mut batch = Vec::with_capacity(cfg.batching.batch_size);

    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("writer loop shutting down, draining queue");
                while let Ok(ev) = db_rx.try_recv() {
                    batch.push(ev);
                    if batch.len() >= cfg.batching.batch_size {
                        flush_batch(&db, &mut batch).await;
                    }
                }
                flush_batch(&db, &mut batch).await;
                break;
            }
            _ = ticker.tick() => {
                flush_batch(&db, &mut batch).await;
            }
            maybe = db_rx.recv() => {
                match maybe {
                    Some(event) => {
                        let enqueue_started = Instant::now();
                        batch.push(event);
                        if batch.len() >= cfg.batching.batch_size {
                            flush_batch(&db, &mut batch).await;
                        }
                        metrics::histogram!("syslog_pipeline_enqueue_latency_ms").record(enqueue_started.elapsed().as_millis() as f64);
                    }
                    None => {
                        flush_batch(&db, &mut batch).await;
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

async fn flush_batch(db: &PgWriter, batch: &mut Vec<SyslogEvent>) {
    if batch.is_empty() {
        return;
    }

    let started = Instant::now();
    match db.write_batch(batch).await {
        Ok(inserted) => {
            metrics::counter!("syslog_db_written_total").increment(inserted as u64);
            metrics::histogram!("syslog_db_flush_latency_ms").record(started.elapsed().as_millis() as f64);
        }
        Err(err) => {
            warn!(error=%err, count=batch.len(), "failed to persist batch; writing dead-letter logs");
            for event in batch.iter() {
                warn!(target: "dead_letter", raw=%event.raw_message, error=?event.error, "dead-letter event");
            }
            metrics::counter!("syslog_dead_letter_total").increment(batch.len() as u64);
        }
    }
    batch.clear();
}
