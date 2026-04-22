use crate::{config::AppConfig, model::SyslogEvent};
use anyhow::{Context, Result};
use chrono::{Datelike, Utc};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use tokio::time::{Duration, sleep};
use tokio_postgres::NoTls;
use tracing::{error, warn};

#[derive(Clone)]
pub struct PgWriter {
    pool: Pool,
    retry_attempts: usize,
    retry_backoff_ms: u64,
}

impl PgWriter {
    pub async fn new(config: &AppConfig) -> Result<Self> {
        let pg_cfg: tokio_postgres::Config = config.db.dsn.parse().context("invalid PostgreSQL DSN")?;
        let manager = Manager::from_config(
            pg_cfg,
            NoTls,
            ManagerConfig {
                recycling_method: RecyclingMethod::Fast,
            },
        );

        let pool = Pool::builder(manager)
            .max_size(config.db.pool_max_size)
            .build()
            .context("failed to build PostgreSQL pool")?;

        let writer = Self {
            pool,
            retry_attempts: config.retry.attempts,
            retry_backoff_ms: config.retry.backoff_ms,
        };

        if config.db.auto_migrate {
            writer.bootstrap().await?;
        }

        Ok(writer)
    }

    async fn bootstrap(&self) -> Result<()> {
        let client = self.pool.get().await.context("pool acquire failed")?;
        let ddl = include_str!("../sql/schema.sql");
        client.batch_execute(ddl).await.context("schema bootstrap failed")?;

        let now = Utc::now();
        let partition_name = format!("syslog_events_{}_{:02}", now.year(), now.month());
        let start = format!("{:04}-{:02}-01", now.year(), now.month());
        let (next_year, next_month) = if now.month() == 12 {
            (now.year() + 1, 1)
        } else {
            (now.year(), now.month() + 1)
        };
        let end = format!("{:04}-{:02}-01", next_year, next_month);

        let partition_sql = format!(
            "CREATE TABLE IF NOT EXISTS {partition_name} PARTITION OF syslog_events FOR VALUES FROM ('{start}') TO ('{end}');"
        );
        client
            .batch_execute(&partition_sql)
            .await
            .context("partition bootstrap failed")?;
        Ok(())
    }

    pub async fn write_batch(&self, events: &[SyslogEvent]) -> Result<usize> {
        if events.is_empty() {
            return Ok(0);
        }

        let mut attempt = 0usize;
        loop {
            attempt += 1;
            match self.write_batch_once(events).await {
                Ok(count) => return Ok(count),
                Err(err) => {
                    if attempt >= self.retry_attempts {
                        error!(attempt, error = %err, "db write permanently failed");
                        return Err(err);
                    }
                    warn!(attempt, error = %err, "db write failed, retrying");
                    sleep(Duration::from_millis(self.retry_backoff_ms * attempt as u64)).await;
                }
            }
        }
    }

    async fn write_batch_once(&self, events: &[SyslogEvent]) -> Result<usize> {
        let mut client = self.pool.get().await.context("pool acquire failed")?;
        let tx = client.transaction().await.context("tx begin failed")?;

        let stmt = tx
            .prepare_cached(
                "INSERT INTO syslog_events (
                    event_ts, host, app_name, procid, msgid,
                    facility, severity, structured_data, message,
                    raw_message, format, valid, received_at, source,
                    protocol, error
                ) VALUES (
                    $1,$2,$3,$4,$5,
                    $6,$7,$8,$9,
                    $10,$11,$12,$13,$14,
                    $15,$16
                )",
            )
            .await
            .context("prepare insert failed")?;

        for event in events {
            let fmt = format!("{:?}", event.format).to_lowercase();
            let protocol = format!("{:?}", event.protocol).to_lowercase();
            tx.execute(
                &stmt,
                &[
                    &event.timestamp,
                    &event.host,
                    &event.app_name,
                    &event.procid,
                    &event.msgid,
                    &event.facility,
                    &event.severity,
                    &event.structured_data,
                    &event.message,
                    &event.raw_message,
                    &fmt,
                    &event.valid,
                    &event.received_at,
                    &event.source,
                    &protocol,
                    &event.error,
                ],
            )
            .await
            .context("insert failed")?;
        }

        tx.commit().await.context("tx commit failed")?;
        Ok(events.len())
    }
}

#[cfg(test)]
mod tests {
    use super::PgWriter;
    use crate::config::AppConfig;

    #[tokio::test]
    #[ignore = "requires TEST_DATABASE_DSN"]
    async fn can_bootstrap_db() {
        let mut cfg = AppConfig::default();
        if let Ok(dsn) = std::env::var("TEST_DATABASE_DSN") {
            cfg.db.dsn = dsn;
        }
        let writer = PgWriter::new(&cfg).await;
        assert!(writer.is_ok());
    }
}
