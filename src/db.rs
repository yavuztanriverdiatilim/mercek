use crate::{config::AppConfig, model::SyslogEvent};
use anyhow::{Context, Result};
use chrono::{Datelike, Utc};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use tokio::time::{Duration, sleep};
use tokio_postgres::NoTls;
use tracing::{error, warn};

#[derive(Debug, Clone, PartialEq, Eq)]
struct NginxAccessLog {
    remote_addr: String,
    remote_ident: Option<String>,
    remote_user: Option<String>,
    time_local: Option<String>,
    request_line: Option<String>,
    request_method: Option<String>,
    request_path: Option<String>,
    request_protocol: Option<String>,
    status: Option<i32>,
    body_bytes_sent: Option<i64>,
    http_referer: Option<String>,
    http_user_agent: Option<String>,
    http_x_forwarded_for: Option<String>,
}

enum EventWriteTarget {
    SyslogOnly,
    NginxAccessOnly(Box<NginxAccessLog>),
}

#[derive(Clone)]
pub struct PgWriter {
    pool: Pool,
    retry_attempts: usize,
    retry_backoff_ms: u64,
}

impl PgWriter {
    pub async fn new(config: &AppConfig) -> Result<Self> {
        let pg_cfg: tokio_postgres::Config =
            config.db.dsn.parse().context("invalid PostgreSQL DSN")?;
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
        client
            .batch_execute(ddl)
            .await
            .context("schema bootstrap failed")?;

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
                    sleep(Duration::from_millis(
                        self.retry_backoff_ms * attempt as u64,
                    ))
                    .await;
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

        let nginx_stmt = tx
            .prepare_cached(
                "INSERT INTO nginx_access_logs (
                    event_ts, host, app_name,
                    raw_message, received_at, source,
                    remote_addr, remote_ident, remote_user,
                    time_local, request_line, request_method,
                    request_path, request_protocol, status,
                    body_bytes_sent, http_referer, http_user_agent,
                    http_x_forwarded_for
                ) VALUES (
                    $1,$2,$3,
                    $4,$5,$6,
                    $7,$8,$9,
                    $10,$11,$12,
                    $13,$14,$15,
                    $16,$17,$18,
                    $19
                )",
            )
            .await
            .context("prepare nginx insert failed")?;

        for event in events {
            let fmt = format!("{:?}", event.format).to_lowercase();
            let protocol = format!("{:?}", event.protocol).to_lowercase();
            match event_write_target(event) {
                EventWriteTarget::NginxAccessOnly(nginx) => {
                    tx.execute(
                        &nginx_stmt,
                        &[
                            &event.timestamp,
                            &event.host,
                            &event.app_name,
                            &event.raw_message,
                            &event.received_at,
                            &event.source,
                            &nginx.remote_addr,
                            &nginx.remote_ident,
                            &nginx.remote_user,
                            &nginx.time_local,
                            &nginx.request_line,
                            &nginx.request_method,
                            &nginx.request_path,
                            &nginx.request_protocol,
                            &nginx.status,
                            &nginx.body_bytes_sent,
                            &nginx.http_referer,
                            &nginx.http_user_agent,
                            &nginx.http_x_forwarded_for,
                        ],
                    )
                    .await
                    .context("insert nginx access log failed")?;
                }
                EventWriteTarget::SyslogOnly => {
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
            }
        }

        tx.commit().await.context("tx commit failed")?;
        Ok(events.len())
    }
}

fn parse_nginx_access_log(message: &str) -> Option<NginxAccessLog> {
    let tokens = tokenize_nginx_line(message);
    if tokens.len() < 7 {
        return None;
    }

    let remote_addr = tokens.first()?.clone();
    let remote_ident = none_if_dash(tokens.get(1)?);
    let remote_user = none_if_dash(tokens.get(2)?);
    let time_local = tokens.get(3).and_then(|v| none_if_dash(v));
    let request_line = tokens.get(4).and_then(|v| none_if_dash(v));
    let status = tokens.get(5).and_then(|v| v.parse::<i32>().ok());
    let body_bytes_sent = tokens.get(6).and_then(|v| {
        if v == "-" {
            None
        } else {
            v.parse::<i64>().ok()
        }
    });
    let http_referer = tokens.get(7).and_then(|v| none_if_dash(v));
    let http_user_agent = tokens.get(8).and_then(|v| none_if_dash(v));
    let http_x_forwarded_for = tokens.get(9).and_then(|v| none_if_dash(v));

    let (request_method, request_path, request_protocol) = request_line
        .as_deref()
        .map(split_request_line)
        .unwrap_or((None, None, None));

    Some(NginxAccessLog {
        remote_addr,
        remote_ident,
        remote_user,
        time_local,
        request_line,
        request_method,
        request_path,
        request_protocol,
        status,
        body_bytes_sent,
        http_referer,
        http_user_agent,
        http_x_forwarded_for,
    })
}

fn event_write_target(event: &SyslogEvent) -> EventWriteTarget {
    let is_nginx = event
        .app_name
        .as_deref()
        .map(|v| v.eq_ignore_ascii_case("nginx"))
        .unwrap_or(false);
    if is_nginx && let Some(nginx) = parse_nginx_access_log(&event.message) {
        return EventWriteTarget::NginxAccessOnly(Box::new(nginx));
    }
    EventWriteTarget::SyslogOnly
}

fn tokenize_nginx_line(message: &str) -> Vec<String> {
    let chars: Vec<char> = message.chars().collect();
    let mut i = 0usize;
    let mut out = Vec::new();

    while i < chars.len() {
        while i < chars.len() && chars[i].is_whitespace() {
            i += 1;
        }
        if i >= chars.len() {
            break;
        }

        if chars[i] == '"' {
            i += 1;
            let start = i;
            while i < chars.len() && chars[i] != '"' {
                i += 1;
            }
            out.push(chars[start..i].iter().collect());
            if i < chars.len() && chars[i] == '"' {
                i += 1;
            }
            continue;
        }

        if chars[i] == '[' {
            i += 1;
            let start = i;
            while i < chars.len() && chars[i] != ']' {
                i += 1;
            }
            out.push(chars[start..i].iter().collect());
            if i < chars.len() && chars[i] == ']' {
                i += 1;
            }
            continue;
        }

        let start = i;
        while i < chars.len() && !chars[i].is_whitespace() {
            i += 1;
        }
        out.push(chars[start..i].iter().collect());
    }

    out
}

fn none_if_dash(v: &str) -> Option<String> {
    if v == "-" { None } else { Some(v.to_string()) }
}

fn split_request_line(input: &str) -> (Option<String>, Option<String>, Option<String>) {
    let mut parts = input.split_whitespace();
    (
        parts.next().map(|v| v.to_string()),
        parts.next().map(|v| v.to_string()),
        parts.next().map(|v| v.to_string()),
    )
}

#[cfg(test)]
mod tests {
    use super::{EventWriteTarget, PgWriter};
    use crate::{
        config::AppConfig,
        model::{EventFormat, InputProtocol, SyslogEvent},
    };
    use chrono::Utc;

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

    #[test]
    fn parses_nginx_combined_access_log() {
        let line = r#"127.0.0.1 - - [22/Apr/2026:12:34:56 +0000] "GET /healthz HTTP/1.1" 200 12 "-" "curl/8.5.0""#;
        let parsed = super::parse_nginx_access_log(line).expect("should parse nginx access log");
        assert_eq!(parsed.remote_addr, "127.0.0.1");
        assert_eq!(parsed.remote_ident, None);
        assert_eq!(parsed.remote_user, None);
        assert_eq!(parsed.request_method.as_deref(), Some("GET"));
        assert_eq!(parsed.request_path.as_deref(), Some("/healthz"));
        assert_eq!(parsed.request_protocol.as_deref(), Some("HTTP/1.1"));
        assert_eq!(parsed.status, Some(200));
        assert_eq!(parsed.body_bytes_sent, Some(12));
        assert_eq!(parsed.http_user_agent.as_deref(), Some("curl/8.5.0"));
    }

    #[test]
    fn parses_nginx_access_log_with_forwarded_for() {
        let line = r#"10.0.0.10 - john [22/Apr/2026:12:34:56 +0000] "POST /api/login HTTP/1.1" 401 321 "https://example.com" "Mozilla/5.0" "203.0.113.12""#;
        let parsed = super::parse_nginx_access_log(line).expect("should parse nginx access log");
        assert_eq!(parsed.remote_addr, "10.0.0.10");
        assert_eq!(parsed.remote_user.as_deref(), Some("john"));
        assert_eq!(parsed.request_method.as_deref(), Some("POST"));
        assert_eq!(parsed.status, Some(401));
        assert_eq!(parsed.body_bytes_sent, Some(321));
        assert_eq!(parsed.http_referer.as_deref(), Some("https://example.com"));
        assert_eq!(parsed.http_x_forwarded_for.as_deref(), Some("203.0.113.12"));
    }

    #[test]
    fn routes_nginx_access_log_to_nginx_only() {
        let event = sample_event(
            Some("nginx"),
            r#"127.0.0.1 - - [22/Apr/2026:12:34:56 +0000] "GET /healthz HTTP/1.1" 200 12 "-" "curl/8.5.0""#,
        );
        let target = super::event_write_target(&event);
        assert!(matches!(target, EventWriteTarget::NginxAccessOnly(_)));
    }

    #[test]
    fn routes_non_nginx_to_syslog_only() {
        let event = sample_event(
            Some("sshd"),
            r#"127.0.0.1 - - [22/Apr/2026:12:34:56 +0000] "GET /healthz HTTP/1.1" 200 12 "-" "curl/8.5.0""#,
        );
        let target = super::event_write_target(&event);
        assert!(matches!(target, EventWriteTarget::SyslogOnly));
    }

    #[test]
    fn routes_unparseable_nginx_to_syslog_only() {
        let event = sample_event(Some("nginx"), "invalid-nginx-line");
        let target = super::event_write_target(&event);
        assert!(matches!(target, EventWriteTarget::SyslogOnly));
    }

    fn sample_event(app_name: Option<&str>, message: &str) -> SyslogEvent {
        SyslogEvent {
            timestamp: None,
            host: Some("localhost".to_string()),
            app_name: app_name.map(ToString::to_string),
            procid: None,
            msgid: None,
            facility: None,
            severity: None,
            structured_data: None,
            message: message.to_string(),
            raw_message: message.to_string(),
            format: EventFormat::Rfc3164,
            valid: true,
            received_at: Utc::now(),
            source: None,
            protocol: InputProtocol::Udp,
            error: None,
        }
    }
}
