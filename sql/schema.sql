CREATE TABLE IF NOT EXISTS syslog_events (
    id BIGSERIAL,
    event_ts TIMESTAMPTZ NULL,
    host TEXT NULL,
    app_name TEXT NULL,
    procid TEXT NULL,
    msgid TEXT NULL,
    facility SMALLINT NULL,
    severity SMALLINT NULL,
    structured_data JSONB NULL,
    message TEXT NOT NULL,
    raw_message TEXT NOT NULL,
    format TEXT NOT NULL,
    valid BOOLEAN NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    source TEXT NULL,
    protocol TEXT NOT NULL,
    error TEXT NULL,
    PRIMARY KEY (id, received_at)
) PARTITION BY RANGE (received_at);

CREATE TABLE IF NOT EXISTS syslog_events_default PARTITION OF syslog_events DEFAULT;

CREATE INDEX IF NOT EXISTS idx_syslog_events_received_at ON syslog_events (received_at DESC);
CREATE INDEX IF NOT EXISTS idx_syslog_events_host ON syslog_events (host);
CREATE INDEX IF NOT EXISTS idx_syslog_events_severity ON syslog_events (severity);
CREATE INDEX IF NOT EXISTS idx_syslog_events_app_name ON syslog_events (app_name);

CREATE TABLE IF NOT EXISTS nginx_access_logs (
    id BIGSERIAL PRIMARY KEY,
    event_ts TIMESTAMPTZ NULL,
    host TEXT NULL,
    app_name TEXT NULL,
    raw_message TEXT NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    source TEXT NULL,
    remote_addr TEXT NOT NULL,
    remote_ident TEXT NULL,
    remote_user TEXT NULL,
    time_local TEXT NULL,
    request_line TEXT NULL,
    request_method TEXT NULL,
    request_path TEXT NULL,
    request_protocol TEXT NULL,
    status INTEGER NULL,
    body_bytes_sent BIGINT NULL,
    http_referer TEXT NULL,
    http_user_agent TEXT NULL,
    http_x_forwarded_for TEXT NULL
);

CREATE INDEX IF NOT EXISTS idx_nginx_access_logs_received_at ON nginx_access_logs (received_at DESC);
CREATE INDEX IF NOT EXISTS idx_nginx_access_logs_status ON nginx_access_logs (status);
CREATE INDEX IF NOT EXISTS idx_nginx_access_logs_remote_addr ON nginx_access_logs (remote_addr);
