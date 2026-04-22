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
