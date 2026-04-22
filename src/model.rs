use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::net::SocketAddr;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum InputProtocol {
    Udp,
    Tcp,
    Tls,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawMessage {
    pub payload: String,
    pub source: Option<SocketAddr>,
    pub protocol: InputProtocol,
    pub received_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EventFormat {
    Rfc3164,
    Rfc5424,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyslogEvent {
    pub timestamp: Option<DateTime<Utc>>,
    pub host: Option<String>,
    pub app_name: Option<String>,
    pub procid: Option<String>,
    pub msgid: Option<String>,
    pub facility: Option<i16>,
    pub severity: Option<i16>,
    pub structured_data: Option<Value>,
    pub message: String,
    pub raw_message: String,
    pub format: EventFormat,
    pub valid: bool,
    pub received_at: DateTime<Utc>,
    pub source: Option<String>,
    pub protocol: InputProtocol,
    pub error: Option<String>,
}

impl SyslogEvent {
    pub fn invalid(raw: RawMessage, error: impl Into<String>) -> Self {
        Self {
            timestamp: None,
            host: None,
            app_name: None,
            procid: None,
            msgid: None,
            facility: None,
            severity: None,
            structured_data: None,
            message: raw.payload.clone(),
            raw_message: raw.payload,
            format: EventFormat::Unknown,
            valid: false,
            received_at: raw.received_at,
            source: raw.source.map(|s| s.to_string()),
            protocol: raw.protocol,
            error: Some(error.into()),
        }
    }
}
