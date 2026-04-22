use crate::model::{EventFormat, RawMessage, SyslogEvent};
use chrono::{Datelike, NaiveDateTime, TimeZone, Utc};
use serde_json::{Map, Value};

pub fn parse(raw: RawMessage) -> SyslogEvent {
    let msg = raw.payload.trim();
    let (pri, rest) = match parse_pri(msg) {
        Some(v) => v,
        None => return SyslogEvent::invalid(raw, "missing PRI"),
    };

    let facility = Some((pri / 8) as i16);
    let severity = Some((pri % 8) as i16);

    match detect_format(rest) {
        EventFormat::Rfc5424 => parse_rfc5424(raw, rest, facility, severity),
        EventFormat::Rfc3164 => parse_rfc3164(raw, rest, facility, severity),
        EventFormat::Unknown => SyslogEvent::invalid(raw, "unknown syslog format"),
    }
}

fn parse_pri(msg: &str) -> Option<(u8, &str)> {
    let end = msg.find('>')?;
    if !msg.starts_with('<') || end <= 1 {
        return None;
    }
    let pri: u8 = msg[1..end].parse().ok()?;
    Some((pri, msg[end + 1..].trim_start()))
}

fn detect_format(rest: &str) -> EventFormat {
    if let Some((first, _)) = rest.split_once(' ') {
        if first.parse::<u16>().is_ok() {
            return EventFormat::Rfc5424;
        }
    }

    if rest.len() >= 16 && is_rfc3164_ts(&rest[0..15.min(rest.len())]) {
        return EventFormat::Rfc3164;
    }

    EventFormat::Unknown
}

fn is_rfc3164_ts(s: &str) -> bool {
    let trimmed = s.trim();
    if trimmed.len() < 12 {
        return false;
    }
    matches!(
        &trimmed[0..3.min(trimmed.len())],
        "Jan" | "Feb" | "Mar" | "Apr" | "May" | "Jun" | "Jul" | "Aug" | "Sep" | "Oct" | "Nov" | "Dec"
    )
}

fn parse_rfc3164(raw: RawMessage, rest: &str, facility: Option<i16>, severity: Option<i16>) -> SyslogEvent {
    let parts: Vec<&str> = rest.split_whitespace().collect();
    if parts.len() < 4 {
        return SyslogEvent::invalid(raw, "invalid RFC3164 payload");
    }

    let ts_token = parts[0..3].join(" ");
    let current_year = Utc::now().year();
    let dt = NaiveDateTime::parse_from_str(&format!("{} {}", current_year, ts_token), "%Y %b %e %H:%M:%S")
        .ok()
        .map(|d| Utc.from_utc_datetime(&d));

    let host = parts.get(3).map(|s| s.to_string());
    let msg_start = nth_index(rest, ' ', 4).unwrap_or(rest.len());
    let msg_body = rest[msg_start..].trim().to_string();

    let (app_name, procid, message) = split_tag_and_message(&msg_body);

    SyslogEvent {
        timestamp: dt,
        host,
        app_name,
        procid,
        msgid: None,
        facility,
        severity,
        structured_data: None,
        message,
        raw_message: raw.payload,
        format: EventFormat::Rfc3164,
        valid: true,
        received_at: raw.received_at,
        source: raw.source.map(|s| s.to_string()),
        protocol: raw.protocol,
        error: None,
    }
}

fn parse_rfc5424(raw: RawMessage, rest: &str, facility: Option<i16>, severity: Option<i16>) -> SyslogEvent {
    let mut iter = rest.splitn(8, ' ');
    let _version = iter.next();
    let ts = iter.next();
    let host = iter.next();
    let app = iter.next();
    let procid = iter.next();
    let msgid = iter.next();
    let sd_and_msg = iter.next();

    if ts.is_none() || host.is_none() || app.is_none() || procid.is_none() || msgid.is_none() || sd_and_msg.is_none() {
        return SyslogEvent::invalid(raw, "invalid RFC5424 header");
    }

    let timestamp = ts
        .and_then(|t| if t == "-" { None } else { Some(t) })
        .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
        .map(|t| t.with_timezone(&Utc));

    let (structured_data, message) = parse_structured_data_and_message(sd_and_msg.unwrap_or_default());

    SyslogEvent {
        timestamp,
        host: normalize_dash(host),
        app_name: normalize_dash(app),
        procid: normalize_dash(procid),
        msgid: normalize_dash(msgid),
        facility,
        severity,
        structured_data,
        message,
        raw_message: raw.payload,
        format: EventFormat::Rfc5424,
        valid: true,
        received_at: raw.received_at,
        source: raw.source.map(|s| s.to_string()),
        protocol: raw.protocol,
        error: None,
    }
}

fn normalize_dash(v: Option<&str>) -> Option<String> {
    match v {
        Some("-") | None => None,
        Some(s) => Some(s.to_string()),
    }
}

fn parse_structured_data_and_message(input: &str) -> (Option<Value>, String) {
    let input = input.trim();
    if input == "-" {
        return (None, String::new());
    }

    if !input.starts_with('[') {
        return (None, input.to_string());
    }

    let mut depth = 0usize;
    let mut end = None;
    for (i, c) in input.char_indices() {
        match c {
            '[' => depth += 1,
            ']' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    end = Some(i + 1);
                    break;
                }
            }
            _ => {}
        }
    }

    if let Some(end_idx) = end {
        let sd_raw = &input[..end_idx];
        let msg = input[end_idx..].trim_start().to_string();
        return (Some(parse_sd_to_json(sd_raw)), msg);
    }

    (None, input.to_string())
}

fn parse_sd_to_json(sd: &str) -> Value {
    let mut sections = Vec::new();
    let mut current = String::new();
    let mut depth = 0usize;

    for c in sd.chars() {
        if c == '[' {
            depth += 1;
        }
        if depth > 0 {
            current.push(c);
        }
        if c == ']' {
            depth = depth.saturating_sub(1);
            if depth == 0 && !current.is_empty() {
                sections.push(current.clone());
                current.clear();
            }
        }
    }

    let mut arr = Vec::new();
    for sec in sections {
        let inner = sec.trim_matches(&['[', ']'][..]);
        let mut split = inner.split_whitespace();
        let id = split.next().unwrap_or_default().to_string();
        let mut map = Map::new();
        map.insert("id".to_string(), Value::String(id));

        for kv in split {
            if let Some((k, v)) = kv.split_once('=') {
                map.insert(k.to_string(), Value::String(v.trim_matches('"').to_string()));
            }
        }
        arr.push(Value::Object(map));
    }

    Value::Array(arr)
}

fn split_tag_and_message(msg: &str) -> (Option<String>, Option<String>, String) {
    if let Some((tag, body)) = msg.split_once(':') {
        let tag = tag.trim();
        if let Some((app, pid)) = tag.split_once('[') {
            return (
                Some(app.trim().to_string()),
                Some(pid.trim_end_matches(']').trim().to_string()),
                body.trim().to_string(),
            );
        }
        return (Some(tag.to_string()), None, body.trim().to_string());
    }

    (None, None, msg.to_string())
}

fn nth_index(s: &str, ch: char, n: usize) -> Option<usize> {
    let mut count = 0usize;
    for (idx, c) in s.char_indices() {
        if c == ch {
            count += 1;
            if count == n {
                return Some(idx);
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use crate::model::{EventFormat, InputProtocol, RawMessage};
    use chrono::Utc;

    #[test]
    fn parses_rfc5424() {
        let raw = RawMessage {
            payload: "<34>1 2024-03-01T12:00:00Z myhost app 123 ID47 [exampleSDID@32473 iut=3 eventSource=Application] hello".into(),
            source: None,
            protocol: InputProtocol::Tcp,
            received_at: Utc::now(),
        };
        let e = super::parse(raw);
        assert!(e.valid);
        assert!(matches!(e.format, EventFormat::Rfc5424));
        assert_eq!(e.host.as_deref(), Some("myhost"));
    }

    #[test]
    fn parses_rfc3164() {
        let raw = RawMessage {
            payload: "<13>Feb  5 17:32:18 host app[123]: test message".into(),
            source: None,
            protocol: InputProtocol::Udp,
            received_at: Utc::now(),
        };
        let e = super::parse(raw);
        assert!(e.valid);
        assert!(matches!(e.format, EventFormat::Rfc3164));
        assert_eq!(e.host.as_deref(), Some("host"));
        assert_eq!(e.app_name.as_deref(), Some("app"));
    }

    #[test]
    fn marks_invalid_messages() {
        let raw = RawMessage {
            payload: "no-pri payload".into(),
            source: None,
            protocol: InputProtocol::Udp,
            received_at: Utc::now(),
        };
        let e = super::parse(raw);
        assert!(!e.valid);
        assert!(matches!(e.format, EventFormat::Unknown));
    }
}
