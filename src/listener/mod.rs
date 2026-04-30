use crate::{
    config::{AppConfig, QueuePolicy},
    model::{InputProtocol, RawMessage},
};
use anyhow::{Context, Result};
use chrono::Utc;
use std::{net::SocketAddr, sync::Arc};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, BufReader},
    net::{TcpListener, UdpSocket},
    sync::{Semaphore, broadcast, mpsc},
};
use tokio_rustls::{TlsAcceptor, rustls};
use tracing::{error, info, warn};

pub async fn spawn_listeners(
    cfg: Arc<AppConfig>,
    ingress_tx: mpsc::Sender<RawMessage>,
    mut shutdown_rx: broadcast::Receiver<()>,
) -> Result<()> {
    let mut handles = Vec::new();

    if cfg.listeners.enable_udp {
        let tx = ingress_tx.clone();
        let cfg2 = cfg.clone();
        let mut srx = shutdown_rx.resubscribe();
        handles.push(tokio::spawn(async move {
            if let Err(e) = run_udp(cfg2, tx, &mut srx).await {
                error!(error = %e, "udp listener failed");
            }
        }));
    }

    if cfg.listeners.enable_tcp {
        let tx = ingress_tx.clone();
        let cfg2 = cfg.clone();
        let mut srx = shutdown_rx.resubscribe();
        handles.push(tokio::spawn(async move {
            if let Err(e) = run_tcp(cfg2, tx, &mut srx).await {
                error!(error = %e, "tcp listener failed");
            }
        }));
    }

    if cfg.listeners.enable_tls {
        let tx = ingress_tx;
        let cfg2 = cfg.clone();
        handles.push(tokio::spawn(async move {
            if let Err(e) = run_tls(cfg2, tx, &mut shutdown_rx).await {
                error!(error = %e, "tls listener failed");
            }
        }));
    }

    for h in handles {
        let _ = h.await;
    }
    Ok(())
}

async fn run_udp(
    cfg: Arc<AppConfig>,
    ingress_tx: mpsc::Sender<RawMessage>,
    shutdown_rx: &mut broadcast::Receiver<()>,
) -> Result<()> {
    let socket = UdpSocket::bind(&cfg.listeners.udp_addr)
        .await
        .with_context(|| format!("udp bind failed: {}", cfg.listeners.udp_addr))?;
    info!(addr = %cfg.listeners.udp_addr, "udp listener started");

    let mut buf = vec![0u8; cfg.limits.max_message_bytes];
    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("udp listener shutting down");
                break;
            }
            recv = socket.recv_from(&mut buf) => {
                let (size, src) = match recv {
                    Ok(v) => v,
                    Err(e) => {
                        warn!(error = %e, "udp recv failed; continuing");
                        continue;
                    }
                };
                if size == cfg.limits.max_message_bytes {
                    warn!(source = %src, "udp message hit max size limit, possibly truncated");
                }
                let payload = String::from_utf8_lossy(&buf[..size]).trim_end_matches('\0').trim().to_string();
                forward_message(&cfg, &ingress_tx, payload, Some(src), InputProtocol::Udp).await;
            }
        }
    }

    Ok(())
}

async fn run_tcp(
    cfg: Arc<AppConfig>,
    ingress_tx: mpsc::Sender<RawMessage>,
    shutdown_rx: &mut broadcast::Receiver<()>,
) -> Result<()> {
    let listener = TcpListener::bind(&cfg.listeners.tcp_addr)
        .await
        .with_context(|| format!("tcp bind failed: {}", cfg.listeners.tcp_addr))?;
    info!(addr = %cfg.listeners.tcp_addr, "tcp listener started");

    let semaphore = Arc::new(Semaphore::new(cfg.limits.max_tcp_connections));

    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("tcp listener shutting down");
                break;
            }
            accept = listener.accept() => {
                let (stream, addr) = match accept {
                    Ok(v) => v,
                    Err(e) => {
                        warn!(error = %e, "tcp accept failed; continuing");
                        continue;
                    }
                };
                let permit = match semaphore.clone().try_acquire_owned() {
                    Ok(p) => p,
                    Err(_) => {
                        warn!("tcp connection limit reached; dropping new connection");
                        continue;
                    }
                };

                let tx = ingress_tx.clone();
                let cfg2 = cfg.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    if let Err(e) = read_stream_lines(stream, addr, InputProtocol::Tcp, cfg2, tx).await {
                        warn!(error=%e, source=%addr, "tcp session ended with error");
                    }
                });
            }
        }
    }

    Ok(())
}

async fn run_tls(
    cfg: Arc<AppConfig>,
    ingress_tx: mpsc::Sender<RawMessage>,
    shutdown_rx: &mut broadcast::Receiver<()>,
) -> Result<()> {
    let cert_path = cfg
        .listeners
        .tls_cert_path
        .as_deref()
        .context("tls_cert_path must be set when TLS is enabled")?;
    let key_path = cfg
        .listeners
        .tls_key_path
        .as_deref()
        .context("tls_key_path must be set when TLS is enabled")?;

    let certs = load_certs(cert_path)?;
    let key = load_key(key_path)?;

    let tls_cfg = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .context("invalid tls cert/key")?;
    let acceptor = TlsAcceptor::from(Arc::new(tls_cfg));

    let listener = TcpListener::bind(&cfg.listeners.tls_addr)
        .await
        .with_context(|| format!("tls bind failed: {}", cfg.listeners.tls_addr))?;
    info!(addr = %cfg.listeners.tls_addr, "tls listener started");

    let semaphore = Arc::new(Semaphore::new(cfg.limits.max_tcp_connections));

    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("tls listener shutting down");
                break;
            }
            accept = listener.accept() => {
                let (stream, addr) = match accept {
                    Ok(v) => v,
                    Err(e) => {
                        warn!(error = %e, "tls accept failed; continuing");
                        continue;
                    }
                };
                let permit = match semaphore.clone().try_acquire_owned() {
                    Ok(p) => p,
                    Err(_) => {
                        warn!("tls connection limit reached; dropping new connection");
                        continue;
                    }
                };

                let tx = ingress_tx.clone();
                let cfg2 = cfg.clone();
                let acceptor = acceptor.clone();

                tokio::spawn(async move {
                    let _permit = permit;
                    match acceptor.accept(stream).await {
                        Ok(tls_stream) => {
                            if let Err(e) = read_stream_lines(tls_stream, addr, InputProtocol::Tls, cfg2, tx).await {
                                warn!(error=%e, source=%addr, "tls session ended with error");
                            }
                        }
                        Err(e) => warn!(error=%e, source=%addr, "tls handshake failed"),
                    }
                });
            }
        }
    }

    Ok(())
}

async fn read_stream_lines<S: AsyncRead + Unpin>(
    stream: S,
    source: SocketAddr,
    protocol: InputProtocol,
    cfg: Arc<AppConfig>,
    ingress_tx: mpsc::Sender<RawMessage>,
) -> Result<()> {
    let reader = BufReader::new(stream);
    let mut lines = reader.lines();
    while let Some(line) = lines.next_line().await? {
        let msg = line.trim();
        if msg.is_empty() {
            continue;
        }
        if msg.len() > cfg.limits.max_message_bytes {
            warn!(source=%source, size=msg.len(), "message exceeded configured max size");
            continue;
        }
        forward_message(
            &cfg,
            &ingress_tx,
            msg.to_string(),
            Some(source),
            protocol.clone(),
        )
        .await;
    }
    Ok(())
}

async fn forward_message(
    cfg: &AppConfig,
    tx: &mpsc::Sender<RawMessage>,
    payload: String,
    source: Option<SocketAddr>,
    protocol: InputProtocol,
) {
    let event = RawMessage {
        payload,
        source,
        protocol,
        received_at: Utc::now(),
    };

    match cfg.queue.policy {
        QueuePolicy::DropNewest => {
            if tx.try_send(event).is_err() {
                metrics::counter!("syslog_dropped_total", "reason" => "queue_full", "policy" => "drop_newest").increment(1);
            }
        }
        QueuePolicy::Backpressure => {
            if tx.send(event).await.is_err() {
                warn!("failed to enqueue message: receiver closed");
            }
        }
    }
}

fn load_certs(path: &str) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>> {
    let bytes = std::fs::read(path).with_context(|| format!("failed to read cert: {path}"))?;
    let mut cursor = std::io::Cursor::new(bytes);
    let certs = rustls_pemfile::certs(&mut cursor).collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(certs)
}

fn load_key(path: &str) -> Result<rustls::pki_types::PrivateKeyDer<'static>> {
    let bytes = std::fs::read(path).with_context(|| format!("failed to read key: {path}"))?;
    {
        let mut cursor = std::io::Cursor::new(&bytes);
        if let Some(key) = rustls_pemfile::pkcs8_private_keys(&mut cursor).next() {
            return Ok(key?.into());
        }
    }

    let mut cursor = std::io::Cursor::new(&bytes);
    if let Some(key) = rustls_pemfile::rsa_private_keys(&mut cursor).next() {
        return Ok(key?.into());
    }

    anyhow::bail!("no supported private key found in {path}")
}
