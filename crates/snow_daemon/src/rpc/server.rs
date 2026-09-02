use super::*;
use snow_mcp::protocol::frame::{
    FrameRead, MAX_JSON_RPC_RESPONSE_BYTES, RESULT_TOO_LARGE_CODE, read_frame,
    request_too_large_data, result_too_large_data,
};

#[derive(Clone)]
pub struct JsonRpcServer {
    state: Arc<DaemonState>,
    endpoint: IpcEndpoint,
    shutdown: Arc<Notify>,
    drain_timeout: Duration,
    /// When `Some`, the daemon shuts itself down after this much time with no
    /// connected clients. `None` disables idle shutdown (pinned daemon).
    idle_timeout: Option<Duration>,
}

const CONNECTION_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// Default idle window after which an otherwise-unused daemon self-terminates.
/// Lazily auto-spawned daemons would otherwise linger until reboot.
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(30 * 60);

impl JsonRpcServer {
    pub fn new(state: Arc<DaemonState>, endpoint: impl Into<IpcEndpoint>) -> Self {
        Self {
            state,
            endpoint: endpoint.into(),
            shutdown: Arc::new(Notify::new()),
            drain_timeout: CONNECTION_DRAIN_TIMEOUT,
            idle_timeout: Some(DEFAULT_IDLE_TIMEOUT),
        }
    }

    /// Configure idle self-shutdown. `Some(d)` shuts the daemon down after `d`
    /// of inactivity with no connected clients; `None` disables it.
    pub fn with_idle_timeout(mut self, idle_timeout: Option<Duration>) -> Self {
        self.idle_timeout = idle_timeout;
        self
    }

    #[cfg(test)]
    pub(crate) fn with_drain_timeout(mut self, drain_timeout: Duration) -> Self {
        self.drain_timeout = drain_timeout;
        self
    }

    pub async fn serve(self) -> Result<()> {
        self.serve_until(shutdown_signal()).await
    }

    pub async fn serve_until<F>(self, shutdown: F) -> Result<()>
    where
        F: Future<Output = Result<(), std::io::Error>>,
    {
        prepare_listener(&self.endpoint).await?;
        let listener = self
            .endpoint
            .listen()
            .with_context(|| format!("failed to bind {}", self.endpoint))?;
        eprintln!("snow_daemon: json-rpc listening on {}", self.endpoint);
        let tracker = TaskTracker::new();
        tokio::pin!(shutdown);

        // Activity tracking for idle self-shutdown: a count of in-flight
        // connections and the instant of the most recent connect/disconnect.
        let active = Arc::new(AtomicUsize::new(0));
        let last_activity = Arc::new(Mutex::new(Instant::now()));
        let idle_monitor = self.idle_timeout.map(|timeout| {
            let active = Arc::clone(&active);
            let last_activity = Arc::clone(&last_activity);
            let shutdown = Arc::clone(&self.shutdown);
            tokio::task::spawn_local(idle_monitor(timeout, active, last_activity, shutdown))
        });

        loop {
            tokio::select! {
                accept = listener.accept() => {
                    let stream = accept?;
                    let state = Arc::clone(&self.state);
                    let shutdown = Arc::clone(&self.shutdown);
                    let active = Arc::clone(&active);
                    let last_activity = Arc::clone(&last_activity);
                    active.fetch_add(1, Ordering::SeqCst);
                    touch(&last_activity);
                    tracker.spawn_local(async move {
                        if let Err(err) = handle_connection(stream, state, shutdown).await {
                            eprintln!("json-rpc connection error: {err:#}");
                        }
                        active.fetch_sub(1, Ordering::SeqCst);
                        touch(&last_activity);
                    });
                }
                _ = &mut shutdown => {
                    break;
                }
                _ = self.shutdown.notified() => {
                    break;
                }
            }
        }

        if let Some(handle) = idle_monitor {
            handle.abort();
        }
        tracker.close();
        let _ = cleanup_listener(&self.endpoint).await;
        let _ = tokio::time::timeout(self.drain_timeout, tracker.wait()).await;

        Ok(())
    }
}

/// Record activity (a connect or disconnect) by resetting the idle clock.
fn touch(last_activity: &Mutex<Instant>) {
    if let Ok(mut guard) = last_activity.lock() {
        *guard = Instant::now();
    }
}

/// Trigger `shutdown` once there have been zero connected clients for at least
/// `timeout`. Polls on a fraction of the timeout so a short configured window
/// (e.g. in tests) is honored promptly without busy-spinning a long one.
async fn idle_monitor(
    timeout: Duration,
    active: Arc<AtomicUsize>,
    last_activity: Arc<Mutex<Instant>>,
    shutdown: Arc<Notify>,
) {
    let tick = (timeout / 4).clamp(Duration::from_millis(50), Duration::from_secs(60));
    loop {
        tokio::time::sleep(tick).await;
        if active.load(Ordering::SeqCst) != 0 {
            continue;
        }
        let idle_for = last_activity
            .lock()
            .map(|guard| guard.elapsed())
            .unwrap_or_default();
        if idle_for >= timeout {
            shutdown.notify_one();
            break;
        }
    }
}

async fn prepare_listener(endpoint: &IpcEndpoint) -> std::io::Result<()> {
    if let Some(path) = endpoint.filesystem_path() {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent).await?;
        }
        let _ = fs::remove_file(path).await;
    }
    Ok(())
}

async fn cleanup_listener(endpoint: &IpcEndpoint) -> std::io::Result<()> {
    if let Some(path) = endpoint.filesystem_path() {
        let _ = fs::remove_file(path).await;
    }
    Ok(())
}

async fn shutdown_signal() -> Result<(), std::io::Error> {
    #[cfg(unix)]
    {
        let mut terminate = signal::unix::signal(signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = signal::ctrl_c() => result,
            _ = terminate.recv() => Ok(()),
        }
    }

    #[cfg(not(unix))]
    {
        signal::ctrl_c().await
    }
}

pub(crate) async fn handle_connection<S>(
    stream: S,
    state: Arc<DaemonState>,
    shutdown: Arc<Notify>,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (reader, mut writer) = tokio::io::split(stream);
    let mut reader = BufReader::new(reader);
    let mut frame = Vec::new();

    loop {
        let frame_status = match read_frame(&mut reader, &mut frame).await {
            Ok(status) => status,
            Err(err) if is_peer_disconnect(&err) => break,
            Err(err) => return Err(err.into()),
        };
        if frame_status == FrameRead::Eof {
            break;
        }
        let close_after_response = frame_status == FrameRead::TooLarge;

        let mut should_shutdown = false;
        let response = match frame_status {
            FrameRead::TooLarge => JsonRpcResponse::error(
                None,
                RESULT_TOO_LARGE_CODE,
                "request frame exceeds maximum size",
                Some(request_too_large_data()),
            ),
            FrameRead::Frame => match serde_json::from_slice::<JsonRpcRequest>(&frame) {
                Ok(request) => {
                    should_shutdown =
                        RpcMethod::from_method(&request.method) == RpcMethod::Shutdown;
                    dispatch(request, &state).await
                }
                Err(err) => JsonRpcResponse::error(
                    None,
                    -32700,
                    "parse error",
                    Some(json!({ "details": err.to_string() })),
                ),
            },
            FrameRead::Eof => unreachable!("EOF was handled before response construction"),
        };

        let payload = bounded_payload(response)?;
        if let Err(err) = writer.write_all(&payload).await {
            if is_peer_disconnect(&err) {
                break;
            }
            return Err(err.into());
        }
        if let Err(err) = writer.write_all(b"\n").await {
            if is_peer_disconnect(&err) {
                break;
            }
            return Err(err.into());
        }
        writer.flush().await?;

        if close_after_response {
            break;
        }

        if should_shutdown {
            shutdown.notify_one();
            break;
        }
    }

    Ok(())
}

pub(crate) fn bounded_payload(response: JsonRpcResponse) -> Result<Vec<u8>> {
    let payload = serde_json::to_vec(&response)?;
    if payload.len() <= MAX_JSON_RPC_RESPONSE_BYTES {
        return Ok(payload);
    }

    let fallback = JsonRpcResponse::error(
        response.id,
        RESULT_TOO_LARGE_CODE,
        "result exceeds maximum size",
        Some(result_too_large_data(payload.len())),
    );
    let fallback_payload = serde_json::to_vec(&fallback)?;
    if fallback_payload.len() <= MAX_JSON_RPC_RESPONSE_BYTES {
        return Ok(fallback_payload);
    }

    Ok(serde_json::to_vec(&JsonRpcResponse::error(
        None,
        RESULT_TOO_LARGE_CODE,
        "result exceeds maximum size",
        Some(result_too_large_data(payload.len())),
    ))?)
}

fn is_peer_disconnect(err: &std::io::Error) -> bool {
    matches!(
        err.kind(),
        ErrorKind::BrokenPipe
            | ErrorKind::ConnectionReset
            | ErrorKind::ConnectionAborted
            | ErrorKind::UnexpectedEof
    )
}
