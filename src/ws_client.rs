//! WebSocket client with connection, authentication, and reconnection logic.
//!
//! Connection flow:
//! 1. Connect to `ws://host:9000/api/user/sync?lang=zh-cn&count=N`
//! 2. Send `Authorization|"token"`
//! 3. Receive auth response (code 1 or 200 = success)
//! 4. Send `ClientInfo|{"name":"fns-cli","type":"ObsidianPlugin","version":"0.1.0"}`
//! 5. Ready for sync operations

#![allow(dead_code)]

use crate::config::{AppConfig, ClientConfig, ServerConfig};
use crate::error::FnsError;
use crate::protocol::{
    Action, ClientAction, Response, SEPARATOR, decode_message, encode_simple_message,
};
use futures_util::{SinkExt, StreamExt};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use tokio::time::{Duration, sleep};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async, tungstenite::protocol::Message,
};
use tracing::{debug, error, info, warn};

/// Maximum WebSocket message size (128MB)
const MAX_MESSAGE_SIZE: usize = 128 * 1024 * 1024;

/// Maximum reconnection delay in seconds
const MAX_RECONNECT_DELAY_SECS: u64 = 300;

/// Delay after max retries exceeded before reset (60 seconds)
const MAX_RETRIES_RESET_DELAY_SECS: u64 = 60;

/// WebSocket stream type
pub type WsStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

/// WebSocket client with auth and reconnection support.
pub struct WsClient {
    /// Server configuration (api, token, vault)
    config: ServerConfig,
    /// Client configuration (reconnect settings)
    client_config: ClientConfig,
    /// Connection count (incremented on each connect)
    connect_count: u32,
    /// Whether currently authenticated
    is_authenticated: bool,
    /// Queue for messages sent before authentication
    msg_queue: VecDeque<String>,
    /// Callback invoked after successful reconnection (not on initial connection)
    on_reconnect_handler: Option<Arc<Mutex<Box<dyn FnMut() + Send + Sync>>>>,
}

impl std::fmt::Debug for WsClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WsClient")
            .field("config", &self.config)
            .field("client_config", &self.client_config)
            .field("connect_count", &self.connect_count)
            .field("is_authenticated", &self.is_authenticated)
            .field("msg_queue_len", &self.msg_queue.len())
            .field("on_reconnect_handler", &self.on_reconnect_handler.is_some())
            .finish()
    }
}

impl WsClient {
    /// Create a new WebSocket client.
    pub fn new(app_config: &AppConfig) -> Self {
        Self {
            config: app_config.server.clone(),
            client_config: app_config.client.clone(),
            connect_count: 0,
            is_authenticated: false,
            msg_queue: VecDeque::new(),
            on_reconnect_handler: None,
        }
    }

    /// Create a new WebSocket client from server config only.
    pub fn with_config(config: ServerConfig, client_config: ClientConfig) -> Self {
        Self {
            config,
            client_config,
            connect_count: 0,
            is_authenticated: false,
            msg_queue: VecDeque::new(),
            on_reconnect_handler: None,
        }
    }

    /// Get the current connection count.
    pub fn connect_count(&self) -> u32 {
        self.connect_count
    }

    /// Check if currently authenticated.
    pub fn is_authenticated(&self) -> bool {
        self.is_authenticated
    }

    /// Register a callback to be invoked after successful reconnection.
    ///
    /// The callback is NOT called on the initial connection.
    /// It is called after each successful reconnect when `connect_count > 1`.
    pub fn on_reconnect<F>(&mut self, handler: F)
    where
        F: FnMut() + Send + Sync + 'static,
    {
        self.on_reconnect_handler = Some(Arc::new(Mutex::new(Box::new(handler))));
    }

    /// Build the WebSocket URL for connection.
    fn build_url(&self) -> String {
        // Convert http/https to ws/wss
        let ws_api = if let Some(rest) = self.config.api.strip_prefix("https://") {
            format!("wss://{}", rest)
        } else if let Some(rest) = self.config.api.strip_prefix("http://") {
            format!("ws://{}", rest)
        } else {
            self.config.api.clone()
        };

        format!(
            "{}/api/user/sync?lang=zh-cn&client=ObsidianPlugin&clientName=fns-cli&clientVersion=0.1.0&count={}",
            ws_api.trim_end_matches('/'),
            self.connect_count
        )
    }

    /// Connect to the WebSocket server.
    ///
    /// Returns the WebSocket stream on success.
    /// Increments `connect_count` on each call.
    pub async fn connect(&mut self) -> Result<WsStream, FnsError> {
        self.is_authenticated = false;
        self.connect_count += 1;

        let url = self.build_url();
        info!(url = %url, "Connecting to WebSocket");

        // Connect with tokio-tungstenite
        // NO client-side ping/pong - server sends pings, client auto-replies with pong
        let (ws_stream, _) = connect_async(&url).await.map_err(|e| FnsError::WebSocket {
            message: format!("Failed to connect to {}: {}", url, e),
        })?;

        info!("WebSocket connected");
        Ok(ws_stream)
    }

    /// Authenticate with the server.
    ///
    /// Sends `Authorization|"token"` and waits for response.
    /// Success codes: 1 or 200.
    pub async fn authenticate(&mut self, ws: &mut WsStream) -> Result<(), FnsError> {
        let token = normalize_auth_token(&self.config.token);

        // Send authorization message
        let auth_msg = encode_simple_message(&Action::Client(ClientAction::Authorization), &token);

        debug!("Sending authorization");
        ws.send(Message::Text(auth_msg.into()))
            .await
            .map_err(|e| FnsError::WebSocket {
                message: format!("Failed to send authorization: {}", e),
            })?;

        // Wait for response
        let response = ws
            .next()
            .await
            .ok_or_else(|| FnsError::WebSocket {
                message: "Connection closed before auth response".to_string(),
            })?
            .map_err(|e| FnsError::WebSocket {
                message: format!("Failed to receive auth response: {}", e),
            })?;

        match response {
            Message::Text(text) => {
                let (action, data) = decode_message(&text).map_err(|e| FnsError::Protocol {
                    message: format!("Failed to decode auth response: {}", e),
                })?;

                debug!(action = ?action, "Received auth response");

                // Check if this is an authorization response
                if action != Action::Client(ClientAction::Authorization) {
                    return Err(FnsError::Protocol {
                        message: format!("Expected Authorization response, got {:?}", action),
                    });
                }

                // Parse response to check code
                let response: Response =
                    serde_json::from_value(data.clone()).map_err(|e| FnsError::Protocol {
                        message: format!("Failed to parse auth response: {}", e),
                    })?;

                // Success codes: 1 or 200 (also any non-zero code <= 200 is success per Python impl)
                if response.code != 0 && response.code <= 200 {
                    self.is_authenticated = true;
                    info!(code = response.code, "Authentication successful");
                    Ok(())
                } else {
                    let msg = response.message.clone();
                    error!(code = response.code, msg = %msg, "Authentication failed");
                    Err(FnsError::WebSocket {
                        message: format!("Authentication failed (code={}): {}", response.code, msg),
                    })
                }
            }
            Message::Close(close_frame) => {
                let reason = close_frame
                    .map(|f| f.to_string())
                    .unwrap_or_else(|| "no reason".to_string());
                Err(FnsError::WebSocket {
                    message: format!("Connection closed during auth: {}", reason),
                })
            }
            _ => Err(FnsError::Protocol {
                message: "Unexpected message type during auth".to_string(),
            }),
        }
    }

    /// Send client info to the server.
    ///
    /// Sends `ClientInfo|{"name":"fns-cli","type":"ObsidianPlugin","version":"0.1.0"}`.
    pub async fn send_client_info(&mut self, ws: &mut WsStream) -> Result<(), FnsError> {
        let client_info = serde_json::json!({
            "name": "fns-cli",
            "type": "ObsidianPlugin",
            "version": "0.1.0"
        });

        let msg = format!(
            "{}{}{}",
            ClientAction::ClientInfo,
            SEPARATOR,
            serde_json::to_string(&client_info)?
        );

        debug!(msg = %msg, "Sending client info");
        ws.send(Message::Text(msg.into()))
            .await
            .map_err(|e| FnsError::WebSocket {
                message: format!("Failed to send client info: {}", e),
            })?;

        info!("Client info sent");
        Ok(())
    }

    /// Send a message, queuing if not yet authenticated.
    ///
    /// If `is_authenticated` is false, the message is added to `msg_queue`.
    /// Otherwise, sends immediately via WebSocket.
    pub async fn send(&mut self, ws: &mut WsStream, msg: String) -> Result<(), FnsError> {
        if !self.is_authenticated {
            debug!(msg = %msg, "Queuing message (not authenticated)");
            self.msg_queue.push_back(msg);
            return Ok(());
        }

        debug!(msg = %msg, "Sending message");
        ws.send(Message::Text(msg.into()))
            .await
            .map_err(|e| FnsError::WebSocket {
                message: format!("Failed to send message: {}", e),
            })
    }

    /// Flush all queued messages.
    ///
    /// Drains `msg_queue` and sends each message via WebSocket.
    /// Should be called after successful authentication.
    pub async fn flush_queue(&mut self, ws: &mut WsStream) -> Result<(), FnsError> {
        let count = self.msg_queue.len();
        if count == 0 {
            return Ok(());
        }

        info!(count = count, "Flushing queued messages");
        while let Some(msg) = self.msg_queue.pop_front() {
            ws.send(Message::Text(msg.into()))
                .await
                .map_err(|e| FnsError::WebSocket {
                    message: format!("Failed to send queued message: {}", e),
                })?;
        }

        info!("Queue flushed");
        Ok(())
    }

    /// Perform the full connection flow: connect → auth → client info.
    ///
    /// Returns the authenticated WebSocket stream.
    pub async fn connect_and_auth(&mut self) -> Result<WsStream, FnsError> {
        let mut ws = self.connect().await?;
        self.authenticate(&mut ws).await?;
        self.send_client_info(&mut ws).await?;
        self.flush_queue(&mut ws).await?;
        Ok(ws)
    }

    /// Calculate reconnection delay with exponential backoff.
    ///
    /// Formula: `min(base * 2^(attempt-1), 300)` seconds.
    fn calculate_delay(&self, attempt: u32) -> Duration {
        let base = self.client_config.reconnect_base_delay;
        let delay_secs = if attempt == 0 {
            base
        } else {
            // Cap the shift at 63 to prevent overflow (max u64 is 2^64 - 1)
            let shift = std::cmp::min(attempt - 1, 63);
            let multiplier = 1u64 << shift;
            // Use saturating multiplication to prevent overflow
            let exponential = base.saturating_mul(multiplier);
            std::cmp::min(exponential, MAX_RECONNECT_DELAY_SECS)
        };
        Duration::from_secs(delay_secs)
    }

    /// Run with automatic reconnection.
    ///
    /// Calls `handler` with the WebSocket stream. On connection loss,
    /// reconnects with exponential backoff and calls `handler` again.
    ///
    /// The handler should return `Ok(())` to continue or `Err` to stop.
    pub async fn run_with_reconnect<F, Fut>(&mut self, mut handler: F) -> Result<(), FnsError>
    where
        F: FnMut(WsStream) -> Fut,
        Fut: std::future::Future<Output = Result<(), FnsError>>,
    {
        let max_retries = self.client_config.reconnect_max_retries;
        let mut retries = 0u32;

        loop {
            match self.connect_and_auth().await {
                Ok(ws) => {
                    // Invoke reconnect callback if this is a reconnection (not initial connection)
                    if self.connect_count > 1 {
                        if let Some(handler) = &self.on_reconnect_handler {
                            if let Ok(mut cb) = handler.lock() {
                                cb();
                            }
                        }
                    }
                    retries = 0;
                    match handler(ws).await {
                        Ok(()) => {
                            // Handler completed normally, exit
                            break Ok(());
                        }
                        Err(FnsError::WebSocket { message }) if message.contains("reconnect") => {
                            // Handler requested reconnect
                            warn!("Handler requested reconnect");
                            continue;
                        }
                        Err(e) => {
                            // Handler error, check if connection-related
                            warn!(error = ?e, "Handler error, attempting reconnect");
                        }
                    }
                }
                Err(e) => {
                    warn!(error = ?e, "Connection failed");
                }
            }

            // Connection lost or handler error - attempt reconnect
            retries += 1;

            if retries > max_retries {
                error!(
                    max_retries = max_retries,
                    "Max reconnect retries exceeded, waiting before reset"
                );
                sleep(Duration::from_secs(MAX_RETRIES_RESET_DELAY_SECS)).await;
                retries = 0;
                continue;
            }

            let delay = self.calculate_delay(retries);
            info!(
                delay_secs = delay.as_secs(),
                attempt = retries,
                max_retries = max_retries,
                "Reconnecting after delay"
            );
            sleep(delay).await;
        }
    }
}

fn normalize_auth_token(token: &str) -> String {
    let trimmed = token.trim();
    let without_bearer = trimmed
        .strip_prefix("Bearer ")
        .or_else(|| trimmed.strip_prefix("bearer "))
        .unwrap_or(trimmed)
        .trim();

    without_bearer
        .strip_prefix('"')
        .and_then(|s| s.strip_suffix('"'))
        .unwrap_or(without_bearer)
        .to_string()
}

/// Connection state for tracking WebSocket status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// Not connected
    Disconnected,
    /// WebSocket connected, awaiting auth
    Connected,
    /// Authenticated and ready
    Authenticated,
    /// Reconnecting after connection loss
    Reconnecting,
}

impl std::fmt::Display for ConnectionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectionState::Disconnected => write!(f, "Disconnected"),
            ConnectionState::Connected => write!(f, "Connected"),
            ConnectionState::Authenticated => write!(f, "Authenticated"),
            ConnectionState::Reconnecting => write!(f, "Reconnecting"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_delay() {
        let mut config = AppConfig::default();
        config.client.reconnect_base_delay = 3;
        let client = WsClient::new(&config);

        // First attempt: base delay
        assert_eq!(client.calculate_delay(1), Duration::from_secs(3));

        // Second attempt: base * 2
        assert_eq!(client.calculate_delay(2), Duration::from_secs(6));

        // Third attempt: base * 4
        assert_eq!(client.calculate_delay(3), Duration::from_secs(12));

        // Large attempt should cap at MAX_RECONNECT_DELAY_SECS (300)
        assert_eq!(client.calculate_delay(100), Duration::from_secs(300));
    }

    #[test]
    fn test_build_url() {
        let mut config = AppConfig::default();
        config.server.api = "https://server.example.com".to_string();
        let mut client = WsClient::new(&config);
        client.connect_count = 1;

        let url = client.build_url();
        assert_eq!(
            url,
            "wss://server.example.com/api/user/sync?lang=zh-cn&client=ObsidianPlugin&clientName=fns-cli&clientVersion=0.1.0&count=1"
        );
    }

    #[test]
    fn test_build_url_http() {
        let mut config = AppConfig::default();
        config.server.api = "http://localhost:8080".to_string();
        let mut client = WsClient::new(&config);
        client.connect_count = 5;

        let url = client.build_url();
        assert_eq!(
            url,
            "ws://localhost:8080/api/user/sync?lang=zh-cn&client=ObsidianPlugin&clientName=fns-cli&clientVersion=0.1.0&count=5"
        );
    }

    #[test]
    fn test_build_url_trailing_slash() {
        let mut config = AppConfig::default();
        config.server.api = "https://server.example.com/".to_string();
        let mut client = WsClient::new(&config);
        client.connect_count = 1;

        let url = client.build_url();
        assert_eq!(
            url,
            "wss://server.example.com/api/user/sync?lang=zh-cn&client=ObsidianPlugin&clientName=fns-cli&clientVersion=0.1.0&count=1"
        );
    }

    #[test]
    fn test_normalize_auth_token() {
        assert_eq!(normalize_auth_token("abc.def.ghi"), "abc.def.ghi");
        assert_eq!(
            normalize_auth_token(" Bearer abc.def.ghi \n"),
            "abc.def.ghi"
        );
        assert_eq!(normalize_auth_token("\"abc.def.ghi\""), "abc.def.ghi");
        assert_eq!(
            normalize_auth_token("Bearer \"abc.def.ghi\""),
            "abc.def.ghi"
        );
    }

    #[test]
    fn test_connection_state_display() {
        assert_eq!(ConnectionState::Disconnected.to_string(), "Disconnected");
        assert_eq!(ConnectionState::Connected.to_string(), "Connected");
        assert_eq!(ConnectionState::Authenticated.to_string(), "Authenticated");
        assert_eq!(ConnectionState::Reconnecting.to_string(), "Reconnecting");
    }

    #[test]
    fn test_message_queued_before_auth() {
        let config = AppConfig::default();
        let client = WsClient::new(&config);
        assert!(!client.is_authenticated());
        assert_eq!(client.msg_queue.len(), 0);
    }

    #[test]
    fn test_queue_initialization() {
        let config = AppConfig::default();
        let client = WsClient::new(&config);
        assert!(client.msg_queue.is_empty());
    }

    #[test]
    fn test_queue_with_config() {
        let config = ServerConfig::default();
        let client_config = ClientConfig::default();
        let client = WsClient::with_config(config, client_config);
        assert!(client.msg_queue.is_empty());
    }

    #[test]
    fn test_on_reconnect_handler_registration() {
        let config = AppConfig::default();
        let mut client = WsClient::new(&config);
        assert!(client.on_reconnect_handler.is_none());

        client.on_reconnect(|| {});
        assert!(client.on_reconnect_handler.is_some());
    }

    #[test]
    fn test_on_reconnect_handler_invocation() {
        use std::sync::atomic::{AtomicU32, Ordering};
        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();

        let config = AppConfig::default();
        let mut client = WsClient::new(&config);
        client.on_reconnect(move || {
            counter_clone.fetch_add(1, Ordering::SeqCst);
        });

        // Simulate initial connection (connect_count = 1, should NOT invoke)
        client.connect_count = 1;
        if let Some(handler) = &client.on_reconnect_handler {
            if client.connect_count > 1 {
                if let Ok(mut cb) = handler.lock() {
                    cb();
                }
            }
        }
        assert_eq!(counter.load(Ordering::SeqCst), 0);

        // Simulate reconnection (connect_count = 2, should invoke)
        client.connect_count = 2;
        if let Some(handler) = &client.on_reconnect_handler {
            if client.connect_count > 1 {
                if let Ok(mut cb) = handler.lock() {
                    cb();
                }
            }
        }
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_on_reconnect_handler_multiple_reconnects() {
        use std::sync::atomic::{AtomicU32, Ordering};
        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();

        let config = AppConfig::default();
        let mut client = WsClient::new(&config);
        client.on_reconnect(move || {
            counter_clone.fetch_add(1, Ordering::SeqCst);
        });

        // Simulate multiple reconnections
        for expected_count in 2..=5 {
            client.connect_count = expected_count;
            if let Some(handler) = &client.on_reconnect_handler {
                if client.connect_count > 1 {
                    if let Ok(mut cb) = handler.lock() {
                        cb();
                    }
                }
            }
        }
        assert_eq!(counter.load(Ordering::SeqCst), 4);
    }
}
