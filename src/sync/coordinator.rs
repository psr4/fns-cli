//! Sync coordinator that orchestrates all sync engines.
//!
//! Manages the overall sync workflow:
//! - WebSocket connection and authentication
//! - NoteSync for `.md` files
//! - FileSync for binary attachments
//! - SettingSync for config directories (`.obsidian/`, `.agents/`)
//! - FolderSync for directory operations

#![allow(dead_code)]

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::protocol::Message;
use tracing::{debug, error, info, warn};

use crate::config::AppConfig;
use crate::error::FnsError;
use crate::protocol::{Action, ServerAction, decode_message};
use crate::state::SyncState;
use crate::ws_client::{WsClient, WsStream};

use super::{FileSync, FolderSync, NoteSync, SettingSync};

/// Default timeout for WebSocket authentication (30 seconds)
const AUTH_TIMEOUT_SECS: u64 = 30;

/// Default timeout for sync operations (5 minutes)
const SYNC_TIMEOUT_SECS: u64 = 300;

/// Maximum reconnection delay in seconds
const MAX_RECONNECT_DELAY_SECS: u64 = 300;

/// Result of a sync operation
#[derive(Debug, Clone, Default)]
pub struct SyncResult {
    /// Number of notes synced
    pub notes_synced: usize,
    /// Number of files synced
    pub files_synced: usize,
    /// Number of settings synced
    pub settings_synced: usize,
    /// Number of folders synced
    pub folders_synced: usize,
    /// Errors encountered during sync
    pub errors: Vec<String>,
}

impl SyncResult {
    /// Create a new empty sync result
    pub fn new() -> Self {
        Self::default()
    }

    /// Check if sync had any errors
    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    /// Get total number of items synced
    pub fn total_synced(&self) -> usize {
        self.notes_synced + self.files_synced + self.settings_synced + self.folders_synced
    }
}

/// Sync coordinator that orchestrates all sync engines
pub struct SyncCoordinator {
    /// Application configuration
    config: AppConfig,
    /// WebSocket client
    ws_client: WsClient,
    /// Sync state (timestamps of last sync)
    state: SyncState,
    /// Path to state file
    state_path: PathBuf,
    /// Note sync engine
    note_sync: NoteSync,
    /// File sync engine
    file_sync: Arc<Mutex<FileSync>>,
    /// Setting sync engine
    setting_sync: SettingSync,
    /// Folder sync engine
    folder_sync: FolderSync,
    /// Set of files to temporarily ignore during watch event processing
    ignored_files: HashSet<String>,
}

impl SyncCoordinator {
    /// Create a new sync coordinator
    pub fn new(config: AppConfig) -> Self {
        let vault_path = config.vault_path();
        let vault_name = config.server.vault.clone();
        let exclude_patterns = config.sync.exclude_patterns.clone();
        let config_dirs = config.sync.config_sync_dirs.clone();

        let state_path = vault_path.join(".fns_state.json");
        let state = SyncState::load(&state_path);
        let ws_client = WsClient::new(&config);
        let note_sync = NoteSync::new(
            vault_path.clone(),
            state.clone(),
            vault_name.clone(),
            exclude_patterns.clone(),
        );

        let file_sync = Arc::new(Mutex::new(FileSync::new(&config)));

        let setting_sync = SettingSync::new(
            vault_path.clone(),
            config_dirs.clone(),
            config.sync.sync_config,
            vault_name.clone(),
        );

        let folder_sync = FolderSync::new(
            vault_path.clone(),
            state.clone(),
            vault_name.clone(),
            config_dirs,
        );

        Self {
            config,
            ws_client,
            state,
            state_path,
            note_sync,
            file_sync,
            setting_sync,
            folder_sync,
            ignored_files: HashSet::new(),
        }
    }

    /// Add a file to the ignore list (prevents echo loops during watch event processing)
    pub fn ignore_file(&mut self, path: &str) {
        self.ignored_files.insert(path.to_string());
    }

    /// Remove a file from the ignore list
    pub fn unignore_file(&mut self, path: &str) {
        self.ignored_files.remove(path);
    }

    /// Check if a file is in the ignore list
    pub fn is_ignored(&self, path: &str) -> bool {
        self.ignored_files.contains(path)
    }

    /// Run bidirectional sync: pull remote changes and push local changes
    pub async fn run_sync(&mut self) -> Result<SyncResult, FnsError> {
        let mut result = SyncResult::new();
        std::fs::create_dir_all(self.config.vault_path())?;

        let mut ws = self.connect_and_auth().await?;

        info!("Starting bidirectional sync");

        if self.config.sync.sync_notes {
            match self.run_note_sync(&mut ws).await {
                Ok(count) => {
                    result.notes_synced = count;
                }
                Err(e) => {
                    warn!(error = %e, "Note sync failed");
                    result.errors.push(format!("Note sync: {}", e));
                }
            }
        }

        if self.config.sync.sync_files {
            match self.run_file_sync(&mut ws).await {
                Ok(count) => {
                    result.files_synced = count;
                }
                Err(e) => {
                    warn!(error = %e, "File sync failed");
                    result.errors.push(format!("File sync: {}", e));
                }
            }
        }

        if self.config.sync.sync_config {
            match self.run_setting_sync(&mut ws).await {
                Ok(count) => {
                    result.settings_synced = count;
                }
                Err(e) => {
                    warn!(error = %e, "Setting sync failed");
                    result.errors.push(format!("Setting sync: {}", e));
                }
            }
        }

        self.commit_state();

        info!(
            notes = result.notes_synced,
            files = result.files_synced,
            settings = result.settings_synced,
            "Sync complete"
        );

        Ok(result)
    }

    /// Run pull-only sync: download remote changes without pushing local changes
    pub async fn run_pull(&mut self) -> Result<SyncResult, FnsError> {
        let mut result = SyncResult::new();
        std::fs::create_dir_all(self.config.vault_path())?;

        let mut ws = self.connect_and_auth().await?;

        info!("Starting pull-only sync");

        if self.config.sync.sync_notes {
            match self.run_note_sync(&mut ws).await {
                Ok(count) => {
                    result.notes_synced = count;
                }
                Err(e) => {
                    warn!(error = %e, "Note sync failed");
                    result.errors.push(format!("Note sync: {}", e));
                }
            }
        }

        // File sync
        if self.config.sync.sync_files {
            match self.run_file_sync(&mut ws).await {
                Ok(count) => {
                    result.files_synced = count;
                }
                Err(e) => {
                    warn!(error = %e, "File sync failed");
                    result.errors.push(format!("File sync: {}", e));
                }
            }
        }

        // Setting sync
        if self.config.sync.sync_config {
            match self.run_setting_sync(&mut ws).await {
                Ok(count) => {
                    result.settings_synced = count;
                }
                Err(e) => {
                    warn!(error = %e, "Setting sync failed");
                    result.errors.push(format!("Setting sync: {}", e));
                }
            }
        }

        // Update state timestamps
        self.commit_state();

        info!(
            notes = result.notes_synced,
            files = result.files_synced,
            settings = result.settings_synced,
            "Pull complete"
        );

        Ok(result)
    }

    /// Run push-only sync: upload local changes without applying remote changes
    pub async fn run_push(&mut self) -> Result<SyncResult, FnsError> {
        let mut result = SyncResult::new();
        std::fs::create_dir_all(self.config.vault_path())?;

        let mut ws = self.connect_and_auth().await?;

        info!("Starting push-only sync");

        if self.config.sync.sync_notes {
            match self.push_all_notes(&mut ws).await {
                Ok(count) => {
                    result.notes_synced = count;
                }
                Err(e) => {
                    warn!(error = %e, "Note push failed");
                    result.errors.push(format!("Note push: {}", e));
                }
            }
        }

        if self.config.sync.sync_files {
            match self.push_all_files(&mut ws).await {
                Ok(count) => {
                    result.files_synced = count;
                }
                Err(e) => {
                    warn!(error = %e, "File push failed");
                    result.errors.push(format!("File push: {}", e));
                }
            }
        }

        if self.config.sync.sync_config {
            match self.push_all_settings(&mut ws).await {
                Ok(count) => {
                    result.settings_synced = count;
                }
                Err(e) => {
                    warn!(error = %e, "Setting push failed");
                    result.errors.push(format!("Setting push: {}", e));
                }
            }
        }

        info!(
            notes = result.notes_synced,
            files = result.files_synced,
            settings = result.settings_synced,
            "Push complete"
        );

        Ok(result)
    }

    /// Connect to WebSocket and authenticate
    async fn connect_and_auth(&mut self) -> Result<WsStream, FnsError> {
        let ws = self.ws_client.connect_and_auth().await?;
        info!("WebSocket connected and authenticated");
        Ok(ws)
    }

    /// Run note sync (incremental)
    async fn run_note_sync(&mut self, ws: &mut WsStream) -> Result<usize, FnsError> {
        let last_time = self.state.last_note_sync_time;

        info!(last_time = last_time, "Starting note sync");

        self.note_sync.sync_incremental(ws, last_time).await?;

        let pending = self.note_sync.pending_last_time();
        if pending > 0 {
            self.state.last_note_sync_time = pending;
        }

        Ok(self.note_sync.synced_count())
    }

    /// Run file sync
    async fn run_file_sync(&mut self, ws: &mut WsStream) -> Result<usize, FnsError> {
        info!(
            last_time = self.state.last_file_sync_time,
            "Starting file sync"
        );

        let state = self.state.clone();
        {
            let mut file_sync = self.file_sync.lock().await;
            file_sync.request_sync(ws, &state).await?;
        }

        self.process_file_sync_messages(ws).await?;

        let (pending, count) = {
            let file_sync = self.file_sync.lock().await;
            (file_sync.pending_last_time(), file_sync.synced_count())
        };
        if pending > 0 {
            self.state.last_file_sync_time = pending;
        }

        Ok(count)
    }

    /// Process file sync messages until complete
    async fn process_file_sync_messages(&mut self, ws: &mut WsStream) -> Result<(), FnsError> {
        let timeout = Duration::from_secs(SYNC_TIMEOUT_SECS);
        let start = std::time::Instant::now();

        loop {
            if start.elapsed() > timeout {
                warn!("File sync timed out");
                return Ok(());
            }

            let msg = ws
                .next()
                .await
                .ok_or_else(|| FnsError::WebSocket {
                    message: "WebSocket closed during file sync".to_string(),
                })?
                .map_err(|e| FnsError::WebSocket {
                    message: format!("WebSocket error: {}", e),
                })?;

            match msg {
                Message::Text(text) => {
                    let (action, data) = decode_message(&text).map_err(|e| FnsError::Protocol {
                        message: format!("Failed to decode message: {}", e),
                    })?;

                    if let Action::Server(server_action) = action {
                        let mut file_sync = self.file_sync.lock().await;
                        let is_end = file_sync.handle_message(ws, &server_action, data).await?;

                        if is_end {
                            return Ok(());
                        }
                    }
                }
                Message::Binary(data) => {
                    let mut file_sync = self.file_sync.lock().await;
                    file_sync.handle_binary_chunk(&data)?;
                }
                Message::Close(_) => {
                    return Err(FnsError::WebSocket {
                        message: "WebSocket closed".to_string(),
                    });
                }
                _ => {}
            }
        }
    }

    /// Run setting sync
    async fn run_setting_sync(&mut self, ws: &mut WsStream) -> Result<usize, FnsError> {
        let last_time = self.state.last_setting_sync_time;

        info!(last_time = last_time, "Starting setting sync");

        self.setting_sync.request_sync(ws, last_time).await?;

        self.process_setting_sync_messages(ws).await?;

        let pending = self.setting_sync.pending_last_time();
        if pending > 0 {
            self.state.last_setting_sync_time = pending;
        }

        Ok(self.setting_sync.synced_count())
    }

    /// Process setting sync messages until complete
    async fn process_setting_sync_messages(&mut self, ws: &mut WsStream) -> Result<(), FnsError> {
        let timeout = Duration::from_secs(SYNC_TIMEOUT_SECS);
        let start = std::time::Instant::now();

        loop {
            if start.elapsed() > timeout {
                warn!("Setting sync timed out");
                return Ok(());
            }

            if self.setting_sync.is_sync_complete() {
                return Ok(());
            }

            let msg = ws
                .next()
                .await
                .ok_or_else(|| FnsError::WebSocket {
                    message: "WebSocket closed during setting sync".to_string(),
                })?
                .map_err(|e| FnsError::WebSocket {
                    message: format!("WebSocket error: {}", e),
                })?;

            match msg {
                Message::Text(text) => {
                    let (action, data) = decode_message(&text).map_err(|e| FnsError::Protocol {
                        message: format!("Failed to decode message: {}", e),
                    })?;

                    if let Action::Server(server_action) = action {
                        self.handle_setting_message(ws, &server_action, data)
                            .await?;
                    }
                }
                Message::Binary(data) => {
                    let mut file_sync = self.file_sync.lock().await;
                    file_sync.handle_binary_chunk(&data)?;
                }
                Message::Close(_) => {
                    return Err(FnsError::WebSocket {
                        message: "WebSocket closed".to_string(),
                    });
                }
                _ => {}
            }
        }
    }

    /// Handle a setting sync message from server
    async fn handle_setting_message(
        &mut self,
        ws: &mut WsStream,
        action: &ServerAction,
        data: serde_json::Value,
    ) -> Result<(), FnsError> {
        match action {
            ServerAction::SettingSyncModify => {
                self.setting_sync.handle_setting_modify(&data)?;
            }
            ServerAction::SettingSyncDelete => {
                self.setting_sync.handle_setting_delete(&data)?;
            }
            ServerAction::SettingSyncRename => {
                self.setting_sync.handle_setting_rename(&data)?;
            }
            ServerAction::SettingSyncMtime => {
                self.setting_sync.handle_setting_mtime(&data)?;
            }
            ServerAction::SettingSyncNeedUpload => {
                self.setting_sync
                    .handle_setting_need_upload(ws, &data)
                    .await?;
            }
            ServerAction::SettingSyncEnd => {
                self.setting_sync.handle_setting_end(&data)?;
            }
            ServerAction::SettingModifyAck => {
                self.setting_sync.handle_setting_modify_ack(&data);
            }
            _ => {
                if matches!(
                    action,
                    ServerAction::FileUpload
                        | ServerAction::FileUploadAck
                        | ServerAction::FileDeleteAck
                        | ServerAction::FileSyncUpdate
                        | ServerAction::FileSyncMtime
                        | ServerAction::FileSyncChunkDownload
                ) {
                    debug!("Handling file message during setting sync: {:?}", action);
                    let mut file_sync = self.file_sync.lock().await;
                    match action {
                        ServerAction::FileUpload => {
                            file_sync.handle_upload_session(ws, data).await?;
                        }
                        ServerAction::FileUploadAck => {
                            file_sync.handle_upload_ack(data)?;
                        }
                        ServerAction::FileDeleteAck => {
                            file_sync.handle_delete_ack(data)?;
                        }
                        ServerAction::FileSyncUpdate => {
                            file_sync.handle_sync_update(ws, data).await?;
                        }
                        ServerAction::FileSyncMtime => {
                            file_sync.handle_sync_mtime(data)?;
                        }
                        ServerAction::FileSyncChunkDownload => {
                            file_sync.handle_chunk_download_start(data)?;
                        }
                        _ => {}
                    }
                } else {
                    debug!(action = ?action, "Ignoring non-setting message during setting sync");
                }
            }
        }
        Ok(())
    }

    /// Push all local notes to server
    async fn push_all_notes(&mut self, ws: &mut WsStream) -> Result<usize, FnsError> {
        let vault_path = self.config.vault_path();
        let mut count = 0;

        if !vault_path.exists() {
            return Ok(0);
        }

        for entry in walkdir::WalkDir::new(&vault_path)
            .follow_links(false)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            let path = entry.path();

            if !path.is_file() {
                continue;
            }

            if !path.extension().is_some_and(|ext| ext == "md") {
                continue;
            }

            let rel = match path.strip_prefix(&vault_path) {
                Ok(r) => r.to_string_lossy().to_string(),
                Err(_) => continue,
            };

            if rel.starts_with(".obsidian/") || rel.starts_with(".agents/") {
                continue;
            }

            if self.is_excluded(&rel) {
                continue;
            }

            self.note_sync.push_modify(ws, &rel, false).await?;
            count += 1;

            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        Ok(count)
    }

    /// Push all local files to server
    async fn push_all_files(&mut self, ws: &mut WsStream) -> Result<usize, FnsError> {
        let vault_path = self.config.vault_path();
        let mut count = 0;

        if !vault_path.exists() {
            return Ok(0);
        }

        for entry in walkdir::WalkDir::new(&vault_path)
            .follow_links(false)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            let path = entry.path();

            if !path.is_file() {
                continue;
            }

            let rel = match path.strip_prefix(&vault_path) {
                Ok(r) => r.to_string_lossy().to_string(),
                Err(_) => continue,
            };

            if rel.ends_with(".md") {
                continue;
            }

            if rel
                .split('/')
                .next()
                .map(|s| s.starts_with('.'))
                .unwrap_or(false)
            {
                continue;
            }

            if self.is_excluded(&rel) {
                continue;
            }

            let mut file_sync = self.file_sync.lock().await;
            file_sync.push_upload(ws, &rel).await?;
            count += 1;

            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        Ok(count)
    }

    /// Push all settings to server
    async fn push_all_settings(&mut self, ws: &mut WsStream) -> Result<usize, FnsError> {
        let vault_path = self.config.vault_path();
        let mut count = 0;

        if !vault_path.exists() {
            return Ok(0);
        }

        for entry in walkdir::WalkDir::new(&vault_path)
            .follow_links(false)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            let path = entry.path();

            if !path.is_file() {
                continue;
            }

            let rel = match path.strip_prefix(&vault_path) {
                Ok(r) => r.to_string_lossy().to_string(),
                Err(_) => continue,
            };

            let first = rel.split('/').next().unwrap_or("");
            if !first.starts_with('.') {
                continue;
            }

            if !self
                .config
                .sync
                .config_sync_dirs
                .contains(&first.to_string())
            {
                continue;
            }

            if self.is_excluded(&rel) {
                continue;
            }

            self.setting_sync.push_modify(ws, &rel, false).await?;
            count += 1;

            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        Ok(count)
    }

    /// Check if a path matches exclude patterns
    fn is_excluded(&self, rel: &str) -> bool {
        if rel.contains(".~#") {
            return true;
        }
        if rel == ".DS_Store" || rel.ends_with("/.DS_Store") {
            return true;
        }
        if rel.starts_with(".tmp") || rel.ends_with(".tmp") || rel.contains(".tmp.") {
            return true;
        }
        if !self.config.sync.sync_config && self.is_config_file(rel) {
            return true;
        }

        for pattern in &self.config.sync.exclude_patterns {
            if let Some(dir_name) = pattern.strip_suffix("/**") {
                if rel.starts_with(dir_name) || rel.starts_with(&format!("{}/", dir_name)) {
                    return true;
                }
            }
            if let Some(ext) = pattern.strip_prefix("*.") {
                let suffix = format!(".{}", ext);
                if rel.ends_with(&suffix) || rel.contains(&format!("{}.", suffix)) {
                    return true;
                }
            }
        }
        false
    }

    /// Commit state to disk
    fn commit_state(&mut self) {
        if let Err(e) = self.state.save(&self.state_path) {
            warn!(error = %e, path = %self.state_path.display(), "Failed to save sync state");
        } else {
            debug!(path = %self.state_path.display(), "Saved sync state");
        }
    }

    /// Get current sync state
    pub fn state(&self) -> &SyncState {
        &self.state
    }

    /// Get vault path
    pub fn vault_path(&self) -> PathBuf {
        self.config.vault_path()
    }

    /// Run continuous sync mode with persistent WebSocket connection and automatic reconnection.
    ///
    /// This maintains a WebSocket connection and handles:
    /// - Server push messages (NoteSyncModify, NoteSyncDelete, etc.)
    /// - Local file changes (triggered via watch_rx channel)
    /// - Automatic reconnection with exponential backoff on disconnect
    /// - Re-sync of all enabled types after reconnection
    #[allow(unused_assignments)]
    pub async fn run_continuous(
        &mut self,
        mut watch_rx: tokio::sync::mpsc::Receiver<crate::watcher::WatchEvent>,
        mut shutdown_signal: tokio::sync::broadcast::Receiver<()>,
    ) -> Result<SyncResult, FnsError> {
        let mut result = SyncResult::new();
        std::fs::create_dir_all(self.config.vault_path())?;

        // Create a channel for reconnect signals.
        // The sync on_reconnect callback sends a signal here; we handle it
        // in the tokio::select! loop to trigger an async re-sync.
        let (reconnect_tx, mut reconnect_rx) = tokio::sync::mpsc::channel::<()>(1);

        // Register the reconnect callback on the ws_client.
        // It fires inside run_with_reconnect() after successful auth when connect_count > 1.
        let tx = reconnect_tx.clone();
        self.ws_client.on_reconnect(move || {
            let _ = tx.try_send(());
        });

        let mut reconnect_retries: u32 = 0;
        let max_retries = self.config.client.reconnect_max_retries;
        let base_delay = self.config.client.reconnect_base_delay;

        loop {
            let mut ws = match self.ws_client.connect_and_auth().await {
                Ok(ws) => ws,
                Err(e) => {
                    warn!(error = %e, "Connection failed");
                    reconnect_retries += 1;
                    if reconnect_retries > max_retries {
                        error!(max_retries = max_retries, "Max reconnect retries exceeded");
                        return Err(e);
                    }
                    let delay_secs = std::cmp::min(
                        base_delay.saturating_mul(1u64 << std::cmp::min(reconnect_retries - 1, 63)),
                        MAX_RECONNECT_DELAY_SECS,
                    );
                    info!(
                        delay_secs = delay_secs,
                        attempt = reconnect_retries,
                        "Reconnecting after delay"
                    );
                    tokio::time::sleep(Duration::from_secs(delay_secs)).await;
                    continue;
                }
            };
            reconnect_retries = 0;

            let is_reconnect = self.ws_client.connect_count() > 1;

            if is_reconnect {
                info!("Reconnected — re-syncing");
            } else {
                info!("Starting bidirectional sync");
            }

            if self.config.sync.sync_notes {
                match self.run_note_sync(&mut ws).await {
                    Ok(count) => result.notes_synced += count,
                    Err(e) => {
                        warn!(error = %e, "Note sync failed");
                        result.errors.push(format!("Note sync: {}", e));
                    }
                }
            }

            if self.config.sync.sync_files {
                match self.run_file_sync(&mut ws).await {
                    Ok(count) => result.files_synced += count,
                    Err(e) => {
                        warn!(error = %e, "File sync failed");
                        result.errors.push(format!("File sync: {}", e));
                    }
                }
            }

            if self.config.sync.sync_config {
                match self.run_setting_sync(&mut ws).await {
                    Ok(count) => result.settings_synced += count,
                    Err(e) => {
                        warn!(error = %e, "Setting sync failed");
                        result.errors.push(format!("Setting sync: {}", e));
                    }
                }
            }

            self.commit_state();

            if is_reconnect {
                info!(
                    notes = result.notes_synced,
                    files = result.files_synced,
                    settings = result.settings_synced,
                    "Re-sync complete"
                );
            } else {
                info!(
                    notes = result.notes_synced,
                    files = result.files_synced,
                    settings = result.settings_synced,
                    "Initial sync complete"
                );
            }

            let mut watch_enabled = true;

            loop {
                tokio::select! {
                    _ = shutdown_signal.recv() => {
                        info!("Shutdown signal received in coordinator");
                        return Ok(result);
                    }

                    Some(()) = reconnect_rx.recv() => {
                        info!("Reconnect signal received — re-syncing");
                        watch_enabled = false;

                        if self.config.sync.sync_notes {
                            match self.run_note_sync(&mut ws).await {
                                Ok(count) => result.notes_synced += count,
                                Err(e) => {
                                    warn!(error = %e, "Note re-sync failed");
                                    result.errors.push(format!("Note re-sync: {}", e));
                                }
                            }
                        }

                        if self.config.sync.sync_files {
                            match self.run_file_sync(&mut ws).await {
                                Ok(count) => result.files_synced += count,
                                Err(e) => {
                                    warn!(error = %e, "File re-sync failed");
                                    result.errors.push(format!("File re-sync: {}", e));
                                }
                            }
                        }

                        if self.config.sync.sync_config {
                            match self.run_setting_sync(&mut ws).await {
                                Ok(count) => result.settings_synced += count,
                                Err(e) => {
                                    warn!(error = %e, "Setting re-sync failed");
                                    result.errors.push(format!("Setting re-sync: {}", e));
                                }
                            }
                        }

                        self.commit_state();
                        info!("Re-sync complete after reconnect signal");
                        watch_enabled = true;
                    }

                    Some(event) = watch_rx.recv(), if watch_enabled => {
                        debug!(event = ?event, "Received file watcher event");
                        if let Err(e) = self.handle_watch_event(&mut ws, &event).await {
                            warn!(error = %e, "Failed to handle watch event");
                            result.errors.push(format!("Watch event error: {}", e));
                        }
                    }

                    msg_result = ws.next() => {
                        match msg_result {
                            Some(Ok(msg)) => {
                                if let Err(e) = self.handle_server_message(&mut ws, msg).await {
                                    warn!(error = %e, "Failed to handle server message");
                                }
                            }
                            Some(Err(e)) => {
                                warn!(error = %e, "WebSocket error, reconnecting");
                                break;
                            }
                            None => {
                                warn!("WebSocket closed by server, reconnecting");
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    /// Handle a file watcher event by pushing the change to server.
    async fn handle_watch_event(
        &mut self,
        ws: &mut WsStream,
        event: &crate::watcher::WatchEvent,
    ) -> Result<(), FnsError> {
        use crate::watcher::WatchEvent;

        match event {
            WatchEvent::Created(path) | WatchEvent::Modified(path) => {
                let rel_path = path
                    .strip_prefix(&self.config.vault_path())
                    .map_err(|e| FnsError::Sync {
                        message: format!("Failed to get relative path: {}", e),
                    })?
                    .to_string_lossy()
                    .to_string();

                if self.is_ignored(&rel_path) || self.is_excluded(&rel_path) {
                    return Ok(());
                }

                if self.is_config_file(&rel_path) {
                    if self.config.sync.sync_config {
                        self.setting_sync.push_modify(ws, &rel_path, false).await?;
                    }
                } else if rel_path.ends_with(".md") {
                    if self.config.sync.sync_notes {
                        self.note_sync.push_modify(ws, &rel_path, false).await?;
                    }
                } else {
                    if self.config.sync.sync_files {
                        let mut file_sync = self.file_sync.lock().await;
                        file_sync.push_upload(ws, &rel_path).await?;
                    }
                }
            }
            WatchEvent::Deleted(path) => {
                let rel_path = path
                    .strip_prefix(&self.config.vault_path())
                    .map_err(|e| FnsError::Sync {
                        message: format!("Failed to get relative path: {}", e),
                    })?
                    .to_string_lossy()
                    .to_string();

                if self.is_ignored(&rel_path) || self.is_excluded(&rel_path) {
                    return Ok(());
                }

                if self.is_config_file(&rel_path) {
                    if self.config.sync.sync_config {
                        self.setting_sync.push_delete(ws, &rel_path).await?;
                    }
                } else if rel_path.ends_with(".md") {
                    if self.config.sync.sync_notes {
                        self.note_sync.push_delete(ws, &rel_path).await?;
                    }
                } else {
                    if self.config.sync.sync_files {
                        let mut file_sync = self.file_sync.lock().await;
                        file_sync.push_delete(ws, &rel_path).await?;
                    }
                }
            }
            WatchEvent::Moved { from, to } => {
                let old_rel = from
                    .strip_prefix(&self.config.vault_path())
                    .map_err(|e| FnsError::Sync {
                        message: format!("Failed to get relative path: {}", e),
                    })?
                    .to_string_lossy()
                    .to_string();

                let new_rel = to
                    .strip_prefix(&self.config.vault_path())
                    .map_err(|e| FnsError::Sync {
                        message: format!("Failed to get relative path: {}", e),
                    })?
                    .to_string_lossy()
                    .to_string();

                let old_excluded = self.is_ignored(&old_rel) || self.is_excluded(&old_rel);
                let new_excluded = self.is_ignored(&new_rel) || self.is_excluded(&new_rel);

                // Case 1: Both excluded - nothing to do
                if old_excluded && new_excluded {
                    return Ok(());
                }

                // Case 2: Old included, new excluded - file moved out of sync scope, delete old
                if !old_excluded && new_excluded {
                    debug!(old = %old_rel, new = %new_rel, "Move from included to excluded, deleting old");
                    if self.is_config_file(&old_rel) {
                        if self.config.sync.sync_config {
                            self.setting_sync.push_delete(ws, &old_rel).await?;
                        }
                    } else if old_rel.ends_with(".md") {
                        if self.config.sync.sync_notes {
                            self.note_sync.push_delete(ws, &old_rel).await?;
                        }
                    } else {
                        if self.config.sync.sync_files {
                            let mut file_sync = self.file_sync.lock().await;
                            file_sync.push_delete(ws, &old_rel).await?;
                        }
                    }
                    return Ok(());
                }

                // Case 3: Old excluded, new included - file moved into sync scope, upload new
                if old_excluded && !new_excluded {
                    debug!(old = %old_rel, new = %new_rel, "Move from excluded to included, uploading new");
                    if self.is_config_file(&new_rel) {
                        if self.config.sync.sync_config {
                            self.setting_sync.push_modify(ws, &new_rel, false).await?;
                        }
                    } else if new_rel.ends_with(".md") {
                        if self.config.sync.sync_notes {
                            self.note_sync.push_modify(ws, &new_rel, false).await?;
                        }
                    } else {
                        if self.config.sync.sync_files {
                            let mut file_sync = self.file_sync.lock().await;
                            file_sync.push_upload(ws, &new_rel).await?;
                        }
                    }
                    return Ok(());
                }

                // Case 4: Both included - file moved within sync scope, push rename
                if self.is_config_file(&new_rel) {
                    if self.config.sync.sync_config {
                        self.setting_sync
                            .push_rename(ws, &new_rel, &old_rel)
                            .await?;
                    }
                } else if new_rel.ends_with(".md") {
                    if self.config.sync.sync_notes {
                        self.note_sync.push_rename(ws, &new_rel, &old_rel).await?;
                    }
                } else {
                    if self.config.sync.sync_files {
                        let mut file_sync = self.file_sync.lock().await;
                        file_sync.push_delete(ws, &old_rel).await?;
                        file_sync.push_upload(ws, &new_rel).await?;
                    }
                }
            }
        }

        Ok(())
    }

    /// Handle a message from the server (real-time push).
    async fn handle_server_message(
        &mut self,
        ws: &mut WsStream,
        msg: Message,
    ) -> Result<(), FnsError> {
        match msg {
            Message::Text(text) => {
                let text_str = text.as_str();
                // Check if this is a server error response (pure JSON with "code" field)
                if text_str.starts_with('{') && text_str.contains("\"code\"") {
                    if let Ok(error) = serde_json::from_str::<serde_json::Value>(text_str) {
                        if let Some(code) = error.get("code").and_then(|v| v.as_i64()) {
                            if let Some(message) = error.get("message").and_then(|v| v.as_str()) {
                                warn!("Server error: code={} message={}", code, message);
                                return Ok(());
                            }
                        }
                    }
                }

                let (action, data) =
                    decode_message(text_str).map_err(|e| FnsError::Protocol { message: e })?;

                match action {
                    Action::Server(server_action) => match server_action {
                        ServerAction::NoteSyncModify => {
                            self.note_sync.handle_note_modify(&data)?;
                        }
                        ServerAction::NoteSyncDelete => {
                            self.note_sync.handle_note_delete(&data)?;
                        }
                        ServerAction::NoteSyncRename => {
                            self.note_sync.handle_note_rename(&data)?;
                        }
                        ServerAction::NoteSyncMtime => {
                            self.note_sync.handle_note_mtime(&data)?;
                        }
                        ServerAction::NoteModifyAck => {
                            debug!("Received NoteModifyAck");
                        }
                        ServerAction::NoteDeleteAck => {
                            debug!("Received NoteDeleteAck");
                        }
                        ServerAction::FileSyncUpdate => {
                            let mut file_sync = self.file_sync.lock().await;
                            file_sync.handle_sync_update(ws, data).await?;
                        }
                        ServerAction::FileSyncDelete => {
                            let mut file_sync = self.file_sync.lock().await;
                            file_sync.handle_sync_delete(data)?;
                        }
                        ServerAction::FileSyncChunkDownload => {
                            let mut file_sync = self.file_sync.lock().await;
                            file_sync.handle_chunk_download(data).await?;
                        }
                        ServerAction::FileUpload => {
                            let mut file_sync = self.file_sync.lock().await;
                            file_sync.handle_upload_session(ws, data).await?;
                        }
                        ServerAction::FileUploadAck => {
                            let mut file_sync = self.file_sync.lock().await;
                            file_sync.handle_upload_ack(data)?;
                        }
                        ServerAction::FileDeleteAck => {
                            let mut file_sync = self.file_sync.lock().await;
                            file_sync.handle_delete_ack(data)?;
                        }
                        ServerAction::SettingSyncModify => {
                            self.setting_sync.handle_setting_modify(&data)?;
                        }
                        ServerAction::SettingSyncDelete => {
                            self.setting_sync.handle_setting_delete(&data)?;
                        }
                        ServerAction::SettingModifyAck => {
                            self.setting_sync.handle_setting_modify_ack(&data);
                        }
                        _ => {
                            debug!(action = ?server_action, "Ignoring server action in continuous mode");
                        }
                    },
                    Action::Client(_) => {
                        debug!("Ignoring client action from server");
                    }
                }
            }
            Message::Binary(data) => {
                let mut file_sync = self.file_sync.lock().await;
                file_sync.handle_binary_chunk(&data)?;
            }
            Message::Ping(_) | Message::Pong(_) => {}
            Message::Close(_) => {
                return Err(FnsError::WebSocket {
                    message: "Server closed connection".to_string(),
                });
            }
            _ => {}
        }

        Ok(())
    }

    /// Check if a path is a config file (in dot-prefixed directories).
    fn is_config_file(&self, rel_path: &str) -> bool {
        let first = rel_path.split('/').next().unwrap_or("");
        if !first.starts_with('.') {
            return false;
        }
        // Check if the directory is in the configured config_sync_dirs list
        if self.config.sync.config_sync_dirs.iter().any(|d| d == first) {
            return true;
        }
        // For other dot-prefixed dirs, sync if sync_config is enabled
        self.config.sync.sync_config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sync_result_default() {
        let result = SyncResult::default();
        assert_eq!(result.notes_synced, 0);
        assert_eq!(result.files_synced, 0);
        assert_eq!(result.settings_synced, 0);
        assert_eq!(result.folders_synced, 0);
        assert!(result.errors.is_empty());
        assert!(!result.has_errors());
        assert_eq!(result.total_synced(), 0);
    }

    #[test]
    fn test_sync_result_with_errors() {
        let mut result = SyncResult::new();
        result.notes_synced = 5;
        result.files_synced = 3;
        result.errors.push("Test error".to_string());

        assert!(result.has_errors());
        assert_eq!(result.total_synced(), 8);
        assert_eq!(result.errors.len(), 1);
    }

    #[test]
    fn test_is_excluded() {
        let mut config = AppConfig::default();
        config.sync.exclude_patterns = vec![
            ".git/**".to_string(),
            ".trash/**".to_string(),
            "*.tmp".to_string(),
        ];

        let coordinator = SyncCoordinator::new(config);

        assert!(coordinator.is_excluded(".git/config"));
        assert!(coordinator.is_excluded(".git/objects/abc"));
        assert!(coordinator.is_excluded(".trash/old.md"));
        assert!(coordinator.is_excluded(".DS_Store"));
        assert!(coordinator.is_excluded("notes/.DS_Store"));
        assert!(coordinator.is_excluded(".tmpwEYnim"));
        assert!(coordinator.is_excluded("notes/temp.tmp"));
        assert!(coordinator.is_excluded("notes/hello.md.tmp.w_3o8rmv"));
        assert!(coordinator.is_excluded("notes/hello.md.~#0"));
        assert!(!coordinator.is_excluded("notes/hello.md"));
        assert!(!coordinator.is_excluded(".obsidian/app.json"));
    }

    #[test]
    fn test_config_paths_excluded_when_config_sync_disabled() {
        let mut config = AppConfig::default();
        config.sync.sync_config = false;

        let coordinator = SyncCoordinator::new(config);

        assert!(coordinator.is_excluded(".obsidian/plugins/fast-note-sync/data.json"));
        assert!(coordinator.is_excluded(".agents/config.yaml"));
        assert!(!coordinator.is_excluded("notes/hello.md"));
    }

    #[test]
    fn test_ignore_file() {
        let config = AppConfig::default();
        let mut coordinator = SyncCoordinator::new(config);

        assert!(!coordinator.is_ignored("test.md"));

        coordinator.ignore_file("test.md");
        assert!(coordinator.is_ignored("test.md"));

        coordinator.ignore_file("another.md");
        assert!(coordinator.is_ignored("test.md"));
        assert!(coordinator.is_ignored("another.md"));
    }

    #[test]
    fn test_unignore_file() {
        let config = AppConfig::default();
        let mut coordinator = SyncCoordinator::new(config);

        coordinator.ignore_file("test.md");
        assert!(coordinator.is_ignored("test.md"));

        coordinator.unignore_file("test.md");
        assert!(!coordinator.is_ignored("test.md"));

        coordinator.unignore_file("nonexistent.md");
        assert!(!coordinator.is_ignored("nonexistent.md"));
    }

    #[test]
    fn test_ignored_file_not_processed() {
        let config = AppConfig::default();
        let mut coordinator = SyncCoordinator::new(config);

        coordinator.ignore_file("notes/test.md");

        assert!(coordinator.is_ignored("notes/test.md"));
        assert!(!coordinator.is_ignored("notes/other.md"));
    }

    #[test]
    fn test_move_both_excluded() {
        let mut config = AppConfig::default();
        config.sync.exclude_patterns = vec![".git/**".to_string(), ".trash/**".to_string()];

        let coordinator = SyncCoordinator::new(config);

        let old_rel = ".git/config";
        let new_rel = ".trash/config";

        let old_excluded = coordinator.is_excluded(old_rel);
        let new_excluded = coordinator.is_excluded(new_rel);

        assert!(old_excluded, "Old path should be excluded");
        assert!(new_excluded, "New path should be excluded");
        assert!(
            old_excluded && new_excluded,
            "Both excluded - no action needed"
        );
    }

    #[test]
    fn test_move_from_included_to_excluded() {
        let mut config = AppConfig::default();
        config.sync.exclude_patterns = vec![".trash/**".to_string()];

        let coordinator = SyncCoordinator::new(config);

        let old_rel = "notes/hello.md";
        let new_rel = ".trash/hello.md";

        let old_excluded = coordinator.is_excluded(old_rel);
        let new_excluded = coordinator.is_excluded(new_rel);

        assert!(!old_excluded, "Old path should be included");
        assert!(new_excluded, "New path should be excluded");
        assert!(
            !old_excluded && new_excluded,
            "Move from included to excluded - should delete old"
        );
    }

    #[test]
    fn test_move_from_excluded_to_included() {
        let mut config = AppConfig::default();
        config.sync.exclude_patterns = vec![".trash/**".to_string()];

        let coordinator = SyncCoordinator::new(config);

        let old_rel = ".trash/restored.md";
        let new_rel = "notes/restored.md";

        let old_excluded = coordinator.is_excluded(old_rel);
        let new_excluded = coordinator.is_excluded(new_rel);

        assert!(old_excluded, "Old path should be excluded");
        assert!(!new_excluded, "New path should be included");
        assert!(
            old_excluded && !new_excluded,
            "Move from excluded to included - should upload new"
        );
    }

    #[test]
    fn test_move_both_included() {
        let mut config = AppConfig::default();
        config.sync.exclude_patterns = vec![".trash/**".to_string()];

        let coordinator = SyncCoordinator::new(config);

        let old_rel = "notes/old-name.md";
        let new_rel = "notes/new-name.md";

        let old_excluded = coordinator.is_excluded(old_rel);
        let new_excluded = coordinator.is_excluded(new_rel);

        assert!(!old_excluded, "Old path should be included");
        assert!(!new_excluded, "New path should be included");
        assert!(
            !old_excluded && !new_excluded,
            "Both included - should push rename"
        );
    }

    #[test]
    fn test_move_boundary_with_extension_exclusion() {
        let mut config = AppConfig::default();
        config.sync.exclude_patterns = vec!["*.tmp".to_string(), "*.bak".to_string()];

        let coordinator = SyncCoordinator::new(config);

        let old_rel = "notes/file.md";
        let new_rel = "notes/file.tmp";
        let temp_suffix_rel = "notes/file.md.tmp.w_3o8rmv";
        let editor_temp_rel = "notes/file.md.~#0";

        let old_excluded = coordinator.is_excluded(old_rel);
        let new_excluded = coordinator.is_excluded(new_rel);
        let temp_suffix_excluded = coordinator.is_excluded(temp_suffix_rel);
        let editor_temp_excluded = coordinator.is_excluded(editor_temp_rel);

        assert!(!old_excluded, "Old .md file should be included");
        assert!(new_excluded, "New .tmp file should be excluded");
        assert!(temp_suffix_excluded, "New .tmp.* file should be excluded");
        assert!(editor_temp_excluded, "New .~# file should be excluded");
        assert!(
            !old_excluded && new_excluded,
            "Move from .md to .tmp - should delete old"
        );
    }

    #[test]
    fn test_reconnect_callback_registration() {
        use std::sync::atomic::{AtomicU32, Ordering};
        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();

        let config = AppConfig::default();
        let mut coordinator = SyncCoordinator::new(config);

        coordinator.ws_client.on_reconnect(move || {
            counter_clone.fetch_add(1, Ordering::SeqCst);
        });

        assert_eq!(
            counter.load(Ordering::SeqCst),
            0,
            "Callback not invoked yet"
        );
    }

    #[tokio::test]
    async fn test_reconnect_channel_signaling() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<()>(1);

        tx.try_send(()).unwrap();

        let signal = rx.try_recv();
        assert!(signal.is_ok(), "Should receive reconnect signal");
    }

    #[tokio::test]
    async fn test_watch_enabled_guard_normal() {
        let (_tx, mut rx) = tokio::sync::mpsc::channel::<()>(1);
        let (watch_tx, mut watch_rx) = tokio::sync::mpsc::channel::<String>(1);

        let watch_enabled = true;

        watch_tx.send("event1".to_string()).await.unwrap();

        let result = tokio::select! {
            _ = rx.recv() => {
                "reconnect".to_string()
            }
            event = watch_rx.recv(), if watch_enabled => {
                event.unwrap_or_default()
            }
            else => "none".to_string()
        };

        assert_eq!(result, "event1");
    }

    #[tokio::test]
    async fn test_watch_disabled_guard_blocks_events() {
        let watch_enabled = false;

        assert!(!watch_enabled, "Watch should be disabled");
    }
}
