//! Note sync protocol: NoteSync incremental pull + NoteModify/NoteDelete push.
//!
//! Syncs `.md` files in the vault (excluding config directories like .obsidian, .agents).
//! Echo suppression prevents feedback loops when receiving our own changes.

#![allow(dead_code)]

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::protocol::Message;
use tracing::{debug, info, warn};

use crate::error::FnsError;
use crate::hash::{hash_content, hash_path};
use crate::protocol::{
    Action, ClientAction, NoteDeleteRequest, NoteModifyRequest, NoteSyncCheck, NoteSyncRequest,
    ServerAction, decode_message, encode_message,
};
use crate::state::SyncState;
use crate::ws_client::WsStream;

/// Marker for deleted files in echo cache
const DELETED_MARKER: &str = "__deleted__";

/// Note sync engine.
///
/// Handles incremental sync, full sync, and server→client message processing
/// with echo suppression to prevent feedback loops.
pub struct NoteSync {
    /// Local vault directory path.
    pub vault_path: PathBuf,
    /// Sync state (timestamps of last sync).
    pub state: SyncState,
    /// Vault name for protocol messages.
    vault: String,
    /// Exclude patterns (glob-style) for file enumeration.
    exclude_patterns: Vec<String>,
    /// Content hashes of outbound operations for echo suppression.
    /// Maps path → hash (or "__deleted__" for deletions).
    echo_hashes: HashMap<String, String>,
    /// Sync completion state.
    sync_complete: bool,
    /// Expected modify count from NoteSyncEnd.
    expected_modify: usize,
    /// Expected delete count from NoteSyncEnd.
    expected_delete: usize,
    /// Expected upload count from NoteSyncEnd.
    expected_upload: usize,
    /// Received modify count.
    received_modify: usize,
    /// Received delete count.
    received_delete: usize,
    /// Received upload count.
    received_upload: usize,
    /// Got NoteSyncEnd message.
    got_end: bool,
    /// Pending last sync time to commit.
    pending_last_time: i64,
}

async fn read_stable_note_content(path: &Path) -> Result<String, std::io::Error> {
    const MAX_ATTEMPTS: usize = 5;
    const STABLE_DELAY: Duration = Duration::from_millis(150);

    let mut last_len: Option<u64> = None;
    let mut last_modified: Option<SystemTime> = None;
    let mut last_content = String::new();

    for attempt in 0..MAX_ATTEMPTS {
        let content = fs::read_to_string(path)?;
        let metadata = fs::metadata(path)?;
        let modified = metadata.modified().ok();
        let is_stable = Some(metadata.len()) == last_len && modified == last_modified;
        let is_last_attempt = attempt + 1 == MAX_ATTEMPTS;

        if is_stable && (!content.is_empty() || is_last_attempt) {
            return Ok(content);
        }

        last_len = Some(metadata.len());
        last_modified = modified;
        last_content = content;

        if !is_last_attempt {
            tokio::time::sleep(STABLE_DELAY).await;
        }
    }

    Ok(last_content)
}

impl NoteSync {
    /// Create a new NoteSync instance.
    pub fn new(
        vault_path: PathBuf,
        state: SyncState,
        vault: String,
        exclude_patterns: Vec<String>,
    ) -> Self {
        Self {
            vault_path,
            state,
            vault,
            exclude_patterns,
            echo_hashes: HashMap::new(),
            sync_complete: false,
            expected_modify: 0,
            expected_delete: 0,
            expected_upload: 0,
            received_modify: 0,
            received_delete: 0,
            received_upload: 0,
            got_end: false,
            pending_last_time: 0,
        }
    }

    /// Check if sync is complete.
    pub fn is_sync_complete(&self) -> bool {
        self.sync_complete
    }

    /// Return the total number of notes successfully synced (modify + delete + upload).
    pub fn synced_count(&self) -> usize {
        self.received_modify + self.received_delete + self.received_upload
    }

    /// Send incremental NoteSync request.
    ///
    /// 1. Sends `NoteSync` with `lastSyncTime` from state
    /// 2. Receives and applies server changes (modify/delete/rename/need-push)
    /// 3. Returns Ok when sync completes
    pub async fn sync_incremental(
        &mut self,
        ws: &mut WsStream,
        last_sync_time: i64,
    ) -> Result<(), FnsError> {
        self.reset_counters();

        let notes = self.collect_local_notes_filtered(last_sync_time)?;
        let context = uuid::Uuid::new_v4().to_string();

        let request = NoteSyncRequest {
            vault: self.vault.clone(),
            last_time: last_sync_time,
            notes,
            context: Some(context),
        };

        let msg = encode_message(&Action::Client(ClientAction::NoteSync), &request)?;
        info!(
            last_time = last_sync_time,
            note_count = request.notes.len(),
            "Requesting NoteSync"
        );

        ws.send(Message::Text(msg.into()))
            .await
            .map_err(|e| FnsError::WebSocket {
                message: format!("Failed to send NoteSync: {}", e),
            })?;

        // Process server responses until sync completes
        self.process_sync_responses(ws).await
    }

    /// Full sync: send all local notes for comparison.
    ///
    /// 1. Sends `NoteSync` with `lastSyncTime=0` and all local notes
    /// 2. Receives and applies server changes
    /// 3. Waits for `NoteSyncEnd`
    pub async fn sync_full(&mut self, ws: &mut WsStream) -> Result<(), FnsError> {
        self.reset_counters();

        let notes = self.collect_local_notes_all()?;
        let context = uuid::Uuid::new_v4().to_string();

        let request = NoteSyncRequest {
            vault: self.vault.clone(),
            last_time: 0,
            notes,
            context: Some(context),
        };

        let msg = encode_message(&Action::Client(ClientAction::NoteSync), &request)?;
        info!(note_count = request.notes.len(), "Requesting full NoteSync");

        ws.send(Message::Text(msg.into()))
            .await
            .map_err(|e| FnsError::WebSocket {
                message: format!("Failed to send full NoteSync: {}", e),
            })?;

        self.process_sync_responses(ws).await
    }

    /// Process server sync responses until NoteSyncEnd is received and all
    /// expected messages have been handled.
    async fn process_sync_responses(&mut self, ws: &mut WsStream) -> Result<(), FnsError> {
        loop {
            let msg = ws
                .next()
                .await
                .ok_or_else(|| FnsError::WebSocket {
                    message: "WebSocket closed during note sync".to_string(),
                })?
                .map_err(|e| FnsError::WebSocket {
                    message: format!("WebSocket error during note sync: {}", e),
                })?;

            match msg {
                Message::Text(text) => {
                    let (action, data) = decode_message(&text).map_err(|e| FnsError::Protocol {
                        message: format!("Failed to decode message: {}", e),
                    })?;

                    match action {
                        Action::Server(ServerAction::NoteSyncModify) => {
                            self.handle_note_modify(&data)?;
                            self.received_modify += 1;
                            self.check_all_received();
                        }
                        Action::Server(ServerAction::NoteSyncDelete) => {
                            self.handle_note_delete(&data)?;
                            self.received_delete += 1;
                            self.check_all_received();
                        }
                        Action::Server(ServerAction::NoteSyncRename) => {
                            self.handle_note_rename(&data)?;
                        }
                        Action::Server(ServerAction::NoteSyncMtime) => {
                            self.handle_note_mtime(&data)?;
                        }
                        Action::Server(ServerAction::NoteSyncNeedPush) => {
                            self.handle_note_need_push(&data, ws).await?;
                        }
                        Action::Server(ServerAction::NoteSyncEnd) => {
                            self.handle_sync_end(&data)?;
                            if self.sync_complete {
                                return Ok(());
                            }
                        }
                        Action::Server(ServerAction::NoteModifyAck) => {
                            debug!("Received NoteModifyAck");
                        }
                        _ => {
                            debug!(action = ?action, "Ignoring unexpected action during note sync");
                        }
                    }

                    // Check if we've received all expected messages
                    if self.got_end && self.sync_complete {
                        return Ok(());
                    }
                }
                Message::Close(frame) => {
                    let reason = frame
                        .map(|f| f.to_string())
                        .unwrap_or_else(|| "no reason".to_string());
                    return Err(FnsError::WebSocket {
                        message: format!("WebSocket closed during note sync: {}", reason),
                    });
                }
                _ => {
                    debug!("Ignoring non-text message during note sync");
                }
            }
        }
    }

    /// Handle a NoteSyncModify message from the server.
    ///
    /// Echo suppression: if the content hash is in the echo cache, skip writing
    /// and remove the entry from the cache.
    pub fn handle_note_modify(&mut self, msg_data: &serde_json::Value) -> Result<(), FnsError> {
        let data = extract_inner(msg_data);

        let rel_path: String = data
            .get("path")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        let content: String = data
            .get("content")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        let content_hash: String = data
            .get("contentHash")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        let mtime: i64 = data.get("mtime").and_then(|v| v.as_i64()).unwrap_or(0);

        if rel_path.is_empty() {
            return Ok(());
        }

        // Echo suppression: skip if we just pushed this content
        if self.echo_hashes.get(&rel_path) == Some(&content_hash) {
            self.echo_hashes.remove(&rel_path);
            info!(path = %rel_path, hash = %content_hash, "<- NoteSyncModify: echo suppressed");
            return Ok(());
        }

        // Clear any stale __deleted__ marker from previous deletions
        if self.echo_hashes.get(&rel_path) == Some(&DELETED_MARKER.to_string()) {
            self.echo_hashes.remove(&rel_path);
        }

        let full_path = self.vault_path.join(&rel_path);

        // Create parent directories if needed
        if let Some(parent) = full_path.parent() {
            fs::create_dir_all(parent)?;
        }

        // Write content to file
        fs::write(&full_path, &content)?;

        // Set mtime if provided
        if mtime > 0 {
            let ts = UNIX_EPOCH + std::time::Duration::from_millis(mtime as u64);
            if let Err(e) =
                filetime::set_file_mtime(&full_path, filetime::FileTime::from_system_time(ts))
            {
                warn!(path = %rel_path, error = %e, "Failed to set mtime");
            }
        }

        // Update echo cache for the new state
        self.echo_hashes.insert(rel_path.clone(), content_hash);

        info!(path = %rel_path, "<- NoteSyncModify applied");
        Ok(())
    }

    /// Handle a NoteSyncDelete message from the server.
    pub fn handle_note_delete(&mut self, msg_data: &serde_json::Value) -> Result<(), FnsError> {
        let data = extract_inner(msg_data);

        let rel_path: String = data
            .get("path")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        if rel_path.is_empty() {
            return Ok(());
        }

        // Echo suppression: skip if we triggered this delete ourselves
        if self.echo_hashes.get(&rel_path) == Some(&DELETED_MARKER.to_string()) {
            self.echo_hashes.remove(&rel_path);
            debug!(path = %rel_path, "<- NoteSyncDelete: echo suppressed");
            return Ok(());
        }

        let full_path = self.vault_path.join(&rel_path);

        if full_path.exists() {
            fs::remove_file(&full_path)?;
            info!(path = %rel_path, "<- NoteSyncDelete applied");
            self.try_remove_empty_parent(&full_path);
        }

        self.echo_hashes
            .insert(rel_path, DELETED_MARKER.to_string());

        Ok(())
    }

    /// Handle a NoteSyncRename message from the server.
    pub fn handle_note_rename(&mut self, msg_data: &serde_json::Value) -> Result<(), FnsError> {
        let data = extract_inner(msg_data);

        let old_path: String = data
            .get("oldPath")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        let new_path: String = data
            .get("path")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        if old_path.is_empty() || new_path.is_empty() {
            return Ok(());
        }

        // Mark old path as deleted in echo cache
        self.echo_hashes
            .insert(old_path.clone(), DELETED_MARKER.to_string());

        let old_full = self.vault_path.join(&old_path);
        let new_full = self.vault_path.join(&new_path);

        if old_full.exists() {
            // Create parent directories for new location
            if let Some(parent) = new_full.parent() {
                fs::create_dir_all(parent)?;
            }

            fs::rename(&old_full, &new_full)?;

            // Read renamed file to update content hash in echo cache
            if new_full.exists() {
                if let Ok(content) = fs::read_to_string(&new_full) {
                    let hash = hash_content(&content);
                    self.echo_hashes.insert(new_path.clone(), hash);
                }
            }

            info!(old = %old_path, new = %new_path, "<- NoteSyncRename applied");
            self.try_remove_empty_parent(&old_full);
        }

        Ok(())
    }

    /// Handle a NoteSyncMtime message from the server.
    pub fn handle_note_mtime(&mut self, msg_data: &serde_json::Value) -> Result<(), FnsError> {
        let data = extract_inner(msg_data);

        let rel_path: String = data
            .get("path")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        let mtime: i64 = data.get("mtime").and_then(|v| v.as_i64()).unwrap_or(0);

        if rel_path.is_empty() || mtime == 0 {
            return Ok(());
        }

        let full_path = self.vault_path.join(&rel_path);
        if full_path.exists() {
            let ts = UNIX_EPOCH + std::time::Duration::from_millis(mtime as u64);
            if let Err(e) =
                filetime::set_file_mtime(&full_path, filetime::FileTime::from_system_time(ts))
            {
                warn!(path = %rel_path, error = %e, "Failed to set mtime");
            }
        }

        Ok(())
    }

    /// Handle a NoteSyncNeedPush message: re-push local content (force, bypassing echo suppression).
    async fn handle_note_need_push(
        &mut self,
        msg_data: &serde_json::Value,
        ws: &mut WsStream,
    ) -> Result<(), FnsError> {
        let data = extract_inner(msg_data);

        let rel_path: String = data
            .get("path")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        if rel_path.is_empty() {
            return Ok(());
        }

        info!(path = %rel_path, "<- NoteSyncNeedPush");
        self.push_modify(ws, &rel_path, true).await?;
        self.received_upload += 1;
        self.check_all_received();
        Ok(())
    }

    /// Handle NoteSyncEnd: record counters and check if sync is complete.
    fn handle_sync_end(&mut self, msg_data: &serde_json::Value) -> Result<(), FnsError> {
        let data = extract_inner(msg_data);

        let last_time: i64 = data.get("lastTime").and_then(|v| v.as_i64()).unwrap_or(0);

        let need_modify_count: i64 = data
            .get("needModifyCount")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        let need_delete_count: i64 = data
            .get("needDeleteCount")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        let need_upload_count: i64 = data
            .get("needUploadCount")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        info!(
            last_time = last_time,
            need_modify = need_modify_count,
            need_delete = need_delete_count,
            need_upload = need_upload_count,
            "<- NoteSyncEnd"
        );

        self.pending_last_time = last_time;
        self.expected_modify = need_modify_count as usize;
        self.expected_delete = need_delete_count as usize;
        self.expected_upload = need_upload_count as usize;
        self.got_end = true;

        let total_expected = self.expected_modify + self.expected_delete + self.expected_upload;
        if total_expected == 0 {
            self.sync_complete = true;
        } else {
            self.check_all_received();
        }

        Ok(())
    }

    /// Get the pending last sync time.
    pub fn pending_last_time(&self) -> i64 {
        self.pending_last_time
    }

    /// Push a local note modification to the server.
    ///
    /// If `force` is false, skips if the content hash matches echo_hashes (echo dedup).
    /// On success, adds content hash to echo_hashes.
    pub async fn push_modify(
        &mut self,
        ws: &mut WsStream,
        rel_path: &str,
        force: bool,
    ) -> Result<(), FnsError> {
        let full_path = self.vault_path.join(rel_path);
        if !full_path.exists() {
            return Ok(());
        }

        let content = match read_stable_note_content(&full_path).await {
            Ok(c) => c,
            Err(e) => {
                warn!(path = rel_path, error = %e, "Failed to read note file");
                return Ok(());
            }
        };

        let content_hash = hash_content(&content);

        // Echo suppression: skip if we've already pushed this content (unless forced)
        if !force && self.echo_hashes.get(rel_path) == Some(&content_hash) {
            debug!(path = rel_path, "Skipping echo for note modify");
            return Ok(());
        }

        let metadata = fs::metadata(&full_path)?;
        let ctime = metadata
            .created()
            .unwrap_or(SystemTime::UNIX_EPOCH)
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let mtime = metadata
            .modified()
            .unwrap_or(SystemTime::UNIX_EPOCH)
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let request = NoteModifyRequest {
            vault: self.vault.clone(),
            path: rel_path.to_string(),
            path_hash: Some(hash_path(rel_path)),
            content,
            content_hash: Some(content_hash.clone()),
            base_hash: None,
            ctime,
            mtime,
            create_only: false,
        };

        let msg = encode_message(&Action::Client(ClientAction::NoteModify), &request)?;
        debug!("NoteModify JSON: {}", msg);
        info!(path = rel_path, "NoteModify -> server");

        ws.send(Message::Text(msg.into()))
            .await
            .map_err(|e| FnsError::WebSocket {
                message: format!("Failed to send NoteModify: {}", e),
            })?;

        // Update echo cache
        self.echo_hashes.insert(rel_path.to_string(), content_hash);

        Ok(())
    }

    /// Push a local note deletion to the server.
    pub async fn push_delete(&mut self, ws: &mut WsStream, rel_path: &str) -> Result<(), FnsError> {
        // Clear any existing entry for this path
        self.echo_hashes.remove(rel_path);

        let request = NoteDeleteRequest {
            vault: self.vault.clone(),
            path: rel_path.to_string(),
            path_hash: Some(hash_path(rel_path)),
        };

        let msg = encode_message(&Action::Client(ClientAction::NoteDelete), &request)?;
        info!(path = rel_path, "NoteDelete -> server");

        ws.send(Message::Text(msg.into()))
            .await
            .map_err(|e| FnsError::WebSocket {
                message: format!("Failed to send NoteDelete: {}", e),
            })?;

        self.echo_hashes
            .insert(rel_path.to_string(), DELETED_MARKER.to_string());

        Ok(())
    }

    /// Push a note rename: modify at new path, delete at old path.
    pub async fn push_rename(
        &mut self,
        ws: &mut WsStream,
        new_rel: &str,
        old_rel: &str,
    ) -> Result<(), FnsError> {
        self.push_modify(ws, new_rel, false).await?;
        self.push_delete(ws, old_rel).await
    }

    // ── Internal helpers ─────────────────────────────────────────────────

    /// Reset sync tracking counters.
    fn reset_counters(&mut self) {
        self.sync_complete = false;
        self.got_end = false;
        self.expected_modify = 0;
        self.expected_delete = 0;
        self.expected_upload = 0;
        self.received_modify = 0;
        self.received_delete = 0;
        self.received_upload = 0;
        self.pending_last_time = 0;
    }

    /// Check if all expected server messages have been received.
    fn check_all_received(&mut self) {
        if !self.got_end {
            return;
        }

        let total_expected = self.expected_modify + self.expected_delete + self.expected_upload;
        let total_received = self.received_modify + self.received_delete + self.received_upload;

        if total_received >= total_expected {
            info!(
                modify = self.received_modify,
                delete = self.received_delete,
                upload = self.received_upload,
                "NoteSync complete"
            );
            self.sync_complete = true;
        }
    }

    /// Remove empty parent directories up to vault root.
    fn try_remove_empty_parent(&self, file_path: &Path) {
        let mut parent = file_path.parent();

        while let Some(p) = parent {
            if p == self.vault_path {
                break;
            }

            if p.exists() && p.is_dir() {
                if let Ok(mut entries) = fs::read_dir(p) {
                    if entries.next().is_none() {
                        // Directory is empty
                        if let Err(e) = fs::remove_dir(p) {
                            debug!(path = %p.display(), error = %e, "Failed to remove empty dir");
                            break;
                        }
                    } else {
                        break;
                    }
                }
            }

            parent = p.parent();
        }
    }

    /// Collect all .md files in vault (for full sync).
    fn collect_local_notes_all(&self) -> Result<Vec<NoteSyncCheck>, FnsError> {
        self.enumerate_md_files()
    }

    /// Collect .md files modified after last_sync_time (for incremental sync).
    fn collect_local_notes_filtered(
        &self,
        last_sync_time: i64,
    ) -> Result<Vec<NoteSyncCheck>, FnsError> {
        let notes = self
            .enumerate_md_files()?
            .into_iter()
            .filter(|note| note.mtime > last_sync_time)
            .collect();
        Ok(notes)
    }

    /// Enumerate .md files in vault, skipping excluded patterns and config dirs.
    fn enumerate_md_files(&self) -> Result<Vec<NoteSyncCheck>, FnsError> {
        let mut notes = Vec::new();

        if !self.vault_path.exists() {
            return Ok(notes);
        }

        for entry in walkdir::WalkDir::new(&self.vault_path)
            .follow_links(false)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            let path = entry.path();

            if !path.is_file() {
                continue;
            }

            // Check for .md extension
            if !path.extension().is_some_and(|ext| ext == "md") {
                continue;
            }

            // Get relative path
            let rel = match path.strip_prefix(&self.vault_path) {
                Ok(r) => r.to_string_lossy().to_string(),
                Err(_) => continue,
            };

            // Skip config directories (.obsidian, .agents)
            if self.is_config_path(&rel) {
                continue;
            }

            // Skip excluded patterns
            if self.is_excluded(&rel) {
                continue;
            }

            // Read and hash content
            let content = match fs::read(path) {
                Ok(c) => c,
                Err(_) => continue,
            };

            let content_hash = hash_content(&String::from_utf8_lossy(&content));

            let metadata = match fs::metadata(path) {
                Ok(m) => m,
                Err(_) => continue,
            };

            let mtime = metadata
                .modified()
                .unwrap_or(SystemTime::UNIX_EPOCH)
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;

            notes.push(NoteSyncCheck {
                path_hash: hash_path(&rel),
                path: rel,
                content_hash,
                mtime,
            });
        }

        Ok(notes)
    }

    /// Check if a relative path is a config path (should be skipped).
    fn is_config_path(&self, rel: &str) -> bool {
        // Skip .obsidian and .agents directories (handled by SettingSync)
        rel.starts_with(".obsidian/") || rel.starts_with(".agents/")
    }

    /// Check if a relative path matches any exclude pattern.
    fn is_excluded(&self, rel: &str) -> bool {
        if rel.contains(".~#") {
            return true;
        }

        for pattern in &self.exclude_patterns {
            // Handle directory patterns ending with /**
            if let Some(dir_name) = pattern.strip_suffix("/**") {
                if rel.starts_with(dir_name) || rel.starts_with(&format!("{}/", dir_name)) {
                    return true;
                }
            }
            // Handle file patterns like *.tmp. Treat .tmp.* variants as
            // transient files too (for example: note.md.tmp.w_3o8rmv).
            if let Some(ext) = pattern.strip_prefix("*.") {
                let suffix = format!(".{}", ext);
                if rel.ends_with(&suffix) || rel.contains(&format!("{}.", suffix)) {
                    return true;
                }
            }
        }
        false
    }
}

/// Extract the inner data from a server response wrapper.
///
/// Server wraps payloads as `{code, status, message, data: {actual fields}}`.
/// If `data` is present, return it; otherwise return the original value.
fn extract_inner(msg_data: &serde_json::Value) -> &serde_json::Value {
    if let Some(obj) = msg_data.as_object() {
        if let Some(data) = obj.get("data") {
            return data;
        }
    }
    msg_data
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_is_config_path() {
        let sync = NoteSync::new(
            PathBuf::from("/vault"),
            SyncState::default(),
            "test".to_string(),
            vec![],
        );

        assert!(sync.is_config_path(".obsidian/app.json"));
        assert!(sync.is_config_path(".agents/config.yaml"));
        assert!(sync.is_config_path(".obsidian/themes/theme.css"));
        assert!(!sync.is_config_path("notes/hello.md"));
        assert!(!sync.is_config_path("folder/note.md"));
    }

    #[test]
    fn test_is_excluded() {
        let sync = NoteSync::new(
            PathBuf::from("/vault"),
            SyncState::default(),
            "test".to_string(),
            vec![
                ".git/**".to_string(),
                ".trash/**".to_string(),
                "*.tmp".to_string(),
            ],
        );

        assert!(sync.is_excluded(".git/config"));
        assert!(sync.is_excluded(".git/objects/abc"));
        assert!(sync.is_excluded(".trash/old.md"));
        assert!(sync.is_excluded("notes/temp.tmp"));
        assert!(sync.is_excluded("notes/hello.md.tmp.w_3o8rmv"));
        assert!(sync.is_excluded("notes/hello.md.~#0"));
        assert!(!sync.is_excluded("notes/hello.md"));
        assert!(!sync.is_excluded(".obsidian/app.json"));
    }

    #[test]
    fn test_enumerate_md_files() {
        let dir = tempdir().unwrap();
        let vault_path = dir.path().to_path_buf();

        // Create some markdown files
        fs::create_dir_all(vault_path.join("notes")).unwrap();
        fs::write(vault_path.join("notes/hello.md"), "# Hello").unwrap();
        fs::write(vault_path.join("notes/world.md"), "# World").unwrap();
        fs::write(vault_path.join("readme.md"), "# README").unwrap();

        // Create config directory (should be skipped)
        fs::create_dir_all(vault_path.join(".obsidian")).unwrap();
        fs::write(vault_path.join(".obsidian/app.json"), "{}").unwrap();

        // Create non-md file (should be skipped)
        fs::write(vault_path.join("notes/data.txt"), "data").unwrap();

        let sync = NoteSync::new(vault_path, SyncState::default(), "test".to_string(), vec![]);

        let notes = sync.enumerate_md_files().unwrap();

        // Should have 3 markdown files
        assert_eq!(notes.len(), 3);

        let paths: Vec<&str> = notes.iter().map(|n| n.path.as_str()).collect();
        assert!(paths.contains(&"notes/hello.md"));
        assert!(paths.contains(&"notes/world.md"));
        assert!(paths.contains(&"readme.md"));

        // Config file should not be included
        assert!(!paths.contains(&".obsidian/app.json"));
    }

    #[test]
    fn test_echo_suppression() {
        let mut sync = NoteSync::new(
            PathBuf::from("/vault"),
            SyncState::default(),
            "test".to_string(),
            vec![],
        );

        // Add an entry to echo_hashes
        sync.echo_hashes
            .insert("notes/hello.md".to_string(), "12345".to_string());

        // Simulate receiving the same content
        let msg = serde_json::json!({
            "data": {
                "path": "notes/hello.md",
                "content": "test content",
                "contentHash": "12345",
                "mtime": 0,
            }
        });

        // Should be suppressed (no error, but file not written)
        assert_eq!(
            sync.echo_hashes.get("notes/hello.md"),
            Some(&"12345".to_string())
        );
        sync.handle_note_modify(&msg).ok(); // Ignore IO error since path doesn't exist
        assert_eq!(sync.echo_hashes.get("notes/hello.md"), None); // Should be removed
    }

    #[test]
    fn test_echo_cache_revert() {
        let dir = tempdir().unwrap();
        let vault_path = dir.path().to_path_buf();

        let mut sync = NoteSync::new(
            vault_path.clone(),
            SyncState::default(),
            "test".to_string(),
            vec![],
        );

        fs::create_dir_all(vault_path.join("notes")).unwrap();
        let note_path = vault_path.join("notes/test.md");

        // Simulate push_modify with content "A"
        fs::write(&note_path, "content A").unwrap();
        let hash_a = hash_content("content A");
        sync.echo_hashes
            .insert("notes/test.md".to_string(), hash_a.clone());

        // Simulate push_modify with content "B" (user edits to B)
        fs::write(&note_path, "content B").unwrap();
        let hash_b = hash_content("content B");
        sync.echo_hashes
            .insert("notes/test.md".to_string(), hash_b.clone());

        // Simulate push_modify with content "A" again (user reverts to A)
        fs::write(&note_path, "content A").unwrap();
        sync.echo_hashes
            .insert("notes/test.md".to_string(), hash_a.clone());

        // Now simulate receiving the revert from server
        let msg = serde_json::json!({
            "data": {
                "path": "notes/test.md",
                "content": "content A",
                "contentHash": hash_a,
                "mtime": 0,
            }
        });

        // With HashMap, this should be suppressed (echo_hashes has hash_a)
        assert_eq!(sync.echo_hashes.get("notes/test.md"), Some(&hash_a));
        sync.handle_note_modify(&msg).unwrap();
        // Entry should be removed after echo suppression
        assert_eq!(sync.echo_hashes.get("notes/test.md"), None);
    }

    #[test]
    fn test_echo_cache_tombstone_reuse() {
        let dir = tempdir().unwrap();
        let vault_path = dir.path().to_path_buf();

        let mut sync = NoteSync::new(
            vault_path.clone(),
            SyncState::default(),
            "test".to_string(),
            vec![],
        );

        fs::create_dir_all(vault_path.join("notes")).unwrap();
        let note_path = vault_path.join("notes/test.md");

        // Simulate push_delete (server deletes)
        sync.echo_hashes
            .insert("notes/test.md".to_string(), DELETED_MARKER.to_string());

        // Simulate push_modify (user recreates same path)
        fs::write(&note_path, "new content").unwrap();
        let hash_new = hash_content("new content");
        sync.echo_hashes
            .insert("notes/test.md".to_string(), hash_new.clone());

        // Simulate push_delete again (user deletes again)
        sync.echo_hashes
            .insert("notes/test.md".to_string(), DELETED_MARKER.to_string());

        // Now simulate receiving the delete from server
        let msg = serde_json::json!({
            "data": {
                "path": "notes/test.md",
            }
        });

        // With HashMap, this should be suppressed (echo_hashes has __deleted__)
        assert_eq!(
            sync.echo_hashes.get("notes/test.md"),
            Some(&DELETED_MARKER.to_string())
        );
        sync.handle_note_delete(&msg).unwrap();
        // Entry should be removed after echo suppression
        assert_eq!(sync.echo_hashes.get("notes/test.md"), None);
    }

    #[test]
    fn test_sync_state_default() {
        let state = SyncState::default();
        assert_eq!(state.last_note_sync_time, 0);
        assert_eq!(state.last_file_sync_time, 0);
        assert_eq!(state.last_setting_sync_time, 0);
    }
}
