//! File sync engine with chunked transfer support.
//!
//! Implements the FileSync protocol for binary file synchronization:
//! - Chunked upload: split large files into configurable chunks
//! - Chunked download: receive and reassemble chunks from server
//! - Concurrent uploads: multiple parallel upload workers
//! - Stall detection: warn if no progress for 30 seconds

#![allow(dead_code)]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::SinkExt;
use tokio::sync::{Semaphore, mpsc};
use tokio_tungstenite::tungstenite::protocol::Message;
use tracing::{debug, info, warn};

use crate::config::AppConfig;
use crate::error::FnsError;
use crate::hash::{hash_bytes, hash_path};
use crate::protocol::{
    BinaryChunkParser, ChunkReassembler, ClientAction, FileDeleteRequest, FileSyncCheck,
    FileSyncRequest, FileUploadCheckRequest, SEPARATOR, ServerAction, build_binary_chunk,
};
use crate::state::SyncState;
use crate::ws_client::WsStream;

/// Default chunk size (512KB)
const DEFAULT_CHUNK_SIZE: usize = 524_288;

/// Default upload concurrency
const DEFAULT_UPLOAD_CONCURRENCY: usize = 2;

/// Stall detection threshold (30 seconds)
const STALL_THRESHOLD_SECS: u64 = 30;

/// Sentinel value for deleted files in echo hash cache
const DELETED_SENTINEL: &str = "__deleted__";

/// Upload session tracking
#[derive(Debug, Clone)]
pub struct UploadSession {
    pub session_id: String,
    pub path: String,
    pub chunk_size: usize,
    pub total_chunks: usize,
    pub chunks_sent: usize,
}

/// Download session tracking
#[derive(Debug)]
pub struct DownloadSession {
    pub session_id: String,
    pub path: String,
    pub total_size: usize,
    pub total_chunks: usize,
    pub chunk_size: usize,
    pub reassembler: ChunkReassembler,
}

/// File sync engine with chunked transfer support
#[derive(Debug)]
pub struct FileSync {
    /// Vault path for file operations
    vault_path: PathBuf,
    /// Server vault name
    vault_name: String,
    /// Chunk size for transfers (default 512KB)
    chunk_size: usize,
    /// Upload concurrency limit
    upload_concurrency: usize,
    /// Active upload sessions
    active_uploads: HashMap<String, UploadSession>,
    /// Active download sessions
    active_downloads: HashMap<String, DownloadSession>,
    /// Echo hash cache for detecting redundant operations
    echo_hashes: HashMap<String, String>,
    /// Last activity timestamp for stall detection
    last_activity: Instant,
    /// Expected modify count from server
    expected_modify: usize,
    /// Expected delete count from server
    expected_delete: usize,
    /// Expected upload count requested by server
    expected_upload: usize,
    /// Received modify count
    received_modify: usize,
    /// Received delete count
    received_delete: usize,
    /// Received upload requests
    received_upload: usize,
    /// Whether sync end has been received
    got_end: bool,
    /// Pending last_time from server
    pending_last_time: i64,
}

impl FileSync {
    /// Create a new FileSync engine
    pub fn new(config: &AppConfig) -> Self {
        Self {
            vault_path: config.vault_path(),
            vault_name: config.server.vault.clone(),
            chunk_size: config.sync.file_chunk_size,
            upload_concurrency: config.sync.upload_concurrency,
            active_uploads: HashMap::new(),
            active_downloads: HashMap::new(),
            echo_hashes: HashMap::new(),
            last_activity: Instant::now(),
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

    /// Create with explicit parameters
    pub fn with_params(
        vault_path: PathBuf,
        vault_name: String,
        chunk_size: usize,
        upload_concurrency: usize,
    ) -> Self {
        Self {
            vault_path,
            vault_name,
            chunk_size: if chunk_size == 0 {
                DEFAULT_CHUNK_SIZE
            } else {
                chunk_size
            },
            upload_concurrency: if upload_concurrency == 0 {
                DEFAULT_UPLOAD_CONCURRENCY
            } else {
                upload_concurrency
            },
            active_uploads: HashMap::new(),
            active_downloads: HashMap::new(),
            echo_hashes: HashMap::new(),
            last_activity: Instant::now(),
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

    /// Reset sync counters for a new sync session
    pub fn reset(&mut self) {
        self.active_uploads.clear();
        self.active_downloads.clear();
        self.expected_modify = 0;
        self.expected_delete = 0;
        self.expected_upload = 0;
        self.received_modify = 0;
        self.received_delete = 0;
        self.received_upload = 0;
        self.got_end = false;
        self.pending_last_time = 0;
        self.last_activity = Instant::now();
    }

    /// Mark activity for stall detection
    fn mark_activity(&mut self) {
        self.last_activity = Instant::now();
    }

    /// Check if sync is stalled (no activity for STALL_THRESHOLD_SECS)
    pub fn is_stalled(&self) -> bool {
        if !self.got_end {
            return false;
        }
        if !self.active_downloads.is_empty() {
            return false;
        }
        let total_expected = self.expected_modify + self.expected_delete + self.expected_upload;
        let total_received = self.received_modify + self.received_delete + self.received_upload;
        if total_received >= total_expected {
            return false;
        }
        self.last_activity.elapsed() >= Duration::from_secs(STALL_THRESHOLD_SECS)
    }

    /// Check if sync is complete
    pub fn is_complete(&self) -> bool {
        if !self.got_end {
            return false;
        }
        let total_expected = self.expected_modify + self.expected_delete + self.expected_upload;
        let total_received = self.received_modify + self.received_delete + self.received_upload;
        total_received >= total_expected
            && self.active_downloads.is_empty()
            && self.active_uploads.is_empty()
    }

    /// Request file sync from server
    pub async fn request_sync(
        &mut self,
        ws: &mut WsStream,
        _state: &SyncState,
    ) -> Result<(), FnsError> {
        self.reset();

        let files = self.collect_local_files()?;
        let file_count = files.len();
        let context = uuid::Uuid::new_v4().to_string();

        let request = FileSyncRequest {
            vault: self.vault_name.clone(),
            // The server compares against only files changed after lastTime, then treats
            // the remaining client paths as missing on the server. Use a full file index
            // until the server keeps a separate all-files index for this check.
            last_time: 0,
            files,
            context: Some(context),
        };

        let msg = format!(
            "{}{}{}",
            ClientAction::FileSync,
            SEPARATOR,
            serde_json::to_string(&request)?
        );

        info!(
            last_time = request.last_time,
            file_count = file_count,
            "Requesting FileSync"
        );

        ws.send(Message::Text(msg.into()))
            .await
            .map_err(|e| FnsError::WebSocket {
                message: format!("Failed to send FileSync: {}", e),
            })?;

        Ok(())
    }

    /// Collect local files for sync request
    fn collect_local_files(&self) -> Result<Vec<FileSyncCheck>, FnsError> {
        let mut files = Vec::new();

        if !self.vault_path.exists() {
            return Ok(files);
        }

        for entry in walkdir::WalkDir::new(&self.vault_path)
            .follow_links(false)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            if !entry.file_type().is_file() {
                continue;
            }

            let path = entry.path();
            let rel = match path.strip_prefix(&self.vault_path) {
                Ok(r) => r.to_string_lossy().to_string(),
                Err(_) => continue,
            };

            // Skip markdown files (handled by NoteSync)
            if rel.ends_with(".md") {
                continue;
            }

            // Skip dot-prefixed directories (handled by SettingSync)
            if rel
                .split('/')
                .next()
                .map(|s| s.starts_with('.'))
                .unwrap_or(false)
            {
                continue;
            }

            let metadata = std::fs::metadata(path)?;
            let content_hash = self.hash_file_content(path)?;

            files.push(FileSyncCheck {
                path: rel.clone(),
                path_hash: hash_path(&rel),
                content_hash,
                size: metadata.len() as i64,
                ctime: file_ctime(&metadata)?,
                mtime: file_mtime(&metadata)?,
            });
        }

        Ok(files)
    }

    /// Hash file content using the protocol string hash (matches Python and server)
    fn hash_file_content(&self, path: &Path) -> Result<String, FnsError> {
        let bytes = std::fs::read(path)?;
        Ok(hash_bytes(&bytes))
    }

    /// Push upload a file to server
    pub async fn push_upload(&mut self, ws: &mut WsStream, rel_path: &str) -> Result<(), FnsError> {
        let full_path = self.vault_path.join(rel_path);
        if !full_path.exists() {
            return Ok(());
        }

        let content_hash = self.hash_file_content(&full_path)?;
        if self.echo_hashes.get(rel_path).map(|s| s.as_str()) == Some(&content_hash) {
            // debug!(path = rel_path, "Skipping upload - hash matches cache");
            return Ok(());
        }

        let metadata = std::fs::metadata(&full_path)?;
        let request = FileUploadCheckRequest {
            vault: self.vault_name.clone(),
            path: rel_path.to_string(),
            path_hash: hash_path(rel_path),
            content_hash: content_hash.clone(),
            size: metadata.len() as i64,
            ctime: file_ctime(&metadata)?,
            mtime: file_mtime(&metadata)?,
        };

        let msg = format!(
            "{}{}{}",
            ClientAction::FileUploadCheck,
            SEPARATOR,
            serde_json::to_string(&request)?
        );

        info!(path = rel_path, size = metadata.len(), "FileUploadCheck");

        ws.send(Message::Text(msg.into()))
            .await
            .map_err(|e| FnsError::WebSocket {
                message: format!("Failed to send FileUploadCheck: {}", e),
            })?;

        Ok(())
    }

    /// Push delete a file from server
    pub async fn push_delete(&mut self, ws: &mut WsStream, rel_path: &str) -> Result<(), FnsError> {
        self.echo_hashes.remove(rel_path);

        let request = FileDeleteRequest {
            vault: self.vault_name.clone(),
            path: rel_path.to_string(),
            path_hash: Some(hash_path(rel_path)),
        };

        let msg = format!(
            "{}{}{}",
            ClientAction::FileDelete,
            SEPARATOR,
            serde_json::to_string(&request)?
        );

        info!(path = rel_path, "FileDelete");

        ws.send(Message::Text(msg.into()))
            .await
            .map_err(|e| FnsError::WebSocket {
                message: format!("Failed to send FileDelete: {}", e),
            })?;

        self.echo_hashes
            .insert(rel_path.to_string(), DELETED_SENTINEL.to_string());
        Ok(())
    }

    /// Handle server message
    pub async fn handle_message(
        &mut self,
        ws: &mut WsStream,
        action: &ServerAction,
        data: serde_json::Value,
    ) -> Result<bool, FnsError> {
        match action {
            ServerAction::FileSyncUpdate => {
                self.handle_sync_update(ws, data).await?;
            }
            ServerAction::FileSyncDelete => {
                self.handle_sync_delete(data)?;
            }
            ServerAction::FileSyncRename => {
                self.handle_sync_rename(data)?;
            }
            ServerAction::FileSyncMtime => {
                self.handle_sync_mtime(data)?;
            }
            ServerAction::FileSyncChunkDownload => {
                self.handle_chunk_download_start(data)?;
            }
            ServerAction::FileUpload => {
                self.handle_upload_session(ws, data).await?;
            }
            ServerAction::FileUploadAck => {
                self.handle_upload_ack(data)?;
            }
            ServerAction::FileSyncEnd => {
                self.handle_sync_end(data)?;
            }
            _ => {}
        }
        Ok(self.is_complete())
    }

    /// Handle FileSyncUpdate from server
    pub async fn handle_sync_update(
        &mut self,
        ws: &mut WsStream,
        data: serde_json::Value,
    ) -> Result<(), FnsError> {
        self.mark_activity();

        let inner = extract_inner(&data);
        let rel_path: String = inner
            .get("path")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();

        if rel_path.is_empty() {
            return Ok(());
        }

        if is_dot_prefixed_path(&rel_path) {
            debug!(path = rel_path, "FileSyncUpdate ignored for config path");
            self.received_modify += 1;
            return Ok(());
        }

        // Clear any stale __deleted__ marker from previous deletations
        self.echo_hashes.remove(&rel_path);

        // Check if content is inline (small files)
        if let Some(content_b64) = inner.get("content").and_then(|v| v.as_str()) {
            let content = base64_decode(content_b64)?;
            let full_path = self.vault_path.join(&rel_path);

            if let Some(parent) = full_path.parent() {
                std::fs::create_dir_all(parent)?;
            }

            std::fs::write(&full_path, &content)?;

            if let Some(mtime) = inner.get("mtime").and_then(|v| v.as_i64()) {
                set_file_mtime(&full_path, mtime)?;
            }

            let hash = self.hash_file_content(&full_path)?;
            self.echo_hashes.insert(rel_path.clone(), hash);

            info!(path = rel_path, "FileSyncUpdate (inline)");
            self.received_modify += 1;
        } else {
            let msg = format!(
                "{}{}{}",
                ClientAction::FileChunkDownload,
                SEPARATOR,
                serde_json::to_string(&serde_json::json!({
                    "vault": self.vault_name,
                    "path": rel_path,
                    "pathHash": inner.get("pathHash").and_then(|v| v.as_str()).unwrap_or("")
                }))?
            );

            ws.send(Message::Text(msg.into()))
                .await
                .map_err(|e| FnsError::WebSocket {
                    message: format!("Failed to send FileChunkDownload: {}", e),
                })?;

            info!(path = rel_path, "FileSyncUpdate (requesting chunks)");
        }

        Ok(())
    }

    /// Handle FileSyncDelete from server
    pub fn handle_sync_delete(&mut self, data: serde_json::Value) -> Result<(), FnsError> {
        self.mark_activity();

        let inner = extract_inner(&data);
        let rel_path: String = inner
            .get("path")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();

        if rel_path.is_empty() {
            return Ok(());
        }

        if is_dot_prefixed_path(&rel_path) {
            debug!(path = rel_path, "FileSyncDelete ignored for config path");
            self.received_delete += 1;
            return Ok(());
        }

        // Echo suppression: skip if we triggered this delete ourselves
        if self.echo_hashes.get(&rel_path).map(|s| s.as_str()) == Some(DELETED_SENTINEL) {
            debug!(path = rel_path, "FileSyncDelete: echo suppressed");
            self.received_delete += 1;
            return Ok(());
        }

        let full_path = self.vault_path.join(&rel_path);
        if full_path.exists() {
            std::fs::remove_file(&full_path)?;
            info!(path = rel_path, "FileSyncDelete applied");
            self.try_remove_empty_parent(&full_path);
        }

        self.echo_hashes
            .insert(rel_path.clone(), DELETED_SENTINEL.to_string());

        self.received_delete += 1;
        Ok(())
    }

    /// Handle FileSyncRename from server
    fn handle_sync_rename(&mut self, data: serde_json::Value) -> Result<(), FnsError> {
        self.mark_activity();

        let inner = extract_inner(&data);
        let old_path: String = inner
            .get("oldPath")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let new_path: String = inner
            .get("path")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();

        if old_path.is_empty() || new_path.is_empty() {
            return Ok(());
        }

        self.echo_hashes
            .insert(old_path.clone(), DELETED_SENTINEL.to_string());

        let old_full = self.vault_path.join(&old_path);
        let new_full = self.vault_path.join(&new_path);

        if old_full.exists() {
            if let Some(parent) = new_full.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::rename(&old_full, &new_full)?;

            if new_full.exists() {
                let hash = self.hash_file_content(&new_full)?;
                self.echo_hashes.insert(new_path.clone(), hash);
            }

            info!(old = old_path, new = new_path, "FileSyncRename");
        }

        Ok(())
    }

    /// Handle FileSyncMtime from server
    pub fn handle_sync_mtime(&mut self, data: serde_json::Value) -> Result<(), FnsError> {
        self.mark_activity();

        let inner = extract_inner(&data);
        let rel_path: String = inner
            .get("path")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let mtime = inner.get("mtime").and_then(|v| v.as_i64()).unwrap_or(0);

        if rel_path.is_empty() || mtime == 0 {
            return Ok(());
        }

        let full_path = self.vault_path.join(&rel_path);
        if full_path.exists() {
            set_file_mtime(&full_path, mtime)?;
        }

        Ok(())
    }

    /// Handle FileSyncChunkDownload start from server
    pub fn handle_chunk_download_start(&mut self, data: serde_json::Value) -> Result<(), FnsError> {
        self.mark_activity();

        let inner = extract_inner(&data);
        let session_id: String = inner
            .get("sessionId")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let rel_path: String = inner
            .get("path")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let total_size: usize = inner
            .get("size")
            .and_then(|v| v.as_i64())
            .map(|v| v as usize)
            .unwrap_or(0);
        let total_chunks: usize = inner
            .get("totalChunks")
            .and_then(|v| v.as_i64())
            .map(|v| v as usize)
            .unwrap_or(1);
        let chunk_size: usize = inner
            .get("chunkSize")
            .and_then(|v| v.as_i64())
            .map(|v| v as usize)
            .unwrap_or(self.chunk_size);

        if session_id.is_empty() || rel_path.is_empty() {
            return Ok(());
        }

        info!(
            path = rel_path,
            size = total_size,
            chunks = total_chunks,
            "FileSyncChunkDownload start"
        );

        if total_chunks == 0 {
            // Empty file
            let full_path = self.vault_path.join(&rel_path);
            if let Some(parent) = full_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(&full_path, b"")?;
            let hash = self.hash_file_content(&full_path)?;
            self.echo_hashes.insert(rel_path, hash);
            return Ok(());
        }

        // Create download session
        let mut reassembler = ChunkReassembler::new();
        reassembler.set_expected_count(total_chunks as u32);

        self.active_downloads.insert(
            session_id.clone(),
            DownloadSession {
                session_id,
                path: rel_path,
                total_size,
                total_chunks,
                chunk_size,
                reassembler,
            },
        );

        Ok(())
    }

    /// Handle FileSyncChunkDownload message (alias for handle_chunk_download_start)
    pub async fn handle_chunk_download(&mut self, data: serde_json::Value) -> Result<(), FnsError> {
        self.handle_chunk_download_start(data)
    }

    /// Handle FileUpload session from server
    pub async fn handle_upload_session(
        &mut self,
        ws: &mut WsStream,
        data: serde_json::Value,
    ) -> Result<(), FnsError> {
        let inner = extract_inner(&data);
        let session_id: String = inner
            .get("sessionId")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let chunk_size: usize = inner
            .get("chunkSize")
            .and_then(|v| v.as_i64())
            .map(|v| v as usize)
            .unwrap_or(self.chunk_size);
        let rel_path: String = inner
            .get("path")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();

        if session_id.is_empty() || rel_path.is_empty() {
            return Ok(());
        }

        let full_path = self.vault_path.join(&rel_path);
        if !full_path.exists() {
            warn!(path = rel_path, "Upload requested but file missing");
            self.received_upload += 1;
            return Ok(());
        }

        let content_hash = self.hash_file_content(&full_path)?;
        if self.echo_hashes.get(&rel_path).map(|s| s.as_str()) == Some(&content_hash) {
            info!(path = rel_path, "Skipping upload - hash matches cache");
            self.received_upload += 1;
            return Ok(());
        }

        let total_chunks = self
            .upload_file(ws, &session_id, chunk_size, &rel_path, &full_path)
            .await?;

        self.active_uploads.insert(
            session_id.clone(),
            UploadSession {
                session_id,
                path: rel_path.clone(),
                chunk_size,
                total_chunks,
                chunks_sent: total_chunks,
            },
        );
        self.echo_hashes.insert(rel_path, content_hash);

        Ok(())
    }

    /// Upload a file using chunked transfer
    async fn upload_file(
        &mut self,
        ws: &mut WsStream,
        session_id: &str,
        chunk_size: usize,
        rel_path: &str,
        full_path: &Path,
    ) -> Result<usize, FnsError> {
        let file_data = std::fs::read(full_path)?;
        let total = file_data.len();

        let total_chunks = if total == 0 {
            1
        } else {
            (total + chunk_size - 1) / chunk_size
        };

        info!(
            path = rel_path,
            session_id = &session_id[..8.min(session_id.len())],
            chunk_size = chunk_size,
            chunks = total_chunks,
            "Uploading file"
        );

        let mut offset = 0;
        let mut chunk_index = 0u32;

        while offset < total {
            let end = (offset + chunk_size).min(total);
            let chunk_data = &file_data[offset..end];

            let frame = build_binary_chunk(session_id, chunk_index, chunk_data);

            ws.send(Message::Binary(frame.into()))
                .await
                .map_err(|e| FnsError::WebSocket {
                    message: format!("Failed to send binary chunk: {}", e),
                })?;

            offset = end;
            chunk_index += 1;
        }

        // Handle empty files
        if total == 0 {
            let frame = build_binary_chunk(session_id, 0, &[]);
            ws.send(Message::Binary(frame.into()))
                .await
                .map_err(|e| FnsError::WebSocket {
                    message: format!("Failed to send empty chunk: {}", e),
                })?;
        }

        info!(path = rel_path, chunks = chunk_index, "Upload complete");

        Ok(total_chunks)
    }

    /// Handle FileUploadAck from server
    pub fn handle_upload_ack(&mut self, data: serde_json::Value) -> Result<(), FnsError> {
        let inner = extract_inner(&data);
        let rel_path = inner.get("path").and_then(|v| v.as_str()).unwrap_or("");

        let session_id = inner
            .get("sessionId")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let path_from_session = if !session_id.is_empty() {
            self.active_uploads
                .remove(session_id)
                .map(|session| session.path)
        } else {
            let matching_session = self
                .active_uploads
                .iter()
                .find(|(_, session)| session.path == rel_path)
                .map(|(id, session)| (id.clone(), session.path.clone()));
            matching_session.and_then(|(id, path)| self.active_uploads.remove(&id).map(|_| path))
        };

        if let Some(path) = path_from_session {
            self.received_upload += 1;
            debug!(path = path, "FileUploadAck");
        } else if !rel_path.is_empty() {
            debug!(path = rel_path, "FileUploadAck for unknown session");
        }

        Ok(())
    }

    /// Handle FileDeleteAck from server
    pub fn handle_delete_ack(&mut self, data: serde_json::Value) -> Result<(), FnsError> {
        let inner = extract_inner(&data);
        let rel_path = inner.get("path").and_then(|v| v.as_str()).unwrap_or("");

        if !rel_path.is_empty() {
            debug!(path = rel_path, "FileDeleteAck");
        }

        Ok(())
    }

    /// Handle FileSyncEnd from server
    fn handle_sync_end(&mut self, data: serde_json::Value) -> Result<(), FnsError> {
        self.mark_activity();

        let inner = extract_inner(&data);
        self.pending_last_time = inner.get("lastTime").and_then(|v| v.as_i64()).unwrap_or(0);
        self.expected_modify = inner
            .get("needModifyCount")
            .and_then(|v| v.as_i64())
            .map(|v| v as usize)
            .unwrap_or(0);
        self.expected_delete = inner
            .get("needDeleteCount")
            .and_then(|v| v.as_i64())
            .map(|v| v as usize)
            .unwrap_or(0);
        self.expected_upload = inner
            .get("needUploadCount")
            .and_then(|v| v.as_i64())
            .map(|v| v as usize)
            .unwrap_or(0);

        self.got_end = true;

        info!(
            last_time = self.pending_last_time,
            need_modify = self.expected_modify,
            need_delete = self.expected_delete,
            need_upload = self.expected_upload,
            "FileSyncEnd"
        );

        Ok(())
    }

    /// Handle binary chunk from server
    pub fn handle_binary_chunk(&mut self, data: &[u8]) -> Result<bool, FnsError> {
        let parsed =
            BinaryChunkParser::parse(data).map_err(|e| FnsError::Protocol { message: e })?;

        let session_id = parsed.session_id.clone();
        let session = match self.active_downloads.get_mut(&session_id) {
            Some(s) => s,
            None => return Ok(false),
        };

        let is_complete = session
            .reassembler
            .add_chunk(parsed)
            .map_err(|e| FnsError::Protocol { message: e })?;

        if is_complete {
            let session = self
                .active_downloads
                .remove(&session_id)
                .expect("session exists");

            self.finalize_download(session)?;
            return Ok(true);
        }

        // Server pushes all chunks automatically - no need to request missing chunks
        Ok(false)
    }

    /// Finalize a chunked download
    fn finalize_download(&mut self, session: DownloadSession) -> Result<(), FnsError> {
        self.mark_activity();

        let data = session
            .reassembler
            .get_data()
            .ok_or_else(|| FnsError::Sync {
                message: "Download incomplete".to_string(),
            })?;

        let full_path = self.vault_path.join(&session.path);

        if let Some(parent) = full_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        std::fs::write(&full_path, &data)?;

        let hash = self.hash_file_content(&full_path)?;
        self.echo_hashes.insert(session.path.clone(), hash);

        info!(path = session.path, "Chunked download complete");
        self.received_modify += 1;

        Ok(())
    }

    /// Get pending last_time for committing
    pub fn pending_last_time(&self) -> i64 {
        self.pending_last_time
    }

    /// Return the total number of files successfully synced (modify + delete)
    pub fn synced_count(&self) -> usize {
        self.received_modify + self.received_delete + self.received_upload
    }

    /// Try to remove empty parent directories
    fn try_remove_empty_parent(&self, file_path: &Path) {
        let mut parent = file_path.parent();
        while let Some(p) = parent {
            if p == self.vault_path {
                break;
            }
            if p.exists()
                && p.read_dir()
                    .map(|mut d| d.next().is_none())
                    .unwrap_or(false)
            {
                let _ = std::fs::remove_dir(p);
            } else {
                break;
            }
            parent = p.parent();
        }
    }
}

fn is_dot_prefixed_path(path: &str) -> bool {
    path.split('/')
        .next()
        .map(|s| s.starts_with('.'))
        .unwrap_or(false)
}

/// Extract inner data from server response
fn extract_inner(data: &serde_json::Value) -> serde_json::Value {
    if let Some(obj) = data.as_object() {
        if let Some(inner) = obj.get("data") {
            return inner.clone();
        }
    }
    data.clone()
}

/// Get file mtime as milliseconds
fn file_mtime(metadata: &std::fs::Metadata) -> Result<i64, FnsError> {
    use std::time::UNIX_EPOCH;
    let mtime = metadata
        .modified()?
        .duration_since(UNIX_EPOCH)
        .map_err(|e| FnsError::Sync {
            message: format!("Invalid mtime: {}", e),
        })?;
    Ok(mtime.as_millis() as i64)
}

/// Get file ctime in milliseconds
fn file_ctime(metadata: &std::fs::Metadata) -> Result<i64, FnsError> {
    use std::time::UNIX_EPOCH;
    let ctime = metadata
        .created()?
        .duration_since(UNIX_EPOCH)
        .map_err(|e| FnsError::Sync {
            message: format!("Invalid ctime: {}", e),
        })?;
    Ok(ctime.as_millis() as i64)
}

/// Set file mtime from milliseconds
fn set_file_mtime(path: &Path, mtime_ms: i64) -> Result<(), FnsError> {
    use std::time::{Duration, UNIX_EPOCH};
    let time = UNIX_EPOCH + Duration::from_millis(mtime_ms as u64);
    filetime::set_file_mtime(path, filetime::FileTime::from_system_time(time))?;
    Ok(())
}

/// Base64 decode
fn base64_decode(s: &str) -> Result<Vec<u8>, FnsError> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD
        .decode(s)
        .map_err(|e| FnsError::Protocol {
            message: format!("Base64 decode error: {}", e),
        })
}

/// Concurrent upload manager
pub struct ConcurrentUploadManager {
    semaphore: Arc<Semaphore>,
    tx: mpsc::Sender<UploadTask>,
}

pub(crate) struct UploadTask {
    session_id: String,
    chunk_size: usize,
    rel_path: String,
    full_path: PathBuf,
}

impl ConcurrentUploadManager {
    pub(crate) fn new(concurrency: usize) -> (Self, mpsc::Receiver<UploadTask>) {
        let (tx, rx) = mpsc::channel(100);
        (
            Self {
                semaphore: Arc::new(Semaphore::new(concurrency)),
                tx,
            },
            rx,
        )
    }

    /// Queue an upload task
    pub async fn queue(
        &self,
        session_id: String,
        chunk_size: usize,
        rel_path: String,
        full_path: PathBuf,
    ) -> Result<(), FnsError> {
        self.tx
            .send(UploadTask {
                session_id,
                chunk_size,
                rel_path,
                full_path,
            })
            .await
            .map_err(|_| FnsError::Sync {
                message: "Upload queue closed".to_string(),
            })
    }

    /// Acquire a permit for concurrent upload
    pub async fn acquire(&self) -> tokio::sync::SemaphorePermit<'_> {
        self.semaphore.acquire().await.expect("semaphore closed")
    }
}
