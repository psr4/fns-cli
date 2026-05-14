//! Setting (config) sync protocol: SettingSync incremental pull + SettingModify/SettingDelete push.
//!
//! Syncs files in dot-prefixed directories like `.obsidian/`, `.agents/`, etc.
//! Skips binary files (images, fonts, etc.).

#![allow(dead_code)]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

use tracing::{debug, info, warn};
use futures_util::SinkExt;
use tokio_tungstenite::tungstenite::protocol::Message;

use crate::error::FnsError;
use crate::hash::{hash_content, hash_path};
use crate::protocol::{
    Action, ClientAction,
    SettingSyncRequest, SettingSyncCheck,
    encode_message,
};
use crate::ws_client::WsStream;

/// Marker for deleted files in echo cache
const DELETED_MARKER: &str = "__deleted__";

/// Binary file extensions to skip during sync
const BINARY_EXTENSIONS: &[&str] = &[
    "png", "jpg", "jpeg", "gif", "bmp", "ico", "svg",
    "woff", "woff2", "ttf", "eot", "otf",
    "pdf", "zip", "tar", "gz", "rar", "7z",
    "mp3", "mp4", "wav", "avi", "mov", "mkv",
    "exe", "dll", "so", "dylib",
    "db", "sqlite", "sqlite3",
];

/// Setting sync engine
pub struct SettingSync {
    /// Vault root path
    pub vault_path: PathBuf,
    /// Config directories to sync (e.g., [".obsidian", ".agents"])
    pub config_dirs: Vec<String>,
    /// Echo suppression cache: path -> hash (or DELETED_MARKER)
    echo_hashes: HashMap<String, String>,
    /// Whether sync is enabled for general dot-prefixed dirs
    sync_config: bool,
    /// Vault name for protocol messages
    vault: String,
    /// Sync completion state
    sync_complete: bool,
    /// Expected modify count from SettingSyncEnd
    expected_modify: usize,
    /// Expected delete count from SettingSyncEnd
    expected_delete: usize,
    /// Expected upload count from SettingSyncEnd
    expected_upload: usize,
    /// Received modify count
    received_modify: usize,
    /// Received delete count
    received_delete: usize,
    /// Received upload count
    received_upload: usize,
    /// Got SettingSyncEnd message
    got_end: bool,
    /// Pending last sync time to commit
    pending_last_time: i64,
}

impl SettingSync {
    /// Create a new setting sync engine
    pub fn new(
        vault_path: PathBuf,
        config_dirs: Vec<String>,
        sync_config: bool,
        vault: String,
    ) -> Self {
        Self {
            vault_path,
            config_dirs,
            echo_hashes: HashMap::new(),
            sync_config,
            vault,
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

    /// Check if sync is complete
    pub fn is_sync_complete(&self) -> bool {
        self.sync_complete
    }

    /// Send incremental SettingSync request
    pub async fn request_sync(
        &mut self,
        ws: &mut WsStream,
        last_sync_time: i64,
    ) -> Result<(), FnsError> {
        self.reset_counters();
        
        let settings = self.collect_local_settings()?;
        let context = uuid::Uuid::new_v4().to_string();
        
        let request = SettingSyncRequest {
            vault: self.vault.clone(),
            last_time: last_sync_time,
            settings,
            cover: false,
            context: Some(context),
        };

        let msg = encode_message(&Action::Client(ClientAction::SettingSync), &request)?;
        info!(last_time = last_sync_time, settings_count = request.settings.len(), "Requesting SettingSync");
        
        ws.send(Message::Text(msg.into())).await
            .map_err(|e| FnsError::WebSocket {
                message: format!("Failed to send SettingSync: {}", e),
            })?;
        
        Ok(())
    }

    /// Send full sync request (lastTime = 0)
    pub async fn request_full_sync(&mut self, ws: &mut WsStream) -> Result<(), FnsError> {
        self.reset_counters();
        
        let settings = self.collect_local_settings()?;
        let context = uuid::Uuid::new_v4().to_string();
        
        let request = SettingSyncRequest {
            vault: self.vault.clone(),
            last_time: 0,
            settings,
            cover: false,
            context: Some(context),
        };

        let msg = encode_message(&Action::Client(ClientAction::SettingSync), &request)?;
        info!(settings_count = request.settings.len(), "Requesting full SettingSync");
        
        ws.send(Message::Text(msg.into())).await
            .map_err(|e| FnsError::WebSocket {
                message: format!("Failed to send full SettingSync: {}", e),
            })?;
        
        Ok(())
    }

    /// Push a setting modification to server
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

        if !Self::is_text_file(&full_path) {
            debug!(path = rel_path, "Skipping non-text setting file");
            return Ok(());
        }

        let content = match fs::read_to_string(&full_path) {
            Ok(c) => c,
            Err(e) => {
                warn!(path = rel_path, error = %e, "Failed to read setting file");
                return Ok(());
            }
        };

        let content_hash = hash_content(&content);
        
        // Echo suppression: skip if we've already pushed this content (unless forced)
        if !force && self.echo_hashes.get(rel_path) == Some(&content_hash) {
            debug!(path = rel_path, "Skipping echo for setting modify");
            return Ok(());
        }

        let metadata = fs::metadata(&full_path)?;
        let ctime = metadata.created()
            .unwrap_or(SystemTime::UNIX_EPOCH)
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let mtime = metadata.modified()
            .unwrap_or(SystemTime::UNIX_EPOCH)
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let request = serde_json::json!({
            "vault": self.vault,
            "path": rel_path,
            "pathHash": hash_path(rel_path),
            "content": content,
            "contentHash": content_hash,
            "ctime": ctime,
            "mtime": mtime,
        });

        let msg = encode_message(&Action::Client(ClientAction::SettingModify), &request)?;
        info!(path = rel_path, "SettingModify -> server");
        
        ws.send(Message::Text(msg.into())).await
            .map_err(|e| FnsError::WebSocket {
                message: format!("Failed to send SettingModify: {}", e),
            })?;
        
        self.echo_hashes.insert(rel_path.to_string(), content_hash);
        
        Ok(())
    }

    /// Push a setting deletion to server
    pub async fn push_delete(
        &mut self,
        ws: &mut WsStream,
        rel_path: &str,
    ) -> Result<(), FnsError> {
        self.echo_hashes.remove(rel_path);

        let request = serde_json::json!({
            "vault": self.vault,
            "path": rel_path,
            "pathHash": hash_path(rel_path),
        });

        let msg = encode_message(&Action::Client(ClientAction::SettingDelete), &request)?;
        info!(path = rel_path, "SettingDelete -> server");
        
        ws.send(Message::Text(msg.into())).await
            .map_err(|e| FnsError::WebSocket {
                message: format!("Failed to send SettingDelete: {}", e),
            })?;
        
        self.echo_hashes.insert(rel_path.to_string(), DELETED_MARKER.to_string());
        
        Ok(())
    }

    /// Push a setting rename: modify at new path, delete at old path.
    pub async fn push_rename(
        &mut self,
        ws: &mut WsStream,
        new_rel: &str,
        old_rel: &str,
    ) -> Result<(), FnsError> {
        self.push_modify(ws, new_rel, false).await?;
        self.push_delete(ws, old_rel).await?;
        Ok(())
    }

    /// Handle SettingSyncModify message from server
    pub fn handle_setting_modify(&mut self, msg: &serde_json::Value) -> Result<(), FnsError> {
        let data = extract_inner(msg);
        
        let rel_path: String = data.get("path")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        
        let content: String = data.get("content")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        
        let mtime: i64 = data.get("mtime")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        if rel_path.is_empty() {
            return Ok(());
        }

        // Clear any stale __deleted__ marker from previous deletions
        if self.echo_hashes.get(&rel_path) == Some(&DELETED_MARKER.to_string()) {
            self.echo_hashes.remove(&rel_path);
        }

        let full_path = self.vault_path.join(&rel_path);
        
        if let Some(parent) = full_path.parent() {
            fs::create_dir_all(parent)?;
        }

        fs::write(&full_path, &content)?;
        
        if mtime > 0 {
            let ts = UNIX_EPOCH + std::time::Duration::from_millis(mtime as u64);
            if let Err(e) = filetime::set_file_mtime(&full_path, filetime::FileTime::from_system_time(ts)) {
                warn!(path = rel_path, error = %e, "Failed to set mtime");
            }
        }

        let content_hash = hash_content(&content);
        self.echo_hashes.insert(rel_path.clone(), content_hash);
        
        info!(path = rel_path, "<- SettingSyncModify");
        
        self.received_modify += 1;
        self.check_all_received();
        
        Ok(())
    }

    /// Handle SettingSyncDelete message from server
    pub fn handle_setting_delete(&mut self, msg: &serde_json::Value) -> Result<(), FnsError> {
        let data = extract_inner(msg);
        
        let rel_path: String = data.get("path")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        if rel_path.is_empty() {
            return Ok(());
        }

        // Echo suppression: skip if we triggered this delete ourselves
        if self.echo_hashes.get(&rel_path) == Some(&DELETED_MARKER.to_string()) {
            self.echo_hashes.remove(&rel_path);
            debug!(path = rel_path, "<- SettingSyncDelete: echo suppressed");
            return Ok(());
        }

        let full_path = self.vault_path.join(&rel_path);
        
        if full_path.exists() {
            fs::remove_file(&full_path)?;
            info!(path = rel_path, "<- SettingSyncDelete applied");
            self.try_remove_empty_parent(&full_path);
        }
        
        self.echo_hashes.insert(rel_path, DELETED_MARKER.to_string());
        
        self.received_delete += 1;
        self.check_all_received();
        
        Ok(())
    }

    /// Handle SettingSyncRename message from server
    pub fn handle_setting_rename(&mut self, msg: &serde_json::Value) -> Result<(), FnsError> {
        let data = extract_inner(msg);
        
        let old_path: String = data.get("oldPath")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        
        let new_path: String = data.get("path")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        if old_path.is_empty() || new_path.is_empty() {
            return Ok(());
        }

        // Mark old path as deleted in echo cache
        self.echo_hashes.insert(old_path.clone(), DELETED_MARKER.to_string());

        let old_full = self.vault_path.join(&old_path);
        let new_full = self.vault_path.join(&new_path);
        
        if old_full.exists() {
            if let Some(parent) = new_full.parent() {
                fs::create_dir_all(parent)?;
            }
            
            fs::rename(&old_full, &new_full)?;
            info!(old_path = old_path, new_path = new_path, "<- SettingSyncRename");
            
            // Read renamed file to update content hash in echo cache
            if new_full.exists() {
                if let Ok(content) = fs::read_to_string(&new_full) {
                    let content_hash = hash_content(&content);
                    self.echo_hashes.insert(new_path.clone(), content_hash);
                }
            }
            
            self.try_remove_empty_parent(&old_full);
        }
        
        Ok(())
    }

    /// Handle SettingSyncMtime message from server
    pub fn handle_setting_mtime(&mut self, msg: &serde_json::Value) -> Result<(), FnsError> {
        let data = extract_inner(msg);
        
        let rel_path: String = data.get("path")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        
        let mtime: i64 = data.get("mtime")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        if rel_path.is_empty() || mtime == 0 {
            return Ok(());
        }

        let full_path = self.vault_path.join(&rel_path);
        
        if full_path.exists() {
            let ts = UNIX_EPOCH + std::time::Duration::from_millis(mtime as u64);
            if let Err(e) = filetime::set_file_mtime(&full_path, filetime::FileTime::from_system_time(ts)) {
                warn!(path = rel_path, error = %e, "Failed to set mtime");
            }
        }
        
        Ok(())
    }

    /// Handle SettingSyncNeedUpload message from server
    pub async fn handle_setting_need_upload(
        &mut self,
        ws: &mut WsStream,
        msg: &serde_json::Value,
    ) -> Result<(), FnsError> {
        let data = extract_inner(msg);
        
        let need_upload = data.get("needUpload")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        info!(count = need_upload.len(), "<- SettingSyncNeedUpload");
        
        for item in need_upload {
            let rel_path = if let Some(obj) = item.as_object() {
                obj.get("path")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string()
            } else {
                item.as_str()
                    .unwrap_or_default()
                    .to_string()
            };
            
            if !rel_path.is_empty() {
                self.push_modify(ws, &rel_path, false).await?;
                self.received_upload += 1;
            }
        }
        
        self.check_all_received();
        Ok(())
    }

    /// Handle SettingSyncEnd message from server
    pub fn handle_setting_end(&mut self, msg: &serde_json::Value) -> Result<(), FnsError> {
        let data = extract_inner(msg);
        
        let last_time: i64 = data.get("lastTime")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        
        let need_modify_count: i64 = data.get("needModifyCount")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        
        let need_delete_count: i64 = data.get("needDeleteCount")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        
        let need_upload_count: i64 = data.get("needUploadCount")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        info!(
            last_time = last_time,
            need_modify = need_modify_count,
            need_delete = need_delete_count,
            need_upload = need_upload_count,
            "<- SettingSyncEnd"
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

    /// Get the pending last sync time
    pub fn pending_last_time(&self) -> i64 {
        self.pending_last_time
    }

    /// Return the total number of settings successfully synced (modify + delete)
    pub fn synced_count(&self) -> usize {
        self.received_modify + self.received_delete
    }

    /// Check if a file is text (not binary)
    pub fn is_text_file(path: &Path) -> bool {
        if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
            let ext_lower = ext.to_lowercase();
            if BINARY_EXTENSIONS.contains(&ext_lower.as_str()) {
                return false;
            }
        }
        
        if path.exists() {
            if let Ok(mut file) = fs::File::open(path) {
                let mut buffer = [0u8; 8192];
                if let Ok(n) = std::io::Read::read(&mut file, &mut buffer) {
                    if buffer[..n].contains(&0u8) {
                        return false;
                    }
                }
            }
        }
        
        true
    }

    /// Check if a relative path is a config path
    fn is_config_path(&self, rel: &str) -> bool {
        let first = rel.split('/').next().unwrap_or("");
        
        if !first.starts_with('.') {
            return false;
        }
        
        if self.config_dirs.contains(&first.to_string()) {
            return true;
        }
        
        self.sync_config
    }

    /// Collect all local setting files
    fn collect_local_settings(&self) -> Result<Vec<SettingSyncCheck>, FnsError> {
        let mut settings = Vec::new();
        
        if !self.vault_path.exists() {
            return Ok(settings);
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

            let rel = match path.strip_prefix(&self.vault_path) {
                Ok(r) => r.to_string_lossy().to_string(),
                Err(_) => continue,
            };

            if !self.is_config_path(&rel) {
                continue;
            }

            if self.is_excluded(&rel) {
                continue;
            }

            if !Self::is_text_file(path) {
                continue;
            }

            let content = match fs::read(path) {
                Ok(c) => c,
                Err(_) => continue,
            };

            let content_hash = hash_content(&String::from_utf8_lossy(&content));
            
            let metadata = match fs::metadata(path) {
                Ok(m) => m,
                Err(_) => continue,
            };

            let mtime = metadata.modified()
                .unwrap_or(SystemTime::UNIX_EPOCH)
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;

            let _path_hash = hash_path(&rel);
            settings.push(SettingSyncCheck {
                path_hash: hash_path(&rel),
                path: rel,
                content_hash,
                mtime,
            });
        }

        Ok(settings)
    }

    /// Check if path is excluded
    fn is_excluded(&self, rel: &str) -> bool {
        // Exclude .fns_state.json - local state file, should not sync
        if rel == ".fns_state.json" {
            return true;
        }
        let excluded_prefixes = [".git/", ".trash/"];
        for prefix in &excluded_prefixes {
            if rel.starts_with(prefix) {
                return true;
            }
        }
        false
    }

    /// Reset sync counters
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

    /// Check if all expected messages received
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
                "SettingSync complete"
            );
            self.sync_complete = true;
        }
    }

    /// Try to remove empty parent directories
    fn try_remove_empty_parent(&self, file_path: &Path) {
        let mut parent = file_path.parent();
        
        while let Some(p) = parent {
            if p == self.vault_path {
                break;
            }
            
            if p.exists() && p.is_dir() {
                if let Ok(mut entries) = fs::read_dir(p) {
                    if entries.next().is_none() {
                        if let Err(e) = fs::remove_dir(p) {
                            debug!(path = ?p, error = %e, "Failed to remove empty dir");
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
}

/// Extract inner data from server response wrapper
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
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn test_is_text_file_text() {
        let dir = tempdir().unwrap();
        let file = dir.path().join("test.txt");
        fs::write(&file, "Hello, world!").unwrap();
        assert!(SettingSync::is_text_file(&file));
    }

    #[test]
    fn test_is_text_file_binary_extension() {
        let dir = tempdir().unwrap();
        let file = dir.path().join("test.png");
        fs::write(&file, [0x89, 0x50, 0x4E, 0x47]).unwrap();
        assert!(!SettingSync::is_text_file(&file));
    }

    #[test]
    fn test_is_text_file_binary_content() {
        let dir = tempdir().unwrap();
        let file = dir.path().join("test.dat");
        fs::write(&file, [0x00, 0x01, 0x02, 0x03]).unwrap();
        assert!(!SettingSync::is_text_file(&file));
    }

    #[test]
    fn test_is_text_file_json() {
        let dir = tempdir().unwrap();
        let file = dir.path().join("config.json");
        fs::write(&file, r#"{"key": "value"}"#).unwrap();
        assert!(SettingSync::is_text_file(&file));
    }

    #[test]
    fn test_config_path_detection() {
        let sync = SettingSync::new(
            PathBuf::from("/vault"),
            vec![".obsidian".to_string(), ".agents".to_string()],
            true,
            "test".to_string(),
        );

        assert!(sync.is_config_path(".obsidian/app.json"));
        assert!(sync.is_config_path(".agents/config.yaml"));
        assert!(sync.is_config_path(".other/file.txt"));
        assert!(!sync.is_config_path("notes/hello.md"));
        assert!(!sync.is_config_path("regular.txt"));
    }

    #[test]
    fn test_config_path_sync_config_disabled() {
        let sync = SettingSync::new(
            PathBuf::from("/vault"),
            vec![".obsidian".to_string()],
            false,
            "test".to_string(),
        );

        assert!(sync.is_config_path(".obsidian/app.json"));
        assert!(!sync.is_config_path(".other/file.txt"));
        assert!(!sync.is_config_path("notes/hello.md"));
    }

    #[test]
    fn test_collect_local_settings() {
        let dir = tempdir().unwrap();
        let vault_path = dir.path().to_path_buf();
        
        let obsidian = vault_path.join(".obsidian");
        fs::create_dir_all(&obsidian).unwrap();
        fs::write(obsidian.join("app.json"), r#"{"theme": "dark"}"#).unwrap();
        fs::write(obsidian.join("graph.json"), r##"{"color": "#fff"}"##).unwrap();
        
        let agents = vault_path.join(".agents");
        fs::create_dir_all(&agents).unwrap();
        fs::write(agents.join("config.yaml"), "key: value").unwrap();
        
        fs::write(vault_path.join("note.md"), "# Note").unwrap();
        
        fs::write(obsidian.join("icon.png"), [0x89, 0x50, 0x4E, 0x47]).unwrap();
        
        let sync = SettingSync::new(
            vault_path.clone(),
            vec![".obsidian".to_string(), ".agents".to_string()],
            false,
            "test".to_string(),
        );
        
        let settings = sync.collect_local_settings().unwrap();
        
        assert_eq!(settings.len(), 3);
        
        let paths: Vec<&str> = settings.iter().map(|s| s.path.as_str()).collect();
        assert!(paths.contains(&".obsidian/app.json"));
        assert!(paths.contains(&".obsidian/graph.json"));
        assert!(paths.contains(&".agents/config.yaml"));
        
        assert!(!paths.contains(&".obsidian/icon.png"));
        assert!(!paths.contains(&"note.md"));
    }

    #[test]
    fn test_excluded_paths() {
        let sync = SettingSync::new(
            PathBuf::from("/vault"),
            vec![".obsidian".to_string()],
            true,
            "test".to_string(),
        );

        assert!(sync.is_excluded(".git/config"));
        assert!(sync.is_excluded(".trash/old.md"));
        assert!(!sync.is_excluded(".obsidian/app.json"));
        assert!(!sync.is_excluded("notes/hello.md"));
    }

    #[test]
    fn test_echo_hashes_revert() {
        let dir = tempdir().unwrap();
        let vault_path = dir.path().to_path_buf();
        
        let obsidian = vault_path.join(".obsidian");
        fs::create_dir_all(&obsidian).unwrap();
        
        let file_path = obsidian.join("app.json");
        fs::write(&file_path, r#"{"theme": "dark"}"#).unwrap();
        
        let mut sync = SettingSync::new(
            vault_path.clone(),
            vec![".obsidian".to_string()],
            false,
            "test".to_string(),
        );
        
        // Simulate outbound push with hash_a
        let content_a = r#"{"theme": "dark"}"#;
        let hash_a = hash_content(content_a);
        sync.echo_hashes.insert(".obsidian/app.json".to_string(), hash_a.clone());
        
        // Server sends back different content (hash_b)
        let content_b = r#"{"theme": "light"}"#;
        let hash_b = hash_content(content_b);
        
        let msg = serde_json::json!({
            "data": {
                "path": ".obsidian/app.json",
                "content": content_b,
                "mtime": 0
            }
        });
        sync.handle_setting_modify(&msg).unwrap();
        
        // Verify hash_b is now in echo_hashes (not hash_a)
        assert_eq!(sync.echo_hashes.get(".obsidian/app.json"), Some(&hash_b));
        
        // Now simulate revert: server sends back hash_a
        let msg_revert = serde_json::json!({
            "data": {
                "path": ".obsidian/app.json",
                "content": content_a,
                "mtime": 0
            }
        });
        sync.handle_setting_modify(&msg_revert).unwrap();
        
        // File should be written (not echo-suppressed) because hash_a != hash_b
        let written = fs::read_to_string(&file_path).unwrap();
        assert_eq!(written, content_a);
        
        // And echo_hashes should now have hash_a
        assert_eq!(sync.echo_hashes.get(".obsidian/app.json"), Some(&hash_a));
    }

    #[test]
    fn test_echo_hashes_tombstone_reuse() {
        let dir = tempdir().unwrap();
        let vault_path = dir.path().to_path_buf();
        
        let obsidian = vault_path.join(".obsidian");
        fs::create_dir_all(&obsidian).unwrap();
        
        let file_path = obsidian.join("app.json");
        fs::write(&file_path, r#"{"theme": "dark"}"#).unwrap();
        
        let mut sync = SettingSync::new(
            vault_path.clone(),
            vec![".obsidian".to_string()],
            false,
            "test".to_string(),
        );
        
        // Step 1: Inbound delete sets DELETED marker
        let msg_delete = serde_json::json!({
            "data": {
                "path": ".obsidian/app.json"
            }
        });
        sync.handle_setting_delete(&msg_delete).unwrap();
        
        assert_eq!(
            sync.echo_hashes.get(".obsidian/app.json"),
            Some(&DELETED_MARKER.to_string())
        );
        assert!(!file_path.exists());
        
        // Step 2: Recreate the file (simulating inbound modify)
        let content = r#"{"theme": "light"}"#;
        let hash = hash_content(content);
        
        let msg_recreate = serde_json::json!({
            "data": {
                "path": ".obsidian/app.json",
                "content": content,
                "mtime": 0
            }
        });
        sync.handle_setting_modify(&msg_recreate).unwrap();
        
        assert_eq!(sync.echo_hashes.get(".obsidian/app.json"), Some(&hash));
        assert!(file_path.exists());
        
        // Step 3: Another delete - should NOT be echo-suppressed
        sync.handle_setting_delete(&msg_delete).unwrap();
        
        assert_eq!(
            sync.echo_hashes.get(".obsidian/app.json"),
            Some(&DELETED_MARKER.to_string())
        );
        assert!(!file_path.exists());
    }
}
