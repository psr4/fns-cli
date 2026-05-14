//! Folder sync protocol: apply server-pushed folder create/delete/rename events.

#![allow(dead_code)]

use std::fs;
use std::path::PathBuf;

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::protocol::Message;
use tracing::{debug, info};

use crate::error::FnsError;
use crate::protocol::{
    decode_message, encode_message, Action, ClientAction, FolderSyncRequest, FolderSyncCheck,
    ServerAction,
};
use crate::state::SyncState;
use crate::ws_client::WsStream;

pub struct FolderSync {
    pub vault_path: PathBuf,
    pub state: SyncState,
    vault: String,
    config_dirs: Vec<String>,
}

impl FolderSync {
    pub fn new(
        vault_path: PathBuf,
        _state: SyncState,
        vault: String,
        config_dirs: Vec<String>,
    ) -> Self {
        Self {
            vault_path,
            state: SyncState::default(),
            vault,
            config_dirs,
        }
    }

    pub async fn sync(&mut self, ws: &mut WsStream, last_sync_time: i64) -> Result<(), FnsError> {
        let folders = self.collect_local_folders()?;
        let context = uuid::Uuid::new_v4().to_string();

        let request = FolderSyncRequest {
            vault: self.vault.clone(),
            last_time: last_sync_time,
            folders,
            context: Some(context),
        };

        let msg = encode_message(&Action::Client(ClientAction::FolderSync), &request)?;
        info!(
            last_time = last_sync_time,
            folder_count = request.folders.len(),
            "Requesting FolderSync"
        );

        ws.send(Message::Text(msg.into()))
            .await
            .map_err(|e| FnsError::WebSocket {
                message: format!("Failed to send FolderSync: {}", e),
            })?;

        self.process_sync_responses(ws).await
    }

    async fn process_sync_responses(&mut self, ws: &mut WsStream) -> Result<(), FnsError> {
        loop {
            let msg = ws
                .next()
                .await
                .ok_or_else(|| FnsError::WebSocket {
                    message: "WebSocket closed during folder sync".to_string(),
                })?
                .map_err(|e| FnsError::WebSocket {
                    message: format!("WebSocket error during folder sync: {}", e),
                })?;

            match msg {
                Message::Text(text) => {
                    let (action, data) =
                        decode_message(&text).map_err(|e| FnsError::Protocol {
                            message: format!("Failed to decode message: {}", e),
                        })?;

                    match action {
                        Action::Server(ServerAction::FolderSyncModify) => {
                            self.handle_folder_create(&data)?;
                        }
                        Action::Server(ServerAction::FolderSyncDelete) => {
                            self.handle_folder_delete(&data)?;
                        }
                        Action::Server(ServerAction::FolderSyncRename) => {
                            self.handle_folder_rename(&data)?;
                        }
                        Action::Server(ServerAction::FolderSyncEnd) => {
                            self.handle_sync_end(&data)?;
                            return Ok(());
                        }
                        _ => {
                            debug!(action = ?action, "Ignoring unexpected action during folder sync");
                        }
                    }
                }
                Message::Close(frame) => {
                    let reason = frame
                        .map(|f| f.to_string())
                        .unwrap_or_else(|| "no reason".to_string());
                    return Err(FnsError::WebSocket {
                        message: format!("WebSocket closed during folder sync: {}", reason),
                    });
                }
                _ => {
                    debug!("Ignoring non-text message during folder sync");
                }
            }
        }
    }

    pub fn handle_folder_create(&mut self, msg_data: &serde_json::Value) -> Result<(), FnsError> {
        let data = extract_inner(msg_data);

        let rel_path: String = data
            .get("path")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        if rel_path.is_empty() {
            return Ok(());
        }

        if self.is_config_dir(&rel_path) {
            debug!(path = %rel_path, "Ignoring FolderSyncModify for config dir");
            return Ok(());
        }

        let full = self.vault_path.join(&rel_path);
        fs::create_dir_all(&full)?;
        info!(path = %rel_path, "<- FolderSyncModify applied");
        Ok(())
    }

    pub fn handle_folder_delete(&mut self, msg_data: &serde_json::Value) -> Result<(), FnsError> {
        let data = extract_inner(msg_data);

        let rel_path: String = data
            .get("path")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        if rel_path.is_empty() {
            return Ok(());
        }

        if self.is_config_dir(&rel_path) {
            debug!(path = %rel_path, "Ignoring FolderSyncDelete for config dir");
            return Ok(());
        }

        let full = self.vault_path.join(&rel_path);
        if full.exists() {
            fs::remove_dir_all(&full)?;
            info!(path = %rel_path, "<- FolderSyncDelete applied");
            self.cleanup_empty_parents(&full);
        }

        Ok(())
    }

    pub fn handle_folder_rename(
        &mut self,
        msg_data: &serde_json::Value,
    ) -> Result<(), FnsError> {
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

        if self.is_config_dir(&old_path) || self.is_config_dir(&new_path) {
            debug!(
                old = %old_path,
                new = %new_path,
                "Ignoring FolderSyncRename for config dir"
            );
            return Ok(());
        }

        let old_full = self.vault_path.join(&old_path);
        let new_full = self.vault_path.join(&new_path);

        if let Some(parent) = new_full.parent() {
            fs::create_dir_all(parent)?;
        }

        if old_full.exists() {
            fs::rename(&old_full, &new_full)?;
            info!(old = %old_path, new = %new_path, "<- FolderSyncRename applied");
            self.cleanup_empty_parents(&old_full);
        } else {
            fs::create_dir_all(&new_full)?;
            info!(new = %new_path, "<- FolderSyncRename: old path missing, created new dir");
        }

        Ok(())
    }

    fn handle_sync_end(&mut self, msg_data: &serde_json::Value) -> Result<(), FnsError> {
        let data = extract_inner(msg_data);

        let last_time: i64 = data
            .get("lastTime")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        let need_modify_count: i64 = data
            .get("needModifyCount")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        let need_delete_count: i64 = data
            .get("needDeleteCount")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        info!(
            last_time = last_time,
            need_modify = need_modify_count,
            need_delete = need_delete_count,
            "<- FolderSyncEnd"
        );

        Ok(())
    }

    fn is_config_dir(&self, rel_path: &str) -> bool {
        let first = rel_path.split('/').next().unwrap_or("");
        if !first.starts_with('.') {
            return false;
        }
        self.config_dirs.iter().any(|d| d == first)
    }

    fn cleanup_empty_parents(&self, deleted_path: &std::path::Path) {
        let mut current = deleted_path.parent();

        while let Some(p) = current {
            if p == self.vault_path {
                break;
            }

            if p.exists() && p.is_dir() {
                if let Ok(mut entries) = fs::read_dir(p) {
                    if entries.next().is_none() {
                        if let Err(e) = fs::remove_dir(p) {
                            debug!(path = %p.display(), error = %e, "Failed to remove empty parent dir");
                            break;
                        }
                    } else {
                        break;
                    }
                }
            }

            current = p.parent();
        }
    }

    fn collect_local_folders(&self) -> Result<Vec<FolderSyncCheck>, FnsError> {
        let mut folders = Vec::new();

        if !self.vault_path.exists() {
            return Ok(folders);
        }

        for entry in walkdir::WalkDir::new(&self.vault_path)
            .follow_links(false)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            let path = entry.path();

            if !path.is_dir() {
                continue;
            }

            let rel = match path.strip_prefix(&self.vault_path) {
                Ok(r) => r.to_string_lossy().to_string(),
                Err(_) => continue,
            };

            if rel.is_empty() {
                continue;
            }

            if self.is_config_dir(&rel) {
                continue;
            }

            let metadata = match fs::metadata(path) {
                Ok(m) => m,
                Err(_) => continue,
            };

            let mtime = metadata
                .modified()
                .unwrap_or(std::time::SystemTime::UNIX_EPOCH)
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;

            folders.push(FolderSyncCheck {
                path: rel,
                path_hash: crate::hash::hash_path(&folders.last().map_or(String::new(), |f| f.path.clone())),
                mtime,
            });
        }

        let folders: Vec<FolderSyncCheck> = folders
            .into_iter()
            .map(|mut f| {
                f.path_hash = crate::hash::hash_path(&f.path);
                f
            })
            .collect();

        Ok(folders)
    }
}

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
    fn test_is_config_dir() {
        let sync = FolderSync::new(
            PathBuf::from("/vault"),
            SyncState::default(),
            "test".to_string(),
            vec![".obsidian".to_string(), ".agents".to_string()],
        );

        assert!(sync.is_config_dir(".obsidian"));
        assert!(sync.is_config_dir(".agents"));
        assert!(sync.is_config_dir(".obsidian/themes"));
        assert!(sync.is_config_dir(".agents/config"));
        assert!(!sync.is_config_dir("notes"));
        assert!(!sync.is_config_dir("folder/subfolder"));
        assert!(!sync.is_config_dir("regular-dir"));
    }

    #[test]
    fn test_handle_folder_create() {
        let dir = tempdir().unwrap();
        let vault_path = dir.path().to_path_buf();

        let mut sync = FolderSync::new(
            vault_path.clone(),
            SyncState::default(),
            "test".to_string(),
            vec![".obsidian".to_string()],
        );

        let msg = serde_json::json!({
            "data": {
                "path": "notes/project"
            }
        });

        sync.handle_folder_create(&msg).unwrap();
        assert!(vault_path.join("notes/project").exists());
    }

    #[test]
    fn test_handle_folder_create_config_dir_skipped() {
        let dir = tempdir().unwrap();
        let vault_path = dir.path().to_path_buf();

        let mut sync = FolderSync::new(
            vault_path.clone(),
            SyncState::default(),
            "test".to_string(),
            vec![".obsidian".to_string()],
        );

        let msg = serde_json::json!({
            "data": {
                "path": ".obsidian/themes"
            }
        });

        sync.handle_folder_create(&msg).unwrap();
        assert!(!vault_path.join(".obsidian/themes").exists());
    }

    #[test]
    fn test_handle_folder_delete() {
        let dir = tempdir().unwrap();
        let vault_path = dir.path().to_path_buf();

        fs::create_dir_all(vault_path.join("notes/project")).unwrap();
        fs::write(vault_path.join("notes/project/note.md"), "hello").unwrap();

        let mut sync = FolderSync::new(
            vault_path.clone(),
            SyncState::default(),
            "test".to_string(),
            vec![],
        );

        let msg = serde_json::json!({
            "data": {
                "path": "notes/project"
            }
        });

        sync.handle_folder_delete(&msg).unwrap();
        assert!(!vault_path.join("notes/project").exists());
    }

    #[test]
    fn test_handle_folder_delete_config_dir_skipped() {
        let dir = tempdir().unwrap();
        let vault_path = dir.path().to_path_buf();

        fs::create_dir_all(vault_path.join(".obsidian")).unwrap();

        let mut sync = FolderSync::new(
            vault_path.clone(),
            SyncState::default(),
            "test".to_string(),
            vec![".obsidian".to_string()],
        );

        let msg = serde_json::json!({
            "data": {
                "path": ".obsidian"
            }
        });

        sync.handle_folder_delete(&msg).unwrap();
        assert!(vault_path.join(".obsidian").exists());
    }

    #[test]
    fn test_handle_folder_rename() {
        let dir = tempdir().unwrap();
        let vault_path = dir.path().to_path_buf();

        fs::create_dir_all(vault_path.join("old-folder")).unwrap();
        fs::write(vault_path.join("old-folder/note.md"), "hello").unwrap();

        let mut sync = FolderSync::new(
            vault_path.clone(),
            SyncState::default(),
            "test".to_string(),
            vec![],
        );

        let msg = serde_json::json!({
            "data": {
                "oldPath": "old-folder",
                "path": "new-folder"
            }
        });

        sync.handle_folder_rename(&msg).unwrap();
        assert!(vault_path.join("new-folder/note.md").exists());
        assert!(!vault_path.join("old-folder").exists());
    }

    #[test]
    fn test_handle_folder_rename_config_dir_skipped() {
        let dir = tempdir().unwrap();
        let vault_path = dir.path().to_path_buf();

        fs::create_dir_all(vault_path.join(".obsidian")).unwrap();

        let mut sync = FolderSync::new(
            vault_path.clone(),
            SyncState::default(),
            "test".to_string(),
            vec![".obsidian".to_string()],
        );

        let msg = serde_json::json!({
            "data": {
                "oldPath": ".obsidian",
                "path": "obsidian-backup"
            }
        });

        sync.handle_folder_rename(&msg).unwrap();
        assert!(vault_path.join(".obsidian").exists());
        assert!(!vault_path.join("obsidian-backup").exists());
    }

    #[test]
    fn test_handle_folder_rename_missing_old_creates_new() {
        let dir = tempdir().unwrap();
        let vault_path = dir.path().to_path_buf();

        let mut sync = FolderSync::new(
            vault_path.clone(),
            SyncState::default(),
            "test".to_string(),
            vec![],
        );

        let msg = serde_json::json!({
            "data": {
                "oldPath": "nonexistent",
                "path": "created-folder"
            }
        });

        sync.handle_folder_rename(&msg).unwrap();
        assert!(vault_path.join("created-folder").exists());
    }

    #[test]
    fn test_cleanup_empty_parents() {
        let dir = tempdir().unwrap();
        let vault_path = dir.path().to_path_buf();

        fs::create_dir_all(vault_path.join("a/b/c")).unwrap();
        fs::remove_dir(vault_path.join("a/b/c")).unwrap();

        let sync = FolderSync::new(
            vault_path.clone(),
            SyncState::default(),
            "test".to_string(),
            vec![],
        );

        sync.cleanup_empty_parents(&vault_path.join("a/b/c"));

        assert!(!vault_path.join("a").exists());
    }

    #[test]
    fn test_cleanup_empty_parents_stops_at_vault_root() {
        let dir = tempdir().unwrap();
        let vault_path = dir.path().to_path_buf();

        fs::create_dir_all(vault_path.join("empty-dir")).unwrap();
        fs::remove_dir(vault_path.join("empty-dir")).unwrap();

        let sync = FolderSync::new(
            vault_path.clone(),
            SyncState::default(),
            "test".to_string(),
            vec![],
        );

        sync.cleanup_empty_parents(&vault_path.join("empty-dir"));

        assert!(vault_path.exists());
    }
}
