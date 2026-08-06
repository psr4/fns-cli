use std::path::Path;

use serde::{Deserialize, Serialize};
use tempfile::NamedTempFile;
use tracing::warn;

/// Persistent sync state tracking last sync times for different content types.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SyncState {
    /// Unix timestamp (seconds) of last note sync
    pub last_note_sync_time: i64,
    /// Unix timestamp (seconds) of last file sync
    pub last_file_sync_time: i64,
    /// Unix timestamp (seconds) of last setting sync
    pub last_setting_sync_time: i64,
}

impl SyncState {
    /// Load sync state from file. Returns default if file doesn't exist or is corrupted.
    pub fn load(path: &Path) -> Self {
        if !path.exists() {
            return Self::default();
        }

        match std::fs::read_to_string(path) {
            Ok(content) => match serde_json::from_str(&content) {
                Ok(state) => state,
                Err(e) => {
                    warn!("Failed to parse state file {:?}: {}", path, e);
                    Self::default()
                }
            },
            Err(e) => {
                warn!("Failed to read state file {:?}: {}", path, e);
                Self::default()
            }
        }
    }

    /// Save sync state to file atomically (write to temp, then rename).
    pub fn save(&self, path: &Path) -> Result<(), crate::error::FnsError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let temp_file = NamedTempFile::new_in(path.parent().unwrap_or_else(|| Path::new(".")))?;

        serde_json::to_writer_pretty(&temp_file, self)?;

        temp_file
            .persist(path)
            .map_err(|e| crate::error::FnsError::Io { source: e.error })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_state() {
        let state = SyncState::default();
        assert_eq!(state.last_note_sync_time, 0);
        assert_eq!(state.last_file_sync_time, 0);
        assert_eq!(state.last_setting_sync_time, 0);
    }

    #[test]
    fn test_json_serialization() {
        let state = SyncState {
            last_note_sync_time: 1234567890,
            last_file_sync_time: 1234567890,
            last_setting_sync_time: 1234567890,
        };
        let json = serde_json::to_string(&state).unwrap();
        assert_eq!(
            json,
            r#"{"last_note_sync_time":1234567890,"last_file_sync_time":1234567890,"last_setting_sync_time":1234567890}"#
        );
    }

    #[test]
    fn test_json_deserialization() {
        let json = r#"{"last_note_sync_time":1234567890,"last_file_sync_time":1234567890,"last_setting_sync_time":1234567890}"#;
        let state: SyncState = serde_json::from_str(json).unwrap();
        assert_eq!(state.last_note_sync_time, 1234567890);
        assert_eq!(state.last_file_sync_time, 1234567890);
        assert_eq!(state.last_setting_sync_time, 1234567890);
    }

    #[test]
    fn test_load_save_roundtrip() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("state.json");

        let original = SyncState {
            last_note_sync_time: 1111111111,
            last_file_sync_time: 2222222222,
            last_setting_sync_time: 3333333333,
        };

        original.save(&path).unwrap();
        let loaded = SyncState::load(&path);

        assert_eq!(loaded.last_note_sync_time, original.last_note_sync_time);
        assert_eq!(loaded.last_file_sync_time, original.last_file_sync_time);
        assert_eq!(
            loaded.last_setting_sync_time,
            original.last_setting_sync_time
        );
    }

    #[test]
    fn test_load_missing_file_returns_default() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("nonexistent.json");

        let loaded = SyncState::load(&path);

        assert_eq!(loaded.last_note_sync_time, 0);
        assert_eq!(loaded.last_file_sync_time, 0);
        assert_eq!(loaded.last_setting_sync_time, 0);
    }

    #[test]
    fn test_load_corrupted_file_returns_default() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("corrupted.json");

        std::fs::write(&path, "not valid json {{{").unwrap();

        let loaded = SyncState::load(&path);

        assert_eq!(loaded.last_note_sync_time, 0);
        assert_eq!(loaded.last_file_sync_time, 0);
        assert_eq!(loaded.last_setting_sync_time, 0);
    }
}
