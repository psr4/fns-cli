//! File system watcher using the notify crate.
//!
//! Monitors the vault directory for file changes and emits `WatchEvent`s
//! through a channel for processing by the sync engine.

#![allow(dead_code)]

use crate::error::FnsError;
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex};
use tracing::{debug, info, warn};

/// Events emitted by the file watcher.
#[derive(Debug, Clone)]
pub enum WatchEvent {
    /// A new file was created.
    Created(PathBuf),
    /// An existing file was modified.
    Modified(PathBuf),
    /// A file was deleted.
    Deleted(PathBuf),
    /// A file was moved or renamed.
    Moved { from: PathBuf, to: PathBuf },
}

/// File system watcher for the vault directory.
///
/// Uses the `notify` crate to watch for file system events and converts
/// them to `WatchEvent`s sent through a channel.
pub struct FileWatcher {
    /// The underlying notify watcher.
    watcher: RecommendedWatcher,
    /// Path to the vault being watched.
    vault_path: PathBuf,
    /// Sender for processed watch events.
    event_tx: Sender<WatchEvent>,
    /// Receiver for raw notify events.
    raw_rx: Receiver<Result<notify::Event, notify::Error>>,
    /// Patterns to exclude from watching.
    exclude_patterns: Vec<String>,
    /// Known files in the vault (relative paths).
    known_files: Arc<Mutex<HashSet<String>>>,
}

impl FileWatcher {
    /// Creates a new file watcher for the given vault path.
    ///
    /// Returns a tuple of the watcher and a receiver for watch events.
    /// The receiver should be polled to process file system events.
    ///
    /// # Arguments
    ///
    /// * `vault_path` - Path to the vault directory to watch
    /// * `exclude_patterns` - Glob patterns for paths to exclude
    ///
    /// # Errors
    ///
    /// Returns an error if the watcher cannot be created.
    pub fn new(
        vault_path: PathBuf,
        exclude_patterns: Vec<String>,
    ) -> Result<(Self, Receiver<WatchEvent>), FnsError> {
        let (event_tx, event_rx) = channel();
        let (raw_tx, raw_rx) = channel();

        let watcher = RecommendedWatcher::new(raw_tx, notify::Config::default())?;

        let known_files = Arc::new(Mutex::new(HashSet::new()));

        let file_watcher = FileWatcher {
            watcher,
            vault_path,
            event_tx,
            raw_rx,
            exclude_patterns,
            known_files,
        };

        Ok((file_watcher, event_rx))
    }

    /// Starts watching the vault directory.
    ///
    /// The watcher will monitor the directory recursively and send events
    /// through the receiver returned by `new()`.
    ///
    /// # Errors
    ///
    /// Returns an error if watching cannot be started.
    pub fn start(&mut self) -> Result<(), FnsError> {
        info!("Starting file watcher on {:?}", self.vault_path);
        self.watcher
            .watch(&self.vault_path, RecursiveMode::Recursive)?;
        self.initialize_known_files();
        Ok(())
    }

    /// Stops watching the vault directory.
    ///
    /// After calling this, no more events will be sent through the channel.
    pub fn stop(&mut self) {
        info!("Stopping file watcher");
        let _ = self.watcher.unwatch(&self.vault_path);
    }

    /// Returns a reference to the vault path being watched.
    pub fn vault_path(&self) -> &PathBuf {
        &self.vault_path
    }

    /// Checks if a path should be excluded from processing.
    ///
    /// Matches against exclude patterns:
    /// - `.git/**` - matches any path starting with `.git/`
    /// - `.trash/**` - matches any path starting with `.trash/`
    /// - `*.tmp` - matches any file ending with `.tmp`
    /// - `.tmp*` - matches root-level atomic-write temp files
    /// - `.DS_Store` - matches macOS Finder metadata files
    /// - Exact path matches
    pub fn is_excluded(&self, path: &PathBuf) -> bool {
        let rel_path = match path.strip_prefix(&self.vault_path) {
            Ok(rel) => rel,
            Err(_) => path,
        };

        let rel_str = rel_path.to_string_lossy();

        for pattern in &self.exclude_patterns {
            if Self::matches_pattern(&rel_str, pattern) {
                return true;
            }
        }

        let path_str = rel_path.to_string_lossy();
        if path_str.starts_with(".git/") || path_str == ".git" {
            return true;
        }
        if path_str.starts_with(".trash/") || path_str == ".trash" {
            return true;
        }
        if path_str == ".fns_state.json" {
            return true;
        }
        if path_str == ".DS_Store" || path_str.ends_with("/.DS_Store") {
            return true;
        }
        if path_str.starts_with(".tmp") {
            return true;
        }
        if path_str.ends_with(".tmp") || path_str.contains(".tmp.") {
            return true;
        }
        if path_str.contains(".~#") {
            return true;
        }

        false
    }

    /// Initialize the known files set by scanning the vault directory.
    pub fn initialize_known_files(&self) {
        if let Ok(mut known) = self.known_files.lock() {
            known.clear();
            for entry in walkdir::WalkDir::new(&self.vault_path)
                .into_iter()
                .filter_map(|e| e.ok())
            {
                if entry.file_type().is_file() {
                    if let Ok(rel) = entry.path().strip_prefix(&self.vault_path) {
                        if !self.is_excluded(&entry.path().to_path_buf()) {
                            known.insert(rel.to_string_lossy().to_string());
                        }
                    }
                }
            }
        }
    }

    /// Matches a path against a glob-like pattern.
    ///
    /// Supports:
    /// - `**` suffix for recursive matching (e.g., `.git/**`)
    /// - `*` prefix for extension matching (e.g., `*.tmp`)
    /// - `*` suffix for prefix matching (e.g., `.tmp*`)
    /// - Exact matches
    fn matches_pattern(path: &str, pattern: &str) -> bool {
        // Handle recursive patterns like ".git/**"
        if let Some(prefix) = pattern.strip_suffix("/**") {
            return path.starts_with(prefix) || path == prefix;
        }

        // Handle extension patterns like "*.tmp". Treat ".tmp.<suffix>" as
        // excluded too because many editors/agents create transient files such
        // as "note.md.tmp.w_3o8rmv" while writing atomically.
        if let Some(suffix) = pattern.strip_prefix('*') {
            return path.ends_with(suffix) || path.contains(&format!("{}.", suffix));
        }

        // Handle prefix patterns like ".tmp*"
        if let Some(prefix) = pattern.strip_suffix('*') {
            return path.starts_with(prefix);
        }

        // Handle exact matches
        path == pattern
    }

    /// Track a file in the known files set.
    fn track_file(&self, path: &PathBuf) {
        if let Ok(rel) = path.strip_prefix(&self.vault_path) {
            let rel_str = rel.to_string_lossy().to_string();
            if let Ok(mut known) = self.known_files.lock() {
                known.insert(rel_str);
            }
        }
    }

    /// Untrack a file from the known files set.
    fn untrack_file(&self, path: &PathBuf) {
        if let Ok(rel) = path.strip_prefix(&self.vault_path) {
            let rel_str = rel.to_string_lossy().to_string();
            if let Ok(mut known) = self.known_files.lock() {
                known.remove(&rel_str);
            }
        }
    }

    /// Get all known files under a directory.
    fn get_files_under_dir(&self, dir_rel: &str) -> Vec<String> {
        if let Ok(known) = self.known_files.lock() {
            let prefix = if dir_rel.ends_with('/') {
                dir_rel.to_string()
            } else {
                format!("{}/", dir_rel)
            };
            known
                .iter()
                .filter(|f| f.starts_with(&prefix) || *f == dir_rel)
                .cloned()
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Processes a raw notify event and converts it to WatchEvents.
    ///
    /// Returns a vector of WatchEvents (move events may produce multiple).
    pub fn process_event(&self, event: notify::Event) -> Vec<WatchEvent> {
        let mut watch_events = Vec::new();

        if event.paths.iter().all(|path| self.is_excluded(path)) {
            return watch_events;
        }

        debug!(
            "Processing notify event: kind={:?} paths={:?}",
            event.kind, event.paths
        );

        match event.kind {
            notify::EventKind::Create(_) => {
                for path in event.paths {
                    let is_file = path.is_file();
                    let is_excluded = self.is_excluded(&path);
                    debug!(
                        "Create event: path={:?} is_file={} is_excluded={}",
                        path, is_file, is_excluded
                    );
                    if !is_excluded && is_file {
                        self.track_file(&path);
                        watch_events.push(WatchEvent::Created(path));
                    }
                }
            }
            notify::EventKind::Modify(kind) => match kind {
                notify::event::ModifyKind::Name(rename_mode) => match rename_mode {
                    notify::event::RenameMode::To => {
                        for path in event.paths {
                            if !self.is_excluded(&path) && path.is_file() {
                                debug!("Rename To event: {:?}", path);
                                self.track_file(&path);
                                watch_events.push(WatchEvent::Created(path));
                            }
                        }
                    }
                    notify::event::RenameMode::From => {
                        for path in &event.paths {
                            if !self.is_excluded(path) {
                                self.untrack_file(path);
                            }
                        }
                    }
                    notify::event::RenameMode::Both => {
                        if event.paths.len() == 2 {
                            let from = &event.paths[0];
                            let to = &event.paths[1];
                            if !self.is_excluded(from) && !self.is_excluded(to) {
                                self.untrack_file(from);
                                self.track_file(to);
                                watch_events.push(WatchEvent::Moved {
                                    from: from.clone(),
                                    to: to.clone(),
                                });
                            }
                        }
                    }
                    notify::event::RenameMode::Any => {
                        for path in &event.paths {
                            if self.is_excluded(path) {
                                continue;
                            }
                            if path.exists() && path.is_file() {
                                debug!("Rename Any (target): {:?}", path);
                                self.track_file(path);
                                watch_events.push(WatchEvent::Created(path.clone()));
                            } else {
                                debug!("Rename Any (source, now deleted): {:?}", path);
                                if let Ok(rel) = path.strip_prefix(&self.vault_path) {
                                    let rel_str = rel.to_string_lossy();
                                    let victims = self.get_files_under_dir(&rel_str);
                                    if victims.is_empty() {
                                        self.untrack_file(path);
                                        watch_events.push(WatchEvent::Deleted(path.clone()));
                                    } else {
                                        for victim in victims {
                                            if let Ok(mut known) = self.known_files.lock() {
                                                known.remove(&victim);
                                            }
                                            watch_events.push(WatchEvent::Deleted(
                                                self.vault_path.join(&victim),
                                            ));
                                        }
                                    }
                                }
                            }
                        }
                    }
                    _ => {
                        for path in event.paths {
                            if self.is_excluded(&path) {
                                continue;
                            }
                            if path.exists() {
                                watch_events.push(WatchEvent::Modified(path));
                            }
                        }
                    }
                },
                _ => {
                    for path in event.paths {
                        if self.is_excluded(&path) {
                            continue;
                        }
                        if path.exists() {
                            watch_events.push(WatchEvent::Modified(path));
                        }
                    }
                }
            },
            notify::EventKind::Remove(remove_kind) => {
                for path in event.paths {
                    if self.is_excluded(&path) {
                        continue;
                    }
                    match remove_kind {
                        notify::event::RemoveKind::Folder => {
                            // Directory deletion - emit delete for each known file inside
                            if let Ok(rel) = path.strip_prefix(&self.vault_path) {
                                let rel_str = rel.to_string_lossy();
                                let victims = self.get_files_under_dir(&rel_str);
                                for victim in victims {
                                    if let Ok(mut known) = self.known_files.lock() {
                                        known.remove(&victim);
                                    }
                                    watch_events
                                        .push(WatchEvent::Deleted(self.vault_path.join(&victim)));
                                }
                            }
                        }
                        _ => {
                            // File deletion
                            self.untrack_file(&path);
                            watch_events.push(WatchEvent::Deleted(path));
                        }
                    }
                }
            }
            notify::EventKind::Any => {
                // For move events, notify may emit Any with multiple paths.
                // Check if this looks like a move: source doesn't exist, destination does.
                if event.paths.len() == 2 {
                    let from = &event.paths[0];
                    let to = &event.paths[1];

                    if !from.exists() && to.exists() {
                        if !self.is_excluded(from) && !self.is_excluded(to) {
                            watch_events.push(WatchEvent::Moved {
                                from: from.clone(),
                                to: to.clone(),
                            });
                        }
                    }
                }
            }
            _ => {
                debug!("Unhandled event kind: {:?}", event.kind);
            }
        }

        watch_events
    }

    /// Runs the event processing loop.
    ///
    /// This method blocks and processes raw notify events, converting them
    /// to WatchEvents and sending them through the event channel.
    /// Returns when the raw event channel is closed.
    pub fn run(&self) {
        loop {
            match self.raw_rx.recv() {
                Ok(Ok(event)) => {
                    let watch_events = self.process_event(event);
                    for watch_event in watch_events {
                        if let Err(e) = self.event_tx.send(watch_event) {
                            warn!("Failed to send watch event: {}", e);
                            return;
                        }
                    }
                }
                Ok(Err(e)) => {
                    warn!("Watch error: {}", e);
                }
                Err(e) => {
                    info!("Watcher channel closed: {}", e);
                    return;
                }
            }
        }
    }
}

/// Runs the watcher event loop (standalone function for thread spawning).
///
/// This function blocks and processes events from the notify watcher,
/// converting them to WatchEvents and sending them through the channel.
///
/// # Arguments
///
/// * `watcher` - The file watcher to run
/// * `raw_rx` - Receiver for raw notify events
/// * `watch_event_tx` - Sender for processed WatchEvents
pub fn run_watcher_loop(
    watcher: &FileWatcher,
    raw_rx: Receiver<Result<notify::Event, notify::Error>>,
    watch_event_tx: Sender<WatchEvent>,
) {
    loop {
        match raw_rx.recv() {
            Ok(Ok(event)) => {
                let watch_events = watcher.process_event(event);
                for watch_event in watch_events {
                    if let Err(e) = watch_event_tx.send(watch_event) {
                        warn!("Failed to send watch event: {}", e);
                        return;
                    }
                }
            }
            Ok(Err(e)) => {
                warn!("Watch error: {}", e);
            }
            Err(e) => {
                info!("Watcher channel closed: {}", e);
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_matches_pattern_recursive() {
        assert!(FileWatcher::matches_pattern(".git/config", ".git/**"));
        assert!(FileWatcher::matches_pattern(".git/objects/abc", ".git/**"));
        assert!(FileWatcher::matches_pattern(".git", ".git/**"));
        assert!(!FileWatcher::matches_pattern("notes.md", ".git/**"));
    }

    #[test]
    fn test_matches_pattern_extension() {
        assert!(FileWatcher::matches_pattern("file.tmp", "*.tmp"));
        assert!(FileWatcher::matches_pattern("notes.tmp", "*.tmp"));
        assert!(FileWatcher::matches_pattern(
            "notes.md.tmp.w_3o8rmv",
            "*.tmp"
        ));
        assert!(!FileWatcher::matches_pattern("notes.md", "*.tmp"));
    }

    #[test]
    fn test_matches_pattern_exact() {
        assert!(FileWatcher::matches_pattern(
            ".fns_state.json",
            ".fns_state.json"
        ));
        assert!(!FileWatcher::matches_pattern(
            "other.json",
            ".fns_state.json"
        ));
    }

    #[test]
    fn test_is_excluded_default_paths() {
        let vault_path = PathBuf::from("/vault");
        let (watcher, _rx) = FileWatcher::new(vault_path, vec![]).unwrap();

        assert!(watcher.is_excluded(&PathBuf::from("/vault/.git")));
        assert!(watcher.is_excluded(&PathBuf::from("/vault/.git/config")));
        assert!(watcher.is_excluded(&PathBuf::from("/vault/.trash/old.md")));
        assert!(watcher.is_excluded(&PathBuf::from("/vault/.fns_state.json")));
        assert!(watcher.is_excluded(&PathBuf::from("/vault/.DS_Store")));
        assert!(watcher.is_excluded(&PathBuf::from("/vault/notes/.DS_Store")));
        assert!(watcher.is_excluded(&PathBuf::from("/vault/file.tmp")));
        assert!(watcher.is_excluded(&PathBuf::from("/vault/file.md.tmp.w_3o8rmv")));
        assert!(watcher.is_excluded(&PathBuf::from("/vault/.tmpwEYnim")));
        assert!(!watcher.is_excluded(&PathBuf::from("/vault/notes.md")));
    }

    #[test]
    fn test_is_excluded_custom_patterns() {
        let vault_path = PathBuf::from("/vault");
        let patterns = vec![
            "*.tmp".to_string(),
            "drafts/**".to_string(),
            ".obsidian/**".to_string(),
        ];
        let (watcher, _rx) = FileWatcher::new(vault_path, patterns).unwrap();

        assert!(watcher.is_excluded(&PathBuf::from("/vault/file.tmp")));
        assert!(watcher.is_excluded(&PathBuf::from("/vault/file.md.tmp.w_3o8rmv")));
        assert!(watcher.is_excluded(&PathBuf::from("/vault/file.md.~#0")));
        assert!(watcher.is_excluded(&PathBuf::from("/vault/drafts/note.md")));
        assert!(watcher.is_excluded(&PathBuf::from(
            "/vault/.obsidian/plugins/fast-note-sync/data.json"
        )));
        assert!(!watcher.is_excluded(&PathBuf::from("/vault/notes.md")));
    }

    #[test]
    fn test_process_event_ignores_tmp_dotfile() {
        let vault_path = PathBuf::from("/vault");
        let (watcher, _rx) = FileWatcher::new(vault_path, vec![]).unwrap();
        let event = notify::Event {
            kind: notify::EventKind::Modify(notify::event::ModifyKind::Metadata(
                notify::event::MetadataKind::Any,
            )),
            paths: vec![PathBuf::from("/vault/.tmpwEYnim")],
            attrs: notify::event::EventAttributes::new(),
        };

        assert!(watcher.process_event(event).is_empty());
    }
}
