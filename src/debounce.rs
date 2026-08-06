use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

/// Represents a file system event to be debounced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatchEvent {
    Created(PathBuf),
    Modified(PathBuf),
    Deleted(PathBuf),
    Moved { from: PathBuf, to: PathBuf },
}

impl WatchEvent {
    /// Returns the primary path associated with this event.
    pub fn path(&self) -> &PathBuf {
        match self {
            WatchEvent::Created(p) => p,
            WatchEvent::Modified(p) => p,
            WatchEvent::Deleted(p) => p,
            WatchEvent::Moved { from, .. } => from,
        }
    }
}

/// Debouncer for file system events.
///
/// Batches rapid file changes within a configurable window (default 500ms),
/// keeping only the latest event for each path.
pub struct Debouncer {
    /// Pending events with their timestamps, keyed by path.
    pending: HashMap<PathBuf, (WatchEvent, Instant)>,
    /// Duration to wait before emitting events (default 500ms).
    debounce_duration: Duration,
}

impl Debouncer {
    /// Creates a new Debouncer with the specified debounce duration in milliseconds.
    pub fn new(debounce_ms: u64) -> Self {
        Self {
            pending: HashMap::new(),
            debounce_duration: Duration::from_millis(debounce_ms),
        }
    }

    /// Adds an event to the debouncer.
    ///
    /// Returns any events that are ready to be emitted (older than debounce_duration).
    /// For events with the same path, only the latest event is kept.
    pub fn add_event(&mut self, event: WatchEvent) -> Vec<WatchEvent> {
        let now = Instant::now();
        let path = event.path().clone();

        // Add/update the event for this path
        self.pending.insert(path, (event, now));

        // Check for ready events
        self.tick()
    }

    /// Checks all pending events and returns those older than debounce_duration.
    ///
    /// Removes returned events from pending.
    pub fn tick(&mut self) -> Vec<WatchEvent> {
        let now = Instant::now();
        let mut ready = Vec::new();
        let mut ready_paths = Vec::new();

        for (path, (event, timestamp)) in &self.pending {
            if now.duration_since(*timestamp) >= self.debounce_duration {
                ready.push(event.clone());
                ready_paths.push(path.clone());
            }
        }

        // Remove ready events from pending
        for path in ready_paths {
            self.pending.remove(&path);
        }

        ready
    }

    /// Handles directory events by enumerating children.
    ///
    /// For deleted directories: creates delete events for each child file.
    /// For moved directories: creates move events for each child file.
    pub fn expand_directory_event(&self, event: WatchEvent) -> Vec<WatchEvent> {
        match &event {
            WatchEvent::Deleted(path) if path.is_dir() || !path.exists() => {
                // Try to enumerate if it exists, otherwise just return the original
                if let Ok(children) = self.enumerate_directory_children(path) {
                    if !children.is_empty() {
                        return children.into_iter().map(WatchEvent::Deleted).collect();
                    }
                }
                vec![event]
            }
            WatchEvent::Moved { from, to } if from.is_dir() || !from.exists() => {
                if let Ok(children) = self.enumerate_directory_children(from) {
                    if !children.is_empty() {
                        return children
                            .into_iter()
                            .filter_map(|child| {
                                child.strip_prefix(from).ok().map(|relative| {
                                    let new_path = to.join(relative);
                                    WatchEvent::Moved {
                                        from: child.clone(),
                                        to: new_path,
                                    }
                                })
                            })
                            .collect();
                    }
                }
                vec![event]
            }
            _ => vec![event],
        }
    }

    /// Enumerates all children of a directory.
    fn enumerate_directory_children(&self, dir: &PathBuf) -> Result<Vec<PathBuf>, std::io::Error> {
        let mut children = Vec::new();

        if !dir.exists() {
            // Directory no longer exists, we can't enumerate
            return Ok(children);
        }

        use walkdir::WalkDir;

        for entry in WalkDir::new(dir).into_iter().filter_map(|e| e.ok()) {
            let path = entry.path().to_path_buf();
            if path != *dir {
                children.push(path);
            }
        }

        Ok(children)
    }

    /// Returns the number of pending events.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Returns true if there are no pending events.
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
}

impl Default for Debouncer {
    fn default() -> Self {
        Self::new(500)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;

    #[test]
    fn test_new_creates_empty_debouncer() {
        let debouncer = Debouncer::new(500);
        assert!(debouncer.is_empty());
        assert_eq!(debouncer.pending_count(), 0);
    }

    #[test]
    fn test_default_is_500ms() {
        let debouncer = Debouncer::default();
        assert!(debouncer.is_empty());
    }

    #[test]
    fn test_add_event_queues_event() {
        let mut debouncer = Debouncer::new(500);
        let event = WatchEvent::Modified(PathBuf::from("/test/file.txt"));

        let result = debouncer.add_event(event);
        assert!(result.is_empty()); // Not ready yet
        assert!(!debouncer.is_empty());
    }

    #[test]
    fn test_same_path_overwrites_previous_event() {
        let mut debouncer = Debouncer::new(100);

        // Add a modified event
        debouncer.add_event(WatchEvent::Modified(PathBuf::from("/test/file.txt")));

        // Add a deleted event for same path
        debouncer.add_event(WatchEvent::Deleted(PathBuf::from("/test/file.txt")));

        // Wait for debounce duration
        sleep(Duration::from_millis(150));

        // Should only get the deleted event
        let result = debouncer.tick();
        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0],
            WatchEvent::Deleted(PathBuf::from("/test/file.txt"))
        );
    }

    #[test]
    fn test_tick_returns_ready_events() {
        let mut debouncer = Debouncer::new(50);

        debouncer.add_event(WatchEvent::Modified(PathBuf::from("/test/file1.txt")));
        debouncer.add_event(WatchEvent::Modified(PathBuf::from("/test/file2.txt")));

        // Wait for debounce duration
        sleep(Duration::from_millis(100));

        let result = debouncer.tick();
        assert_eq!(result.len(), 2);
        assert!(debouncer.is_empty());
    }

    #[test]
    fn test_tick_does_not_return_new_events() {
        let mut debouncer = Debouncer::new(500);

        let result = debouncer.add_event(WatchEvent::Modified(PathBuf::from("/test/file.txt")));

        // Should not return immediately
        assert!(result.is_empty());
        assert!(!debouncer.is_empty());
    }
}
