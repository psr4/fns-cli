#![allow(dead_code)]

use indicatif::{ProgressBar, ProgressStyle};

/// Manages progress indicators (spinners and progress bars) for sync operations.
pub struct ProgressBarManager {
    progress: Option<ProgressBar>,
}

impl ProgressBarManager {
    pub fn new() -> Self {
        Self { progress: None }
    }

    pub fn start_connection(&mut self, message: &str) {
        let pb = ProgressBar::new_spinner();
        pb.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner} {msg}")
                .unwrap_or_else(|_| ProgressStyle::default_spinner()),
        );
        pb.set_message(message.to_string());
        pb.enable_steady_tick(std::time::Duration::from_millis(100));
        self.progress = Some(pb);
    }

    pub fn finish_connection(&mut self) {
        if let Some(pb) = self.progress.take() {
            pb.finish_with_message("✓ Connected");
        }
    }

    pub fn start_sync(&mut self, total: u64, message: &str) {
        let pb = ProgressBar::new(total);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{msg} [{bar:30}] {pos}/{len} ({eta})")
                .unwrap_or_else(|_| ProgressStyle::default_bar())
                .progress_chars("=>-"),
        );
        pb.set_message(message.to_string());
        self.progress = Some(pb);
    }

    pub fn update_progress(&mut self, current: u64) {
        if let Some(pb) = &self.progress {
            pb.set_position(current);
        }
    }

    pub fn finish_sync(&mut self, message: &str) {
        if let Some(pb) = self.progress.take() {
            pb.finish_with_message(message.to_string());
        }
    }

    /// Abandons the current progress indicator displaying the given error message.
    pub fn abort(&mut self, message: &str) {
        if let Some(pb) = self.progress.take() {
            pb.abandon_with_message(message.to_string());
        }
    }
}

impl Default for ProgressBarManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_progress_manager_creation() {
        let manager = ProgressBarManager::new();
        assert!(manager.progress.is_none());
    }

    #[test]
    fn test_default_implementation() {
        let manager = ProgressBarManager::default();
        assert!(manager.progress.is_none());
    }
}
