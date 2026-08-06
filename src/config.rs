use crate::error::FnsError;
use serde::{Deserialize, Serialize};
use std::env;
use std::path::PathBuf;

/// Default directories treated as config / settings sync
pub const DEFAULT_CONFIG_SYNC_DIRS: [&str; 2] = [".obsidian", ".agents"];

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServerConfig {
    #[serde(default)]
    pub api: String,
    #[serde(default)]
    pub token: String,
    #[serde(default = "default_vault")]
    pub vault: String,
}

fn default_vault() -> String {
    "defaultVault".to_string()
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            api: String::new(),
            token: String::new(),
            vault: default_vault(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SyncConfig {
    #[serde(default = "default_watch_path")]
    pub watch_path: String,
    #[serde(default = "default_true")]
    pub sync_notes: bool,
    #[serde(default = "default_true")]
    pub sync_files: bool,
    #[serde(default = "default_true")]
    pub sync_config: bool,
    #[serde(default = "default_upload_concurrency")]
    pub upload_concurrency: usize,
    #[serde(default = "default_exclude_patterns")]
    pub exclude_patterns: Vec<String>,
    #[serde(default = "default_file_chunk_size")]
    pub file_chunk_size: usize,
    #[serde(default = "default_config_sync_dirs")]
    pub config_sync_dirs: Vec<String>,
}

fn default_watch_path() -> String {
    "./vault".to_string()
}

fn default_true() -> bool {
    true
}

fn default_upload_concurrency() -> usize {
    2
}

fn default_exclude_patterns() -> Vec<String> {
    vec![
        ".git/**".to_string(),
        ".trash/**".to_string(),
        "*.tmp".to_string(),
        ".tmp*".to_string(),
    ]
}

fn default_file_chunk_size() -> usize {
    524288
}

fn default_config_sync_dirs() -> Vec<String> {
    DEFAULT_CONFIG_SYNC_DIRS
        .iter()
        .map(|s| s.to_string())
        .collect()
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            watch_path: default_watch_path(),
            sync_notes: true,
            sync_files: true,
            sync_config: true,
            upload_concurrency: 2,
            exclude_patterns: default_exclude_patterns(),
            file_chunk_size: default_file_chunk_size(),
            config_sync_dirs: default_config_sync_dirs(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClientConfig {
    #[serde(default = "default_reconnect_max_retries")]
    pub reconnect_max_retries: u32,
    #[serde(default = "default_reconnect_base_delay")]
    pub reconnect_base_delay: u64,
    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval: u64,
}

fn default_reconnect_max_retries() -> u32 {
    15
}

fn default_reconnect_base_delay() -> u64 {
    3
}

fn default_heartbeat_interval() -> u64 {
    30
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            reconnect_max_retries: 15,
            reconnect_base_delay: 3,
            heartbeat_interval: 30,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LoggingConfig {
    #[serde(default = "default_log_level")]
    pub level: String,
    #[serde(default)]
    pub file: String,
}

fn default_log_level() -> String {
    "INFO".to_string()
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            file: String::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppConfig {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub sync: SyncConfig,
    #[serde(default)]
    pub client: ClientConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            sync: SyncConfig::default(),
            client: ClientConfig::default(),
            logging: LoggingConfig::default(),
        }
    }
}

impl AppConfig {
    /// Converts the HTTP/HTTPS API URL to WebSocket URL
    pub fn ws_api(&self) -> String {
        let url = self.server.api.trim_end_matches('/');
        if let Some(rest) = url.strip_prefix("https://") {
            format!("wss://{}", rest)
        } else if let Some(rest) = url.strip_prefix("http://") {
            format!("ws://{}", rest)
        } else {
            url.to_string()
        }
    }

    /// Resolves the watch_path to an absolute path
    pub fn vault_path(&self) -> PathBuf {
        PathBuf::from(&self.sync.watch_path)
            .canonicalize()
            .unwrap_or_else(|_| PathBuf::from(&self.sync.watch_path))
    }

    /// Load config from a YAML file with environment variable overrides
    ///
    /// Environment variables:
    /// - `FNS_API`: Overrides `server.api` if set
    /// - `FNS_TOKEN`: Overrides `server.token` if set
    ///
    /// Validation:
    /// - `server.api` must be non-empty (from config or FNS_API)
    /// - `server.token` must be non-empty (from config or FNS_TOKEN)
    pub fn load(path: &str) -> Result<Self, FnsError> {
        let content = std::fs::read_to_string(path)?;
        let mut config: AppConfig = serde_yaml::from_str(&content)?;

        if let Ok(api) = env::var("FNS_API") {
            if !api.is_empty() {
                config.server.api = api;
            }
        }

        if let Ok(token) = env::var("FNS_TOKEN") {
            if !token.is_empty() {
                config.server.token = token;
            }
        }

        if config.server.api.is_empty() {
            return Err(FnsError::Config {
                message:
                    "server.api is required (set in config or via FNS_API environment variable)"
                        .to_string(),
            });
        }

        if config.server.token.is_empty() {
            return Err(FnsError::Config {
                message:
                    "server.token is required (set in config or via FNS_TOKEN environment variable)"
                        .to_string(),
            });
        }

        if config.server.vault.is_empty() {
            return Err(FnsError::Config {
                message: "server.vault is required".to_string(),
            });
        }

        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn test_ws_api_https() {
        let mut config = AppConfig::default();
        config.server.api = "https://server.example.com".to_string();
        assert_eq!(config.ws_api(), "wss://server.example.com");
    }

    #[test]
    fn test_ws_api_http() {
        let mut config = AppConfig::default();
        config.server.api = "http://localhost:8080".to_string();
        assert_eq!(config.ws_api(), "ws://localhost:8080");
    }

    #[test]
    fn test_ws_api_trailing_slash() {
        let mut config = AppConfig::default();
        config.server.api = "https://server.example.com/".to_string();
        assert_eq!(config.ws_api(), "wss://server.example.com");
    }

    #[test]
    fn test_default_values() {
        let config = AppConfig::default();
        assert_eq!(config.server.vault, "defaultVault");
        assert_eq!(config.sync.watch_path, "./vault");
        assert!(config.sync.sync_notes);
        assert_eq!(config.sync.upload_concurrency, 2);
        assert_eq!(config.sync.file_chunk_size, 524288);
        assert_eq!(config.client.reconnect_max_retries, 15);
        assert_eq!(config.logging.level, "INFO");
    }

    #[test]
    fn test_load_missing_file() {
        let result = AppConfig::load("/nonexistent/config.yaml");
        assert!(result.is_err());
    }

    #[test]
    fn test_load_valid_config() {
        use std::io::Write;
        let _lock = ENV_LOCK.lock().unwrap();
        let temp_dir = std::env::temp_dir();
        let config_path = temp_dir.join("fns_test_config.yaml");
        let mut file = std::fs::File::create(&config_path).unwrap();
        writeln!(
            file,
            r#"
server:
  api: "https://api.example.com"
  token: "test-token"
  vault: "myVault"
sync:
  watch_path: "/path/to/vault"
"#
        )
        .unwrap();
        drop(file);

        // SAFETY: Test isolation - clear env vars immediately before load
        unsafe {
            env::remove_var("FNS_API");
            env::remove_var("FNS_TOKEN");
        }
        let config = AppConfig::load(config_path.to_str().unwrap()).unwrap();
        assert_eq!(config.server.api, "https://api.example.com");
        assert_eq!(config.server.token, "test-token");
        assert_eq!(config.server.vault, "myVault");
        assert_eq!(config.sync.watch_path, "/path/to/vault");

        let _ = std::fs::remove_file(&config_path);
    }

    #[test]
    fn test_load_missing_required_fields() {
        use std::io::Write;
        let _lock = ENV_LOCK.lock().unwrap();
        let temp_dir = std::env::temp_dir();
        let config_path = temp_dir.join("fns_test_config_missing.yaml");
        let mut file = std::fs::File::create(&config_path).unwrap();
        writeln!(
            file,
            r#"
server:
  api: ""
  token: ""
  vault: "myVault"
"#
        )
        .unwrap();
        drop(file);

        // SAFETY: Test isolation - clear env vars immediately before load
        unsafe {
            env::remove_var("FNS_API");
            env::remove_var("FNS_TOKEN");
        }
        let result = AppConfig::load(config_path.to_str().unwrap());
        assert!(result.is_err());

        let _ = std::fs::remove_file(&config_path);
    }

    #[test]
    fn test_env_override_api() {
        use std::io::Write;
        let _lock = ENV_LOCK.lock().unwrap();
        let temp_dir = std::env::temp_dir();
        let config_path = temp_dir.join("fns_test_env_override.yaml");
        let mut file = std::fs::File::create(&config_path).unwrap();
        writeln!(
            file,
            r#"
server:
  api: "https://config-api.example.com"
  token: "test-token"
  vault: "myVault"
"#
        )
        .unwrap();
        drop(file);

        // SAFETY: Test isolation - set env var, load, then cleanup
        unsafe {
            env::set_var("FNS_API", "https://env-api.example.com");
        }
        let config = AppConfig::load(config_path.to_str().unwrap()).unwrap();
        assert_eq!(config.server.api, "https://env-api.example.com");
        // SAFETY: Test cleanup
        unsafe {
            env::remove_var("FNS_API");
        }

        let _ = std::fs::remove_file(&config_path);
    }

    #[test]
    fn test_env_override_token() {
        use std::io::Write;
        let _lock = ENV_LOCK.lock().unwrap();
        let temp_dir = std::env::temp_dir();
        let config_path = temp_dir.join("fns_test_env_token.yaml");
        let mut file = std::fs::File::create(&config_path).unwrap();
        writeln!(
            file,
            r#"
server:
  api: "https://api.example.com"
  token: "config-token"
  vault: "myVault"
"#
        )
        .unwrap();
        drop(file);

        // SAFETY: Test isolation - set env var, load, then cleanup
        unsafe {
            env::set_var("FNS_TOKEN", "env-token");
        }
        let config = AppConfig::load(config_path.to_str().unwrap()).unwrap();
        assert_eq!(config.server.token, "env-token");
        // SAFETY: Test cleanup
        unsafe {
            env::remove_var("FNS_TOKEN");
        }

        let _ = std::fs::remove_file(&config_path);
    }
}
