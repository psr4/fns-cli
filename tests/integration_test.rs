use std::io::Write;
use std::path::PathBuf;
use std::sync::Mutex;

use fns_cli::hash::{hash_content, hash_path};
use fns_cli::config::AppConfig;
use fns_cli::state::SyncState;
use fns_cli::cli::{Cli, Commands};
use clap::Parser;

static ENV_LOCK: Mutex<()> = Mutex::new(());

mod hash_tests {
    use super::*;

    #[test]
    fn test_hash_ascii_hello() {
        assert_eq!(hash_content("hello"), "99162322");
    }

    #[test]
    fn test_hash_emoji_rocket() {
        // Emoji test: 🚀 is U+1F680 (128640)
        // Python produces "-538783611" using ord(ch) for code points
        assert_eq!(hash_content("Fast Note Sync 🚀"), "-538783611");
    }

    #[test]
    fn test_hash_empty_string() {
        assert_eq!(hash_content(""), "0");
    }

    #[test]
    fn test_hash_single_character() {
        assert_eq!(hash_content("a"), "97");
    }

    #[test]
    fn test_hash_chinese_characters() {
        let result = hash_content("你好世界");
        assert!(result.parse::<i32>().is_ok());
    }

    #[test]
    fn test_hash_path_function() {
        assert_eq!(hash_path("hello"), hash_content("hello"));
        assert_eq!(hash_path("Fast Note Sync 🚀"), hash_content("Fast Note Sync 🚀"));
    }

    #[test]
    fn test_hash_long_string_overflow() {
        let result = hash_content("test with longer string that should overflow the i32 boundary");
        assert!(result.parse::<i32>().is_ok());
    }

    #[test]
    fn test_hash_consistency_multiple_calls() {
        let input = "consistent hash test";
        let first = hash_content(input);
        let second = hash_content(input);
        let third = hash_content(input);
        assert_eq!(first, second);
        assert_eq!(second, third);
    }
}

mod config_tests {
    use super::*;

    fn create_temp_config(content: &str) -> (tempfile::TempDir, PathBuf) {
        let temp_dir = tempfile::tempdir().unwrap();
        let config_path = temp_dir.path().join("config.yaml");
        let mut file = std::fs::File::create(&config_path).unwrap();
        write!(file, "{}", content).unwrap();
        drop(file);
        (temp_dir, config_path)
    }

    #[test]
    fn test_config_load_valid_yaml() {
        let _lock = ENV_LOCK.lock().unwrap();
        let yaml = r#"
server:
  api: "https://api.example.com"
  token: "test-token-123"
  vault: "myVault"
sync:
  watch_path: "/path/to/vault"
"#;
        let (_temp_dir, config_path) = create_temp_config(yaml);
        
        unsafe {
            std::env::remove_var("FNS_API");
            std::env::remove_var("FNS_TOKEN");
        }
        
        let config = AppConfig::load(config_path.to_str().unwrap()).unwrap();
        assert_eq!(config.server.api, "https://api.example.com");
        assert_eq!(config.server.token, "test-token-123");
        assert_eq!(config.server.vault, "myVault");
        assert_eq!(config.sync.watch_path, "/path/to/vault");
    }

    #[test]
    fn test_config_load_with_defaults() {
        let _lock = ENV_LOCK.lock().unwrap();
        let yaml = r#"
server:
  api: "https://api.example.com"
  token: "test-token"
  vault: "myVault"
"#;
        let (_temp_dir, config_path) = create_temp_config(yaml);
        
        unsafe {
            std::env::remove_var("FNS_API");
            std::env::remove_var("FNS_TOKEN");
        }
        
        let config = AppConfig::load(config_path.to_str().unwrap()).unwrap();
        assert!(config.sync.sync_notes);
        assert!(config.sync.sync_files);
        assert!(config.sync.sync_config);
        assert_eq!(config.sync.upload_concurrency, 2);
        assert_eq!(config.sync.file_chunk_size, 524288);
        assert_eq!(config.client.reconnect_max_retries, 15);
        assert_eq!(config.logging.level, "INFO");
    }

    #[test]
    fn test_config_env_override_api() {
        let _lock = ENV_LOCK.lock().unwrap();
        let yaml = r#"
server:
  api: "https://config-api.example.com"
  token: "test-token"
  vault: "myVault"
"#;
        let (_temp_dir, config_path) = create_temp_config(yaml);
        
        unsafe {
            std::env::set_var("FNS_API", "https://env-api.example.com");
        }
        
        let config = AppConfig::load(config_path.to_str().unwrap()).unwrap();
        assert_eq!(config.server.api, "https://env-api.example.com");
        
        unsafe {
            std::env::remove_var("FNS_API");
        }
    }

    #[test]
    fn test_config_env_override_token() {
        let _lock = ENV_LOCK.lock().unwrap();
        let yaml = r#"
server:
  api: "https://api.example.com"
  token: "config-token"
  vault: "myVault"
"#;
        let (_temp_dir, config_path) = create_temp_config(yaml);
        
        unsafe {
            std::env::set_var("FNS_TOKEN", "env-token-xyz");
        }
        
        let config = AppConfig::load(config_path.to_str().unwrap()).unwrap();
        assert_eq!(config.server.token, "env-token-xyz");
        
        unsafe {
            std::env::remove_var("FNS_TOKEN");
        }
    }

    #[test]
    fn test_config_missing_required_api() {
        let _lock = ENV_LOCK.lock().unwrap();
        let yaml = r#"
server:
  api: ""
  token: "test-token"
  vault: "myVault"
"#;
        let (_temp_dir, config_path) = create_temp_config(yaml);
        
        unsafe {
            std::env::remove_var("FNS_API");
            std::env::remove_var("FNS_TOKEN");
        }
        
        let result = AppConfig::load(config_path.to_str().unwrap());
        assert!(result.is_err());
    }

    #[test]
    fn test_config_missing_required_token() {
        let _lock = ENV_LOCK.lock().unwrap();
        let yaml = r#"
server:
  api: "https://api.example.com"
  token: ""
  vault: "myVault"
"#;
        let (_temp_dir, config_path) = create_temp_config(yaml);
        
        unsafe {
            std::env::remove_var("FNS_API");
            std::env::remove_var("FNS_TOKEN");
        }
        
        let result = AppConfig::load(config_path.to_str().unwrap());
        assert!(result.is_err());
    }

    #[test]
    fn test_config_ws_api_https() {
        let _lock = ENV_LOCK.lock().unwrap();
        let yaml = r#"
server:
  api: "https://server.example.com"
  token: "test-token"
  vault: "myVault"
"#;
        let (_temp_dir, config_path) = create_temp_config(yaml);
        
        unsafe {
            std::env::remove_var("FNS_API");
            std::env::remove_var("FNS_TOKEN");
        }
        
        let config = AppConfig::load(config_path.to_str().unwrap()).unwrap();
        assert_eq!(config.ws_api(), "wss://server.example.com");
    }

    #[test]
    fn test_config_ws_api_http() {
        let _lock = ENV_LOCK.lock().unwrap();
        let yaml = r#"
server:
  api: "http://localhost:8080"
  token: "test-token"
  vault: "myVault"
"#;
        let (_temp_dir, config_path) = create_temp_config(yaml);
        
        unsafe {
            std::env::remove_var("FNS_API");
            std::env::remove_var("FNS_TOKEN");
        }
        
        let config = AppConfig::load(config_path.to_str().unwrap()).unwrap();
        assert_eq!(config.ws_api(), "ws://localhost:8080");
    }

    #[test]
    fn test_config_sync_settings() {
        let _lock = ENV_LOCK.lock().unwrap();
        let yaml = r#"
server:
  api: "https://api.example.com"
  token: "test-token"
  vault: "myVault"
sync:
  sync_notes: false
  sync_files: false
  sync_config: true
  upload_concurrency: 5
  file_chunk_size: 1048576
"#;
        let (_temp_dir, config_path) = create_temp_config(yaml);
        
        unsafe {
            std::env::remove_var("FNS_API");
            std::env::remove_var("FNS_TOKEN");
        }
        
        let config = AppConfig::load(config_path.to_str().unwrap()).unwrap();
        assert!(!config.sync.sync_notes);
        assert!(!config.sync.sync_files);
        assert!(config.sync.sync_config);
        assert_eq!(config.sync.upload_concurrency, 5);
        assert_eq!(config.sync.file_chunk_size, 1048576);
    }
}

mod state_tests {
    use super::*;

    #[test]
    fn test_state_default_values() {
        let state = SyncState::default();
        assert_eq!(state.last_note_sync_time, 0);
        assert_eq!(state.last_file_sync_time, 0);
        assert_eq!(state.last_setting_sync_time, 0);
    }

    #[test]
    fn test_state_json_serialization() {
        let state = SyncState {
            last_note_sync_time: 1234567890,
            last_file_sync_time: 1234567891,
            last_setting_sync_time: 1234567892,
        };
        let json = serde_json::to_string(&state).unwrap();
        assert!(json.contains("\"last_note_sync_time\":1234567890"));
        assert!(json.contains("\"last_file_sync_time\":1234567891"));
        assert!(json.contains("\"last_setting_sync_time\":1234567892"));
    }

    #[test]
    fn test_state_json_deserialization() {
        let json = r#"{"last_note_sync_time":1234567890,"last_file_sync_time":1234567891,"last_setting_sync_time":1234567892}"#;
        let state: SyncState = serde_json::from_str(json).unwrap();
        assert_eq!(state.last_note_sync_time, 1234567890);
        assert_eq!(state.last_file_sync_time, 1234567891);
        assert_eq!(state.last_setting_sync_time, 1234567892);
    }

    #[test]
    fn test_state_save_and_load_roundtrip() {
        let temp_dir = tempfile::tempdir().unwrap();
        let state_path = temp_dir.path().join(".fns_state.json");

        let original = SyncState {
            last_note_sync_time: 1111111111,
            last_file_sync_time: 2222222222,
            last_setting_sync_time: 3333333333,
        };

        original.save(&state_path).unwrap();
        
        assert!(state_path.exists());
        
        let loaded = SyncState::load(&state_path);
        assert_eq!(loaded.last_note_sync_time, original.last_note_sync_time);
        assert_eq!(loaded.last_file_sync_time, original.last_file_sync_time);
        assert_eq!(loaded.last_setting_sync_time, original.last_setting_sync_time);
    }

    #[test]
    fn test_state_load_missing_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let state_path = temp_dir.path().join("nonexistent_state.json");

        let loaded = SyncState::load(&state_path);
        assert_eq!(loaded.last_note_sync_time, 0);
        assert_eq!(loaded.last_file_sync_time, 0);
        assert_eq!(loaded.last_setting_sync_time, 0);
    }

    #[test]
    fn test_state_load_corrupted_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let state_path = temp_dir.path().join("corrupted_state.json");

        std::fs::write(&state_path, "not valid json {{{").unwrap();

        let loaded = SyncState::load(&state_path);
        assert_eq!(loaded.last_note_sync_time, 0);
        assert_eq!(loaded.last_file_sync_time, 0);
        assert_eq!(loaded.last_setting_sync_time, 0);
    }

    #[test]
    fn test_state_python_compatible_format() {
        let json = r#"{"last_note_sync_time":1704067200,"last_file_sync_time":1704067201,"last_setting_sync_time":1704067202}"#;
        let state: SyncState = serde_json::from_str(json).unwrap();
        assert_eq!(state.last_note_sync_time, 1704067200);
        assert_eq!(state.last_file_sync_time, 1704067201);
        assert_eq!(state.last_setting_sync_time, 1704067202);
    }

    #[test]
    fn test_state_file_naming_convention() {
        let temp_dir = tempfile::tempdir().unwrap();
        let state_path = temp_dir.path().join(".fns_state.json");

        let state = SyncState {
            last_note_sync_time: 1,
            last_file_sync_time: 2,
            last_setting_sync_time: 3,
        };

        state.save(&state_path).unwrap();
        
        let loaded_content = std::fs::read_to_string(&state_path).unwrap();
        let loaded_state: SyncState = serde_json::from_str(&loaded_content).unwrap();
        
        assert_eq!(loaded_state.last_note_sync_time, 1);
        assert_eq!(loaded_state.last_file_sync_time, 2);
        assert_eq!(loaded_state.last_setting_sync_time, 3);
    }
}

mod cli_tests {
    use super::*;

    #[test]
    fn test_cli_parse_run_command() {
        let cli = Cli::try_parse_from(["fns-cli", "run"]);
        assert!(cli.is_ok());
        let cli = cli.unwrap();
        assert!(matches!(cli.command, Commands::Run));
    }

    #[test]
    fn test_cli_parse_sync_command() {
        let cli = Cli::try_parse_from(["fns-cli", "sync"]);
        assert!(cli.is_ok());
        let cli = cli.unwrap();
        assert!(matches!(cli.command, Commands::Sync));
    }

    #[test]
    fn test_cli_parse_pull_command() {
        let cli = Cli::try_parse_from(["fns-cli", "pull"]);
        assert!(cli.is_ok());
        let cli = cli.unwrap();
        assert!(matches!(cli.command, Commands::Pull));
    }

    #[test]
    fn test_cli_parse_push_command() {
        let cli = Cli::try_parse_from(["fns-cli", "push"]);
        assert!(cli.is_ok());
        let cli = cli.unwrap();
        assert!(matches!(cli.command, Commands::Push));
    }

    #[test]
    fn test_cli_parse_status_command() {
        let cli = Cli::try_parse_from(["fns-cli", "status"]);
        assert!(cli.is_ok());
        let cli = cli.unwrap();
        assert!(matches!(cli.command, Commands::Status));
    }

    #[test]
    fn test_cli_config_option() {
        let cli = Cli::try_parse_from(["fns-cli", "-c", "/path/to/config.yaml", "sync"]);
        assert!(cli.is_ok());
        let cli = cli.unwrap();
        assert_eq!(cli.config, Some(PathBuf::from("/path/to/config.yaml")));
    }

    #[test]
    fn test_cli_config_long_option() {
        let cli = Cli::try_parse_from(["fns-cli", "--config", "/custom/config.yaml", "status"]);
        assert!(cli.is_ok());
        let cli = cli.unwrap();
        assert_eq!(cli.config, Some(PathBuf::from("/custom/config.yaml")));
    }

    #[test]
    fn test_cli_verbose_flag() {
        let cli = Cli::try_parse_from(["fns-cli", "-v", "sync"]);
        assert!(cli.is_ok());
        let cli = cli.unwrap();
        assert!(cli.verbose);
    }

    #[test]
    fn test_cli_verbose_long_flag() {
        let cli = Cli::try_parse_from(["fns-cli", "--verbose", "run"]);
        assert!(cli.is_ok());
        let cli = cli.unwrap();
        assert!(cli.verbose);
    }

    #[test]
    fn test_cli_no_verbose_by_default() {
        let cli = Cli::try_parse_from(["fns-cli", "sync"]);
        assert!(cli.is_ok());
        let cli = cli.unwrap();
        assert!(!cli.verbose);
    }

    #[test]
    fn test_cli_combined_options() {
        let cli = Cli::try_parse_from(["fns-cli", "-c", "my-config.yaml", "-v", "status"]);
        assert!(cli.is_ok());
        let cli = cli.unwrap();
        assert_eq!(cli.config, Some(PathBuf::from("my-config.yaml")));
        assert!(cli.verbose);
        assert!(matches!(cli.command, Commands::Status));
    }

    #[test]
    fn test_cli_no_config_by_default() {
        let cli = Cli::try_parse_from(["fns-cli", "sync"]);
        assert!(cli.is_ok());
        let cli = cli.unwrap();
        assert!(cli.config.is_none());
    }
}
