mod cli;
mod config;
mod error;
mod hash;
mod http;
mod logging;
mod progress;
mod protocol;
mod signal;
mod state;
mod sync;
mod watcher;
mod ws_client;

use clap::Parser;
use cli::{Cli, Commands};
use config::AppConfig;
use state::SyncState;
use std::process::ExitCode;
use sync::SyncCoordinator;
use watcher::{FileWatcher, WatchEvent};

fn format_timestamp(timestamp: i64) -> String {
    if timestamp == 0 {
        return "Never".to_string();
    }

    let seconds = timestamp % 60;
    let minutes = (timestamp / 60) % 60;
    let hours = (timestamp / 3600) % 24;

    let mut days_since_epoch = timestamp / 86400;
    let mut year = 1970;

    loop {
        let days_in_year = if is_leap_year(year) { 366 } else { 365 };
        if days_since_epoch < days_in_year {
            break;
        }
        days_since_epoch -= days_in_year;
        year += 1;
    }

    let (month, day) = day_of_year_to_month_day(days_since_epoch as i32, year);

    format!(
        "{}-{:02}-{:02} {:02}:{:02}:{:02}",
        year, month, day, hours, minutes, seconds
    )
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

fn day_of_year_to_month_day(day_of_year: i32, year: i32) -> (i32, i32) {
    let days_in_months = if is_leap_year(year) {
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };

    let mut remaining_days = day_of_year;
    for (month_idx, &days_in_month) in days_in_months.iter().enumerate() {
        if remaining_days < days_in_month {
            return ((month_idx + 1) as i32, remaining_days + 1);
        }
        remaining_days -= days_in_month;
    }
    (12, 31)
}

fn run_status(args: &Cli) -> ExitCode {
    let config_path = args
        .config
        .as_ref()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|| "config.yaml".to_string());

    let config = match AppConfig::load(&config_path) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("Error loading config from '{}': {}", config_path, e);
            return ExitCode::FAILURE;
        }
    };

    let vault_path = config.vault_path();
    let state_path = vault_path.join(".fns_state.json");
    let state = SyncState::load(&state_path);
    let ws_url = config.ws_api();

    println!("FastNodeSync CLI v{}", env!("CARGO_PKG_VERSION"));
    println!();
    println!("Configuration:");
    println!("  Server       : {}", config.server.api);
    println!("  WebSocket    : {}", ws_url);
    println!("  Vault        : {}", config.server.vault);
    println!("  Watch path   : {}", config.sync.watch_path);
    println!("  Sync notes   : {}", config.sync.sync_notes);
    println!("  Sync files   : {}", config.sync.sync_files);
    println!("  Sync config  : {}", config.sync.sync_config);
    println!();
    println!("Sync state:");
    println!(
        "  Last note sync    : {}",
        format_timestamp(state.last_note_sync_time)
    );
    println!(
        "  Last file sync    : {}",
        format_timestamp(state.last_file_sync_time)
    );
    println!(
        "  Last setting sync : {}",
        format_timestamp(state.last_setting_sync_time)
    );

    if vault_path.exists() {
        let md_count = walkdir::WalkDir::new(&vault_path)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "md"))
            .count();

        let total_files = walkdir::WalkDir::new(&vault_path)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_file())
            .count();

        println!();
        println!("Local vault:");
        println!("  Notes (.md)  : {}", md_count);
        println!("  Total files  : {}", total_files);
    }

    ExitCode::SUCCESS
}

fn main() -> ExitCode {
    let args = Cli::parse();

    if args.verbose {
        eprintln!("Verbose mode enabled");
    }

    match args.command {
        Commands::Run => run_run_command(&args),
        Commands::Sync => run_sync_command(&args),
        Commands::Pull => run_pull_command(&args),
        Commands::Push => run_push_command(&args),
        Commands::Status => run_status(&args),
    }
}

fn run_pull_command(args: &Cli) -> ExitCode {
    let config_path = args
        .config
        .as_ref()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|| "config.yaml".to_string());

    if args.verbose {
        logging::init_logging(true, None);
    }

    let config = match AppConfig::load(&config_path) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("Error loading config from '{}': {}", config_path, e);
            return ExitCode::FAILURE;
        }
    };

    println!("Connecting to {}...", config.ws_api());
    println!("Starting pull-only sync...");

    let result = tokio::runtime::Runtime::new()
        .expect("Failed to create tokio runtime")
        .block_on(async {
            let mut coordinator = SyncCoordinator::new(config);
            coordinator.run_pull().await
        });

    match result {
        Ok(sync_result) => {
            println!("Pull complete:");
            println!("  Notes: {}", sync_result.notes_synced);
            println!("  Files: {}", sync_result.files_synced);
            println!("  Settings: {}", sync_result.settings_synced);

            if sync_result.has_errors() {
                eprintln!("\nErrors encountered:");
                for error in &sync_result.errors {
                    eprintln!("  - {}", error);
                }
                ExitCode::FAILURE
            } else {
                ExitCode::SUCCESS
            }
        }
        Err(e) => {
            eprintln!("Pull failed: {}", e);
            ExitCode::FAILURE
        }
    }
}

fn run_push_command(args: &Cli) -> ExitCode {
    let config_path = args
        .config
        .as_ref()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|| "config.yaml".to_string());

    if args.verbose {
        logging::init_logging(true, None);
    }

    let config = match AppConfig::load(&config_path) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("Error loading config from '{}': {}", config_path, e);
            return ExitCode::FAILURE;
        }
    };

    println!("Connecting to {}...", config.ws_api());
    println!("Starting push-only sync...");

    let result = tokio::runtime::Runtime::new()
        .expect("Failed to create tokio runtime")
        .block_on(async {
            let mut coordinator = SyncCoordinator::new(config);
            coordinator.run_push().await
        });

    match result {
        Ok(sync_result) => {
            println!("Push complete:");
            println!("  Notes: {}", sync_result.notes_synced);
            println!("  Files: {}", sync_result.files_synced);
            println!("  Settings: {}", sync_result.settings_synced);

            if sync_result.has_errors() {
                eprintln!("\nErrors encountered:");
                for error in &sync_result.errors {
                    eprintln!("  - {}", error);
                }
                ExitCode::FAILURE
            } else {
                ExitCode::SUCCESS
            }
        }
        Err(e) => {
            eprintln!("Push failed: {}", e);
            ExitCode::FAILURE
        }
    }
}

fn run_sync_command(args: &Cli) -> ExitCode {
    let config_path = args
        .config
        .as_ref()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|| "config.yaml".to_string());

    if args.verbose {
        logging::init_logging(true, None);
    }

    let config = match AppConfig::load(&config_path) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("Error loading config from '{}': {}", config_path, e);
            return ExitCode::FAILURE;
        }
    };

    println!("Connecting to {}...", config.ws_api());
    println!("Starting bidirectional sync...");

    let result = tokio::runtime::Runtime::new()
        .expect("Failed to create tokio runtime")
        .block_on(async {
            let mut coordinator = SyncCoordinator::new(config);
            coordinator.run_sync().await
        });

    match result {
        Ok(sync_result) => {
            println!("Sync complete:");
            println!("  Notes: {}", sync_result.notes_synced);
            println!("  Files: {}", sync_result.files_synced);
            println!("  Settings: {}", sync_result.settings_synced);

            if sync_result.has_errors() {
                eprintln!("\nErrors encountered:");
                for error in &sync_result.errors {
                    eprintln!("  - {}", error);
                }
                ExitCode::FAILURE
            } else {
                ExitCode::SUCCESS
            }
        }
        Err(e) => {
            eprintln!("Sync failed: {}", e);
            ExitCode::FAILURE
        }
    }
}

fn run_run_command(args: &Cli) -> ExitCode {
    let config_path = args
        .config
        .as_ref()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|| "config.yaml".to_string());

    logging::init_logging(args.verbose, None);

    let config = match AppConfig::load(&config_path) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("Error loading config from '{}': {}", config_path, e);
            return ExitCode::FAILURE;
        }
    };

    println!("Connecting to {}...", config.ws_api());
    println!("Starting continuous sync mode (press Ctrl+C to stop)...");

    let result = tokio::runtime::Runtime::new()
        .expect("Failed to create tokio runtime")
        .block_on(async { run_continuous_sync(config).await });

    match result {
        Ok(sync_result) => {
            println!("Sync session ended:");
            println!("  Notes: {}", sync_result.notes_synced);
            println!("  Files: {}", sync_result.files_synced);
            println!("  Settings: {}", sync_result.settings_synced);

            if sync_result.has_errors() {
                eprintln!("\nErrors encountered:");
                for error in &sync_result.errors {
                    eprintln!("  - {}", error);
                }
                ExitCode::FAILURE
            } else {
                ExitCode::SUCCESS
            }
        }
        Err(e) => {
            eprintln!("Continuous sync failed: {}", e);
            ExitCode::FAILURE
        }
    }
}

async fn run_continuous_sync(config: AppConfig) -> Result<sync::SyncResult, error::FnsError> {
    // Create shutdown signal handler
    let (_shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let (_signal_sender, mut shutdown) = signal::ShutdownSignal::new();

    // Create file watcher
    let vault_path = config.vault_path();
    let mut exclude_patterns = config.sync.exclude_patterns.clone();
    if !config.sync.sync_config {
        exclude_patterns.extend(
            config
                .sync
                .config_sync_dirs
                .iter()
                .map(|dir| format!("{}/**", dir)),
        );
    }
    let (watcher, watch_rx) = FileWatcher::new(vault_path.clone(), exclude_patterns)?;

    // Start watching
    let mut watcher = watcher;
    watcher.start()?;

    // CRITICAL: Start the event processing loop in a separate thread
    let _watcher_handle = std::thread::spawn(move || {
        watcher.run();
    });

    let mut coordinator = SyncCoordinator::new(config);

    let (tx, watch_rx_tokio) = tokio::sync::mpsc::channel::<WatchEvent>(100);

    let _bridge_handle = tokio::task::spawn_blocking(move || {
        while let Ok(event) = watch_rx.recv() {
            if tx.blocking_send(event).is_err() {
                break;
            }
        }
    });

    println!("Initial sync complete. Watching for changes...");

    // Wait for either sync to complete or shutdown signal
    let _result = tokio::select! {
        r = coordinator.run_continuous(watch_rx_tokio, shutdown_rx) => r?,
        _ = shutdown.wait() => {
            println!("Shutdown signal received");
            sync::SyncResult::default()
        }
    };

    // Force exit - don't wait for threads to clean up
    std::process::exit(0);
}
