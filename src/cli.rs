use clap::{Parser, Subcommand};
use std::path::PathBuf;

/// FastNodeSync CLI - sync Obsidian vaults from the command line.
#[derive(Parser)]
#[command(version, about, long_about = None)]
pub struct Cli {
    /// Path to config.yaml
    #[arg(short, long, global = true)]
    pub config: Option<PathBuf>,

    /// Enable verbose logging
    #[arg(short, long, global = true)]
    pub verbose: bool,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Start continuous sync (watch + push + pull)
    Run,

    /// Run a full bidirectional sync, then exit
    Sync,

    /// Pull remote changes to local vault
    Pull,

    /// Push all local files to remote
    Push,

    /// Show sync state and configuration
    Status,
}
