#![allow(dead_code)]

use thiserror::Error;

#[derive(Error, Debug)]
pub enum FnsError {
    #[error("Configuration error: {message}")]
    Config { message: String },

    #[error("IO error")]
    Io {
        #[source]
        source: std::io::Error,
    },

    #[error("WebSocket error: {message}")]
    WebSocket { message: String },

    #[error("Protocol error: {message}")]
    Protocol { message: String },

    #[error("Sync error: {message}")]
    Sync { message: String },

    #[error("HTTP error")]
    Http {
        #[source]
        source: reqwest::Error,
    },

    #[error("JSON error")]
    Json {
        #[source]
        source: serde_json::Error,
    },

    #[error("YAML error")]
    Yaml {
        #[source]
        source: serde_yaml::Error,
    },

    #[error("Watcher error: {message}")]
    Watcher { message: String },
}

impl From<std::io::Error> for FnsError {
    fn from(source: std::io::Error) -> Self {
        FnsError::Io { source }
    }
}

impl From<serde_json::Error> for FnsError {
    fn from(source: serde_json::Error) -> Self {
        FnsError::Json { source }
    }
}

impl From<serde_yaml::Error> for FnsError {
    fn from(source: serde_yaml::Error) -> Self {
        FnsError::Yaml { source }
    }
}

impl From<notify::Error> for FnsError {
    fn from(err: notify::Error) -> Self {
        FnsError::Watcher {
            message: err.to_string(),
        }
    }
}
