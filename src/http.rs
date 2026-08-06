//! HTTP client wrapper for REST API requests.
//!
//! Provides authenticated HTTP client with JWT token support and
//! standard response format handling.

#![allow(dead_code)]

use crate::protocol::Response;
use reqwest::Client;
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;

/// HTTP client error types
#[derive(Debug, Error)]
pub enum FnsError {
    #[error("HTTP request failed: {0}")]
    Request(#[from] reqwest::Error),

    #[error("API error (code {code}): {message}")]
    Api { code: i32, message: String },

    #[error("Response data is missing")]
    MissingData,

    #[error("JSON parse error: {0}")]
    Json(#[from] serde_json::Error),
}

/// HTTP client for authenticated API requests
pub struct HttpClient {
    base_url: String,
    token: String,
    client: Client,
}

impl HttpClient {
    /// Create a new HTTP client with base URL and authentication token
    pub fn new(base_url: &str, token: &str) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            token: token.to_string(),
            client: Client::new(),
        }
    }

    fn build_url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    /// Make a GET request and parse the response
    pub async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T, FnsError> {
        let url = self.build_url(path);

        let response = self
            .client
            .get(&url)
            .header("Authorization", format!("Bearer {}", self.token))
            .send()
            .await?;

        let api_response: Response<T> = response.json().await?;
        Self::check_response(api_response)
    }

    /// Make a POST request with a body and parse the response
    pub async fn post<T: Serialize, R: DeserializeOwned>(
        &self,
        path: &str,
        body: &T,
    ) -> Result<R, FnsError> {
        let url = self.build_url(path);

        let response = self
            .client
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.token))
            .json(body)
            .send()
            .await?;

        let api_response: Response<R> = response.json().await?;
        Self::check_response(api_response)
    }

    fn check_response<T>(response: Response<T>) -> Result<T, FnsError> {
        if !response.status || response.code < 1 {
            return Err(FnsError::Api {
                code: response.code,
                message: response.message,
            });
        }

        response.data.ok_or(FnsError::MissingData)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_url() {
        let client = HttpClient::new("http://localhost:9000/api", "test-token");
        assert_eq!(
            client.build_url("/user/info"),
            "http://localhost:9000/api/user/info"
        );
    }

    #[test]
    fn test_build_url_trailing_slash() {
        let client = HttpClient::new("http://localhost:9000/api/", "test-token");
        assert_eq!(
            client.build_url("/user/info"),
            "http://localhost:9000/api/user/info"
        );
    }
}
