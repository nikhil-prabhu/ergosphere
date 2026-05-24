//! Pi-hole v6 REST API integration and serialization abstractions.

pub mod client;
pub mod types;

use std::error;

use thiserror::Error;

/// Violations and failure states encountered during API transactional sessions.
#[derive(Debug, Error)]
pub enum ApiError {
    #[error("Underlying transport error: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("Bad request: {0}")]
    BadRequest(String),
    #[error("Authentication failure: {0}")]
    Unauthorized(String),
    #[error("Too many requests: {0}")]
    TooManyRequests(String),
    #[error("Unexpected status code: {0}")]
    UnexpectedStatusCode(reqwest::StatusCode),
    #[error("URL parse error: {0}")]
    UrlParseError(#[from] url::ParseError),
    #[error("Unknown error: {0}")]
    Error(#[from] Box<dyn error::Error + Send + Sync>),
}
