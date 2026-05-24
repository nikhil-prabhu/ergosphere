//! Strongly-typed request and response structures matching Pi-hole v6 schemas.
//!
//! For the detailed API documentation, check out [Pi-hole documentation](https://docs.pi-hole.net/api/)

use std::fmt;
use std::ops::Deref;

use serde::{Deserialize, Serialize};

/// Represents a general Pi-hole v6 REST API response.
///
/// # Schema
///
/// ```json
/// {
///     <PAYLOAD>
///     took: number
/// }
/// ```
///
/// Where `<PAYLOAD>`'s schema depends on the request being made.
///
/// # Examples
///
/// ## Successful `GET` call on the `/auth` endpoint:
///
/// ```json
/// {
///   "session": {
///     "valid": true,
///     "totp": false,
///     "sid": null,
///     "csrf": null,
///     "validity": 300,
///     "message": null
///   },
///   "took": 0.003
/// }
/// ```
///
/// ## Failed `POST` call on the `/auth` endpoint (Status code: 400)
///
/// ```json
/// {
///   "error": {
///     "key": "bad_request",
///     "message": "No valid JSON payload found",
///     "hint": null
///   },
///   "took": 0.003
/// }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiResponse<T> {
    #[serde(flatten)]
    pub payload: ApiResult<T>,
    pub took: f64,
}

/// The result of making a REST API call, either a success or a failure.
///
/// Each variant holds the actual JSON payload returned in the response.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ApiResult<T> {
    Success(T),
    Failure(ApiErrorPayload),
}

/// Represents a general Pi-hole v6 REST API error response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiErrorPayload {
    pub error: ErrorDetails,
}

/// The specific details of the error response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorDetails {
    pub key: String,
    pub message: String,
    pub hint: Option<String>,
}

/// The request payload for authenticating against the `/auth` endpoint.
#[derive(Clone, Serialize, Deserialize)]
pub struct ApiAuthPayload {
    pub password: String,
}

/// The response from the `/auth` endpoint containing the session details for authenticating future calls.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiSessionPayload {
    pub session: SessionDetails,
}

/// The details of an authenticated session, including validity, CSRF token, and other relevant metadata.
#[derive(Clone, Serialize, Deserialize)]
pub struct SessionDetails {
    pub valid: bool,
    pub totp: bool,
    pub sid: Option<String>,
    pub csrf: Option<String>,
    pub validity: u64,
    pub message: Option<String>,
}

/// Structural payload returned by the `GET /info/database` endpoint.
#[derive(Debug, Deserialize)]
pub struct DatabaseInfo {
    /// The size of the database file in bytes.
    pub size: u64,
    /// Unix timestamp tracking when the database file was last modified.
    pub mtime: i64,
    /// The current total query count tracking inside the engine index.
    pub queries: u64,
}

impl fmt::Debug for ApiAuthPayload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ApiAuthPayload")
            .field("password", &"***REDACTED***")
            .finish()
    }
}

impl fmt::Debug for SessionDetails {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionDetails")
            .field("valid", &self.valid)
            .field("totp", &self.totp)
            .field("sid", &self.sid.is_some().then(|| "***REDACTED***"))
            .field("csrf", &self.csrf.is_some().then(|| "***REDACTED***"))
            .field("validity", &self.valid)
            .field("message", &self.message)
            .finish()
    }
}

impl<T> Deref for ApiResponse<T> {
    type Target = ApiResult<T>;

    fn deref(&self) -> &Self::Target {
        &self.payload
    }
}
