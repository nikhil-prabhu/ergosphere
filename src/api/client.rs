//! Stateful HTTP client layer managing persistent sessions with target Pi-hole nodes.

use std::time::Duration;

use reqwest::{Client, StatusCode};
use url::Url;

use crate::api::types::{
    ApiAuthPayload,
    ApiResponse,
    ApiResult,
    ApiSessionPayload,
    SessionDetails,
};
use crate::api::ApiError;

/// An active state client tracking a Pi-hole v6 REST API session.
pub struct ApiClient {
    pub base_url: Url,
    pub http_client: Client,
    pub session: Option<SessionDetails>,
}

impl ApiClient {
    /// Creates a new API client against the Pi-hole v6 REST API endpoint with an uninitialized session.
    ///
    /// # Arguments
    ///
    /// * `raw_url` - The raw URL for the target node. Eg: `"http://192.168.0.2"`.
    pub fn new(raw_url: &str) -> Result<Self, ApiError> {
        let mut base_url = Url::parse(raw_url)?;

        if !base_url.path().ends_with('/') {
            base_url.set_path(&format!("{}/", base_url.path()));
        }
        let base_url = base_url.join("api/")?;

        let http_client = Client::builder()
            .cookie_store(true)
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_default();

        Ok(Self {
            base_url,
            http_client,
            session: None,
        })
    }

    /// Authenticates against the Pi-hole v6 REST API endpoint and initializes a session in the API client.
    ///
    /// # Arguments
    ///
    /// * `password` - The web UI or application password in plaintext.
    pub async fn authenticate(&mut self, password: &str) -> Result<(), ApiError> {
        let auth_endpoint = self.base_url.join("auth/")?;
        let auth_payload = ApiAuthPayload {
            password: password.to_string(),
        };
        let response = self
            .http_client
            .post(auth_endpoint)
            .json(&auth_payload)
            .send()
            .await?;
        let status = response.status();
        let response = response.json::<ApiResponse<ApiSessionPayload>>().await?;

        match &*response {
            ApiResult::Success(payload) => {
                self.session = Some(payload.session.clone());
                Ok(())
            }
            ApiResult::Failure(payload) => match status {
                StatusCode::BAD_REQUEST => Err(ApiError::BadRequest(payload.error.message.clone())),
                StatusCode::UNAUTHORIZED => {
                    Err(ApiError::Unauthorized(payload.error.message.clone()))
                }
                StatusCode::TOO_MANY_REQUESTS => {
                    Err(ApiError::TooManyRequests(payload.error.message.clone()))
                }
                status => Err(ApiError::UnexpectedStatusCode(status)),
            },
        }
    }
}
