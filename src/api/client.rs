//! Stateful HTTP client layer managing persistent sessions with target Pi-hole nodes.

use std::marker;
use std::time::{Duration, Instant};

use bytes::Bytes;
use reqwest::{multipart, Client, StatusCode};
use tracing::{debug, info};
use url::Url;

use crate::api::types::{
    ApiAuthPayload,
    ApiErrorPayload,
    ApiResponse,
    ApiResult,
    ApiSessionPayload,
    SessionDetails,
    TeleporterImportOptions,
};
use crate::api::ApiError;
use crate::consts::ERGOSPHERE_VERSION;

/// The primary Pi-hole node.
pub struct Primary;
/// The replica Pi-hole node.
pub struct Replica;

/// An active state client tracking a Pi-hole v6 REST API session.
pub struct ApiClient<Role> {
    base_url: Url,
    label: Option<String>,
    http_client: Client,
    session: Option<SessionDetails>,
    token_expires_at: Option<Instant>,
    _marker: marker::PhantomData<Role>,
}

impl<Role> ApiClient<Role> {
    /// Creates a new API client for a node against the Pi-hole v6 REST API endpoint
    /// with an uninitialized session.
    ///
    /// # Arguments
    ///
    /// * `raw_url` - The raw URL for the node. Eg: `"http://192.168.0.2"`.
    /// * `label` - An optional custom identifier for the node.
    /// * `timeout` - The timeout for the underlying HTTP client.
    /// * `skip_tls_verification` - Whether to disable TLS certificate verification.
    ///
    /// # Examples
    ///
    /// ## API client for a primary node
    ///
    /// ```rust
    /// use std::time::Duration;
    ///
    /// use crate::api::client::{ApiClient, Primary};
    ///
    /// let _client = ApiClient::<Primary>>:new(
    ///     "http://192.168.0.2",
    ///     Some("pihole-primary".to_string()),
    ///     Duration::from_secs(10),
    ///     false,
    /// );
    /// ```
    ///
    /// ## API client for a replica node
    ///
    /// ```rust
    /// use std::time::Duration;
    ///
    /// use crate::api::client::{ApiClient, Replica};
    ///
    /// let _client = ApiClient::<Replica>::new(
    ///     "http://192.168.0.3",
    ///     Some("pihole-replica".to_string()),
    ///     Duration::from_secs(10),
    ///     false,
    /// );
    /// ```
    pub fn new(
        raw_url: &str,
        label: Option<String>,
        timeout: Duration,
        skip_tls_verification: bool,
    ) -> Result<Self, ApiError> {
        let mut base_url = Url::parse(raw_url)?;

        if !base_url.path().ends_with('/') {
            base_url.set_path(&format!("{}/", base_url.path()));
        }
        let base_url = base_url.join("api/")?;

        let http_client = Client::builder()
            .cookie_store(true)
            .timeout(timeout)
            .tls_danger_accept_invalid_certs(skip_tls_verification)
            .user_agent(format!("ergosphere/{}", ERGOSPHERE_VERSION))
            .build()
            .unwrap_or_default();

        Ok(Self {
            base_url,
            label,
            http_client,
            session: None,
            token_expires_at: None,
            _marker: marker::PhantomData,
        })
    }

    /// Retrieves the CSRF token from the current session if authenticated.
    /// If authentication hasn't been performed yet, or if the authenticated session is missing the
    /// CSRF token, returns an error instead.
    fn get_csrf_token(&self) -> Result<String, ApiError> {
        let Some(session) = self.session.clone() else {
            return Err(ApiError::Unauthorized(
                "Client session is not authenticated. Please authenticate before making API calls."
                    .to_string(),
            ));
        };
        let Some(csrf) = session.csrf else {
            return Err(ApiError::Unauthorized(
                "Authenticated session is missing a valid CSRF token. Please re-authenticate to \
                 obtain a valid session."
                    .to_string(),
            ));
        };

        Ok(csrf)
    }

    /// Convenience function to map a deserialized error response into an `ApiError`.
    ///
    /// # Arguments
    ///
    /// * `status` - The response status code.
    /// * `resp` - The deserialized error response.
    fn map_error_response(status: StatusCode, resp: ApiResponse<ApiErrorPayload>) -> ApiError {
        match &*resp {
            ApiResult::Failure(payload) if payload.error.key == "unauthorized" => {
                ApiError::Unauthorized(payload.error.message.clone())
            }
            ApiResult::Failure(payload) => ApiError::Error(payload.error.message.clone().into()),
            _ => ApiError::UnexpectedStatusCode(status),
        }
    }

    /// Executes a session invalidation call against `/auth`.
    async fn invalidate_session_request(
        http_client: Client,
        auth_endpoint: Url,
        csrf: String,
    ) -> Result<StatusCode, reqwest::Error> {
        let response = http_client
            .delete(auth_endpoint)
            .header("X-CSRF-TOKEN", csrf)
            .send()
            .await?;

        Ok(response.status())
    }

    /// Returns the assigned label, or safely falls back to extracting the hostname/IP
    /// from the URL string for clean diagnostics.
    pub fn identifier(&self) -> String {
        if let Some(ref assigned_label) = self.label {
            return assigned_label.clone();
        }

        self.base_url
            .host_str()
            .unwrap_or_else(|| self.base_url.as_str())
            .to_string()
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

        debug!(endpoint = %auth_endpoint, payload = ?auth_payload, "Authenticating against Pi-hole API");

        let response = self
            .http_client
            .post(auth_endpoint)
            .json(&auth_payload)
            .send()
            .await?;
        let status = response.status();
        let response = response.json::<ApiResponse<ApiSessionPayload>>().await?;

        debug!(response = ?response, status = ?status, "Received response");

        match &*response {
            ApiResult::Success(payload) => {
                debug!(
                    valid = %payload.session.valid,
                    totp = %payload.session.totp,
                    validity = %payload.session.validity,
                    "Authentication session fetched successfully",
                );

                let safety_buffer = 5; // Expire the token early, to account for network latency and processing time before the next call.
                let lifetime =
                    Duration::from_secs(payload.session.validity.saturating_sub(safety_buffer));

                self.session = Some(payload.session.clone());
                self.token_expires_at = Some(Instant::now() + lifetime);
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

    /// Checks if the authenticated session has expired.
    pub fn is_session_expired(&self) -> bool {
        match self.token_expires_at {
            Some(deadline) => Instant::now() >= deadline,
            None => true, // If we never authenticated, it's structurally expired
        }
    }

    /// Invalidates the currently active Pi-hole API session, if one exists.
    pub async fn invalidate_session(&mut self) -> Result<(), ApiError> {
        let auth_endpoint = self.base_url.join("auth/")?;
        let csrf_token = self.get_csrf_token()?;

        debug!(endpoint = %auth_endpoint, "Invalidating API session");

        let status =
            Self::invalidate_session_request(self.http_client.clone(), auth_endpoint, csrf_token)
                .await?;

        if status.is_success() || status == StatusCode::UNAUTHORIZED {
            self.session = None;
            self.token_expires_at = None;
            return Ok(());
        }

        Err(ApiError::UnexpectedStatusCode(status))
    }
}

impl<Role> Drop for ApiClient<Role> {
    fn drop(&mut self) {
        let Ok(auth_endpoint) = self.base_url.join("auth/") else {
            return;
        };

        let http_client = self.http_client.clone();
        let Ok(csrf_token) = self.get_csrf_token() else {
            return;
        };

        // Best-effort cleanup: we cannot await in Drop, so use runtime task or a short-lived fallback runtime.
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let _ = ApiClient::<Role>::invalidate_session_request(
                    http_client,
                    auth_endpoint,
                    csrf_token,
                )
                .await;
            });
        } else {
            std::thread::spawn(move || {
                if let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    runtime.block_on(async move {
                        let _ = ApiClient::<Role>::invalidate_session_request(
                            http_client,
                            auth_endpoint,
                            csrf_token,
                        )
                        .await;
                    });
                }
            });
        }
    }
}

impl ApiClient<Primary> {
    /// Downloads the unified binary configuration Teleporter archive from the primary node.
    pub async fn download_teleporter_archive(&self) -> Result<Bytes, ApiError> {
        let teleporter_endpoint = self.base_url.join("teleporter/")?;
        let csrf_token = self.get_csrf_token()?;

        debug!(endpoint = %teleporter_endpoint, "Downloading teleporter archive");

        let response = self
            .http_client
            .get(teleporter_endpoint)
            .header("X-CSRF-TOKEN", csrf_token)
            .send()
            .await?;
        let status = response.status();

        debug!(response = ?response, status = ?status, "Received response");

        if status.is_success() {
            debug!("Successfully downloaded teleporter archive");
            let bytes = response.bytes().await?;
            return Ok(bytes);
        }

        if let Ok(resp) = response.json::<ApiResponse<ApiErrorPayload>>().await {
            return Err(Self::map_error_response(status, resp));
        }

        Err(ApiError::UnexpectedStatusCode(status))
    }
}

impl ApiClient<Replica> {
    /// Uploads and applies a raw Teleporter archive bundle directly to a replica node.
    ///
    /// # Arguments
    ///
    /// * `archive` - The Teleporter archive bytes.
    /// * `options` - Teleporter import options.
    pub async fn upload_teleporter_archive(
        &self,
        archive: Bytes,
        options: &TeleporterImportOptions,
    ) -> Result<(), ApiError> {
        let teleporter_endpoint = self.base_url.join("teleporter/")?;
        let csrf_token = self.get_csrf_token()?;
        let opts_json = serde_json::to_string(options)?;
        let form = multipart::Form::new()
            .part(
                "file",
                multipart::Part::bytes(archive.to_vec()).file_name("teleporter.zip"),
            )
            .text("import", opts_json);

        debug!(endpoint = %teleporter_endpoint, "Uploading teleporter archive");

        let response = self
            .http_client
            .post(teleporter_endpoint)
            .header("X-CSRF-TOKEN", csrf_token)
            .multipart(form)
            .send()
            .await?;
        let status = response.status();

        debug!(response = ?response, status = ?status, "Received response");

        if status.is_success() {
            return Ok(());
        }

        if let Ok(resp) = response.json::<ApiResponse<ApiErrorPayload>>().await {
            return Err(Self::map_error_response(status, resp));
        }

        Err(ApiError::UnexpectedStatusCode(status))
    }

    /// Orders the replica node's FTL engine to instantly recompile its gravity database
    /// tables from the newly synchronized adlist definitions.
    pub async fn trigger_gravity_rebuild(&self) -> Result<(), ApiError> {
        let gravity_endpoint = self.base_url.join("action/")?.join("gravity/")?;
        let csrf_token = self.get_csrf_token()?;

        debug!(endpoint = %gravity_endpoint, "Triggering gravity action");

        let response = self
            .http_client
            .post(gravity_endpoint)
            .query(&[("color", "false")]) // Disable ANSI colors since we skip logging outputs
            .header("X-CSRF-TOKEN", &csrf_token)
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            return Err(ApiError::UnexpectedStatusCode(status));
        }

        info!(node = %self.identifier(), "Gravity action triggered successfully");
        Ok(())
    }
}
