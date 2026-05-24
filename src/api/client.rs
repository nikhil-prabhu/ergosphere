//! Stateful HTTP client layer managing persistent sessions with target Pi-hole nodes.

use std::marker;
use std::time::Duration;

use bytes::Bytes;
use reqwest::{multipart, Client, StatusCode};
use tracing::debug;
use url::Url;

use crate::api::types::{
    ApiAuthPayload,
    ApiErrorPayload,
    ApiResponse,
    ApiResult,
    ApiSessionPayload,
    DatabaseInfo,
    SessionDetails,
    TeleporterImportOptions,
};
use crate::api::ApiError;

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
    _marker: marker::PhantomData<Role>,
}

impl<Role> ApiClient<Role> {
    /// Retrieves the SID from the current session if authenticated.
    /// If authentication hasn't been performed yet, or if the authenticated session is missing the
    /// SID, returns an error instead.
    fn get_sid(&self) -> Result<String, ApiError> {
        let Some(session) = self.session.clone() else {
            return Err(ApiError::Unauthorized(
                "Client session is not authenticated. Please authenticate before making API calls."
                    .to_string(),
            ));
        };
        let Some(sid) = session.sid else {
            return Err(ApiError::Unauthorized(
                "Authenticated session is missing a valid session ID (SID). Please \
                 re-authenticate to obtain a valid session."
                    .to_string(),
            ));
        };

        Ok(sid)
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

        debug!(target: "api", endpoint = %auth_endpoint, payload = ?auth_payload, "Authenticating against Pi-hole API");

        let response = self
            .http_client
            .post(auth_endpoint)
            .json(&auth_payload)
            .send()
            .await?;
        let status = response.status();
        let response = response.json::<ApiResponse<ApiSessionPayload>>().await?;

        debug!(target: "api", response = ?response, status = ?status, "Received response");

        match &*response {
            ApiResult::Success(payload) => {
                debug!(
                    target: "api",
                    valid = %payload.session.valid,
                    totp = %payload.session.totp,
                    validity = %payload.session.validity,
                    "Authentication session fetched successfully",
                );
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

    /// Extracts the current configuration revision state token from the `/info/database` endpoint,
    /// which tracks the last update timestamp of the gravity database.
    pub async fn get_gravity_state_token(&self) -> Result<i64, ApiError> {
        let info_endpoint = self.base_url.join("info/")?.join("database/")?;
        let sid = self.get_sid()?;

        debug!(target: "api", endpoint = %info_endpoint, "Getting gravity state token");

        let response = self
            .http_client
            .get(info_endpoint)
            .header("X-FTL-SID", &sid)
            .send()
            .await?;
        let status = response.status();
        let response = response.json::<ApiResponse<DatabaseInfo>>().await?;

        debug!(target: "api", response = ?response, status = ?status, "Received response");

        match &*response {
            ApiResult::Success(payload) => {
                debug!(
                    target: "api",
                    size_bytes = %payload.size,
                    current_queries = %payload.queries,
                    "Database metrics fetched successfully",
                );
                Ok(payload.mtime)
            }
            ApiResult::Failure(err) => {
                if err.error.key == "unauthorized" {
                    return Err(ApiError::Unauthorized(err.error.message.clone().into()));
                }
                Err(ApiError::Error(err.error.message.clone().into()))
            }
        }
    }
}

impl ApiClient<Primary> {
    /// Creates a new API client for the primary node against the Pi-hole v6 REST API endpoint
    /// with an uninitialized session.
    ///
    /// # Arguments
    ///
    /// * `raw_url` - The raw URL for the primary node. Eg: `"http://192.168.0.2"`.
    /// * `label` - An optional custom identifier for the node.
    pub fn new(raw_url: &str, label: Option<String>) -> Result<Self, ApiError> {
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
            label,
            http_client,
            session: None,
            _marker: marker::PhantomData,
        })
    }

    /// Downloads the unified binary configuration Teleporter archive from the primary node.
    pub async fn download_teleporter_archive(&self) -> Result<Bytes, ApiError> {
        let teleporter_endpoint = self.base_url.join("teleporter/")?;
        let sid = self.get_sid()?;

        debug!(target: "api", endpoint = %teleporter_endpoint, "Downloading teleporter archive");

        let response = self
            .http_client
            .get(teleporter_endpoint)
            .header("X-FTL-SID", &sid)
            .send()
            .await?;
        let status = response.status();

        debug!(target: "api", response = ?response, status = ?status, "Received response");

        if status.is_success() {
            debug!(target: "api", "Successfully downloaded teleporter archive");
            let bytes = response.bytes().await?;
            return Ok(bytes);
        }

        if let Ok(resp) = response.json::<ApiResponse<ApiErrorPayload>>().await {
            if let ApiResult::Failure(payload) = &*resp {
                if payload.error.key == "unauthorized" {
                    return Err(ApiError::Unauthorized(payload.error.message.clone().into()));
                }
                return Err(ApiError::Error(payload.error.message.clone().into()));
            }
        }

        Err(ApiError::UnexpectedStatusCode(status))
    }
}

impl ApiClient<Replica> {
    /// Creates a new API client for the replica node against the Pi-hole v6 REST API endpoint
    /// with an uninitialized session.
    ///
    /// # Arguments
    ///
    /// * `raw_url` - The raw URL for the replica node. Eg: `"http://192.168.0.3"`.
    /// * `label` - An optional custom identifier for the node.
    pub fn new(raw_url: &str, label: Option<String>) -> Result<Self, ApiError> {
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
            label,
            http_client,
            session: None,
            _marker: marker::PhantomData,
        })
    }

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
        let sid = self.get_sid()?;
        let opts_json = serde_json::to_string(options)?;
        let form = multipart::Form::new()
            .part(
                "file",
                multipart::Part::bytes(archive.to_vec()).file_name("teleporter.zip"),
            )
            .text("import", opts_json);

        debug!(target: "api", endpoint = %teleporter_endpoint, "Uploading teleporter archive");

        let response = self
            .http_client
            .post(teleporter_endpoint)
            .header("X-FTL-SID", &sid)
            .multipart(form)
            .send()
            .await?;
        let status = response.status();

        debug!(target: "api", response = ?response, status = ?status, "Received response");

        if status.is_success() {
            return Ok(());
        }

        if let Ok(resp) = response.json::<ApiResponse<ApiErrorPayload>>().await {
            if let ApiResult::Failure(payload) = &*resp {
                if payload.error.key == "unauthorized" {
                    return Err(ApiError::Unauthorized(payload.error.message.clone().into()));
                }
                return Err(ApiError::Error(payload.error.message.clone().into()));
            }
        }

        Err(ApiError::UnexpectedStatusCode(status))
    }
}
