//! Core orchestration engine managing event processing, debouncing, and API synchronization.

use std::fmt::Display;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;
use bytes::Bytes;
use tokio::sync::mpsc;
use tracing::{debug, error, info};

use crate::api::client::{ApiClient, Primary, Replica};
use crate::api::types::TeleporterImportOptions;
use crate::config::AppConfig;
use crate::consts::{PIHOLE_CONFIG_FILE, PIHOLE_GRAVITY_DB};
use crate::watcher::WatcherEvent;

/// Orchestrates the event consumer runtime, linking filesystem triggers to network executions.
pub struct Daemon {
    config: AppConfig,
    event_receiver: mpsc::Receiver<WatcherEvent>,
    last_known_hash: Option<String>,
    primary_client: ApiClient<Primary>,
    replica_clients: Vec<ApiClient<Replica>>,
}

#[derive(Debug)]
enum SyncMode {
    Full,
    Selective,
}

impl Display for SyncMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SyncMode::Full => write!(f, "full"),
            SyncMode::Selective => write!(f, "selective"),
        }
    }
}

impl Daemon {
    /// Instantiates a unified daemon execution frame with persistent type-safe clients.
    pub fn new(
        config: AppConfig,
        event_receiver: mpsc::Receiver<WatcherEvent>,
    ) -> anyhow::Result<Self> {
        let client_timeout = Duration::from_secs(config.daemon.client_timeout_seconds);
        let primary_client = ApiClient::<Primary>::new(
            &config.primary.url,
            config.primary.label.clone(),
            client_timeout,
            config.daemon.client_skip_tls_verification,
        )
        .with_context(|| {
            format!(
                "Failed to initialize primary node client using URL: '{}'",
                config.primary.url
            )
        })?;

        let mut replica_clients = Vec::new();
        for replica_conf in &config.replicas {
            let replica_client = ApiClient::<Replica>::new(
                &replica_conf.url,
                replica_conf.label.clone(),
                client_timeout,
                config.daemon.client_skip_tls_verification,
            )
            .with_context(|| {
                format!(
                    "Failed to initialize replica node client using URL: '{}'",
                    replica_conf.url
                )
            })?;
            replica_clients.push(replica_client);
        }

        Ok(Self {
            config,
            event_receiver,
            last_known_hash: None,
            primary_client,
            replica_clients,
        })
    }

    /// Reads targets and aggregates content into a deterministic hash token to detect true content drift.
    async fn calculate_global_state_hash(&self) -> String {
        use sha2::{Digest, Sha256};

        let watch_path = PathBuf::from(&self.config.daemon.watch_directory);
        let mut hasher = Sha256::new();

        if let Ok(config_bytes) = tokio::fs::read(watch_path.join(PIHOLE_CONFIG_FILE)).await {
            hasher.update(b"config:");
            hasher.update(&config_bytes);
        }

        if let Ok(gravity_bytes) = tokio::fs::read(watch_path.join(PIHOLE_GRAVITY_DB)).await {
            hasher.update(b"gravity:");
            hasher.update(&gravity_bytes);
        }

        let finalize_result = hasher.finalize();
        finalize_result
            .iter()
            .map(|byte| format!("{:02x}", byte))
            .collect::<String>()
    }

    /// Dedicated internal helper to authenticate all managed API clients at once.
    async fn authenticate_all_clients(&mut self) -> anyhow::Result<()> {
        info!("Authenticating clients...");

        self.primary_client
            .authenticate(&self.config.primary.password)
            .await
            .with_context(|| {
                format!(
                    "Failed to authenticate primary node client '{}'",
                    self.primary_client.identifier()
                )
            })?;

        for (i, replica_conf) in self.config.replicas.iter().enumerate() {
            self.replica_clients[i]
                .authenticate(&replica_conf.password)
                .await
                .with_context(|| {
                    format!(
                        "Failed to authenticate replica node client '{}'",
                        self.replica_clients[i].identifier(),
                    )
                })?;
        }

        info!("Clients authenticated successfully");
        Ok(())
    }

    /// Best-effort invalidation of active API sessions across all nodes.
    pub async fn shutdown(&mut self) {
        info!("Invalidating active API sessions before shutdown...");

        if let Err(e) = self.primary_client.invalidate_session().await {
            error!(
                target = %self.primary_client.identifier(),
                error = %e,
                "Failed to invalidate primary session"
            );
        }

        for replica in &mut self.replica_clients {
            if let Err(e) = replica.invalidate_session().await {
                error!(
                    target = %replica.identifier(),
                    error = %e,
                    "Failed to invalidate replica session",
                );
            }
        }
    }

    /// Runs the synchronization pipeline once, bypassing event gates, the debounce clock and state token checks.
    pub async fn run_once(&mut self) -> anyhow::Result<()> {
        self.authenticate_all_clients().await?;

        let current_hash = self.calculate_global_state_hash().await;

        info!(target = %self.primary_client.identifier(), "Downloading teleporter archive");
        let archive = self
            .primary_client
            .download_teleporter_archive()
            .await
            .with_context(|| {
                format!(
                    "Failed to download teleporter bundle from primary node '{}'",
                    self.primary_client.identifier()
                )
            })?;

        info!(
            target = %self.primary_client.identifier(),
            bytes = %archive.len(),
            "Teleporter archive downloaded successfully",
        );

        let sync_opts = self.config.get_teleporter_import_options();

        self.replica_sync_loop(archive, &sync_opts).await;

        self.last_known_hash = Some(current_hash);
        Ok(())
    }

    /// Launches the persistent async block consumer loop.
    ///
    /// # Arguments
    ///
    /// * `force_sync` - Whether to forcefully run a synchronization first before starting the consumer loop.
    pub async fn run(&mut self, force_sync: bool) {
        if force_sync {
            info!(force_sync = %force_sync, "Starting core event loop");
            if let Err(e) = self.run_once().await {
                error!(error = %e, "Sync error");
            }
        } else {
            if let Err(e) = self.authenticate_all_clients().await {
                error!(error = %e, "Authentication failure");
                self.shutdown().await;
                return;
            }
            let initial_hash = self.calculate_global_state_hash().await;
            self.last_known_hash = Some(initial_hash);
        }

        while let Some(event) = self.event_receiver.recv().await {
            match event {
                WatcherEvent::StateChange(_events) => {
                    info!("Debounced state change detected. Executing pipeline...");
                    if let Err(e) = self.execute_sync_pipeline().await {
                        error!(error = %e, "Pipeline execution failure");
                    }
                }
                WatcherEvent::WatcherError(err) => {
                    error!(error = %err, "Fatal error received from monitoring thread");
                    break;
                }
            }
        }

        self.shutdown().await;
    }

    /// The core synchronization pipeline sequence mapped to reactive filesystem triggers.
    async fn execute_sync_pipeline(&mut self) -> anyhow::Result<()> {
        let current_hash = self.calculate_global_state_hash().await;
        let sync_mode = if self.config.sync.full_sync {
            SyncMode::Full
        } else {
            SyncMode::Selective
        };
        let replicas = self.replica_clients.len();

        if Some(&current_hash) == self.last_known_hash.as_ref() {
            info!("State unchanged. Skipping sync");
            return Ok(());
        }

        let primary_expired = self.primary_client.is_session_expired();
        let replica_expired = self
            .replica_clients
            .iter()
            .any(ApiClient::<Replica>::is_session_expired);

        if primary_expired || replica_expired {
            debug!(
                primary_expired = primary_expired,
                replica_expired = replica_expired,
                "At least one session appears stale. Refreshing sessions for all nodes"
            );
            self.authenticate_all_clients().await?;
        }

        let last_known_hash = self.last_known_hash.as_deref().unwrap_or("");
        debug!(
            old_state_token = %last_known_hash,
            new_state_token = %current_hash,
            "State content tokens differ",
        );
        info!(
            mode = %sync_mode,
            replicas = %replicas,
            "State change confirmed. Initializing sync",
        );

        info!(
            target = self.primary_client.identifier(),
            "Downloading teleporter archive bundle"
        );
        let archive = self
            .primary_client
            .download_teleporter_archive()
            .await
            .with_context(|| {
                format!(
                    "Failed to download teleporter bundle from primary node '{}'",
                    self.primary_client.identifier()
                )
            })?;

        info!(
            target = %self.primary_client.identifier(),
            bytes = %archive.len(),
            "Teleporter archive downloaded successfully",
        );

        let sync_opts = self.config.get_teleporter_import_options();

        self.replica_sync_loop(archive, &sync_opts).await;

        self.last_known_hash = Some(current_hash);

        Ok(())
    }

    /// Loops over the replica clients, uploading the teleporter archive and triggering gravity rebuilds sequentially.
    async fn replica_sync_loop(&mut self, archive: Bytes, sync_opts: &TeleporterImportOptions) {
        for replica in &self.replica_clients {
            info!(target = %replica.identifier(), "Uploading teleporter archive bundle");

            if let Err(e) = replica
                .upload_teleporter_archive(archive.clone(), &sync_opts)
                .await
            {
                error!(
                    target = %replica.identifier(),
                    error = %e,
                    "Failed to upload teleporter archive bundle",
                );
                continue;
            }

            let run_gravity = self.config.sync.run_gravity;
            if !run_gravity {
                info!(
                    target = %replica.identifier(),
                    run_gravity = self.config.sync.run_gravity,
                    "Skipping gravity action"
                );
                continue;
            }

            info!(target = %replica.identifier(), run_gravity = run_gravity,  "Running gravity action");

            match replica.trigger_gravity_rebuild().await {
                Ok(_) => {
                    info!(
                        target = %replica.identifier(),
                        "Replica is fully synchronized and up to date",
                    )
                }
                Err(e) => {
                    error!(
                        target = %replica.identifier(),
                        error = %e,
                        "Failed to run gravity action",
                    )
                }
            }
        }
    }
}
