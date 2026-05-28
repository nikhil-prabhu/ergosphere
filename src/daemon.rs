//! Core orchestration engine managing event processing, debouncing, and API synchronization.

use std::error;
use std::path::PathBuf;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::mpsc;
use tracing::{debug, error, info};

use crate::api::client::{ApiClient, Primary, Replica};
use crate::api::types::TeleporterImportOptions;
use crate::config::AppConfig;
use crate::consts::{PIHOLE_CONFIG_FILE, PIHOLE_GRAVITY_DB};
use crate::watcher::DaemonEvent;

/// Orchestrates the event consumer runtime, linking filesystem triggers to network executions.
pub struct Daemon {
    config: AppConfig,
    event_receiver: mpsc::Receiver<DaemonEvent>,
    last_known_timestamp: Option<i64>,
    primary_client: ApiClient<Primary>,
    replica_clients: Vec<ApiClient<Replica>>,
}

impl Daemon {
    /// Instantiates a unified daemon execution frame with persistent type-safe clients.
    pub fn new(
        config: AppConfig,
        event_receiver: mpsc::Receiver<DaemonEvent>,
    ) -> Result<Self, Box<dyn error::Error>> {
        let client_timeout = Duration::from_secs(config.daemon.client_timeout_seconds);
        let primary_client = ApiClient::<Primary>::new(
            &config.primary.url,
            config.primary.label.clone(),
            client_timeout,
            config.daemon.client_skip_tls_verification,
        )?;

        let mut replica_clients = Vec::new();
        for replica_conf in &config.replicas {
            let replica_client = ApiClient::<Replica>::new(
                &replica_conf.url,
                replica_conf.label.clone(),
                client_timeout,
                config.daemon.client_skip_tls_verification,
            )?;
            replica_clients.push(replica_client);
        }

        Ok(Self {
            config,
            event_receiver,
            last_known_timestamp: None,
            primary_client,
            replica_clients,
        })
    }

    /// Pulls a file's mtime from the filesystem, returning a `0` fallback if missing.
    async fn get_file_mtime(&self, path: &PathBuf) -> i64 {
        match tokio::fs::metadata(path).await {
            Ok(meta) => match meta.modified() {
                Ok(time) => match time.duration_since(std::time::SystemTime::UNIX_EPOCH) {
                    Ok(duration) => duration.as_secs() as i64,
                    Err(_) => {
                        error!("File modified time is before UNIX epoch.");
                        0
                    }
                },
                Err(_) => 0,
            },
            Err(_) => 0,
        }
    }

    /// Aggregates the local filesystem states into a single validation token.
    async fn calculate_global_state_token(&self) -> i64 {
        let watch_path = PathBuf::from(&self.config.daemon.watch_directory);
        let config_mtime = self
            .get_file_mtime(&watch_path.join(PIHOLE_CONFIG_FILE))
            .await;
        let gravity_mtime = self
            .get_file_mtime(&watch_path.join(PIHOLE_GRAVITY_DB))
            .await;

        config_mtime + gravity_mtime
    }

    /// Dedicated internal helper to authenticate all managed API clients at once.
    async fn authenticate_all_clients(&mut self) -> Result<(), Box<dyn error::Error>> {
        info!("Authenticating programmatic connection handles against core nodes...");

        self.primary_client
            .authenticate(&self.config.primary.password)
            .await?;

        for (i, replica_conf) in self.config.replicas.iter().enumerate() {
            self.replica_clients[i]
                .authenticate(&replica_conf.password)
                .await?;
        }

        info!("All remote connection scopes authenticated successfully.");
        Ok(())
    }

    /// Best-effort invalidation of active API sessions across all nodes.
    pub async fn shutdown(&mut self) {
        info!("Invalidating active API sessions before shutdown...");

        if let Err(e) = self.primary_client.invalidate_session().await {
            error!(
                target = %self.primary_client.identifier(),
                "Failed to invalidate primary session: {e}"
            );
        }

        for replica in &mut self.replica_clients {
            if let Err(e) = replica.invalidate_session().await {
                error!(target = %replica.identifier(), "Failed to invalidate replica session: {e}");
            }
        }
    }

    /// Runs the synchronization pipeline once, bypassing event gates, the debounce clock and state token checks.
    pub async fn run_once(&mut self) -> Result<(), Box<dyn error::Error>> {
        if let Err(e) = self.authenticate_all_clients().await {
            error!("Failed to establish cluster authentication for one-off sync: {e}");
            return Err(e);
        }

        let current_timestamp = self.calculate_global_state_token().await;

        info!("Downloading configuration bundle package from primary node...");
        let archive = self.primary_client.download_teleporter_archive().await?;
        info!(
            target = %self.primary_client.identifier(),
            "Archive bundle downloaded successfully ({} bytes)",
            archive.len()
        );

        let sync_opts = self.config.get_teleporter_import_options();

        self.replica_sync_loop(archive, &sync_opts).await;

        // Cache the state token so the following daemon watcher cycles don't duplicate this work
        self.last_known_timestamp = Some(current_timestamp);
        Ok(())
    }

    /// Launches the persistent async block consumer loop.
    ///
    /// # Arguments
    ///
    /// * `force_sync` - Whether to forcefully run a synchronization first before starting the consumer loop.
    pub async fn run(&mut self, force_sync: bool) {
        info!("Starting core event processing loop...");

        if force_sync {
            info!("Bypassing event gates via `--force-sync`. Starting baseline catch-up sync...");
            if let Err(e) = self.run_once().await {
                error!("Startup baseline force-sync encountered an error: {e}");
                info!("Resuming regular background event loop monitoring fallback operations.");
            }
        } else {
            if let Err(e) = self.authenticate_all_clients().await {
                error!("Fatal: Failed to establish initial node authentication: {e}");
                self.shutdown().await;
                return;
            }
            let initial_token = self.calculate_global_state_token().await;
            self.last_known_timestamp = Some(initial_token);
        }

        while let Some(event) = self.event_receiver.recv().await {
            match event {
                DaemonEvent::FileModified => {
                    info!("Database write detected. Entering safety debounce window...");

                    if self.debounce().await {
                        info!("Debounce window cleared. Launching reactive sync pipeline...");
                        if let Err(e) = self.execute_sync_pipeline().await {
                            error!("Pipeline execution failure: {e}");
                        }
                    }
                }
                DaemonEvent::WatcherError(err) => {
                    error!("Fatal error received from monitoring thread: {err}");
                    break;
                }
            }
        }

        self.shutdown().await;
    }

    /// Absorbs rapid, cascading filesystem modifications using an adjustable sleep clock window.
    async fn debounce(&mut self) -> bool {
        let debounce_duration = Duration::from_secs(self.config.daemon.debounce_seconds);
        loop {
            tokio::select! {
                _ = tokio::time::sleep(debounce_duration) => {
                    return true;
                }
                next_event = self.event_receiver.recv() => {
                    if let Some(DaemonEvent::FileModified) = next_event {
                        debug!("Cascading write detected. Resetting debounce clock...");
                    } else if let Some(DaemonEvent::WatcherError(_)) = next_event {
                        return false;
                    }
                }
            }
        }
    }

    /// The core synchronization pipeline sequence mapped to reactive filesystem triggers.
    async fn execute_sync_pipeline(&mut self) -> Result<(), Box<dyn error::Error>> {
        let current_timestamp = self.calculate_global_state_token().await;

        if Some(current_timestamp) == self.last_known_timestamp {
            debug!(timestamp = %current_timestamp, "Sync skipped: remote structural metadata remains unchanged.");
            return Ok(());
        }

        let primary_expired = self.primary_client.is_session_expired();
        let replica_expired = self
            .replica_clients
            .iter()
            .any(ApiClient::<Replica>::is_session_expired);

        if primary_expired || replica_expired {
            debug!(
                primary_expired,
                replica_expired,
                "At least one node session appears stale. Refreshing credentials for all nodes..."
            );
            self.authenticate_all_clients().await?;
        }

        info!(
            old_token = ?self.last_known_timestamp,
            new_token = %current_timestamp,
            "Configuration change detected. Initializing propagation sync...",
        );

        info!("Downloading configuration bundle package from primary node...");
        let archive = self.primary_client.download_teleporter_archive().await?;
        info!(
            target = %self.primary_client.identifier(),
            "Archive bundle downloaded successfully ({} bytes)",
            archive.len()
        );

        let sync_opts = self.config.get_teleporter_import_options();

        self.replica_sync_loop(archive, &sync_opts).await;

        self.last_known_timestamp = Some(current_timestamp);

        Ok(())
    }

    /// Loops over the replica clients, uploading the teleporter archive and triggering gravity rebuilds sequentially.
    async fn replica_sync_loop(&mut self, archive: Bytes, sync_opts: &TeleporterImportOptions) {
        for replica in &self.replica_clients {
            info!(target = %replica.identifier(), "Uploading payload to replica...");

            if let Err(e) = replica
                .upload_teleporter_archive(archive.clone(), &sync_opts)
                .await
            {
                error!(target = %replica.identifier(), "Failed to upload teleporter archive: {e}");
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

            info!(target = %replica.identifier(), run_gravity = run_gravity,  "Requesting active gravity database recompilation...");

            match replica.trigger_gravity_rebuild().await {
                Ok(_) => {
                    info!(target = %replica.identifier(), "Replica is fully synchronized and up to date.")
                }
                Err(e) => {
                    error!(target = %replica.identifier(), "Failed to trigger gravity rebuild: {e}")
                }
            }
        }
    }
}
