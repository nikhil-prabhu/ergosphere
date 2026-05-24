//! Core orchestration engine managing event processing, debouncing, and API synchronization.

use std::error;
use std::path::PathBuf;
use std::time::Duration;

use tokio::sync::mpsc;
use tracing::{debug, error, info};

use crate::api::client::{ApiClient, Primary, Replica};
use crate::api::types::TeleporterImportOptions;
use crate::watcher::DaemonEvent;

/// Orchestrates the event consumer runtime, linking filesystem triggers to network executions.
pub struct Daemon {
    primary_client: ApiClient<Primary>,
    replicate_clients: Vec<ApiClient<Replica>>,
    event_receiver: mpsc::Receiver<DaemonEvent>,
    debounce_duration: Duration,
    last_known_timestamp: Option<i64>,
    watch_directory: PathBuf,
}

impl Daemon {
    /// Instantiates a unified daemon execution frame.
    ///
    /// # Arguments
    ///
    /// * `primary_client` - The API client for the primary Pi-hole.
    /// * `replica_clients` - The API clients for the Pi-hole replicas.
    /// * `event_receiver` - The receiver to read filesystem watcher events from.
    /// * `debounce_delay_secs` - The safety debounce window duration in seconds.
    pub fn new(
        primary_client: ApiClient<Primary>,
        replicate_clients: Vec<ApiClient<Replica>>,
        event_receiver: mpsc::Receiver<DaemonEvent>,
        debounce_delay_secs: u64,
        watch_directory: PathBuf,
    ) -> Self {
        Self {
            primary_client,
            replicate_clients,
            event_receiver,
            debounce_duration: Duration::from_secs(debounce_delay_secs),
            last_known_timestamp: None,
            watch_directory,
        }
    }

    /// Pulls a file's mtime from the filesystem, returning a `0` fallback if missing.
    async fn get_file_mtime(&self, path: &PathBuf) -> i64 {
        match tokio::fs::metadata(path).await {
            Ok(meta) => match meta.modified() {
                Ok(time) => match time.duration_since(std::time::UNIX_EPOCH) {
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
        let config_mtime = self
            .get_file_mtime(&self.watch_directory.join("pihole.toml"))
            .await;
        let gravity_mtime = self
            .get_file_mtime(&self.watch_directory.join("gravity.db"))
            .await;

        config_mtime + gravity_mtime
    }

    /// Launches the persistent async block consumer loop.
    pub async fn run(mut self) {
        info!("Starting core event processing loop...");

        let initial_token = self.calculate_global_state_token().await;
        self.last_known_timestamp = Some(initial_token);

        while let Some(event) = self.event_receiver.recv().await {
            match event {
                DaemonEvent::FileModified(_) => {
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
    }

    /// Absorbs rapid, cascading filesystem modifications using an adjustable sleep clock window (safety debounce).
    ///
    /// Returns `true` if the window closed quietly without interruptions.
    async fn debounce(&mut self) -> bool {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(self.debounce_duration) => {
                    return true;
                }

                next_event = self.event_receiver.recv() => {
                    if let Some(DaemonEvent::FileModified(_)) = next_event {
                        debug!("Cascading write detected. Resetting debounce clock...");
                    } else if let Some(DaemonEvent::WatcherError(_)) = next_event {
                        return false;
                    }
                }
            }
        }
    }

    /// The core synchronization pipeline sequence that ensures that the gravity database is rebuilt
    /// after the teleporter sync.
    async fn execute_sync_pipeline(&mut self) -> Result<(), Box<dyn error::Error>> {
        info!("Pulling fresh teleporter configuration from primary...");
        // TODO: let backup = self.primary_client.get_teleporter_payload().await?;
        let current_timestamp = self.calculate_global_state_token().await;

        if Some(current_timestamp) == self.last_known_timestamp {
            debug!(timestamp = %current_timestamp, "Sync skipped: remote structural metadata remains unchanged.");
            return Ok(());
        }

        info!(
            old_token = ?self.last_known_timestamp,
            new_token = %current_timestamp,
            "Configuration change detected. Initialization propagation sync...",
        );

        info!("Download configuration bundle package from primary node...");
        let archive = self.primary_client.download_teleporter_archive().await?;
        info!(
            target = %self.primary_client.identifier(),
            "Archive bundle downloaded successfully ({} bytes)",
            archive.len()
        );

        // TODO: replace `default()` with actual values read from config file.
        let sync_opts = TeleporterImportOptions::default();
        for replica in &self.replicate_clients {
            info!(target = %replica.identifier(), "Uploading payload to replica...");

            if let Err(e) = replica
                .upload_teleporter_archive(archive.clone(), &sync_opts)
                .await
            {
                error!(target = %replica.identifier(), "Failed to upload teleporter archive: {e}");
                continue;
            }

            info!(target = %replica.identifier(), "Requesting active gravity database recompilation...");

            match replica.trigger_gravity_rebuild().await {
                Ok(_) => {
                    info!(target = %replica.identifier(), "Replica is fully synchronized and up to date.")
                }
                Err(e) => {
                    error!(target = %replica.identifier(), "Failed to trigger gravity rebuild: {e}")
                }
            }

            self.last_known_timestamp = Some(current_timestamp);
        }

        Ok(())
    }
}
