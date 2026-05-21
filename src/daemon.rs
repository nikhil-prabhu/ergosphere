//! Core orchestration engine managing event processing, debouncing, and API synchronization.

use std::error;
use std::time::Duration;

use tokio::sync::mpsc;

use crate::api::client::ApiClient;
use crate::watcher::DaemonEvent;

/// Orchestrates the event consumer runtime, linking filesystem triggers to network executions.
pub struct Daemon {
    primary_client: ApiClient,
    replicate_clients: Vec<ApiClient>,
    event_receiver: mpsc::Receiver<DaemonEvent>,
    debounce_duration: Duration,
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
        primary_client: ApiClient,
        replicate_clients: Vec<ApiClient>,
        event_receiver: mpsc::Receiver<DaemonEvent>,
        debounce_delay_secs: u64,
    ) -> Self {
        Self {
            primary_client,
            replicate_clients,
            event_receiver,
            debounce_duration: Duration::from_secs(debounce_delay_secs),
        }
    }

    /// Launches the persistent async block consumer loop.
    pub async fn run(mut self) {
        println!("Starting core event processing loop...");

        while let Some(event) = self.event_receiver.recv().await {
            match event {
                DaemonEvent::FileModified(_) => {
                    println!("Database write detected. Entering safety debounce window...");

                    if self.debounce().await {
                        println!("Debounce window cleared. Launching reactive sync pipeline...");
                        if let Err(e) = self.execute_sync_pipeline().await {
                            eprintln!("Pipeline execution failure: {e}");
                        }
                    }
                }
                DaemonEvent::WatcherError(err) => {
                    eprintln!("Fatal error received from monitoring thread: {err}");
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
                        println!("Cascading write detected. Resetting debounce clock...");
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
        println!("Pulling fresh teleporter configuration from primary...");
        // TODO: let backup = self.primary_client.get_teleporter_payload().await?;

        for replica in &self.replicate_clients {
            println!("Transmitting configuration state to replica endpoint...");
            // TODO: replica.push_teleporter_payload(&backup).await?;

            println!("Triggering active gravity database compilation on replica...");
            // TODO: replica.trigger_gravity_rebuild().await?;
            println!("Replica synchronization complete.");
        }

        Ok(())
    }
}
