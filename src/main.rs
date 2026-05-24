use std::error;
use std::path::PathBuf;
use std::time::Duration;

use tokio::sync::mpsc;
use tracing::{error, info};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use crate::api::client::{ApiClient, Primary, Replica};
use crate::api::types::TeleporterImportOptions;
use crate::watcher::{DaemonEvent, DbWatcher};

mod api;
mod config;
mod daemon;
mod watcher;

#[tokio::main]
async fn main() -> Result<(), Box<dyn error::Error>> {
    tracing_subscriber::registry()
        .with(fmt::layer().with_writer(std::io::stdout))
        .with(EnvFilter::from_default_env())
        .init();

    info!("Initializing testing environment...");

    // TODO: Replace with proper non-hardcoded values (read from config) before shipping.
    let mut primary_client = ApiClient::<Primary>::new("http://localhost:8080")?;
    let mut replica_client = ApiClient::<Replica>::new("http://localhost:8081")?;
    primary_client.authenticate("password").await?;
    replica_client.authenticate("password").await?;

    let last_updated = primary_client.get_gravity_state_token().await?;
    info!(
        "Current gravity.db last updated timestamp: {}",
        last_updated
    );

    let (tx, mut rx) = mpsc::channel(32);
    let target_db = PathBuf::from("./etc-pihole-primary");
    let _watcher = DbWatcher::new(&target_db, tx)?;

    info!("Watching target: {}", target_db.display());
    info!(
        "Modify this file or run `touch {}` to test triggers...",
        target_db.display()
    );

    while let Some(daemon_event) = rx.recv().await {
        match daemon_event {
            DaemonEvent::FileModified(_event) => {
                info!("File change detected. Entering debounce window...");

                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(3)) => {
                        info!("Debounce window cleared. Triggering reactive sync execution...");

                        // TODO: replace with actual sync execution pipeline.
                        let archive = primary_client.download_teleporter_archive().await?;
                        let opts = TeleporterImportOptions::default();
                        let _ = replica_client.upload_teleporter_archive(archive, &opts).await?;

                        info!("Reactive sync execution completed");
                    }
                    next_event = rx.recv() => {
                        if next_event.is_some() {
                            info!("Cascading write detected. Resetting debounce clock...");
                        }
                    }
                }
            }
            DaemonEvent::WatcherError(err) => {
                error!("Fatal daemon background thread error: {}", err);
                break;
            }
        }
    }

    Ok(())
}
