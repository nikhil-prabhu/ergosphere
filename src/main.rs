use std::error;
use std::path::PathBuf;
use std::time::Duration;

use tokio::sync::mpsc;
use tracing::{error, info};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use crate::api::client::ApiClient;
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

    let mut _client = ApiClient::new("http://localhost:8080")?;
    _client.authenticate("password").await?;

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
