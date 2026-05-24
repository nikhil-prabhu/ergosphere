use std::io;
use std::path::PathBuf;

use tokio::sync::mpsc;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{fmt, EnvFilter};

use crate::api::client::{ApiClient, Primary, Replica};
use crate::config::AppConfig;
use crate::daemon::Daemon;
use crate::watcher::DbWatcher;

mod api;
mod config;
mod daemon;
mod watcher;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(fmt::layer().with_writer(io::stdout))
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let settings = AppConfig::load()?;
    let watch_dir = PathBuf::from(&settings.daemon.watch_directory);
    let mut primary_client =
        ApiClient::<Primary>::new(&settings.primary.url, settings.primary.label.clone())?;

    primary_client
        .authenticate(&settings.primary.password)
        .await?;

    let mut replica_clients = Vec::new();
    for replica_conf in &settings.replicas {
        let mut replica_client =
            ApiClient::<Replica>::new(&replica_conf.url, replica_conf.label.clone())?;
        replica_client.authenticate(&replica_conf.password).await?;
        replica_clients.push(replica_client);
    }

    let (tx, rx) = mpsc::channel(32);
    let _watcher = DbWatcher::new(&watch_dir, tx)?;
    let daemon = Daemon::new(
        primary_client,
        replica_clients,
        rx,
        settings.daemon.debounce_seconds,
        watch_dir.clone(),
    );

    daemon.run().await;

    Ok(())
}
