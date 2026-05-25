use std::io;

use tokio::sync::mpsc;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{fmt, EnvFilter};

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

    let config = AppConfig::load()?;

    let (tx, rx) = mpsc::channel(32);
    let _watcher = DbWatcher::new(&config.daemon.watch_directory, tx)?;
    let daemon = Daemon::new(config, rx)?;

    daemon.run().await;

    Ok(())
}
