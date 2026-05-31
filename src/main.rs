use std::io;

use clap::Parser;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::mpsc;
use tracing::{error, info};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{fmt, EnvFilter};

use crate::args::{CliArgs, Commands};
use crate::config::AppConfig;
use crate::daemon::Daemon;
use crate::watcher::DbWatcher;

mod api;
mod args;
mod config;
pub(crate) mod consts;
mod daemon;
mod watcher;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(fmt::layer().with_writer(io::stderr))
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let args = CliArgs::parse();
    let config = AppConfig::load()?;

    match args.command {
        Commands::Sync { daemon, force_sync } => {
            if daemon {
                info!("Running as daemon...");

                let (tx, rx) = mpsc::channel(32);
                let _watcher = DbWatcher::new(&config.daemon.watch_directory, tx)?;
                let mut daemon_engine = Daemon::new(config, rx)?;
                let mut sigterm = signal(SignalKind::terminate())?;

                tokio::select! {
                    _ = daemon_engine.run(force_sync) => {}
                    _ = tokio::signal::ctrl_c() => {
                        info!("Received Ctrl+C. Shutting down daemon...");
                        daemon_engine.shutdown().await;
                    }
                    _ = sigterm.recv() => {
                        info!("Received SIGTERM. Shutting down daemon...");
                        daemon_engine.shutdown().await;
                    }
                }
            } else {
                info!("Executing a single-pass cluster state synchronization...");

                // One-off sync mode: pass a dummy bound channel since no watcher runs
                let (_, rx) = mpsc::channel(1);
                let mut daemon_engine = Daemon::new(config, rx)?;

                if let Err(e) = daemon_engine.run_once().await {
                    daemon_engine.shutdown().await;
                    error!("One-off synchronization pass encountered a fatal error: {e}");
                    std::process::exit(1);
                }

                daemon_engine.shutdown().await;
                info!("One-off synchronization completed successfully.");
            }
        }
    }

    Ok(())
}
