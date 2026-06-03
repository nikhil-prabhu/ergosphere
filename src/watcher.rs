//! Reactive filesystem monitoring subsystem for `ergosphere`.
//!
//! This module provides a high-level abstraction over the OS-native filesystem
//! event engine (via the `notify` crate). It monitors the primary Pi-hole
//! configuration database for write or modification transactions and routes
//! those system occurrences back to the central asynchronous async loop.
//!
//! # Architecture
//!
//! Core OS filesystem event hooks execute synchronously within a dedicated system
//! thread pool managed by the kernel abstraction layer. To prevent blocking or
//! starving the primary async scheduling framework, this subsystem wraps a bounded
//! multi-producer, single-consumer (`tokio::sync::mpsc`) channel to safely pass
//! signals into the async runtime.
//!
//! ```text
//! [Kernel/FS Events] ──> [notify Thread Pool] ──(blocking_send)──> [Tokio MPSC Channel] ──> [Daemon Event Loop]
//! ```

use std::path::Path;
use std::time::Duration;

use notify::{RecommendedWatcher, RecursiveMode};
use notify_debouncer_full::{DebouncedEvent, Debouncer, RecommendedCache, new_debouncer};
use tokio::sync::mpsc::Sender;
use tracing::debug;

use crate::consts::{PIHOLE_CONFIG_FILE, PIHOLE_GRAVITY_DB};

/// Errors that can occur during the setup or execution of the filesystem watcher.
///
/// This enum captures initial synchronous tracking registration failures as well as
/// unexpected asynchronous channel errors that happen during background processing.
#[derive(Debug, thiserror::Error)]
pub enum WatcherError {
    #[error("Notify filesystem framework error: {0}")]
    Notify(#[from] notify::Error),
    #[error("Failed to route file event over internal tokio channel")]
    ChannelSend,
}

/// The core asynchronous communication protocol for Ergosphere's event loop.
///
/// Events are emitted by the background filesystem watcher thread and consumed by the
/// main daemon synchronization loop. This multi-producer, single-consumer channel architecture
/// safely bridges the gap between synchronous OS filesystem hooks and the async execution runtime.
#[derive(Debug)]
pub enum DaemonEvent {
    StateChange(Vec<DebouncedEvent>),
    WatcherError(WatcherError),
}

/// An active filesystem event listener anchoring an OS-native directory monitor.
///
/// This struct manages the lifecycle of a background filesystem tracking engine.
/// It must remain alive in memory for the duration of the monitoring session;
/// dropping this struct will automatically unregister the underlying OS kernel
/// hooks and spin down the background worker thread.
pub struct DbWatcher {
    _debouncer: Debouncer<RecommendedWatcher, RecommendedCache>,
}

impl DbWatcher {
    /// Spawns an OS-native file watcher that monitors the specified Pi-hole
    /// config directory and broadcasts modification events back over an async channel.
    ///
    /// # Arguments
    ///
    /// * `pihole_dir` - The path to the Pi-hole configuration directory (usually `/etc/pihole`).
    /// * `debounce_seconds` - The time window used by the underlying debouncer engine to group file writes.
    /// * `event_sender` - An async channel sender to route events back to the main application.
    pub fn new<P: AsRef<Path>>(
        pihole_dir: P,
        debounce_seconds: u64,
        event_sender: Sender<DaemonEvent>,
    ) -> Result<Self, WatcherError> {
        let thread_sender = event_sender.clone();
        let debounce_duration = Duration::from_secs(debounce_seconds);

        let mut debouncer = new_debouncer(
            debounce_duration,
            None,
            move |res: Result<Vec<DebouncedEvent>, Vec<notify::Error>>| match res {
                Ok(events) => {
                    let should_trigger = events.iter().any(|debounced_event| {
                        debounced_event.paths.iter().any(|p| {
                            let filename = p.file_name().unwrap_or_default().to_string_lossy();
                            debug!(filename = %filename, "File modified batch intercepted");

                            filename == PIHOLE_GRAVITY_DB
                                || filename == PIHOLE_CONFIG_FILE
                                || filename.starts_with(PIHOLE_CONFIG_FILE)
                        })
                    });

                    if should_trigger {
                        if let Err(_) =
                            thread_sender.blocking_send(DaemonEvent::StateChange(events))
                        {
                            let _ = thread_sender.blocking_send(DaemonEvent::WatcherError(
                                WatcherError::ChannelSend,
                            ));
                        }
                    }
                }
                Err(errors) => {
                    if let Some(first_err) = errors.into_iter().next() {
                        let _ = thread_sender.blocking_send(DaemonEvent::WatcherError(
                            WatcherError::Notify(first_err),
                        ));
                    }
                }
            },
        )?;

        debouncer.watch(pihole_dir.as_ref(), RecursiveMode::NonRecursive)?;

        debug!("Watcher initialized");

        Ok(Self {
            _debouncer: debouncer,
        })
    }
}
