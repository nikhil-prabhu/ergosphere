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

use notify::{Config, Event, RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::mpsc::Sender;

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
    FileModified(Event),
    WatcherError(WatcherError),
}

/// An active filesystem event listener anchoring an OS-native directory monitor.
///
/// This struct manages the lifecycle of a background filesystem tracking engine.
/// It must remain alive in memory for the duration of the monitoring session;
/// dropping this struct will automatically unregister the underlying OS kernel
/// hooks and spin down the background worker thread.
pub struct DbWatcher {
    _watcher: RecommendedWatcher,
}

impl DbWatcher {
    /// Spawns an OS-native file watcher that monitors a specified path
    /// and broadcasts modification events back over an async channel.
    ///
    /// # Arguments
    ///
    /// * `target_path` - The path to the file or directory to be monitored for changes.
    /// * `event_sender` - An async channel sender to route events back to the main application.
    pub fn new<P: AsRef<Path>>(
        target_path: P,
        event_sender: Sender<DaemonEvent>,
    ) -> Result<Self, WatcherError> {
        let thread_sender = event_sender.clone();

        let mut watcher = RecommendedWatcher::new(
            move |res: Result<Event, notify::Error>| match res {
                Ok(event) => {
                    if event.kind.is_modify() {
                        if let Err(_) =
                            thread_sender.blocking_send(DaemonEvent::FileModified(event))
                        {
                            let _ = thread_sender.blocking_send(DaemonEvent::WatcherError(
                                WatcherError::ChannelSend,
                            ));
                        }
                    }
                }
                Err(e) => {
                    let _ = thread_sender
                        .blocking_send(DaemonEvent::WatcherError(WatcherError::Notify(e)));
                }
            },
            Config::default(),
        )?;

        watcher.watch(target_path.as_ref(), RecursiveMode::NonRecursive)?;

        Ok(Self { _watcher: watcher })
    }
}
