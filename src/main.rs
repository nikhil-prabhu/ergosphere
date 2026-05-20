use std::path::PathBuf;
use std::time::Duration;
use std::{error, fs};
use tokio::sync::mpsc;

use crate::watcher::{DaemonEvent, DbWatcher};

mod watcher;

#[tokio::main]
async fn main() -> Result<(), Box<dyn error::Error>> {
    println!("Initializing testing environment...");

    let (tx, mut rx) = mpsc::channel(32);
    let target_db = PathBuf::from("./test.db");

    if !target_db.exists() {
        fs::File::create(&target_db)?;
    }

    let _watcher = DbWatcher::new(&target_db, tx)?;

    println!("Watching target: {}", target_db.display());
    println!(
        "Modify this file or run `touch {}` to test triggers...",
        target_db.display()
    );

    while let Some(daemon_event) = rx.recv().await {
        match daemon_event {
            DaemonEvent::FileModified(_event) => {
                println!("File change detected. Entering debounce window...");

                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(3)) => {
                        println!("Debounce window cleared. Triggering reactive sync execution...");
                    }
                    next_event = rx.recv() => {
                        if next_event.is_some() {
                            println!("Cascading write detected. Resetting debounce clock...");
                        }
                    }
                }
            }
            DaemonEvent::WatcherError(err) => {
                eprintln!("Fatal daemon background thread error: {}", err);
                break;
            }
        }
    }

    Ok(())
}
