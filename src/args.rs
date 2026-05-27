use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(
    name = "ergosphere",
    author = "Nikhil Prabhu",
    version,
    about = "A reactive, event-driven push replication daemon for Pi-hole v6 replica setups",
    long_about = "ergosphere monitors structural database alterations on a primary Pi-hole \
                  instance \nand automatically replicates states across replica cluster node \
                  topologies in real-time."
)]
pub struct CliArgs {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Synchronize state from the primary node to replica nodes
    Sync {
        /// Run as a long-running background daemon, monitoring filesystem events
        #[arg(short, long)]
        daemon: bool,

        /// Queue an immediate catch-up synchronization cycle upon daemon initialization
        /// (Requires `--daemon`)
        #[arg(short, long, requires = "daemon")]
        force_sync: bool,
    },
}
