//! Application configuration orchestration and layering system.
//!
//! # Example config file
//!
//! ```toml
//! [daemon]
//! debounce_seconds = 4
//! watch_directory = "./etc-pihole-primary"
//!
//! [primary]
//! label = "pihole-primary"
//! url = "http://192.168.0.2:8080"
//! password = "password"
//!
//! [[replicas]]
//! label = "pihole-replica1"
//! url = "http://192.168.0.3:8081"
//! password = "password"
//!
//! [[replicas]]
//! url = "http://192.168.0.4:8082"
//! password = "password"
//! ```

use std::path::Path;

use config::{Config, ConfigError, Environment, File};
use serde::{Deserialize, Serialize};

use crate::api::types::GravityImportOptions;

/// Strongly-typed structural map of all runtime parameters of Ergosphere.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub daemon: DaemonSettings,
    pub primary: NodeSettings,
    pub replicas: Vec<NodeSettings>,
    pub sync: SyncSettings,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonSettings {
    /// Safety sleep duration window to absorb rapid filesystem cascading writes.
    pub debounce_seconds: u64,
    /// Root Pi-hole config directory path holding the target `gravity.db` and `pihole.toml` files.
    pub watch_directory: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeSettings {
    /// An optional label for the node.
    pub label: Option<String>,
    /// URL targeting the Pi-hole v6 API engine (Eg: http://192.168.0.2:8080)
    pub url: String,
    /// The web UI password or application password.
    pub password: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncSettings {
    /// Synchronize everything (i.e. enable all Teleporter import options).
    /// If `true` overrides all other sync options.
    #[serde(default = "default_true")]
    pub full_sync: bool,
    #[serde(default = "default_false")]
    pub config: bool,
    #[serde(default = "default_false")]
    pub dhcp_leases: bool,
    #[serde(default)]
    pub gravity: GravityImportOptions,
}

fn default_true() -> bool {
    true
}

fn default_false() -> bool {
    false
}

impl AppConfig {
    /// Layer and load configuration from disk files and environment overrides.
    pub fn load() -> Result<Self, ConfigError> {
        const PIHOLE_CONFIG_DIR: &str = "/etc/pihole";
        const ERGOSPHERE_CONFIG_FILE: &str = "config.toml";

        let mut builder = Config::builder()
            .set_default("daemon.debounce_seconds", 3)?
            .set_default("daemon.watch_directory", String::from(PIHOLE_CONFIG_DIR))?
            .set_default("sync.full_sync", true)?
            .set_default("sync.config", false)?
            .set_default("sync.dhcp_leases", false)?
            .set_default("sync.gravity.group", true)?
            .set_default("sync.gravity.adlist", true)?
            .set_default("sync.gravity.adlist_by_group", true)?
            .set_default("sync.gravity.domainlist", true)?
            .set_default("sync.gravity.domainlist_by_group", true)?
            .set_default("sync.gravity.client", true)?
            .set_default("sync.gravity.client_by_group", true)?
            .add_source(File::with_name("ergosphere").required(false));

        if let Some(mut config_dir) = dirs::config_dir() {
            config_dir.push("ergosphere");
            config_dir.push(ERGOSPHERE_CONFIG_FILE);

            if config_dir.exists() {
                builder = builder.add_source(File::from(config_dir));
            }
        }

        // Local override (helpful for dev testing)
        if Path::new(ERGOSPHERE_CONFIG_FILE).exists() {
            builder = builder.add_source(File::with_name(ERGOSPHERE_CONFIG_FILE));
        }

        builder = builder.add_source(Environment::with_prefix("ERGOSPHERE").separator("__"));

        let mut loaded_conf: Self = builder.build()?.try_deserialize()?;

        if loaded_conf.sync.full_sync {
            loaded_conf.sync.config = true;
            loaded_conf.sync.dhcp_leases = true;
            loaded_conf.sync.gravity = GravityImportOptions {
                group: true,
                adlist: true,
                adlist_by_group: true,
                domainlist: true,
                domainlist_by_group: true,
                client: true,
                client_by_group: true,
            }
        }

        Ok(loaded_conf)
    }
}
