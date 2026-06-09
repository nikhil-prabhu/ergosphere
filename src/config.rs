//! Application configuration orchestration and layering system.
//!
//! # Example Configuration
//!
//! ```toml
//! [daemon]
//! client_timeout_seconds = 20
//! client_skip_tls_verification = false
//! debounce_seconds = 3
//! watch_directory = "/etc/pihole"
//! timezone = "Asia/Kolkata"
//!
//! [primary]
//! label = "pihole-primary"
//! url = "http://192.168.0.2"
//! password = "password"
//!
//! [[replicas]]
//! label = "pihole-replica1"
//! url = "http://192.168.0.3"
//! password = "password"
//!
//! [[replicas]]
//! label = "pihole-replica2"
//! url = "http://192.168.0.4"
//! password = "password"
//!
//! [sync]
//! run_gravity = true
//! full_sync = false
//! config = false
//! dhcp_leases = false
//!
//! [sync.gravity]
//! group = true
//! adlist = true
//! adlist_by_group = true
//! domainlist = true
//! client = true
//! client_by_group = true
//! ```

use std::collections::BTreeMap;
#[cfg(debug_assertions)]
use std::path::Path;
use std::path::PathBuf;

use config::{Config, ConfigError, Environment, File};
use serde::de::{DeserializeOwned, Error};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::from_str;
use smart_default::SmartDefault;
use tracing::debug;

use crate::api::types::{GravityImportOptions, TeleporterImportOptions};
use crate::consts::{
    ERGOSPHERE_CONFIG_FILE,
    ERGOSPHERE_DAEMON_CLIENT_TIMEOUT_SECONDS,
    ERGOSPHERE_DAEMON_DEBOUNCE_SECONDS,
    PIHOLE_CONFIG_DIR,
};

/// Strongly-typed structural map of all runtime parameters of Ergosphere.
#[derive(Debug, Clone, Serialize, Deserialize, SmartDefault)]
pub struct AppConfig {
    pub daemon: DaemonSettings,
    pub primary: NodeSettings,
    #[default(_code = "vec![Default::default()]")]
    #[serde(deserialize_with = "de_replicas")]
    pub replicas: Vec<NodeSettings>,
    pub sync: SyncSettings,
}

#[derive(SmartDefault, Debug, Clone, Serialize, Deserialize)]
pub struct DaemonSettings {
    /// The timeout (in seconds) for the HTTP client.
    #[default(ERGOSPHERE_DAEMON_CLIENT_TIMEOUT_SECONDS)]
    pub client_timeout_seconds: u64,
    /// Whether the HTTP client should skip TLS verification.
    #[default = false]
    pub client_skip_tls_verification: bool,
    /// Safety sleep duration window to absorb rapid filesystem cascading writes.
    #[default(ERGOSPHERE_DAEMON_DEBOUNCE_SECONDS)]
    pub debounce_seconds: u64,
    /// Root Pi-hole config directory path holding the target `gravity.db` and `pihole.toml` files.
    #[default(_code = "PathBuf::from(PIHOLE_CONFIG_DIR)")]
    pub watch_directory: PathBuf,
    /// Explicit IANA timezone configuration override for logging (e.g., "Asia/Kolkata")
    #[default(None)]
    pub timezone: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, SmartDefault)]
pub struct NodeSettings {
    /// An optional label for the node.
    #[default(None)]
    pub label: Option<String>,
    /// URL targeting the Pi-hole v6 API engine (Eg: http://192.168.0.2:8080)
    #[default = ""]
    pub url: String,
    /// The web UI password or application password.
    #[default = ""]
    pub password: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, SmartDefault)]
pub struct SyncSettings {
    /// Whether to run the gravity update action on the replica node after synchronization.
    #[default = false]
    pub run_gravity: bool,
    /// Synchronize everything (i.e. enable all Teleporter import options).
    /// If `true` overrides all other sync options.
    #[default = true]
    pub full_sync: bool,
    #[default = true]
    pub config: bool,
    #[default = true]
    pub dhcp_leases: bool,
    pub gravity: GravityImportOptions,
}

/// Custom deserializer for the `[[replicas]]` section of our config.
///
/// This is required as a manual workaround because the `config` crate parses environment variable
/// array index overrides (e.g., `REPLICAS__0__URL`) as discrete string-keyed maps rather than
/// sequential vectors, causing a type mismatch error during standard deserialization.
///
/// Credit for this workaround: [config-rs/issues/658#issuecomment-3807400516](https://github.com/rust-cli/config-rs/issues/658#issuecomment-3807400516)
fn de_replicas<'de, T, D>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    T: DeserializeOwned,
    D: Deserializer<'de>,
{
    /// Represents the possible config or environment variable value styles for the replicas config.
    ///
    /// # Normal config file (Eg: TOML)
    ///
    /// ```toml
    /// [[replicas]]
    /// label = "label"
    /// url = "url"
    /// password = "password"
    /// ```
    ///
    /// # Index style
    ///
    /// ```bash
    /// ERGOSPHERE_REPLICAS__0__LABEL=label
    /// ERGOSPHERE_REPLICAS__0__URL=url
    /// ERGOSPHERE_REPLICAS__0__PASSWORD=password
    /// ```
    ///
    /// # JSON string
    ///
    /// ```bash
    /// ERGOSPHERE_REPLICAS='[{"label": "label", "url": "url", "password": "password"}]'
    /// ```
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum DeReplicas<T> {
        Map(BTreeMap<String, T>),
        Vec(Vec<T>),
        Json(String),
    }

    match DeReplicas::<T>::deserialize(deserializer)? {
        DeReplicas::Map(m) => Ok(m.into_values().collect()),
        DeReplicas::Vec(v) => Ok(v),
        DeReplicas::Json(s) => from_str(&s).map_err(Error::custom),
    }
}

impl AppConfig {
    /// Layer and load configuration from disk files and environment overrides.
    /// If no configuration layout exists anywhere, drops a fresh blueprint file
    /// on disk and exits safely to prevent accidental unconfigured pipeline runs.
    pub fn load() -> Result<Self, ConfigError> {
        #[allow(unused_mut)]
        let mut local_dev_config_exists = false;
        #[cfg(debug_assertions)]
        {
            if Path::new(ERGOSPHERE_CONFIG_FILE).exists() {
                local_dev_config_exists = true;
            }
        }

        if let Some(mut config_dir) = dirs::config_dir() {
            config_dir.push("ergosphere");

            if !config_dir.exists() {
                let _ = std::fs::create_dir_all(&config_dir);
            }

            config_dir.push(ERGOSPHERE_CONFIG_FILE);

            // Trigger generation and immediate exit only if no configuration source exists anywhere.
            if !config_dir.exists() && !local_dev_config_exists {
                let default_template = AppConfig::default();

                if let Ok(toml_string) = toml::to_string_pretty(&default_template) {
                    let header_comments = "# Ergosphere Configuration Profile\n# Customize \
                                           settings below to connect your primary and replica \
                                           nodes.\n\n";
                    let finalized_file_content = format!("{header_comments}{toml_string}");

                    if std::fs::write(&config_dir, finalized_file_content).is_ok() {
                        println!(
                            "Welcome to ergosphere!\n\n[+] Initialized new configuration \
                             blueprint at: {}\n\nPlease modify this file with your actual \
                             primary/replica Pi-hole parameters, \npasswords, and sync \
                             constraints before restarting the daemon.\n",
                            config_dir.display()
                        );
                        std::process::exit(0);
                    }
                }
            }
        }

        let mut builder = Config::builder()
            .set_default(
                "daemon.client_timeout_seconds",
                ERGOSPHERE_DAEMON_CLIENT_TIMEOUT_SECONDS,
            )?
            .set_default("daemon.client_skip_tls_verification", false)?
            .set_default(
                "daemon.debounce_seconds",
                ERGOSPHERE_DAEMON_DEBOUNCE_SECONDS,
            )?
            .set_default("daemon.watch_directory", String::from(PIHOLE_CONFIG_DIR))?
            .set_default("sync.run_gravity", false)?
            .set_default("sync.full_sync", true)?
            .set_default("sync.config", true)?
            .set_default("sync.dhcp_leases", true)?
            .set_default("sync.gravity.group", true)?
            .set_default("sync.gravity.adlist", true)?
            .set_default("sync.gravity.adlist_by_group", true)?
            .set_default("sync.gravity.domainlist", true)?
            .set_default("sync.gravity.domainlist_by_group", true)?
            .set_default("sync.gravity.client", true)?
            .set_default("sync.gravity.client_by_group", true)?;

        if let Some(mut config_dir) = dirs::config_dir() {
            config_dir.push("ergosphere");
            config_dir.push(ERGOSPHERE_CONFIG_FILE);

            if config_dir.exists() {
                builder = builder.add_source(File::from(config_dir));
            }
        }

        // Local override in dev profile (helpful for dev testing)
        #[cfg(debug_assertions)]
        if local_dev_config_exists {
            builder = builder.add_source(File::with_name(ERGOSPHERE_CONFIG_FILE));
        }

        builder = builder.add_source(
            Environment::with_prefix("ERGOSPHERE")
                .prefix_separator("_")
                .separator("__"),
        );

        let mut loaded_conf: Self = builder.build()?.try_deserialize()?;

        if loaded_conf.sync.full_sync {
            loaded_conf.sync.config = true;
            loaded_conf.sync.dhcp_leases = true;
            loaded_conf.sync.gravity = GravityImportOptions::default();
        }

        debug!(config = ?loaded_conf, "Loaded configuration");
        Ok(loaded_conf)
    }

    /// Helper utility to retrieve the Teleporter import options from the config file.
    pub fn get_teleporter_import_options(&self) -> TeleporterImportOptions {
        TeleporterImportOptions {
            config: self.sync.config,
            dhcp_leases: self.sync.dhcp_leases,
            gravity: self.sync.gravity.clone(),
        }
    }
}
