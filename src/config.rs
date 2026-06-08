//! Application configuration orchestration and layering system.

#[cfg(debug_assertions)]
use std::path::Path;
use std::path::PathBuf;

use config::{Config, ConfigError, Environment, File};
use serde::{Deserialize, Serialize};
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

        builder = builder.add_source(Environment::with_prefix("ERGOSPHERE").separator("__"));

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
