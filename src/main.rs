use std::fmt::Display;
use std::io::{self, IsTerminal};
use std::process::ExitCode;

use clap::Parser;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::mpsc;
use tracing::{error, info};
use tracing_core::Field;
use tracing_error::ErrorLayer;
use tracing_subscriber::field::{RecordFields, Visit};
use tracing_subscriber::fmt::format;
use tracing_subscriber::fmt::format::{FormatFields, Writer};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{fmt, EnvFilter};

use crate::args::{CliArgs, Commands};
use crate::config::AppConfig;
use crate::consts::ERGOSPHERE_VERSION;
use crate::daemon::Daemon;
use crate::watcher::Watcher;

mod api;
mod args;
mod config;
pub(crate) mod consts;
mod daemon;
mod watcher;

#[derive(Debug)]
enum RunMode {
    Once,
    Daemon,
}

/// A custom field formatter that colors field variable keys distinct from the message text.
struct ColoredFieldsFormatter;

struct ColoredFieldsVisitor<'a, 'writer> {
    writer: &'a mut Writer<'writer>,
    result: std::fmt::Result,
    is_first: bool,
    use_color: bool,
}

impl Display for RunMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Once => write!(f, "once"),
            Self::Daemon => write!(f, "daemon"),
        }
    }
}

impl<'writer> FormatFields<'writer> for ColoredFieldsFormatter {
    fn format_fields<R>(&self, mut writer: Writer<'writer>, fields: R) -> std::fmt::Result
    where
        R: RecordFields,
    {
        let use_color = writer.has_ansi_escapes();
        let mut visitor = ColoredFieldsVisitor {
            writer: &mut writer,
            result: Ok(()),
            is_first: true,
            use_color,
        };
        fields.record(&mut visitor);
        visitor.result
    }
}

impl<'a, 'writer> Visit for ColoredFieldsVisitor<'a, 'writer> {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if self.result.is_err() {
            return;
        }

        if field.name() == "message" {
            self.result = write!(self.writer, "{:?}", value);
            return;
        }

        let prefix = if self.is_first { " " } else { " " };
        self.is_first = false;

        if self.use_color {
            let key_color = "\x1b[36m";
            let reset = "\x1b[0m";
            self.result = write!(
                self.writer,
                "{}{}{}{}={:?}",
                prefix,
                key_color,
                field.name(),
                reset,
                value
            );
        } else {
            self.result = write!(self.writer, "{}{}={:?}", prefix, field.name(), value);
        }
    }
}

/// Evaluates arguments, shell environments, and TTY states to deduce
/// if color codes should be generated.
fn determine_color_usage(cli_color_opt: Option<bool>) -> bool {
    if let Some(explicit_choice) = cli_color_opt {
        return explicit_choice;
    }

    if std::env::var("NO_COLOR").is_ok() {
        return false;
    }

    if std::env::var("TERM").map(|v| v == "dumb").unwrap_or(false) {
        return false;
    }

    io::stderr().is_terminal()
}

/// Core application execution runner. Propagates all errors back to the main entrypoint.
async fn run(args: CliArgs) -> anyhow::Result<()> {
    let config = AppConfig::load()?;

    match args.command {
        Commands::Sync { daemon, force_sync } => {
            let mode = if daemon {
                RunMode::Daemon
            } else {
                RunMode::Once
            };

            info!(mode = %mode, version = %ERGOSPHERE_VERSION, "Starting ergosphere");

            if daemon {
                let (tx, rx) = mpsc::channel(32);
                let _watcher = Watcher::new(
                    &config.daemon.watch_directory,
                    config.daemon.debounce_seconds,
                    tx,
                )?;
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
                let (_, rx) = mpsc::channel(1);
                let mut daemon_engine = Daemon::new(config, rx)?;

                if let Err(e) = daemon_engine.run_once().await {
                    daemon_engine.shutdown().await;
                    return Err(e);
                }

                info!("Synchronization successful. Shutting down...");
                daemon_engine.shutdown().await;
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> ExitCode {
    let args_res = CliArgs::try_parse();
    let use_color = match &args_res {
        Ok(parsed_args) => determine_color_usage(parsed_args.color),
        Err(_) => determine_color_usage(None),
    };

    #[allow(unused_assignments, unused_mut)]
    let mut log_level = EnvFilter::new("info");
    #[cfg(debug_assertions)]
    {
        log_level = EnvFilter::new("debug");
    }
    tracing_subscriber::registry()
        .with(
            fmt::layer()
                .with_writer(io::stderr)
                .with_ansi(use_color)
                .event_format(format().with_target(true))
                .fmt_fields(ColoredFieldsFormatter),
        )
        .with(ErrorLayer::default())
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| log_level))
        .init();

    let args = match args_res {
        Ok(valid_args) => valid_args,
        Err(clap_error) => clap_error.exit(),
    };

    if let Err(err) = run(args).await {
        error!(
            error = ?err,
            "Fatal",
        );
        return ExitCode::FAILURE;
    }

    ExitCode::SUCCESS
}
