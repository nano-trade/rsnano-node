use crate::cli::GlobalArgs;
use clap::Parser;
use rsnano_daemon::DaemonBuilder;
use rsnano_node::config::NodeFlags;
use tracing_subscriber::EnvFilter;

#[derive(Parser, PartialEq, Debug)]
pub(crate) struct RunDaemonArgs {
    /// Turn off automatic wallet backup process
    #[arg(long)]
    disable_backup: bool,
    /// Turn off the ability for ongoing bootstraps to occur
    #[arg(long)]
    disable_ongoing_bootstrap: bool,
    /// Turn off the request loop
    #[arg(long)]
    disable_request_loop: bool,
    /// Turn off the rep crawler process
    #[arg(long)]
    disable_rep_crawler: bool,
    /// Do not provide any telemetry data to nodes requesting it. Responses are still made to requests, but they will have an empty payload.
    #[arg(long)]
    disable_providing_telemetry_metrics: bool,
    /// Disable deletion of unchecked blocks after processing.
    #[arg(long)]
    disable_block_processor_unchecked_deletion: bool,
    /// Disables block republishing by disabling the local_block_broadcaster component
    #[arg(long)]
    disable_block_processor_republishing: bool,
    /// Allow multiple connections to the same peer in bootstrap attempts
    #[arg(long)]
    allow_bootstrap_peers_duplicates: bool,
    /// Enable experimental ledger pruning
    #[arg(long)]
    enable_pruning: bool,
    /// Enable voting
    #[arg(long)]
    enable_voting: bool,
    /// Increase bootstrap processor limits to allow more blocks before hitting full state and verify/write more per database call. Also disable deletion of processed unchecked blocks.
    #[arg(long)]
    fast_bootstrap: bool,
    /// Increase batch signature verification size in block processor, default 0 (limited by config signature_checker_threads), unlimited for fast_bootstrap
    #[arg(long)]
    block_processor_verification_size: Option<usize>,
}

impl RunDaemonArgs {
    pub(crate) fn run_daemon(&self, global_args: GlobalArgs) -> anyhow::Result<()> {
        init_tracing();
        let network = global_args.network;
        let flags = self.get_flags();
        DaemonBuilder::new(network)
            .flags(flags)
            .data_path(&global_args.data_path)
            .run(shutdown_signal())
    }

    pub(crate) fn get_flags(&self) -> NodeFlags {
        let mut flags = NodeFlags::new();
        flags.disable_backup = self.disable_backup;
        flags.disable_ongoing_bootstrap = self.disable_ongoing_bootstrap;
        flags.disable_rep_crawler = self.disable_rep_crawler;
        flags.disable_request_loop = self.disable_request_loop;
        flags.disable_providing_telemetry_metrics = self.disable_providing_telemetry_metrics;
        flags.disable_block_processor_unchecked_deletion =
            self.disable_block_processor_unchecked_deletion;
        flags.disable_block_processor_republishing = self.disable_block_processor_republishing;
        flags.allow_bootstrap_peers_duplicates = self.allow_bootstrap_peers_duplicates;
        flags.enable_pruning = self.enable_pruning;
        flags.enable_voting = self.enable_voting;
        flags.fast_bootstrap = self.fast_bootstrap;
        flags
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

fn init_tracing() {
    let dirs = std::env::var(EnvFilter::DEFAULT_ENV).unwrap_or(String::from("info"));
    let filter = EnvFilter::builder().parse_lossy(dirs);
    let value = std::env::var("NANO_LOG");
    let log_style = value.as_ref().map(|i| i.as_str()).unwrap_or_default();
    match log_style {
        "json" => {
            tracing_subscriber::fmt::fmt()
                .json()
                .with_env_filter(filter)
                .init();
        }
        "noansi" => {
            tracing_subscriber::fmt::fmt()
                .with_env_filter(filter)
                .with_ansi(false)
                .init();
        }
        _ => {
            tracing_subscriber::fmt::fmt()
                .with_env_filter(filter)
                .with_ansi(true)
                .init();
        }
    }
    tracing::debug!(log_style, ?value, "init tracing");
}
