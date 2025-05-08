mod generate_config;
mod run_daemon;

use crate::cli::{build_node, GlobalArgs};
use clap::{CommandFactory, Parser, Subcommand};
use generate_config::GenerateConfigArgs;
use rsnano_node::telemetry::{rsnano_build_info, rsnano_version_string};
use run_daemon::RunDaemonArgs;

#[derive(Parser, PartialEq, Debug)]
pub(crate) struct NodeCommand {
    #[command(subcommand)]
    pub subcommand: Option<NodeSubcommands>,
}

#[derive(Subcommand, PartialEq, Debug)]
pub(crate) enum NodeSubcommands {
    /// Start node daemon.
    Run(RunDaemonArgs),
    /// Initialize the data folder, if it is not already initialised.
    ///
    /// This command is meant to be run when the data folder is empty, to populate it with the genesis block.
    Initialize,
    /// Prints out version.
    Version,
    /// Writes node or rpc configuration to stdout, populated with defaults suitable for this system.
    ///
    /// Pass the configuration type node or rpc.
    /// See also use_defaults.
    GenerateConfig(GenerateConfigArgs),
}

impl NodeCommand {
    pub(crate) fn run(&self, global_args: GlobalArgs) -> anyhow::Result<()> {
        match &self.subcommand {
            Some(NodeSubcommands::Run(args)) => args.run_daemon(global_args)?,
            Some(NodeSubcommands::Initialize) => initialize(global_args)?,
            Some(NodeSubcommands::GenerateConfig(args)) => args.generate_config()?,
            Some(NodeSubcommands::Version) => print_version(),
            None => NodeCommand::command().print_long_help()?,
        }

        Ok(())
    }
}

fn print_version() {
    println!("{}", rsnano_version_string());
    println!("{}", rsnano_build_info());
}

fn initialize(global_args: GlobalArgs) -> anyhow::Result<()> {
    build_node(&global_args)?;
    Ok(())
}
