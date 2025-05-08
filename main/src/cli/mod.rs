use anyhow::{anyhow, Context};
use clap::{CommandFactory, Parser, Subcommand};
use commands::{
    config::ConfigCommand, ledger::LedgerCommand, node::NodeCommand, utils::UtilsCommand,
    wallets::WalletsCommand,
};
use rsnano_core::{Networks, PrivateKeyFactory};
use rsnano_node::{working_path_for, Node, NodeBuilder};
use rsnano_nullable_console::Console;
use std::{path::PathBuf, str::FromStr};

mod commands;

#[derive(Parser)]
pub(crate) struct Cli {
    /// Uses the supplied network (live, test, beta or dev)
    #[arg(long)]
    network: Option<String>,

    /// Uses the supplied path as the data directory
    #[arg(long)]
    data_path: Option<String>,

    #[command(subcommand)]
    pub command: Option<Commands>,
}

impl Cli {
    pub(crate) fn run(&self, infra: &mut CliInfrastructure) -> anyhow::Result<()> {
        let global_args = self.get_global_args()?;

        match &self.command {
            Some(Commands::Wallets(command)) => command.run(global_args)?,
            Some(Commands::Utils(command)) => command.run(infra)?,
            Some(Commands::Node(command)) => command.run(global_args)?,
            Some(Commands::Ledger(command)) => command.run(global_args)?,
            Some(Commands::Config(command)) => command.run(global_args)?,
            None => Cli::command().print_long_help()?,
        }
        Ok(())
    }

    fn get_global_args(&self) -> anyhow::Result<GlobalArgs> {
        let network = self.get_network()?;
        let data_path = self.get_data_path()?;
        Ok(GlobalArgs { network, data_path })
    }

    fn get_network(&self) -> anyhow::Result<Networks> {
        self.network
            .as_ref()
            .map(|str| Networks::from_str(str).map_err(|e| anyhow!(e)))
            .transpose()
            .map(|net| net.unwrap_or(Networks::NanoLiveNetwork))
    }

    fn get_data_path(&self) -> anyhow::Result<PathBuf> {
        if let Some(path) = &self.data_path {
            return PathBuf::from_str(path).context("Not a valid data path");
        }
        working_path_for(self.get_network()?).ok_or_else(|| anyhow!("No data path found"))
    }
}

pub(crate) struct GlobalArgs {
    pub network: Networks,
    pub data_path: PathBuf,
}

#[derive(Subcommand)]
pub(crate) enum Commands {
    /// Commands related to configs
    Config(ConfigCommand),
    /// Commands related to the ledger
    Ledger(LedgerCommand),
    /// Commands related to running the node
    Node(NodeCommand),
    /// Utils related to keys and accounts
    Utils(UtilsCommand),
    /// Commands to manage wallets
    Wallets(WalletsCommand),
}

pub(crate) fn build_node(args: &GlobalArgs) -> anyhow::Result<Node> {
    NodeBuilder::new(args.network)
        .data_path(&args.data_path)
        .finish()
}

#[derive(Default)]
pub(crate) struct CliInfrastructure {
    pub key_factory: PrivateKeyFactory,
    pub console: Console,
}

impl CliInfrastructure {
    #[allow(dead_code)]
    pub fn new_null() -> Self {
        Self {
            key_factory: PrivateKeyFactory::new_null(),
            console: Console::new_null(),
        }
    }
}
