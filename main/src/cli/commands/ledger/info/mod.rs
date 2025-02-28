use anyhow::Result;
use clap::{CommandFactory, Parser, Subcommand};
use peers::PeersArgs;

pub(crate) mod peers;

#[derive(Subcommand)]
pub(crate) enum InfoSubcommands {
    /// Displays peer IPv6:port connections
    Peers(PeersArgs),
}

#[derive(Parser)]
pub(crate) struct InfoCommand {
    #[command(subcommand)]
    pub subcommand: Option<InfoSubcommands>,
}

impl InfoCommand {
    pub(crate) fn run(&self) -> Result<()> {
        match &self.subcommand {
            Some(InfoSubcommands::Peers(args)) => args.peers()?,
            None => InfoCommand::command().print_long_help()?,
        }

        Ok(())
    }
}
