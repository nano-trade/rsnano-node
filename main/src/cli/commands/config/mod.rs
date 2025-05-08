mod current;
mod default;

use crate::cli::GlobalArgs;
use clap::{CommandFactory, Parser, Subcommand};
use current::CurrentArgs;
use default::DefaultArgs;

#[derive(Parser, PartialEq, Debug)]
pub(crate) struct ConfigCommand {
    #[command(subcommand)]
    pub subcommand: Option<ConfigSubcommands>,
}

#[derive(Subcommand, PartialEq, Debug)]
pub(crate) enum ConfigSubcommands {
    /// Prints the default configs.
    Default(DefaultArgs),
    /// Prints the current configs
    Current(CurrentArgs),
}

impl ConfigCommand {
    pub(crate) fn run(&self, global_args: GlobalArgs) -> anyhow::Result<()> {
        match &self.subcommand {
            Some(ConfigSubcommands::Default(args)) => args.default()?,
            Some(ConfigSubcommands::Current(args)) => args.current(global_args)?,
            None => ConfigCommand::command().print_long_help()?,
        }

        Ok(())
    }
}
