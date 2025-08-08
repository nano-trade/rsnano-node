mod clear;
mod info;
mod roll_back;

use clap::{CommandFactory, Parser, Subcommand};

use rsnano_nullable_lmdb::LmdbEnvironmentFactory;

use crate::cli::GlobalArgs;
use clear::ClearCommand;
use info::InfoCommand;
use roll_back::roll_back;
use rsnano_store_lmdb::default_ledger_lmdb_options;

#[derive(Parser, PartialEq, Debug)]
pub(crate) struct LedgerCommand {
    #[command(subcommand)]
    pub subcommand: Option<LedgerSubcommands>,
}

#[derive(Subcommand, PartialEq, Debug)]
pub(crate) enum LedgerSubcommands {
    /// Commands that get some info from the ledger
    Info(InfoCommand),
    /// Commands that clear some component of the ledger
    Clear(ClearCommand),
    /// Compacts the database
    Vacuum,
    /// Similar to vacuum but does not replace the existing database
    Snapshot,
    /// Roll back an unconfirmed block
    RollBack(HashArgs),
}

#[derive(Parser, PartialEq, Debug)]
pub(crate) struct HashArgs {
    /// Clears the confirmation height of the account
    #[arg(long)]
    hash: String,
}

pub(crate) fn run_ledger_command(
    global_args: GlobalArgs,
    cmd: LedgerCommand,
) -> anyhow::Result<()> {
    match cmd.subcommand {
        Some(LedgerSubcommands::Info(command)) => command.run(global_args)?,
        Some(LedgerSubcommands::Clear(command)) => command.run(global_args)?,
        Some(LedgerSubcommands::Vacuum) => vacuum(global_args)?,
        Some(LedgerSubcommands::Snapshot) => snapshot(global_args)?,
        Some(LedgerSubcommands::RollBack(args)) => roll_back(global_args, args)?,
        None => LedgerCommand::command().print_long_help()?,
    }

    Ok(())
}

fn vacuum(global_args: GlobalArgs) -> anyhow::Result<()> {
    let ledger_path = global_args.data_path.join("data.ldb");
    let options = default_ledger_lmdb_options(ledger_path);
    let env = LmdbEnvironmentFactory::default().create(options)?;
    rsnano_store_lmdb::vacuum(env)
}

fn snapshot(global_args: GlobalArgs) -> anyhow::Result<()> {
    let source_path = global_args.data_path.join("data.ldb");
    let snapshot_path = global_args.data_path.join("snapshot.ldb");

    println!(
        "Database snapshot of {:?} to {:?} in progress",
        source_path, snapshot_path
    );

    println!("This may take a while...");

    let options = default_ledger_lmdb_options(source_path);
    let env = LmdbEnvironmentFactory::default().create(options)?;
    env.copy_db(&snapshot_path)?;

    println!(
        "Snapshot completed, This can be found at {:?}",
        snapshot_path
    );

    Ok(())
}
