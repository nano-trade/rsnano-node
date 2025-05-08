mod clear;
mod info;

use crate::cli::GlobalArgs;
use anyhow::Context;
use clap::{CommandFactory, Parser, Subcommand};
use clear::ClearCommand;
use info::InfoCommand;
use rsnano_store_lmdb::LmdbEnvFactory;
use std::fs;

#[derive(Subcommand)]
pub(crate) enum LedgerSubcommands {
    /// Commands that get some info from the ledger
    Info(InfoCommand),
    /// Commands that clear some component of the ledger
    Clear(ClearCommand),
    /// Compacts the database
    Vacuum,
    /// Similar to vacuum but does not replace the existing database
    Snapshot,
}

#[derive(Parser)]
pub(crate) struct LedgerCommand {
    #[command(subcommand)]
    pub subcommand: Option<LedgerSubcommands>,
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
        None => LedgerCommand::command().print_long_help()?,
    }

    Ok(())
}

fn vacuum(global_args: GlobalArgs) -> anyhow::Result<()> {
    let data_path = global_args.data_path.clone();
    let source_path = data_path.join("data.ldb");
    let backup_path = data_path.join("backup.vacuum.ldb");
    let vacuum_path = data_path.join("vacuumed.ldb");

    println!("Vacuuming database copy in {:?}", data_path);
    println!("This may take a while...");

    let env = LmdbEnvFactory::default().create_env(&source_path)?;
    env.copy_db(&vacuum_path)?;

    println!("Finalizing");

    fs::rename(&source_path, &backup_path).context("Failed to rename source to backup")?;
    fs::rename(&vacuum_path, &source_path).context("Failed to rename vacuum to source")?;
    fs::remove_file(&backup_path).context("Failed to remove backup file")?;

    println!("Vacuum completed");

    Ok(())
}

fn snapshot(global_args: GlobalArgs) -> anyhow::Result<()> {
    let source_path = global_args.data_path.join("data.ldb");
    let snapshot_path = global_args.data_path.join("snapshot.ldb");

    println!(
        "Database snapshot of {:?} to {:?} in progress",
        source_path, snapshot_path
    );

    println!("This may take a while...");

    let env = LmdbEnvFactory::default().create_env(&source_path)?;
    env.copy_db(&snapshot_path)?;

    println!(
        "Snapshot completed, This can be found at {:?}",
        snapshot_path
    );

    Ok(())
}
