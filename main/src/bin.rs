mod cli;

use anyhow::Result;
use clap::Parser;
use cli::{Cli, CliInfrastructure, CommandLineArgs};

fn main() -> Result<()> {
    let args = CommandLineArgs::parse();
    let mut infra = CliInfrastructure::default();
    Cli {}.run(&mut infra, args)?;
    Ok(())
}
