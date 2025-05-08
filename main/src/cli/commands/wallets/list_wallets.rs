use crate::cli::{build_node2, GlobalArgs};
use anyhow::{anyhow, Result};
use clap::{ArgGroup, Parser};
use rsnano_core::Account;

#[derive(Parser)]
#[command(group = ArgGroup::new("input")
    .args(&["data_path"]))]
pub(crate) struct ListWalletsArgs {
    /// Uses the supplied path as the data directory
    #[arg(long, group = "input")]
    data_path: Option<String>,
}

impl ListWalletsArgs {
}
