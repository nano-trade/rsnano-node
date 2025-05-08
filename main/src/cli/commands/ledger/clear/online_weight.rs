use crate::cli::get_path;
use anyhow::Result;
use clap::{ArgGroup, Parser};
use rsnano_store_lmdb::{LmdbEnvFactory, LmdbOnlineWeightStore};

#[derive(Parser)]
#[command(group = ArgGroup::new("input")
    .args(&["data_path", "network"]))]
pub(crate) struct OnlineWeightArgs {
    /// Uses the supplied path as the data directory
    #[arg(long, group = "input")]
    data_path: Option<String>,
    /// Uses the supplied network (live, test, beta or dev)
    #[arg(long, group = "input")]
    network: Option<String>,
}

impl OnlineWeightArgs {
}
