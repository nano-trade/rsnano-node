use crate::cli::get_path;
use anyhow::Result;
use clap::Parser;
use rsnano_store_lmdb::LmdbEnvFactory;

#[derive(Parser)]
pub(crate) struct SnapshotArgs {
    /// Uses the supplied path as the data directory
    #[arg(long, group = "input")]
    data_path: Option<String>,
    /// Uses the supplied network (live, test, beta or dev)
    #[arg(long, group = "input")]
    network: Option<String>,
}

impl SnapshotArgs {
}
