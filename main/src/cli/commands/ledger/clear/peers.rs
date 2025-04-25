use crate::cli::get_path;
use anyhow::Result;
use clap::{ArgGroup, Parser};
use rsnano_store_lmdb::{LmdbEnvFactory, LmdbPeerStore};

#[derive(Parser)]
#[command(group = ArgGroup::new("input")
    .args(&["data_path", "network"]))]
pub(crate) struct PeersArgs {
    /// Uses the supplied path as the data directory
    #[arg(long, group = "input")]
    data_path: Option<String>,
    /// Uses the supplied network (live, test, beta or dev)
    #[arg(long, group = "input")]
    network: Option<String>,
}

impl PeersArgs {
    pub(crate) fn peers(&self) -> Result<()> {
        let path = get_path(&self.data_path, &self.network).join("data.ldb");
        let env = LmdbEnvFactory::default().create_env(&path)?;
        let peer_store = LmdbPeerStore::new(&env)?;
        let mut txn = env.tx_begin_write();

        peer_store.clear(&mut txn);

        println!("Peers were cleared from the database");
        Ok(())
    }
}
