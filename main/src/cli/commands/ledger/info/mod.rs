use crate::cli::GlobalArgs;
use clap::{CommandFactory, Parser, Subcommand};
use rsnano_store_lmdb::{LmdbEnvFactory, LmdbPeerStore};

#[derive(Subcommand, PartialEq, Debug)]
pub(crate) enum InfoSubcommands {
    /// Displays peer IPv6:port connections
    Peers,
}

#[derive(Parser, PartialEq, Debug)]
pub(crate) struct InfoCommand {
    #[command(subcommand)]
    pub subcommand: Option<InfoSubcommands>,
}

impl InfoCommand {
    pub(crate) fn run(&self, global_args: GlobalArgs) -> anyhow::Result<()> {
        match &self.subcommand {
            Some(InfoSubcommands::Peers) => self.peers(global_args)?,
            None => InfoCommand::command().print_long_help()?,
        }

        Ok(())
    }

    fn peers(&self, global_args: GlobalArgs) -> anyhow::Result<()> {
        let path = global_args.data_path.join("data.ldb");
        let env = LmdbEnvFactory::default().create_env(&path)?;
        let peer_store = LmdbPeerStore::new(&env)?;
        let txn = env.tx_begin_read();

        for peer in peer_store.iter(&txn) {
            println!("{:?}", peer.0);
        }

        Ok(())
    }
}
