use crate::cli::{build_node, GlobalArgs};
use anyhow::anyhow;
use clap::Parser;
use rand::Rng;
use rsnano_core::{RawKey, WalletId};
use rsnano_node::wallets::WalletsExt;

#[derive(Parser)]
pub(crate) struct CreateWalletArgs {
    /// Optional seed of the new wallet
    #[arg(long)]
    seed: Option<String>,
    /// Optional password of the new wallet
    #[arg(long)]
    password: Option<String>,
}

impl CreateWalletArgs {
    pub(crate) fn create_wallet(&self, global_args: GlobalArgs) -> anyhow::Result<()> {
        let node = build_node(&global_args)?;
        let wallet_id = WalletId::from_bytes(rand::rng().random());

        node.wallets.create(wallet_id);
        println!("{:?}", wallet_id);

        let password = self.password.clone().unwrap_or_default();

        node.wallets
            .rekey(&wallet_id, &password)
            .map_err(|e| anyhow!("Failed to set wallet password: {:?}", e))?;

        node.wallets.ensure_wallet_is_unlocked(wallet_id, &password);

        if let Some(seed) = &self.seed {
            let key = RawKey::decode_hex(seed)?;

            node.wallets
                .change_seed(wallet_id, &key, 0)
                .map_err(|e| anyhow!("Failed to set wallet seed: {:?}", e))?;
        }

        Ok(())
    }
}
