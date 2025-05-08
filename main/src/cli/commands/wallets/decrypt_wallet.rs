use crate::cli::{build_node, GlobalArgs};
use anyhow::anyhow;
use clap::Parser;
use rsnano_core::WalletId;
use rsnano_node::wallets::WalletsExt;

#[derive(Parser, PartialEq, Debug)]
pub(crate) struct DecryptWalletArgs {
    /// The wallet to be decrypted
    #[arg(long)]
    wallet: String,
    /// Optional password to unlock the wallet
    #[arg(long)]
    password: Option<String>,
}

impl DecryptWalletArgs {
    pub(crate) fn decrypt_wallet(&self, global_args: GlobalArgs) -> anyhow::Result<()> {
        let node = build_node(&global_args)?;
        let wallet_id = WalletId::decode_hex(&self.wallet)?;
        let password = self.password.clone().unwrap_or_default();

        node.wallets.ensure_wallet_is_unlocked(wallet_id, &password);

        let seed = node
            .wallets
            .get_seed(wallet_id)
            .map_err(|e| anyhow!("Failed to get wallet seed: {:?}", e))?;

        println!("Seed: {:?}", seed);
        Ok(())
    }
}
