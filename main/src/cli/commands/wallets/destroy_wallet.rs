use crate::cli::{build_node, GlobalArgs};
use clap::Parser;
use rsnano_core::WalletId;
use rsnano_node::wallets::WalletsExt;

#[derive(Parser)]
pub(crate) struct DestroyWalletArgs {
    /// The wallet to be destroyed
    #[arg(long)]
    wallet: String,
    /// Optional password to unlock the wallet
    #[arg(long)]
    password: Option<String>,
}

impl DestroyWalletArgs {
    pub(crate) fn destroy_wallet(&self, global_args: GlobalArgs) -> anyhow::Result<()> {
        let node = build_node(&global_args)?;
        let wallet_id = WalletId::decode_hex(&self.wallet)?;
        let password = self.password.clone().unwrap_or_default();
        node.wallets.ensure_wallet_is_unlocked(wallet_id, &password);
        node.wallets.destroy(&wallet_id);
        Ok(())
    }
}
