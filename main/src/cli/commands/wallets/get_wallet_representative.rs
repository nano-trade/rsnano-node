use crate::cli::{build_node, GlobalArgs};
use anyhow::anyhow;
use clap::Parser;
use rsnano_core::{Account, WalletId};
use rsnano_node::wallets::WalletsExt;

#[derive(Parser, PartialEq, Debug)]
pub(crate) struct GetWalletRepresentativeArgs {
    /// Gets the representative of the supplied <wallet>
    #[arg(long)]
    wallet: String,
    /// Optional password to unlock the wallet
    #[arg(long)]
    password: Option<String>,
}

impl GetWalletRepresentativeArgs {
    pub(crate) fn get_wallet_representative(&self, global_args: GlobalArgs) -> anyhow::Result<()> {
        let node = build_node(&global_args)?;
        let wallet_id = WalletId::decode_hex(&self.wallet)?;
        let password = self.password.clone().unwrap_or_default();

        node.wallets.ensure_wallet_is_unlocked(wallet_id, &password);

        let representative = node
            .wallets
            .get_representative(wallet_id)
            .map_err(|e| anyhow!("Failed to get wallet representative: {:?}", e))?;

        println!(
            "Representative: {:?}",
            Account::from(representative).encode_account()
        );

        Ok(())
    }
}
