use crate::cli::{build_node, GlobalArgs};
use anyhow::anyhow;
use clap::Parser;
use rsnano_core::{Account, WalletId};
use rsnano_node::wallets::WalletsExt;

#[derive(Parser, PartialEq, Debug)]
pub(crate) struct SetWalletRepresentativeArgs {
    /// Sets the representative for the supplied <wallet>
    #[arg(long)]
    wallet: String,
    /// Sets the supplied account as the wallet representative
    #[arg(long)]
    account: String,
    /// Optional password to unlock the wallet
    #[arg(long)]
    password: Option<String>,
}

impl SetWalletRepresentativeArgs {
    pub(crate) fn set_representative_wallet(&self, global_args: GlobalArgs) -> anyhow::Result<()> {
        let node = build_node(&global_args)?;
        let wallet_id = WalletId::decode_hex(&self.wallet)?;
        let representative = Account::decode_account(&self.account)?.into();
        let password = self.password.clone().unwrap_or_default();

        node.wallets.ensure_wallet_is_unlocked(wallet_id, &password);

        node.wallets
            .set_representative(wallet_id, representative, false)
            .map_err(|e| anyhow!("Failed to set wallet representative: {:?}", e))?;

        Ok(())
    }
}
