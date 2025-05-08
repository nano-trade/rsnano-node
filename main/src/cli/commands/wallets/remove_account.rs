use crate::cli::{build_node, GlobalArgs};
use anyhow::{anyhow, Result};
use clap::Parser;
use rsnano_core::{Account, WalletId};
use rsnano_node::wallets::WalletsExt;

#[derive(Parser, PartialEq, Debug)]
pub(crate) struct RemoveAccountArgs {
    /// Removes the account from the supplied wallet
    #[arg(long)]
    wallet: String,
    /// Removes the account from the supplied wallet
    #[arg(long)]
    account: String,
    /// Optional password to unlock the wallet
    #[arg(long)]
    password: Option<String>,
}

impl RemoveAccountArgs {
    pub(crate) fn remove_account(&self, global_args: GlobalArgs) -> Result<()> {
        let node = build_node(&global_args)?;
        let wallet_id = WalletId::decode_hex(&self.wallet)?;
        let password = self.password.clone().unwrap_or_default();
        let account = Account::decode_account(&self.account)?.into();

        node.wallets.ensure_wallet_is_unlocked(wallet_id, &password);

        node.wallets
            .remove_key(&wallet_id, &account)
            .map_err(|e| anyhow!("Failed to remove account: {:?}", e))?;

        Ok(())
    }
}
