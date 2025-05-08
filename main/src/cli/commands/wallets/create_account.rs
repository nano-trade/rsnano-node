use crate::cli::{build_node, GlobalArgs};
use anyhow::anyhow;
use clap::Parser;
use rsnano_core::{Account, WalletId};
use rsnano_node::wallets::WalletsExt;

#[derive(Parser)]
pub(crate) struct CreateAccountArgs {
    /// Creates an account in the supplied <wallet>
    #[arg(long)]
    wallet: String,
    /// Optional password to unlock the wallet
    #[arg(long)]
    password: Option<String>,
}

impl CreateAccountArgs {
    pub(crate) fn create_account(&self, global_args: GlobalArgs) -> anyhow::Result<()> {
        let node = build_node(&global_args)?;
        let wallet = WalletId::decode_hex(&self.wallet)?;
        let password = self.password.clone().unwrap_or_default();

        node.wallets.ensure_wallet_is_unlocked(wallet, &password);

        let public_key = node
            .wallets
            .deterministic_insert2(&wallet, false)
            .map_err(|e| anyhow!("Failed to insert wallet: {:?}", e))?;

        println!("Account: {:?}", Account::from(public_key).encode_account());

        Ok(())
    }
}
