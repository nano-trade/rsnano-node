use std::time::Duration;

use anyhow::anyhow;
use tokio::{sync::mpsc::Sender, time::sleep};
use tracing::info;

use rsnano_core::{Amount, Block, PrivateKey, RawKey, WalletId};
use rsnano_rpc_client::NanoRpcClient;
use rsnano_rpc_messages::SendArgs;

/// Periodically publishes a high priority block and tracks confirmation time
pub(crate) struct HighPrioCheck {
    tx_block: Sender<Block>,
}

impl HighPrioCheck {
    pub(crate) fn new(tx_block: Sender<Block>) -> Self {
        Self { tx_block }
    }

    pub(crate) async fn create_prio_accounts(
        &self,
        rpc_client: &NanoRpcClient,
        wallet_id: WalletId,
    ) -> anyhow::Result<()> {
        let account = rpc_client
            .account_list(wallet_id)
            .await?
            .accounts
            .first()
            .cloned()
            .ok_or_else(|| anyhow!("Wallet is empty"))?;

        for i in 0..10 {
            let key = account_key(i);
            info!(
                "Creating high prio account {}: {}",
                i,
                key.account().encode_account()
            );
            rpc_client
                .send(SendArgs {
                    wallet: wallet_id,
                    source: account,
                    destination: key.account(),
                    amount: Amount::millinano(1500), // bucket 16
                    work: Some(0.into()),
                    id: None,
                })
                .await?;
        }

        info!("Waiting for confirmations...");
        loop {
            let count = rpc_client.block_count().await?;
            if count.count.inner() == count.cemented.inner() {
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }
        Ok(())
    }

    pub(crate) async fn sync_accounts(&self) -> anyhow::Result<()> {
        Ok(())
    }

    pub(crate) async fn run(&mut self) {}
}

fn account_key(index: usize) -> PrivateKey {
    RawKey::from((1000 + index) as u64).into()
}
