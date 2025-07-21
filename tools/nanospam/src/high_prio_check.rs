use anyhow::anyhow;
use rsnano_core::{Amount, Block, PrivateKey, RawKey};
use rsnano_rpc_client::NanoRpcClient;
use rsnano_rpc_messages::SendArgs;
use std::time::Duration;
use tokio::{sync::mpsc::Sender, time::sleep};
use tracing::info;

/// Periodically publishes a high priority block and tracks confirmation time
pub(crate) struct HighPrioCheck<'a> {
    tx_block: Sender<Block>,
    rpc_client: &'a NanoRpcClient,
}

impl<'a> HighPrioCheck<'a> {
    pub(crate) fn new(tx_block: Sender<Block>, rpc_client: &'a NanoRpcClient) -> Self {
        Self {
            tx_block,
            rpc_client,
        }
    }

    pub(crate) async fn create_prio_accounts(&self) -> anyhow::Result<()> {
        let wallet_id = self
            .rpc_client
            .wallet_list()
            .await?
            .first()
            .cloned()
            .ok_or_else(|| anyhow!("No wallet id found"))?;

        let account = self
            .rpc_client
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
            self.rpc_client
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
            let count = self.rpc_client.block_count().await?;
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
