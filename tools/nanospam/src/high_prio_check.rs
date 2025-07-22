use std::{
    collections::{HashMap, HashSet},
    sync::Mutex,
    time::{Duration, Instant},
};

use anyhow::anyhow;
use tokio::{select, sync::mpsc::Sender, time::sleep};
use tracing::info;

use crate::domain::DelayedBlocks;
use rsnano_core::{
    Account, Amount, Block, BlockHash, JsonBlock, Link, PrivateKey, PublicKey, RawKey,
    StateBlockArgs, WalletId,
};
use rsnano_rpc_client::NanoRpcClient;
use rsnano_rpc_messages::SendArgs;
use tokio_util::sync::CancellationToken;

const PRIO_ACCOUNTS: usize = 20;
const INITIAL_ACCOUNT_BALANCE: Amount = Amount::millinano(1500); // bucket 16

/// Periodically publishes a high priority block and tracks confirmation time
pub(crate) struct HighPrioCheck<'a> {
    rpc_client: &'a NanoRpcClient,
    delayed_blocks: &'a Mutex<DelayedBlocks>,
    /// prio account => key + frontier hash + height
    accounts: HashMap<Account, (PrivateKey, BlockHash, u64)>,
    tracker: &'a Mutex<HighPrioTracker>,
}

impl<'a> HighPrioCheck<'a> {
    pub(crate) fn new(
        rpc_client: &'a NanoRpcClient,
        delayed_blocks: &'a Mutex<DelayedBlocks>,
        tracker: &'a Mutex<HighPrioTracker>,
    ) -> Self {
        Self {
            rpc_client,
            delayed_blocks,
            accounts: prio_account_keys()
                .map(|k| (k.account(), (k, BlockHash::zero(), 0)))
                .collect(),
            tracker,
        }
    }

    pub(crate) async fn create_prio_accounts(&mut self, wallet_id: WalletId) -> anyhow::Result<()> {
        info!("Creating high priority accounts...");
        let account = self
            .rpc_client
            .account_list(wallet_id)
            .await?
            .accounts
            .first()
            .cloned()
            .ok_or_else(|| anyhow!("Wallet is empty"))?;

        let keys: Vec<_> = self
            .accounts
            .values()
            .map(|(key, _, _)| key.clone())
            .collect();

        for key in keys {
            let send_block = self
                .rpc_client
                .send(SendArgs {
                    wallet: wallet_id,
                    source: account,
                    destination: key.account(),
                    amount: INITIAL_ACCOUNT_BALANCE,
                    work: Some(0.into()),
                    id: None,
                })
                .await?;

            let receive_block: Block = StateBlockArgs {
                key: &key,
                previous: BlockHash::zero(),
                representative: key.public_key(),
                balance: INITIAL_ACCOUNT_BALANCE,
                link: send_block.block.into(),
                work: 0.into(),
            }
            .into();

            let receive_hash = receive_block.hash();
            self.rpc_client
                .process(JsonBlock::from(receive_block))
                .await?;

            self.accounts
                .insert(key.account(), (key.clone(), receive_hash, 1));
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

    pub(crate) async fn sync_accounts(&mut self) -> anyhow::Result<()> {
        let keys: Vec<_> = self
            .accounts
            .values()
            .map(|(key, _, _)| key.clone())
            .collect();

        for key in keys {
            let result = self.rpc_client.account_info(key.account()).await?;
            self.accounts.insert(
                key.account(),
                (key, result.frontier, result.block_count.inner()),
            );
        }

        Ok(())
    }

    pub(crate) async fn run(&mut self, cancel_token: CancellationToken, tx_block: Sender<Block>) {
        loop {
            select! {
                _ = cancel_token.cancelled() => { break;},
                _ =sleep(Duration::from_secs(10)) => {}
            }

            let (key, frontier, height) = self
                .accounts
                .values()
                .min_by(|(_, _, x), (_, _, y)| x.cmp(y))
                .unwrap();

            let block: Block = StateBlockArgs {
                key,
                previous: *frontier,
                representative: PublicKey::from(*height),
                balance: INITIAL_ACCOUNT_BALANCE,
                link: Link::zero(),
                work: 0.into(),
            }
            .into();

            let account = key.account();

            {
                let mut delayed = self.delayed_blocks.lock().unwrap();
                if delayed.is_finished() {
                    break;
                }
                delayed.insert(block.clone());
            }

            let hash = block.hash();
            self.tracker.lock().unwrap().enqueued(hash);
            tx_block.send(block).await.unwrap();
            let (_, frontier, height) = self.accounts.get_mut(&account).unwrap();
            *frontier = hash;
            *height += 1;
        }
    }
}

fn prio_account_keys() -> impl Iterator<Item = PrivateKey> {
    (0..PRIO_ACCOUNTS).map(account_key)
}

fn account_key(index: usize) -> PrivateKey {
    RawKey::from((1000 + index) as u64).into()
}

#[derive(Default)]
pub(crate) struct HighPrioTracker {
    enqueued: HashSet<BlockHash>,
    published: HashMap<BlockHash, Instant>,
}

impl HighPrioTracker {
    pub(crate) fn enqueued(&mut self, hash: BlockHash) {
        self.enqueued.insert(hash);
    }

    pub(crate) fn published(&mut self, hash: BlockHash) {
        if self.enqueued.remove(&hash) {
            self.published.insert(hash, Instant::now());
            info!("High prio block published: {hash}");
        }
    }

    pub(crate) fn confirmed(&mut self, hash: BlockHash) {
        if let Some(published) = self.published.remove(&hash) {
            info!(
                "High prio block confirmed: {hash}. Conf time: {} ms",
                published.elapsed().as_millis()
            );
        }
    }
}
