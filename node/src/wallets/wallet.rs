use anyhow::Context;
use rsnano_core::{KeyDerivationFunction, PrivateKey, PublicKey, Root, WorkNonce};
use rsnano_ledger::{AnySet, Ledger};
use rsnano_store_lmdb::{LmdbEnv, LmdbWalletStore, LmdbWriteTransaction, Transaction};
use rsnano_work::WorkThresholds;
use std::{
    collections::HashSet,
    path::Path,
    sync::{Arc, Mutex},
};
use tracing::warn;

pub struct Wallet {
    pub representatives: Mutex<HashSet<PublicKey>>,
    pub store: Arc<LmdbWalletStore>,
    ledger: Arc<Ledger>,
    work_thresholds: WorkThresholds,
}

impl Wallet {
    pub fn new(
        ledger: Arc<Ledger>,
        work_thresholds: WorkThresholds,
        env: &LmdbEnv,
        fanout: usize,
        kdf: KeyDerivationFunction,
        representative: PublicKey,
        wallet_path: &Path,
    ) -> anyhow::Result<Self> {
        let store = LmdbWalletStore::new(fanout, kdf, env, &representative, &wallet_path)
            .context("could not create wallet store")?;

        Ok(Self {
            representatives: Mutex::new(HashSet::new()),
            store: Arc::new(store),
            ledger,
            work_thresholds,
        })
    }

    pub fn new_from_json(
        ledger: Arc<Ledger>,
        work_thresholds: WorkThresholds,
        env: &LmdbEnv,
        fanout: usize,
        kdf: KeyDerivationFunction,
        wallet_path: &Path,
        json: &str,
    ) -> anyhow::Result<Self> {
        let store = LmdbWalletStore::new_from_json(fanout, kdf, env, &wallet_path, json)
            .context("could not create wallet store")?;

        Ok(Self {
            representatives: Mutex::new(HashSet::new()),
            store: Arc::new(store),
            ledger,
            work_thresholds,
        })
    }

    pub fn work_update(
        &self,
        txn: &mut LmdbWriteTransaction,
        pub_key: &PublicKey,
        root: &Root,
        work: WorkNonce,
    ) {
        debug_assert!(self.work_thresholds.validate_entry(root, work));
        debug_assert!(self.store.exists(txn, pub_key));
        let latest = self.ledger.any().latest_root(&pub_key.into());
        if latest == *root {
            self.store.work_put(txn, pub_key, work);
        } else {
            warn!("Cached work no longer valid, discarding");
        }
    }

    pub fn deterministic_check(&self, txn: &dyn Transaction, index: u32) -> u32 {
        let mut result = index;
        let any = self.ledger.any();
        let mut i = index + 1;
        let mut n = index + 64;
        while i < n {
            let prv = self.store.deterministic_key(txn, i);
            let pair = PrivateKey::from_bytes(prv.as_bytes());
            // Check if account received at least 1 block
            let latest = any.account_head(&pair.account());
            match latest {
                Some(_) => {
                    result = i;
                    // i + 64 - Check additional 64 accounts
                    // i/64 - Check additional accounts for large wallets. I.e. 64000/64 = 1000 accounts to check
                    n = i + 64 + (i / 64);
                }
                None => {
                    // Check if there are pending blocks for account
                    if any.receivable_exists(pair.account()) {
                        result = i;
                        n = i + 64 + (i / 64);
                    }
                }
            }

            i += 1;
        }
        result
    }

    pub fn live(&self) -> bool {
        self.store.is_open()
    }
}
