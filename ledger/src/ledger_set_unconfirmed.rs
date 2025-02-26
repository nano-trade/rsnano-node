use rsnano_core::BlockHash;
use rsnano_store_lmdb::{LmdbReadTransaction, LmdbStore};

pub struct LedgerSetUnconfirmed<'a> {
    store: &'a LmdbStore,
    tx: LmdbReadTransaction,
}

impl<'a> LedgerSetUnconfirmed<'a> {
    pub fn new(store: &'a LmdbStore, tx: LmdbReadTransaction) -> Self {
        Self { store, tx }
    }

    pub fn block_exists(&self, hash: &BlockHash) -> bool {
        if hash.is_zero() {
            return false;
        }

        let Some(block) = self.store.block.get(&self.tx, hash) else {
            return false;
        };

        let conf_info = self
            .store
            .confirmation_height
            .get(&self.tx, &block.account())
            .unwrap_or_default();

        block.height() > conf_info.height
    }
}
