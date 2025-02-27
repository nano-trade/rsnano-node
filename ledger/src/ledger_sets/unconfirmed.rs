use rsnano_core::BlockHash;
use rsnano_store_lmdb::{LmdbReadTransaction, LmdbStore};

pub trait UnconfirmedSet {
    fn block_exists(&self, hash: &BlockHash) -> bool;
}

/// Unconfirmed Blocks of the ledger.
/// It owns the DB transaction
pub struct OwningUnconfirmedSet<'a> {
    store: &'a LmdbStore,
    tx: LmdbReadTransaction,
}

impl<'a> OwningUnconfirmedSet<'a> {
    pub fn new(store: &'a LmdbStore, tx: LmdbReadTransaction) -> Self {
        Self { store, tx }
    }

    fn borrowing_set(&'a self) -> BorrowingUnconfirmedSet<'a> {
        BorrowingUnconfirmedSet {
            store: self.store,
            tx: &self.tx,
        }
    }
}

impl<'a> UnconfirmedSet for OwningUnconfirmedSet<'a> {
    fn block_exists(&self, hash: &BlockHash) -> bool {
        self.borrowing_set().block_exists(hash)
    }
}

/// Unconfirmed Blocks of the ledger
/// It borrows the DB transaction
pub struct BorrowingUnconfirmedSet<'a> {
    store: &'a LmdbStore,
    tx: &'a LmdbReadTransaction,
}

impl<'a> UnconfirmedSet for BorrowingUnconfirmedSet<'a> {
    fn block_exists(&self, hash: &BlockHash) -> bool {
        if hash.is_zero() {
            return false;
        }

        let Some(block) = self.store.block.get(self.tx, hash) else {
            return false;
        };

        let conf_info = self
            .store
            .confirmation_height
            .get(self.tx, &block.account())
            .unwrap_or_default();

        block.height() > conf_info.height
    }
}
