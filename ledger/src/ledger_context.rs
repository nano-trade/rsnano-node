use std::sync::Arc;

use rsnano_core::{Account, Amount, ConfirmationHeightInfo, Networks};
use rsnano_stats::Stats;
use rsnano_store_lmdb::{LmdbStore, LmdbWriteTransaction, TestDbFile};
use rsnano_work::WorkThresholds;

use crate::{test_helpers::AccountBlockFactory, Ledger, LedgerConstants, RepWeightCache};

pub struct LedgerContext {
    pub ledger: Arc<Ledger>,
    _db_file: TestDbFile,
}

impl LedgerContext {
    pub fn empty() -> Self {
        let work = WorkThresholds::none();
        let ledger_constants = LedgerConstants::new(work, Networks::NanoDevNetwork);
        Self::with_constants(ledger_constants)
    }

    pub fn empty_dev() -> Self {
        Self::with_constants(LedgerConstants::dev())
    }

    pub fn with_constants(constants: LedgerConstants) -> Self {
        let db_file = TestDbFile::random();
        let store = Arc::new(LmdbStore::open(&db_file.path).build().unwrap());
        let rep_weights = Arc::new(RepWeightCache::new());
        let stats = Arc::new(Stats::default());
        let ledger = Arc::new(
            Ledger::new(store.clone(), constants, Amount::zero(), rep_weights, stats).unwrap(),
        );

        LedgerContext {
            ledger,
            _db_file: db_file,
        }
    }

    pub(crate) fn genesis_block_factory(&self) -> AccountBlockFactory {
        AccountBlockFactory::genesis(&self.ledger)
    }

    pub(crate) fn block_factory(&self) -> AccountBlockFactory {
        AccountBlockFactory::new(&self.ledger)
    }

    pub fn inc_confirmation_height(&self, txn: &mut LmdbWriteTransaction, account: &Account) {
        let mut height = self
            .ledger
            .store
            .confirmation_height
            .get(txn, account)
            .unwrap_or_else(|| ConfirmationHeightInfo {
                height: 0,
                frontier: self.ledger.account_info(txn, account).unwrap().head,
            });
        height.height = height.height + 1;
        self.ledger
            .store
            .confirmation_height
            .put(txn, account, &height);
    }
}
