use std::sync::Arc;

use rsnano_core::{Account, ConfirmationHeightInfo, Networks};
use rsnano_store_lmdb::{TestDbFile, Writer};
use rsnano_work::WorkThresholds;

use crate::{test_helpers::AccountBlockFactory, Ledger, LedgerBuilder, LedgerConstants, LedgerSet};

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

        let ledger = Arc::new(
            LedgerBuilder::new(&db_file.path)
                .constants(constants)
                .finish()
                .unwrap(),
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

    pub fn inc_confirmation_height(&self, account: &Account) {
        let mut txn = self.ledger.store.tx_begin_write(Writer::Testing);
        let frontier = self.ledger.any().get_account(account).unwrap().head;
        let mut height = self
            .ledger
            .store
            .confirmation_height
            .get(&txn, account)
            .unwrap_or_else(|| ConfirmationHeightInfo {
                height: 0,
                frontier,
            });
        height.height = height.height + 1;
        self.ledger
            .store
            .confirmation_height
            .put(&mut txn, account, &height);
    }
}
