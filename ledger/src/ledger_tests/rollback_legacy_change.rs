use rsnano_core::{Account, Amount};

use crate::{ledger_constants::DEV_GENESIS_PUB_KEY, AnySet, Ledger, LedgerInserter};

#[test]
fn rollback_dependent_blocks_too() {
    let ledger = Ledger::new_null();
    let inserter = LedgerInserter::new(&ledger);

    let change = inserter.genesis().legacy_change(123);
    let send = inserter.genesis().legacy_send(Account::from(1), 100);

    ledger.roll_back(&change.hash()).unwrap();

    assert_eq!(ledger.any().get_block(&send.hash()), None);
    assert_eq!(ledger.weight(&DEV_GENESIS_PUB_KEY), Amount::MAX);
}
