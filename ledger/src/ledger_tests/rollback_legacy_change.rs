use rsnano_core::Amount;

use crate::{ledger_constants::DEV_GENESIS_PUB_KEY, ledger_tests::LedgerContext, AnySet};

#[test]
fn rollback_dependent_blocks_too() {
    let ctx = LedgerContext::empty();
    let mut txn = ctx.ledger.rw_txn();
    let genesis = ctx.genesis_block_factory();

    let mut change = genesis.legacy_change(&txn).build();
    ctx.ledger.process(&mut txn, &mut change).unwrap();

    let mut send = genesis.legacy_send(&txn).build();
    ctx.ledger.process(&mut txn, &mut send).unwrap();

    ctx.ledger.rollback(&mut txn, &change.hash()).unwrap();
    txn.commit();

    assert_eq!(ctx.ledger.any().get_block(&send.hash()), None);

    assert_eq!(ctx.ledger.weight(&DEV_GENESIS_PUB_KEY), Amount::MAX);
}
