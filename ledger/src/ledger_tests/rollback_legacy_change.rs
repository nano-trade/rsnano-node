use rsnano_core::Amount;

use crate::{ledger_constants::DEV_GENESIS_PUB_KEY, ledger_tests::LedgerContext, AnySet};

#[test]
fn rollback_dependent_blocks_too() {
    let ctx = LedgerContext::empty();
    let genesis = ctx.genesis_block_factory();

    let change = genesis.legacy_change2().build();
    ctx.ledger.process_one(&change).unwrap();

    let send = genesis.legacy_send2().build();
    ctx.ledger.process_one(&send).unwrap();

    ctx.ledger.rollback2(&change.hash()).unwrap();

    assert_eq!(ctx.ledger.any().get_block(&send.hash()), None);

    assert_eq!(ctx.ledger.weight(&DEV_GENESIS_PUB_KEY), Amount::MAX);
}
