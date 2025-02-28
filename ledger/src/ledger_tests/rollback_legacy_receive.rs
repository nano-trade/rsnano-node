use rsnano_core::PendingKey;

use crate::{
    ledger_tests::LedgerContext, test_helpers::setup_legacy_receive_block, AnySet, LedgerSet,
    DEV_GENESIS_ACCOUNT,
};

#[test]
fn clear_successor() {
    let ctx = LedgerContext::empty();
    let mut txn = ctx.ledger.rw_txn();
    let receive = setup_legacy_receive_block(&ctx, &mut txn);
    txn.commit();

    ctx.ledger.rollback2(&receive.receive_block.hash()).unwrap();

    assert_eq!(
        ctx.ledger.any().block_successor(&receive.open_block.hash()),
        None
    );
}

#[test]
fn update_account_info() {
    let ctx = LedgerContext::empty();
    let mut txn = ctx.ledger.rw_txn();
    let receive = setup_legacy_receive_block(&ctx, &mut txn);
    txn.commit();

    ctx.ledger.rollback2(&receive.receive_block.hash()).unwrap();

    let account_info = ctx
        .ledger
        .any()
        .get_account(&receive.destination.account())
        .unwrap();

    assert_eq!(account_info.head, receive.open_block.hash());
    assert_eq!(account_info.block_count, 1);
    assert_eq!(account_info.balance, receive.open_block.balance());
}

#[test]
fn rollback_pending_info() {
    let ctx = LedgerContext::empty();
    let mut txn = ctx.ledger.rw_txn();
    let receive = setup_legacy_receive_block(&ctx, &mut txn);
    txn.commit();

    ctx.ledger.rollback2(&receive.receive_block.hash()).unwrap();

    let pending = ctx
        .ledger
        .any()
        .get_pending(&PendingKey::new(
            receive.destination.account(),
            receive.send_block.hash(),
        ))
        .unwrap();

    assert_eq!(pending.source, *DEV_GENESIS_ACCOUNT);
    assert_eq!(pending.amount, receive.amount_received);
}

#[test]
fn rollback_vote_weight() {
    let ctx = LedgerContext::empty();
    let mut txn = ctx.ledger.rw_txn();
    let receive = setup_legacy_receive_block(&ctx, &mut txn);
    txn.commit();

    ctx.ledger.rollback2(&receive.receive_block.hash()).unwrap();

    assert_eq!(
        ctx.ledger.weight(&receive.destination.public_key()),
        receive.expected_balance - receive.amount_received
    );
}
