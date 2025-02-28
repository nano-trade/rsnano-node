use rsnano_core::{Amount, PendingKey};

use crate::{
    ledger_constants::{DEV_GENESIS_PUB_KEY, LEDGER_CONSTANTS_STUB},
    ledger_tests::setup_legacy_open_block,
    test_helpers::{setup_legacy_send_block, LegacySendBlockResult},
    AnySet, ConfirmedSet, LedgerSet, DEV_GENESIS_ACCOUNT, DEV_GENESIS_HASH,
};

use super::LedgerContext;

#[test]
fn update_vote_weight() {
    let ctx = LedgerContext::empty();

    rollback_send_block(&ctx);

    assert_eq!(
        ctx.ledger.weight(&DEV_GENESIS_PUB_KEY),
        LEDGER_CONSTANTS_STUB.genesis_amount
    );
}

#[test]
fn update_account_store() {
    let ctx = LedgerContext::empty();

    rollback_send_block(&ctx);

    let account_info = ctx.ledger.any().get_account(&DEV_GENESIS_ACCOUNT).unwrap();
    assert_eq!(account_info.block_count, 1);
    assert_eq!(account_info.head, *DEV_GENESIS_HASH);
    assert_eq!(account_info.balance, LEDGER_CONSTANTS_STUB.genesis_amount);
    assert_eq!(ctx.ledger.account_count(), 1);
}

#[test]
fn remove_from_pending_store() {
    let ctx = LedgerContext::empty();

    let send = rollback_send_block(&ctx);

    let pending = ctx.ledger.any().get_pending(&PendingKey::new(
        send.destination.account(),
        send.send_block.hash(),
    ));
    assert_eq!(pending, None);
}

#[test]
fn update_confirmation_height_store() {
    let ctx = LedgerContext::empty();

    rollback_send_block(&ctx);

    let conf_height = ctx
        .ledger
        .confirmed()
        .get_conf_info(&DEV_GENESIS_ACCOUNT)
        .unwrap();

    assert_eq!(conf_height.frontier, *DEV_GENESIS_HASH);
    assert_eq!(conf_height.height, 1);
}

#[test]
fn rollback_dependent_blocks_too() {
    let ctx = LedgerContext::empty();
    let open = setup_legacy_open_block(&ctx);

    // Rollback send block. This requires the rollback of the open block first.
    ctx.ledger.rollback(&open.send_block.hash()).unwrap();

    assert_eq!(
        ctx.ledger.any().account_balance(&DEV_GENESIS_ACCOUNT),
        LEDGER_CONSTANTS_STUB.genesis_amount
    );

    assert_eq!(
        ctx.ledger
            .any()
            .account_balance(&open.destination.account()),
        Amount::zero()
    );

    assert!(ctx
        .ledger
        .any()
        .get_account(&open.destination.account())
        .is_none());

    let pending = ctx.ledger.any().get_pending(&PendingKey::new(
        open.destination.account(),
        *DEV_GENESIS_HASH,
    ));
    assert_eq!(pending, None);
}

fn rollback_send_block<'a>(ctx: &'a LedgerContext) -> LegacySendBlockResult<'a> {
    let send = setup_legacy_send_block(ctx);
    ctx.ledger.rollback(&send.send_block.hash()).unwrap();
    send
}
