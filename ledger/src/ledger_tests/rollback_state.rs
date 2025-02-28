use super::LedgerContext;
use crate::{
    ledger_constants::{DEV_GENESIS_PUB_KEY, LEDGER_CONSTANTS_STUB},
    ledger_tests::AccountBlockFactory,
    AnySet, LedgerSet, DEV_GENESIS_ACCOUNT, DEV_GENESIS_HASH,
};
use rsnano_core::{Amount, Epoch, PendingInfo, PendingKey, PublicKey};

#[test]
fn rollback_send() {
    let ctx = LedgerContext::empty();
    let genesis = ctx.genesis_block_factory();

    let send = genesis.send().build();
    ctx.ledger.process_one(&send).unwrap();

    ctx.ledger.rollback(&send.hash()).unwrap();
    let any = ctx.ledger.any();

    assert_eq!(any.block_exists(&send.hash()), false);
    assert_eq!(
        any.account_balance(&DEV_GENESIS_ACCOUNT),
        LEDGER_CONSTANTS_STUB.genesis_amount
    );
    assert_eq!(
        ctx.ledger.weight(&DEV_GENESIS_PUB_KEY),
        LEDGER_CONSTANTS_STUB.genesis_amount
    );
    assert_eq!(
        any.get_pending(&PendingKey::new(*DEV_GENESIS_ACCOUNT, send.hash())),
        None
    );
    assert_eq!(any.block_successor(&DEV_GENESIS_HASH), None);
}

#[test]
fn rollback_receive() {
    let ctx = LedgerContext::empty();
    let genesis = ctx.genesis_block_factory();

    let amount_sent = Amount::raw(50);
    let send = genesis
        .send()
        .amount_sent(amount_sent)
        .link(genesis.account())
        .build();
    ctx.ledger.process_one(&send).unwrap();

    let receive = genesis.receive(send.hash()).build();
    ctx.ledger.process_one(&receive).unwrap();

    ctx.ledger.rollback(&receive.hash()).unwrap();
    let any = ctx.ledger.any();

    assert_eq!(any.block_exists(&receive.hash()), false);
    assert_eq!(
        any.account_balance(&DEV_GENESIS_ACCOUNT),
        LEDGER_CONSTANTS_STUB.genesis_amount - amount_sent
    );
    assert_eq!(
        ctx.ledger.weight(&DEV_GENESIS_PUB_KEY),
        LEDGER_CONSTANTS_STUB.genesis_amount - amount_sent
    );
    assert_eq!(
        any.get_pending(&PendingKey::new(*DEV_GENESIS_ACCOUNT, send.hash())),
        Some(PendingInfo {
            source: *DEV_GENESIS_ACCOUNT,
            amount: amount_sent,
            epoch: Epoch::Epoch0
        })
    );
    assert_eq!(any.block_successor(&send.hash()), None);
}

#[test]
fn rollback_received_send() {
    let ctx = LedgerContext::empty();
    let genesis = ctx.genesis_block_factory();
    let destination = AccountBlockFactory::new(&ctx.ledger);

    let send = genesis.send().link(destination.account()).build();
    ctx.ledger.process_one(&send).unwrap();

    let open = destination.open(send.hash()).build();
    ctx.ledger.process_one(&open).unwrap();

    ctx.ledger.rollback(&send.hash()).unwrap();
    let any = ctx.ledger.any();

    assert_eq!(
        any.get_pending(&PendingKey::new(*DEV_GENESIS_ACCOUNT, send.hash())),
        None
    );
    assert_eq!(any.block_exists(&send.hash()), false);
    assert_eq!(any.block_exists(&open.hash()), false);
    assert_eq!(
        any.account_balance(&DEV_GENESIS_ACCOUNT),
        LEDGER_CONSTANTS_STUB.genesis_amount
    );
    assert_eq!(
        ctx.ledger.weight(&DEV_GENESIS_PUB_KEY),
        LEDGER_CONSTANTS_STUB.genesis_amount
    );
    assert_eq!(any.account_balance(&destination.account()), Amount::zero());
}

#[test]
fn rollback_rep_change() {
    let ctx = LedgerContext::empty();
    let genesis = ctx.genesis_block_factory();
    let representative = PublicKey::from(1);

    let change = genesis.change().representative(representative).build();
    ctx.ledger.process_one(&change).unwrap();

    ctx.ledger.rollback(&change.hash()).unwrap();
    let any = ctx.ledger.any();

    assert_eq!(any.block_exists(&change.hash()), false);
    assert_eq!(
        any.account_balance(&DEV_GENESIS_ACCOUNT),
        LEDGER_CONSTANTS_STUB.genesis_amount
    );
    assert_eq!(
        ctx.ledger.weight(&DEV_GENESIS_PUB_KEY),
        LEDGER_CONSTANTS_STUB.genesis_amount
    );
    assert_eq!(ctx.ledger.weight(&representative), Amount::zero());
}

#[test]
fn rollback_open() {
    let ctx = LedgerContext::empty();
    let genesis = ctx.genesis_block_factory();
    let destination = AccountBlockFactory::new(&ctx.ledger);

    let amount_sent = Amount::raw(50);
    let send = genesis
        .send()
        .link(destination.account())
        .amount_sent(amount_sent)
        .build();
    ctx.ledger.process_one(&send).unwrap();

    let open = destination.open(send.hash()).build();
    ctx.ledger.process_one(&open).unwrap();

    ctx.ledger.rollback(&open.hash()).unwrap();
    let any = ctx.ledger.any();

    assert_eq!(any.block_exists(&open.hash()), false);
    assert_eq!(any.account_balance(&destination.account()), Amount::zero());
    assert_eq!(
        ctx.ledger.weight(&DEV_GENESIS_PUB_KEY),
        LEDGER_CONSTANTS_STUB.genesis_amount - amount_sent
    );
    assert_eq!(
        any.get_pending(&PendingKey::new(destination.account(), send.hash()))
            .unwrap(),
        PendingInfo {
            source: *DEV_GENESIS_ACCOUNT,
            amount: Amount::raw(50),
            epoch: Epoch::Epoch0
        }
    );
}

#[test]
fn rollback_send_with_rep_change() {
    let ctx = LedgerContext::empty();
    let genesis = ctx.genesis_block_factory();

    let representative = PublicKey::from(1);
    let send = genesis.send().representative(representative).build();
    ctx.ledger.process_one(&send).unwrap();

    ctx.ledger.rollback(&send.hash()).unwrap();
    let any = ctx.ledger.any();

    assert_eq!(any.block_exists(&send.hash()), false);
    assert_eq!(
        any.account_balance(&DEV_GENESIS_ACCOUNT),
        LEDGER_CONSTANTS_STUB.genesis_amount
    );
    assert_eq!(
        ctx.ledger.weight(&DEV_GENESIS_PUB_KEY),
        LEDGER_CONSTANTS_STUB.genesis_amount
    );
    assert_eq!(ctx.ledger.weight(&representative), Amount::zero());
}

#[test]
fn rollback_receive_with_rep_change() {
    let ctx = LedgerContext::empty();
    let genesis = ctx.genesis_block_factory();

    let representative = PublicKey::from(1);
    let send = genesis.send().link(genesis.account()).build();
    ctx.ledger.process_one(&send).unwrap();

    let receive = genesis
        .receive(send.hash())
        .representative(representative)
        .build();
    ctx.ledger.process_one(&receive).unwrap();

    ctx.ledger.rollback(&receive.hash()).unwrap();
    let any = ctx.ledger.any();

    assert_eq!(any.block_exists(&receive.hash()), false);
    assert_eq!(
        any.account_balance(&DEV_GENESIS_ACCOUNT),
        send.balance_field().unwrap()
    );
    assert_eq!(
        ctx.ledger.weight(&DEV_GENESIS_PUB_KEY),
        send.balance_field().unwrap()
    );
    assert_eq!(ctx.ledger.weight(&representative), Amount::zero());
}
