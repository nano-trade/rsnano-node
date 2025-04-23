use super::LedgerContext;
use crate::{
    ledger_constants::{DEV_GENESIS_PUB_KEY, LEDGER_CONSTANTS_STUB},
    ledger_tests::AccountBlockFactory,
    AnySet, Ledger, LedgerInserter, LedgerSet, DEV_GENESIS_ACCOUNT, DEV_GENESIS_HASH,
};
use rsnano_core::{Account, Amount, Epoch, PendingInfo, PendingKey, PrivateKey, PublicKey};

#[test]
fn rollback_send() {
    let ledger = Ledger::new_null();
    let inserter = LedgerInserter::new(&ledger);
    let send = inserter.genesis().send(Account::from(1), 100);

    ledger.rollback(&send.hash()).unwrap();
    let any = ledger.any();

    assert_eq!(any.block_exists(&send.hash()), false);
    assert_eq!(any.account_balance(&DEV_GENESIS_ACCOUNT), Amount::MAX);
    assert_eq!(ledger.weight(&DEV_GENESIS_PUB_KEY), Amount::MAX);
    assert_eq!(
        any.get_pending(&PendingKey::new(*DEV_GENESIS_ACCOUNT, send.hash())),
        None
    );
    assert_eq!(any.block_successor(&DEV_GENESIS_HASH), None);
}

#[test]
fn rollback_receive() {
    let ledger = Ledger::new_null();
    let inserter = LedgerInserter::new(&ledger);

    let amount_sent = Amount::raw(50);
    let send = inserter
        .genesis()
        .send(ledger.genesis().account(), amount_sent);
    let receive = inserter.genesis().receive(send.hash());

    ledger.rollback(&receive.hash()).unwrap();
    let any = ledger.any();

    assert_eq!(any.block_exists(&receive.hash()), false);
    assert_eq!(
        any.account_balance(&DEV_GENESIS_ACCOUNT),
        Amount::MAX - amount_sent
    );
    assert_eq!(
        ledger.weight(&DEV_GENESIS_PUB_KEY),
        Amount::MAX - amount_sent
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
    let ledger = Ledger::new_null();
    let inserter = LedgerInserter::new(&ledger);
    let destination = PrivateKey::from(1);

    let send = inserter.genesis().send(&destination, 1);
    let open = inserter.account(&destination).receive(send.hash());

    ledger.rollback(&send.hash()).unwrap();

    let any = ledger.any();
    assert_eq!(
        any.get_pending(&PendingKey::new(*DEV_GENESIS_ACCOUNT, send.hash())),
        None
    );
    assert_eq!(any.block_exists(&send.hash()), false);
    assert_eq!(any.block_exists(&open.hash()), false);
    assert_eq!(any.account_balance(&DEV_GENESIS_ACCOUNT), Amount::MAX);
    assert_eq!(ledger.weight(&DEV_GENESIS_PUB_KEY), Amount::MAX);
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
