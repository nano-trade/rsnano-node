use std::sync::{atomic::Ordering, Arc};

use rsnano_core::{
    utils::{new_test_timestamp, UnixTimestamp, TEST_ENDPOINT_1},
    Account, Amount, BlockHash, PrivateKey, PublicKey, Root, SavedBlock, TestBlockBuilder,
    DEV_GENESIS_KEY,
};
use rsnano_stats::Stats;
use rsnano_store_lmdb::Writer;

use crate::{
    ledger_constants::{DEV_GENESIS_BLOCK, DEV_GENESIS_PUB_KEY, LEDGER_CONSTANTS_STUB},
    test_helpers::{
        setup_legacy_open_block, setup_open_block, AccountBlockFactory, SavedBlockLatticeBuilder,
    },
    AnySet, ConfirmedSet, Ledger, LedgerContext, LedgerInserter, RepWeightCache,
    DEV_GENESIS_ACCOUNT, DEV_GENESIS_HASH,
};

mod empty_ledger;
mod pruning;
mod receivable_iteration;
mod rollback_legacy_change;
mod rollback_legacy_receive;
mod rollback_legacy_send;
mod rollback_state;

#[test]
fn ledger_successor() {
    let ledger = Ledger::new_null();
    let inserter = LedgerInserter::new(&ledger);
    let send = inserter.genesis().send(Account::from(1), 1000);

    assert_eq!(
        ledger
            .any()
            .block_successor_by_qualified_root(&ledger.genesis().qualified_root()),
        Some(ledger.genesis().hash())
    );

    assert_eq!(
        ledger
            .any()
            .block_successor_by_qualified_root(&send.qualified_root()),
        Some(send.hash())
    );
}

#[test]
fn latest_root_empty() {
    let ledger = Ledger::new_null();
    assert_eq!(ledger.any().latest_root(&Account::from(1)), Root::from(1));
}

#[test]
fn latest_root() {
    let ledger = Ledger::new_null();
    let inserter = LedgerInserter::new(&ledger);
    let send = inserter.genesis().send(Account::from(1), 1000);

    assert_eq!(
        ledger.any().latest_root(&ledger.genesis().account()),
        send.hash().into()
    );
}

#[test]
fn send_open_receive_vote_weight() {
    let ledger = Ledger::new_null();
    let inserter = LedgerInserter::new(&ledger);
    let receiver = PrivateKey::from(1);

    let send1 = inserter.genesis().send(&receiver, 50);
    let send2 = inserter.genesis().send(&receiver, 50);
    inserter.account(&receiver).receive(send1.hash());
    inserter.account(&receiver).receive(send2.hash());

    assert_eq!(ledger.weight(&receiver.public_key()), Amount::raw(100));
    assert_eq!(
        ledger.weight(&ledger.genesis().account().into()),
        Amount::MAX - Amount::raw(100)
    );
}

#[test]
fn send_open_receive_rollback() {
    let ledger = Ledger::new_null();
    let inserter = LedgerInserter::new(&ledger);
    let receiver = PrivateKey::from(1);

    let send1 = inserter.genesis().send(&receiver, 50);
    let send2 = inserter.genesis().send(&receiver, 50);
    let open = inserter.account(&receiver).receive(send1.hash());
    let receive = inserter.account(&receiver).receive(send2.hash());

    let rep_account = PublicKey::from(2);
    let change = inserter.genesis().change(rep_account);

    ledger.rollback(&receive.hash()).unwrap();

    assert_eq!(ledger.weight(&receiver.public_key()), Amount::raw(50));
    assert_eq!(ledger.weight(&DEV_GENESIS_PUB_KEY), Amount::zero());
    assert_eq!(ledger.weight(&rep_account), Amount::MAX - Amount::raw(100));

    ledger.rollback(&open.hash()).unwrap();

    assert_eq!(ledger.weight(&receiver.public_key()), Amount::zero());
    assert_eq!(ledger.weight(&DEV_GENESIS_PUB_KEY), Amount::zero());
    assert_eq!(ledger.weight(&rep_account), Amount::MAX - Amount::raw(100));

    ledger.rollback(&change.hash()).unwrap();

    assert_eq!(ledger.weight(&receiver.public_key()), Amount::zero());
    assert_eq!(ledger.weight(&rep_account), Amount::zero());
    assert_eq!(
        ledger.weight(&DEV_GENESIS_PUB_KEY),
        Amount::MAX - Amount::raw(100)
    );

    ledger.rollback(&send2.hash()).unwrap();

    assert_eq!(ledger.weight(&receiver.public_key()), Amount::zero());
    assert_eq!(ledger.weight(&rep_account), Amount::zero());
    assert_eq!(
        ledger.weight(&DEV_GENESIS_PUB_KEY),
        Amount::MAX - Amount::raw(50)
    );

    ledger.rollback(&send1.hash()).unwrap();

    assert_eq!(ledger.weight(&receiver.public_key()), Amount::zero());
    assert_eq!(ledger.weight(&rep_account), Amount::zero());
    assert_eq!(ledger.weight(&DEV_GENESIS_PUB_KEY), Amount::MAX);
}

#[test]
fn block_destination_source() {
    let ctx = LedgerContext::empty();
    let ledger = &ctx.ledger;
    let genesis = ctx.genesis_block_factory();
    let dest_account = Account::from(1000);

    let send_to_dest = genesis.legacy_send().destination(dest_account).build();
    let send_to_dest = ctx.ledger.process_one(&send_to_dest).unwrap();

    let mut send_to_self = genesis.legacy_send().destination(genesis.account()).build();
    let send_to_self = ctx.ledger.process_one(&mut send_to_self).unwrap();

    let receive = genesis.legacy_receive2(send_to_self.hash()).build();
    let receive = ctx.ledger.process_one(&receive).unwrap();

    let send_to_dest_2 = genesis.send().link(dest_account).build();
    let send_to_dest_2 = ctx.ledger.process_one(&send_to_dest_2).unwrap();

    let send_to_self_2 = genesis.send().link(genesis.account()).build();
    let send_to_self_2 = ctx.ledger.process_one(&send_to_self_2).unwrap();

    let receive2 = genesis.receive(send_to_self_2.hash()).build();
    let receive2 = ctx.ledger.process_one(&receive2).unwrap();

    assert_eq!(
        ledger.any().block_balance(&receive2.hash()),
        Some(receive2.balance_field().unwrap())
    );
    assert_eq!(send_to_dest.destination(), Some(dest_account));
    assert_eq!(send_to_dest.source(), None);

    assert_eq!(send_to_self.destination(), Some(*DEV_GENESIS_ACCOUNT));
    assert_eq!(send_to_self.source(), None);

    assert_eq!(receive.destination(), None);
    assert_eq!(receive.source(), Some(send_to_self.hash()));

    assert_eq!(send_to_dest_2.destination(), Some(dest_account));
    assert_eq!(send_to_dest_2.source(), None);

    assert_eq!(send_to_self_2.destination(), Some(*DEV_GENESIS_ACCOUNT));
    assert_eq!(send_to_self_2.source(), None);

    assert_eq!(receive2.destination(), None);
    assert_eq!(receive2.source(), Some(send_to_self_2.hash()));
}

#[test]
fn state_account() {
    let ledger = Ledger::new_null();
    let inserter = LedgerInserter::new(&ledger);
    let send = inserter.genesis().send(Account::from(1), 1000);

    assert_eq!(
        ledger.any().block_account(&send.hash()),
        Some(ledger.genesis().account())
    );
}

mod dependents_confirmed {
    use super::*;
    use crate::AnySet;

    #[test]
    fn genesis_is_confirmed() {
        let ledger = Ledger::new_null();

        assert_eq!(
            ledger
                .any()
                .dependents_confirmed_for_unsaved_block(&ledger.genesis()),
            true
        );
    }

    #[test]
    fn send_dependents_are_confirmed_if_previous_block_is_confirmed() {
        let ledger = Ledger::new_null();
        let inserter = LedgerInserter::new(&ledger);
        let send = inserter.genesis().send(Account::from(1), 1000);

        assert_eq!(
            ledger.any().dependents_confirmed_for_unsaved_block(&send),
            true
        );
    }

    #[test]
    fn send_dependents_are_unconfirmed_if_previous_block_is_unconfirmed() {
        let ledger = Ledger::new_null();
        let inserter = LedgerInserter::new(&ledger);

        inserter.genesis().send(Account::from(1), 1000);
        let send2 = inserter.genesis().send(Account::from(2), 2000);

        assert_eq!(
            ledger.any().dependents_confirmed_for_unsaved_block(&send2),
            false
        );
    }

    #[test]
    fn open_dependents_are_unconfirmed_if_send_block_is_unconfirmed() {
        let ledger = Ledger::new_null();
        let inserter = LedgerInserter::new(&ledger);
        let destination = PrivateKey::from(1);

        let send = inserter.genesis().send(&destination, 1000);
        let open = inserter.account(&destination).receive(send.hash());

        assert_eq!(
            ledger.any().dependents_confirmed_for_unsaved_block(&open),
            false
        );
    }

    #[test]
    fn open_dependents_are_confirmed_if_send_block_is_confirmed() {
        let ledger = Ledger::new_null();
        let inserter = LedgerInserter::new(&ledger);
        let destination = PrivateKey::from(1);

        let send = inserter.genesis().send(&destination, 1000);
        ledger.confirm(send.hash());

        let open = inserter.account(&destination).receive(send.hash());

        assert_eq!(
            ledger.any().dependents_confirmed_for_unsaved_block(&open),
            true
        );
    }

    #[test]
    fn receive_dependents_are_unconfirmed_if_send_block_is_unconfirmed() {
        let ctx = LedgerContext::empty();

        let destination = ctx.block_factory();

        let send1 = ctx
            .genesis_block_factory()
            .send()
            .link(destination.account())
            .build();
        ctx.ledger.process_one(&send1).unwrap();
        ctx.ledger.confirm(send1.hash());

        let send2 = ctx
            .genesis_block_factory()
            .send()
            .link(destination.account())
            .build();
        ctx.ledger.process_one(&send2).unwrap();

        let open = destination.open(send1.hash()).build();
        ctx.ledger.process_one(&open).unwrap();

        ctx.ledger.confirm(open.hash());

        let receive = destination.receive(send2.hash()).build();
        ctx.ledger.process_one(&receive).unwrap();

        assert_eq!(
            ctx.ledger
                .any()
                .dependents_confirmed_for_unsaved_block(&receive),
            false
        );
    }

    #[test]
    fn receive_dependents_are_unconfirmed_if_previous_block_is_unconfirmed() {
        let ctx = LedgerContext::empty();

        let destination = ctx.block_factory();

        let send1 = ctx
            .genesis_block_factory()
            .send()
            .link(destination.account())
            .build();
        ctx.ledger.process_one(&send1).unwrap();

        ctx.ledger.confirm(send1.hash());

        let send2 = ctx
            .genesis_block_factory()
            .send()
            .link(destination.account())
            .build();
        ctx.ledger.process_one(&send2).unwrap();

        ctx.ledger.confirm(send2.hash());

        let open = destination.open(send1.hash()).build();
        ctx.ledger.process_one(&open).unwrap();

        let receive = destination.receive(send2.hash()).build();
        ctx.ledger.process_one(&receive).unwrap();

        assert_eq!(
            ctx.ledger
                .any()
                .dependents_confirmed_for_unsaved_block(&receive),
            false
        );
    }

    #[test]
    fn receive_dependents_are_confirmed_if_previous_block_and_send_block_are_confirmed() {
        let ctx = LedgerContext::empty();

        let destination = ctx.block_factory();

        let send1 = ctx
            .genesis_block_factory()
            .send()
            .link(destination.account())
            .build();
        ctx.ledger.process_one(&send1).unwrap();

        ctx.ledger.confirm(send1.hash());

        let send2 = ctx
            .genesis_block_factory()
            .send()
            .link(destination.account())
            .build();
        ctx.ledger.process_one(&send2).unwrap();

        ctx.ledger.confirm(send2.hash());

        let open = destination.open(send1.hash()).build();
        ctx.ledger.process_one(&open).unwrap();

        ctx.ledger.confirm(open.hash());

        let receive = destination.receive(send2.hash()).build();
        ctx.ledger.process_one(&receive).unwrap();

        assert_eq!(
            ctx.ledger
                .any()
                .dependents_confirmed_for_unsaved_block(&receive),
            true
        );
    }

    #[test]
    fn dependents_confirmed_pruning() {
        let ctx = LedgerContext::empty();
        ctx.ledger.enable_pruning();
        let destination = ctx.block_factory();

        let send1 = ctx
            .genesis_block_factory()
            .send()
            .amount_sent(Amount::raw(1))
            .link(destination.account())
            .build();

        ctx.ledger.process_one(&send1).unwrap();

        ctx.ledger.confirm(send1.hash());

        let send2 = ctx
            .genesis_block_factory()
            .send()
            .link(destination.account())
            .build();
        ctx.ledger.process_one(&send2).unwrap();

        ctx.ledger.confirm(send2.hash());

        assert_eq!(ctx.ledger.prune_one(&send2.hash(), 1), 2);

        let receive1 = TestBlockBuilder::state()
            .account(destination.account())
            .previous(0)
            .balance(Amount::raw(1))
            .link(send1.hash())
            .key(&destination.key)
            .build();

        assert_eq!(
            ctx.ledger
                .any()
                .dependents_confirmed_for_unsaved_block(&receive1),
            true
        );
    }
}

#[test]
fn block_confirmed() {
    let ctx = LedgerContext::empty();
    assert_eq!(
        ctx.ledger
            .confirmed()
            .block_exists_or_pruned(&DEV_GENESIS_HASH),
        true
    );

    let destination = ctx.block_factory();
    let send = ctx
        .genesis_block_factory()
        .send()
        .link(destination.account())
        .build();

    // Must be safe against non-existing blocks
    assert_eq!(
        ctx.ledger.confirmed().block_exists_or_pruned(&send.hash()),
        false
    );

    ctx.ledger.process_one(&send).unwrap();

    assert_eq!(
        ctx.ledger.confirmed().block_exists_or_pruned(&&send.hash()),
        false
    );

    ctx.ledger.confirm(send.hash());

    assert_eq!(
        ctx.ledger.confirmed().block_exists_or_pruned(&send.hash()),
        true
    );
}

#[test]
fn ledger_cache() {
    let ctx = LedgerContext::empty();
    let genesis = ctx.genesis_block_factory();
    let total = 10u64;

    struct ExpectedCache {
        account_count: u64,
        block_count: u64,
        cemented_count: u64,
        pruned_count: u64,
    }

    // Check existing ledger (incremental cache update) and reload on a new ledger
    for i in 0..total {
        let mut expected = ExpectedCache {
            account_count: 1 + i,
            block_count: 1 + 2 * (i + 1) - 2,
            cemented_count: 1 + 2 * (i + 1) - 2,
            pruned_count: i,
        };

        let check_impl = |ledger: &Ledger, expected: &ExpectedCache| {
            assert_eq!(ledger.account_count(), expected.account_count);
            assert_eq!(ledger.block_count(), expected.block_count);
            assert_eq!(ledger.confirmed_count(), expected.cemented_count);
            assert_eq!(ledger.pruned_count(), expected.pruned_count);
        };

        let cache_check = |ledger: &Ledger, expected: &ExpectedCache| {
            check_impl(ledger, expected);

            ctx.ledger.store.cache.reset();
            let new_ledger = Ledger::new(
                ctx.ledger.store.clone(),
                LEDGER_CONSTANTS_STUB.clone(),
                Amount::zero(),
                Arc::new(RepWeightCache::new()),
                Arc::new(Stats::default()),
            )
            .unwrap();
            check_impl(&new_ledger, expected);
        };

        let destination = ctx.block_factory();
        let send = genesis.send().link(destination.account()).build();
        ctx.ledger.process_one(&send).unwrap();
        expected.block_count += 1;
        cache_check(&ctx.ledger, &expected);

        let open = destination.open(send.hash()).build();
        ctx.ledger.process_one(&open).unwrap();
        expected.block_count += 1;
        expected.account_count += 1;
        cache_check(&ctx.ledger, &expected);

        {
            ctx.inc_confirmation_height(&DEV_GENESIS_ACCOUNT);
            ctx.ledger
                .store
                .cache
                .confirmed_count
                .fetch_add(1, Ordering::Relaxed);
            expected.cemented_count += 1;
        }
        cache_check(&ctx.ledger, &expected);

        {
            ctx.inc_confirmation_height(&destination.account());
            ctx.ledger
                .store
                .cache
                .confirmed_count
                .fetch_add(1, Ordering::Relaxed);
            expected.cemented_count += 1;
        }
        cache_check(&ctx.ledger, &expected);

        {
            let mut txn = ctx.ledger.store.tx_begin_write(Writer::Testing);
            ctx.ledger.store.pruned.put(&mut txn, &open.hash());
            ctx.ledger
                .store
                .cache
                .pruned_count
                .fetch_add(1, Ordering::Relaxed);
            expected.pruned_count += 1;
        }
        cache_check(&ctx.ledger, &expected);
    }
}

#[test]
fn is_send_genesis() {
    assert_eq!(DEV_GENESIS_BLOCK.is_send(), false);
}

#[test]
fn is_send_state() {
    let ctx = LedgerContext::empty();
    let open = setup_open_block(&ctx);
    assert_eq!(open.send_block.is_send(), true);
    assert_eq!(open.open_block.is_send(), false);
}

#[test]
fn is_send_legacy() {
    let ctx = LedgerContext::empty();
    let open = setup_legacy_open_block(&ctx);
    assert_eq!(open.send_block.is_send(), true);
    assert_eq!(open.open_block.is_send(), false);
}

#[test]
fn sideband_height() {
    let ctx = LedgerContext::empty();
    let genesis = ctx.genesis_block_factory();
    let dest1 = ctx.block_factory();
    let dest2 = ctx.block_factory();
    let dest3 = ctx.block_factory();

    let send = genesis.legacy_send().destination(genesis.account()).build();
    ctx.ledger.process_one(&send).unwrap();

    let receive = genesis.legacy_receive2(send.hash()).build();
    ctx.ledger.process_one(&receive).unwrap();

    let change = genesis.legacy_change().build();
    ctx.ledger.process_one(&change).unwrap();

    let state_send1 = genesis.send().link(dest1.account()).build();
    ctx.ledger.process_one(&state_send1).unwrap();

    let state_send2 = genesis.send().link(dest2.account()).build();
    ctx.ledger.process_one(&state_send2).unwrap();

    let state_send3 = genesis.send().link(dest3.account()).build();
    ctx.ledger.process_one(&state_send3).unwrap();

    let state_open = dest1.open(state_send1.hash()).build();
    ctx.ledger.process_one(&state_open).unwrap();

    let epoch = dest1.epoch_v1().build();
    ctx.ledger.process_one(&epoch).unwrap();

    let epoch_open = dest2.epoch_v1_open().build();
    ctx.ledger.process_one(&epoch_open).unwrap();

    let state_receive = dest2.receive(state_send2.hash()).build();
    ctx.ledger.process_one(&state_receive).unwrap();

    let open = dest3.legacy_open(state_send3.hash()).build();
    ctx.ledger.process_one(&open).unwrap();

    let assert_sideband_height = |hash: &BlockHash, expected_height: u64| {
        let block = ctx.ledger.any().get_block(hash).unwrap();
        assert_eq!(block.height(), expected_height);
    };

    assert_sideband_height(&DEV_GENESIS_HASH, 1);
    assert_sideband_height(&send.hash(), 2);
    assert_sideband_height(&receive.hash(), 3);
    assert_sideband_height(&change.hash(), 4);
    assert_sideband_height(&state_send1.hash(), 5);
    assert_sideband_height(&state_send2.hash(), 6);
    assert_sideband_height(&state_send3.hash(), 7);

    assert_sideband_height(&state_open.hash(), 1);
    assert_sideband_height(&epoch.hash(), 2);

    assert_sideband_height(&epoch_open.hash(), 1);
    assert_sideband_height(&state_receive.hash(), 2);

    assert_sideband_height(&open.hash(), 1);
}

#[test]
fn configured_peers_response() {
    let endpoint = TEST_ENDPOINT_1;
    let now = new_test_timestamp();
    let ledger = Ledger::new_null_builder().peers([(endpoint, now)]).finish();
    let tx = ledger.store.tx_begin_read();
    assert_eq!(ledger.store.peer.iter(&tx).next().unwrap(), (endpoint, now));
}

#[test]
fn block_priority() {
    let mut lattice = SavedBlockLatticeBuilder::new();
    lattice.set_now(UnixTimestamp::new(10));
    let send = lattice.genesis().send(&*DEV_GENESIS_KEY, Amount::nano(500));
    lattice.set_now(UnixTimestamp::new(20));
    let receive = lattice.genesis().receive(&send);
    let ledger = Ledger::new_null_builder()
        .block(&send)
        .block(&receive)
        .finish();

    let prio = ledger.any().block_priority(&receive);

    assert_eq!(prio.balance, receive.balance());
    assert_eq!(prio.time, send.timestamp().into());
}

#[test]
fn linked_account_for_change_block() {
    let ledger = Ledger::new_null();
    let block = SavedBlock::new_test_change_block();
    assert_eq!(ledger.any().linked_account(&block), None);
}

#[test]
fn linked_account_for_send_block() {
    let ledger = Ledger::new_null();
    let block = SavedBlock::new_test_send_block();
    assert_eq!(
        ledger.any().linked_account(&block),
        Some(block.destination_or_link())
    );
}

#[test]
fn linked_account_for_receive_block() {
    let sender = PrivateKey::from(1);
    let receiver = PrivateKey::from(2);

    let send_block = TestBlockBuilder::state()
        .key(&sender)
        .link(&receiver)
        .is_send()
        .build_saved();

    let receive_block = TestBlockBuilder::state()
        .key(&receiver)
        .link(send_block.hash())
        .is_receive()
        .build_saved();

    let ledger = Ledger::new_null_builder().block(&send_block).finish();
    assert_eq!(
        ledger.any().linked_account(&receive_block),
        Some(sender.account())
    );
}
