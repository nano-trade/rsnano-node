use rsnano_core::{
    Amount, Block, Epoch, OpenBlockArgs, PendingKey, PrivateKey, ReceiveBlockArgs, StateBlockArgs,
    TestBlockBuilder, WorkNonce, DEV_GENESIS_KEY,
};

use crate::{AnySet, Ledger, LedgerInserter, LedgerSet, DEV_GENESIS_ACCOUNT, DEV_GENESIS_HASH};

#[test]
fn pruning_action() {
    let ledger = Ledger::new_null();
    let inserter = LedgerInserter::new(&ledger);
    ledger.enable_pruning();

    let genesis_account = ledger.genesis().account();
    let send1 = inserter.genesis().send(genesis_account, 100);
    ledger.confirm(send1.hash());

    let send2 = inserter.genesis().send(genesis_account, 100);
    ledger.confirm(send2.hash());

    // Prune...
    assert_eq!(ledger.prune_one(&send1.hash(), 1), 1);
    assert_eq!(ledger.prune_one(&DEV_GENESIS_HASH, 1), 0);

    let mut any = ledger.any();
    assert!(any
        .get_pending(&PendingKey::new(genesis_account, send1.hash()))
        .is_some());

    assert_eq!(any.block_exists(&send1.hash()), false);
    assert!(any.block_exists_or_pruned(&send1.hash()),);
    assert!(any.block_exists(&DEV_GENESIS_HASH));
    assert!(any.block_exists(&send2.hash()));

    // Receiving pruned block
    let receive1 = TestBlockBuilder::state()
        .account(genesis_account)
        .previous(send2.hash())
        .balance(Amount::MAX - Amount::raw(100))
        .link(send1.hash())
        .key(&DEV_GENESIS_KEY)
        .work(u64::MAX)
        .build();
    ledger.process_one(&receive1).unwrap();

    any = ledger.any();
    assert!(any.block_exists(&receive1.hash()));
    assert_eq!(
        any.get_pending(&PendingKey::new(genesis_account, send1.hash())),
        None
    );
    let receive1_stored = any.get_block(&receive1.hash()).unwrap();
    assert_eq!(&receive1, &*receive1_stored);
    assert_eq!(receive1_stored.height(), 4);
    assert!(receive1_stored.is_receive());

    // Middle block pruning
    assert!(any.block_exists(&send2.hash()));
    ledger.confirm(send2.hash());
    assert_eq!(ledger.prune_one(&send2.hash(), 1), 1);

    any = ledger.any();
    assert_eq!(any.block_exists(&send2.hash()), false);
    assert!(any.block_exists_or_pruned(&send2.hash()));
}

#[test]
fn pruning_large_chain() {
    let ledger = Ledger::new_null();
    let inserter = LedgerInserter::new(&ledger);
    ledger.enable_pruning();
    let send_receive_pairs = 20;
    let genesis_account = ledger.genesis().account();
    let mut last_hash = ledger.genesis().hash();

    for _ in 0..send_receive_pairs {
        let send = inserter.genesis().send(genesis_account, 1);
        let receive = inserter.genesis().receive(send.hash());
        last_hash = receive.hash();
    }
    assert_eq!(ledger.block_count(), send_receive_pairs * 2 + 1);
    ledger.confirm(last_hash);

    // Pruning action
    assert_eq!(
        ledger.prune_one(&last_hash, 5),
        send_receive_pairs as usize * 2
    );

    let txn = ledger.store.tx_begin_read();
    assert!(ledger.store.pruned.exists(&txn, &last_hash));
    assert!(ledger.store.block.exists(&txn, &DEV_GENESIS_HASH));
    assert_eq!(ledger.store.block.exists(&txn, &last_hash), false);
    assert_eq!(ledger.store.pruned.count(&txn), ledger.pruned_count());
    assert_eq!(
        ledger.store.block.count(&txn),
        ledger.block_count() - ledger.pruned_count()
    );
    assert_eq!(ledger.store.pruned.count(&txn), send_receive_pairs * 2);
    assert_eq!(ledger.store.block.count(&txn), 1);
}

#[test]
fn pruning_source_rollback() {
    let ledger = Ledger::new_null();
    ledger.enable_pruning();
    let inserter = LedgerInserter::new(&ledger);
    inserter.genesis().epoch_v1();

    let send1 = inserter.genesis().send(ledger.genesis().account(), 100);
    let send2 = inserter.genesis().send(ledger.genesis().account(), 100);

    ledger.confirm(send2.hash());

    // Pruning action
    assert_eq!(ledger.prune_one(&send1.hash(), 1), 2);

    // Receiving pruned block
    let receive1: Block = StateBlockArgs {
        key: &DEV_GENESIS_KEY,
        previous: send2.hash(),
        representative: send2.representative_field().unwrap(),
        balance: Amount::MAX - Amount::raw(100),
        link: send1.hash().into(),
        work: WorkNonce::new(u64::MAX),
    }
    .into();
    ledger.process_one(&receive1).unwrap();

    // Rollback receive block
    ledger.roll_back(&receive1.hash()).unwrap();

    let any = ledger.any();
    let info2 = any
        .get_pending(&PendingKey::new(*DEV_GENESIS_ACCOUNT, send1.hash()))
        .unwrap();
    assert_ne!(info2.source, *DEV_GENESIS_ACCOUNT); // Tradeoff to not store pruned blocks accounts
    assert_eq!(info2.amount, Amount::raw(100));
    assert_eq!(info2.epoch, Epoch::Epoch1);

    // Process receive block again
    ledger.process_one(&receive1).unwrap();

    let any = ledger.any();
    assert_eq!(
        any.get_pending(&PendingKey::new(*DEV_GENESIS_ACCOUNT, send1.hash())),
        None
    );
    assert_eq!(ledger.pruned_count(), 2);
    assert_eq!(ledger.block_count(), 5);
}

#[test]
fn pruning_source_rollback_legacy() {
    let ledger = Ledger::new_null();
    ledger.enable_pruning();
    let inserter = LedgerInserter::new(&ledger);

    let send1 = inserter
        .genesis()
        .legacy_send(ledger.genesis().account(), 100);

    let destination = PrivateKey::from(42);
    let send2 = inserter.genesis().legacy_send(&destination, 100);

    let send3 = inserter
        .genesis()
        .legacy_send(ledger.genesis().account(), 100);

    ledger.confirm(send2.hash());

    // Pruning action
    assert_eq!(ledger.prune_one(&send2.hash(), 1), 2);

    // Receiving pruned block
    let receive1: Block = ReceiveBlockArgs {
        key: &DEV_GENESIS_KEY,
        previous: send3.hash(),
        source: send1.hash(),
        work: WorkNonce::new(u64::MAX),
    }
    .into();
    ledger.process_one(&receive1).unwrap();

    // Rollback receive block
    ledger.roll_back(&receive1.hash()).unwrap();

    let mut any = ledger.any();
    let info3 = any
        .get_pending(&PendingKey::new(*DEV_GENESIS_ACCOUNT, send1.hash()))
        .unwrap();
    assert_ne!(info3.source, *DEV_GENESIS_ACCOUNT); // Tradeoff to not store pruned blocks accounts
    assert_eq!(info3.amount, Amount::raw(100));
    assert_eq!(info3.epoch, Epoch::Epoch0);

    // Process receive block again
    ledger.process_one(&receive1).unwrap();

    any = ledger.any();
    assert_eq!(
        any.get_pending(&PendingKey::new(*DEV_GENESIS_ACCOUNT, send1.hash())),
        None
    );
    assert_eq!(ledger.pruned_count(), 2);
    assert_eq!(ledger.block_count(), 5);

    // Receiving pruned block (open)
    let open1: Block = OpenBlockArgs {
        key: &destination,
        source: send2.hash(),
        representative: destination.public_key(),
        work: WorkNonce::new(u64::MAX),
    }
    .into();
    ledger.process_one(&open1).unwrap();

    // Rollback open block
    ledger.roll_back(&open1.hash()).unwrap();

    any = ledger.any();
    let info4 = any
        .get_pending(&PendingKey::new(destination.account(), send2.hash()))
        .unwrap();
    assert_ne!(info4.source, *DEV_GENESIS_ACCOUNT); // Tradeoff to not store pruned blocks accounts
    assert_eq!(info4.amount, Amount::raw(100));
    assert_eq!(info4.epoch, Epoch::Epoch0);

    // Process open block again
    ledger.process_one(&open1).unwrap();

    any = ledger.any();
    assert_eq!(
        any.get_pending(&PendingKey::new(destination.account(), send2.hash())),
        None
    );
    assert_eq!(ledger.pruned_count(), 2);
    assert_eq!(ledger.block_count(), 6);
}

#[test]
fn pruning_legacy_blocks() {
    let ledger = Ledger::new_null();
    ledger.enable_pruning();
    let inserter = LedgerInserter::new(&ledger);
    let destination = PrivateKey::from(42);

    let send1 = inserter.genesis().legacy_send(*DEV_GENESIS_ACCOUNT, 1);
    let receive1 = inserter.genesis().legacy_receive(send1.hash());
    let change1 = inserter.genesis().legacy_change(&destination);
    let send2 = inserter.genesis().legacy_send(&destination, 1);
    let open1 = inserter.account(&destination).legacy_open(send2.hash());
    let send3 = inserter
        .account(&destination)
        .legacy_send(*DEV_GENESIS_ACCOUNT, 1);

    ledger.confirm(change1.hash());
    ledger.confirm(open1.hash());

    // Pruning action
    assert_eq!(ledger.prune_one(&change1.hash(), 2), 3);
    assert_eq!(ledger.prune_one(&open1.hash(), 1), 1);

    let txn = ledger.store.tx_begin_read();
    assert!(ledger.store.block.exists(&txn, &DEV_GENESIS_HASH));
    assert_eq!(ledger.store.block.exists(&txn, &send1.hash()), false);
    assert_eq!(ledger.store.pruned.exists(&txn, &send1.hash()), true);
    assert_eq!(ledger.store.block.exists(&txn, &receive1.hash()), false);
    assert_eq!(ledger.store.pruned.exists(&txn, &receive1.hash()), true);
    assert_eq!(ledger.store.block.exists(&txn, &change1.hash()), false);
    assert_eq!(ledger.store.pruned.exists(&txn, &change1.hash()), true);
    assert_eq!(ledger.store.block.exists(&txn, &send2.hash()), true);
    assert_eq!(ledger.store.block.exists(&txn, &open1.hash()), false);
    assert_eq!(ledger.store.pruned.exists(&txn, &open1.hash()), true);
    assert_eq!(ledger.store.block.exists(&txn, &send3.hash()), true);
    assert_eq!(ledger.pruned_count(), 4);
    assert_eq!(ledger.block_count(), 7);
    assert_eq!(ledger.store.pruned.count(&txn), 4);
    assert_eq!(ledger.store.block.count(&txn), 3);
}

#[test]
fn pruning_safe_functions() {
    let ledger = Ledger::new_null();
    ledger.enable_pruning();
    let inserter = LedgerInserter::new(&ledger);

    let send1 = inserter.genesis().send(*DEV_GENESIS_ACCOUNT, 1);
    let send2 = inserter.genesis().send(*DEV_GENESIS_ACCOUNT, 1);
    ledger.confirm(send1.hash());

    // Pruning action
    assert_eq!(ledger.prune_one(&send1.hash(), 1), 1);
    let any = ledger.any();

    // Safe ledger actions
    assert!(any.block_balance(&send1.hash()).is_none());
    assert_eq!(
        any.block_balance(&send2.hash()).unwrap(),
        send2.balance_field().unwrap()
    );

    assert_eq!(any.block_amount(&send2.hash()), None);
    assert_eq!(any.block_account(&send1.hash()), None);
    assert_eq!(any.block_account(&send2.hash()), Some(*DEV_GENESIS_ACCOUNT));
}

#[test]
fn hash_root_random() {
    let ledger = Ledger::new_null();
    let inserter = LedgerInserter::new(&ledger);
    ledger.enable_pruning();

    let send1 = inserter.genesis().send(*DEV_GENESIS_ACCOUNT, 1);
    let send2 = inserter.genesis().send(*DEV_GENESIS_ACCOUNT, 1);

    ledger.confirm(send1.hash());

    // Pruning action
    assert_eq!(ledger.prune_one(&send1.hash(), 1), 1);
    let any = ledger.any();

    // Prunned block will not be included in the random selection because it's not in the blocks set
    {
        let mut done = false;
        let mut iteration = 0;
        while !done && iteration < 16 {
            iteration += 1;
            let blocks = any.random_blocks(10);
            // Random blocks should repeat if the ledger is smaller than the requested count
            assert_eq!(blocks.len(), 10);
            let first = &blocks[0];
            done = first.hash() == send1.hash();
        }
        assert_eq!(done, false);
    }

    // Genesis and send2 should be included in the random selection
    {
        let mut done = false;
        let mut iteration = 0;
        while !done {
            iteration += 1;
            let blocks = any.random_blocks(1);
            assert_eq!(blocks.len(), 1);
            let first = &blocks[0];
            done = first.hash() == send2.hash();
            assert!(iteration < 1000);
        }
    }
    {
        let mut done = false;
        let mut iteration = 0;
        while !done {
            iteration += 1;
            let blocks = any.random_blocks(1);
            let first = &blocks[0];
            done = first.hash() == *DEV_GENESIS_HASH;
            assert!(iteration < 1000);
        }
    }
}
