use rsnano_core::{Amount, Epoch, PendingKey, TestBlockBuilder};
use rsnano_store_lmdb::Writer;

use crate::{
    ledger_constants::LEDGER_CONSTANTS_STUB, ledger_tests::LedgerContext,
    test_helpers::upgrade_genesis_to_epoch_v1, AnySet, LedgerSet, DEV_GENESIS_HASH,
};

#[test]
fn pruning_action() {
    let ctx = LedgerContext::empty();
    ctx.ledger.enable_pruning();
    let genesis = ctx.genesis_block_factory();

    let send1 = genesis
        .send()
        .amount_sent(100)
        .link(genesis.account())
        .build();
    ctx.ledger.process_one(&send1).unwrap();

    let mut txn = ctx.ledger.rw_txn(Writer::Testing);
    ctx.ledger.confirm(&mut txn, send1.hash());
    txn.commit();

    let send2 = genesis
        .send()
        .amount_sent(100)
        .link(genesis.account())
        .build();
    ctx.ledger.process_one(&send2).unwrap();

    txn.renew();
    ctx.ledger.confirm(&mut txn, send2.hash());
    txn.commit();

    // Prune...
    assert_eq!(ctx.ledger.prune_one(&send1.hash(), 1), 1);
    assert_eq!(ctx.ledger.prune_one(&DEV_GENESIS_HASH, 1), 0);

    let mut any = ctx.ledger.any();
    assert!(any
        .get_pending(&PendingKey::new(genesis.account(), send1.hash()))
        .is_some());

    assert_eq!(any.block_exists(&send1.hash()), false);
    assert!(any.block_exists_or_pruned(&send1.hash()),);
    assert!(any.block_exists(&DEV_GENESIS_HASH));
    assert!(any.block_exists(&send2.hash()));

    // Receiving pruned block
    let receive1 = TestBlockBuilder::state()
        .account(genesis.account())
        .previous(send2.hash())
        .balance(LEDGER_CONSTANTS_STUB.genesis_amount - Amount::raw(100))
        .link(send1.hash())
        .key(&genesis.key)
        .build();
    ctx.ledger.process_one(&receive1).unwrap();

    any = ctx.ledger.any();
    assert!(any.block_exists(&receive1.hash()));
    assert_eq!(
        any.get_pending(&PendingKey::new(genesis.account(), send1.hash())),
        None
    );
    let receive1_stored = any.get_block(&receive1.hash()).unwrap();
    assert_eq!(&receive1, &*receive1_stored);
    assert_eq!(receive1_stored.height(), 4);
    assert!(receive1_stored.is_receive());

    // Middle block pruning
    assert!(any.block_exists(&send2.hash()));
    txn.renew();
    ctx.ledger.confirm(&mut txn, send2.hash());
    txn.commit();
    assert_eq!(ctx.ledger.prune_one(&send2.hash(), 1), 1);

    any = ctx.ledger.any();
    assert_eq!(any.block_exists(&send2.hash()), false);
    assert!(any.block_exists_or_pruned(&send2.hash()));
}

#[test]
fn pruning_large_chain() {
    let ctx = LedgerContext::empty();
    ctx.ledger.enable_pruning();
    let genesis = ctx.genesis_block_factory();
    let send_receive_pairs = 20;
    let mut last_hash = *DEV_GENESIS_HASH;

    for _ in 0..send_receive_pairs {
        let send = genesis.send().link(genesis.account()).build();
        ctx.ledger.process_one(&send).unwrap();

        let receive = genesis.receive(send.hash()).build();
        ctx.ledger.process_one(&receive).unwrap();

        last_hash = receive.hash();
    }
    assert_eq!(ctx.ledger.block_count(), send_receive_pairs * 2 + 1);
    let mut txn = ctx.ledger.rw_txn(Writer::Testing);
    ctx.ledger.confirm(&mut txn, last_hash);
    txn.commit();

    // Pruning action
    assert_eq!(
        ctx.ledger.prune_one(&last_hash, 5),
        send_receive_pairs as usize * 2
    );

    txn.renew();
    assert!(ctx.ledger.store.pruned.exists(&txn, &last_hash));
    assert!(ctx.ledger.store.block.exists(&txn, &DEV_GENESIS_HASH));
    assert_eq!(ctx.ledger.store.block.exists(&txn, &last_hash), false);
    assert_eq!(
        ctx.ledger.store.pruned.count(&txn),
        ctx.ledger.pruned_count()
    );
    assert_eq!(
        ctx.ledger.store.block.count(&txn),
        ctx.ledger.block_count() - ctx.ledger.pruned_count()
    );
    assert_eq!(ctx.ledger.store.pruned.count(&txn), send_receive_pairs * 2);
    assert_eq!(ctx.ledger.store.block.count(&txn), 1);
}

#[test]
fn pruning_source_rollback() {
    let ctx = LedgerContext::empty();
    ctx.ledger.enable_pruning();
    let genesis = ctx.genesis_block_factory();

    upgrade_genesis_to_epoch_v1(&ctx);

    let send1 = genesis
        .send()
        .amount_sent(100)
        .link(genesis.account())
        .build();
    ctx.ledger.process_one(&send1).unwrap();

    let send2 = genesis
        .send()
        .amount_sent(100)
        .link(genesis.account())
        .build();
    ctx.ledger.process_one(&send2).unwrap();

    let mut txn = ctx.ledger.rw_txn(Writer::Testing);
    ctx.ledger.confirm(&mut txn, send2.hash());
    txn.commit();

    // Pruning action
    assert_eq!(ctx.ledger.prune_one(&send1.hash(), 1), 2);

    // Receiving pruned block
    let receive1 = TestBlockBuilder::state()
        .account(genesis.account())
        .previous(send2.hash())
        .balance(LEDGER_CONSTANTS_STUB.genesis_amount - Amount::raw(100))
        .link(send1.hash())
        .key(&genesis.key)
        .build();
    ctx.ledger.process_one(&receive1).unwrap();

    // Rollback receive block
    ctx.ledger.rollback(&receive1.hash()).unwrap();

    let any = ctx.ledger.any();
    let info2 = any
        .get_pending(&PendingKey::new(genesis.account(), send1.hash()))
        .unwrap();
    assert_ne!(info2.source, genesis.account()); // Tradeoff to not store pruned blocks accounts
    assert_eq!(info2.amount, Amount::raw(100));
    assert_eq!(info2.epoch, Epoch::Epoch1);

    // Process receive block again
    ctx.ledger.process_one(&receive1).unwrap();

    let any = ctx.ledger.any();
    assert_eq!(
        any.get_pending(&PendingKey::new(genesis.account(), send1.hash())),
        None
    );
    assert_eq!(ctx.ledger.pruned_count(), 2);
    assert_eq!(ctx.ledger.block_count(), 5);
}

#[test]
fn pruning_source_rollback_legacy() {
    let ctx = LedgerContext::empty();
    ctx.ledger.enable_pruning();
    let genesis = ctx.genesis_block_factory();

    let send1 = genesis
        .legacy_send()
        .destination(genesis.account())
        .amount(100)
        .build();
    ctx.ledger.process_one(&send1).unwrap();

    let destination = ctx.block_factory();
    let send2 = genesis
        .legacy_send()
        .destination(destination.account())
        .amount(100)
        .build();
    ctx.ledger.process_one(&send2).unwrap();

    let mut send3 = genesis
        .legacy_send()
        .destination(genesis.account())
        .amount(100)
        .build();
    ctx.ledger.process_one(&mut send3).unwrap();

    let mut txn = ctx.ledger.rw_txn(Writer::Testing);
    ctx.ledger.confirm(&mut txn, send2.hash());
    txn.commit();

    // Pruning action
    assert_eq!(ctx.ledger.prune_one(&send2.hash(), 1), 2);

    // Receiving pruned block
    let receive1 = TestBlockBuilder::legacy_receive()
        .previous(send3.hash())
        .source(send1.hash())
        .sign(&genesis.key)
        .build();
    ctx.ledger.process_one(&receive1).unwrap();

    // Rollback receive block
    ctx.ledger.rollback(&receive1.hash()).unwrap();

    let mut any = ctx.ledger.any();
    let info3 = any
        .get_pending(&PendingKey::new(genesis.account(), send1.hash()))
        .unwrap();
    assert_ne!(info3.source, genesis.account()); // Tradeoff to not store pruned blocks accounts
    assert_eq!(info3.amount, Amount::raw(100));
    assert_eq!(info3.epoch, Epoch::Epoch0);

    // Process receive block again
    ctx.ledger.process_one(&receive1).unwrap();

    any = ctx.ledger.any();
    assert_eq!(
        any.get_pending(&PendingKey::new(genesis.account(), send1.hash())),
        None
    );
    assert_eq!(ctx.ledger.pruned_count(), 2);
    assert_eq!(ctx.ledger.block_count(), 5);

    // Receiving pruned block (open)
    let open1 = TestBlockBuilder::legacy_open()
        .source(send2.hash())
        .sign(&destination.key)
        .build();
    ctx.ledger.process_one(&open1).unwrap();

    // Rollback open block
    ctx.ledger.rollback(&open1.hash()).unwrap();

    any = ctx.ledger.any();
    let info4 = any
        .get_pending(&PendingKey::new(destination.account(), send2.hash()))
        .unwrap();
    assert_ne!(info4.source, genesis.account()); // Tradeoff to not store pruned blocks accounts
    assert_eq!(info4.amount, Amount::raw(100));
    assert_eq!(info4.epoch, Epoch::Epoch0);

    // Process open block again
    ctx.ledger.process_one(&open1).unwrap();

    any = ctx.ledger.any();
    assert_eq!(
        any.get_pending(&PendingKey::new(destination.account(), send2.hash())),
        None
    );
    assert_eq!(ctx.ledger.pruned_count(), 2);
    assert_eq!(ctx.ledger.block_count(), 6);
}

#[test]
fn pruning_legacy_blocks() {
    let ctx = LedgerContext::empty();
    ctx.ledger.enable_pruning();
    let genesis = ctx.genesis_block_factory();
    let destination = ctx.block_factory();

    let send1 = genesis.legacy_send().destination(genesis.account()).build();
    ctx.ledger.process_one(&send1).unwrap();

    let receive1 = genesis.legacy_receive2(send1.hash()).build();
    ctx.ledger.process_one(&receive1).unwrap();

    let change1 = genesis
        .legacy_change()
        .representative(destination.public_key())
        .build();
    ctx.ledger.process_one(&change1).unwrap();

    let send2 = genesis
        .legacy_send()
        .destination(destination.account())
        .build();
    ctx.ledger.process_one(&send2).unwrap();

    let open1 = destination.legacy_open(send2.hash()).build();
    ctx.ledger.process_one(&open1).unwrap();

    let send3 = destination
        .legacy_send()
        .destination(genesis.account())
        .build();
    ctx.ledger.process_one(&send3).unwrap();

    let mut txn = ctx.ledger.rw_txn(Writer::Testing);
    ctx.ledger.confirm(&mut txn, change1.hash());
    ctx.ledger.confirm(&mut txn, open1.hash());
    txn.commit();

    // Pruning action
    assert_eq!(ctx.ledger.prune_one(&change1.hash(), 2), 3);
    assert_eq!(ctx.ledger.prune_one(&open1.hash(), 1), 1);

    txn.renew();
    assert!(ctx.ledger.store.block.exists(&txn, &DEV_GENESIS_HASH));
    assert_eq!(ctx.ledger.store.block.exists(&txn, &send1.hash()), false);
    assert_eq!(ctx.ledger.store.pruned.exists(&txn, &send1.hash()), true);
    assert_eq!(ctx.ledger.store.block.exists(&txn, &receive1.hash()), false);
    assert_eq!(ctx.ledger.store.pruned.exists(&txn, &receive1.hash()), true);
    assert_eq!(ctx.ledger.store.block.exists(&txn, &change1.hash()), false);
    assert_eq!(ctx.ledger.store.pruned.exists(&txn, &change1.hash()), true);
    assert_eq!(ctx.ledger.store.block.exists(&txn, &send2.hash()), true);
    assert_eq!(ctx.ledger.store.block.exists(&txn, &open1.hash()), false);
    assert_eq!(ctx.ledger.store.pruned.exists(&txn, &open1.hash()), true);
    assert_eq!(ctx.ledger.store.block.exists(&txn, &send3.hash()), true);
    assert_eq!(ctx.ledger.pruned_count(), 4);
    assert_eq!(ctx.ledger.block_count(), 7);
    assert_eq!(ctx.ledger.store.pruned.count(&txn), 4);
    assert_eq!(ctx.ledger.store.block.count(&txn), 3);
}

#[test]
fn pruning_safe_functions() {
    let ctx = LedgerContext::empty();
    ctx.ledger.enable_pruning();
    let genesis = ctx.genesis_block_factory();

    let send1 = genesis.send().link(genesis.account()).build();
    ctx.ledger.process_one(&send1).unwrap();

    let send2 = genesis.send().link(genesis.account()).build();
    ctx.ledger.process_one(&send2).unwrap();

    let mut txn = ctx.ledger.rw_txn(Writer::Testing);
    ctx.ledger.confirm(&mut txn, send1.hash());
    txn.commit();

    // Pruning action
    assert_eq!(ctx.ledger.prune_one(&send1.hash(), 1), 1);
    let any = ctx.ledger.any();

    // Safe ledger actions
    assert!(any.block_balance(&send1.hash()).is_none());
    assert_eq!(
        any.block_balance(&send2.hash()).unwrap(),
        send2.balance_field().unwrap()
    );

    assert_eq!(any.block_amount(&send2.hash()), None);
    assert_eq!(any.block_account(&send1.hash()), None);
    assert_eq!(any.block_account(&send2.hash()), Some(genesis.account()));
}

#[test]
fn hash_root_random() {
    let ctx = LedgerContext::empty();
    ctx.ledger.enable_pruning();
    let genesis = ctx.genesis_block_factory();

    let send1 = genesis.send().link(genesis.account()).build();
    ctx.ledger.process_one(&send1).unwrap();

    let send2 = genesis.send().link(genesis.account()).build();
    ctx.ledger.process_one(&send2).unwrap();

    let mut txn = ctx.ledger.rw_txn(Writer::Testing);
    ctx.ledger.confirm(&mut txn, send1.hash());
    txn.commit();

    // Pruning action
    assert_eq!(ctx.ledger.prune_one(&send1.hash(), 1), 1);
    let any = ctx.ledger.any();

    // Prunned block will not be included in the random selection because it's not in the blocks set
    {
        let mut done = false;
        let mut iteration = 0;
        while !done && iteration < 42 {
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
