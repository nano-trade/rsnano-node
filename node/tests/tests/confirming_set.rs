use std::time::Duration;

use rsnano_core::{Amount, PrivateKey};
use rsnano_ledger::{test_helpers::UnsavedBlockLatticeBuilder, LedgerSet, Writer};
use rsnano_stats::{DetailType, Direction, StatType};
use test_helpers::{
    assert_always_eq, assert_timely, assert_timely2, assert_timely_eq, assert_timely_eq2,
    start_election, System,
};

// The callback and confirmation history should only be updated after confirmation height is set (and not just after voting)
#[test]
fn confirmed_history() {
    let mut system = System::new();
    let mut config = System::default_config_without_backlog_scan();
    config.bootstrap.enable = false;
    let node = system.build_node().config(config).finish();

    let mut lattice = UnsavedBlockLatticeBuilder::new();
    let key1 = PrivateKey::new();
    let send = lattice.genesis().send(&key1, Amount::nano(1000));
    let send1 = lattice.genesis().send(&key1, Amount::nano(1000));

    node.process_multi(&[send.clone(), send1.clone()]);

    start_election(&node, &send1.hash());
    {
        // The write guard prevents the confirmation height processor doing any writes
        let _write_guard = node.ledger.wait(Writer::Testing);

        // Confirm send1
        node.force_confirm(&send1.hash());
        assert_timely_eq(
            Duration::from_secs(10),
            || node.active.read().unwrap().len(),
            0,
        );
        assert_eq!(node.recently_cemented.lock().unwrap().len(), 0);
        assert_eq!(node.active.read().unwrap().len(), 0);

        assert_eq!(node.ledger.confirmed().block_exists(&send.hash()), false);

        assert_timely(Duration::from_secs(10), || {
            node.ledger
                .store
                .env
                .write_queue
                .contains(Writer::ConfirmationHeight)
        });

        // Confirm that no inactive callbacks have been called when the
        // confirmation height processor has already iterated over it, waiting to write
        assert_always_eq(
            Duration::from_millis(50),
            || {
                node.stats.count(
                    StatType::ConfirmationObserver,
                    DetailType::InactiveConfHeight,
                    Direction::Out,
                )
            },
            0,
        );
    }

    assert_timely(Duration::from_secs(10), || {
        !node
            .ledger
            .store
            .env
            .write_queue
            .contains(Writer::ConfirmationHeight)
    });

    assert_timely2(|| node.ledger.confirmed().block_exists(&send.hash()));

    assert_timely_eq(
        Duration::from_secs(10),
        || node.active.read().unwrap().len(),
        0,
    );
    assert_timely_eq2(
        || node.stats().get("confirmation_observer", "active_quorum"),
        1,
    );

    // Each block that's confirmed is in the recently_cemented history
    assert_timely_eq2(|| node.recently_cemented.lock().unwrap().len(), 2);
    assert_eq!(node.active.read().unwrap().len(), 0);

    // Confirm the callback is not called under this circumstance
    assert_timely_eq2(
        || node.stats().get("confirmation_observer", "active_quorum"),
        1,
    );
    assert_timely_eq2(|| node.stats().get("confirmation_observer", "inactive"), 1);
    assert_timely_eq2(
        || {
            node.stats.count(
                StatType::ConfirmationHeight,
                DetailType::BlocksConfirmed,
                Direction::In,
            )
        },
        2,
    );
    assert_eq!(node.ledger.confirmed_count(), 3);
}

#[test]
fn dependent_election() {
    let mut system = System::new();
    let config = System::default_config_without_backlog_scan();
    let node = system.build_node().config(config).finish();

    let mut lattice = UnsavedBlockLatticeBuilder::new();
    let key1 = PrivateKey::new();
    let send = lattice.genesis().send(&key1, Amount::nano(1000));
    let send1 = lattice.genesis().send(&key1, Amount::nano(1000));
    let send2 = lattice.genesis().send(&key1, Amount::nano(1000));
    node.process_multi(&[send.clone(), send1.clone(), send2.clone()]);

    // This election should be confirmed as active_conf_height
    start_election(&node, &send1.hash());
    // Start an election and confirm it
    start_election(&node, &send2.hash());
    node.force_confirm(&send2.hash());

    // Wait for blocks to be confirmed in ledger, callbacks will happen after
    assert_timely_eq2(
        || {
            node.stats.count(
                StatType::ConfirmationHeight,
                DetailType::BlocksConfirmed,
                Direction::In,
            )
        },
        3,
    );
    // Once the item added to the confirming set no longer exists, callbacks have completed
    assert_timely2(|| !node.confirming_set.contains(&send2.hash()));

    assert_timely_eq2(
        || node.stats().get("confirmation_observer", "active_quorum"),
        1,
    );
    assert_timely_eq2(
        || {
            node.stats()
                .get("confirmation_observer", "active_confirmation_height")
        },
        1,
    );
    assert_timely_eq2(|| node.stats().get("confirmation_observer", "inactive"), 1);
    assert_eq!(node.ledger.confirmed_count(), 4);
}
