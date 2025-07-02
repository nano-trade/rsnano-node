use std::{sync::Arc, time::Duration};

use rsnano_core::{utils::UnixMillisTimestamp, Account, Amount, Block, PublicKey};
use rsnano_ledger::{test_helpers::UnsavedBlockLatticeBuilder, DEV_GENESIS_PUB_KEY};
use rsnano_messages::ConfirmReq;
use rsnano_network::Channel;
use rsnano_node::{
    config::{NodeFlags, DEV_NETWORK_PARAMS},
    consensus::{
        election::{Election, ElectionBehavior},
        ConfirmationSolicitor,
    },
    representatives::PeeredRepInfo,
};
use rsnano_stats::{DetailType, Direction, StatType};
use test_helpers::System;

#[test]
#[ignore = "WIP"]
fn batches() {
    let mut system = System::new();
    let mut flags = NodeFlags::default();
    flags.disable_request_loop = true;
    flags.disable_rep_crawler = true;
    let node1 = system.build_node().flags(flags.clone()).finish();
    let node2 = system.build_node().flags(flags).finish();
    let channel1 = node2
        .network
        .read()
        .unwrap()
        .find_node_id(&node1.node_id.public_key().into())
        .unwrap()
        .clone();

    // Solicitor will only solicit from this representative
    let representative = PeeredRepInfo {
        rep_key: *DEV_GENESIS_PUB_KEY,
        channel: channel1,
        weight: Amount::nano(100_000),
    };
    let representatives = vec![representative];

    let mut solicitor = ConfirmationSolicitor::new(
        &DEV_NETWORK_PARAMS,
        &node2.network,
        node2.message_flooder.lock().unwrap().clone(),
    );
    solicitor.prepare(&representatives);

    let mut lattice = UnsavedBlockLatticeBuilder::new();
    let send = lattice.genesis().send(Account::from(123), 100);
    let send = node2.process(send);

    {
        for _ in 0..ConfirmReq::HASHES_MAX {
            let election = Election::new(
                send.clone(),
                ElectionBehavior::Priority,
                Duration::from_secs(1),
                node2.steady_clock.now(),
            );
            assert_eq!(solicitor.add(&election), true);
        }
        // Reached the maximum amount of requests for the channel
        let election = Election::new(
            send.clone(),
            ElectionBehavior::Priority,
            Duration::from_secs(1),
            node2.steady_clock.now(),
        );
        // Broadcasting should be immediate
        assert_eq!(
            0,
            node2
                .stats
                .count(StatType::Message, DetailType::Publish, Direction::Out)
        );
        //solicitor.broadcast_winner_block(&election).unwrap();
    }
    // One publish through directed broadcasting and another through random flooding
    assert_eq!(
        2,
        node2
            .stats
            .count(StatType::Message, DetailType::Publish, Direction::Out)
    );
    solicitor.flush();
    assert_eq!(
        1,
        node2
            .stats
            .count(StatType::Message, DetailType::ConfirmReq, Direction::Out)
    );
}

#[test]
#[ignore = "WIP"]
fn different_hashes() {
    let mut system = System::new();
    let mut flags = NodeFlags::default();
    flags.disable_request_loop = true;
    flags.disable_rep_crawler = true;
    let node1 = system.build_node().flags(flags.clone()).finish();
    let node2 = system.build_node().flags(flags).finish();
    let channel1 = node2
        .network
        .read()
        .unwrap()
        .find_node_id(&node1.node_id.public_key().into())
        .unwrap()
        .clone();
    // Solicitor will only solicit from this representative
    let representative = PeeredRepInfo {
        rep_key: *DEV_GENESIS_PUB_KEY,
        channel: channel1,
        weight: Amount::nano(100_000),
    };
    let representatives = vec![representative];

    let mut solicitor = ConfirmationSolicitor::new(
        &DEV_NETWORK_PARAMS,
        &node2.network,
        node2.message_flooder.lock().unwrap().clone(),
    );
    solicitor.prepare(&representatives);

    let mut lattice = UnsavedBlockLatticeBuilder::new();
    let send = lattice.genesis().send(Account::from(123), 100);
    let send = node2.process(send);

    let mut election = Election::new(
        send.clone(),
        ElectionBehavior::Priority,
        Duration::from_secs(1),
        node2.steady_clock.now(),
    );
    // Add a vote for something else, not the winner
    let another_block = Block::new_test_instance();
    election.try_add_fork(&another_block, Amount::nano(1));
    election.add_vote(
        *DEV_GENESIS_PUB_KEY,
        another_block.hash(),
        UnixMillisTimestamp::new(1),
        node2.steady_clock.now(),
    );
    // Ensure the request and broadcast goes through
    assert_eq!(solicitor.add(&election), true);
    //solicitor.broadcast_winner_block(&election).unwrap();
    // One publish through directed broadcasting and another through random flooding

    assert_eq!(
        2,
        node2
            .stats
            .count(StatType::Message, DetailType::Publish, Direction::Out)
    );
    solicitor.flush();
    assert_eq!(
        1,
        node2
            .stats
            .count(StatType::Message, DetailType::ConfirmReq, Direction::Out)
    );
}

#[test]
#[ignore = "WIP"]
fn bypass_max_requests_cap() {
    let mut system = System::new();
    let mut flags = NodeFlags::default();
    flags.disable_request_loop = true;
    flags.disable_rep_crawler = true;
    let _node1 = system.build_node().flags(flags.clone()).finish();
    let node2 = system.build_node().flags(flags).finish();

    let mut solicitor = ConfirmationSolicitor::new(
        &DEV_NETWORK_PARAMS,
        &node2.network,
        node2.message_flooder.lock().unwrap().clone(),
    );

    let mut representatives = Vec::new();
    const MAX_REPRESENTATIVES: usize = 50;
    for i in 0..=MAX_REPRESENTATIVES {
        // Make a temporary channel associated with node2
        let rep = PeeredRepInfo {
            rep_key: PublicKey::from(i as u64),
            channel: Arc::new(Channel::new_test_instance_with_id(i)),
            weight: Amount::nano(100_000),
        };
        representatives.push(rep);
    }
    assert_eq!(representatives.len(), MAX_REPRESENTATIVES + 1);
    solicitor.prepare(&representatives);

    let mut lattice = UnsavedBlockLatticeBuilder::new();
    let send = lattice.genesis().send(Account::from(123), 100);
    let send = node2.process(send);

    let mut election = Election::new(
        send.clone(),
        ElectionBehavior::Priority,
        Duration::from_secs(1),
        node2.steady_clock.now(),
    );
    // Add a vote for something else, not the winner
    let another_block = Block::new_test_instance();
    election.try_add_fork(&another_block, Amount::nano(1));
    for rep in &representatives {
        election.add_vote(
            rep.rep_key,
            another_block.hash(),
            UnixMillisTimestamp::new(1),
            node2.steady_clock.now(),
        );
    }
    // Ensure the request and broadcast goes through
    assert_eq!(solicitor.add(&election), true);
    //solicitor.broadcast_winner_block(&election).unwrap();
    // All requests went through, the last one would normally not go through due to the cap but a vote for a different hash does not count towards the cap
    // TODO port remainder of test!
}
