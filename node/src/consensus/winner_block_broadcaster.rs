use std::{cmp::max, collections::HashMap, sync::Arc, time::Duration};

use rsnano_core::{Block, BlockHash, Networks, PublicKey};
use rsnano_messages::{Message, Publish};
use rsnano_network::{bandwidth_limiter::RateLimiter, TrafficType};
use rsnano_nullable_clock::{SteadyClock, Timestamp};
use rsnano_stats::{StatsCollection, StatsSource};

use super::{bounded_hash_map::BoundedHashMap, election::VoteSummary};
use crate::transport::MessageFlooder;
use rsnano_output_tracker::{OutputListenerMt, OutputTrackerMt};

/// Broadcasts the winner block of an election
pub(crate) struct WinnerBlockBroadcaster {
    clock: Arc<SteadyClock>,
    broadcast_tracker: BroadcastTracker,
    message_flooder: MessageFlooder,
    rebroadcast_limiter: RateLimiter,
    broadcast_listener: OutputListenerMt<BlockHash>,
}

impl WinnerBlockBroadcaster {
    pub(crate) fn new(
        clock: Arc<SteadyClock>,
        network: Networks,
        message_flooder: MessageFlooder,
    ) -> Self {
        Self {
            clock,
            broadcast_tracker: BroadcastTracker::new(network),
            message_flooder,
            // TODO: Make rate limit configurable
            rebroadcast_limiter: RateLimiter::with_burst_ratio(100, 2.0),
            broadcast_listener: OutputListenerMt::default(),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn new_null() -> Self {
        let clock = Arc::new(SteadyClock::new_null());
        let network = Networks::NanoLiveNetwork;
        Self::new(clock, network, MessageFlooder::new_null())
    }

    #[allow(dead_code)]
    pub fn track(&self) -> Arc<OutputTrackerMt<BlockHash>> {
        self.broadcast_listener.track()
    }

    pub fn try_broadcast_winner(
        &mut self,
        winner_block: &Block,
        votes: &HashMap<PublicKey, VoteSummary>,
    ) {
        let now = self.clock.now();
        let winner_hash = winner_block.hash();
        self.broadcast_listener.emit(winner_hash);

        if !self.broadcast_tracker.should_broadcast(now, &winner_hash) {
            return;
        }

        // Maximum amount of directed broadcasts to be sent per election
        let max_election_broadcasts = max(
            self.message_flooder.network.read().unwrap().fanout(1.0) / 2,
            1,
        );

        if !self.rebroadcast_limiter.should_pass(1) {
            return;
        }

        let winner_msg = Message::Publish(Publish::new_forward(winner_block.clone()));

        let peered_prs = self
            .message_flooder
            .online_reps
            .lock()
            .unwrap()
            .peered_principal_reps();

        let mut count = 0;
        // Directed broadcasting to principal representatives
        for i in &peered_prs {
            if count >= max_election_broadcasts {
                break;
            }
            let should_broadcast = if let Some(existing) = votes.get(&i.rep_key) {
                // Don't rebroadcast to a PR if this PR has voted for the block!
                existing.hash != winner_hash
            } else {
                true
            };
            if should_broadcast {
                count += 1;
                self.message_flooder
                    .try_send(&i.channel, &winner_msg, TrafficType::BlockBroadcast);
            }
        }

        // Random flood for block propagation
        // TODO: Avoid broadcasting to the same peers that were already broadcasted to
        self.message_flooder
            .flood(&winner_msg, TrafficType::BlockBroadcast, 0.5);

        self.broadcast_tracker.insert(now, winner_hash);
    }
}

impl StatsSource for WinnerBlockBroadcaster {
    fn collect_stats(&self, result: &mut StatsCollection) {
        self.broadcast_tracker.collect_stats(result);
    }
}

struct BroadcastTracker {
    last_broadcasts: BoundedHashMap<BlockHash, Timestamp>,
    broadcast_interval: Duration,
    broadcast_initial: u64,
    broadcast_repeat: u64,
}

impl BroadcastTracker {
    pub fn new(network: Networks) -> Self {
        Self {
            last_broadcasts: BoundedHashMap::new(1024 * 32),
            broadcast_interval: match network {
                Networks::NanoDevNetwork => Duration::from_millis(500),
                _ => Duration::from_secs(150),
            },
            broadcast_initial: 0,
            broadcast_repeat: 0,
        }
    }

    pub fn insert(&mut self, now: Timestamp, hash: BlockHash) -> bool {
        let is_initial = self.last_broadcasts.insert(hash, now).is_none();

        if is_initial {
            self.broadcast_initial += 1;
        } else {
            self.broadcast_repeat += 1;
        }

        is_initial
    }

    pub fn should_broadcast(&self, now: Timestamp, block_hash: &BlockHash) -> bool {
        // Broadcast the block if enough time has passed since the last broadcast (or it's the first broadcast)
        if let Some(last_broadcast) = self.last_broadcasts.get(block_hash) {
            last_broadcast.elapsed(now) >= self.broadcast_interval
        } else {
            true
        }
    }
}

impl Default for BroadcastTracker {
    fn default() -> Self {
        Self::new(Networks::NanoLiveNetwork)
    }
}

impl StatsSource for BroadcastTracker {
    fn collect_stats(&self, result: &mut StatsCollection) {
        result.insert(
            "election",
            "broadcast_block_initial",
            self.broadcast_initial,
        );
        result.insert("election", "broadcast_block_repeat", self.broadcast_repeat);
    }
}
