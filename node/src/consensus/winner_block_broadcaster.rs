use std::{cmp::max, collections::HashMap, sync::Arc, time::Duration};

use rsnano_core::{Block, BlockHash, Networks, PublicKey};
use rsnano_nullable_clock::{SteadyClock, Timestamp};
use rsnano_stats::{DetailType, StatType, Stats};

use super::{bounded_hash_map::BoundedHashMap, election::VoteSummary};
use crate::transport::MessageFlooder;
use rsnano_messages::{Message, Publish};
use rsnano_network::{bandwidth_limiter::RateLimiter, TrafficType};

/// Broadcasts the winner block of an election
pub(crate) struct WinnerBlockBroadcaster {
    stats: Arc<Stats>,
    clock: Arc<SteadyClock>,
    last_broadcasts: BoundedHashMap<BlockHash, Timestamp>,
    broadcast_interval: Duration,
    message_flooder: MessageFlooder,
    rebroadcast_limiter: RateLimiter,
}

impl WinnerBlockBroadcaster {
    pub(crate) fn new(
        stats: Arc<Stats>,
        clock: Arc<SteadyClock>,
        network: Networks,
        message_flooder: MessageFlooder,
    ) -> Self {
        Self {
            stats,
            clock,
            last_broadcasts: BoundedHashMap::new(1024 * 32),
            broadcast_interval: match network {
                Networks::NanoDevNetwork => Duration::from_millis(500),
                _ => Duration::from_secs(150),
            },
            message_flooder,
            // TODO: Make rate limit configurable
            rebroadcast_limiter: RateLimiter::with_burst_ratio(100, 2.0),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn new_null() -> Self {
        let stats = Arc::new(Stats::default());
        let clock = Arc::new(SteadyClock::new_null());
        let network = Networks::NanoLiveNetwork;
        Self::new(stats, clock, network, MessageFlooder::new_null())
    }

    pub fn try_broadcast_winner(
        &mut self,
        winner_block: &Block,
        votes: &HashMap<PublicKey, VoteSummary>,
    ) {
        let winner_hash = winner_block.hash();
        if self.should_broadcast(&winner_hash) {
            // Maximum amount of directed broadcasts to be sent per election
            let max_election_broadcasts = max(
                self.message_flooder.network.read().unwrap().fanout(1.0) / 2,
                1,
            );

            if self.rebroadcast_limiter.should_pass(1) {
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
                        self.message_flooder.try_send(
                            &i.channel,
                            &winner_msg,
                            TrafficType::BlockBroadcast,
                        );
                    }
                }

                // Random flood for block propagation
                // TODO: Avoid broadcasting to the same peers that were already broadcasted to
                self.message_flooder
                    .flood(&winner_msg, TrafficType::BlockBroadcast, 0.5);

                let is_initial = self
                    .last_broadcasts
                    .insert(winner_hash, self.clock.now())
                    .is_none();

                self.stats.inc(
                    StatType::Election,
                    if is_initial {
                        DetailType::BroadcastBlockInitial
                    } else {
                        DetailType::BroadcastBlockRepeat
                    },
                );
            }
        }
    }

    fn should_broadcast(&self, block_hash: &BlockHash) -> bool {
        // Broadcast the block if enough time has passed since the last broadcast (or it's the first broadcast)
        if let Some(last_broadcast) = self.last_broadcasts.get(block_hash) {
            last_broadcast.elapsed(self.clock.now()) >= self.broadcast_interval
        } else {
            true
        }
    }
}
