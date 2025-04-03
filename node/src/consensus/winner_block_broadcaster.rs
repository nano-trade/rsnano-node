use std::{sync::Arc, time::Duration};

use rsnano_core::{BlockHash, Networks};
use rsnano_nullable_clock::{SteadyClock, Timestamp};
use rsnano_stats::{DetailType, StatType, Stats};

use super::{bounded_hash_map::BoundedHashMap, ConfirmationSolicitor, Election};

/// Broadcasts the winner block of an election
pub(crate) struct WinnerBlockBroadcaster {
    stats: Arc<Stats>,
    clock: Arc<SteadyClock>,
    last_broadcasts: BoundedHashMap<BlockHash, Timestamp>,
    broadcast_interval: Duration,
}

impl WinnerBlockBroadcaster {
    pub(crate) fn new(stats: Arc<Stats>, clock: Arc<SteadyClock>, network: Networks) -> Self {
        Self {
            stats,
            clock,
            last_broadcasts: BoundedHashMap::new(1024 * 32),
            broadcast_interval: match network {
                Networks::NanoDevNetwork => Duration::from_millis(500),
                _ => Duration::from_secs(150),
            },
        }
    }

    pub fn try_broadcast_winner(
        &mut self,
        solicitor: &mut ConfirmationSolicitor,
        election: &Election,
    ) {
        let winner_hash = election.winner().hash();
        if self.should_broadcast(&winner_hash) {
            if solicitor.broadcast_winner_block(election).is_ok() {
                let is_initial = self
                    .last_broadcasts
                    .insert(election.winner().hash(), self.clock.now())
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
