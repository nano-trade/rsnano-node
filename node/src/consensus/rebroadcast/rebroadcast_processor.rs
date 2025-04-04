use std::{
    sync::{
        atomic::{AtomicUsize, Ordering::Relaxed},
        Arc,
    },
    time::Duration,
};

use rsnano_core::Vote;
use rsnano_messages::{ConfirmAck, Message};
use rsnano_network::TrafficType;
use rsnano_stats::{StatsCollection, StatsSource};

use super::history::{RebroadcastError, RebroadcastHistory};
use crate::transport::MessageFlooder;
use rsnano_ledger::RepWeightCache;
use rsnano_nullable_clock::{SteadyClock, Timestamp};
use strum::{EnumCount, IntoEnumIterator};

/// Rebroadcasts a given vote if necessary
pub(super) struct RebroadcastProcessor {
    message_flooder: MessageFlooder,
    history: RebroadcastHistory,
    rep_weights: Arc<RepWeightCache>,
    clock: Arc<SteadyClock>,
    stats: Arc<RebroadcastStats>,
    last_rep_weight_update: Timestamp,
}

impl RebroadcastProcessor {
    pub(super) fn new(
        message_flooder: MessageFlooder,
        rep_weights: Arc<RepWeightCache>,
        clock: Arc<SteadyClock>,
        stats: Arc<RebroadcastStats>,
    ) -> Self {
        Self {
            message_flooder,
            history: RebroadcastHistory::default(),
            rep_weights,
            last_rep_weight_update: clock.now(),
            clock,
            stats,
        }
    }

    pub fn rebroadcast(&mut self, vote: &Vote) -> bool {
        self.stats.processed.fetch_add(1, Relaxed);

        let now = self.clock.now();

        let voter_weight = {
            let weights = self.rep_weights.read();

            if self.last_rep_weight_update.elapsed(now) > Duration::from_secs(60) {
                self.history.update_weights(&weights);
            }

            weights.get(&vote.voter).cloned().unwrap_or_default()
        };

        const NETWORK_FANOUT_SCALE: f32 = 1.0;

        // Wait for spare capacity if our network traffic is too high
        if !self
            .message_flooder
            .check_capacity(TrafficType::VoteRebroadcast, NETWORK_FANOUT_SCALE)
        {
            self.stats.cooldown.fetch_add(1, Relaxed);
            return false;
        }

        match self.history.check_and_record(vote, voter_weight, now) {
            Ok(()) => {
                self.update_stats(vote);
                let message = self.create_ack_message(vote);

                let sent = self.message_flooder.flood(
                    &message,
                    TrafficType::VoteRebroadcast,
                    NETWORK_FANOUT_SCALE,
                );

                self.stats.sent.fetch_add(sent, Relaxed);
            }
            Err(err) => {
                self.stats.errors[err as usize].fetch_add(1, Relaxed);
            }
        }

        true
    }

    fn create_ack_message(&self, vote: &Vote) -> Message {
        Message::ConfirmAck(ConfirmAck::new_with_rebroadcasted_vote(vote.clone()))
    }

    fn update_stats(&self, vote: &Vote) {
        self.stats.rebroadcast.fetch_add(1, Relaxed);

        self.stats
            .rebroadcast_hashes
            .fetch_add(vote.hashes.len(), Relaxed);
    }
}

#[derive(Default)]
pub(crate) struct RebroadcastStats {
    processed: AtomicUsize,
    sent: AtomicUsize,
    rebroadcast: AtomicUsize,
    rebroadcast_hashes: AtomicUsize,
    errors: [AtomicUsize; RebroadcastError::COUNT],
    cooldown: AtomicUsize,
}

impl RebroadcastStats {
    const KEY: &str = "vote_rebroadcaster";
}

impl StatsSource for RebroadcastStats {
    fn collect_stats(&self, result: &mut StatsCollection) {
        result.insert(Self::KEY, "process", self.processed.load(Relaxed));
        result.insert(Self::KEY, "sent", self.sent.load(Relaxed));
        result.insert(Self::KEY, "rebroadcast", self.rebroadcast.load(Relaxed));
        result.insert(
            Self::KEY,
            "rebroadcast_hashes",
            self.rebroadcast_hashes.load(Relaxed),
        );
        result.insert(Self::KEY, "cooldown", self.cooldown.load(Relaxed));

        for err in RebroadcastError::iter() {
            result.insert(
                Self::KEY,
                err.as_str(),
                self.errors[err as usize].load(Relaxed),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::FloodEvent;
    use rsnano_core::Vote;

    #[test]
    fn rebroadcast_vote() {
        let vote = Vote::new_test_instance();

        let floods = run_processor(TestInput { vote: vote.clone() });

        assert_eq!(
            floods,
            vec![FloodEvent {
                message: Message::ConfirmAck(ConfirmAck::new_with_rebroadcasted_vote(vote)),
                traffic_type: TrafficType::VoteRebroadcast,
                scale: 1.0
            }]
        )
    }

    fn run_processor(input: TestInput) -> Vec<FloodEvent> {
        let message_flooder = MessageFlooder::new_null();
        let flood_tracker = message_flooder.track_floods();
        let rep_weights = Arc::new(RepWeightCache::new());
        let clock = Arc::new(SteadyClock::new_null());
        let stats = Arc::new(RebroadcastStats::default());
        let mut processor = RebroadcastProcessor::new(message_flooder, rep_weights, clock, stats);

        processor.rebroadcast(&input.vote);

        flood_tracker.output()
    }

    struct TestInput {
        pub vote: Vote,
    }
}
