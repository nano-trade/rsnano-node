use std::sync::Arc;

use rsnano_core::Vote;
use rsnano_messages::{ConfirmAck, Message};
use rsnano_network::TrafficType;
use rsnano_stats::{DetailType, StatType, Stats};

use crate::transport::MessageFlooder;

/// Rebroadcasts a given vote if necessary
pub(super) struct RebroadcastProcessor {
    message_flooder: MessageFlooder,
    stats: Arc<Stats>,
}

impl RebroadcastProcessor {
    pub(super) fn new(message_flooder: MessageFlooder, stats: Arc<Stats>) -> Self {
        Self {
            message_flooder,
            stats,
        }
    }

    pub fn rebroadcast(&mut self, vote: &Vote) {
        self.update_stats(vote);
        let message = self.create_ack_message(vote);

        let sent = self
            .message_flooder
            .flood(&message, TrafficType::VoteRebroadcast, 0.5);

        self.stats
            .add(StatType::VoteRebroadcaster, DetailType::Sent, sent as u64);
    }

    fn create_ack_message(&self, vote: &Vote) -> Message {
        Message::ConfirmAck(ConfirmAck::new_with_rebroadcasted_vote(vote.clone()))
    }

    fn update_stats(&self, vote: &Vote) {
        self.stats
            .inc(StatType::VoteRebroadcaster, DetailType::Rebroadcast);

        self.stats.add(
            StatType::VoteRebroadcaster,
            DetailType::RebroadcastHashes,
            vote.hashes.len() as u64,
        );
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
                scale: 0.5
            }]
        )
    }

    fn run_processor(input: TestInput) -> Vec<FloodEvent> {
        let message_flooder = MessageFlooder::new_null();
        let flood_tracker = message_flooder.track_floods();
        let stats = Arc::new(Stats::default());
        let mut processor = RebroadcastProcessor::new(message_flooder, stats);

        processor.rebroadcast(&input.vote);

        flood_tracker.output()
    }

    struct TestInput {
        pub vote: Vote,
    }
}
