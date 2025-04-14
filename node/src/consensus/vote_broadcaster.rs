use std::{
    ops::Deref,
    sync::{Arc, Mutex},
};

use rsnano_core::{Vote, VoteSource};
use rsnano_messages::{ConfirmAck, Message};
use rsnano_network::TrafficType;
use rsnano_stats::{DetailType, StatType, Stats};

use super::VoteProcessorQueue;
use crate::transport::MessageFlooder;

/// Broadcast a vote to PRs and some non-PRs
pub struct VoteBroadcaster {
    vote_processor_queue: Arc<VoteProcessorQueue>,
    message_flooder: Mutex<MessageFlooder>,
    stats: Arc<Stats>,
}

impl VoteBroadcaster {
    pub fn new(
        vote_processor_queue: Arc<VoteProcessorQueue>,
        message_flooder: MessageFlooder,
        stats: Arc<Stats>,
    ) -> Self {
        Self {
            vote_processor_queue,
            message_flooder: Mutex::new(message_flooder),
            stats,
        }
    }

    /// Broadcast vote to PRs and some non-PRs
    pub fn broadcast(&self, vote: Arc<Vote>) {
        let ack = Message::ConfirmAck(ConfirmAck::new_with_own_vote(vote.deref().clone()));

        let stat_type = if vote.is_final() {
            StatType::VoteGeneratorFinal
        } else {
            StatType::VoteGenerator
        };

        self.vote_processor_queue
            .enqueue(vote, None, VoteSource::Live, None);

        let count = self
            .message_flooder
            .lock()
            .unwrap()
            .flood_prs_and_some_non_prs(&ack, TrafficType::Vote, 2.0);

        self.stats
            .add(stat_type, DetailType::SentPr, count.principal_reps as u64);
        self.stats.add(
            stat_type,
            DetailType::SentNonPr,
            count.non_principal_reps as u64,
        );
    }
}
