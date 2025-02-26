use super::VoteProcessorQueue;
use crate::{
    stats::{DetailType, StatType, Stats},
    transport::MessageFlooder,
};
use rsnano_core::{Vote, VoteSource};
use rsnano_messages::{ConfirmAck, Message};
use rsnano_network::TrafficType;
use std::{
    ops::Deref,
    sync::{Arc, Mutex},
};

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

        self.vote_processor_queue.vote(vote, None, VoteSource::Live);

        let count = self
            .message_flooder
            .lock()
            .unwrap()
            .flood_prs_and_some_non_prs(&ack, TrafficType::Vote, 2.0);

        self.stats.add(
            StatType::VoteGenerator,
            DetailType::SentPr,
            count.principal_reps as u64,
        );
        self.stats.add(
            StatType::VoteGenerator,
            DetailType::SentNonPr,
            count.non_principal_reps as u64,
        );
    }
}
