use std::{collections::HashMap, sync::Arc};

use rsnano_core::{BlockHash, Root};
use rsnano_messages::{ConfirmReq, Message};
use rsnano_network::{Channel, ChannelId, TrafficType};

use super::election::Election;
use crate::{representatives::PeeredRepInfo, transport::MessageFlooder};

/// This struct accepts elections that need further votes before they can be confirmed and bundles them in to confirm_req packets
pub struct ConfirmationSolicitor {
    /// Maximum amount of requests to be sent per election, bypassed if an existing vote is for a different hash
    max_election_requests: usize,
    representatives: Vec<PeeredRepInfo>,
    requests: HashMap<ChannelId, (Arc<Channel>, Vec<(BlockHash, Root)>)>,
    prepared: bool,
    message_flooder: MessageFlooder,
}

impl ConfirmationSolicitor {
    pub fn new(message_flooder: MessageFlooder) -> Self {
        Self {
            max_election_requests: 50,
            prepared: false,
            representatives: Vec::new(),
            requests: HashMap::new(),
            message_flooder,
        }
    }

    /// Prepare object for batching election confirmation requests
    pub fn prepare(&mut self, representatives: &[PeeredRepInfo]) {
        debug_assert!(!self.prepared);
        self.requests.clear();
        self.representatives = representatives.to_vec();
        self.prepared = true;
    }

    /// Add an election that needs to be confirmed. Returns true if successfully added
    pub fn add(&mut self, election: &Election) -> bool {
        debug_assert!(self.prepared);
        let mut added = false;
        let mut rep_request_count = 0;
        let winner = election.winner();
        let mut to_remove = Vec::new();
        for rep in &self.representatives {
            if rep_request_count >= self.max_election_requests {
                break;
            }
            let mut full_queue = false;
            let existing_vote = election.votes().get(&rep.rep_key);
            let is_final = if let Some(vote) = existing_vote {
                !election.has_quorum() || vote.is_final_vote()
            } else {
                false
            };
            let different_hash = if let Some(existing) = existing_vote {
                existing.hash != winner.hash()
            } else {
                false
            };
            if existing_vote.is_none() || !is_final || different_hash {
                let should_drop = rep.channel.should_drop(TrafficType::ConfirmationRequests);

                if !should_drop {
                    let rep_channel = rep.channel.clone();
                    let (_, request_queue) = self
                        .requests
                        .entry(rep_channel.channel_id())
                        .or_insert_with(|| (rep_channel, Vec::new()));

                    request_queue.push((winner.hash(), winner.root()));

                    if !different_hash {
                        rep_request_count += 1;
                    }
                    added = true;
                } else {
                    full_queue = true;
                }
            }
            if full_queue {
                to_remove.push(rep.rep_key);
            }
        }

        if !to_remove.is_empty() {
            self.representatives
                .retain(|i| !to_remove.contains(&i.rep_key));
        }

        added
    }

    /// Dispatch bundled requests to each channel
    pub fn flush(&mut self) {
        debug_assert!(self.prepared);
        for (channel, requests) in self.requests.values() {
            let mut roots_hashes = Vec::new();
            for root_hash in requests {
                roots_hashes.push(root_hash.clone());
                if roots_hashes.len() == ConfirmReq::HASHES_MAX {
                    let req = Message::ConfirmReq(ConfirmReq::new(roots_hashes));
                    self.message_flooder.try_send(
                        &channel,
                        &req,
                        TrafficType::ConfirmationRequests,
                    );
                    roots_hashes = Vec::new();
                }
            }
            if !roots_hashes.is_empty() {
                let req = Message::ConfirmReq(ConfirmReq::new(roots_hashes));
                self.message_flooder
                    .try_send(channel, &req, TrafficType::ConfirmationRequests);
            }
        }
        self.prepared = false;
    }
}
