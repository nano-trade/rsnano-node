use std::sync::{
    mpsc::{Receiver, SyncSender},
    Arc, Mutex,
};

use rsnano_core::{utils::MemoryStream, VoteSource};
use rsnano_messages::NetworkFilter;
use rsnano_network::ChannelId;

use crate::{
    block_processing::{BlockProcessor, BlockSource},
    cementation::ConfirmingSet,
    consensus::{
        election_schedulers::ElectionSchedulers, AecEvent, BlockVoter, BootstrapElectionActivator,
        VoteCache, VoteCacheProcessor, VoteRebroadcastQueue,
    },
    recently_cemented_inserter::RecentlyCementedInserter,
    NodeEvent,
};

/// Processes events from the active election container
pub(crate) struct AecEventProcessor {
    pub(crate) receiver: Receiver<AecEvent>,
    pub(crate) vote_cache_processor: Arc<VoteCacheProcessor>,
    pub(crate) vote_cache: Arc<Mutex<VoteCache>>,
    pub(crate) node_event_sender: Option<SyncSender<NodeEvent>>,
    pub(crate) election_schedulers: Arc<ElectionSchedulers>,
    pub(crate) network_filter: Arc<NetworkFilter>,
    pub(crate) bootstrap_election_activator: BootstrapElectionActivator,
    pub(crate) block_voter: Arc<BlockVoter>,
    pub(crate) recently_cemented_inserter: RecentlyCementedInserter,
    pub(crate) vote_rebroadcast_queue: Arc<VoteRebroadcastQueue>,
    pub(crate) block_processor: Arc<BlockProcessor>,
    pub(crate) confirming_set: Arc<ConfirmingSet>,
}

impl AecEventProcessor {
    pub(crate) fn run(&mut self) {
        while let Ok(event) = self.receiver.recv() {
            match event {
                AecEvent::ElectionStarted(hash) => {
                    self.bootstrap_election_activator.election_started(hash);
                    self.block_voter.try_vote(&hash);
                    self.vote_cache_processor.trigger(hash);
                    if let Some(tx) = &self.node_event_sender {
                        tx.send(NodeEvent::ElectionStarted(hash)).unwrap();
                    }
                }
                AecEvent::ElectionStopped(hash) => {
                    if let Some(tx) = &self.node_event_sender {
                        tx.send(NodeEvent::ElectionStopped(hash)).unwrap();
                    }
                }
                AecEvent::ElectionConfirmed(election) => {
                    // In some edge cases block might get rolled back while the election
                    // is confirming, reprocess it to ensure it's present in the ledger
                    self.block_processor.add(
                        election.winner.clone().into(),
                        BlockSource::Election,
                        ChannelId::LOOPBACK,
                    );

                    self.confirming_set.add(election.clone());
                }
                AecEvent::BlockAddedToElection(hash) => self.vote_cache_processor.trigger(hash),
                AecEvent::BlockDiscarded(block) => {
                    let mut buf = MemoryStream::new();
                    block.serialize_without_block_type(&mut buf);
                    self.network_filter.clear_bytes(buf.as_bytes());
                }
                AecEvent::VoteProcessed(vote, voter_weight, source, results) => {
                    // Cache the votes that didn't match any election
                    if source != VoteSource::Cache {
                        self.vote_cache
                            .lock()
                            .unwrap()
                            .insert(&vote, voter_weight, &results);
                    }

                    self.vote_rebroadcast_queue
                        .handle_processed_vote(&vote, &results);
                }
                AecEvent::BlockConfirmed(block, election) => {
                    if let Some(tx) = &self.node_event_sender {
                        tx.send(NodeEvent::BlockConfirmed(block, election.clone()))
                            .unwrap();
                    }
                    self.recently_cemented_inserter.insert(election);
                }
                AecEvent::VacancyUpdated => self.election_schedulers.notify(),
            }
        }
    }
}
