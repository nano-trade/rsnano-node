use std::{
    collections::HashMap,
    mem::size_of,
    sync::{mpsc::SyncSender, Arc, Mutex, RwLock, Weak},
};

use rsnano_core::{utils::ContainerInfo, BlockHash, Vote, VoteCode, VoteSource};

use super::{Election, RecentlyConfirmedCache, VoteApplier};

pub enum VoteRouterEvent {
    VoteProcessed(Arc<Vote>, VoteSource, HashMap<BlockHash, VoteCode>),
}

/// This class routes votes to their associated election
/// This class holds a weak_ptr as this container does not own the elections
/// Routing entries are removed periodically if the weak_ptr has expired
pub struct VoteRouter {
    // Mapping of block hashes to elections.
    // Election already contains the associated block
    elections: HashMap<BlockHash, Weak<Mutex<Election>>>,
    recently_confirmed: Arc<RwLock<RecentlyConfirmedCache>>,
    vote_applier: Arc<VoteApplier>,
    event_senders: Vec<SyncSender<VoteRouterEvent>>,
}

impl VoteRouter {
    pub fn new(
        recently_confirmed: Arc<RwLock<RecentlyConfirmedCache>>,
        vote_applier: Arc<VoteApplier>,
    ) -> Self {
        Self {
            elections: HashMap::new(),
            recently_confirmed,
            vote_applier,
            event_senders: Vec::new(),
        }
    }

    pub fn add_event_sink(&mut self, sink: SyncSender<VoteRouterEvent>) {
        self.event_senders.push(sink);
    }

    pub fn stop(&mut self) {
        self.event_senders.clear();
    }

    pub fn clean_up(&mut self) {
        self.elections
            .retain(|_, election| election.strong_count() > 0);
    }

    /// This is meant to be a fast check and may return false positives
    /// if weak pointers have expired, but we don't care about that here
    pub fn contains(&mut self, hash: &BlockHash) -> bool {
        self.elections.contains_key(hash)
    }

    /// Add a route for 'hash' to 'election'
    /// Existing routes will be replaced
    /// Election must hold the block for the hash being passed in
    pub fn connect(&mut self, hash: BlockHash, election: Weak<Mutex<Election>>) {
        self.elections.insert(hash, election);
    }

    /// Remove all routes to this election
    pub fn disconnect_election(&mut self, election: &Election) {
        for hash in election.candidate_blocks().keys() {
            self.elections.remove(hash);
        }
    }

    /// Remove all routes to this election
    pub fn disconnect(&mut self, hash: &BlockHash) {
        self.elections.remove(hash);
    }

    pub fn election(&self, hash: &BlockHash) -> Option<Arc<Mutex<Election>>> {
        self.elections.get(hash)?.upgrade()
    }

    /// Route vote to associated elections
    /// Distinguishes replay votes, cannot be determined if the block is not in any election
    /// If 'filter' parameter is non-zero, only elections for the specified hash are notified.
    /// This eliminates duplicate processing when triggering votes from the vote_cache as the result of a specific election being created.
    pub fn vote_filter(
        &self,
        vote: &Arc<Vote>,
        source: VoteSource,
        filter: &BlockHash,
    ) -> HashMap<BlockHash, VoteCode> {
        debug_assert!(vote.validate().is_ok());
        // If present, filter should be set to one of the hashes in the vote
        debug_assert!(filter.is_zero() || vote.hashes.iter().any(|h| h == filter));

        let mut results = HashMap::new();
        let mut process = HashMap::new();
        {
            let recently_confirmed = self.recently_confirmed.read().unwrap();
            for hash in &vote.hashes {
                // Ignore votes for other hashes if a filter is set
                if !filter.is_zero() && hash != filter {
                    continue;
                }

                // Ignore duplicate hashes (should not happen with a well-behaved voting node)
                if results.contains_key(hash) {
                    continue;
                }

                let election = self.elections.get(hash).and_then(|e| e.upgrade());
                if let Some(election) = election {
                    process.insert(*hash, election.clone());
                } else {
                    if !recently_confirmed.hash_exists(hash) {
                        results.insert(*hash, VoteCode::Indeterminate);
                    } else {
                        results.insert(*hash, VoteCode::Replay);
                    }
                }
            }
        }

        for (block_hash, election) in process {
            let vote_result = self.vote_applier.vote(
                &election,
                &vote.voter,
                vote.timestamp(),
                &block_hash,
                source,
            );
            results.insert(block_hash, vote_result);
        }

        self.notify_vote_processed(vote, source, &results);

        results
    }

    /// Route vote to associated elections
    /// Distinguishes replay votes, cannot be determined if the block is not in any election
    pub fn vote(&self, vote: &Arc<Vote>, source: VoteSource) -> HashMap<BlockHash, VoteCode> {
        self.vote_filter(vote, source, &BlockHash::zero())
    }

    pub fn active(&self, hash: &BlockHash) -> bool {
        if let Some(existing) = self.elections.get(hash) {
            existing.strong_count() > 0
        } else {
            false
        }
    }

    fn notify_vote_processed(
        &self,
        vote: &Arc<Vote>,
        source: VoteSource,
        results: &HashMap<BlockHash, VoteCode>,
    ) {
        for sender in &self.event_senders {
            sender
                .send(VoteRouterEvent::VoteProcessed(
                    vote.clone(),
                    source,
                    results.clone(),
                ))
                .unwrap();
        }
    }

    pub fn container_info(&self) -> ContainerInfo {
        [(
            "elections",
            self.elections.len(),
            size_of::<BlockHash>() + size_of::<Weak<Mutex<Election>>>(),
        )]
        .into()
    }
}
