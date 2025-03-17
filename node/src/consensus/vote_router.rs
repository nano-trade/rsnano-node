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

    pub fn active(&self, hash: &BlockHash) -> bool {
        if let Some(existing) = self.elections.get(hash) {
            existing.strong_count() > 0
        } else {
            false
        }
    }

    pub fn notify_vote_processed(
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
