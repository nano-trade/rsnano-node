use std::{
    collections::HashMap,
    mem::size_of,
    sync::{Arc, Mutex},
};

use rsnano_core::{utils::ContainerInfo, BlockHash};

use super::Election;

/// This class routes votes to their associated election
pub(crate) struct VoteRouter {
    // Mapping of block hashes to elections.
    // Election already contains the associated block
    elections: HashMap<BlockHash, Arc<Mutex<Election>>>,
}

impl VoteRouter {
    pub fn new() -> Self {
        Self {
            elections: HashMap::new(),
        }
    }

    /// Add a route for 'hash' to 'election'
    /// Existing routes will be replaced
    /// Election must hold the block for the hash being passed in
    pub fn connect(&mut self, hash: BlockHash, election: Arc<Mutex<Election>>) {
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

    pub fn election(&self, hash: &BlockHash) -> Option<&Arc<Mutex<Election>>> {
        self.elections.get(hash)
    }

    pub fn is_active(&self, hash: &BlockHash) -> bool {
        self.elections.contains_key(hash)
    }

    pub fn container_info(&self) -> ContainerInfo {
        [(
            "elections",
            self.elections.len(),
            size_of::<BlockHash>() + size_of::<Arc<Mutex<Election>>>(),
        )]
        .into()
    }
}
