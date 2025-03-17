use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard, RwLock, Weak},
    time::{Duration, SystemTime},
};

use rsnano_nullable_clock::SteadyClock;
use tracing::trace;

use rsnano_core::{
    utils::UnixMillisTimestamp, Amount, BlockHash, PublicKey, Vote, VoteCode, VoteSource,
};
use rsnano_ledger::Ledger;
use rsnano_network::ChannelId;
use rsnano_stats::{DetailType, StatType, Stats};

use super::{
    election_schedulers::ElectionSchedulers, CementingElectionsCache, Election, LocalVoteHistory,
    RecentlyConfirmedCache, VoteGenerators, VoteRouter,
};
use crate::{
    block_processing::{BlockProcessor, BlockSource},
    cementation::ConfirmingSet,
    config::NetworkParams,
    representatives::OnlineReps,
    wallets::Wallets,
};

pub struct VoteApplier2 {
    pub vote_router: Arc<Mutex<VoteRouter>>,
    pub recently_confirmed: Arc<RwLock<RecentlyConfirmedCache>>,
    pub vote_applier: Arc<VoteApplier>,
}

impl VoteApplier2 {
    /// Route vote to associated elections
    /// Distinguishes replay votes, cannot be determined if the block is not in any election
    pub fn vote(&self, vote: &Arc<Vote>, source: VoteSource) -> HashMap<BlockHash, VoteCode> {
        self.vote_filter(vote, source, &BlockHash::zero())
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
        let guard = self.vote_router.lock().unwrap();
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

                let election = guard.election(hash);
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

        guard.notify_vote_processed(vote, source, &results);

        results
    }
}

/// Applies a vote to an election
pub struct VoteApplier {
    ledger: Arc<Ledger>,
    network_params: NetworkParams,
    online_reps: Arc<Mutex<OnlineReps>>,
    stats: Arc<Stats>,
    vote_generators: Arc<VoteGenerators>,
    block_processor: Arc<BlockProcessor>,
    history: Arc<LocalVoteHistory>,
    wallets: Arc<Wallets>,
    recently_confirmed: Arc<RwLock<RecentlyConfirmedCache>>,
    confirming_set: Arc<ConfirmingSet>,
    election_schedulers: RwLock<Option<Weak<ElectionSchedulers>>>,
    clock: Arc<SteadyClock>,
    cementing_elections_cache: Arc<Mutex<CementingElectionsCache>>,
}

impl VoteApplier {
    pub(crate) fn new(
        ledger: Arc<Ledger>,
        network_params: NetworkParams,
        online_reps: Arc<Mutex<OnlineReps>>,
        stats: Arc<Stats>,
        vote_generators: Arc<VoteGenerators>,
        block_processor: Arc<BlockProcessor>,
        history: Arc<LocalVoteHistory>,
        wallets: Arc<Wallets>,
        recently_confirmed: Arc<RwLock<RecentlyConfirmedCache>>,
        confirming_set: Arc<ConfirmingSet>,
        clock: Arc<SteadyClock>,
        cementing_elections_cache: Arc<Mutex<CementingElectionsCache>>,
    ) -> Self {
        Self {
            ledger,
            network_params,
            online_reps,
            stats,
            vote_generators,
            block_processor,
            history,
            wallets,
            recently_confirmed,
            confirming_set,
            election_schedulers: RwLock::new(None),
            clock,
            cementing_elections_cache,
        }
    }

    pub(crate) fn set_election_schedulers(&self, schedulers: &Arc<ElectionSchedulers>) {
        *self.election_schedulers.write().unwrap() = Some(Arc::downgrade(&schedulers));
    }

    pub fn vote(
        &self,
        election_mutex: &Arc<Mutex<Election>>,
        rep: &PublicKey,
        timestamp: UnixMillisTimestamp,
        block_hash: &BlockHash,
        vote_source: VoteSource,
    ) -> VoteCode {
        let weight = self.ledger.weight(rep);
        if !self.network_params.network.is_dev_network()
            && weight <= self.online_reps.lock().unwrap().minimum_principal_weight()
        {
            return VoteCode::Indeterminate;
        }

        let mut election = election_mutex.lock().unwrap();

        if let Some(last_vote) = election.votes().get(rep) {
            if last_vote.timestamp > timestamp {
                return VoteCode::Replay;
            }
            if last_vote.timestamp == timestamp && !(last_vote.hash < *block_hash) {
                return VoteCode::Replay;
            }

            let max_vote = timestamp == UnixMillisTimestamp::MAX && last_vote.timestamp < timestamp;

            let mut past_cooldown = true;
            // Only cooldown live votes
            if vote_source != VoteSource::Cache {
                let cooldown = self.cooldown_time(weight);
                past_cooldown = last_vote.time <= SystemTime::now() - cooldown;
            }

            if !max_vote && !past_cooldown {
                return VoteCode::Ignored;
            }
        }
        election.add_vote(*rep, timestamp, *block_hash);

        if vote_source != VoteSource::Cache {
            // Representative is defined as online if replying to live votes or rep_crawler queries
            self.online_reps
                .lock()
                .unwrap()
                .vote_observed(*rep, self.clock.now());
        }

        self.stats.inc(StatType::Election, DetailType::Vote);
        self.stats.inc(StatType::ElectionVote, vote_source.into());
        tracing::trace!(
            account = %rep,
            hash = %block_hash,
            %timestamp,
            ?vote_source,
            ?weight,
            "vote processed");

        self.confirm_if_quorum(election, election_mutex);
        VoteCode::Vote
    }

    /// Calculates minimum time delay between subsequent votes when processing non-final votes
    fn cooldown_time(&self, weight: Amount) -> Duration {
        let online_stake = { self.online_reps.lock().unwrap().trended_or_minimum_weight() };
        if weight > online_stake / 20 {
            // Reps with more than 5% weight
            Duration::from_secs(1)
        } else if weight > online_stake / 100 {
            // Reps with more than 1% weight
            Duration::from_secs(5)
        } else {
            // The rest of smaller reps
            Duration::from_secs(15)
        }
    }

    fn remove_votes(&self, election: &mut Election, hash: &BlockHash) {
        if self.wallets.voting_enabled() {
            // Remove votes from election
            let root = election.qualified_root().root;
            let list_generated_votes = self.history.votes(&root, hash, false);
            for vote in list_generated_votes {
                election.remove_vote(&vote.voter);
            }
            // Clear votes cache
            self.history.erase(&root);
        }
    }

    fn confirm_if_quorum(
        &self,
        mut election: MutexGuard<Election>,
        election_mutex: &Arc<Mutex<Election>>,
    ) {
        if election.is_confirmed() {
            return;
        }

        let quorum_delta = self.online_reps.lock().unwrap().quorum_delta();

        let old_winner = election.winner().hash();
        let old_final = election.is_final();

        election.update_tallies(&self.ledger.rep_weights.read(), quorum_delta);

        let winner_changed = election.winner().hash() != old_winner;
        if winner_changed {
            self.remove_votes(&mut election, &old_winner);
            // Roll back the previous winner and add the new winner to the ledger
            self.block_processor.force(election.winner().clone().into());
        }

        if election.is_final() {
            if !old_final && self.wallets.voting_enabled() {
                self.vote_generators.generate_final_vote(
                    &election.qualified_root().root,
                    &election.winner().hash(),
                );
                election.voted();
            }
            if election.is_confirmed() {
                // In some edge cases block might get rolled back while the election
                // is confirming, reprocess it to ensure it's present in the ledger
                self.block_processor.add(
                    election.winner().clone().into(),
                    BlockSource::Election,
                    ChannelId::LOOPBACK,
                );
                drop(election);
                self.election_confirmed(election_mutex.clone());
            }
        }
    }

    pub fn election_confirmed(&self, election: Arc<Mutex<Election>>) {
        let (winner_hash, root) = {
            let e = election.lock().unwrap();
            (e.winner().hash(), e.qualified_root().clone())
        };

        self.recently_confirmed
            .write()
            .unwrap()
            .put(root.clone(), winner_hash);

        self.stats.inc(StatType::Election, DetailType::ConfirmOnce);
        trace!( qualified_root = ?root, "election confirmed");

        self.cementing_elections_cache
            .lock()
            .unwrap()
            .insert(election);

        self.confirming_set.add(winner_hash);
    }
}
