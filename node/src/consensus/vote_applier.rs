use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, MutexGuard, RwLock, Weak},
    time::{Duration, SystemTime},
};

use rsnano_nullable_clock::SteadyClock;
use tracing::trace;

use rsnano_core::{
    Amount, BlockHash, DescTallyKey, MaybeSavedBlock, PublicKey, VoteCode, VoteSource,
};
use rsnano_ledger::{Election, Ledger, VoteInfo};
use rsnano_network::ChannelId;
use rsnano_stats::{DetailType, StatType, Stats};

use super::{
    election_schedulers::ElectionSchedulers, LocalVoteHistory, RecentlyConfirmedCache,
    VoteGenerators,
};
use crate::{
    block_processing::{BlockProcessor, BlockSource},
    cementation::ConfirmingSet,
    config::NetworkParams,
    representatives::OnlineReps,
    wallets::Wallets,
};

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
        }
    }

    pub(crate) fn set_election_schedulers(&self, schedulers: &Arc<ElectionSchedulers>) {
        *self.election_schedulers.write().unwrap() = Some(Arc::downgrade(&schedulers));
    }

    /// Calculates minimum time delay between subsequent votes when processing non-final votes
    pub fn cooldown_time(&self, weight: Amount) -> Duration {
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

    pub fn remove_votes(&self, election: &mut Election, hash: &BlockHash) {
        if self.wallets.voting_enabled() {
            // Remove votes from election
            let root = *election.root();
            let list_generated_votes = self.history.votes(&root, hash, false);
            for vote in list_generated_votes {
                election.remove_vote(&vote.voter);
            }
            // Clear votes cache
            self.history.erase(&root);
        }
    }

    pub fn have_quorum(&self, tally: &BTreeMap<DescTallyKey, MaybeSavedBlock>) -> bool {
        let mut it = tally.keys();
        let first = it.next().map(|i| i.amount()).unwrap_or_default();
        let second = it.next().map(|i| i.amount()).unwrap_or_default();
        let delta = self.online_reps.lock().unwrap().quorum_delta();
        first - second >= delta
    }

    pub fn have_quorum2(&self, tally: &BTreeMap<DescTallyKey, BlockHash>) -> bool {
        let mut it = tally.keys();
        let first = it.next().map(|i| i.amount()).unwrap_or_default();
        let second = it.next().map(|i| i.amount()).unwrap_or_default();
        let delta = self.online_reps.lock().unwrap().quorum_delta();
        first - second >= delta
    }

    pub fn confirm_if_quorum(
        &self,
        mut election: MutexGuard<Election>,
        election_mutex: &Arc<Mutex<Election>>,
    ) {
        let quorum_delta = self.online_reps.lock().unwrap().quorum_delta();

        election.calculate_tallies(&self.ledger.rep_weights);
        let (amount, winner_hash) = election.tallies().first_key_value().unwrap();
        let amount = amount.amount();
        let winner_hash = winner_hash.clone();
        let block = election
            .candidate_blocks()
            .get(&winner_hash)
            .unwrap()
            .clone();
        election.set_tally(amount);
        let final_weight = election.final_weight;
        election.set_final_tally(final_weight);

        let status_winner_hash = election.winner_hash();

        let mut sum_tallies = Amount::zero();
        for k in election.tallies().keys() {
            sum_tallies += k.amount();
        }

        if sum_tallies >= quorum_delta && winner_hash != status_winner_hash {
            election.set_winner(block.clone());
            self.remove_votes(&mut election, &status_winner_hash);
            self.block_processor.force(block.clone().into());
        }

        if self.have_quorum2(election.tallies()) {
            if election.swap_quorum_on() && self.wallets.voting_enabled() {
                self.vote_generators
                    .generate_final_vote(election.root(), &status_winner_hash);
                election.vote_broadcasted();
            }
            if election.final_weight >= quorum_delta {
                // In some edge cases block might get rolled back while the election
                // is confirming, reprocess it to ensure it's present in the ledger
                self.block_processor
                    .add(block.into(), BlockSource::Election, ChannelId::LOOPBACK);
                if election.update_status_to_confirmed() {
                    drop(election);
                    self.election_confirmed(election_mutex.clone());
                } else {
                    self.stats
                        .inc(StatType::Election, DetailType::ConfirmOnceFailed);
                }
            }
        }
    }

    pub fn election_confirmed(&self, election: Arc<Mutex<Election>>) {
        let (winner_hash, root) = {
            let e = election.lock().unwrap();
            (e.winner_hash(), e.qualified_root().clone())
        };

        self.recently_confirmed
            .write()
            .unwrap()
            .put(root.clone(), winner_hash);

        self.stats.inc(StatType::Election, DetailType::ConfirmOnce);
        trace!( qualified_root = ?root, "election confirmed");

        self.confirming_set.add_with_election(winner_hash, election);
    }

    pub fn vote(
        &self,
        election_mutex: &Arc<Mutex<Election>>,
        rep: &PublicKey,
        timestamp: u64,
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

            let max_vote = timestamp == u64::MAX && last_vote.timestamp < timestamp;

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
        election.add_vote(*rep, VoteInfo::new(timestamp, *block_hash));

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
            timestamp,
            ?vote_source,
            ?weight,
            "vote processed");

        if !election.is_confirmed() {
            self.confirm_if_quorum(election, election_mutex);
        }
        VoteCode::Vote
    }
}
