use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex, MutexGuard, RwLock, Weak},
    time::{Duration, SystemTime},
};

use tracing::trace;

use rsnano_core::{Amount, BlockHash, MaybeSavedBlock, PublicKey, VoteCode, VoteSource};
use rsnano_ledger::Ledger;
use rsnano_network::ChannelId;
use rsnano_stats::{DetailType, StatType, Stats};

use super::{
    election_schedulers::ElectionSchedulers, Election, ElectionData, LocalVoteHistory,
    RecentlyConfirmedCache, TallyKey, VoteGenerators,
};
use crate::{
    block_processing::{BlockProcessor, BlockSource},
    cementation::ConfirmingSet,
    config::{NetworkParams, NodeConfig},
    consensus::{ElectionState, VoteInfo},
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
    node_config: NodeConfig,
    history: Arc<LocalVoteHistory>,
    wallets: Arc<Wallets>,
    recently_confirmed: Arc<RecentlyConfirmedCache>,
    confirming_set: Arc<ConfirmingSet>,
    election_schedulers: RwLock<Option<Weak<ElectionSchedulers>>>,
}

impl VoteApplier {
    pub(crate) fn new(
        ledger: Arc<Ledger>,
        network_params: NetworkParams,
        online_reps: Arc<Mutex<OnlineReps>>,
        stats: Arc<Stats>,
        vote_generators: Arc<VoteGenerators>,
        block_processor: Arc<BlockProcessor>,
        node_config: NodeConfig,
        history: Arc<LocalVoteHistory>,
        wallets: Arc<Wallets>,
        recently_confirmed: Arc<RecentlyConfirmedCache>,
        confirming_set: Arc<ConfirmingSet>,
    ) -> Self {
        Self {
            ledger,
            network_params,
            online_reps,
            stats,
            vote_generators,
            block_processor,
            node_config,
            history,
            wallets,
            recently_confirmed,
            confirming_set,
            election_schedulers: RwLock::new(None),
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

    pub fn tally_impl(&self, guard: &mut ElectionData) -> BTreeMap<TallyKey, MaybeSavedBlock> {
        let mut block_weights: HashMap<BlockHash, Amount> = HashMap::new();
        let mut final_weights: HashMap<BlockHash, Amount> = HashMap::new();
        for (account, info) in &guard.last_votes {
            let rep_weight = self.ledger.weight(account);
            *block_weights.entry(info.hash).or_default() += rep_weight;
            if info.timestamp == u64::MAX {
                *final_weights.entry(info.hash).or_default() += rep_weight;
            }
        }
        guard.last_tally.clear();
        for (&hash, &weight) in &block_weights {
            guard.last_tally.insert(hash, weight);
        }
        let mut result = BTreeMap::new();
        for (hash, weight) in &block_weights {
            if let Some(block) = guard.last_blocks.get(hash) {
                result.insert(TallyKey(*weight), block.clone());
            }
        }
        // Calculate final votes sum for winner
        if !final_weights.is_empty() && !result.is_empty() {
            let winner_hash = result.first_key_value().unwrap().1.hash();
            if let Some(final_weight) = final_weights.get(&winner_hash) {
                guard.final_weight = *final_weight;
            }
        }
        result
    }

    pub fn remove_votes(
        &self,
        election: &Election,
        guard: &mut MutexGuard<ElectionData>,
        hash: &BlockHash,
    ) {
        if self.node_config.enable_voting && self.wallets.voting_reps_count() > 0 {
            // Remove votes from election
            let root = election.lock().root;
            let list_generated_votes = self.history.votes(&root, hash, false);
            for vote in list_generated_votes {
                guard.last_votes.remove(&vote.voting_account);
            }
            // Clear votes cache
            self.history.erase(&root);
        }
    }

    pub fn have_quorum(&self, tally: &BTreeMap<TallyKey, MaybeSavedBlock>) -> bool {
        let mut it = tally.keys();
        let first = it.next().map(|i| i.amount()).unwrap_or_default();
        let second = it.next().map(|i| i.amount()).unwrap_or_default();
        let delta = self.online_reps.lock().unwrap().quorum_delta();
        first - second >= delta
    }
}

pub trait VoteApplierExt {
    fn vote(
        &self,
        election: &Arc<Election>,
        rep: &PublicKey,
        timestamp: u64,
        block_hash: &BlockHash,
        vote_source: VoteSource,
    ) -> VoteCode;
    fn confirm_if_quorum(&self, election_lock: MutexGuard<ElectionData>, election: &Arc<Election>);
    fn confirm_once(&self, election_lock: &mut ElectionData, election: &Arc<Election>);
}

impl VoteApplierExt for Arc<VoteApplier> {
    fn vote(
        &self,
        election: &Arc<Election>,
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

        let mut guard = election.lock();

        if let Some(last_vote) = guard.last_votes.get(rep) {
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
        guard
            .last_votes
            .insert(*rep, VoteInfo::new(timestamp, *block_hash));

        if vote_source != VoteSource::Cache {
            if let Some(callback) = &guard.live_vote_callback {
                callback(*rep);
            }
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

        if !guard.is_confirmed() {
            self.confirm_if_quorum(guard, election);
        }
        VoteCode::Vote
    }

    fn confirm_if_quorum(
        &self,
        mut election_lock: MutexGuard<ElectionData>,
        election: &Arc<Election>,
    ) {
        let tally = self.tally_impl(&mut election_lock);
        assert!(!tally.is_empty());
        let (amount, block) = tally.first_key_value().unwrap();
        let winner_hash = block.hash();
        election_lock.status.tally = amount.amount();
        election_lock.status.final_tally = election_lock.final_weight;
        let status_winner_hash = election_lock.status.winner.as_ref().unwrap().hash();
        let mut sum = Amount::zero();
        for k in tally.keys() {
            sum += k.amount();
        }
        if sum >= self.online_reps.lock().unwrap().quorum_delta()
            && winner_hash != status_winner_hash
        {
            election_lock.status.winner = Some(block.clone());
            self.remove_votes(election, &mut election_lock, &status_winner_hash);
            self.block_processor.force(block.clone().into());
        }

        if self.have_quorum(&tally) {
            if election_lock.swap_quorum_on()
                && self.node_config.enable_voting
                && self.wallets.voting_reps_count() > 0
            {
                election_lock.status.vote_broadcast_count += 1;
                self.vote_generators
                    .generate_final_vote(&election_lock.root, &status_winner_hash);
            }
            let quorum_delta = self.online_reps.lock().unwrap().quorum_delta();
            if election_lock.final_weight >= quorum_delta {
                // In some edge cases block might get rolled back while the election
                // is confirming, reprocess it to ensure it's present in the ledger
                self.block_processor.add(
                    (**block).clone(),
                    BlockSource::Election,
                    ChannelId::LOOPBACK,
                );
                self.confirm_once(&mut election_lock, election);
            }
        }
    }

    fn confirm_once(&self, election_lock: &mut ElectionData, election: &Arc<Election>) {
        let just_confirmed = election_lock.state != ElectionState::Confirmed;
        election_lock.state = ElectionState::Confirmed;

        if just_confirmed {
            election_lock.update_status_to_confirmed();
            let status = election_lock.status.clone();

            self.recently_confirmed.put(
                election_lock.qualified_root.clone(),
                status.winner.as_ref().unwrap().hash(),
            );

            self.stats.inc(StatType::Election, DetailType::ConfirmOnce);
            trace!(
                qualified_root = ?election_lock.qualified_root,
                "election confirmed"
            );

            self.confirming_set.add_with_election(
                status.winner.as_ref().unwrap().hash(),
                Some(election.clone()),
            );
        } else {
            self.stats
                .inc(StatType::Election, DetailType::ConfirmOnceFailed);
        }
    }
}
