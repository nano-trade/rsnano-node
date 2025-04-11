use super::{stopped_counter::StoppedCounter, vote_counter::VoteCounter};
use crate::consensus::election::{Election, ElectionBehavior};
use rsnano_core::VoteSource;
use rsnano_stats::{StatsCollection, StatsSource};
use strum::{EnumCount, IntoEnumIterator};

#[derive(Default)]
pub(crate) struct AecStats {
    vote_counter: VoteCounter,
    stopped_counter: StoppedCounter,
    pub ticked: u64,
    pub cooldown_count: u64,
    pub recover_count: u64,
    pub conflict_counter: u64,
    pub started_counter: u64,
    pub started_by_behavor: [u64; ElectionBehavior::COUNT],
}

impl AecStats {
    pub fn stopped(&mut self, election: &Election) {
        self.stopped_counter.stopped(election);
    }

    pub fn voted(&mut self, source: VoteSource) {
        self.vote_counter.count(source);
    }
}

impl StatsSource for AecStats {
    fn collect_stats(&self, result: &mut StatsCollection) {
        result.insert("active_elections", "loop", self.ticked);
        result.insert("active_elections", "cooldown", self.cooldown_count);
        result.insert("active_elections", "recovered", self.recover_count);
        result.insert("active_elections", "block_conflict", self.conflict_counter);
        result.insert("active_elections", "started", self.started_counter);
        for behavior in ElectionBehavior::iter() {
            result.insert(
                "active_elections_started",
                behavior.as_str(),
                self.started_by_behavor[behavior as usize],
            );
        }

        self.vote_counter.collect_stats(result);
        self.stopped_counter.collect_stats(result);
    }
}
