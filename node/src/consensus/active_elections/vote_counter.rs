use rsnano_stats::{Direction, StatsCollection, StatsSource};
use strum::{EnumCount, IntoEnumIterator};

use rsnano_core::VoteSource;

pub(super) struct VoteCounter {
    votes: u64,
    by_source: [u64; VoteSource::COUNT],
}

impl VoteCounter {
    pub(super) fn new() -> Self {
        Self {
            votes: 0,
            by_source: [0; VoteSource::COUNT],
        }
    }

    pub fn votes(&self) -> u64 {
        self.votes
    }

    pub fn votes_by(&self, source: VoteSource) -> u64 {
        self.by_source[source as usize]
    }

    pub fn count(&mut self, source: VoteSource) {
        self.votes += 1;
        self.by_source[source as usize] += 1;
    }
}

impl StatsSource for VoteCounter {
    fn collect_stats(&self, result: &mut StatsCollection) {
        result.insert("election", "vote", Direction::In, self.votes);
        for source in VoteSource::iter() {
            result.insert(
                "election_vote",
                source.as_str(),
                Direction::In,
                self.by_source[source as usize],
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nothing_counted() {
        let counter = VoteCounter::new();
        assert_eq!(counter.votes(), 0);
        assert_eq!(counter.votes_by(VoteSource::Live), 0);
        assert_eq!(counter.votes_by(VoteSource::Cache), 0);
        assert_eq!(counter.votes_by(VoteSource::Rebroadcast), 0);
    }

    #[test]
    fn count_one_vote() {
        let mut counter = VoteCounter::new();

        counter.count(VoteSource::Live);

        assert_eq!(counter.votes(), 1);
        assert_eq!(counter.votes_by(VoteSource::Live), 1);
        assert_eq!(counter.votes_by(VoteSource::Cache), 0);
        assert_eq!(counter.votes_by(VoteSource::Rebroadcast), 0);
    }

    #[test]
    fn count_multiple_votes() {
        let mut counter = VoteCounter::new();

        counter.count(VoteSource::Live);
        counter.count(VoteSource::Live);
        counter.count(VoteSource::Rebroadcast);

        assert_eq!(counter.votes(), 3);
        assert_eq!(counter.votes_by(VoteSource::Live), 2);
        assert_eq!(counter.votes_by(VoteSource::Cache), 0);
        assert_eq!(counter.votes_by(VoteSource::Rebroadcast), 1);
    }

    #[test]
    fn collect_stats() {
        let mut stats = StatsCollection::new();
        let mut counter = VoteCounter::new();
        counter.count(VoteSource::Live);
        counter.count(VoteSource::Live);
        counter.count(VoteSource::Rebroadcast);

        counter.collect_stats(&mut stats);

        assert_eq!(stats.get("election", "vote", Direction::In), 3);
        assert_eq!(stats.get("election_vote", "live", Direction::In), 2);
        assert_eq!(stats.get("election_vote", "rebroadcast", Direction::In), 1);
        assert_eq!(stats.get("election_vote", "cache", Direction::In), 0);
    }
}
