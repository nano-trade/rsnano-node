use strum::EnumCount;

use rsnano_core::VoteSource;

pub(super) struct VoteCounter {
    votes: usize,
    by_source: [usize; VoteSource::COUNT],
}

impl VoteCounter {
    pub(super) fn new() -> Self {
        Self {
            votes: 0,
            by_source: [0; VoteSource::COUNT],
        }
    }

    pub fn votes(&self) -> usize {
        self.votes
    }

    pub fn votes_by(&self, source: VoteSource) -> usize {
        self.by_source[source as usize]
    }

    pub fn count(&mut self, source: VoteSource) {
        self.votes += 1;
        self.by_source[source as usize] += 1;
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
}
