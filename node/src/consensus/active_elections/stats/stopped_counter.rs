use strum::{EnumCount, IntoEnumIterator};

use rsnano_stats::{StatsCollection, StatsSource};

use crate::consensus::election::{Election, ElectionBehavior, ElectionState};

#[derive(Default)]
pub(super) struct StoppedCounter {
    stopped: u64,
    confirmed_count: u64,
    unconfirmed: u64,
    by_state: [u64; ElectionState::COUNT],
    dropped: [u64; ElectionBehavior::COUNT],
    confirmed: [u64; ElectionBehavior::COUNT],
    timeout: [u64; ElectionBehavior::COUNT],
    cancelled: [u64; ElectionBehavior::COUNT],
}

impl StoppedCounter {
    pub(super) fn new() -> Self {
        Default::default()
    }

    pub(crate) fn stopped(&mut self, election: &Election) {
        self.stopped += 1;
        if election.is_confirmed() {
            self.confirmed_count += 1;
        } else {
            self.unconfirmed += 1;
        }
        self.by_state[election.state() as usize] += 1;
        match election.state() {
            ElectionState::Passive | ElectionState::Active => {
                self.dropped[election.behavior() as usize] += 1
            }
            ElectionState::Confirmed | ElectionState::ExpiredConfirmed => {
                self.confirmed[election.behavior() as usize] += 1
            }
            ElectionState::ExpiredUnconfirmed => self.timeout[election.behavior() as usize] += 1,
            ElectionState::Cancelled => self.cancelled[election.behavior() as usize] += 1,
        }
    }
}

impl StatsSource for StoppedCounter {
    fn collect_stats(&self, result: &mut StatsCollection) {
        result.insert("active_elections", "stopped", self.stopped);
        result.insert("active_elections", "confirmed", self.confirmed_count);
        result.insert("active_elections", "unconfirmed", self.unconfirmed);
        for state in ElectionState::iter() {
            result.insert(
                "active_elections_stopped",
                state.as_str(),
                self.by_state[state as usize],
            );
        }

        for behavior in ElectionBehavior::iter() {
            result.insert(
                "active_elections_dropped",
                behavior.as_str(),
                self.dropped[behavior as usize],
            );
            result.insert(
                "active_elections_confirmed",
                behavior.as_str(),
                self.confirmed[behavior as usize],
            );
            result.insert(
                "active_elections_timeout",
                behavior.as_str(),
                self.timeout[behavior as usize],
            );
            result.insert(
                "active_elections_cancelled",
                behavior.as_str(),
                self.cancelled[behavior as usize],
            );
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use rsnano_core::SavedBlock;
    use rsnano_nullable_clock::Timestamp;
    use std::time::Duration;

    #[test]
    fn collect_stats_empty() {
        let mut stats = StatsCollection::new();
        let stopped_counter = StoppedCounter::new();

        stopped_counter.collect_stats(&mut stats);

        assert_eq!(stats.get("active_elections", "stopped"), 0);
        assert_eq!(stats.get("active_elections", "confirmed"), 0);
        assert_eq!(stats.get("active_elections", "unconfirmed"), 0);

        // Assert that all stats are zero
        for (stats_key, _) in stats.iter() {
            assert_eq!(stats.get(stats_key.stat, stats_key.detail), 0);
        }
    }

    #[test]
    fn stop_election() {
        let mut stats = StatsCollection::new();
        let mut stopped_counter = StoppedCounter::new();
        let election = Election::new_test_instance_with(SavedBlock::new_test_instance());

        stopped_counter.stopped(&election);
        stopped_counter.collect_stats(&mut stats);

        assert_eq!(stats.get("active_elections", "stopped"), 1);
        assert_eq!(stats.get("active_elections", "confirmed"), 0);
        assert_eq!(stats.get("active_elections", "unconfirmed"), 1);
    }

    #[test]
    fn stop_multiple_elections() {
        let mut stats = StatsCollection::new();
        let mut stopped_counter = StoppedCounter::new();
        let election1 = Election::new_test_instance_with(SavedBlock::new_test_instance());
        let election2 = Election::new(
            SavedBlock::new_test_receive_block(),
            ElectionBehavior::Optimistic,
            Duration::from_millis(1000),
            Timestamp::new_test_instance(),
        );

        stopped_counter.stopped(&election1);
        stopped_counter.stopped(&election2);
        stopped_counter.collect_stats(&mut stats);

        assert_eq!(stats.get("active_elections", "stopped"), 2);
        assert_eq!(stats.get("active_elections", "confirmed"), 0);
        assert_eq!(stats.get("active_elections", "unconfirmed"), 2);
        assert_eq!(stats.get("active_elections_dropped", "optimistic"), 1);
    }
}
