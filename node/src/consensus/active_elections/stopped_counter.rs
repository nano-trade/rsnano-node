use strum::{EnumCount, IntoEnumIterator};

use rsnano_stats::{StatsCollection, StatsSource};

use crate::consensus::{Election, ElectionBehavior, ElectionState};

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
