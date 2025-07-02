use crate::ledger_event_processor::LedgerEventProcessorPlugin;
use bounded_vec_deque::BoundedVecDeque;
use chrono::Utc;
use rsnano_ledger::LedgerEvent;
use rsnano_stats::{StatsCollection, StatsSource};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

const STATS_KEY: &str = "confirmation_time";
const DEFAULT_SAMPLE_SIZE: usize = 1000;

/// Tracks duration for p90 p95 and p99 of the last 1000 confirmations
pub(crate) struct ConfTimeStats {
    durations: BoundedVecDeque<u64>,
}

impl ConfTimeStats {
    pub fn new(sample_size: usize) -> Self {
        Self {
            durations: BoundedVecDeque::new(sample_size),
        }
    }

    pub fn add(&mut self, duration: Duration) {
        self.durations.push_back(duration.as_millis() as u64);
    }
}

impl Default for ConfTimeStats {
    fn default() -> Self {
        Self::new(DEFAULT_SAMPLE_SIZE)
    }
}

impl StatsSource for ConfTimeStats {
    fn collect_stats(&self, result: &mut StatsCollection) {
        let mut sorted: Vec<_> = self.durations.iter().cloned().collect();
        sorted.sort();

        let percentile = |perc: usize| -> u64 {
            if sorted.is_empty() {
                0
            } else {
                sorted[sorted.len() * perc / 100]
            }
        };

        result.insert(STATS_KEY, "p50", percentile(50));
        result.insert(STATS_KEY, "p90", percentile(90));
        result.insert(STATS_KEY, "p95", percentile(95));
        result.insert(STATS_KEY, "p99", percentile(99));
    }
}

pub(crate) struct TrackConfirmationTimes {
    stats: Arc<Mutex<ConfTimeStats>>,
}

impl TrackConfirmationTimes {
    pub fn stats(&self) -> Arc<Mutex<ConfTimeStats>> {
        self.stats.clone()
    }
}

impl Default for TrackConfirmationTimes {
    fn default() -> Self {
        Self {
            stats: Arc::new(Mutex::new(Default::default())),
        }
    }
}

impl LedgerEventProcessorPlugin for TrackConfirmationTimes {
    fn process(&mut self, event: &LedgerEvent) {
        if let LedgerEvent::BlocksConfirmed(blocks) = event {
            let now = Utc::now();
            let mut stats = self.stats.lock().unwrap();

            for (block, _) in blocks {
                let conf_time = (now - block.sideband().timestamp.utc())
                    .to_std()
                    .unwrap_or_default();
                stats.add(conf_time);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn emtpy() {
        assert_stats(&[], 0, 0, 0, 0);
    }

    #[test]
    fn one_confirmation() {
        assert_stats(&[Duration::from_millis(123)], 123, 123, 123, 123);
    }

    #[test]
    fn two_confirmations() {
        assert_stats(
            &[Duration::from_millis(100), Duration::from_millis(200)],
            200,
            200,
            200,
            200,
        );
    }

    #[test]
    fn three_confirmations() {
        assert_stats(
            &[
                Duration::from_millis(100),
                Duration::from_millis(200),
                Duration::from_millis(300),
            ],
            200,
            300,
            300,
            300,
        );
    }

    #[test]
    fn one_thousand_confirmations() {
        let mut durations = Vec::with_capacity(1000);
        for i in 1..=1000 {
            durations.push(Duration::from_millis(i));
        }

        assert_stats(&durations, 501, 901, 951, 991);
    }

    #[test]
    fn only_consider_latest_1000_entries() {
        let mut durations = Vec::with_capacity(1500);
        for i in 1..=1500 {
            durations.push(Duration::from_millis(i));
        }

        assert_stats(&durations, 1001, 1401, 1451, 1491);
    }

    fn assert_stats(conf_times: &[Duration], p50: u64, p90: u64, p95: u64, p99: u64) {
        let mut conf_stats = ConfTimeStats::new(1000);
        for time in conf_times {
            conf_stats.add(*time);
        }
        let mut result = StatsCollection::default();
        conf_stats.collect_stats(&mut result);
        assert_eq!(result.get(STATS_KEY, "p50"), p50, "p50");
        assert_eq!(result.get(STATS_KEY, "p90"), p90, "p90");
        assert_eq!(result.get(STATS_KEY, "p95"), p95, "p95");
        assert_eq!(result.get(STATS_KEY, "p99"), p99, "p99");
    }
}
