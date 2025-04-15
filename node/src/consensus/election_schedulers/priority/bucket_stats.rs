use rsnano_stats::{StatsCollection, StatsSource};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Default)]
pub struct BucketStats {
    pub cancelled: AtomicUsize,
    pub activate_success: AtomicUsize,
    pub activate_failed_duplicate: AtomicUsize,
    pub activate_failed_confirmed: AtomicUsize,
}

const STATS_KEY: &'static str = "election_bucket";

impl StatsSource for BucketStats {
    fn collect_stats(&self, result: &mut StatsCollection) {
        result.insert(
            STATS_KEY,
            "cancel_lowest",
            self.cancelled.load(Ordering::Relaxed),
        );
        result.insert(
            STATS_KEY,
            "activate_success",
            self.activate_success.load(Ordering::Relaxed),
        );
        result.insert(
            STATS_KEY,
            "activate_failed_duplicate",
            self.activate_failed_duplicate.load(Ordering::Relaxed),
        );
        result.insert(
            STATS_KEY,
            "activate_failed_confirmed",
            self.activate_failed_confirmed.load(Ordering::Relaxed),
        );
    }
}
