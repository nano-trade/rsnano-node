use rsnano_stats::{StatsCollection, StatsSource};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

#[derive(Default)]
pub struct BucketStats {
    pub cancelled: AtomicU64,
    pub activate_success: AtomicU64,
    pub activate_failed_duplicate: AtomicU64,
    /// Activation of a block failed, because it was recently confirmed
    pub activate_failed_confirmed: AtomicU64,
    /// A low-prio election got replaced by one with a higher priority
    pub replaced: AtomicU64,
}

const STATS_KEY: &'static str = "election_bucket";

impl StatsSource for BucketStats {
    fn collect_stats(&self, result: &mut StatsCollection) {
        result.insert(STATS_KEY, "cancel_lowest", self.cancelled.load(Relaxed));
        result.insert(
            STATS_KEY,
            "activate_success",
            self.activate_success.load(Relaxed),
        );
        result.insert(
            STATS_KEY,
            "activate_failed_duplicate",
            self.activate_failed_duplicate.load(Relaxed),
        );
        result.insert(
            STATS_KEY,
            "activate_failed_confirmed",
            self.activate_failed_confirmed.load(Relaxed),
        );
        result.insert(STATS_KEY, "replaced", self.replaced.load(Relaxed));
    }
}
