use rsnano_stats::{StatsCollection, StatsSource};

/// Tracks duration for p90 p95 and p99 of the last 1000 confirmations
#[derive(Default)]
pub(crate) struct ConfTimeStats {}

impl StatsSource for ConfTimeStats {
    fn collect_stats(&self, result: &mut StatsCollection) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore = "todo"]
    fn emtpy() {
        let conf_stats = ConfTimeStats::default();
        let mut result = StatsCollection::default();
        conf_stats.collect_stats(&mut result);
        assert_eq!(result.get("confirmation_time", "p90"), 0);
        assert_eq!(result.get("confirmation_time", "p95"), 0);
        assert_eq!(result.get("confirmation_time", "p99"), 0);
    }
}
