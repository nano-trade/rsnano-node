use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    u64,
};

use rsnano_core::utils::{CancellationToken, Runnable};

use crate::Direction;

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct StatsKey {
    pub stat: &'static str,
    pub detail: &'static str,
    pub dir: Direction,
}

impl StatsKey {
    pub const fn new(stat: &'static str, detail: &'static str, dir: Direction) -> Self {
        Self { stat, detail, dir }
    }
}

pub struct StatsCollection(HashMap<StatsKey, u64>);

impl StatsCollection {
    pub fn new() -> Self {
        Self(Default::default())
    }

    pub fn get(&self, stat: &'static str, detail: &'static str, dir: Direction) -> u64 {
        let key = StatsKey { stat, detail, dir };
        self.0.get(&key).cloned().unwrap_or_default()
    }

    pub fn insert(
        &mut self,
        stat: &'static str,
        detail: &'static str,
        dir: Direction,
        value: impl Into<u64>,
    ) {
        let key = StatsKey { stat, detail, dir };
        self.0.insert(key, value.into());
    }

    pub fn iter(&self) -> impl Iterator<Item = (&StatsKey, &u64)> {
        self.0.iter()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl Default for StatsCollection {
    fn default() -> Self {
        Self::new()
    }
}

pub trait StatsSource {
    fn collect_stats(&self, result: &mut StatsCollection);
}

pub struct StatsCollector {
    stats: Arc<Mutex<StatsCollection>>,
    sources: Vec<Arc<dyn StatsSource + Send + Sync>>,
    tmp_stats: StatsCollection,
}

impl StatsCollector {
    pub fn new(stats: Arc<Mutex<StatsCollection>>) -> Self {
        Self {
            stats,
            sources: Vec::new(),
            tmp_stats: StatsCollection::new(),
        }
    }

    pub fn add_source(&mut self, source: Arc<dyn StatsSource + Send + Sync>) {
        self.sources.push(source);
    }

    pub fn collect(&mut self) {
        for source in &self.sources {
            source.collect_stats(&mut self.tmp_stats);
        }
        let mut guard = self.stats.lock().unwrap();
        std::mem::swap(&mut *guard, &mut self.tmp_stats);
    }
}

impl Runnable for StatsCollector {
    fn run(&mut self, _cancel_token: &CancellationToken) {
        self.collect();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    #[test]
    fn collect_nothing() {
        let stats = Arc::new(Mutex::new(StatsCollection::new()));
        let mut collector = StatsCollector::new(stats.clone());
        collector.collect();
        let result = stats.lock().unwrap();
        assert_eq!(result.len(), 0);
        assert_eq!(result.get("a", "b", Direction::In), 0);
    }

    #[test]
    fn collect_from_one_source() {
        let stats = Arc::new(Mutex::new(StatsCollection::new()));
        let mut collector = StatsCollector::new(stats.clone());
        collector.add_source(Arc::new(StubStatsSource::new("a", "b", Direction::In)));
        collector.collect();
        let result = stats.lock().unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result.get("a", "b", Direction::In), 1);
        assert_eq!(result.get("c", "d", Direction::Out), 0);
    }

    #[test]
    fn collect_from_multiple_source() {
        let stats = Arc::new(Mutex::new(StatsCollection::new()));
        let mut collector = StatsCollector::new(stats.clone());
        collector.add_source(Arc::new(StubStatsSource::new("a", "b", Direction::In)));
        collector.add_source(Arc::new(StubStatsSource::new("c", "d", Direction::Out)));
        collector.collect();
        let result = stats.lock().unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result.get("a", "b", Direction::In), 1);
        assert_eq!(result.get("c", "d", Direction::Out), 1);
    }

    #[test]
    fn collect_twice() {
        let stats = Arc::new(Mutex::new(StatsCollection::new()));
        let mut collector = StatsCollector::new(stats.clone());
        collector.add_source(Arc::new(StubStatsSource::new("a", "b", Direction::In)));

        collector.collect();
        collector.collect();

        let result = stats.lock().unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result.get("a", "b", Direction::In), 2);
    }

    const TEST_KEY1: StatsKey = StatsKey::new("a", "b", Direction::In);
    const TEST_KEY2: StatsKey = StatsKey::new("c", "d", Direction::Out);

    struct StubStatsSource {
        stat: &'static str,
        detail: &'static str,
        dir: Direction,
        value: AtomicU64,
    }

    impl StubStatsSource {
        fn new(stat: &'static str, detail: &'static str, dir: Direction) -> Self {
            Self {
                stat,
                detail,
                dir,
                value: AtomicU64::new(1),
            }
        }
    }

    impl StatsSource for StubStatsSource {
        fn collect_stats(&self, result: &mut StatsCollection) {
            result.insert(
                self.stat,
                self.detail,
                self.dir,
                self.value.fetch_add(1, Ordering::Relaxed),
            );
        }
    }
}
