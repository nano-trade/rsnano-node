use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard},
    u64,
};

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

    pub fn get(&self, stat: &'static str, detail: &'static str) -> u64 {
        Self::get_dir(&self, stat, detail, Direction::In)
    }

    pub fn get_dir(&self, stat: &'static str, detail: &'static str, dir: Direction) -> u64 {
        let key = StatsKey { stat, detail, dir };
        self.0.get(&key).cloned().unwrap_or_default()
    }

    pub fn insert(&mut self, stat: &'static str, detail: &'static str, value: impl Into<u64>) {
        self.insert_dir(stat, detail, Direction::In, value);
    }

    pub fn insert_dir(
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
    stats: Mutex<StatsCollection>,
    sources: Vec<Arc<dyn StatsSource + Send + Sync>>,
}

impl StatsCollector {
    pub fn new() -> Self {
        Self {
            stats: Mutex::new(StatsCollection::new()),
            sources: Vec::new(),
        }
    }

    pub fn add_source(&mut self, source: Arc<dyn StatsSource + Send + Sync>) {
        self.sources.push(source);
    }

    pub fn collect(&self) -> MutexGuard<StatsCollection> {
        let mut stats = self.stats.lock().unwrap();
        for source in &self.sources {
            source.collect_stats(&mut stats);
        }
        stats
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    #[test]
    fn collect_nothing() {
        let collector = StatsCollector::new();
        let result = collector.collect();
        assert_eq!(result.len(), 0);
        assert_eq!(result.get_dir("a", "b", Direction::In), 0);
    }

    #[test]
    fn collect_from_one_source() {
        let mut collector = StatsCollector::new();
        collector.add_source(Arc::new(StubStatsSource::new("a", "b", Direction::In)));
        let result = collector.collect();
        assert_eq!(result.len(), 1);
        assert_eq!(result.get_dir("a", "b", Direction::In), 1);
        assert_eq!(result.get_dir("c", "d", Direction::Out), 0);
    }

    #[test]
    fn collect_from_multiple_source() {
        let mut collector = StatsCollector::new();
        collector.add_source(Arc::new(StubStatsSource::new("a", "b", Direction::In)));
        collector.add_source(Arc::new(StubStatsSource::new("c", "d", Direction::Out)));
        let result = collector.collect();
        assert_eq!(result.len(), 2);
        assert_eq!(result.get_dir("a", "b", Direction::In), 1);
        assert_eq!(result.get_dir("c", "d", Direction::Out), 1);
    }

    #[test]
    fn collect_twice() {
        let mut collector = StatsCollector::new();
        collector.add_source(Arc::new(StubStatsSource::new("a", "b", Direction::In)));

        drop(collector.collect());
        let result = collector.collect();

        assert_eq!(result.len(), 1);
        assert_eq!(result.get_dir("a", "b", Direction::In), 2);
    }

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
            result.insert_dir(
                self.stat,
                self.detail,
                self.dir,
                self.value.fetch_add(1, Ordering::Relaxed),
            );
        }
    }
}
