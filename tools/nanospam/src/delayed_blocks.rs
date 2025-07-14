use rsnano_core::{Block, BlockHash};
use rustc_hash::FxHashMap;
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

const DELAY_LIMIT: Duration = Duration::from_secs(10);

pub(crate) struct DelayedBlocks {
    /// block + publish timestamp
    blocks: FxHashMap<BlockHash, PublishInfo>,
    by_time: BTreeMap<Instant, Vec<BlockHash>>,
    finished: bool,
}

impl DelayedBlocks {
    pub(crate) fn new() -> Self {
        Self {
            blocks: FxHashMap::default(),
            by_time: BTreeMap::new(),
            finished: false,
        }
    }

    pub fn next(&mut self, now: Instant) -> Option<Block> {
        let mut entry = self.by_time.first_entry()?;
        let sent = entry.key().clone();
        let block_hashes = entry.get_mut();
        if now < sent + DELAY_LIMIT {
            return None;
        }

        let hash = block_hashes.pop().unwrap();
        if block_hashes.is_empty() {
            entry.remove();
        }

        let info = self.blocks.get_mut(&hash).unwrap();
        info.last_publish = None;
        Some(info.block.clone())
    }

    pub fn insert(&mut self, block: Block) {
        let hash = block.hash();
        if let Some(info) = self.blocks.insert(hash, PublishInfo::new(block)) {
            if let Some(old_sent) = info.last_publish {
                self.remove_from_time_index(&hash, old_sent);
            }
        }
    }

    pub fn published(&mut self, hash: &BlockHash, timestamp: Instant) {
        if let Some(info) = self.blocks.get_mut(hash) {
            if info.first_publish.is_none() {
                info.first_publish = Some(timestamp);
            }
            let old_sent = info.last_publish;
            info.last_publish = Some(timestamp);

            if let Some(old_sent) = old_sent {
                self.remove_from_time_index(hash, old_sent);
            }
            self.by_time.entry(timestamp).or_default().push(*hash);
        }
    }

    pub fn confirmed(&mut self, hash: &BlockHash, timestamp: Instant) -> Option<Duration> {
        if let Some(info) = self.blocks.remove(hash) {
            if let Some(sent) = info.last_publish {
                self.remove_from_time_index(hash, sent);
            }
            info.first_publish.map(|i| timestamp.duration_since(i))
        } else {
            None
        }
    }

    pub fn len(&self) -> usize {
        self.blocks.len()
    }

    pub fn finished(&mut self) {
        self.finished = true;
    }

    pub fn is_finished(&self) -> bool {
        self.finished && self.len() == 0
    }

    fn remove_from_time_index(&mut self, hash: &BlockHash, sent: Instant) {
        let mut hashes = self.by_time.remove(&sent).unwrap();
        hashes.retain(|h| h != hash);
        if !hashes.is_empty() {
            self.by_time.insert(sent, hashes);
        }
    }
}

struct PublishInfo {
    block: Block,
    first_publish: Option<Instant>,
    last_publish: Option<Instant>,
}

impl PublishInfo {
    fn new(block: Block) -> Self {
        Self {
            block,
            first_publish: None,
            last_publish: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    #[test]
    fn empty() {
        let mut delayed = DelayedBlocks::new();
        let now = Instant::now();
        assert!(delayed.next(now).is_none());
        assert_eq!(delayed.len(), 0);
    }

    #[test]
    fn add_block() {
        let mut delayed = DelayedBlocks::new();
        let now = Instant::now();
        let block = Block::new_test_instance();
        let hash = block.hash();
        delayed.insert(block);
        delayed.published(&hash, now);
        assert!(delayed.next(now + Duration::from_millis(500)).is_none());
        assert_eq!(delayed.len(), 1);
    }

    #[test]
    fn get_delayed_block() {
        let mut delayed = DelayedBlocks::new();
        let now = Instant::now();
        let block = Block::new_test_instance();
        let hash = block.hash();
        delayed.insert(block);
        delayed.published(&hash, now);

        let delayed_block = delayed.next(now + DELAY_LIMIT).unwrap();

        assert_eq!(delayed_block.hash(), hash);
        assert_eq!(delayed.len(), 1);
        assert!(delayed.next(now + DELAY_LIMIT).is_none());
    }

    #[test]
    fn remove_confirmed_block() {
        let mut delayed = DelayedBlocks::new();
        let now = Instant::now();
        let block = Block::new_test_instance();
        let hash = block.hash();
        delayed.insert(block);
        delayed.published(&hash, now);

        delayed.confirmed(&hash, now);

        assert_eq!(delayed.len(), 0);
    }

    #[test]
    fn update_block_time_when_inserted_twice() {
        let mut delayed = DelayedBlocks::new();
        let time_a = Instant::now();
        let time_b = Instant::now() + Duration::from_secs(1);
        let block = Block::new_test_instance();

        delayed.insert(block.clone());
        delayed.insert(block.clone());
        delayed.published(&block.hash(), time_a);
        delayed.published(&block.hash(), time_b);

        assert_eq!(delayed.len(), 1);
        assert_eq!(delayed.by_time.len(), 1);
        assert_eq!(*delayed.by_time.first_key_value().unwrap().0, time_b);
    }

    #[test]
    fn allow_multiple_blocks_with_same_sent_timestamp() {
        let mut delayed = DelayedBlocks::new();
        let now = Instant::now();
        let block_a = Block::new_test_instance_with_key(1);
        let block_b = Block::new_test_instance_with_key(2);
        delayed.insert(block_a.clone());
        delayed.insert(block_b.clone());
        delayed.published(&block_a.hash(), now);
        delayed.published(&block_b.hash(), now);
        assert_eq!(delayed.len(), 2);
        assert_eq!(delayed.by_time.len(), 1);

        let popped_a = delayed.next(now + DELAY_LIMIT).unwrap();
        let popped_b = delayed.next(now + DELAY_LIMIT).unwrap();
        assert!(delayed.next(now + DELAY_LIMIT).is_none());

        assert_eq!(popped_a.hash(), block_b.hash());
        assert_eq!(popped_b.hash(), block_a.hash());
    }

    #[test]
    fn confirm_blocks_with_same_sent_timestamp() {
        let mut delayed = DelayedBlocks::new();
        let now = Instant::now();
        let block_a = Block::new_test_instance_with_key(1);
        let block_b = Block::new_test_instance_with_key(2);
        delayed.insert(block_a.clone());
        delayed.insert(block_b.clone());
        delayed.published(&block_a.hash(), now);
        delayed.published(&block_b.hash(), now);

        delayed.confirmed(&block_a.hash(), now);

        assert_eq!(
            delayed.next(now + DELAY_LIMIT).unwrap().hash(),
            block_b.hash()
        );
    }
}
