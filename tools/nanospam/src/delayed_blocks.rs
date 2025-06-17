use rsnano_core::{Block, BlockHash};
use std::{
    collections::{BTreeMap, HashMap},
    time::{Duration, Instant},
};

const DELAY_LIMIT: Duration = Duration::from_secs(5);

pub(crate) struct DelayedBlocks {
    blocks: HashMap<BlockHash, (Block, Instant)>,
    by_time: BTreeMap<Instant, Vec<BlockHash>>,
}

impl DelayedBlocks {
    pub(crate) fn new() -> Self {
        Self {
            blocks: HashMap::new(),
            by_time: BTreeMap::new(),
        }
    }

    pub fn pop(&mut self, now: Instant) -> Option<Block> {
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

        let (block, _) = self.blocks.remove(&hash).unwrap();
        Some(block)
    }

    pub fn insert(&mut self, block: Block, sent: Instant) {
        let hash = block.hash();
        if let Some((_, old_sent)) = self.blocks.insert(hash, (block, sent)) {
            self.remove_from_time_index(&hash, old_sent);
        }
        self.by_time.entry(sent).or_default().push(hash);
    }

    pub fn confirmed(&mut self, hash: &BlockHash) {
        if let Some((_, sent)) = self.blocks.remove(hash) {
            self.remove_from_time_index(hash, sent);
        }
    }

    pub fn len(&self) -> usize {
        self.blocks.len()
    }

    fn remove_from_time_index(&mut self, hash: &BlockHash, sent: Instant) {
        let mut hashes = self.by_time.remove(&sent).unwrap();
        hashes.retain(|h| h != hash);
        if !hashes.is_empty() {
            self.by_time.insert(sent, hashes);
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
        assert!(delayed.pop(now).is_none());
        assert_eq!(delayed.len(), 0);
    }

    #[test]
    fn add_block() {
        let mut delayed = DelayedBlocks::new();
        let now = Instant::now();
        let block = Block::new_test_instance();
        delayed.insert(block, now);
        assert!(delayed.pop(now + Duration::from_millis(500)).is_none());
        assert_eq!(delayed.len(), 1);
    }

    #[test]
    fn pop_delayed_block() {
        let mut delayed = DelayedBlocks::new();
        let now = Instant::now();
        let block = Block::new_test_instance();
        let hash = block.hash();
        delayed.insert(block, now);

        let popped = delayed.pop(now + DELAY_LIMIT).unwrap();

        assert_eq!(popped.hash(), hash);
        assert_eq!(delayed.len(), 0);
    }

    #[test]
    fn remove_confirmed_block() {
        let mut delayed = DelayedBlocks::new();
        let now = Instant::now();
        let block = Block::new_test_instance();
        let hash = block.hash();
        delayed.insert(block, now);

        delayed.confirmed(&hash);

        assert_eq!(delayed.len(), 0);
    }

    #[test]
    fn update_block_time_when_inserted_twice() {
        let mut delayed = DelayedBlocks::new();
        let time_a = Instant::now();
        let time_b = Instant::now() + Duration::from_secs(1);
        let block = Block::new_test_instance();

        delayed.insert(block.clone(), time_a);
        delayed.insert(block, time_b);

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
        delayed.insert(block_a.clone(), now);
        delayed.insert(block_b.clone(), now);
        assert_eq!(delayed.len(), 2);
        assert_eq!(delayed.by_time.len(), 1);

        let popped_a = delayed.pop(now + DELAY_LIMIT).unwrap();
        let popped_b = delayed.pop(now + DELAY_LIMIT).unwrap();
        assert!(delayed.pop(now + DELAY_LIMIT).is_none());

        assert_eq!(popped_a.hash(), block_b.hash());
        assert_eq!(popped_b.hash(), block_a.hash());
    }

    #[test]
    fn confirm_blocks_with_same_sent_timestamp() {
        let mut delayed = DelayedBlocks::new();
        let now = Instant::now();
        let block_a = Block::new_test_instance_with_key(1);
        let block_b = Block::new_test_instance_with_key(2);
        delayed.insert(block_a.clone(), now);
        delayed.insert(block_b.clone(), now);

        delayed.confirmed(&block_a.hash());

        assert_eq!(
            delayed.pop(now + DELAY_LIMIT).unwrap().hash(),
            block_b.hash()
        );
    }
}
