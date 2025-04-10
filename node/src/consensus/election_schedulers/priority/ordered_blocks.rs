use rsnano_core::{utils::BlockPriority, BlockHash, SavedBlock};
use std::{
    cmp::Ordering,
    collections::{BTreeSet, HashSet},
};

#[derive(Debug, Eq)]
pub(super) struct BlockEntry {
    pub priority: BlockPriority,
    pub block: SavedBlock,
}

impl BlockEntry {
    pub fn new(block: SavedBlock, priority: BlockPriority) -> Self {
        Self { priority, block }
    }

    pub fn hash(&self) -> BlockHash {
        self.block.hash()
    }
}

impl Ord for BlockEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        let prio_order = self.priority.cmp(&other.priority);
        match prio_order {
            Ordering::Equal => self.block.hash().cmp(&other.block.hash()),
            _ => prio_order,
        }
    }
}

impl PartialOrd for BlockEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for BlockEntry {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.block.hash() == other.block.hash()
    }
}

/// BlockEntries ordered by timestamp
#[derive(Default)]
pub(super) struct OrderedBlocks {
    hashes: HashSet<BlockHash>,
    by_priority: BTreeSet<BlockEntry>,
}

impl OrderedBlocks {
    pub fn len(&self) -> usize {
        self.hashes.len()
    }

    pub fn contains(&self, hash: &BlockHash) -> bool {
        self.hashes.contains(hash)
    }

    pub fn insert(&mut self, entry: BlockEntry) -> bool {
        if self.hashes.contains(&entry.hash()) {
            return false;
        }

        self.hashes.insert(entry.hash());
        self.by_priority.insert(entry);
        true
    }

    pub fn highest_prio(&self) -> Option<&BlockEntry> {
        self.by_priority.last()
    }

    pub fn pop_highest_prio(&mut self) -> Option<BlockEntry> {
        let first = self.by_priority.pop_last()?;
        self.hashes.remove(&first.hash());
        Some(first)
    }

    pub fn pop_lowest_prio(&mut self) -> Option<BlockEntry> {
        let last = self.by_priority.pop_first()?;
        self.hashes.remove(&last.hash());
        Some(last)
    }

    pub fn iter(&self) -> impl Iterator<Item = &BlockEntry> {
        self.by_priority.iter().rev()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsnano_core::{utils::UnixTimestamp, Amount, PrivateKey};

    #[test]
    fn empty() {
        let mut blocks = OrderedBlocks::default();
        assert_eq!(blocks.len(), 0);
        assert_eq!(blocks.contains(&BlockHash::from(1)), false);
        assert_eq!(blocks.highest_prio(), None);
        assert_eq!(blocks.pop_highest_prio(), None);
        assert_eq!(blocks.pop_lowest_prio(), None);
    }

    #[test]
    fn insert_one() {
        let mut blocks = OrderedBlocks::default();

        let (hash, entry) = create_entry(UnixTimestamp::new(123), 1);
        blocks.insert(entry);

        assert_eq!(blocks.len(), 1);
        assert!(blocks.contains(&hash));
        assert_eq!(blocks.highest_prio().unwrap().hash(), hash);
        assert_eq!(blocks.iter().count(), 1);
    }

    #[test]
    fn insert_multiple() {
        let mut blocks = OrderedBlocks::default();

        let (hash1, entry1) = create_entry(UnixTimestamp::new(123), 1);
        let (hash2, entry2) = create_entry(UnixTimestamp::new(456), 2);

        blocks.insert(entry1);
        blocks.insert(entry2);

        assert_eq!(blocks.len(), 2);
        assert!(blocks.contains(&hash1));
        assert!(blocks.contains(&hash2));
    }

    #[test]
    fn order_by_timestamp() {
        let mut blocks = OrderedBlocks::default();

        let (hash1, entry1) = create_entry(UnixTimestamp::new(333), 1);
        let (hash2, entry2) = create_entry(UnixTimestamp::new(111), 2);
        let (hash3, entry3) = create_entry(UnixTimestamp::new(222), 3);

        blocks.insert(entry1);
        blocks.insert(entry2);
        blocks.insert(entry3);

        assert_eq!(blocks.highest_prio().unwrap().block.hash(), hash2);
        assert_eq!(
            blocks.iter().map(|i| i.hash()).collect::<Vec<_>>(),
            [hash2, hash3, hash1]
        );
    }

    #[test]
    fn pop_highest_prio() {
        let mut blocks = OrderedBlocks::default();

        let (_, entry1) = create_entry(UnixTimestamp::new(333), 1);
        let (hash2, entry2) = create_entry(UnixTimestamp::new(111), 2);
        let (_, entry3) = create_entry(UnixTimestamp::new(222), 3);

        blocks.insert(entry1);
        blocks.insert(entry2);
        blocks.insert(entry3);

        let popped = blocks.pop_highest_prio().unwrap();
        assert_eq!(popped.hash(), hash2);
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks.contains(&hash2), false);
        assert_eq!(blocks.hashes.len(), 2);
        assert_eq!(blocks.by_priority.len(), 2);
    }

    #[test]
    fn pop_lowest_prio() {
        let mut blocks = OrderedBlocks::default();

        let (hash1, entry1) = create_entry(UnixTimestamp::new(333), 1);
        let (_, entry2) = create_entry(UnixTimestamp::new(111), 2);
        let (_, entry3) = create_entry(UnixTimestamp::new(222), 3);

        blocks.insert(entry1);
        blocks.insert(entry2);
        blocks.insert(entry3);

        let popped = blocks.pop_lowest_prio().unwrap();
        assert_eq!(popped.hash(), hash1);
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks.contains(&hash1), false);
        assert_eq!(blocks.hashes.len(), 2);
        assert_eq!(blocks.by_priority.len(), 2);
    }

    fn create_entry(time: UnixTimestamp, key: impl Into<PrivateKey>) -> (BlockHash, BlockEntry) {
        let entry = BlockEntry {
            priority: BlockPriority::new(Amount::nano(1), time.into()),
            block: SavedBlock::new_test_instance_with_key(key),
        };
        let hash = entry.block.hash();
        (hash, entry)
    }
}
