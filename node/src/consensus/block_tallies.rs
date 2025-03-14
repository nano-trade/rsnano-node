use std::collections::BTreeMap;

use rsnano_core::{Amount, BlockHash, DescTallyKey};

/// Counts the tally per block in an election.
/// It is sorted by descending tally
#[derive(Default)]
pub struct BlockTallies {
    tallies: BTreeMap<DescTallyKey, BlockHash>,
}

impl BlockTallies {
    pub(crate) fn new() -> Self {
        Default::default()
    }

    pub fn len(&self) -> usize {
        self.tallies.len()
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = (Amount, BlockHash)> + use<'_> {
        self.tallies.iter().map(convert_entry)
    }

    pub fn tallies(&self) -> impl Iterator<Item = Amount> + use<'_> {
        self.tallies.keys().map(|k| k.amount())
    }

    pub fn winner(&self) -> Option<(Amount, BlockHash)> {
        self.tallies.first_key_value().map(convert_entry)
    }

    pub fn lowest(&self) -> Option<(Amount, BlockHash)> {
        self.tallies.last_key_value().map(convert_entry)
    }

    pub fn get(&self, hash: &BlockHash) -> Option<Amount> {
        self.tallies.iter().find_map(|(tally, h)| {
            if h == hash {
                Some(tally.amount())
            } else {
                None
            }
        })
    }

    pub fn contains(&self, hash: &BlockHash) -> bool {
        self.tallies.iter().any(|(_, h)| h == hash)
    }

    pub fn insert(&mut self, tally: Amount, hash: BlockHash) {
        self.tallies.insert(DescTallyKey(tally), hash);
    }

    pub fn clear(&mut self) {
        self.tallies.clear();
    }

    pub fn sum(&self) -> Amount {
        self.tallies().sum()
    }

    pub fn check_quorum(&self, quorum_delta: Amount) -> bool {
        let mut it = self.tallies();
        let first = it.next().unwrap_or_default();
        let second = it.next().unwrap_or_default();
        first - second >= quorum_delta
    }
}

fn convert_entry((tally, hash): (&DescTallyKey, &BlockHash)) -> (Amount, BlockHash) {
    (tally.amount(), *hash)
}
