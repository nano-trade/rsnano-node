use std::collections::{BTreeMap, HashMap};

use rsnano_core::{utils::UnixTimestamp, QualifiedRoot};

/// Data about an active election that was caused by this bucket
pub(super) struct BucketElection {
    pub root: QualifiedRoot,
    pub priority: UnixTimestamp,
}

#[derive(Default)]
pub(super) struct BucketElections {
    by_root: HashMap<QualifiedRoot, BucketElection>,
    sequenced: Vec<QualifiedRoot>,
    by_priority: BTreeMap<UnixTimestamp, Vec<QualifiedRoot>>,
}

impl BucketElections {
    pub fn insert(&mut self, entry: BucketElection) {
        let root = entry.root.clone();
        let priority = entry.priority;
        let old = self.by_root.insert(root.clone(), entry);
        if let Some(old) = old {
            self.erase_indices(old);
        }
        self.sequenced.push(root.clone());
        self.by_priority.entry(priority).or_default().push(root);
    }

    pub fn entry_with_lowest_priority(&self) -> Option<&BucketElection> {
        self.by_priority
            .first_key_value()
            .and_then(|(_, roots)| self.by_root.get(&roots[0]))
    }

    pub fn lowest_priority(&self) -> UnixTimestamp {
        self.by_priority
            .first_key_value()
            .map(|(prio, _)| *prio)
            .unwrap_or_default()
    }

    pub fn len(&self) -> usize {
        self.sequenced.len()
    }

    pub fn erase(&mut self, root: &QualifiedRoot) {
        if let Some(entry) = self.by_root.remove(root) {
            self.erase_indices(entry)
        }
    }

    fn erase_indices(&mut self, entry: BucketElection) {
        let keys = self.by_priority.get_mut(&entry.priority).unwrap();
        if keys.len() == 1 {
            self.by_priority.remove(&entry.priority);
        } else {
            keys.retain(|i| *i != entry.root);
        }
        self.sequenced.retain(|i| *i != entry.root);
    }
}
