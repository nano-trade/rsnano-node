use std::collections::{BTreeMap, HashMap};

use rsnano_core::{utils::TimePriority, QualifiedRoot};

/// Data about an active election that was caused by this bucket
#[derive(Debug, PartialEq, Eq, Clone)]
pub(super) struct BucketElection {
    pub root: QualifiedRoot,
    pub priority: TimePriority,
}

#[derive(Default)]
pub(super) struct BucketElections {
    by_root: HashMap<QualifiedRoot, BucketElection>,
    sequenced: Vec<QualifiedRoot>,
    by_priority: BTreeMap<TimePriority, Vec<QualifiedRoot>>,
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

    pub fn entry_with_highest_priority(&self) -> Option<&BucketElection> {
        self.by_priority
            .last_key_value()
            .and_then(|(_, roots)| self.by_root.get(&roots[0]))
    }

    pub fn highest_priority(&self) -> TimePriority {
        self.by_priority
            .last_key_value()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emtpy() {
        let elections = BucketElections::default();
        assert_eq!(elections.len(), 0);
        assert_eq!(elections.entry_with_highest_priority(), None);
        assert_eq!(elections.highest_priority(), TimePriority::MIN);
    }

    #[test]
    fn insert_one() {
        let mut elections = BucketElections::default();
        let entry = BucketElection {
            root: QualifiedRoot::new_test_instance(),
            priority: TimePriority::new(123),
        };

        elections.insert(entry.clone());

        assert_eq!(elections.len(), 1);
        assert_eq!(elections.highest_priority(), entry.priority);
        assert_eq!(
            elections.entry_with_highest_priority().unwrap().root,
            entry.root
        );
    }

    #[test]
    fn insert_multiple() {
        let mut elections = BucketElections::default();
        let entry1 = BucketElection {
            root: QualifiedRoot::new(1.into(), 2.into()),
            priority: TimePriority::new(123),
        };

        let entry2 = BucketElection {
            root: QualifiedRoot::new(3.into(), 4.into()),
            priority: TimePriority::new(123),
        };

        let entry3 = BucketElection {
            root: QualifiedRoot::new(5.into(), 6.into()),
            priority: TimePriority::new(456),
        };

        elections.insert(entry1.clone());
        elections.insert(entry2.clone());
        elections.insert(entry3.clone());

        assert_eq!(elections.len(), 3);
        assert_eq!(
            elections.entry_with_highest_priority().unwrap().root,
            entry1.root
        );
    }

    #[test]
    fn erase() {
        let mut elections = BucketElections::default();
        let entry1 = BucketElection {
            root: QualifiedRoot::new(1.into(), 2.into()),
            priority: TimePriority::new(123),
        };

        let entry2 = BucketElection {
            root: QualifiedRoot::new(3.into(), 4.into()),
            priority: TimePriority::new(123),
        };

        let entry3 = BucketElection {
            root: QualifiedRoot::new(5.into(), 6.into()),
            priority: TimePriority::new(456),
        };

        elections.insert(entry1.clone());
        elections.insert(entry2.clone());
        elections.insert(entry3.clone());

        elections.erase(&entry3.root);
        assert_eq!(elections.len(), 2);
        assert_eq!(elections.by_root.len(), 2);
        assert_eq!(elections.by_priority.len(), 1);

        elections.erase(&entry2.root);
        assert_eq!(elections.len(), 1);
        assert_eq!(elections.by_root.len(), 1);
        assert_eq!(elections.by_priority.len(), 1);

        elections.erase(&entry1.root);
        assert_eq!(elections.len(), 0);
        assert_eq!(elections.by_root.len(), 0);
        assert_eq!(elections.by_priority.len(), 0);
    }
}
