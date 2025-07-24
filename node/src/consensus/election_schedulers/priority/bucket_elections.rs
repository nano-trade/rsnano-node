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
    by_priority: BTreeMap<TimePriority, Vec<QualifiedRoot>>,
}

impl BucketElections {
    pub fn contains(&self, root: &QualifiedRoot) -> bool {
        self.by_root.contains_key(root)
    }

    pub fn insert(&mut self, entry: BucketElection) {
        let root = entry.root.clone();
        let priority = entry.priority;
        let old = self.by_root.insert(root.clone(), entry);
        if let Some(old) = old {
            self.erase_indices(&old);
        }
        self.by_priority.entry(priority).or_default().push(root);
    }

    pub fn pop_lowest_priority(&mut self) -> Option<BucketElection> {
        let (_, roots) = self.by_priority.first_key_value()?;
        let root = roots[0].clone();
        self.erase(&root)
    }

    pub fn lowest_priority(&self) -> Option<TimePriority> {
        self.by_priority.first_key_value().map(|(prio, _)| *prio)
    }

    pub fn len(&self) -> usize {
        self.by_root.len()
    }

    pub fn erase(&mut self, root: &QualifiedRoot) -> Option<BucketElection> {
        let entry = self.by_root.remove(root)?;
        self.erase_indices(&entry);
        Some(entry)
    }

    fn erase_indices(&mut self, entry: &BucketElection) {
        let keys = self.by_priority.get_mut(&entry.priority).unwrap();
        if keys.len() == 1 {
            self.by_priority.remove(&entry.priority);
        } else {
            keys.retain(|i| *i != entry.root);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emtpy() {
        let elections = BucketElections::default();
        assert_eq!(elections.len(), 0);
        assert_eq!(elections.entry_with_lowest_priority(), None);
        assert_eq!(elections.lowest_priority(), None);
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
        assert_eq!(elections.lowest_priority(), Some(entry.priority));
        assert_eq!(
            elections.entry_with_lowest_priority().unwrap().root,
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
            elections.entry_with_lowest_priority().unwrap().root,
            entry3.root
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
