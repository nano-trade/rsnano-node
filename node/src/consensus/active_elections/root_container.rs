use std::collections::HashMap;

use rsnano_core::QualifiedRoot;

use crate::consensus::Election;

use super::ErasedCallback;

pub(crate) struct Entry {
    pub root: QualifiedRoot,
    pub election: Election,
    pub erased_callback: Option<ErasedCallback>,
}

/// Contains elections and their qualified roots
#[derive(Default)]
pub(crate) struct RootContainer {
    by_root: HashMap<QualifiedRoot, Entry>,
    sequenced: Vec<QualifiedRoot>,
}

impl RootContainer {
    pub const ELEMENT_SIZE: usize = size_of::<QualifiedRoot>() * 2 + size_of::<Election>();

    pub fn insert(&mut self, entry: Entry) {
        let root = entry.root.clone();
        if self.by_root.insert(root.clone(), entry).is_none() {
            self.sequenced.push(root);
        }
    }

    pub fn get(&self, root: &QualifiedRoot) -> Option<&Entry> {
        self.by_root.get(root)
    }

    pub fn get_mut(&mut self, root: &QualifiedRoot) -> Option<&mut Entry> {
        self.by_root.get_mut(root)
    }

    pub fn drain_filter(&mut self, mut predicate: impl FnMut(&Entry) -> bool) -> Vec<Entry> {
        let to_remove: Vec<_> = self
            .by_root
            .values()
            .filter_map(|i| {
                if predicate(i) {
                    Some(i.root.clone())
                } else {
                    None
                }
            })
            .collect();

        let mut removed = Vec::new();
        for root in to_remove {
            if let Some(entry) = self.erase(&root) {
                removed.push(entry);
            }
        }

        removed
    }

    pub fn erase(&mut self, root: &QualifiedRoot) -> Option<Entry> {
        let erased = self.by_root.remove(root);
        if erased.is_some() {
            self.sequenced.retain(|x| x != root)
        }
        erased
    }

    pub fn clear(&mut self) {
        self.sequenced.clear();
        self.by_root.clear();
    }

    pub fn len(&self) -> usize {
        self.sequenced.len()
    }

    pub fn iter_sequenced(&self) -> impl Iterator<Item = &Entry> {
        self.sequenced.iter().map(|r| self.by_root.get(r).unwrap())
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut Entry> {
        self.by_root.values_mut()
    }
}
