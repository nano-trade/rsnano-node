use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use rsnano_core::QualifiedRoot;
use rsnano_ledger::Election;

use super::ErasedCallback;

pub(crate) struct Entry {
    pub root: QualifiedRoot,
    pub election: Arc<Mutex<Election>>,
    pub erased_callback: Option<ErasedCallback>,
}

/// Contains elections and their qualified roots
#[derive(Default)]
pub(crate) struct RootContainer {
    by_root: HashMap<QualifiedRoot, Entry>,
    sequenced: Vec<QualifiedRoot>,
}

impl RootContainer {
    pub const ELEMENT_SIZE: usize =
        size_of::<QualifiedRoot>() * 2 + size_of::<Arc<Mutex<Election>>>();

    pub fn insert(&mut self, entry: Entry) {
        let root = entry.root.clone();
        if self.by_root.insert(root.clone(), entry).is_none() {
            self.sequenced.push(root);
        }
    }

    pub fn get(&self, root: &QualifiedRoot) -> Option<&Entry> {
        self.by_root.get(root)
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
}
