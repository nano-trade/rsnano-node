use super::RepresentativeEntry;
use rsnano_core::PublicKey;
use std::collections::HashMap;

#[derive(Default)]
pub(crate) struct RepresentativeContainer {
    entries: HashMap<PublicKey, RepresentativeEntry>,
}

impl RepresentativeContainer {
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn contains(&self, rep: &PublicKey) -> bool {
        self.entries.contains_key(rep)
    }

    pub fn get(&self, rep: &PublicKey) -> Option<&RepresentativeEntry> {
        self.entries.get(rep)
    }

    pub fn get_mut(&mut self, rep: &PublicKey) -> Option<&mut RepresentativeEntry> {
        self.entries.get_mut(rep)
    }

    pub fn entries(&self) -> impl Iterator<Item = &RepresentativeEntry> {
        self.entries.values()
    }

    pub fn entries_mut(&mut self) -> impl Iterator<Item = &mut RepresentativeEntry> {
        self.entries.values_mut()
    }

    pub fn insert(&mut self, entry: RepresentativeEntry) {
        self.entries.insert(entry.representative, entry);
    }

    pub fn remove(&mut self, rep: &PublicKey) {
        self.entries.remove(rep);
    }
}
