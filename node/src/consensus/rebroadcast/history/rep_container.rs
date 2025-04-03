use super::RepresentativeEntry;
use rsnano_core::{Amount, PublicKey};
use std::collections::{BTreeMap, HashMap};

#[derive(Default)]
pub(crate) struct RepresentativeContainer {
    entries: HashMap<PublicKey, RepresentativeEntry>,
    by_weight: BTreeMap<Amount, Vec<PublicKey>>,
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

    pub fn get_or_insert(
        &mut self,
        rep: PublicKey,
        create: impl FnOnce() -> RepresentativeEntry,
    ) -> &mut RepresentativeEntry {
        let is_new = !self.entries.contains_key(&rep);
        let entry = self.entries.entry(rep).or_insert_with(create);
        if is_new {
            self.by_weight.entry(entry.weight).or_default().push(rep);
        }
        entry
    }

    pub fn entries(&self) -> impl Iterator<Item = &RepresentativeEntry> {
        self.entries.values()
    }

    pub fn change_weights(&mut self, rep_weights: &HashMap<PublicKey, Amount>) {
        for entry in self.entries.values_mut() {
            let new_weight = rep_weights
                .get(&entry.representative)
                .cloned()
                .unwrap_or_default();

            if new_weight != entry.weight {
                remove_from_weight_index(&mut self.by_weight, entry.weight, &entry.representative);
                entry.weight = new_weight;
                self.by_weight
                    .entry(new_weight)
                    .or_default()
                    .push(entry.representative);
            }
        }
    }

    pub fn insert(&mut self, entry: RepresentativeEntry) {
        self.entries.insert(entry.representative, entry);
    }

    pub fn remove(&mut self, rep: &PublicKey) {
        if let Some(removed) = self.entries.remove(rep) {
            remove_from_weight_index(&mut self.by_weight, removed.weight, rep);
        }
    }

    pub fn lowest_weight(&self) -> Amount {
        self.by_weight
            .first_key_value()
            .map(|(weight, _)| *weight)
            .unwrap_or_default()
    }

    pub fn remove_lowest_weight(&mut self) {
        let Some(mut weight_entry) = self.by_weight.first_entry() else {
            return;
        };

        let lowest_reps = weight_entry.get_mut();
        let to_remove = lowest_reps.pop().unwrap();
        if lowest_reps.is_empty() {
            weight_entry.remove();
        }

        self.entries.remove(&to_remove);
    }
}

fn remove_from_weight_index(
    by_weight: &mut BTreeMap<Amount, Vec<PublicKey>>,
    weight: Amount,
    rep: &PublicKey,
) {
    let reps = by_weight.get_mut(&weight).unwrap();
    if reps.len() > 1 {
        reps.retain(|r| r != rep);
    } else {
        by_weight.remove(&weight);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsnano_core::Amount;
    use std::time::Duration;

    #[test]
    fn remove_lowest_tally_rep() {
        let mut container = RepresentativeContainer::default();

        let rep1 = PublicKey::from(1);
        let rep2 = PublicKey::from(2);
        let rep3 = PublicKey::from(3);
        let rep4 = PublicKey::from(4);

        let entry1 = test_entry(rep1, Amount::from(100));
        let entry2 = test_entry(rep2, Amount::from(50));
        let entry3 = test_entry(rep3, Amount::from(75));
        let entry4 = test_entry(rep4, Amount::from(75));

        container.get_or_insert(entry1.representative, move || entry1);
        container.get_or_insert(entry2.representative, move || entry2);
        container.get_or_insert(entry3.representative, move || entry3);
        container.get_or_insert(entry4.representative, move || entry4);

        container.remove_lowest_weight();
        assert_eq!(container.len(), 3);
        assert_eq!(container.contains(&rep2), false);
        assert_eq!(container.contains(&rep4), true);
        assert_eq!(container.contains(&rep3), true);
        assert_eq!(container.contains(&rep1), true);

        container.remove_lowest_weight();
        assert_eq!(container.len(), 2);
        assert_eq!(container.contains(&rep3), true);
        assert_eq!(container.contains(&rep1), true);

        container.remove_lowest_weight();
        assert_eq!(container.len(), 1);
        assert_eq!(container.contains(&rep1), true);

        container.remove_lowest_weight();
        assert_eq!(container.len(), 0);
    }

    #[test]
    fn remove() {
        let mut container = RepresentativeContainer::default();

        let rep1 = PublicKey::from(1);
        let rep2 = PublicKey::from(2);
        let rep3 = PublicKey::from(3);
        let rep4 = PublicKey::from(4);

        let entry1 = test_entry(rep1, Amount::from(100));
        let entry2 = test_entry(rep2, Amount::from(50));
        let entry3 = test_entry(rep3, Amount::from(75));
        let entry4 = test_entry(rep4, Amount::from(75));

        container.get_or_insert(entry1.representative, move || entry1);
        container.get_or_insert(entry2.representative, move || entry2);
        container.get_or_insert(entry3.representative, move || entry3);
        container.get_or_insert(entry4.representative, move || entry4);

        container.remove(&rep2);
        container.remove(&rep3);

        assert_eq!(container.len(), 2);
        assert_eq!(container.by_weight.len(), 2);
    }

    #[test]
    fn change_weight() {
        let mut container = RepresentativeContainer::default();

        let rep1 = PublicKey::from(1);
        let rep2 = PublicKey::from(2);
        let rep3 = PublicKey::from(3);
        let rep4 = PublicKey::from(4);

        let entry1 = test_entry(rep1, Amount::from(100));
        let entry2 = test_entry(rep2, Amount::from(50));
        let entry3 = test_entry(rep3, Amount::from(75));
        let entry4 = test_entry(rep4, Amount::from(75));

        container.get_or_insert(entry1.representative, move || entry1);
        container.get_or_insert(entry2.representative, move || entry2);
        container.get_or_insert(entry3.representative, move || entry3);
        container.get_or_insert(entry4.representative, move || entry4);

        let rep_weights = [
            (rep1, 75.into()),
            (rep2, 50.into()),
            (rep3, 10.into()),
            (rep4, 75.into()),
        ]
        .into();
        container.change_weights(&rep_weights);

        container.remove_lowest_weight();
        assert_eq!(container.contains(&rep3), false);

        container.remove_lowest_weight();
        assert_eq!(container.contains(&rep2), false);

        container.remove_lowest_weight();
        assert_eq!(container.contains(&rep1), false);
    }

    fn test_entry(key: PublicKey, weight: Amount) -> RepresentativeEntry {
        RepresentativeEntry::new(key, weight, 1, Duration::from_secs(42))
    }
}
