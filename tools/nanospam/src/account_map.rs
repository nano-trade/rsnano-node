use rand::seq::IndexedRandom;
use rsnano_core::{Account, Amount, BlockHash, PrivateKey};
use std::collections::{HashMap, HashSet};

#[derive(Default)]
pub(crate) struct AccountMap {
    accounts: HashMap<Account, AccountState>,
    accounts_vec: Vec<Account>,
    empty: HashSet<Account>,
    non_empty: HashSet<Account>,

    /// Account => Send block hash + amount sent
    receivable: HashMap<Account, Vec<(BlockHash, Amount)>>,
}

struct AccountState {
    key: PrivateKey,
    frontier: BlockHash,
    balance: Amount,
}

impl AccountMap {
    pub fn fill(&mut self, count: usize) {
        for _ in 0..count {
            let key = PrivateKey::new();
            self.add_unopened(key);
        }
    }

    pub fn add_unopened(&mut self, key: PrivateKey) {
        let account = key.account();
        self.empty.insert(account);
        self.accounts_vec.push(account);
        self.accounts.insert(
            account,
            AccountState {
                key,
                frontier: BlockHash::zero(),
                balance: Amount::zero(),
            },
        );
    }

    pub fn random_account(&self) -> Option<Account> {
        self.accounts_vec.choose(&mut rand::rng()).cloned()
    }

    pub fn all_accounts_empty(&self) -> bool {
        self.non_empty.is_empty()
    }

    pub fn process_send(&mut self, destination: Account, send_hash: BlockHash, amount: Amount) {
        self.receivable
            .entry(destination)
            .or_default()
            .push((send_hash, amount));
    }

    pub fn contains(&self, account: &Account) -> bool {
        self.accounts.contains_key(account)
    }

    pub fn get_receivable(&self, account: &Account) -> Option<(BlockHash, Amount)> {
        let entries = self.receivable.get(account)?;
        entries.first().cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ntest::assert_false;

    #[test]
    fn empty() {
        let map = AccountMap::default();
        assert!(map.all_accounts_empty());
        assert_eq!(map.get_receivable(&1.into()), None);
        assert_false!(map.contains(&1.into()));
        assert_eq!(map.random_account(), None);
    }

    #[test]
    fn add_one_account() {
        let mut map = AccountMap::default();
        let key = PrivateKey::from(1);

        map.add_unopened(key.clone());

        assert!(map.contains(&key.account()));
        assert_eq!(map.random_account(), Some(key.account()));
    }

    #[test]
    fn process_send() {
        let mut map = AccountMap::default();
        let send_hash = BlockHash::from(42);
        let dest_key = PrivateKey::from(100);
        let amount = Amount::nano(12_345);
        map.add_unopened(dest_key.clone());

        map.process_send(dest_key.account(), send_hash, amount);

        assert_eq!(
            map.get_receivable(&dest_key.account()),
            Some((send_hash, amount))
        );
    }
}
