use rand::seq::IndexedRandom;
use rsnano_core::{Account, Amount, BlockHash, PrivateKey};
use std::collections::{HashMap, HashSet};

#[derive(Default)]
pub(crate) struct AccountMap {
    account_states: HashMap<Account, AccountState>,
    all_accounts: Vec<Account>,
    empty_accounts: HashSet<Account>,
    accounts_that_can_send: HashSet<Account>,

    /// Account => Send block hash + amount sent
    receivable: HashMap<Account, Vec<(BlockHash, Amount)>>,
}

pub(crate) struct AccountState {
    pub key: PrivateKey,
    pub frontier: BlockHash,
    pub balance: Amount,
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
        self.empty_accounts.insert(account);
        self.all_accounts.push(account);
        self.account_states.insert(
            account,
            AccountState {
                key,
                frontier: BlockHash::zero(),
                balance: Amount::zero(),
            },
        );
    }

    pub fn state(&self, account: &Account) -> Option<&AccountState> {
        self.account_states.get(account)
    }

    pub fn random_account(&self) -> Option<Account> {
        self.all_accounts.choose(&mut rand::rng()).cloned()
    }

    pub fn process_send(
        &mut self,
        source: Account,
        destination: Account,
        send_hash: BlockHash,
        amount: Amount,
    ) {
        self.receivable
            .entry(destination)
            .or_default()
            .push((send_hash, amount));

        if let Some(state) = self.account_states.get_mut(&source) {
            state.balance -= amount;
            if state.balance.is_zero() {
                self.accounts_that_can_send.remove(&source);
            }
        }
    }

    pub fn process_receive(&mut self, receiver: &Account, send_hash: &BlockHash) {
        let entries = self
            .receivable
            .get_mut(receiver)
            .expect("no receivables found");

        let pos = entries
            .iter()
            .position(|(hash, _)| hash == send_hash)
            .expect("no receivable entry found for given send hash");

        let (_, sent) = entries.remove(pos);

        if entries.is_empty() {
            self.receivable.remove(receiver);
        }

        self.account_states.get_mut(receiver).unwrap().balance += sent;
        self.accounts_that_can_send.insert(*receiver);
    }

    pub fn contains(&self, account: &Account) -> bool {
        self.account_states.contains_key(account)
    }

    pub fn get_receivable(&self, account: &Account) -> Option<(BlockHash, Amount)> {
        let entries = self.receivable.get(account)?;
        entries.first().cloned()
    }

    pub fn next_receivable(&self) -> Option<(Account, BlockHash, Amount)> {
        self.receivable
            .iter()
            .next()
            .and_then(|(account, entries)| {
                entries
                    .iter()
                    .next()
                    .map(|(hash, amount)| (*account, *hash, *amount))
            })
    }

    pub fn random_account_that_can_send(&self) -> Option<&AccountState> {
        self.accounts_that_can_send
            .iter()
            .next()
            .and_then(|account| self.account_states.get(account))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ntest::assert_false;

    #[test]
    fn empty() {
        let map = AccountMap::default();
        assert_eq!(map.get_receivable(&1.into()), None);
        assert_eq!(map.next_receivable(), None);
        assert_false!(map.contains(&1.into()));
        assert_eq!(map.random_account(), None);
        assert!(map.state(&Account::from(1)).is_none());
        assert!(map.random_account_that_can_send().is_none());
    }

    #[test]
    fn add_one_account() {
        let mut map = AccountMap::default();
        let key = PrivateKey::from(1);

        map.add_unopened(key.clone());

        assert!(map.contains(&key.account()));
        assert_eq!(
            map.state(&key.account()).unwrap().key.account(),
            key.account()
        );
        assert_eq!(map.random_account(), Some(key.account()));
        assert!(map.random_account_that_can_send().is_none());
    }

    #[test]
    fn process_send() {
        let mut map = AccountMap::default();
        let send_hash = BlockHash::from(42);
        let dest_key = PrivateKey::from(100);
        let dest_account = dest_key.account();
        let amount = Amount::nano(12_345);
        map.add_unopened(dest_key.clone());

        map.process_send(TEST_GENESIS_ACCOUNT, dest_account, send_hash, amount);

        assert_eq!(map.get_receivable(&dest_account), Some((send_hash, amount)));
        assert_eq!(
            map.next_receivable(),
            Some((dest_account, send_hash, amount))
        );
        assert!(map.random_account_that_can_send().is_none());
        assert_eq!(map.state(&dest_account).unwrap().balance, Amount::zero());
    }

    #[test]
    fn process_send_reduces_balance_of_sender() {
        let mut map = AccountMap::default();
        let key = PrivateKey::from(100);

        map.add_unopened(key.clone());

        let send_genesis_hash = BlockHash::from(42);
        let send_hash = BlockHash::from(43);

        let amount = Amount::nano(12_345);

        map.process_send(
            TEST_GENESIS_ACCOUNT,
            key.account(),
            send_genesis_hash,
            amount,
        );
        map.process_receive(&key.account(), &send_genesis_hash);

        map.process_send(key.account(), key.account(), send_hash, Amount::nano(1));

        assert_eq!(
            map.state(&key.account()).unwrap().balance,
            Amount::nano(12_344)
        );
    }

    #[test]
    fn remove_from_sendable_accounts_when_complete_balance_sent() {
        let mut map = AccountMap::default();
        let key_a = PrivateKey::from(100);
        let key_b = PrivateKey::from(200);

        let account_a = key_a.account();
        let account_b = key_a.account();

        map.add_unopened(key_a.clone());
        map.add_unopened(key_b.clone());

        let send_hash_a = BlockHash::from(42);
        let send_hash_b = BlockHash::from(43);

        let amount = Amount::nano(12_345);

        map.process_send(TEST_GENESIS_ACCOUNT, account_a, send_hash_a, amount);
        map.process_receive(&account_a, &send_hash_a);

        map.process_send(account_a, account_b, send_hash_b, amount);

        assert_false!(map.accounts_that_can_send.contains(&account_a));
    }

    #[test]
    fn process_receive() {
        let mut map = AccountMap::default();
        let send_hash = BlockHash::from(42);
        let dest_key = PrivateKey::from(100);
        let dest_account = dest_key.account();
        let amount = Amount::nano(12_345);
        map.add_unopened(dest_key.clone());

        map.process_send(TEST_GENESIS_ACCOUNT, dest_account, send_hash, amount);
        map.process_receive(&dest_account, &send_hash);

        assert!(map.next_receivable().is_none());
        assert_eq!(map.state(&dest_account).unwrap().balance, amount);
        assert_eq!(
            map.random_account_that_can_send().unwrap().key.account(),
            dest_account
        );
    }

    const TEST_GENESIS_ACCOUNT: Account = Account::from_bytes([1; 32]);
}
