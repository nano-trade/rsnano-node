use rand::{
    rng,
    seq::{IndexedRandom, IteratorRandom},
};
use rsnano_core::{Account, Amount, BlockHash, PrivateKey};
use std::collections::{HashMap, HashSet};

#[derive(Default)]
pub(crate) struct AccountMap {
    pub account_states: HashMap<Account, AccountState>,
    all_accounts: Vec<Account>,
    empty_accounts: HashSet<Account>,
    active_accounts: HashSet<Account>,
    active_accounts_vec: Vec<Account>,
    confirmed_accounts: HashSet<Account>,

    /// Account => Send block hash + amount sent
    receivable: HashMap<Account, Vec<(BlockHash, Amount)>>,

    /// Accounts that can receive and the send is confirmed
    /// Receiving account + send hash => amount
    confirmed_receivable: HashMap<(Account, BlockHash), Amount>,
    unconfirmed: HashMap<BlockHash, (Account, Option<Account>)>,
}

pub(crate) struct AccountState {
    pub key: PrivateKey,
    pub confirmed_frontier: BlockHash,
    pub unconfirmed_frontier: BlockHash,
    pub balance: Amount,
}

impl AccountState {
    pub fn confirmed(&self) -> bool {
        self.confirmed_frontier == self.unconfirmed_frontier
    }
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
                confirmed_frontier: BlockHash::zero(),
                unconfirmed_frontier: BlockHash::zero(),
                balance: Amount::zero(),
            },
        );
        self.confirmed_accounts.insert(account);
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
            state.unconfirmed_frontier = send_hash;
            state.balance -= amount;
        }
        self.unconfirmed
            .insert(send_hash, (source, Some(destination)));
        self.confirmed_accounts.remove(&source);

        if self.active_accounts.insert(destination) {
            self.active_accounts_vec.push(destination);
        }
    }

    pub fn process_receive(
        &mut self,
        receiver: Account,
        send_hash: BlockHash,
        receive_hash: BlockHash,
    ) {
        let entries = self
            .receivable
            .get_mut(&receiver)
            .expect("no receivables found");

        let pos = entries
            .iter()
            .position(|(hash, _)| *hash == send_hash)
            .expect("no receivable entry found for given send hash");

        let (send_hash, amount) = entries.remove(pos);

        if entries.is_empty() {
            self.receivable.remove(&receiver);
        }
        self.confirmed_receivable.remove(&(receiver, send_hash));
        self.confirmed_accounts.remove(&receiver);

        let state = self.account_states.get_mut(&receiver).unwrap();
        state.balance += amount;
        state.unconfirmed_frontier = receive_hash;
        self.unconfirmed.insert(receive_hash, (receiver, None));
    }

    pub fn confirm(&mut self, hash: BlockHash) {
        let Some((account, destination)) = self.unconfirmed.remove(&hash) else {
            return;
        };

        if let Some(dest) = destination {
            if let Some(entries) = self.receivable.get(&dest) {
                if let Some((_, amount)) = entries.iter().find(|(h, _)| *h == hash) {
                    self.confirmed_receivable.insert((dest, hash), *amount);
                }
            }
        }

        let Some(state) = self.account_states.get_mut(&account) else {
            return;
        };
        state.confirmed_frontier = hash;
        if state.confirmed() {
            self.confirmed_accounts.insert(account);
        }
    }

    pub fn contains(&self, account: &Account) -> bool {
        self.account_states.contains_key(account)
    }

    pub fn get_receivable(&self, account: &Account) -> Option<(BlockHash, Amount)> {
        let entries = self.receivable.get(account)?;
        entries.first().cloned()
    }

    pub fn next_receivable(&self) -> Option<(Account, BlockHash, Amount)> {
        self.confirmed_receivable
            .iter()
            .find_map(|((receiving_account, send_hash), amount)| {
                if self.confirmed_accounts.contains(receiving_account) {
                    Some((*receiving_account, *send_hash, *amount))
                } else {
                    None
                }
            })
    }

    pub fn random_account_that_can_send(&self) -> Option<&AccountState> {
        for _ in 0..100 {
            let account = self.active_accounts_vec.iter().choose(&mut rng())?;
            let state = self.account_states.get(account).unwrap();
            if state.confirmed() && !state.balance.is_zero() {
                return Some(state);
            }
        }
        None
    }

    pub fn should_send_genesis(&self) -> bool {
        self.empty_accounts.len() == self.all_accounts.len()
            && self.receivable.is_empty()
            && self.unconfirmed.is_empty()
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
        map.confirm(send_hash);

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
        let receive_hash = BlockHash::from(44);

        let amount = Amount::nano(12_345);

        map.process_send(
            TEST_GENESIS_ACCOUNT,
            key.account(),
            send_genesis_hash,
            amount,
        );
        map.confirm(send_genesis_hash);
        map.process_receive(key.account(), send_genesis_hash, receive_hash);
        map.confirm(receive_hash);
        map.process_send(key.account(), key.account(), send_hash, Amount::nano(1));
        map.confirm(send_hash);

        assert_eq!(
            map.state(&key.account()).unwrap().balance,
            Amount::nano(12_344)
        );
        assert_eq!(
            map.state(&key.account()).unwrap().confirmed_frontier,
            send_hash
        );
    }

    #[test]
    fn process_receive() {
        let mut map = AccountMap::default();
        let send_hash = BlockHash::from(42);
        let receive_hash = BlockHash::from(43);
        let dest_key = PrivateKey::from(100);
        let dest_account = dest_key.account();
        let amount = Amount::nano(12_345);
        map.add_unopened(dest_key.clone());

        map.process_send(TEST_GENESIS_ACCOUNT, dest_account, send_hash, amount);
        map.confirm(send_hash);
        map.process_receive(dest_account, send_hash, receive_hash);
        map.confirm(receive_hash);

        assert!(map.next_receivable().is_none());
        assert_eq!(map.state(&dest_account).unwrap().balance, amount);
        assert_eq!(
            map.state(&dest_account).unwrap().confirmed_frontier,
            receive_hash
        );
        assert_eq!(
            map.random_account_that_can_send().unwrap().key.account(),
            dest_account
        );
    }

    const TEST_GENESIS_ACCOUNT: Account = Account::from_bytes([1; 32]);
}
