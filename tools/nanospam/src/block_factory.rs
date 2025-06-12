use rand::seq::IndexedRandom;
use rsnano_core::{Account, Amount, Block, BlockHash, PendingInfo, PrivateKey, StateBlockArgs};
use std::collections::{HashMap, HashSet};

#[derive(Default)]
pub(crate) struct AccountMap {
    accounts: HashMap<Account, AccountState>,
    accounts_vec: Vec<Account>,
    empty: HashSet<Account>,
    non_empty: HashSet<Account>,
    receivable: HashMap<Account, Vec<PendingInfo>>,
}

impl AccountMap {
    pub(crate) fn fill(&mut self, count: usize) {
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
            key.public_key().into(),
            AccountState {
                key,
                frontier: BlockHash::zero(),
                balance: Amount::zero(),
            },
        );
    }

    pub fn random_account(&mut self) -> Account {
        self.accounts_vec.choose(&mut rand::rng()).unwrap().clone()
    }

    pub fn all_accounts_empty(&self) -> bool {
        self.non_empty.is_empty()
    }

    pub fn process(&mut self, block: &Block) {
        let account = block.account_field().unwrap();
        todo!();
    }
}

pub(crate) struct BlockFactory {
    genesis_key: PrivateKey,
    genesis_hash: BlockHash,
    max_blocks: usize,
    created: usize,
    account_map: AccountMap,
}

impl BlockFactory {
    // Send from genesis
    const INITIAL_AMOUNT_SENT: Amount = Amount::nano(100_000_000);

    pub(crate) fn new(
        genesis_key: PrivateKey,
        genesis_hash: BlockHash,
        account_map: AccountMap,
        max_blocks: usize,
    ) -> Self {
        Self {
            genesis_key,
            genesis_hash,
            max_blocks,
            created: 0,
            account_map,
        }
    }

    pub fn create_next(&mut self) -> Option<Block> {
        if self.created >= self.max_blocks {
            return None;
        }

        let source = self.account_map.random_account();
        let target = self.account_map.random_account();

        let block = if self.account_map.all_accounts_empty() {
            let block: Block = StateBlockArgs {
                key: &self.genesis_key,
                previous: self.genesis_hash,
                representative: self.genesis_key.public_key(),
                balance: Amount::MAX - Self::INITIAL_AMOUNT_SENT,
                link: target.into(),
                work: 0.into(),
            }
            .into();

            block
        } else {
            todo!();
        };

        self.account_map.process(&block);
        self.created += 1;
        Some(block)
    }
}

struct AccountState {
    key: PrivateKey,
    frontier: BlockHash,
    balance: Amount,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::LazyLock;

    static TEST_GENESIS_KEY: LazyLock<PrivateKey> = LazyLock::new(|| PrivateKey::from(42));
    const TEST_GENESIS_HASH: BlockHash = BlockHash::from_bytes([10; 32]);
    const MAX_BLOCKS: usize = 4;

    #[test]
    #[ignore = "wip"]
    fn initial_send_from_genesis() {
        let mut block_factory = BlockFactory::new(
            TEST_GENESIS_KEY.clone(),
            TEST_GENESIS_HASH,
            test_account_map(),
            MAX_BLOCKS,
        );
        let block = block_factory.create_next().unwrap();
        assert_eq!(block.account_field().unwrap(), TEST_GENESIS_KEY.account());
    }

    fn test_account_map() -> AccountMap {
        let mut map = AccountMap::default();
        map.add_unopened(1.into());
        map.add_unopened(2.into());
        map.add_unopened(3.into());
        map.add_unopened(4.into());
        map.add_unopened(5.into());
        map
    }
}
