use crate::account_map::AccountMap;
use rand::Rng;
use rsnano_core::{Amount, Block, BlockHash, PrivateKey, StateBlockArgs};

pub(crate) struct BlockFactory {
    genesis_key: PrivateKey,
    genesis_hash: BlockHash,
    max_blocks: usize,
    created: usize,
    account_map: AccountMap,
}

pub(crate) enum BlockResult {
    Block(Block),
    Waiting,
}

impl BlockResult {
    pub fn unwrap(self) -> Block {
        match self {
            BlockResult::Block(block) => block,
            BlockResult::Waiting => panic!("Expected block, but was in waiting state"),
        }
    }
}

impl BlockFactory {
    // Send from genesis
    pub const INITIAL_AMOUNT_SENT: Amount = Amount::nano(100_000_000);

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

    pub fn create_next(&mut self) -> Option<BlockResult> {
        if self.created >= self.max_blocks {
            return None;
        }

        if let Some((receiver, send_hash, amount_sent)) = self.account_map.next_receivable() {
            let state = self.account_map.state(&receiver).unwrap();
            assert!(state.confirmed());
            let receive: Block = StateBlockArgs {
                key: &state.key,
                previous: state.confirmed_frontier,
                representative: state.key.public_key(),
                balance: state.balance + amount_sent,
                link: send_hash.into(),
                work: 0.into(),
            }
            .into();

            self.account_map
                .process_receive(receiver, send_hash, receive.hash());

            self.created += 1;
            Some(BlockResult::Block(receive))
        } else {
            if let Some(state) = self.account_map.random_account_that_can_send() {
                assert!(state.confirmed());
                let destination = self.account_map.random_account().unwrap();
                let new_balance: Amount = rand::rng().random_range(..state.balance.number()).into();
                let amount_sent = state.balance - new_balance;

                let send: Block = StateBlockArgs {
                    key: &state.key,
                    previous: state.confirmed_frontier,
                    representative: state.key.public_key(),
                    balance: new_balance,
                    link: destination.into(),
                    work: 0.into(),
                }
                .into();

                self.account_map.process_send(
                    state.key.account(),
                    destination,
                    send.hash(),
                    amount_sent,
                );
                self.created += 1;

                Some(BlockResult::Block(send))
            } else if self.account_map.should_send_genesis() {
                let destination = self.account_map.random_account().unwrap();

                // Initial send from genesis account
                let genesis_send: Block = StateBlockArgs {
                    key: &self.genesis_key,
                    previous: self.genesis_hash,
                    representative: self.genesis_key.public_key(),
                    balance: Amount::MAX - Self::INITIAL_AMOUNT_SENT,
                    link: destination.into(),
                    work: 0.into(),
                }
                .into();

                self.account_map.process_send(
                    self.genesis_key.account(),
                    destination,
                    genesis_send.hash(),
                    Self::INITIAL_AMOUNT_SENT,
                );

                self.created += 1;
                Some(BlockResult::Block(genesis_send))
            } else {
                Some(BlockResult::Waiting)
            }
        }
    }

    pub fn confirm(&mut self, hash: BlockHash) {
        self.account_map.confirm(hash);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{sync::LazyLock, time::Instant};

    static TEST_GENESIS_KEY: LazyLock<PrivateKey> = LazyLock::new(|| PrivateKey::from(42));
    const TEST_GENESIS_HASH: BlockHash = BlockHash::from_bytes([10; 32]);
    const MAX_BLOCKS: usize = 4;

    #[test]
    fn initial_send_from_genesis_to_random_account() {
        let mut block_factory = BlockFactory::new(
            TEST_GENESIS_KEY.clone(),
            TEST_GENESIS_HASH,
            test_account_map(),
            MAX_BLOCKS,
        );
        let block = block_factory.create_next().unwrap().unwrap();
        let account = block.account_field().unwrap();
        let destination = block.destination_or_link();

        assert_eq!(account, TEST_GENESIS_KEY.account());
        assert_eq!(
            block.balance_field().unwrap(),
            Amount::MAX - BlockFactory::INITIAL_AMOUNT_SENT
        );
        assert!(block_factory.account_map.contains(&destination));
        assert!(block_factory
            .account_map
            .get_receivable(&destination)
            .is_some());
    }

    #[test]
    fn receive_from_genesis() {
        let mut block_factory = BlockFactory::new(
            TEST_GENESIS_KEY.clone(),
            TEST_GENESIS_HASH,
            test_account_map(),
            MAX_BLOCKS,
        );
        // genesis send
        let send_genesis = block_factory.create_next().unwrap().unwrap();
        block_factory.confirm(send_genesis.hash());
        let account = send_genesis.destination_or_link();

        let receive = block_factory.create_next().unwrap().unwrap();
        assert_eq!(receive.account_field().unwrap(), account);
        assert_eq!(receive.link_field().unwrap(), send_genesis.hash().into());
    }

    #[test]
    fn send_from_random_account() {
        let mut block_factory = BlockFactory::new(
            TEST_GENESIS_KEY.clone(),
            TEST_GENESIS_HASH,
            test_account_map(),
            MAX_BLOCKS,
        );
        let send_genesis = block_factory.create_next().unwrap().unwrap();
        let account_a = send_genesis.destination_or_link();
        block_factory.confirm(send_genesis.hash());
        let receive_genesis = block_factory.create_next().unwrap().unwrap();
        block_factory.confirm(receive_genesis.hash());
        let send = block_factory.create_next().unwrap().unwrap();
        block_factory.confirm(send.hash());
        let account_b = send.destination_or_link();
        assert_eq!(
            send.account_field().unwrap(),
            account_a,
            "incorrect send account"
        );
        assert!(block_factory.account_map.contains(&account_b));
        assert!(block_factory
            .account_map
            .get_receivable(&account_b)
            .is_some());
    }

    #[test]
    fn send_last_raw() {
        let mut account_map = test_account_map();
        let key = PrivateKey::from(100);
        let send_hash = BlockHash::from(1);
        let receive_hash = BlockHash::from(2);
        account_map.add_unopened(key.clone());
        account_map.process_send(
            TEST_GENESIS_KEY.account(),
            key.account(),
            send_hash,
            Amount::raw(1),
        );
        account_map.confirm(send_hash);
        account_map.process_receive(key.account(), send_hash, receive_hash);
        account_map.confirm(receive_hash);

        let mut block_factory = BlockFactory::new(
            TEST_GENESIS_KEY.clone(),
            TEST_GENESIS_HASH,
            account_map,
            MAX_BLOCKS,
        );

        let block = block_factory.create_next().unwrap().unwrap();
        assert_eq!(block.balance_field().unwrap(), Amount::zero());
        assert_eq!(
            block_factory
                .account_map
                .get_receivable(&block.destination_or_link())
                .unwrap()
                .1,
            Amount::raw(1)
        );
    }

    #[test]
    #[ignore = "run manually only"]
    fn benchmark() {
        let mut account_map = AccountMap::default();
        for _ in 0..30_000 {
            account_map.add_unopened(PrivateKey::new());
        }

        let block_count = 10_000_000;

        let mut block_factory = BlockFactory::new(
            TEST_GENESIS_KEY.clone(),
            TEST_GENESIS_HASH,
            account_map,
            block_count,
        );

        let mut start = Instant::now();
        let mut created_batch = 0;
        while let Some(BlockResult::Block(b)) = block_factory.create_next() {
            block_factory.confirm(b.hash());
            created_batch += 1;
            if created_batch == 50_000 {
                println!(
                    "Created {} blocks. {} bps",
                    created_batch,
                    (created_batch as f64 / start.elapsed().as_secs_f64()) as i32
                );
                start = Instant::now();
                created_batch = 0;
            }
        }
        println!(
            "Created {} blocks. {} bps",
            created_batch,
            (created_batch as f64 / start.elapsed().as_secs_f64()) as i32
        );
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
